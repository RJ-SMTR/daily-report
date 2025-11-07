import json
import os
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from pydantic import BaseModel, field_validator, Field
from typing import Optional, ClassVar


class Dataset(BaseModel):
    id: Optional[str] = Field(default=None, alias="_id")
    name: str
    description: str

    def __eq__(self, other):
        if not isinstance(other, Dataset):
            return NotImplemented

        return self.name == other.name and self.description == other.description

    @field_validator("name", mode="before")
    @classmethod
    def _sanitize_name(cls, v: str) -> str:
        return (v or "").strip()

    @field_validator("description", mode="before")
    @classmethod
    def _sanitize_description(cls, v: str | None) -> str:
        return (v or "").strip()

    @classmethod
    def from_raw(cls, raw):
        if isinstance(raw, dict):
            return cls.model_validate(raw)

        if isinstance(raw, bigquery.Dataset):
            return cls(name=raw.dataset_id, description=raw.description or "")

    def sync(self, new):
        self.id = new.id


class Table(BaseModel):
    id: str | None = Field(default=None, alias="_id")
    name: str
    description: str
    dataset: Dataset

    def __eq__(self, other):
        if not isinstance(other, Table):
            return NotImplemented

        return (
            self.name == other.name
            and self.description == other.description
            and self.dataset.id == other.dataset.id
        )

    @field_validator("name", mode="before")
    @classmethod
    def _sanitize_name(cls, v: str) -> str:
        return (v or "").strip()

    @field_validator("description", mode="before")
    @classmethod
    def _sanitize_description(cls, v: str) -> str:
        return (v or "").strip()

    @classmethod
    def from_raw(cls, raw, dataset: Dataset):
        if isinstance(raw, dict):
            return cls(**raw, dataset=dataset)

        if isinstance(raw, bigquery.Table):
            return cls(
                name=raw.table_id, description=raw.description or "", dataset=dataset
            )

    def sync(self, new):
        self.id = new.id


class Type(BaseModel):
    id: str | None = Field(default=None, alias="_id")
    name: str


class TypeMap:

    _instance = None

    def __init__(self):
        self._map = {}
        self.catalog_api = CatalogAPI()
        self._populate()

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __getitem__(self, key):
        if not key in self._map:
            self.catalog_api.insert_type(key)
            self._populate()
        return self._map[key]

    def _populate(self):
        self._map = {}
        types = self.catalog_api.get_types()
        for bq_type in types:
            self._map[bq_type.name] = bq_type


class Column(BaseModel):
    id: str | None = Field(default=None, alias="_id")
    name: str
    description: str
    is_partition: bool
    type: Type
    table: Table
    _type_map: ClassVar[TypeMap | None] = None

    def __eq__(self, other):
        if not isinstance(other, Column):
            return NotImplemented

        return (
            self.name == other.name
            and self.description == other.description
            and self.is_partition == other.is_partition
            and self.type.id == other.type.id
            and self.table.id == other.table.id
        )

    @field_validator("name", mode="before")
    @classmethod
    def _sanitize_name(cls, v: str) -> str:
        return (v or "").strip()

    @field_validator("description", mode="before")
    @classmethod
    def _sanitize_description(cls, v: str) -> str:
        return (v or "").strip()

    # Isso existe meramente pra satisfazer a ordem das declaracoes
    @classmethod
    def _set_type_map(cls) -> TypeMap:
        if cls._type_map is None:
            cls._type_map = TypeMap()
        return cls._type_map

    @classmethod
    def from_raw(cls, raw, table: Table, is_partition=False):
        cls._set_type_map()
        assert cls._type_map is not None
        if isinstance(raw, dict):
            bq_type = cls._type_map[raw["bigqueryType"]["name"]]
            return cls(
                type=bq_type, is_partition=raw["isPartition"], table=table, **raw
            )

        if isinstance(raw, bigquery.SchemaField):
            bq_type = cls._type_map[raw.field_type]
            return cls(
                name=raw.name,
                description=raw.description or "",
                type=bq_type,
                table=table,
                is_partition=is_partition,
            )

    def sync(self, new):
        self.id = new.id


class CloudTable(BaseModel):
    id: str | None = Field(default=None, alias="_id")
    project_id: str = Field(default="rj-smtr", alias="gcpProjectId")
    dataset_id: str = Field(alias="gcpDatasetId")
    table_id: str = Field(alias="gcpTableId")
    table: Table

    def __eq__(self, other):
        if not isinstance(other, CloudTable):
            return NotImplemented

        return (
            self.project_id == other.project_id
            and self.dataset_id == other.dataset_id
            and self.table_id == other.table_id
        )

    @classmethod
    def from_raw(cls, raw=None, table=None):
        if isinstance(raw, dict) and table is not None:
            return cls(**raw, table=table)

        if raw is None and table is not None:
            return cls(
                gcpDatasetId=table.dataset.name, gcpTableId=table.name, table=table
            )

    def sync(self, new):
        self.id = new.id


class BigQueryAPI:
    _instance = None

    def __init__(self):
        self.client = self._get_bq_client()

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _get_bq_client(self) -> bigquery.Client:
        json_acct_info = json.loads(os.getenv("CREDENTIALS") or "", strict=False)
        credentials = service_account.Credentials.from_service_account_info(
            json_acct_info
        )
        client = bigquery.Client(credentials=credentials)
        return client

    def get_datasets(self) -> list[Dataset]:
        project_id = "rj-smtr"
        datasets = self.client.list_datasets(project=project_id)
        exclude_staging = True
        exclude_test = True
        exclude_logs = True
        dataset_list = []
        for dataset in datasets:
            dataset_id: str = dataset.dataset_id
            if dataset_id not in ["bilhetagem"]:
                continue
            if exclude_staging and dataset_id.endswith("_staging"):
                continue
            if exclude_test and "test" in dataset_id:
                continue
            if exclude_logs and (
                dataset_id.startswith("logs_") or dataset_id.endswith("_logs")
            ):
                continue
            dataset_obj = self.client.get_dataset(f"{project_id}.{dataset_id}")
            dataset_list.append(Dataset.from_raw(dataset_obj))

        return dataset_list

    def get_tables(self, dataset: Dataset) -> list[Table]:
        project_id = "rj-smtr"
        tables = self.client.list_tables(f"{project_id}.{dataset.name}")
        table_list = []
        for table in tables:
            table_obj = self.client.get_table(table.full_table_id.replace(":", "."))
            table_list.append(Table.from_raw(table_obj, dataset=dataset))
        return table_list

    def get_columns(self, table: Table) -> list[Column]:
        project_id = "rj-smtr"
        table_obj = self.client.get_table(
            f"{project_id}.{table.dataset.name}.{table.name}"
        )
        partition_column = (
            table_obj.time_partitioning.field if table_obj.time_partitioning else None
        )
        columns = []
        for column in table_obj.schema:
            columns.append(
                Column.from_raw(
                    column, is_partition=column.name == partition_column, table=table
                )
            )
        return columns


class CatalogAPI:
    _instance = None

    def __init__(self):
        self.url = os.getenv("URL") or ""
        self.email = os.getenv("EMAIL")
        self.password = os.getenv("PASSWORD")
        self.auth()

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def auth(self) -> None:
        mutation = f"""
                mutation TokenAuth($email: String!, $password: String!) {{
                        tokenAuth(email: $email, password: $password) {{
                        token
                        payload
                        refreshExpiresIn
                    }}
                }}
                """
        variables = json.dumps({"email": self.email, "password": self.password})
        payload = {"query": mutation, "variables": variables}

        r = requests.post(self.url, data=payload)
        r.raise_for_status()
        self.token = r.json()["data"]["tokenAuth"]["token"]

    def _post(self, data) -> dict:
        headers = {"Authorization": f"Bearer {self.token}"}
        r = requests.post(self.url, headers=headers, data=data)
        r.raise_for_status()
        response = r.json()
        if "errors" in data and any(
            "Signature has expired" in err["message"] for err in data["errors"]
        ):
            self.auth()
            headers = {"Authorization": f"Bearer {self.token}"}
            r = requests.post(self.url, headers=headers, data=data)
            response = r.json()
        return response

    def _delete(self, struct, id) -> dict:
        mutation = """
            mutation {
              Delete%s(
                id: "%s"
              ) {
                ok,
                errors
              } 
            }""" % (
            struct,
            id,
        )

        return self._post({"query": mutation})

    def _insert_update(self, struct: str, fields: dict) -> dict:
        args = []
        for field in fields:
            if isinstance(fields[field], bool):
                arg = f"{field}: {str(fields[field]).lower()}"
            elif isinstance(fields[field], list):
                arg = f"{field}: []"
            else:
                arg = f'{field}: """{fields[field]}"""'
            args.append(arg)
        fields_string = ",".join(args)
        mutation = """
            mutation {
              CreateUpdate%s(
                input: {
                    %s
                }
              ) {
                clientMutationId
              } 
            }""" % (
            struct,
            fields_string,
        )
        return self._post({"query": mutation})

    def _get(self, struct, fields, filter=None):
        fields_string = ",".join(fields)
        filter_string = (
            '(%s: "%s")' % (filter["field"], filter["value"]) if filter else ""
        )
        query = """
            {
              all%s %s {
                edges {
                  node {
                    %s
                  }
                }
              }
            }
        """ % (
            struct,
            filter_string,
            fields_string,
        )
        data = self._post({"query": query})
        elems = data["data"][f"all{struct}"]["edges"]
        return elems

    def delete_dataset(self, dataset: Dataset) -> dict:
        return self._delete("Dataset", dataset.id)

    def delete_table(self, table: Table) -> dict:
        return self._delete("Table", table.id)

    def delete_column(self, column: Column) -> dict:
        return self._delete("Column", column.id)

    def delete_cloud_table(self, cloud_table: CloudTable) -> dict:
        return self._delete("CloudTable", cloud_table.id)

    def insert_dataset(self, dataset: Dataset) -> dict:
        fields = {
            "slug": dataset.name,
            "name": dataset.name,
            "organization": "e4bc48de-9dea-4fbb-9671-1bc24d37a8d4",
            "themes": [],
            "description": dataset.description,
        }
        return self._insert_update("Dataset", fields)

    def insert_table(self, table: Table) -> dict:
        fields = {
            "slug": table.name,
            "name": table.name,
            "description": table.description,
            "dataset": table.dataset.id,
        }
        return self._insert_update("Table", fields)

    def insert_column(self, column: Column) -> dict:
        fields = {
            "name": column.name,
            "bigqueryType": column.type.id,
            "description": column.description,
            "table": column.table.id,
            "isPartition": column.is_partition,
        }
        return self._insert_update("Column", fields)

    def insert_cloud_table(self, cloud_table: CloudTable) -> dict:
        fields = {
            "table": cloud_table.table.id,
            "gcpTableId": cloud_table.table_id,
            "gcpDatasetId": cloud_table.dataset_id,
            "gcpProjectId": cloud_table.project_id,
        }
        return self._insert_update("CloudTable", fields)

    def insert_type(self, bq_type: Type):
        fields = {"name": bq_type.name}

        return self._insert_update("BigQueryType", fields)

    def update_dataset(self, dataset: Dataset) -> dict:
        fields = {
            "id": dataset.id,
            "description": dataset.description,
        }
        return self._insert_update("Dataset", fields)

    def update_table(self, table: Table) -> dict:
        fields = {
            "id": table.id,
            "description": table.description,
        }
        return self._insert_update("Table", fields)

    def update_column(self, column: Column) -> dict:
        fields = {
            "id": column.id,
            "bigqueryType": column.type.id,
            "description": column.description,
            "isPartition": column.is_partition,
        }
        return self._insert_update("Column", fields)

    def update_cloud_table(self, cloud_table: CloudTable) -> dict:
        fields = {
            "id": cloud_table.id,
            "table": cloud_table.table.id,
            "gcpTableId": cloud_table.table_id,
            "gcpDatasetId": cloud_table.dataset_id,
            "gcpProjectId": cloud_table.project_id,
        }
        return self._insert_update("CloudTable", fields)

    def get_datasets(self) -> list[Dataset]:
        fields = ["_id", "name", "description"]
        elems = self._get("Dataset", fields)
        datasets = []
        for elem in elems:
            datasets.append(Dataset.from_raw(elem["node"]))
        return datasets

    def get_tables(self, dataset: Dataset) -> list[Table]:
        fields = ["_id", "name", "description", "partitions"]
        filter = {"field": "dataset_Id", "value": dataset.id}
        elems = self._get("Table", fields, filter=filter)
        tables = []
        for elem in elems:
            tables.append(Table.from_raw(elem["node"], dataset=dataset))
        return tables

    def get_columns(self, table: Table) -> list[Column]:
        fields = [
            "_id",
            "name",
            "description",
            "isPartition",
            "bigqueryType {_id, name}",
        ]
        filter = {"field": "table_Id", "value": table.id}
        elems = self._get("Column", fields, filter=filter)
        columns = []
        for elem in elems:
            columns.append(Column.from_raw(elem["node"], table=table))
        return columns

    def get_cloud_tables(self, table: Table) -> list[CloudTable]:
        fields = ["_id", "gcpProjectId", "gcpDatasetId", "gcpTableId"]
        filter = {"field": "table_Id", "value": table.id}
        elems = self._get("Cloudtable", fields, filter=filter)
        cloud_tables = []
        for elem in elems:
            cloud_tables.append(CloudTable.from_raw(elem["node"], table=table))
        return cloud_tables

    def get_types(self):
        fields = ["_id", "name"]
        elems = self._get("Bigquerytype", fields)
        types = []
        for elem in elems:
            types.append(Type.model_validate(elem["node"]))
        return types


class Synchronizer:

    def __init__(self):
        self.catalog_api = CatalogAPI()
        self.bigquery_api = BigQueryAPI()

    def sync_datasets(self) -> None:
        datasets_catalog = self.catalog_api.get_datasets()
        datasets_bq = self.bigquery_api.get_datasets()
        catalog_map = {dataset.name: dataset for dataset in datasets_catalog}
        bq_map = {dataset.name: dataset for dataset in datasets_bq}

        obsolete = catalog_map.keys() - bq_map.keys()
        new = bq_map.keys() - catalog_map.keys()
        intersection = catalog_map.keys() & bq_map.keys()

        for dataset_name in obsolete:
            dataset = catalog_map[dataset_name]
            self.catalog_api.delete_dataset(dataset)

        for dataset_name in new:
            dataset = bq_map[dataset_name]
            self.catalog_api.insert_dataset(dataset)

        for dataset_name in intersection:
            dataset_catalog = catalog_map[dataset_name]
            dataset_bq = bq_map[dataset_name]
            if dataset_catalog != dataset_bq:
                dataset_bq.sync(dataset_catalog)
                self.catalog_api.update_dataset(dataset_catalog)
        print("Dataset sync complete")

    def sync_tables(self) -> None:
        datasets = self.catalog_api.get_datasets()
        for dataset in datasets:
            tables_catalog = self.catalog_api.get_tables(dataset)
            tables_bq = self.bigquery_api.get_tables(dataset)
            catalog_map = {table.name: table for table in tables_catalog}
            bq_map = {table.name: table for table in tables_bq}

            obsolete = catalog_map.keys() - bq_map.keys()
            new = bq_map.keys() - catalog_map.keys()
            intersection = catalog_map.keys() & bq_map.keys()

            for table_name in obsolete:
                table = catalog_map[table_name]
                self.catalog_api.delete_table(table)

            for table_name in new:
                table = bq_map[table_name]
                self.catalog_api.insert_table(table)

            for table_name in intersection:
                table_catalog = catalog_map[table_name]
                table_bq = bq_map[table_name]
                if table_catalog != table_bq:
                    table_bq.sync(table_catalog)
                    self.catalog_api.update_table(table_bq)

        print("Table sync complete")

    def sync_columns(self) -> None:
        datasets = self.catalog_api.get_datasets()

        for dataset in datasets:
            tables = self.catalog_api.get_tables(dataset)

            for table in tables:
                columns_catalog = self.catalog_api.get_columns(table)
                columns_bq = self.bigquery_api.get_columns(table)

                catalog_map = {column.name: column for column in columns_catalog}
                bq_map = {column.name: column for column in columns_bq}

                obsolete = catalog_map.keys() - bq_map.keys()
                new = bq_map.keys() - catalog_map.keys()
                intersection = catalog_map.keys() & bq_map.keys()

                for column_name in obsolete:
                    column = catalog_map[column_name]
                    self.catalog_api.delete_column(column)

                for column_name in new:
                    column = bq_map[column_name]
                    self.catalog_api.insert_column(column)

                for column_name in intersection:
                    column_catalog = catalog_map[column_name]
                    column_bq = bq_map[column_name]
                    if column_catalog != column_bq:
                        column_bq.sync(column_catalog)
                        self.catalog_api.update_column(column_bq)

        print("Column sync complete")

    def sync_cloud_tables(self) -> None:
        datasets = self.catalog_api.get_datasets()
        for dataset in datasets:
            tables = self.catalog_api.get_tables(dataset)
            for table in tables:
                cloud_tables = self.catalog_api.get_cloud_tables(table)
                new_ct = CloudTable.from_raw(table=table)
                if not cloud_tables:
                    assert new_ct is not None
                    self.catalog_api.insert_cloud_table(new_ct)
                    continue
                for ct in cloud_tables[:1]:
                    if new_ct != ct:
                        assert new_ct is not None
                        new_ct.sync(ct)
                        self.catalog_api.update_cloud_table(new_ct)

                for ct in cloud_tables[1:]:
                    self.catalog_api.delete_cloud_table(ct)

        print("Cloud table sync complete")

    def sync(self) -> None:
        self.sync_datasets()
        self.sync_tables()
        self.sync_columns()
        self.sync_cloud_tables()
