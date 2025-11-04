import json
import os
import requests
from google.cloud import bigquery
from google.oauth2 import service_account


def auth(url: str, email: str | None, password: str | None) -> str:
    mutation = f"""
            mutation TokenAuth($email: String!, $password: String!) {{
                    tokenAuth(email: $email, password: $password) {{
                    token
                    payload
                    refreshExpiresIn
                }}
            }}
            """
    variables = json.dumps({"email": email, "password": password})
    payload = {"query": mutation, "variables": variables}

    r = requests.post(url, data=payload)

    return r.json()["data"]["tokenAuth"]["token"]


def get_datasets_catalog(url: str, token: str) -> dict:

    query = """
        query {
          allDataset {
            edges {
              node {
                _id,
                name,
                description
              }
            }
          }
        }
        """
    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": query}
    )
    elems = list(map(lambda x: x["node"], r.json()["data"][f"allDataset"]["edges"]))
    new_dict = {}
    for elem in elems:
        new_dict[elem["name"]] = {"id": elem["_id"], "description": elem["description"]}

    return new_dict


def get_datasets_bq(client: bigquery.Client) -> dict:
    project_id = "rj-smtr"
    datasets = client.list_datasets(project=project_id)
    exclude_staging = True
    exclude_test = True
    exclude_logs = True
    new_dict = {}
    for dataset in datasets:
        dataset_id: str = dataset.dataset_id
        if dataset_id != "bilhetagem":
            continue
        if exclude_staging and dataset_id.endswith("_staging"):
            continue
        if exclude_test and "test" in dataset_id:
            continue
        if exclude_logs and (
            dataset_id.startswith("logs_") or dataset_id.endswith("_logs")
        ):
            continue
        dataset_obj = client.get_dataset(f"{project_id}.{dataset_id}")
        new_dict[dataset_id] = {"description": dataset_obj.description}

    return new_dict


def delete_dataset(url: str, dataset_id: str, token: str) -> dict:
    mutation = """
        mutation {
          DeleteDataset(
            id: "%s"
          ) {
            ok,
            errors
          } 
        }
"""

    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        data={"query": mutation % dataset_id},
    )
    print(r.text)
    return r.json()


def insert_dataset(
    url: str, dataset_name: str, dataset_description: str, token: str
) -> dict:
    mutation = '''
        mutation {
          CreateUpdateDataset(
            input: {
              slug: "%s",
              name: "%s",
              organization: "e4bc48de-9dea-4fbb-9671-1bc24d37a8d4",
              themes: [],
              description: """%s"""
            }
          ) {
            clientMutationId
          } 
        }''' % (
        dataset_name,
        dataset_name,
        dataset_description.strip() if dataset_description else "",
    )

    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": mutation}
    )
    print(r.text)
    return r.json()


def update_dataset(
    url: str, dataset_catalog_id: str, dataset_description: str, token: str
) -> dict:
    mutation = '''
        mutation {
          CreateUpdateDataset(
            input: {
              id: "%s",
              description: """%s"""
            }
          ) {
            clientMutationId
          } 
        }''' % (
        dataset_catalog_id,
        dataset_description.strip() if dataset_description else "",
    )

    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": mutation}
    )
    print(r.text)
    return r.json()


def sync_datasets(
    client: bigquery.Client,
    url: str,
    datasets_catalog: dict,
    datasets_bq: dict,
    token: str,
) -> None:
    catalog_set = set(datasets_catalog.keys())
    bq_set = set(datasets_bq.keys())

    obsolete = catalog_set - bq_set
    new = bq_set - catalog_set
    intersection = catalog_set.intersection(bq_set)

    for dataset_id in obsolete:
        print(dataset_id)
        delete_dataset(url, datasets_catalog[dataset_id]["id"], token)

    for dataset_id in new:
        insert_dataset(url, dataset_id, datasets_bq[dataset_id]["description"], token)

    for dataset_id in intersection:
        if (
            datasets_bq[dataset_id]["description"]
            and datasets_bq[dataset_id]["description"].strip()
            != datasets_catalog[dataset_id]["description"]
        ):
            update_dataset(
                url,
                datasets_catalog[dataset_id]["id"],
                datasets_bq[dataset_id]["description"],
                token,
            )
        elif (
            not datasets_bq[dataset_id]["description"]
            and datasets_bq[dataset_id]["description"]
            != datasets_catalog[dataset_id]["description"]
        ):
            update_dataset(
                url,
                datasets_catalog[dataset_id]["id"],
                datasets_bq[dataset_id]["description"],
                token,
            )

    print("Dataset sync complete")


def get_tables_catalog(url: str, dataset_catalog_id: str, token: str) -> dict:

    query = """
    {
      allDataset(id: "%s") {
        edges {
          node {
            _id,
            tables {
              edges {
                node {
                  _id,
                  name,
                  description
                }
              }
            }
          }
        }
      }
    }""" % (
        dataset_catalog_id
    )
    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": query}
    )
    elems = list(
        map(
            lambda x: x["node"],
            r.json()["data"][f"allDataset"]["edges"][0]["node"]["tables"]["edges"],
        )
    )
    new_dict = {}
    for elem in elems:
        new_dict[elem["name"]] = {"id": elem["_id"], "description": elem["description"]}

    return new_dict


def get_tables_bq(client: bigquery.Client, dataset_id: str) -> dict:
    project_id = "rj-smtr"
    tables = client.list_tables(f"{project_id}.{dataset_id}")
    new_dict = {}
    for table in tables:
        table_id = table.table_id
        table_obj = client.get_table(table.full_table_id.replace(":", "."))
        partition_column = (
            table_obj.time_partitioning.field if table_obj.time_partitioning else None
        )
        new_dict[table_id] = {
            "description": table_obj.description,
            "partition": partition_column,
        }

    return new_dict


def delete_table(url: str, table_id: str, token: str) -> dict:
    mutation = (
        """
        mutation {
          DeleteTable(
            id: "%s"
          ) {
            ok,
            errors
          } 
        }"""
        % table_id
    )

    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": mutation}
    )
    print(r.text)
    return r.json()


def insert_table(
    url: str,
    table_name: str,
    table_description: str,
    dataset_catalog_id: str,
    token: str,
) -> dict:
    mutation = '''
            mutation {
      CreateUpdateTable(
        input: {
          slug: "%s",
          name: "%s",
          description: """%s""",
          dataset: "%s",          
        }
      ) {
        clientMutationId
      } 
    }''' % (
        table_name,
        table_name,
        table_description.strip() if table_description else "",
        dataset_catalog_id,
    )

    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": mutation}
    )
    print(r.text)
    return r.json()


def update_table(
    url: str, table_catalog_id: str, table_description: str, token: str
) -> dict:
    mutation = '''
        mutation {
      CreateUpdateTable(
        input: {
          id: "%s",
          description: """%s""",      
        }
      ) {
        clientMutationId
      } 
    }''' % (
        table_catalog_id,
        table_description.strip() if table_description else "",
    )

    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": mutation}
    )
    print(r.text)
    return r.json()


def sync_tables(
    client: bigquery.Client, url: str, datasets_catalog: dict, token: str
) -> None:

    for dataset_id in datasets_catalog:
        tables_catalog = get_tables_catalog(
            url, datasets_catalog[dataset_id]["id"], token
        )
        tables_bq = get_tables_bq(client, dataset_id)

        catalog_set = set(tables_catalog.keys())
        bq_set = set(tables_bq.keys())

        obsolete = catalog_set - bq_set
        new = bq_set - catalog_set
        intersection = catalog_set.intersection(bq_set)

        print(obsolete, new, intersection)

        for table_id in obsolete:
            delete_table(url, tables_catalog[table_id]["id"], token)

        for table_id in new:
            insert_table(
                url,
                table_id,
                tables_bq[table_id]["description"],
                datasets_catalog[dataset_id]["id"],
                token,
            )

        for table_id in intersection:
            if (
                tables_bq[table_id]["description"]
                and tables_bq[table_id]["description"].strip()
                != tables_catalog[table_id]["description"]
            ):
                update_table(
                    url,
                    tables_catalog[table_id]["id"],
                    tables_bq[table_id]["description"],
                    token,
                )
            elif (
                not tables_bq[table_id]["description"]
                and tables_bq[table_id]["description"]
                != tables_catalog[table_id]["description"]
            ):
                update_dataset(
                    url,
                    tables_catalog[table_id]["id"],
                    tables_bq[table_id]["description"],
                    token,
                )

    print("Table sync complete")


def get_columns_catalog(url: str, table_catalog_id: str, token: str) -> dict:
    print(table_catalog_id)
    query = """
    {
      allTable(id: "%s") {
        edges {
          node {
            _id,
            name,
            columns {
              edges {
                node {
                  _id,
                  name,
                  description,
                  bigqueryType {
                    _id,
                    name
                  },
                  isPartition
                }
              }
            }
          }
        }
      }
    }""" % (
        table_catalog_id
    )
    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": query}
    )
    elems = list(
        map(
            lambda x: x["node"],
            r.json()["data"][f"allTable"]["edges"][0]["node"]["columns"]["edges"],
        )
    )
    new_dict = {}
    for elem in elems:
        new_dict[elem["name"]] = {
            "id": elem["_id"],
            "description": elem["description"],
            "type": elem["bigqueryType"]["name"],
            "is_partition": elem["isPartition"],
        }

    return new_dict


def get_columns_bq(client: bigquery.Client, dataset_id: str, table_id: str) -> dict:
    project_id = "rj-smtr"
    table = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
    partition_column = (
        table.time_partitioning.field if table.time_partitioning else None
    )
    new_dict = {}
    for column in table.schema:
        new_dict[column.name] = {
            "type": column.field_type,
            "description": column.description,
            "is_partition": column.name == partition_column,
        }
    return new_dict


def delete_column(url: str, column_id: str, token: str) -> dict:
    mutation = (
        """
        mutation {
          DeleteColumn(
            id: "%s"
          ) {
            ok,
            errors
          } 
        }"""
        % column_id
    )

    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": mutation}
    )
    print(r.text)
    return r.json()


def insert_column(
    url: str,
    column_name: str,
    column_type: str,
    column_description: str,
    table_catalog_id: str,
    is_partition: bool,
    token: str,
) -> dict:
    types = {
        "BOOLEAN": "732966bc-8b72-4134-9fb4-590eb6d72097",
        "BYTES": "a4dffb33-947f-4c47-8be9-c1206659bd26",
        "DATE": "c40775dc-4c18-4214-bade-e0356ee5dba5",
        "DATETIME": "71949ced-2d10-4ffe-ae50-17c77e210f67",
        "FLOAT": "2b584843-e5d4-4a3b-99e4-9135e022ea52",
        "GEOGRAPHY": "65055640-b78b-4a45-b503-890164ec9b3a",
        "INTEGER": "e1ce20fa-9cfc-4492-8203-8fae1c4e950c",
        "JSON": "c07065a5-24ce-4e1b-9f33-8da4a370894b",
        "NUMERIC": "b8ea0945-adef-4956-a36e-08687b3af76d",
        "RECORD": "de365b95-28cd-473b-9d0a-7f580fbd05e0",
        "STRING": "1ace76cc-06a9-4b82-8e46-fc82e086d5e1",
        "TIME": "efc690b4-d95d-49b5-b26d-e09bb0b857f8",
        "TIMESTAMP": "43e0df69-3497-4c05-8725-a545b8c93623",
    }

    mutation = """
                mutation {
      CreateUpdateColumn(
        input: {
          name: "%s",
          bigqueryType: "%s",
          description: "%s",
          table: "%s",
          isPartition: %s
        }
      ) {
        clientMutationId
      } 
    }""" % (
        column_name,
        types[column_type],
        column_description.strip() if column_description else "",
        table_catalog_id,
        "true" if is_partition else "false",
    )

    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": mutation}
    )
    print(r.text)
    return r.json()


def update_column(
    url: str,
    column_catalog_id: str,
    column_type: str,
    column_description: str,
    is_partition: bool,
    token: str,
) -> dict:
    types = {
        "BOOLEAN": "732966bc-8b72-4134-9fb4-590eb6d72097",
        "BYTES": "a4dffb33-947f-4c47-8be9-c1206659bd26",
        "DATE": "c40775dc-4c18-4214-bade-e0356ee5dba5",
        "DATETIME": "71949ced-2d10-4ffe-ae50-17c77e210f67",
        "FLOAT": "2b584843-e5d4-4a3b-99e4-9135e022ea52",
        "GEOGRAPHY": "65055640-b78b-4a45-b503-890164ec9b3a",
        "INTEGER": "e1ce20fa-9cfc-4492-8203-8fae1c4e950c",
        "JSON": "c07065a5-24ce-4e1b-9f33-8da4a370894b",
        "NUMERIC": "b8ea0945-adef-4956-a36e-08687b3af76d",
        "RECORD": "de365b95-28cd-473b-9d0a-7f580fbd05e0",
        "STRING": "1ace76cc-06a9-4b82-8e46-fc82e086d5e1",
        "TIME": "efc690b4-d95d-49b5-b26d-e09bb0b857f8",
        "TIMESTAMP": "43e0df69-3497-4c05-8725-a545b8c93623",
    }
    mutation = """
                mutation {
      CreateUpdateColumn(
        input: {
          id: "%s",
          bigqueryType: "%s",
          description: "%s",
          isPartition: %s
        }
      ) {
        clientMutationId
      } 
    }""" % (
        column_catalog_id,
        types[column_type],
        column_description.strip() if column_description else "",
        "true" if is_partition else "false",
    )

    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": mutation}
    )
    print(r.text)
    return r.json()


def sync_columns(
    client: bigquery.Client, url: str, datasets_catalog: dict, token: str
) -> None:

    for dataset_id in datasets_catalog:
        tables_catalog = get_tables_catalog(
            url, datasets_catalog[dataset_id]["id"], token
        )

        for table_id in tables_catalog:
            columns_catalog = get_columns_catalog(
                url, tables_catalog[table_id]["id"], token
            )
            columns_bq = get_columns_bq(client, dataset_id, table_id)

            catalog_set = set(columns_catalog.keys())
            bq_set = set(columns_bq.keys())

            obsolete = catalog_set - bq_set
            new = bq_set - catalog_set
            intersection = catalog_set.intersection(bq_set)

            print(obsolete, new, intersection)

            for column_id in obsolete:
                delete_column(url, columns_catalog[column_id]["id"], token)

            for column_id in new:
                insert_column(
                    url,
                    column_id,
                    columns_bq[column_id]["type"],
                    columns_bq[column_id]["description"],
                    tables_catalog[table_id]["id"],
                    columns_bq[column_id]["is_partition"],
                    token,
                )

            for column_id in intersection:
                cond1 = (
                    columns_bq[column_id]["description"]
                    and columns_bq[column_id]["description"].strip()
                    != columns_catalog[column_id]["description"]
                )
                cond2 = (
                    not columns_bq[column_id]["description"]
                    and columns_bq[column_id]["description"]
                    != columns_catalog[column_id]["description"]
                )
                cond3 = (
                    columns_bq[column_id]["is_partition"]
                    != columns_catalog[column_id]["is_partition"]
                )
                if cond1 or cond2 or cond3:
                    update_column(
                        url,
                        columns_catalog[column_id]["id"],
                        columns_bq[column_id]["type"],
                        columns_bq[column_id]["description"],
                        columns_bq[column_id]["is_partition"],
                        token,
                    )

    print("Dataset sync complete")


def get_cloud_tables_catalog(url: str, table_id: str, token: str) -> dict:

    query = """
        {
          allCloudtable (table_Id: "%s") {
            edges {
              node {
                _id,
                gcpTableId,
                gcpDatasetId
              }
            }
          }
        }""" % (
        table_id
    )
    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": query}
    )
    elems = list(map(lambda x: x["node"], r.json()["data"][f"allCloudtable"]["edges"]))
    print(elems)
    if elems:
        return elems[0]
    else:
        return {}


def insert_cloud_table(
    url: str, table_catalog_id: str, table_name: str, dataset_name: str, token: str
) -> dict:
    mutation = """
    mutation {
      CreateUpdateCloudTable(
        input: {
          table: "%s",
          gcpTableId: "%s",
          gcpDatasetId: "%s",
          gcpProjectId: "rj-smtr",
        }
      ) {
        clientMutationId
      }
    }""" % (
        table_catalog_id,
        table_name,
        dataset_name,
    )

    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": mutation}
    )
    print(r.text)
    return r.json()


def update_cloud_table(
    url: str,
    cloud_table_id: str,
    table_catalog_id: str,
    table_name: str,
    dataset_name: str,
    token: str,
):
    mutation = """
        mutation {
          CreateUpdateCloudTable(
            input: {
              id: "%s",
              table: "%s",
              gcpTableId: "%s",
              gcpDatasetId: "%s",
              gcpProjectId: "rj-smtr",
            }
          ) {
            clientMutationId
          }
        }""" % (
        cloud_table_id,
        table_catalog_id,
        table_name,
        dataset_name,
    )

    print(mutation)
    r = requests.post(
        url, headers={"Authorization": f"Bearer {token}"}, data={"query": mutation}
    )
    print(r.text)
    return r.json()


def sync_cloud_tables(url: str, datasets_catalog: dict, token: str) -> None:

    for dataset_id in datasets_catalog:
        tables_catalog = get_tables_catalog(
            url, datasets_catalog[dataset_id]["id"], token
        )
        for table_id in tables_catalog:
            cloud_table = get_cloud_tables_catalog(
                url, tables_catalog[table_id]["id"], token
            )

            if cloud_table:
                update_cloud_table(
                    url,
                    cloud_table["_id"],
                    tables_catalog[table_id]["id"],
                    table_id,
                    dataset_id,
                    token,
                )
            else:
                insert_cloud_table(
                    url, tables_catalog[table_id]["_id"], table_id, dataset_id, token
                )
    print("Table sync complete")


def sync() -> None:
    json_acct_info = json.loads(os.getenv("CREDENTIALS"), strict=False)
    credentials = service_account.Credentials.from_service_account_info(json_acct_info)
    client = bigquery.Client(credentials=credentials)

    url = "https://api.catalogo-dados.mobilidade.rio/api/v1/graphql"
    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")
    token = auth(url, email, password)

    datasets_catalog = get_datasets_catalog(url, token)
    datasets_bq = get_datasets_bq(client)

    sync_datasets(client, url, datasets_catalog, datasets_bq, token)
    datasets_catalog = get_datasets_catalog(url, token)
    datasets_bq = get_datasets_bq(client)

    sync_tables(client, url, datasets_catalog, token)
    datasets_catalog = get_datasets_catalog(url, token)

    sync_columns(client, url, datasets_catalog, token)
    datasets_catalog = get_datasets_catalog(url, token)

    sync_cloud_tables(url, datasets_catalog, token)
