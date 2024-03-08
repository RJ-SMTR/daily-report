"""
Classes for retrieving or inserting data
from/to external sources
"""
from datetime import datetime, timedelta
import os

from prefect.client import Client
from datetime import datetime



class PrefectAPI:
    """
    Retrives data from prefect API
    """

    def __init__(self):
        self.client = Client(
            api_server=os.getenv("PREFECT_API_SERVER"),
            api_key=os.getenv("PREFECT_API_KEY"),
            tenant_id=os.getenv("PREFECT_TENANT_ID"),
        )

    def get_tenants(self):
        query = '''
            query {
                tenant {
                id,
                name
                }
            }
        '''

        result = self.client.graphql(query=query)
        return result['data']['tenant']

    def get_daily_report(self, now: datetime, tenant_id: str):
        start = now - timedelta(days=1)
        stop = now
        is_schedule_active = True
        project = 'production'

        fail_count_query = '''
            query (
                $tenant_id:uuid!,
                $is_schedule_active: Boolean!,
                $project: String!,
                $min_start_time: timestamptz!,
                $max_start_time: timestamptz!,
                $state: String!
                ){ 
                flow (
                    where: {
                    tenant_id: {_eq: $tenant_id},
                    is_schedule_active: {_eq: $is_schedule_active},
                    project: {name: {_eq: $project}},
                    
                    }
                ){
                    id,
                    name,
                    flow_runs_aggregate (where: {
                        scheduled_start_time: {
                            _gte: $min_start_time,
                            _lt: $max_start_time,
                        },
                        idempotency_key: {_like: "%auto-scheduled%"},
                        state: {_eq: $state}
                    }){
                    aggregate {
                        count
                    }
                    }
                    } 
                }
        '''

        fail_count_args = {
            "tenant_id": tenant_id,
            "is_schedule_active": is_schedule_active,
            "project": project,
            "min_start_time": start.isoformat(),
            "max_start_time": stop.isoformat(),
            "state": "Failed"
        }

        failed_result = self.client.graphql(query=fail_count_query, variables=fail_count_args)
        
        total_count_query = '''
            query (
                $tenant_id: uuid!,
                $is_schedule_active: Boolean!,
                $project: String!,
                $min_start_time: timestamptz!,
                $max_start_time: timestamptz!
                ){ 
                flow (
                    where: {
                    tenant_id: {_eq: $tenant_id},
                    is_schedule_active: {_eq: $is_schedule_active},
                    project: {name: {_eq: $project}},
                    
                    }
                ){
                    id,
                    name,
                    flow_runs_aggregate (where: {
                        scheduled_start_time: {
                            _gte: $min_start_time,
                            _lt: $max_start_time,
                        },
                        idempotency_key: {_like: "%auto-scheduled%"},
                        state: {_neq: "Cancelled"},
                    }){
                    aggregate {
                        count
                    }
                    }
                    } 
                }
        '''

        total_count_args = {
            "tenant_id": tenant_id,
            "is_schedule_active": is_schedule_active,
            "project": project,
            "min_start_time": start.isoformat(),
            "max_start_time": stop.isoformat(),
            "state": "Failed"
        }

        total_result = self.client.graphql(query=total_count_query, variables=total_count_args)

        return failed_result, total_result

    def get_flow_schedules(self, tenant_id: str = '', archived:bool=False, is_schedule_active:bool=True, project:str='production'):
        
        query = '''
            query (
                $tenant_id: uuid!,
                $exclude: String!,
                $archived: Boolean!,
                $is_schedule_active: Boolean!,
                $project: String!,                
                ) {
                flow (
                    where: {
                    name: {_nlike: $exclude},
                    tenant_id: {_eq: $tenant_id},
                    archived: {_eq: $archived},
                    is_schedule_active: {_eq: $is_schedule_active},
                    project: {name: {_eq: $project}},
                    }
                ) {
                    id,
                    name,
                    schedule,
                    version
                }
            }
        '''

        variables = {
            'tenant_id': tenant_id,
            'exclude': '%(subflow)%',
            'archived': archived,
            'is_schedule_active': is_schedule_active,
            'project': project,
        }

        result = self.client.graphql(query=query, variables=variables)
        return result['data']['flow']

    def get_yesterday_flow_runs(self, now: datetime, tenant_id: str):
        start = now - timedelta(days=1)
        stop = now
        is_schedule_active = True
        project = 'production'
        archived=False

        total_count_query = '''
            query (
                $tenant_id:uuid!,
                $exclude: String!,
                $project: String!,
                $min_start_time: timestamptz!,
                $max_start_time: timestamptz!){ 
                    flow (
                    where: {
                        tenant_id: {_eq: $tenant_id},
                        name: {_nlike: $exclude},
                        project: {name: {_eq: $project}},
                    }){
                        id,
                        name,
                        version
                        project {
                        name
                        }
                        flow_runs_aggregate (
                        where: {
                            scheduled_start_time: {
                            _gte: $min_start_time,
                            _lt: $max_start_time,
                            },
                            state: {
                              _nin: ["Scheduled", "Cancelled", "Submitted"]
                            }
                        }) {
                            aggregate{
                            count
                            }
                        }
                }
            }

        '''

        total_count_args = {
            "tenant_id": tenant_id,
            "exclude": "%(subflow)%",
            "project": project,
            "min_start_time": start.isoformat(),
            "max_start_time": stop.isoformat()
        }

        total_result = self.client.graphql(query=total_count_query, variables=total_count_args)
        return total_result['data']['flow']

    def get_scheduled_count(self, now: datetime, tenant_id: str):
        start = now - timedelta(days=1)
        stop = now
        is_schedule_active = True
        project = 'production'
        archived=False

        total_count_query = '''
            query (
                $tenant_id:uuid!,
                $exclude: String!,
                $project: String!,
                $min_start_time: timestamptz!,
                $max_start_time: timestamptz!){ 
                    flow (
                    where: {
                        tenant_id: {_eq: $tenant_id},
                        name: {_nlike: $exclude},
                        project: {name: {_eq: $project}},
                    }){
                        id,
                        name,
                        version
                        project {
                        name
                        }
                        flow_runs_aggregate (
                        where: {
                            scheduled_start_time: {
                            _gte: $min_start_time,
                            _lt: $max_start_time,
                            },
                            state: {
                              _nin: ["Cancelled"]
                            }
                        }) {
                            aggregate{
                            count
                            }
                        }
                }
            }

        '''

        total_count_args = {
            "tenant_id": tenant_id,
            "exclude": "%(subflow)%",
            "project": project,
            "min_start_time": start.isoformat(),
            "max_start_time": stop.isoformat()
        }

        total_result = self.client.graphql(query=total_count_query, variables=total_count_args)
        return total_result['data']['flow']

    def get_flows_versions(self, tenant_id: str = '', project: str = 'production') -> dict:
        query = '''
            query (
                $tenant_id:uuid!,
                $project: String!,
                $exclude: String!,
                ){
                flow (
                    where: 
                    {
                    tenant_id: {_eq: $tenant_id},
                    name: {_nlike: $exclude}, 
                    project: {name: {_eq: $project}}
                    },
                    order_by: {name: asc, version: desc}) {
                        id,
                        name,
                        created,
                        version,      
                }
            }
            '''
        args = {
            "tenant_id": tenant_id,
            "project": project,
            "exclude": "%(subflow)%",
        }

        result = self.client.graphql(query=query, variables=args)
        return result['data']['flow']
    
