import json
import logging
import numpy as np
import os
import pandas as pd
import pytz
import requests
import sys
import traceback


from celery import Celery
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from datetime import datetime, timedelta
from dateutil import parser
from prefect.client import Client


app = Celery('main', broker=os.getenv('REDIS_CELERY'))
app.conf.timezone = 'UTC'

p_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('logs.log')
p_logger.addHandler(file_handler)
c_logger = get_task_logger(__name__)

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(crontab(hour=11, minute=0),
                             daily_report.s(), name='Daily Report')


def get_data(**kwargs):
    query = '''
        query (
            $tenant_id: uuid!,
            $project: String!,
            $exclude: String!,
            $min_start_time: timestamptz!,
            $max_start_time: timestamptz!
        ){ 
        flow (
            where: {
            tenant_id: {_eq: $tenant_id},
            name: {_nlike: $exclude},
            project: {name: {_eq: $project}},
            }
            ){
                id,
                name,
                    is_schedule_active,
                    archived,
                created,
                version,
                schedule,
                flow_runs (where: {
                scheduled_start_time: {
                    _gte: $min_start_time,
                    _lt: $max_start_time,
                }
                }){
                    id,
                    flow_id,
                    state,
                    scheduled_start_time,
                    idempotency_key
                }        
            }
        }
    '''

    args = {
        'tenant_id': kwargs['tenant_id'],
        'exclude': kwargs['exclude'],
        'project': kwargs['project_name'],
        'min_start_time': kwargs['start_time'],
        'max_start_time': kwargs['end_time']
    }

    client = Client(
        api_server=kwargs['api_server'],
        api_key=kwargs['api_key'],
        tenant_id=kwargs['tenant_id'],
    )

    result = client.graphql(query=query, variables=args)

    flows = pd.json_normalize(result['data']['flow'])
    flows = flows.rename(columns={'id': 'flow_id'})
    flow_runs = pd.DataFrame()
    for _, flow in flows.iterrows():
        df_flow_run = pd.DataFrame(flow['flow_runs'])
        df_flow_run['flow_name'] = flow['name']
        df_flow_run['version'] = flow['version']
        flow_runs = pd.concat([flow_runs, df_flow_run])
    
    return flows, flow_runs

def get_expected_execution_count(flows: pd.DataFrame, now: datetime) -> pd.DataFrame:

    def sum_schedules(clocks):
        total = 0
        if isinstance(clocks, list):
            for clock in clocks:
                if clock['type'] == 'IntervalClock':
                    interval = clock['interval']//1000000
                    total += len(pd.date_range(start=yesterday, end=now, freq=timedelta(seconds=interval), inclusive='left'))
        return total

    yesterday = now - timedelta(hours=24)
    flows['total_expected_runs'] = flows['schedule.clocks'].apply(sum_schedules)

    for i, flow in flows.iterrows():
        if not flow['version'] in flow['versions']:
            flows.loc[i, 'total_expected_runs'] = 0
        elif not flow['active']:
            flows.loc[i, 'total_expected_runs'] = 0
    return flows[['flow_id', 'total_expected_runs']]

def get_failed_runs(flow_runs: pd.DataFrame) -> pd.DataFrame:
    failed_runs = flow_runs[flow_runs['state'] == 'Failed']
    failed_runs = failed_runs.groupby(by=['flow_id']).count().reset_index()
    failed_runs['failed_runs'] = failed_runs['id']
    failed_runs = failed_runs[['flow_id', 'failed_runs']]
    return failed_runs

def get_execution_count(flow_runs: pd.DataFrame) -> pd.DataFrame:
    executed_runs = flow_runs[flow_runs['state'] != 'Cancelled']
    executed_runs = executed_runs.groupby(by=['flow_id']).count().reset_index()
    executed_runs['executed_runs'] = executed_runs['id']
    executed_runs = executed_runs[['flow_id', 'executed_runs']]
    return executed_runs

def get_active_flow_versions_yesterday(flows: pd.DataFrame, now: datetime) -> pd.DataFrame:
    active_versions_yesterday = {}
    flows['created'] = pd.to_datetime(flows['created'])
    flows = flows.sort_values(by=['name', 'created', 'version'], ascending=False)
    flow_names = flows['name'].unique()

    for flow_name in flow_names:
        active_versions_yesterday[flow_name] = []
        for _, row in flows[flows['name'] == flow_name].iterrows():
            if row['created'] >= now:
                pass
            elif now - timedelta(hours=24) <= row['created'] < now:
                active_versions_yesterday[flow_name].append(row['version'])
            else:
                active_versions_yesterday[flow_name].append(row['version'])
                break
    
    return pd.DataFrame(active_versions_yesterday.items(), columns=['name', 'versions'])

def get_wrong_version_runs(flow_runs: pd.DataFrame, active_flow_versions_yesterday: pd.DataFrame) -> pd.DataFrame:
    wrong_version_runs = {}
    agg_runs = flow_runs[~flow_runs['state'].isin(['Scheduled', 'Cancelled', 'Submitted'])]
    agg_runs = agg_runs.groupby(by=['flow_id', 'flow_name', 'version']).count().reset_index()
    agg_runs['count'] = agg_runs['id']
    agg_runs = agg_runs[['flow_id', 'flow_name', 'version', 'count']]
    agg_runs = agg_runs[agg_runs['count'] > 0]

    for flow_name in agg_runs['flow_name'].unique():
        ran_versions = set(agg_runs[agg_runs['flow_name'] == flow_name]['version'])
        
        if flow_name in list(active_flow_versions_yesterday['name'].unique()):
            active_versions_yesterday = set(active_flow_versions_yesterday[active_flow_versions_yesterday['name'] == flow_name].iloc[0, 1])
            wrong_versions = ran_versions.difference(active_versions_yesterday)
            count = agg_runs[(agg_runs['flow_name'] == flow_name) & (agg_runs['version'].isin(wrong_versions))]['count'].sum()
            wrong_version_runs[flow_name] = {}
            wrong_version_runs[flow_name]['wrong_versions'] = wrong_versions
            wrong_version_runs[flow_name]['count'] = count
        else:
            wrong_version_runs[flow_name] = {}
            wrong_version_runs[flow_name]['wrong_versions'] = set(agg_runs[agg_runs['flow_name'] == flow_name]['version'])
            wrong_version_runs[flow_name]['count'] = agg_runs[agg_runs['flow_name'] == flow_name]['count'].sum()  
    
    flows = list(wrong_version_runs.keys())
    values = list(wrong_version_runs.values())
    dicio = {'name': flows, 'wrong_versions_sets': list(map(lambda x : x['wrong_versions'], values)), 'wrong_versions_count': list(map(lambda x : x['count'], values))}
    df = pd.DataFrame(dicio)
    return df

def get_active_schedules(flows: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df['name'] = flows['name'].unique()
    df['active'] = None
    for i, row in df.iterrows():
        active_flows = flows[(flows['name'] == row['name']) & (flows['is_schedule_active'] == True) & (flows['archived'] == False)]
        if len(active_flows) > 0:
            df.loc[i, 'active'] = True
        else:
            df.loc[i, 'active'] = False
    
    return df

def get_md_bold_text(text):
    return f'**{text}**'

def send_message(flows_agg: pd.DataFrame, webhook: str, role_id: str, now: datetime):
    content = f'Bom dia, <@&{role_id}>! :coffee:'
    author = {
        'name': 'Reporte Diário de Pipelines',
        'url': 'http://prefect.dados.rio'
    }
    headers = {"Content-Type": "application/json"}
    timestamp = now.isoformat()
    if flows_agg.empty:
        message = {
            "content": content,
            "embeds": [{
                "color": 5941248,
                "timestamp": timestamp,
                "author": author,
                "fields": [
                    {
                        "name": ":partying_face: Nenhum problema hoje!",
                        "value": ""
                    }
                ]
            }]
            }
        data = message
        r = requests.post(webhook, headers=headers, json=data)
        c_logger.info(str(r.status_code) + r.text)
        p_logger.info(str(r.status_code) + r.text)
        return
    
    red_circle = ':red_circle:'
    yellow_circle = ':yellow_circle:'
    green_circle = ':green_circle:'

    fields = []   

    for _, item in flows_agg.iterrows():
        if item['priority'] == 2:
            emoji = red_circle
        elif item['priority'] == 1:
            emoji = yellow_circle
        else:
            emoji = green_circle
        
        fails = f"Falhas: {int(item['failed_runs'])} ({item['failed_percentage']:.1%})"
        if item['failed_runs']:
            fails = get_md_bold_text(fails)

        
        if item['not_executed'] > 0:
            not_run = f"**Não executadas: {int(item['not_executed'])} ({item['not_executed_percentage']:.1%})**"
        else:
            not_run = f"Não executadas: 0 ({0:.1%})"

        wrong_version_count = f"Runs não esperadas: {int(item['wrong_versions_count'])}"
        if item['wrong_versions_count'] > 0:
            wrong_version_count = get_md_bold_text(wrong_version_count)

        if item['wrong_versions_sets']:
            wrong_versions = f"**Versão incorreta: {item['wrong_versions_sets']}**"
        else:
            wrong_versions = "Versão incorreta: {}"

        if item['not_executed'] < 0:
            not_scheduled = f"**Não agendadas: {int(-item['not_executed'])}**"
        else:
            not_scheduled = f"Não agendadas: 0"


        value = {
            "name": f"{emoji} {item['name']}",
            "value": f"""- Runs esperadas: {item['total_expected_runs']} - {fails}, {not_run}\n- {wrong_version_count} - {wrong_versions}, {not_scheduled}"""
        }
        fields.append(value)

    
    
    max_priority = max(flows_agg['priority'])
    if max_priority == 2:
        color = 16515072
    elif max_priority == 1:
        color = 16754176
    else:
        color = 5941248
    
    message = {
        "content": content,
        "embeds": [{
            "color": color,
            "timestamp": timestamp,
            "author": author,
            "fields": fields + [{
                "name": ":link: Ver mais em:",
                "value": "http://prefect.dados.rio"
            }
            ]
        }]
        }
    
    
    data = message
    r = requests.post(webhook, headers=headers, json=data)
    c_logger.info(str(r.status_code) + r.text)
    p_logger.info(str(r.status_code) + r.text)

def generate_daily_report(now: datetime, config: dict):
    start_time = now - timedelta(hours=24)
    query_args = {
        'api_server': config['api_server'],
        'api_key': config['api_key'],
        'tenant_id': config['tenant_id'],
        'project_name': config['project_name'],
        'exclude': '%(subflow)%',
        'start_time': start_time.isoformat(),
        'end_time': now.isoformat(),
    }


    flows, flow_runs = get_data(**query_args)
    if flow_runs.empty:
        return flow_runs
    
    failed_runs = get_failed_runs(flow_runs)
    executed_runs = get_execution_count(flow_runs)
    active_flow_versions_yesterday = get_active_flow_versions_yesterday(flows, now)
    wrong_version_runs = get_wrong_version_runs(flow_runs, active_flow_versions_yesterday)
    active_schedules = get_active_schedules(flows)


    flows = flows.merge(active_schedules, how='left', on='name')

    flows = flows.merge(failed_runs, how='left')
    flows['failed_runs'] = flows['failed_runs'].fillna(0)

    flows = flows.merge(executed_runs, how='left')
    flows['executed_runs'] = flows['executed_runs'].fillna(0)

    flows = flows.merge(active_flow_versions_yesterday, how='left', on='name')
    flows['versions'] = flows['versions'].apply(lambda x: x if isinstance(x, list) else [])

    flows = flows.merge(wrong_version_runs, how='left')
    flows['wrong_versions_sets'] = flows['wrong_versions_sets'].apply(lambda x: x if isinstance(x, set) else set())
    flows['wrong_versions_count'] = flows['wrong_versions_count'].fillna(0)

    expected_execution_count = get_expected_execution_count(flows, now)
    flows = flows.merge(expected_execution_count, how='left')

    flows_agg = flows.groupby(by=['name']).agg(
        {'failed_runs': 'sum',
        'executed_runs': 'sum',
        'versions': lambda x : x.iloc[0],
        'wrong_versions_sets': lambda x : x.iloc[0],
        'wrong_versions_count': lambda x : x.iloc[0],
        'total_expected_runs': 'max',
        'active': 'max',
        }).reset_index()

    filter_fail = (flows_agg['failed_runs'] > 0)
    filter_exec_runs = (flows_agg['executed_runs'] != flows_agg['total_expected_runs'])
    filter_wr_versions = (flows_agg['wrong_versions_count'] > 0)
    flows_agg = flows_agg[filter_fail | filter_exec_runs | filter_wr_versions]

    flows_agg.loc[:, 'failed_percentage'] = flows_agg['failed_runs']/flows_agg['executed_runs']
    flows_agg.loc[:, 'not_executed'] = flows_agg['total_expected_runs'] - flows_agg['executed_runs']
    flows_agg.loc[:, 'not_executed_percentage'] = flows_agg['not_executed']/flows_agg['total_expected_runs']
    flows_agg.loc[~np.isfinite(flows_agg['not_executed_percentage']), 'not_executed_percentage'] = 0
    flows_agg['priority'] = None

    for i, row in flows_agg.iterrows():
        if row['failed_percentage'] >= 0.2 or row['executed_runs'] < row['total_expected_runs']:
            flows_agg.loc[i, 'priority'] = 2
        elif row['failed_percentage'] >= 0.1:
            flows_agg.loc[i, 'priority'] = 1
        else:
            flows_agg.loc[i, 'priority'] = 0
    
    flows_agg = flows_agg.sort_values(by=['priority'], ascending=False)
    return flows_agg

@app.task
def daily_report(now: datetime = None):
    if now is None:
        now = datetime.now(pytz.timezone("America/Sao_Paulo"))
    with open('config.json') as fp:
        configs = json.load(fp)
        
        for config in configs:
            try:
                report = generate_daily_report(now, config)
                send_message(report, config['webhook'], config['discord_role_id'], now)
            except Exception:
                c_logger.error(traceback.format_exc())
                p_logger.error(traceback.format_exc())


   
if __name__ == '__main__':
    
    if len(sys.argv) > 1:
        now = parser.parse(sys.argv[1]).astimezone(
                        pytz.timezone("America/Sao_Paulo")
        )
        daily_report(now)
    else:
        daily_report()
