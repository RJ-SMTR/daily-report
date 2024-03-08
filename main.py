import os
import traceback
import pandas as pd
import pytz

from modules.data import PrefectAPI
from celery import Celery
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from datetime import datetime, timedelta
from modules.models import Schedule
from modules.utils import get_theoretical_scheduled_runs
from modules.notifications import DailyReport


app = Celery('main', broker=os.getenv('REDIS_CELERY'))
app.conf.timezone = 'UTC'
logger = get_task_logger(__name__)

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(crontab(hour=11, minute=0),
                             daily_report.s(),)

def get_failed_runs(now: datetime, prefect_api: PrefectAPI, tenant: str) -> list[dict]:
    '''
    Retorna uma lista de dicionarios com flows que falharam

    Args:
        now (datetime): Um datetime de agora  
        prefect_api (PrefectAPI): Um objeto que interage com a API do Prefect

    Returns:
        list: Lista de dicionarios com o nome do flow, a quantidade de falhas, a quantidade de execucoes totais e a porcentagem de falha
    ---
    Formato do retorno:
    [{'flow_name': 'SMTR - GPS SPPO',
    'failed_count': 144,
    'total_count': 1440,
    'percentage': 0.1}, ...]
    '''

    failed_count = {}
    total_count = {}
    failed_runs = []

    fail, total = prefect_api.get_daily_report(now, tenant)

    for flow in fail['data']['flow']:
        if not flow['name'] in failed_count:
            failed_count[flow['name']
                            ] = flow['flow_runs_aggregate']['aggregate']['count']
        else:
            failed_count[flow['name']
                            ] += flow['flow_runs_aggregate']['aggregate']['count']

    for flow in total['data']['flow']:
        if not flow['name'] in total_count:
            total_count[flow['name']
                        ] = flow['flow_runs_aggregate']['aggregate']['count']
        else:
            total_count[flow['name']
                        ] += flow['flow_runs_aggregate']['aggregate']['count']

    for flow_name in failed_count:
        if failed_count[flow_name] > 0:
            data = {
                'flow_name': flow_name,
                'failed_count': failed_count[flow_name],
                'total_count': total_count[flow_name],
                'percentage': failed_count[flow_name]/total_count[flow_name]
            }
            failed_runs.append(data)

    failed_runs.sort(key=lambda x : x['percentage'], reverse=True)
    return failed_runs

def get_expected_execution_count(now: datetime, prefect_api: PrefectAPI, tenant: str) -> dict:
    '''
    Retorna um dicionario com a quantidade de execucoes que as flows vigentes devem ter 

    Args:
        now (datetime): Um datetime de agora  
        prefect_api (PrefectAPI): Um objeto que interage com a API do Prefect

    Returns:
        dict: Dicionario onde o nome do flow eh a chave e o valor eh a quantidade esperada de execucoes
    ---
    Formato do retorno:
    {'SMTR - GPS SPPO': 1440,
    'SMTR Bilhetagem': 24,
    ...}
    '''
    schedules = {}
    times_should_have_run = {}

    flows = prefect_api.get_flow_schedules(tenant)

    for flow in flows:
        flow_name = flow['name']
        schedules[flow_name] = []
        if flow['schedule']:
            for clock in flow['schedule']['clocks'].to_list():
                clock['flow_id'] = flow['id']
                schedule = Schedule(**clock)
                schedules[flow_name].append(schedule)
        else:
            print('Schedule errada:', flow['id'])
    

    for schedule in schedules:
        times_should_have_run[schedule] = set()
        for clock in schedules[schedule]:
            scheduled_runs = get_theoretical_scheduled_runs(
                now, clock.start_date, clock.interval)
            times_should_have_run[schedule] = times_should_have_run[schedule].union(
                scheduled_runs)

    for flow in times_should_have_run:
        times_should_have_run[flow] = len(times_should_have_run[flow])
    
    return times_should_have_run

def get_ran_flow_versions(now: datetime, prefect_api: PrefectAPI, tenant: str) -> dict:
    '''
    Retorna um dicionario com as versoes das flows que foram executadas 

    Args:
        now (datetime): Um datetime de agora  
        prefect_api (PrefectAPI): Um objeto que interage com a API do Prefect

    Returns:
        dict: Dicionario onde o nome do flow eh a chave e o valor sao as versoes que executaram
    ---
    Formato do retorno:
    {'SMTR - GPS SPPO': [121, 135, 144],
    'SMTR Bilhetagem': [12, 21],
    ...}
    '''

    flow_versions = {}
    yesterday_flow_runs = prefect_api.get_yesterday_flow_runs(now, tenant)
    for run in yesterday_flow_runs:
        if run['flow_runs_aggregate']['aggregate']['count'] > 0:
            if not run['name'] in flow_versions:            
                flow_versions[run['name']] = []
            flow_versions[run['name']].append(run['version'])
    return flow_versions

def get_active_flow_versions_yesterday(now: datetime, prefect_api: PrefectAPI, tenant: str) -> dict:
    '''
    Retorna um dicionario com as versoes das flows vigentes nas ultimas 24h

    Args:
        now (datetime): Um datetime de agora  
        prefect_api (PrefectAPI): Um objeto que interage com a API do Prefect

    Returns:
        dict: Dicionario onde o nome do flow eh a chave e o valor sao as versoes que executaram
    ---
    Formato do retorno:
    {'SMTR - GPS SPPO': [121, 135, 144],
    'SMTR Bilhetagem': [12, 21],
    ...}
    '''
    
    active_versions_yesterday = {}
    flow_versions = prefect_api.get_flows_versions(tenant)
    df = pd.DataFrame(flow_versions)
    df['created'] = pd.to_datetime(df['created'])
    flow_names = df['name'].unique()

    for flow_name in flow_names:
        active_versions_yesterday[flow_name] = []
        for _, row in df[df['name'] == flow_name].iterrows():
            if row['created'] >= now:
                pass
            elif now - timedelta(hours=24) <= row['created'] < now:
                active_versions_yesterday[flow_name].append(row['version'])
            else:
                active_versions_yesterday[flow_name].append(row['version'])
                break


    return active_versions_yesterday

def get_execution_count(now: datetime, prefect_api: PrefectAPI, tenant: str) -> dict:
    '''
    Retorna um dicionario com as versoes das flows vigentes 

    Args:
        now (datetime): Um datetime de agora  
        prefect_api (PrefectAPI): Um objeto que interage com a API do Prefect

    Returns:
        dict: Dicionario onde o nome do flow eh a chave e o valor eh a quantidade de vezes que o flow executou
    ---
    Formato do retorno:
    {'SMTR - GPS SPPO': 1332,
    'SMTR Bilhetagem': 12,
    ...}
    '''
    times_had_run = {}
    yesterday_flow_runs = prefect_api.get_scheduled_count(now, tenant)
   
    for run in yesterday_flow_runs:
        if run['name'] not in times_had_run:
            times_had_run[run['name']] = 0
        times_had_run[run['name']
                        ] += run['flow_runs_aggregate']['aggregate']['count']

    times_had_run = dict(
        filter(lambda pair: pair[1] > 0, times_had_run.items()))
    return times_had_run


def get_wrong_version_runs(now: datetime, prefect_api: PrefectAPI, active_flow_versions_yesterday, tenant: str) -> dict:
    '''
    Retorna um dicionario com as runs que executaram em uma versao errada

    Args:
        now (datetime): Um datetime de agora  
        prefect_api (PrefectAPI): Um objeto que interage com a API do Prefect

    Returns:
        dict: Dicionario onde o nome do flow eh a chave e o valor eh um dicionario com a versao e a quantidade de execucoes
    ---
    Formato do retorno:
    {'SMTR - GPS SPPO': {'wrong_versions': {123, 105}, 'count': 50},
    'SMTR Bilhetagem': {'wrong_versions': {9, 12}, 'count': 3}},
    ...}
    '''
    yesterday_flow_runs = prefect_api.get_yesterday_flow_runs(now, tenant)
    df = pd.json_normalize(yesterday_flow_runs)
    df = df[df['flow_runs_aggregate.aggregate.count'] > 0]
    wrong_version_runs = {}

    for flow_name in df['name'].unique():
        ran_versions = set(df[df['name'] == flow_name]['version'])
        if flow_name in active_flow_versions_yesterday:
            active_versions_yesterday = set(active_flow_versions_yesterday[flow_name])
            wrong_versions = ran_versions.difference(active_versions_yesterday)
            count = df[(df['name'] == flow_name) & (df['version'].isin(wrong_versions))]['flow_runs_aggregate.aggregate.count'].sum()
            wrong_version_runs[flow_name] = {}
            wrong_version_runs[flow_name]['wrong_versions'] = wrong_versions
            wrong_version_runs[flow_name]['count'] = count
        else:
            wrong_version_runs[flow_name] = {}
            wrong_version_runs[flow_name]['wrong_versions'] = set(df[df['name'] == flow_name]['version'])
            wrong_version_runs[flow_name]['count'] = df[df['name'] == flow_name]['flow_runs_aggregate.aggregate.count'].sum()  

    return wrong_version_runs



@app.task
def daily_report():
    # from dateutil import parser
    # import pytz
    # now = parser.parse('2024-03-03 08:00-03:00').astimezone(
    #                     pytz.timezone("America/Sao_Paulo")
    #                )
    try:
        now = datetime.now(pytz.timezone("America/Sao_Paulo"))
        
        prefect_api = PrefectAPI()

        tenants = prefect_api.get_tenants()

        for tenant in tenants:
            
            failed_runs = get_failed_runs(now=now, prefect_api=prefect_api, tenant=tenant)
            expected_execution_count = get_expected_execution_count(now=now, prefect_api=prefect_api, tenant=tenant['id'])
            execution_count = get_execution_count(now=now, prefect_api=prefect_api, tenant=tenant['id'])
            ran_flow_versions = get_ran_flow_versions(now=now, prefect_api=prefect_api, tenant=tenant['id'])
            active_flow_versions_yesterday = get_active_flow_versions_yesterday(now=now, prefect_api=prefect_api, tenant=tenant['id'])
            wrong_version_runs = get_wrong_version_runs(now, prefect_api, active_flow_versions_yesterday, tenant=tenant['id'])

            problematic_flows = {}

            for flow in expected_execution_count:
                if flow not in execution_count:
                    # [qty actually scheduled, qty should have been scheduled, percentage]
                    problematic_flows[flow] = { 'scheduled_count': 0, 'expected_scheduled_count': expected_execution_count[flow], 'percentage': 0 }
                elif expected_execution_count[flow] != execution_count[flow]:
                    problematic_flows[flow] = {'scheduled_count': execution_count[flow], 'expected_scheduled_count': expected_execution_count[flow], 'percentage': execution_count[flow]/expected_execution_count[flow]}

            for flow in execution_count:
                if flow not in expected_execution_count:
                    problematic_flows[flow] = {'scheduled_count': execution_count[flow], 'expected_scheduled_count': 0, 'percentage': float('inf')}

            for flow in problematic_flows:
                problematic_flows[flow]['flow_name'] = flow
                problematic_flows[flow]['failed_count'] = 0
                problematic_flows[flow]['total_count'] = 0
                problematic_flows[flow]['failed_percentage'] = 0
            

            for flow in failed_runs:
                if flow['flow_name'] in problematic_flows:
                    problematic_flows[flow['flow_name']]['flow_name'] = flow['flow_name']
                    problematic_flows[flow['flow_name']]['failed_count'] = flow['failed_count']
                    problematic_flows[flow['flow_name']]['total_count'] = flow['total_count']
                    problematic_flows[flow['flow_name']]['failed_percentage'] = flow['percentage']
                else:
                    problematic_flows[flow['flow_name']] = {}
                    problematic_flows[flow['flow_name']]['flow_name'] = flow['flow_name']
                    problematic_flows[flow['flow_name']]['failed_count'] = flow['failed_count']
                    problematic_flows[flow['flow_name']]['total_count'] = flow['total_count']
                    problematic_flows[flow['flow_name']]['failed_percentage'] = flow['percentage']
                    if flow['flow_name'] in expected_execution_count:
                        problematic_flows[flow['flow_name']]['expected_scheduled_count'] = expected_execution_count[flow['flow_name']]
                    else:
                        problematic_flows[flow['flow_name']]['expected_scheduled_count'] = 0
                    if flow['flow_name'] in execution_count:
                        problematic_flows[flow['flow_name']]['scheduled_count'] = execution_count[flow['flow_name']]
                    else:
                        problematic_flows[flow['flow_name']]['scheduled_count'] = 0
                    if problematic_flows[flow['flow_name']]['expected_scheduled_count'] == 0:
                        problematic_flows[flow['flow_name']]['percentage'] = float('inf')
                    else:
                        problematic_flows[flow['flow_name']]['percentage'] = problematic_flows[flow['flow_name']]['scheduled_count']/problematic_flows[flow['flow_name']]['expected_scheduled_count']
            


            for flow in problematic_flows:
                if problematic_flows[flow]['failed_percentage'] >= 0.2 or problematic_flows[flow]['scheduled_count'] < problematic_flows[flow]['expected_scheduled_count']:
                    problematic_flows[flow]['priority'] = 2
                elif problematic_flows[flow]['failed_percentage'] >= 0.1:
                    problematic_flows[flow]['priority'] = 1
                else:
                    problematic_flows[flow]['priority'] = 0

                if not 'flow_name' in problematic_flows[flow]:
                    problematic_flows[flow]['flow_name'] = flow
            

            for flow in problematic_flows:
                if flow in wrong_version_runs:
                    problematic_flows[flow]['wrong_versions'] = wrong_version_runs[flow]['wrong_versions']
                    problematic_flows[flow]['wrong_version_count'] = wrong_version_runs[flow]['count']
                else:
                    problematic_flows[flow]['wrong_versions'] = set()
                    problematic_flows[flow]['wrong_version_count'] = 0
            items = list(problematic_flows.values())
            items.sort(key=lambda x: x['priority'], reverse=True)
            daily_report = DailyReport(now, items)
            daily_report.send_discord_message()
    except Exception:
        fp = open('log.txt', 'w')
        fp.write(traceback.format_exc())
        fp.close()

if __name__ == '__main__':
    daily_report()

