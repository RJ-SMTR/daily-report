from datetime import datetime
import os
import requests



class DailyReport:

    def __init__(self, now: datetime, items: dict) -> None:
        self.now = now
        self.items = items
        self.message = self.get_message()
    
    def get_fields(self):
        fields = []

        red_circle = ':red_circle:'
        yellow_circle = ':yellow_circle:'
        green_circle = ':green_circle:'

        for item in self.items:
            if item['priority'] == 2:
                emoji = red_circle
            elif item['priority'] == 1:
                emoji = yellow_circle
            else:
                emoji = green_circle
            
            if item['failed_count']:
                fails = f"**Falhas: {item['failed_count']} ({item['failed_percentage']:.1%})**"
            else:
                fails = f"Falhas: {item['failed_count']} ({item['failed_percentage']:.1%})"

            if item['percentage'] < 1:
                not_run = f"**Não executadas: {item['expected_scheduled_count'] - item['scheduled_count'] if item['percentage'] < 1 else 0} ({(item['expected_scheduled_count'] - item['scheduled_count'])/item['expected_scheduled_count'] if item['expected_scheduled_count'] != 0 else 0:.1%})**"
            else:
                not_run = f"Não executadas: {item['expected_scheduled_count'] - item['scheduled_count'] if item['percentage'] < 1 else 0} ({(item['expected_scheduled_count'] - item['scheduled_count'])/item['expected_scheduled_count'] if item['expected_scheduled_count'] != 0 else 0:.1%})"
            
            if item['wrong_version_count']:
                wrong_version_count = f"**Runs não esperadas: {item['wrong_version_count']}**"
            else:
                wrong_version_count = f"Runs não esperadas: {item['wrong_version_count']}"
            
            if item['wrong_versions']:
                wrong_versions = f"**Versão incorreta: {item['wrong_versions']}**"
            else:
                wrong_versions = "Versão incorreta: {}"

            if item['percentage'] > 1:
                not_scheduled = f"**Não agendadas: {item['scheduled_count'] - item['expected_scheduled_count']}**"
            else:
                not_scheduled = f"Não agendadas: 0"


            value = {
                "name": f"{emoji} {item['flow_name']}",
                "value": f"""- Runs esperadas: {item['expected_scheduled_count']} - {fails}, {not_run}\n- {wrong_version_count} - {wrong_versions}, {not_scheduled}"""
            }
            fields.append(value)

        return fields


    def get_message(self):
        content = f'Bom dia, <@&{os.getenv("DISCORD_ROLE_DADOS")}>! :coffee:'
        timestamp = self.now.isoformat()
        color = 16515072
        author = {
            'name': 'Reporte Diário de Pipelines',
            'url': 'http://prefect.dados.rio'
        }
        fields = self.get_fields()
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
        return message


    def send_discord_message(self) -> None:
        headers = {"Content-Type": "application/json"}
        data = self.message
        print(data)
        r = requests.post(os.getenv("DISCORD_DAILY_REPORT_ENDPOINT"), headers=headers, json=data)
        print(r.status_code, r.text)
