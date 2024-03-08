# SQuaD
Sistema de Qualidade de Dados

# TODO
- [] Precisamos olhar o objeto schedule dos flows para ver a hora de início e o intervalo.
Porque o código, até agora, não identifica runs que não foram agendadas mas deveriam. 

- [] Fechar o cursor do db

- [] Verificar se o tempo do relógio atual é maior que o scheduled_start_time e se o start_time é nulo para detectar flows que nunca começaram

- [] Adicionar variável de ambiente para o redis do monitor no yaml


# Regras de negócio

## Para falhas de piplines de 24 horas:

"SMTR: Viagens SPPO": 24h
"SMTR: RHO - Captura": 24h
"SMTR: RDO - Captura": 24h
"SMTR: Dados diários - Publicação `datario`": 24h 24h+15
"SMTR: Captura - veiculo.sppo_licenciamento_stu": 24h
"SMTR: Captura - veiculo.sppo_infracao": 24h
"SMTR: Subsídio SPPO Apuração": 24h

Alertar todas na hora.

## Para falhas de piplines de 1 hora:

"SMTR: GPS BRT - Materialização": 1h
"SMTR - GPS SPPO Recapturas": 1h

Alertar todas na hora.

## Para falhas de piplines de 10 minutos:

"SMTR: GPS SPPO - Realocação (captura)": 10m

Alertar se ocorrer 3 falhas em um intervalo de 1 hora na mesma pipeline.
Limite: 10m

## Para falhas de piplines de 1 minuto:

"SMTR: GPS BRT - Captura": 1m
"SMTR: GPS STPL - Captura": 1m
"SMTR: GPS SPPO - Captura": 1m

Alertar se ocorrer 10 falhas em um intervalo de 15 minutos na mesma pipeline.
Limite: 1m
