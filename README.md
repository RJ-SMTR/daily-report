# Daily-Report
Relatório diário dos eventos que ocorreram com as pipelines do Prefect

- Runs esperadas: quantidade de runs que deveriam ter sido executadas
- Falhas: quantidade de runs que tiveram o status 'Failed'
- Não executadas: quantidade de runs que faltou ser executada para chegar no número de 'Runs esperadas'
- Runs não esperadas: quantidade de runs que executaram em uma versão errada
- Versão incorreta: conjunto com todas as versões incorretas de flows que tiveram runs executadas
- Não agendadas: quantidade de runs que superaram o número de 'Runs esperadas'

## CLI
Esta aplicação pode ser usada como um programa de linha de comando.\
Você pode gerar relatórios passando uma timestamp ISO como argumento.\
O daily-report está rodando em um cluster *us-central1-c*.\
Para evitar maiores confusões, o relógio está setado para UTC.\
Então lembre-se de passar as strings de horário com 3h a mais do que o horário de Brasília desejado.\
O comando de exemplo gera o relatório sobre tudo o que ocorreu entre 09/10/2024 08:00:00 e 10/10/2024 08:00:00.
```console
$ python main.py "2024-10-10 11:00:00"
```

## TODO
- [ ] Adicionar comando para exibição de quais relatório estão disponíveis
- [ ] Poder selecionar qual relatório quer gerar
- [ ] Poder enviar o relatório para um webhook do Discord arbitrário
