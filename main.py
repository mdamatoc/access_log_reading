import pandas as pd
import progressbar
import hashlib
import re


regex = r'^(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) (-) (-) \[(\d{2}\/\w{3}\/\d{4} \d{2}:\d{2}:\d{2} \+\d{4})\] (.+?(?=\ )) (\d{3}) (\d*) \"(.+?(?=\"))\" \"(.+?(?=\"))\"$'
header = ['remote_host', 'identity', 'username', 'date', 'request', 'status_code', 'response_time', 'referrer', 'user_agent']


# Motivation of choosing | as the separator

# After a quick exploratory data analysis and reading README.md provided by e-mail, it's clear that there is no pipe in
# this .log, so, the reading method will use pipe as separator because the default separator is comma, which is possible
# in the .log. If the reading method tries to change the engine from C (default) to Python, while trying to support the
# None separator, it will automatically forces the column to slit into other ones. This behavior is not consistent
# with the reading strategy, so it won't be used.

access_log = pd.read_csv('test-access-001-1.log', header=None, sep='|')

widgets = ['[', progressbar.Timer(format=f'Reading and processing log: %(elapsed)s'), '] ', progressbar.Bar('#'), ' (', progressbar.ETA(), ') ']
bar = progressbar.ProgressBar(max_value=access_log.shape[0], widgets=widgets).start()

search = []
for row in access_log.iterrows():
    bar.update(row[0])
    search.append(re.search(regex, row[1][0]).groups())

data_frame = pd.DataFrame(search, columns=header)
data_frame['request'] = data_frame['request'].map(lambda x: x.lstrip('"').rstrip('"'))
bar.finish()


"""
PRIMEIRO ITEM

Formatar uma saída do log em json contendo a lista de request apresentada no log,
cada objeto dentro da lista deve conter as propriedades de uma entrada no log como
remote_host, date, request, status_code, response_time, reffer, user_agent.
"""

print("Processing item 1")
data_frame.to_json('item_1_lista_de_requests.json', orient='records', lines=True)


"""
SEGUNDO ITEM

Encontrar os 10 maiores tempos de resposta com sucesso do servidor na chamada GET /maunal/ com a origem do tráfego
igual a "http://localhost/svnview?repos=devel&rev=latest&root=SVNview/tmpl&list_revs=1"

Interpretando:

resposta com sucesso -> status_code = 200

A chamada 'GET /maunal/' não existe, mas parece um erro de digitacao. Serão buscados requests que iniciem com 'GET /manual/'

referrer = 'http://localhost/svnview?repos=devel&rev=latest&root=SVNview/tmpl&list_revs=1'
"""

print("Processing item 2")
response_time_data_frame = data_frame[['request', 'status_code', 'response_time', 'referrer']].copy()
response_time_data_frame['response_time'] = response_time_data_frame['response_time'].astype(int)
response_time_data_frame['status_code'] = response_time_data_frame['status_code'].astype(int)

response_time_data_frame = response_time_data_frame.loc[response_time_data_frame['request'].str.startswith('GET /manual/')]
response_time_data_frame = response_time_data_frame.loc[response_time_data_frame['referrer'].str.startswith('http://localhost/svnview?repos=devel&rev=latest&root=SVNview/tmpl&list_revs=1')]
response_time_data_frame = response_time_data_frame.loc[response_time_data_frame['status_code'] == 200]
longer_response_time = response_time_data_frame.nlargest(n=10, columns='response_time')['response_time']

longer_response_time.to_csv('item_2_maiores_tempos_dentre_especificacoes.csv', index=False)


"""
TERCEIRO ITEM

Formatar uma saída em arquivo físico do access.log com a data em formato UNIX timestamp %Y-%m-%d %H:%M:%S
 e o IP convertido em um hash MD5
 
Interpretando:

A coluna 'date' é uma Series de strings que representam datetimes com timezones. Primeiro, converte-se de string para datetime,
posteriormente calcula-se o timezone UTC equivalente, para padronizar o datetime, já que é desejado que o resultado
não tenha timezone. Por fim, transforma-se o datetime obtido para o formato UNIX.

A coluna 'remote_host' contém o IP que deve ser convertido em hash MD5. De maneira anterior a conversão, o IP deve
possuir um encoding: usarei o LATIN-1, por ser mais completo e não apresentar variação expressiva no desempenho do
projeto. Ainda neste item, é necessário retornar o dado codificado. Existem dois métodos capazes de executar essa
atividade, um deles, o digest(), retorna o IP em hash MD5 em byte-format, o outro, o hexdigest(), retorna o IP em
hexadecimais. Como não há especificação de formato de output, o hexadecimal foi escolhido por ser mais simples
de ser salvo em um arquivo de texto.
"""

print("Processing item 3")
unix_hash_data_frame = data_frame.copy()

unix_hash_data_frame['date'] = pd.to_datetime(unix_hash_data_frame['date']).dt.tz_convert(tz='UTC').dt.strftime("%Y-%m-%d %H:%M:%S")

unix_hash_data_frame['remote_host'] = data_frame['remote_host'].apply(lambda x: hashlib.md5('192.141.19.154'.encode("latin-1")).hexdigest())

unix_hash_data_frame[['remote_host', 'date']].to_json('item_3_date_unix_hash_md5_ip.json', orient='records', lines=True)


"""
QUARTO ITEM

Formatar uma saída em arquivo físico agrupando a soma total de requests por dia do ano
"""

print("Processing item 4")
grouped_requests_data_frame = data_frame[['date']].copy()

grouped_requests_data_frame['date'] = pd.to_datetime(grouped_requests_data_frame['date']).dt.tz_convert(tz='UTC').dt.tz_localize(None)
grouped_requests_data_frame['amount'] = 1

grouped_requests_data_frame.set_index('date', inplace=True)
grouped_requests_data_frame = grouped_requests_data_frame.resample('D').sum()

grouped_requests_data_frame.to_csv('item_4_total_requests_por_dia_do_ano.csv', sep='|')


"""
QUINTO ITEM

Formatar uma saída em arquivo físico com endereços de IP únicos, um IP por linha, contidos no log com a última data de
request realizado pelo remote IP
"""

print("Processing item 5")
total_requests_ip_data_frame = data_frame[['remote_host', 'date']].copy()

total_requests_ip_data_frame['date'] = pd.to_datetime(total_requests_ip_data_frame['date']).dt.tz_convert(tz='UTC').dt.tz_localize(None).dt.date

total_requests_ip_data_frame['date'] = pd.to_datetime(total_requests_ip_data_frame['date'])
total_requests_ip_data_frame = total_requests_ip_data_frame[total_requests_ip_data_frame.groupby('remote_host').date.transform('max') == total_requests_ip_data_frame['date']]

total_requests_ip_data_frame.to_csv('item_5_enderecos_ip_unicos_ultima_data_de_request.csv', index=False, sep='|')

print("Done!")
