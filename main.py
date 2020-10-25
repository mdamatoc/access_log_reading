import pandas as pd
import re
import progressbar
import hashlib


regex = r'^(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) (-) (-) \[(\d{2}\/\w{3}\/\d{4} \d{2}:\d{2}:\d{2} \+\d{4})\] (.+?(?=\ )) (\d{3}) (\d*) \"(.+?(?=\"))\" \"(.+?(?=\"))\"$'
header = ['remote_host', 'identity', 'username', 'date', 'request', 'status_code', 'response_time', 'referrer', 'user_agent']

"""
Motivation of choosing | as the separator

After a quick exploratory data analysis and reading README.md, it's clear that there is no pipe in this .log, so,
the reading method will pipe as separator because the default separator is comma, which is possible in the .log. If the
reading method tries to change the engine from C (default) to Python, while trying to support the None separator, it
will automatically forces the column split to slit into other ones. This behavior is not consistent with the reading
strategy, so it won't be used.
"""

access_log = pd.read_csv('teste.log', header=None, sep='|')

# widgets = ['[', progressbar.Timer(format='elapsed time: %(elapsed)s'), '] ', progressbar.Bar('#'), ' (', progressbar.ETA(), ') ']
# bar = progressbar.ProgressBar(max_value=access_log.shape[0], widgets=widgets).start()

search = []
for row in access_log.iterrows():
    # bar.update(row[0])
    search.append(re.search(regex, row[1][0]).groups())

data_frame = pd.DataFrame(search, columns=header)
data_frame['request'] = data_frame['request'].map(lambda x: x.lstrip('"').rstrip('"'))

"""
PRIMEIRO ITEM

Formatar uma saída do log em json contendo a lista de request apresentada no log,
cada objeto dentro da lista deve conter as propriedades de uma entrada no log como
 remote_host, date, request, status_code, response_time, reffer, user_agent.
"""
data_frame.to_json('item_1_lista_de_requests.json', orient='records', lines=True)

"""
SEGUNDO ITEM

Encontrar os 10 maiores tempos de resposta com sucesso do servidor na chamada GET /maunal/ com a origem do tráfego
igual a "http://localhost/svnview?repos=devel&rev=latest&root=SVNview/tmpl&list_revs=1"

Interpretando:
resposta com sucesso -> status_code = 200
chamada 'GET /maunal/' nao existe, mas parece um erro de digitacao. Buscarei requests que iniciem com 'GET /manual/'
referrer = 'http://localhost/svnview?repos=devel&rev=latest&root=SVNview/tmpl&list_revs=1'
"""

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
possuir um enconding: usarei UTF-8, pois não é necessário utilizar um enconding mais denso como LATIN-1, posto que IP's
possuem somente números e pontos. Ainda neste item, é necessário retornar o dado codificado. Existem dois métodos 
capazes de executar essa atividade, um deles, o digest(), retorna o IP em hash MD5 em byte-format, o outro, o hexdigest(),
retorna o IP em hexadecimais. Como não especificação de formato de output, o hexadecimal foi escolhido por ser mais simples
de ser salvo em um arquivo de texto.
"""

unix_hash_data_frame = data_frame.copy()

unix_hash_data_frame['date'] = pd.to_datetime(unix_hash_data_frame['date']).dt.tz_convert(tz='UTC').dt.strftime("%Y-%m-%d %H:%M:%S")

unix_hash_data_frame['remote_host'] = data_frame['remote_host'].apply(lambda x: hashlib.md5('192.141.19.154'.encode("latin-1")).hexdigest())

unix_hash_data_frame.to_json('lista_3_date_unix_hash_md5_ip.json', orient='records', lines=True)

"""
Formatar uma saída em arquivo físico agrupando a soma total de requests por dia do ano
"""