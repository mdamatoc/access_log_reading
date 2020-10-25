import pandas as pd
import re
import progressbar
import time


regex = r'^(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) (-) (-) \[(\d{2}/\w{3}/\d{4} \d{2}:\d{2}:\d{2} \+\d{4})\] \"(.+?(?=\"))\" (\d{3}) (\d*) \"(.+?(?=\"))\" \"(.+?(?=\"))\"$'

"""
Motivation of choosing | as the separator

After a quick exploratory data analysis and reading README.md, it's clear that there is no pipe in this .log, so,
the reading method will pipe as separator because the default separator is comma, which is possible in the .log. If the
reading method tries to change the engine from C (default) to Python, while trying to support the None separator, it
will automatically forces the column split to slit into other ones. This behavior is not consistent with the reading
strategy, so it won't be used.
"""

access_log = pd.read_csv('test-access-001-1.log', header=None, sep='|')

# widgets = ['[', progressbar.Timer(format='elapsed time: %(elapsed)s'), '] ', progressbar.Bar('*'), ' (', progressbar.ETA(), ') ']
# bar = progressbar.ProgressBar(max_value=access_log.shape[0], widgets=widgets).start()

search = []
# 192.141.19.154 - - [12/Jan/2020 02:57:26 +0200] "GET /apache_pb.gif HTTP/1.1" 200 79262 "http://localhost/manual/"
# "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36 OPR/43.0.2442.991"

for row in access_log.iterrows():
    # bar.update(row[0])
    try:
        search.append(re.search(regex, row[1][0]).groups())
    except AttributeError:  # todo existem requisicoes de cliente que nao estao com aspas duplas no log
        print(f'{row[1][0]} is out ot pattern because there is no " around the client request')

data_frame = pd.DataFrame(search, columns=['host', 'identity', 'username', 'timestamp', 'request', 'response_status_code', 'response_time', 'referrer', 'user_agent'])

print('\n\n\n')
pd.set_option('display.max_columns', 60)
print(data_frame)

# for row in access_log.iterrows():
#     bar.update(row[0])
#
#     line_content = row[1][0]
#
#     m = re.fullmatch(regex, line_content)
#     data = list(m.groups())
#
#     data_frame_len = len(data_frame)
#     data_frame.loc[data_frame_len] = data
