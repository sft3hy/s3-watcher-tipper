import sys, clickhouse_connect
try:
    c = clickhouse_connect.get_client(host='localhost', port=8123)
    res = c.query("DESCRIBE TABLE sigint_data").result_rows
    for row in res:
        if row[0] == 'event_time':
            print("event_time type:", row[1])
except Exception as e:
    print(e)
