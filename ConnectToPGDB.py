import this

import psycopg2


def get_cur():
    conn = psycopg2.connect(

        database='postgres',
        user='postgres',
        password="123456",
        host='localhost',
        port='5432'
    )
    try:
        cur = conn.cursor()
        return cur
    except:
        print('An error occured while connecting to PG DB.')


'''

cur.execute("select * from public.employee_manager")

rows = cur.fetchall()
for row in rows:
    print(row)
'''


# the close module should put in finally block in the execute part
def close_con(cur):
    cur.close()
    conn.close()
