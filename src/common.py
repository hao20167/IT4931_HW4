import psycopg2
from psycopg2.extras import execute_values

def db():
    return psycopg2.connect(host='db',dbname='hw4',user='postgres',password='hw4')

def insert_speed(rows):
    if not rows:
        return
    with db() as c:
        with c.cursor() as q:
            for row in rows:
                execute_values(q,'INSERT INTO speed_events VALUES %s ON CONFLICT DO NOTHING',[row])
