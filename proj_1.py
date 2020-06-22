import psycopg2  # install with pip: psycopg2-binary
import psycopg2.extras
import csv
import pandas
import json as JSON  # json to manage data


def connect_to_db(pdm_test):
    credential = "host=127.0.0.1 dbname={} user=postgres password=postgres".format(pdm_test)
    conn = psycopg2.connect(credential, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn


def create_schema(conn, a):
    cur = conn.cursor()
    cur.execute(""" CREATE SCHEMA IF NOT EXISTS {}""".format(a))
    conn.commit()


def create_table(conn, schema, my_table):
    cur = conn.cursor()
    cur.execute(""" CREATE TABLE IF NOT EXISTS {}.{}
          (id SERIAL,
           sepal_length REAL,
           sepal_width REAL,
           petal_length REAL,
           petal_width REAL,
           sepal_area REAL,
           petal_area REAL,
           name TEXT)""".format(schema, my_table))
    conn.commit()


def load_data(conn, my_schema_ed, table, csv_file):
    cur = conn.cursor()

    cmd = "COPY {}.{}(sepal_length ,sepal_width ,petal_length ,petal_width ,name )" \
          " FROM '{}' " \
          "DELIMITER ',' CSV HEADER;"
    cur.execute(cmd.format(my_schema_ed, table,csv_file))
    conn.commit()

def get_area_iris(my_schema_ed,table):
    curr = conn.cursor()
    curr.execute("SELECT * FROM {}.{} WHERE sepal_area IS NULL or petal_area IS NULL".format(my_schema_ed,table))
    data = pandas.DataFrame(curr.fetchall())
    #print(data)
    return data

def edit_area_iris(conn , data, my_schema_ed,table):
    data.petal_area = data.petal_length * data.petal_width
    data.sepal_area = data.sepal_length * data.sepal_width

    cur = conn.cursor()

    for index, row in data.iterrows():   # execute moi lignes par lignes
        terminal1 = 'update {}.{} set petal_area = %s where id = %s'.format(my_schema_ed,table)
        terminal2 = 'update {}.{} set sepal_area = %s where id = %s'.format(my_schema_ed,table)
        cur.execute(terminal1, (row['petal_area'], row['id']))
        cur.execute(terminal2, (row['sepal_area'], row['id']))
    conn.commit()




# ----------------------------------------------------------------------------------------------------

conn = connect_to_db('ppm')
create_schema(conn, 'test')
create_table(conn, 'test', 'ed')
load_data(conn, 'test', 'ed', '/Users/edwinson/PycharmProjects/school_project/iris.csv')
tab = get_area_iris('test','ed')
edit_area_iris(conn,tab,'test','ed')
print(tab)

print('COOL')
