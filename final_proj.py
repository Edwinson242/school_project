import psycopg2  # install with pip: psycopg2-binary
import psycopg2.extras
import pandas
import proj_2 as pj # where the imported module proj_2 is our second class project on navitia
import json as JSON  # json to manage data
import matplotlib.pyplot as plt


# ALL THE DEFINED FUNCTIONS ARE LISTED BELOW

def connect_to_database(pdm):
    credential = "host=91.121.117.24 dbname=pdm user=pdm password=12pdm365"
    connexion = psycopg2.connect(credential, cursor_factory=psycopg2.extras.RealDictCursor)
    return connexion


def create_my_own_schema(connexion, schema):
    cur = connexion.cursor()
    cur.execute(""" CREATE SCHEMA IF NOT EXISTS {}""".format(schema))
    connexion.commit()


def create_my_table(connexion, schema, table):
    cur = connexion.cursor()
    cur.execute(""" CREATE TABLE IF NOT EXISTS {}.{} AS (
    SELECT 
    id, 
    address, 
    school, 
    null::integer as duration,
    geom FROM public.student )""".format(schema, table))
    connexion.commit()


def read_schools_data(csv_file):
    return pandas.read_csv('/Users/edwinson/PycharmProjects/school_project/schools.csv')


def get_school_coord(data):
    coord = []
    for index, row in data.iterrows():
        coord.append(pj.geocoding(row.address))
    #   print(JSON.dumps(coord, indent=4))
    return coord


def get_duration_values(schema, table):
    cur = connexion.cursor()
    cur.execute(
        "SELECT id, school , duration ,ST_X(geom) , ST_Y(geom) "  # ST_X(geom) pour passer de Easting, Northing à LON, LAT
        " FROM {}.{} WHERE duration IS NULL".format(schema, table))
    data = pandas.DataFrame(cur.fetchall())
    return data


def update_duration_values(conn, data, school, schema, table):
    school_dict = {}
    for index, row in school.iterrows():  # execute moi ligne par ligne
        school_dict[row.school] = row.test # Lons , Lats for the location of schools since we have school coord
    dur = []
    for indexs, rows in data.iterrows():
        dict = {'lat': rows.st_x, 'lon': rows.st_y} # Lons , Lats, for the location of students
        var = pj.best_journey(dict, school_dict[rows.school])
        dur.append(var)
        print(JSON.dumps(dur, indent=4))
    #   print(index)

    return dur


def upd_DB(connexion, tab):
    for index, row in tab.iterrows():
        cur = connexion.cursor()
        cur.execute("""UPDATE ed_schema.student SET duration = {} where id = {}""".format(row.duration, row.id))

    connexion.commit()



def add_new_columns(connexion, lat, lon, schema, table):
    cur = connexion.cursor()
    # cur.execute(""" ALTER TABLE ed_schema.student ADD COLUMN {} FLOAT, ADD COLUMN {} FLOAT""".format(lat, lon))

    connexion.commit()


def update_new_columns(connexion, schema, table):

        cur = connexion.cursor()
        cur.execute("""UPDATE ed_schema.student SET lat = ST_X(geom) WHERE lat IS NULL""")
        cur.execute("""UPDATE ed_schema.student set lon = ST_Y(geom) WHERE lon IS NULL""")
        connexion.commit()


def get_my_table(schema, table):
    cur = connexion.cursor()
    cur.execute("""SELECT * FROM {}.{} """.format(schema, table))
    ed = pandas.DataFrame(cur.fetchall())
    return ed

# VISUALIZING THE DATA WITH PANDAS & CREATING A PLOT

def results(df):
    print(ed_student.groupby(ed_student['school']).describe())
    ed_student.groupby(ed_student['school']).describe().to_csv('statistics_results.csv')
    box, ax = plt.subplots(figsize=(12, 8)) # figure ratio
    # List of the column to keep, grouping by, the axes to use
    ed_student.boxplot(column=['duration'], by='school', ax=ax)
    plt.show()
    # Save the figure
    box.savefig('boxing_graph.png')



# --------------------------------------------------------------------------------------------------------------

# CALLED FUNCTIONS ARE LISTED BELOW

connexion = connect_to_database('pdm')
create_my_own_schema(connexion, 'ed_schema')
create_my_table(connexion, 'ed_schema', 'student')
school = read_schools_data('/Users/edwinson/PycharmProjects/school_project/schools.csv')
coord = get_school_coord(school)
school['test'] = coord
data = get_duration_values('ed_schema', 'student')
dur = update_duration_values(connexion, data, school, 'ed_schema', 'student')
data['duration'] = dur
upd_DB(connexion, data)
add_new_columns(connexion, 'lat', 'lon', 'ed_schema', 'student')
update_new_columns(connexion, 'ed_schema', 'student')
ed_student = get_my_table('ed_schema', 'student')
results(ed_student)






