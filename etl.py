import mysql.connector
import pandas as pd
import random

conn1 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="si_prac1_db"
)
conn2 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="si_prac2_db"
)
cursor1 = conn1.cursor()
cursor2 = conn2.cursor()


def format_content():

    sql_query = "SELECT * FROM peliculas_series;"
    cursor1.execute(sql_query)
    res = cursor1.fetchall()

    cols = ["show_id", "type", "title", "director", "cast", "country", "date_added", "release_year", "rating", "duration", "listed_in", "description"]
    df = pd.DataFrame(res,columns=cols)

    for i in range(len(df)):
        row = list(df.iloc[i])

        row[7] = int(row[7]) if row[7] is not None else None
        row[9] = int(row[9]) if row[9] is not None else None

        subtype = None
        if row[1] == "Movie":
            if row[9] >= 90:
                subtype = "+90"
            else:
                subtype = "-90"
        elif row[1] == "TV Show":
            if row[9] == 1:
                subtype = "S1"
            elif row[9] == 2:
                subtype = "S2"
            else:
                subtype = "S2+"
        
        row.append(subtype)

        sql_query = "INSERT INTO content VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        cursor2.execute(sql_query, row)
        conn2.commit()

def copy_users():
    sql_query = "SELECT * FROM usuarios;"
    cursor1.execute(sql_query)
    res = cursor1.fetchall()

    cols = ["uid", "name", "last_login"]
    df = pd.DataFrame(res,columns=cols)

    for i in range(len(df)):
        row = list(df.iloc[i])

        row[0] = int(row[0])

        sql_query = "INSERT INTO users VALUES (%s, %s, %s);"
        cursor2.execute(sql_query, row)
        conn2.commit()

def generate_date():
    
    year = "2024"
    month_days = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    tid = 0
    for i in range(12):
        month = i+1

        for j in range(month_days[i]):
            day = j+1
            
            row = [tid,year,month,day]

            sql_query = "INSERT INTO date VALUES(%s, %s, %s, %s);"
            cursor2.execute(sql_query,row)
            conn2.commit()
        
            tid += 1

def generate_hours():
    
    for i in range(0,24):
        row = [i]
        sql_query = "INSERT INTO hours VALUES(%s);"
        cursor2.execute(sql_query,row)
        conn2.commit()

def random_date():
    
    r = random.random()

    if r < 0.15:    # Navidad
        return random.choice(list(range(0,7))+list(range(355,366)))
    elif r < 0.5:   # Verano
        return random.choice(list(range(152,244)))
    else:           # Resto
        return random.choice(list(range(7,152))+list(range(244,355)))
    
def random_hour():
    r = random.random()

    if r < 0.4:     # Tarde-Noche
        return random.choice(list(range(19,24))+list([0]))
    elif r < 0.55:  # Comida-Siesta
        return random.choice(list(range(15,17)))
    elif r < 0.67:
        return random.choice(list(range(2,7)))
    else:           # Resto
        return random.choice(list([1]) + list(range(7,15)) + list(range(17,19)))

def format_views():
    sql_query = "SELECT * FROM visionados;"
    cursor1.execute(sql_query)
    res = cursor1.fetchall()

    cols = ["vid", "show_id", "uid", "rating"]
    df = pd.DataFrame(res,columns=cols)

    df["viewings"] = 1
    df["tid"]= df.apply(lambda _: random_date(), axis=1)
    df["hid"]= df.apply(lambda _: random_hour(), axis=1)
    df = df[["vid", "show_id", "uid", "tid", "hid", "rating", "viewings"]]

    for i in range(len(df)):
            row = list(df.iloc[i])

            row[0] = int(row[0]) if row[0] is not None else None
            row[2] = int(row[2]) if row[2] is not None else None
            row[3] = int(row[3]) if row[3] is not None else None
            row[4] = int(row[4]) if row[4] is not None else None
            row[5] = int(row[5]) if row[5] is not None else None
            row[6] = int(row[6]) if row[6] is not None else None

            sql_query = "INSERT INTO views VALUES (%s, %s, %s, %s, %s, %s, %s);"
            cursor2.execute(sql_query, row)
            conn2.commit()




print("Quieres sobreescribir los datos actuales?(y/n)")
c = input()
if c == "y":
    print("Sobreescribiendo datos...")

    sql_query = "DELETE FROM views;"
    cursor2.execute(sql_query)
    sql_query = "DELETE FROM users;"
    cursor2.execute(sql_query)
    sql_query = "DELETE FROM content;"
    cursor2.execute(sql_query)
    sql_query = "DELETE FROM date;"
    cursor2.execute(sql_query)
    sql_query = "DELETE FROM hours;"
    cursor2.execute(sql_query)

    format_content()
    copy_users()
    generate_date()
    generate_hours()
    format_views()

