import mysql.connector
import pandas as pd
import numpy as np
import random


def generate_random_dates(n):
    dates = []
    #years = list(range(2000,2025))
    for _ in range(n):
        # Elegir una temporada con sesgo
        season = np.random.choice(["navidad", "verano", "normal"], p=[0.35, 0.35, 0.3])

        if season == "navidad":
            day = str(random.randint(20, 31))
            month = "12"
        elif season == "verano":
            month = str(random.randint(6, 8))
            day = str(random.randint(1, 30))
        else:  # normal
            month = str(random.randint(1, 12))
            day = str(random.randint(1, 28))

        
        #year = str(random.choice(years))
        year = "2024"
        if len(day)==1:
            day="0"+day
        if len(month)==1:
            month="0"+month

        date = year+"-"+month+"-"+day
        
        dates.append(date)
    return dates

def generate_random_hours(n):
    hours = []
    for _ in range(n):
        time_slot = np.random.choice(["primetime", "resto"], p=[0.7, 0.3])
        if time_slot == "primetime":
            hour = random.choice(list(range(19, 24)) + list(range(0, 2)))  # 19-23, 0-1
        else:
            hour = random.randint(2, 18)
        hours.append(hour)
    return hours

# Conectar con mysql
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="si_prac1_db"
)
cursor = conn.cursor()

# peliculas_series

sql_query = "SELECT * FROM peliculas_series;"
cursor.execute(sql_query)
res = cursor.fetchall()

cols = ["show_id", "type", "title", "director", "cast", "country", "date_added", "release_year", "rating", "duration", "listed_in", "description"]
df_peliculas_series = pd.DataFrame(res,columns=cols)

df_peliculas_series.to_csv("peliculas_series.csv", index=False)


# usuarios

sql_query = "SELECT * FROM usuarios;"
cursor.execute(sql_query)
res = cursor.fetchall()

cols = ["uid", "name", "last_login"]
df_usuarios = pd.DataFrame(res,columns=cols)

df_usuarios.to_csv("usuarios.csv", index=False)


# tiempo

n = 100
df_tiempo = pd.DataFrame({
    "tid": range(1, n+1),
    "date": generate_random_dates(n),
    "hour": generate_random_hours(n)
})

df_tiempo.to_csv("tiempo.csv",index=False)


# visionados
sql_query = "SELECT * FROM visionados;"
cursor.execute(sql_query)
res = cursor.fetchall()

cols = ["vid", "show_id", "uid", "rating"]
df_visionados = pd.DataFrame(res,columns=cols)

df_visionados["tid"] = df_visionados.apply(lambda row: random.choice(df_tiempo["tid"].tolist()), axis=1)
df_visionados["num_visionados"] = 1

df_visionados_new = df_visionados[["vid", "uid", "tid", "show_id", "rating", "num_visionados"]]

df_visionados_new.to_csv("visionados.csv", index=False)







