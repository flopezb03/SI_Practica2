import mysql.connector
import pandas as pd
import numpy as np
import random


def import_usuarios():
    df_usuarios = pd.read_csv("usuarios.csv")

    for i in range(len(df_usuarios)):
        row = list(df_usuarios.iloc[i])

        row[0] = int(row[0])

        sql_query = "INSERT INTO usuarios VALUES (%s, %s, %s);"
        cursor.execute(sql_query, row)
        conn.commit()

def import_tiempo():
    df_tiempo = pd.read_csv("tiempo.csv")

    for i in range(len(df_tiempo)):
        row = list(df_tiempo.iloc[i])

        row[0] = int(row[0])
        row[2] = int(row[2])

        sql_query = "INSERT INTO tiempo VALUES (%s, %s, %s);"
        cursor.execute(sql_query, row)
        conn.commit()

def import_peliculas_series():
    df_peliculas_series = pd.read_csv("peliculas_series.csv")
    df_peliculas_series = df_peliculas_series.where(pd.notnull(df_peliculas_series), None)

    for i in range(len(df_peliculas_series)):
        row = list(df_peliculas_series.iloc[i])

        row[7] = int(row[7]) if row[7] is not None else None
        row[9] = int(row[9]) if row[9] is not None else None

        sql_query = "INSERT INTO peliculas_series VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        cursor.execute(sql_query, row)
        conn.commit()

def import_visionados():
    df_visionados = pd.read_csv("visionados.csv")

    for i in range(len(df_visionados)):
        row = list(df_visionados.iloc[i])

        row[0] = int(row[0])
        row[1] = int(row[1])
        row[2] = int(row[2])
        row[4] = int(row[4])
        row[5] = int(row[5])

        sql_query = "INSERT INTO visionados VALUES (%s, %s, %s, %s, %s, %s);"
        cursor.execute(sql_query, row)
        conn.commit()





# Conectar con mysql
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="si_prac2_db"
)
cursor = conn.cursor()



