import mysql.connector
import numpy as np
import random
import pandas as pd


def generate_random_users(n):
    name_list = ["Jorge","Gerardo","Daniel","Lidia","Julia","Sergio","Victor","Enrique",
                 "Alvaro","Alonso","Ivan","Paula","Natalia","Marcos","Alejandro","Fernando",
                 "Jose Ignacio", "Pablo", "Aitor", "Maria", "Jon", "Cesar", "Gustavo", 
                 "Javier", "David", "Jesus", "Aaron", "Francisco", "Gabriel", "Ana", "Loreto",
                 "Oleksandr"]
    max_year = 2025
    min_year = 2024
    for i in range(n):
        uid = i
        name = name_list[random.randint(0,len(name_list)-1)]
        year = str(random.randint(min_year,max_year))
        month = str(random.randint(1,12))
        match month:
            case "1" | "3" | "5" | "7" | "8" | "10" | "12":
                max_day = 31
            case "4" | "6" | "9" | "11":
                max_day = 30
            case "2":
                max_day = 28
        day = str(random.randint(1,max_day))
        hour = str(random.randint(0,23))
        minutes = str(random.randint(0,59))
        seconds = str(random.randint(0,59))

        month = '0'+month if len(month) == 1 else month
        day = '0'+day if len(day) == 1 else day
        hour = '0'+hour if len(hour) == 1 else hour
        minutes = '0'+minutes if len(minutes) == 1 else minutes
        seconds = '0'+seconds if len(seconds) == 1 else seconds

        datetime = year+'-'+month+'-'+day+' '+hour+'-'+minutes+'-'+seconds

        row = [uid,name,datetime]

        sql_query = "INSERT INTO usuarios VALUES (%s, %s, %s);"
        cursor.execute(sql_query, row)
        conn.commit()




conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="si_prac1_db"
)
cursor = conn.cursor()



sql_query = "SELECT COUNT(*) FROM usuarios;"
cursor.execute(sql_query)
rows_usuarios = cursor.fetchall()[0][0]
if rows_usuarios == 0:
    print("Generando usuarios...")
    generate_random_users(200)
    sql_query = "SELECT COUNT(*) FROM usuarios;"
    cursor.execute(sql_query)
    rows_usuarios = cursor.fetchall()[0][0]


sql_query = "SELECT COUNT(*) FROM visionados;"
cursor.execute(sql_query)
rows_visionados = cursor.fetchall()[0][0]

if rows_visionados == 0:
    print("Generando visionados...")

    sql_query = "SELECT show_id FROM peliculas_series WHERE type='Movie';"
    cursor.execute(sql_query)
    res = cursor.fetchall()
    mdf = pd.DataFrame(res,columns=["show_id"])

    sql_query = "SELECT show_id FROM peliculas_series WHERE type='TV Show';"
    cursor.execute(sql_query)
    res = cursor.fetchall()
    sdf = pd.DataFrame(res,columns=["show_id"])

    vid = 0
    for user in range(rows_usuarios):
        u_type = random.randint(0,3)
        match u_type:
            case 0: 
                m_vis = int(np.random.normal(loc=20, scale=5))
                m_vis = min(max(m_vis, 1), 35)
                s_vis = int(np.random.normal(loc=20, scale=5))
                s_vis = min(max(s_vis, 1), 35)
            case 1: 
                m_vis = int(np.random.normal(loc=20, scale=5))
                m_vis = min(max(m_vis, 1), 35)
                s_vis = int(np.random.normal(loc=60, scale=5))
                s_vis = min(max(s_vis, 45), 80)
            case 2: 
                m_vis = int(np.random.normal(loc=60, scale=5))
                m_vis = min(max(m_vis, 45), 80)
                s_vis = int(np.random.normal(loc=20, scale=5))
                s_vis = min(max(s_vis, 1), 35)
            case 3: 
                m_vis = int(np.random.normal(loc=60, scale=5))
                m_vis = min(max(m_vis, 45), 80)
                s_vis = int(np.random.normal(loc=60, scale=5))
                s_vis = min(max(s_vis, 45), 80)

        for i in range(m_vis):
            psid = random.randint(0,149)

            row = [vid,mdf["show_id"].iloc[psid],user,random.randint(1,10)]

            sql_query = "INSERT INTO visionados VALUES (%s, %s, %s, %s);"
            cursor.execute(sql_query, row)
            conn.commit()
            vid += 1
        for i in range(s_vis):
            psid = random.randint(0,149)

            row = [vid,sdf["show_id"].iloc[psid],user,random.randint(1,10)]

            sql_query = "INSERT INTO visionados VALUES (%s, %s, %s, %s);"
            cursor.execute(sql_query, row)
            conn.commit()
            vid += 1




