import mysql.connector
import pandas as pd
import numpy as np
import random
from ipywidgets import widgets
from IPython.display import display
import matplotlib.pyplot as plt
import os
from data_warehouse import Data_warehouse



# Conectar con mysql
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="si_prac2_db"
    )
    cursor = conn.cursor()
except mysql.connector.Error:
    print("No se ha podido conectar con el almacen de datos")


# Ej1
ej1_type = widgets.Dropdown(options=["", "Peliculas", "Series"], value="", description="Tipo:")
ej1_subtype = widgets.Dropdown(options=[], description="Subtipo:", disabled=True)
ej1_n = widgets.IntSlider(value=10, min=1, max=30, step=1, description="Top N:", continuous_update=False)

def ej1_widgets():
    def update_ej1_widgets(change):
        if ej1_type.value == "Series":
            ej1_subtype.options = ["", "1 Temporada", "2 Temporadas", ">2 Temporadas"]
            ej1_subtype.disabled = False
        elif ej1_type.value == "Peliculas":
            ej1_subtype.options = ["", "<90 minutos", ">90 minutos"]
            ej1_subtype.disabled = False
        else:
            ej1_subtype.options = []
            ej1_subtype.disabled = True

    ej1_type.observe(update_ej1_widgets, "value")

    display(ej1_type, ej1_subtype, ej1_n)

def ej1_query(type, subtype, n):

    dw = Data_warehouse()

    dw.roll("users")
    dw.roll("hours")
    dw.roll("date")
    dw.roll("date")
    dw.roll("date")

    match type:
        case "Peliculas":
            match subtype:
                case "":
                    dw.sliceNdice("content","type","Movie")
                case None:
                    dw.sliceNdice("content","type","Movie")
                case "<90 minutos":
                    dw.sliceNdice("content","subtype","-90")
                case ">90 minutos":
                    dw.sliceNdice("content","subtype","+90")
        case "Series":
            match subtype:
                case "":
                    dw.sliceNdice("content","type","TV Show")
                case None:
                    dw.sliceNdice("content","type","TV Show")
                case "1 Temporada":
                    dw.sliceNdice("content","subtype","S1")
                case "2 Temporadas":
                    dw.sliceNdice("content","subtype","S2")
                case ">2 Temporadas":
                    dw.sliceNdice("content","subtype","S2+")
        case "":
            pass

    out = pd.merge(dw.views,dw.content, on="show_id")
    out = out[["show_id", "title", "viewings"]].sort_values(by="viewings", ascending=False).head(n).sort_values(by="viewings",ascending=True)

    return out

def ej1_display():
    # Recoger parametros
    type = ej1_type.value
    subtype = ej1_subtype.value
    n = ej1_n.value

    # Consulta
    df = ej1_query(type,subtype,n)

    # Dibujar grafica    
    plt.figure(figsize=(20, n*0.75))
    plt.barh(df["title"], df["viewings"])
    plt.ylabel(type)
    plt.xlabel("Visualizaciones")
    plt.grid(True, axis='x')

    plt.show()




# Ej2 
ej2_year = widgets.Dropdown(options=["2024"], value="2024",description="Año: ")
ej2_month = widgets.Dropdown(options=["Anual", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"], value="Anual", description="Mes: ")

def ej2_widgets():

    display(ej2_year, ej2_month)

def ej2_query(y,m):
    
    dw = Data_warehouse()

    dw.roll("date")
    dw.roll("content")
    dw.roll("content")
    dw.roll("content")
    dw.roll("users")

    match m:
        case "Anual":
            dw.roll("date")
        case "Enero":
            dw.sliceNdice("date","month",1)
        case "Febrero":
            dw.sliceNdice("date","month",2)
        case "Marzo":
            dw.sliceNdice("date","month",3)
        case "Abril":
            dw.sliceNdice("date","month",4)
        case "Mayo":
            dw.sliceNdice("date","month",5)
        case "Junio":
            dw.sliceNdice("date","month",6)
        case "Julio":
            dw.sliceNdice("date","month",7)
        case "Agosto":
            dw.sliceNdice("date","month",8)
        case "Septiembre":
            dw.sliceNdice("date","month",9)
        case "Octubre":
            dw.sliceNdice("date","month",10)
        case "Noviembre":
            dw.sliceNdice("date","month",11)
        case "Diciembre":
            dw.sliceNdice("date","month",12)

    dw.sliceNdice("date","year",int(y))

    return dw.views[["hid","viewings"]]

def ej2_display():
    # Recoger parametros
    y = ej2_year.value
    m = ej2_month.value

    # Consulta
    df = ej2_query(y,m)

    # Dibujar grafica
    plt.figure(figsize=(20,10))
    plt.bar(df["hid"], df["viewings"])
    plt.xticks(ticks=range(24))
    plt.grid(True, axis='y')
    plt.show()
    



# Ej3
ej3_user = widgets.IntText(value=0,description="ID de usuario")

def ej3_widgets():
    display(ej3_user)

def ej3():
    # Recoger parametros
    v = ej3_user.value
    query = "SELECT uid FROM users WHERE uid="+str(v)
    cursor.execute(query)
    res = cursor.fetchall()
    
    if res == []:
        print("ID de usuario no valida")
        return False


    # Consulta de todos las visualizaciones
    query = """
    SELECT 
        show_id,
        uid,
        rating
    FROM views
    """
    cursor.execute(query)
    res = cursor.fetchall()
    cols = ["show_id","uid", "rating"]
    df_views = pd.DataFrame(res,columns=cols)

    # Visualizaciones de V
    vdf = df_views[df_views["uid"]==v]

    # Modulo de V
    u1_mod = np.sqrt((vdf["rating"] ** 2).sum())

    # Consulta de todos los usuarios
    query = "SELECT uid FROM users"
    cursor.execute(query)
    res = cursor.fetchall()
    df_users = pd.DataFrame(res,columns=["uid"])
    df_users = df_users[df_users["uid"]!=v]

    # Bucle de seleccion de usuario U mas parecido a V
    min_cos = (None,1.1)
    for u in df_users["uid"]:
        # Visualizaciones de U
        udf = df_views[df_views["uid"] == u]

        # Join de visualizaciones (todas)
        vudf = pd.merge(vdf,udf,on="show_id",how="outer").fillna(0)

        # Modulo de U
        u2_mod = np.sqrt((udf["rating"] ** 2).sum())
        # Producto vectorial V*U
        u1u2 = (vudf["rating_x"] * vudf["rating_y"]).sum()

        # Coseno
        cos = u1u2/(u1_mod*u2_mod)
        if cos < min_cos[1]:
            min_cos = (u,cos)

    # Sacar las peliculas_series recomendables por U
    u_ms = df_views[df_views["uid"] == min_cos[0]]
    recommendables = u_ms[~u_ms["show_id"].isin(vdf["show_id"])].sort_values(by="rating",ascending=False)

    # Salida
    query = "SELECT name FROM users WHERE uid="+str(v)
    cursor.execute(query)
    res1 = cursor.fetchall()
    query = "SELECT name FROM users WHERE uid="+str(min_cos[0])
    cursor.execute(query)
    res2 = cursor.fetchall()

    query = "SELECT show_id,title FROM content WHERE show_id='"+recommendables["show_id"].iloc[0]+"'"
    cursor.execute(query)
    res3 = cursor.fetchall()

    print("El usuario mas parecido de "+str(v)+"("+res1[0][0]+")"+" es "+str(min_cos[0])+"("+res2[0][0]+")"+". La recomendacion es "+res3[0][0]+"("+res3[0][1]+")")
    

# Ej4
def ej4():

    # Consulta
    dw = Data_warehouse()

    dw.roll("date")
    dw.roll("date")
    dw.roll("date")
    dw.roll("hours")
    dw.roll("content")
    dw.roll("content")

    dw_m = dw.copy()
    dw_m.sliceNdice("content","type","Movie")
    df_m = dw_m.views[["uid","viewings"]]

    dw_s = dw.copy()
    dw_s.sliceNdice("content","type","TV Show")
    df_s = dw_s.views[["uid","viewings"]]

    df = pd.merge(df_m,df_s,on="uid").fillna(0)
    df = df.rename(columns={"viewings_x": "movies_viewings","viewings_y": "series_viewings"})
    df = df[["movies_viewings", "series_viewings"]]

    # Pasar a csv
    df.to_csv("cluster_ms.csv", index=False)

    # Usar weka para algorimto de clustering (SimpleKMeans)
    os.system('java -cp weka.jar weka.clusterers.SimpleKMeans -init 0 -max-candidates 100 -periodic-pruning 10000 -min-density 2.0 -t1 -1.25 -t2 -1.0 -N 4 -A "weka.core.EuclideanDistance -R first-last" -I 500 -num-slots 1 -S 10 -t cluster_ms.csv -p 0 > cluster_res.csv')

    # Leer resultado
    df_aux = pd.read_csv("cluster_res.csv",sep="\s+",header=None)
    df["cluster"] = df_aux[1]

    # Grafica
    plt.figure(figsize=(12, 12))
    for c in sorted(df["cluster"].unique()):
        cluster_df = df[df["cluster"] == c]
        plt.scatter(cluster_df["movies_viewings"], cluster_df["series_viewings"])

    plt.xlabel("Películas vistas")
    plt.ylabel("Series vistas")
    plt.grid(True)
    plt.show()








