import pandas as pd
import mysql.connector



class Data_warehouse:
    def __init__(self, dw=None):
        self.views = None
        self.users = None
        self.date = None
        self.content = None
        self.hours = None

        self.users_g = "user"
        self.date_g = "day"
        self.content_g = "ms"
        self.hours_g = "hour"

        self.conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="si_prac2_db"
        )
        self.cursor = self.conn.cursor()
        if dw is None:
            query = "SELECT * FROM views"
            self.cursor.execute(query)
            res = self.cursor.fetchall()
            cols = ["vid", "show_id", "uid", "tid", "hid", "rating", "viewings"]
            self.views = pd.DataFrame(res,columns=cols)

            query = "SELECT * FROM users"
            self.cursor.execute(query)
            res = self.cursor.fetchall()
            cols = ["uid", "name", "last_login"]
            self.users = pd.DataFrame(res,columns=cols)

            query = "SELECT * FROM date"
            self.cursor.execute(query)
            res = self.cursor.fetchall()
            cols = ["tid", "year", "month","day"]
            self.date = pd.DataFrame(res,columns=cols)

            query = "SELECT * FROM hours"
            self.cursor.execute(query)
            res = self.cursor.fetchall()
            cols = ["hid"]
            self.hours = pd.DataFrame(res,columns=cols)

            query = "SELECT * FROM content"
            self.cursor.execute(query)
            res = self.cursor.fetchall()
            cols = ["show_id", "type", "title", "director", "cast", "country", "date_added", "release_year", "rating", "duration", "listed_in", "description", "subtype"]
            self.content = pd.DataFrame(res,columns=cols)
        else:
            self.views = dw.views.copy()
            self.users = dw.users.copy()
            self.date = dw.date.copy()
            self.content = dw.content.copy()
            self.hours = dw.hours.copy()

            self.users_g = dw.users_g
            self.date_g = dw.date_g
            self.content_g = dw.content_g
            self.hours_g = dw.hours_g    

    def sliceNdice(self, dim, att, cond):
        
        match dim:
            case "content":
                self.content = self.content[self.content[att] == cond]
                self.views = self.views[self.views["show_id"].isin(self.content["show_id"])]
            case "date":
                self.date = self.date[self.date[att] == cond]
                self.views = self.views[self.views["tid"].isin(self.date["tid"])]
            case "user":
                self.user = self.user[self.user[att] == cond]
                self.views = self.views[self.views["uid"].isin(self.user["uid"])]
            case "hours":
                self.hours = self.hours[self.hours[att] == cond]
                self.views = self.views[self.views["hid"].isin(self.hours["hid"])]

    def roll(self, dim):
        match dim:
            case "date":
                if self.date_g == "day":
                    date_aux = self.date[["tid", "year", "month"]].copy()
                    date_aux["tid_aux"] = date_aux.groupby(["year", "month"]).ngroup()
                    self.date = date_aux[["tid_aux", "year", "month"]].drop_duplicates()
                    self.date = self.date.rename(columns={"tid_aux": "tid"})

                    views_aux = self.views.merge(date_aux[["tid", "tid_aux"]], on="tid")
                    self.views = views_aux.groupby(["uid", "show_id","hid", "tid_aux"]).agg({"viewings": "sum","rating": "mean"}).reset_index()
                    self.views = self.views.rename(columns={"tid_aux": "tid"})
                    
                    self.date_g = "month"
                elif self.date_g == "month":
                    date_aux = self.date[["tid", "year"]].copy()
                    date_aux["tid_aux"] = date_aux.groupby(["year"]).ngroup()
                    self.date = date_aux[["tid_aux", "year"]].drop_duplicates()
                    self.date = self.date.rename(columns={"tid_aux": "tid"})

                    views_aux = self.views.merge(date_aux[["tid", "tid_aux"]], on="tid")
                    self.views = views_aux.groupby(["uid", "show_id","hid", "tid_aux"]).agg({"viewings": "sum","rating": "mean"}).reset_index()
                    self.views = self.views.rename(columns={"tid_aux": "tid"})

                    self.date_g = "year"
                elif self.date_g == "year":
                    self.date = self.date[["tid"]]
                    self.date["tid"] = "all"
                    self.date = self.date.drop_duplicates()

                    self.views = self.views.groupby(["uid", "show_id", "hid"]).agg({"viewings": "sum","rating": "mean"}).reset_index()
                    self.views["tid"] = "all"

                    self.date_g = "all"
            case "hours":
                if self.hours_g == "hour":
                    self.hours = self.hours[["hid"]]
                    self.hours["hid"] = "all"
                    self.hours = self.hours.drop_duplicates()

                    self.views = self.views.groupby(["uid", "show_id", "tid"]).agg({"viewings": "sum","rating": "mean"}).reset_index()
                    self.views["hid"] = "all"

                    self.hours_g = "all"
            case "content":
                if self.content_g == "ms":
                    ms_aux = self.content[["show_id", "type", "subtype"]].copy()
                    ms_aux["show_id_aux"] = ms_aux.groupby(["type", "subtype"]).ngroup()
                    self.content = ms_aux[["show_id_aux", "type", "subtype"]].drop_duplicates()
                    self.content = self.content.rename(columns={"show_id_aux": "show_id"})

                    views_aux = self.views.merge(ms_aux[["show_id", "show_id_aux"]], on="show_id")
                    self.views = views_aux.groupby(["uid", "tid","hid", "show_id_aux"]).agg({"viewings": "sum","rating": "mean"}).reset_index()
                    self.views = self.views.rename(columns={"show_id_aux": "show_id"})
                    
                    self.content_g = "subtype"
                elif self.content_g == "subtype":
                    ms_aux = self.content[["show_id", "type"]].copy()
                    ms_aux["show_id_aux"] = ms_aux.groupby(["type"]).ngroup()
                    self.content = ms_aux[["show_id_aux", "type"]].drop_duplicates()
                    self.content = self.content.rename(columns={"show_id_aux": "show_id"})

                    views_aux = self.views.merge(ms_aux[["show_id", "show_id_aux"]], on="show_id")
                    self.views = views_aux.groupby(["uid", "tid","hid", "show_id_aux"]).agg({"viewings": "sum","rating": "mean"}).reset_index()
                    self.views = self.views.rename(columns={"show_id_aux": "show_id"})
                    
                    self.content_g = "type"
                elif self.content_g == "type":
                    self.content = self.content[["show_id"]]
                    self.content["show_id"] = "all"
                    self.content = self.content.drop_duplicates()

                    self.views = self.views.groupby(["uid", "hid", "tid"]).agg({"viewings": "sum","rating": "mean"}).reset_index()
                    self.views["show_id"] = "all"

                    self.content_g = "all"
            case "users":
                if self.users_g == "user":
                    self.users = self.users[["uid"]]
                    self.users["uid"] = "all"
                    self.users = self.users.drop_duplicates()

                    self.views = self.views.groupby(["show_id", "hid", "tid"]).agg({"viewings": "sum","rating": "mean"}).reset_index()
                    self.views["uid"] = "all"

                    self.users_g = "all"
            
    def drill(self, dim):
        pass

    def copy(self):
        return Data_warehouse(self)

