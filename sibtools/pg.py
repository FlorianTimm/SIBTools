# -*- coding: utf-8 -*-
"""
Überträgt Daten aus der SIB oder in die SIB
Ergänzung PostGIS-Schnittstelle

Version: 2018.12.20
"""
from sibtools import DataSource, DataTarget, Geometry
import pg8000
from datetime import datetime


class PgData (DataSource, DataTarget):
    """
    PostGIS-Datenquelle/-ziel
    """

    def __init__(self, host, port, db, username, password, select_sql=None, insert_table=None, insert_fieldnames=None):
        """
        Erstellt eine neue PostGIS-Datenquelle
        :param host:
        :type host: str
        :param port:
        :type port: int
        :param db:
        :type db: str
        :param username:
        :type username: str
        :param password:
        :type password: str
        :param select_sql: SQL-Abfrage zum Export
                [Einfache Select-Abfrage für Export aus PostGIS (SELECT vnk, nnk, vst, bst, sonstwas FROM tab;)
        :type select_sql: str
        :param insert_table: Tabelle, in welche importiert werden soll (schema.table)
        :type insert_table: str
        :param insert_fieldnames: Zu importierende Felder
        :type insert_fieldnames: list
        """
        self.__db_connection = pg8000.connect(username, host, None, port, db, password)
        self.__db_cursor = self.__db_connection.cursor()
        self.__select_sql = select_sql
        self.__insert_table = insert_table
        self.__insert_fieldnames = insert_fieldnames

        self.__columns = {}
        self.__select_executed = False

    def create_table(self, datasource, insert_table):
        """
        Erstellt eine Tabelle gemäß der Datenquelle
        :param datasource: Datenquelle
        :type datasource: DataSource
        :param insert_table: Tabelle, in welche importiert werden soll (schema.table)
        :type insert_table: str
        """

        sql = "CREATE TABLE IF NOT EXISTS " \
              + insert_table + \
              " (gid serial primary key"
        insert_fieldnames = []
        zellen = datasource.get_columns()

        typ = {str: "character varying",
               int: "bigint",
               bool: "boolean",
               float: "decimal",
               datetime: "timestamp",
               Geometry: "geometry"}

        for zelle in zellen.keys():
            # print(zellen[zelle])
            sql += ", " + zelle + " "
            if zellen[zelle] in typ:
                sql += typ[zellen[zelle]]
            else:
                sql += "character varying"
            insert_fieldnames.append(zelle)

        sql += ")"
        print(sql)
        self.__db_cursor.execute(sql)
        self.__db_cursor.execute("TRUNCATE TABLE " + insert_table)
        self.set_insert_config(insert_table, insert_fieldnames)

    def config_select(self, select_sql):
        """
        Setzt die Parameter für den Datenexport
        :param select_sql: SQL-Abfrage zum Export
                [Einfache Select-Abfrage für Export aus PostGIS (SELECT vnk, nnk, vst, bst, sonstwas FROM tab;)
        :type select_sql: str
        """
        self.__select_sql = select_sql

    def set_insert_config(self,  insert_table, insert_fieldnames):
        """
        Setzt die Parameter für den Datenimport
        :param insert_table: Tabelle, in welche importiert werden soll (schema.table)
        :type insert_table: str
        :param insert_fieldnames: Zu importierende Felder
        :type insert_fieldnames: list
        """
        self.__insert_table = insert_table
        self.__insert_fieldnames = insert_fieldnames

    def __del__(self):
        self.__db_connection.commit()
        self.__db_cursor.close()
        self.__db_connection.close()

    def reset_line(self):
        """
        Setzt den Iterator auf den Startwert zurück
        """
        self.__execute_select_query()
        # self.__db_cursor.scroll(0, 'absolute')

    def _read_line(self):
        """
        Liest eine Zeile des Importes
        :return: Datenzeile des Datensatzes
        :rtype: dict
        """
        if not self.__select_executed:
            self.__execute_select_query()
        col = list(self._get_columns().keys())

        answer = {}
        try:
            z = self.__db_cursor.fetchone()
            if z is None:
                return None
            for i in range(len(z)):
                answer[col[i]] = z[i]
        except ValueError:
            pass
        return answer

    def _get_columns(self):
        """
        Gibt die Spalten des Importes zurück
        :return: Spalten der Quelle
        :rtype: dict
        """
        if len(self.__columns) > 0:
            return self.__columns

        column_types = {
            16: bool,
            23: int,
            25: str,
            701: float,
            1043: str
        }

        if not self.__select_executed:
            self.__execute_select_query()
        columns = {}
        for c in self.__db_cursor.description:
            # print(c)
            if c[1] not in column_types:
                columns[c[0].decode("utf-8")] = None
            else:
                columns[c[0].decode("utf-8")] = column_types[c[1]]
        self.__columns = columns
        return columns

    def __execute_select_query(self):
        if self.__select_sql is None:
            raise Exception("Vorher SELECT-Config ausfüllen")
        self.__db_cursor.execute(self.__select_sql)
        self.__select_executed = True

    def execute_sql(self, sql):
        """
        Führt einen SQL-Befehl auf der Datenbank durch
        ACHTUNG: Führt Befehle direkt aus, kann zu Fehlern/Datenverlust führen
        :param sql: SQL-Befehl
        :type sql: str
        """
        self.__db_cursor.execute(sql)

    def fetchone(self):
        """
        Gibt den nächsten Datensatz roh zurück
        ACHTUNG: Führt Befehle direkt aus, kann zu Fehlern/Datenverlust führen
        :return: Datensatz
        :rtype: list
        """
        return self.__db_cursor.fetchone()

    def write(self, data_source):
        """
        Schreibt eine Zeile zum Importieren
        :param data_source: List Datenzeile des Importes
        :type data_source: DataSource
        :return: Erfolgreich importiert?
        :rtype: bool
        """
        if len(self.__insert_fieldnames) is None or self.__insert_table is None:
            raise Exception("Vorher INSERT-Config ausfüllen")

        liste = []
        zellen = data_source.get_columns()

        sql = "INSERT INTO " + self.__insert_table + "("
        for c in self.__insert_fieldnames:
            sql += c + ","
        sql = sql[:-1] + ") values ("
        for field in self.__insert_fieldnames:
            if field in zellen and zellen[field] == Geometry:
                sql += "ST_GeomFromGML(%s),"
            else:
                sql += "%s,"
        sql = sql[:-1] + ")"

        print(sql)

        while True:
            data = data_source.read_line()
            zeile = []
            if data is None:
                break
            for field in self.__insert_fieldnames:
                if field in data:
                    d = data[field]
                    zeile.append(str(d))
                else:
                    zeile.append(None)
            liste.append(zeile)

        self.__db_cursor.executemany(sql, liste)
        self.__db_connection.commit()

