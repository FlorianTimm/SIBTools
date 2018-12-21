# -*- coding: utf-8 -*-
"""
Überträgt Daten aus der SIB oder in die SIB
Ergänzung PostGIS-Schnittstelle

Version: 2018.12.20
"""
from sibtools import DataSource, DataTarget
import psycopg2
import psycopg2.extras


class PgData (DataSource, DataTarget):
    """
    PostGIS-Datenquelle/-ziel
    """

    def __init__(self, host, port, db, username, password, sql):
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
        :param sql: SQL-Abfrage zum Im- oder Export
                [Einfache Select-Abfrage für Export aus PostGIS (SELECT vnk, nnk, vst, bst, sonstwas FROM tab;),
                Insert-PreparedStatement für Import (INSERT INTO tab (vnk, nnk, vst, bst, sonstwas)
                    VALUES (%s,%s,%s,%s);)]
        :type sql: str
        """
        login = "host='" + host + "' dbname='" + db + "' port='" + str(port) + \
                "' user='" + username + "' password='" + password + "'"
        self.__db_connection = psycopg2.connect(login)
        self.__sql = sql
        self.__db_cursor = None

    def __del__(self):
        self.__db_connection.commit()
        self.__db_cursor.close()
        self.__db_connection.close()

    def reset_line(self):
        """
        Setzt den Iterator auf den Startwert zurück
        """
        if self.__db_cursor is None:
            self.__execute_select_query()
        self.__db_cursor.scroll(0, 'absolute')

    def _read_line(self):
        """
        Liest eine Zeile des Importes
        :return: Datenzeile des Datensatzes
        :rtype: dict
        """
        if self.__db_cursor is None:
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
        column_types = {
            16: bool,
            23: int,
            25: str,
            701: float,
            1043: str,
            16400: None
        }

        if self.__db_cursor is None:
            self.__execute_select_query()
        columns = {}
        for c in self.__db_cursor.description:
            columns[c.name] = column_types[c.type_code]
        return columns

    def __execute_select_query(self):
        self.__db_cursor = self.__db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.__db_cursor.execute(self.__sql)

    def write(self, data_source):
        """
        Schreibt eine Zeile zum Importieren
        :param data_source: List Datenzeile des Importes
        :type data_source: DataSource
        :return: Erfolgreich importiert?
        :rtype: bool
        """
        if self.__db_cursor is None:
            self.__prepare_insert_statement()
        self.__db_cursor.execute(data_source)

        # TODO Funktion schreiben
        pass

    def __prepare_insert_statement(self):
        """
        Bereitet das SQL-Statement für den Insert vor
        """
        self.__db_cursor = self.__db_connection.cursor()
        self.__db_cursor.prepare(self.__sql)
