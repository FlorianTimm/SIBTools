# -*- coding: utf-8 -*-
"""
Überträgt Daten aus der SIB oder in die SIB
Ergänzung OraSchnittstelle

Version: 2018.12.20
"""
from sibtools import DataSource
import datetime
import cx_Oracle


class OraData (DataSource):
    __col_types = {
        cx_Oracle.STRING: str,
        cx_Oracle.NUMBER: int,
        cx_Oracle.DATETIME: datetime.datetime,
        cx_Oracle.FIXED_CHAR: str
    }

    def __init__(self, host, port, db, username, password, sql):
        """
        Erstellt eine neue Oracle-Datenquelle
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
        :param sql: SQL-Abfrage zum Export
                [Einfache Select-Abfrage für Export aus PostGIS (SELECT vnk, nnk, vst, bst, sonstwas FROM tab;)]
        :type sql: str
        """
        self._con = cx_Oracle.connect(username, password, host + ":" + str(port) + "/" + db)
        self.__ora_cursor = self._con.cursor()
        self._sql = sql
        self._executed = False
        self.__columns = None

    def __del__(self):
        self.__ora_cursor.close()
        self._con.close()

    def __check_query(self):
        if not self._executed:
            self.__ora_cursor.execute(self._sql)
            self._executed = True

    def _read_line(self):
        """
        Liest eine Zeile des Importes
        :return: Datenzeile des Datensatzes
        :rtype: list
        """
        self.__check_query()
        col = list(self._get_columns().keys())

        answer = {}
        try:
            z = self.__ora_cursor.fetchone()
            if z is None:
                return None
            for i in range(len(z)):
                answer[col[i]] = z[i]
        except ValueError:
            pass
        return answer

    def reset_line(self):
        """
        Setzt den Iterator auf den Startwert zurück
        """
        self.__ora_cursor.scroll(0, 'absolute')

    def _get_columns(self):
        """
        Gibt die Spalten des Importes zurück
        :return: Spalten der Quelle
        :rtype: dict
        """
        if self.__columns is not None:
            return self.__columns

        self.__check_query()
        self.__columns = {}
        print(self.__ora_cursor.description)
        for field in self.__ora_cursor.description:
            self.__columns[field[0]] = self.__col_types[field[1]]
            if field[1] is cx_Oracle.NUMBER and field[5] > 0:
                self.__columns[field[0]] = float
        return self.__columns
