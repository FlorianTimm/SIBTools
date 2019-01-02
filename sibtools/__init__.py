# -*- coding: utf-8 -*-
"""
Überträgt Daten aus der SIB oder in die SIB

Version: 2018.12.20
"""
import os
from datetime import datetime

import requests
from xml.etree import ElementTree
import xml.dom.minidom

# Python 3 - abstrakte Klassen
# import abc

# Nicht-Standar-Python
from requests.auth import HTTPBasicAuth
import dbf


class DataSource (object):  # (abc.ABC):
    """
    Abstrakte Datenquelle für den Export
    """
    _rename_dict = {}
    _add_columns = {}
    _remove_columns = []
    _von_netzknoten = None
    _nach_netzknoten = None

    def read_line(self):
        """
        Liest eine Zeile des Importes
        :return: Datenzeile des Datensatzes
        :rtype: dict
        """
        line = self._read_line()

        if line is None:
            return None

        for att in self._rename_dict:
            if att in line:
                line[self._rename_dict[att]] = line.pop(att)

        for att in self._remove_columns:
            if att in line:
                del line[att]

        # print(self._add_columns)
        for att in self._add_columns:
            line[att] = self._add_columns[att]

        if self._von_netzknoten is not None and self._von_netzknoten in line:
            line['vtkNummer'], line['vnkLfd'], line['vzusatz'] = self.__teile_nk(line[self._von_netzknoten])
            del line[self._von_netzknoten]
        if self._nach_netzknoten is not None and self._nach_netzknoten in line:
            line['ntkNummer'], line['nnkLfd'], line['nzusatz'] = self.__teile_nk(line[self._nach_netzknoten])
            del line[self._nach_netzknoten]

        return line

    # @abc.abstractmethod
    def reset_line(self):
        """
        Setzt den Iterator auf den Startwert zurück
        """
        raise Exception("Abstrakte Methode aufgerufen")

    def show(self, limit=10):
        """
        Gibt die Werte der Quelle aus
        :param limit: Maximale Anzahl der Zeilen (default=10)
        :type limit: int
        """
        self.reset_line()
        spalten = self.get_columns()
        t = ""
        for spalte in spalten:
            t += str(spalte) + "\t"
        print(t)

        for i in range(limit):
            z = self.read_line()
            if z is None:
                break
            t = ""
            for spalte in spalten:
                if spalte in z:
                    t += str(z[spalte]) + "\t"
            print(t)
        self.reset_line()

    @staticmethod
    def __teile_nk(netzknoten):
        """
        Teilt Netzknoten 123405678Z in tk=1234, lfd=5678, zusatz=Z
        :param netzknoten: Netzknoten
        :type netzknoten: str
        :return: TK-Blatt, Lfd. Nummer, Zusatz
        :rtype: tuple
        """
        # print(zeile)
        tk = int(netzknoten[0:4])
        lfd = int(netzknoten[4:9])
        if len(netzknoten) > 9:
            zusatz = netzknoten[9]
        else:
            zusatz = "O"
        return tk, lfd, zusatz

    # @abc.abstractmethod
    def _read_line(self):
        """
        Liest eine Zeile des Importes
        :return: Datenzeile des Datensatzes
        :rtype: dict
        """
        print("Dies sollte nie erscheinen...")

    def get_columns(self):
        """
        Gibt die Spalten des Importes zurück
        :return: Spalten der Quelle
        :rtype: dict
        """
        columns = self._get_columns()

        if columns is None:
            return None

        for att in self._rename_dict:
            if att in columns:
                columns[self._rename_dict[att]] = columns[self._rename_dict[att]]

        for att in self._remove_columns:
            if att in columns:
                columns.pop(att)

        for att in self._add_columns:
            columns[att] = type(self._add_columns[att])

        if self._von_netzknoten is not None and self._von_netzknoten in columns:
            columns['vtkNummer'] = int
            columns['vnkLfd'] = int
            columns['vzusatz'] = str
        if self._nach_netzknoten is not None and self._nach_netzknoten in columns:
            columns['ntkNummer'] = int
            columns['nnkLfd'] = int
            columns['nzusatz'] = str

        return columns

    # @abc.abstractmethod
    def _get_columns(self):
        """
        Gibt die Spalten des Importes zurück
        :return: Spalten der Quelle
        :rtype: dict
        """
        raise Exception("Abstrakte Methode aufgerufen")

    def rename_attributes(self, rename_dict):
        """
        Benennt die Attribute um
        :param rename_dict: Dictionary mit Umbenenung: {'alt1': 'neu1', 'alt2': 'neu2'}
        :type rename_dict: dict
        :return:
        """
        self._rename_dict = rename_dict

    def remove_columns(self, remove_columns):
        """
        Entfernt Spalten
        :param remove_columns: Liste der zu entfernenden Spalten (default = [])
        :type remove_columns: list
        """
        self._remove_columns = remove_columns

    def add_columns(self, add_columns):
        """
        Entfernt Spalten
        :param add_columns: Dict der hinzuzufügenden Spalten, {'bearbeiter': 'NUTZER', 'stand': datetime(2017, 12, 10}}
        :type add_columns: dict
        """
        self._add_columns = add_columns

    def nk2tklfdz(self, von_netzknoten, nach_netzknoten):
        """
        Zerteilt die Netzknoten für den WFS
        :param von_netzknoten: von Netzknoten
        :type von_netzknoten: str
        :param nach_netzknoten: nach Netzknoten
        :type nach_netzknoten: str
        """
        self._von_netzknoten = von_netzknoten
        self._nach_netzknoten = nach_netzknoten


class DataTarget (object):  # (abc.ABC):
    """
    Abstraktes Datenziel für den Import
    """

    # @abc.abstractmethod
    def write(self, data_source):
        """
        Schreibt eine Zeile zum Importieren
        :param data_source: List Datenzeile des Importes
        :type data_source: DataSource
        :return: Erfolgreich importiert?
        :rtype: bool
        """
        raise Exception("Abstrakte Methode aufgerufen")


class CsvData (DataSource, DataTarget):
    """
    CSV-Datenquelle/-ziel
    """
    __lines = None

    def __init__(self, filename):
        """
        Erzeugt eine neue CSV-Datenquelle/-ziel
        :param filename: Dateiname
        :type filename: str
        """
        self.__filename = filename

    def _read_line(self):
        """
        Liest eine Zeile des Importes
        :return: Datenzeile des Datensatzes
        :rtype: list
        """
        zeile = {}
        col = list(self._get_columns().keys())
        if self.__lines is None:
            self.__get_data_from_csv()

        if len(self.__lines) <= 0:
            return None

        line = self.__lines.pop(0)
        csv_zeile = line.split(";")

        for c in range(len(csv_zeile)):
            if csv_zeile[c] == "":
                pass
            elif csv_zeile[c][0] == "\"" and csv_zeile[c][-1] == "\"":
                zeile[col[c]] = csv_zeile[c][1:-1]
            else:
                zeile[col[c]] = csv_zeile[c]
        return zeile

    def reset_line(self):
        """
        Setzt den Iterator auf den Startwert zurück
        """
        self.__get_data_from_csv()

    def __get_data_from_csv(self):
        csv = open(self.__filename, "r")
        self.__lines = csv.readlines()
        csv.close()
        self.__lines.pop(0)

    def write(self, datasource):
        """
        Schreibt eine Zeile zum Importieren
        :param datasource: Importdatensatz
        :type datasource: DataSource
        :return: Erfolgreich importiert?
        :rtype: bool
        """
        csv = open(self.__filename, "w+")
        txt = ""
        columns = datasource.get_columns()

        if columns is None:
            return False

        for col in columns:
            txt += "\"" + col + "\";"
        txt = txt[:-1] + "\n"
        while True:
            line = datasource.read_line()
            if line is None:
                break
            for col in columns:
                c_type = columns[col]
                if col in line:
                    if line[col] is None:
                        pass
                    elif c_type in [int, float]:
                        txt += str(line[col]).replace(".", ",")
                    else:
                        txt += "\"" + str(line[col]) + "\""
                txt += ";"
            txt = txt[:-1] + "\n"
            csv.write(txt)
            txt = ""
        csv.close()

    def _get_columns(self):
        """
        Gibt die Spalten des Importes zurück
        :return: Spalten der Quelle
        :rtype: dict
        """
        columns = {}
        csv = open(self.__filename, "r")

        for c in csv.readline()[:-1].split(";"):
            if c[0] == "\"" and c[-1] == "\"":
                columns[c[1:-1]] = str
            else:
                columns[c] = str
        csv.close()
        return columns


class DbfData (DataSource, DataTarget):
    __zeile = 0

    def __init__(self, filename):
        """
        Dbf-Datenquelle
        :param filename: Dateiname
        :type filename: str
        """
        self.__filename = filename
        self.__table = None

    def __del__(self):
        if self.__table is not None:
            self.__table.close()

    def _read_line(self):
        """
        Liest eine Zeile des Importes
        :return: Datenzeile des Datensatzes
        :rtype: dict
        """
        if self.__table is None:
            self.__table = dbf.Table(self.__filename)
            # table = dbf.Table("sort.DBF")
            self.__table.open(mode=dbf.READ_ONLY)

        if self.__zeile >= len(self.__table):
            return None
        zeile = self.__table[self.__zeile]
        liste = {}
        for att in self.get_columns():
            f = zeile[att]
            if f is None or (type(zeile[att]) == str and zeile[att].strip() == ""):
                continue
            if type(f) == str:
                f = f.strip()
            if f == "":
                continue
            liste[att] = f
        self.__zeile += 1
        return liste

    def reset_line(self):
        """
        Setzt den Iterator auf den Startwert zurück
        """
        self.__zeile = 0

    __rename_col = {}

    def write(self, datasource):
        """
        Schreibt eine Zeile zum Importieren
        :param datasource: Importdaten
        :type datasource: DataSource
        :return: Erfolgreich importiert?
        :rtype: bool
        """
        spalten = datasource.get_columns()

        if self.__table is None:
            if os.path.isfile(self.__filename):
                self.__table = dbf.Table(self.__filename)
                # table = dbf.Table("sort.DBF")
                self.__table.open(mode=dbf.READ_WRITE)
                for f in self.__table.structure():
                    fk = f.split(" ")[0]
                    self.__rename_col[fk] = fk
            else:
                types = ""
                col = {}
                for spalte in spalten:
                    spaltenname = spalte.replace(".", "_").lower()
                    if len(spaltenname) > 10:
                        spaltenname = spaltenname[:10]

                    i = 0
                    while spaltenname in col:
                        spaltenname = spaltenname[:-len(str(i))] + str(i)
                        i += 1

                    self.__rename_col[spalte] = spaltenname
                    col[spaltenname] = True

                    if spalten[spalte] == int:
                        types += spaltenname + " N(19,0);"
                    elif spalten[spalte] == float:
                        types += spaltenname + " N(19,5);"
                    elif spalten[spalte] == datetime:
                        types += spaltenname + " D;"
                    else:
                        types += spaltenname + " C(255);"
                print(self.__rename_col)
                self.__table = dbf.Table(self.__filename, types[:-1])
                self.__table.open(mode=dbf.READ_WRITE)

        while True:
            zeile = datasource.read_line()
            if zeile is None:
                break
            liste = {}
            for z in zeile:
                if z in self.__rename_col:
                    print(zeile[z])
                    if type(zeile[z]) == str:
                        liste[self.__rename_col[z]] = str(zeile[z])\
                            .replace("ä", "ae")\
                            .replace("Ä", "Ae")\
                            .replace("ö", "oe")\
                            .replace("Ö", "oe")\
                            .replace("ü", "ue")\
                            .replace("Ü", "Ue")\
                            .replace("ß", "ss")
                    else:
                        liste[self.__rename_col[z]] = zeile[z]
            # print(liste)
            for l in liste:
                print(l + " (" + str(type(liste[l])) + "): " + str(liste[l]))
            self.__table.append(liste)

    def _get_columns(self):
        """
        Gibt die Spalten des Importes zurück
        :return: Spalten der Quelle
        :rtype: dict
        """
        liste = {}
        for field in self.__table.structure():
            arr = field.split(" ")
            # print(arr[1])
            if arr[1][0] == "N" and arr[1][-3:] == ",0)":
                typ = int
            elif arr[1][0] == "N":
                typ = float
            elif arr[1][0] == "D":
                typ = datetime
            else:
                typ = str
            liste[arr[0]] = typ
        return liste


class WfsData(DataSource):
    _wfs_filter = ""

    def __init__(self, url, feature_type=None, username=None, password=None):
        """
        Erzeugt eine neue WFS-Datenquelle
        :param url: WFS-URL
        :type url: str
        :param feature_type: Name des FeatureTypes
        :type feature_type: str
        :param username: Falls notwendig: Benutzername
        :type username: str
        :param password: Falls notwendig: Passwort
        :type password: str
        """
        self._url = url
        self._feature_type = feature_type
        self._username = username
        self._password = password

    def set_filter(self, wfs_filter):
        """
        Setzt einen Filter
        :param wfs_filter: Filter im Excelstyle UND(GLEICH(vnk, "123456789"),GROESSERGLEICH(BST, "5")
                        Werte sind immer anzugeben in "" (auch Zahlen)
                        Felder ohne
                        Funktionen:
                        UND, ODER, KLEINER, KLEINERGLEICH, GROESSER, GROESSERGLEICH, GLEICH, NICHT
        :type wfs_filter: str
        """
        # prüfen
        if wfs_filter.count("(") != wfs_filter.count(")"):
            raise Exception("Anzahl der öffnenden und schließenden Klammern unterscheidet sich")
        self._wfs_filter = "<ogc:Filter>" + self.__to_filter(wfs_filter) + "</ogc:Filter>"

    def __to_filter(self, text):
        text = text.strip()
        # print(text)

        if (text[0] == "\"" and text[-1] == "\"") or (text[0] == "'" and text[-1] == "'"):
            return "<ogc:Literal>" + text[1:-1] + "</ogc:Literal>"
        anfang = text.find("(")
        ende = text.rfind(")")

        if anfang == -1 or ende == -1:
            r = "<ogc:PropertyName>"
            if type(self) == PublicWfsData:
                dft = self.describe_feature_type()
                if text not in dft:
                    raise Exception("Filter-Feld nicht vorhanden")
                if 'klartext' in dft[text]:
                    r += text + "/@luk"
                else:
                    r += text
            else:
                r += text
            r += "</ogc:PropertyName>"
            return r

        befehl = text[0:anfang]
        mitte = text[anfang+1:ende]
        # print(mitte)

        befehl_wfs = ""
        if befehl == "UND":
            befehl_wfs = "ogc:And"
        elif befehl == "ODER":
            befehl_wfs = "ogc:Or"
        elif befehl == "NICHT":
            befehl_wfs = "ogc:Not"
        elif befehl == "GLEICH" or befehl == "IDENTISCH":
            befehl_wfs = "ogc:PropertyIsEqualTo"
        elif befehl == "KLEINER":
            befehl_wfs = "ogc:PropertyIsLessThan"
        elif befehl == "KLEINERGLEICH":
            befehl_wfs = "ogc:PropertyIsLessThanOrEqualTo"
        elif befehl == "GROESSER":
            befehl_wfs = "ogc:PropertyIsGreaterThan"
        elif befehl == "GROESSERGLEICH":
            befehl_wfs = "ogc:PropertyIsGreaterThanOrEqualTo"
        elif befehl == "ZWISCHEN":
            befehl_wfs = "ogc:PropertyIsBetween"

        r = "<" + befehl_wfs + ">"
        for einzeln in WfsData.__split_ok(mitte):
            # print(einzeln)
            r += self.__to_filter(einzeln)
        r += "</" + befehl_wfs + ">"
        return r

    @staticmethod
    def __split_ok(text):
        # print(text)
        liste = []
        k = 0
        te = False
        td = False
        letzter = 0
        for i in range(len(text)):
            # print(text[i])
            if te and text[i] == "\'" and not (i > 0 and text[i-1] == "\\"):
                te = False
            elif td and text[i] == "\"" and not (i > 0 and text[i-1] == "\\"):
                td = False
            elif te or td:
                pass
            elif text[i] == "\'" and not (i > 0 and text[i-1] == "\\"):
                te = True
            elif text[i] == "\"" and not (i > 0 and text[i-1] == "\\"):
                td = True
            elif text[i] == "(":
                k += 1
            elif k == 0 and (text[i] == "," or text[i] == ";"):
                liste.append(text[letzter:i].strip())
                letzter = i+1
            elif text[i] == ")":
                k -= 1
        if letzter != len(text) - 1:
            liste.append(text[letzter:].strip())
        return liste

    def set_feature_type(self, feature_type):
        """
        Erlaubt es, den FeatureType einmalig zusetzen, falls nicht bereits im Konstruktur geschehen
        :param feature_type: Name des FeatureTypes
        :type feature_type: str
        """
        if self._feature_type is not None:
            raise Exception("FeatureType bereits gesetzt")
        self._feature_type = feature_type

    def _read_line(self):
        """
        Liest eine Zeile des Importes
        :return: Datenzeile des Datensatzes
        :rtype: list
        """
        if self._feature_type is None:
            raise Exception("Kein FeatureType ausgewählt")

        # TODO Funktion erstellen
        pass

    def _get_columns(self):
        """
        Gibt die Spalten des Importes zurück
        :return: Spalten der Quelle
        :rtype: dict
        """
        columns = {}
        dft = self.describe_feature_type()

        for att in dft:
            columns[att] = str
        # TODO genauen Datentypen festlegen und ggf. später auch als diesen parsen
        # print(columns)
        return columns

    def list_feature_types(self):
        """
        Lädt eine Liste aller Feature Types des WFS
        :return: Liste aller Feature Types des WFS
        :rtype: list
        """
        # http://lverkpa001.fhhnet.stadt.hamburg.de:8380/publicWFS/WFS?Request=DescribeFeatureType&typeName=Oteinzelbaum
        soap = """<?xml version="1.0" encoding="ISO-8859-1"?>
            <wfs:GetCapabilities service="WFS" version="1.0.0" xmlns="http://www.opengis.net/wfs" 
            xmlns:wfs="http://www.opengis.net/wfs" xmlns:gml="http://www.opengis.net/gml" 
            xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" 
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
            xsi:schemaLocation="http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.0.0/WFS-basic.xsd">
            </wfs:GetCapabilities>"""
        response = self._soap_request(soap)
        tree = ElementTree.fromstring(response)
        types = []
        # print(self.pretty_xml(self._soap_request(soap)))

        for obj in tree.findall('{http://www.opengis.net/wfs}FeatureTypeList/'
                                '{http://www.opengis.net/wfs}FeatureType'):
            itx = obj.find('{http://www.opengis.net/wfs}Name')
            bez = obj.find('{http://www.opengis.net/wfs}Title')
            if itx is not None and bez is not None:
                types.append([itx.text, bez.text])
        return types

    __featureDescr = {}

    def describe_feature_type(self, feature_type=None):
        """
        Lädt die Beschreibung des Feature Types vom WFS
        :param feature_type: FeatureType (default: FeatureType der Klasse)
        :return: Dictionary mit den Metadaten
        :rtype: dict
        """
        if feature_type is None:
            feature_type = self._feature_type

        if feature_type in self.__featureDescr:
            return self.__featureDescr[feature_type]

        soap = """<?xml version="1.0" encoding="ISO-8859-1"?>
            <wfs:DescribeFeatureType service="WFS" version="1.1.0" xmlns="http://www.opengis.net/wfs" 
            xmlns:wfs="http://www.opengis.net/wfs" xmlns:gml="http://www.opengis.net/gml" 
            xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" 
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
            xsi:schemaLocation="http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.0.0/WFS-basic.xsd">
                <wfs:TypeName>""" + feature_type + """</wfs:TypeName>
            </wfs:DescribeFeatureType>"""
        response = self._soap_request(soap)
        tree = ElementTree.fromstring(response)
        attributes = {}
        # print(self.pretty_xml(self._soap_request(soap)))

        for obj in tree.findall('{http://www.w3.org/2001/XMLSchema}complexType/'
                                '{http://www.w3.org/2001/XMLSchema}complexContent/'
                                '{http://www.w3.org/2001/XMLSchema}extension/'
                                '{http://www.w3.org/2001/XMLSchema}sequence/'
                                '{http://www.w3.org/2001/XMLSchema}element'):

            zeile = {}
            attributes[obj.attrib['name']] = zeile
            ann = obj.find('{http://www.w3.org/2001/XMLSchema}annotation')
            if ann is None:
                continue
            zeile['bezeichnung'] = ann.find('{http://www.w3.org/2001/XMLSchema}documentation').text

            app = ann.find('{http://www.w3.org/2001/XMLSchema}appinfo')
            if app is None:
                continue

            read_only = app.find('{http://xml.novasib.de}readOnly')
            if read_only is not None and read_only.text == 'true':
                zeile['read_only'] = True
            klartext = app.find('{http://xml.novasib.de}typeName')
            if klartext is not None:
                zeile['klartext'] = klartext.text

            typ = obj.find('{http://www.w3.org/2001/XMLSchema}simpleType/'
                           '{http://www.w3.org/2001/XMLSchema}restriction')
            if typ is None:
                continue

            zeile['type'] = typ.attrib['base']
            length = typ.find('{http://www.w3.org/2001/XMLSchema}maxLength')
            if length is not None:

                zeile['type'] += "(" + length.attrib['value'] + ")"
            digits = typ.find('{http://www.w3.org/2001/XMLSchema}totalDigits')
            if digits is not None:
                zeile['type'] += "(" + digits.attrib['value'] + ")"

        self.__featureDescr[feature_type] = attributes
        return self.__featureDescr[feature_type]

    def _soap_request(self, soap):
        """
        Führt einen SOAP-Request an dem WFS durch
        :param soap: SOAP-XML-Anfrage
        :return: Antwort des WFS (XML)
        :rtype: str
        """
        # headers = {'content-type': 'application/soap+xml'}
        headers = {'content-type': 'text/xml'}
        login = HTTPBasicAuth(self._username, self._password)
        response = requests.post(self._url, data=soap, headers=headers, auth=login)
        # print(soap)
        return response.content

    def do_soap_request(self, soap):
        """
        Führt einen SOAP-Request an dem WFS durch
        :param soap: SOAP-XML-Anfrage
        :return: Antwort des WFS (XML)
        :rtype: str
        """
        test = input("Achtung: Mit diesem Befehl werden ungeprüft Befehle an den WFS übermittelt."
                     "Hierdurch können Daten ungewollt verändert werden - Auch ein Totalverlust von Daten ist möglich! "
                     "Wollen Sie dieses wirklich? (JA): ")
        if test == "JA":
            print("Befehl wird ausgeführt...")
            return self.__pretty_xml(self._soap_request(soap))
        else:
            print("Befehl wurde abgebrochen!")
            return None

    @staticmethod
    def __pretty_xml(xml_data):
        """
        Verbessert die Lesbarkeit von XML-Daten (Zeilenumbrüche, Einrückungen)
        :param xml_data: XML-Daten ohne Zeilenumbrüche
        :return: XML mit Zeilenumbrüchen und Einrückungen
        :rtype: str
        """
        x = xml.dom.minidom.parseString(xml_data)
        return x.toprettyxml()


class PublicWfsData (WfsData, DataTarget):

    def __init__(self, url, username, password, feature_type=None, kurzfassen=True, klartexte_anhaengen=False):
        """
        Daten über den PublicWFS der SIB exportieren oder importieren
        :param url: URL zum publicWFS
        :type url: str
        :param username: Benutzername des WFS
        :type username: str
        :param password: Passwort des WFS
        :type password: str
        :param feature_type: Name des FeatureTypes
        :type feature_type: str
        :param kurzfassen: Legt fest, ob bei Klartextfeldern href, luk und typeName ausgegeben werden oder nur luk
        :type kurzfassen: bool
        :param klartexte_anhaengen: Legt fest, ob Klartextfeldern jeweils der
                komplette Klartextdatensatz angehängt werden soll
        :type klartexte_anhaengen: bool
        """
        super(PublicWfsData, self).__init__(url, feature_type, username, password)
        self.__klartexte = {}
        self.daten = []
        self.row_number = -1
        self.__kurzfassen = kurzfassen
        self.__klartexte_anhaengen = klartexte_anhaengen

    __columns = {}

    def _get_columns(self):
        """
        Gibt die Spalten des Importes zurück
        :return: Spalten der Quelle
        :rtype: dict
        """
        if len(self.__columns) > 0:
            return self.__columns

        dft = self.describe_feature_type()

        for att in dft:
            typ = str
            if 'type' in dft[att]:
                typ = self.__describe_ft2type(dft[att]['type'])
            self.__columns[att] = typ
            if 'klartext' in dft[att]:
                if not self.__kurzfassen:
                    self.__columns[att + ".href"] = str
                    self.__columns[att + ".typeName"] = str
                    self.__columns[att + ".luk"] = typ
                if self.__klartexte_anhaengen:
                    dft2 = self.describe_feature_type(dft[att]['klartext'])
                    for att2 in dft2:
                        typ2 = str
                        if 'type' in dft2[att2]:
                            typ2 = self.__describe_ft2type(dft2[att2]['type'])
                        self.__columns[att + "." + att2] = typ2
        return self.__columns

    @staticmethod
    def __describe_ft2type(dft_type):
        """
        Formt den Typ aus der DescribeFeatureType-Funktion um in Python
        :param dft_type: Typ aus DFT
        :type dft_type: str
        :return: Python-Typ
        :rtype: type
        """
        if dft_type.find('string') > 0:
            return str
        elif dft_type.find('integer') > 0:
            return int
        elif dft_type.find('float') > 0:
            return float
        elif dft_type.find('datetime') > 0 or dft_type.find('date') > 0:
            return datetime
        return str

    def _read_line(self):
        """
        Liest eine Zeile des Importes
        :return: Datenzeile des Datensatzes als Dictionary
        :rtype: dict
        """
        if self.row_number < 0:
            self.__parse_features()

        self.row_number += 1

        if len(self.daten) > self.row_number:
            return self.daten[self.row_number]
        return None

    def reset_line(self):
        """
        Setzt den Iterator zurück
        """
        self.row_number = -1

    def write(self, datasource):
        """
        Schreibt eine Zeile zum Importieren
        :param datasource: Datenquelle des Importes
        :type datasource: DataSource
        :return: Erfolgreich importiert?
        :rtype: bool
        """
        req = """<?xml version="1.0" encoding="ISO-8859-1"?>
                <wfs:Transaction service="WFS" version="1.0.0"
                xmlns="http://xml.novasib.de"
                xmlns:wfs="http://www.opengis.net/wfs"
                xmlns:gml="http://www.opengis.net/gml"
                xmlns:ogc="http://www.opengis.net/ogc"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                xsi:schemaLocation="http://xml.novasib.de 
                http://localhost:20031/public_wfs/WFS?Request&#61;DescribeFeatureType&#38;TYPENAME&#61;Otrastanlage 
                http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.0.0/WFS-transaction.xsd">
                <wfs:Insert>"""

        while True:
            d = datasource.read_line()
            if d is None:
                break
            req += self.__make_xml(d)

        req += "</wfs:Insert></wfs:Transaction>"

        print(self.__pretty_xml(req))
        antwort = self._soap_request(req)
        print(antwort)
        return str(antwort).find("SUCCESS") > 0

    def __make_xml(self, zeile):
        """
        Erzeugt XML aus einer Datenzeile
        :param zeile: Datenzeile
        :type zeile: dict
        :return: XML-String
        :rtype: str
        """
        felder = self.describe_feature_type()

        xmlt = "<" + self._feature_type + ">"
        for att in zeile:
            if att not in felder:
                print("Feld nicht vorhanden: " + att)
            elif 'read_only' in felder[att]:
                print("nur lesbar: " + att)
            elif 'klartext' in felder[att]:
                kt = self.__get_klartext_href(felder[att]['klartext'], zeile[att])
                xmlt += "<" + att + " xlink:href=\"" + kt + "\" typeName=\"" + felder[att]['klartext'] +\
                        "\" luk=\"" + str(zeile[att]) + "\" />"
            else:
                xmlt += "<" + att + ">" + str(zeile[att]) + "</" + att + ">"

        xmlt += "</" + self._feature_type + ">"
        return xmlt

    def __parse_features(self):
        """
        Formt WFS-Antworten in eine Liste von Dictionarys um
        :return: Liste von Dictionarys mit den Attributen
        :rtype: list
        """
        # col = self._get_columns()
        for obj in self.__load_features(wfs_filter=self._wfs_filter):
            d = {}
            for i in obj.getchildren():
                att = i.tag.split('}', 1)[1]
                if i.text is not None:
                    d[att] = self.__transform_type(att, i.text)
                    continue
                if 'luk' in i.attrib:
                    d[att] = self.__transform_type(att, i.attrib['luk'])
                if self.__kurzfassen:
                    continue
                for a in i.attrib:
                    att2 = att + "." + a.split('}', 1).pop()
                    d[att2] = self.__transform_type(att2, i.attrib[a])
                if not self.__klartexte_anhaengen:
                    continue
                if 'typeName' in i.attrib and \
                        '{http://www.w3.org/1999/xlink}href' in i.attrib and \
                        i.attrib['typeName'] not in ['AsbAbschn', 'Projekt']:
                    kt = self.__get_klartext(i.attrib['typeName'], i.attrib['{http://www.w3.org/1999/xlink}href'])
                    for kt_att in kt:
                        d[att + "." + kt_att] = self.__transform_type(att + "." + kt_att, kt[kt_att])
            # print(d)
            self.daten.append(d)

    def __transform_type(self, attribut, wert):
        col = self._get_columns()

        if attribut in col:
            if col[attribut] == datetime and len(wert) > 10:
                return datetime.strptime(wert, "%Y-%m-%dT%H:%M:%S")
            elif col[attribut] == datetime:
                return datetime.strptime(wert, "%Y-%m-%d")
            try:
                return col[attribut](wert)
            except TypeError:
                pass
        return wert

    def __get_klartext(self, feature_type, href):
        """
        Liefert zu einem Klartextfeld den entsprechenden Klartext-Datensatz
        :param feature_type: typeName des Klartextes
        :type feature_type: str
        :param href: href der Klartextes
        :type href: str
        :return: Dictionary mit dem kompletten Klartext-Datensatz
        :rtype: dict
        """
        kt = self.__load_klartext(feature_type)
        if href in kt['nach_href']:
            return kt['nach_href'][href]
        else:
            raise Exception("Klartext zu " + href + " nicht in " + feature_type + " gefunden!")

    def __get_klartext_href(self, feature_type, klartext):
        """
        Liefert zu einem Klartextfeld den entsprechenden href
        :param feature_type: typeName des Klartextes
        :type feature_type: str
        :param klartext: Klartext (XYZ)
        :type klartext: str
        :return: href der Klartextes
        :rtype: str
        """
        # print(klartext)
        kt = self.__load_klartext(feature_type)
        if klartext in kt['nach_abk']:
            return kt['nach_abk'][klartext]['href']
        else:
            raise Exception("Klartext " + klartext + " nicht in " + feature_type + " gefunden!")

    def __load_features(self, feature_type=None, wfs_filter=""):
        """
        Stellt eine GetFeature-Anfrage an den publicWFS und fibt alle XML-Einträge zurück
        :param feature_type: FeatureType, das geladen werden soll (default: FeatureType der Klasse)
        :type feature_type: str
        :return: Liste der XML-Einträge
        :rtype: list
        """
        if feature_type is None:
            feature_type = self._feature_type
        body = """<?xml version="1.0" encoding="ISO-8859-1"?>
            <wfs:GetFeature service="WFS" version="1.0.0" 
                xmlns="http://www.opengis.net/wfs" xmlns:wfs="http://www.opengis.net/wfs" 
                xmlns:gml="http://www.opengis.net/gml" 
                xmlns:ogc="http://www.opengis.net/ogc" 
                xmlns:xlink="http://www.w3.org/1999/xlink" 
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                xsi:schemaLocation="http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.0.0/WFS-basic.xsd">
                <wfs:Query typeName='""" + feature_type + """'>""" + wfs_filter + """</wfs:Query>
            </wfs:GetFeature>"""
        # print(body)
        response = self._soap_request(body)
        tree = ElementTree.fromstring(response)
        return tree.findall('{http://xml.novasib.de}Objekt/{http://xml.novasib.de}' + feature_type)

    def __load_klartext(self, klartext):
        """
        Lädt einen Klartext als Dictionary
        :param klartext: Klartext der SIB (It...)
        :type klartext: str
        :return: dict
        """
        if klartext in self.__klartexte:
            return self.__klartexte[klartext]

        kt = {'nach_abk': {},
              'nach_href': {}}
        for obj in self.__load_features(klartext):
            k = {'luk': str(obj.attrib['luk']),
                 'href': '#' + obj.attrib['fid']}
            for a in obj.getchildren():
                att = a.tag.split('}', 1).pop()
                k[att] = a.text
            kt['nach_abk'][k['luk']] = k
            kt['nach_href'][k['href']] = k

        if len(kt['nach_abk']) == 0:
            print("Keine Klartexte zu " + klartext + " geladen")
        else:
            print(str(len(kt['nach_abk'])) + " Klartexte zu " + klartext + " geladen")

        self.__klartexte[klartext] = kt
        return kt
