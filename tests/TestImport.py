from sibtools import *
from sibtools.ora import *


def pa(liste):
    if type(liste) == list:
        for i in liste:
            print(i)
    elif type(liste) == dict:
        for i in liste:
            print(i + ": " + str(liste[i]))


# Mit OracleDB verbinden
o = OraData("10.62.42.25", 6543, "verklhhp", "SYSADM5", "sysadm5", "SELECT * FROM SYSADM5.Oteinzelbaum")


# Baum aus PostGIS Tabelle in Datenbank importieren
"""
p = PgData('gv-srv-w00118', 5433, 'geo', 'postgres', 'Hamburg01!',
           "SELECT *, CONCAT(art,'~',gattung) baumart FROM strassenbaum.import LIMIT 3")
#print(p.read_line())
p.nk2tklfdz('von_netzknoten', 'nach_netzknoten')
w = PublicWfsData("http://gv-srv-w00118:20031/publicWFS/WFS?", "WFS", "wfs", "Oteinzelbaum")
p.rename_attributes({'von_station': 'vst',
                     'bis_station': 'bst'})
p.add_columns({'bearbeiter': 'TIMM',
               'stand': '2018-12-20',
               'quelle': '20'})
print(p.read_line())
print(w.get_columns())
w.write(p)
"""

# # Sonstiges
# w.write(p)

# print(w.describe_feature_type('Itlaermart'))
# print(w.read_line())
# print(w.read_line())
# o = OraData("10.62.42.25", 6543, "verklhhp", "SYSADM5", "sysadm5", "SELECT * FROM SYSADM5.Oteinzelbaum")
# c = CsvData("hallo.csv")
# c.write(o)

# print(c.get_columns())


# print(w_pro.read_line())

# p = PgData('gv-srv-w00118', 5433, 'geo', 'postgres', 'Hamburg01!', 'SELECT * FROM strassenbaum.import LIMIT 5')
# print(p.get_columns())
# print(p.read_line())

#print(w.describe_feature_type())

# print(w.read_line())





# while True:
#     line = w_pro.read_line()
#     if line is None:
#         break
#     print(line)

#c3 = CsvData("test3.csv")
#print(w.write(c3))



# Klartexte von einer zur anderen DB kopieren
"""
w_pro = PublicWfsData("http://lverkpa001.fhhnet.stadt.hamburg.de:8380/publicWFS/WFS?", "WFS", "wfs", "Itebgattung")
w = PublicWfsData("http://gv-srv-w00118:20031/publicWFS/WFS?", "WFS", "wfs", "Itebgattung")
w_pro.remove_columns(['objektId'])
w.write(w_pro)

w_pro = PublicWfsData("http://lverkpa001.fhhnet.stadt.hamburg.de:8380/publicWFS/WFS?", "WFS", "wfs", "Itebart")
w = PublicWfsData("http://gv-srv-w00118:20031/publicWFS/WFS?", "WFS", "wfs", "Itebart")
w_pro.remove_columns(['objektId'])
w.write(w_pro)
"""


# Alle Kla
"""w_pro = PublicWfsData("http://lverkpa001.fhhnet.stadt.hamburg.de:8380/publicWFS/WFS?", "WFS", "wfs", "Otunfdat")
df = w_pro.describe_feature_type()

liste = []
for feld in df:
    if 'klartext' in df[feld] and not df[feld]['klartext'] in liste:
        liste.append(df[feld]['klartext'])

pa(liste[2:-4]) # Rausfiltern von Projekt, allgLage, Quelle, KoordHerkunft...

for klar in liste[2:-4]:
    w_p = PublicWfsData("http://lverkpa001.fhhnet.stadt.hamburg.de:8380/publicWFS/WFS?", "WFS", "wfs", klar)
    w_t = PublicWfsData("http://gv-srv-w00118:20031/publicWFS/WFS?", "WFS", "wfs", klar)
    w_p.remove_columns(['objektId'])
    # pa(w_p.describe_feature_type())
    print(w_t.write(w_p))"""

"""
w_pro = PublicWfsData("http://lverkpa001.fhhnet.stadt.hamburg.de:8380/publicWFS/WFS?", "WFS", "wfs", "Otzst")
df = w_pro.describe_feature_type()
# pa(df)

liste = []
for feld in df:
    if 'klartext' in df[feld] and not df[feld]['klartext'] in liste:
        liste.append(df[feld]['klartext'])

pa(liste[2:-4]) # Rausfiltern von Projekt, allgLage, Quelle, KoordHerkunft...

for klar in liste[2:-4]:
    w_p = PublicWfsData("http://lverkpa001.fhhnet.stadt.hamburg.de:8380/publicWFS/WFS?", "WFS", "wfs", klar)
    w_t = PublicWfsData("http://gv-srv-w00118:20031/publicWFS/WFS?", "WFS", "wfs", klar)
    w_p.remove_columns(['objektId'])
    # pa(w_p.describe_feature_type())
    print(w_t.write(w_p))"""
w_p = PublicWfsData("http://lverkpw001.fhhnet.stadt.hamburg.de/publicWFS/WFS?", "WFS", "wfs", "Oteinzelbaum",
                    kurzfassen=False, klartexte_anhaengen=True)
# pa(w_p.describe_feature_type())
# w_p.set_filter("ODER(GROESSER(krone, '21'),GROESSER(stammdurchm, '1.5'))")
w_p.set_filter("ODER(GLEICH(gattung, 'AR1'),GLEICH(gattung, 'AR2'))")
w_p.show()

# c = CsvData("grosseBaeume.csv")
# c.write(w_p)



