# Python SIB-Tools

*Pythons tools for a street managing software used in Germany, therefore only 
german description*

## Beschreibung
Diese Tools sollen den Datenaustausch aus der TTSIB vereinfachen. Zum Datenimport 
wird der PublicWFS genutzt. Die Software basiert auf öffentlichen Schnittstellen der 
Software und ist weder im Auftrag von NovaSIB noch mit dessen expliziter Zustimmung
beim Fachbereich Verkehrsdaten, Landesbetrieb Geoinformation und Vermessung, Hamburg 
für eigene Zwecke entstanden.

Es wird keinerlei Haftung übernommen! (siehe auch MIT-Lizenz)


## Installation
*zukünftig*

`pip install sibtools`

### Vorraussetzungen
* requests
* dbf
* certifi
* aenum

*Für DB-Zugriffe*
* psycopg2
* cx_Oracle

## Nutzung
### Beispiel
Alle Einzelbäume der Gattung Eiben (Klartext: EIB) mit einem Stammdurchmesser 
kleiner als einen halben Meter in eine CSV-Datei schreiben
```
wfs_source = PublicWfsData('localhost', 'nutzer', 'passwort', 'Oteinzelbaum')
wfs_source.set_filter('UND(GLEICH(gattung, "EIB"),KLEINER(stammdu, "0.5"))')
w_p.show()

csv_target = CsvData("grosseBaeume.csv") 
csv_target.write(wfs_source)
```


##Links

[Landesbetrieb Geoinformation und Vermessung](https://www.hamburg.de/bsw/landesbetrieb-geoinformation-und-vermessung/)

[Website des Hersteller der TTSIB](https://www.novasib.de/produkte/)