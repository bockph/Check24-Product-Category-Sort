
import pandas as pd

if __name__=="__main__":

    translation = pd.read_csv("check_verivox_translation.csv",delimiter=";")
    check_data = pd.read_csv("check_case_one.csv")
    verivox_data = pd.read_csv("verivox_case_one.csv")

    verivox_translated = pd.merge(verivox_data,translation,on=["Anbieter","Tarif"],how="left").reset_index(drop=True)
    print(verivox_translated)
    print(verivox_translated.info())

    check_verivox = pd.merge(check_data,verivox_translated,suffixes=["_Check24","_VERIVOX"],left_on=["Anbieter","Tarif"],right_on=["Anbieter Check","Tarif Check"],how="outer")
    print(check_verivox.head())
    print(check_verivox.info())
    check_verivox['Preis Monat Check24']=check_verivox['Preis Monat']
    check_verivox['Preis Jahr Check24']=check_verivox['Preis Jahr']

    check_verivox['Preis Monat Verivox (1 year)']=check_verivox['Preis Monat(1 year)']
    check_verivox['Preis Monat Verivox (3 years)']=check_verivox['Preis Monat(3 years)']
    check_verivox['Preis Jahr Verivox (1 year)']=check_verivox['Preis Jahr(1 year)']
    check_verivox['Preis Jahr Verivox (3 years)']=check_verivox['Preis Jahr(3 years)']
    check_verivox['Kautionsdarlehen_VERIVOX']=check_verivox['Kautionsdarlehen']
    check_verivox['Diff Preis Monat'] = -check_verivox['Preis Monat Check24'] + check_verivox[
        'Preis Monat Verivox (1 year)']
    check_verivox['Diff Preis Jahr'] = -check_verivox['Preis Jahr Check24'] + check_verivox[
        'Preis Jahr Verivox (1 year)']
    check_verivox['Diff Preis Monat (3 years)'] = -check_verivox['Preis Monat Check24'] + check_verivox[
    'Preis Monat Verivox (3 years)']
    check_verivox['Diff Preis Jahr (3 years)'] = -check_verivox['Preis Jahr Check24'] + check_verivox['Preis Jahr Verivox (3 years)']

    check_verivox=check_verivox[['Anbieter_Check24', 'Tarif_Check24','Tarif_VERIVOX',
     'Preis Monat Check24', 'Preis Monat Verivox (1 year)','Diff Preis Monat',
     'Preis Monat Verivox (3 years)','Diff Preis Monat (3 years)',
     'Preis Jahr Check24','Preis Jahr Verivox (1 year)','Diff Preis Jahr',
    'Preis Jahr Verivox (3 years)','Diff Preis Jahr (3 years)',
     'Selbstbeteiligung_Check24','Selbstbeteiligung_VERIVOX',
     'Deckungssumme_Check24','Deckungssumme_VERIVOX',
     'Kautionsdarlehen_VERIVOX',
     'Monatliche Zahlungsweise',
     'Highlighted Feature']]
     #  'Kostenlose Erstberatung', 'Wartezeit',
     # 'Beratung zum Aufhebungsvertrag', 'Telefonische Schadensmeldung',
     # 'Risikoreiche Kapitalanlagen',
     # 'Kostenlose Erstberatung mit Einschränkungen',
     # 'Vertragslaufzeit: 3 Jahre', 'Digitaler Versicherer',
     # 'Keine kostenlose Erstberatung', 'Keine Deckung ausserhalb Europas',
     # 'Kein Verwaltungs-Rechtsschutz', 'Kapitalanlagen nicht versichert',
     # 'Beitragsgarantie für 3 Jahre', 'Vertragsrecht: ab 750 € Streitwert',
     # 'Discount Percentage', 'Mindest Laufzeit',
     #   'Folgeereignistheorie',]]
    check_verivox.to_csv("./check_verivox_case1.csv")
   # print( check_verivox.columns.to_list)