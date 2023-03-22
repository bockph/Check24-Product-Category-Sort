import pandas as pd

if __name__ == "__main__":
    translation = pd.read_csv("tariff_names/check_verivox_translation.csv", delimiter=";")
    check_data = pd.read_csv("./comparisons/check_" + case_name + ".csv")
    verivox_data = pd.read_csv("./comparisons/verivox_" + case_name + ".csv")

    verivox_translated = pd.merge(verivox_data, translation, on=["Anbieter", "Tarif"], how="left").reset_index(
        drop=True)

    check_verivox = pd.merge(check_data, verivox_translated, suffixes=["_Check24", "_VERIVOX"],
                             left_on=["Anbieter", "Tarif"], right_on=["Anbieter Check", "Tarif Check"], how="outer")

    check_verivox['Diff Preis Monat'] = check_verivox['Preis Monat_VERIVOX'] - check_verivox['Preis Monat_Check24']
    check_verivox['Diff Preis Jahr'] = check_verivox['Preis Jahr_VERIVOX'] - check_verivox['Preis Jahr_Check24']
    check_verivox['Kautionsdarlehen_VERIVOX'] = check_verivox['Kautionsdarlehen']
    check_verivox = check_verivox[['Anbieter_Check24', 'Tarif_Check24', 'Tarif_VERIVOX',
                                   'Preis Monat_Check24', 'Preis Monat_VERIVOX', 'Diff Preis Monat',
                                   'Preis Jahr_Check24', 'Preis Jahr_VERIVOX', 'Diff Preis Jahr',

                                   'Selbstbeteiligung_Check24', 'Selbstbeteiligung_VERIVOX',
                                   'Deckungssumme_Check24', 'Deckungssumme_VERIVOX',
                                   'Kautionsdarlehen_VERIVOX',
                                   # 'Monatliche Zahlungsweise',
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
    check_verivox.drop_duplicates().to_csv("./comparisons/check_verivox_" + case_name + ".csv")
