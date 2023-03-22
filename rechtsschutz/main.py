import urllib3
import pandas as pd
from bs4 import BeautifulSoup
import json
from datetime import date, datetime


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


def retrieve_check_page(url):
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    # print(response.data)
    soup = BeautifulSoup(response.data)
    return soup


def process_check_result(div, monthly):
    try:
        provider = div['data-provider-name']
    except Exception as e:
        provider = "Not found"

    try:
        tariff_name = div['data-tariff-name']
    except Exception as e:
        tariff_name = "Not found"

    try:
        price = div['data-tariff-price']
    except Exception as e:
        price = "Not found"

    try:
        extra_text_highlited = div['data-label-texts']
    except Exception as e:
        extra_text_highlited = "Not found"

    print(provider)
    print(tariff_name)
    print(price)
    print(extra_text_highlited)
    # get top level shown information
    try:
        tariff_characteristics = []
        inner_content = div.find("div", {"class": "tariff_bullets"})
        bullet_elems = inner_content.findAll("li", {"class": "c24_bullet"})
        for bullet in bullet_elems:
            try:
                description = bullet.find("span", {"class": "bullet_tooltip"})
                description.div.decompose()
                description = description.get_text()
                value = bullet.find("span", {"class": "bullet_value"})
                if value:
                    value = value.get_text()
                else:
                    value = "True"
            except Exception as e:
                print(e)
                print(bullet.find("span", {"class": "bullet_tooltip"}))
                print(bullet.find("span", {"class": "bullet_value"}))
            tariff_characteristics.append((description, value))
        print(tariff_characteristics)
    except Exception as e:
        print(e)
        tariff_characteristics = []
    result_dict = dict()
    result_dict['Anbieter'] = provider
    result_dict['Tarif'] = tariff_name
    if monthly:
        result_dict['Preis Monat'] = float(price) / 12
    else:
        result_dict['Preis Jahr'] = float(price)
    # result_dict['Monatliche Zahlungsweise']=monthly

    result_dict['Highlighted Feature'] = extra_text_highlited

    result_dict = result_dict | dict(tariff_characteristics)

    return result_dict


#
def create_check_url(monthly, marital_status, employement_status, birthdate, zipcode, selfparticipation, module_priv,
                     module_job, module_traffic, module_living, module_rental):
    # marital_status = "single"  # single|family|couple|singlewithchild
    # employement_status = "employee"  # employee|selfemployed|publicservice|student|pensioner|unemployed
    # birthdate = "01.10.2001"
    # zipcode = "85356"
    # costsharing="150"
    # module_priv = "yes"
    # module_job = "yes"
    # module_traffic = "yes"
    # module_living = "no"
    # module_rental = "no"
    if monthly:
        paymentperiod = "month"  # month|year
    else:
        paymentperiod = "year"
    url = "https://rechtsschutz.check24.de/rsv/vergleichsergebnis/?maritalstatus=" + marital_status + "&" \
                                                                                                      "employmentstatus=" + employement_status + "&employmentstatus_partner=employee&birthdate=" + birthdate + "&" \
                                                                                                                                                                                                               "zipcode=" + zipcode + "&" \
                                                                                                                                                                                                                                      "module_priv=" + module_priv + "&module_job=" + module_job + "&module_traffic=" + module_traffic + \
          "&module_living=" + module_living + "&module_rental=" + module_rental + "&" \
                                                                                  "leasedresidentialunits=1&grossannualrentalincome=3000&costsharing=" + selfparticipation + "&" \
                                                                                                                                                                             "paymentperiod=" + paymentperiod + "&alternativeOfferOriginal=0&offerType=unknown&" \
                                                                                                                                                                                                                "sortfield=provider&sortorder=asc&tariff_attribute=%5B%5D&tariff_tag=%5B%5D&tariff_package=%5B%5D&" \
                                                                                                                                                                                                                "gradefilter=5&min_stars=0&performance_filter_selected=true&previous_damages_amount=0&utilized_lawyer_last_3_month=no&" \
                                                                                                                                                                                                                "provider_testing=0&showAlternativeOfferArag="

    return url


def create_verivox_url(contract_runtime, marital_status, employement_status, birthdate, zipcode, selfparticipation,
                       module_priv,
                       module_job, module_traffic, module_living):
    age = str(calculate_age(datetime.strptime(birthdate, "%d.%m.%Y")))
    assert marital_status == "single" or marital_status == "family"
    # zip="85356"
    # familyStatus="single"#single|family
    # age="21"
    #
    #
    # jobStatus = "employee"
    # selfParticipation="150"
    # minimumContractTerm =str(contract_runtime)
    tariff_sections = []  # ["private","working","trafficFamily"]#,"living"]
    if module_priv == "yes": tariff_sections.append('private')
    if module_job == "yes": tariff_sections.append('working')
    if module_traffic == "yes": tariff_sections.append('trafficFamily')
    if module_living == "yes": tariff_sections.append('living')

    sections = '&'.join(
        ["tariffSections[" + str(idx) + "]=" + tariff_type for idx, tariff_type in enumerate(tariff_sections)])
    if marital_status == "family":
        age = age + "&partnerAge=" + age
    url = "https://service.verivox.de/applications/insurance/service/search/lpi/search?eventHash=e975d19ed398df57f64618671cc3d0d0&isAdditionalQuestionsChange=false&" \
          "age=" + age + "&" \
                         "zip=" + zipcode + "&" \
                                            "city=&insuranceExchange=no&damages=no&additionalQuestions=false&" \
                                            "familyStatus=" + marital_status + "&" \
          + sections + "&" \
                       "jobStatus=" + employement_status + "&" \
                                                           "selfParticipation=" + selfparticipation + "&" \
                                                                                                      "wayOfPayment=1&minimumContractTerm=" + str(
        contract_runtime) + "&" \
                            "independentActivity=false&damagesBeforeInsurance=false&vxRating=all&cost[selectedMinValue]=0&" \
                            "cost[selectedMaxValue]=43&cost[min]=21&cost[max]=43&workGroupRecommendation=0&sumsOfCoverage=1&depositLoan=50000&actionCommittedBasis=false&" \
                            "aliment=no&specialCriminalLaw=false&sortAlgo=price&limit=20&offset=0"
    return url


def retrieve_verivox_page(url):
    http = urllib3.PoolManager()
    print(url)
    response = http.request('GET', url)
    return json.loads(response.data.decode("utf-8"))


def process_verivox_result(result, contract_runtime=1):
    result_dict = dict()
    result_dict['Anbieter'] = result['insurerId']
    result_dict['Tarif'] = result['name']
    result_dict['Preis Monat'] = float(result['prices']['1'])
    result_dict['Preis Jahr'] = float(result['prices']['12'])
    result_dict['Discount Percentage'] = float(result['discountPercentage'])
    # result_dict['Mindest Laufzeit']=contract_runtime

    for benefit in result['benefits']:
        name, value = benefit['name'].split(":")
        result_dict[name] = value

    return result_dict


if __name__ == "__main__":
    case_name = "case_one"
    # monthly=True
    marital_status = 'single'
    employement_status = 'employee'
    birthdate = '01.10.2001'
    zipcode = '85356'
    selfparticipation = '150'
    module_priv = 'yes'
    module_job = 'yes'
    module_traffic = 'yes'
    module_living = 'no'
    module_rental = 'no'
    contract_runtime = 1  # 1|3

    # create request urls
    verivox_url_oneyear = create_verivox_url(1, marital_status, employement_status, birthdate, zipcode,
                                             selfparticipation, module_priv,
                                             module_job, module_traffic, module_living)
    verivox_url_threeyear = create_verivox_url(3, marital_status, employement_status, birthdate, zipcode,
                                               selfparticipation, module_priv,
                                               module_job, module_traffic, module_living)
    check_url_month = create_check_url(True, marital_status, employement_status, birthdate, zipcode, selfparticipation,
                                       module_priv,
                                       module_job, module_traffic, module_living, module_rental)
    check_url_year = create_check_url(False, marital_status, employement_status, birthdate, zipcode, selfparticipation,
                                      module_priv,
                                      module_job, module_traffic, module_living, module_rental)

    # retrieve data from urls
    check_response_month = retrieve_check_page(check_url_month)
    verivox_response_oneyear = retrieve_verivox_page(verivox_url_oneyear)
    check_response_year = retrieve_check_page(check_url_year)
    verivox_response_threeyear = retrieve_verivox_page(verivox_url_threeyear)

    # process verivox results
    ##oneyear
    verivox_results = []
    for offer in verivox_response_oneyear['offers']:
        verivox_results.append(process_verivox_result(offer, contract_runtime=1))

    verivox_data_oneyear = pd.DataFrame(verivox_results)
    ##twoyear
    verivox_results = []
    for offer in verivox_response_threeyear['offers']:
        verivox_results.append(process_verivox_result(offer, contract_runtime=3))

    verivox_data_threeyear = pd.DataFrame(verivox_results)
    # get rows with different values if more contract time in verivox
    additional_row_with_threeyear = pd.concat([verivox_data_oneyear, verivox_data_threeyear]).drop_duplicates(
        keep=False)

    # add contracttime flag
    verivox_data_oneyear['contract_time'] = 1
    additional_row_with_threeyear['contract_time'] = 3
    print(additional_row_with_threeyear)
    pd_final_verivox_data = pd.concat([verivox_data_oneyear, additional_row_with_threeyear])
    # verivox_data_oneyear.to_csv("./test_verivox_one.csv")
    # verivox_data_threeyear.to_csv("./test_verivox_three.csv")
    # pd_final_verivox_data = pd.merge(verivox_data_oneyear,
    #                                  verivox_data_threeyear[['Anbieter', 'Tarif','Preis Monat', 'Preis Jahr']],
    #                                on=['Anbieter', 'Tarif'],suffixes=["(1 year)","(3 years)"],how="left")
    pd_final_verivox_data.to_csv("./comparisons/verivox_" + case_name + ".csv")

    # final_verivox_data[['Anbieter','Tarif']].to_csv("./verivox_names_1.csv")
    #

    # process check
    ##create monthly results
    results = check_response_month.findAll("div", {'class': "result_box"})
    dict_results = []
    for result in results:
        dict_results.append(process_check_result(result, monthly=True))

    pd_check_month_data = pd.DataFrame(dict_results)

    ##create yearly results
    results = check_response_year.findAll("div", {'class': "result_box"})
    dict_results = []
    for result in results:
        dict_results.append(process_check_result(result, monthly=False))

    pd_check_year_data = pd.DataFrame(dict_results)
    # #
    # # print(len(pd_check_year_data))
    # # print(len(pd_check_month_data))
    #
    pd_final_check_data = pd.merge(pd_check_month_data, pd_check_year_data[['Anbieter', 'Tarif', 'Preis Jahr']],
                                   on=['Anbieter', 'Tarif'])
    # pd_final_check_data = pd_check_month_data
    pd_final_check_data.to_csv("./comparisons/check_" + case_name + ".csv")

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
