from search import get_categories
from BertEncoder import BertEncoder
# from sort import compare,encode_bert
if __name__=="__main__":
    search_query ="Gefrierschrank"
    encoder = BertEncoder(bert_url = 'bert-base-german-cased')
    category_list = get_categories(search_query)
    # print(category_list)
    # category_list_encoded = encoder(category_list)
    #
    # search_query_encoded = encoder([search_query])
    # sorted_list, string_sorted, string_unsorted = compare(search_query_encoded,category_list_encoded=category_list_encoded,category_list=category_list,encoder=encoder)
    # print(string_unsorted)
    # print(string_sorted)
    # get_categories("kuehlschrank")