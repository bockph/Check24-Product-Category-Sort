
import BertEncoder
import torch

def compare_web_app(search,category_list_eng,category_list_ger,encoder):
    query_vector=encoder([search],[12])

    list_vector=encoder(category_list_eng,[12])
    dist = torch.cdist(query_vector,list_vector)
    new_list =[]
    for idx,category in enumerate(category_list_ger):
        new_list.append((category,round(dist[idx].item(),2)))
    sorted_list =sorted(new_list, key=lambda d: d[1])
    s=""
    string_sorted =""
    string_unsorted=""
    for idx,(name,similiarity) in enumerate(sorted_list):
        string_sorted+=str(idx)+" "+name+" "+str(similiarity)+"\n"
    for idx,name in enumerate(category_list_ger):
        string_unsorted+=str(idx)+" "+name+"\n"
    return sorted_list,string_sorted,string_unsorted



def compare(search_query_encoded,category_list_encoded,category_list,encoder):

    dist = torch.cdist(search_query_encoded,category_list_encoded)
    new_list =[]
    for idx,category in enumerate(category_list):
        new_list.append((category,round(dist[idx].item(),2)))
    sorted_list =sorted(new_list, key=lambda d: d[1])
    s=""
    string_sorted =""
    string_unsorted=""
    for idx,(name,similiarity) in enumerate(sorted_list):
        string_sorted+=str(idx)+" "+name+" "+str(similiarity)+"\n"
    for idx,name in enumerate(category_list):
        string_unsorted+=str(idx)+" "+name+"\n"
    return sorted_list,string_sorted,string_unsorted