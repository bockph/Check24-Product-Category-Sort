import data_loader as dl
def compare_group_data():
    group_old, group_new = dl.import_comparison_group_data(from_db=True)
    diff = group_old.compare(group_new)

    print("stop")
if __name__ == '__main__':
    compare_group_data()
