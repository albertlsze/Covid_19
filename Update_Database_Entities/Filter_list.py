import numpy as np

def filter_list(database_col,current_list):
    database_col = database_col['Parameter'].values
    database_col = np.array([x.lower() if isinstance(x, str) else x for x in database_col])
    database_col = set(database_col)

    filter_list = []
    for i in current_list:
        if i.lower() in database_col:
            filter_list.append(i)
    return filter_list