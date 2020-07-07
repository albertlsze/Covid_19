import numpy as np

def read_file(filename):
    with open(filename) as f:
        file = f.readlines()
    return np.array(file)

def write_file(outfile,str_list):
    with open(outfile,'w') as f:
        for item in str_list:
            f.write("%s" % item)