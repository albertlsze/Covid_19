import pandas as pd

def Open_Excel(filename):
    excel_dict = {}
    temp = pd.ExcelFile(filename)
    for i in temp.sheet_names:
        excel_dict[i] = temp.parse(i)
    return excel_dict

def to_excel(filename,data_dict):
    writer = pd.ExcelWriter(filename)
    for tab in data_dict:
        data_dict[tab].to_excel(writer,sheet_name=tab,index=False)
    writer.save()