import csv
from math import ceil
months_dict = { 
               1:'JAN',
               2:'FEV',
               3:'MAR',
               4:'ABR',
               5:'MAI',
               6:'JUN',
               7:'JUL',
               8:'AGO',
               9:'SET',
               10:'OUT',
               11:'NOV',
               12:'DEZ',
}

hash = {}

def generate_year_month_map():
    global hash
    datapk = 1
    for year in range(2000,2023): 
        for month in range(1,13):
            hash[(year,months_dict[month])] = datapk
            datapk += 1

def map_year_month(tuple_year_month):
    global hash
    
    return hash[(tuple_year_month[0], tuple_year_month[1])]

def map_year_month_number(tuple_year_month):
    global hash
    
    return hash[(tuple_year_month[0], months_dict[tuple_year_month[1]])]

def write_data(arquivo):
    datapk = 1
    with open(arquivo,'w') as file:
        writer = csv.writer(file,delimiter=";")
        #writer.writerow(['datapk','mes','mes_nome','trimestre','semestre','ano'])
        for year in range(2000,2023): 
            for month in range(1,13):
                writer.writerow([datapk,month,months_dict[month],ceil(month/3),ceil(month/6),year])
                hash[(year,months_dict[month])] = datapk
                datapk += 1
