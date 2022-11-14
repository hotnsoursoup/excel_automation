
import pandas as pd
import xml.etree.ElementTree as et
import time
import re
from datetime import datetime
from tqdm.auto import tqdm
import os
import pyodbc as pydo
import xlsxwriter
from openpyxl import workbook
import logging
import numpy as np


def reshape_xml(data):
    xtree = et.ElementTree(et.fromstring(data))
    root = xtree.getroot()
    list = []
    for node in root:
    # Only pulls out nodes that exist in the XMLFIELDS column of 
    # the config file/configsheet/xmlfields. 
       if node.tag in xmlcols.values: 
            if bool(re.search(reg, str(node.text))):
                text = node.text.strip()
                text.replace(',', '###')
                list.append(node.tag + "=" + text)
    list.sort()
    return list

def get_excel_col(column):
    col_num = df.columns.get_loc(column)
    col_letter = chr(ord('@')+col_num+1)
    col_ref = col_letter + ':' + col_letter
    return col_ref, col_letter, col_num

def build_col_dict(data):
    col_dict = {}
    for col in data.columns:
        col_ref, col_letter, col_num = get_excel_col(col)
        col_dict[col] = {'col_ref': col_ref, 
        'col_letter': col_letter, 'col_num': col_num}
    return col_dict

def group_rows_by_count(string, delimiter, count):
    items = string.split(',')
    n = count
    group_string = ' ,'.join([
        delimiter.join(items[i:i+n]) for i in range(0, len(items), n)])
    return group_string

    #Checking directory information based on execution env
try:
    __file__
except NameError:
    wrkdir = os.getcwd()
    print('NameError, wrkdir = ', wrkdir)
    pass
else:
    wrkdir = os.path.dirname(__file__)
    print('__file__ present, ', __file__)
    pass

#### SCRIPT CONFIG ITEMS ####
verbose = True
dsn = 'DSN=DS_DATAMART'
capture_data = True # Unused atm...
reg = re.compile('[AZaz0-9]')
now = datetime.now()
time = now.strftime("%m-%d-%Y")
logfile = wrkdir + "\logs\w69_logfile_" + time + ".log"
pd.io.formats.excel.ExcelFormatter.header_style = None

#############################

logflags = ['e','d']
#logging.FileHandler(logfile, mode='a', encoding=None, delay=False)
logging.basicConfig(filename=logfile,
    format='%asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%d-%m%Y %H:$M:%S')
logger = logging.getLogger(__name__)

#############################
workbook = None
conn = None

try:
    print('Starting Script... Importing Data')
    filename = wrkdir + '\w69_config.xlsx'
    workbook = pd.ExcelFile(filename)
    index_sheet = pd.read_excel(workbook, 'Config')
    xmlcols = index_sheet["XMLFIELDS"].dropna()
    pf = index_sheet["PRIMARYFIELDS"].dropna().str.upper()
    query = index_sheet["QUERY"][0]
    report_name = index_sheet["BASE_REPORT_NAME"][0] + "_"
    workbook.close()
    logger.info('Workbook Import Completed')
############# DB Related ##############
    conn = pydo.connect(dsn)
    data = pd.read_sql(query, conn)
    conn.close()
    logger.info('Data successfully queried from %s', dsn)

except PermissionError as e:
    if verbose:
        print('Close the config file and try again')
    logger.exception(e)
except Exception as e:
    if workbook:
        workbook.close()
    if conn:
        conn.close()
   # logging.info('Unknown reviewed Exception', exc_info=True)
    logger.exception(e)
if verbose:
    print('Completed item import')
    xml = data["FILEDATA"]
df = pd.DataFrame()
# select primary fields(pf), can use columns.intersection
df2 = data[data.columns[data.columns.isin(pf)]] 

if verbose:
    print('Starting Data Transform')
for row in tqdm(xml, total=(len(xml)), desc="Transforming Data"):
    string = row.split('&&')
    #Checks if there are two XML packages in the string as set 
    # by the SQL query for filedata
    if len(string) > 1:
        l1 = reshape_xml(string[0])
        l2 = reshape_xml(string[1])
        if len(l1) == 0:
            newstring = ','.join(l2)
        elif len(l2) == 0:
            newstring = ','.join(l1)
        else:
            #Don't need to compare lists if one list only has 
            # 1 item or completely the same
            if (len(l1) < 2) or (len(l2) < 2) or (l1 == l2):
                    newstring = ', '.join(l1) + '@@@' + ', '.join(l2)
            else:
                #extracting what's different about each
                l1 = set(l1) - set(l2) 
                l2 = set(l2) - set(l1)
                if len(l1) == 0:
                    newstring = ','.join(l2)
                    newstring = group_rows_by_count(newstring, ',', 5)#
                elif len(l2) == 0:
                    newstring = ','.join(l1)
                    newstring = group_rows_by_count(newstring, ',', 5)#
                else:
                    l1 = ', '.join(l1)
                    l2 = ', '.join(l2)
                    newstring = group_rows_by_count(l1, ',', 5) + '@@@' 
                    + group_rows_by_count(l2, ',', 5)# 

    else: #Only one XML entry found
        newstring = ', '.join(reshape_xml(row))
    ps = pd.Series(newstring, name='DATA')
    df = df.append(ps, ignore_index=True)
    
df = pd.concat([df2, df], axis=1) # Adding back primary columns to DF.
df["FIRSTNAME"] = df["FIRSTNAME"] + " " + df["LASTNAME"]
df.rename(columns={
    0:"DATA", 'MNTDATTIM':'DATE', 'FIRSTNAME':'NAME'}, inplace=True)
df = df.sort_values(by='DATE', ascending=False)
df = df.drop(columns=['INDEX','LASTNAME'], axis=1)

if verbose:
    print('Completed Data Transform')

now = datetime.now()
#time = now.strftime("%m-%d-%Y-%H%M%S")
filename = wrkdir + "\Reports\\" + report_name 
+ now.strftime("%m-%d-%Y_%H%M%S") + ".xlsx"
import time
dict = build_col_dict(df)

if verbose:
    print('Starting Excel Write')
############# Workbook Initial Creation and Format Config ############## 
workbook = xlsxwriter.Workbook(filename)
worksheet = workbook.add_worksheet()
format_headerrow = workbook.add_format({
    'font_color':'white', 'bold': True, 'bg_color':'#4F81BD'})
format_default = workbook.add_format({'text_wrap': True})
format_altrow1 = workbook.add_format({'bg_color':'#8DB4E2'})
format_altrow2 = workbook.add_format({'bg_color':'#C5D9F1'})
format_old = workbook.add_format({'font_color':'Red'})
format_new = workbook.add_format({'font_color':'Green'})
########################################################################
for col in df.columns:
    col_num = dict[col]['col_num']
    col_len = len(col)+2
    worksheet.write(0, dict[col]['col_num'], col, format_headerrow)
    if col != "DATA":
        maxlen = df[col].astype(str).str.len().max()+2
        maxlen = max(maxlen, col_len)
        worksheet.set_column(col_num, col_num, maxlen)
    else:
        maxlen = 150
        worksheet.set_column(col_num, col_num, maxlen)

for index, row in df.iterrows():
    index = index + 1
    if index % 2 == 0:

        format = format_altrow1
    else:
        format = format_altrow2
    for col in df.columns:
        if col == "DATA":
            format.set_text_wrap()
            datacol = row[col].replace('@@@', '\r\n\r\n').replace('###', ',')
            worksheet.write(index, dict[col]['col_num'], datacol, format)
        elif col == "DATE":
            format.set_num_format('mmm d yyyy hh:mm:ss')
            worksheet.write(index, dict[col]['col_num'], row[col], format)
        else:
            worksheet.write(index, dict[col]['col_num'], row[col], format)
workbook.close(); 