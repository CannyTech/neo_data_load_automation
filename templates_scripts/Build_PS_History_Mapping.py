import csv
import shutil
import re

# Create Follwoing folder and change the Rootdir defintion

Rootdir = "D:\\Data\\InformaticaCloud\\Python"
output_path = Rootdir + "\\"
#print(output_path)
Template_path = Rootdir + "\\Template"

# Create Follwoing Sub folder
#   Template
#   Output_Scripts
# Place the Snowparms under Neo Folder
# Place the Template file under Template

snowparms     = Template_path + "\\Snowparms_PS.csv"
histvar       = Template_path + "\\Hist Variables.csv"
template_file = Template_path + "\\PS_Hist_Merge_Template.txt"
bat_template_file = Template_path + "\\bat_Template_calls_Python.txt"
output_temp   = Rootdir + '\\Temp\\'


csv_file_hist = open(histvar)
csv_reader_hist = csv.reader(csv_file_hist, delimiter=',')
next(csv_reader_hist)
for row in csv_reader_hist:
    HIST, STAGE, HIST_COL_NAME, STG_COL_NAME, DELETED_COL_NAME, PYTHON_SCRIPT_NAME,PROCESS_NAME = row
    string1 = HIST
    string2 = STAGE
    string3 = HIST_COL_NAME
    string4 = STG_COL_NAME
    string5 = PYTHON_SCRIPT_NAME
    string6 = DELETED_COL_NAME
    string7 = 'PROCESS_NAME_REPLACE'
    string8 = PROCESS_NAME

csv_file = open(snowparms)
csv_reader = csv.reader(csv_file, delimiter=',')
next(csv_reader)
readvarkey = 'PK1 PK2 PK3 PK4 PK5 PK6 PK7 PK8 PK9 PK10 PK11 PK12 PK13 PK14 PK15 PK16 PK17 PK18 PK19 PK20 PK21 PK22 PK23 PK24'
for row in csv_reader:
    TF_NAME, MAPPING, MCT_NAME, BATCH_SCRIPT_NAME, PYTHON_SCRIPT_NAME, HIST_TABLE, STG_TABLE, HIST_COL_NAME, STG_COL_NAME, DELETED_COL_NAME, PK1, PK2, PK3, PK4, PK5, PK6, PK7, PK8, PK9, PK10, PK11, PK12, PK13, PK14, PK15, PK16, PK17, PK18, PK19, PK20, PK21, PK22, PK23, PK24 = row
    output_file = output_temp + PYTHON_SCRIPT_NAME
    bat_file = output_path + BATCH_SCRIPT_NAME
    output_new = output_path + PYTHON_SCRIPT_NAME
    shutil.copy2(template_file, output_file)
    print(bat_template_file)
    print(bat_file)
    shutil.copy2(bat_template_file, bat_file)

    with open(bat_file) as f:
        s = f.read()

    with open(bat_file, 'w') as f:
        s = re.sub(string5, PYTHON_SCRIPT_NAME, s)
        f.write(s)

    with open(output_file) as f:
        s = f.read()

    with open(output_file, 'w') as f:
        s = re.sub(string5, PYTHON_SCRIPT_NAME, s)
        f.write(s)

    with open(output_file, 'w') as f:
        s = re.sub(string1, HIST_TABLE, s)
        f.write(s)

    with open(output_file, 'w') as f:
        s = re.sub(string2, STG_TABLE, s)
        f.write(s)

    with open(output_file, 'w') as f:
        s = re.sub(string3, HIST_COL_NAME, s)
        f.write(s)

    with open(output_file, 'w') as f:
        s = re.sub(string4, STG_COL_NAME, s)
        f.write(s)

    with open(output_file, 'w') as f:
        s = re.sub(string6, DELETED_COL_NAME, s)
        f.write(s)

    with open(output_file, 'w') as f:
        s = re.sub(string7, string8, s)
        f.write(s)

    from collections import namedtuple

    readvar = namedtuple('readvar', readvarkey)
    tempvar = readvar(PK1, PK2, PK3, PK4, PK5, PK6, PK7, PK8, PK9, PK10, PK11, PK12, PK13, PK14, PK15, PK16, PK17, PK18, PK19, PK20, PK21, PK22, PK23, PK24)


    stringcol = "PKKEYS01"
    pkeys = tempvar.PK1
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS02"
    pkeys = tempvar.PK2
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS03"
    pkeys = tempvar.PK3
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS04"
    pkeys = tempvar.PK4
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS05"
    pkeys = tempvar.PK5
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS06"
    pkeys = tempvar.PK6
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS07"
    pkeys = tempvar.PK7
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS08"
    pkeys = tempvar.PK8
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS09"
    pkeys = tempvar.PK9
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS10"
    pkeys = tempvar.PK10
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS11"
    pkeys = tempvar.PK11
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS12"
    pkeys = tempvar.PK12
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS13"
    pkeys = tempvar.PK13
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS14"
    pkeys = tempvar.PK14
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS15"
    pkeys = tempvar.PK15
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS16"
    pkeys = tempvar.PK16
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS17"
    pkeys = tempvar.PK17
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS18"
    pkeys = tempvar.PK18
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS19"
    pkeys = tempvar.PK19
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS20"
    pkeys = tempvar.PK20
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS21"
    pkeys = tempvar.PK21
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS22"
    pkeys = tempvar.PK22
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS23"
    pkeys = tempvar.PK23
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)

    stringcol = "PKKEYS24"
    pkeys = tempvar.PK24
    if not pkeys:
        pkeys = "PRIMARY_KEYS_TO_BE_DELETED"
    with open(output_file, 'w') as f:
        s = re.sub(stringcol, pkeys, s)
        f.write(s)


    delete_words = ['PRIMARY_KEYS_TO_BE_DELETED']

    with open(output_file) as oldfile, open(output_new, 'w') as newfile:
        for line in oldfile:
            if not any(delete_word in line for delete_word in delete_words):
                newfile.write(line)