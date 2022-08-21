"""
Amex Credit Card default Prediction Code.py  
The objective of this competition is to predict the probability that a customer does not pay back their credit card balance amount in the future based on their monthly customer profile. The target binary variable is calculated by observing 18 months performance window after the latest credit card statement, and if the customer does not pay due amount in 120 days after their latest statement date it is considered a default event.
The dataset contains aggregated profile features for each customer at each statement date. Features are anonymized and normalized, and fall into the following general categories:

D_* = Delinquency variables
S_* = Spend variables
P_* = Payment variables
B_* = Balance variables
R_* = Risk variables
with the following features being categorical:

['B_30', 'B_38', 'D_114', 'D_116', 'D_117', 'D_120', 'D_126', 'D_63', 'D_64', 'D_66', 'D_68']

Your task is to predict, for each customer_ID, the probability of a future payment default (target = 1).

Note that the negative class has been subsampled for this dataset at 5%, and thus receives a 20x weighting in the scoring metric.

Files
train_data.csv - training data with multiple statement dates per customer_ID
train_labels.csv - target label for each customer_ID
test_data.csv - corresponding test data; your objective is to predict the target label for each customer_ID
sample_submission.csv - a sample submission file in the correct format

Contact 
--------
Kyndryl GCP Guild Moderator: Ramamurthy V 
GCP Contact: gcpguild@gmail.com
Date: June 21, 2022.
Revised : July 8, 2022
Contributors: 122 key members from Google Cloud Search API.

For SerpAPI Key request, please write an email request with an email subject of 'request for SerpApi Key'

"""
#-----------------------------------------------------#-----------------------------------------------------
githubserpaijson = "https://github.com/gcpguild/amex/blob/main/bigquery_generator_amex.py"

import json, re, csv, os, unicodedata, requests, string, platform
import ijson
from bs4 import BeautifulSoup
from pathlib import Path
from six.moves.urllib.request import urlopen

import pandas as pd
from os import listdir

import urllib.request

import numpy as np

from requests import request
from urllib.error import URLError

from itertools import filterfalse
import json, re, csv, unicodedata, string, sys, glob
from pathlib import Path
import pandas as pd
from subprocess import check_output
#----------------------------------------------------------
projectID="hci-project-313508"
dataset="amex_credit_card"
URI = "gs://nature_labs/train_labels/test_labels.csv"
tbl = "amex_customers"
cust_type = 'test'
ext_table_name = ("{}_{}".format(tbl,cust_type))
#---------------------------------------------------------------
def removen(string):
    for m in ('\n', '\r'):
        clean_string = re.sub(m, '', string)
        clean_string = clean_string.replace(m, '')
        clean_string = clean_string.rstrip()
        clean_string = clean_string.strip(m)
        clean_string = re.sub(m,' ', clean_string)
               
        mymano = ''
        for x in clean_string:
            mymano += ''+ x
        return mymano
#---------------------------------------------------
def fullyqualifydirs(mylist):
    mydircode = N.join(mylist)
    return mydircode
#-----------------------------------------------------
def find_csv_filenames( path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]
# --------------------------------------------------------------------
def mkingdirs(givenlist):
    mymanog = N.join(givenlist)
    mk_dir = Path(mymanog)
    mk_dir.mkdir(parents=True, exist_ok=True)
    return mk_dir
#-------------------------------------------------------------------
def dataadding(inputfilename, head, l):
    tf = pd.DataFrame(l)
    modetw =  'a'
    tf.to_csv(inputfilename, encoding='utf-8', mode=modetw, index=False, header = False)
#---------------------------------------------------------------------------------

def prt(p):

    width = len(p) + 4
    print('┏' + "━"*width + "┓")
    print('┃' + p.center(width) + '┃')
    print('┗' + "━"*width + "┛")
#-------------------------------------------------------

myos = platform.system()

if (myos == "Linux" or myos == "linux2"):
    # linux
    N="/"
    homedirectory = str(Path.home())
    print(homedirectory)
    
    mylist = [ homedirectory, 'serpapi/python/app/nature-labs/amex' ]
    basedir = fullyqualifydirs(mylist)
    print(basedir)
elif myos == "win32" or myos == "Windows":
    # Windows 
    N="\\"    
    basedir = 'C:\\serpapi\\python\\app\\nature-labs\\amex'  
#--------------------------------------------------------------
if not basedir:
    basedir = os.getcwd()

#-----------------------------------------------------------------------------------------------------
mylist = [basedir, 'initialization.py']
initialdirectoryconfig = fullyqualifydirs(mylist)
#------------------------------------------------------------------------------------------------------
getdirectory = removen(check_output([sys.executable, initialdirectoryconfig], universal_newlines=True))
myoutdir = [basedir, 'output' ]
outputdir = mkingdirs(myoutdir)

mylist = [ getdirectory, 'test' ]

csv_in_path_to_dir = find_csv_filenames( path_to_dir = fullyqualifydirs(mylist))

serpapijsongooglecloud = csv_in_path_to_dir 

n = 7
for l in serpapijsongooglecloud:

    mylist = [ getdirectory,'test', l ]
    googlecloudservicecsv = fullyqualifydirs(mylist)

   
    path = Path(googlecloudservicecsv)

    if not path.is_file():
        pi="\'amex data is missing: \' :"
        p = ("{} {}".format(pi,googlecloudservicecsv))
        prt(p)
        exit(1)
    print(googlecloudservicecsv)
    df = pd.read_csv(googlecloudservicecsv, nrows=n)
    file_id = removen(re.sub(r'^.+/([^/]+)$', r'\1', os.path.splitext(googlecloudservicecsv)[0]).split('\\')[-1])

    print(file_id)
    df.head()
    colname = df.columns
    df.info()
    df.describe()
    datatypes=dict(df.dtypes)
    #-----------------------------------------------------

def switch(check_data_type):
    dict={
              'object': 'STRING',
              'int64' : 'INT64',
              'float64': 'FLOAT64',
              'DATE'  : 'DATE'
          }
    return dict.get(check_data_type, 'Unable to find Data Type')
#-----------------------------------------------------------------
datearray=['date', 'DATE', 'Date']
fldnames=[]
for fld in colname:
   
    for cdatesrt in (datearray):
        check_date_return = fld.find(cdatesrt)
        check_date_lu=cdatesrt
        if(check_date_return != -1):
            check_data_type='DATE'
            break    
        else:
            dtdef=df[fld].dtypes
            check_data_type = str(dtdef)
        
    flddty=switch(check_data_type)
    dc=fld
   
    dc = re.sub('[^A-Za-z0-9]+', ' ', dc)
    dc = dc.strip()
    dc = dc.rstrip()
    dc = dc.lstrip()
    dc = re.sub("\s", "_", dc)  
    dc = re.sub(r"[^\w\s]", '', dc)
    dc = re.sub(r"\s+", '_', dc)
    ddc=dc
  
    tblsting=("{} {}".format(dc, flddty))
    fldnames.append(tblsting)
#-----------------------------------------

bqf = ("{}{}{}{}{}".format('BQ_', tbl, '_', cust_type, ".sql"))

mylist = [ basedir, 'output' , bqf]

bqfile = fullyqualifydirs(mylist)

L=[]
lc=1
ll=len(fldnames)

for fldy in (fldnames):
  
    fldy=("{}\t{}".format("\t",fldy))
    L.append(fldy)
    if (lc == ll):
        N="\n"
    else:
        N=",\n"
    lc += 1
    L.append(N)


cene = open(bqfile, 'w')


tbe=ext_table_name
tbe= re.sub('[^A-Za-z0-9]+', ' ', tbe)
tbe = tbe.strip()
tbe = tbe.rstrip()
tbe = tbe.lstrip()
tbe = re.sub("\s", "_", tbe)  
tbe = re.sub(r"[^\w\s]", '', tbe)
tbe = re.sub(r"\s+", '_', tbe)
   
ts=("{} {} {}".format("--Generated schema for table:",tbe,"--"))
fulltblname=("{}.{}.{}".format(projectID,dataset,tbe))
line1=("{} {}{}{}".format("CREATE EXTERNAL TABLE","`",fulltblname,"`\n"))



line2="(\n"
line3="\n)"
s = """
OPTIONS(
skip_leading_rows=1,
format="CSV",
"""
uris=("{}{}{}{}".format("uris=[","\"",URI,"\"]"))
line4="\n);"

cene.write(line1)
cene.write(line2)
cene.writelines(L)

cene.write(line3)

cene.write(s)
cene.write(uris)

cene.write(line4)

cene.close()


pi="\'Create 'Table' GCP Big Query \' : "
p = ("{} {}".format(pi,bqfile))
prt(p)