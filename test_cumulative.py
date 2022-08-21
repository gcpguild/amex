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
githubserpaijson = "https://github.com/gcpguild/searpapi/blob/main/parseserpapijsongooglecloud.py"

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
myoutdir = [basedir, 'schema_output' ]
outputdir = mkingdirs(myoutdir)

mylist = [ basedir, 'schema', cust_type ]

csv_in_path_to_dir = find_csv_filenames( path_to_dir = fullyqualifydirs(mylist))

serpapijsongooglecloud = csv_in_path_to_dir 


for l in serpapijsongooglecloud:

    mylist = [ basedir,'schema', cust_type, l ]
    googlecloudservicecsv = fullyqualifydirs(mylist)

   
    path = Path(googlecloudservicecsv)

    if not path.is_file():
        pi="\'amex data is missing: \' :"
        p = ("{} {}".format(pi,googlecloudservicecsv))
        prt(p)
        exit(1)

    df = pd.read_csv(googlecloudservicecsv,  usecols=[0,2])
   
    values = df.values
    #-----------------------------------------------------

#-----------------------------------------------------------------
valnames=[]
c = [ 'D', 'S', 'P', 'B', 'R']
D = []
S = [] 
P = [] 
B = [] 
R = []
#------------------------------------------
for v in values:
    print(v)
    fc = ''.join([str(elem) for elem in v[0]])
    
    if (fc[:1] == c[0]):
        #  IFNULL(t.Project,0) AS Project_Score, 
        #
        if (v[1] == 'ifnull'):
            i = ("{}{}{}{}".format('IFNULL(', fc,',0) AS ', fc))
            D.append(i)
    if (fc[:1] == c[1]):
        S.append(fc)
    if (fc[:1] == c[2]):
        P.append(fc)
    if (fc[:1] == c[3]):
        B.append(fc)
    if (fc[:1] == c[4]):
        R.append(fc)
#----------------------------------------- 

print(D)

bqf = ("{}{}{}{}{}".format('BQ_', tbl, '_', cust_type, ".sql"))

mylist = [ basedir, 'schema_output'  , bqf]

bqfile = fullyqualifydirs(mylist)


cene = open(bqfile, 'w')


tbe=ext_table_name
tbe= re.sub('[^A-Za-z0-9]+', ' ', tbe)
tbe = tbe.strip()
tbe = tbe.rstrip()
tbe = tbe.lstrip()
tbe = re.sub("\s", "_", tbe)  
tbe = re.sub(r"[^\w\s]", '', tbe)
tbe = re.sub(r"\s+", '_', tbe)
   
ts=("{}{}{}{}{}{}{}{}{}{}".format("--Generated SELECT for table:",tbe,"--", '\n','#standardSQL', '\n','SELECT', '\n\t', 'customer_ID,','\n'))

cene.write(ts)


L=[]
lc = 1
ll=len(D)

for f in range(0, ll):
    fs = ''.join([str(elem) for elem in D[f]])
    fld = ("{}\t{}".format("\t",fs))
    L.append(fld)
    if (lc == ll):
        E="\n"
    else:
        E=",\n"
    lc += 1
    L.append(E)

cene.writelines(L)

fulltblname=("{}.{}.{}".format(projectID,dataset,tbe))
line1=("\n\t{} {}{}{}".format("FROM ","`",fulltblname,"`\n"))
cene.write(line1)
cene.close()

pi="\'Create 'Table' GCP Big Query \' : "
p = ("{} {}".format(pi,bqfile))
prt(p)