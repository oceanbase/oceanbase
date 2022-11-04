import math
import numpy as np
from scipy.optimize import leastsq
from scipy.optimize import curve_fit
import sys
from lmfit import Model
import getopt
import subprocess
import os
import re


types_to_test = {'bigint':'bigint', 'double':'double', 'float':'float', 'timestamp':'timestamp', 'number':'number(20,3)','v1':'varchar(1)','v32':'varchar(32)', 'v64':'varchar(64)', 'v128':'varchar(128)'}

def run_cmd(cmd):
    #print cmd
    res = ''
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while True:
        line = p.stdout.readline()
        res += line
        if line:
            #print line.strip()
            sys.stdout.flush()
        else:
            break
    p.wait()
    return res.strip()

def rm_if_exist(filename):
    if os.path.exists(filename):
        os.remove(filename)

def extract_kv(k, src):
    pat=k + ':\s*[\d\.e\-\+]+'
    mat = re.compile(pat)
    return float(mat.findall(src)[0].split()[1])

for t in sorted(types_to_test.keys()):
    result_file_name = 'rowstore.result.' + t
    prep_file_name = 'rowstore.prep.' + t
    model_file = 'rowstore.model.' + t
    fit_file = 'rowstore.fit.' + t
    rm_if_exist(prep_file_name)
    run_cmd("./preprocess.py -i %s -o %s -t 7 -C 3 -d" % (result_file_name, prep_file_name))
    fitres = run_cmd("./fit_material.py -i " + prep_file_name + " -o " + model_file)
    # print fitres
    run_cmd("./apply_material_model.py -i %s -o %s -m %s" % (prep_file_name, fit_file, model_file))
    Trow_col = extract_kv('Trow_col', fitres)
    Trow_once = extract_kv('Trow_once', fitres)
    print types_to_test[t] + ":"
    print "  " + str(Trow_col)
    print "  " + str(Trow_once)







