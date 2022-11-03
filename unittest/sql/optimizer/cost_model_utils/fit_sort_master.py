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


types_to_test = {'bigint':['bigint', 0.0266846, 0.07364082], 'double': ['double', 0.02970336, 0.07228732], 'float':['float', 0.02512819, 0.07295116], 'timestamp':['timestamp', 0.02998249, 0.07265038],
                 'number':['number(20,3)', 0.08238981, 0.15730252], 'v32':['varchar(32)', 0.08476897, 0.07518651], 'v64':['varchar(64)', 0.13678196, 0.05033624], 'v128':['varchar(128)', 0.22601192, 2.2963e-08]}

def run_cmd(cmd):
    print cmd
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
    result_file_name = 'sort.result.' + t
    prep_file_name = 'sort.prep.' + t
    model_file = 'sort.model.' + t
    fit_file = 'sort.fit.' + t
    if not os.path.exists(result_file_name):
        continue
    rm_if_exist(prep_file_name)
    rm_if_exist(model_file)
    rm_if_exist(fit_file)
    run_cmd("./preprocess.py -i %s -o %s -t 7 -C 4 -d" % (result_file_name, prep_file_name))
    cmd = "./fit_sort.py -i %s -R %s -C %s -o %s" % (prep_file_name, str(types_to_test[t][2]), str(types_to_test[t][1]), model_file)
    print cmd
    fitres = run_cmd(cmd)
    # print fitres
    appres = run_cmd("./apply_sort_model.py -i %s -o %s -m %s" % (prep_file_name, fit_file, model_file))
    print appres
    #print fitres
    # Treserve_cell = extract_kv('Treserve_cell', fitres)
    # Tcompare = extract_kv('Tcompare', fitres)
    # Tmiss_K1 = extract_kv('Tmiss_K1', fitres)
    # Turn = extract_kv('Turn', fitres)
    # # Trow_once = extract_kv('Trow_once', fitres)
    # print types_to_test[t][0] + ":"
    # # print "  Treserve_cell:\t" + str(Treserve_cell)
    # print "  Tcompare:\t" + str(Tcompare)
    # print "  Tmiss_K1:\t" + str(Tmiss_K1)
    # print "  Turn:\t" + str(Turn)
    # print "  Trow_once:\t" + str(Trow_once)







