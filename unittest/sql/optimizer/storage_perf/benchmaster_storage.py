#!/bin/env python
__author__ = 'dongyun.zdy'
import subprocess as sp
import os
import sys
from random import randint
import re

def run_cmd(cmd):
    print cmd
    res = ''
    p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT)
    while True:
        line = p.stdout.readline()
        res += line
        if line:
            print line.strip()
            sys.stdout.flush()
        else:
            break
    p.wait()
    return res

run_cmd('./benchmaster_storage_scan.py')
run_cmd('./benchmaster_storage_get.py')
