#!/bin/env python
__author__ = 'dongyun.zdy'
import multiprocessing as mp
import subprocess as sp
import os
import signal
import sys

inner_arg = ""

def single_bench(arg):
    cmd = "./storage_bench %s"%(arg)
    sp.check_call(cmd, shell = True)

if __name__ == '__main__':
    if len(sys.argv) < 5 or len(sys.argv) > 6:
        print "wrong arg"
    else:
        res_file = sys.argv[1]
        begin = int(sys.argv[2])
        end = int(sys.argv[3])
        step = int(sys.argv[4])
        if len(sys.argv) == 6:
            inner_arg = sys.argv[5]

        print "benchmark start"
        if os.path.exists(res_file) :
            os.remove(res_file)

        sp.check_call("./storage_bench -r 10000 -b 1 -e 1000 -R", shell = True)

        #arg = 0.1
        for i in xrange(begin,end+1,step):
            #arg = int(arg * 10)
            arg = "-r 10000 -b 1 -e %s %s >> %s" % (i, inner_arg, res_file)
            single_bench(arg)
        print "benchmark done"
        print "#"






