#!/bin/env python2
'''
cat a.h b.h | ./gen-ns.py > ns.h
'''
import re
import sys

def help(): print __doc__
not sys.stdin.isatty() or help() or sys.exit()

symlist = set(re.findall('my_([_a-zA-Z0-9]+)', sys.stdin.read()))
def_list = ['#define my_%s tns(_%s)'%(s, s) for s in symlist]
undef_list = ['#undef my_%s'%(s) for s in symlist]
print '''
#ifndef __ns__
#define __ns__
%s
#else
#undef __ns__
#undef tns
%s
#endif
''' %('\n'.join(def_list), '\n'.join(undef_list))
