#!/usr/bin/env python
#remove pointer in explain extented output
import re;
import os;
import sys;

# get file name
origin_file = sys.argv[1]
tmp_file = 'pointer.tmp'

## copy file
os.system('cp ' + origin_file + ' ' + tmp_file)

## open file
origin_rw = open(origin_file, 'r+')
tmp_rw = open(tmp_file, 'r+')

origin_rw.truncate()
for line in tmp_rw:
    new_line = re.sub('\(0x[A-Za-z0-9]*\)', '', line)
    origin_rw.write(new_line);
