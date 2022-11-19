#!/usr/bin/python

import csv
gbk_col=[]
with open('uni_gbk_tab', 'r') as fd:
  reader = csv.reader(fd)
  for row in reader:
    data=row[0].split('\t')
    gbk_col.append(data[0])

with open('uni_gbk_array', 'w') as fd:
  line_str = ''
  count = 0
  for gbk_data in gbk_col:
    if (gbk_data == 0):
      line_str += '     0,'
    else:
      line_str += '0x' + gbk_data + ','
    count+=1
    if (count % 8 == 0):
      line_str += '\n'
      fd.write(line_str)
      line_str = ''

  if len(line_str) > 0:
    fd.write(line_str)
    

  


