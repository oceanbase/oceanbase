#!/usr/bin/python

import csv
gbk_col=[]
uni_col=[]
with open('gbk_uni_tab', 'r') as fd:
  reader = csv.reader(fd)
  for row in reader:
    data=row[0].split('\t')
    gbk_col.append(data[0])
    uni_col.append(data[1])

gbk_col_full=[]
uni_col_full=[]
gbk_col_idx = 0;
gbk_col_max = len(gbk_col)

for gbk_encoding in range(0x8140, 0xFEFF):
  gbk_col_full.append(gbk_encoding)

  if gbk_col_idx >= gbk_col_max:
    uni_col_full.append(0)
  else:
    cur_encoding = int(gbk_col[gbk_col_idx], 16)
    if (cur_encoding > gbk_encoding):
      uni_col_full.append(0)
    elif (cur_encoding == gbk_encoding):
      uni_col_full.append(int(uni_col[gbk_col_idx], 16))
      gbk_col_idx+=1
    else:
      exit(-1)

print len(uni_col_full), len(gbk_col_full)

with open('gbk_uni_array', 'w') as fd:
  line_str = ''
  count = 0
  for uni_data in uni_col_full:
    if (uni_data == 0):
      line_str += '     0,'
    else:
      line_str += "0x{0:0{1}X},".format(uni_data, 4)
    count+=1
    if (count % 8 == 0):
      line_str += '\n'
      fd.write(line_str)
      line_str = ''

  if len(line_str) > 0:
    fd.write(line_str)
  


