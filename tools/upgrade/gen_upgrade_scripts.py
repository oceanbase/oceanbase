#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import stat

def get_py_filename_list(except_filter_filename_list):
  py_filename_list = []
  filename_list = os.listdir(os.path.dirname(os.path.abspath(sys.argv[0])))
  for filename in filename_list:
    if filename.endswith('.py'):
      is_filtered = False
      for except_filter_filename in except_filter_filename_list:
        if filename == except_filter_filename:
          is_filtered = True
          break
      if False == is_filtered:
        py_filename_list.append(filename)
  py_filename_list.sort()
  return py_filename_list

def get_concat_sub_files_lines(py_filename_list, file_splitter_line, \
    sub_filename_line_prefix, sub_file_module_end_line):
  concat_sub_files_lines = []
  # 写入__init__.py
  concat_sub_files_lines.append(file_splitter_line + '\n')
  concat_sub_files_lines.append(sub_filename_line_prefix + '__init__.py\n')
  concat_sub_files_lines.append('##!/usr/bin/env python\n')
  concat_sub_files_lines.append('## -*- coding: utf-8 -*-\n')
  # 写入其他py文件
  for sub_py_filename in py_filename_list:
    sub_py_file = open(sub_py_filename, 'r')
    sub_py_file_lines = sub_py_file.readlines()
    concat_sub_files_lines.append(file_splitter_line + '\n')
    concat_sub_files_lines.append(sub_filename_line_prefix + sub_py_filename + '\n')
    for sub_py_file_line in sub_py_file_lines:
      concat_sub_files_lines.append('#' + sub_py_file_line)
    sub_py_file.close()
  concat_sub_files_lines.append(file_splitter_line + '\n')
  concat_sub_files_lines.append(sub_file_module_end_line + '\n')
  return concat_sub_files_lines

def gen_upgrade_script(filename, concat_sub_files_lines, extra_lines_str):
  os.chmod(filename, stat.S_IRUSR + stat.S_IWUSR + stat.S_IXUSR + stat.S_IRGRP + stat.S_IXGRP + stat.S_IROTH + stat.S_IXOTH)
  file = open(filename, 'w')
  file.write('#!/usr/bin/env python\n')
  file.write('# -*- coding: utf-8 -*-\n')
  for concat_sub_files_line in concat_sub_files_lines:
    file.write(concat_sub_files_line)
  file.write('\n')
  file.write(extra_lines_str)
  file.close()
  os.chmod(filename, stat.S_IRUSR + stat.S_IXUSR + stat.S_IRGRP + stat.S_IXGRP + stat.S_IROTH + stat.S_IXOTH)

def get_main_func_str(run_filename):
  return """
if __name__ == '__main__':
  cur_filename = sys.argv[0][sys.argv[0].rfind(os.sep)+1:]
  (cur_file_short_name,cur_file_ext_name1) = os.path.splitext(sys.argv[0])
  (cur_file_real_name,cur_file_ext_name2) = os.path.splitext(cur_filename)
  sub_files_dir_suffix = '_extract_files_' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f') + '_' + random_str()
  sub_files_dir = cur_file_short_name + sub_files_dir_suffix
  sub_files_short_dir = cur_file_real_name + sub_files_dir_suffix
  split_py_files(sub_files_dir)
  sub_files_absolute_dir = os.path.abspath(sub_files_dir)
  sys.path.append(sub_files_absolute_dir)
  from {run_module_name} import do_upgrade_by_argv
  do_upgrade_by_argv(sys.argv[1:])
""".format(run_module_name = run_filename[0:run_filename.rfind('.')])

def get_pre_and_post_extra_lines_strs(upgrade_pre_filename, upgrade_post_filename, \
    do_upgrade_pre_filename, do_upgrade_post_filename, \
    file_splitter_line, sub_filename_line_prefix, sub_file_module_end_line):
  upgrade_common_lines = """
from __future__ import print_function, absolute_import
import os
import sys
import datetime
from random import Random

class SplitError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

def random_str(rand_str_len = 8):
  str = ''
  chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
  length = len(chars) - 1
  random = Random()
  for i in range(rand_str_len):
    str += chars[random.randint(0, length)]
  return str

def split_py_files(sub_files_dir):
  char_enter = '\\n'
  file_splitter_line = '{file_splitter_line}'
  sub_filename_line_prefix = '{sub_filename_line_prefix}'
  sub_file_module_end_line = '{sub_file_module_end_line}'
  os.makedirs(sub_files_dir)
  print('succeed to create run dir: ' + sub_files_dir + char_enter)
  cur_file = open(sys.argv[0], 'r')
  cur_file_lines = cur_file.readlines()
  cur_file_lines_count = len(cur_file_lines)
  sub_file_lines = []
  sub_filename = ''
  begin_read_sub_py_file = False
  is_first_splitter_line = True
  i = 0
  while i < cur_file_lines_count:
    if (file_splitter_line + char_enter) != cur_file_lines[i]:
      if begin_read_sub_py_file:
        sub_file_lines.append(cur_file_lines[i])
    else:
      if is_first_splitter_line:
        is_first_splitter_line = False
      else:
        #读完一个子文件了，写到磁盘中
        sub_file = open(sub_files_dir + '/' + sub_filename, 'w')
        for sub_file_line in sub_file_lines:
          sub_file.write(sub_file_line[1:])
        sub_file.close()
        #清空sub_file_lines
        sub_file_lines = []
      #再读取下一行的文件名或者结束标记
      i += 1
      if i >= cur_file_lines_count:
        raise SplitError('invalid line index:' + str(i) + ', lines_count:' + str(cur_file_lines_count))
      elif (sub_file_module_end_line + char_enter) == cur_file_lines[i]:
        print('succeed to split all sub py files')
        break
      else:
        mark_idx = cur_file_lines[i].find(sub_filename_line_prefix)
        if 0 != mark_idx:
          raise SplitError('invalid sub file name line, mark_idx = ' + str(mark_idx) + ', line = ' + cur_file_lines[i])
        else:
          sub_filename = cur_file_lines[i][len(sub_filename_line_prefix):-1]
          begin_read_sub_py_file = True
    i += 1
  cur_file.close()

""".format(file_splitter_line = file_splitter_line, \
    sub_filename_line_prefix = sub_filename_line_prefix, \
    sub_file_module_end_line = sub_file_module_end_line)
  upgrade_pre_main_func_lines_str = get_main_func_str(do_upgrade_pre_filename)
  upgrade_post_main_func_lines_str = get_main_func_str(do_upgrade_post_filename)
  upgrade_pre_extra_lines_str = upgrade_common_lines + '\n' + upgrade_pre_main_func_lines_str
  upgrade_post_extra_lines_str = upgrade_common_lines + '\n' + upgrade_post_main_func_lines_str
  return (upgrade_pre_extra_lines_str, upgrade_post_extra_lines_str)

if __name__ == '__main__':
  upgrade_pre_filename = 'upgrade_pre.py'
  upgrade_post_filename = 'upgrade_post.py'
  do_upgrade_pre_filename = 'do_upgrade_pre.py'
  do_upgrade_post_filename = 'do_upgrade_post.py'
  obcdc_compatible_filename = 'gen_obcdc_compatiable_info.py'
  cur_filename = sys.argv[0][sys.argv[0].rfind(os.sep)+1:]
  except_filter_filename_list = [cur_filename, upgrade_pre_filename, upgrade_post_filename, obcdc_compatible_filename]
  file_splitter_line = '####====XXXX======######==== I am a splitter ====######======XXXX====####'
  sub_filename_line_prefix = '#filename:'
  sub_file_module_end_line = '#sub file module end'
  (upgrade_pre_extra_lines_str, upgrade_post_extra_lines_str) = \
      get_pre_and_post_extra_lines_strs(upgrade_pre_filename, upgrade_post_filename, \
      do_upgrade_pre_filename, do_upgrade_post_filename, \
      file_splitter_line, sub_filename_line_prefix, sub_file_module_end_line)
  py_filename_list = get_py_filename_list(except_filter_filename_list)
  concat_sub_files_lines = get_concat_sub_files_lines(py_filename_list, file_splitter_line, \
      sub_filename_line_prefix, sub_file_module_end_line)
  gen_upgrade_script(upgrade_pre_filename, concat_sub_files_lines, upgrade_pre_extra_lines_str)
  gen_upgrade_script(upgrade_post_filename, concat_sub_files_lines, upgrade_post_extra_lines_str)

