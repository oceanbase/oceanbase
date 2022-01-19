#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

def clear_action_codes(action_filename_list, action_begin_line, \
    action_end_line, is_special_upgrade_code):
  char_enter = '\n'
  for action_filename in action_filename_list:
    new_action_file_lines = []
    action_file = open(action_filename, 'r')
    action_file_lines = action_file.readlines()
    is_action_codes = False
    for action_file_line in action_file_lines:
      if is_action_codes and action_file_line == (action_end_line + char_enter):
        is_action_codes = False
      if not is_action_codes:
        new_action_file_lines.append(action_file_line)
      if not is_action_codes and action_file_line == (action_begin_line + char_enter):
        is_action_codes = True
    action_file.close()
    new_action_file = open(action_filename, 'w')
    for new_action_file_line in new_action_file_lines:
      if is_special_upgrade_code:
        if new_action_file_line == (action_end_line + char_enter):
          new_action_file.write('  return\n')
      new_action_file.write(new_action_file_line)
    new_action_file.close()

def regenerate_upgrade_script():
  print('\n=========run gen_upgrade_scripts.py, begin=========\n')
  info = os.popen('./gen_upgrade_scripts.py;')
  print(info.read())
  print('\n=========run gen_upgrade_scripts.py, end=========\n')

if __name__ == '__main__':
  action_begin_line = '####========******####======== actions begin ========####******========####'
  action_end_line = '####========******####========= actions end =========####******========####'
  action_filename_list = \
      [\
      'normal_ddl_actions_pre.py',\
      'normal_ddl_actions_post.py',\
      'normal_dml_actions_pre.py',\
      'normal_dml_actions_post.py',\
      'each_tenant_dml_actions_pre.py',\
      'each_tenant_dml_actions_post.py',\
      'each_tenant_ddl_actions_post.py'\
      ]
  special_upgrade_filename_list = \
      [\
      'special_upgrade_action_pre.py',\
      'special_upgrade_action_post.py'
      ]
  clear_action_codes(action_filename_list, action_begin_line, action_end_line, False)
  clear_action_codes(special_upgrade_filename_list, action_begin_line, action_end_line, True)
  regenerate_upgrade_script()


