#!/bin/env python2
# -*- coding: utf-8 -*-
# Author:
#
# Example:
#   ./get-oer-define.py ORA-12463
#
# Project:
#   http://gitlab.alibaba-inc.com/jim.wjh/OER-tool
#

import sys
import re
import os
from os.path import expanduser

Usage = 'invalid argument, eg: ./get_oer_define.py ORA-00000'

if len(sys.argv) != 2:
    print (Usage)
else:
    OER_GIT_PATH = expanduser("~") + "/OER_DATA"
    OB_FLOW_PATH = os.environ.get('OB_FLOW_WORK_DIR')
    if len(OB_FLOW_PATH) > 0 and os.path.exists(OB_FLOW_PATH):
      OER_GIT_PATH = OB_FLOW_PATH + "/OER_DATA"
    ERR_FILE_PATH = OER_GIT_PATH + '/ORA_V12c'

    if not os.path.exists(OER_GIT_PATH):
        os.system('git clone http://gitlab.alibaba-inc.com/jim.wjh/OER.git ' + OER_GIT_PATH)

    target_ora_str = sys.argv[1]
    target_ora_str_parts = sys.argv[1].split('-')
    if target_ora_str_parts[0] != "ORA" or len(target_ora_str_parts[1]) != 5:
      print (Usage)
      sys.exit(0)
    target_ora_num = int(target_ora_str_parts[1])
    target_file = ''

    for filename in os.listdir(ERR_FILE_PATH):
        fragment = filename.split('_')
        if filename[:3] == 'ORA' and int(fragment[1]) <= target_ora_num and target_ora_num <= int(fragment[2]):
            target_file = filename
            break;

    f = open(ERR_FILE_PATH + '/' + filename, 'r')
    is_recording = False
    result = ''
    for line in f.readlines():
        if line[:3] == 'ORA':
            if is_recording:
                break
            elif line[:9] == target_ora_str:
                is_recording = True
        if is_recording and re.search('[\S]+', line) != None:
            result += "//" + line

    exclude_patterns = [
        "\\([^)]*\\)", #括号中的内容
        "\\'[^']*\\'", #引号中的内容
        ": string",    #字符串
        "string.string",    #字符串
    ]

    replacements = [
        (": string", ": %.*s"),
        ("string", "%.*s"),
    ]

    if len(result) > 0:
        first_line = result.split('\n')[0]

        oer_msg = first_line[12:].strip()

        oer_msg_refined = oer_msg
        for pattern in exclude_patterns:
            oer_msg_refined = re.sub(pattern, '', oer_msg_refined)
        oer_msg_refined = ' '.join(oer_msg_refined.split())

        err_define = '_'.join(filter(None, oer_msg_refined.split(' '))).upper()
        ob_err_define = 'OB_ERR_' + err_define

        #生成改造过的用户错误信息
        user_err_msg = oer_msg_refined
        user_err_msg_ext = oer_msg
        replace_happend = False
        for replacement in replacements:
            if None != re.search(replacement[0], user_err_msg_ext):
              user_err_msg_ext = re.sub(replacement[0], replacement[1], user_err_msg_ext)
              replace_happend = True
        user_err_msg_ext = ' '.join(user_err_msg_ext.split())
        print ('\033[31mGenerated changes:\033[0m')
        print (result)
        print ('\033[31mGenerated changes for file ob_errno.def:\033[0m')
        print ('DEFINE_ORACLE_ERROR' + ('_EXT' if replace_happend else '') +
               '('
                  + ob_err_define + ', -####' + ', -1' + ', "HY000"'
                  + ', "' + user_err_msg + '"' + ((', "' + user_err_msg_ext + '"') if replace_happend else '')
                  + ', ' + str(target_ora_num)
                  + ', "' + user_err_msg + '"' + ((', "' + user_err_msg_ext + '"') if replace_happend else '')
                  + ');'
              )
