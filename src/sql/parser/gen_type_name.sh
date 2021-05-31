#!/bin/sh
echo '/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */'
echo -e '#include "sql/parser/ob_item_type.h"'
echo -e "const char* get_type_name(int type)\n{"
echo -e "\tswitch(type){"
sed -rn 's/\s*(T_[_A-Z1-9]+)[ =0-9]*,/\tcase \1 : return \"\1\";/p' $1
echo -e '\tdefault:return "Unknown";\n\t}\n}'
