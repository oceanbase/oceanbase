/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_SQL_CHARACTER_SET_H_
#define OB_SQL_CHARACTER_SET_H_
#include <stdint.h>
#include "lib/string/ob_string.h"
namespace oceanbase
{
namespace sql
{
extern int32_t get_char_number_from_name(const common::ObString &name);
extern const char *get_char_name_from_number(const int32_t number);
}
}
#endif
