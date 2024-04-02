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
#define USING_LOG_PREFIX SHARE

#include "ob_balance_define.h"

namespace oceanbase
{
namespace share
{
bool need_balance_table(const schema::ObSimpleTableSchemaV2 &table_schema)
{
  //TODO not support nonduplicate and duplicate exchange
  bool need_balance = false;
  const char* table_type_str = NULL;
  need_balance = check_if_need_balance_table(table_schema, table_type_str);
  return need_balance;
}

bool check_if_need_balance_table(
    const schema::ObSimpleTableSchemaV2 &table_schema,
    const char *&table_type_str)
{
  bool need_balance = false;
  if (table_schema.is_duplicate_table()) {
    table_type_str = "DUPLICATE TABLE";
  } else if (table_schema.is_index_table() && !table_schema.is_global_index_table()) {
    table_type_str = "LOCAL INDEX";
  } else {
    table_type_str = ob_table_type_str(table_schema.get_table_type());
  }
  need_balance = !table_schema.is_duplicate_table()
      && (table_schema.is_user_table()
      || table_schema.is_global_index_table()
      || table_schema.is_tmp_table());
  return need_balance;
}

}
}
