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
  return !table_schema.is_duplicate_table()
       && (table_schema.is_user_table()
      || table_schema.is_global_index_table()
      || table_schema.is_tmp_table());
}

}
}
