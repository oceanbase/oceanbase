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

#ifndef OCEANBASE_HEAP_ORGANIZED_TABLE_UTIL_H_
#define OCEANBASE_HEAP_ORGANIZED_TABLE_UTIL_H_

#include "share/schema/ob_table_schema.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace share
{
class ObHeapTableUtil
{
public:
  static int generate_pk_increment_column(
    schema::ObTableSchema &table_schema,
    const uint64_t available_col_id,
    const uint64_t rowkey_position);
  static bool is_table_with_clustering_key(
    const bool is_table_without_pk,
    const bool is_table_with_hidden_pk_column);
  static int get_hidden_clustering_key_column_id(
    const ObTableSchema &table_schema,
    uint64_t &column_id);
};
}
}

 #endif