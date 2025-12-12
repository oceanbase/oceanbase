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

namespace oceanbase
{
namespace common
{
  class ObArenaAllocator;
  class ObTabletID;
  class ObDatum;
}

namespace storage
{
class ObDirectLoadVector;
}

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
    const schema::ObTableSchema &table_schema,
    uint64_t &column_id);
  static int handle_hidden_clustering_key_column(
    common::ObArenaAllocator &allocator,
    const common::ObTabletID &tablet_id,
    common::ObDatum &datum);
  static int fill_hidden_clustering_key_for_vector(
    common::ObArenaAllocator &allocator,
    storage::ObDirectLoadVector *hidden_pk_vector,
    storage::ObDirectLoadVector *tablet_id_vector,
    const bool is_single_part,
    const common::ObTabletID &single_tablet_id,
    const int64_t row_start,
    const int64_t count);
};
}
}

 #endif