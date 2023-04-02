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

#define USING_LOG_PREFIX SQL_OPT
#include "share/stat/ob_opt_table_stat.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_macro_utils.h"
namespace oceanbase {
namespace common {
using namespace sql;

OB_DEF_SERIALIZE(ObOptTableStat) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              table_id_,
              partition_id_,
              object_type_,
              row_count_,
              avg_row_size_
              );
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObOptTableStat) {
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              table_id_,
              partition_id_,
              object_type_,
              row_count_,
              avg_row_size_
              );
  return len;
}

OB_DEF_DESERIALIZE(ObOptTableStat) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              table_id_,
              partition_id_,
              object_type_,
              row_count_,
              avg_row_size_
              );
  return ret;
}

int ObOptTableStat::merge_table_stat(const ObOptTableStat &other)
{
  int ret = OB_SUCCESS;
  if (table_id_ != other.get_table_id() ||
      partition_id_ != other.get_partition_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("two stat do not match", K(ret));
  } else {
    double avg_len = 0;
    other.get_avg_row_size(avg_len);
    add_avg_row_size(avg_len, other.get_row_count());
    row_count_ += other.get_row_count();
    stattype_locked_ = other.get_stattype_locked();
  }

  return ret;
}
}
}
