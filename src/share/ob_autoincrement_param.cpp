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

#include "share/ob_autoincrement_param.h"

namespace oceanbase {
namespace share {
int64_t AutoincKey::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id), K_(table_id), K_(column_id));
  J_OBJ_END();
  return pos;
}
int64_t AutoincParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("tenant_id", tenant_id_, "autoinc_table_id", autoinc_table_id_, "autoinc_first_part_num",autoinc_first_part_num_, "autoinc_table_part_num", autoinc_table_part_num_, "autoinc_col_id", autoinc_col_id_,"autoinc_col_index", autoinc_col_index_, "autoinc_update_col_index", autoinc_update_col_index_,"autoinc_col_type", autoinc_col_type_, "total_value_count_", total_value_count_, "autoinc_desired_count",autoinc_desired_count_, "autoinc_old_value_index", autoinc_old_value_index_, "autoinc_increment",autoinc_increment_, "autoinc_offset", autoinc_offset_, "curr_value_count", curr_value_count_,"global_value_to_sync", global_value_to_sync_, "value_to_sync", value_to_sync_, "sync_flag", sync_flag_,"is_ignore", is_ignore_, "autoinc_intervals_count", autoinc_intervals_count_, "part_level", part_level_,"paritition key", pkey_, "auto_increment_cache_size", auto_increment_cache_size_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(AutoincParam, tenant_id_, autoinc_table_id_, autoinc_table_part_num_, autoinc_col_id_,
    autoinc_col_index_, autoinc_update_col_index_, autoinc_col_type_, total_value_count_, autoinc_desired_count_,
    autoinc_old_value_index_, autoinc_increment_, autoinc_offset_, autoinc_first_part_num_, part_level_, pkey_,
    auto_increment_cache_size_);

}  // end namespace share
}  // end namespace oceanbase
