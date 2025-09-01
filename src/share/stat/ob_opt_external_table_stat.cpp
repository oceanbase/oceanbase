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

#include "share/stat/ob_opt_external_table_stat.h"

namespace oceanbase {
namespace share {

OB_DEF_SERIALIZE(ObOptExternalTableStat) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, catalog_id_, database_name_,
              table_name_, partition_value_, row_count_, file_num_, data_size_,
              last_analyzed_, partition_num_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObOptExternalTableStat) {
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, len, tenant_id_, catalog_id_, database_name_,
              table_name_, partition_value_, row_count_, file_num_, data_size_,
              last_analyzed_, partition_num_);
  return len;
}

OB_DEF_DESERIALIZE(ObOptExternalTableStat) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, catalog_id_, database_name_,
              table_name_, partition_value_, row_count_, file_num_, data_size_,
              last_analyzed_, partition_num_);
  return ret;
}

int ObOptExternalTableStat::assign(const ObOptExternalTableStat &other) {
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  catalog_id_ = other.catalog_id_;
  database_name_ = other.database_name_;
  table_name_ = other.table_name_;
  partition_value_ = other.partition_value_;
  row_count_ = other.row_count_;
  file_num_ = other.file_num_;
  data_size_ = other.data_size_;
  partition_num_ = other.partition_num_;
  last_analyzed_ = other.last_analyzed_;
  return ret;
}

} // namespace share
} // namespace oceanbase
