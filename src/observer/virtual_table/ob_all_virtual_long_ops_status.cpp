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

#define USING_LOG_PREFIX STORAGE

#include "ob_all_virtual_long_ops_status.h"

using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::storage;

ObAllVirtualLongOpsStatus::ObAllVirtualLongOpsStatus() : is_inited_(false), iter_(), ip_()
{}

int ObAllVirtualLongOpsStatus::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllVirtualLongOpsStatus has been inited twice", K(ret));
  } else if (OB_FAIL(iter_.init())) {
    LOG_WARN("fail to init iterator", K(ret));
  } else if (!GCTX.self_addr_.ip_to_string(ip_, sizeof(ip_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to convert ip to string", K(ret), "addr", GCTX.self_addr_);
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualLongOpsStatus::convert_stat_to_row(const ObILongOpsStat& stat, ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualLongOpsStatus has not been inited", K(ret));
  } else if (OB_UNLIKELY(!stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(stat));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // tenant id
          cur_row_.cells_[i].set_int(stat.get_key().tenant_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // sid
          cur_row_.cells_[i].set_int(stat.get_key().sid_);
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // op_name
          cur_row_.cells_[i].set_varchar(stat.get_key().name_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // target
          cur_row_.cells_[i].set_varchar(stat.get_key().target_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // svr_ip
          cur_row_.cells_[i].set_varchar(ip_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // svr_port
          cur_row_.cells_[i].set_int(GCTX.self_addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // start_time
          cur_row_.cells_[i].set_int(stat.common_value_.start_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // finish_time
          cur_row_.cells_[i].set_int(stat.common_value_.finish_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // elapsed_time
          cur_row_.cells_[i].set_int(stat.common_value_.elapsed_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // remaining_time
          cur_row_.cells_[i].set_int(stat.common_value_.remaining_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // last_update_time
          cur_row_.cells_[i].set_int(stat.common_value_.last_update_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // percentage
          cur_row_.cells_[i].set_int(stat.common_value_.percentage_);
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // message
          cur_row_.cells_[i].set_varchar(stat.common_value_.message_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualLongOpsStatus::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObILongOpsStatHandle handle;
  ObILongOpsStat* stat = NULL;
  row = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualLongOpsStatus has not been inited", K(ret));
  } else if (OB_FAIL(iter_.get_next_stat(handle))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next stat", K(ret));
    }
  } else if (OB_ISNULL(stat = handle.get_resource_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, part stat must not be NULL", K(ret));
  } else if (OB_FAIL(stat->estimate_cost())) {
    LOG_WARN("fail to estimate partition cost", K(ret));
  } else if (OB_FAIL(convert_stat_to_row(*stat, row))) {
    LOG_WARN("fail to convert stat to row", K(ret));
  }
  return ret;
}
