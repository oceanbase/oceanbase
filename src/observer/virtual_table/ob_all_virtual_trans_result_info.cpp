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

#include "observer/virtual_table/ob_all_virtual_trans_result_info.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_group.h"

namespace oceanbase {
using namespace common;
using namespace transaction;

namespace observer {
void ObGVTransResultInfo::reset()
{
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(partition_buffer_, 0, OB_MIN_BUFFER_SIZE);
  memset(trans_id_buffer_, 0, OB_MIN_BUFFER_SIZE);
  ObVirtualTableScannerIterator::reset();
}

void ObGVTransResultInfo::destroy()
{
  trans_service_ = NULL;
  reset();
}

int ObGVTransResultInfo::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_ || NULL == trans_service_) {
    SERVER_LOG(WARN,
        "invalid argument, allocator_ or trans_service_ is null",
        "allocator",
        OB_P(allocator_),
        "trans_service",
        OB_P(trans_service_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = trans_service_->iterate_partition(partition_iter_))) {
    TRANS_LOG(WARN, "iterate partition error", K(ret));
  } else if (!partition_iter_.is_ready()) {
    TRANS_LOG(WARN, "ObPartitionIterator is not ready");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_SUCCESS != (ret = trans_result_info_stat_iter_.set_ready())) {
    TRANS_LOG(WARN, "ObTransLockStatIterator set ready error", K(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVTransResultInfo::get_next_trans_result_info_(ObTransResultInfoStat& trans_result_info)
{
  int ret = OB_SUCCESS;

  ObTransResultInfoStat cur_trans_result_info;
  ObPartitionKey partition;
  bool bool_ret = true;

  while (bool_ret && OB_SUCCESS == ret) {
    if (OB_ITER_END == (ret = trans_result_info_stat_iter_.get_next(cur_trans_result_info))) {
      if (OB_SUCCESS != (ret = partition_iter_.get_next(partition))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "ObPartitionIterator get next partition error", K(ret));
        }
      } else {
        trans_result_info_stat_iter_.reset();
        if (OB_SUCCESS !=
            (ret = trans_service_->iterate_trans_result_info_in_TRIM(partition, trans_result_info_stat_iter_))) {
          TRANS_LOG(WARN, "iterate transaction result info stat error", K(ret), K(partition));
        } else {
          // do nothing
        }
      }
    } else {
      bool_ret = false;
    }
  }

  if (OB_SUCC(ret)) {
    trans_result_info = cur_trans_result_info;
  }

  return ret;
}

int ObGVTransResultInfo::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;

  ObTransResultInfoStat trans_result_info;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare start to read error", K(ret), K(start_to_read_));
  } else if (OB_FAIL(get_next_trans_result_info_(trans_result_info))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "ObGVTransResultInfo iter error", K(ret));
    } else {
      // do nothing
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
          /*
        case OB_APP_MIN_COLUMN_ID:
          // tenant_id
          cur_row_.cells_[i].set_int(trans_result_info.get_tenant_id());
          break;*/
        case OB_APP_MIN_COLUMN_ID:
          // trans_id
          (void)trans_result_info.get_trans_id().to_string(trans_id_buffer_, OB_MIN_BUFFER_SIZE);
          cur_row_.cells_[i].set_varchar(trans_id_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_ip
          (void)trans_result_info.get_addr().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // svr_port
          cur_row_.cells_[i].set_int(trans_result_info.get_addr().get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // partition
          if (trans_result_info.get_partition().is_valid()) {
            (void)trans_result_info.get_partition().to_string(partition_buffer_, OB_MIN_BUFFER_SIZE);
            cur_row_.cells_[i].set_varchar(partition_buffer_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cur_row_.cells_[i].set_varchar(ObString("NULL"));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // state
          cur_row_.cells_[i].set_int(trans_result_info.get_state());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // commit version
          cur_row_.cells_[i].set_int(trans_result_info.get_commit_version());
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // min_log_id
          cur_row_.cells_[i].set_int(trans_result_info.get_min_log_id());
          break;
      }
    }  // for
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

}  // namespace observer
}  // namespace oceanbase
