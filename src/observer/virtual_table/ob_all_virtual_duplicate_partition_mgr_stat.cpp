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

#include "observer/virtual_table/ob_all_virtual_duplicate_partition_mgr_stat.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_group.h"

namespace oceanbase {
using namespace common;
using namespace transaction;

namespace observer {
void ObGVDuplicatePartitionMgrStat::reset()
{
  ip_buffer_[0] = '\0';
  dup_table_lease_info_buffer_[0] = '\0';
  ObVirtualTableScannerIterator::reset();
}

void ObGVDuplicatePartitionMgrStat::destroy()
{
  trans_service_ = NULL;
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(dup_table_lease_info_buffer_, 0, OB_MAX_BUFFER_SIZE);

  ObVirtualTableScannerIterator::reset();
}

int ObGVDuplicatePartitionMgrStat::prepare_start_to_read_()
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
  } else if (OB_SUCCESS != (ret = duplicate_partition_stat_iter_.set_ready())) {
    TRANS_LOG(WARN, "ObDuplicatePartitionStatIterator set ready error", K(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVDuplicatePartitionMgrStat::get_next_duplicate_partition_mgr_stat_(ObDuplicatePartitionStat& stat)
{
  int ret = OB_SUCCESS;
  ObDuplicatePartitionStat cur_stat;
  ObPartitionKey partition;
  bool bool_ret = true;

  while (bool_ret && OB_SUCCESS == ret) {
    if (OB_ITER_END == (ret = duplicate_partition_stat_iter_.get_next(cur_stat))) {
      if (OB_SUCCESS != (ret = partition_iter_.get_next(partition))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "ObPartitionIterator get next partition error", K(ret));
        }
      } else {
        duplicate_partition_stat_iter_.reset();
        if (OB_SUCCESS !=
            (ret = trans_service_->iterate_duplicate_partition_stat(partition, duplicate_partition_stat_iter_))) {
          TRANS_LOG(WARN, "iterate duplicate partition stat error", K(ret), K(partition));
        } else {
          // do nothing
        }
      }
    } else {
      bool_ret = false;
    }
  }

  if (OB_SUCC(ret)) {
    stat = cur_stat;
  }

  return ret;
}

int ObGVDuplicatePartitionMgrStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObDuplicatePartitionStat duplicate_partition_stat;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare start to read error", K(ret), K(start_to_read_));
  } else if (OB_FAIL(get_next_duplicate_partition_mgr_stat_(duplicate_partition_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "ObGVDuplicatePartitionMgrStat iter error", K(ret));
    } else {
      // do nothing
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // tenant_id
          cur_row_.cells_[i].set_int(duplicate_partition_stat.get_partition().get_tenant_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_ip
          (void)duplicate_partition_stat.get_addr().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // svr_port
          cur_row_.cells_[i].set_int(duplicate_partition_stat.get_addr().get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // table_id
          cur_row_.cells_[i].set_int(duplicate_partition_stat.get_partition().get_table_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // partition id
          cur_row_.cells_[i].set_int(duplicate_partition_stat.get_partition().get_partition_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // partition lease list
          (void)duplicate_partition_stat.get_lease_list().to_string(dup_table_lease_info_buffer_, OB_MAX_BUFFER_SIZE);
          cur_row_.cells_[i].set_varchar(dup_table_lease_info_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // is master
          cur_row_.cells_[i].set_int(duplicate_partition_stat.is_master() ? 1 : 0);
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // cur log id
          cur_row_.cells_[i].set_int(duplicate_partition_stat.get_cur_log_id());
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

}  // namespace observer

}  // namespace oceanbase
