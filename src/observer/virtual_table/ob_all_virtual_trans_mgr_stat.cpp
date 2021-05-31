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

#include "observer/virtual_table/ob_all_virtual_trans_mgr_stat.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_group.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase {
namespace observer {
void ObGVTransPartitionMgrStat::reset()
{
  ip_buffer_[0] = '\0';
  memstore_version_buffer_[0] = '\0';

  ObVirtualTableScannerIterator::reset();
}

void ObGVTransPartitionMgrStat::destroy()
{
  trans_service_ = NULL;
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(memstore_version_buffer_, 0, common::MAX_VERSION_LENGTH);

  ObVirtualTableScannerIterator::reset();
}

int ObGVTransPartitionMgrStat::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;

  if (NULL == allocator_ || NULL == trans_service_) {
    SERVER_LOG(WARN,
        "invalid argument, allocator_ or trans_service_ is null",
        "allocator",
        OB_P(allocator_),
        "trans_service",
        OB_P(trans_service_));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_SUCCESS != (ret = trans_service_->iterate_partition_mgr_stat(trans_partition_mgr_stat_iter_))) {
    SERVER_LOG(WARN, "iterate partition manager error", K(ret));
  } else if (!trans_partition_mgr_stat_iter_.is_ready()) {
    SERVER_LOG(WARN, "ObPartitionMgrIterator is not ready");
    ret = OB_ERR_UNEXPECTED;
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVTransPartitionMgrStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObTransPartitionStat trans_partition_stat;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare start to read error", K(ret), K(start_to_read_));
  } else if (OB_SUCCESS != (ret = trans_partition_mgr_stat_iter_.get_next(trans_partition_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "ObTransPartitionMgrIterator get next error", K(ret));
    } else {
      SERVER_LOG(DEBUG, "ObTransPartitionMgrIterator iter end success");
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          (void)trans_partition_stat.get_addr().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(trans_partition_stat.get_addr().get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // table_id
          cur_row_.cells_[i].set_int(trans_partition_stat.get_partition().table_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // partition_id
          cur_row_.cells_[i].set_int(trans_partition_stat.get_partition().get_partition_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // ctx_type
          cur_row_.cells_[i].set_int(trans_partition_stat.get_ctx_type());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // is_master_
          cur_row_.cells_[i].set_int(trans_partition_stat.is_master() ? 1 : 0);
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // is_frozen_
          cur_row_.cells_[i].set_int(trans_partition_stat.is_frozen() ? 1 : 0);
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // is_stopped_
          cur_row_.cells_[i].set_int(trans_partition_stat.is_stopped() ? 1 : 0);
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // read_only_count_
          cur_row_.cells_[i].set_int(trans_partition_stat.get_read_only_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // active_read_write_cout_
          cur_row_.cells_[i].set_int(trans_partition_stat.get_active_read_write_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // active_memstore_version
          (void)trans_partition_stat.get_active_memstore_version().version_to_string(
              memstore_version_buffer_, common::MAX_VERSION_LENGTH);
          cur_row_.cells_[i].set_varchar(memstore_version_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // total_ctx_count
          cur_row_.cells_[i].set_int(trans_partition_stat.get_total_ctx_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // with_dep_trans_count
          cur_row_.cells_[i].set_int(trans_partition_stat.get_with_dependency_trx_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // without_dep_trans_count
          cur_row_.cells_[i].set_int(trans_partition_stat.get_without_dependency_trx_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // endtrans_by_prev_count
          cur_row_.cells_[i].set_int(trans_partition_stat.get_end_trans_by_prev_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          // endtrans_by_checkpoint_count
          cur_row_.cells_[i].set_int(trans_partition_stat.get_end_trans_by_checkpoint_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          // endtrans_by_self_count
          cur_row_.cells_[i].set_int(trans_partition_stat.get_end_trans_by_self_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          // mgr addr
          cur_row_.cells_[i].set_int(trans_partition_stat.get_mgr_addr());
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column_id", K(ret), K(col_id));
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
