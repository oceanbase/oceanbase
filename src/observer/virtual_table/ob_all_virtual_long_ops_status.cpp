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
using namespace oceanbase::share;

ObAllVirtualLongOpsStatus::ObAllVirtualLongOpsStatus()
  : ObVirtualTableScannerIterator(),
    addr_(), longops_value_(), longops_iter_()
{
}

ObAllVirtualLongOpsStatus::~ObAllVirtualLongOpsStatus()
{
  reset();
}

void ObAllVirtualLongOpsStatus::reset()
{
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualLongOpsStatus::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (!longops_iter_.is_inited() && OB_FAIL(ObLongopsMgr::get_instance().begin_iter(longops_iter_))) {
    LOG_WARN("fail to begin longops iter", K(ret));
  } else if (OB_FAIL(longops_iter_.get_next(effective_tenant_id_, longops_value_))) {
    LOG_WARN("fail to get next longops value", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // tenant id
          cur_row_.cells_[i].set_int(longops_value_.tenant_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // sid
          cur_row_.cells_[i].set_int(longops_value_.sid_);
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // op_name
          cur_row_.cells_[i].set_varchar(longops_value_.op_name_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // target
          cur_row_.cells_[i].set_varchar(longops_value_.target_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // svr_ip
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to execute ip_to_string", K(ret));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // svr_port
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // start_time
          cur_row_.cells_[i].set_int(longops_value_.start_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // finish_time
          cur_row_.cells_[i].set_int(longops_value_.finish_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // elapsed_seconds
          cur_row_.cells_[i].set_int(longops_value_.elapsed_seconds_);
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // time_remaining
          cur_row_.cells_[i].set_int(longops_value_.time_remaining_);
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // last_update_time
          cur_row_.cells_[i].set_int(longops_value_.last_update_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // percentage
          cur_row_.cells_[i].set_int(longops_value_.percentage_);
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // message
          cur_row_.cells_[i].set_varchar(longops_value_.message_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 13: {
          // trace id
          int len = longops_value_.trace_id_.to_string(trace_id_, OB_MAX_TRACE_ID_BUFFER_SIZE);
          cur_row_.cells_[i].set_varchar(trace_id_, len);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid col_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}
