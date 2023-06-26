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

#include "ob_all_virtual_macro_block_marker_status.h"
#include "lib/utility/ob_print_utils.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace observer
{

ObAllVirtualMacroBlockMarkerStatus::ObAllVirtualMacroBlockMarkerStatus()
  : svr_ip_(),
    marker_status_(),
    is_end_(false)
{
}

ObAllVirtualMacroBlockMarkerStatus::~ObAllVirtualMacroBlockMarkerStatus()
{
}

int ObAllVirtualMacroBlockMarkerStatus::init (
    const blocksstable::ObMacroBlockMarkerStatus &marker_status)
{
  int ret = OB_SUCCESS;

  if (start_to_read_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else {
    marker_status.fill_comment(comment_, sizeof(comment_)); // ignore ret
    marker_status_ = marker_status;
    is_end_ = false;
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualMacroBlockMarkerStatus::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (!start_to_read_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else {
    const int64_t col_count = output_column_ids_.count();
    ObCollationType collcation_type = ObCharset::get_default_collation(
        ObCharset::get_default_charset());
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
      case OB_APP_MIN_COLUMN_ID: {
        // svr_ip
        if (!OBSERVER.get_self().ip_to_string(svr_ip_, sizeof(svr_ip_))) {
          ret = OB_BUF_NOT_ENOUGH;
          SERVER_LOG(WARN, "buffer not enough", K(ret));
        } else {
          cur_row_.cells_[i].set_varchar(svr_ip_);
          cur_row_.cells_[i].set_collation_type(collcation_type);
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 1: {
        // svr_port
        cur_row_.cells_[i].set_int(OBSERVER.get_self().get_port());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: {
        // total_block_count
        cur_row_.cells_[i].set_int(marker_status_.total_block_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: {
        // reserved_block_count
        cur_row_.cells_[i].set_int(marker_status_.reserved_block_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: {
        // meta_block_count
        cur_row_.cells_[i].set_int(marker_status_.linked_block_count_
                                 + marker_status_.ids_block_count_
                                 + marker_status_.index_block_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 5: {
        // shared_meta_block_count
        cur_row_.cells_[i].set_int(marker_status_.shared_meta_block_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 6: {
        // tmp_file_block_count
        cur_row_.cells_[i].set_int(marker_status_.tmp_file_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 7: {
        // data_block_count
        cur_row_.cells_[i].set_int(marker_status_.data_block_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 8: {
        // shared_data_block_count
        cur_row_.cells_[i].set_int(marker_status_.shared_data_block_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 9: {
        // disk_block_count_
        cur_row_.cells_[i].set_int(marker_status_.disk_block_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 10: {
        // bloomfilter_count_
        cur_row_.cells_[i].set_int(marker_status_.bloomfiter_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 11: {
        // hold_count_
        cur_row_.cells_[i].set_int(marker_status_.hold_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 12: {
        // pending_free_count_
        cur_row_.cells_[i].set_int(marker_status_.pending_free_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 13: {
        // free_count_
        cur_row_.cells_[i].set_int(marker_status_.free_count_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 14: {
        // mark_cost_time
        cur_row_.cells_[i].set_int(marker_status_.mark_cost_time_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 15: {
        // sweep_cost_time
        cur_row_.cells_[i].set_int(marker_status_.sweep_cost_time_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 16: {
        // start_marker_time
        cur_row_.cells_[i].set_timestamp(marker_status_.start_time_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 17: {
        // last_marker_end_time
        cur_row_.cells_[i].set_timestamp(marker_status_.last_end_time_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 18: {
        // whether finished marking
        cur_row_.cells_[i].set_bool(marker_status_.mark_finished_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 19: {
        // comment
        cur_row_.cells_[i].set_varchar(comment_);
        cur_row_.cells_[i].set_collation_type(collcation_type);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "invalid coloum_id", K(ret), K(col_id));
      }
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_end_ = true;
    row = &cur_row_;
  }
  return ret;
}

void ObAllVirtualMacroBlockMarkerStatus::reset()
{
  marker_status_.reuse();
  is_end_ = false;
}

} // observer
} // oceanbase
