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

#include "observer/ob_server.h"
#include "ob_all_virtual_election_event_history.h"

namespace oceanbase {
using namespace common;
using namespace election;
namespace observer {
void ObGVElectionEventHistory::reset()
{
  ip_buffer_[0] = '\0';
  event_buffer_[0] = '\0';
  info_buffer_[0] = '\0';
  current_leader_ip_port_buffer_[0] = '\0';
  event_history_iter_.reset();
  ObVirtualTableScannerIterator::reset();
}

void ObGVElectionEventHistory::destroy()
{
  election_mgr_ = NULL;
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(event_buffer_, 0, sizeof(event_buffer_));
  memset(info_buffer_, 0, sizeof(info_buffer_));
  memset(current_leader_ip_port_buffer_, 0, common::OB_IP_PORT_STR_BUFF);
  ObVirtualTableScannerIterator::reset();
}

int ObGVElectionEventHistory::prepare_to_read_()
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;
  if (NULL == allocator_ || NULL == election_mgr_) {
    SERVER_LOG(WARN, "invalid argument", KP_(allocator), KP_(election_mgr));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_SUCCESS != (ret = election_mgr_->iterate_election_event_history(event_history_iter_))) {
    SERVER_LOG(WARN, "iterate election event history error", K(ret));
  } else if (OB_SUCCESS != (ret = event_history_iter_.set_ready())) {
    SERVER_LOG(WARN, "iterate election event history begin error", K(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVElectionEventHistory::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObElectionEventHistory event_history;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_to_read_())) {
    SERVER_LOG(WARN, "prepare to read error.", K(ret), K_(start_to_read));
  } else if (OB_SUCCESS != (ret = event_history_iter_.get_next(event_history))) {
    if (OB_ITER_END == ret) {
      SERVER_LOG(DEBUG, "iterate election event history iter end", K(ret));
    } else {
      SERVER_LOG(WARN, "get next election event history error.", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          cur_row_.cells_[i].set_timestamp(event_history.get_timestamp());
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_ip
          (void)event_history.get_addr().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // svr_port
          cur_row_.cells_[i].set_int(event_history.get_addr().get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // table_id
          cur_row_.cells_[i].set_int(event_history.get_table_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // partition_idx
          cur_row_.cells_[i].set_int(event_history.get_partition_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          strncpy(event_buffer_, event_history.get_event_name_cstr(), sizeof(event_buffer_) - 1);
          event_buffer_[sizeof(event_buffer_) - 1] = '\0';
          cur_row_.cells_[i].set_varchar(event_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // current_leader
          (void)event_history.get_current_leader().to_string(
              current_leader_ip_port_buffer_, common::OB_IP_PORT_STR_BUFF);
          cur_row_.cells_[i].set_varchar(current_leader_ip_port_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          strncpy(info_buffer_, event_history.get_event_info_cstr(), sizeof(info_buffer_) - 1);
          info_buffer_[sizeof(info_buffer_) - 1] = '\0';
          cur_row_.cells_[i].set_varchar(info_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "invalid coloum_id", K(ret), K(col_id));
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
