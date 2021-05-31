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
#include "share/config/ob_server_config.h"
#include "storage/ob_partition_group.h"
#include "ob_all_virtual_election_info.h"

namespace oceanbase {
using namespace common;
using namespace election;
namespace observer {
void ObGVElectionInfo::reset()
{
  ip_buffer_[0] = '\0';
  current_leader_ip_port_buffer_[0] = '\0';
  previous_leader_ip_port_buffer_[0] = '\0';
  proposal_leader_ip_port_buffer_[0] = '\0';
  member_list_buffer_[0] = '\0';
  ObVirtualTableScannerIterator::reset();
}

void ObGVElectionInfo::destroy()
{
  election_mgr_ = NULL;
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(current_leader_ip_port_buffer_, 0, common::OB_IP_PORT_STR_BUFF);
  memset(previous_leader_ip_port_buffer_, 0, common::OB_IP_PORT_STR_BUFF);
  memset(proposal_leader_ip_port_buffer_, 0, common::OB_IP_PORT_STR_BUFF);
  memset(member_list_buffer_, 0, common::MAX_MEMBER_LIST_LENGTH);
  ObVirtualTableScannerIterator::reset();
}

int ObGVElectionInfo::prepare_to_read_()
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;
  if (NULL == allocator_ || NULL == election_mgr_) {
    SERVER_LOG(WARN, "invalid argument", KP_(allocator), KP_(election_mgr));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_SUCCESS != (ret = election_mgr_->iterate_election_info(election_info_iter_))) {
    SERVER_LOG(WARN, "iterate election info error", K(ret));
  } else if (OB_SUCCESS != (ret = election_info_iter_.set_ready())) {
    SERVER_LOG(WARN, "iterate election info begin error", K(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVElectionInfo::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObElectionInfo election_info;
  const int64_t election_blacklist_interval = ObServerConfig::get_instance().election_blacklist_interval;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_to_read_())) {
    SERVER_LOG(WARN, "prepare to read error.", K(ret), K_(start_to_read));
  } else if (OB_SUCCESS != (ret = election_info_iter_.get_next(election_info))) {
    if (OB_ITER_END == ret) {
      SERVER_LOG(DEBUG, "iterate election info iter end", K(ret));
    } else {
      SERVER_LOG(WARN, "get next election info error.", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      int64_t remaining_ts =
          election_blacklist_interval - (ObTimeUtility::current_time() - election_info.get_last_leader_revoke_ts());
      if (remaining_ts < 0) {
        remaining_ts = 0;
      }
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          (void)election_info.get_self().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(election_info.get_self().get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // table_id
          cur_row_.cells_[i].set_int(election_info.get_table_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // partition_idx
          cur_row_.cells_[i].set_int(election_info.get_partition_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // is_running
          cur_row_.cells_[i].set_int(election_info.is_running());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // is_changing_leader
          cur_row_.cells_[i].set_int(election_info.is_changing_leader());
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // current_leader
          (void)election_info.get_current_leader().to_string(current_leader_ip_port_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(current_leader_ip_port_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // previous_leader
          (void)election_info.get_previous_leader().ip_to_string(
              previous_leader_ip_port_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(previous_leader_ip_port_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // proposal_leader
          (void)election_info.get_proposal_leader().ip_to_string(
              proposal_leader_ip_port_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(proposal_leader_ip_port_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // member_list
          election_info.get_member_list().to_string(member_list_buffer_, common::MAX_MEMBER_LIST_LENGTH);
          cur_row_.cells_[i].set_varchar(member_list_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // replica_num
          cur_row_.cells_[i].set_int(election_info.get_replica_num());
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // lease_start
          cur_row_.cells_[i].set_int(election_info.get_lease_start());
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // lease_end
          cur_row_.cells_[i].set_int(election_info.get_lease_end());
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // time_offset
          cur_row_.cells_[i].set_int(election_info.get_time_offset());
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // active_timestamp
          cur_row_.cells_[i].set_int(election_info.get_active_timestamp());
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          // T1_timestamp
          cur_row_.cells_[i].set_int(election_info.get_T1_timestamp());
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          // leader_epoch
          cur_row_.cells_[i].set_int(election_info.get_leader_epoch());
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          // state
          cur_row_.cells_[i].set_int(election_info.get_state());
          break;
        case OB_APP_MIN_COLUMN_ID + 18:
          // role
          cur_row_.cells_[i].set_int(election_info.get_role());
          break;
        case OB_APP_MIN_COLUMN_ID + 19:
          // stage
          cur_row_.cells_[i].set_int(election_info.get_stage());
          break;
        case OB_APP_MIN_COLUMN_ID + 20:
          // eg_id hash
          cur_row_.cells_[i].set_uint64(election_info.get_eg_id_hash());
          break;
        case OB_APP_MIN_COLUMN_ID + 21:
          // remaining_time_in_blacklist
          cur_row_.cells_[i].set_int(remaining_ts);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "invalid coloum_id", K(ret), K(col_id));
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
