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
#include "storage/ob_partition_group.h"
#include "ob_all_virtual_election_group_info.h"

namespace oceanbase {
using namespace common;
using namespace election;
namespace observer {
void ObGVElectionGroupInfo::reset()
{
  ip_buffer_[0] = '\0';
  current_leader_ip_port_buffer_[0] = '\0';
  previous_leader_ip_port_buffer_[0] = '\0';
  proposal_leader_ip_port_buffer_[0] = '\0';
  member_list_buffer_[0] = '\0';
  ObVirtualTableScannerIterator::reset();
}

void ObGVElectionGroupInfo::destroy()
{
  election_mgr_ = NULL;
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(current_leader_ip_port_buffer_, 0, common::OB_IP_PORT_STR_BUFF);
  memset(previous_leader_ip_port_buffer_, 0, common::OB_IP_PORT_STR_BUFF);
  memset(proposal_leader_ip_port_buffer_, 0, common::OB_IP_PORT_STR_BUFF);
  memset(member_list_buffer_, 0, common::MAX_MEMBER_LIST_LENGTH);
  ObVirtualTableScannerIterator::reset();
}

int ObGVElectionGroupInfo::prepare_to_read_()
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;
  if (NULL == allocator_ || NULL == election_mgr_) {
    SERVER_LOG(WARN, "invalid argument", KP_(allocator), KP_(election_mgr));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_SUCCESS != (ret = election_mgr_->iterate_election_group_info(election_info_iter_))) {
    SERVER_LOG(WARN, "iterate election info error", K(ret));
  } else if (OB_SUCCESS != (ret = election_info_iter_.set_ready())) {
    SERVER_LOG(WARN, "iterate election info begin error", K(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVElectionGroupInfo::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObElectionGroupInfo eg_info;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_to_read_())) {
    SERVER_LOG(WARN, "prepare to read error.", K(ret), K_(start_to_read));
  } else if (OB_SUCCESS != (ret = election_info_iter_.get_next(eg_info))) {
    if (OB_ITER_END == ret) {
      SERVER_LOG(DEBUG, "iterate election info iter end", K(ret));
    } else {
      SERVER_LOG(WARN, "get next election info error.", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          (void)eg_info.get_self().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(eg_info.get_self().get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // eg_id_hash
          cur_row_.cells_[i].set_uint64(eg_info.get_eg_id_hash());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // is_running
          cur_row_.cells_[i].set_bool(eg_info.is_running());
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // create_time
          cur_row_.cells_[i].set_int(eg_info.get_create_time());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // eg_version
          cur_row_.cells_[i].set_int(eg_info.get_eg_version());
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // eg_part_cnt
          cur_row_.cells_[i].set_int(eg_info.get_eg_part_cnt());
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // is_all_part_merged_in
          cur_row_.cells_[i].set_bool(eg_info.is_all_part_merged_in());
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // is_priority_allow_reappoint
          cur_row_.cells_[i].set_bool(eg_info.is_priority_allow_reappoint());
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // tenant_id
          cur_row_.cells_[i].set_int(static_cast<int64_t>(eg_info.get_tenant_id()));
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // is_candidate
          cur_row_.cells_[i].set_bool(eg_info.is_candidate());
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // system_score
          cur_row_.cells_[i].set_int(eg_info.get_system_score());
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // current_leader
          (void)eg_info.get_current_leader().ip_to_string(current_leader_ip_port_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(current_leader_ip_port_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // member_list
          eg_info.get_member_list().to_string(member_list_buffer_, common::MAX_MEMBER_LIST_LENGTH);
          cur_row_.cells_[i].set_varchar(member_list_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // replica_num
          cur_row_.cells_[i].set_int(eg_info.get_replica_num());
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          // takeover_t1_timestamp
          cur_row_.cells_[i].set_int(eg_info.get_takeover_t1_timestamp());
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          // T1_timestamp
          cur_row_.cells_[i].set_int(eg_info.get_T1_timestamp());
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          // lease_start
          cur_row_.cells_[i].set_int(eg_info.get_lease_start());
          break;
        case OB_APP_MIN_COLUMN_ID + 18:
          // lease_end
          cur_row_.cells_[i].set_int(eg_info.get_lease_end());
          break;
        case OB_APP_MIN_COLUMN_ID + 19:
          // role
          cur_row_.cells_[i].set_int(eg_info.get_role());
          break;
        case OB_APP_MIN_COLUMN_ID + 20:
          // state
          cur_row_.cells_[i].set_int(eg_info.get_state());
          break;
        case OB_APP_MIN_COLUMN_ID + 21:
          // pre_destroy_state
          cur_row_.cells_[i].set_bool(eg_info.is_pre_destroy_state());
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
