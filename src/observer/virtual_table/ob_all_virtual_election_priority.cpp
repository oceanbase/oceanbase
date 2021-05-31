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

#include "ob_all_virtual_election_priority.h"
#include "clog/ob_partition_log_service.h"
#include "election/ob_election_priority.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
namespace observer {
ObAllVirtualElectionPriority::ObAllVirtualElectionPriority(storage::ObPartitionService* partition_service)
    : partition_service_(partition_service), partition_iter_(NULL)
{
  if (NULL == (partition_iter_ = partition_service_->alloc_pg_iter())) {
    SERVER_LOG(WARN, "alloc_scan_iter fail", KP(partition_iter_));
  }
}

ObAllVirtualElectionPriority::~ObAllVirtualElectionPriority()
{
  destroy();
}

void ObAllVirtualElectionPriority::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(finish_get_election_priority_())) {
    CLOG_LOG(WARN, "finish_get_election_priority_ failed", K(ret));
  } else {
    // do nothing
  }
}

int ObAllVirtualElectionPriority::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  while (OB_FAIL(inner_get_next_row_(row)) && OB_EAGAIN == ret) {
    // continue
  }
  return ret;
}

int ObAllVirtualElectionPriority::inner_get_next_row_(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroup* partition = NULL;
  clog::ObIPartitionLogService* pls = NULL;
  if (NULL == partition_service_ || NULL == allocator_ || NULL == partition_iter_) {
    ret = OB_NOT_INIT;
  } else {
    election::ObElectionPriority priority;
    clog::ObCandidateInfo candidate_info;
    if (!start_to_read_ && OB_FAIL(prepare_get_election_priority_())) {
      CLOG_LOG(WARN, "prepare_get_election_priority_ fail", K(ret));
    } else if (OB_FAIL(partition_iter_->get_next(partition))) {
      if (OB_ITER_END == ret) {
        SERVER_LOG(TRACE, "get_election_priority finish", K(ret));
      } else {
        SERVER_LOG(ERROR, "get_election_priority error", K(ret));
      }
    } else if (NULL == partition) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "get partition failed", K(ret));
    } else {
      common::ObPartitionKey partition_key = partition->get_partition_key();
      if (NULL == (pls = partition->get_log_service())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "get_log_service failed", K(ret), K(partition_key));
      } else if (OB_FAIL(pls->on_get_election_priority(priority))) {
        SERVER_LOG(TRACE, "on_get_election_priority failed", K(ret), K(partition_key));
      } else if (OB_FAIL(pls->get_candidate_info(candidate_info))) {
        SERVER_LOG(TRACE, "get_candidate_info failed", K(ret), K(partition_key));
      } else {
        const int64_t col_count = output_column_ids_.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
          uint64_t col_id = output_column_ids_.at(i);
          switch (col_id) {
            case OB_APP_MIN_COLUMN_ID: {
              memset(server_ip_buff_, 0, common::OB_IP_STR_BUFF);
              if (!addr_.ip_to_string(server_ip_buff_, common::OB_IP_STR_BUFF)) {
                SERVER_LOG(WARN, "ip_to_string failed");
              }
              cur_row_.cells_[i].set_varchar(ObString::make_string(server_ip_buff_));
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 1: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(addr_.get_port()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 2: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(partition_key.get_table_id()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 3: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(partition_key.get_partition_id()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 4: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(partition_key.get_partition_cnt()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 5: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(candidate_info.get_role()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 6: {
              cur_row_.cells_[i].set_bool(priority.is_candidate());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 7: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(priority.get_membership_version()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 8: {
              cur_row_.cells_[i].set_uint64(static_cast<uint64_t>(priority.get_log_id()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 9: {
              cur_row_.cells_[i].set_uint64(static_cast<uint64_t>(priority.get_locality()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 10: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(priority.get_system_score()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 11: {
              cur_row_.cells_[i].set_bool(candidate_info.is_tenant_active());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 12: {
              cur_row_.cells_[i].set_bool(candidate_info.on_revoke_blacklist());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 13: {
              cur_row_.cells_[i].set_bool(candidate_info.on_loop_blacklist());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 14: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(candidate_info.get_replica_type()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 15: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(candidate_info.get_server_status()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 16: {
              cur_row_.cells_[i].set_bool(candidate_info.is_clog_disk_full());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 17: {
              cur_row_.cells_[i].set_bool(candidate_info.is_offline());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 18: {
              cur_row_.cells_[i].set_bool(candidate_info.is_need_rebuild());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 19: {
              cur_row_.cells_[i].set_bool(candidate_info.is_partition_candidate());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 20: {
              cur_row_.cells_[i].set_bool(candidate_info.is_disk_error());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 21: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(candidate_info.get_memstore_percent()));
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              break;
            }
          }
        }
      }

      // For partitions in the migration process or read-only replica, the above operation may return errors,
      // in this case, should skip these errors and continue to iterate
      if (OB_FAIL(ret)) {
        ret = OB_EAGAIN;
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualElectionPriority::prepare_get_election_priority_()
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;
  if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualElectionPriority::finish_get_election_priority_()
{
  int ret = OB_SUCCESS;
  if (NULL == partition_iter_ || NULL == partition_service_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not need to revert", KP(partition_iter_), KP(partition_service_));
  } else {
    partition_service_->revert_pg_iter(partition_iter_);
  }
  partition_iter_ = NULL;
  partition_service_ = NULL;
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
