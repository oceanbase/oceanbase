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

#include "ob_all_virtual_clog_stat.h"
#include "clog/ob_log_virtual_stat.h"
#include "clog/ob_partition_log_service.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_mgr.h"

namespace oceanbase {
namespace observer {
ObAllVirtualClogStat::ObAllVirtualClogStat(storage::ObPartitionService* partition_service)
    : partition_service_(partition_service), partition_iter_(NULL)
{
  if (NULL == (partition_iter_ = partition_service_->alloc_pg_iter())) {
    SERVER_LOG(WARN, "alloc_scan_iter fail", KP(partition_iter_));
  }
}

ObAllVirtualClogStat::~ObAllVirtualClogStat()
{
  destroy();
}

void ObAllVirtualClogStat::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(finish_get_clog_stat_())) {
    CLOG_LOG(WARN, "finish_get_clog_stat_ failed", K(ret));
  } else {
    // do nothing
  }
}

int ObAllVirtualClogStat::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  storage::ObIPartitionGroup* partition = NULL;
  clog::ObIPartitionLogService* pls = NULL;
  clog::ObClogVirtualStat* clog_stat = NULL;
  if (NULL == partition_service_ || NULL == allocator_ || NULL == partition_iter_) {
    ret = OB_NOT_INIT;
  } else {
    if (!start_to_read_ && OB_FAIL(prepare_get_clog_stat_())) {
      CLOG_LOG(WARN, "prepare_get_clog_stat_ fail", K(ret));
    } else if (OB_FAIL(partition_iter_->get_next(partition))) {
      if (OB_ITER_END == ret) {
        SERVER_LOG(INFO, "get_clog_stat finish", K(ret));
      } else {
        SERVER_LOG(ERROR, "get_clog_stat error", K(ret));
      }
    } else if (NULL == partition) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "get partition failed", K(ret));
    } else {
      common::ObPartitionKey partition_key = partition->get_partition_key();
      if (NULL == (pls = partition->get_log_service())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "get_log_service failed", K(ret), K(partition_key));
      } else if (NULL == (clog_stat = pls->alloc_clog_virtual_stat())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "get_clog_virtual_stat fail", K(ret), K(partition_key));
      } else {
        const int64_t col_count = output_column_ids_.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
          uint64_t col_id = output_column_ids_.at(i);
          switch (col_id) {
            case OB_APP_MIN_COLUMN_ID: {
              memset(server_ip_buff_, 0, common::OB_IP_STR_BUFF);
              if (OB_FAIL(clog_stat->get_server_ip(server_ip_buff_, common::OB_IP_STR_BUFF))) {
                SERVER_LOG(WARN, "get_server_ip fail", K(ret), K(partition_key));
              } else {
                cur_row_.cells_[i].set_varchar(ObString::make_string(server_ip_buff_));
                cur_row_.cells_[i].set_collation_type(
                    ObCharset::get_default_collation(ObCharset::get_default_charset()));
              }
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 1: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(clog_stat->get_port()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 2: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(clog_stat->get_table_id()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 3: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(clog_stat->get_partition_idx()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 4: {
              cur_row_.cells_[i].set_int(static_cast<int64_t>(clog_stat->get_partition_cnt()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 5: {
              cur_row_.cells_[i].set_varchar(ObString::make_string(clog_stat->get_replicate_role()));
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 6: {
              cur_row_.cells_[i].set_varchar(ObString::make_string(clog_stat->get_replicate_state()));
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 7: {
              clog_stat->get_leader().to_string(leader_buff_, sizeof(leader_buff_));
              cur_row_.cells_[i].set_varchar(ObString::make_string(leader_buff_));
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 8: {
              cur_row_.cells_[i].set_uint64(clog_stat->get_last_index_log_id());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 9: {
              int64_t start_timestamp = OB_INVALID_TIMESTAMP;
              if (OB_FAIL(clog_stat->get_last_index_log_timestamp(start_timestamp))) {
                SERVER_LOG(WARN, "get_sw_start_log_timestamp failed", K(ret), K(partition_key));
              } else {
                cur_row_.cells_[i].set_timestamp(start_timestamp);
              }
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 10: {
              cur_row_.cells_[i].set_uint64(clog_stat->get_max_log_id());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 11: {
              clog_stat->get_freeze_version().to_string(freeze_version_buff_, sizeof(freeze_version_buff_));
              cur_row_.cells_[i].set_varchar(ObString::make_string(freeze_version_buff_));
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 12: {
              clog_stat->get_curr_member_list().to_string(member_list_buff_, sizeof(member_list_buff_));
              cur_row_.cells_[i].set_varchar(ObString::make_string(member_list_buff_));
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 13: {
              cur_row_.cells_[i].set_uint64(clog_stat->get_member_ship_log_id());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 14: {
              cur_row_.cells_[i].set_bool(clog_stat->is_offline());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 15: {
              cur_row_.cells_[i].set_bool(clog_stat->is_in_sync());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 16: {
              cur_row_.cells_[i].set_uint64(clog_stat->get_start_log_id());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 17: {
              clog_stat->get_parent().to_string(parent_buff_, sizeof(parent_buff_));
              cur_row_.cells_[i].set_varchar(ObString::make_string(parent_buff_));
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 18: {
              clog_stat->get_children_list().to_string(children_list_buff_, sizeof(children_list_buff_));
              cur_row_.cells_[i].set_varchar(ObString::make_string(children_list_buff_));
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 19: {
              cur_row_.cells_[i].set_uint64(0);  // Deprecated column
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 20: {
              cur_row_.cells_[i].set_uint64(0);  // Deprecated column
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 21: {
              cur_row_.cells_[i].set_int(clog_stat->get_replica_type());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 22: {
              cur_row_.cells_[i].set_bool(clog_stat->allow_gc());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 23: {
              cur_row_.cells_[i].set_int(static_cast<int32_t>(clog_stat->get_quorum()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 24: {
              cur_row_.cells_[i].set_bool(clog_stat->is_need_rebuild());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 25: {
              cur_row_.cells_[i].set_uint64(clog_stat->get_next_replay_ts_delta());
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        }
      }
      if (NULL != clog_stat && OB_SUCCESS != (tmp_ret = pls->revert_clog_virtual_stat(clog_stat))) {
        SERVER_LOG(WARN, "revert_clog_virtual_stat fail", K(tmp_ret), KP(clog_stat));
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualClogStat::prepare_get_clog_stat_()
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

int ObAllVirtualClogStat::finish_get_clog_stat_()
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
