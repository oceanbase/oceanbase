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

#include "observer/virtual_table/ob_all_virtual_trans_stat.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_group.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase {
namespace observer {
void ObGVTransStat::reset()
{
  ip_buffer_[0] = '\0';
  memstore_version_buffer_[0] = '\0';
  partition_buffer_[0] = '\0';
  participants_buffer_[0] = '\0';
  trans_id_buffer_[0] = '\0';
  proxy_sessid_buffer_[0] = '\0';
  prev_trans_info_buffer_[0] = '\0';
  next_trans_info_buffer_[0] = '\0';

  ObVirtualTableScannerIterator::reset();
}

void ObGVTransStat::destroy()
{
  trans_service_ = NULL;
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(memstore_version_buffer_, 0, common::MAX_VERSION_LENGTH);
  memset(partition_buffer_, 0, OB_MIN_BUFFER_SIZE);
  memset(participants_buffer_, 0, OB_MAX_BUFFER_SIZE);
  memset(trans_id_buffer_, 0, OB_MIN_BUFFER_SIZE);
  memset(proxy_sessid_buffer_, 0, OB_MIN_BUFFER_SIZE);
  memset(prev_trans_info_buffer_, 0, OB_MAX_BUFFER_SIZE);
  memset(next_trans_info_buffer_, 0, OB_MAX_BUFFER_SIZE);

  ObVirtualTableScannerIterator::reset();
}

int ObGVTransStat::prepare_start_to_read_()
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
  } else if (OB_SUCCESS != (ret = trans_stat_iter_.set_ready())) {  // set ready for the first count
    TRANS_LOG(WARN, "ObTransStatIterator set ready error", K(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVTransStat::get_next_trans_info_(ObTransStat& trans_stat)
{
  int ret = OB_SUCCESS;
  ObTransStat tmp_trans_stat;
  ObPartitionKey partition;
  bool bool_ret = true;

  while (bool_ret && OB_SUCCESS == ret) {
    if (OB_ITER_END == (ret = trans_stat_iter_.get_next(tmp_trans_stat))) {
      if (OB_SUCCESS != (ret = partition_iter_.get_next(partition))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "ObPartitionIterator get next partition error", K(ret));
        }
      } else {
        trans_stat_iter_.reset();
        if (OB_SUCCESS != (ret = trans_service_->iterate_trans_stat(partition, trans_stat_iter_))) {
          TRANS_LOG(WARN, "iterate transaction stat error", K(ret), K(partition));
        }
      }
    } else {
      bool_ret = false;
    }
  }

  if (OB_SUCC(ret)) {
    trans_stat = tmp_trans_stat;
  }

  return ret;
}

int ObGVTransStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObTransStat trans_stat;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare start to read error", K(ret), K(start_to_read_));
  } else if (OB_SUCCESS != (ret = get_next_trans_info_(trans_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "ObGVTransStat iter error", K(ret));
    } else {
      SERVER_LOG(DEBUG, "ObGVTransStat iter end success");
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID:
          cur_row_.cells_[i].set_int(trans_stat.get_tenant_id());
          break;
        case SVR_IP:
          (void)trans_stat.get_addr().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(trans_stat.get_addr().get_port());
          break;
        case INC_NUM:
          cur_row_.cells_[i].set_int(trans_stat.get_trans_id().get_inc_num());
          break;
        case SESSION_ID:
          cur_row_.cells_[i].set_int(trans_stat.get_session_id());
          break;
        case PROXY_ID:
          if (trans_stat.get_proxy_session_id() > 0) {
            ObAddr client_server;
            // parse client info
            (void)get_addr_by_proxy_sessid(trans_stat.get_proxy_session_id(), client_server);
            if (client_server.is_valid()) {
              client_server.to_string(proxy_sessid_buffer_, OB_MIN_BUFFER_SIZE);
              cur_row_.cells_[i].set_varchar(proxy_sessid_buffer_);
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            } else {
              cur_row_.cells_[i].set_varchar(ObString("NULL"));
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
          } else {
            cur_row_.cells_[i].set_varchar(ObString("NULL"));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case TRANS_TYPE:
          // trans_type : SP_TRANS(0), MIN_SP_TRANS(1), DIST_TRANS(2)
          cur_row_.cells_[i].set_int(trans_stat.get_trans_type());
          break;
        case TRANS_ID:
          (void)trans_stat.get_trans_id().to_string(trans_id_buffer_, OB_MIN_BUFFER_SIZE);
          cur_row_.cells_[i].set_varchar(trans_id_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case IS_EXITING:
          cur_row_.cells_[i].set_int(trans_stat.is_exiting() ? 1 : 0);
          break;
        case IS_READONLY:
          cur_row_.cells_[i].set_int(trans_stat.is_readonly() ? 1 : 0);
          break;
        case IS_DECIDED:
          cur_row_.cells_[i].set_int(trans_stat.has_decided() ? 1 : 0);
          break;
        case TRANS_MODE:
          cur_row_.cells_[i].set_varchar(trans_stat.is_dirty() ? ObString("delayed cleanout") : ObString("normal"));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case ACTIVE_MEMSTORE_VERSION:
          (void)trans_stat.get_active_memstore_version().version_to_string(
              memstore_version_buffer_, common::MAX_VERSION_LENGTH);
          cur_row_.cells_[i].set_varchar(memstore_version_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case PARTITION:
          if (trans_stat.get_partition().is_valid()) {
            (void)trans_stat.get_partition().to_string(partition_buffer_, OB_MIN_BUFFER_SIZE);
            cur_row_.cells_[i].set_varchar(partition_buffer_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cur_row_.cells_[i].set_varchar(ObString("NULL"));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case PARTICIPANTS:
          // if participants' count is equal to 0, then its value is NULL
          if (0 < trans_stat.get_participants().count()) {
            (void)trans_stat.get_participants().to_string(participants_buffer_, OB_MAX_BUFFER_SIZE);
            cur_row_.cells_[i].set_varchar(participants_buffer_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cur_row_.cells_[i].set_varchar(ObString("NULL"));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case AUTOCOMMIT:
          cur_row_.cells_[i].set_int(trans_stat.get_trans_param().is_autocommit());
          break;
        case TRANS_CONSISTENCY:
          cur_row_.cells_[i].set_int(trans_stat.get_trans_param().get_consistency_type());
          break;
        case CTX_CREATE_TIME:
          cur_row_.cells_[i].set_timestamp(trans_stat.get_ctx_create_time());
          // SERVER_LOG(INFO, "ctx_survive_time timestamp", "timstamp", trans_stat.get_ctx_create_time());
          break;
        case EXPIRED_TIME:
          cur_row_.cells_[i].set_timestamp(trans_stat.get_trans_expired_time());
          // SERVER_LOG(INFO, "expired_time timestamp:", "timestamp", trans_stat.get_ctx_create_time());
          break;
        case REFER:
          // ref for transaction context
          cur_row_.cells_[i].set_int(trans_stat.get_trans_refer_cnt());
          break;
        case SQL_NO:
          cur_row_.cells_[i].set_int(trans_stat.get_sql_no());
          break;
        case STATE:
          cur_row_.cells_[i].set_int(trans_stat.get_state());
          break;
        case PART_TRANS_ACTION:
          cur_row_.cells_[i].set_int(trans_stat.get_part_trans_action());
          break;
        case LOCK_FOR_READ_RETRY_COUNT:
          cur_row_.cells_[i].set_int(trans_stat.get_lock_for_read_retry_count());
          break;
        case CTX_ADDR:
          cur_row_.cells_[i].set_int(trans_stat.get_ctx_addr());
          break;
        case PREV_TRANS_ARR:
          if (0 < trans_stat.get_prev_trans_arr().count()) {
            (void)trans_stat.get_prev_trans_arr().to_string(prev_trans_info_buffer_, OB_MAX_BUFFER_SIZE);
            cur_row_.cells_[i].set_varchar(prev_trans_info_buffer_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cur_row_.cells_[i].set_varchar(ObString("NULL"));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case PREV_TRANS_COUNT:
          cur_row_.cells_[i].set_int(trans_stat.get_prev_trans_arr().count());
          break;
        case NEXT_TRANS_ARR:
          if (0 < trans_stat.get_next_trans_arr().count()) {
            (void)trans_stat.get_next_trans_arr().to_string(next_trans_info_buffer_, OB_MAX_BUFFER_SIZE);
            cur_row_.cells_[i].set_varchar(next_trans_info_buffer_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cur_row_.cells_[i].set_varchar(ObString("NULL"));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case NEXT_TRANS_COUNT:
          cur_row_.cells_[i].set_int(trans_stat.get_next_trans_arr().count());
          break;
        case PREV_TRANS_COMMIT_COUNT:
          cur_row_.cells_[i].set_int(trans_stat.get_prev_trans_commit_count());
          break;
        case CTX_ID:
          cur_row_.cells_[i].set_int(trans_stat.get_ctx_id());
          break;
        case PENDING_LOG_SIZE:
          cur_row_.cells_[i].set_int(trans_stat.get_pending_log_size());
          break;
        case FLUSHED_LOG_SIZE:
          cur_row_.cells_[i].set_int(trans_stat.get_flushed_log_size());
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid coloum_id", K(ret), K(col_id));
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
