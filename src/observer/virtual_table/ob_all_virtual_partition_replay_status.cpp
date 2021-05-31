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

#include "observer/virtual_table/ob_all_virtual_partition_replay_status.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_group.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase {
namespace observer {

ObAllVirtualPartitionReplayStatus::ObAllVirtualPartitionReplayStatus()
    : ObVirtualTableScannerIterator(), ps_(NULL), addr_(), ptt_iter_(NULL)
{}

ObAllVirtualPartitionReplayStatus::~ObAllVirtualPartitionReplayStatus()
{
  reset();
}

void ObAllVirtualPartitionReplayStatus::reset()
{
  if (NULL != ptt_iter_) {
    if (NULL == ps_) {
      SERVER_LOG(ERROR, "partition_service_ is null");
    } else {
      ps_->revert_pg_iter(ptt_iter_);
      ptt_iter_ = NULL;
    }
  }
  addr_.reset();
  ps_ = NULL;
  ip_buf_[0] = '\0';
  post_barrier_status_[0] = '\0';
  last_replay_log_type_[0] = '\0';
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualPartitionReplayStatus::get_post_barrier_status(int64_t post_barrier_status, char* buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const char* status = NULL;

  switch (post_barrier_status) {
    case POST_BARRIER_SUBMITTED:
      status = "POST_BARRIER_SUBMITTED";
      break;
    case POST_BARRIER_FINISHED:
      status = "POST_BARRIER_FINISHED";
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      break;
  }

  if (OB_SUCCESS == ret && NULL != buf && buf_len > 0) {
    if (0 > snprintf(buf, buf_len, status)) {
      SERVER_LOG(WARN, "string copy failed", K(status), K(ret));
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

int ObAllVirtualPartitionReplayStatus::get_last_replay_log_type(
    int64_t last_replay_log_type, char* buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const char* status = ObStorageLogTypeToString::storage_log_type_to_string(last_replay_log_type);
  if (OB_SUCCESS == ret && NULL != buf && buf_len > 0) {
    if (0 > snprintf(buf, buf_len, status)) {
      SERVER_LOG(WARN, "string copy failed", K(status), K(ret));
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObAllVirtualPartitionReplayStatus::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  ObReplayStatus* replay_status = NULL;

  if (NULL == allocator_ || NULL == ps_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or partition_service_ shouldn't be NULL", K(allocator_), K(ps_), K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells of cur row is NULL", K(ret));
  } else if (NULL == ptt_iter_ && NULL == (ptt_iter_ = ps_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "fail to alloc partition iter", K(ret));
  } else if (OB_FAIL(ptt_iter_->get_next(partition))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "scan next partition failed", K(ret));
    }
  } else if (NULL == partition) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "get partition failed", K(ret));
  } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "get replay status failed", K(partition), K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    ObPartitionKey pkey = partition->get_partition_key();

    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            SERVER_LOG(WARN, "ip_to_string failed", K(addr_));
          }
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // tenant_id
          cur_row_.cells_[i].set_int(extract_tenant_id(pkey.table_id_));
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // pure_table_id
          cur_row_.cells_[i].set_int(pkey.table_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // parition_idx
          cur_row_.cells_[i].set_int(pkey.get_partition_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // partition_cnt
          cur_row_.cells_[i].set_int(pkey.get_partition_cnt());
          break;
        // pending_task_count
        case OB_APP_MIN_COLUMN_ID + 6:
          cur_row_.cells_[i].set_int(replay_status->get_pending_task_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          cur_row_.cells_[i].set_int(replay_status->get_retried_task_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          if (OB_FAIL(get_post_barrier_status(
                  replay_status->get_post_barrier_status(), post_barrier_status_, sizeof(post_barrier_status_)))) {
            SERVER_LOG(WARN, "convert freeze_submit_status to string failed", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(post_barrier_status_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          cur_row_.cells_[i].set_int(replay_status->is_enabled());
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          cur_row_.cells_[i].set_uint64(partition->get_log_service()->get_next_index_log_id() - 1);
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          cur_row_.cells_[i].set_uint64(replay_status->get_last_task_log_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 12: {
          // last_replay_log_type
          int ret_get = OB_SUCCESS;
          if (OB_SUCCESS !=
              (ret_get = get_last_replay_log_type(
                   replay_status->get_last_task_log_type(), last_replay_log_type_, sizeof(last_replay_log_type_)))) {
            ret = ret_get;
            SERVER_LOG(WARN, "convert last replay log type to string failed", K(ret), K(ret_get));
          } else {
            cur_row_.cells_[i].set_varchar(last_replay_log_type_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 13:
          cur_row_.cells_[i].set_int(replay_status->get_total_submitted_task_num());
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          cur_row_.cells_[i].set_int(replay_status->get_total_replayed_task_num());
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          cur_row_.cells_[i].set_uint64(replay_status->get_next_submit_log_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          cur_row_.cells_[i].set_int(replay_status->get_next_submit_log_ts());
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          cur_row_.cells_[i].set_uint64(replay_status->get_last_slide_out_log_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 18:
          cur_row_.cells_[i].set_int(replay_status->get_last_slide_out_log_ts());
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  } else {
    // revert ptt_iter_ no matter ret is OB_ITER_END or other errors
    if (NULL != ps_ && NULL != ptt_iter_) {
      ps_->revert_pg_iter(ptt_iter_);
      ptt_iter_ = NULL;
    }
  }

  return ret;
}

}  // namespace observer
}  // namespace oceanbase
