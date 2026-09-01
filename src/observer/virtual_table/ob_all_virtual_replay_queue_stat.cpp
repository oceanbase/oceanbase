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

#include "ob_all_virtual_replay_queue_stat.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
namespace observer
{
int ObAllVirtualReplayQueueStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    auto func_iter_ls = [&](const logservice::ObReplayStatus &replay_status) -> int
    {
      int ret = OB_SUCCESS;
      const int64_t queue_size = replay_status.get_replay_queue_size();
      for (int64_t idx = 0; OB_SUCC(ret) && idx < queue_size; ++idx) {
        logservice::LSReplayQueueStat queue_stat;
        if (OB_FAIL(replay_status.get_queue_stat(idx, queue_stat))) {
          SERVER_LOG(WARN, "get_queue_stat failed", K(ret), K(idx));
        } else if (OB_FAIL(insert_stat_(queue_stat))) {
          SERVER_LOG(WARN, "insert stat failed", K(ret), K(idx));
        } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
          SERVER_LOG(WARN, "add row failed", KR(ret), K(queue_stat));
        }
      }
      return ret;
    };
    auto func_iterate_tenant = [&func_iter_ls]() -> int
    {
      int ret = OB_SUCCESS;
      logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
      if (NULL == log_service) {
        SERVER_LOG(INFO, "tenant has no ObLogService", K(MTL_ID()));
      } else if (OB_FAIL(log_service->iterate_replay(func_iter_ls))) {
        SERVER_LOG(WARN, "iter ls failed", K(ret));
      }
      return ret;
    };
    if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(func_iterate_tenant))) {
      SERVER_LOG(WARN, "iter tenant failed", K(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "get next row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualReplayQueueStat::insert_stat_(logservice::LSReplayQueueStat &queue_stat)
{
  int ret = OB_SUCCESS;
  const int64_t count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID:
        // tenant_id
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      case OB_APP_MIN_COLUMN_ID + 1:
        // ls_id
        cur_row_.cells_[i].set_int(queue_stat.ls_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 2:
        // svr_ip
        if (false == GCTX.self_addr().ip_to_string(ip_, common::OB_IP_PORT_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 3:
        // svr_port
        cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
        break;
      case OB_APP_MIN_COLUMN_ID + 4:
        // queue_idx
        cur_row_.cells_[i].set_int(queue_stat.queue_idx_);
        break;
      case OB_APP_MIN_COLUMN_ID + 5:
        // min_unreplayed_lsn
        cur_row_.cells_[i].set_uint64(queue_stat.min_unreplayed_lsn_.val_);
        break;
      case OB_APP_MIN_COLUMN_ID + 6:
        // min_unreplayed_scn
        cur_row_.cells_[i].set_uint64(queue_stat.min_unreplayed_scn_.get_val_for_inner_table_field());
        break;
      case OB_APP_MIN_COLUMN_ID + 7:
        // replay_hint
        cur_row_.cells_[i].set_int(queue_stat.replay_hint_);
        break;
      case OB_APP_MIN_COLUMN_ID + 8:
        // log_type
        if (OB_FAIL(log_base_type_to_string(queue_stat.log_type_, log_type_str_, sizeof(log_type_str_)))) {
          SERVER_LOG(WARN, "log_base_type_to_string failed", K(ret), K(queue_stat.log_type_));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(log_type_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 9:
        // first_handle_ts
        cur_row_.cells_[i].set_int(queue_stat.first_handle_ts_);
        break;
      case OB_APP_MIN_COLUMN_ID + 10:
        // last_handle_ts
        cur_row_.cells_[i].set_int(queue_stat.last_handle_ts_);
        break;
      case OB_APP_MIN_COLUMN_ID + 11:
        // retry_cost
        cur_row_.cells_[i].set_int(queue_stat.retry_cost_);
        break;
      case OB_APP_MIN_COLUMN_ID + 12:
        // task_count
        cur_row_.cells_[i].set_int(queue_stat.task_count_);
        break;
      case OB_APP_MIN_COLUMN_ID + 13:
        // pre_barrier_count
        cur_row_.cells_[i].set_int(queue_stat.pre_barrier_count_);
        break;
      case OB_APP_MIN_COLUMN_ID + 14:
        // err_ret_code
        cur_row_.cells_[i].set_int(queue_stat.err_ret_code_);
        break;
      case OB_APP_MIN_COLUMN_ID + 15:
        // has_fatal_error
        cur_row_.cells_[i].set_bool(queue_stat.has_fatal_error_);
        break;
      case OB_APP_MIN_COLUMN_ID + 16:
        // is_idle
        cur_row_.cells_[i].set_bool(queue_stat.is_idle_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unkown column");
        break;
    }
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase
