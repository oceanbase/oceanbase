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

#include "ob_trans_event.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/lock/ob_spin_lock.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
using namespace common;

namespace transaction
{

void ObTransStatItem::add(const int64_t value)
{
  bool bool_ret = true;
  const int64_t now = ObClockGenerator::getClock();

  if (value <= 0) {
    TRANS_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid argument", K(value)) ;
  } else {
    while (bool_ret) {
      bool need_print = false;
      {
        ObLockGuard<ObSpinLock> lock_guard(lock_);
        if (now - last_time_ < STAT_INTERVAL) {
          total_ += value;
          bool_ret = false;
        } else if (now - last_time_ >= STAT_INTERVAL * STAT_BUFFER_COUNT) {
          last_time_ = (now / 1000000 / 60) * 1000000 * 60;
          stat_idx_ = 0;
          save_ = total_;
        } else {
          last_time_ += STAT_INTERVAL;
          stat_buf_[stat_idx_++] = total_ - save_;
          save_ = total_;
          if (STAT_BUFFER_COUNT <= stat_idx_) {
            stat_idx_ = 0;
            need_print = true;
          }
        }
      }
      if (need_print) {
        TRANS_LOG(INFO, "transaction statistics", K_(item_name), "value", *this);
      }
    }
  }
}

void ObTransStatItem::reset()
{
  for (int64_t i = 0; i < STAT_BUFFER_COUNT; i++) {
    stat_buf_[i] = 0;
  }
  total_ = 0;
  save_ = 0;
  stat_idx_ = 0;
  last_time_ = 0;
}


int64_t ObTransStatItem::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len , pos, "[");
  for (int64_t i = 0; i < STAT_BUFFER_COUNT; ++i) {
    (void)databuff_print_obj(buf, buf_len, pos, stat_buf_[i]);
    (void)databuff_printf(buf, buf_len , pos, ", ");
  }
  databuff_printf(buf, buf_len , pos, "]");

  return pos;
}

void ObTransStatistic::add_sys_trans_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  // EVENT_ADD(TRANS_SYSTEM_TRANS_COUNT, value);
  //sys_trans_count_stat_.add(value);
}

void ObTransStatistic::add_user_trans_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_USER_TRANS_COUNT, value);
  //user_trans_count_stat_.add(value);
}

void ObTransStatistic::add_commit_trans_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_COMMIT_COUNT, value);
}

void ObTransStatistic::add_rollback_trans_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_ROLLBACK_COUNT, value);
}

void ObTransStatistic::add_trans_start_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_START_COUNT, value);
}
void ObTransStatistic::add_trans_timeout_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_TIMEOUT_COUNT, value);
}

void ObTransStatistic::add_trans_total_used_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_TOTAL_USED_TIME, value);
  //trans_total_used_time_stat_.add(value);
}

void ObTransStatistic::add_elr_enable_trans_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_ELR_ENABLE_COUNT, value);
}
void ObTransStatistic::add_elr_unable_trans_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_ELR_UNABLE_COUNT, value);
}
void ObTransStatistic::add_read_elr_row_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(READ_ELR_ROW_COUNT, value);
}

void ObTransStatistic::add_local_stmt_count(const uint64_t tenant_id, const int64_t value)
{
  UNUSED(value);
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_LOCAL_STMT_COUNT, value);
  //local_stmt_stat_.add(value);
}

void ObTransStatistic::add_remote_stmt_count(const uint64_t tenant_id, const int64_t value)
{
  UNUSED(value);
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_REMOTE_STMT_COUNT, value);
  //remote_stmt_stat_.add(value);
}

void ObTransStatistic::add_distributed_stmt_count(const uint64_t tenant_id, const int64_t value)
{
  UNUSED(value);
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_DISTRIBUTED_STMT_COUNT, value);
  //distributed_stmt_stat_.add(value);
}


void ObTransStatistic::add_clog_submit_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_COMMIT_LOG_SUBMIT_COUNT, value);
  //clog_submit_count_stat_.add(value);
}

void ObTransStatistic::add_clog_sync_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_COMMIT_LOG_SYNC_TIME, value);
  //clog_sync_time_stat_.add(value);
}

void ObTransStatistic::add_clog_sync_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_COMMIT_LOG_SYNC_COUNT, value);
  //clog_sync_count_stat_.add(value);
}

void ObTransStatistic::add_trans_commit_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_COMMIT_TIME, value);
  //trans_commit_time_stat_.add(value);
}

void ObTransStatistic::add_trans_rollback_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_ROLLBACK_TIME, value);
  //trans_rollback_time_stat_.add(value);
}

void ObTransStatistic::add_readonly_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_READONLY_COUNT, value);
}

void ObTransStatistic::add_local_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_LOCAL_COUNT, value);
}

void ObTransStatistic::add_dist_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_DIST_COUNT, value);
}

void ObTransStatistic::add_redo_log_replay_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(REDO_LOG_REPLAY_COUNT, value);
}

void ObTransStatistic::add_redo_log_replay_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(REDO_LOG_REPLAY_TIME, value);
}

void ObTransStatistic::add_prepare_log_replay_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(PREPARE_LOG_REPLAY_COUNT, value);
}

void ObTransStatistic::add_prepare_log_replay_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(PREPARE_LOG_REPLAY_TIME, value);
}

void ObTransStatistic::add_commit_log_replay_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(COMMIT_LOG_REPLAY_COUNT, value);
}

void ObTransStatistic::add_commit_log_replay_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(COMMIT_LOG_REPLAY_TIME, value);
}

void ObTransStatistic::add_abort_log_replay_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(ABORT_LOG_REPLAY_COUNT, value);
}

void ObTransStatistic::add_abort_log_replay_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(ABORT_LOG_REPLAY_TIME, value);
}

void ObTransStatistic::add_clear_log_replay_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(CLEAR_LOG_REPLAY_COUNT, value);
}

void ObTransStatistic::add_clear_log_replay_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(CLEAR_LOG_REPLAY_TIME, value);
}

void ObTransStatistic::add_sp_redo_log_cb_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(SP_REDO_LOG_CB_COUNT, value);
}

void ObTransStatistic::add_sp_redo_log_cb_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(SP_REDO_LOG_CB_TIME, value);
}

void ObTransStatistic::add_sp_commit_log_cb_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(SP_COMMIT_LOG_CB_COUNT, value);
}

void ObTransStatistic::add_sp_commit_log_cb_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(SP_COMMIT_LOG_CB_TIME, value);
}

void ObTransStatistic::add_redo_log_cb_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(REDO_LOG_CB_COUNT, value);
}

void ObTransStatistic::add_redo_log_cb_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(REDO_LOG_CB_TIME, value);
}

void ObTransStatistic::add_prepare_log_cb_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(PREPARE_LOG_CB_COUNT, value);
}

void ObTransStatistic::add_prepare_log_cb_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(PREPARE_LOG_CB_TIME, value);
}

void ObTransStatistic::add_commit_log_cb_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(COMMIT_LOG_CB_COUNT, value);
}

void ObTransStatistic::add_commit_log_cb_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(COMMIT_LOG_CB_TIME, value);
}

void ObTransStatistic::add_abort_log_cb_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(ABORT_LOG_CB_COUNT, value);
}

void ObTransStatistic::add_abort_log_cb_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(ABORT_LOG_CB_TIME, value);
}

void ObTransStatistic::add_clear_log_cb_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(CLEAR_LOG_CB_COUNT, value);
}

void ObTransStatistic::add_clear_log_cb_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(CLEAR_LOG_CB_TIME, value);
}

void ObTransStatistic::add_trans_callback_sql_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_CALLBACK_SQL_COUNT, value);
}

void ObTransStatistic::add_trans_callback_sql_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_CALLBACK_SQL_TIME, value);
}

void ObTransStatistic::add_trans_mt_end_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_MT_END_COUNT, value);
}

void ObTransStatistic::add_trans_mt_end_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_MT_END_TIME, value);
}

void ObTransStatistic::add_memstore_mutator_size(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(CLOG_MEMSTORE_MUTATOR_TOTAL_SIZE, value);
}
void ObTransStatistic::add_stmt_timeout_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_STRONG_CONSISTENCY_STMT_TIMEOUT_COUNT, value);
}
void ObTransStatistic::add_slave_read_stmt_timeout_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_SLAVE_READ_STMT_TIMEOUT_COUNT, value);
}
void ObTransStatistic::add_slave_read_stmt_retry_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_SLAVE_READ_STMT_RETRY_COUNT, value);
}
void ObTransStatistic::add_fill_redo_log_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_FILL_REDO_LOG_COUNT, value);
}
void ObTransStatistic::add_fill_redo_log_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_FILL_REDO_LOG_TIME, value);
}
void ObTransStatistic::add_submit_trans_log_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_SUBMIT_LOG_COUNT, value);
}
void ObTransStatistic::add_submit_trans_log_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_SUBMIT_LOG_TIME, value);
}
void ObTransStatistic::add_gts_request_total_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(GTS_REQUEST_TOTAL_COUNT, value);
}
void ObTransStatistic::add_gts_acquire_total_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(GTS_ACQUIRE_TOTAL_TIME, value);
}
void ObTransStatistic::add_gts_acquire_total_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(GTS_ACQUIRE_TOTAL_COUNT, value);
}
void ObTransStatistic::add_gts_acquire_total_wait_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(GTS_ACQUIRE_TOTAL_WAIT_COUNT, value);
}
void ObTransStatistic::add_gts_wait_elapse_total_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(GTS_WAIT_ELAPSE_TOTAL_TIME, value);
}
void ObTransStatistic::add_gts_wait_elapse_total_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(GTS_WAIT_ELAPSE_TOTAL_COUNT, value);
}
void ObTransStatistic::add_gts_wait_elapse_total_wait_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(GTS_WAIT_ELAPSE_TOTAL_WAIT_COUNT, value);
}
void ObTransStatistic::add_gts_rpc_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(GTS_RPC_COUNT, value);
}

void ObTransStatistic::add_gts_try_acquire_total_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(GTS_TRY_ACQUIRE_TOTAL_COUNT, value);
}

void ObTransStatistic::add_gts_try_wait_elapse_total_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(GTS_TRY_WAIT_ELAPSE_TOTAL_COUNT, value);
}

void ObTransStatistic::add_stmt_total_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_STMT_TOTAL_COUNT, value);
}

void ObTransStatistic::add_stmt_interval_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_STMT_INTERVAL_TIME, value);
}

void ObTransStatistic::add_trans_multi_partition_update_stmt_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_MULTI_PARTITION_UPDATE_STMT_COUNT, value);
}

void ObTransStatistic::add_batch_commit_trans_count(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  //EVENT_ADD(TRANS_BATCH_COMMIT_COUNT, value);
}

void ObTransStatistic::add_trans_log_total_size(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(CLOG_TRANS_LOG_TOTAL_SIZE, value);
}

void ObTransStatistic::add_local_trans_total_used_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_LOCAL_TOTAL_USED_TIME, value);
}
void ObTransStatistic::add_dist_trans_total_used_time(const uint64_t tenant_id, const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id);
  EVENT_ADD(TRANS_DIST_TOTAL_USED_TIME, value);
}

} // transaction
} // oceanbase
