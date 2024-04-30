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

#define USING_LOG_PREFIX TRANS
#include "ob_xa_trans_event.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
using namespace common;

namespace transaction
{

int ObXATransStatistics::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "xa trans statistics init twice", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObXATransStatistics::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  last_print_stat_ts_ = 0;
  last_print_active_ts_ = 0;
  active_xa_stmt_count_ = 0;
  active_xa_ctx_count_ = 0;
  is_inited_ = false;
}

void ObXATransStatistics::inc_xa_start_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_START_TOTAL_COUNT, 1);
  ATOMIC_INC(&xa_start_count_);
}

void ObXATransStatistics::add_xa_start_total_used_time(const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_START_TOTAL_USED_TIME, value);
  ATOMIC_FAA(&xa_start_used_time_us_, value);
}

void ObXATransStatistics::inc_xa_start_remote_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_START_REMOTE_COUNT, 1);
  ATOMIC_INC(&xa_start_remote_count_);
}

void ObXATransStatistics::inc_xa_start_fail_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_START_FAIL_COUNT, 1);
  ATOMIC_INC(&xa_start_fail_count_);
}

void ObXATransStatistics::inc_xa_end_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_END_TOTAL_COUNT, 1);
  ATOMIC_INC(&xa_end_count_);
}

void ObXATransStatistics::add_xa_end_total_used_time(const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_END_TOTAL_USED_TIME, value);
  ATOMIC_FAA(&xa_end_used_time_us_, value);
}

void ObXATransStatistics::inc_xa_end_remote_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_END_REMOTE_COUNT, 1);
  ATOMIC_INC(&xa_end_remote_count_);
}

void ObXATransStatistics::inc_xa_end_fail_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_END_FAIL_COUNT, 1);
  ATOMIC_INC(&xa_end_fail_count_);
}

void ObXATransStatistics::inc_xa_prepare_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_PREPARE_TOTAL_COUNT, 1);
  ATOMIC_INC(&xa_prepare_count_);
}

void ObXATransStatistics::add_xa_prepare_total_used_time(const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_PREPARE_TOTAL_USED_TIME, value);
  ATOMIC_FAA(&xa_prepare_used_time_us_, value);
}

void ObXATransStatistics::inc_xa_prepare_remote_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_PREPARE_REMOTE_COUNT, 1);
  ATOMIC_INC(&xa_prepare_remote_count_);
}

void ObXATransStatistics::inc_xa_prepare_fail_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_PREPARE_FAIL_COUNT, 1);
  ATOMIC_INC(&xa_prepare_fail_count_);
}

void ObXATransStatistics::inc_xa_commit_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_COMMIT_TOTAL_COUNT, 1);
  ATOMIC_INC(&xa_commit_count_);
}

void ObXATransStatistics::add_xa_commit_total_used_time(const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_COMMIT_TOTAL_USED_TIME, value);
  ATOMIC_FAA(&xa_commit_used_time_us_, value);
}

void ObXATransStatistics::inc_xa_commit_remote_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_COMMIT_REMOTE_COUNT, 1);
  ATOMIC_INC(&xa_commit_remote_count_);
}

void ObXATransStatistics::inc_xa_commit_fail_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_COMMIT_FAIL_COUNT, 1);
  ATOMIC_INC(&xa_commit_fail_count_);
}

void ObXATransStatistics::inc_xa_rollback_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_ROLLBACK_TOTAL_COUNT, 1);
  ATOMIC_INC(&xa_rollback_count_);
}

void ObXATransStatistics::add_xa_rollback_total_used_time(const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_ROLLBACK_TOTAL_USED_TIME, value);
  ATOMIC_FAA(&xa_rollback_used_time_us_, value);
}

void ObXATransStatistics::inc_xa_rollback_remote_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_ROLLBACK_REMOTE_COUNT, 1);
  ATOMIC_INC(&xa_rollback_remote_count_);
}

void ObXATransStatistics::inc_xa_rollback_fail_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_ROLLBACK_FAIL_COUNT, 1);
  ATOMIC_INC(&xa_rollback_fail_count_);
}

void ObXATransStatistics::inc_xa_trans_start_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_TRANS_START_COUNT, 1);
  ATOMIC_INC(&xa_trans_start_count_);
}

void ObXATransStatistics::inc_xa_read_only_trans_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_READ_ONLY_TRANS_TOTAL_COUNT, 1);
  ATOMIC_INC(&xa_read_only_trans_count_);
}

void ObXATransStatistics::inc_xa_one_phase_commit_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_ONE_PHASE_COMMIT_TOTAL_COUNT, 1);
  ATOMIC_INC(&xa_one_phase_commit_count_);
}

void ObXATransStatistics::inc_xa_inner_sql_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_INNER_SQL_TOTAL_COUNT, 1);
  ATOMIC_INC(&xa_inner_sql_count_);
}

void ObXATransStatistics::add_xa_inner_sql_total_used_time(const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_INNER_SQL_TOTAL_USED_TIME, value);
  ATOMIC_FAA(&xa_inner_sql_used_time_us_, value);
}

void ObXATransStatistics::inc_xa_inner_rpc_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_INNER_RPC_TOTAL_COUNT, 1);
  ATOMIC_INC(&xa_inner_rpc_count_);
}

void ObXATransStatistics::add_xa_inner_rpc_total_used_time(const int64_t value)
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_INNER_RPC_TOTAL_USED_TIME, value);
  ATOMIC_FAA(&xa_inner_rpc_used_time_us_, value);
}

void ObXATransStatistics::inc_xa_inner_sql_ten_ms_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_INNER_SQL_TEN_MS_COUNT, 1);
  ATOMIC_INC(&xa_inner_sql_ten_ms_count_);
}

void ObXATransStatistics::inc_xa_inner_sql_twenty_ms_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_INNER_SQL_TWENTY_MS_COUNT, 1);
  ATOMIC_INC(&xa_inner_sql_twenty_ms_count_);
}

void ObXATransStatistics::inc_xa_inner_rpc_ten_ms_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_INNER_RPC_TEN_MS_COUNT, 1);
  ATOMIC_INC(&xa_inner_rpc_ten_ms_count_);
}

void ObXATransStatistics::inc_xa_inner_rpc_twenty_ms_total_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(XA_INNER_RPC_TWENTY_MS_COUNT, 1);
  ATOMIC_INC(&xa_inner_rpc_twenty_ms_count_);
}

void ObXATransStatistics::inc_active_xa_stmt_count()
{
  ATOMIC_INC(&active_xa_stmt_count_);
}

void ObXATransStatistics::dec_active_xa_stmt_count()
{
  ATOMIC_DEC(&active_xa_stmt_count_);
}

void ObXATransStatistics::inc_active_xa_ctx_count()
{
  ATOMIC_INC(&active_xa_ctx_count_);
}

void ObXATransStatistics::dec_active_xa_ctx_count()
{
  ATOMIC_DEC(&active_xa_ctx_count_);
}

void ObXATransStatistics::inc_compensate_record_count()
{
  ATOMIC_INC(&xa_compensate_record_count_);
}

void ObXATransStatistics::try_print_xa_statistics()
{
  static const int64_t STAT_INTERVAL = 9000000; // 9 seconds
  const int64_t current_ts = ObTimeUtility::current_time();
  const int64_t last_print_stat_ts = ATOMIC_LOAD(&last_print_stat_ts_);
  if (current_ts - last_print_stat_ts >= STAT_INTERVAL) {
    if (ATOMIC_BCAS(&last_print_stat_ts_, last_print_stat_ts, current_ts)) {
      int64_t xa_start_count = ATOMIC_LOAD(&xa_start_count_);
      int64_t xa_end_count = ATOMIC_LOAD(&xa_end_count_);
      int64_t xa_prepare_count = ATOMIC_LOAD(&xa_prepare_count_);
      int64_t xa_commit_count = ATOMIC_LOAD(&xa_commit_count_);
      int64_t xa_rollback_count = ATOMIC_LOAD(&xa_rollback_count_);
      int64_t xa_inner_sql_count = ATOMIC_LOAD(&xa_inner_sql_count_);
      if (0 != xa_inner_sql_count || 0 != xa_start_count || 0 != xa_end_count
          || 0 != xa_prepare_count || 0 != xa_commit_count || 0 != xa_rollback_count) {
        // for xa stmt
        FLOG_INFO("xa stmt statistics",
            "xa_start_count", xa_start_count,
            "xa_start_latency", (0 == xa_start_count) ? 0 : ATOMIC_LOAD(&xa_start_used_time_us_) / xa_start_count,
            "xa_start_remote_count", ATOMIC_LOAD(&xa_start_remote_count_),
            "xa_start_fail_count", ATOMIC_LOAD(&xa_start_fail_count_),
            "xa_end_count", xa_end_count,
            "xa_end_latency", (0 == xa_end_count) ? 0 : ATOMIC_LOAD(&xa_end_used_time_us_) / xa_end_count,
            "xa_end_remote_count", ATOMIC_LOAD(&xa_end_remote_count_),
            "xa_end_fail_count", ATOMIC_LOAD(&xa_end_fail_count_),
            "xa_prepare_count", xa_prepare_count,
            "xa_prepare_latency", (0 == xa_prepare_count) ? 0 : ATOMIC_LOAD(&xa_prepare_used_time_us_) / xa_prepare_count,
            "xa_prepare_remote_count", ATOMIC_LOAD(&xa_prepare_remote_count_),
            "xa_prepare_fail_count", ATOMIC_LOAD(&xa_prepare_fail_count_),
            "xa_commit_count", xa_commit_count,
            "xa_commit_latency", (0 == xa_commit_count) ? 0 : ATOMIC_LOAD(&xa_commit_used_time_us_) / xa_commit_count,
            "xa_commit_remote_count", ATOMIC_LOAD(&xa_commit_remote_count_),
            "xa_commit_fail_count", ATOMIC_LOAD(&xa_commit_fail_count_),
            "xa_rollback_count", xa_rollback_count,
            "xa_rollback_latency", (0 == xa_rollback_count) ? 0 : ATOMIC_LOAD(&xa_rollback_used_time_us_) / xa_rollback_count,
            "xa_rollback_remote_count", ATOMIC_LOAD(&xa_rollback_remote_count_),
            "xa_rollback_fail_count", ATOMIC_LOAD(&xa_rollback_fail_count_),
            K(last_print_stat_ts));
        // for xa trans
        xa_inner_sql_count = ATOMIC_LOAD(&xa_inner_sql_count_);
        int64_t xa_trans_start_count = ATOMIC_LOAD(&xa_trans_start_count_);
        int64_t xa_inner_rpc_count = ATOMIC_LOAD(&xa_inner_rpc_count_);
        FLOG_INFO("xa trans statistics",
            "xa_trans_start_count", xa_trans_start_count,
            "xa_read_only_trans_count", ATOMIC_LOAD(&xa_read_only_trans_count_),
            "xa_one_phase_commit_count", ATOMIC_LOAD(&xa_one_phase_commit_count_),
            "xa_inner_sql_count", xa_inner_sql_count,
            "xa_inner_sql_latency", (0 == xa_inner_sql_count) ? 0 : ATOMIC_LOAD(&xa_inner_sql_used_time_us_) / xa_inner_sql_count,
            "xa_inner_sql_ten_ms_count", ATOMIC_LOAD(&xa_inner_sql_ten_ms_count_),
            "xa_inner_sql_twenty_ms_count", ATOMIC_LOAD(&xa_inner_sql_twenty_ms_count_),
            "xa_inner_rpc_count", xa_inner_rpc_count,
            "xa_inner_rpc_latency", (0 == xa_inner_rpc_count) ? 0 : ATOMIC_LOAD(&xa_inner_rpc_used_time_us_) / xa_inner_rpc_count,
            "xa_inner_rpc_ten_ms_count", ATOMIC_LOAD(&xa_inner_rpc_ten_ms_count_),
            "xa_inner_rpc_twenty_ms_count", ATOMIC_LOAD(&xa_inner_rpc_twenty_ms_count_),
            "duration", (current_ts - last_print_stat_ts));
      }
      // for inner logic
      int64_t xa_compensate_record_count = ATOMIC_LOAD(&xa_compensate_record_count_);
      if (0 != xa_compensate_record_count) {
        FLOG_INFO("xa statistics of inner logic",
            "xa_compensate_record_count", xa_compensate_record_count);
      }
      // reset
      // NOTE that the active info should not be reset
      ATOMIC_STORE(&xa_start_count_, 0);
      ATOMIC_STORE(&xa_start_used_time_us_, 0);
      ATOMIC_STORE(&xa_start_remote_count_, 0);
      ATOMIC_STORE(&xa_start_fail_count_, 0);
      ATOMIC_STORE(&xa_end_count_, 0);
      ATOMIC_STORE(&xa_end_used_time_us_, 0);
      ATOMIC_STORE(&xa_end_remote_count_, 0);
      ATOMIC_STORE(&xa_end_fail_count_, 0);
      ATOMIC_STORE(&xa_prepare_count_, 0);
      ATOMIC_STORE(&xa_prepare_used_time_us_, 0);
      ATOMIC_STORE(&xa_prepare_remote_count_, 0);
      ATOMIC_STORE(&xa_prepare_fail_count_, 0);
      ATOMIC_STORE(&xa_commit_count_, 0);
      ATOMIC_STORE(&xa_commit_used_time_us_, 0);
      ATOMIC_STORE(&xa_commit_remote_count_, 0);
      ATOMIC_STORE(&xa_commit_fail_count_, 0);
      ATOMIC_STORE(&xa_rollback_count_, 0);
      ATOMIC_STORE(&xa_rollback_used_time_us_, 0);
      ATOMIC_STORE(&xa_rollback_remote_count_, 0);
      ATOMIC_STORE(&xa_rollback_fail_count_, 0);
      ATOMIC_STORE(&xa_trans_start_count_, 0);
      ATOMIC_STORE(&xa_read_only_trans_count_, 0);
      ATOMIC_STORE(&xa_one_phase_commit_count_, 0);
      ATOMIC_STORE(&xa_inner_sql_count_, 0);
      ATOMIC_STORE(&xa_inner_sql_used_time_us_, 0);
      ATOMIC_STORE(&xa_inner_rpc_count_, 0);
      ATOMIC_STORE(&xa_inner_rpc_used_time_us_, 0);
      ATOMIC_STORE(&xa_inner_sql_ten_ms_count_, 0);
      ATOMIC_STORE(&xa_inner_sql_twenty_ms_count_, 0);
      ATOMIC_STORE(&xa_inner_rpc_ten_ms_count_, 0);
      ATOMIC_STORE(&xa_inner_rpc_twenty_ms_count_, 0);
      ATOMIC_STORE(&xa_compensate_record_count_, 0);
    }
    // for active info
    int64_t active_xa_stmt_count = ATOMIC_LOAD(&active_xa_stmt_count_);
    int64_t active_xa_ctx_count = ATOMIC_LOAD(&active_xa_ctx_count_);
    if (0 != active_xa_stmt_count || 0 != active_xa_ctx_count) {
      FLOG_INFO("active xa statistics",
          "active_xa_stmt_count", active_xa_stmt_count,
          "active_xa_ctx_count", active_xa_ctx_count);
    }
  }
}

void ObXATransStatistics::try_print_active(const int64_t current_ts)
{
  static const int64_t STAT_INTERVAL = 1000000; // 1 seconds
  static const int64_t ACTIVE_STMT_THRESHOLD = 40;
  const int64_t last_print_ts = ATOMIC_LOAD(&last_print_active_ts_);
  const int64_t active_xa_stmt_count = ATOMIC_LOAD(&active_xa_stmt_count_);
  if ((current_ts - last_print_ts >= STAT_INTERVAL)
      && active_xa_stmt_count > ACTIVE_STMT_THRESHOLD) {
    if (ATOMIC_BCAS(&last_print_active_ts_, last_print_ts, current_ts)) {
      FLOG_INFO("active xa statistics",
          "active_xa_stmt_count", active_xa_stmt_count,
          "active_xa_ctx_count", ATOMIC_LOAD(&active_xa_ctx_count_),
          K(last_print_ts));
    }
  }
}

int ObDBLinkTransStatistics::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "xa trans statistics init twice", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObDBLinkTransStatistics::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  last_print_stat_ts_ = 0;
  is_inited_ = false;
}

void ObDBLinkTransStatistics::inc_dblink_trans_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(DBLINK_TRANS_COUNT, 1);
  ATOMIC_INC(&dblink_trans_count_);
}

void ObDBLinkTransStatistics::inc_dblink_trans_callback_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(DBLINK_TRANS_CALLBACK_COUNT, 1);
  ATOMIC_INC(&dblink_trans_callback_count_);
}

void ObDBLinkTransStatistics::inc_dblink_trans_fail_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(DBLINK_TRANS_FAIL_COUNT, 1);
  ATOMIC_INC(&dblink_trans_fail_count_);
}

void ObDBLinkTransStatistics::inc_dblink_trans_promotion_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(DBLINK_TRANS_PROMOTION_COUNT, 1);
  ATOMIC_INC(&dblink_trans_promotion_count_);
}

void ObDBLinkTransStatistics::inc_dblink_trans_commit_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(DBLINK_TRANS_COMMIT_COUNT, 1);
  ATOMIC_INC(&dblink_trans_commit_count_);
}

void ObDBLinkTransStatistics::add_dblink_trans_commit_used_time(const int64_t used_time)
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(DBLINK_TRANS_COMMIT_USED_TIME, used_time);
  ATOMIC_FAA(&dblink_trans_commit_used_time_, used_time);
}

void ObDBLinkTransStatistics::inc_dblink_trans_commit_fail_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(DBLINK_TRANS_COMMIT_FAIL_COUNT, 1);
  ATOMIC_INC(&dblink_trans_commit_fail_count_);
}

void ObDBLinkTransStatistics::inc_dblink_trans_rollback_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(DBLINK_TRANS_ROLLBACK_COUNT, 1);
  ATOMIC_INC(&dblink_trans_rollback_count_);
}

void ObDBLinkTransStatistics::add_dblink_trans_rollback_used_time(const int64_t used_time)
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(DBLINK_TRANS_ROLLBACK_USED_TIME, used_time);
  ATOMIC_FAA(&dblink_trans_rollback_used_time_, used_time);
}

void ObDBLinkTransStatistics::inc_dblink_trans_rollback_fail_count()
{
  common::ObTenantStatEstGuard guard(tenant_id_);
  EVENT_ADD(DBLINK_TRANS_ROLLBACK_FAIL_COUNT, 1);
  ATOMIC_INC(&dblink_trans_rollback_fail_count_);
}

void ObDBLinkTransStatistics::inc_dblink_trans_xa_start_count()
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_START_COUNT, 1);
  ATOMIC_INC(&dblink_trans_xa_start_count_);
}

void ObDBLinkTransStatistics::add_dblink_trans_xa_start_used_time(const int64_t used_time)
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_START_USED_TIME, used_time);
  ATOMIC_FAA(&dblink_trans_xa_start_used_time_, used_time);
}

void ObDBLinkTransStatistics::inc_dblink_trans_xa_start_fail_count()
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_START_FAIL_COUNT, 1);
  ATOMIC_INC(&dblink_trans_xa_start_fail_count_);
}

void ObDBLinkTransStatistics::inc_dblink_trans_xa_end_count()
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_END_COUNT, 1);
  ATOMIC_INC(&dblink_trans_xa_end_count_);
}

void ObDBLinkTransStatistics::add_dblink_trans_xa_end_used_time(const int64_t used_time)
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_END_USED_TIME, used_time);
  ATOMIC_FAA(&dblink_trans_xa_end_used_time_, used_time);
}

void ObDBLinkTransStatistics::inc_dblink_trans_xa_end_fail_count()
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_END_FAIL_COUNT, 1);
  ATOMIC_INC(&dblink_trans_xa_end_fail_count_);
}

void ObDBLinkTransStatistics::inc_dblink_trans_xa_prepare_count()
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_PREPARE_COUNT, 1);
  ATOMIC_INC(&dblink_trans_xa_prepare_count_);
}

void ObDBLinkTransStatistics::add_dblink_trans_xa_prepare_used_time(const int64_t used_time)
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_PREPARE_USED_TIME, used_time);
  ATOMIC_FAA(&dblink_trans_xa_prepare_used_time_, used_time);
}

void ObDBLinkTransStatistics::inc_dblink_trans_xa_prepare_fail_count()
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_PREPARE_FAIL_COUNT, 1);
  ATOMIC_INC(&dblink_trans_xa_prepare_fail_count_);
}

void ObDBLinkTransStatistics::inc_dblink_trans_xa_commit_count()
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_COMMIT_COUNT, 1);
  ATOMIC_INC(&dblink_trans_xa_commit_count_);
}

void ObDBLinkTransStatistics::add_dblink_trans_xa_commit_used_time(const int64_t used_time)
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_COMMIT_USED_TIME, used_time);
  ATOMIC_FAA(&dblink_trans_xa_commit_used_time_, used_time);
}

void ObDBLinkTransStatistics::inc_dblink_trans_xa_commit_fail_count()
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_COMMIT_FAIL_COUNT, 1);
  ATOMIC_INC(&dblink_trans_xa_commit_fail_count_);
}

void ObDBLinkTransStatistics::inc_dblink_trans_xa_rollback_count()
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_ROLLBACK_COUNT, 1);
  ATOMIC_INC(&dblink_trans_xa_rollback_count_);
}

void ObDBLinkTransStatistics::add_dblink_trans_xa_rollback_used_time(const int64_t used_time)
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_ROLLBACK_USED_TIME, used_time);
  ATOMIC_FAA(&dblink_trans_xa_rollback_used_time_, used_time);
}

void ObDBLinkTransStatistics::inc_dblink_trans_xa_rollback_fail_count()
{
  // common::ObTenantStatEstGuard guard(tenant_id_);
  // EVENT_ADD(DBLINK_TRANS_XA_ROLLBACK_FAIL_COUNT, 1);
  ATOMIC_INC(&dblink_trans_xa_rollback_fail_count_);
}

void ObDBLinkTransStatistics::try_print_dblink_statistics()
{
  static const int64_t STAT_INTERVAL = 9000000; // 9 seconds
  const int64_t current_ts = ObTimeUtility::current_time();
  const int64_t last_print_stat_ts = ATOMIC_LOAD(&last_print_stat_ts_);
  if (current_ts - last_print_stat_ts >= STAT_INTERVAL) {
    if (ATOMIC_BCAS(&last_print_stat_ts_, last_print_stat_ts, current_ts)) {
      int64_t dblink_trans_count = ATOMIC_LOAD(&dblink_trans_count_);
      int64_t dblink_trans_commit_count = ATOMIC_LOAD(&dblink_trans_commit_count_);
      int64_t dblink_trans_rollback_count = ATOMIC_LOAD(&dblink_trans_rollback_count_);
      if (0 != dblink_trans_count
          || 0 != dblink_trans_commit_count
          || 0 != dblink_trans_rollback_count) {
        FLOG_INFO("dblink trans statistics",
            "dblink_trans_count", dblink_trans_count,
            "dblink_trans_fail_count", ATOMIC_LOAD(&dblink_trans_fail_count_),
            "dblink_trans_promotion_count", ATOMIC_LOAD(&dblink_trans_promotion_count_),
            "dblink_trans_callback_count", ATOMIC_LOAD(&dblink_trans_callback_count_),
            "dblink_trans_commit_count", dblink_trans_commit_count,
            "dblink_trans_commit_latency", (0 == dblink_trans_commit_count)
                                           ? 0 : ATOMIC_LOAD(&dblink_trans_commit_used_time_) / dblink_trans_commit_count,
            "dblink_trans_commit_fail_count", ATOMIC_LOAD(&dblink_trans_commit_fail_count_),
            "dblink_trans_rollback_count", dblink_trans_rollback_count,
            "dblink_trans_rollback_latency", (0 == dblink_trans_rollback_count)
                                             ? 0 : ATOMIC_LOAD(&dblink_trans_rollback_used_time_) / dblink_trans_rollback_count,
            "dblink_trans_rollback_fail_count", ATOMIC_LOAD(&dblink_trans_rollback_fail_count_),
            "duration", (current_ts - last_print_stat_ts));
        int64_t dblink_trans_xa_start_count = ATOMIC_LOAD(&dblink_trans_xa_start_count_);
        int64_t dblink_trans_xa_end_count = ATOMIC_LOAD(&dblink_trans_xa_end_count_);
        int64_t dblink_trans_xa_prepare_count = ATOMIC_LOAD(&dblink_trans_xa_prepare_count_);
        int64_t dblink_trans_xa_commit_count = ATOMIC_LOAD(&dblink_trans_xa_commit_count_);
        int64_t dblink_trans_xa_rollback_count = ATOMIC_LOAD(&dblink_trans_xa_rollback_count_);
        FLOG_INFO("dblink trans xa statistics",
            "dblink_trans_xa_start_count", dblink_trans_xa_start_count,
            "dblink_trans_xa_start_latency", (0 == dblink_trans_xa_start_count)
                                             ? 0 : ATOMIC_LOAD(&dblink_trans_xa_start_used_time_) / dblink_trans_xa_start_count,
            "dblink_trans_xa_start_fail_count", ATOMIC_LOAD(&dblink_trans_xa_start_fail_count_),
            "dblink_trans_xa_end_count", dblink_trans_xa_end_count,
            "dblink_trans_xa_end_latency", (0 == dblink_trans_xa_end_count)
                                             ? 0 : ATOMIC_LOAD(&dblink_trans_xa_end_used_time_) / dblink_trans_xa_end_count,
            "dblink_trans_xa_end_fail_count", ATOMIC_LOAD(&dblink_trans_xa_end_fail_count_),
            "dblink_trans_xa_prepare_count", dblink_trans_xa_prepare_count,
            "dblink_trans_xa_prepare_latency", (0 == dblink_trans_xa_prepare_count)
                                             ? 0 : ATOMIC_LOAD(&dblink_trans_xa_prepare_used_time_) / dblink_trans_xa_prepare_count,
            "dblink_trans_xa_prepare_fail_count", ATOMIC_LOAD(&dblink_trans_xa_prepare_fail_count_),
            "dblink_trans_xa_commit_count", dblink_trans_xa_commit_count,
            "dblink_trans_xa_commit_latency", (0 == dblink_trans_xa_commit_count)
                                             ? 0 : ATOMIC_LOAD(&dblink_trans_xa_commit_used_time_) / dblink_trans_xa_commit_count,
            "dblink_trans_xa_commit_fail_count", ATOMIC_LOAD(&dblink_trans_xa_commit_fail_count_),
            "dblink_trans_xa_rollback_count", dblink_trans_xa_rollback_count,
            "dblink_trans_xa_rollback_latency", (0 == dblink_trans_xa_rollback_count)
                                             ? 0 : ATOMIC_LOAD(&dblink_trans_xa_rollback_used_time_) / dblink_trans_xa_rollback_count,
            "dblink_trans_xa_rollback_fail_count", ATOMIC_LOAD(&dblink_trans_xa_rollback_fail_count_),
            K(last_print_stat_ts));
      }
      ATOMIC_STORE(&dblink_trans_count_, 0);
      ATOMIC_STORE(&dblink_trans_fail_count_, 0);
      ATOMIC_STORE(&dblink_trans_promotion_count_, 0);
      ATOMIC_STORE(&dblink_trans_commit_count_, 0);
      ATOMIC_STORE(&dblink_trans_commit_used_time_, 0);
      ATOMIC_STORE(&dblink_trans_commit_fail_count_, 0);
      ATOMIC_STORE(&dblink_trans_rollback_count_, 0);
      ATOMIC_STORE(&dblink_trans_rollback_used_time_, 0);
      ATOMIC_STORE(&dblink_trans_rollback_fail_count_, 0);
      ATOMIC_STORE(&dblink_trans_xa_start_count_, 0);
      ATOMIC_STORE(&dblink_trans_xa_start_used_time_, 0);
      ATOMIC_STORE(&dblink_trans_xa_start_fail_count_, 0);
      ATOMIC_STORE(&dblink_trans_xa_end_count_, 0);
      ATOMIC_STORE(&dblink_trans_xa_end_used_time_, 0);
      ATOMIC_STORE(&dblink_trans_xa_end_fail_count_, 0);
      ATOMIC_STORE(&dblink_trans_xa_prepare_count_, 0);
      ATOMIC_STORE(&dblink_trans_xa_prepare_used_time_, 0);
      ATOMIC_STORE(&dblink_trans_xa_prepare_fail_count_, 0);
      ATOMIC_STORE(&dblink_trans_xa_commit_count_, 0);
      ATOMIC_STORE(&dblink_trans_xa_commit_used_time_, 0);
      ATOMIC_STORE(&dblink_trans_xa_commit_fail_count_, 0);
      ATOMIC_STORE(&dblink_trans_xa_rollback_count_, 0);
      ATOMIC_STORE(&dblink_trans_xa_rollback_used_time_, 0);
      ATOMIC_STORE(&dblink_trans_xa_rollback_fail_count_, 0);
      ATOMIC_STORE(&dblink_trans_callback_count_, 0);
    }
  }
}

} // transaction
} // oceanbase
