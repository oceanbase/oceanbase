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

#ifndef OCEANBASE_TRANSACTION_OB_XA_TRANS_EVENT_
#define OCEANBASE_TRANSACTION_OB_XA_TRANS_EVENT_

#include "ob_trans_define.h"

namespace oceanbase
{
namespace transaction
{
class ObXATransStatistics
{
public:
  ObXATransStatistics()
    : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), last_print_stat_ts_(0), last_print_active_ts_(0),
      xa_start_count_(0), xa_start_remote_count_(0), xa_start_fail_count_(0), xa_start_used_time_us_(0),
      xa_end_count_(0), xa_end_remote_count_(0), xa_end_fail_count_(0), xa_end_used_time_us_(0),
      xa_prepare_count_(0), xa_prepare_remote_count_(0), xa_prepare_fail_count_(0), xa_prepare_used_time_us_(0),
      xa_rollback_count_(0), xa_rollback_remote_count_(0), xa_rollback_fail_count_(0), xa_rollback_used_time_us_(0),
      xa_commit_count_(0), xa_commit_remote_count_(0), xa_commit_fail_count_(0), xa_commit_used_time_us_(0),
      xa_trans_start_count_(0), xa_read_only_trans_count_(0), xa_one_phase_commit_count_(0),
      xa_inner_sql_count_(0), xa_inner_sql_used_time_us_(0), xa_inner_rpc_count_(0), xa_inner_rpc_used_time_us_(0),
      xa_inner_sql_ten_ms_count_(0), xa_inner_sql_twenty_ms_count_(0),
      xa_inner_rpc_ten_ms_count_(0), xa_inner_rpc_twenty_ms_count_(0),
      active_xa_stmt_count_(0), active_xa_ctx_count_(0), xa_compensate_record_count_(0) {}
  ~ObXATransStatistics() { destroy(); }
  int init(const uint64_t tenant_id);
  void reset();
  void destroy() { reset(); }

  // total count of xa start
  void inc_xa_start_total_count();
  // total used time of xa start
  void add_xa_start_total_used_time(const int64_t value);
  // total count of remote xa start executed successfully
  void inc_xa_start_remote_count();
  // total count of failed xa start
  void inc_xa_start_fail_count();
  // total count of xa end
  void inc_xa_end_total_count();
  // total used time of xa end
  void add_xa_end_total_used_time(const int64_t value);
  // total count of remote xa end executed successfully
  void inc_xa_end_remote_count();
  // total count of failed xa end
  void inc_xa_end_fail_count();
  // total count of xa prepare
  void inc_xa_prepare_total_count();
  // total used time of xa prepare
  void add_xa_prepare_total_used_time(const int64_t value);
  // total count of remote xa prepare executed successfully
  void inc_xa_prepare_remote_count();
  // total count of failed xa prepare
  void inc_xa_prepare_fail_count();
  // total count of xa rollback
  void inc_xa_rollback_total_count();
  // total used time of xa rollback
  void add_xa_rollback_total_used_time(const int64_t value);
  // total count of remote xa rollback
  void inc_xa_rollback_remote_count();
  // total count of failed xa rollback
  void inc_xa_rollback_fail_count();
  // total count of xa commit
  void inc_xa_commit_total_count();
  // total used time of xa commit
  void add_xa_commit_total_used_time(const int64_t value);
  // total count of remote xa commit
  void inc_xa_commit_remote_count();
  // total count of failed xa commit
  void inc_xa_commit_fail_count();
  // total count xa trans has been started
  void inc_xa_trans_start_count();
  // total count of read only xa trans
  void inc_xa_read_only_trans_total_count();
  // total count of xa trans with one phase commit
  void inc_xa_one_phase_commit_total_count();
  // total count of inner sql in xa stmt
  void inc_xa_inner_sql_total_count();
  // total used time of inner sql in xa stmt
  void add_xa_inner_sql_total_used_time(const int64_t value);
  // total count of inner rpc in xa stmt
  void inc_xa_inner_rpc_total_count();
  // total used time of inner rpc in xa stmt
  void add_xa_inner_rpc_total_used_time(const int64_t value);
  // increment active xa stmt count
  void inc_active_xa_stmt_count();
  // decrement active xa stmt count
  void dec_active_xa_stmt_count();
  // increase total count of inner sql whose latency is greater than 10ms
  void inc_xa_inner_sql_ten_ms_total_count();
  // increase total count of inner sql whose latency is greater than 20ms
  void inc_xa_inner_sql_twenty_ms_total_count();
  // increase total count of inner rpc whose latency is greater than 10ms
  void inc_xa_inner_rpc_ten_ms_total_count();
  // increase total count of inner rpc whose latency is greater than 20ms
  void inc_xa_inner_rpc_twenty_ms_total_count();
  // increment active xa ctx count
  void inc_active_xa_ctx_count();
  // decrement active xa ctx count
  void dec_active_xa_ctx_count();
  // increment compensate record count
  void inc_compensate_record_count();

public:
  void try_print_xa_statistics();
  void try_print_active(const int64_t start_ts);

private:
  DISALLOW_COPY_AND_ASSIGN(ObXATransStatistics);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t last_print_stat_ts_;
  int64_t last_print_active_ts_;
  // for xa statistics
  int64_t xa_start_count_;
  int64_t xa_start_remote_count_;
  int64_t xa_start_fail_count_;
  int64_t xa_start_used_time_us_;
  int64_t xa_end_count_;
  int64_t xa_end_remote_count_;
  int64_t xa_end_fail_count_;
  int64_t xa_end_used_time_us_;
  int64_t xa_prepare_count_;
  int64_t xa_prepare_remote_count_;
  int64_t xa_prepare_fail_count_;
  int64_t xa_prepare_used_time_us_;
  int64_t xa_rollback_count_;
  int64_t xa_rollback_remote_count_;
  int64_t xa_rollback_fail_count_;
  int64_t xa_rollback_used_time_us_;
  int64_t xa_commit_count_;
  int64_t xa_commit_remote_count_;
  int64_t xa_commit_fail_count_;
  int64_t xa_commit_used_time_us_;
  int64_t xa_trans_start_count_;
  int64_t xa_read_only_trans_count_;
  int64_t xa_one_phase_commit_count_;
  int64_t xa_inner_sql_count_;
  int64_t xa_inner_sql_used_time_us_;
  int64_t xa_inner_rpc_count_;
  int64_t xa_inner_rpc_used_time_us_;
  int64_t xa_inner_sql_ten_ms_count_;
  int64_t xa_inner_sql_twenty_ms_count_;
  int64_t xa_inner_rpc_ten_ms_count_;
  int64_t xa_inner_rpc_twenty_ms_count_;
  // active info
  int64_t active_xa_stmt_count_;
  int64_t active_xa_ctx_count_;
  // inner logic
  int64_t xa_compensate_record_count_;
};

class ObDBLinkTransStatistics
{
public:
  ObDBLinkTransStatistics()
    : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), last_print_stat_ts_(0),
      dblink_trans_count_(0), dblink_trans_fail_count_(0),
      dblink_trans_promotion_count_(0), dblink_trans_callback_count_(0),
      dblink_trans_commit_count_(0), dblink_trans_commit_used_time_(0),
      dblink_trans_commit_fail_count_(0),
      dblink_trans_rollback_count_(0), dblink_trans_rollback_used_time_(0),
      dblink_trans_rollback_fail_count_(0),
      dblink_trans_xa_start_count_(0), dblink_trans_xa_start_used_time_(0),
      dblink_trans_xa_start_fail_count_(0),
      dblink_trans_xa_end_count_(0), dblink_trans_xa_end_used_time_(0),
      dblink_trans_xa_end_fail_count_(0),
      dblink_trans_xa_prepare_count_(0), dblink_trans_xa_prepare_used_time_(0),
      dblink_trans_xa_prepare_fail_count_(0),
      dblink_trans_xa_commit_count_(0), dblink_trans_xa_commit_used_time_(0),
      dblink_trans_xa_commit_fail_count_(0),
      dblink_trans_xa_rollback_count_(0), dblink_trans_xa_rollback_used_time_(0),
      dblink_trans_xa_rollback_fail_count_(0) {}
  ~ObDBLinkTransStatistics() { destroy(); }
  int init(const uint64_t tenant_id);
  void reset();
  void destroy() { reset(); }
public:
  // increment dblink trans count
  void inc_dblink_trans_count();
  // increment failed dblink trans count
  void inc_dblink_trans_fail_count();
  // increment dblink trans promotion count
  void inc_dblink_trans_promotion_count();
  // increment dblink trans callback count
  void inc_dblink_trans_callback_count();
  // increment dblink trans commit count
  void inc_dblink_trans_commit_count();
  // add dblink trans commit used time
  void add_dblink_trans_commit_used_time(const int64_t used_time);
  // increment failed dblink trans commit count
  void inc_dblink_trans_commit_fail_count();
  // increment dblink trans rollback count
  void inc_dblink_trans_rollback_count();
  // add dblink trans rollback used time
  void add_dblink_trans_rollback_used_time(const int64_t used_time);
  // increment failed dblink trans rollback count
  void inc_dblink_trans_rollback_fail_count();
  // increment dblink trans xa_start count
  void inc_dblink_trans_xa_start_count();
  // add dblink trans xa_start used time
  void add_dblink_trans_xa_start_used_time(const int64_t used_time);
  // increment failed dblink trans xa_start count
  void inc_dblink_trans_xa_start_fail_count();
  // increment dblink trans xa_end count
  void inc_dblink_trans_xa_end_count();
  // add dblink trans xa_end used time
  void add_dblink_trans_xa_end_used_time(const int64_t used_time);
  // increment failed dblink trans xa_end count
  void inc_dblink_trans_xa_end_fail_count();
  // increment dblink trans xa_prepare count
  void inc_dblink_trans_xa_prepare_count();
  // add dblink trans xa_prepare used time
  void add_dblink_trans_xa_prepare_used_time(const int64_t used_time);
  // increment failed dblink trans xa_prepare count
  void inc_dblink_trans_xa_prepare_fail_count();
  // increment dblink trans xa_commit count
  void inc_dblink_trans_xa_commit_count();
  // add dblink trans xa_commit used time
  void add_dblink_trans_xa_commit_used_time(const int64_t used_time);
  // increment failed dblink trans xa_commit count
  void inc_dblink_trans_xa_commit_fail_count();
  // increment dblink trans xa_rollback count
  void inc_dblink_trans_xa_rollback_count();
  // add dblink trans xa_rollback used time
  void add_dblink_trans_xa_rollback_used_time(const int64_t used_time);
  // increment failed dblink trans xa_rollback count
  void inc_dblink_trans_xa_rollback_fail_count();
public:
  void try_print_dblink_statistics();
private:
  DISALLOW_COPY_AND_ASSIGN(ObDBLinkTransStatistics);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t last_print_stat_ts_;
  // for statistics
  int64_t dblink_trans_count_;
  int64_t dblink_trans_fail_count_;
  int64_t dblink_trans_promotion_count_;
  int64_t dblink_trans_callback_count_;
  int64_t dblink_trans_commit_count_;
  int64_t dblink_trans_commit_used_time_;
  int64_t dblink_trans_commit_fail_count_;
  int64_t dblink_trans_rollback_count_;
  int64_t dblink_trans_rollback_used_time_;
  int64_t dblink_trans_rollback_fail_count_;
  int64_t dblink_trans_xa_start_count_;
  int64_t dblink_trans_xa_start_used_time_;
  int64_t dblink_trans_xa_start_fail_count_;
  int64_t dblink_trans_xa_end_count_;
  int64_t dblink_trans_xa_end_used_time_;
  int64_t dblink_trans_xa_end_fail_count_;
  int64_t dblink_trans_xa_prepare_count_;
  int64_t dblink_trans_xa_prepare_used_time_;
  int64_t dblink_trans_xa_prepare_fail_count_;
  int64_t dblink_trans_xa_commit_count_;
  int64_t dblink_trans_xa_commit_used_time_;
  int64_t dblink_trans_xa_commit_fail_count_;
  int64_t dblink_trans_xa_rollback_count_;
  int64_t dblink_trans_xa_rollback_used_time_;
  int64_t dblink_trans_xa_rollback_fail_count_;
};

} // transaction
} // oceanbase


#endif // OCEANABAE_TRANSACTION_OB_XA_TRANS_EVENT_
