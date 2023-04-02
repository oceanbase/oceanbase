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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_EVENT_
#define OCEANBASE_TRANSACTION_OB_TRANS_EVENT_

#include "ob_trans_define.h"

namespace oceanbase
{
namespace transaction
{
class ObTransStatItem
{
public:
  explicit ObTransStatItem(const char *const item_name)
      : item_name_(item_name), lock_(common::ObLatchIds::TX_STAT_ITEM_LOCK) { reset(); }
  ~ObTransStatItem() { destroy(); }
  void reset();
  void destroy() { reset(); }
  void add(const int64_t value);
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransStatItem);
private:
  static const int64_t STAT_BUFFER_COUNT  = 60;
  static const int64_t STAT_INTERVAL = 1 * 1000 * 1000;
private:
  const char *const item_name_;
  int64_t total_;
  int64_t save_;
  int64_t last_time_;
  int64_t stat_buf_[STAT_BUFFER_COUNT];
  int64_t stat_idx_;
  mutable common::ObSpinLock lock_;
};

class ObTransStatistic
{
public:
  static ObTransStatistic &get_instance()
  {
    static ObTransStatistic trans_statistic_;
    return trans_statistic_;
  }
  ~ObTransStatistic() {}

  // count the number of system transactions
  void add_sys_trans_count(const uint64_t tenant_id, const int64_t value);
  // count the number of user transactions
  void add_user_trans_count(const uint64_t tenant_id, const int64_t value);
  // count the number of committed transactions
  void add_commit_trans_count(const uint64_t tenant_id, const int64_t value);
  // count the number of aborted transactions
  void add_rollback_trans_count(const uint64_t tenant_id, const int64_t value);
  // count the number of elr enable transactions
  void add_elr_enable_trans_count(const uint64_t tenant_id, const int64_t value);
  // count the number of elr unable transactions
  void add_elr_unable_trans_count(const uint64_t tenant_id, const int64_t value);
  void add_read_elr_row_count(const uint64_t tenant_id, const int64_t value);
  // count the number of timeout transactions: count when commit the transaction(end_trans)
  void add_trans_timeout_count(const uint64_t tenant_id, const int64_t value);
  // count how many transactions are started, via start_trans
  void add_trans_start_count(const uint64_t tenant_id, const int64_t value);
  // count the total time of transactions, from start_trans to the end of receiving ack from coordinator
  void add_trans_total_used_time(const uint64_t tenant_id, const int64_t value);
  // count the total time of local trans
  void add_local_trans_total_used_time(const uint64_t tenant_id, const int64_t value);
  // count the total time of distributed trans
  void add_dist_trans_total_used_time(const uint64_t tenant_id, const int64_t value);
  // count the number of local stmt
  void add_local_stmt_count(const uint64_t, const int64_t value);
  // count the number of remote stmt
  void add_remote_stmt_count(const uint64_t, const int64_t value);
  // count the number of distributed stmt
  void add_distributed_stmt_count(const uint64_t, const int64_t value);
  // count the number of total stmt
  void add_stmt_total_count(const uint64_t tenant_id, const int64_t value);
  // count the interval time between statements
  void add_stmt_interval_time(const uint64_t tenant_id, const int64_t value);
  // count the number of submitted logs
  void add_clog_submit_count(const uint64_t tenant_id, const int64_t value);
  // Count the time from submitting a log to calling back the transaction module after flushing majority
  void add_clog_sync_time(const uint64_t tenant_id, const int64_t value);
  // Count the number of calling back transaction modules through asynchronously submitting log
  void add_clog_sync_count(const uint64_t tenant_id, const int64_t value);
  // Count the time from beginning commit to receiving the commit response
  void add_trans_commit_time(const uint64_t tenant_id, const int64_t value);
  //Count the time from beginning rollback to receiving the abort response
  void add_trans_rollback_time(const uint64_t tenant_id, const int64_t value);
  // count the number of transactions with 0 participant
  void add_readonly_count(const uint64_t tenant_id, const int64_t value);
  // count the number of transactions with a single participant
  void add_local_count(const uint64_t tenant_id, const int64_t value);
  // count the number of transactions with multiple participant
  void add_dist_count(const uint64_t tenant_id, const int64_t value);
  // count how many redo logs are replayed
  void add_redo_log_replay_count(const uint64_t tenant_id, const int64_t value);
  // count the time spent on replaying redo logs
  void add_redo_log_replay_time(const uint64_t tenant_id, const int64_t value);
  // count how many prepare logs are replayed
  void add_prepare_log_replay_count(const uint64_t tenant_id, const int64_t value);
  // count the time spent on replaying prepare logs
  void add_prepare_log_replay_time(const uint64_t tenant_id, const int64_t value);
  // count how many commit logs are replayed
  void add_commit_log_replay_count(const uint64_t tenant_id, const int64_t value);
  // count the time spent on replaying commit logs
  void add_commit_log_replay_time(const uint64_t tenant_id, const int64_t value);
  // count how many abort logs are replayed
  void add_abort_log_replay_count(const uint64_t tenant_id, const int64_t value);
  // count the time spent on replaying abort logs
  void add_abort_log_replay_time(const uint64_t tenant_id, const int64_t value);
  // count how many clear logs are replayed
  void add_clear_log_replay_count(const uint64_t tenant_id, const int64_t value);
  // count the time spent on replaying clear logs
  void add_clear_log_replay_time(const uint64_t tenant_id, const int64_t value);
  // count the number of callbacks of sp redo logs
  void add_sp_redo_log_cb_count(const uint64_t tenant_id, const int64_t value);
  // count the callback time of sp redo log
  void add_sp_redo_log_cb_time(const uint64_t tenant_id, const int64_t value);
  // count the number of callbacks of sp commit logs
  void add_sp_commit_log_cb_count(const uint64_t tenant_id, const int64_t value);
  // count the callback time of sp commit log
  void add_sp_commit_log_cb_time(const uint64_t tenant_id, const int64_t value);
  // count the number of callbacks of redo logs
  void add_redo_log_cb_count(const uint64_t tenant_id, const int64_t value);
  // count the callback time of redo log
  void add_redo_log_cb_time(const uint64_t tenant_id, const int64_t value);
  // count the number of callbacks of prepare logs
  void add_prepare_log_cb_count(const uint64_t tenant_id, const int64_t value);
  // count the callback time of prepare log
  void add_prepare_log_cb_time(const uint64_t tenant_id, const int64_t value);
  // count the number of callbacks of commit logs
  void add_commit_log_cb_count(const uint64_t tenant_id, const int64_t value);
  // count the callback time of commit log
  void add_commit_log_cb_time(const uint64_t tenant_id, const int64_t value);
  // count the number of callbacks of abort logs
  void add_abort_log_cb_count(const uint64_t tenant_id, const int64_t value);
  // count the callback time of abort log
  void add_abort_log_cb_time(const uint64_t tenant_id, const int64_t value);
  // count the number of callbacks of clear logs
  void add_clear_log_cb_count(const uint64_t tenant_id, const int64_t value);
  // count the callback time of clear log
  void add_clear_log_cb_time(const uint64_t tenant_id, const int64_t value);
  // count the number of callback sqls
  void add_trans_callback_sql_count(const uint64_t tenant_id, const int64_t value);
  // count the time spent on calling back sql
  void add_trans_callback_sql_time(const uint64_t tenant_id, const int64_t value);
  // count how many times trans memtable end is called
  void add_trans_mt_end_count(const uint64_t tenant_id, const int64_t value);
  // count the time spent on calling trans memtable end
  void add_trans_mt_end_time(const uint64_t tenant_id, const int64_t value);
  // count the size of mutator
  void add_memstore_mutator_size(const uint64_t tenant_id, const int64_t value);
  // count the number of timeout statements
  void add_stmt_timeout_count(const uint64_t tenant_id, const int64_t value);
  // count the number of timeout slave read statements
  void add_slave_read_stmt_timeout_count(const uint64_t tenant_id, const int64_t value);
  // count the number of retry times of slave read statements
  void add_slave_read_stmt_retry_count(const uint64_t tenant_id, const int64_t value);
  // count the number of filling redo log
  void add_fill_redo_log_count(const uint64_t tenant_id, const int64_t value);
  // count the time of filling redo log
  void add_fill_redo_log_time(const uint64_t tenant_id, const int64_t value);
  // count the number of submitting trans log
  void add_submit_trans_log_count(const uint64_t tenant_id, const int64_t value);
  // count the time of submitting trans log
  void add_submit_trans_log_time(const uint64_t tenant_id, const int64_t value);
  // count the number of multiple-partition update statements of non-system tenant
  void add_trans_multi_partition_update_stmt_count(const uint64_t tenant_id, const int64_t value);
  // count the number of total requests processed by gts mgr
  void add_gts_request_total_count(const uint64_t tenant_id, const int64_t value);
  // count the total time spent on acquiring gts
  void add_gts_acquire_total_time(const uint64_t tenant_id, const int64_t value);
  // count the total count of gts acqusition
  void add_gts_acquire_total_count(const uint64_t tenant_id, const int64_t value);
  // count the number of gts wait: gts_acquire_total_count = not_wait_count + wait_count
  void add_gts_acquire_total_wait_count(const uint64_t tenant_id, const int64_t value);
  // count the total time spent on waiting gts elapse
  void add_gts_wait_elapse_total_time(const uint64_t tenant_id, const int64_t value);
  // count the number of waitting gts
  void add_gts_wait_elapse_total_count(const uint64_t tenant_id, const int64_t value);
  void add_gts_wait_elapse_total_wait_count(const uint64_t tenant_id, const int64_t value);
  // Count the number of rpc requests initiated by the gts client
  void add_gts_rpc_count(const uint64_t tenant_id, const int64_t value);
  // Count the total number of obtaining gts synchronously
  void add_gts_try_acquire_total_count(const uint64_t tenant_id, const int64_t value);
  // count the total number of synchronously waitting gts
  void add_gts_try_wait_elapse_total_count(const uint64_t tenant_id, const int64_t value);
  // count the number of batch commit trans
  void add_batch_commit_trans_count(const uint64_t tenant_id, const int64_t value);
  void add_trans_log_total_size(const uint64_t tenant_id, const int64_t value);

private:
  ObTransStatistic() : sys_trans_count_stat_("trans_sys_count"), user_trans_count_stat_("trans_user_count"),
      trans_commit_count_stat_("trans_commit_count"), trans_abort_count_stat_("trans_abort_count"),
      trans_timeout_count_stat_("trans_timeout_count"), trans_start_count_stat_("trans_start_count"),
      trans_total_used_time_stat_("trans_total_used_time"), clog_submit_count_stat_("clog_submit_count"),
      clog_sync_count_stat_("clog_sync_count"), clog_sync_time_stat_("clog_sync_time"),
      trans_commit_time_stat_("trans_commit_time"), trans_rollback_time_stat_("trans_rollback_time"),
      trans_single_partition_count_stat_("trans_single_partition_count"),
      trans_multi_partition_count_stat_("trans_single_partition_count"),
      distributed_stmt_stat_("distributed_stmt_count"), local_stmt_stat_("local_stmt_stat"),
      remote_stmt_stat_("remote_stmt_stat"), redo_replay_count_stat_("redo_replay_count"),
      redo_replay_time_stat_("redo_replay_time"), prepare_replay_count_stat_("prepare_replay_count"),
      prepare_replay_time_stat_("prepare_replay_time"), commit_replay_count_stat_("commit_replay_count"),
      commit_replay_time_stat_("commit_replay_time"), abort_replay_count_stat_("abort_replay_count"),
      abort_replay_time_stat_("abort_replay_time"), clear_replay_count_stat_("clear_replay_count"),
      clear_replay_time_stat_("clear_replay_time"), stmt_timeout_count_stat_("stmt_timeout_count"),
      slave_read_stmt_timeout_count_stat_("slave_read_timeout_count"),
      slave_read_retry_count_stat_("slave_read_retry_count"), fill_redo_log_count_stat_("fill_redo_log_count"),
      fill_redo_log_time_stat_("fill_redo_log_time"), submit_trans_log_count_stat_("submit_trans_log_count"),
      submit_trans_log_time_stat_("submit_trans_log_time"), trans_multi_partition_update_stmt_count_stat_("trans_multi_partition_update_stmt_count"),
      gts_request_total_count_stat_("gts_req_total_count"), gts_acquire_total_time_stat_("gts_acquire_total_time"),
      gts_acquire_total_count_stat_("gts_acquire_total_count"), gts_acquire_total_wait_count_stat_("gts_acquire_total_wait_count"),
      gts_wait_elapse_total_time_stat_("gts_wait_elapse_total_time"), gts_wait_elapse_total_count_stat_("gts_wait_elapse_total_count"),
      gts_wait_elapse_total_wait_count_stat_("gts_wait_elapse_total_wait_count"), gts_rpc_count_stat_("gts_rpc_count"),
      gts_try_acquire_total_count_stat_("gts_try_acquire_total_count"), gts_try_wait_elapse_total_count_stat_("gts_try_wait_elapse_total_count"),
      batch_commit_trans_count_stat_("btach_commit_trans_count") {}
  DISALLOW_COPY_AND_ASSIGN(ObTransStatistic);
private:
  ObTransStatItem sys_trans_count_stat_;
  ObTransStatItem user_trans_count_stat_;
  ObTransStatItem trans_commit_count_stat_;
  ObTransStatItem trans_abort_count_stat_;
  ObTransStatItem trans_timeout_count_stat_;
  ObTransStatItem trans_start_count_stat_;
  ObTransStatItem trans_total_used_time_stat_;
  ObTransStatItem clog_submit_count_stat_;
  ObTransStatItem clog_sync_count_stat_;
  ObTransStatItem clog_sync_time_stat_;
  ObTransStatItem trans_commit_time_stat_;
  ObTransStatItem trans_rollback_time_stat_;
  ObTransStatItem trans_single_partition_count_stat_;
  ObTransStatItem trans_multi_partition_count_stat_;
  ObTransStatItem distributed_stmt_stat_;
  ObTransStatItem local_stmt_stat_;
  ObTransStatItem remote_stmt_stat_;
  ObTransStatItem redo_replay_count_stat_;
  ObTransStatItem redo_replay_time_stat_;
  ObTransStatItem prepare_replay_count_stat_;
  ObTransStatItem prepare_replay_time_stat_;
  ObTransStatItem commit_replay_count_stat_;
  ObTransStatItem commit_replay_time_stat_;
  ObTransStatItem abort_replay_count_stat_;
  ObTransStatItem abort_replay_time_stat_;
  ObTransStatItem clear_replay_count_stat_;
  ObTransStatItem clear_replay_time_stat_;
  ObTransStatItem stmt_timeout_count_stat_;
  ObTransStatItem slave_read_stmt_timeout_count_stat_;
  ObTransStatItem slave_read_retry_count_stat_;
  ObTransStatItem fill_redo_log_count_stat_;
  ObTransStatItem fill_redo_log_time_stat_;
  ObTransStatItem submit_trans_log_count_stat_;
  ObTransStatItem submit_trans_log_time_stat_;
  ObTransStatItem trans_multi_partition_update_stmt_count_stat_;
  // According to the following indicators, we can get,
  // 1. reuse rate of gts: 1 - (gts_wait_total_count / gts_acquire_total_count)
  // 2. average time cost of acquiring gts:  gts_acquire_total_time / gts_acquire_total_count)
  // 3. the number of requests processed by gts mgr: gts_request_total_count
  ObTransStatItem gts_request_total_count_stat_;
  ObTransStatItem gts_acquire_total_time_stat_;
  ObTransStatItem gts_acquire_total_count_stat_;
  ObTransStatItem gts_acquire_total_wait_count_stat_;
  ObTransStatItem gts_wait_elapse_total_time_stat_;
  ObTransStatItem gts_wait_elapse_total_count_stat_;
  ObTransStatItem gts_wait_elapse_total_wait_count_stat_;
  ObTransStatItem gts_rpc_count_stat_;
  ObTransStatItem gts_try_acquire_total_count_stat_;
  ObTransStatItem gts_try_wait_elapse_total_count_stat_;
  ObTransStatItem batch_commit_trans_count_stat_;
};

} // transaction
} // oceanbase

// count the number of commit/abort transactions
#define TX_STAT_COMMIT_INC ObTransStatistic::get_instance().add_commit_trans_count(tenant_id_, 1);
#define TX_STAT_ROLLBACK_INC ObTransStatistic::get_instance().add_rollback_trans_count(tenant_id_, 1);
#define TX_STAT_START_INC ObTransStatistic::get_instance().add_trans_start_count(tenant_id_, 1);
#define TX_STAT_TIMEOUT_INC ObTransStatistic::get_instance().add_trans_timeout_count(tenant_id_, 1);
#define TX_STAT_TIME_USED(time) ObTransStatistic::get_instance().add_trans_total_used_time(tenant_id_, time);
#define TX_STAT_COMMIT_TIME_USED(time) ObTransStatistic::get_instance().add_trans_commit_time(tenant_id_, time);
#define TX_STAT_ROLLBACK_TIME_USED(time) ObTransStatistic::get_instance().add_trans_rollback_time(tenant_id_, time);
#define TX_STAT_DIST_INC ObTransStatistic::get_instance().add_dist_count(tenant_id_, 1);
#define TX_STAT_LOCAL_INC ObTransStatistic::get_instance().add_local_count(tenant_id_, 1);
#define TX_STAT_READONLY_INC ObTransStatistic::get_instance().add_readonly_count(tenant_id_, 1);
#define TX_STAT_ELR_ENABLE_TRANS_INC ObTransStatistic::get_instance().add_elr_enable_trans_count(MTL_ID(), 1);
#define TX_STAT_ELR_UNABLE_TRANS_INC ObTransStatistic::get_instance().add_elr_unable_trans_count(MTL_ID(), 1);
#define TX_STAT_READ_ELR_ROW_COUNT_INC transaction::ObTransStatistic::get_instance().add_read_elr_row_count(MTL_ID(), 1);
#define TX_STAT_LOCAL_TOTAL_TIME_USED(time) ObTransStatistic::get_instance().add_local_trans_total_used_time(MTL_ID(), time);
#define TX_STAT_DIST_TOTAL_TIME_USED(time) ObTransStatistic::get_instance().add_dist_trans_total_used_time(MTL_ID(), time);

// TODO: following events is not used, do clean up
// count the interval time between statements
#define TRANS_STAT_STMT_INTERVAL_TIME(tenant_id, last_end_stmt_ts)                       \
  { if (last_end_stmt_ts > 0) {ObTransStatistic::get_instance().add_stmt_interval_time(tenant_id, ObClockGenerator::getClock() - last_end_stmt_ts);} }

#define TRANS_STAT_STMT_TOTAL_COUNT_INC(tenant_id)                               \
  { ObTransStatistic::get_instance().add_stmt_total_count(tenant_id, 1); }

#define TRANS_MULTI_PARTITION_UPDATE_STMT_COUNT_INC(tenant_id)                  \
  { ObTransStatistic::get_instance().add_trans_multi_partition_update_stmt_count(tenant_id, 1); }

#endif // OCEANABAE_TRANSACTION_OB_TRANS_EVENT_
