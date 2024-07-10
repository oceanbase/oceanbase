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

#ifndef OCEABASE_STORAGE_TENANT_FREEZER_
#define OCEABASE_STORAGE_TENANT_FREEZER_

#include "lib/atomic/ob_atomic.h"
#include "lib/list/ob_list.h"
#include "lib/literals/ob_literals.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/thread/thread_mgr.h"
#include "lib/thread/thread_mgr_interface.h"
#include "share/ob_occam_timer.h"
#include "share/ob_tenant_mgr.h"
#include "storage/tx_storage/ob_tenant_freezer_rpc.h"
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "storage/compaction/ob_compaction_util.h"

namespace oceanbase
{
namespace storage
{
class ObTenantFreezer;
class ObTenantTxDataFreezeGuard;

class ObTenantFreezerStat
{
public:
  static const int64_t MAX_FREEZER_MERGE_TYPE = 3;
  enum ObFreezerMergeType
  {
    UNNECESSARY_TYPE = -1,
    MINI_MERGE       = 0,
    MINOR_MERGE      = 1,
    MAJOR_MERGE      = 2,
    MAX_MERGE_TYPE   = 3
  };
  ObTenantFreezerStat() { reset(); }
  ~ObTenantFreezerStat() {}
public:
  int64_t last_captured_timestamp_;
  // captured data size from last captured time
  int64_t captured_data_size_;
  int64_t captured_freeze_times_;

  int64_t captured_merge_time_cost_[ObFreezerMergeType::MAX_MERGE_TYPE];
  int64_t captured_merge_times_[ObFreezerMergeType::MAX_MERGE_TYPE];

  int64_t last_captured_retire_clock_;

  ObFreezerMergeType switch_to_freezer_merge_type(const compaction::ObMergeType type);

  const char *freezer_merge_type_to_str(const ObFreezerMergeType merge_type);

  bool is_useful_freezer_merge_type(const ObFreezerMergeType merge_type);

  void reset(int64_t retire_clock = 0);

  void refresh();

  void add_freeze_event();

  void add_merge_event(const compaction::ObMergeType type, const int64_t cost);

  void print_activity_metrics();

  void assign(const ObTenantFreezerStat stat);

  TO_STRING_KV(K_(last_captured_timestamp),
               K_(captured_data_size),
               K_(captured_freeze_times),
               K_(last_captured_retire_clock));
};

class ObTenantFreezerStatHistory
{
public:
  // 5(day in working week) * 24(hour in day) * 2(half of an hour in an hour)
  static const int64_t MAX_HISTORY_LENGTH = 5 * 24 * 2;
  ObTenantFreezerStatHistory(): start_(0), length_(0) {}

  void add_activity_metric(const ObTenantFreezerStat stat);

  void reset();
public:
  int64_t start_;
  int64_t length_;
  ObTenantFreezerStat history_[MAX_HISTORY_LENGTH];
};

// this is used for tenant freeze, all the freeze task should call the function of this unit.
class ObTenantFreezer
{
friend ObTenantTxDataFreezeGuard;
friend class ObFreezer;
struct PeriodicalUpdateValueCache {
  PeriodicalUpdateValueCache() : value_(false), update_ts_(0) {}
  void reset()
  {
    value_ = false;
    update_ts_ = 0;
  }
  bool value_;
  int64_t update_ts_;
};

public:
  const static int64_t TIME_WHEEL_PRECISION = 100_ms;
  const static int64_t SLOW_FREEZE_INTERVAL = 30_s;
  const static int FREEZE_TRIGGER_THREAD_NUM= 1;
  const static int FREEZE_THREAD_NUM= 5;
  const static int64_t FREEZE_TRIGGER_INTERVAL = 2_s;
  const static int64_t UPDATE_INTERVAL = 100_ms;
  const static int64_t MAX_FREEZE_TIMEOUT_US = 1800 * 1000 * 1000; // 30 min
  // replay use 1G/s
  const static int64_t REPLAY_RESERVE_MEMSTORE_BYTES = 100 * 1024 * 1024; // 100 MB
  const static int64_t MEMSTORE_USED_CACHE_REFRESH_INTERVAL = 100_ms;
  static double MDS_TABLE_FREEZE_TRIGGER_TENANT_PERCENTAGE;

public:
  ObTenantFreezer();
  ~ObTenantFreezer();
  static int mtl_init(ObTenantFreezer* &m);
  int init();
  void destroy();
  int start();
  int stop();
  void wait();

  // freeze all the checkpoint unit of this tenant.
  int tenant_freeze();

  // freeze a ls, if the ls is freezing, do nothing and return OB_ENTRY_EXIST.
  // if there is some process hold the ls lock or a OB_EAGAIN occur, we will retry
  // until timeout.
  int ls_freeze(const share::ObLSID &ls_id);
  // freeze a tablet
  int tablet_freeze(const common::ObTabletID &tablet_id,
                    const bool need_rewrite_tablet_meta = false,
                    const bool is_sync = false);
  int tablet_freeze(share::ObLSID ls_id,
                    const common::ObTabletID &tablet_id,
                    const bool need_rewrite_tablet_meta = false,
                    const bool is_sync = false);
  // check if this tenant's memstore is out of range, and trigger minor/major freeze.
  int check_and_do_freeze();

  int do_freeze_diagnose();

  // used for replay to check whether can enqueue another replay task
  bool is_replay_pending_log_too_large(const int64_t pending_size);
  // If the tenant's freeze process is slowed, we will only freeze one time every
  // SLOW_FREEZE_INTERVAL.
  // set the tenant freeze process slowed. used while the tablet's max memtable
  // number meet.
  // @param[in] tablet_id, which tablet slow the freeze process.
  // @param[in] retire_clock, the memtable's retire clock.
  int set_tenant_slow_freeze(const common::ObTabletID &tablet_id,
                             const int64_t retire_clock);
  // uset the slow freeze flag.
  // if the tenant freeze process is slowed by this tablet, then unset it.
  // @param[in] tablet_id, the tablet who want to unset the slow freeze flag.
  //                       unset success if the tablet is the one who slow the tenant.
  //                       else do nothing.
  int unset_tenant_slow_freeze(const common::ObTabletID &tablet_id);
  // check whether the tenant mem limit, memstore limit has been changed.
  // @param[in] curr_lower_limit, the new lower limit
  // @param[in] curr_upper_limit, the new upper limit
  bool is_tenant_mem_changed(const int64_t curr_lower_limit,
                             const int64_t curr_upper_limit) const;
  // set tenant mem limit, both for min and max memory limit.
  // @param[in] lower_limit, the min memory limit will be set.
  // @param[in] upper_limit, the max memory limit will be set.
  int set_tenant_mem_limit(const int64_t lower_limit,
                           const int64_t upper_limit);
  // get the tenant mem limit, both min and max memory limit.
  // @param[out] lower_limit, the min memory limit set now.
  // @param[out] upper_limit, the max memory limit set now.
  int get_tenant_mem_limit(int64_t &lower_limit,
                           int64_t &upper_limit) const;
  // get the tenant memstore info.
  int get_tenant_memstore_cond(int64_t &active_memstore_used,
                               int64_t &total_memstore_used,
                               int64_t &memstore_freeze_trigger,
                               int64_t &memstore_limit,
                               int64_t &freeze_cnt,
                               const bool force_refresh = true);
  // get the tenant memstore used
  int get_tenant_memstore_used(int64_t &total_memstore_used,
                               const bool force_refresh = true);
  // get the tenant memstore limit.
  int get_tenant_memstore_limit(int64_t &mem_limit);
  // get the memstore limit percentage
  static int64_t get_memstore_limit_percentage();
  // this is used to check if the tenant's memstore is out at user side.
  int check_memstore_full(bool &is_out_of_mem);
  // this is used for internal check rather than user side.
  int check_memstore_full_internal(bool &is_out_of_mem);
  // this check if a major freeze is needed
  bool tenant_need_major_freeze();
  // used to print a log.
  static int rpc_callback();
  // update the memstore limit use sysconf.
  int reload_config();
  // print the tenant usage info into print_buf.
  // @param[out] print_buf, the buf is used to print.
  // @param[in] buf_len, the buf length.
  // @param[in/out] pos, from which position to print and return the print position.
  int print_tenant_usage(char *print_buf,
                         int64_t buf_len,
                         int64_t &pos);
  // if major freeze is failed and need retry, set the major freeze into at retry_major_info_.
  const ObRetryMajorInfo &get_retry_major_info() const { return retry_major_info_; }
  void record_freeze_failed_tablet(const ObTabletID &tablet_id);
  void erase_freeze_failed_tablet(const ObTabletID &tablet_id);
  void set_retry_major_info(const ObRetryMajorInfo &retry_major_info)
  {
    retry_major_info_ = retry_major_info;
  }
  static int64_t get_freeze_trigger_interval() { return FREEZE_TRIGGER_INTERVAL; }
  bool exist_ls_freezing();
  bool exist_ls_throttle_is_skipping();
  bool memstore_remain_memory_is_exhausting();

  // freezer stat collector and generator
  void add_merge_event(const compaction::ObMergeType type, const int64_t cost)
  {
    freezer_stat_.add_merge_event(type, cost);
  }

  void get_freezer_stat_history_snapshot(int64_t &length);

  void get_freezer_stat_from_history(int64_t pos, ObTenantFreezerStat& stat);

private:
  int get_tenant_memstore_cond_(int64_t &active_memstore_used,
                                int64_t &total_memstore_used,
                                int64_t &memstore_freeze_trigger,
                                int64_t &memstore_limit,
                                int64_t &freeze_cnt,
                                const bool force_refresh = true);
  int check_memstore_full_(bool &last_result,
                           int64_t &last_check_timestamp,
                           bool &is_out_of_mem,
                           const bool from_user = true);
  static int ls_freeze_(ObLS *ls, const bool is_sync, const int64_t abs_timeout_ts);
  static int ls_freeze_all_unit_(ObLS *ls,
                                 const int64_t abs_timeout_ts = INT64_MAX);
  static int tablet_freeze_(ObLS *ls,
                            const common::ObTabletID &tablet_id,
                            const bool need_rewrite_tablet_meta,
                            const bool is_sync,
                            const int64_t abs_timeout_ts);
  // freeze all the ls of this tenant.
  // return the first failed code.
  int tenant_freeze_();
  // we can only deal with freeze one by one.
  // set tenant freezing will prevent a new freeze.
  int set_tenant_freezing_();
  // unset tenant freezing flag.
  // @param[in] rollback_freeze_cnt, reduce the tenant's freeze count by 1, if true.
  int unset_tenant_freezing_(const bool rollback_freeze_cnt);
  static int64_t get_freeze_trigger_percentage_();
  static int64_t get_memstore_limit_percentage_();
  int post_freeze_request_(const storage::ObFreezeType freeze_type,
                           const int64_t try_frozen_version);
  int retry_failed_major_freeze_(bool &triggered);
  int get_global_frozen_scn_(int64_t &frozen_version);
  int post_tx_data_freeze_request_();
  int post_mds_table_freeze_request_();
  int get_tenant_mem_usage_(ObTenantFreezeCtx &ctx);
  int get_tenant_mem_stat_(ObTenantStatistic &stat);
  static int get_freeze_trigger_(ObTenantFreezeCtx &ctx);
  bool need_freeze_(const ObTenantFreezeCtx &ctx);
  bool is_major_freeze_turn_();
  int do_major_if_need_(const bool need_freeze);
  int do_minor_freeze_(const ObTenantFreezeCtx &ctx);
  int do_major_freeze_(const int64_t try_frozen_scn);
  void log_frozen_memstore_info_if_need_(const ObTenantFreezeCtx &ctx);
  void halt_prewarm_if_need_(const ObTenantFreezeCtx &ctx);
  int check_and_freeze_normal_data_(ObTenantFreezeCtx &ctx);
  int check_and_freeze_tx_data_();
  int check_and_freeze_mds_table_();

  int get_tenant_tx_data_mem_used_(int64_t &tenant_tx_data_frozen_mem_used,
                                   int64_t &tenant_tx_data_active_mem_used,
                                   bool for_statistic_print = false);

  int get_ls_tx_data_memory_info_(ObLS *ls,
                                  int64_t &ls_tx_data_frozen_mem_used,
                                  int64_t &ls_tx_data_active_mem_used,
                                  bool for_statistic_print = false);

private:
  bool is_inited_;
  bool is_freezing_tx_data_;
  ObTenantInfo tenant_info_;                  // store the mem limit, memstore limit and etc.
  obrpc::ObTenantFreezerRpcProxy rpc_proxy_;  // used to trigger minor/major freeze
  obrpc::ObTenantFreezerRpcCb tenant_mgr_cb_; // callback after the trigger rpc finish.
  obrpc::ObSrvRpcProxy *svr_rpc_proxy_;
  obrpc::ObCommonRpcProxy *common_rpc_proxy_;
  const share::ObRsMgr *rs_mgr_;
  ObAddr self_;
  ObRetryMajorInfo retry_major_info_;

  common::ObOccamThreadPool freeze_trigger_pool_;
  common::ObOccamTimer freeze_trigger_timer_;
  common::ObOccamTimerTaskRAIIHandle timer_handle_;
  common::ObOccamThreadPool freeze_thread_pool_;
  ObSpinLock freeze_thread_pool_lock_;

  // diagnose only, we capture the freeze stats every 30 minutes
  ObTenantFreezerStat freezer_stat_;
  // diagnose only, we capture the freeze history in one monthes
  ObTenantFreezerStatHistory freezer_history_;
  PeriodicalUpdateValueCache throttle_is_skipping_cache_;
  PeriodicalUpdateValueCache memstore_remain_memory_is_exhausting_cache_;
};

class ObTenantTxDataFreezeGuard
{
public:
  ObTenantTxDataFreezeGuard() : can_freeze_(false), tenant_freezer_(nullptr) {}
  ~ObTenantTxDataFreezeGuard() { reset(); }

  int init(ObTenantFreezer *tenant_freezer)
  {
    int ret = OB_SUCCESS;
    reset();
    if (OB_ISNULL(tenant_freezer)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid tx data table", KR(ret));
    } else {
      can_freeze_ = (false == ATOMIC_CAS(&(tenant_freezer->is_freezing_tx_data_), false, true));
      if (can_freeze_) {
        tenant_freezer_ = tenant_freezer;
      }
    }
    return ret;
  }

  void reset()
  {
    can_freeze_ = false;
    if (OB_NOT_NULL(tenant_freezer_)) {
      ATOMIC_STORE(&(tenant_freezer_->is_freezing_tx_data_), false);
      tenant_freezer_ = nullptr;
    }
  }

  bool can_freeze() { return can_freeze_; }

private:
  bool can_freeze_;
  ObTenantFreezer *tenant_freezer_;
};
}
}
#endif
