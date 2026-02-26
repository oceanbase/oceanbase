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

#include "ob_xa_inner_table_gc_worker.h"
#include "ob_xa_service.h"
#include "storage/tx/ob_trans_service.h"
#include "lib/ash/ob_active_session_guard.h"

namespace oceanbase
{

using namespace common;

namespace transaction
{

int ObXAInnerTableGCWorker::init(ObXAService *txs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(txs)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(txs));
  } else {
    xa_service_ = txs;
    is_inited_ = true;
  }
  return ret;
}

int ObXAInnerTableGCWorker::start()
{
  int ret = OB_SUCCESS;
  share::ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "xa gc worker not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    TRANS_LOG(ERROR, "XA gc worker thread start error", K(ret));
  } else {
    max_gc_cost_time_ = GC_INTERVAL;
    TRANS_LOG(INFO, "XA gc worker thread start");
  }
  return ret;
}

// 1. in mysql mode, only meta leader collect invalid records
// 2. no need gc for sys tenant temporarily
void ObXAInnerTableGCWorker::run1()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const share::ObLSID meta_ls_id(share::ObLSID::SYS_LS_ID);
  const uint64_t tenant_id = MTL_ID();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t last_scan_ts = ObTimeUtil::current_time();

  bool has_decided_mode = false;
  bool is_oracle_mode = false;
  int64_t start_delay =  ObRandom::rand(1, 100);
  // use start delay avoid diffient thread do gc on the same time
  lib::set_thread_name("ObXAGCWorker");
  {
    ObBKGDSessInActiveGuard inactive_guard;
    for (int64_t i = 0; i < start_delay && !has_set_stop(); ++i) {
      sleep(1);
    }
  }

  constexpr int64_t gc_interval_upper_bound = 24L * (3600L * 1000L * 1000L); //upper bound is 24h
  int64_t tmp_start_delay = GCONF._xa_gc_interval; // default is 3600 000 000
  int64_t gc_interval = std::max(2 * max_gc_cost_time_, tmp_start_delay);
  gc_interval = std::min(gc_interval, gc_interval_upper_bound);

  if (OB_SUCCESS != (tmp_ret = share::ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id,
          is_oracle_mode))) {
    TRANS_LOG(WARN, "check oracle mode failed", K(ret));
  } else {
    has_decided_mode = true;
  }
  while (!has_set_stop()) {
    if (is_user_tenant(tenant_id) && has_decided_mode) {
      int64_t gc_cost_time = 0;
      int64_t before_gc_ts = ObTimeUtil::current_time();
      if (before_gc_ts - last_scan_ts > gc_interval) {
        if (is_oracle_mode) {
          // oracle mode
          if (OB_FAIL(xa_service_->gc_invalid_xa_record(tenant_id))) {
            TRANS_LOG(WARN, "gc invalid xa record failed", K(ret), K(tenant_id),
                      K(gc_interval), K(last_scan_ts), K(before_gc_ts));
          } else {
            // update last scan ts
            last_scan_ts = ObTimeUtil::current_time();
            gc_cost_time = last_scan_ts - before_gc_ts; // compute gc cost
            max_gc_cost_time_ = std::max(max_gc_cost_time_, gc_cost_time);
            TRANS_LOG(INFO, "clean invalid records", K(tenant_id), K(ret), K(gc_cost_time));
          }
        } else {
          // mysql mode
          common::ObAddr leader_addr;
          if (OB_FAIL(MTL(ObTransService *)->get_location_adapter()->nonblock_get_leader(
                  GCONF.cluster_id, meta_tenant_id, meta_ls_id, leader_addr))) {
            TRANS_LOG(WARN, "get leader failed", K(ret));
          } else if (GCTX.self_addr() == leader_addr) {
            if (OB_FAIL(xa_service_->gc_record_for_mysql())) {
              TRANS_LOG(WARN, "gc record failed", K(ret));
            } else {
              // update last scan ts
              last_scan_ts = ObTimeUtil::current_time();
              gc_cost_time = last_scan_ts - before_gc_ts;
              max_gc_cost_time_ = std::max(max_gc_cost_time_, gc_cost_time);
              TRANS_LOG(INFO, "clean invalid records", K(ret), K(gc_cost_time));
            }
          }
        }
      }
    } else if (is_user_tenant(tenant_id) && !has_decided_mode) {
      if (OB_SUCCESS != (tmp_ret = share::ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id,
              is_oracle_mode))) {
        TRANS_LOG(WARN, "check oracle mode failed", K(ret));
      } else {
        has_decided_mode = true;
      }
    }
    //sleep 20 secnd whether gc succ or not
    {
      ObBKGDSessInActiveGuard inactive_guard;
      SLEEP(20);
    }
    // try refresh gc_interval,
    // if gc falied, and not update last_scan_ts, update gc_interval can be effective
    // gc_interval ～ [20s, 24h]
    tmp_start_delay = GCONF._xa_gc_interval;
    gc_interval = std::max(2 * max_gc_cost_time_, tmp_start_delay); // gc interval mini value is (2 * 10s)
    gc_interval = std::min(gc_interval, gc_interval_upper_bound);
  }
  return;
}

void ObXAInnerTableGCWorker::stop()
{
  TRANS_LOG(INFO, "XA gc worker thread stop");
  share::ObThreadPool::stop();
}

void ObXAInnerTableGCWorker::wait()
{
  TRANS_LOG(INFO, "XA gc worker thread wait");
  share::ObThreadPool::wait();
}

void ObXAInnerTableGCWorker::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    is_inited_ = false;
  }
}

}//transaction

}//oceanbase
