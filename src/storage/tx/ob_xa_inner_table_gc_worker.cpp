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
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server.h"
#include "share/rc/ob_tenant_base.h"

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

void ObXAInnerTableGCWorker::run1()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  int64_t last_scan_ts = ObTimeUtil::current_time();

  bool is_oracle_mode = false;
  int64_t start_delay =  ObRandom::rand(1, 100);

  // use start delay avoid diffient thread do gc on the same time
  lib::set_thread_name("ObXAGCWorker");
  for (int64_t i = 0; i < start_delay && !has_set_stop(); ++i) {
    sleep(1);
  }

  constexpr int64_t gc_interval_upper_bound = 24L * (3600L * 1000L * 1000L); //upper bound is 24h
  int64_t tmp_start_delay = GCONF._xa_gc_interval; // default is 3600 000 000
  int64_t gc_interval = std::max(2 * max_gc_cost_time_, tmp_start_delay);
  gc_interval = std::min(gc_interval, gc_interval_upper_bound);

  int64_t gc_cost_time = 0;
  int64_t before_gc_ts = 0;

  while (!has_set_stop()) {
    before_gc_ts = ObTimeUtil::current_time();
    if (before_gc_ts - last_scan_ts > gc_interval) {
      if (is_user_tenant(tenant_id)
          && OB_SUCC(share::ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))
          && is_oracle_mode) {
        if (OB_FAIL(xa_service_->gc_invalid_xa_record(tenant_id))) {
          TRANS_LOG(WARN, "gc invalid xa record failed", K(ret), K(tenant_id),
                    K(gc_interval), K(last_scan_ts), K(before_gc_ts));
        } else {
          // update last scan ts
          last_scan_ts = ObTimeUtil::current_time();
          gc_cost_time = last_scan_ts - before_gc_ts; // compute gc cost
          max_gc_cost_time_ = std::max(max_gc_cost_time_, gc_cost_time);
          TRANS_LOG(INFO, "scan xa inner table for one round", K(tenant_id), K(ret), K(gc_cost_time));
        }
      } else {
        last_scan_ts = before_gc_ts;
      }
    }
    //sleep 20 secnd whether gc succ or not
    SLEEP(20);
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
