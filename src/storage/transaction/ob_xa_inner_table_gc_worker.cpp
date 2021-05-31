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
#include "ob_trans_service.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server.h"

namespace oceanbase {

using namespace common;

namespace transaction {

int ObXAInnerTableGCWorker::init(ObTransService* txs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(txs)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(txs));
  } else {
    txs_ = txs;
    is_inited_ = true;
  }
  return ret;
}

int ObXAInnerTableGCWorker::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "xa gc worker not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    TRANS_LOG(ERROR, "XA gc worker thread start error", K(ret));
  } else {
    TRANS_LOG(INFO, "XA gc worker thread start");
  }
  return ret;
}

void ObXAInnerTableGCWorker::run1()
{
  int ret = OB_SUCCESS;
  omt::TenantIdList tenant_id_list;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t last_scan_ts = 0;
  // const int64_t INTERVAL_SECONDS = 60 * 60; //1 hour
  int64_t random_start_delay = ObRandom::rand(1, GCONF._xa_gc_interval);
  random_start_delay = (random_start_delay > 1000000) ? (random_start_delay / 1000000) : 1;

  for (int64_t i = 0; i < random_start_delay && !has_set_stop(); ++i) {
    sleep(1);
  }

  while (!has_set_stop()) {
    if (ObTimeUtil::fast_current_time() - last_scan_ts > GCONF._xa_gc_interval) {
      tenant_id_list.reset();
      GCTX.omt_->get_tenant_ids(tenant_id_list);
      for (int64_t i = 0; i < tenant_id_list.size() && !has_set_stop(); ++i) {
        tenant_id = tenant_id_list.at(i);
        if ((OB_SYS_TENANT_ID != tenant_id) && !is_virtual_tenant_id(tenant_id) &&
            OB_FAIL(txs_->gc_invalid_xa_record(tenant_id))) {
          TRANS_LOG(WARN, "gc invalid xa record failed", K(ret), K(tenant_id));
        }
      }
      TRANS_LOG(INFO, "scan xa inner table for one round");
      last_scan_ts = ObTimeUtil::fast_current_time();
    } else {
      sleep(1);  // 1 seconds
    }
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

}  // namespace transaction

}  // namespace oceanbase
