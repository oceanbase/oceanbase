/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RS

#include "rootserver/mview/ob_mview_mds_op_task.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"

namespace oceanbase {
namespace rootserver {

ObMViewMdsOpTask::ObMViewMdsOpTask()
  : is_inited_(false),
    in_sched_(false),
    is_stop_(true),
    tenant_id_(OB_INVALID_TENANT_ID),
    last_sched_ts_(0)
{
}

ObMViewMdsOpTask::~ObMViewMdsOpTask() {}

int ObMViewMdsOpTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewMdsOpTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = MTL_ID();
    is_inited_ = true;
  }
  return ret;
}

int ObMViewMdsOpTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMdsOpTask not init", KR(ret), KP(this));
  } else {
    is_stop_ = false;
    if (!in_sched_ && OB_FAIL(schedule_task(MVIEW_MDS_OP_INTERVAL, true /*repeat*/))) {
      LOG_WARN("fail to schedule ObMViewMdsOpTask", KR(ret));
    } else {
      in_sched_ = true;
    }
  }
  return ret;
}

void ObMViewMdsOpTask::stop()
{
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
}

void ObMViewMdsOpTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
  wait_task();
  tenant_id_ = OB_INVALID_TENANT_ID;
  last_sched_ts_ = 0;
}

void ObMViewMdsOpTask::wait() { wait_task(); }

int ObMViewMdsOpTask::update_mview_mds_op()
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id(share::ObLSID::SYS_LS_ID);
  ObLSHandle ls_handle;
  transaction::ObLSTxCtxIterator ls_tx_ctx_iter;
  ObMViewMaintenanceService::MViewMdsOpMap &mview_mds_map = MTL(ObMViewMaintenanceService*)->get_mview_mds_op();
  hash::ObHashSet<transaction::ObTransID> tx_set;
  int64_t start_ts = ObTimeUtil::current_time();
  if (OB_FAIL(tx_set.create(32))) {
    LOG_WARN("create tx set failed", KR(ret));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get_ls failed", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->iterate_tx_ctx(ls_tx_ctx_iter))) {
    LOG_WARN("construt tx_ctx iter failed", KR(ret));
  } else {
    transaction::ObPartTransCtx *tx_ctx = nullptr;
    while (OB_SUCC(ret)) {
      ObMViewOpArg arg;
      bool need_collect = false;
      if (OB_FAIL(ls_tx_ctx_iter.get_next_tx_ctx(tx_ctx))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(tx_ctx->collect_mview_mds_op(need_collect, arg))) {
        LOG_WARN("collect mview mds failed", KR(ret));
      } else if (need_collect && OB_FAIL(mview_mds_map.set_refactored(tx_ctx->get_trans_id(), arg, 1))) {
        LOG_WARN("set mview mds failed", KR(ret));
      } else if (need_collect && OB_FAIL(tx_set.set_refactored(tx_ctx->get_trans_id()))) {
        LOG_WARN("set mview mds tx_id failed", KR(ret));
      }
      if (OB_NOT_NULL(tx_ctx)) {
        ls_tx_ctx_iter.revert_tx_ctx(tx_ctx);
        tx_ctx = nullptr;
      }
    }
  }

  ObSEArray<transaction::ObTransID, 2> del_tx_id;
  LOG_INFO("mview_mds_op", "tx_count", tx_set.size());
  for (ObMViewMaintenanceService::MViewMdsOpMap::iterator it = mview_mds_map.begin();OB_SUCC(ret) && it != mview_mds_map.end(); it++) {
    if (OB_FAIL(tx_set.exist_refactored(it->first))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("mview_mds_op", "txid", it->first, "mds", it->second, "cost", start_ts - it->second.read_snapshot_/1000);
      } else if (OB_HASH_NOT_EXIST) {
        if (OB_FAIL(del_tx_id.push_back(it->first))) {
          LOG_WARN("del_tx_id push failed", KR(ret), K(it->first));
        }
      } else {
        LOG_WARN("check tx_id failed", KR(ret), K(it->first));
      }
    }
  }
  for (int64_t idx = 0; idx < del_tx_id.count() && OB_SUCC(ret); idx++) {
    if (OB_FAIL(mview_mds_map.erase_refactored(del_tx_id.at(idx))))  {
      LOG_WARN("erash tx_id failed", KR(ret), K(del_tx_id.at(idx)));
    }
  }
  int64_t end_ts = ObTimeUtil::current_time();
  LOG_INFO("update_mview_mds", KR(ret), K(tx_set), K(del_tx_id), "cost", end_ts - start_ts);
  return ret;
}

void ObMViewMdsOpTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool need_sched = false;
  int64_t curr_ts = ObTimeUtil::current_time();
  if (last_sched_ts_ <= MTL(ObMViewMaintenanceService*)->get_mview_mds_ts()) {
    need_sched = true;
  } else if (curr_ts - last_sched_ts_ > 60 * 1000 * 1000) {
    need_sched = true;
  }
  if (!need_sched) {
  } else if (OB_FAIL(update_mview_mds_op())) {
    LOG_WARN("update_mview_mds_op failed", KR(ret));
  } else {
    last_sched_ts_ = curr_ts;
  }
}

} // namespace rootserver
} // namespace oceanbase
