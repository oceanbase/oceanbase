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

#define USING_LOG_PREFIX SHARE

#include "ob_tablet_autoincrement_service.h"
#include "logservice/ob_log_service.h"
#include "rootserver/ob_root_service.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_rpc_struct.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/scn.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace share
{

int ObTabletAutoincMgr::init(const common::ObTabletID &tablet_id, const int64_t cache_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet autoinc mgr init twice", K_(is_inited), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    cache_size_ = cache_size;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletAutoincMgr::set_interval(const ObTabletAutoincParam &param, ObTabletCacheInterval &interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet autoinc mgr is not inited", K(ret));
  } else if (next_value_ + interval.cache_size_ - 1 > curr_node_.cache_end_) {
    if (prefetch_node_.is_valid()) {
      curr_node_.cache_start_ = prefetch_node_.cache_start_;
      curr_node_.cache_end_ = prefetch_node_.cache_end_;
      prefetch_node_.reset();
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }

  if (OB_SUCC(ret)) {
    if (next_value_ < curr_node_.cache_start_) {
      next_value_ = curr_node_.cache_start_;
    }
    const uint64_t start = next_value_;
    const uint64_t end = MIN(curr_node_.cache_end_, start + interval.cache_size_ - 1);
    next_value_ = end + 1;
    interval.set(start, end);
  }
  return ret;
}

int ObTabletAutoincMgr::fetch_interval(const ObTabletAutoincParam &param, ObTabletCacheInterval &interval) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet autoinc mgr is not inited", K(ret));
  } else {
    const int64_t TRY_LOCK_INTERVAL = 1000L; // 1ms
    while (true) {
      if (OB_SUCCESS != mutex_.trylock()) {
        ob_usleep<common::ObWaitEventIds::STORAGE_AUTOINC_FETCH_CONFLICT_SLEEP>(TRY_LOCK_INTERVAL);
        THIS_WORKER.sched_run();
      } else {
        break;
      }
    }
    last_refresh_ts_ = ObTimeUtility::current_time();
    // TODO(shuangcan): may need to optimize the lock performance here
    if (OB_SUCC(set_interval(param, interval))) {
      if (prefetch_condition()) {
        if (OB_FAIL(fetch_new_range(param, tablet_id_, prefetch_node_))) {
          LOG_WARN("failed to prefetch tablet node", K(param), K(ret));
        }
      }
    } else if (OB_SIZE_OVERFLOW == ret) {
      if (OB_FAIL(fetch_new_range(param, tablet_id_, curr_node_))) {
        LOG_WARN("failed to fetch tablet node", K(param), K(ret));
      } else if (OB_FAIL(set_interval(param, interval))) {
        LOG_WARN("failed to alloc cache handle", K(param), K(ret));
      }
    }
    mutex_.unlock();
  }
  return ret;
}

int ObTabletAutoincMgr::fetch_interval_without_cache(const ObTabletAutoincParam &param, ObTabletCacheInterval &interval) {
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  ObTabletCacheNode node;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet autoinc mgr is not inited", K(ret));
  } else if (OB_FAIL(fetch_new_range(param, tablet_id_, node))) {
    LOG_WARN("failed to fetch tablet node", K(param), K(ret));
  } else {
    interval.set(node.cache_start_, node.cache_end_);
  }
  return ret;
}

int ObTabletAutoincMgr::fetch_new_range(const ObTabletAutoincParam &param,
                                        const common::ObTabletID &tablet_id,
                                        ObTabletCacheNode &node)
{
  int ret = OB_SUCCESS;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  share::ObLocationService *location_service = nullptr;
  ObAddr leader_addr;
  ObLSID ls_id;
  bool is_cache_hit = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet auto increment service is not inited", K(ret), K(param), K(tablet_id));
  } else if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)
      || OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root service or location_cache is null", K(ret), KP(srv_rpc_proxy), KP(location_service));
  } else if (OB_FAIL(location_service->get(param.tenant_id_, tablet_id, 0/*expire_renew_time*/, is_cache_hit, ls_id))) {
    LOG_WARN("fail to get log stream id", K(ret), K(tablet_id));
  // try to use location cache first, if the cache is wrong, try force renew.
  } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id,
                                                  param.tenant_id_,
                                                  ls_id,
                                                  false,/*force_renew*/
                                                  leader_addr))) {
    LOG_WARN("get leader failed", K(ret), K(ls_id));
  } else {
    obrpc::ObFetchTabletSeqArg arg;
    obrpc::ObFetchTabletSeqRes res;
    arg.cache_size_ = MAX(cache_size_, param.auto_increment_cache_size_); // TODO(shuangcan): confirm this
    arg.tenant_id_ = param.tenant_id_;
    arg.tablet_id_ = tablet_id;
    arg.ls_id_ = ls_id;

    bool finish = false;
    for (int64_t retry_times = 0; OB_SUCC(ret) && !finish; retry_times++) {
      if (OB_FAIL(srv_rpc_proxy->to(leader_addr).fetch_tablet_autoinc_seq_cache(arg, res))) {
        LOG_WARN("fail to fetch autoinc cache for tablets", K(ret), K(retry_times), K(arg));
      } else {
        finish = true;
      }
      if (OB_FAIL(ret) && is_retryable(ret)) {
        const bool need_refresh_leader = OB_NOT_MASTER == ret;
        ob_usleep<common::ObWaitEventIds::STORAGE_AUTOINC_FETCH_RETRY_SLEEP>(RETRY_INTERVAL);
        res.reset();
        if (OB_FAIL(THIS_WORKER.check_status())) { // overwrite ret
          LOG_WARN("failed to check status", K(ret));
        } else if (need_refresh_leader) {
          if (OB_FAIL(location_service->get(param.tenant_id_, tablet_id, INT64_MAX/*expire_renew_time*/, is_cache_hit, ls_id))) {
            LOG_WARN("fail to get log stream id", K(ret), K(ret), K(tablet_id));
          } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id,
                                                          param.tenant_id_,
                                                          ls_id,
                                                          true/*force_renew*/,
                                                          leader_addr))) {
            LOG_WARN("force get leader failed", K(ret), K(ret), K(ls_id));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      node.cache_start_ = res.cache_interval_.start_;
      node.cache_end_ = res.cache_interval_.end_;
      if (node.cache_end_ == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get autoinc cache", K(ret));
      } else if (tablet_id.is_user_normal_rowid_table_tablet() && node.cache_end_ > OB_MAX_AUTOINC_SEQ_VALUE) {
        ret = OB_HEAP_TABLE_EXAUSTED;
        LOG_DBA_ERROR(OB_HEAP_TABLE_EXAUSTED, "msg", "The hidden primary key sequence has exhausted", K(tablet_id), "current_seq", node.cache_end_);
        LOG_WARN("tablet autoinc seq has reached max", K(ret), K(node));
      } else {
        LOG_INFO("fetch new range success", K(tablet_id), K(node));
      }
    }
  }


  return ret;
}

ObTabletAutoincrementService::ObTabletAutoincrementService()
  : is_inited_(false), node_allocator_(), tablet_autoinc_mgr_map_(), init_node_mutexs_()
{
}

ObTabletAutoincrementService::~ObTabletAutoincrementService()
{
}

int ObTabletAutoincrementService::acquire_mgr(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    const int64_t init_cache_size,
    ObTabletAutoincMgr *&autoinc_mgr)
{
  int ret = OB_SUCCESS;
  ObTabletAutoincKey key;
  key.tenant_id_ = tenant_id;
  key.tablet_id_ = tablet_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet auto increment service is not inited", K(ret), K(key));
  } else if (OB_UNLIKELY(!key.is_valid() || nullptr != autoinc_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(tablet_autoinc_mgr_map_.get(key, autoinc_mgr))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get from map failed", K(ret));
    } else {
      lib::ObMutex &mutex = init_node_mutexs_[key.tablet_id_.id() % INIT_NODE_MUTEX_NUM];
      lib::ObMutexGuard guard(mutex);
      if (OB_ENTRY_NOT_EXIST == (ret = tablet_autoinc_mgr_map_.get(key, autoinc_mgr))) {
        if (NULL == (autoinc_mgr = op_alloc(ObTabletAutoincMgr))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc table mgr", K(ret));
        } else if (OB_FAIL(autoinc_mgr->init(key.tablet_id_, init_cache_size))) {
          LOG_WARN("fail to init tablet autoinc mgr", K(ret), K(key));
        } else if (OB_FAIL(tablet_autoinc_mgr_map_.insert_and_get(key, autoinc_mgr))) {
          LOG_WARN("failed to create table node", K(ret));
        }
        if (OB_FAIL(ret) && autoinc_mgr != nullptr) {
          op_free(autoinc_mgr);
          autoinc_mgr = nullptr;
        }
      }
    }
  }
  return ret;
}

void ObTabletAutoincrementService::release_mgr(ObTabletAutoincMgr *autoinc_mgr)
{
  tablet_autoinc_mgr_map_.revert(autoinc_mgr);
  return;
}

int ObTabletAutoincrementService::get_autoinc_seq(const uint64_t tenant_id, const common::ObTabletID &tablet_id, uint64_t &autoinc_seq)
{
  int ret = OB_SUCCESS;
  const int64_t auto_increment_cache_size = 10000; //TODO(shuangcan): fix me
  ObTabletAutoincParam param;
  param.tenant_id_ = tenant_id;
  ObTabletAutoincMgr *autoinc_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet auto increment service is not inited", K(ret));
  } else if (OB_FAIL(acquire_mgr(tenant_id, tablet_id, auto_increment_cache_size, autoinc_mgr))) {
    LOG_WARN("failed to acquire mgr", K(ret));
  } else {
    ObTabletCacheInterval interval(tablet_id, 1/*cache size*/);
    lib::ObMutex &mutex = init_node_mutexs_[tablet_id.id() % INIT_NODE_MUTEX_NUM];
    lib::ObMutexGuard guard(mutex);
    lib::DisableSchedInterGuard sched_guard;
    if (OB_ISNULL(autoinc_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("autoinc mgr is unexpected null", K(ret));
    } else if (OB_FAIL(autoinc_mgr->fetch_interval(param, interval))) {
      LOG_WARN("fail to fetch interval", K(ret), K(param));
    } else if (OB_FAIL(interval.next_value(autoinc_seq))) {
      LOG_WARN("fail to get next value", K(ret));
    }
  }
  if (nullptr != autoinc_mgr) {
    release_mgr(autoinc_mgr);
  }
  return ret;
}

ObTabletAutoincrementService &ObTabletAutoincrementService::get_instance()
{
  static ObTabletAutoincrementService autoinc_service;
  return autoinc_service;
}

int ObTabletAutoincrementService::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(node_allocator_.init(sizeof(ObTabletAutoincMgr), ObModIds::OB_AUTOINCREMENT))) {
    LOG_WARN("failed to init table node allocator", K(ret));
  } else if (OB_FAIL(tablet_autoinc_mgr_map_.init())) {
    LOG_WARN("failed to init table node map", K(ret));
  } else {
    for (int64_t i = 0; i < INIT_NODE_MUTEX_NUM; ++i) {
      init_node_mutexs_[i].set_latch_id(common::ObLatchIds::TABLET_AUTO_INCREMENT_SERVICE_LOCK);
    }
    is_inited_ = true;
  }
  return ret;
}

int ObTabletAutoincrementService::get_tablet_cache_interval(const uint64_t tenant_id,
                                                            ObTabletCacheInterval &interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet auto increment service is not inited", K(ret));
  } else {
    const int64_t auto_increment_cache_size = MAX(interval.cache_size_, 10000); //TODO(shuangcan): fix me
    ObTabletAutoincParam param;
    param.tenant_id_ = tenant_id;
    param.auto_increment_cache_size_ = auto_increment_cache_size;
    ObTabletAutoincMgr *autoinc_mgr = nullptr;
    if (OB_FAIL(acquire_mgr(tenant_id, interval.tablet_id_, auto_increment_cache_size, autoinc_mgr))) {
      LOG_WARN("failed to acquire mgr", K(ret));
    } else if (OB_ISNULL(autoinc_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("autoinc mgr is unexpected null", K(ret));
    } else if (OB_FAIL(autoinc_mgr->fetch_interval_without_cache(param, interval))) {
      LOG_WARN("fail to fetch interval", K(ret), K(param));
    }
    if (nullptr != autoinc_mgr) {
      release_mgr(autoinc_mgr);
    }
  }

  return ret;
}

ObTabletAutoincSeqRpcHandler::ObTabletAutoincSeqRpcHandler()
  : is_inited_(false), bucket_lock_()
{
}

ObTabletAutoincSeqRpcHandler::~ObTabletAutoincSeqRpcHandler()
{
}

ObTabletAutoincSeqRpcHandler &ObTabletAutoincSeqRpcHandler::get_instance()
{
  static ObTabletAutoincSeqRpcHandler autoinc_rpc_handler;
  return autoinc_rpc_handler;
}

int ObTabletAutoincSeqRpcHandler::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(bucket_lock_.init(BUCKET_LOCK_BUCKET_CNT))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTabletAutoincSeqRpcHandler::fetch_tablet_autoinc_seq_cache(
    const ObFetchTabletSeqArg &arg,
    ObFetchTabletSeqRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle;
      share::ObLSID ls_id = arg.ls_id_;
      ObRole role = common::INVALID_ROLE;
      ObTabletHandle tablet_handle;
      ObTabletAutoincInterval autoinc_interval;
      const ObTabletID &tablet_id = arg.tablet_id_;
      int64_t proposal_id = -1;
      ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
      if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(ls_id, role, proposal_id))) {
        LOG_WARN("get palf role failed", K(ret));
      } else if (!is_strong_leader(role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("follwer received FetchTabletsSeq rpc", K(ret), K(ls_id));
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(ls_id));
      } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle))) {
        LOG_WARN("failed to get tablet", KR(ret), K(arg));
      } else if (OB_FAIL(tablet_handle.get_obj()->fetch_tablet_autoinc_seq_cache(
          arg.cache_size_, autoinc_interval))) {
        LOG_WARN("failed to fetch tablet autoinc seq on tablet", K(ret), K(tablet_id));
      } else {
        res.cache_interval_ = autoinc_interval;
      }
    }
  }
  return ret;
}

int ObTabletAutoincSeqRpcHandler::batch_get_tablet_autoinc_seq(
    const obrpc::ObBatchGetTabletAutoincSeqArg &arg,
    obrpc::ObBatchGetTabletAutoincSeqRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle;
      share::ObLSID ls_id = arg.ls_id_;
      ObRole role = common::INVALID_ROLE;
      int64_t proposal_id = -1;
      if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(ls_id, role, proposal_id))) {
        LOG_WARN("get palf role failed", K(ret));
      } else if (!is_strong_leader(role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("follwer received FetchTabletsSeq rpc", K(ret), K(ls_id));
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(ls_id));
      } else if (OB_FAIL(res.autoinc_params_.reserve(arg.src_tablet_ids_.count()))) {
        LOG_WARN("failed to reserve autoinc param", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.src_tablet_ids_.count(); i++) {
          int tmp_ret = OB_SUCCESS;
          const ObTabletID &src_tablet_id = arg.src_tablet_ids_.at(i);
          ObTabletHandle tablet_handle;
          share::ObMigrateTabletAutoincSeqParam autoinc_param;
          autoinc_param.src_tablet_id_ = src_tablet_id;
          autoinc_param.dest_tablet_id_ = arg.dest_tablet_ids_.at(i);
          ObBucketHashRLockGuard lock_guard(bucket_lock_, src_tablet_id.hash());
          ObTabletAutoincSeq autoinc_seq;
          if (OB_TMP_FAIL(ls_handle.get_ls()->get_tablet(src_tablet_id, tablet_handle))) {
            LOG_WARN("failed to get tablet", K(tmp_ret), K(src_tablet_id));
          } else if (OB_TMP_FAIL(tablet_handle.get_obj()->get_latest_autoinc_seq(autoinc_seq))) {
            LOG_WARN("failed to get latest autoinc seq", K(tmp_ret));
          } else if (OB_TMP_FAIL(autoinc_seq.get_autoinc_seq_value(autoinc_param.autoinc_seq_))) {
            LOG_WARN("failed to get autoinc seq value", K(tmp_ret));
          }
          autoinc_param.ret_code_ = tmp_ret;
          if (OB_FAIL(res.autoinc_params_.push_back(autoinc_param))) {
            LOG_WARN("failed to push autoinc param", K(ret), K(autoinc_param));
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletAutoincSeqRpcHandler::batch_set_tablet_autoinc_seq(
    const obrpc::ObBatchSetTabletAutoincSeqArg &arg,
    obrpc::ObBatchSetTabletAutoincSeqRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(res.autoinc_params_.assign(arg.autoinc_params_))) {
    LOG_WARN("failed to assign autoinc params", K(ret), K(arg));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      share::ObLSID ls_id = arg.ls_id_;
      ObRole role = common::INVALID_ROLE;
      int64_t proposal_id = -1;
      if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(ls_id, role, proposal_id))) {
        LOG_WARN("get palf role failed", K(ret));
      } else if (!is_strong_leader(role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("follwer received FetchTabletsSeq rpc", K(ret), K(ls_id));
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(ls_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < res.autoinc_params_.count(); i++) {
          int tmp_ret = OB_SUCCESS;
          ObTabletHandle tablet_handle;
          share::ObMigrateTabletAutoincSeqParam &autoinc_param = res.autoinc_params_.at(i);
          ObBucketHashWLockGuard lock_guard(bucket_lock_, autoinc_param.dest_tablet_id_.hash());
          if (OB_TMP_FAIL(ls_handle.get_ls()->get_tablet(autoinc_param.dest_tablet_id_, tablet_handle))) {
            LOG_WARN("failed to get tablet", K(tmp_ret), K(autoinc_param));
          } else if (OB_TMP_FAIL(tablet_handle.get_obj()->update_tablet_autoinc_seq(autoinc_param.autoinc_seq_,
                                                                                    SCN::max_scn()))) {
            LOG_WARN("failed to update tablet autoinc seq", K(tmp_ret), K(autoinc_param));
          }
          autoinc_param.ret_code_ = tmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObTabletAutoincSeqRpcHandler::replay_update_tablet_autoinc_seq(
    const ObLS *ls,
    const ObTabletID &tablet_id,
    const uint64_t autoinc_seq,
    const SCN &replay_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ls == nullptr || !tablet_id.is_valid() || autoinc_seq == 0 || !replay_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(autoinc_seq), K(replay_scn));
  } else {
    ObTabletHandle tablet_handle;
    ObBucketHashWLockGuard guard(bucket_lock_, tablet_id.hash());
    if (OB_FAIL(ls->replay_get_tablet(tablet_id,
                                      replay_scn,
                                      tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("tablet may be deleted, skip this log", K(ret), K(tablet_id), K(replay_scn));
        ret = OB_SUCCESS;
      } else if (OB_EAGAIN == ret) {
        // retry replay again
      } else {
        LOG_WARN("fail to replay get tablet, retry again", K(ret), K(tablet_id), K(replay_scn));
        ret = OB_EAGAIN;
      }
    } else if (OB_FAIL(tablet_handle.get_obj()->update_tablet_autoinc_seq(autoinc_seq, replay_scn))) {
      LOG_WARN("failed to update tablet auto inc seq", K(ret), K(autoinc_seq), K(replay_scn));
    }
  }
  return ret;
}

}
}
