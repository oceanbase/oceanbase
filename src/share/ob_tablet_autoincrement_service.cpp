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
      if (OB_FAIL(ret)) {
        const bool force_refresh_leader = OB_NOT_MASTER == ret || OB_LS_NOT_EXIST == ret || OB_TABLET_NOT_EXIST == ret || OB_TENANT_NOT_IN_SERVER == ret;
        if (is_retryable(ret)) {
          ob_usleep<common::ObWaitEventIds::STORAGE_AUTOINC_FETCH_RETRY_SLEEP>(RETRY_INTERVAL);
          res.reset();
          if (OB_FAIL(THIS_WORKER.check_status())) { // overwrite ret
            LOG_WARN("failed to check status", K(ret));
          }
        }
        if (OB_SUCC(ret) && force_refresh_leader) {
          if (OB_FAIL(location_service->get(param.tenant_id_, tablet_id, INT64_MAX/*expire_renew_time*/, is_cache_hit, arg.ls_id_))) {
            LOG_WARN("fail to get log stream id", K(ret), K(ret), K(tablet_id));
          } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id,
                                                          param.tenant_id_,
                                                          arg.ls_id_,
                                                          true/*force_renew*/,
                                                          leader_addr))) {
            LOG_WARN("force get leader failed", K(ret), K(ret), K(arg.ls_id_));
          }
        } else {
          (void)location_service->renew_tablet_location(param.tenant_id_, tablet_id, ret, true/*is_nonblock*/);
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
  lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "AutoincMgr");
  SET_USE_500(attr);
  if (OB_FAIL(node_allocator_.init(sizeof(ObTabletAutoincMgr), ObModIds::OB_AUTOINCREMENT))) {
    LOG_WARN("failed to init table node allocator", K(ret));
  } else if (OB_FAIL(tablet_autoinc_mgr_map_.init(attr))) {
    LOG_WARN("failed to init table node map", K(ret));
  } else {
    for (int64_t i = 0; i < INIT_NODE_MUTEX_NUM; ++i) {
      init_node_mutexs_[i].set_latch_id(common::ObLatchIds::TABLET_AUTO_INCREMENT_SERVICE_LOCK);
    }
    is_inited_ = true;
  }
  return ret;
}

void ObTabletAutoincrementService::destroy()
{
  tablet_autoinc_mgr_map_.destroy();
  node_allocator_.destroy();
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
}
}
