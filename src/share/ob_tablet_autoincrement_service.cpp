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
#include "storage/ob_tablet_autoinc_seq_rpc_handler.h"

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

int ObTabletAutoincMgr::clear_cache_if_fallback_for_mlog(
    const uint64_t current_value)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet autoinc mgr is not inited", K(ret));
  } else {
    const int64_t TRY_LOCK_INTERVAL = 1000L; // 1ms
    uint64_t cache_value = 0;

    while (true) {
      if (OB_SUCCESS != mutex_.trylock()) {
        ob_usleep<common::ObWaitEventIds::STORAGE_AUTOINC_FETCH_CONFLICT_SLEEP>(TRY_LOCK_INTERVAL);
        THIS_WORKER.sched_run();
      } else {
        break;
      }
    }

    if (prefetch_node_.is_valid() && curr_node_.is_valid()
        && curr_node_.cache_end_ + 1 !=  prefetch_node_.cache_start_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("curr_node_ and prefetch_node_ is invalid", KPC(this));
    }

    if (OB_FAIL(ret)) {
    } else if (prefetch_node_.is_valid()) {
      cache_value = prefetch_node_.cache_end_;
    } else if (curr_node_.is_valid()) {
      cache_value = curr_node_.cache_end_;
    }

    if (OB_FAIL(ret)) {
    } else if (0 == cache_value) {
      LOG_INFO("inc cache is empty, skip check", KPC(this));
    } else if (cache_value + 1 < current_value) {
      LOG_INFO("auto inc seq fallback, need clear cache", K(ret), KPC(this), K(current_value));
      curr_node_.reset();
      prefetch_node_.reset();
      next_value_ = 1;
    } else if (cache_value + 1 > current_value) {
    }

    mutex_.unlock();
  }
  return ret;
}

int ObTabletAutoincMgr::fetch_interval(const ObTabletAutoincParam &param, ObTabletCacheInterval &interval)
{
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

int ObTabletAutoincMgr::fetch_interval_without_cache(const ObTabletAutoincParam &param, ObTabletCacheInterval &interval)
{
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
  bool is_cache_hit = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet auto increment service is not inited", K(ret), K(param), K(tablet_id));
  } else if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)
      || OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root service or location_cache is null", K(ret), KP(srv_rpc_proxy), KP(location_service));
  } else {
    obrpc::ObFetchTabletSeqArg arg;
    obrpc::ObFetchTabletSeqRes res;
    arg.cache_size_ = MAX(cache_size_, param.auto_increment_cache_size_); // TODO(shuangcan): confirm this
    arg.tenant_id_ = param.tenant_id_;
    arg.tablet_id_ = tablet_id;
    // arg.ls_id_ will be filled by location_service->get

    bool finish = false;
    for (int64_t retry_times = 0; OB_SUCC(ret) && !finish; retry_times++) {
      const int64_t rpc_timeout = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : obrpc::ObRpcProxy::MAX_RPC_TIMEOUT;
      if (OB_FAIL(location_service->get(param.tenant_id_, tablet_id, 0/*expire_renew_time*/, is_cache_hit, arg.ls_id_))) {
        LOG_WARN("fail to get log stream id", K(ret), K(tablet_id));
      } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id,
                                                      param.tenant_id_,
                                                      arg.ls_id_,
                                                      false,/*force_renew*/
                                                      leader_addr))) {
        LOG_WARN("get leader failed", K(ret), K(arg.ls_id_));
      } else if (GCTX.self_addr() == leader_addr) {
        if (OB_FAIL(ObTabletAutoincSeqRpcHandler::get_instance().fetch_tablet_autoinc_seq_cache(arg, res))) {
          LOG_WARN("fail to fetch autoinc cache for tablets", K(ret), K(retry_times), K(arg), K(rpc_timeout));
        }
      } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr).by(param.tenant_id_).timeout(rpc_timeout).fetch_tablet_autoinc_seq_cache(arg, res))) {
        LOG_WARN("fail to fetch autoinc cache for tablets", K(ret), K(retry_times), K(arg), K(rpc_timeout));
      }
      if (OB_SUCC(ret)) {
        finish = true;
      }
      if (OB_FAIL(ret)) {
        (void)location_service->renew_tablet_location(param.tenant_id_, tablet_id, ret, !is_block_renew_location(ret)/*is_nonblock*/);
        if (is_retryable(ret)) {
          // overwrite ret
          if (OB_UNLIKELY(rpc_timeout <= 0)) {
            ret = OB_TIMEOUT;
            LOG_WARN("timeout", K(ret), K(rpc_timeout));
          } else if (OB_FAIL(THIS_WORKER.check_status())) {
            LOG_WARN("failed to check status", K(ret));
          } else {
            res.reset();
            ob_usleep<common::ObWaitEventIds::STORAGE_AUTOINC_FETCH_RETRY_SLEEP>(RETRY_INTERVAL);
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
        if (OB_FAIL(tablet_autoinc_mgr_map_.alloc_value(autoinc_mgr))) {
          LOG_WARN("failed to alloc table mgr", K(ret));
        } else if (OB_FAIL(autoinc_mgr->init(key.tablet_id_, init_cache_size))) {
          LOG_WARN("fail to init tablet autoinc mgr", K(ret), K(key));
        } else if (OB_FAIL(tablet_autoinc_mgr_map_.insert_and_get(key, autoinc_mgr))) {
          LOG_WARN("failed to create table node", K(ret));
        }
        if (OB_FAIL(ret) && autoinc_mgr != nullptr) {
          tablet_autoinc_mgr_map_.free_value(autoinc_mgr);
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

int ObTabletAutoincrementService::get_autoinc_seq(const uint64_t tenant_id, const common::ObTabletID &tablet_id, uint64_t &autoinc_seq, const int64_t auto_increment_cache_size)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_sequence_load);
  int ret = OB_SUCCESS;
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

int ObTabletAutoincrementService::get_autoinc_seq_for_mlog(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    uint64_t &autoinc_seq)
{
  int ret = OB_SUCCESS;
  const int64_t auto_increment_cache_size = ObTabletAutoincrementService::DEFAULT_CACHE_SIZE;
  ObTabletAutoincParam param;
  param.tenant_id_ = tenant_id;
  ObTabletAutoincMgr *autoinc_mgr = nullptr;

  uint64_t current_value = 0;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  bool is_committed = true;
  bool seq_impossible_fallback =  false;

  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "FetchAutoSeq"));
  ObTabletAutoincSeq tmp_autoinc_seq;
  mds::MdsWriter writer;// will be removed later
  mds::TwoPhaseCommitState trans_stat;// will be removed later
  share::SCN trans_version;// will be removed later

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet auto increment service is not inited", K(ret));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ls_service or log_service", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls", K(ret), K(ls_id));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id));
  }

  if (OB_SUCC(ret)) {
    bool need_retry = false;
    const int64_t abs_timeout_us = THIS_WORKER.is_timeout_ts_valid() ?
      THIS_WORKER.get_timeout_ts() : ObTimeUtility::current_time() + GCONF.rpc_timeout;
    do {
      need_retry = false;
      if (OB_FAIL(tablet_handle.get_obj()->get_latest_autoinc_seq(tmp_autoinc_seq, allocator, writer, trans_stat, trans_version))) {
        if (OB_EMPTY_RESULT == ret) {
          seq_impossible_fallback = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get auto inc seq", K(ret));
        }
      } else if (OB_UNLIKELY(mds::TwoPhaseCommitState::ON_COMMIT != trans_stat)) {
        if (ObTimeUtility::current_time() > abs_timeout_us) {
          ret = OB_TIMEOUT;
          LOG_WARN("get auto inc timeout", K(ret), K(abs_timeout_us));
        } else {
          need_retry = true;
          usleep(100);
        }
      } else if (OB_FAIL(tmp_autoinc_seq.get_autoinc_seq_value(current_value))) {
        LOG_WARN("failed to get autoinc seq value", K(ret), K(tmp_autoinc_seq));
      }
    } while (need_retry);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(acquire_mgr(tenant_id, tablet_id, auto_increment_cache_size, autoinc_mgr))) {
    LOG_WARN("failed to acquire mgr", K(ret));
  } else {
    ObTabletCacheInterval interval(tablet_id, 1/*cache size*/);
    if (OB_ISNULL(autoinc_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("autoinc mgr is unexpected null", K(ret));
    } else if (!seq_impossible_fallback
               && OB_FAIL(autoinc_mgr->clear_cache_if_fallback_for_mlog(current_value))) {
      LOG_WARN("failed to clear_cache_if_fallback_for_mlog", K(ret));
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

int ObTabletAutoincrementService::clear_tablet_autoinc_seq_cache(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet auto increment service is not inited", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    ObTabletAutoincKey key;
    key.tenant_id_ = tenant_id;
    key.tablet_id_ = tablet_ids.at(i);
    lib::ObMutex &mutex = init_node_mutexs_[key.tablet_id_.id() % INIT_NODE_MUTEX_NUM];
    lib::ObMutexGuardWithTimeout guard(mutex, abs_timeout_us);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("failed to lock", K(ret));
    } else if (OB_FAIL(tablet_autoinc_mgr_map_.del(key))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to del tablet autoinc", K(ret), K(key));
      }
    }
  }
  return ret;
}

int ObTabletAutoincCacheCleaner::add_single_table(const schema::ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_table_with_hidden_pk_column() || table_schema.is_aux_lob_meta_table()) {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    ObArray<ObTabletID> tablet_ids;
    if (OB_UNLIKELY(tenant_id != tenant_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant id mismatch", K(ret), K(tenant_id), K(tenant_id_));
    } else if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
      LOG_WARN("failed to get tablet ids", K(ret));
    } else if (OB_FAIL(append(tablet_ids_, tablet_ids))) {
      LOG_WARN("failed to append tablet ids", K(ret));
    }
  }
  return ret;
}

// add user table and its related tables that use tablet autoinc, e.g., lob meta table
int ObTabletAutoincCacheCleaner::add_table(schema::ObSchemaGetterGuard &schema_guard, const schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_single_table(table_schema))) {
    LOG_WARN("failed to add single table", K(ret));
  }

  if (OB_SUCC(ret)) {
    const uint64_t lob_meta_tid = table_schema.get_aux_lob_meta_tid();
    if (OB_INVALID_ID != lob_meta_tid) {
      const ObTableSchema *lob_meta_table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(table_schema.get_tenant_id(), lob_meta_tid, lob_meta_table_schema))) {
        LOG_WARN("failed to get aux table schema", K(ret), K(lob_meta_tid));
      } else if (OB_ISNULL(lob_meta_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table schema", K(ret), K(lob_meta_tid));
      } else if (OB_FAIL(add_single_table(*lob_meta_table_schema))) {
        LOG_WARN("failed to add single table", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletAutoincCacheCleaner::add_database(const schema::ObDatabaseSchema &database_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = database_schema.get_tenant_id();
  const uint64_t database_id = database_schema.get_database_id();
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
  ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_database(tenant_id,
                                                                database_id,
                                                                table_schemas))) {
    LOG_WARN("fail to get table ids in database", K(tenant_id), K(database_id), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", K(ret));
      } else if (OB_FAIL(add_single_table(*table_schema))) {
        LOG_WARN("fail to lock_table", KR(ret), KPC(table_schema));
      }
    }
  }
  return ret;
}

int ObTabletAutoincCacheCleaner::commit(const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObTabletAutoincCacheCleaner", 1 * 1000 * 1000);
  ObTabletAutoincrementService &tablet_autoinc_service = share::ObTabletAutoincrementService::get_instance();
  uint64_t data_version = 0;
  common::ObZone zone;
  common::ObSEArray<common::ObAddr, 8> server_list;
  ObUnitInfoGetter ui_getter;
  obrpc::ObClearTabletAutoincSeqCacheArg arg;
  const ObLSID unused_ls_id = SYS_LS;
  int64_t abs_timeout_us = ObTimeUtility::current_time() + timeout_us;
  const ObTimeoutCtx &ctx = ObTimeoutCtx::get_ctx();
  if (THIS_WORKER.is_timeout_ts_valid()) {
    abs_timeout_us = std::min(abs_timeout_us, THIS_WORKER.get_timeout_ts());
  }
  if (ctx.is_timeout_set()) {
    abs_timeout_us = std::min(abs_timeout_us, ctx.get_abs_timeout());
  }
  if (ctx.is_trx_timeout_set()) {
    abs_timeout_us = std::min(abs_timeout_us, ObTimeUtility::current_time() + ctx.get_trx_timeout_us());
  }

  const int64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  if ((min_cluster_version >= MOCK_CLUSTER_VERSION_4_2_3_0 && min_cluster_version < CLUSTER_VERSION_4_3_0_0)
      || min_cluster_version >= CLUSTER_VERSION_4_3_0_1) {
    if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("srv_rpc_proxy or sql_proxy in GCTX is null", K(ret), K(GCTX.srv_rpc_proxy_), K(GCTX.sql_proxy_));
    } else if (OB_FAIL(ui_getter.init(*GCTX.sql_proxy_, &GCONF))) {
      LOG_WARN("init unit info getter failed", K(ret));
    } else if (OB_FAIL(ui_getter.get_tenant_servers(tenant_id_, server_list))) {
      LOG_WARN("get tenant servers failed", K(ret));
    } else if (OB_FAIL(arg.init(tablet_ids_, unused_ls_id))) {
      LOG_WARN("failed to init clear tablet autoinc arg", K(ret));
    } else {
      bool need_clean_self = false;
      rootserver::ObClearTabletAutoincSeqCacheProxy proxy(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::clear_tablet_autoinc_seq_cache);
      for (int64_t i = 0; i < server_list.count(); i++) { // overwrite ret
        const common::ObAddr &addr = server_list.at(i);
        const int64_t cur_timeout_us = abs_timeout_us - ObTimeUtility::current_time();
        if (cur_timeout_us <= 0) {
          ret = OB_TIMEOUT;
          break;
        } else if (GCTX.self_addr() == addr) {
          need_clean_self = true;
        } else {
          if (OB_FAIL(proxy.call(addr, cur_timeout_us, tenant_id_, arg))) {
            LOG_WARN("failed to send rpc call", K(ret), K(addr), K(timeout_us), K(tenant_id_));
          }
        }
      }
      if (need_clean_self) { // overwrite ret
        if (OB_FAIL(tablet_autoinc_service.clear_tablet_autoinc_seq_cache(tenant_id_, tablet_ids_, abs_timeout_us))) {
          LOG_WARN("failed to clear tablet autoinc", K(ret));
        }
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(proxy.wait())) {
        LOG_WARN("wait batch result failed", K(tmp_ret), K(ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

}
}
