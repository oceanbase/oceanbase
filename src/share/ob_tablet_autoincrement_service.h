/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_TABLET_AUTOINCREMENT_SERVICE_H_
#define OCEANBASE_SHARE_OB_TABLET_AUTOINCREMENT_SERVICE_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/allocator/ob_small_allocator.h"
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/ob_rpc_struct.h"
#include "share/rpc/ob_async_rpc_proxy.h"

namespace oceanbase
{
namespace share
{
RPC_F(obrpc::OB_SYNC_TABLET_AUTOINC_SEQ_CACHE, obrpc::ObSyncTabletSeqCacheArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_SYNC_TABLET_AUTOINC_SEQ_CACHE>::Response, ObSyncTabletAutoincSeqCacheProxy);

struct ObTabletCacheNode
{
public:
  ObTabletCacheNode() : cache_start_(0), cache_end_(0) {}

  void reset() { cache_start_ = 0; cache_end_ = 0; }
  bool is_valid() { return cache_end_ != 0; }

  TO_STRING_KV(K_(cache_start),
               K_(cache_end));
public:
  uint64_t cache_start_;
  uint64_t cache_end_;
};

class ObTabletAutoincMgr: public common::LinkHashValue<ObTabletAutoincKey>
{
public:
  ObTabletAutoincMgr()
    : mutex_(ObLatchIds::TABLET_AUTO_INCREMENT_MGR_LOCK),
      tablet_id_(),
      next_value_(1),
      last_refresh_ts_(common::ObTimeUtility::current_time()),
      sync_value_(0),
      prefetching_(false),
      is_inited_(false)
  {}
  virtual ~ObTabletAutoincMgr()
  {
    destroy();
  }

  int init(const common::ObTabletID &tablet_id);
  int fetch_interval(const ObTabletAutoincParam &param, const bool prefetch, ObTabletCacheInterval &interval);
  static int fetch_interval_without_cache(const ObTabletAutoincParam &param, const ObTabletID &tablet_id, ObTabletCacheInterval &interval);
  int sync_insert_value_global(const ObTabletAutoincParam &param, const uint64_t insert_value);
  void destroy() {}
  int clear_cache_if_fallback_for_mlog(
      const uint64_t current_value);
  int sync_tablet_autoinc_seq_cache(const uint64_t sync_value, const int64_t abs_timeout_us);

  TO_STRING_KV(K_(tablet_id),
               K_(next_value),
               K_(last_refresh_ts),
               K_(curr_node),
               K_(prefetch_node),
               K_(sync_value),
               K_(prefetching),
               K_(is_inited));
private:
  static const int64_t TRY_LOCK_INTERVAL = 1000L; // 1ms

  int set_interval(ObTabletCacheInterval &interval);
  static int fetch_new_range(const ObTabletAutoincParam &param,
                      const common::ObTabletID &tablet_id,
                      const uint64_t sync_value,
                      ObTabletCacheNode &node);
  bool prefetch_condition()
  {
    return !prefetch_node_.is_valid() &&
        (next_value_ - curr_node_.cache_start_) * PREFETCH_THRESHOLD > curr_node_.cache_end_ - curr_node_.cache_start_;
  }
  static bool is_retryable(int ret)
  {
    return OB_NOT_MASTER == ret || OB_NOT_INIT == ret || OB_TIMEOUT == ret || OB_EAGAIN == ret || OB_LS_NOT_EXIST == ret || OB_TABLET_NOT_EXIST == ret || OB_TENANT_NOT_IN_SERVER == ret || OB_LS_LOCATION_NOT_EXIST == ret;
  }
  static bool is_block_renew_location(int ret)
  {
    return OB_LOCATION_LEADER_NOT_EXIST == ret || OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_NO_READABLE_REPLICA == ret
      || OB_NOT_MASTER == ret || OB_RS_NOT_MASTER == ret || OB_RS_SHUTDOWN == ret || OB_PARTITION_NOT_EXIST == ret || OB_LOCATION_NOT_EXIST == ret
      || OB_PARTITION_IS_STOPPED == ret || OB_SERVER_IS_INIT == ret || OB_SERVER_IS_STOPPING == ret || OB_TENANT_NOT_IN_SERVER == ret
      || OB_TRANS_RPC_TIMEOUT == ret || OB_USE_DUP_FOLLOW_AFTER_DML == ret || OB_TRANS_STMT_NEED_RETRY == ret
      || OB_LS_NOT_EXIST == ret || OB_TABLET_NOT_EXIST == ret || OB_LS_LOCATION_NOT_EXIST == ret || OB_PARTITION_IS_BLOCKED == ret || OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST == ret
      || OB_GET_LOCATION_TIME_OUT == ret;
  }
  void try_prefetch(const ObTabletAutoincParam &param);
  int sync_tablet_seq_cache(const uint64_t x);
  int sync_tablet_seq_cache_all(const uint64_t tenant_id, const ObTabletID &tablet_id, const uint64_t sync_value);
private:
  static const int64_t PREFETCH_THRESHOLD = 4;
  static const int64_t RETRY_INTERVAL = 100 * 1000L; // 100ms
  lib::ObMutex mutex_;
  common::ObTabletID tablet_id_;
  uint64_t next_value_;
  int64_t  last_refresh_ts_; // use this to determine active tablet
  ObTabletCacheNode curr_node_;
  ObTabletCacheNode prefetch_node_;
  uint64_t sync_value_;
  bool prefetching_;
  bool is_inited_;
};

class ObTabletAutoincMgrAllocHandle
{
public:
  typedef LinkHashNode<ObTabletAutoincKey> TabletAutoincNode;
  typedef ObTabletAutoincMgr TabletAutoincMgr;
  static ObTabletAutoincMgr* alloc_value() { return op_reclaim_alloc(TabletAutoincMgr); }
  static void free_value(ObTabletAutoincMgr* val) { op_reclaim_free(val); val = nullptr; }
  static TabletAutoincNode* alloc_node(ObTabletAutoincMgr* val) { UNUSED(val); return op_reclaim_alloc(TabletAutoincNode); }
  static void free_node(TabletAutoincNode* node) { op_reclaim_free(node); node = nullptr; }
};

class ObTabletAutoincCacheCleaner final
{
public:
  static const int64_t DEFAULT_TIMEOUT_US = 1 * 1000 * 1000;
  ObTabletAutoincCacheCleaner(const uint64_t tenant_id) : tenant_id_(tenant_id), tablet_ids_() {}
  ~ObTabletAutoincCacheCleaner() {}
  int add_table(schema::ObSchemaGetterGuard &schema_guard, const schema::ObTableSchema &table_schema);
  int add_single_table(const schema::ObSimpleTableSchemaV2 &table_schema);
  int add_database(const schema::ObDatabaseSchema &database_schema);
  int commit(const int64_t timeout_us = DEFAULT_TIMEOUT_US);
  TO_STRING_KV(K_(tenant_id), K_(tablet_ids));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletAutoincCacheCleaner);
  uint64_t tenant_id_;
  ObArray<ObTabletID> tablet_ids_;
};

class ObTabletAutoincrementService
{
public:
  static ObTabletAutoincrementService &get_instance();
  static const int64_t DEFAULT_CACHE_SIZE = 10000;
  static const int64_t LOB_CACHE_SIZE = 100000;
  int init();
  void destroy();
  int get_tablet_cache_interval(const uint64_t tenant_id,
                                ObTabletCacheInterval &interval);
  int get_autoinc_seq(const uint64_t tenant_id, const common::ObTabletID &tablet_id, uint64_t &autoinc_seq, const int64_t cache_size=ObTabletAutoincrementService::DEFAULT_CACHE_SIZE);
  int get_autoinc_seq_for_mlog(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      uint64_t &autoinc_seq);
  int sync_insert_value_global(const uint64_t tenant_id, ObIArray<RandomPartSyncTabletCtx> &sync_ctxs);
  int clear_tablet_autoinc_seq_cache(const uint64_t tenant_id, const common::ObIArray<common::ObTabletID> &tablet_ids, const int64_t abs_timeout_us);
  int sync_tablet_autoinc_seq_cache(const obrpc::ObSyncTabletSeqCacheArg &arg, const int64_t abs_timeout_us);
private:
  int acquire_mgr(const uint64_t tenant_id, const common::ObTabletID &tablet_id, ObTabletAutoincMgr *&autoinc_mgr);
  void release_mgr(ObTabletAutoincMgr *autoinc_mgr);
  int sync_insert_value_global(const uint64_t tenant_id, const ObTabletID &tablet_id, const int64_t value_to_sync);

  ObTabletAutoincrementService();
  ~ObTabletAutoincrementService();

private:
  typedef common::ObLinkHashMap<ObTabletAutoincKey, ObTabletAutoincMgr, ObTabletAutoincMgrAllocHandle> TabletAutoincMgrMap;
  const static int INIT_NODE_MUTEX_NUM = 10243L;
  bool is_inited_;
  common::ObSmallAllocator node_allocator_;
  TabletAutoincMgrMap tablet_autoinc_mgr_map_;
  struct InitNodeMutexWrapper {
    lib::ObMutex mutex_;
    InitNodeMutexWrapper() : mutex_(common::ObLatchIds::TABLET_AUTO_INCREMENT_SERVICE_LOCK) {}
  };
  InitNodeMutexWrapper init_node_mutexs_[INIT_NODE_MUTEX_NUM];
};


} // end namespace share
} // end namespace oceanbase
#endif
