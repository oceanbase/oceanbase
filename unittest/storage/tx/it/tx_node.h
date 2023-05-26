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

#ifndef OCEANBASE_TRANSACTION_TEST_TX_NODE_DEFINE_
#define OCEANBASE_TRANSACTION_TEST_TX_NODE_DEFINE_
#define private public
#define protected public
#include "lib/objectpool/ob_server_object_pool.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_alive_server_tracer.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/ls/ob_freezer.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_scond.h"
#include "storage/tx_storage/ob_ls_map.h"

#include "../mock_utils/msg_bus.h"
#include "../mock_utils/basic_fake_define.h"
#include "../mock_utils/ob_fake_tx_rpc.h"

namespace oceanbase {
using namespace transaction;
using namespace share;
using namespace common;

namespace transaction {
template<class T>
class QueueConsumer : public share::ObThreadPool
{
public:
  QueueConsumer(ObString name,
                ObSpScLinkQueue *q,
                std::function<int(T*)> func):
    name_(name), queue_(q), func_(func), cond_() {}
  virtual int start() {
    int ret = OB_SUCCESS;
    ObThreadPool::set_run_wrapper(MTL_CTX());
    stop_ = false;
    ret = ObThreadPool::start();
    TRANS_LOG(INFO, "start.QeueueConsumer", K(ret), KPC(this));
    return ret;
  }
  void stop() {
    TRANS_LOG(INFO, "stop.QueueConsumerr", K_(is_sleeping), KPC(this));
    stop_ = true;
    cond_.signal();
    ObThreadPool::stop();
  }
  void run1() {
    while(!stop_) {
      ObLink *e = queue_->pop();
      if (e) {
        T *t = static_cast<T*>(e);
        func_(t);
      } else if (ATOMIC_BCAS(&is_sleeping_, false, true)) {
        TRANS_LOG(INFO, "to sleeping", KPC(this));
        auto key = cond_.get_key();
        cond_.wait(key, 1000);
        TRANS_LOG(INFO, "wakeup", KPC(this));
      }
    }
  }
  void wakeup() { if (ATOMIC_BCAS(&is_sleeping_, true, false)) { cond_.signal(); } }
  void set_name(ObString &name) { name_ = name; }
  TO_STRING_KV(KP(this), K_(name), KP_(queue), K(queue_->empty()), K_(stop));
private:
  ObString name_;
  bool stop_;
  ObSpScLinkQueue *queue_;
  std::function<int(T*)> func_;
  common::SimpleCond cond_;
  bool is_sleeping_ = false;
};
class ObTxNode;
class ObTxDescGuard {
public:
  ObTxDescGuard() = delete;
  ObTxDescGuard(const ObTxDescGuard &r) = delete;
  ObTxDescGuard &operator =(ObTxDescGuard r) = delete;
  ObTxDescGuard &operator =(const ObTxDescGuard &r) = delete;
  ObTxDescGuard(ObTxNode *tx_node, ObTxDesc *tx_desc)
    : tx_node_(tx_node), tx_desc_(tx_desc) {}
  ObTxDescGuard(ObTxDescGuard && m) {
    tx_desc_ = m.tx_desc_;
    tx_node_ = m.tx_node_;
    m.tx_desc_ = NULL;
    m.tx_node_ = NULL;
  }
  int release();
  ~ObTxDescGuard();
  ObTxDesc &get_tx_desc() { return *tx_desc_; }
  bool is_valid() const { return tx_desc_ != NULL; }
private:
  ObTxNode *tx_node_;
  ObTxDesc *tx_desc_;
};

enum ObTxNodeRole {
  Leader = 1,
  Follower = 2,
};

class ObTxNode final : public MsgEndPoint {
public:
  ObTxNode(const int64_t ls_id,
           const ObAddr &addr,
           MsgBus &msg_bus);
  ~ObTxNode();
  int start();
  void set_as_follower_replica(ObTxNode& leader_replica) {
    role_ = ObTxNodeRole::Follower;
    fake_tx_log_adapter_ = leader_replica.fake_tx_log_adapter_;
  }

public:
  TO_STRING_KV(KP(this), K(addr_), K_(ls_id));
  ObTxDescGuard get_tx_guard();
  // the simple r/w interface
  int read(ObTxDesc &tx, const int64_t key, int64_t &value, const ObTxIsolationLevel iso = ObTxIsolationLevel::RC);
  int read(const ObTxReadSnapshot &snapshot,
           const int64_t key,
           int64_t &value);
  int write(ObTxDesc &tx, const int64_t key, const int64_t value);
  int write(ObTxDesc &tx,
            const ObTxReadSnapshot &snapshot,
            const int64_t key,
            const int64_t value);
  int atomic_write(ObTxDesc &tx, const int64_t key, const int64_t value,
                   const int64_t expire_ts, const ObTxParam &tx_param);
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const int64_t ts_ns);

  int write_begin(ObTxDesc &tx, const ObTxReadSnapshot &snapshot, ObStoreCtx& write_store_ctx);
  int write_one_row(ObStoreCtx& write_store_ctx, const int64_t key, const int64_t value);
  int write_end(ObStoreCtx& write_store_ctx);

  // delegate txn control interface
#define DELEGATE_TENANT_WITH_RET(delegate_obj, func_name, ret)  \
  template <typename ...Args>                                   \
  ret func_name(Args &&...args) __attribute__((optnone)) {      \
    ObTenantEnv::set_tenant(&tenant_);                          \
    TRANS_LOG(INFO, "[call_tx_api]", KPC(this));                \
    return delegate_obj.func_name(std::forward<Args>(args)...); \
  }
  DELEGATE_TENANT_WITH_RET(txs_, acquire_tx, int);
  DELEGATE_TENANT_WITH_RET(txs_, release_tx, int);
  DELEGATE_TENANT_WITH_RET(txs_, start_tx, int);
  DELEGATE_TENANT_WITH_RET(txs_, rollback_tx, int);
  DELEGATE_TENANT_WITH_RET(txs_, commit_tx, int);
  DELEGATE_TENANT_WITH_RET(txs_, abort_tx, int);
  DELEGATE_TENANT_WITH_RET(txs_, submit_commit_tx, int);
  DELEGATE_TENANT_WITH_RET(txs_, get_read_snapshot, int);
  DELEGATE_TENANT_WITH_RET(txs_, create_implicit_savepoint, int);
  DELEGATE_TENANT_WITH_RET(txs_, create_explicit_savepoint, int);
  DELEGATE_TENANT_WITH_RET(txs_, rollback_to_explicit_savepoint, int);
  DELEGATE_TENANT_WITH_RET(txs_, release_explicit_savepoint, int);
  DELEGATE_TENANT_WITH_RET(txs_, rollback_to_implicit_savepoint, int);
  DELEGATE_TENANT_WITH_RET(txs_, interrupt, int);
  // tx free route
  DELEGATE_TENANT_WITH_RET(txs_, calc_txn_free_route, int);
#define  DELEGATE_X__(t)                                                \
  DELEGATE_TENANT_WITH_RET(txs_, txn_free_route__update_##t##_state, int); \
  DELEGATE_TENANT_WITH_RET(txs_, txn_free_route__serialize_##t##_state, int); \
  DELEGATE_TENANT_WITH_RET(txs_, txn_free_route__get_##t##_state_serialize_size, int64_t);
#define  DELEGATE_X_(t) DELEGATE_X__(t)
  LST_DO(DELEGATE_X_, (), static, dynamic, parts, extra);
  DELEGATE_TENANT_WITH_RET(txs_, tx_free_route_check_alive, int);
#undef DELEGATE_X_
#undef DELEGATE_X__
#undef DELEGATE_TENANT_WITH_RET
  int get_tx_ctx(const share::ObLSID &ls_id, const ObTransID &tx_id, ObPartTransCtx *&ctx) {
    return txs_.tx_ctx_mgr_.get_tx_ctx(ls_id, tx_id, false, ctx);
  }
  int revert_tx_ctx(ObPartTransCtx *ctx) { return txs_.tx_ctx_mgr_.revert_tx_ctx(ctx); }
public:
  struct MsgPack : ObLink {
    MsgPack(const ObAddr &addr, ObString &body, bool is_sync_msg = false)
      : recv_time_(ObTimeUtility::current_time()), sender_(addr),
        body_(body), is_sync_msg_(is_sync_msg), resp_ready_(false), resp_(), cond_() {}
    int64_t recv_time_;
    ObAddr sender_;
    ObString body_;
    bool is_sync_msg_;
    bool resp_ready_;
    ObString resp_;
    common::SimpleCond cond_; //used to synchronize process-thread and io-thread
  };
  int recv_msg(const ObAddr &sender, ObString &msg);
  int sync_recv_msg(const ObAddr &sender, ObString &msg, ObString &resp);
  int handle_msg_(MsgPack *pkt);
private:
  int create_memtable_(const int64_t tablet_id, memtable::ObMemtable *& mt);
  int create_ls_(const ObLSID ls_id);
  int drop_ls_(const ObLSID ls_id);
  int recv_msg_callback_(TxMsgCallbackMsg &msg);
  void wait_all_redolog_applied()
  { while (fake_tx_log_adapter_->get_inflight_cnt() != 0) usleep(1000); }

  int wait_all_tx_ctx_is_destoryed()
  {
    int ret = OB_SUCCESS;
    ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
    OZ(txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(ls_id_, ls_tx_ctx_mgr));
    int i = 0;
    for (i = 0; i < 2000; ++i) {
      if (0 == ls_tx_ctx_mgr->get_tx_ctx_count()) break;
      usleep(500);
    }
    if (2000 == i) {
      ret = OB_ERR_UNEXPECTED;
      LOG_INFO("print all tx begin", K(ret));
      const bool verbose = true;
      ls_tx_ctx_mgr->print_all_tx_ctx(ObLSTxCtxMgr::MAX_HASH_ITEM_PRINT, verbose);
      LOG_INFO("print all tx end", K(ret));
    }
    OZ(txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
    return ret;
  }
  void dump_msg_queue_();
public:
  static ObFakeLocationAdapter &get_location_adapter_() {
    static ObFakeLocationAdapter l;
    return l;
  }
  static ObFakeGtiSource &get_gti_source_() {
    static ObFakeGtiSource txIdGenerator;
    return txIdGenerator;
  }
  static ObFakeTsMgr &get_ts_mgr_() {
    static ObFakeTsMgr gts;
    return gts;
  }
public:
  // helpers
  int64_t ts_after_us(int64_t d) const { return ObTimeUtility::current_time() + d; }
  int64_t ts_after_ms(int64_t d) const { return ObTimeUtility::current_time() + d * 1000; }
private:
  static void reset_localtion_adapter() {
    get_location_adapter_().reset();
  }

public:
  void add_drop_msg_type(TX_MSG_TYPE type) {
    drop_msg_type_set_.set_refactored(type);
  }
  void del_drop_msg_type(TX_MSG_TYPE type) {
    drop_msg_type_set_.erase_refactored(type);
  }
public:
  ObString name_; char name_buf_[32];
  ObAddr addr_;
  ObLSID ls_id_;
  int64_t tenant_id_;
  ObTenantBase tenant_;
  common::ObServerObjectPool<ObPartTransCtx> fake_part_trans_ctx_pool_;
  ObTransService txs_;
  memtable::ObMemtable *memtable_;
  ObSEArray<ObColDesc, 2> columns_;
  // msg_handler
  ObSpScLinkQueue msg_queue_;
  QueueConsumer<MsgPack> msg_consumer_;
  // fake objects
  storage::ObTenantMetaMemMgr t3m_;
  ObFakeTransRpc fake_rpc_;
  ObFakeDupTableRpc fake_dup_table_rpc_;
  ObFakeGtiSource fake_gti_source_;
  ObFakeTsMgr fake_ts_mgr_;
  obrpc::ObSrvRpcProxy rpc_proxy_;
  share::schema::ObMultiVersionSchemaService schema_service_;
  share::ObAliveServerTracer server_tracer_;
  tablelock::ObLockMemtable lock_memtable_;
  ObLockTable fake_lock_table_;
  ObFakeTxTable fake_tx_table_;
  ObTenantFreezer fake_tenant_freezer_;
  ObLS fake_ls_;
  ObFreezer fake_freezer_;
  ObTxNodeRole role_;
  ObFakeTxLogAdapter* fake_tx_log_adapter_;
  ObTabletMemtableMgr fake_memtable_mgr_;
  storage::ObLS mock_ls_; // TODO mock required member on LS
  common::hash::ObHashSet<int16_t> drop_msg_type_set_;
  ObLSMap fake_ls_map_;
  std::function<int(int,void *)> extra_msg_handler_;
};

} // transaction

} // oceanbase
#endif //OCEANBASE_TRANSACTION_TEST_TX_NODE_DEFINE_
