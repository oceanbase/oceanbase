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

#define USING_LOG_PREFIX TRANS
#include "tx_node.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "lib/utility/ob_unify_serialize.h"         // OB_UNIS_VERSION
#define FAST_FAIL() \
  do {                                                          \
  if (OB_FAIL(ret)) {                                           \
    TRANS_LOG(ERROR, "[tx node crash] fast-fail for easy debug", K(ret)); \
    ob_abort();                                                 \
  }                                                             \
} while(0);

namespace oceanbase {
namespace common {
int ObClusterVersion::get_tenant_data_version(const uint64_t tenant_id, uint64_t &data_version)
{
  data_version = DATA_CURRENT_VERSION;
  return OB_SUCCESS;
}
}
namespace share
{
void* ObMemstoreAllocator::alloc(AllocHandle& handle, int64_t size, const int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  int64_t align_size = upper_align(size, sizeof(int64_t));
  uint64_t tenant_id = arena_.get_tenant_id();
  bool is_out_of_mem = false;
  if (!handle.is_id_valid()) {
    COMMON_LOG(TRACE, "MTALLOC.first_alloc", KP(&handle.mt_));
    LockGuard guard(lock_);
    if (!handle.is_id_valid()) {
      handle.set_clock(arena_.retired());
      hlist_.set_active(handle);
    }
  }
  return arena_.alloc(handle.id_, handle.arena_handle_, align_size);
}
}

namespace storage {

int ObTxTable::online()
{
  ATOMIC_INC(&epoch_);
  ATOMIC_STORE(&state_, TxTableState::ONLINE);
  return OB_SUCCESS;
}

}  // namespace storage

namespace transaction {

ObTxDescGuard::~ObTxDescGuard() {
  if (tx_desc_) {
    release();
  }
}
int ObTxDescGuard::release() {
  int ret = OB_SUCCESS;
  if (tx_desc_) {
    tx_node_->release_tx(*tx_desc_);
    tx_desc_ = NULL;
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

ObString ObTxNode::get_identifer_str()
{
  struct ID {
    ObAddr addr;
    int64_t ls_id;
    DECLARE_TO_STRING {
      int64_t pos = 0;
      int32_t pos0 = 0;
      addr.addr_to_buffer(buf, buf_len, pos0);
      pos += pos0;
      BUF_PRINTF("_%ld", ls_id);
      return pos;
    }
  } identifer = {
    .addr = addr_,
    .ls_id = ls_id_.id()
  };
  int64_t pos = identifer.to_string(buf_, sizeof(buf_));
  return ObString(pos, buf_);
}
ObTxNode::ObTxNode(const int64_t ls_id,
                   const ObAddr &addr,
                   MsgBus &msg_bus) :
  name_(name_buf_),
  addr_(addr),
  ls_id_(ls_id),
  tenant_id_(1001),
  tenant_(tenant_id_, 0, 10, *GCTX.cgroup_ctrl_),
  fake_part_trans_ctx_pool_(1001, false, false, 4),
  memtable_(NULL),
  msg_consumer_(get_identifer_str(),
                &msg_queue_,
                std::bind(&ObTxNode::handle_msg_,
                          this, std::placeholders::_1)),
  t3m_(tenant_id_),
  fake_rpc_(&msg_bus, addr, &get_location_adapter_()),
  lock_memtable_(),
  fake_tx_log_adapter_(nullptr),
  stop_request_retry_(false),
  req_consumer_(ObString("TxNode"),
                &req_queue_,
                std::bind(&ObTxNode::handle_request_retry_,
                          this, std::placeholders::_1)),
  req_msg_consumer_(ObString("TxNode"),
                &req_msg_queue_,
                std::bind(&ObTxNode::handle_request_msg_,
                          this, std::placeholders::_1)),
  fake_lock_wait_mgr_(1001, &msg_bus, addr)
{
  fake_lock_wait_mgr_.set_req_queue(&req_queue_);
  fake_lock_wait_mgr_.set_req_queue_cond(req_consumer_.get_cond());
  fake_lock_wait_mgr_.set_tenant_id(tenant_id_);
  fake_part_trans_ctx_pool_.init();
  GCTX.self_addr_seq_.set_addr(addr_);
  addr.to_string(name_buf_, sizeof(name_buf_));
  msg_consumer_.set_name(name_);
  req_msg_consumer_.set_name(name_);
  req_consumer_.set_name(name_);
  role_ = Leader;
  tenant_.enable_tenant_ctx_check_ = false;
  tenant_.set(&fake_tenant_freezer_);
  tenant_.set(&fake_part_trans_ctx_pool_);
  fake_shared_mem_alloc_mgr_.init();
  tenant_.set(&fake_shared_mem_alloc_mgr_);
  tenant_.set(&fake_lock_wait_mgr_);
  tenant_.start();
  ObTenantEnv::set_tenant(&tenant_);
  ObTableHandleV2 lock_memtable_handle;
  lock_memtable_handle.set_table(&lock_memtable_, &t3m_, ObITable::LOCK_MEMTABLE);
  lock_memtable_.key_.table_type_ = ObITable::LOCK_MEMTABLE;
  fake_ls_.ls_meta_.ls_id_ = ls_id_;
  fake_ls_.ls_tablet_svr_.lock_memtable_mgr_.t3m_ = &t3m_;
  fake_ls_.ls_tablet_svr_.lock_memtable_mgr_.add_memtable_(lock_memtable_handle);
  fake_lock_table_.is_inited_ = true;
  fake_lock_table_.parent_ = &fake_ls_;
  fake_lock_table_.lock_mt_mgr_ = &(fake_ls_.ls_tablet_svr_.lock_memtable_mgr_);
  fake_tenant_freezer_.is_inited_ = true;
  fake_tenant_freezer_.tenant_info_.is_loaded_ = true;
  fake_tenant_freezer_.tenant_info_.mem_memstore_limit_ = INT64_MAX;
  // memtable.freezer
  fake_freezer_.freeze_flag_ = 0;// is_freeze() = false;
  GCTX.sql_proxy_ = &sql_proxy_;
  // txn service
  int ret = OB_SUCCESS;
  OZ(txs_.init(addr,
               &fake_rpc_,
               &fake_dup_table_rpc_,
               &get_location_adapter_(),
               &get_gti_source_(),
               &get_ts_mgr_(),
               &rpc_proxy_,
               &schema_service_,
               &server_tracer_));
  tenant_.set(&txs_);
  OZ(fake_opt_stat_mgr_.init(tenant_id_));
  OZ(fake_lock_wait_mgr_.init());
  tenant_.set(&fake_lock_wait_mgr_);
  tenant_.set(&fake_opt_stat_mgr_);
  ls_service_.is_inited_ = true;
  OZ(ls_service_.ls_map_.init(tenant_id_, lib::ObMallocAllocator::get_instance()));
  tenant_.set(&ls_service_);
  OZ (create_memtable_(100000, memtable_));
  {
    ObColDesc col_desc;
    col_desc.col_id_ = 1;
    col_desc.col_type_.set_type(ObObjType::ObIntType);
    col_desc.col_order_ = common::ObOrderType::ASC;
    columns_.push_back(col_desc);
    col_desc.col_id_ = 2;
    columns_.push_back(col_desc);
  }
  OZ(msg_bus.regist(addr, *this));
  OZ(drop_msg_type_set_.create(16));
  FAST_FAIL();
}
ObTxDescGuard ObTxNode::get_tx_guard() {
  int ret = OB_SUCCESS;
  ObTxDesc *tx = NULL;
  OZ(txs_.acquire_tx(tx));
  ObTxDescGuard x = ObTxDescGuard(this, tx);
  return x;
}
int ObTxNode::start() {
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(&tenant_);

  if (ObTxNodeRole::Leader == role_) {
    fake_tx_log_adapter_ = new ObFakeTxLogAdapter();
    OZ(fake_tx_log_adapter_->start());
  }
  get_ts_mgr_().reset();
  OZ(msg_consumer_.start());
  OZ(req_msg_consumer_.start());
  OZ(req_consumer_.start());
  OZ(txs_.start());
  OZ(create_ls_(ls_id_));
  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  OZ(txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(ls_id_, ls_tx_ctx_mgr));
  if (ObTxNodeRole::Leader == role_) {
    OZ(ls_tx_ctx_mgr->switch_to_leader());
    wait_all_redolog_applied();
    CK(ls_tx_ctx_mgr->is_master_());
  }
  fake_lock_wait_mgr_.start();
  if (ls_tx_ctx_mgr) {
    fake_tx_table_.tx_ctx_table_.ls_tx_ctx_mgr_ = ls_tx_ctx_mgr;
    fake_tx_table_.is_inited_ = true;
    fake_tx_table_.ls_ = &fake_ls_;
    fake_tx_table_.online();
    int tx_data_table_offset = offsetof(storage::ObTxTable, tx_data_table_);
    void* ls_tx_data_table_ptr = (void*)((int64_t)&(fake_ls_.tx_table_) + tx_data_table_offset);
    ls_tx_data_table_ptr = &fake_tx_table_.tx_data_table_;
    fake_ls_.tx_table_.is_inited_ = true;
    fake_ls_.tx_table_.online();
    fake_ls_.ls_meta_.clog_checkpoint_scn_ = share::SCN::max_scn();
  } else {
    abort();
  }
  return ret;
}

struct MsgInfo {
  void *msg_ptr_ = NULL;
  int64_t recv_time_ = 0;
  ObAddr sender_;
  bool is_callback_msg_ = false;
  bool is_lock_wait_mgr_msg_ = false;
  bool is_request_msg_ = false;
  bool is_sync_msg_ = false;
  TxMsgCallbackMsg callback_msg_;
  int16_t msg_type_;
  int64_t size_;
  const char* buf_;
  TO_STRING_KV(K_(msg_ptr),
               K_(recv_time),
               K_(sender),
               K_(is_callback_msg),
               K_(is_lock_wait_mgr_msg),
               K_(is_sync_msg),
               K_(msg_type),
               K_(callback_msg),
               K_(size));
};

int get_msg_info(ObTxNode::MsgPack *pkt, MsgInfo& msg_info)
{
  int ret = OB_SUCCESS;
  const char* buf = pkt->body_.ptr();
  int64_t size = pkt->body_.length();
  int64_t pos = 0;
  char cat = 0;  int16_t pcode = 0;
  OZ (serialization::decode(buf, size, pos, cat));
  if (cat == 2) {
    OZ (serialization::decode(buf, size, pos, pcode));
    msg_info.msg_type_ = pcode;
    msg_info.buf_ = buf + pos;
    msg_info.size_ = size - pos;
    msg_info.is_request_msg_ = true;
  } else if (cat == 3) {
    OZ (serialization::decode(buf, size, pos, pcode));
    msg_info.msg_type_ = pcode;
    msg_info.buf_ = buf + pos;
    msg_info.size_ = size - pos;
    msg_info.is_lock_wait_mgr_msg_ = true;
  } else if (cat == 1) {
    msg_info.is_callback_msg_ = true;
    OZ (msg_info.callback_msg_.deserialize(buf, size, pos));
    msg_info.msg_type_ = msg_info.callback_msg_.type_;
  } else {
    OZ (serialization::decode(buf, size, pos, pcode));
    msg_info.msg_type_ = pcode;
    msg_info.buf_ = buf + pos;
    msg_info.size_ = size - pos;
  }
  msg_info.is_sync_msg_ = pkt->is_sync_msg_;
  msg_info.msg_ptr_ = (void*)pkt->body_.ptr();
  msg_info.recv_time_ = pkt->recv_time_;
  msg_info.sender_ = pkt->sender_;
  return ret;
}

void ObTxNode::dump_msg_queue_()
{
  int ret = OB_SUCCESS;
  MsgPack *msg = NULL;
  int i = 0;
  while(OB_SUCC(msg_queue_.pop((ObLink*&)msg))) {
    ++i;
    MsgInfo msg_info;
    OZ (get_msg_info(msg, msg_info));
    TRANS_LOG(INFO,"[dump_msg]", K(i), K(msg_info), K(ret), KPC(this));
  }
}

void ObTxNode::wait_all_msg_consumed()
{
  while (msg_queue_.size() > 0 || !msg_consumer_.is_idle()) {
    if (REACH_TIME_INTERVAL(200_ms)) {
      TRANS_LOG(INFO, "wait msg_queue to be empty", K(msg_queue_.size()), KPC(this));
    }
    usleep(5_ms);
  }
}

void ObTxNode::wait_tx_log_synced()
{
  while(fake_tx_log_adapter_->get_inflight_cnt() > 0) {
    if (REACH_TIME_INTERVAL(200_ms)) {
      TRANS_LOG(INFO, "wait tx log synced...", K(fake_tx_log_adapter_->get_inflight_cnt()), KPC(this));
    }
    usleep(5_ms);
  }
}

ObTxNode::~ObTxNode() __attribute__((optnone)) {
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "destroy TxNode", KPC(this));
  ObTenantEnv::set_tenant(&tenant_);
  OZ(txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(fake_tx_table_.tx_ctx_table_.ls_tx_ctx_mgr_));
  fake_tx_table_.tx_ctx_table_.ls_tx_ctx_mgr_ = nullptr;
  bool is_tx_clean = false;
  int retry_cnt = 0;
  do {
    usleep(2000);
    txs_.block_tx(ls_id_, is_tx_clean);
  } while(!is_tx_clean && ++retry_cnt < 1000);
  OX(txs_.stop());
  OZ(txs_.wait_());
  fake_lock_wait_mgr_.stop();
  fake_lock_wait_mgr_.wait();
  fake_lock_wait_mgr_.destroy();
  OZ(drop_ls_(ls_id_));
  if (role_ == Leader && fake_tx_log_adapter_) {
    fake_tx_log_adapter_->stop();
    fake_tx_log_adapter_->wait();
    fake_tx_log_adapter_->destroy();
  }
  msg_consumer_.stop();
  msg_consumer_.wait();
  req_msg_consumer_.stop();
  req_msg_consumer_.wait();
  req_consumer_.stop();
  req_consumer_.wait();
  dump_msg_queue_();
  //fake_tx_table_.is_inited_ = false;
  if (memtable_) {
    delete memtable_;
  }
  if (role_ == Leader && fake_tx_log_adapter_) {
    delete fake_tx_log_adapter_;
  }
  fake_ls_.ls_meta_.ls_id_ = ObLSID(1001);
  FAST_FAIL();
  ObTenantEnv::set_tenant(NULL);
}

int ObTxNode::create_memtable_(const int64_t tablet_id, memtable::ObMemtable *&mt) {
  int ret = OB_SUCCESS;
  memtable::ObMemtable *t = new memtable::ObMemtable();
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::DATA_MEMTABLE;
  table_key.tablet_id_ = tablet_id;
  table_key.scn_range_.start_scn_.convert_for_gts(100);
  table_key.scn_range_.end_scn_.set_max();
  ObLSHandle ls_handle;
  ls_handle.set_ls(ls_service_.ls_map_, fake_ls_, ObLSGetMod::DATA_MEMTABLE_MOD);
  #ifdef TX_NODE_MEMTABLE_USE_HASH_INDEX_FLAG
    const bool use_hash_index = TX_NODE_MEMTABLE_USE_HASH_INDEX_FLAG;
  #else
    const bool use_hash_index = true;  // 默认值为 true
  #endif
  TRANS_LOG(INFO, "create_memtable_with_use_hash_index", K(use_hash_index), KPC(this));
  OZ (t->init(table_key, ls_handle, &fake_freezer_, &fake_memtable_mgr_, 0, 0, use_hash_index));
  if (OB_SUCC(ret)) {
    mt = t;
  } else { delete t; }
  return ret;
}

int ObTxNode::create_ls_(const ObLSID ls_id) {
  int ret = OB_SUCCESS;
  OZ(txs_.tx_ctx_mgr_.create_ls(tenant_id_,
                                ls_id,
                                &fake_tx_table_,
                                &fake_lock_table_,
                                *fake_ls_.get_tx_svr(),
                                (ObITxLogParam*)0x01,
                                fake_tx_log_adapter_));
  if (Leader == role_) {
    OZ(get_location_adapter_().fill(ls_id, addr_));
  }
  fake_ls_.ls_meta_.ls_id_ = ls_id;
  fake_ls_.get_tx_svr()->online();
  fake_ls_.get_ref_mgr().inc(ObLSGetMod::TXSTORAGE_MOD);
  MTL(ObLSService*)->ls_map_.add_ls(fake_ls_);
  return ret;
}

int ObTxNode::drop_ls_(const ObLSID ls_id) {
  int ret = OB_SUCCESS;
  OZ(txs_.tx_ctx_mgr_.remove_ls(ls_id, true));
  get_location_adapter_().remove(ls_id);
  OZ(MTL(ObLSService*)->ls_map_.del_ls(ls_id), ls_id);
  return ret;
}

int ObTxNode::recv_msg_callback_(TxMsgCallbackMsg &msg)
{
  TRANS_LOG(INFO, "recv msg callback", K(msg), KPC(this));
  ObTenantEnv::set_tenant(&tenant_);
  int ret = OB_SUCCESS;
  switch(msg.type_) {
  case TxMsgCallbackMsg::SAVEPOINT_ROLLBACK:
    // ignore, has changed to use async resp msg
    break;
  case TxMsgCallbackMsg::NORMAL:
    OZ(txs_.handle_trans_msg_callback(msg.sender_ls_id_,
                                      msg.receiver_ls_id_,
                                      msg.tx_id_,
                                      msg.orig_msg_type_,
                                      msg.tx_rpc_result_.status_,
                                      msg.receiver_addr_,
                                      msg.request_id_,
                                      msg.tx_rpc_result_.private_data_));
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
  }
  TRANS_LOG(INFO, "handle msg callback done", K(ret), K(msg), KPC(this));
  return ret;
}

int ObTxNode::recv_msg(const ObAddr &sender, ObString &m)
{
  TRANS_LOG(INFO, "recv_msg", K(sender), "msg_ptr", OB_P(m.ptr()), KPC(this));
  int ret = OB_SUCCESS;
  auto pkt = new MsgPack(sender, m);
  OZ(msg_queue_.push(pkt));
  msg_consumer_.wakeup();
  return ret;
}

int ObTxNode::sync_recv_msg(const ObAddr &sender, ObString &m, ObString &resp)
{
  TRANS_LOG(INFO, "sync_recv_msg", K(sender), "msg_ptr", OB_P(m.ptr()), KPC(this));
  int ret = OB_SUCCESS;
  auto pkt = new MsgPack(sender, m, true);
  OZ(msg_queue_.push(pkt));
  msg_consumer_.wakeup();
  pkt->cond_.wait(pkt->cond_.get_key(), 500000);
  if (pkt->resp_ready_) {
    OX(resp = pkt->resp_);
  } else {
    ret = OB_TIMEOUT;
    TRANS_LOG(ERROR, "wait resp timeout",K(ret));
  }
  ob_free((void*)const_cast<char*>(pkt->body_.ptr()));
  delete pkt;
  return ret;
}

int ObTxNode::handle_msg_(MsgPack *pkt)
{
  ObTenantEnv::set_tenant(&tenant_);
  TRANS_LOG(INFO, "begin to handle_msg", "msg_ptr", OB_P(pkt->body_.ptr()), KPC(this));
  int ret = OB_SUCCESS;
  MsgInfo msg_info;
  OZ (get_msg_info(pkt, msg_info));
  auto sender = msg_info.sender_;
  const char* buf = msg_info.buf_;
  int64_t size = msg_info.size_;
  int64_t pos = 0;
  if (OB_SUCC(ret) && msg_info.is_callback_msg_) {
    return recv_msg_callback_(msg_info.callback_msg_);
  }
  int16_t msg_type = msg_info.msg_type_;
  if (OB_HASH_EXIST == drop_msg_type_set_.exist_refactored(msg_type)) {
    TRANS_LOG(WARN, "drop msg", K(msg_type), KPC(this));
    return OB_SUCCESS;
  }
  if (msg_info.is_lock_wait_mgr_msg_) {
    OZ(handle_lwm_msg_(msg_type, buf, size, pos));
  } else if (msg_info.is_request_msg_) {
    OZ(receive_request_msg_(msg_type, buf, size, pos));
  } else {
    switch (msg_type) {
    #define TX_MSG_HANDLER__(t, clz, func)                  \
      case t:                                               \
      {                                                     \
        clz msg;                                            \
        ObTransRpcResult rslt;                              \
        OZ(msg.deserialize(buf, size, pos));                \
        TRANS_LOG(TRACE, "handle_msg::", K(msg), KPC(this));        \
        auto status = txs_.func(msg, rslt);                 \
        rslt.status_ = status;                              \
        OZ(fake_rpc_.send_msg_callback(sender, msg, rslt)); \
        break;                                              \
      }
      TX_MSG_HANDLER__(TX_COMMIT, ObTxCommitMsg, handle_trans_commit_request);
      TX_MSG_HANDLER__(TX_COMMIT_RESP, ObTxCommitRespMsg, handle_trans_commit_response);
      TX_MSG_HANDLER__(TX_ABORT, ObTxAbortMsg, handle_trans_abort_request);
      TX_MSG_HANDLER__(KEEPALIVE, ObTxKeepaliveMsg, handle_trans_keepalive);
      TX_MSG_HANDLER__(KEEPALIVE_RESP, ObTxKeepaliveRespMsg, handle_trans_keepalive_response);
      TX_MSG_HANDLER__(ROLLBACK_SAVEPOINT_RESP, ObTxRollbackSPRespMsg, handle_sp_rollback_response);
    #undef TX_MSG_HANDLER__
      case TX_FREE_ROUTE_CHECK_ALIVE:
        {
          ObTxFreeRouteCheckAliveMsg msg;
          OZ(msg.deserialize(buf, size, pos));
          TRANS_LOG(TRACE, "handle_msg", K(msg), KPC(this));
          OZ(txs_.tx_free_route_handle_check_alive(msg, OB_TRANS_CTX_NOT_EXIST));
          break;
        }
      case TX_FREE_ROUTE_CHECK_ALIVE_RESP:
        {
          ObTxFreeRouteCheckAliveRespMsg msg;
          OZ(msg.deserialize(buf, size, pos));
          TRANS_LOG(TRACE, "handle_msg", K(msg), KPC(this));
          // can not handle by tx node, call extra handler
          if (extra_msg_handler_) {
            OZ(extra_msg_handler_(msg_type, &msg));
          }
          break;
        }
      case TX_FREE_ROUTE_PUSH_STATE:
        {
          ObTxFreeRoutePushState msg;
          OZ(msg.deserialize(buf, size, pos));
          TRANS_LOG(TRACE, "handle_msg", K(msg), KPC(this));
          OZ(txs_.tx_free_route_handle_push_state(msg));
          ObTxFreeRoutePushStateResp resp;
          resp.ret_ = ret;
          int64_t buf_len = resp.get_serialize_size();
          char *buf = (char*)ob_malloc(buf_len, ObNewModIds::TEST);
          int64_t pos = 0;
          OZ(resp.serialize(buf, buf_len, pos));
          pkt->resp_ = ObString(buf_len, buf);
          break;
        }
      case ROLLBACK_SAVEPOINT:
        {
          ObTxRollbackSPMsg msg;
          obrpc::ObTxRpcRollbackSPResult rslt;
          OZ(msg.deserialize(buf, size, pos));
          TRANS_LOG(TRACE, "handle_msg", K(msg), KPC(this));
          OZ(txs_.handle_sp_rollback_request(msg, rslt), msg);
          break;
        }
      case TX_2PC_PREPARE_REQ:
      case TX_2PC_PREPARE_RESP:
      case TX_2PC_PRE_COMMIT_REQ:
      case TX_2PC_PRE_COMMIT_RESP:
      case TX_2PC_COMMIT_REQ:
      case TX_2PC_COMMIT_RESP:
      case TX_2PC_ABORT_REQ:
      case TX_2PC_ABORT_RESP:
      case TX_2PC_CLEAR_REQ:
      case TX_2PC_CLEAR_RESP:
        OZ(txs_.handle_tx_batch_req(msg_type, buf + pos, size - pos, false));
      break;
      default:
        if (msg_info.is_lock_wait_mgr_msg_) {
        } else {
          ret = OB_NOT_SUPPORTED;
        }
    }
  }

  if (msg_info.is_sync_msg_) {
    pkt->resp_ready_ = true;
    pkt->cond_.signal();
  } else {
    ob_free((void*)const_cast<char*>(pkt->body_.ptr()));
    delete pkt;
  }
  TRANS_LOG(INFO, "handle_msg done", K(ret), KPC(this));
  return ret;
}

int ObTxNode::handle_lwm_msg_(const int16_t msg_type,
                              const char *buf,
                              const int64_t size,
                              int64_t &pos)
{
  int ret = OB_SUCCESS;
  int real_type = msg_type;
  switch (real_type) {
    case LWM_DST_ENQUEUE: {
      ObLockWaitMgrDstEnqueueMsg msg;
      OZ(msg.deserialize(buf, size, pos));
      TRANS_LOG(TRACE, "handle_msg", K(msg), KPC(this));
      ObLockWaitMgrRpcResult result;
      OZ(fake_lock_wait_mgr_.handle_inform_dst_enqueue_req(msg, result));
      break;
    }
    case LWM_DST_ENQUEUE_RESP: {
      ObLockWaitMgrDstEnqueueRespMsg msg;
      OZ(msg.deserialize(buf, size, pos));
      TRANS_LOG(TRACE, "handle_msg", K(msg), KPC(this));
      ObLockWaitMgrRpcResult result;
      OZ(fake_lock_wait_mgr_.handle_dst_enqueue_resp(msg, result));
      break;
    }
    case LWM_CHECK_NODE_STATE:
    case LWM_CHECK_NODE_STATE_RESP: {
      OZ(fake_lock_wait_mgr_.handle_batch_req(real_type, buf + pos, size - pos));
      break;
    }
    case LWM_LOCK_RELEASE: {
      ObLockWaitMgrLockReleaseMsg msg;
      OZ(msg.deserialize(buf, size, pos));
      TRANS_LOG(TRACE, "handle_msg", K(msg), KPC(this));
      ObLockWaitMgrRpcResult result;
      OZ(fake_lock_wait_mgr_.handle_lock_release_req(msg, result));
      break;
    }
    case LWM_WAKE_UP: {
      ObLockWaitMgrWakeUpRemoteMsg msg;
      OZ(msg.deserialize(buf, size, pos));
      TRANS_LOG(TRACE, "handle_msg", K(msg), KPC(this));
      ObLockWaitMgrRpcResult result;
      OZ(fake_lock_wait_mgr_.handle_wake_up_req(msg, result));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObTxNode::receive_request_msg_(const int16_t msg_type,
                                  const char* buf,
                                  int64_t data_len,
                                  int64_t &pos)
{
  int ret = OB_SUCCESS;
  switch (msg_type) {
    case FAKE_REQUEST_TYPE::FAKE_WRITE_REQUEST: {
      TRANS_LOG(TRACE, "recevie write req", K(msg_type));
      ObFakeWriteRequestMsg* msg = new ObFakeWriteRequestMsg();
      ObTxDesc *tx_buf = new ObTxDesc();
      msg->tx_ = tx_buf;
      msg->deserialize(buf, data_len, pos);
      // int64_t version = 0;
      // int64_t len = 0;
      // LST_DO_CODE(OB_UNIS_DECODE,
      //             version,
      //             len,
      //             msg->type_,
      //             msg->send_addr_);
      // if (OB_FAIL(txs_.acquire_tx(buf, data_len, pos, msg->tx_))) {
      //   msg->tx_ = NULL;
      //   TRANS_LOG(WARN, "acquire tx by deserialize fail", K(msg_type), K(data_len), K(pos), K(ret));
      // }
      // LST_DO_CODE(OB_UNIS_DECODE,
      //             msg->key_,
      //             msg->value_,
      //             msg->tx_param_);
      TRANS_LOG(TRACE, "handle_request_msg", KPC(msg));
      req_msg_queue_.push(msg);
      req_msg_consumer_.wakeup();
      break;
    }
    case FAKE_REQUEST_TYPE::FAKE_WRITE_REQUEST_RESP: {
      ObFakeWriteRequestRespMsg* msg = new ObFakeWriteRequestRespMsg();
      msg->deserialize(buf, data_len, pos);
      TRANS_LOG(TRACE, "handle_request_msg", KPC(msg));
      req_msg_queue_.push(msg);
      req_msg_consumer_.wakeup();
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(TRACE, "handle_request_msg", K(msg_type));
      break;
    }
  }
  return ret;
}

int ObTxNode::handle_request_msg_(ObFakeRequestMsg *msg)
{
  int ret = OB_SUCCESS;
  if (msg->type_ == FAKE_REQUEST_TYPE::FAKE_WRITE_REQUEST) {
    ObFakeWriteRequestMsg *w_req = static_cast<ObFakeWriteRequestMsg*>(msg);
    ObTxDesc *tx = w_req->tx_;
    int64_t key = w_req->key_;
    int64_t value = w_req->value_;
    int64_t expire_ts = w_req->expire_ts_;
    bool need_inc_seq = w_req->inc_seq_;
    ObTxParam tx_param = w_req->tx_param_;
    bool unused = false;
    TRANS_LOG(TRACE, "handle write request", K(ret), KPC(tx), K(key), K(value), K(expire_ts), K(tx_param), K(need_inc_seq));
    if (OB_FAIL(write(*tx, key, value))) {
      TRANS_LOG(WARN, "remote write fail", K(ret), KPC(tx), K(key), K(value), K(tx_param));
    }
    ObFakeWriteRequestRespMsg resp_msg;
    ObTxExecResult tx_result;
    resp_msg.ret_ = ret;
    resp_msg.tx_id_ = tx->tid();
    resp_msg.send_addr_ = addr_;
    resp_msg.key_ = key;
    resp_msg.value_ = value;
    int tmp_ret = txs_.get_tx_exec_result(*tx, tx_result);
    if (need_inc_seq && !tx_result.conflict_info_array_.empty()) {
      TRANS_LOG(TRACE, "inc seq",
        K(ret), KPC(tx), K(key), K(value), K(tx_param), K(resp_msg));
      for (int i = 0; i < tx_result.conflict_info_array_.count(); i++) {
        tx_result.conflict_info_array_.at(i).conflict_happened_addr_ = addr_;
        fake_lock_wait_mgr_.inc_seq(tx_result.conflict_info_array_[i].conflict_hash_);
      }
    }
    if (OB_FAIL(tmp_ret)) {
      TRANS_LOG(WARN, "get exec result fail",
        K(ret), KPC(tx), K(key), K(value), K(tx_param), K(resp_msg));
    }
    if (OB_FAIL(resp_msg.exec_result_.assign(tx_result))) {
      TRANS_LOG(WARN, "assign exec result fail",
        K(ret), KPC(tx), K(key), K(value), K(tx_param), K(resp_msg));
    } else if (OB_FAIL(fake_rpc_.post_msg(w_req->send_addr_, resp_msg))) {
      TRANS_LOG(WARN, "remote write response fail",
        K(ret), KPC(tx), K(key), K(value), K(tx_param), K(resp_msg));
    }
    delete w_req->tx_;
    delete w_req;
  } else if (msg->type_ == FAKE_REQUEST_TYPE::FAKE_WRITE_REQUEST_RESP) {
    ObFakeWriteRequestRespMsg *w_resp = static_cast<ObFakeWriteRequestRespMsg*>(msg);
    TRANS_LOG(INFO, "handle write resp", KPC(w_resp));
    int64_t tx_id = w_resp->tx_id_;
    ObTxExecResult &exec_result = w_resp->exec_result_;
    if (tx_map_.find(tx_id) == tx_map_.end()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "not find tx desc", K(ret), KPC(w_resp), KP(&tx_map_), KP(this));
    } else {
      ObTxDesc *tx = tx_map_[tx_id];
      if (OB_FAIL(txs_.add_tx_exec_result(*tx, exec_result))) {
        LOG_WARN("merge response partition failed", K(ret), K(exec_result));
      } else if (OB_FAIL(tx_retry_control_map_.find(tx) == tx_retry_control_map_.end())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tx retry control should not be null", K(ret), KPC(tx));
      } else {
        ObTxRetryControl &retry_ctrl = tx_retry_control_map_[tx];
        ObLockWaitNode* node = retry_ctrl.node_;
        int tmp_ret = w_resp->ret_;
        int unfinished_task_num = retry_ctrl.dec_unfinished_task_num();
        ObLockWaitMgr::get_thread_node() = node;

        int exec_ret = w_resp->ret_;
        bool unused = false;
        if (tmp_ret != OB_TRY_LOCK_ROW_CONFLICT && tmp_ret != OB_SUCCESS) {
          retry_ctrl.exec_ret_ = tmp_ret;
        } else if (tmp_ret == OB_TRY_LOCK_ROW_CONFLICT) {
          if (retry_ctrl.cflict_key_ == 0) {
            retry_ctrl.cflict_key_ = w_resp->key_;
          }
          retry_ctrl.exec_ret_ = retry_ctrl.exec_ret_ == OB_SUCCESS ? tmp_ret : retry_ctrl.exec_ret_;
        }
        if (unfinished_task_num == 0) {
          TRANS_LOG(DEBUG, "task all finish", K(ret), K(retry_ctrl), K(w_resp));
          if (retry_ctrl.last_cflict_key_ != 0) {
            ObLockWaitMgr::get_thread_last_wait_hash_() = hash_rowkey(memtable_->get_tablet_id(), retry_ctrl.last_cflict_key_);
            ObLockWaitMgr::get_thread_last_wait_addr_() = retry_ctrl.last_cflict_addr_;
          } else {
            ObLockWaitMgr::get_thread_last_wait_hash_() = 0;
          }
          if (OB_FAIL(retry_ctrl.exec_ret_) && retry_ctrl.sp_.is_valid()) {
            OZ(rollback_to_implicit_savepoint(*tx, retry_ctrl.sp_, retry_ctrl.expire_ts_, nullptr));
          }
          if (OB_FAIL(retry_ctrl.exec_ret_) && retry_ctrl.exec_ret_ != OB_TRY_LOCK_ROW_CONFLICT) {
            OZ(txs_.abort_tx(*tx, OB_TRANS_NEED_ROLLBACK), tx);
            retry_ctrl.tx_end();
            ObMockLockWaitMgr::clear_thread_node();
            tx_retry_control_map_.erase(tx);
            tx_map_.erase(tx->tid().get_id());
            TRANS_LOG(DEBUG, "erase tx desc", KP(this), KP(&tx_map_), K(tx->tid().get_id()));
          } else if (OB_TRY_LOCK_ROW_CONFLICT == retry_ctrl.exec_ret_) {
            retry_ctrl.need_retry_ = true;
            // need retry
            handle_lock_conflict(tx);
            tx->get_conflict_info_array().reset();
            // node->hash_ = retry_ctrl.cflict_key_;
            bool unused = false;
            if(!fake_lock_wait_mgr_.post_process(true, unused)) {
              TRANS_LOG(INFO, "wait failed", KP(this), KPC(tx));
              tx->get_conflict_info_array().reset();
              fake_lock_wait_mgr_.repost(node);
            }
            retry_ctrl.last_cflict_addr_ = node->exec_addr_;
          } else if (OB_SUCC(retry_ctrl.exec_ret_)) {
            retry_ctrl.finish_one_req();
          }
        }
      }
    }
    delete w_resp;
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "not support msg", K(ret), KPC(msg));
  }

  return ret;
}

ObLockWaitNode* ObTxNode::get_wait_head(uint64_t key)
{
  uint64_t hash = hash_rowkey(memtable_->get_tablet_id(), key);
  return fake_lock_wait_mgr_.get_wait_head(hash);
}

ObLockWaitNode* ObTxNode::fetch_wait_head(uint64_t key)
{
  uint64_t hash = hash_rowkey(memtable_->get_tablet_id(), key);
  return fake_lock_wait_mgr_.fetch_wait_head(hash);
}

int ObTxNode::read(ObTxDesc &tx, const int64_t key, int64_t &value, const ObTxIsolationLevel iso)
{
  int ret = OB_SUCCESS;
  ObTxReadSnapshot snapshot;
  OZ(get_read_snapshot(tx,
                       iso,
                       ts_after_ms(50),
                       snapshot));
  OZ(read(snapshot, key, value));
  return ret;
}
int ObTxNode::read(const ObTxReadSnapshot &snapshot,
                   const int64_t key,
                   int64_t &value)
{
  TRANS_LOG(INFO, "read", K(key), K(snapshot), KPC(this));
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(&tenant_);
  ObStoreCtx read_store_ctx;
  read_store_ctx.ls_ = &fake_ls_;
  read_store_ctx.ls_id_ = ls_id_;
  OZ(txs_.get_read_store_ctx(snapshot, false, 5000ll * 1000, read_store_ctx));
  // HACK, refine: mock LS's each member in some way
  read_store_ctx.mvcc_acc_ctx_.tx_table_guards_.tx_table_guard_.init(&fake_tx_table_);
  read_store_ctx.mvcc_acc_ctx_.abs_lock_timeout_ts_ = ObTimeUtility::current_time() + 5000ll * 1000;
  blocksstable::ObDatumRow row;
  {
    ObTableIterParam iter_param;
    ObArenaAllocator allocator;
    ObTableReadInfo read_info;
    const int64_t schema_version = 100;
    read_info.init(allocator, schema_version, 1, false, columns_, nullptr/*storage_cols_index*/);
    iter_param.table_id_ = 1;
    iter_param.tablet_id_ = 100;
    iter_param.read_info_ = &read_info;
    iter_param.out_cols_project_ = NULL;
    iter_param.agg_cols_project_ = NULL;
    iter_param.is_multi_version_minor_merge_ = false;
    iter_param.need_scn_ = true;
    iter_param.pushdown_filter_ = NULL;
    iter_param.vectorized_enabled_ = false;

    storage::ObTableAccessContext access_context;
    {
      access_context.is_inited_ = true;
      access_context.use_fuse_row_cache_ = false;
      access_context.need_scn_ = true;
      access_context.store_ctx_ = &read_store_ctx;
      access_context.query_flag_.read_latest_ = true;
      access_context.stmt_allocator_ = &allocator;
      access_context.allocator_ = &allocator;
    }
    ObDatumRowkey row_key;
    ObStorageDatum row_key_obj;
    ObObj key_obj;
    row_key_obj.set_int(key);
    key_obj.set_int(key);
    row_key.assign(&row_key_obj, 1);
    row_key.store_rowkey_.assign(&key_obj, 1);
    OZ(memtable_->get(iter_param, access_context, row_key, row));
    STORAGE_LOG(INFO, "read_result", K(row), KPC(this));
  }
  OZ(txs_.revert_store_ctx(read_store_ctx));
  if (OB_SUCC(ret)) {
    if (row.row_flag_.is_exist()) {
      ObArenaAllocator allocator;
      blocksstable::ObNewRowBuilder new_row_builder;
      storage::ObStoreRow store_row;
      OZ(new_row_builder.init(columns_, allocator));
      OZ(new_row_builder.build_store_row(row, store_row));
      OX(value = store_row.row_val_.cells_[1].get_int());
    } else if (row.row_flag_.is_not_exist()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected row result", K(ret), K(row.row_flag_), K(row), KPC(this));
    }
  }
  return ret;
}

int ObTxNode::atomic_write(ObTxDesc &tx, const int64_t key, const int64_t value,
                           const int64_t expire_ts, const ObTxParam &tx_param)
{
  int ret = OB_SUCCESS;
  ObTxSEQ sp;
  OZ(create_implicit_savepoint(tx, tx_param, sp, true));
  OZ(write(tx, key, value));
  if (sp.is_valid() && OB_FAIL(ret)) {
    OZ(rollback_to_implicit_savepoint(tx, sp, expire_ts, nullptr));
  }
  return ret;
}
int ObTxNode::write(ObTxDesc &tx, const int64_t key, const int64_t value, const int16_t branch)
{
  int ret = OB_SUCCESS;
  ObTxReadSnapshot snapshot;
   OZ(get_read_snapshot(tx,
                       tx.isolation_,
                       ts_after_ms(50),
                       snapshot));
  OZ(write(tx, snapshot, key, value, branch));
  return ret;
}
int ObTxNode::write(ObTxDesc &tx,
                    const ObTxReadSnapshot &snapshot,
                    const int64_t key,
                    const int64_t value,
                    const int16_t branch)
{
  TRANS_LOG(INFO, "write", K(key), K(value), K(snapshot), K(tx), KPC(this));
  int ret = OB_SUCCESS;
  const transaction::ObSerializeEncryptMeta *encrypt_meta = NULL;
  ObTenantEnv::set_tenant(&tenant_);
  ObStoreCtx write_store_ctx;
  auto iter = new ObTableStoreIterator();
  iter->reset();
  ObITable *mtb = memtable_;
  iter->add_table(mtb);
  write_store_ctx.ls_ = &fake_ls_;
  write_store_ctx.ls_id_ = ls_id_;
  write_store_ctx.table_iter_ = iter;
  write_store_ctx.branch_ = branch;
  write_store_ctx.timeout_ = tx.get_expire_ts();
  concurrent_control::ObWriteFlag write_flag;
  OZ(txs_.get_write_store_ctx(tx,
                              snapshot,
                              write_flag,
                              write_store_ctx));
  write_store_ctx.mvcc_acc_ctx_.tx_table_guards_.tx_table_guard_.init(&fake_tx_table_);
  ObArenaAllocator allocator;
  ObDatumRow row;
  ObStorageDatum cols[2] = {ObStorageDatum(), ObStorageDatum()};
  cols[0].set_int(key);
  cols[1].set_int(value);
  row.count_ = 2;
  row.storage_datums_ = cols;
  row.row_flag_ = blocksstable::ObDmlFlag::DF_UPDATE;
  row.trans_id_.reset();

  ObTableIterParam param;
  ObTableAccessContext context;
  ObVersionRange trans_version_range;
  const bool read_latest = true;
  ObQueryFlag query_flag;
  ObTableReadInfo read_info;

  const int64_t schema_version = 100;
  read_info.init(allocator, 2, 1, false, columns_, nullptr/*storage_cols_index*/);

  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
  query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
  query_flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;

  param.table_id_ = 1;
  param.tablet_id_ = 1;
  param.read_info_ = &read_info;

  context.init(query_flag, write_store_ctx, allocator, trans_version_range);
  const ObMemtableSetArg arg(&row,
                             &columns_,
                             NULL, /*update_idx*/
                             NULL, /*old_row*/
                             1,    /*row_count*/
                             false /*check_exist*/,
                             encrypt_meta);
  OZ(memtable_->set(param, context, arg));
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(txs_.revert_store_ctx(write_store_ctx))) {
    TRANS_LOG(WARN, "revert store ctx failed", KR(tmp_ret), K(write_store_ctx));
  }
  delete iter;
  return ret;
}

// int ObTxNode::get_memtable_key(const int64_t key,
//                                const int64_t value,
//                                ObMemtableKey &mem_key)
// {
//   int ret = OB_SUCCESS;
//   ObArenaAllocator allocator;
//   ObStoreRow row;
//   ObObj cols[2] = {ObObj(key), ObObj(value)};
//   row.capacity_ = 2;
//   row.row_val_.cells_ = cols;
//   row.row_val_.count_ = 2;
//   row.flag_ = blocksstable::ObDmlFlag::DF_UPDATE;
//   row.trans_id_.reset();

//   ObTableIterParam param;
//   ObTableAccessContext context;
//   ObVersionRange trans_version_range;
//   const bool read_latest = true;
//   ObQueryFlag query_flag;
//   ObTableReadInfo read_info;

//   const int64_t schema_version = 100;
//   read_info.init(allocator, 2, 1, false, columns_, nullptr/*storage_cols_index*/);

//   trans_version_range.base_version_ = 0;
//   trans_version_range.multi_version_start_ = 0;
//   trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
//   query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
//   query_flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;

//   param.table_id_ = 1;
//   param.tablet_id_ = 1;
//   param.read_info_ = &read_info;

//   ObMemtableKeyGenerator mtk_generator;
//   if (OB_FAIL(mtk_generator.init(&row, 1, param.get_schema_rowkey_count(), columns_))) {
//     TRANS_LOG(WARN, "fail to generate memtable keys", K(*context.store_ctx_), KR(ret));
//   } else {
//     mem_key = mtk_generator[0];
//   }
//   TRANS_LOG(DEBUG, "generator memtable key", K(ret), K(key), K(value));
//   return ret;
// }

int ObTxNode::write_begin(ObTxDesc &tx,
                          const ObTxReadSnapshot &snapshot,
                          ObStoreCtx& write_store_ctx)
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(&tenant_);
  auto iter = new ObTableStoreIterator();
  iter->reset();
  ObITable *mtb = memtable_;
  iter->add_table(mtb);
  write_store_ctx.ls_id_ = ls_id_;
  write_store_ctx.ls_ = &fake_ls_;
  write_store_ctx.table_iter_ = iter;
  concurrent_control::ObWriteFlag write_flag;
  OZ(txs_.get_write_store_ctx(tx,
                              snapshot,
                              write_flag,
                              write_store_ctx));
  return ret;
}

int ObTxNode::write_one_row(ObStoreCtx& write_store_ctx, const int64_t key, const int64_t value)
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(&tenant_);

  ObArenaAllocator allocator;
  ObTableReadInfo read_info;
  const transaction::ObSerializeEncryptMeta *encrypt_meta = NULL;
  const int64_t schema_version = 100;
  read_info.init(allocator, 2, 1, false, columns_, nullptr/*storage_cols_index*/);
  ObDatumRow row;
  ObStorageDatum cols[2] = {ObStorageDatum(), ObStorageDatum()};
  cols[0].set_int(key);
  cols[1].set_int(value);
  row.row_flag_ = blocksstable::ObDmlFlag::DF_UPDATE;
  row.storage_datums_ = cols;
  row.count_ = 2;

  ObTableIterParam param;
  ObTableAccessContext context;
  ObVersionRange trans_version_range;
  const bool read_latest = true;
  ObQueryFlag query_flag;


  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
  query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
  query_flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;

  param.table_id_ = 1;
  param.tablet_id_ = 1;
  param.read_info_ = &read_info;

  OZ(context.init(query_flag, write_store_ctx, allocator, trans_version_range));

  const ObMemtableSetArg arg(&row,
                             &columns_,
                             NULL, /*update_idx*/
                             NULL, /*old_row*/
                             1,    /*row_count*/
                             false /*check_exist*/,
                             encrypt_meta);

  OZ(memtable_->set(param, context, arg));

  return ret;
}

int ObTxNode::write_end(ObStoreCtx& write_store_ctx)
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(&tenant_);

  delete write_store_ctx.table_iter_;
  write_store_ctx.table_iter_ = nullptr;
  OZ(txs_.revert_store_ctx(write_store_ctx));

  return ret;
}

int ObTxNode::replay(const void *buffer,
                     const int64_t nbytes,
                     const palf::LSN &lsn,
                     const int64_t ts_ns)
{
  ObTenantEnv::set_tenant(&tenant_);
  int ret = OB_SUCCESS;
  logservice::ObLogBaseHeader base_header;
  int64_t tmp_pos = 0;
  const char *log_buf = static_cast<const char *>(buffer);
  if (OB_FAIL(base_header.deserialize(log_buf, nbytes, tmp_pos))) {
    LOG_WARN("log base header deserialize error", K(ret));
  } else {
    share::SCN log_scn;
    log_scn.convert_for_tx(ts_ns);
    ObFakeTxReplayExecutor executor(&fake_ls_,
                                    ls_id_,
                                    tenant_id_,
                                    fake_ls_.get_tx_svr(),
                                    lsn,
                                    log_scn,
                                    base_header);
    executor.set_memtable(memtable_);
    if (OB_FAIL(executor.execute(log_buf, nbytes, tmp_pos))) {
      LOG_WARN("replay tx log error", K(ret), K(lsn), K(ts_ns));
    } else {
      LOG_INFO("replay tx log succ", K(ret), K(lsn), K(ts_ns));
    }
  }
  return ret;
}

void ObTxNode::init_tx_fake_request(ObTxDesc &tx,
                                    const std::vector<SingleWrite> &writes,
                                    int64_t expire_ts,
                                    const ObTxParam &tx_param)
{
  tx_retry_control_map_[&tx].req_writes_ = writes;
  tx_retry_control_map_[&tx].total_task_num_ = writes.size();
  tx_retry_control_map_[&tx].unfinished_task_num_ = writes.size();
  tx_retry_control_map_[&tx].expire_ts_ = expire_ts;
  tx_retry_control_map_[&tx].tx_param_ = tx_param;
  LOG_DEBUG("tx id", K(tx.tid().get_id()), K(tx_map_[tx.tid().get_id()]), KP(this));
}

// key should be key&1 == 1, to avoid to be regarded as dummy node in lock wait mgr
int ObTxNode::write_req_with_lock_wait(ObTxDesc &tx,
                                       const std::vector<SingleWrite> &writes,
                                       const int64_t expire_ts,
                                       const ObTxParam &tx_param,
                                       bool &wait_succ,
                                       bool need_change_seq)
{
  int ret = OB_SUCCESS;
  init_tx_fake_request(tx, writes, expire_ts, tx_param);
  bool need_wait = false;
  wait_succ = false;
  need_wait = false;
  ObLockWaitNode *node = NULL;
  ObTxRetryControl &retry_ctrl = tx_retry_control_map_[&tx];
  retry_ctrl.exec_ret_ = OB_SUCCESS;
  retry_ctrl.need_retry_ = false;
  node = tx_retry_control_map_[&tx].node_;
  if (NULL == node) {
    node = new ObLockWaitNode;
    retry_ctrl.node_ = node;
    node->sessid_ = 1;
    lock_wait_node_to_tx_map_[node] = &tx;
  }
  retry_ctrl.last_cflict_key_= retry_ctrl.cflict_key_;
  retry_ctrl.last_cflict_addr_ = node->exec_addr_;
  retry_ctrl.cflict_key_= 0;
  int64_t recv_ts = ObTimeUtility::current_time();
  node->recv_ts_ = 0;
  // node->last_wait_hash_ = hash_rowkey(memtable_->get_tablet_id(), retry_ctrl.last_cflict_key_);
  setup(*node, recv_ts);
  ObTxSEQ sp;
  OZ(create_implicit_savepoint(tx, tx_param, sp, true));
  tx_map_[tx.tid().get_id()] = &tx;
  retry_ctrl.sp_ = sp;
  int unfinished_task_num = -1;
  int64_t cflict_key = -1;
  for (auto &single_write : writes) {
    int tmp_ret = OB_SUCCESS;
    if (single_write.runner_addr_ == addr_) {
      // local task
      tmp_ret = write(tx, single_write.key_value_.first, single_write.key_value_.second);
      if (tmp_ret != OB_TRY_LOCK_ROW_CONFLICT && tmp_ret != OB_SUCCESS) {
        retry_ctrl.exec_ret_ = tmp_ret;
      } else if (tmp_ret == OB_TRY_LOCK_ROW_CONFLICT && retry_ctrl.exec_ret_ != OB_TRY_LOCK_ROW_CONFLICT) {
        retry_ctrl.exec_ret_ = retry_ctrl.exec_ret_ == OB_SUCCESS ? tmp_ret : retry_ctrl.exec_ret_;
        retry_ctrl.cflict_key_ = single_write.key_value_.first;
      }
      ret = retry_ctrl.exec_ret_;
      unfinished_task_num = retry_ctrl.dec_unfinished_task_num();
    } else {
      // remote task
      ObFakeWriteRequestMsg msg;
      msg.send_addr_ = addr_;
      msg.tx_ = &tx;
      msg.key_ = single_write.key_value_.first;
      msg.value_ = single_write.key_value_.second;
      msg.expire_ts_ = expire_ts;
      msg.tx_param_ = tx_param;
      msg.inc_seq_ = need_change_seq;
      if (OB_FAIL(fake_rpc_.post_msg(single_write.runner_addr_, msg))) {
        TRANS_LOG(WARN, "post remote write request fail",
          K(ret), K(tx), K(msg), K(expire_ts), K(tx_param));
      }
    }
  }
  if (unfinished_task_num == 0) {
    if (OB_FAIL(retry_ctrl.exec_ret_) && retry_ctrl.sp_.is_valid()) {
      OZ(rollback_to_implicit_savepoint(tx, retry_ctrl.sp_, expire_ts, nullptr));
    }
    if (retry_ctrl.exec_ret_ == OB_TRY_LOCK_ROW_CONFLICT) {
      retry_ctrl.need_retry_ = true;
      // need retry
      handle_lock_conflict(&tx);
      tx.get_conflict_info_array().reset();
      if (need_change_seq) {
        fake_lock_wait_mgr_.inc_seq(node->hash_);
        fake_lock_wait_mgr_.inc_seq(retry_ctrl.cflict_key_);
      }
      // node->hash_ = retry_ctrl.cflict_key_;
      wait_succ = fake_lock_wait_mgr_.post_process(true, need_wait);
      if (!wait_succ && !need_change_seq) {
        // not repost for change seq test
        fake_lock_wait_mgr_.repost(node);
      }
    } else if (OB_FAIL(retry_ctrl.exec_ret_)) {
      OZ(txs_.abort_tx(tx, OB_TRANS_NEED_ROLLBACK), tx);
      retry_ctrl.tx_end();
      ObMockLockWaitMgr::clear_thread_node();
      tx_retry_control_map_.erase(&tx);
      tx_map_.erase(tx.tid().get_id());
      TRANS_LOG(DEBUG, "erase tx desc", KP(this), KP(&tx_map_), K(tx.tid().get_id()));
    } else if (OB_SUCC(retry_ctrl.exec_ret_)) {
      retry_ctrl.finish_one_req();
    }
  }
  TRANS_LOG(DEBUG, "write request", K(ret), K(retry_ctrl), K(wait_succ), K(writes[0].key_value_.first), K(writes[0].key_value_.second), K(tx));
  return ret;
}

int ObTxNode::single_atomic_write_request(ObTxDesc &tx,
                                          const ObAddr &runner_addr,
                                          const int64_t key,
                                          const int64_t value,
                                          const int64_t expire_ts,
                                          const ObTxParam &tx_param,
                                          bool &wait_succ,
                                          bool need_change_seq)
{
  SingleWrite single_write;
  single_write.key_value_ = {key, value};
  single_write.runner_addr_ = runner_addr;
  return write_req_with_lock_wait(tx, {single_write}, expire_ts, tx_param, wait_succ, need_change_seq);
}


int ObTxNode::commit_with_retry_ctrl(ObTxDesc &tx, const int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(txs_.commit_tx(tx, expire_ts, NULL))) {
    LOG_WARN("commit tx error", K(ret), K(tx), K(expire_ts));
  } else if (tx_retry_control_map_.find(&tx) != tx_retry_control_map_.end()) {
    ObTxRetryControl &retry_ctrl = tx_retry_control_map_[&tx];
    // if (!retry_ctrl.tx_writes_empty()) {
    //   // wake up all lock wait tx
    //   std::vector<int64_t> &tx_writes = retry_ctrl.tx_writes_hash_;
    //   for (auto hash : tx_writes) {
    //     fake_lock_wait_mgr_.wakeup_key(hash);
    //   }
    // }
    delete retry_ctrl.node_;
    retry_ctrl.tx_end();
    ObMockLockWaitMgr::clear_thread_node();
    tx_retry_control_map_.erase(&tx);
    tx_map_.erase(tx.tid().get_id());
    TRANS_LOG(DEBUG, "erase tx desc", KP(this), KP(&tx_map_), K(tx.tid().get_id()));
  }
  return ret;
}


int ObTxNode::handle_request_retry_(ObLockWaitNode *node)
{
  int ret = OB_SUCCESS;
  bool unused = false;
  auto tx_iter = lock_wait_node_to_tx_map_.find(node);
  bool stop_retry = ATOMIC_LOAD(&stop_request_retry_);
  TRANS_LOG(DEBUG, "retry request", K(ret), KPC(node));
  if (tx_iter == lock_wait_node_to_tx_map_.end()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx has already end", K(ret), KPC(node));
  } else if (stop_retry) {
    TRANS_LOG(INFO, "retry has paused", K(ret), KPC(node));
  } else {
    ObTxDesc *tx = tx_iter->second;
    if (tx == NULL) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "tx should not be NULL", K(ret), KPC(node));
    } else if (tx_retry_control_map_.find(tx) == tx_retry_control_map_.end()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "tx has no need retry write", K(ret), KPC(node), KPC(tx));
    } else {
      ObTxRetryControl &retry_ctrl = tx_retry_control_map_[tx];
      bool unused = false;
      write_req_with_lock_wait(*tx, retry_ctrl.req_writes_,
        retry_ctrl.expire_ts_, retry_ctrl.tx_param_, unused);
    }
  }
  return ret;
}

} // transaction
} // oceanbase
