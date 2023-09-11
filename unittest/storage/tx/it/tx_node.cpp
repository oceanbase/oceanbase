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
#include "share/scn.h"
#define FAST_FAIL() \
  do {                                                          \
  if (OB_FAIL(ret)) {                                           \
    TRANS_LOG(ERROR, "[tx node crash] fast-fail for easy debug", K(ret)); \
    ob_abort();                                                 \
  }                                                             \
} while(0);

namespace oceanbase {
namespace common
{
void* ObGMemstoreAllocator::alloc(AllocHandle& handle, int64_t size)
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


ObTxNode::ObTxNode(const int64_t ls_id,
                   const ObAddr &addr,
                   MsgBus &msg_bus) :
  name_(name_buf_),
  addr_(addr),
  ls_id_(ls_id),
  tenant_id_(1001),
  tenant_(tenant_id_),
  fake_part_trans_ctx_pool_(1001, false, false, 4),
  memtable_(NULL),
  msg_consumer_(ObString("TxNode"),
                &msg_queue_,
                std::bind(&ObTxNode::handle_msg_,
                          this, std::placeholders::_1)),
  t3m_(tenant_id_),
  fake_rpc_(&msg_bus, addr, &get_location_adapter_()),
  lock_memtable_(),
  fake_tx_log_adapter_(nullptr)
{
  fake_part_trans_ctx_pool_.init();
  addr.to_string(name_buf_, sizeof(name_buf_));
  msg_consumer_.set_name(name_);
  role_ = Leader;
  tenant_.set(&fake_tenant_freezer_);
  tenant_.set(&fake_part_trans_ctx_pool_);
  ObTenantEnv::set_tenant(&tenant_);
  ObTableHandleV2 lock_memtable_handle;
  lock_memtable_handle.set_table(&lock_memtable_, &t3m_, ObITable::LOCK_MEMTABLE);
  fake_lock_table_.is_inited_ = true;
  fake_lock_table_.parent_ = &fake_ls_;
  lock_memtable_.key_.table_type_ = ObITable::LOCK_MEMTABLE;
  fake_ls_.ls_tablet_svr_.lock_memtable_mgr_.t3m_ = &t3m_;
  fake_ls_.ls_tablet_svr_.lock_memtable_mgr_.table_type_ = ObITable::TableType::LOCK_MEMTABLE;
  fake_ls_.ls_tablet_svr_.lock_memtable_mgr_.add_memtable_(lock_memtable_handle);
  fake_tenant_freezer_.is_inited_ = true;
  fake_tenant_freezer_.tenant_info_.is_loaded_ = true;
  fake_tenant_freezer_.tenant_info_.mem_memstore_limit_ = INT64_MAX;
  // memtable.freezer
  fake_freezer_.freeze_flag_ = 0;// is_freeze() = false;
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
  OZ(msg_consumer_.start());
  OZ(txs_.start());
  OZ(create_ls_(ls_id_));
  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  OZ(txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(ls_id_, ls_tx_ctx_mgr));
  if (ObTxNodeRole::Leader == role_) {
    OZ(ls_tx_ctx_mgr->switch_to_leader());
    wait_all_redolog_applied();
    CK(ls_tx_ctx_mgr->is_master_());
  }
  if (ls_tx_ctx_mgr) {
    fake_tx_table_.tx_ctx_table_.ls_tx_ctx_mgr_ = ls_tx_ctx_mgr;
    fake_tx_table_.is_inited_ = true;
    fake_tx_table_.ls_ = &fake_ls_;
    fake_tx_table_.online();
    int tx_data_table_offset = offsetof(storage::ObTxTable, tx_data_table_);
    void* ls_tx_data_table_ptr = (void*)((int64_t)&(mock_ls_.tx_table_) + tx_data_table_offset);
    ls_tx_data_table_ptr = &fake_tx_table_.tx_data_table_;
    mock_ls_.tx_table_.is_inited_ = true;
    mock_ls_.tx_table_.online();
    mock_ls_.ls_meta_.clog_checkpoint_scn_ = share::SCN::max_scn();
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
  bool is_sync_msg_ = false;
  TxMsgCallbackMsg callback_msg_;
  int16_t msg_type_;
  int64_t size_;
  const char* buf_;
  TO_STRING_KV(K_(msg_ptr),
               K_(recv_time),
               K_(sender),
               K_(is_callback_msg),
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
  if (cat == 1) {
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
  while(OB_NOT_NULL((msg = (MsgPack*)msg_queue_.pop()))) {
    ++i;
    MsgInfo msg_info;
    OZ (get_msg_info(msg, msg_info));
    TRANS_LOG(INFO,"[dump_msg]", K(i), K(msg_info), K(ret), KPC(this));
  }
}

ObTxNode::~ObTxNode() __attribute__((optnone)) {
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "destroy TxNode", KPC(this));
  ObTenantEnv::set_tenant(&tenant_);
  OZ(txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(fake_tx_table_.tx_ctx_table_.ls_tx_ctx_mgr_));
  fake_tx_table_.tx_ctx_table_.ls_tx_ctx_mgr_ = nullptr;
  OX(txs_.stop());
  OZ(txs_.wait_());
  if (role_ == Leader && fake_tx_log_adapter_) {
    OZ(drop_ls_(ls_id_));
    fake_tx_log_adapter_->stop();
    fake_tx_log_adapter_->wait();
    fake_tx_log_adapter_->destroy();
  }
  msg_consumer_.stop();
  msg_consumer_.wait();
  dump_msg_queue_();
  //fake_tx_table_.is_inited_ = false;
  if (memtable_) {
    delete memtable_;
  }
  if (role_ == Leader && fake_tx_log_adapter_) {
    delete fake_tx_log_adapter_;
  }
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
  ls_handle.set_ls(fake_ls_map_, fake_ls_, ObLSGetMod::DATA_MEMTABLE_MOD);
  OZ (t->init(table_key, ls_handle, &fake_freezer_, &fake_memtable_mgr_, 0, 0));
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
                                *mock_ls_.get_tx_svr(),
                                (ObITxLogParam*)0x01,
                                fake_tx_log_adapter_));
  if (Leader == role_) {
    OZ(get_location_adapter_().fill(ls_id, addr_));
  }
  return ret;
}

int ObTxNode::drop_ls_(const ObLSID ls_id) {
  int ret = OB_SUCCESS;
  OZ(txs_.tx_ctx_mgr_.remove_ls(ls_id, true));
  OZ(get_location_adapter_().remove(ls_id));
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
  msg_queue_.push(pkt);
  msg_consumer_.wakeup();
  return ret;
}

int ObTxNode::sync_recv_msg(const ObAddr &sender, ObString &m, ObString &resp)
{
  TRANS_LOG(INFO, "sync_recv_msg", K(sender), "msg_ptr", OB_P(m.ptr()), KPC(this));
  int ret = OB_SUCCESS;
  auto pkt = new MsgPack(sender, m, true);
  msg_queue_.push(pkt);
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
    ret = OB_NOT_SUPPORTED;
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
  read_store_ctx.ls_ = &mock_ls_;
  read_store_ctx.ls_id_ = ls_id_;
  OZ(txs_.get_read_store_ctx(snapshot, false, 5000ll * 1000, read_store_ctx));
  // HACK, refine: mock LS's each member in some way
  read_store_ctx.mvcc_acc_ctx_.tx_table_guards_.tx_table_guard_.init(&fake_tx_table_);
  read_store_ctx.mvcc_acc_ctx_.abs_lock_timeout_ = ObTimeUtility::current_time() + 5000ll * 1000;
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
      storage::ObStoreRow store_row;
      OZ(row.to_store_row(columns_, store_row));
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
int ObTxNode::write(ObTxDesc &tx, const int64_t key, const int64_t value)
{
  int ret = OB_SUCCESS;
  ObTxReadSnapshot snapshot;
   OZ(get_read_snapshot(tx,
                       tx.isolation_,
                       ts_after_ms(50),
                       snapshot));
  OZ(write(tx, snapshot, key, value));
  return ret;
}
int ObTxNode::write(ObTxDesc &tx,
                    const ObTxReadSnapshot &snapshot,
                    const int64_t key,
                    const int64_t value)
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
  write_store_ctx.ls_ = &mock_ls_;
  write_store_ctx.ls_id_ = ls_id_;
  write_store_ctx.table_iter_ = iter;
  concurrent_control::ObWriteFlag write_flag;
  OZ(txs_.get_write_store_ctx(tx,
                              snapshot,
                              write_flag,
                              write_store_ctx));
  write_store_ctx.mvcc_acc_ctx_.tx_table_guards_.tx_table_guard_.init(&fake_tx_table_);
  ObArenaAllocator allocator;
  ObStoreRow row;
  ObObj cols[2] = {ObObj(key), ObObj(value)};
  row.capacity_ = 2;
  row.row_val_.cells_ = cols;
  row.row_val_.count_ = 2;
  row.flag_ = blocksstable::ObDmlFlag::DF_UPDATE;
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
  OZ(memtable_->set(param, context, columns_, row, encrypt_meta));
  OZ(txs_.revert_store_ctx(write_store_ctx));
  delete iter;
  return ret;
}

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
  ObStoreRow row;
  ObObj cols[2] = {ObObj(key), ObObj(value)};
  row.flag_ = blocksstable::ObDmlFlag::DF_INSERT;
  row.row_val_.cells_ = cols;
  row.row_val_.count_ = 2;

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

  OZ(memtable_->set(param, context, columns_, row, encrypt_meta));

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
  } else if (OB_FAIL(ObFakeTxReplayExecutor::execute(&mock_ls_, mock_ls_.get_tx_svr(), log_buf, nbytes,
                                                     tmp_pos, lsn, ts_ns, base_header.get_replay_hint(),
                                                     ls_id_, tenant_id_, memtable_))) {
    LOG_WARN("replay tx log error", K(ret), K(lsn), K(ts_ns));
  } else {
    LOG_INFO("replay tx log succ", K(ret), K(lsn), K(ts_ns));
  }
  return ret;
}
} // transaction
} // oceanbase
