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

#include <gtest/gtest.h>
#include <thread>
#include <functional>
#define private public
#define protected public
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#define USING_LOG_PREFIX TRANS
#include "tx_node.h"
#include "../mock_utils/async_util.h"
#include "test_tx_dsl.h"
namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;
namespace omt {
  bool the_ctrl_of_enable_transaction_free_route = true;
  ObTenantConfig *ObTenantConfigMgr::get_tenant_config_with_lock(const uint64_t tenant_id,
                                                                 const uint64_t fallback_tenant_id /* = 0 */,
                                                                 const uint64_t timeout_us /* = 0 */) const
  {
    static ObTenantConfig cfg;
    cfg._enable_transaction_internal_routing = the_ctrl_of_enable_transaction_free_route;
    cfg.writing_throttling_trigger_percentage = 100;
    return &cfg;
  }
}

namespace transaction {
/*
 * tx free route:
 *
 * 1) Request ---(1)----> Node 1 ----> Response
 * Request : Action + Tx-State-List
 * (1):
 * 1. session.sync_tx_state
 * 2. session.post_tx_state_sync
 * 3. handle Action
 * 4. session.calc_tx_state
 * 5. Response.add_sync_state
 * 6. Response.add_result
 *
 * -----------------------------------------------
 * Mock Proxy
 * Proxy:
 *  1. add_backends_(ObServerNode, ObServerNode)
 *  2. handle(request, response)
 *      route_table.route(request.type, server)
 *      server.handle(request, response)
 *  {
 *     Route_Table : route(RequestType, ObServerNode)
 *     session_state: state_list_ { ObString, }
 *  }
 * Mock ObServer
 * 1. handle(request, response)
 *     sync_tx_state(request.sate_)
 * 2. push_state(PushStateMsg, Resp)
 *     sync_tx_state(msg)
 * 3. check_tx_alive(Msg)
 */
struct SyncTxState {
  enum T { STATIC, DYNAMIC, PARTS, EXTRA } type_;
  ObString data_;
  const char* type_name_(T t) const {
    static const char * const names[] = {
      "STATIC", "DYNAMIC", "PARTS", "EXTRA"
    };
    return names[t];
  }
  TO_STRING_KV("type", type_name_(type_), K(data_.length()))
};

struct ObReq {
  enum T { START_TX, COMMIT_TX, ROLLBACK_TX, READ, WRITE, DUMMY_WRITE, SAVEPOINT, ROLLBACK_SAVEPOINT, RELEASE_SAVEPOINT } type_;
  union {
    int64_t read_key_;
    int64_t write_key_;
    const char *savepoint_name_;
  };
  int stick_hash_ = 0;
  bool is_serializable_isolation_ = false;
  int64_t write_value_;
  bool txn_free_route_support_;
  ObSEArray<SyncTxState, 4> tx_state_list_;
  void *hook_ctx_ = NULL;
  const char* type_name_(T t) const {
    static const char * const names[] = {
    "START_TX", "COMMIT_TX", "ROLLBACK_TX", "READ", "WRITE", "DUMMY_WRITE", "SAVEPOINT", "ROLLBACK_SAVEPOINT", "RELEASE_SAVEPOINT" };
    return names[t];
  }
  TO_STRING_KV("type", type_name_(type_), K_(read_key), K_(write_key), K_(txn_free_route_support), K_(tx_state_list));
public:
  void set_stick_hash(int i) { stick_hash_ = i; }
  static ObReq mk_start_tx() { ObReq req; req.type_ = START_TX; return req; }
  static ObReq mk_commit_tx() { ObReq req; req.type_ = COMMIT_TX; return req; }
  static ObReq mk_rollback_tx() { ObReq req; req.type_ = ROLLBACK_TX; return req; }
  static ObReq mk_savepoint(const char *name) {
    ObReq req;
    req.type_ = SAVEPOINT;
    req.savepoint_name_ = name;
    return req;
  }
  static ObReq mk_rollback_savepoint(const char *name) {
    ObReq req;
    req.type_ = ROLLBACK_SAVEPOINT;
    req.savepoint_name_ = name;
    return req;
  }
  static ObReq mk_release_savepoint(const char *name) {
    ObReq req;
    req.type_ = RELEASE_SAVEPOINT;
    req.savepoint_name_ = name;
    return req;
  }
  static ObReq mk_read(const int64_t read_key) {
    ObReq req;
    req.type_ = READ;
    req.is_serializable_isolation_ = false;
    req.read_key_ = read_key;
    return req;
  }
  static ObReq mk_serializable_read(const int64_t read_key) {
    ObReq req;
    req.type_ = READ;
    req.is_serializable_isolation_ = true;
    req.read_key_ = read_key;
    return req;
  }
  static ObReq mk_write(const int64_t write_key, const int64_t write_value) {
    ObReq req;
    req.type_ = WRITE;
    req.write_key_ = write_key;
    req.write_value_ = write_value;
    return req;
  }
  static ObReq mk_dummy_write(const int64_t write_key, const int64_t write_value) {
    ObReq req;
    req.type_ = DUMMY_WRITE;
    req.write_key_ = write_key;
    req.write_value_ = write_value;
    return req;
  }
};

struct ObResp {
  int64_t read_value_;
  ObTransID tx_id_;
  bool in_txn_;
  bool can_free_route_;
  ObSEArray<SyncTxState, 4> tx_state_list_;
  int ret_;
  TO_STRING_KV(K_(ret), K_(tx_id), K_(in_txn), K_(can_free_route), K_(tx_state_list));
};

class MockObServer {
public:
  MockObServer(const int64_t ls_id, const char*addr, const int32_t port, MsgBus &msg_bus)
    : addr_(ObAddr(ObAddr::VER::IPV4, addr, port)),
      tx_node_(ls_id, addr_, msg_bus),
      allocator_(), session_()
  {
    session_.test_init(1, 1111, 2222, &allocator_);
    tx_node_.extra_msg_handler_ = [this](int type, void *msg) -> int {
      return this->handle_msg_(type, msg);
    };
  }
  ~MockObServer() {
    if (session_.get_tx_desc()) {
      tx_node_.release_tx(*session_.get_tx_desc());
      session_.get_tx_desc() = NULL;
    }
  }
  int start() { return tx_node_.start(); }
public:
  int handle(ObReq&, ObResp&);
  int receive_push_tx_state(ObTxFreeRoutePushState &state);
  int receive_check_tx_alive(ObTxFreeRouteCheckAliveMsg &msg);
public:
  enum HOOK { RECV_REQ, POST_SYNC_STATE, PRE_HANDLE, POST_HANDLE, POST_CALC_FREE_ROUTE, PRE_SEND_RESP, MAX_HOOK_NUM};
  void setup_hook(HOOK hk, std::function<void(MockObServer &, ObReq&, ObResp&)> func)
  {
    hooks_[hk] = func;
  }
  void reset_hooks() {
    for(int i =0; i<MAX_HOOK_NUM; i++) hooks_[i] = nullptr;
  }
  int check_tx_exist(ObTransID *tx_id) {
    int ret = OB_SUCCESS;
    ObTxDesc *it = NULL;
    if (OB_FAIL(tx_node_.txs_.tx_desc_mgr_.get(*tx_id, it))) {
    } else if (it) {
      tx_node_.txs_.tx_desc_mgr_.revert(*it);
    }
    return ret;
  }
  int check_tx_sanity() {
    int ret = OB_SUCCESS;
    auto tx_id = session_.tx_desc_->tx_id_;
    ObTxDesc *it = NULL;
    if (OB_FAIL(tx_node_.txs_.tx_desc_mgr_.get(tx_id, it))) {
    } else if (it != session_.tx_desc_) {
      ret = OB_ERR_UNEXPECTED;
    }
    if (it) tx_node_.txs_.tx_desc_mgr_.revert(*it);
    return ret;
  }
private:
  std::function<void(MockObServer&, ObReq&, ObResp&)> hooks_[MAX_HOOK_NUM];
private:
  int do_handle_(ObReq &req, ObResp &resp);
  int assign_resp_tx_state_(ObResp &resp, ObTxDesc *tx_desc, ObTxnFreeRouteCtx &ctx);
  int handle_msg_(int type, void *msg);
  int handle_msg_check_alive_resp__(ObTxFreeRouteCheckAliveRespMsg *msg);
  ObAddr addr_;
  ObTxNode tx_node_;
  ObMalloc allocator_;
  sql::ObSQLSessionInfo session_;
  TO_STRING_KV(K_(tx_node), K_(session));
};

class MockObProxy {
public:
  MockObProxy(const char*addr, const int32_t port, MsgBus &msg_bus)
    : addr_(ObAddr::VER::IPV4, addr, port), msg_bus_(msg_bus),
      txn_free_route_support_(true),
      can_free_route_(false),
      tx_state_list_(),
      in_txn_(false),
      tx_start_backend_(NULL),
      backends_()
  {}
public:
  struct TxStateInfo {
    SyncTxState state_;
    int version_;
    TO_STRING_KV(K_(state), K_(version))
  };
  ObAddr addr_;
  MsgBus &msg_bus_;
  bool txn_free_route_support_;
  bool can_free_route_;
  TxStateInfo tx_state_list_[4];
  struct BackendInfo {
    BackendInfo(): server_(NULL), tx_state_version_() {}
    MockObServer *server_;
    int tx_state_version_[4];
    TO_STRING_KV(K_(server),
                 "TxVer_0", tx_state_version_[0],
                 "TxVer_1", tx_state_version_[1],
                 "TxVer_2", tx_state_version_[2],
                 "TxVer_3", tx_state_version_[3]
                 );
  };
public:
  int add_backend(MockObServer &server);
  int handle(ObReq&, ObResp&);
public:
  enum HOOK { POST_ROUTE, PRE_HANDLE_RESP, POST_HANDLE_RESP, MAX_HOOK_NUM };
  void setup_hook(HOOK hk, std::function<void(MockObProxy&, ObReq&, ObResp&, BackendInfo*)> func)
  {
    hooks_[hk] = func;
  }
  void reset_hooks() {
    for(int i =0; i<MAX_HOOK_NUM; i++) hooks_[i] = nullptr;
  }
private:
  std::function<void(MockObProxy&, ObReq&, ObResp&, BackendInfo*)> hooks_[MAX_HOOK_NUM];
private:
  int route_(ObReq &req, BackendInfo *&backend);
  int do_handle_(ObReq &req, ObResp &resp);
  int assign_req_tx_state_(ObReq &req, BackendInfo *backend);
  int update_backend_tx_state_(ObResp &resp, BackendInfo *backend);
  bool in_txn_;
  BackendInfo *tx_start_backend_;
  ObSEArray<BackendInfo, 4> backends_;
};

int MockObProxy::add_backend(MockObServer &server)
{
  int ret = OB_SUCCESS;
  BackendInfo b;
  b.server_ = &server;
  backends_.push_back(b);
  return ret;
}

#define CALL_PROXY_HOOK(HK_NAME)                                        \
  do {                                                                  \
    if (hooks_[HK_NAME]) { hooks_[HK_NAME](*this, req, resp, backend); } \
  } while(0)

int MockObProxy::handle(ObReq &req, ObResp &resp)
{
  int ret = OB_SUCCESS;
  BackendInfo *backend = NULL;
  route_(req, backend);
  CALL_PROXY_HOOK(POST_ROUTE);
  assign_req_tx_state_(req, backend);
  TRANS_LOG(INFO, "[MockObProxy] send req:", K(req));
  backend->server_->handle(req, resp);
  TRANS_LOG(INFO, "[MockObProxy] receive resp:", K(resp));
  CALL_PROXY_HOOK(PRE_HANDLE_RESP);
  if (resp.in_txn_ && !in_txn_) {
    in_txn_ = true;
    tx_start_backend_ = backend;
  } else if (!resp.in_txn_ && in_txn_) {
    in_txn_ = false;
    tx_start_backend_ = NULL;
  }
  if (in_txn_) {
    can_free_route_ = resp.can_free_route_;
  }
  update_backend_tx_state_(resp, backend);
  CALL_PROXY_HOOK(POST_HANDLE_RESP);
  return ret;
}

int MockObProxy::route_(ObReq &req, BackendInfo *&backend)
{
  int ret = OB_SUCCESS;
  backend = NULL;
  bool free_route = !in_txn_ || can_free_route_;
  if (free_route) {
    switch(req.type_) {
    case ObReq::T::READ:
      backend = &backends_[req.read_key_ % backends_.count()];
      break;
    case ObReq::T::WRITE:
    case ObReq::T::DUMMY_WRITE:
      backend = &backends_[req.write_key_ % backends_.count()];
      break;
    default:
      if (in_txn_) {
        backend = tx_start_backend_;
      } else if (req.stick_hash_){
        backend = &backends_[req.stick_hash_ % backends_.count()];
      } else {
        backend = &backends_[random() % backends_.count()];
      }
    }
  } else {
    backend = tx_start_backend_;
  }
  if (!backend) {
    TRANS_LOG(ERROR, "[MockProxy] tx backend is null");
    ob_abort();
  }
  TRANS_LOG(INFO, "[MockObProxy] choose backend:", KPC(backend), K(req), K_(in_txn),
            "TxVer_0", tx_state_list_[0].version_,
            "TxVer_1", tx_state_list_[1].version_,
            "TxVer_2", tx_state_list_[2].version_,
            "TxVer_3", tx_state_list_[3].version_);
  return ret;
}

int MockObProxy::assign_req_tx_state_(ObReq &req, BackendInfo *backend)
{
  int ret = OB_SUCCESS;
  req.tx_state_list_.reset();
  for (int i =0; i < 4; i++) {
    if (backend->tx_state_version_[i] < tx_state_list_[i].version_) {
      req.tx_state_list_.push_back(tx_state_list_[i].state_);
    }
  }
  req.txn_free_route_support_ = in_txn_ ? can_free_route_ : txn_free_route_support_;
  return ret;
}

int MockObProxy::update_backend_tx_state_(ObResp &resp, BackendInfo *backend)
{
  int ret = OB_SUCCESS;
  for (int i =0; i < 4; i++) {
    if (backend->tx_state_version_[i] > tx_state_list_[i].version_) {
      TRANS_LOG(ERROR, "!!bug");
      ob_abort();
    } else {
      backend->tx_state_version_[i] = tx_state_list_[i].version_;
    }
  }
  for (int i =0; i < resp.tx_state_list_.count(); i++ ) {
    auto &st = resp.tx_state_list_[i];
    auto t = st.type_;
    tx_state_list_[t].version_ +=1;
    tx_state_list_[t].state_ = st;
    backend->tx_state_version_[t] = tx_state_list_[t].version_;
  }
  return ret;
}

#define LOWER_STATIC static
#define LOWER_DYNAMIC dynamic
#define LOWER_PARTS parts
#define LOWER_EXTRA extra
#define UPPER_static STATIC
#define UPPER_dynamic DYNAMIC
#define UPPER_parts PARTS
#define UPPER_extra EXTRA

#define CALL_SERVER_HOOK(HK_NAME)                                       \
  do {                                                                  \
    if (hooks_[HK_NAME]) { hooks_[HK_NAME](*this, req, resp); }         \
  } while(0)

int MockObServer::handle(ObReq &req, ObResp &resp)
{
  GCONF.self_addr_ = addr_;
  int ret = OB_SUCCESS;
  CALL_SERVER_HOOK(RECV_REQ);
  auto &tx_desc = session_.get_tx_desc();
  session_.set_txn_free_route(req.txn_free_route_support_);
  auto &free_route_ctx = session_.get_txn_free_route_ctx();
  for (int i =0; i < req.tx_state_list_.count(); i++ ) {
    auto &s = req.tx_state_list_.at(i);
    const char *buf = s.data_.ptr();
    int64_t len = s.data_.length();
    int64_t pos = 0;
    switch(s.type_) {
#define TX_STATE_UPDATE__(T, tn)                                        \
      case SyncTxState::T:                                              \
        if (OB_SUCC(ret) &&                                             \
            OB_FAIL(tx_node_.txn_free_route__update_##tn##_state(session_.get_sessid(), tx_desc, free_route_ctx, buf, len, pos))) { \
          TRANS_LOG(ERROR, "update txn state fail", K(ret), "type", #T); \
        } else if (pos != len) {                                        \
          TRANS_LOG(WARN, "[maybe] pos != len, consume buffer incomplete", K(ret), K(pos), K(len), "state_type", #T); \
        }                                                               \
        break;
#define TX_STATE_UPDATE_(T, tn) TX_STATE_UPDATE__(T, tn)
#define TX_STATE_UPDATE(T) TX_STATE_UPDATE_(T, CONCAT(LOWER_, T))
      LST_DO(TX_STATE_UPDATE, (), STATIC, DYNAMIC, PARTS, EXTRA);
#undef TX_STATE_UPDATE_
#undef TX_STATE_UPDATE
    default:
      ob_abort();
    }
  }
  CALL_SERVER_HOOK(POST_SYNC_STATE);
  session_.post_sync_session_info();
  CALL_SERVER_HOOK(PRE_HANDLE);
  do_handle_(req, resp);
  // ObTenantEnv::set_tenant(&tx_node_.tenant_);
  // omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  // bool tnt_enable = tenant_config->enable_transaction_free_route;
  // TRANS_LOG(INFO, "[MockObServer] tenant_confi:", K(tenant_config.is_valid()), K(tnt_enable));
  CALL_SERVER_HOOK(POST_HANDLE);
  TRANS_LOG(INFO, "[MockObServer] before calc_txn_free_route", "my_addr", addr_,
            "txn_free_route_ctx", session_.get_txn_free_route_ctx());
  if (OB_FAIL(tx_node_.calc_txn_free_route(tx_desc, session_.get_txn_free_route_ctx()))) {
    TRANS_LOG(ERROR, "calc_txn_free_route fail", K(ret));
    ob_abort();
  } else {
    TRANS_LOG(INFO, "[MockObServer] after calc_txn_free_route", "my_addr", addr_,
              "txn_free_route_ctx", session_.get_txn_free_route_ctx());
  }
  CALL_SERVER_HOOK(POST_CALC_FREE_ROUTE);
  if (OB_FAIL(assign_resp_tx_state_(resp, tx_desc, session_.get_txn_free_route_ctx()))) {
    TRANS_LOG(ERROR, "calc_txn_free_route fail", K(ret));
    ob_abort();
  }
  CALL_SERVER_HOOK(PRE_SEND_RESP);
  return ret;
}

int MockObServer::do_handle_(ObReq &req, ObResp &resp)
{
  int ret = OB_SUCCESS;
  // switch req.action:
  // call tx_node's read/write/start_tx/commit_tx/rollback_tx ...
  auto &tx_desc = session_.get_tx_desc();
  if (!tx_desc) {
    ret = tx_node_.acquire_tx(tx_desc, session_.get_sessid());
    if (OB_FAIL(ret)) {
      resp.ret_ = ret;
      TRANS_LOG(WARN, "acquire tx failed", K(ret));
      return ret;
    }
  }
  switch(req.type_) {
  case ObReq::T::START_TX: {
    PREPARE_TX_PARAM(tx_param)
    ret = tx_node_.start_tx(*tx_desc, tx_param);
    if (OB_SUCC(ret)) { resp.tx_id_ = tx_desc->tx_id_; }
  }
    break;
  case ObReq::T::COMMIT_TX:
    ret = tx_node_.commit_tx(*tx_desc, 2 * 1000 * 1000L);
    tx_node_.release_tx(*tx_desc);
    tx_desc = NULL;
    break;
  case ObReq::T::ROLLBACK_TX:
    ret = tx_node_.rollback_tx(*tx_desc);
    tx_node_.release_tx(*tx_desc);
    tx_desc = NULL;
    break;
  case ObReq::T::SAVEPOINT:
    ret = tx_node_.create_explicit_savepoint(*tx_desc, ObString(req.savepoint_name_), 0, false);
    break;
  case ObReq::T::ROLLBACK_SAVEPOINT:
    ret = tx_node_.rollback_to_explicit_savepoint(*tx_desc, ObString(req.savepoint_name_), 1 * 1000 * 1000, 0);
    break;
  case ObReq::T::RELEASE_SAVEPOINT:
    ret = tx_node_.release_explicit_savepoint(*tx_desc, ObString(req.savepoint_name_), 0);
    break;
  case ObReq::T::READ:
    if (req.is_serializable_isolation_) {
      ret = tx_node_.read(*tx_desc, req.read_key_, resp.read_value_, ObTxIsolationLevel::SERIAL);
    } else {
      ret = tx_node_.read(*tx_desc, req.read_key_, resp.read_value_);
    }
    break;
  case ObReq::T::WRITE: {
    PREPARE_TX_PARAM(tx_param);
    ret = tx_node_.atomic_write(*tx_desc, req.write_key_, req.write_value_, 5 * 1000 * 1000L, tx_param);
  }
    break;
  case ObReq::T::DUMMY_WRITE: {
    PREPARE_TX_PARAM(tx_param);
    ObTxSEQ sp;
    ret = tx_node_.create_implicit_savepoint(*tx_desc, tx_param, sp, true);
  }
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected type", K(ret), K(req.type_));
    ob_abort();
  }
  resp.ret_ = ret;
  return ret;
}

int MockObServer::assign_resp_tx_state_(ObResp &resp, ObTxDesc *tx_desc, ObTxnFreeRouteCtx &ctx)
{
  int ret = OB_SUCCESS;
  resp.tx_state_list_.reset();
  // for each changed state
  // encode to tx_state obj
  // push tx_state obj to resp.tx_state_list_
#define ENCODE_TX_STATE_(t)                                             \
  if (OB_SUCC(ret) && ctx.t##_changed_) {                               \
    int64_t len = tx_node_.txn_free_route__get_##t##_state_serialize_size(tx_desc, ctx); \
    char *buf = (char*)ob_malloc(len, ObMemAttr(OB_SERVER_TENANT_ID, ObNewModIds::TEST));                                  \
    int64_t pos = 0;                                                    \
    if (OB_FAIL(tx_node_.txn_free_route__serialize_##t##_state(session_.get_sessid(), tx_desc, ctx, buf, len, pos))) { \
      TRANS_LOG(ERROR, "serialize fail", K(ret), K(tx_desc));           \
    } else {                                                            \
      SyncTxState state;                                                \
      state.data_ = ObString(len, buf);                                 \
      state.type_ = SyncTxState::T::UPPER_##t;                          \
      ret = resp.tx_state_list_.push_back(state);                       \
    }                                                                   \
  }
#define ENCODE_TX_STATE(t)  ENCODE_TX_STATE_(t)
  LST_DO(ENCODE_TX_STATE, (), static, dynamic, parts, extra);
#undef ENCODE_TX_STATE_
#undef ENCODE_TX_STATE
  resp.in_txn_ = OB_NOT_NULL(tx_desc) && tx_desc->in_tx_for_free_route();
  resp.can_free_route_ = ctx.can_free_route();
  return ret;
}
int MockObServer::handle_msg_(int type, void *msg)
{
  switch(type) {
    case TX_MSG_TYPE::TX_FREE_ROUTE_CHECK_ALIVE_RESP:
      return handle_msg_check_alive_resp__((ObTxFreeRouteCheckAliveRespMsg*)msg);
  default:
    ob_abort();
  }
  return OB_SUCCESS;
}

int MockObServer::handle_msg_check_alive_resp__(ObTxFreeRouteCheckAliveRespMsg *msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(msg->ret_)) {
    if (ret == OB_TRANS_CTX_NOT_EXIST) {
      TRANS_LOG(INFO, "[MockObserver] [tx free route] check txn alive : txn has quit", K(ret), KPC(session_.tx_desc_));
      ret = OB_SUCCESS;
      if (session_.tx_desc_) {
        if (session_.tx_desc_->tx_id_ != msg->tx_id_) {
          TRANS_LOG(ERROR, "bug", K(msg->tx_id_));
          ob_abort();
        }
        tx_node_.release_tx(*session_.tx_desc_);
        session_.tx_desc_ = NULL;
      }
    } else {
      TRANS_LOG(INFO, "[MockObserver] [tx free route] check txn alive : fail", K(ret), KPC(session_.tx_desc_));
    }
  } else {
    TRANS_LOG(INFO, "[MockObserver] [tx free route] check txn alive success", KPC(session_.tx_desc_), K_(session));
  }
  return ret;
}
} // transaction

class ObTestTxFreeRoute : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    const uint64_t tv = ObTimeUtility::current_time();
    ObCurTraceId::set(&tv);
    GCONF._ob_trans_rpc_timeout = 500;
    ObClockGenerator::init();
    omt::the_ctrl_of_enable_transaction_free_route = true;
    common::ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_1_0_0);
    const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> starting test : %s", test_name);
  }
  virtual void TearDown() override
  {
    const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> tearDown test : %s", test_name);
    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }
  MsgBus bus_;
};
#define A_OK(k) ASSERT_EQ(OB_SUCCESS, k)
#define A_T(k) ASSERT_TRUE(k)
#define A_F(k) ASSERT_FALSE(k)
#define A_EQ(r, k) ASSERT_EQ(r, k)
#define A_NULL(r) ASSERT_EQ(r, (decltype(r))NULL)
#define A_NOT_NULL(r) ASSERT_NE(r, (decltype(r))NULL)

#define TXFR_CREATE_OBSERVER(addr, idx) \
  MockObServer server##idx(idx ## 000, addr, 8888, bus_);       \
  A_OK(server##idx.start());                                    \
  A_OK(proxy.add_backend(server##idx));
#define TXFR_TEST_SETUP(proxy_addr, server1, ...)               \
  MockObProxy proxy(proxy_addr, 4444, bus_);                    \
  LST_DO2(TXFR_CREATE_OBSERVER, (), server1, ##__VA_ARGS__);
#define FIRST_1(a, ...) a
#define FIRST_0() 0
#define ARG_COUNT_(a, b, c, ...) c
#define ARG_COUNT(...) ARG_COUNT_(a, ##__VA_ARGS__, 1, 0)
#define EX_START_TX(...)                        \
  {                                             \
    ObResp resp;                                \
    auto req = ObReq::mk_start_tx();            \
    if (ARG_COUNT(__VA_ARGS__)) {               \
      req.set_stick_hash(CONCAT(FIRST_, ARG_COUNT(__VA_ARGS__))(__VA_ARGS__));   \
    }                                           \
    A_OK(proxy.handle(req, resp));              \
    A_T(resp.tx_id_.is_valid());                \
  }
#define EX_READ(k, v)                           \
  {                                             \
    ObResp resp;                                \
    auto req = ObReq::mk_read(k);               \
    A_OK(proxy.handle(req, resp));              \
    A_OK(resp.ret_);                            \
    A_EQ(resp.read_value_, v);                  \
  }
#define EX_SERIALIZABLE_READ(k, v)              \
  {                                             \
    ObResp resp;                                \
    auto req = ObReq::mk_serializable_read(k);  \
    A_OK(proxy.handle(req, resp));              \
    A_OK(resp.ret_);                            \
    A_EQ(resp.read_value_, v);                  \
  }
#define EX_WRITE(k, v)                          \
  {                                             \
    ObResp resp;                                \
    auto req = ObReq::mk_write(k, v);           \
    A_OK(proxy.handle(req, resp));              \
    A_OK(resp.ret_);                            \
  }
#define EX_DUMMY_WRITE(k, v)                    \
  {                                             \
    ObResp resp;                                \
    auto req = ObReq::mk_dummy_write(k, v);     \
    A_OK(proxy.handle(req, resp));              \
    A_OK(resp.ret_);                            \
  }

#define EX_SAVEPOINT(k, ...)                    \
  {                                             \
    char savepoint[10];                         \
    sprintf(savepoint, "%d", k);                 \
    ObResp resp;                                \
    auto req = ObReq::mk_savepoint(savepoint);  \
    if (ARG_COUNT(__VA_ARGS__)) {               \
      req.set_stick_hash(CONCAT(FIRST_, ARG_COUNT(__VA_ARGS__))(__VA_ARGS__)); \
    }                                           \
    A_OK(proxy.handle(req, resp));              \
    A_OK(resp.ret_);                            \
  }
#define EX_ROLLBACK_SAVEPOINT(k)                \
  {                                             \
    char savepoint[10];                         \
    sprintf(savepoint, "%d", k);                 \
    ObResp resp;                                \
    auto req = ObReq::mk_rollback_savepoint(savepoint); \
    A_OK(proxy.handle(req, resp));              \
    A_OK(resp.ret_);                            \
  }
#define EX_RELEASE_SAVEPOINT(k)                 \
  {                                             \
    char savepoint[10];                         \
    sprintf(savepoint, "%d", k);                \
    ObResp resp;                                \
    auto req = ObReq::mk_release_savepoint(savepoint);  \
    A_OK(proxy.handle(req, resp));              \
    A_OK(resp.ret_);                            \
  }
#define EX_COMMIT_TX()                          \
  {                                             \
    ObResp resp;                                \
    auto req = ObReq::mk_commit_tx();           \
    A_OK(proxy.handle(req, resp));              \
    A_OK(resp.ret_);                            \
  }
#define EX_ROLLBACK_TX()                        \
  {                                             \
    ObResp resp;                                \
    auto req = ObReq::mk_rollback_tx();         \
    A_OK(proxy.handle(req, resp));              \
    A_OK(resp.ret_);                            \
  }
#define CHECK_ALIVE(server)                                             \
  do {                                                                  \
    auto &session = server.session_;                                    \
    A_OK(server.tx_node_.tx_free_route_check_alive(session.txn_free_route_ctx_, *session.tx_desc_, session.get_sessid())); \
  } while(0)
#define WRAP_BLOCK(x) { x; }
#define EXPECT_PROXY(hk, ...)                                           \
  do {                                                                  \
    auto func = [&](MockObProxy &proxy, ObReq &req, ObResp &resp, MockObProxy::BackendInfo *backend) -> void { \
      LST_DO(WRAP_BLOCK, (), __VA_ARGS__);                              \
    };                                                                  \
    proxy.setup_hook(MockObProxy::HOOK::hk, func);                      \
  } while(0)
#define EXPECT_SERVER(svr, hk, ...)                                     \
  do {                                                                  \
    auto func = [&](MockObServer &server, ObReq &req, ObResp &resp) -> void { \
      auto &txn_free_route_ctx = server.session_.txn_free_route_ctx_;  \
      LST_DO(WRAP_BLOCK, (), __VA_ARGS__);                              \
    };                                                                  \
    svr.setup_hook(MockObServer::HOOK::hk, func);                       \
  } while(0)
#define RESET_HOOK_(svr) svr.reset_hooks()
#define RESET_HOOK(svr, ...)                    \
  LST_DO(RESET_HOOK_, (;), svr, __VA_ARGS__);

#define RESET_HOOKS_2() RESET_HOOK(proxy, server1, server2)

TEST_F(ObTestTxFreeRoute, basic)
{
  TXFR_TEST_SETUP("127.0.0.1", "127.0.0.2", "127.0.0.3");
  TRANS_LOG(INFO, "+++++ explicit start txn");
  for (int k = 0; k< 3; k++) {
    EX_START_TX();
    for(int i = 0 ; i< 10; i++) {
      EX_WRITE(k * 100 + i, k * 110 + i);
      EX_READ(k * 100 + i, k * 110 + i);
    }
    EX_COMMIT_TX();
  }
  TRANS_LOG(INFO, "+++++ implicit start txn");
  for (int k = 3; k< 6; k++) {
    for(int i = 0 ; i< 10; i++) {
      EX_READ((k-1) * 100 + i, (k-1) * 110 + i);
      EX_WRITE(k * 100 + i, k * 110 + i);
      EX_READ(k * 100 + i, k * 110 + i);
    }
    EX_COMMIT_TX();
  }
  TRANS_LOG(INFO, "+++++ implicit start txn with SERIALIZABLE");
  for (int k = 6; k< 9; k++) {
    for(int i = 0 ; i< 10; i++) {
      if (k % 2 == 1) {
        EX_SERIALIZABLE_READ((k-1) * 100 + i, (k-1) * 110 + i);
      } else {
        EX_READ((k-1) * 100 + i, (k-1) * 110 + i);
      }
      EX_WRITE(k * 100 + i, k * 110 + i);
      if ((k + i) % 2 == 0) {
        EX_SERIALIZABLE_READ(k * 100 + i, k * 110 + i);
      } else {
        EX_READ(k * 100 + i, k * 110 + i);
      }
    }
    EX_COMMIT_TX();
  }
  TRANS_LOG(INFO, "+++++ savepoint");
  for (int k = 6; k< 9; k++) {
    for(int i = 0 ; i< 10; i++) {
      EX_SAVEPOINT(k * 100 + i);
      if (i == 5) {
        EX_ROLLBACK_SAVEPOINT(k * 100);
      } else if (i == 7) {
        EX_RELEASE_SAVEPOINT(k * 100 + 6);
      }
    }
    EX_COMMIT_TX();
  }
}
TEST_F(ObTestTxFreeRoute, savepoint)
{
  TXFR_TEST_SETUP("127.0.0.1", "127.0.0.2", "127.0.0.3");
  EX_SAVEPOINT(100);
  EX_WRITE(101, 1);
  EX_COMMIT_TX();
}
TEST_F(ObTestTxFreeRoute, serializalbe_read)
{
}
TEST_F(ObTestTxFreeRoute, implicit_start_tx)
{
}
TEST_F(ObTestTxFreeRoute, participants_update)
{
}

TEST_F(ObTestTxFreeRoute, sync_static_reuse_idle)
{
  TXFR_TEST_SETUP("127.0.0.1", "127.0.0.2", "127.0.0.3");
  // write 100 = 1
  EX_START_TX(2);
  EX_WRITE(100, 1);
  EX_COMMIT_TX();
  // plain read, acquire IDLE tx on server 1
  EX_READ(100, 1);
  A_NOT_NULL(server1.session_.tx_desc_);
  A_F(server1.session_.tx_desc_->tx_id_.is_valid());
  // start tx on server2
  EX_START_TX(2);
  EX_WRITE(101, 2);
  // write on server1, static state will synced to it
  EX_WRITE(102, 3);
  A_EQ(server2.session_.tx_desc_->tx_id_, server1.session_.tx_desc_->tx_id_);
  EX_COMMIT_TX();
}

namespace transaction {
#ifdef NDEBUG
  int64_t MAX_STATE_SIZE = 4096L;
#else
  extern int64_t MAX_STATE_SIZE;
#endif
}

TEST_F(ObTestTxFreeRoute, txn_too_large_fallback)
{
  TXFR_TEST_SETUP("127.0.0.1", "127.0.0.2", "127.0.0.3");
  EX_START_TX(1);
  oceanbase::transaction::MAX_STATE_SIZE = 1L;
  EX_WRITE(100, 1);
  EX_WRITE(101, 2);
  EX_WRITE(102, 3);
  EX_COMMIT_TX();
  oceanbase::transaction::MAX_STATE_SIZE = 4096L;
}

TEST_F(ObTestTxFreeRoute, keep_alive)
{
  TXFR_TEST_SETUP("127.0.0.1", "127.0.0.2", "127.0.0.3");
  EX_START_TX(1);   // txn on server 2
  EX_WRITE(100, 1); // write 100 on server 1
  EX_WRITE(101, 2); // write 101 on server 2
  EX_COMMIT_TX();   // commit on server 2
  // NOW txn has released on server 2, send check from server 1
  A_NOT_NULL(server1.session_.tx_desc_);
  CHECK_ALIVE(server1);
  while(true) {
    if (OB_ISNULL(server1.session_.tx_desc_)) {
      break;
    }
    usleep(1000);
  }
  A_NULL(server1.session_.tx_desc_);
}

TEST_F(ObTestTxFreeRoute, upgrade_to_4_1)
{
  TXFR_TEST_SETUP("127.0.0.1", "127.0.0.2", "127.0.0.3");
  common::ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_0_0_0);
  EX_START_TX(1);
  A_T(proxy.in_txn_);
  A_F(proxy.can_free_route_);
  EX_WRITE(100,1);
  EX_WRITE(101,2);
  EX_COMMIT_TX();
}

// TEST_F(ObTestTxFreeRoute, twiddle_knob_on_the_fly)
// {
//   TXFR_TEST_SETUP("127.0.0.1", "127.0.0.2", "127.0.0.3");
//   // previous is on
//   EX_START_TX(1);
//   EX_WRITE(102,2);
//   EX_WRITE(103,2);
//   EX_COMMIT_TX();
//   omt::the_ctrl_of_enable_transaction_free_route = false;
//   // off -> on
//   EX_START_TX(1);
//   A_T(proxy.in_txn_);
//   A_F(proxy.can_free_route_);
//   EX_WRITE(100, 1); // on server 2
//   omt::the_ctrl_of_enable_transaction_free_route = true;
//   A_T(proxy.in_txn_);
//   A_F(proxy.can_free_route_);
//   EX_WRITE(101, 1); // on server 2
//   EX_COMMIT_TX();
//   // on -> off
//   EX_START_TX(1);
//   A_T(proxy.in_txn_);
//   A_T(proxy.can_free_route_);
//   EX_WRITE(100, 1); // on server 1
//   omt::the_ctrl_of_enable_transaction_free_route = false;
//   A_T(proxy.in_txn_);
//   A_T(proxy.can_free_route_);
//   EX_WRITE(101, 1); // on server 2
//   EX_COMMIT_TX();
// }


TEST_F(ObTestTxFreeRoute, sample)
{
  TXFR_TEST_SETUP("127.0.0.1", "127.0.0.2", "127.0.0.3");
  EXPECT_PROXY(POST_ROUTE,
               A_EQ(backend->server_, &server2),
               A_EQ(backend->tx_state_version_[0], 0));
  EXPECT_SERVER(server2, RECV_REQ,
                A_T(req.txn_free_route_support_));
  EXPECT_SERVER(server2, PRE_HANDLE,
                A_F(txn_free_route_ctx.in_txn_before_handle_request_),
                A_F(txn_free_route_ctx.can_free_route_),
                A_F(txn_free_route_ctx.static_changed_),
                A_F(txn_free_route_ctx.dynamic_changed_),
                A_F(txn_free_route_ctx.parts_changed_),
                A_F(txn_free_route_ctx.extra_changed_));
  EXPECT_SERVER(server2, POST_CALC_FREE_ROUTE,
                A_F(txn_free_route_ctx.in_txn_before_handle_request_),
                A_T(txn_free_route_ctx.can_free_route_),
                A_T(txn_free_route_ctx.static_changed_),
                A_T(txn_free_route_ctx.dynamic_changed_),
                A_T(txn_free_route_ctx.parts_changed_),
                A_T(txn_free_route_ctx.extra_changed_),
                A_F(txn_free_route_ctx.flag_.is_tx_terminated()),
                A_F(txn_free_route_ctx.flag_.is_fallback()));
  EX_START_TX(1);
  RESET_HOOKS_2();
  EXPECT_PROXY(POST_ROUTE, A_EQ(backend->server_, &server2));
  EXPECT_SERVER(server2, RECV_REQ,
                A_T(req.tx_state_list_.empty()));
  EXPECT_SERVER(server2, PRE_HANDLE,
               A_T(txn_free_route_ctx.in_txn_before_handle_request_),
               A_NOT_NULL(server.session_.tx_desc_));
  EX_WRITE(101, 1);
  RESET_HOOKS_2();
  EXPECT_PROXY(POST_ROUTE, A_EQ(backend->server_, &server1));
  EXPECT_SERVER(server1, RECV_REQ,
                A_EQ(req.tx_state_list_.count(), 4));
  EXPECT_SERVER(server1, PRE_HANDLE,
                A_T(txn_free_route_ctx.in_txn_before_handle_request_),
                A_NOT_NULL(server.session_.tx_desc_));
  EX_WRITE(102, 1);
  RESET_HOOKS_2();
  auto prev_tx_id = server1.session_.tx_desc_->tx_id_;
  EX_COMMIT_TX();
  EXPECT_PROXY(POST_ROUTE, A_EQ(backend->server_, &server1));
  EXPECT_SERVER(server1, RECV_REQ,
                A_EQ(req.tx_state_list_.count(), 4),
                // previouse txn's desc is exist
                A_NOT_NULL(server.session_.tx_desc_),
                A_EQ(server.session_.tx_desc_->tx_id_, prev_tx_id));
  EXPECT_SERVER(server1, PRE_HANDLE,
                A_F(txn_free_route_ctx.in_txn_before_handle_request_),
                // txn should be released by synced terminated txn state
                A_NULL(server.session_.tx_desc_));
  EXPECT_SERVER(server1, POST_CALC_FREE_ROUTE,
                A_F(txn_free_route_ctx.in_txn_before_handle_request_),
                A_NOT_NULL(server.session_.tx_desc_));
  EX_START_TX(2);
  RESET_HOOKS_2();
  EX_COMMIT_TX();

  // replace extra: session has idle tx (with txid)
  // eg:
  // step 1: NODE1: DELETE FROM T WHERE 1 = 0; ==> create savepoint (will alloc txid)
  //   (because it has no modifies, txn state will in IDLE afte stmt executed)
  // step 2: NODE2: SAVEPOINT s0               ==> will return EXTRA INFO, in trans is true (to proxy)
  // step 3: NODE1: INSERT INTO T              ==> will sync EXTRA INFO down, will replace session's tx on step 1

  // step1
  RESET_HOOKS_2();
  EXPECT_PROXY(POST_ROUTE, A_EQ(backend->server_, &server2));
  EXPECT_SERVER(server2, POST_CALC_FREE_ROUTE,
                A_F(txn_free_route_ctx.in_txn_before_handle_request_),
                A_F(txn_free_route_ctx.can_free_route_),
                A_F(txn_free_route_ctx.static_changed_),
                A_F(txn_free_route_ctx.dynamic_changed_),
                A_F(txn_free_route_ctx.parts_changed_),
                A_F(txn_free_route_ctx.extra_changed_),
                A_F(txn_free_route_ctx.flag_.is_tx_terminated()));
  EX_DUMMY_WRITE(201,100);
  // step2
  RESET_HOOKS_2();
  EXPECT_PROXY(POST_ROUTE, A_EQ(backend->server_, &server1));
  EXPECT_SERVER(server1, POST_CALC_FREE_ROUTE,
                A_F(txn_free_route_ctx.in_txn_before_handle_request_),
                A_T(txn_free_route_ctx.can_free_route_),
                A_F(txn_free_route_ctx.static_changed_),
                A_F(txn_free_route_ctx.dynamic_changed_),
                A_F(txn_free_route_ctx.parts_changed_),
                A_T(txn_free_route_ctx.extra_changed_),
                A_F(txn_free_route_ctx.flag_.is_tx_terminated()));
  EX_SAVEPOINT(202, 102);
  // step3
  RESET_HOOKS_2();
  EXPECT_PROXY(POST_ROUTE, A_EQ(backend->server_, &server2));
  EXPECT_SERVER(server2, RECV_REQ,
                A_T(server.session_.tx_desc_->tx_id_.is_valid()),
                A_OK(server.check_tx_sanity()),
                req.hook_ctx_ = (void*)new ObTransID(server.session_.tx_desc_->tx_id_));
  EXPECT_SERVER(server2, PRE_HANDLE,
                A_T(txn_free_route_ctx.in_txn_before_handle_request_),
                A_EQ(OB_ENTRY_NOT_EXIST, server.check_tx_exist((ObTransID*)req.hook_ctx_)),
                A_EQ(OB_SUCCESS, server.check_tx_sanity()));
  EX_WRITE(201, 1001);
  RESET_HOOKS_2();
  EX_COMMIT_TX();
}
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_tx_free_route.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_tx_free_route.log", true, false,
                       "test_tx_free_route.log", // rs
                       "test_tx_free_route.log", // election
                       "test_tx_free_route.log"); // audit
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
