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
#ifndef OCEANBASE_TRANSACTION_OB_TRANS_FREE_ROUTE_
#define OCEANBASE_TRANSACTION_OB_TRANS_FREE_ROUTE_
namespace oceanbase {
namespace transaction {
class ObTxDesc;
union ObTxnFreeRouteFlag {
  int8_t v_;
  static const int TX_TERMINATED_OFFSET = 0;
  static const int FALLBACK_OFFSET = 1;
  static const int IDLE_RELEASED_OFFSET = 2;
  static const int STATE_MASK = ~(1 << 7);
  static const int WITH_VERSION_OFFSET = 7;
  // it is terminated (committed or rollbacked)
  bool is_tx_terminated() const { return (v_ & (1 << TX_TERMINATED_OFFSET)) != 0; }
  void set_tx_terminated() { v_ |= (1 << TX_TERMINATED_OFFSET); }
  // it is fallbacked to fixed route
  bool is_fallback() const { return (v_ & (1 << FALLBACK_OFFSET)) !=0; }
  void set_fallback() { v_ |= (1 << FALLBACK_OFFSET); }
    // it is released during session idle, by doing check alive
  bool is_idle_released() const { return (v_ & (1 << IDLE_RELEASED_OFFSET)) !=0; }
  void set_idle_released() { v_ |= (1 << IDLE_RELEASED_OFFSET); }
    // identify new Pkt format : Header part has version
  bool is_with_version() const { return (v_ & (1 << WITH_VERSION_OFFSET)) !=0; }
  void set_with_version(bool b) { if (b) { v_ |= (1 << WITH_VERSION_OFFSET); } else { v_ &= ~(1 << WITH_VERSION_OFFSET); } }
  bool is_return_normal_state() const { return  (v_ & STATE_MASK) == 0; }
  void reset() { v_ = 0; }
  TO_STRING_KV(K_(v));
};

union ObTxnFreeRouteAuditRecord
{
  void reset() { v_ = 0; }
  uint64_t v_;
  struct {
    bool proxy_flag_: 1; // 1
    bool svr_flag_: 1;   // 2
    bool calculated_: 1; // 3
    bool tx_start_: 1;   // 4
    bool tx_term_: 1;    // 5
    bool free_route_: 1; // 6
    bool fallback_: 1;   // 7
    bool tx_switch_: 1;  // 8
    bool upd_static_: 1; // 9
    bool upd_parts_: 1;  // 10
    bool upd_dyn_: 1;    // 11
    bool upd_extra_: 1;  // 12
    bool upd_term_: 1;   // 13
    bool upd_fallback_: 1; // 14
    bool upd_clean_tx_: 1; // 15
    bool upd_reset_snapshot_: 1; // 16
    bool chg_static_: 1; // 17
    bool chg_dyn_: 1;    // 18
    bool chg_parts_: 1;  // 19
    bool chg_extra_: 1;  // 20
    bool start_node_: 1; // 21
    bool push_state_: 1; // 22
    bool ret_fallback_: 1; // 23
    bool ret_term_: 1;   // 24
    bool xa_: 1;        // 25
    bool xa_tightly_couple_: 1; // 26
    bool assoc_xa_orig_ :1; // 27
    bool alloc_tx_ :1; // 28
    bool reuse_tx_ :1; // 29
    bool replace_tx_ :1; // 30
  };
};

enum TxnFreeRouteState {
  STATIC = 0, DYNAMIC = 1, PARTICIPANT = 2, EXTRA = 3, _CNT_VAL = 4
};

struct ObTxnFreeRouteCtx {
  friend class ObTransService;
  ObTxnFreeRouteCtx() { reset(); }
  ~ObTxnFreeRouteCtx() { reset(); }
  void reset() {
    start_session_id_ = 0;
    local_version_ = 1;
    global_version_ = 0;
    global_version_water_mark_ = 0;
    is_txn_switch_ = false;
    txn_addr_.reset();
    tx_id_.reset();
    prev_tx_id_.reset();
    is_proxy_support_ = false;
    in_txn_before_handle_request_ = false;
    can_free_route_ = false;
    is_fallbacked_ = false;
    MEMSET(state_sync_infos_, 0, sizeof(state_sync_infos_));
    reset_changed_();
    audit_record_.reset();
  }
  void set_sessid(const uint32_t sessid) { session_id_ = sessid; }
  void set_start_sessid(const uint32_t sessid) { start_session_id_ = sessid; }
  void init_before_update_state(bool proxy_support);
  void init_before_handle_request(ObTxDesc *txdesc);
  bool is_temp(const ObTxDesc &tx) const;
  bool can_free_route() const { return can_free_route_ && !is_fallbacked_; }
  bool is_changed() const { return _changed_ != 0; }
  bool is_static_changed() const { return static_changed_; }
  bool is_dynamic_changed() const { return dynamic_changed_; }
  bool is_parts_changed() const { return parts_changed_; }
  bool is_extra_changed() const { return extra_changed_; }
  void set_idle_released() { flag_.set_idle_released(); }
  bool is_idle_released() const { return flag_.is_idle_released(); }
  bool has_calculated() const { return calculated_; }
  void set_calculated() { calculated_ = true; }
  int64_t get_local_version() const { return local_version_; }
  int64_t get_global_version() const { return global_version_; }
  void inc_update_global_version(const int64_t v) { if (global_version_ < v) { global_version_ = v; } }
  void inc_global_version() { ++global_version_; }
  void reset_audit_record() { audit_record_.reset(); }
  const ObTransID &get_prev_tx_id() const { return prev_tx_id_; }
  const ObTransID &get_tx_id() const { return tx_id_; }
  const ObTxnFreeRouteFlag &get_flag() const { return flag_; }
  uint32_t get_session_id() const { return session_id_; }
  uint32_t get_start_session_id() const { return start_session_id_; }
  uint64_t get_audit_record() const { return audit_record_.v_; }
  int state_update_verify_by_version(const TxnFreeRouteState state,
                                     const int64_t version,
                                     const uint32_t backend_sess_id,
                                     bool &dup) const;
  void update_last_synced_state(const TxnFreeRouteState state, uint32_t backend_sess_id, const int64_t version)
  {
    state_sync_infos_[state].last_backend_sess_id_ = backend_sess_id;
    state_sync_infos_[state].last_version_ = version;
    inc_update_global_version(version);
    if (TxnFreeRouteState::STATIC == state) {
      is_txn_switch_ = true;
      global_version_water_mark_ = version;
    }
  }
private:
  void reset_changed_() {
    _changed_ = false;
    flag_.reset();
    calculated_ = false;
  }
  // the session this ctx belongs to
  uint32_t session_id_;
  // for normal trx: transaction original session
  // for xa trx: xa-start session
  uint32_t start_session_id_;
  // the local_version updated when session handle a request
  // from proxy which caused txn state synced
  // it is used as request id for checkAlive request
  // do prevent stale checkAlive release txn state
  // updated by state sync of later request
  int64_t local_version_;
  // the global_version was update by each backend session
  // when they update txn state and propagated in txn state
  // sync via OBProxy
  int64_t global_version_;
  // the txn left boundary version, it's updated when txn started
  int64_t global_version_water_mark_;
  // remember txn is switched by sync 'static' state
  bool is_txn_switch_;
  // address of where txn started
  // updated when receive request
  // if no txn alive, set to 0.0.0.0
  common::ObAddr txn_addr_;
  // current tx_id which setup before handle request
  // and updated after handle request
  // if `switch txn` happended, prev_tx_id_ is used to
  // save the before one
  ObTransID tx_id_;
  // the tx_id of previouse txn which implicit committed
  // by current request/stmt/command and create a new txn
  // also names with `switch txn` (prev_tx_id_ -> tx_id_)
  ObTransID prev_tx_id_;
  // proxy's hint of support future free route
  // used to fallback on txn start node
  // used to decide free route when txn start
  // updated when receive request
  bool is_proxy_support_;
  // updated when receive request
  // remember whether txn active before handle new request
  bool in_txn_before_handle_request_;
  // on txn start node:
  //   setup post handle request
  //   remember the decision
  // on other txn node:
  //   setup pre handle request
  //   if txn is valid and its address is remote
  bool can_free_route_;
  // on txn start node:
  //   remember the fallback decision
  // on other txn node:
  //   reset pre handle request
  //   setup post handle request, remember fallback decision
  bool is_fallbacked_;
  // record each state's synced info, used to reject stale and duplicate sync
  struct StateSyncInfo {
    StateSyncInfo(): last_backend_sess_id_(0), last_version_(0) {}
    uint32_t last_backend_sess_id_;
    int64_t last_version_;
    TO_STRING_KV(K_(last_backend_sess_id), K_(last_version));
  } state_sync_infos_[TxnFreeRouteState::_CNT_VAL];
  // following are changed after request process
  // used to mark state changed and special state
  // need to return to proxy
  // these fields will be set each request
  // NOTE:
  // code should not depends on these before request process
  union {
    uint8_t _changed_;
    struct {
      bool static_changed_ : 1;
      bool dynamic_changed_ : 1;
      bool parts_changed_ : 1;
      bool extra_changed_ : 1;
    };
  };
  // used do de-duplicate calculation
  bool calculated_ :1;
  // set after handle request
  // reset before handle request
  ObTxnFreeRouteFlag flag_;
  ObTxnFreeRouteAuditRecord audit_record_;
private:
  template<typename T, int N>
  struct _ForRawArrayDisplay {
    _ForRawArrayDisplay(const T (&a)[N]): a_(a) {}
    const T (&a_)[N];
    DEFINE_TO_STRING({
        J_ARRAY_START();
        for(int i = 0; i < N; i++) {  BUF_PRINTO(a_[i]);  J_COMMA(); }
        J_ARRAY_END();
    });
  };
  template<typename T, int N>
  const _ForRawArrayDisplay<T, N> for_display_(const T (&a)[N]) const { return _ForRawArrayDisplay<T, N>(a); }
public:
  TO_STRING_KV(K_(tx_id),
               K_(txn_addr),
               K_(is_proxy_support),
               K_(in_txn_before_handle_request),
               K_(can_free_route),
               K_(is_fallbacked),
               K_(static_changed),
               K_(dynamic_changed),
               K_(parts_changed),
               K_(extra_changed),
               K_(calculated),
               K_(flag),
               K_(local_version),
               K_(global_version),
               K_(global_version_water_mark),
               "state_sync_infos", for_display_(state_sync_infos_),
               "audit_record", audit_record_.v_);
};
}
}
#endif // OCEANBASE_TRANSACTION_OB_TRANS_FREE_ROUTE_
