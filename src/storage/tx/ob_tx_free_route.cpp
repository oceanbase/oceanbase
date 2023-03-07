#include "ob_trans_service.h"
#include "lib/utility/serialization.h"

/*
 * The exception handle of txn state update with synchronization via proxy
 *
 * the exception on handle update txn state from proxy
 * may fall into two categroies:
 * 1. un-retryable : state itself is insane, sanity check logic found a bug
 * 2. retryable    : system memory alloc fail
 *
 * how to handle these exception ?
 *
 * after we receive above exceptions, before return error packet to proxy
 * it is required to classify error's into categories, and on proxy side:
 * 1. for retryable error, choose other observer as backend to retry
 * 2. for unretryable error, disconnect with current observer and retry
 *    on other backend
 *    * what if it occurred on txn started server ?
 *      just disconnect with client
 *
 * the simple way to handle it correctly is to disconnect client connection
 * and all backend connection for both of these two categories exception
 */

namespace oceanbase{
namespace transaction{
using namespace common::serialization;

#ifdef NDEBUG
#define MAX_STATE_SIZE  (4 * 1024) // 4KB
#else
int64_t MAX_STATE_SIZE = 4 * 1024; // 4KB
#endif
#define TX_START_OR_RESUME_ADDR(tx) ((tx)->is_xa_trans() ? (tx)->xa_start_addr_ : (tx)->addr_)
#define TX_START_OR_RESUME_LOCAL(tx) (TX_START_OR_RESUME_ADDR(tx) == GCONF.self_addr_)

bool ObTxnFreeRouteCtx::is_temp(const ObTxDesc &tx) const
{
  //return !TX_START_OR_RESUME_LOCAL(&tx);
  UNUSED(tx);
  return txn_addr_.is_valid() && txn_addr_ != GCONF.self_addr_;
}
void ObTxnFreeRouteCtx::init_before_update_state(bool proxy_support)
{
  is_proxy_support_ = proxy_support;
  global_version_water_mark_ = global_version_;
  is_txn_switch_ = false;
}

void ObTxnFreeRouteCtx::init_before_handle_request(ObTxDesc *tx)
{
  in_txn_before_handle_request_ = false;
  audit_record_.proxy_flag_ = is_proxy_support_;
  if (OB_NOT_NULL(tx)) {
    ObSpinLockGuard guard(tx->lock_);
    in_txn_before_handle_request_ = tx->in_tx_for_free_route_();
    txn_addr_ = TX_START_OR_RESUME_ADDR(tx);
    tx_id_ = tx->tx_id_;
    tx->state_change_flags_.reset();
    if (txn_addr_ != GCONF.self_addr_) {
      can_free_route_ = true;
      is_fallbacked_ = false;
    } else if (can_free_route_ && !is_fallbacked_ && !is_proxy_support_) {
      is_fallbacked_ = true;
    }
  } else {
    txn_addr_.reset();
    tx_id_.reset();
  }
  reset_changed_();
  ++local_version_;
#ifndef NDEBUG
  TRANS_LOG(INFO, "[tx free route] after sync state and before handle request", KPC(tx), KPC(this));
#endif
}

int ObTransService::clean_txn_state_(ObTxDesc *&tx, const ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  // sanity : the tx not started on this session
  // because the tx should terminated on txn-start-node
  // and other temporary-txn-node was expected to driven txn-state clean by txn-state sync/update
  //
  // if such insanity happened, it may the proxy was corrupted, we should disconnect
  // with proxy, because such error can not been repaired via retry
  bool release_ref = false, release = false;
  {
    ObSpinLockGuard guard(tx->lock_);
    if (TX_START_OR_RESUME_LOCAL(tx)) {
      if (tx->tx_id_ == tx_id) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "try to clean txn state on txn start node", K(ret), KPC(tx));
      } else if (tx->is_in_tx()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "try to clean txn state while tx is active", K(ret), KPC(tx), K(tx_id));
      }
    }
    if (OB_SUCC(ret)) {
      if (tx->is_xa_trans() && tx->addr_ == self_) {
      // on XA orignal, release ref
        release_ref = true;
      } else {
        release = true;
      }
    }
  }
  if (release_ref) { release_tx_ref(*tx); tx = NULL; }
  if (release) { ret = release_tx(*tx); tx = NULL; }
#ifndef NDEBUG
  TRANS_LOG(INFO, "[tx free route] clean-txn-state", K(ret));
#endif
  return ret;
}

int ObTransService::txn_free_route__sanity_check_fallback_(ObTxDesc *tx, ObTxnFreeRouteCtx &ctx)
{
  // the txn state with partial flag should only received on txn-start-node
  // this happended when temporary-txn-node fallback and follow request will with
  // this type of txn state send to the txn-start-node
  //
  // if a temporary-txn-node receive such txn-state, reject the request to proxy
  // and proxy should re-route it to txn-start-node
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tx)) {
    ObSpinLockGuard guard(tx->lock_);
    if (tx->addr_ != self_ && tx->xa_start_addr_ != self_) {
      ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
      TRANS_LOG(ERROR, "tmp node receive fallback notify packet", K(ret), KPC(tx));
    } else if (!ctx.is_fallbacked_) {
      // if we receive an fallback flag from temporary-txn-node, set the ctx's fallbacked
      // indicate current txn has been fallbacked (by temporary-txn-node)
      ctx.is_fallbacked_ = true;
    }
  }
  return ret;
}

inline int ObTransService::txn_state_update_verify_by_version_(const ObTxnFreeRouteCtx &ctx, const int64_t version)
{
  int ret = OB_SUCCESS;
  // if ctx is switch to new txn in this request
  // water_mark was established by static state
  // the following state (dyn, parts, extra) should be >= water_mark
  if (ctx.is_txn_switch_) {
    if (ctx.global_version_water_mark_ > version) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "the state is stale", K(ret), K(version));
    }
  // otherwise, the new state's version should be > water_mark
  } else if (ctx.global_version_water_mark_ == version) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "duplicated state sync", K(ret), K(version));
  } else if (ctx.global_version_water_mark_ > version) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "the state is stale", K(ret), K(version));
  }
  return ret;
}

#define DECODE_HEADER()                                                 \
  ObTxnFreeRouteFlag flag;                                              \
  ObTransID tx_id;                                                      \
  int64_t global_version;                                               \
  {                                                                     \
    int64_t tmp_tx_id = 0;                                              \
    if (OB_FAIL(OB_E(EventTable::EN_TX_FREE_ROUTE_UPDATE_STATE_ERROR, session_id) OB_SUCCESS)) { \
      TRANS_LOG(ERROR, "inject failure", K(ret), KPC(tx), K(session_id)); \
    } else if (OB_FAIL(decode_i64(buf, len, pos, &tmp_tx_id))) {        \
      TRANS_LOG(ERROR, "decode tx_id fail", K(ret));                    \
    } else if (FALSE_IT(tx_id = ObTransID(tmp_tx_id))) {                \
    } else if (OB_FAIL(decode_i64(buf, len, pos, &global_version))) {   \
      TRANS_LOG(ERROR, "decode global_version fail", K(ret));           \
    } else if (OB_FAIL(decode_i8(buf, len, pos, &flag.v_))) {           \
      TRANS_LOG(ERROR, "decode flag fail", K(ret));                     \
    } else if (OB_FAIL(txn_state_update_verify_by_version_(ctx, global_version))) { \
    } else if (!tx_id.is_valid()) {                                     \
      ret = OB_ERR_UNEXPECTED;                                          \
      TRANS_LOG(ERROR, "tx id is invalid", K(ret));                     \
    } else if (ctx.global_version_ < global_version) {                  \
      ctx.global_version_ = global_version;                             \
    }                                                                   \
  }


#define ENCODE_HEADER()                                                 \
  if (OB_FAIL(OB_E(EventTable::EN_TX_FREE_ROUTE_ENCODE_STATE_ERROR, session_id) OB_SUCCESS)) { \
    TRANS_LOG(ERROR, "inject failure", K(ret), KPC(tx), K(session_id)); \
  } else if (!ctx.tx_id_.is_valid()) {                                  \
    ret = OB_ERR_UNEXPECTED;                                            \
    TRANS_LOG(ERROR, "tx_id is invalid", K(ret), K(ctx));               \
  } else if (OB_FAIL(encode_i64(buf, len, pos, ctx.tx_id_.get_id()))) { \
    TRANS_LOG(WARN, "encode tx_id fail", K(ret));                       \
  } else if (OB_FAIL(encode_i64(buf, len, pos, ctx.global_version_))) { \
    TRANS_LOG(WARN, "encode global_version fail", K(ret));              \
  } else if (OB_FAIL(encode_i8(buf, len, pos, ctx.flag_.v_))) {         \
    TRANS_LOG(WARN, "encode flag fail", K(ret));                        \
  }

#define ENCODE_HEADER_LENGTH()                                          \
  int64_t l = encoded_length_i64(ctx.tx_id_.get_id())                   \
    + encoded_length_i64(ctx.global_version_)                           \
    + encoded_length_i8(ctx.flag_.v_)

int ObTransService::txn_free_route__kill_session_(const uint32_t session_id)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = NULL;
  sql::ObSessionGetterGuard guard(*GCTX.session_mgr_, session_id);
  if (OB_FAIL(guard.get_session(session))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "get session fail", K(ret), K(session_id));
    }
  } else if (OB_FAIL(GCTX.session_mgr_->kill_session(*session))) {
    TRANS_LOG(WARN, "kill session failed", K(ret), K(session_id));
  }
  return ret;
}

int ObTransService::txn_free_route__handle_tx_exist_(const ObTransID &tx_id, ObTxnFreeRouteAuditRecord &audit_record, ObTxDesc *&tx)
{
  int ret = OB_SUCCESS;
  ObTxDesc *tmp_tx = NULL;
  tx = NULL;
  if (OB_FAIL(tx_desc_mgr_.get(tx_id, tmp_tx))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "get tx fail", K(ret), K(tx_id));
    } else { ret = OB_SUCCESS; }
  } else if (OB_ISNULL(tmp_tx)) {
  } else if (!tmp_tx->is_xa_trans()) {
    // some session hold this txn already, close the session and release this txn
    // then continue with retry
    auto assoc_sess_id = tmp_tx->assoc_sess_id_;
    TRANS_LOG(WARN, "tx found associate with other session, will kill the session",
              K(session_id), K(assoc_sess_id), K(tx_id));
    if (OB_FAIL(txn_free_route__kill_session_(assoc_sess_id))) {
      TRANS_LOG(WARN, "kill old session failed", K(ret), K(assoc_sess_id));
    } else if (OB_FAIL(release_tx(*tmp_tx))) {
      TRANS_LOG(WARN, "release tx failed", K(ret), K(assoc_sess_id), K(tx_id));
    } else {
      int tmp_ret = tx_desc_mgr_.get(tx_id, tmp_tx);
      if (OB_ENTRY_NOT_EXIST != tmp_ret) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "release tx while tx is exist", K(ret), K(tx_id), K(tmp_ret));
      }
      if (OB_NOT_NULL(tmp_tx)) {
        tx_desc_mgr_.revert(*tmp_tx);
      }
    }
  } else if (tmp_tx->addr_ != self_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "XA-tx found but not on the orignal", K(ret), K_(self), K(tx_id), K_(tmp_tx->addr));
    tx_desc_mgr_.revert(*tmp_tx);
  } else {
    tx = tmp_tx;
    audit_record.assoc_xa_orig_ = true;
    TRANS_LOG(INFO, "found XA-tx on its original, ref acquried", K(tx_id));
  }
  return ret;
}

int ObTransService::txn_free_route__update_static_state(const uint32_t session_id,
                                                        ObTxDesc *&tx,
                                                        ObTxnFreeRouteCtx &ctx,
                                                        const char* buf,
                                                        const int64_t len,
                                                        int64_t &pos)
{
  int ret = OB_SUCCESS;
  bool need_add_tx = false;
  auto &audit_record = ctx.audit_record_;
  audit_record.upd_static_ = true;
  auto before_tx_id = OB_NOT_NULL(tx) ? tx->tx_id_ : ObTransID();
  DECODE_HEADER();
  if (OB_SUCC(ret)) {
    ctx.is_txn_switch_ = true;
    ctx.global_version_water_mark_ = global_version;
  }
  if (OB_FAIL(ret)) {
  } else if (flag.is_tx_terminated_) {
    audit_record.upd_term_ = true;
    audit_record.upd_clean_tx_ = OB_NOT_NULL(tx);
    if (OB_NOT_NULL(tx) && OB_FAIL(clean_txn_state_(tx, tx_id))) {
      TRANS_LOG(WARN, "cleanup prev txn state fail", K(ret), K(tx_id), K(tx));
    }
  } else if (flag.is_fallback_) {
    audit_record.upd_fallback_ = true;
    ret = txn_free_route__sanity_check_fallback_(tx, ctx);
  } else {
    if (OB_ISNULL(tx)) {
      if (OB_FAIL(txn_free_route__handle_tx_exist_(tx_id, audit_record, tx))) {
        TRANS_LOG(WARN, "handle tx exist fail", K(ret), K(tx_id));
      } else if (OB_ISNULL(tx)) {
        audit_record.alloc_tx_ = true;
        if (OB_FAIL(acquire_tx(tx, session_id))) {
          // if acquire tx failed, it may retryable: alloc-memory failed
          TRANS_LOG(WARN, "acquire tx for decode failed", K(ret));
        } else { need_add_tx = true; }
      }
    } else if (!tx->tx_id_.is_valid()) {
      // reuse, overwrite
      need_add_tx = true;
      audit_record.reuse_tx_ = true;
    } else if (tx->tx_id_ != tx_id) {
      // replace
      audit_record.replace_tx_ = true;
      tx_desc_mgr_.remove(*tx);
      need_add_tx = true;
    } else {
      // update
      // NOTE: for XA join/resume will cause `static state` re-synced
      if (tx->state_ != ObTxDesc::State::IDLE && !tx->is_xa_trans()) {
        ret = OB_ERR_UNEXPECTED;
        ObSpinLockGuard guard(tx->lock_);
        TRANS_LOG(ERROR, "txn static update must with IDLE state", K(ret), K(session_id), KPC(tx));
      }
    }
    if (OB_SUCC(ret)) {
      auto start_ts = ObTimeUtility::current_time();
      ObSpinLockGuard guard(tx->lock_);
      if (OB_FAIL(tx->decode_static_state(buf, len, pos))) {
        // unretryable
        TRANS_LOG(WARN, "decode static state failed", K(ret));
      } else if (tx->addr_ == self_ && tx->sess_id_ != session_id && !tx->is_xa_trans()) {
        // receive static state which was born in this node, and its session is not current session
        // this can only happened for XA which xa_start on remote
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "receive tx static-state born in this node of another session", K(ret),
                  K(tx->sess_id_), K(session_id), KPC(tx));
      } else if (need_add_tx && !tx->is_xa_trans()
                 && OB_FAIL(tx_desc_mgr_.add_with_txid(tx->tx_id_, *tx))) {
        // unretryable
        TRANS_LOG(WARN, "add tx to mgr failed", K(ret), KPC(tx));
      } else if (FALSE_IT(tx->flags_.REPLICA_ = tx->addr_ != self_)) {
        // mark as REPLICA_ for all temporary node
      } else if (FALSE_IT(tx->flags_.SHADOW_ = tx->is_xa_trans() && tx->addr_ != self_)) {
        // mark as SHADOW_ for XA's temporary node, exclude XA orig node
      }
      auto elapsed_us = ObTimeUtility::current_time() - start_ts;
      ObTransTraceLog &tlog = tx->get_tlog();
      REC_TRANS_TRACE_EXT(&tlog, tx_free_route_update_static, OB_Y(ret),
                          OB_ID(txid), tx_id.get_id(),
                          OB_ID(from), before_tx_id.get_id(),
                          OB_ID(time_used), elapsed_us,
                          OB_ID(length), len,
                          OB_ID(tag1), need_add_tx,
                          OB_ID(ref), tx->get_ref(),
                          OB_ID(thread_id), GETTID());
    }
  }
#ifndef NDEBUG
  TRANS_LOG(INFO, "update-static", K(tx_id), K(flag));
#endif
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[tx-free-route::update_state]", K(ret), K(flag), K(before_tx_id), K(tx_id),
              K(session_id), K(ctx), KP(tx));
  }
  return ret;
}

int ObTransService::update_logic_clock_(const int64_t logic_clock)
{
  // if logic clock drift too much, disconnect required
  int ret = OB_SUCCESS;
  int64_t one_day_us = 24L * 3600 * 1000 * 1000;
  if (logic_clock - ObClockGenerator::getClock() > one_day_us ||
      ObClockGenerator::getClock() - logic_clock > one_day_us) {
    // bug, invalid logic_clock value
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(ObSequence::update_max_seq_no(logic_clock))) {
  }
  return ret;
}

int ObTransService::txn_free_route__update_dynamic_state(const uint32_t session_id,
                                                         ObTxDesc *&tx,
                                                         ObTxnFreeRouteCtx &ctx,
                                                         const char* buf,
                                                         const int64_t len,
                                                         int64_t &pos)
{
  int ret = OB_SUCCESS;
  auto &audit_record = ctx.audit_record_;
  audit_record.upd_dyn_ = true;
  int64_t logic_clock = 0;
  DECODE_HEADER();
  if (OB_FAIL(ret)) {
  } else if (flag.is_tx_terminated_) {
    audit_record.upd_term_ = true;
    if (OB_NOT_NULL(tx)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "tx should be null: released in static state update", K(ret), K(tx->tx_id_));
    }
  } else if (flag.is_fallback_) {
    audit_record.upd_fallback_ = true;
    ret = txn_free_route__sanity_check_fallback_(tx, ctx);
  } else if (OB_ISNULL(tx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "tx should not be null", K(ret), K(tx_id), K(flag), K(session_id));
  } else {
    auto start_ts = ObTimeUtility::current_time();
    ObSpinLockGuard guard(tx->lock_);
    if (!tx->tx_id_.is_valid()) {
      // bug, dynamic state exist, txn should be active
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "tx id should be valid", K(ret), KPC(tx));
    } else if (OB_FAIL(decode_i64(buf, len, pos, &logic_clock))) {
      TRANS_LOG(ERROR, "decode logic clock fail", K(ret));
    } else if (OB_FAIL(update_logic_clock_(logic_clock))) {
      TRANS_LOG(ERROR, "update logic clock fail", K(ret));
    } else if (OB_FAIL(tx->decode_dynamic_state(buf, len, pos))) {
      TRANS_LOG(ERROR, "decode dynamic state fail", K(ret));
    }
    auto elapsed_us = ObTimeUtility::current_time() - start_ts;
    ObTransTraceLog &tlog = tx->get_tlog();
    REC_TRANS_TRACE_EXT(&tlog, tx_free_route_update_dynamic, OB_Y(ret),
                        OB_ID(time_used), elapsed_us,
                        OB_ID(txid), tx_id.get_id(),
                        OB_ID(logic_clock), logic_clock,
                        OB_ID(length), len,
                        OB_ID(ref), tx->get_ref(),
                        OB_ID(thread_id), GETTID());
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[tx-free-route::update_state]", K(ret), K(flag), K(tx_id), K(logic_clock),
              K(session_id), K(ctx), KP(tx));
  }
  return ret;
}

int ObTransService::txn_free_route__update_parts_state(const uint32_t session_id,
                                                       ObTxDesc *&tx,
                                                       ObTxnFreeRouteCtx &ctx,
                                                       const char* buf,
                                                       const int64_t len,
                                                       int64_t &pos)
{
  int ret = OB_SUCCESS;
  auto &audit_record = ctx.audit_record_;
  audit_record.upd_parts_ = true;
  DECODE_HEADER();
  if (OB_FAIL(ret)) {
  } else if (flag.is_tx_terminated_) {
    audit_record.upd_term_ = true;
    // [prev req] : [action]
    // <commit>   : do nothing
    // <start_tx> : cleanup
    // <select RR>: tx is null
    // <savepoint>: tx is null
    if (OB_NOT_NULL(tx)) {
      ObSpinLockGuard guard(tx->lock_);
      tx->parts_.reset();
    }
  } else if (flag.is_fallback_) {
    audit_record.upd_fallback_ = true;
    ret = txn_free_route__sanity_check_fallback_(tx, ctx);
  } else if (OB_ISNULL(tx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "tx should not be null", K(ret), K(tx_id), K(flag), K(session_id));
  } else {
    auto start_ts = ObTimeUtility::current_time();
    ObSpinLockGuard guard(tx->lock_);
    if (!tx->tx_id_.is_valid()) {
      // bug, dynamic state exist, txn should be active
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "tx id should be active", K(ret), K(tx_id), K(tx->tx_id_));
    } else if (OB_FAIL(tx->decode_parts_state(buf, len, pos))) {
      TRANS_LOG(WARN, "decode participants fail", K(ret));
    }
    auto elapsed_us = ObTimeUtility::current_time() - start_ts;
    ObTransTraceLog &tlog = tx->get_tlog();
    REC_TRANS_TRACE_EXT(&tlog, tx_free_route_update_participants, OB_Y(ret),
                        OB_ID(txid), tx_id.get_id(),
                        OB_ID(time_used), elapsed_us,
                        OB_ID(length), len,
                        OB_ID(ref), tx->get_ref(),
                        OB_ID(thread_id), GETTID());
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[tx-free-route::update_state]", K(ret), K(flag), K(tx_id), K(session_id), K(ctx), KP(tx));
  }
  return ret;
}

int ObTransService::txn_free_route__update_extra_state(const uint32_t session_id,
                                                       ObTxDesc *&tx,
                                                       ObTxnFreeRouteCtx &ctx,
                                                       const char* buf,
                                                       const int64_t len,
                                                       int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t logic_clock = 0;
  auto &audit_record = ctx.audit_record_;
  audit_record.upd_extra_ = true;
  DECODE_HEADER();
  if (OB_FAIL(ret)) {
  } else if (flag.is_tx_terminated_) {
    audit_record.upd_term_ = true;
    // [prev req] : [action]
    // <start_tx> : cleanup snapshot_version_, snapshot_scn
    // <savepoint>: not this branch
    // <select RR>: not this branch
    // <commit>   : tx is NULL, do nothing
    if (OB_NOT_NULL(tx)) {
      audit_record.upd_reset_snapshot_ = true;
      ObSpinLockGuard guard(tx->lock_);
      tx->snapshot_version_.reset();
      tx->snapshot_scn_ = 0;
    }
  } else if (flag.is_fallback_) {
    audit_record.upd_fallback_ = true;
    ret = txn_free_route__sanity_check_fallback_(tx, ctx);
  } else {
    bool add_tx = OB_ISNULL(tx);
    bool replace_tx = OB_NOT_NULL(tx) && tx->tx_id_ != tx_id;
    auto before_tx_id = OB_NOT_NULL(tx) ? tx->tx_id_ : ObTransID();
    audit_record.replace_tx_ = replace_tx;
    audit_record.alloc_tx_ = add_tx;
    if (OB_FAIL(decode_i64(buf, len, pos, &logic_clock))) {
      TRANS_LOG(ERROR, "decode logic clock fail", K(ret));
    } else if (OB_FAIL(update_logic_clock_(logic_clock))) {
      TRANS_LOG(ERROR, "update logic clock fail", K(ret));
    }
    if (OB_SUCC(ret) && add_tx && OB_FAIL(acquire_tx(tx, session_id))) {
      // only has savepoints or txn scope snapshot, txn not started
      // acquire tx to hold extra info
      TRANS_LOG(WARN, "acquire tx fail", K(ret));
    }
    if (OB_SUCC(ret) && replace_tx && tx->tx_id_.is_valid()) {
      if (OB_UNLIKELY(tx->in_tx_for_free_route())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "try overwrite tx which is active", K(ret), K(tx_id), K(tx->tx_id_));
      } else if (OB_FAIL(tx_desc_mgr_.remove(*tx))) {
        TRANS_LOG(WARN, "unregister old tx fail", K(ret), K(tx->tx_id_));
      }
    }
    if (OB_SUCC(ret)) {
      auto start_ts = ObTimeUtility::current_time();
      ObSpinLockGuard guard(tx->lock_);
      if (OB_FAIL(tx->decode_extra_state(buf, len, pos))) {
        TRANS_LOG(ERROR, "decode extra fail", K(ret));
      } else if ((add_tx || replace_tx) && OB_FAIL(tx_desc_mgr_.add_with_txid(tx->tx_id_, *tx))) {
        TRANS_LOG(WARN, "add tx to mgr fail", K(ret));
      } else if (FALSE_IT(tx->flags_.REPLICA_ = tx->addr_ != self_)) {
      }
      if (OB_FAIL(ret) && add_tx) {
        release_tx(*tx);
        tx = NULL;
      } else {
        auto elapsed_us = ObTimeUtility::current_time() - start_ts;
        ObTransTraceLog &tlog = tx->get_tlog();
        REC_TRANS_TRACE_EXT(&tlog, tx_free_route_update_extra, OB_Y(ret),
                            OB_ID(txid), tx_id.get_id(),
                            OB_ID(from), before_tx_id.get_id(),
                            OB_ID(time_used), elapsed_us,
                            OB_ID(logic_clock), logic_clock,
                            OB_ID(length), len,
                            OB_ID(tag1), add_tx,
                            OB_ID(tag2), replace_tx,
                            OB_ID(ref), tx->get_ref(),
                            OB_ID(thread_id), GETTID());
      }
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[tx-free-route::update_state]", K(ret), K(flag), K(tx_id), K(logic_clock),
              K(session_id), K(ctx), KP(tx));
    if (OB_NOT_NULL(tx)) {
      ObSpinLockGuard guard(tx->lock_);
      TRANS_LOG(INFO, "dump tx", KPC(tx));
    }
  }
  return ret;
}

#define TXN_ENCODE_LOGIC_CLOCK                                          \
  if (OB_FAIL(ret)) {                                                   \
  } else if (OB_FAIL(encode_i64(buf, len, pos, ObSequence::inc_and_get_max_seq_no()))) { \
    TRANS_LOG(WARN, "encode logic clock fail", K(ret));                 \
  }

#define TXN_ENCODE_LOGIC_CLOCK_LENGTH l += encoded_length_i64(INT64_MAX)

#define PRE_ENCODE_(X) TXN_ENCODE_ ## X
#define PRE_ENCODE(X) PRE_ENCODE_(X)
#define TXN_ENCODE_NORMAL_STATE_X(X, ...)                               \
  if (OB_SUCC(ret) && ctx.flag_.is_return_normal_state()) {             \
    if (OB_ISNULL(tx)) {                                                \
      ret = OB_ERR_UNEXPECTED;                                          \
      TRANS_LOG(WARN, "tx is null", K(ret));                            \
    }                                                                   \
    LST_DO(PRE_ENCODE, (;), ##__VA_ARGS__);                             \
    if (OB_SUCC(ret)) {                                                 \
      ObSpinLockGuard guard(tx->lock_);                                 \
      if (OB_FAIL(tx->encode_##X##_state(buf, len, pos))) {             \
        TRANS_LOG(WARN, "encode state fail", K(ret), KPC(tx));          \
      }                                                                 \
    }                                                                   \
  }

#define PRE_ENCODE_LENGTH_(X) TXN_ENCODE_ ## X ## _LENGTH
#define PRE_ENCODE_LENGTH(X) PRE_ENCODE_LENGTH_(X)
#define ENCODE_NORMAL_STATE_LENGTH(X, ...)                              \
  if (ctx.flag_.is_return_normal_state()) {                             \
    LST_DO(PRE_ENCODE_LENGTH, (;), ##__VA_ARGS__);                      \
    ObSpinLockGuard guard(tx->lock_);                                   \
    l += tx->X##_state_encoded_length();                                \
  }

#define TXN_FREE_ROUTE_SERIALIZE_PARAM                                  \
  const uint32_t session_id, ObTxDesc *tx, ObTxnFreeRouteCtx &ctx, char* buf, const int64_t len, int64_t &pos
#define DEF_TXN_FREE_ROUTE_SERIALIZE(X)                                 \
  int ObTransService::txn_free_route__serialize_##X##_state(TXN_FREE_ROUTE_SERIALIZE_PARAM)
#define DEF_TXN_FREE_ROUTE_SERIALIZE_LENGTH(X)                          \
  int64_t ObTransService::txn_free_route__get_##X##_state_serialize_size(ObTxDesc *tx, ObTxnFreeRouteCtx &ctx)

#define SERIALIZE_IMPL(type, ...)                                       \
  DEF_TXN_FREE_ROUTE_SERIALIZE(type)                                    \
  {                                                                     \
    int ret = OB_SUCCESS;                                               \
    ENCODE_HEADER();                                                    \
    TXN_ENCODE_NORMAL_STATE_X(type, ##__VA_ARGS__);                     \
    return ret;                                                         \
  }                                                                     \
  DEF_TXN_FREE_ROUTE_SERIALIZE_LENGTH(type)                              \
  {                                                                     \
    ENCODE_HEADER_LENGTH();                                             \
    ENCODE_NORMAL_STATE_LENGTH(type, ##__VA_ARGS__);                    \
    return l;                                                           \
  }

SERIALIZE_IMPL(static);
SERIALIZE_IMPL(dynamic, LOGIC_CLOCK);
SERIALIZE_IMPL(parts);
SERIALIZE_IMPL(extra, LOGIC_CLOCK);

int ObTransService::calc_txn_free_route(ObTxDesc *tx, ObTxnFreeRouteCtx &ctx)
{
  // 1. decide whether need return txn state totally
  //    1. if required, decide which part of state need to be returned
  // 2. decide whether need fallback : disable free route for current txn
  //    1. if required, mark ObTxnFreeRouteCtx.can_free_route_ = false
  //       1. if txn started remotedly, push txn state to remote

  //
  // The calculation of Transaction Free Route property
  //
  // The txn state transfer classify to four categories (from -> to):
  // [1] IDLE -> ACTIVE
  // eg.
  // 1) the BEGIN stmt
  // 2) first DML stmt executed for autocommit=0 session
  // 3) other TCLs caused txn into active
  // actions:
  // a. decide txn_free_route is allowed
  //    via proxy's passthorugh flag, tenant's config and the txn's state will not cause fallback
  // b. remember the decision on `ctx`
  //
  // [2] ACTIVE -> ACTIVE
  // eg.
  // 1) DML stmt after txn has been actived
  // 2) SELECT stmt after txn has been actived
  // 3) other queries which don't change data
  // 4) BEGIN stmt, which cause current txn commit and start a new txn
  //    * this will caused txn state transform : ACTIVE -> TERMINATE -> IDLE ->ACTIVE
  //      and the final state is ACTIVE, which fall into this category
  //      because start txn will caused the trueth of all state changed, it is also covered here
  // actions:
  // a. if on txn-start-node, and txn-free-route's decision is on and has not been fallbacked
  //    if so, check whether txn's current state is too large and need to fallback
  //    1) if need fallback, just remember the falllback decision on `ctx`
  //    2) otherwise, txn continiue to return normal changed state
  // b. if on txn-start-node, but txn-free-route is disabled or it has been fallbacked
  //    further decision is not required, just return
  // c. if on temporary-txn-node, it must be the txn-free-route is enabled
  //    1) check whether txn's current state is too large and need to fallback
  //       if so, distinguish to two cases:
  //       i) the txn's total size is over the proxy's max recieveable size
  //          in such case, it's required to push current txn's state to txn-start-node directly
  //          if push is timeouted or some other errors, we will try to return the state to proxy
  //            and the proxy will try its best to handle it, otherwise it will shutdown the session
  //       ii) the txn's total size is not overflow, but max participant count reach the configed limit
  //           in such case fallback will be set on `ctx` but the state not push to txn-start-node
  //    2) otherwise, txn continue to return normal changed state
  // [3] ACTIVE -> TERMINATED/IDLE
  // eg.
  // 1) COMMIT/ROLLBACK stmt
  // 2) BEGIN stmt which cause current txn commit and the commit was failed
  //    * the txn's final state is not in_txn(IDLE/TERMINATED) because the BEGIN has not been issued
  // actions:
  // a. return tx terminate flag as state, this cause proxy cleanup its state for this txn implicitly
  //    and also cause following request sent to other temporary-txn-node cleanup its state for this
  //    txn implicitly
  // [4] IDLE -> IDLE
  // eg.
  // 1) execute query with session's auto_commit=true
  // 2) SELECT stmt or other read-only COMMAND

  //
  // The Txn State Contents
  //
  // The State return policy:
  // for txn terminated or fallbacked situations, return special txn state to accomplish these purpose:
  // 1) cleanup the state on proxy when txn terminated, which cause its memory was released
  // 2) cleanup the state on temporary-txn-node when next request sent to it, which cause txn release
  //    on these node after txn terminated
  // 3) cleanup the state on proxy when fallback happend, which cause its memory was released
  // 4) hint the txn-start-node sens the txn has been fallbacked on an temporary-txn-node
  //
  // the State package returned is orgnized as following structure:
  //
  // [1byte flag][detailed changed tx state byte string]
  //
  // the flag has two bit currently:
  // `is_fallbacked` bit: indicate whether processing node has decided to fallback and the state has
  //                      been pushed to txn-start-node
  // `is_terminated` bit: indicate the txn has finished
  //
  // if niether of these flag was setted, the detailed changed txn state is exist as the content of
  // normal state, otherwise it's empty
  auto &audit_record = ctx.audit_record_;
  if (OB_NOT_NULL(tx)) {
    tx->lock_.lock();
  }
  int ret = OB_SUCCESS;
  bool in_txn = OB_NOT_NULL(tx) && tx->in_tx_for_free_route_();
  bool prev_in_txn = ctx.in_txn_before_handle_request_;
  bool proxy_support = ctx.is_proxy_support_;
  bool is_xa = OB_NOT_NULL(tx) && tx->is_xa_trans();
  bool is_xa_tightly_couple = is_xa && tx->xa_tightly_couple_;
  bool is_tx_start = !prev_in_txn && in_txn;      // IDLE => ACTIVE
  bool is_tx_terminated = prev_in_txn && !in_txn; // ACTIVE => ROLLBACK/COMMIT/IDLE
  bool is_tx_active_to_active = prev_in_txn && in_txn; // ACTIVE => ACTIVE
  bool support_free_route = false;

  bool return_normal_state = false, return_terminated_state = false, return_fallback_state = false;
  int64_t state_size = 0;

  if (is_tx_start) {
    audit_record.tx_start_ = true;
    ctx.can_free_route_ = false;
    ctx.is_fallbacked_ = false;
    ctx.tx_id_ = tx->tx_id_;
    ctx.txn_addr_ = self_;
    if (proxy_support) {
      if (!is_xa_tightly_couple) {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
        if (OB_LIKELY(tenant_config.is_valid())) {
          if (tenant_config->_enable_transaction_internal_routing) {
            audit_record.svr_flag_ = true;
            if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_1_0_0) {
              TRANS_LOG(DEBUG, "observer not upgrade to 4_1_0_0");
            } else  if (!need_fallback_(*tx, state_size)) {
              support_free_route = true;
              ctx.can_free_route_ = true;
              return_normal_state = true;
            } else {
              ctx.is_fallbacked_ = true;
              TRANS_LOG(TRACE, "txn free route is enabled but need fallback", K(state_size));
            }
          } else {
            audit_record.svr_flag_ = false;
          }
        }
      }
    }
  }
  if (is_tx_terminated) {
    audit_record.tx_term_ = true;
    // current node should be the txn's start node
    if (self_ != ctx.txn_addr_) {
      if (ctx.is_idle_released()) {
        // it is idle released before send response
        // in such case, the txn has terminated on txn start node
        // the proxy should close this connection
        ret = OB_TRANS_KILLED;
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "txn terminated not on txn start node", K(ret), K_(ctx.txn_addr), K_(self));
      }
    } else if (ctx.can_free_route_) {
      return_terminated_state = true;
    }
  }
  bool fallback_happened = false, fallback_state_synced = false;
  if (is_tx_active_to_active) {
    // if on txn start node, and if free_route is open,
    // refer proxy switch to do fallback
    if (self_ == ctx.txn_addr_) {
      if (ctx.tx_id_ != tx->tx_id_) {
        // implicit commit and start new tx
        ctx.tx_id_ = tx->tx_id_;
      }
      if (ctx.can_free_route_ && !ctx.is_fallbacked_) {
        if (!proxy_support || need_fallback_(*tx, state_size)) {
          ctx.is_fallbacked_ = true;
          fallback_state_synced = true;
          fallback_happened = true;
          return_fallback_state = true;
        } else {
          return_normal_state = true;
        }
      }
    }
    // if on other nodes, refer transaction size to do fallback
    else {
      if (need_fallback_(*tx, state_size)) {
        if (state_size > MAX_STATE_SIZE) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(push_tx_state_to_remote_(*tx, ctx.txn_addr_))) {
            // fallback to tx start node fail, ignore, let proxy handle big packet
            TRANS_LOG(WARN, "push tx state to txn node fail", K(tmp_ret));
            audit_record.push_state_ = true;
            return_normal_state = true;
          } else {
            fallback_state_synced = true;
            return_fallback_state = true;
          }
        } else {
          return_normal_state = true;
        }
        fallback_happened = true;
        ctx.is_fallbacked_ = true;
      } else {
        return_normal_state = true;
      }
    }
  }

  //
  // calc state changed
  //
  if (OB_SUCC(ret)) {
    if (return_normal_state) {
      if (is_xa && is_tx_start) {
        // XA START same as START TX
        ctx.static_changed_ = true;
        ctx.dynamic_changed_ = true;
        ctx.parts_changed_ = true;
        ctx.extra_changed_ = true;
      } else {
        bool changed = false;
        changed |= (ctx.static_changed_ = tx->is_static_changed());
        changed |= (ctx.dynamic_changed_ = tx->is_dynamic_changed());
        changed |= (ctx.parts_changed_ = tx->is_parts_changed());
        changed |= (ctx.extra_changed_ = tx->is_extra_changed());
        if (is_tx_start && !changed) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "txn start but none state changed", K(ret), KPC(tx), K(ctx));
        }
      }
    }
    if (return_fallback_state) {
      audit_record.ret_fallback_ = true;
      ctx.flag_.is_fallback_ = true;
      ctx.static_changed_ = true;
      ctx.dynamic_changed_ = true;
      ctx.parts_changed_ = true;
      ctx.extra_changed_ = true;
    }
    if (return_terminated_state) {
      audit_record.ret_term_ = true;
      ctx.flag_.is_tx_terminated_ = true;
      ctx.static_changed_ = true;
      ctx.dynamic_changed_ = true;
      ctx.parts_changed_ = true;
      ctx.extra_changed_ = true;
    }
  }
  if (OB_NOT_NULL(tx)) {
    tx->lock_.unlock();
  }
  // finally, set free_route to false when txn not active
  if (!in_txn) {
    ctx.can_free_route_ = false;
  }
  if (ctx.is_changed()) {
    ctx.inc_global_version();
  }
  ctx.set_calculated();
  // audit record
  audit_record.calculated_ = true;
  audit_record.free_route_ = ctx.can_free_route_;
  audit_record.fallback_ = ctx.is_fallbacked_;
  audit_record.chg_static_ = ctx.static_changed_;
  audit_record.chg_dyn_ = ctx.dynamic_changed_;
  audit_record.chg_parts_ = ctx.parts_changed_;
  audit_record.chg_extra_ = ctx.extra_changed_;
  audit_record.start_node_ = self_ == ctx.txn_addr_;
  audit_record.xa_ = is_xa;
  audit_record.xa_tightly_couple_ = is_xa_tightly_couple;
#ifndef NDEBUG
  ObTransID tx_id = tx ? tx->tx_id_ : ObTransID();
  TRANS_LOG(INFO, "[tx free route] calc tx free route properities done", K(ret),
            K(is_tx_start), K(is_tx_terminated), K(is_tx_active_to_active),
            K(prev_in_txn), K(is_xa), K(is_xa_tightly_couple), K(proxy_support),
            K(support_free_route),
            K(fallback_happened), K(fallback_state_synced),
            K(state_size),
            K(return_normal_state), K(return_fallback_state), K(return_terminated_state),
            K(ctx), K(tx_id), K(lbt()));
#endif
  return ret;
}

bool ObTransService::need_fallback_(ObTxDesc &tx, int64_t &total_size)
{
  bool fallback = false;
  total_size = OB_E(EventTable::EN_TX_FREE_ROUTE_STATE_SIZE, tx.tx_id_) tx.estimate_state_size();
  if (total_size > MAX_STATE_SIZE) {
    fallback = true;
  }
  return fallback;
}

int ObTransService::push_tx_state_to_remote_(ObTxDesc &tx, const ObAddr &txn_addr)
{
  int ret = OB_SUCCESS;
  auto start_ts = ObTimeUtility::current_time();
  ObTxFreeRoutePushState state;
  state.tenant_id_ = tenant_id_;
  int64_t len = 0;
  uint64_t flag = 0;
#define static_FLAG  1 << 3
#define dynamic_FLAG 1 << 2
#define parts_FLAG   1 << 1
#define extra_FLAG   1 << 0
#define TX_FREE_ROUTE_ADD_LEN(T) TX_FREE_ROUTE_ADD_LEN_(T)
#define TX_FREE_ROUTE_ADD_LEN_(T)              \
  if (tx.is_##T##_changed()) {                 \
    flag |= T##_FLAG;                          \
    len += tx.T##_state_encoded_length();      \
  }
  LST_DO(TX_FREE_ROUTE_ADD_LEN, (), static, dynamic, parts, extra);
  char *buf = (char*)ob_malloc(len, lib::ObMemAttr(tenant_id_, "TxnFreeRoute"));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(ERROR, "[tx free route] alloc state mem failed", KR(ret), K(len));
  } else {
    int64_t pos = 0;
#define TX_FREE_ROUTE_ENCODE(TYPE) TX_FREE_ROUTE_ENCODE_(TYPE)
#define TX_FREE_ROUTE_ENCODE_(TYPE)                                     \
    state.TYPE##_offset_ = pos;                                         \
    if (tx.is_##TYPE##_changed()) {                                     \
      ret = tx.encode_##TYPE##_state(buf, len, pos);                    \
    }
    LST_DO(TX_FREE_ROUTE_ENCODE, (), static, dynamic, parts, extra);
    state.buf_ = ObString(len, buf);
  }
  if (OB_SUCC(ret)) {
    state.tx_id_ = tx.tx_id_;
    state.logic_clock_ = ObSequence::inc_and_get_max_seq_no();
    ObTxFreeRoutePushStateResp resp;
    if (OB_FAIL(rpc_->sync_access(txn_addr, state, resp))) {
      TRANS_LOG(WARN, "[tx free route] push state fail", K(ret));
    } else if (OB_FAIL(resp.ret_)) {
      TRANS_LOG(WARN, "[tx free route] push state fail", K(ret));
    } else {
      TRANS_LOG(INFO, "[tx free route] push txn state success", K(txn_addr), K(tx));
    }
  }
  auto elapsed_us = ObTimeUtility::current_time() - start_ts;
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, tx_free_route_send_state, OB_Y(ret),
                      OB_ID(time_used), elapsed_us,
                      OB_ID(flag), (void*)flag,
                      OB_ID(length), len,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

int ObTransService::tx_free_route_handle_push_state(const ObTxFreeRoutePushState &state)
{
  int ret = OB_SUCCESS;
  // 1. get tx
  // 2. update logic clock
  // 3. update via txdesc.update_state : msg.static_state_ / msg.dynamic / msg.parts / msg.extra
  ObTxDesc *tx = NULL;
  ObTransID tx_id;
  if (OB_FAIL(tx_desc_mgr_.get(state.tx_id_, tx))) {
    TRANS_LOG(WARN, "get tx fail", K(ret));
  } else if (OB_ISNULL(tx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx is null", K(ret));
  } else if (OB_FAIL(update_logic_clock_(state.logic_clock_))) {
    TRANS_LOG(WARN, "update logic clock fail", K(ret));
  } else {
    auto start_ts = ObTimeUtility::current_time();
    const char *buf = state.buf_.ptr();
    int64_t buf_len = state.buf_.length();
    int64_t static_len = state.dynamic_offset_;
    int64_t dynamic_len = state.parts_offset_ - state.dynamic_offset_;
    int64_t parts_len = state.extra_offset_ - state.parts_offset_;
    int64_t extra_len = buf_len - state.extra_offset_;
    int tmp_ret = OB_SUCCESS;
    uint64_t flag = 0;
    ObSpinLockGuard guard(tx->lock_);
    tx_id = tx->tx_id_;
    if (static_len > 0) {
      flag |= 1 << 3;
      int64_t pos = 0;
      if (OB_TMP_FAIL(tx->decode_static_state(buf, static_len, pos))) {
        TRANS_LOG(WARN, "decode static state fail", K(ret));
      }
      ret = COVER_SUCC(tmp_ret);
    }
    if (dynamic_len > 0) {
      flag |= 1 << 2;
      int64_t pos = 0;
      if (OB_TMP_FAIL(tx->decode_dynamic_state(buf + state.dynamic_offset_, dynamic_len, pos))) {
        TRANS_LOG(WARN, "decode dynamic state fail", K(ret));
      }
      ret = COVER_SUCC(tmp_ret);
    }
    if (parts_len > 0) {
      flag |= 1 << 1;
      int64_t pos = 0;
      if (OB_TMP_FAIL(tx->decode_parts_state(buf + state.parts_offset_, parts_len, pos))) {
        TRANS_LOG(WARN, "decode parts state fail", K(ret));
      }
      ret = COVER_SUCC(tmp_ret);
    }
    if (extra_len > 0) {
      flag |= 1 << 0;
      int64_t pos = 0;
      if (OB_TMP_FAIL(tx->decode_extra_state(buf + state.extra_offset_, extra_len, pos))) {
        TRANS_LOG(WARN, "decode extra state fail", K(ret));
      }
      ret = COVER_SUCC(tmp_ret);
    }
    auto elapsed_us = ObTimeUtility::current_time() - start_ts;
    ObTransTraceLog &tlog = tx->get_tlog();
    REC_TRANS_TRACE_EXT(&tlog, tx_free_route_recv_state, OB_Y(ret),
                        OB_ID(time_used), elapsed_us,
                        OB_ID(logic_clock), state.logic_clock_,
                        OB_ID(flag), (void*)flag,
                        OB_ID(length), buf_len,
                        OB_ID(ref), tx->get_ref(),
                        OB_ID(thread_id), GETTID());
  }
  if (OB_NOT_NULL(tx)) {
    tx_desc_mgr_.revert(*tx);
  }
  TRANS_LOG(INFO, "[tx free route] handle push tx state", K(ret), K(state), K(tx_id));
  return ret;
}

int ObTransService::tx_free_route_check_alive(ObTxnFreeRouteCtx &ctx, const ObTxDesc &tx, const uint32_t session_id)
{
  int ret = OB_SUCCESS;
  // 1. skip txn born node
  // 2. skip txn is idle state
  if (ctx.txn_addr_.is_valid() && ctx.txn_addr_ != self_ && tx.is_in_tx()) {
    common::ObCurTraceId::init(self_);
    ObTxFreeRouteCheckAliveMsg m;
    m.request_id_ = ctx.get_local_version();
    m.tx_id_ = tx.tx_id_;
    m.sender_ = self_;
    m.receiver_ = ctx.txn_addr_;
    m.req_sess_id_ = session_id;
    m.tx_sess_id_ = tx.sess_id_;
    ret = rpc_->post_msg(ctx.txn_addr_, m);
    bool print_log = OB_FAIL(ret);
#ifndef NDEBUG
    print_log = true;
#endif
    if (print_log) {
      TRANS_LOG(INFO, "[txn free route] check txn alive", K(ret), "request_id", m.request_id_,
                "txn_addr", ctx.txn_addr_, "tx_id", tx.tx_id_, K(session_id));
    }
    common::ObCurTraceId::reset();
  }
  return ret;
}

int ObTransService::tx_free_route_handle_check_alive(const ObTxFreeRouteCheckAliveMsg &msg, const int retcode)
{
  int ret = OB_SUCCESS;
  ObTxFreeRouteCheckAliveRespMsg m;
  m.request_id_ = msg.request_id_;
  m.receiver_   = msg.sender_;
  m.sender_     = self_;
  m.tx_id_      = msg.tx_id_;
  m.req_sess_id_ = msg.req_sess_id_;
  m.ret_        = retcode;
  ret = rpc_->post_msg(m.receiver_, m);
#ifndef NDEBUG
  TRANS_LOG(INFO, "[txn free route] handle check txn alive", K(retcode), K_(msg.sender),
            K_(msg.tx_id), K_(msg.req_sess_id), K_(msg.request_id));
#endif
  return ret;
}

}
}
