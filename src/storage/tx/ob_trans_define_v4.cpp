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

#include "ob_trans_define.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_ctx.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "observer/ob_server.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_define.h"
#include "common/storage/ob_sequence.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/stat/ob_latch_define.h"
#include "ob_trans_functor.h"
#include "ob_tx_stat.h"
#include "ob_xa_ctx.h"

#define USING_LOG_PREFIX TRANS
namespace oceanbase
{
using namespace oceanbase::share;
namespace transaction
{
ObTxIsolationLevel tx_isolation_from_str(const ObString &s)
{
  static const ObString LEVEL_NAME[4] =
    {
     "READ-UNCOMMITTED",
     "READ-COMMITTED",
     "REPEATABLE-READ",
     "SERIALIZABLE"
    };
  ObTxIsolationLevel r = ObTxIsolationLevel::INVALID;
  for (int32_t i = 0; i < 4; i++) {
    if (0 == LEVEL_NAME[i].case_compare(s)) {
      r = static_cast<ObTxIsolationLevel>(i);
      break;
    }
  }
  return r;
}

const ObString &get_tx_isolation_str(const ObTxIsolationLevel isolation)
{
  static const ObString EMPTY_STR;
  static const ObString LEVEL_STR[4] =
    {
     "READ-UNCOMMITTED",
     "READ-COMMITTED",
     "REPEATABLE-READ",
     "SERIALIZABLE"
    };
  const ObString *isolation_str = &EMPTY_STR;
  switch (isolation) {
  case ObTxIsolationLevel::RU: isolation_str = &LEVEL_STR[0]; break;
  case ObTxIsolationLevel::RC: isolation_str = &LEVEL_STR[1]; break;
  case ObTxIsolationLevel::RR: isolation_str = &LEVEL_STR[2]; break;
  case ObTxIsolationLevel::SERIAL: isolation_str = &LEVEL_STR[3]; break;
  default: break;
  }
  return *isolation_str;
}

ObTxSavePoint::ObTxSavePoint()
  : type_(T::INVL), scn_(), session_id_(0), user_create_(false), name_() {}

ObTxSavePoint::ObTxSavePoint(const ObTxSavePoint &a)
{
  *this = a;
}

ObTxSavePoint &ObTxSavePoint::operator=(const ObTxSavePoint &a)
{
  type_ = a.type_;
  scn_ = a.scn_;
  switch(type_) {
  case T::SAVEPOINT:
  case T::STASH: {
    name_ = a.name_;
    session_id_ = a.session_id_;
    user_create_ = a.user_create_;
    break;
  }
  case T::SNAPSHOT: snapshot_ = a.snapshot_; break;
  default: break;
  }
  return *this;
}

bool ObTxSavePoint::operator==(const ObTxSavePoint &a) const
{
  bool is_equal = false;
  if (type_ == a.type_ && scn_== a.scn_) {
    switch(type_) {
    case T::SAVEPOINT:
    case T::STASH: {
      is_equal = name_ == a.name_ && session_id_ == a.session_id_ && user_create_ == a.user_create_;
      break;
    }
    case T::SNAPSHOT: is_equal = snapshot_ == a.snapshot_; break;
    default: break;
    }
  }
  return is_equal;
}

ObTxSavePoint::~ObTxSavePoint()
{
  release();
}

void ObTxSavePoint::release()
{
  type_ = T::INVL;
  snapshot_ = NULL;
  scn_.reset();
  session_id_ = 0;
  user_create_ = false;
}

void ObTxSavePoint::rollback()
{
  if (is_snapshot() && snapshot_) {
    snapshot_->valid_ = false;
  }
  release();
}

void ObTxSavePoint::init(ObTxReadSnapshot *snapshot)
{
  type_ = T::SNAPSHOT;
  snapshot_ = snapshot;
  scn_ = snapshot->core_.scn_;
}

int ObTxSavePoint::init(const ObTxSEQ &scn, const ObString &name, const uint32_t session_id, const bool user_create, const bool stash)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(name_.assign(name))) {
    if (OB_BUF_NOT_ENOUGH == ret) {
      //rewrite ret
      ret = OB_ERR_TOO_LONG_IDENT;
    }
    TRANS_LOG(WARN, "invalid savepoint name", K(ret), K(name));
  } else {
    type_ = stash ? T::STASH : T::SAVEPOINT;
    scn_ = scn;
    session_id_ = session_id;
    user_create_ = user_create;
  }
  return ret;
}

DEF_TO_STRING(ObTxSavePoint)
{
  int64_t pos = 0;
  J_OBJ_START();
  switch(type_) {
  case T::SAVEPOINT: J_KV("savepoint", name_); break;
  case T::SNAPSHOT:  J_KV(KPC_(snapshot)); break;
  case T::STASH: J_KV("stash_savepoint", name_); break;
  default: J_KV("invalid", true);
  }
  J_COMMA();
  J_KV(K_(scn));
  J_COMMA();
  J_KV(K_(session_id));
  J_COMMA();
  J_KV(K_(user_create));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObTxExecResult, incomplete_, parts_,
                    conflict_txs_,// FARM COMPAT WHITELIST for cflict_txs_
                    conflict_info_array_);
OB_SERIALIZE_MEMBER(ObTxSnapshot, tx_id_, version_, scn_, elr_);
OB_SERIALIZE_MEMBER(ObTxReadSnapshot,
                    valid_,
                    core_,
                    source_,
                    uncertain_bound_,
                    snapshot_lsid_,
                    parts_,
                    snapshot_ls_role_,
                    committed_,
                    snapshot_acquire_addr_);
OB_SERIALIZE_MEMBER(ObTxPart, id_, addr_, epoch_, first_scn_, last_scn_);

DEFINE_SERIALIZE(ObTxDesc::FLAG::FOR_FIXED_SER_VAL)
{
  int ret = OB_SUCCESS;
  return serialization::encode_i64(buf, buf_len, pos, v_);
}
DEFINE_DESERIALIZE(ObTxDesc::FLAG::FOR_FIXED_SER_VAL)
{
  int ret = OB_SUCCESS;
  return serialization::decode_i64(buf, data_len, pos, (int64_t*)&v_);
}
DEFINE_GET_SERIALIZE_SIZE(ObTxDesc::FLAG::FOR_FIXED_SER_VAL)
{
  return serialization::encoded_length_i64(v_);
}

uint64_t ObTxDesc::FLAG::COMPAT_FOR_EXEC::get_serialize_v_() const
{
  FLAG ret_flag;
  FLAG this_flag;
  this_flag.compat_for_exec_.v_ = v_;
  ret_flag.compat_for_exec_.v_ = 0;
#define _SET_FLAG_(x) ret_flag.x = this_flag.x
  LST_DO(_SET_FLAG_, (;),
         EXPLICIT_,
         SHADOW_,
         REPLICA_,
         TRACING_);
#undef _SET_FLAG_
  return ret_flag.compat_for_exec_.v_;
}

uint64_t ObTxDesc::FLAG::COMPAT_FOR_TX_ROUTE::get_serialize_v_() const
{
  FLAG ret_flag;
  FLAG this_flag;
  this_flag.compat_for_tx_route_.v_ = v_;
  ret_flag.compat_for_tx_route_.v_ = 0;
#define _SET_FLAG_(x) ret_flag.x = this_flag.x
  LST_DO(_SET_FLAG_, (;),
         EXPLICIT_,
         SHADOW_,
         REPLICA_,
         TRACING_,
         RELEASED_,
         PARTS_INCOMPLETE_,
         PART_EPOCH_MISMATCH_,
         WITH_TEMP_TABLE_,
         DEFER_ABORT_);
#undef _SET_FLAG_
  return ret_flag.compat_for_tx_route_.v_;
}

#define DEF_SERIALIZE_COMPAT_FOR_TX_DESC_FLAG(THE_TYPE) \
  DEFINE_SERIALIZE(ObTxDesc::FLAG::THE_TYPE)     \
  {                                              \
    int ret = OB_SUCCESS;                                               \
    const uint64_t compat_v = get_serialize_v_();                       \
    return serialization::encode_vi64(buf, buf_len, pos, compat_v);     \
  }                                                                     \
  DEFINE_DESERIALIZE(ObTxDesc::FLAG::THE_TYPE)                          \
  {                                                                     \
    int ret = OB_SUCCESS;                                               \
    return serialization::decode_vi64(buf, data_len, pos, (int64_t*)&v_); \
  }                                                                     \
  DEFINE_GET_SERIALIZE_SIZE(ObTxDesc::FLAG::THE_TYPE)                   \
  {                                                                     \
    const uint64_t compat_v = get_serialize_v_();                       \
    return serialization::encoded_length_vi64(compat_v);                \
  }

DEF_SERIALIZE_COMPAT_FOR_TX_DESC_FLAG(COMPAT_FOR_EXEC)
DEF_SERIALIZE_COMPAT_FOR_TX_DESC_FLAG(COMPAT_FOR_TX_ROUTE)

OB_SERIALIZE_MEMBER(ObTxDesc,
                    tenant_id_,
                    cluster_id_,
                    cluster_version_,
                    sess_id_,
                    addr_,
                    tx_id_,
                    isolation_,
                    access_mode_,
                    op_sn_,
                    state_,
                    flags_.compat_for_exec_,
                    expire_ts_,
                    active_ts_,
                    timeout_us_,
                    lock_timeout_us_,
                    active_scn_,
                    parts_,
                    xid_,
                    flags_.for_serialize_v_,
                    seq_base_);
OB_SERIALIZE_MEMBER(ObTxParam,
                    timeout_us_,
                    lock_timeout_us_,
                    access_mode_,
                    isolation_,
                    cluster_id_);
OB_SERIALIZE_MEMBER(ObTxSavePoint,
                    type_,
                    scn_,
                    session_id_,
                    user_create_,
                    name_);

OB_SERIALIZE_MEMBER(ObTxInfo,
                    tenant_id_,
                    cluster_id_,
                    cluster_version_,
                    addr_,
                    tx_id_,
                    isolation_,
                    access_mode_,
                    snapshot_version_,
                    snapshot_uncertain_bound_,
                    op_sn_,
                    alloc_ts_,
                    active_ts_,
                    timeout_us_,
                    expire_ts_,
                    active_scn_,
                    parts_,
                    session_id_,
                    savepoints_,
                    seq_base_);
OB_SERIALIZE_MEMBER(ObTxStmtInfo,
                    tx_id_,
                    op_sn_,
                    parts_,
                    state_,
                    savepoints_);

int ObTxDesc::trans_deep_copy(const ObTxDesc &x)
{
  UNUSED(x);
  return OB_NOT_SUPPORTED;
}
ObTxDesc::ObTxDesc()
  : tenant_id_(0),
    cluster_id_(-1),
    trace_info_(),
    cluster_version_(0),
    seq_base_(0),
    tx_consistency_type_(ObTxConsistencyType::INVALID),
    addr_(),
    tx_id_(),
    xid_(),
    xa_tightly_couple_(true),
    xa_start_addr_(),
    isolation_(ObTxIsolationLevel::RC), // default is RC
    access_mode_(ObTxAccessMode::INVL),   // default is INVL
    snapshot_version_(),
    snapshot_uncertain_bound_(0),
    snapshot_scn_(),
    sess_id_(0),
    assoc_sess_id_(0),
    global_tx_type_(ObGlobalTxType::PLAIN),
    op_sn_(0),                          // default is from 0
    state_(State::INVL),
    flags_({ 0 }),
    state_change_flags_({ 0 }),
    alloc_ts_(-1),
    active_ts_(-1),
    timeout_us_(-1),
    lock_timeout_us_(-1),
    expire_ts_(INT64_MAX),              // never expire by default
    commit_ts_(-1),
    finish_ts_(-1),
    active_scn_(),
    min_implicit_savepoint_(),
    last_branch_id_(0),
    parts_(),
    savepoints_(),
    conflict_txs_(),
    coord_id_(),
    commit_expire_ts_(0),
    commit_parts_(),
    commit_version_(),
    commit_out_(-1),
    commit_times_(0),
    commit_start_scn_(),
    abort_cause_(0),
    can_elr_(false),
    lock_(common::ObLatchIds::TX_DESC_LOCK),
    commit_cb_lock_(common::ObLatchIds::TX_DESC_COMMIT_LOCK),
    commit_cb_(NULL),
    cb_tid_(-1),
    exec_info_reap_ts_(0),
    brpc_mask_set_(),
    rpc_cond_(),
    commit_task_(),
    xa_ctx_(NULL)
#ifdef ENABLE_DEBUG_LOG
  , alloc_link_()
#endif
{}

/**
 * Wrap txDesc to IDLE state cleanup txn dirty state
 * keep txn parameters and resource allocated before
 * txn active:
 * - savepoint
 * - txn-level snapshot
 *
 * Be Careful when you make any change here
 *
 * caller must hold txDesc.lock_
 */
int ObTxDesc::switch_to_idle()
{
  tx_id_.reset();
  trace_info_.reset();
  flags_.switch_to_idle_();
  state_change_flags_.reset();
  active_ts_ = 0;
  timeout_us_ = 0;
  lock_timeout_us_ = -1;
  expire_ts_ = INT64_MAX;
  commit_ts_ = 0;
  finish_ts_ = 0;
  active_scn_.reset();
  parts_.reset();
  conflict_txs_.reset();
  coord_id_.reset();
  commit_parts_.reset();
  commit_version_.reset();
  commit_out_ = 0;
  commit_times_ = 0;
  commit_start_scn_.set_min();
  abort_cause_ = 0;
  can_elr_ = false;
  commit_cb_ = NULL;
  cb_tid_ = -1;
  exec_info_reap_ts_ = 0;
  commit_task_.reset();
  state_ = State::IDLE;
  return OB_SUCCESS;
}

inline void ObTxDesc::FLAG::switch_to_idle_()
{
  const FLAG sv = *this;
  v_ = 0;
  REPLICA_ = sv.REPLICA_;
}

// this function helper will update current flag with the given
// and ensure private flags will not be overriden
ObTxDesc::FLAG ObTxDesc::FLAG::update_with(const ObTxDesc::FLAG &flag)
{
  ObTxDesc::FLAG ret = flag;
#define KEEP_PRIVATE_(x) ret.x = x
  LST_DO(KEEP_PRIVATE_, (;),
         SHADOW_,        // private for each scheduler node
         REPLICA_,       // private for each scheduler node
         INTERRUPTED_,   // private on original scheduler
         RELEASED_,      // private on original scheduler
         BLOCK_);        // private for single stmt scope
#undef KEEP_PRIVATE_
  // do merge for some flags, because it may be set asynchorously
  // PART_ABORTED may be set on original scheduler while stmt executing on remote scheduler
  // in such case, it should not be override by state update from remote scheduler
  ret.PART_ABORTED_ |= PART_ABORTED_;
  return ret;
}

ObTxDesc::~ObTxDesc()
{
  reset();
}

void ObTxDesc::reset()
{
#ifndef NDEBUG
  FORCE_PRINT_TRACE(&tlog_, "[tx desc trace]");
#else
  if (state_ == State::IDLE || state_ == State::COMMITTED) {
    if (finish_ts_ - commit_ts_ > 5 * 1000 * 1000) {
      FORCE_PRINT_TRACE(&tlog_, "[tx slow commit][tx desc trace]");
    }
  } else if (flags_.SHADOW_) { /* skip clone's destory */}
  else {
    FORCE_PRINT_TRACE(&tlog_, "[tx desc trace]");
  }
#endif
  TRANS_LOG(DEBUG, "reset txdesc", KPC(this), K(lbt()));
  tenant_id_ = 0;
  cluster_id_ = -1;
  trace_info_.reset();
  cluster_version_ = 0;
  seq_base_ = 0;
  tx_consistency_type_ = ObTxConsistencyType::INVALID;

  addr_.reset();
  tx_id_.reset();
  xid_.reset();
  xa_tightly_couple_ = true;
  xa_start_addr_.reset();
  isolation_ = ObTxIsolationLevel::INVALID;
  access_mode_ = ObTxAccessMode::INVL;
  snapshot_version_.reset();
  snapshot_uncertain_bound_ = 0;
  snapshot_scn_.reset();
  global_tx_type_ = ObGlobalTxType::PLAIN;

  op_sn_ = -1;

  state_ = State::INVL;

  flags_.v_ = 0;
  flags_.SHADOW_ = true;
  state_change_flags_.reset();

  alloc_ts_ = -1;
  active_ts_ = -1;
  timeout_us_ = -1;
  lock_timeout_us_ = -1;
  expire_ts_ = -1;
  commit_ts_ = -1;
  finish_ts_ = -1;

  active_scn_.reset();
  min_implicit_savepoint_.reset();
  last_branch_id_ = 0;
  parts_.reset();
  savepoints_.reset();
  conflict_txs_.reset();

  coord_id_.reset();
  commit_expire_ts_ = -1;
  commit_parts_.reset();
  commit_version_.reset();
  commit_out_ = -1;
  commit_times_ = 0;
  commit_start_scn_.set_min();
  abort_cause_ = 0;
  can_elr_ = false;

  commit_cb_ = NULL;
  cb_tid_ = -1;
  exec_info_reap_ts_ = 0;
  brpc_mask_set_.reset();
  rpc_cond_.reset();
  commit_task_.reset();
  xa_ctx_ = NULL;
  tlog_.reset();
  xa_ctx_ = NULL;
}

const ObString &ObTxDesc::get_tx_state_str() const {
  static const ObString TxStateName[] =
    {
     ObString("INVALID"),
     ObString("IDLE"),
     ObString("ACTIVE"),
     ObString("IMPLICIT_ACTIVE"),
     ObString("ROLLBACK_SAVEPOINT"),
     ObString("IN_TERMINATE"),
     ObString("ABORTED"),
     ObString("ROLLED_BACK"),
     ObString("COMMIT_TIMEOUT"),
     ObString("COMMIT_UNKNOWN"),
     ObString("COMMITTED"),
     ObString("XA_PREPARING"),
     ObString("XA_COMMITTING"),
     ObString("XA_COMMITTED"),
     ObString("XA_ROLLBACKING"),
     ObString("XA_ROLLBACKED"),
     ObString("UNNAMED STATE")
    };
  const int state = MIN((int)state_, sizeof(TxStateName) / sizeof(ObString) - 1);
  return TxStateName[state];
}

void ObTxDesc::print_trace_() const
{
  FORCE_PRINT_TRACE(&tlog_, "[tx desc trace]");
}

void ObTxDesc::print_trace()
{
  int ret = OB_SUCCESS;
  bool self_locked = lock_.self_locked();
  if (!self_locked && OB_FAIL(lock_.lock())) {
    TRANS_LOG(WARN, "lock failed", K(ret));
  } else {
    FORCE_PRINT_TRACE(&tlog_, "[tx desc trace]");
  }
  if (!self_locked && OB_SUCC(ret)) {
    lock_.unlock();
  }
}

void ObTxDesc::dump_and_print_trace()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_.trylock())) {
    TRANS_LOG(WARN, "acquire lock fail", K(ret), KP(this), K(tx_id_));
  } else {
    share::ObTaskController::get().allow_next_syslog();
    TRANS_LOG(INFO, "[tx desc dump]", KPC(this));
    print_trace_();
    lock_.unlock();
  }
}

bool ObTxDesc::in_tx_or_has_extra_state()
{
  ObSpinLockGuard guard(lock_);
  return in_tx_or_has_extra_state_();
}

bool ObTxDesc::in_tx_or_has_extra_state_() const
{
  return is_in_tx() || has_extra_state_();
}

bool ObTxDesc::has_extra_state_() const
{
  if (snapshot_version_.is_valid()) {
    return true;
  }
  // TODO(yunxing.cyx): refine this iter for performance
  ARRAY_FOREACH_NORET(savepoints_, i) {
    if (savepoints_[i].is_savepoint()) {
      return true;
    }
  }
  return false;
}

bool ObTxDesc::in_tx_for_free_route()
{
  ObSpinLockGuard guard(lock_);
  return in_tx_for_free_route_();
}

bool ObTxDesc::in_tx_for_free_route_()
{
  return (addr_.is_valid() && (addr_ != GCONF.self_addr_)) // txn free route temporary node
    || in_tx_or_has_extra_state_();
}

bool ObTxDesc::contain_savepoint(const ObString &sp)
{
  bool hit = false;
  ARRAY_FOREACH_X(savepoints_, i, cnt, !hit) {
    ObTxSavePoint &it = savepoints_[cnt - 1 - i];
    if (it.is_savepoint() && it.name_ == sp) {
      hit = true;
    }
    if (it.is_stash()) { break; }
  }
  return hit;
}

int ObTxDesc::update_part_(ObTxPart &a, const bool append)
{
  int ret = OB_SUCCESS;
  bool hit = false;
  ARRAY_FOREACH_NORET(parts_, i) {
    ObTxPart &p = parts_[i];
    if (p.id_ == a.id_) {
      hit = true;
      if (p.epoch_ == ObTxPart::EPOCH_DEAD) {
        if (a.epoch_ > 0) { // unexpected: from dead to alive
          flags_.PART_EPOCH_MISMATCH_ = true;
          ret = OB_TRANS_NEED_ROLLBACK;
          TRANS_LOG(WARN, "epoch missmatch", K(ret), K(a), K(p));
        }
      } else if (p.epoch_ <= 0) {
        p.epoch_ = a.epoch_;
      } else if (a.epoch_ > 0 && p.epoch_ != a.epoch_) {
        flags_.PART_EPOCH_MISMATCH_ = true;
        ret = OB_TRANS_NEED_ROLLBACK;
        TRANS_LOG(WARN, "tx-part epoch changed", K(ret), K(a), K(p));
      }
      if (OB_SUCC(ret)) {
        if (a.addr_.is_valid()) { p.addr_ = a.addr_; }
        p.first_scn_ = MIN(a.first_scn_, p.first_scn_);
        p.last_scn_ = p.last_scn_.is_max() ? a.last_scn_ : MAX(a.last_scn_, p.last_scn_);
        p.last_touch_ts_ = exec_info_reap_ts_ + 1;
      }
      break;
    }
  }

  if (ObTxDesc::State::IMPLICIT_ACTIVE == state_ && !active_scn_.is_valid()) {
    /*
     * it is a first stmt's retry, we should set active scn
     * to enable recognizing it is first stmt
     */
    active_scn_ = get_tx_seq();
  }

  if (!hit) {
    if (append) {
      a.last_touch_ts_ = exec_info_reap_ts_ + 1;
      if (OB_FAIL(parts_.push_back(a))) {
        flags_.PARTS_INCOMPLETE_ = true;
        TRANS_LOG(WARN, "push fail, set incomplete", K(ret), K_(parts), K(a));
      } else {
        implicit_start_tx_();
      }
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  state_change_flags_.PARTS_CHANGED_ = true;
  return ret;
}

void ObTxDesc::post_rb_savepoint_(ObTxPartRefList &parts, const ObTxSEQ &savepoint)
{
  ARRAY_FOREACH_NORET(parts, i) {
    parts[i].last_scn_ = savepoint;
  }
  state_change_flags_.PARTS_CHANGED_ = true;
}

int ObTxDesc::update_clean_part(const share::ObLSID &id,
                                const int64_t epoch,
                                const ObAddr &addr)
{
  ObTxPart p;
  p.id_ = id;
  p.epoch_ = epoch;
  p.addr_ = addr;
  p.first_scn_ = ObTxSEQ::MAX_VAL();
  p.last_scn_ = get_tx_seq();
  return update_part_(p, false);
}

/*
 * update_part - update txn participant info
 *
 * if failed, txn was marked with PARTS_INCOMPLETE
 */
int ObTxDesc::update_part(ObTxPart &p)
{
  ObSpinLockGuard guard(lock_);
  return update_part_(p, true);
}

/*
 * update_parts - update txn participants info
 *
 * if failed, txn was marked with PARTS_INCOMPLETE
 */
int ObTxDesc::update_parts(const share::ObLSArray &list)
{
  int ret = OB_SUCCESS, tmp_ret = ret;
  ARRAY_FOREACH_NORET(list, i) {
    const ObLSID &it = list[i];
    ObTxPart n;
    n.id_ = it;
    n.epoch_ = ObTxPart::EPOCH_UNKNOWN;
    n.first_scn_ = ObTxSEQ::MAX_VAL();
    n.last_scn_ = ObTxSEQ::MAX_VAL();
    if (OB_TMP_FAIL(update_part_(n))) {
      ret = tmp_ret;
    }
  }
  return ret;
}

void ObTxDesc::implicit_start_tx_()
{
  if (parts_.count() > 0 && state_ == ObTxDesc::State::IDLE) {
    state_ = ObTxDesc::State::IMPLICIT_ACTIVE;
    active_ts_ = ObClockGenerator::getClock();
    expire_ts_ = active_ts_ + timeout_us_;
    active_scn_ = get_tx_seq();
    state_change_flags_.mark_all();
  }
}

int64_t ObTxDesc::get_expire_ts() const
{
  /*
   * expire_ts was setup when tx state switch to ACTIVE | IMPLICIT_ACTIVE
   * because create TxCtx (which need acquire tx expire_ts) happens before
   * tx state switch to IMPLICIT_ACTIVE
   */
  return expire_ts_ == INT64_MAX ?
    ObClockGenerator::getClock() + timeout_us_ : expire_ts_;
}

int ObTxDesc::update_parts_(const ObTxPartList &list)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(list, i) {
    bool hit = false;
    const ObTxPart &a = list[i];
    ret = update_part_(const_cast<ObTxPart&>(a));
  }
  return ret;
}

int ObTxDesc::merge_exec_info_with(const ObTxDesc &src)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(update_parts_(src.parts_))) {
    TRANS_LOG(WARN, "update parts failed", K(ret), KPC(this), K(src));
  }
  if (src.flags_.PARTS_INCOMPLETE_) {
    flags_.PARTS_INCOMPLETE_ = true;
    TRANS_LOG(WARN, "src is incomplete, set dest incomplete also", K(ret), K(src));
  }
  return ret;
}

int ObTxDesc::get_inc_exec_info(ObTxExecResult &exec_info)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (exec_info_reap_ts_ >= 0) {
    ARRAY_FOREACH(parts_, i) {
      ObTxPart &p = parts_[i];
      if (p.last_touch_ts_ > exec_info_reap_ts_ &&
          OB_FAIL(exec_info.parts_.push_back(p))) {
        TRANS_LOG(WARN, "push fail", K(ret), K(p), KPC(this), K(exec_info));
      }
    }
    if (OB_FAIL(ret) || flags_.PARTS_INCOMPLETE_) {
      exec_info.incomplete_ = true;
      TRANS_LOG(WARN, "set incomplete", K(ret), K(flags_.PARTS_INCOMPLETE_));
    }
    exec_info_reap_ts_ += 1;
  }
  if (OB_SUCC(ret) && OB_SUCC(exec_info.merge_cflict_txs(conflict_txs_))) {
    conflict_txs_.reset();
  }
  DETECT_LOG(TRACE, "merge conflict txs to exec result", K(conflict_txs_), K(exec_info));
  return ret;
}

int ObTxDesc::add_exec_info(const ObTxExecResult &exec_info)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(update_parts_(exec_info.parts_))) {
    TRANS_LOG(WARN, "update parts failed", K(ret), KPC(this), K(exec_info));
  }
  if (exec_info.incomplete_) {
    flags_.PARTS_INCOMPLETE_ = true;
    TRANS_LOG(WARN, "exec_info is incomplete set incomplete also", K(ret), K(exec_info));
  }
  (void) merge_conflict_txs_(exec_info.conflict_txs_);
  DETECT_LOG(TRACE, "add exec result conflict txs to desc", K(conflict_txs_), K(exec_info));
  return ret;
}

int ObTxDesc::set_commit_cb(ObITxCallback *cb)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(commit_cb_lock_);
  if (OB_NOT_NULL(commit_cb_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "commit_cb not null", K(ret), KP_(commit_cb), K(tx_id_), KP(cb));
  } else {
    commit_cb_ = cb;
  }
  return ret;
}


//try to acquire commit_cb_lock only if commit_cb_ not null
inline bool ObTxDesc::acq_commit_cb_lock_if_need_()
{
  int ret = OB_SUCCESS;
  bool succ = false;
  int cnt = 0;
  do {
    ret = commit_cb_lock_.trylock();
    if (ret == OB_EAGAIN) {
      if (OB_NOT_NULL(commit_cb_)) {
        if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {
          TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "use too much time wait lock", K_(tx_id));
        }
        if (++cnt < 200) { PAUSE(); }
        else { ob_usleep(5000); }
      }
    } else if (OB_FAIL(ret)) {
      TRANS_LOG(ERROR, "try lock failed", K(ret), K_(tx_id));
    } else {
      succ = true;
    }
  } while (ret == OB_EAGAIN && OB_NOT_NULL(commit_cb_));
  return succ;
}

/*
 * execute_commit_cb - callback caller after commit finished
 *
 * because user supllied callback may do anything, it should take care
 * when process the callback, especially callback call into transaction
 * side again via these interfaces:
 * 1) release_tx
 * 2) reuse_tx
 *
 * in reuse_tx situ, it's required to wait all referents of current txn
 * to quit except current thread. so if two of thread is going here and
 * executing 'execute_commit_cb', the former will wait the later quit
 * while the later is blocking on 'commit_cb_lock_' to wait the former
 * quit, which introduce a deadlock.
 *
 * in order to prevent such situ, calling thread must try lock and check
 * there requirement of continue the calling procedure, as in the above
 * situ, the later thread is not required to go ahead, instead they can
 * shortcut and return.
 * for more detail, refer to 'acq_commit_cb_lock_if_need_' function.
 */
bool ObTxDesc::execute_commit_cb()
{
  bool executed = false;
  /*
   * load_acquire state_ and commit_out_
   * pair with ObTransService::handle_tx_commit_result_
   */
  ATOMIC_LOAD_ACQ((int*)&state_);
  if (is_tx_end() || is_xa_terminate_state_()) {
    ObTransID tx_id = tx_id_;
    ObITxCallback *cb = commit_cb_;
    int ret = OB_SUCCESS;
     if (OB_NOT_NULL(commit_cb_) && acq_commit_cb_lock_if_need_()) {
      if (OB_NOT_NULL(commit_cb_)) {
        executed = true;
        cb = commit_cb_;
        commit_cb_ = NULL;
        if (0 <= cb_tid_) {
#ifdef ENABLE_DEBUG_LOG
          ob_abort();
#endif
          TRANS_LOG(ERROR, "unexpected error happen, cb_tid_ should smaller than 0",
                    KP(this), K(tx_id), KP(cb_tid_));
        }
        ATOMIC_STORE_REL(&cb_tid_, GETTID());
        // NOTE: it is required add trace event before callback,
        // because txDesc may be released after callback called
        REC_TRANS_TRACE_EXT(&tlog_, exec_commit_cb,
                            OB_ID(arg), (void*)cb,
                            OB_ID(ref), get_ref(),
                            OB_ID(thread_id), GETTID());
        commit_cb_lock_.unlock();
        cb->callback(commit_out_);
      } else {
        commit_cb_lock_.unlock();
      }
    }
    TRANS_LOG(TRACE, "execute_commit_cb", KP(this), K(tx_id), KP(cb), K(executed));
  }
  return executed;
}

ObITxCallback *ObTxDesc::cancel_commit_cb()
{
  int ret = OB_SUCCESS;
  ObITxCallback* commit_cb = nullptr;

  /* cancel may called from `commit_cb_` it self */
  bool self_locked = commit_cb_lock_.self_locked();
  if (!self_locked && OB_FAIL(commit_cb_lock_.lock())) {
    TRANS_LOG(ERROR, "lock failed", K(ret), K_(tx_id));
  } else {
    if (OB_NOT_NULL(commit_cb_)) {
      commit_cb = commit_cb_;
      commit_cb_ = NULL;
    }
    if (!self_locked) {
      commit_cb_lock_.unlock();
    }
  }

  return commit_cb;
}

bool ObTxDesc::is_xa_terminate_state_() const
{
  return state_ == State::SUB_PREPARED ||
    state_ == State::SUB_COMMITTED ||
    state_ == State::SUB_ROLLBACKED;
}

bool ObTxDesc::has_implicit_savepoint() const
{
  return min_implicit_savepoint_.is_valid();
}
void ObTxDesc::add_implicit_savepoint(const ObTxSEQ savepoint)
{
  if (!min_implicit_savepoint_.is_valid() || min_implicit_savepoint_ > savepoint ) {
    min_implicit_savepoint_ = savepoint;
  }
}
void ObTxDesc::release_all_implicit_savepoint()
{
  min_implicit_savepoint_.reset();
}
void ObTxDesc::release_implicit_savepoint(const ObTxSEQ savepoint)
{
  if (min_implicit_savepoint_ == savepoint) {
    min_implicit_savepoint_.reset();
  }
  // invalid txn snapshot if it was created after the savepoint
  if (snapshot_version_.is_valid() && savepoint < snapshot_scn_) {
    TRANS_LOG(INFO, "release txn snapshot_version", K_(snapshot_version),
              K(savepoint), K_(snapshot_scn), K_(tx_id));
    snapshot_version_.reset();
  }
}

int ObTxDesc::fetch_conflict_txs(ObIArray<ObTransIDAndAddr> &array)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(array.assign(conflict_txs_))) {
    DETECT_LOG(WARN, "fail to fetch conflict txs", K(ret), K(conflict_txs_));
  }
  conflict_txs_.reset();
  return ret;
}

int ObTxDesc::add_conflict_tx(const ObTransIDAndAddr conflict_tx) {
  ObSpinLockGuard guard(lock_);
  return add_conflict_tx_(conflict_tx);
}

int ObTxDesc::add_conflict_tx_(const ObTransIDAndAddr &conflict_tx) {
  int ret = OB_SUCCESS;
  if (conflict_txs_.count() >= MAX_RESERVED_CONFLICT_TX_NUM) {
    ret = OB_SIZE_OVERFLOW;
    int64_t max_reserved_conflict_tx_num = MAX_RESERVED_CONFLICT_TX_NUM;
    DETECT_LOG(WARN, "too many conflict trans id", K(max_reserved_conflict_tx_num), K(conflict_txs_), K(conflict_tx));
  } else if (!is_contain(conflict_txs_, conflict_tx)) {
    if (OB_FAIL(conflict_txs_.push_back(conflict_tx))) {
      DETECT_LOG(WARN, "fail to push conflict tx to conflict_txs_", K(ret), K(conflict_txs_), K(conflict_tx));
    }
  }
  return ret;
}

int ObTxDesc::merge_conflict_txs(const ObIArray<ObTransIDAndAddr> &conflict_txs)
{
  ObSpinLockGuard guard(lock_);
  return merge_conflict_txs_(conflict_txs);
}

int ObTxDesc::merge_conflict_txs_(const ObIArray<ObTransIDAndAddr> &conflict_txs)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t idx = 0; idx < conflict_txs.count() && OB_SUCC(tmp_ret); ++idx) {
    // This function should try its best to push the conflict_tx into the array.
    // However, whether the insertion is successful or not
    // should not affect the normal execution process.
    // So we just use tmp_ret to catch the error code here.
    if (OB_TMP_FAIL(add_conflict_tx_(conflict_txs.at(idx)))) {
      DETECT_LOG(WARN, "fail to add conflict tx to conflict_txs_", K(tmp_ret), K(conflict_txs_), K(conflict_txs.at(idx)));
    }
  }
  return ret;
}

// get global trans type for session
// 1. if xid is empty or xa_ctx is null, PLAIN is returned.
// 2. if xid from session is equal to xid in desc and global_tx_type is DBLINK_TRANS,
//    DBLINK_TRANS is returned.
// 3. if xid from session is not equal to xid in desc and global_tx_type is DBLINK_TRANS,
//    XA_TRANS is returned.
ObGlobalTxType ObTxDesc::get_global_tx_type(const ObXATransID &xid) const
{
  ObGlobalTxType tx_type = ObGlobalTxType::PLAIN;
  if (NULL == xa_ctx_ || xid_.empty()) {
    // return PLAIN
  } else if (!xid_.all_equal_to(xid)) {
    tx_type = ObGlobalTxType::XA_TRANS;
  } else {
    tx_type = global_tx_type_;
  }
  return tx_type;
}
int ObTxDesc::get_parts_copy(ObTxPartList &copy_parts)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(copy_parts.assign(parts_))) {
    TRANS_LOG(WARN, "TxDesc get participants copy error", K(ret), KPC(this));
  }
  return ret;
}

int ObTxDesc::get_savepoints_copy(ObTxSavePointList &copy_savepoints)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(copy_savepoints.assign(savepoints_))) {
    TRANS_LOG(WARN, "TxDesc get savepoints copy error", K(ret), KPC(this));
  }
  return ret;
}

void ObTxDesc::set_xa_ctx(ObXACtx *xa_ctx)
{
  xa_ctx_ = xa_ctx;
  if (OB_NOT_NULL(xa_ctx)) {
    xa_tightly_couple_ = xa_ctx->is_tightly_coupled();
    xa_start_addr_ = GCONF.self_addr_;
  }
}

ObTxParam::ObTxParam()
  : timeout_us_(0),
    lock_timeout_us_(-1),
    access_mode_(ObTxAccessMode::RW),
    isolation_(ObTxIsolationLevel::RC),
    cluster_id_(0)
{}
bool ObTxParam::is_valid() const
{
  return timeout_us_ > 0
    && lock_timeout_us_ >= -1
    && access_mode_ != ObTxAccessMode::INVL
    && isolation_ != ObTxIsolationLevel::INVALID
    && cluster_id_ > 0;
}
ObTxParam::~ObTxParam()
{
  timeout_us_ = 0;
  lock_timeout_us_ = -1;
  access_mode_ = ObTxAccessMode::INVL;
  isolation_ = ObTxIsolationLevel::INVALID;
  cluster_id_ = -1;
}

ObTxSnapshot::ObTxSnapshot()
  : version_(), tx_id_(), scn_(), elr_(false) {}

ObTxSnapshot::~ObTxSnapshot()
{
  scn_.reset();
  elr_ = false;
}

void ObTxSnapshot::reset()
{
  version_.reset();
  tx_id_.reset();
  scn_.reset();
  elr_ = false;
}

ObTxSnapshot &ObTxSnapshot::operator=(const ObTxSnapshot &r)
{
  version_ = r.version_;
  tx_id_ = r.tx_id_;
  scn_ = r.scn_;
  elr_ = r.elr_;
  return *this;
}

ObTxReadSnapshot::ObTxReadSnapshot()
  : valid_(false),
    committed_(false),
    core_(),
    source_(SRC::INVL),
    snapshot_lsid_(),
    snapshot_ls_role_(common::ObRole::INVALID_ROLE),
    snapshot_acquire_addr_(),
    uncertain_bound_(0),
    parts_()
{}

ObTxReadSnapshot::~ObTxReadSnapshot()
{
  valid_ = false;
  committed_ = false;
  source_ = SRC::INVL;
  snapshot_ls_role_ = common::INVALID_ROLE;
  snapshot_acquire_addr_.reset();
  uncertain_bound_ = 0;
}

void ObTxReadSnapshot::reset()
{
  valid_ = false;
  committed_ = false;
  core_.reset();
  source_ = SRC::INVL;
  snapshot_lsid_.reset();
  snapshot_ls_role_ = common::INVALID_ROLE;
  snapshot_acquire_addr_.reset();
  uncertain_bound_ = 0;
  parts_.reset();
}

int ObTxReadSnapshot::assign(const ObTxReadSnapshot &from)
{
  int ret = OB_SUCCESS;
  valid_ = from.valid_;
  committed_ = from.committed_;
  core_ = from.core_;
  source_ = from.source_;
  snapshot_lsid_ = from.snapshot_lsid_;
  snapshot_ls_role_ = from.snapshot_ls_role_;
  snapshot_acquire_addr_ = from.snapshot_acquire_addr_;
  uncertain_bound_ = from.uncertain_bound_;
  if (OB_FAIL(parts_.assign(from.parts_))) {
   TRANS_LOG(WARN, "assign snapshot fail", K(ret), K(from));
  }
  return ret;
}

void ObTxReadSnapshot::init_weak_read(const SCN snapshot)
{
  core_.version_ = snapshot;
  core_.tx_id_.reset();
  core_.scn_.reset();
  core_.elr_ = false;
  source_ = SRC::WEAK_READ_SERVICE;
  parts_.reset();
  valid_ = true;
}

void ObTxReadSnapshot::init_special_read(const SCN snapshot)
{
  core_.version_ = snapshot;
  core_.tx_id_.reset();
  core_.scn_.reset();
  core_.elr_ = false;
  source_ = SRC::SPECIAL;
  parts_.reset();
  valid_ = true;
}

void ObTxReadSnapshot::init_ls_read(const share::ObLSID &ls_id, const ObTxSnapshot &core)
{
  core_ = core;
  source_ = SRC::LS;
  snapshot_lsid_ = ls_id;
  valid_ = true;
}

void ObTxReadSnapshot::specify_snapshot_scn(const share::SCN snapshot)
{
  core_.version_ = snapshot;
  source_ = SRC::SPECIAL;
}

void ObTxReadSnapshot::wait_consistency()
{
  if (SRC::GLOBAL == source_) {
    const int64_t ts = MonotonicTs::current_time().mts_;
    if (ts < uncertain_bound_) {
      ob_usleep(uncertain_bound_ - ts);
    }
  }
}

const char* ObTxReadSnapshot::get_source_name() const
{
  static const char* const SRC_NAME[] = { "INVALID", "GTS", "LOCAL", "WEAK_READ", "USER_SPECIFIED", "NONE" };
  return SRC_NAME[(int)source_];
}

/*
 * format snapshot info for sql audit display
 * contains: src, ls_id, ls_role, parts
 * 1. local select:
 *   "src:LOCAL;ls_id:1001;ls_role:LEADER;parts:[1001,1002]"
 * 2. glocal select, with no ls and ls_role: "src:GTS;parts[1001]"
 * 3. insert: "NONE"
 * 4. longer than 128 chars, with "..." in the end
 *   "src:GLOBAL;ls_id:1001;ls_role:LEADER;parts:[1001,1002,102..."
 */
int ObTxReadSnapshot::format_source_for_display(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invaild arguments", K(ret), KPC(this), K(buf_len));
  } else {
    const char *snapshot_src = get_source_name();
    const char *ls_role = role_to_string(snapshot_ls_role_);
    uint64_t ls_id = snapshot_lsid_.id();
    int n = 0;
    bool need_fill = true;
    if (SRC::NONE == source_) {
      // insert has no stmt snapshot
      need_fill = false;
      n = snprintf(buf, buf_len, "NONE");
    } else if (SRC::GLOBAL != source_) {
      n = snprintf(buf, buf_len, "src:%s;ls_id:%ld;ls_role:%s;parts:[",
                   snapshot_src, ls_id, ls_role);
    } else {
      // GLOBAL snapshot not display ls_id and ls_role
      n = snprintf(buf, buf_len, "src:%s;parts:[", snapshot_src);
    }
    if (n < 0){
      ret = OB_UNEXPECT_INTERNAL_ERROR;
      TRANS_LOG(WARN, "fail to fill snapshot source", K(ret), KPC(this), K(n), K(pos), K(buf_len));
    } else if(need_fill) {
      pos += n;
      bool buf_not_enough = false;
      for (int i = 0; i < parts_.count(); i++) {
        n = snprintf(buf + pos, buf_len - pos, "%ld,", parts_[i].left_.id());
        if (n < 0) {
          ret = OB_UNEXPECT_INTERNAL_ERROR;
          TRANS_LOG(WARN, "fail to fill snapshot source", K(ret), KPC(this), K(n), K(pos), K(buf_len));
        } else if (n > buf_len - pos) {
          buf_not_enough = true;
          break;
        } else {
          pos += n;
        }
      }
      if (buf_not_enough) {
        // buf full, parts fill not complete
        int remain_cnt = buf_len - pos;
        pos = remain_cnt < 4 ? buf_len - 4 : pos;
        buf[pos] = '.';
        buf[pos + 1] = '.';
        buf[pos + 2] = '.';
        buf[pos + 3] = '\0';
      } else if(parts_.count() > 0) {
        buf[pos - 1] = ']';
      } else {
        buf[pos] = ']';
        buf[pos + 1] = '\0';
      }
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "fail to generate snapshot source", KPC(this), K(pos), K(buf_len), K(ObString(buf)));
  }
  return ret;
}

ObTxExecResult::ObTxExecResult()
  : allocator_("TxExecResult", 0 == MTL_ID() ? OB_SERVER_TENANT_ID : MTL_ID()),
    incomplete_(false),
    touched_ls_list_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
    parts_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
    conflict_txs_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_)
{}

ObTxExecResult::~ObTxExecResult()
{
  incomplete_ = false;
}

void ObTxExecResult::reset()
{
  incomplete_ = false;
  touched_ls_list_.reset();
  parts_.reset();
  conflict_txs_.reset();
  allocator_.reset();
}

int ObTxExecResult::add_touched_ls(const share::ObLSID ls)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(touched_ls_list_.push_back(ls))) {
    incomplete_ = true;
    TRANS_LOG(WARN, "add touched ls failed, set incomplete", K(ret), K(ls));
  }
  return ret;
}

int ObTxExecResult::add_touched_ls(const ObIArray<share::ObLSID> &ls_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ARRAY_FOREACH_NORET(ls_list, i) {
    if (OB_TMP_FAIL(add_touched_ls(ls_list.at(i)))) {
      TRANS_LOG(WARN, "add fail", K(ret));
      ret = tmp_ret;
    }
  }
  if (OB_FAIL(ret)) {
    incomplete_ = true;
    TRANS_LOG(WARN, "add fail, set incomplete", K(ret), KPC(this));
  }
  return ret;
}

template<typename T>
static int append_dedup(ObIArray<T> &a, const ObIArray<T> &b)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(b, i) {
    if (!is_contain(a, b.at(i))) { ret = a.push_back(b.at(i)); }
  }
  return ret;
}

int ObTxExecResult::merge_result(const ObTxExecResult &r)
{
  int ret = OB_SUCCESS;
  TRANS_LOG(TRACE, "txExecResult.merge with.start", K(r), KPC(this), K(lbt()));
  incomplete_ |= r.incomplete_;
  if (OB_FAIL(append_dedup(parts_, r.parts_))) {
    incomplete_ = true;
    TRANS_LOG(WARN, "merge fail, set incomplete", K(ret), KPC(this));
  } else if (OB_FAIL(append_dedup(touched_ls_list_, r.touched_ls_list_))) {
    incomplete_ = true;
    TRANS_LOG(WARN, "merge touched_ls_list fail, set incomplete", K(ret), KPC(this));
  }
  if (OB_SUCC(ret)) {
    ret = merge_cflict_txs(r.conflict_txs_);
  }
  if (incomplete_) {
    TRANS_LOG(TRACE, "tx result incomplete:", KP(this));
  }

  TRANS_LOG(TRACE, "txExecResult.merge with.end", KPC(this));
  return ret;
}

int ObTxExecResult::merge_cflict_txs(const common::ObIArray<transaction::ObTransIDAndAddr> &txs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_dedup(conflict_txs_, txs))) {
    DETECT_LOG(WARN, "append fail", KR(ret), KPC(this), K(txs));
  }
  return ret;
}

int ObTxExecResult::assign(const ObTxExecResult &r)
{
  int ret = OB_SUCCESS;
  incomplete_ = r.incomplete_;
  if (OB_FAIL(parts_.assign(r.parts_))) {
    incomplete_ = true;
    TRANS_LOG(WARN, "assign fail, set incomplete", K(ret), KPC(this));
  } else if (OB_FAIL(touched_ls_list_.assign(r.touched_ls_list_))) {
    incomplete_ = true;
    TRANS_LOG(WARN, "assign touched_ls_list fail, set incomplete", K(ret), KPC(this));
  }
  conflict_txs_.assign(r.conflict_txs_);
  return ret;
}

ObTxPart::ObTxPart()
  : id_(),
    addr_(),
    epoch_(-1),
    first_scn_(),
    last_scn_(),
    last_touch_ts_(0)
{}
ObTxPart::~ObTxPart()
{
  epoch_ = -1;
  first_scn_.reset();
  last_scn_.reset();
  last_touch_ts_ = 0;
}

int ObTxDescMgr::init(std::function<int(ObTransID&)> tx_id_allocator, const lib::ObMemAttr &mem_attr)
{
  int ret = OB_SUCCESS;
  OV(!inited_, OB_INIT_TWICE);
  OV(stoped_);
  OZ(map_.init(mem_attr));
  if (OB_SUCC(ret)) {
    tx_id_allocator_ = tx_id_allocator;
    inited_ = true;
    stoped_ = true;
  }
  int active_cnt = map_.alloc_cnt();
  TRANS_LOG(INFO, "txDescMgr.init", K(ret), K(inited_), K(stoped_), K(active_cnt));
  return ret;
}
int ObTxDescMgr::start()
{
  int ret = OB_SUCCESS;
  CK(inited_);
  CK(stoped_);
  OX(stoped_ = false);
  int active_cnt = map_.alloc_cnt();
  TRANS_LOG(INFO, "txDescMgr.start", K(inited_), K(stoped_), K(active_cnt));
  return ret;
}

class StopTxDescFunctor
{
public:
  StopTxDescFunctor(ObTransService &txs): txs_(txs) {}
  bool operator()(ObTxDesc *tx_desc)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(tx_desc) || !tx_desc->is_valid()) {
      TRANS_LOG(ERROR, "stop tx desc invalid argument", KPC(tx_desc));
    } else {
      TRANS_LOG(INFO, "stop tx desc", "tx_id", tx_desc->get_tx_id());
      if (OB_FAIL(txs_.stop_tx(*tx_desc))) {
        TRANS_LOG(ERROR, "stop tx desc fail", K(ret));
      } else {
        TRANS_LOG(INFO, "stop tx desc succeed");
      }
    }
    return true;
  }
  ObTransService &txs_;
};

class PrintTxDescFunctor
{
public:
  explicit PrintTxDescFunctor(const int64_t max_print_cnt) : max_print_cnt_(max_print_cnt) {}
  bool operator()(ObTxDesc *tx_desc)
  {
    bool bool_ret = false;
    if (OB_NOT_NULL(tx_desc) && max_print_cnt_-- > 0) {
      tx_desc->print_trace();
      bool_ret = true;
    }
    return bool_ret;
  }
  int64_t max_print_cnt_;
};

int ObTxDescMgr::stop()
{
  int ret = OB_SUCCESS;

  stoped_ = true;
  int active_cnt = map_.alloc_cnt();

  StopTxDescFunctor fn(txs_);
  if (OB_FAIL(map_.for_each(fn))) {
    TRANS_LOG(ERROR, "for each transaction desc error", KR(ret));
  }
  TRANS_LOG(INFO, "txDescMgr.stop", K(inited_), K(stoped_), K(active_cnt));
  return OB_SUCCESS;
}

int ObTxDescMgr::wait()
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_US = 100 * 1000;
  const int64_t MAX_RETRY_TIMES = 50;
  int active_cnt = 0;
  if (inited_) {
    int i = 0;
    bool done = false;
    while (!done && i++ < MAX_RETRY_TIMES) {
      active_cnt = map_.alloc_cnt();
      if (!active_cnt) {
        TRANS_LOG(INFO, "txDescMgr.wait done.");
        done = true;
        break;
      }
      TRANS_LOG(WARN, "txDescMgr.waiting.", K(active_cnt));
      ob_usleep(SLEEP_US);
    }
    if (!done) {
      ret = OB_TIMEOUT;
      TRANS_LOG(WARN, "txDescMgr.wait timeout", K(ret));
      PrintTxDescFunctor fn(128);
#ifdef ENABLE_DEBUG_LOG
      (void)map_.alloc_handle_.for_each(fn);
#else
      (void)map_.for_each(fn);
#endif
    }
  }
  TRANS_LOG(INFO, "txDescMgr.wait", K(ret), K(inited_), K(stoped_), K(active_cnt));
  return ret;
}

void ObTxDescMgr::destroy() { inited_ = false; }
int ObTxDescMgr::alloc(ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  OV(inited_, OB_NOT_INIT);
  OV(!stoped_, OB_IN_STOP_STATE);
  OZ(map_.alloc_value(tx_desc));
  OX(tx_desc->inc_ref(1));
  return ret;
}
void ObTxDescMgr::free(ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  OV(inited_, OB_NOT_INIT);
  OX(map_.free_value(tx_desc));
}
int ObTxDescMgr::add(ObTxDesc &tx_desc)
{
  int ret = OB_SUCCESS;
  ObTransID tx_id;
  OV(inited_, OB_NOT_INIT);
  OV(!stoped_, OB_IN_STOP_STATE);
  CK(!tx_desc.get_tx_id().is_valid());
  OZ(tx_id_allocator_(tx_id));
  // set_tx_id should before insert_and_get
  OX(tx_desc.set_tx_id(tx_id));
  OZ(map_.insert(tx_id, &tx_desc), tx_desc);
  // if fail revert tx_desc.tx_id_ member
  if (OB_FAIL(ret) && tx_id.is_valid()) {
    tx_desc.reset_tx_id();
  }
  OX(tx_desc.flags_.SHADOW_ = false);
  TRANS_LOG(TRACE, "txDescMgr.register trans", K(ret), K(tx_id), K(tx_desc));
  return ret;
}

int ObTxDescMgr::add_with_txid(const ObTransID &tx_id, ObTxDesc &tx_desc)
{
  int ret = OB_SUCCESS;
  ObTransID desc_tx_id = tx_desc.get_tx_id();
  if (!inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTxDescMgr not inited", K(ret));
  } else if (stoped_) {
    ret = OB_IN_STOP_STATE;
    TRANS_LOG(WARN, "ObTxDescMgr has been stopped", K(ret));
  } else if (!tx_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tx_id), K(tx_desc));
  } else if (desc_tx_id.is_valid() && desc_tx_id != tx_id) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx desc with different tx id", K(ret), K(tx_id), K(tx_desc));
  }

  if (OB_SUCC(ret)) {
    if (!desc_tx_id.is_valid()) { tx_desc.set_tx_id(tx_id); }
    if (OB_FAIL(map_.insert(tx_id, &tx_desc))) {
      TRANS_LOG(WARN, "fail to register trans", K(ret), K(tx_desc));
    }
    // if fail revert tx_desc.tx_id_ member
    if (OB_FAIL(ret) && !desc_tx_id.is_valid()) { tx_desc.reset_tx_id(); }
    if (OB_SUCC(ret) && tx_desc.flags_.SHADOW_) { tx_desc.flags_.SHADOW_ = false; }
  }
  TRANS_LOG(TRACE, "txDescMgr.register trans with txid", K(ret), KP(&tx_desc), K(tx_id),
      K(map_.alloc_cnt()));
  return ret;
}

int ObTxDescMgr::get(const ObTransID &tx_id, ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  OV(inited_, OB_NOT_INIT);
  if (OB_SUCC(ret)) {
    ret = map_.get(tx_id, tx_desc);
  }
  TRANS_LOG(TRACE, "txDescMgr.get trans", K(tx_id), KP(tx_desc));
  return ret;
}

void ObTxDescMgr::revert(ObTxDesc &tx)
{
  int ret = OB_SUCCESS;
  ObTransID tx_id = tx.get_tx_id();
  OV(inited_, OB_NOT_INIT);
  if (OB_SUCC(ret)) {
    map_.revert(&tx);
  }
  // tx_id may be invalid when tx was reused before.
  TRANS_LOG(TRACE, "txDescMgr.revert trans", K(tx_id), KP(&tx));
}

int ObTxDescMgr::remove(ObTxDesc &tx)
{
  int ret = OB_SUCCESS;
  ObTransID tx_id = tx.get_tx_id();
  TRANS_LOG(TRACE, "txDescMgr.unregister trans:", K(tx_id), KP(&tx));
  OV(inited_, OB_NOT_INIT);
  OX(map_.del(tx_id, &tx));
  OX(tx.flags_.SHADOW_ = true);
  return ret;
}

int ObTxDescMgr::acquire_tx_ref(const ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  ObTxDesc *tx_desc;
  CK(trans_id.is_valid());
  OZ(get(trans_id, tx_desc), trans_id);
  LOG_TRACE("txDescMgr.acquire tx ref", K(ret), K(trans_id), KP(tx_desc));
  return ret;
}

int ObTxDescMgr::release_tx_ref(ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(tx_desc));
  OX(revert(*tx_desc));
  LOG_TRACE("txDescMgr.release tx ref", K(ret), KP(tx_desc));
  return ret;
}

int ObTxDescMgr::iterate_tx_scheduler_stat(ObTxSchedulerStatIterator &tx_scheduler_stat_iter)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    TRANS_LOG(WARN, "ObTxDescMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    IterateTxSchedulerFunctor fn(tx_scheduler_stat_iter);
    if (OB_FAIL(map_.for_each(fn))) {
      TRANS_LOG(WARN, "for each transaction scheduler error", KR(ret));
    }
  }

  return ret;
}

//           TAKEOVER REVOKE   RESUME SWITCH_GRACEFUL
// LEADER    N        FOLLOWER N      FOLLOWER
// FOLLOWER  LEADER   FOLLOWER LEADER FOLLOWER
int TxCtxStateHelper::switch_state(const int64_t op)
{
  int ret = OB_SUCCESS;
  static const int64_t N = TxCtxRoleState::INVALID;
  static const int64_t STATE_MAP[TxCtxRoleState::MAX][TxCtxOps::MAX] = {
      {N, TxCtxRoleState::FOLLOWER, N, TxCtxRoleState::FOLLOWER},
      {TxCtxRoleState::LEADER, TxCtxRoleState::FOLLOWER, TxCtxRoleState::LEADER, TxCtxRoleState::FOLLOWER}};
  if (OB_UNLIKELY(!TxCtxOps::is_valid(op))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(op));
  } else if (OB_UNLIKELY(!TxCtxRoleState::is_valid(state_))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected state", K(state_));
  } else {
    const int64_t new_state = STATE_MAP[state_][op];
    if (OB_UNLIKELY(!TxCtxRoleState::is_valid(new_state))) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      last_state_ = state_;
      state_ = new_state;
      is_switching_ = true;
    }
  }
  return ret;
}

void TxCtxStateHelper::restore_state()
{
  if (is_switching_) {
    is_switching_ = false;
    state_ = last_state_;
  }
}

int ObTxDesc::alloc_branch_id(const int64_t count, int16_t &branch_id)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (count > MAX_BRANCH_ID_VALUE - last_branch_id_) {
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
    TRANS_LOG(WARN, "can not alloc branch_id", KR(ret), K(count), KPC(this));
  } else {
    branch_id = last_branch_id_ + 1;
    last_branch_id_ += count;
  }
  return ret;
}
void ObTxDesc::mark_part_abort(const ObTransID tx_id, const int abort_cause)
{
  ObSpinLockGuard guard(lock_);
  if (tx_id == tx_id_ && state_ < State::IN_TERMINATE && !flags_.PART_ABORTED_) {
    flags_.PART_ABORTED_ = true;
    abort_cause_ = abort_cause;
  }
}

int64_t ObTxDesc::get_coord_epoch() const
{
  int64_t epoch = -1;

  if (OB_UNLIKELY(!coord_id_.is_valid())) {
    epoch = -1;
  } else {
    ARRAY_FOREACH_NORET(commit_parts_, i) {
      const ObTxExecPart &part = commit_parts_[i];
      if (coord_id_ == part.ls_id_) {
        epoch = part.exec_epoch_;
      }
    }
  }

  return epoch;
}

// 1. clear transaction level snapshot
// 2. clear savepoints
int ObTxDesc::clear_state_for_autocommit_retry()
{
  ObSpinLockGuard guard(lock_);
  if (tx_id_.is_valid()) {
    savepoints_.reset();
    if (isolation_ == ObTxIsolationLevel::RR || isolation_ == ObTxIsolationLevel::SERIAL) {
      snapshot_version_.reset();
      snapshot_scn_.reset();
      snapshot_uncertain_bound_ = 0;
      TRANS_LOG(TRACE, "", KPC(this));
    }
  }
  return OB_SUCCESS;
}

} // transaction
} // oceanbase
#undef USING_LOG_PREFIX
