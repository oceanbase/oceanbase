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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_DEFINE_V4_
#define OCEANBASE_TRANSACTION_OB_TRANS_DEFINE_V4_

#include <cstdint>
#include <functional>
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_mask_set2.h"
#include "lib/container/ob_se_array.h"
#include "lib/list/ob_list.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/trace/ob_trace_event.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/container/ob_tuple.h"
#include "common/ob_role.h"
#include "share/ob_cluster_version.h"
#include "share/ob_ls_id.h"
#include "share/ob_light_hashmap.h"
#include "storage/tx/ob_trans_define.h"
#include "common/ob_simple_iterator.h"
#include "share/ob_common_id.h"
#include "storage/memtable/ob_row_conflict_info.h"

namespace oceanbase
{
namespace transaction
{

class ObTxSchedulerStat;

struct ObTransIDAndAddr { // deadlock needed
  OB_UNIS_VERSION(1);
public:
  ObTransIDAndAddr() = default;
  ObTransIDAndAddr(const ObTransID id, const common::ObAddr &addr) : tx_id_(id), scheduler_addr_(addr) {}
  TO_STRING_KV(K_(tx_id), K_(scheduler_addr));
  bool operator==(const ObTransIDAndAddr &rhs) const { return tx_id_ == rhs.tx_id_ &&
                                                              scheduler_addr_ == rhs.scheduler_addr_; }
  bool is_valid() const { return tx_id_.is_valid() && scheduler_addr_.is_valid(); }
  ObTransID tx_id_;
  common::ObAddr scheduler_addr_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, ObTransIDAndAddr, tx_id_, scheduler_addr_);

class ObITxCallback
{
public:
  virtual void callback(int ret) = 0;
};

template<typename L, typename R>
struct ObPair
{
  L left_; R right_;
public:
  ObPair() : left_(), right_() {}
  ObPair(const L &l, const R &r): left_(l), right_(r) {}
  ObPair& operator=(const ObPair &b)
  { left_ = b.left_; right_ = b.right_; return *this; }
  inline bool operator==(const ObPair &b) const {
    return b.left_ == left_ && b.right_ == right_;
  }
  TO_STRING_KV(K_(left), K_(right));
  NEED_SERIALIZE_AND_DESERIALIZE;
};

template<typename L, typename R>
int ObPair<L, R>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, left_, right_);
  return ret;
}
template<typename L, typename R>
int ObPair<L, R>::deserialize(const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, left_, right_);
  return ret;
}

template<typename L, typename R>
int64_t ObPair<L, R>::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, left_, right_);
  return len;
}

template<typename T, int N = 4>
class ObRefList
{
private:
  ObSEArray<T*, N> ref_list_;
public:
  T& operator [](int index) { return *ref_list_[index]; }
  const T& operator [](int index) const { return *ref_list_[index]; }
  int push_back(T &p) { return ref_list_.push_back(&p); }
  int64_t count() const { return ref_list_.count(); }
  DECLARE_TO_STRING {
    int64_t pos = 0;
    J_ARRAY_START();
    ARRAY_FOREACH_NORET(ref_list_, i) {
      pos += ref_list_[i]->to_string(buf + pos, buf_len - pos);
      J_COMMA();
    }
    J_ARRAY_END();
    return pos;
  }
};


#define OB_TX_ABORT_CAUSE_LIST                          \
  _XX(PARTICIPANT_IS_CLEAN)                             \
  _XX(TX_RESULT_INCOMPLETE)                             \
  _XX(IN_CONSIST_STATE)                                 \
  _XX(SAVEPOINT_ROLLBACK_FAIL)                          \
  _XX(IMPLICIT_ROLLBACK)                                \
  _XX(SESSION_DISCONNECT)                       /*5*/   \
  _XX(STOP)                                             \
  _XX(PARTICIPANT_STATE_INCOMPLETE)                     \
  _XX(PARTICIPANTS_SET_INCOMPLETE)                      \
  _XX(PARTICIPANT_KILLED_FORCEDLY)                      \
  _XX(PARTICIPANT_KILLED_GRACEFULLY)            /*10*/  \
  _XX(PARTICIPANT_SWITCH_FOLLOWER_FORCEDLY)             \
  _XX(PARTICIPANT_SWITCH_LEADER_DATA_INCOMPLETE)        \
  _XX(END_STMT_FAIL)                                    \
  _XX(EXPLICIT_ROLLBACK)                                \
  _XX(CREATE_SAVEPOINT_FAIL)                            \
  // used for dblink create savepoint

enum ObTxAbortCause
{
#define _XX(X) X,
OB_TX_ABORT_CAUSE_LIST
#undef _XX
};

struct ObTxAbortCauseNames {
  static char const* of(int i) {
    static const char* names[] = {
#define _XX(X) #X,
  OB_TX_ABORT_CAUSE_LIST
#undef _XX
    };
    if (i < 0) { return common::ob_error_name(i); }
    if (sizeof(names)/ sizeof(char*) <= i) { return "unknown"; }
    return names[i];
  }
};

#undef OB_TX_ABORT_CAUSE_LIST

enum class ObTxClass { USER, SYS };

enum class ObTxConsistencyType
{
  INVALID = 0,
  CURRENT_READ = 1,
  BOUNDED_STALENESS_READ = 2, // AKA. WeakRead
};

enum class ObTxIsolationLevel
{
  INVALID = -1,
  RU = 0,
  RC = 1,
  RR = 2,
  SERIAL = 3,
};

extern ObTxIsolationLevel tx_isolation_from_str(const ObString &s);

extern const ObString &get_tx_isolation_str(const ObTxIsolationLevel isolation);

enum class ObTxAccessMode
{
  INVL = -1, RW = 0, RD_ONLY = 1, STANDBY_RD_ONLY = 2
};

struct ObTxParam
{
  ObTxParam();
  bool is_valid() const;
  ~ObTxParam();
  int64_t timeout_us_;
  int64_t lock_timeout_us_;
  ObTxAccessMode access_mode_;
  ObTxIsolationLevel isolation_;
  int64_t cluster_id_;
  TO_STRING_KV(K_(cluster_id),
               K_(timeout_us),
               K_(lock_timeout_us),
               K_(access_mode),
               K_(isolation));
  OB_UNIS_VERSION(1);
};

union ObTxPartFlag
{
  int64_t flag_val_;
  struct FlagBit
  {
    unsigned int is_dup_ls_ : 1;
    bool is_clean_          : 1; // no Write happended, even rollbacked
    TO_STRING_KV(K_(is_dup_ls), K_(is_clean));
  } flag_bit_;

  ObTxPartFlag() { reset(); }

  void reset()
  {
    flag_val_ = 0;
  }

  bool is_dup_ls() const { return flag_bit_.is_dup_ls_ == 1; }
  void set_dup_ls() { flag_bit_.is_dup_ls_ = 1; }
  bool is_clean() const { return flag_bit_.is_clean_; }
  void set_clean() { flag_bit_.is_clean_ = true; }
  void set_dirty() { flag_bit_.is_clean_ = false; }
};

struct ObTxPart
{
  static const int64_t EPOCH_UNKNOWN = -1;
  static const int64_t EPOCH_DEAD = -2;
  ObTxPart();
  ~ObTxPart();
  share::ObLSID id_;             // identifier, the logstream
  ObAddr addr_;           // its latest address
  int64_t epoch_;         // used to judge a ctx not revived
  ObTxSEQ first_scn_;      // used to judge a ctx is clean in scheduler view
  ObTxSEQ last_scn_;       // used to get rollback savepoint set
  int64_t last_touch_ts_; // used to judge a ctx retouched after a time point
  ObTxPartFlag flag_;     // used to describe some special attributes of a participant
  bool operator==(const ObTxPart &rhs) const { return id_ == rhs.id_ && addr_ == rhs.addr_; }
  bool operator!=(const ObTxPart &rhs) const { return !operator==(rhs); }
  bool is_clean() const { return flag_.is_clean(); }
  bool is_without_valid_write() const { return !first_scn_.is_valid() || last_scn_ < first_scn_; }
  bool is_without_ctx() const { return is_without_ctx(epoch_); }
  static bool is_without_ctx(int64_t epoch) { return EPOCH_DEAD == epoch; }
  TO_STRING_KV(K_(id), K_(addr), K_(epoch), K_(first_scn), K_(last_scn), K_(last_touch_ts));
  OB_UNIS_VERSION(1);
};

typedef ObSEArray<ObTxPart, 4> ObTxPartList;
typedef ObRefList<ObTxPart, 4> ObTxPartRefList;
typedef ObPair<share::ObLSID, int64_t> ObTxLSEpochPair;

// internal core snapshot for read data
class ObTxSnapshot
{
  friend class ObTxReadSnapshot;
  friend class ObTransService;
public:
  share::SCN version_;
  ObTransID tx_id_;
  ObTxSEQ scn_;
  bool elr_; // whether allowed read elr
public:
  TO_STRING_KV(K_(version), K_(tx_id), K_(scn));
  ObTxSnapshot();
  ObTxSnapshot(const share::SCN &version);
  ~ObTxSnapshot();
  void reset();
  ObTxSnapshot &operator=(const ObTxSnapshot &r);
  bool is_valid() const { return version_.is_valid(); }
  const share::SCN &version() const { return version_; }
  const ObTransID &tx_id() const { return tx_id_; }
  void set_tx_id(const ObTransID &tx_id) { tx_id_ = tx_id; }
  const ObTxSEQ &tx_seq() const { return scn_; }
  void set_elr(const bool elr) { elr_ = elr; }
  bool is_elr() const { return elr_; }
  OB_UNIS_VERSION(1);
};

// snapshot used to consistency read
class ObTxReadSnapshot
{
  friend class ObTransService;
public:
  bool valid_;              // used by cursor check snapshot state
  bool committed_;          // used by cursor check snapshot state
  ObTxSnapshot core_;
  enum class SRC {
    INVL = 0,
    GLOBAL = 1,             // normal snapshot, external consistency complied
    LS = 2,                 // only access one logstream
    WEAK_READ_SERVICE = 3,  // do a bounded stale read, without linearizable consistency
    SPECIAL = 4,            // user specify
    NONE = 5,               // won't read, global snapshot not required
  } source_;
  share::ObLSID snapshot_lsid_;    // for source_ = LOCAL                                  //
  common::ObRole snapshot_ls_role_; // for source_ = LS, only can be used for dup_table with a
                                    // max_commit_ts from the follower
  ObAddr snapshot_acquire_addr_;    // snapshot version acquired from which server
  int64_t uncertain_bound_; // for source_ GLOBAL
  ObSEArray<ObTxLSEpochPair, 1> parts_;
public:
  void init_weak_read(const share::SCN snapshot);
  void init_special_read(const share::SCN snapshot);
  void init_none_read() { valid_ = true; source_ = SRC::NONE; }
  void init_ls_read(const share::ObLSID &ls_id, const ObTxSnapshot &core);
  void specify_snapshot_scn(const share::SCN snapshot);
  void wait_consistency();
  const char* get_source_name() const;
  const ObTxSnapshot &snapshot() const { return core_; }
  const share::SCN &version() const { return core_.version(); }
  const ObTransID &tx_id() const { return core_.tx_id(); }
  void set_tx_id(const ObTransID &tx_id) { core_.set_tx_id(tx_id); }
  const ObTxSEQ &tx_seq() const { return core_.tx_seq(); }
  bool is_weak_read() const { return SRC::WEAK_READ_SERVICE == source_; };
  bool is_none_read() const { return SRC::NONE == source_; }
  bool is_special() const { return SRC::SPECIAL == source_; }
  bool is_ls_snapshot() const { return SRC::LS == source_; }
  bool is_valid() const { return valid_; }
  void invalid() { valid_ = false; }
  bool is_committed() const { return committed_; }
  int format_source_for_display(char *buf, const int64_t buf_len) const;
  const ObAddr get_snapshot_acquire_addr() const { return snapshot_acquire_addr_; }
  void reset();
  int assign(const ObTxReadSnapshot &);
  void try_set_read_elr();
  bool read_elr() const { return core_.is_elr(); }
  /**
   * only used for lob, other situation DONOT use
   *
   * special serialize interface for lob to avoid lob locator too large
   */
  int serialize_for_lob(const share::ObLSID &ls_id, SERIAL_PARAMS) const;
  int deserialize_for_lob(DESERIAL_PARAMS);
  int64_t get_serialize_size_for_lob(const share::ObLSID &ls_id) const;
  /**
   * deprecated interface, DONOT use !
   *
   * only used for lob, other situation DONOT use
   *
   * offline ddl with lob can only get ObTxSnapshot
   * so provide one interface to build ObTxReadSnapshot from ObTxSnapshot
   * master no need use this
   */
  int build_snapshot_for_lob(const ObTxSnapshot &core, const share::ObLSID &ls_id);
  /**
   * deprecated interface, DONOT use !
   *
   * only used for lob, other situation DONOT use
   *
   * in upgarge, old lob locator will use ObMemLobTxInfo store tx info
   * so provide one interface to build ObTxReadSnapshot from ObMemLobTxInfo
   */
  int build_snapshot_for_lob(
      const int64_t snapshot_version,
      const int64_t snapshot_tx_id,
      const int64_t snapshot_seq,
      const share::ObLSID &ls_id);
  /**
   * only used for lob, other situation DONOT use
   *
   * determine whether the current snapshot is within a transaction
   *
   * Return:
   * true  - not in tx
   */
  bool is_not_in_tx_snapshot() const
  {
    return ! core_.tx_id_.is_valid() && ! core_.scn_.is_valid() && core_.version_.is_valid();
  }
  /**
   * refresh snapshot's tx_seq_no to current time
   */
  int refresh_seq_no(const int64_t tx_seq_base);

  void convert_to_out_tx();

  ObTxReadSnapshot();
  ~ObTxReadSnapshot();
  TO_STRING_KV(KP(this),
               K_(valid),
               K_(source),
               K_(core),
               K_(uncertain_bound),
               K_(snapshot_lsid),
               K_(snapshot_ls_role),
               K_(snapshot_acquire_addr),
               K_(parts),
               K_(committed));
  OB_UNIS_VERSION(1);
  DISABLE_COPY_ASSIGN(ObTxReadSnapshot);
};

class ObTxSavePoint
{
  friend class ObTransService;
  friend class ObTxDesc;
private:
  enum class T { INVL= 0, SAVEPOINT= 1, SNAPSHOT= 2, STASH= 3 } type_;
  ObTxSEQ scn_;
  /* The savepoint should be unique to the session,
    and the session id is required to distinguish the
    savepoint for the multi-branch scenario of xa */
  uint32_t session_id_;
  /*
    used by XA, synchronize savepoint should exclude internal savepoint
    (eg. 'PL IMPLICIT_SAVEPOINT')
  */
  bool user_create_;
  union {
    ObTxReadSnapshot *snapshot_;
    common::ObFixedLengthString<128> name_;
  };
public:
  ObTxSavePoint();
  ~ObTxSavePoint();
  ObTxSavePoint(const ObTxSavePoint &s);
  ObTxSavePoint &operator=(const ObTxSavePoint &a);
  bool operator==(const ObTxSavePoint &a) const;
  void release();
  void rollback();
  int init(const ObTxSEQ &scn,
           const ObString &name,
           const uint32_t session_id,
           const bool user_create,
           const bool stash = false);
  void init(ObTxReadSnapshot *snapshot);
  bool is_savepoint() const { return type_ == T::SAVEPOINT || type_ == T::STASH; }
  bool is_snapshot() const { return type_ == T::SNAPSHOT; }
  bool is_stash() const { return type_ == T::STASH; }
  bool is_user_savepoint() const { return type_ == T::SAVEPOINT && user_create_; }
  ObString get_savepoint_name() const { return name_.str(); }
  DECLARE_TO_STRING;
  OB_UNIS_VERSION(1);
};

typedef ObSEArray<ObTxSavePoint, 4> ObTxSavePointList;

class ObTxExecResult
{
  friend class ObTransService;
  friend class ObTxDesc;
  OB_UNIS_VERSION(1);
  TransModulePageAllocator allocator_;
  bool incomplete_; // TODO: (yunxing.cyx) remove, required before sql use new API
  share::ObLSArray touched_ls_list_;
  ObTxPartList parts_;
  ObSArray<ObTransIDAndAddr> conflict_txs_; // FARM COMPAT WHITELIST for cflict_txs_
  ObSArray<storage::ObRowConflictInfo> conflict_info_array_;
public:
  ObTxExecResult();
  ~ObTxExecResult();
  void reset();
  TO_STRING_KV(K_(incomplete), K_(parts), K_(touched_ls_list), K_(conflict_txs));
  void set_incomplete() {
    TRANS_LOG(TRACE, "tx result incomplete:", KP(this));
    incomplete_ = true;
  }
  int merge_cflict_txs(const common::ObIArray<ObTransIDAndAddr> &txs);
  bool is_incomplete() const { return incomplete_; }
  int add_touched_ls(const share::ObLSID ls);
  int add_touched_ls(const ObIArray<share::ObLSID> &ls_list);
  const share::ObLSArray &get_touched_ls() const { return touched_ls_list_; }
  int merge_result(const ObTxExecResult &r);
  int assign(const ObTxExecResult &r);
  const ObSArray<ObTransIDAndAddr> &get_conflict_txs() const { return conflict_txs_; }
  DISABLE_COPY_ASSIGN(ObTxExecResult);
};

class RollbackMaskSet
{
public:
  RollbackMaskSet() : rollback_parts_(NULL) {}
  int init(share::ObCommonID tx_msg_id, ObTxRollbackParts &parts) {
    ObSpinLockGuard guard(lock_);
    tx_msg_id_ = tx_msg_id;
    rollback_parts_ = &parts;
    return mask_set_.init(&parts);
  }
  int get_not_mask(ObTxRollbackParts &remain) {
    ObSpinLockGuard guard(lock_);
    return mask_set_.get_not_mask(remain);
  }
  bool is_mask(const ObTxExecPart &part) {
    ObSpinLockGuard guard(lock_);
    return mask_set_.is_mask(part);
  }
  int mask(const ObTxExecPart &part) {
    ObSpinLockGuard guard(lock_);
    return mask_set_.mask(part);
  }
  int unmask(const ObTxExecPart &part) {
    ObSpinLockGuard guard(lock_);
    return mask_set_.unmask(part);
  }
  bool is_all_mask() {
    ObSpinLockGuard guard(lock_);
    return mask_set_.is_all_mask();
  }
  share::ObCommonID get_tx_msg_id() const {
    return tx_msg_id_;
  }
  void reset() {
    ObSpinLockGuard guard(lock_);
    tx_msg_id_.reset();
    rollback_parts_ = NULL;
    mask_set_.reset();
  }
  int merge_part(const share::ObLSID add_ls_id,
                 const int64_t exec_epoch,
                 const int64_t transfer_epoch);
  int find_part(const share::ObLSID ls_id,
                const int64_t orig_epoch,
                const int64_t transfer_epoch,
                ObTxExecPart &part);
private:
  ObSpinLock lock_;
  share::ObCommonID tx_msg_id_;
  ObTxRollbackParts *rollback_parts_;
  common::ObMaskSet2<ObTxExecPart> mask_set_;
};

class ObTxDesc final : public share::ObLightHashLink<ObTxDesc>
{
  static constexpr const char *OP_LABEL = "TX_DESC_VALUE";
  static constexpr int64_t MAX_RESERVED_CONFLICT_TX_NUM = 30;
  friend class ObTransService;
  friend class ObTxDescMgr;
  friend class ObPartTransCtx;
  friend class StopTxDescFunctor;
  friend class ObTxStmtInfo;
  friend class IterateTxSchedulerFunctor;
  friend class ObTxnFreeRouteCtx;
  OB_UNIS_VERSION(1);
protected:
  uint64_t tenant_id_;        // FIXME: removable
  // Identify the ownership of data when the A database and
  // the B database synchronize data with each other
  int64_t cluster_id_;
  ObTraceInfo trace_info_;
  uint64_t cluster_version_;  // compatible handle when upgrade
  int64_t seq_base_;          // tx_seq's base value, use to calculate absolute value of tx_seq
  ObTxConsistencyType tx_consistency_type_; // transaction level consistency_type : strong or bounded read

  common::ObAddr addr_;                // where we site
  ObTransID tx_id_;                    // identifier
  ObXATransID xid_;                    // xa info if participant in XA
  bool xa_tightly_couple_;              // xa mode is tightly couple or loosely couple TODO: setup in xa_service
  common::ObAddr xa_start_addr_;       // the xa start's address
  ObTxIsolationLevel isolation_;       // isolation level
  ObTxAccessMode access_mode_;         // READ_ONLY | READ_WRITE
  share::SCN snapshot_version_;           // snapshot for RR | SERIAL Isolation
  int64_t snapshot_uncertain_bound_;   // uncertain bound of @snapshot_version_
  ObTxSEQ snapshot_scn_;               // the time of acquire @snapshot_version_
  uint32_t sess_id_;                   // sesssion id of txn start, for XA it is XA_START session id
  uint32_t assoc_sess_id_;             // the session which associated with
  ObGlobalTxType global_tx_type_;      // global trans type, i.e., xa or dblink

  uint64_t op_sn_;                     // Tx level operation sequence No

  enum class State : int               // State of Tx
  {
    INVL,
    IDLE,               // created
    ACTIVE,             // explicit started
    IMPLICIT_ACTIVE,    // implicit started
    ROLLBACK_SAVEPOINT, // rolling back to savepoint
    IN_TERMINATE,       // committing, aborting
    ABORTED,            // internal rolled back
    ROLLED_BACK,        // rolled back
    COMMIT_TIMEOUT,     // commit timeouted
    COMMIT_UNKNOWN,     // commit complted but result unknown, either committed or aborted
    COMMITTED,          // committed
    SUB_PREPARING,      // XA prepare started
    SUB_PREPARED,       // XA prepare response received
    SUB_COMMITTING,     // XA commit started
    SUB_COMMITTED,      // XA commit response received
    SUB_ROLLBACKING,    // XA rollback started
    SUB_ROLLBACKED,     // XA rollback response received
  } state_;

  union FLAG                         // flags
  {
    uint64_t v_;
    struct FOR_FIXED_SER_VAL {
      uint64_t v_;
      TO_STRING_KV(K_(v));
      NEED_SERIALIZE_AND_DESERIALIZE;
    } for_serialize_v_;
    struct COMPAT_FOR_TX_ROUTE {
      uint64_t v_;
      uint64_t get_serialize_v_() const;
      TO_STRING_KV(K_(v));
      NEED_SERIALIZE_AND_DESERIALIZE;
    } compat_for_tx_route_;
    struct COMPAT_FOR_EXEC {
      uint64_t v_;
      uint64_t get_serialize_v_() const;
      TO_STRING_KV(K_(v));
      NEED_SERIALIZE_AND_DESERIALIZE;
    } compat_for_exec_;
    struct
    {
      bool EXPLICIT_:1;              // txn is explicted start
      bool SHADOW_:1;                // this tx desc is a shadow copy, is not registered with tx_desc_mgr
      bool REPLICA_:1;               // a replica of primary/original, its state is transient, without whole lifecyle
      bool TRACING_:1;               // tracing the Tx
      bool INTERRUPTED_: 1;          // a single for blocking operation
      bool RELEASED_: 1;             // after released, commit can give up
      bool BLOCK_: 1;                // tx is blocking within some loop
      bool PARTS_INCOMPLETE_: 1;     // participants set incomplete (trans must abort)
      bool PART_EPOCH_MISMATCH_: 1;  // participant's born epoch mismatched
      bool WITH_TEMP_TABLE_: 1;      // with txn level temporary table
      bool DEFER_ABORT_: 1;          // need do abort in txn start node
      bool PART_ABORTED_: 1;         // some participant is aborted or in delay-abort state (trans must abort)
    };
    void switch_to_idle_();
    FLAG update_with(const FLAG &flag);
  } flags_;
  static_assert(sizeof(FLAG) == sizeof(int64_t), "ObTxDesc::FLAG should sizeof(int64_t)");
  union STATE_CHANGE_FLAG
  {
    uint8_t v_;
    struct {
      bool STATIC_CHANGED_:1;
      bool DYNAMIC_CHANGED_:1;
      bool PARTS_CHANGED_:1;
      bool EXTRA_CHANGED_:1;
    };
    void reset() { v_ = 0;}
    void mark_all() { v_ = 0xFF; }
  } state_change_flags_;

  int64_t alloc_ts_;                 // time of allocated
  int64_t active_ts_;                // time of ACTIVE | IMPLICIT_ACTIVE
  int64_t timeout_us_;               // tx parameters from ObTxParam
  int64_t lock_timeout_us_;          // lock conflict wait timeout in micorsecond
  int64_t expire_ts_;                // tick when ACTIVE
  int64_t commit_ts_;                // COMMIT start time
  int64_t finish_ts_;                // COMMIT/ABORT finish time

  ObTxSEQ active_scn_;               // logical time of ACTIVE | IMPLICIT_ACTIVE
  ObTxSEQ min_implicit_savepoint_;   // mininum of implicit savepoints
  int16_t last_branch_id_;           // branch_id allocator, reset when stmt start
  ObTxPartList parts_;               // participant list
  ObTxSavePointList savepoints_;     // savepoints established
  // conflict_txs_ is used to store conflict trans id when try acquire row lock failed(meet lock conflict)
  // this information will used to detect deadlock
  // conflict_txs_ is valid when transaction is not executed on local
  // on scheduler, conflict_txs_ merges all participants executed results on remote
  // on participant, conflict_txs_ temporary stores conflict information, and will be read by upper layers, bring back to scheduler
  ObSArray<ObTransIDAndAddr> conflict_txs_; // FARM COMPAT WHITELIST for cflict_txs_
  ObSArray<storage::ObRowConflictInfo> conflict_info_array_;

  // used during commit
  share::ObLSID coord_id_;           // coordinator ID
  int64_t commit_expire_ts_;         // commit operation deadline
  ObTxCommitParts commit_parts_;    // participants to do commit
  share::SCN commit_version_;        // Tx commit version
  int commit_out_;                   // the commit result
  int commit_times_;                 // times of sent commit request
  share::SCN commit_start_scn_;      // scn of starting to commit
  /* internal abort cause */
  int16_t abort_cause_;              // Tx Aborted cause
  bool unused_can_elr_;
private:
  ObSEArray<uint64_t, 1> modified_tables_; // used in cursor test read uncommitted
private:
  // FOLLOWING are runtime auxiliary fields
  mutable ObSpinLock lock_;
  ObSpinLock commit_cb_lock_;       // protect commit_cb_ field
  ObITxCallback *commit_cb_;        // async commit callback
  int64_t cb_tid_;                  // commit callback thread id
  int64_t exec_info_reap_ts_;       // the time reaping incremental tx exec info
  RollbackMaskSet brpc_mask_set_;   // used in message driven savepoint rollback
  ObTransCond rpc_cond_;            // used in message driven savepoint rollback

  ObTxTimeoutTask commit_task_;     // commit retry task
  ObXACtx *xa_ctx_;                 // xa context
  ObTransTraceLog tlog_;
#ifdef ENABLE_DEBUG_LOG
  struct DLink {
    DLink(): next_(this), prev_(this) {}
    void reset() { next_ = this; prev_ = this; }
    void insert(DLink &n) {
      next_->prev_ = &n;
      n.next_ = next_;
      n.prev_ = this;
      next_ = &n;
    }
    void remove() {
      next_->prev_ = prev_;
      prev_->next_ = next_;
    }
    DLink *next_;
    DLink *prev_;
  } alloc_link_;
#endif
  static constexpr int16_t MAX_BRANCH_ID_VALUE = ~(1 << 15) & 0xFFFF; // 15bits
  static constexpr int64_t MAX_TRANS_TIMEOUT_US = INT64_MAX - 1_day;
private:
  /* these routine should be called by txn-service only to avoid corrupted state */
  void reset();
  void set_tx_id(const ObTransID &tx_id);
  void reset_tx_id();
  // udpate clean part's unknown field
  int update_clean_part(const share::ObLSID &id,
                        const int64_t epoch,
                        const ObAddr &addr);
  int add_clean_part_if_absent(const share::ObLSID &id,
                               const int64_t epoch,
                               const ObAddr &addr,
                               const bool is_dup);
  int update_part(ObTxPart &p);
  int update_parts(const share::ObLSArray &parts);
  int switch_to_idle();
  int set_commit_cb(ObITxCallback *cb);
  bool execute_commit_cb();
private:
  int update_part_(ObTxPart &p, const bool append = true, const bool check_only_if_exist = false);
  int add_conflict_tx_(const ObTransIDAndAddr &conflict_tx);
  int merge_conflict_txs_(const ObIArray<ObTransIDAndAddr> &conflict_ids);
  int update_parts_(const ObTxPartList &list);
  void post_rb_savepoint_(ObTxPartRefList &parts, const ObTxSEQ &savepoint);
  void implicit_start_tx_();
  bool acq_commit_cb_lock_if_need_();
  bool has_extra_state_() const;
  bool in_tx_or_has_extra_state_() const;
  bool in_tx_for_free_route_();
  void print_trace_() const;
public:
  ObTxDesc();
  ~ObTxDesc();
  TO_STRING_KV(KP(this),
               K_(tx_id),
               K_(state),
               K_(addr),
               K_(tenant_id),
               "session_id", sess_id_,
               "assoc_session_id", assoc_sess_id_,
               "xid", PC((!xid_.empty() ? &xid_ : (ObXATransID*)nullptr)),
               "xa_mode", xid_.empty() ? "" : (xa_tightly_couple_ ? "tightly" : "loosely"),
               K_(xa_start_addr),
               K_(access_mode),
               K_(tx_consistency_type),
               K_(isolation),
               K_(snapshot_version),
               K_(snapshot_scn),
               K_(active_scn),
               K_(op_sn),
               K_(alloc_ts),
               K_(active_ts),
               K_(commit_ts),
               K_(finish_ts),
               K_(timeout_us),
               K_(lock_timeout_us),
               K_(expire_ts),
               K_(coord_id),
               K_(parts),
               K_(exec_info_reap_ts),
               K_(commit_version),
               K_(commit_times),
               KP_(commit_cb),
               K_(cluster_id),
               K_(cluster_version),
               K_(seq_base),
               K_(flags_.SHADOW),
               K_(flags_.INTERRUPTED),
               K_(flags_.BLOCK),
               K_(flags_.REPLICA),
               K_(conflict_txs),
               K_(abort_cause),
               K_(commit_expire_ts),
               K(commit_task_.is_registered()),
               K_(modified_tables),
               K_(ref));
  bool support_branch() const { return seq_base_ > 0; }
  // used by SQL alloc branch_id refer the min branch_id allowed
  // because branch_id bellow this is reserved for internal use
  static int branch_id_offset() { return MAX_CALLBACK_LIST_COUNT; }
  static bool is_alloced_branch_id(int branch_id) { return branch_id >= branch_id_offset(); }
  int alloc_branch_id(const int64_t count, int16_t &branch_id);
  int fetch_conflict_txs(ObIArray<ObTransIDAndAddr> &array);
  void reset_conflict_txs()
  { ObSpinLockGuard guard(lock_); conflict_txs_.reset(); }
  int add_conflict_tx(const ObTransIDAndAddr conflict_tx);
  int merge_conflict_txs(const ObIArray<ObTransIDAndAddr> &conflict_ids);
  bool has_conflict_txs() const { return conflict_txs_.count() > 0; }
  bool contain(const ObTransID &trans_id) const { return tx_id_ == trans_id; } /*used by TransHashMap*/
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_cluster_id(uint64_t cluster_id) { cluster_id_ = cluster_id; }
  uint64_t get_cluster_id() const { return cluster_id_; }
  uint32_t get_session_id() const { return sess_id_; }
  uint32_t get_assoc_session_id() const { return assoc_sess_id_; }
  ObAddr get_addr() const { return addr_; }
  uint64_t get_cluster_version() const { return cluster_version_; }
  ObTxConsistencyType get_tx_consistency_type() const { return tx_consistency_type_; }
  ObTxIsolationLevel get_isolation_level() const { return isolation_; }
  const ObTransID &tid() const { return tx_id_; }
  bool is_valid() const { return !is_in_tx() || tx_id_.is_valid(); }
  ObTxAccessMode get_tx_access_mode() const { return access_mode_; }
  bool is_rdonly() const { return access_mode_ == ObTxAccessMode::RD_ONLY || access_mode_ == ObTxAccessMode::STANDBY_RD_ONLY; }
  bool is_clean() const { return parts_.empty(); }
  bool is_shadow() const  { return flags_.SHADOW_; }
  bool is_explicit() const { return flags_.EXPLICIT_; }
  void set_with_temporary_table() { flags_.WITH_TEMP_TABLE_ = true; }
  bool with_temporary_table() const { return flags_.WITH_TEMP_TABLE_; }
  int64_t get_op_sn() const { return op_sn_; }
  void inc_op_sn(const uint64_t num = 1) { state_change_flags_.DYNAMIC_CHANGED_ = true; ATOMIC_AAF(&op_sn_, num); }
  share::SCN get_commit_version() const { return commit_version_; }
  bool contain_savepoint(const ObString &sp);
  bool is_tx_end() {
    return is_committed() || is_rollbacked();
  }
  bool is_committing() {
    return state_ == State::IN_TERMINATE
      || state_ == State::SUB_PREPARING
      || state_ == State::SUB_COMMITTING
      || state_ == State::SUB_ROLLBACKING;
  }
  bool is_terminated() {
    return state_ == State::ABORTED || is_tx_end();
  }
  bool is_committed() {
    return state_ == State::COMMITTED
      || state_ == State::COMMIT_TIMEOUT
      || state_ == State::COMMIT_UNKNOWN
      || state_ == State::SUB_COMMITTED;
  }
  bool is_rollbacked() {
    return state_ == State::ROLLED_BACK
      || state_ == State::SUB_ROLLBACKED;
  }
  bool is_commit_unsucc() {
    return state_ == State::COMMIT_TIMEOUT
      || state_ == State::COMMIT_UNKNOWN
      || state_ == State::ROLLED_BACK;
  }
  bool is_sub2pc() {
    return state_ >= State::SUB_PREPARING
      && state_ <= State::SUB_ROLLBACKED;
  }
  bool is_aborted() const { return state_ == State::ABORTED; }
  bool is_tx_timeout() { return expire_ts_ > 0 && ObClockGenerator::getClock() > expire_ts_; }
  bool is_tx_commit_timeout() { return commit_expire_ts_ > 0 && ObClockGenerator::getClock() > commit_expire_ts_;}
  void set_xa_ctx(ObXACtx *xa_ctx);
  ObXACtx *get_xa_ctx() { return xa_ctx_; }
  void set_xid(const ObXATransID &xid) { xid_ = xid; }
  void set_sessid(const uint32_t session_id) { sess_id_ = session_id; }
  void set_assoc_sessid(const uint32_t session_id) { assoc_sess_id_ = session_id; }
  const ObXATransID &get_xid() const { return xid_; }
  void reset_xid() { xid_.reset(); }
  bool is_xa_trans() const { return !xid_.empty(); }
  bool is_xa_tightly_couple() const { return xa_tightly_couple_; }
  void set_xa_start_addr(common::ObAddr &addr) { xa_start_addr_ = addr; }
  common::ObAddr xa_start_addr() const { return xa_start_addr_; }
  void reset_for_xa() { xa_ctx_ = NULL; }
  int trans_deep_copy(const ObTxDesc &x);
  int64_t get_active_ts() const { return active_ts_; }
  int64_t get_expire_ts() const;
  int64_t get_tx_lock_timeout() const { return lock_timeout_us_; }
  bool is_in_tx() const { return state_ > State::IDLE; }
  bool is_tx_active() const { return state_ >= State::ACTIVE && state_ < State::IN_TERMINATE; }
  void print_trace();
  void dump_and_print_trace();
  bool in_tx_or_has_extra_state() const;
  bool in_tx_for_free_route();
  const ObTransID &get_tx_id() const { return tx_id_; }
  ObITxCallback *get_end_tx_cb() { return commit_cb_; }
  void reset_end_tx_cb() { commit_cb_ = NULL; }
  const ObString &get_tx_state_str() const;
  int merge_exec_info_with(const ObTxDesc &other);
  int get_inc_exec_info(ObTxExecResult &exec_info);
  int add_exec_info(const ObTxExecResult &exec_info);
  bool has_implicit_savepoint() const;
  void add_implicit_savepoint(const ObTxSEQ savepoint);
  void release_all_implicit_savepoint();
  void release_implicit_savepoint(const ObTxSEQ savepoint);
  ObTransTraceLog &get_tlog() { return tlog_; }
  bool is_xa_terminate_state_() const;
  // for dblink
  ObGlobalTxType get_global_tx_type(const ObXATransID &xid) const;
  void set_global_tx_type(const ObGlobalTxType global_tx_type)
  { global_tx_type_ = global_tx_type; }
  bool need_rollback() { return state_ == State::ABORTED; }
  int64_t get_timeout_us() const { return timeout_us_; }
  share::SCN get_snapshot_version() { return snapshot_version_; }
  ObITxCallback *cancel_commit_cb();
  int get_parts_copy(ObTxPartList &copy_parts);
  int get_savepoints_copy(ObTxSavePointList &copy_savepoints);
  // free route
#define DEF_FREE_ROUTE_DECODE_(name)                                    \
  int encode_##name##_state(char *buf, const int64_t len, int64_t &pos); \
  int decode_##name##_state(const char *buf, const int64_t len, int64_t &pos); \
  int64_t name##_state_encoded_length();                                \
  static int display_##name##_state(const char *buf, const int64_t len, int64_t &pos); \
  int encode_##name##_state_for_verify(char *buf, const int64_t len, int64_t &pos); \
  int64_t name##_state_encoded_length_for_verify();                     \
  int64_t est_##name##_size__()
#define DEF_FREE_ROUTE_DECODE(name) DEF_FREE_ROUTE_DECODE_(name)
LST_DO(DEF_FREE_ROUTE_DECODE, (;), static, dynamic, parts, extra);
#undef DEF_FREE_ROUTE_DECODE
#undef DEF_FREE_ROUTE_DECODE_
  int64_t estimate_state_size();
  bool is_static_changed() { return state_change_flags_.STATIC_CHANGED_; }
  bool is_dynamic_changed() { return state_ > State::IDLE && state_change_flags_.DYNAMIC_CHANGED_; }
  bool is_parts_changed() { return state_ > State::IDLE && state_change_flags_.PARTS_CHANGED_; };
  bool is_extra_changed() { return state_change_flags_.EXTRA_CHANGED_; };
  void set_explicit() { flags_.EXPLICIT_ = true; }
  void clear_interrupt() { flags_.INTERRUPTED_ = false; }
  void mark_part_abort(const ObTransID tx_id, const int abort_cause);
  int64_t get_coord_epoch() const;
  int get_and_inc_tx_seq(const int16_t branch, const int N, ObTxSEQ &tx_seq) const;
  ObTxSEQ inc_and_get_tx_seq(int16_t branch) const;
  ObTxSEQ get_tx_seq(int64_t seq_abs = 0) const;
  ObTxSEQ get_min_tx_seq() const;
  int clear_state_for_autocommit_retry();
  int64_t get_seq_base() const { return seq_base_; }
  int add_modified_tables(const ObIArray<uint64_t> &tables);
  bool has_modify_table(const uint64_t table_id) const;
  DISABLE_COPY_ASSIGN(ObTxDesc);
  bool is_all_parts_clean() const;
  bool is_all_parts_without_valid_write() const;
};

// Is used to store and travserse all TxScheduler's Stat information;
typedef common::ObSimpleIterator<ObTxSchedulerStat,
        ObModIds::OB_TRANS_VIRTUAL_TABLE_TRANS_STAT, 16> ObTxSchedulerStatIterator;


class ObTxDescMgr final
{
public:
  ObTxDescMgr(ObTransService &txs): inited_(false), stoped_(true), tx_id_allocator_(), txs_(txs) {}
 ~ObTxDescMgr() { inited_ = false; stoped_ = true; }
  int init(std::function<int(ObTransID&)> tx_id_allocator, const lib::ObMemAttr &mem_attr);
  int start();
  int stop();
  int wait();
  void destroy();
  int alloc(ObTxDesc *&tx_desc);
  void free(ObTxDesc *tx_desc);
  int add(ObTxDesc &tx_desc);
  int add_with_txid(const ObTransID &tx_id, ObTxDesc &tx_desc);
  int get(const ObTransID &tx_id, ObTxDesc *&tx_desc);
  void revert(ObTxDesc &tx);
  int remove(ObTxDesc &tx);
  int acquire_tx_ref(const ObTransID &trans_id);
  int release_tx_ref(ObTxDesc *tx_desc);
  int64_t get_alloc_count() const { return map_.alloc_cnt(); }
  int64_t get_total_count() const { return map_.count(); }
  int iterate_tx_scheduler_stat(ObTxSchedulerStatIterator &tx_scheduler_stat_iter);
  struct {
    bool inited_: 1;
    bool stoped_: 1;
  };
  class ObTxDescAlloc
  {
  public:
    ObTxDescAlloc(): alloc_cnt_(0)
#ifdef ENABLE_DEBUG_LOG
                   , lk_()
                   , list_()
#endif
   {}
#ifdef ENABLE_DEBUG_LOG
    ~ObTxDescAlloc()
    {
      ObSpinLockGuard guard(lk_);
      list_.remove();
    }
#endif
   ObTxDesc* alloc_value()
   {
     ATOMIC_INC(&alloc_cnt_);
     ObTxDesc *it = op_alloc_v2(ObTxDesc);
#ifdef ENABLE_DEBUG_LOG
     if (OB_NOT_NULL(it)) {
       ObSpinLockGuard guard(lk_);
       list_.insert(it->alloc_link_);
     }
#endif
      return it;
    }
    void free_value(ObTxDesc *v)
    {
      if (NULL != v) {
        ATOMIC_DEC(&alloc_cnt_);
#ifdef ENABLE_DEBUG_LOG
        ObSpinLockGuard guard(lk_);
        v->alloc_link_.remove();
#endif
        op_free_v2(v);
      }
    }
    static void force_free(ObTxDesc *v)
    {
      op_free_v2(v);
    }
    int64_t get_alloc_cnt() const { return ATOMIC_LOAD(&alloc_cnt_); }
#ifdef ENABLE_DEBUG_LOG
    template<typename Function>
    int for_each(Function &fn)
    {
      int ret = OB_SUCCESS;
      ObSpinLockGuard guard(lk_);
      ObTxDesc::DLink *n = list_.next_;
      while(n != &list_) {
        ObTxDesc *tx = CONTAINER_OF(n, ObTxDesc, alloc_link_);
        ret = fn(tx);
        n = n->next_;
      }
      return ret;
    }
#endif
    private:
      int64_t alloc_cnt_;
#ifdef ENABLE_DEBUG_LOG
      ObSpinLock lk_;
      ObTxDesc::DLink list_;
#endif
  };
  static void force_release(ObTxDesc &tx) {
    if (tx.dec_ref(1) == 0) {
      ObTxDescAlloc::force_free(&tx);
    }
  }
  share::ObLightHashMap<ObTransID, ObTxDesc, ObTxDescAlloc, common::SpinRWLock, 1 << 16 /*bucket_num*/> map_;
  std::function<int(ObTransID&)> tx_id_allocator_;
  ObTransService &txs_;
};

class ObTxInfo
{
  friend class ObTransService;
  friend class ObXACtx;
  OB_UNIS_VERSION(1);
protected:
  uint64_t tenant_id_;
  int64_t cluster_id_;
  uint64_t cluster_version_;
  int64_t seq_base_;
  common::ObAddr addr_;
  ObTransID tx_id_;
  ObTxIsolationLevel isolation_;
  ObTxAccessMode access_mode_;
  share::SCN snapshot_version_;
  int64_t snapshot_uncertain_bound_;
  uint64_t op_sn_;
  int64_t alloc_ts_;
  int64_t active_ts_;
  int64_t timeout_us_;
  int64_t expire_ts_;
  int64_t finish_ts_;
  ObTxSEQ active_scn_;
  ObTxPartList parts_;
  uint32_t session_id_ = 0;
  ObTxSavePointList savepoints_;
public:
  ObTxInfo(): seq_base_(0) {}
  TO_STRING_KV(K_(tenant_id),
               K_(session_id),
               K_(tx_id),
               K_(access_mode),
               K_(isolation),
               K_(snapshot_version),
               K_(active_scn),
               K_(op_sn),
               K_(alloc_ts),
               K_(active_ts),
               K_(timeout_us),
               K_(expire_ts),
               K_(seq_base),
               K_(parts),
               K_(cluster_id),
               K_(cluster_version),
               K_(savepoints));
  // TODO xa
  bool is_valid() const { return tx_id_.is_valid(); }
  const ObTransID &tid() const { return tx_id_; }
};

class ObTxStmtInfo
{
  friend class ObTransService;
  friend class ObXACtx;
  OB_UNIS_VERSION(1);
protected:
  ObTransID tx_id_;
  uint64_t op_sn_;
  ObTxPartList parts_;
  ObTxDesc::State state_;
  ObTxSavePointList savepoints_;
public:
  TO_STRING_KV(K_(tx_id),
               K_(op_sn),
               K_(parts),
               K_(state),
               K_(savepoints));
  // TODO xa
  bool is_valid() const { return tx_id_.is_valid(); }
  const ObTransID &tid() const { return tx_id_; }
  bool need_rollback() const  { return state_ == ObTxDesc::State::ABORTED; }
};

class TxCtxRoleState
{
public:
  static const int64_t INVALID = -1;
  static const int64_t LEADER = 0;
  static const int64_t FOLLOWER = 1;
  static const int64_t MAX = 2;

  static bool is_valid(const int64_t state)
  { return state > INVALID && state < MAX; }
};
class TxCtxOps
{
public:
  static const int64_t INVALID = -1;
  static const int64_t TAKEOVER = 0;
  static const int64_t REVOKE = 1;
  static const int64_t RESUME = 2;
  static const int64_t SWITCH_GRACEFUL = 3;
  static const int64_t MAX = 4;

  static bool is_valid(const int64_t ops)
  { return ops > INVALID && ops < MAX; }
};
class TxCtxStateHelper
{
public:
  explicit TxCtxStateHelper(int64_t &state) : state_(state),
                                         last_state_(TxCtxRoleState::INVALID),
                                         is_switching_(false) {}
  ~TxCtxStateHelper() {}
  int switch_state(const int64_t op);
  void restore_state();
private:
  int64_t &state_;
  int64_t last_state_;
  bool is_switching_;
};

typedef lib::ObLockGuardWithTimeout<ObSpinLock> ObSpinLockGuardWithTimeout;

#define REC_TRANS_TRACE(recorder_ptr, trace_event) do {   \
  if (NULL != recorder_ptr) {                             \
    REC_TRACE(*recorder_ptr, trace_event);                \
  }                                                       \
} while (0)

#define REC_TRANS_TRACE_EXT(recorder_ptr, trace_event, pairs...) do {  \
  if (NULL != recorder_ptr) {                                          \
    REC_TRACE_EXT(*recorder_ptr, trace_event, ##pairs);                \
  }                                                                    \
} while (0)

#define REC_TRANS_TRACE_EXT2(recorder_ptr, trace_event, pairs...) do { \
  if (NULL != recorder_ptr) {                                          \
    REC_TRACE_EXT(*recorder_ptr, trace_event, ##pairs, OB_ID(opid), opid_);\
  }                                                                    \
} while (0)

inline ObTxSEQ ObTxDesc::get_tx_seq(int64_t seq_abs) const
{
  int64_t seq = seq_abs > 0 ? seq_abs : ObSequence::get_max_seq_no();
  if (OB_LIKELY(support_branch())) {
    if (seq < seq_base_) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "seq_abs is less seq_base_", K(seq_abs), K(tx_id_), K(seq_base_));
      return ObTxSEQ::INVL();
    }
    return ObTxSEQ(seq - seq_base_, 0);
  } else {
    return ObTxSEQ::mk_v0(seq);
  }
}

inline ObTxSEQ ObTxDesc::get_min_tx_seq() const
{
  if (OB_LIKELY(support_branch())) {
    return ObTxSEQ(1, 0);
  } else {
    return ObTxSEQ::mk_v0(1);
  }
}

inline int ObTxDesc::get_and_inc_tx_seq(const int16_t branch,
                                        const int N,
                                        ObTxSEQ &tx_seq) const
{
  int ret = OB_SUCCESS;
  int64_t seq = 0;
  if (OB_FAIL(ObSequence::get_and_inc_max_seq_no(N, seq))) {
    TRANS_LOG(ERROR, "inc max seq no failed", K(ret), K(N));
  } else if (OB_LIKELY(support_branch())) {
    tx_seq = ObTxSEQ(seq - seq_base_, branch);
  } else {
    tx_seq = ObTxSEQ::mk_v0(seq);
  }
  return ret;
}

inline ObTxSEQ ObTxDesc::inc_and_get_tx_seq(int16_t branch) const
{
  int64_t seq = ObSequence::inc_and_get_max_seq_no();
  if (OB_LIKELY(support_branch())) {
    return ObTxSEQ(seq - seq_base_, branch);
  } else {
    return ObTxSEQ::mk_v0(seq);
  }
}

enum ObTxCleanPolicy {
  FAST_ROLLBACK = 1, ROLLBACK = 2, KEEP = 3
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TRANS_DEFINE_V4_
