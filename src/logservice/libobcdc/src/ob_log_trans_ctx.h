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
 *
 *  TransCtx: Context for Distributed Transactions
 */

#ifndef OCEANBASE_LIBOBCDC_TRANS_CTX_
#define OCEANBASE_LIBOBCDC_TRANS_CTX_

#include "share/ob_define.h"                      // OB_*
#include "lib/allocator/ob_allocator.h"           // ObIAllocator
#include "lib/container/ob_se_array.h"            // ObSEArray
#include "lib/allocator/page_arena.h"             // ObArenaAllocator
#include "lib/lock/ob_spin_lock.h"                // ObSpinLock
#include "lib/queue/ob_link_queue.h"              // ObSpLinkQueue
#include "storage/tx/ob_trans_define.h"  // ObTransID

#include "ob_log_part_trans_task.h"               // PartTransTask, IStmtTask, DmlStmtTask

namespace oceanbase
{
namespace libobcdc
{

class ObLogLSMgr;
class IObLogTransCtxMgr;
class TransCtx;

// Participant information
struct TransPartInfo
{
  PartTransTask           *obj_;
  logservice::TenantLSID  tls_id_;

  explicit TransPartInfo(const logservice::TenantLSID &tls_id) : obj_(NULL), tls_id_(tls_id)
  {}

  bool operator < (const TransPartInfo &other) const
  {
    return tls_id_ < other.tls_id_;
  }

  TO_STRING_KV(KPC_(obj), K_(tls_id));
};

struct TransPartInfoCompare
{
  bool operator() (const TransPartInfo &p1, const TransPartInfo &p2) const
  {
    return p1 < p2;
  }
};

// Allows assignment and copy constructs for use in priority queues
class TransCtxSortElement
{
public:
  TransCtxSortElement();
  ~TransCtxSortElement();

public:
  int init(TransCtx &trans_ctx_host,
      const transaction::ObTransID &trans_id,
      const int64_t trans_commit_version);
  void reset();

  TransCtx *get_trans_ctx_host() { return trans_ctx_host_; }
  const transaction::ObTransID &get_trans_id() const { return trans_id_; }
  int64_t get_trans_commit_version() const {  return trans_commit_version_; }

  struct TransCtxCmp
  {
    bool operator() (const TransCtxSortElement &tx1, const TransCtxSortElement &tx2) const;
  };

  TO_STRING_KV(
      K_(trans_id),
      K_(trans_commit_version));

private:
  TransCtx                  *trans_ctx_host_;
  transaction::ObTransID    trans_id_;
  int64_t                   trans_commit_version_;  // Transaction commit version
};

class TransCtx
{
public:
  enum
  {
    TRANS_CTX_STATE_DISCARDED = -1,           // Deprecated and useless, the transaction context will be deleted
    TRANS_CTX_STATE_INVALID = 0,              // invalid
    TRANS_CTX_STATE_PREPARED = 1,             // ready
    TRANS_CTX_STATE_PARTICIPANT_READY = 2,    // participant aggregation ready
    TRANS_CTX_STATE_SEQUENCED = 3,            // sequence done
    TRANS_CTX_STATE_DATA_READY = 4,           // data is ready when complete formated in memory-mode or stored in storage-mode
    TRANS_CTX_STATE_COMMITTED = 5,            // complete commit, waiting in user queue
    TRANS_CTX_STATE_PARTICIPANT_REVERTED = 6, // all participant objects have been reclaimed
    TRANS_CTX_STATE_MAX
  };

  enum { DATA_OP_TIMEOUT = 10 * 1000 * 1000 };

public:
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t DEFAULT_DEP_SET_SIZE = 64;
  static const int64_t PRINT_LOG_INTERVAL = 10 * _SEC_;

public:
  TransCtx();
  virtual ~TransCtx();

public:
  void reset();
  int init(IObLogTransCtxMgr *host);

  /// Prepare the list of participants and initialize the transaction context: ensure that the state is no longer INVALID upon return
  /// 1. If in INVALID state, initialize it and prepare the list of participants
  ///   a) If the current participant is in service, the initialisation is successful and the state changes to PREPARED
  ///   b) If the current participant is not serviced, the initialisation fails and the state changes to DISCARDED and is deprecated
  /// 2. return INVALID_ERROR if in DISCARDED state
  /// 3. If in any other state, return success directly
  ///
  /// @param [in] part_trans_task Target participant
  /// @param [in] ls_mgr        Partition transaction manager to determine if the participant is served
  /// @param [in] stop_flag       Exit status flag
  /// @param [out] need_discard   Return value: marks whether the transaction context needs to be deprecated
  ///
  /// @retval OB_SUCCESS         Success
  /// @retval OB_INVALID_ERROR   This transaction context is deprecated
  /// @retval OB_IN_STOP_STATE   exist
  /// @retval other_error_code   Fail
  int prepare(
      PartTransTask &part_trans_task,
      ObLogLSMgr &ls_mgr,
      const bool print_ls_not_serve_info,
      volatile bool &stop_flag,
      bool &need_discard);

  /// Adds participants, determines if they are in service
  /// Requires status of at least PREPARED, not INVALID or DISCARDED
  ///
  /// @param [in]   part_trans_task           Target participant
  /// @param [out]  is_part_trans_served      Return value: marks whether the partition transaction is being served
  /// @param [out]  is_all_participants_ready Return value: marks if all participants are READY
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval other_error_code  Fail
  int add_participant(
      PartTransTask &part_trans_task,
      bool &is_part_trans_served,
      bool &is_all_participants_ready);

  /// Sequence the transaction
  /// Set the sequence number and schema verison, and set the status to SEQUENCED
  /// Requires that all participants are gathered, i.e. the status is PARTICIPANT_READY
  ///
  /// @param seq                sequence
  /// @param schema_version     schema version
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval other_error_code  Fail
  int sequence(const int64_t seq, const int64_t schema_version);

  /// Wait all participant which has been data ready
  /// Requires that sequencing has been completed, i.e., the status is SEQUENCED
  /// If all participants have been data ready, the state is advanced to TRANS_CTX_STATE_DATA_READY
  ///
  /// @retval OB_SUCCESS          succ
  /// @retval Other return values fail
  int wait_data_ready(const int64_t timeout,
      volatile bool &stop_flag);

  /// mark status to COMMITTED
  /// Requires all participants to have completed formatting, i.e. a status of SEQUENCED
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval other_error_code  Fail
  int commit();

  /// Increase the number of participants that can be reclaimed
  /// Request status of committed: COMMITTED
  ///
  /// @param [out] all_participant_revertable  Are all participants recoverable
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval other_error_code  Fail
  int inc_revertable_participant_count(bool &all_participant_revertable);

  /// Reclaim all participants
  /// Requires a status of COMMITTED and that all participants can be reclaimed, i.e. the number of reclaimed participants is equal to the number of all participants
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval other_error_code  Fail
  int revert_participants();

public:
  static bool is_state_valid(const int state) { return state >= TRANS_CTX_STATE_DISCARDED && state < TRANS_CTX_STATE_MAX; }
  bool is_participants_ready() { return TRANS_CTX_STATE_PARTICIPANT_READY == state_; }
  bool is_sequenced() const { return TRANS_CTX_STATE_SEQUENCED == state_; }
  bool is_data_ready() const { return TRANS_CTX_STATE_DATA_READY == state_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const transaction::ObTransID &get_trans_id() const { return trans_id_; }
  const common::ObString &get_trans_id_str() const { return trans_id_str_; }
  const common::ObString &get_major_version_str() const { return major_version_str_; }
  TransCtxSortElement &get_trx_sort_elem() { return trx_sort_elem_; }
  PartTransTask *get_participant_objs() { return ready_participant_objs_; }
  int64_t get_ready_participant_count() const { return ready_participant_count_; }

  int64_t get_total_br_count() const { return ATOMIC_LOAD(&total_br_count_); }
  void inc_total_br_count_() { ATOMIC_INC(&total_br_count_); }
  void set_total_br_count(const int64_t total_br_count) { ATOMIC_SET(&total_br_count_, total_br_count); }
  bool is_all_br_committed() const { return is_trans_sorted() && (ATOMIC_LOAD(&total_br_count_) == ATOMIC_LOAD(&committed_br_count_)); }
  void inc_committed_br_count() { ATOMIC_INC(&committed_br_count_); }
  int get_committed_br_count() const { return ATOMIC_LOAD(&committed_br_count_); }
  int64_t get_valid_part_trans_task_count() const { return ATOMIC_LOAD(&valid_part_trans_task_count_); }
  void set_valid_part_trans_task_count(const int64_t valid_part_trans_task_count) { ATOMIC_SET(&valid_part_trans_task_count_, valid_part_trans_task_count); }

  int lock();
  int unlock();
  int get_tenant_id(uint64_t &tenant_id) const;
  bool has_ddl_participant() const;

  // for unittest start
  int set_trans_id(const transaction::ObTransID &trans_id);
  const TransPartInfo *get_participants() const { return participants_; }
  int64_t get_participant_count() const { return participant_count_; }
  int get_state() const;

  int set_state(const int target_state);
  int set_ready_participant_count(const int64_t count);
  int64_t get_revertable_participant_count() const;
  int set_ready_participant_objs(PartTransTask *part_trans_task);
  int append_sorted_br(ObLogBR *br);
  void set_trans_redo_dispatched() { ATOMIC_SET(&is_trans_redo_dispatched_, true); }
  bool is_trans_sorted() const { return ATOMIC_LOAD(&is_trans_sorted_); }
  void set_trans_sorted() { ATOMIC_SET(&is_trans_sorted_, true); }
  /// check if trans has valid br
  /// note: call this function before any br output
  /// @retval OB_SUCCESS      trans has valid br to output
  /// @retval OB_EMPTY_RESULT trans doesn't has valid br until sorter process finish
  /// @retval other_err_code  unexpected error
  int has_valid_br(volatile bool &stop_flag);
  int pop_br_for_committer(ObLogBR *&br);
  // for unittest end

public:
  const char *print_state() const
  {
    const char *ret = "UNKNOWN";
    switch (state_) {
    case TRANS_CTX_STATE_DISCARDED:
      ret = "DISCARDED";
      break;
    case TRANS_CTX_STATE_INVALID:
      ret = "INVALID";
      break;
    case TRANS_CTX_STATE_PREPARED:
      ret = "PREPARED";
      break;
    case TRANS_CTX_STATE_PARTICIPANT_READY:
      ret = "PARTICIPANT_READY";
      break;
    case TRANS_CTX_STATE_SEQUENCED:
      ret = "SEQUENCED";
      break;
    case TRANS_CTX_STATE_DATA_READY:
      ret = "DATA_READY";
      break;
    case TRANS_CTX_STATE_COMMITTED:
      ret = "COMMITTED";
      break;
    case TRANS_CTX_STATE_PARTICIPANT_REVERTED:
      ret = "PARTICIPANT_REVERTED";
      break;
    default:
      ret = "UNKNOWN";
      break;
    }
    return ret;
  }

public:
  TO_STRING_KV(
      "state", print_state(),
      K_(trx_sort_elem),
      K_(seq),
      K_(participants),
      K_(participant_count),
      KP_(ready_participant_objs),
      K_(ready_participant_count),
      K_(total_br_count),
      K_(committed_br_count),
      K_(revertable_participant_count),
      K_(is_trans_redo_dispatched),
      K_(is_trans_sorted));

private:
  // Prepare the transaction context
  // INVALID -> PREPARED
  int prepare_(
      ObLogLSMgr &ls_mgr,
      const PartTransTask &host,
      const bool print_ls_not_serve_info,
      volatile bool &stop_flag);
  int inc_ls_trans_count_on_serving_(
      const transaction::ObTransID &trans_id,
      const share::ObLSID &ls_id,
      const palf::LSN &commit_log_lsn,
      const bool print_ls_not_serve_info,
      ObLogLSMgr &ls_mgr,
      bool &is_serving,
      volatile bool &stop_flag);
  int add_ready_participant_(PartTransTask &part_trans_task,
      bool &is_part_trans_served,
      bool &is_all_participants_ready);
  void switch_state_(const int target_state);
  int init_participant_array_(const int64_t part_count);
  void destroy_participant_array_();
  int build_ready_participants_list_();
  int init_trans_id_str_();
  // only init major version str for observer version less than 2_0_0
  int init_major_version_str_(const common::ObVersion &freeze_version);
  // wait until trans redo dispatched.
  void wait_trans_redo_dispatched_();

private:
  IObLogTransCtxMgr         *host_;

  int8_t                    state_;
  uint64_t                  tenant_id_;
  transaction::ObTransID    trans_id_;
  common::ObString          trans_id_str_;
  common::ObString          major_version_str_;
  TransCtxSortElement       trx_sort_elem_;

  // sequence info
  int64_t                   seq_;                       //Global sequence number of the distributed transaction

  // Participant Information
  TransPartInfo             *participants_;            // Participant Information
  int64_t                   participant_count_;        // Participant count
  PartTransTask             *ready_participant_objs_;  // List of added participant objects
  int64_t                   ready_participant_count_;  // Amount of added participant objects

  int64_t                   total_br_count_ CACHE_ALIGNED;
  int64_t                   committed_br_count_ CACHE_ALIGNED;
  int64_t                   valid_part_trans_task_count_;   // Number of valid participants count

  // status info
  int64_t                   revertable_participant_count_;  // Number of participants able to be released
  bool                      is_trans_redo_dispatched_ CACHE_ALIGNED;
  bool                      is_trans_sorted_ CACHE_ALIGNED;
  common::ObSpLinkQueue     br_out_queue_;

  // allocator
  common::ObArenaAllocator  allocator_;
  mutable common::ObSpinLock lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(TransCtx);
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_TRANS_CTX_ */
