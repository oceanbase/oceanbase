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
#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_BASE_H
#define STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_BASE_H

#include "lib/container/ob_array.h"
#include "lib/ob_define.h"
#include "lib/profile/ob_trace_id.h"
#include "storage/checkpoint/ob_common_checkpoint.h"
#include "lib/function/ob_function.h"
#include "runtime_utility/mds_lock.h"
#include "storage/multi_data_source/mds_table_mgr.h"
#include "observer/virtual_table/ob_mds_event_buffer.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/multi_data_source/runtime_utility/list_helper.h"
#include "storage/multi_data_source/runtime_utility/mds_tlocal_info.h"
#include "storage/checkpoint/ob_checkpoint_diagnose.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace storage
{
class ObTabletPointer;
namespace mds
{
extern TLOCAL(MdsTLocalInfo, TLOCAL_MDS_INFO);
template <typename K, typename V>
class MdsRow;
template <typename K, typename V>
class MdsUnit;
template <typename K, typename V>
class UserMdsNode;
class MdsNode;
class MdsCtx;
class MdsWriter;
class MdsDumpKV;
class MdsNodeInfoForVirtualTable;
enum class MdsTableType {
  LS_INNER_TABLE = 1,
  NORMAL_TABLE = 2,
  UNKNOWN = 3,
};

struct ObMdsGlobalSequencer
{
  static int64_t generate_senquence() { return ATOMIC_AAF(&get_instance().sequence_, 1); }
  static ObMdsGlobalSequencer &get_instance() {
    static ObMdsGlobalSequencer instance;
    return instance;
  }
private:
  ObMdsGlobalSequencer() : sequence_(0) {}
  int64_t sequence_;
};

class MdsTableBase : public ListNode<MdsTableBase>
{
  template <typename K, typename V>
  friend class MdsRow;
  template <typename K, typename V>
  friend class MdsUnit;
  friend class MdsNode;
  template <typename K, typename V>
  friend class UserMdsNode;
protected:
  enum State : uint8_t {
    UNKNOWN = 0,
    INIT,
    WRITTING,// replay or set
    END,
  };
  const char *state_to_string(State state) const {
    switch (state) {
    case State::UNKNOWN: return "UNKNOWN";
    case State::INIT: return "INIT";
    case State::WRITTING: return "WRITTING";
    default: return "UNEXPECTED";
    }
  }
  static constexpr bool StateChecker[static_cast<int>(State::END)][static_cast<int>(State::END)] = {
    {0, 1, 0},// from UNKNOWN, only allowed to switch to INIT
    {0, 0, 1},// from INIT, allowed to switch to WRITTING
    {0, 0, 1},// from WRITTING, not allowed to switch to any state
  };
  int advance_state_to(State new_state) const;
public:
  MdsTableBase()
  : state_(State::UNKNOWN),
  ls_id_(),
  tablet_id_(),
  flushing_scn_(),
  last_inner_recycled_scn_(share::SCN::min_scn()),
  rec_scn_(share::SCN::max_scn()),
  max_aborted_scn_(share::SCN::min_scn()),
  total_node_cnt_(0),
  construct_sequence_(0),
  lock_(),
  trace_id_(checkpoint::INVALID_TRACE_ID) { construct_sequence_ = ObMdsGlobalSequencer::generate_senquence(); }
  virtual ~MdsTableBase()
  {
    REPORT_CHECKPOINT_DIAGNOSE_INFO(update_start_gc_time_for_checkpoint_unit, this);
  }
  int init(const ObTabletID tablet_id,
           const share::ObLSID ls_id,
           const share::SCN mds_ckpt_scn_from_tablet,// this is used to filter replayed nodes after removed action
           ObTabletPointer *pointer,
           ObMdsTableMgr *p_mgr);
  virtual int set(int64_t unit_id,
                  void *key,
                  void *data,
                  bool is_rvalue,
                  MdsCtx &ctx,
                  const int64_t lock_timeout_us) = 0;
  virtual int replay(int64_t unit_id,
                     void *key,
                     void *data,
                     bool is_rvalue,
                     MdsCtx &ctx,
                     const share::SCN &scn) = 0;
  virtual int remove(int64_t unit_id,
                     void *key,
                     MdsCtx &ctx,
                     const int64_t lock_timeout_us) = 0;
  virtual int replay_remove(int64_t unit_id,
                            void *key,
                            MdsCtx &ctx,
                            const share::SCN &scn) = 0;
  virtual int get_latest(int64_t unit_id,
                         void *key,
                         ObFunction<int(void *)> &op,
                         MdsWriter &writer,// FIXME(xuwang.txw): should not exposed, will be removed later
                         TwoPhaseCommitState &trans_stat,// FIXME(xuwang.txw): should not exposed, will be removed later
                         share::SCN &trans_version,// FIXME(xuwang.txw): should not exposed, will be removed later
                         const int64_t read_seq) const = 0;
  virtual int get_latest_committed(int64_t unit_id,
                                   void *key,
                                   ObFunction<int(void *)> &op) const = 0;
  virtual int get_tablet_status_node(ObFunction<int(void *)> &op,
                                     const int64_t read_seq) const = 0;
  virtual int get_snapshot(int64_t unit_id,
                           void *key,
                           ObFunction<int(void *)> &op,
                           const share::SCN &snapshot,
                           const int64_t timeout_us) const = 0;
  virtual int get_by_writer(int64_t unit_id,
                            void *key,
                            ObFunction<int(void *)> &op,
                            const MdsWriter &writer,
                            const share::SCN &snapshot,
                            const transaction::ObTxSEQ read_seq,
                            const int64_t timeout_us) const = 0;
  virtual int is_locked_by_others(int64_t unit_id,
                                  void *key,
                                  bool &is_locked,
                                  const MdsWriter &self) const = 0;
  virtual int scan_all_nodes_to_dump(ObFunction<int(const MdsDumpKV&)> &for_each_op,
                                     const int64_t mds_construct_sequence,
                                     const bool for_flush,
                                     const ScanRowOrder scan_row_order,
                                     const ScanNodeOrder scan_node_order) const = 0;
  virtual void on_flush(const share::SCN &flushed_scn, const int flush_ret) = 0;
  virtual int try_recycle(const share::SCN recycle_scn) = 0;
  share::ObLSID get_ls_id() const;
  int64_t get_node_cnt() const;
  virtual share::SCN get_rec_scn();
  virtual int operate(const ObFunction<int(MdsTableBase &)> &operation) = 0;
  virtual int flush(share::SCN need_advanced_rec_scn_lower_limit, share::SCN max_decided_scn) = 0;
  virtual ObTabletID get_tablet_id() const;
  virtual bool is_flushing() const;
  virtual int fill_virtual_info(ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array) const = 0;
  virtual int forcely_remove_nodes(const char *reason, share::SCN redo_scn_limit) = 0;
  void mark_removed_from_t3m(ObTabletPointer *pointer);// need called in del tablet phase
  void mark_switched_to_empty_shell();
  bool is_switched_to_empty_shell() const;
  bool is_removed_from_t3m() const;
  bool is_construct_sequence_matched(const int64_t seq) const { return seq == construct_sequence_; }
  int64_t get_removed_from_t3m_ts() const;
  int64_t get_trace_id() const { return trace_id_; }
  void set_trace_id(int64_t trace_id)
  {
    ADD_CHECKPOINT_DIAGNOSE_INFO_AND_SET_TRACE_ID(checkpoint::ObCheckpointUnitDiagnoseInfo, trace_id);
  }
  VIRTUAL_TO_STRING_KV(KP(this));
protected:
  void inc_valid_node_cnt();
  void dec_valid_node_cnt();
  void try_advance_rec_scn(const share::SCN scn);
  void try_decline_rec_scn(const share::SCN scn);
  void try_advance_max_aborted_scn(const share::SCN scn);
  int get_ls_max_consequent_callbacked_scn_(share::SCN &max_consequent_callbacked_scn) const;
  int register_to_mds_table_mgr();
  int unregister_from_mds_table_mgr();// call when marked deleted or released directly
  int unregister_from_removed_recorder();// call when marked deleted
  int merge(const int64_t construct_sequence, const share::SCN &flushing_scn);
  void report_construct_event_(const char *file = __builtin_FILE(),
                               const uint32_t line = __builtin_LINE(),
                               const char *function_name = __builtin_FUNCTION()) {
    int ret = OB_SUCCESS;
    observer::MdsEvent event;
    event.record_thread_info_();
    event.info_str_.reset();
    event.event_ = "CONSTRUCTED";
    observer::MdsEventKey key(MTL_ID(),
                              ls_id_,
                              tablet_id_);
    observer::ObMdsEventBuffer::append(key, event, this, file, line, function_name);
  }
  void report_destruct_event_(const char *file = __builtin_FILE(),
                              const uint32_t line = __builtin_LINE(),
                              const char *function_name = __builtin_FUNCTION()) {
    int ret = OB_SUCCESS;
    observer::MdsEvent event;
    event.record_thread_info_();
    event.info_str_.reset();
    event.event_ = "DESTRUCTED";
    observer::MdsEventKey key(MTL_ID(),
                              ls_id_,
                              tablet_id_);
    observer::ObMdsEventBuffer::append(key, event, this, file, line, function_name);
  }
  template <int N>
  void report_rec_scn_event_(const char (&event_str)[N],
                             share::SCN old_scn,
                             share::SCN new_scn,
                             const char *file = __builtin_FILE(),
                             const uint32_t line = __builtin_LINE(),
                             const char *function_name = __builtin_FUNCTION()) {
    int ret = OB_SUCCESS;
    observer::MdsEvent event;
    constexpr int64_t buffer_size = 1_KB;
    char stack_buffer[buffer_size] = { 0 };
    int64_t pos = 0;
    databuff_printf(stack_buffer, buffer_size, pos, old_scn);
    databuff_printf(stack_buffer, buffer_size, pos, " -> ");
    databuff_printf(stack_buffer, buffer_size, pos, new_scn);
    event.record_thread_info_();
    event.info_str_.assign(stack_buffer, pos);
    event.event_ = event_str;
    observer::MdsEventKey key(MTL_ID(),
                              ls_id_,
                              tablet_id_);
    observer::ObMdsEventBuffer::append(key, event, this, file, line, function_name);
  }
  template <int N>
  void report_flush_event_(const char (&event_str)[N],
                           share::SCN flush_scn,
                           const char *file = __builtin_FILE(),
                           const uint32_t line = __builtin_LINE(),
                           const char *function_name = __builtin_FUNCTION()) {
    int ret = OB_SUCCESS;
    observer::MdsEvent event;
    constexpr int64_t buffer_size = 1_KB;
    char stack_buffer[buffer_size] = { 0 };
    int64_t pos = 0;
    databuff_printf(stack_buffer, buffer_size, pos, "flush_scn:");
    databuff_printf(stack_buffer, buffer_size, pos, flush_scn);
    event.record_thread_info_();
    event.info_str_.assign(stack_buffer, pos);
    event.event_ = event_str;
    observer::MdsEventKey key(MTL_ID(),
                              ls_id_,
                              tablet_id_);
    observer::ObMdsEventBuffer::append(key, event, this, file, line, function_name);

    REPORT_CHECKPOINT_DIAGNOSE_INFO(update_schedule_dag_info, this, rec_scn_, rec_scn_, flushing_scn_);

  }
  template <int N>
  void report_on_flush_event_(const char (&event_str)[N],
                              share::SCN flush_scn,
                              const char *file = __builtin_FILE(),
                              const uint32_t line = __builtin_LINE(),
                              const char *function_name = __builtin_FUNCTION()) {
    int ret = OB_SUCCESS;
    observer::MdsEvent event;
    constexpr int64_t buffer_size = 1_KB;
    char stack_buffer[buffer_size] = { 0 };
    int64_t pos = 0;
    databuff_printf(stack_buffer, buffer_size, pos, "flush_scn:");
    databuff_printf(stack_buffer, buffer_size, pos, flush_scn);
    event.record_thread_info_();
    event.info_str_.assign(stack_buffer, pos);
    event.event_ = event_str;
    observer::MdsEventKey key(MTL_ID(),
                              ls_id_,
                              tablet_id_);
    observer::ObMdsEventBuffer::append(key, event, this, file, line, function_name);
    observer::ObMdsEventBuffer::append(key, event, this, file, line, function_name);

    REPORT_CHECKPOINT_DIAGNOSE_INFO(update_merge_info_for_checkpoint_unit, this);
  }
  void report_recycle_event_(share::SCN recycle_scn,
                             const char *file = __builtin_FILE(),
                             const uint32_t line = __builtin_LINE(),
                             const char *function_name = __builtin_FUNCTION()) {
    int ret = OB_SUCCESS;
    observer::MdsEvent event;
    constexpr int64_t buffer_size = 1_KB;
    char stack_buffer[buffer_size] = { 0 };
    int64_t pos = 0;
    databuff_printf(stack_buffer, buffer_size, pos, "recycle_scn:");
    databuff_printf(stack_buffer, buffer_size, pos, recycle_scn);
    event.record_thread_info_();
    event.info_str_.assign(stack_buffer, pos);
    event.event_ = "RECYCLE";
    observer::MdsEventKey key(MTL_ID(),
                              ls_id_,
                              tablet_id_);
    observer::ObMdsEventBuffer::append(key, event, this, file, line, function_name);
  }
public:
  struct DebugInfo {
    DebugInfo()
    : do_init_tablet_pointer_(nullptr),
    do_remove_tablet_pointer_(nullptr),
    init_ts_(0),
    last_remove_ts_(0),
    remove_ts_(0),
    switch_to_empty_shell_ts_(0),
    last_flush_ts_(0),
    init_trace_id_(),
    remove_trace_id_() {}
    TO_STRING_KV(KP_(do_init_tablet_pointer), KP_(do_remove_tablet_pointer), KTIME_(init_ts), KTIME_(last_remove_ts),
                 KTIME_(remove_ts), KTIME_(last_flush_ts), KTIME_(switch_to_empty_shell_ts), K_(init_trace_id), K_(remove_trace_id));
    ObTabletPointer *do_init_tablet_pointer_;// can not be accessed, just record it to debug
    ObTabletPointer *do_remove_tablet_pointer_;// can not be accessed, just record it to debug
    int64_t init_ts_;
    int64_t last_remove_ts_;
    int64_t remove_ts_;
    int64_t switch_to_empty_shell_ts_;
    int64_t last_flush_ts_;
    ObCurTraceId::TraceId init_trace_id_;
    ObCurTraceId::TraceId remove_trace_id_;
  } debug_info_;// 120B
  mutable State state_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  share::SCN flushing_scn_;// To tell if this mds table is flushing
  share::SCN last_inner_recycled_scn_;// To filter repeated release operation, and filter replay operation
  share::SCN rec_scn_;// To CLOG to recycle
  share::SCN max_aborted_scn_;// To record max aborted scn ever seen, to judge if can advance rec_scn to MAX after flushed(only if flushed scn >= max_aborted_scn_), otherwise rec_scn can only be advanced to flushed_scn
  int64_t total_node_cnt_;// To tell if this mds table is safety to destroy
  int64_t construct_sequence_;// To filter invalid dump DAG
  MdsTableMgrHandle mgr_handle_;
  mutable MdsLock lock_;
  // a round checkpoint identifier for checkpoint diagnose
  int64_t trace_id_;
};

bool check_node_scn_beflow_flush(const MdsNode &node, const share::SCN &flush_scn);

}
}
}
#endif
