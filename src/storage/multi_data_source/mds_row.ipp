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

#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_ROW_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_ROW_IPP

#include "lib/ob_errno.h"
#include "share/ob_cluster_version.h"
#include "share/ob_errno.h"
#include "storage/multi_data_source/mds_table_base.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#include "storage/tx/ob_tx_seq.h"
#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_ROW_H_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_ROW_H_IPP
#include "mds_row.h"
#endif

namespace oceanbase
{
namespace storage
{
namespace mds
{

// this function can only be used in this file,
// and must be called from for_each_lock_ method with lock protection.
template <typename K, typename V>
static int check_node_snapshot(const UserMdsNode<K, V> &node,
                               const share::SCN snapshot,
                               const RetryParam &retry_param,
                               bool &can_read)
{
  #define PRINT_WRAPPER KR(ret), KPC(node), K(typeid(READ_OP).name()), K(retry_times), K(can_read),\
                        K(*this), K(snapshot_converted)
  int ret = OB_SUCCESS;
  can_read = false;
  share::SCN snapshot_converted = snapshot;
  if (snapshot_converted == share::SCN::max_scn()) {
    snapshot_converted = share::SCN::scn_dec(snapshot_converted);
  }
  if (!node.is_decided_() && node.get_prepare_version_() <= snapshot_converted) {
    ret = OB_EAGAIN;
    if (retry_param.check_reach_print_interval_and_update()) {
      MDS_LOG(WARN, "mds row lock_for_read conflict");
    }
  } else if (node.is_committed_() && node.get_commit_version_() <= snapshot_converted) {
    can_read = true;
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename ValueType>
struct MdsRow<K, V>::IteratorBase {// Bidirectional iterators
  // iterator traits
  using difference_type = int64_t;
  using value_type = ValueType;
  using pointer = ValueType *;
  using reference = ValueType &;
  using iterator_category = std::bidirectional_iterator_tag;
  // method needed define
  IteratorBase() : p_node_(nullptr) {}
  IteratorBase(ValueType *val) : p_node_(val) {}
  IteratorBase(const IteratorBase<ValueType> &rhs) : p_node_(rhs.p_node_) {}
  IteratorBase<ValueType> &operator=(const IteratorBase<ValueType> &rhs) { p_node_ = rhs.p_node_; return *this; }
  bool operator==(const IteratorBase<ValueType> &rhs) { return p_node_ == rhs.p_node_; }
  bool operator!=(const IteratorBase<ValueType> &rhs) { return p_node_ != rhs.p_node_; }
  ValueType &operator*() { return *p_node_; }
  ValueType *operator->() { return p_node_; }
  UserMdsNode<K, V> *p_node_;
};

template <typename K, typename V>
template <typename ValueType>
struct MdsRow<K, V>::NormalIterator : public IteratorBase<ValueType> {
  NormalIterator() : IteratorBase<ValueType>() {}
  NormalIterator(const NormalIterator &rhs) : IteratorBase<ValueType>(rhs) {}
  NormalIterator(ValueType *val) : IteratorBase<ValueType>(val) {}
  NormalIterator<ValueType> &operator++() {
    IteratorBase<ValueType>::p_node_ = (UserMdsNode<K, V> *)IteratorBase<ValueType>::
                                       p_node_->ListNode<UserMdsNode<K, V>>::next();
    return *this;
  }
  NormalIterator<ValueType> operator++(int) {
    NormalIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
  NormalIterator<ValueType> &operator--() {
    IteratorBase<ValueType>::p_node_ = (UserMdsNode<K, V> *)IteratorBase<ValueType>::
                                       p_node_->ListNode<UserMdsNode<K, V>>::prev();
    return *this;
  }
  NormalIterator<ValueType> operator--(int) {
    NormalIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
};

template <typename K, typename V>
template <typename ValueType>
struct MdsRow<K, V>::ReverseIterator : public IteratorBase<ValueType> {
  ReverseIterator() : IteratorBase<ValueType>() {}
  ReverseIterator(const ReverseIterator &rhs) : IteratorBase<ValueType>(rhs) {}
  ReverseIterator(ValueType *val) : IteratorBase<ValueType>(val) {}
  ReverseIterator<ValueType> &operator++() {
    IteratorBase<ValueType>::p_node_ = (UserMdsNode<K, V> *)IteratorBase<ValueType>::
                                       p_node_->ListNode<UserMdsNode<K, V>>::prev();
    return *this;
  }
  ReverseIterator<ValueType> operator++(int) {
    ReverseIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
  ReverseIterator<ValueType> &operator--() {
    IteratorBase<ValueType>::p_node_ = (UserMdsNode<K, V> *)IteratorBase<ValueType>::
                                       p_node_->ListNode<UserMdsNode<K, V>>::next();
    return *this;
  }
  ReverseIterator<ValueType> operator--(int) {
    ReverseIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
};

template <typename K, typename V>
typename MdsRow<K, V>::iterator MdsRow<K, V>::begin()
{ return iterator((UserMdsNode<K, V>*)(ListNode<UserMdsNode<K, V>>*)sorted_list_.list_head_); }
template <typename K, typename V>
typename MdsRow<K, V>::iterator MdsRow<K, V>::end()
{ return iterator(nullptr); }
template <typename K, typename V>
typename MdsRow<K, V>::const_iterator MdsRow<K, V>::cbegin()
{ return const_iterator((UserMdsNode<K, V>*)(ListNode<UserMdsNode<K, V>>*)sorted_list_.list_head_); }
template <typename K, typename V>
typename MdsRow<K, V>::const_iterator MdsRow<K, V>::cend()
{ return const_iterator(nullptr); }
template <typename K, typename V>
typename MdsRow<K, V>::reverse_iterator MdsRow<K, V>::rbegin()
{ return reverse_iterator((UserMdsNode<K, V>*)(ListNode<UserMdsNode<K, V>>*)sorted_list_.list_tail_); }
template <typename K, typename V>
typename MdsRow<K, V>::reverse_iterator MdsRow<K, V>::rend()
{ return reverse_iterator(nullptr); }
template <typename K, typename V>
typename MdsRow<K, V>::const_reverse_iterator MdsRow<K, V>::crbegin()
{ return const_reverse_iterator((UserMdsNode<K, V>*)(ListNode<UserMdsNode<K, V>>*)sorted_list_.list_tail_); }
template <typename K, typename V>
typename MdsRow<K, V>::const_reverse_iterator MdsRow<K, V>::crend()
{ return const_reverse_iterator(nullptr); }

template <typename K, typename V>
struct DestroyOP {
  DestroyOP(MdsRow<K, V> &row) : row_(row) {}
  bool operator()(const UserMdsNode<K, V> &node) {
    UserMdsNode<K, V> &cast_node = const_cast<UserMdsNode<K, V> &>(node);
    row_.sorted_list_.del((ListNodeBase*)(ListNode<UserMdsNode<K, V>>*)&cast_node);
    MdsFactory::destroy(&cast_node);
    return false;
  }
  MdsRow<K, V> &row_;
};
template <typename K, typename V>
MdsRow<K, V>::~MdsRow()// all mds nodes lived with RAII, owned by MdsRow
{
  MdsWLockGuard lg(MdsRowBase<K, V>::lock_);
  if (!sorted_list_.empty()) {
    MDS_LOG_RET(WARN, OB_SUCCESS, "release mds mode when mds row destructed", K(*this));
    sorted_list_.for_each_node_from_tail_to_head_until_true(DestroyOP<K, V>(*this));
  }
}

struct CheckNodeExistOp {
  CheckNodeExistOp(share::SCN scn) : scn_(scn) {}
  template <typename K, typename V>
  int operator()(const UserMdsNode<K, V> &node) {
    int ret = OB_SUCCESS;
    if (scn_ == node.redo_scn_) {
      ret = OB_ENTRY_EXIST;
    }
    return ret;
  }
  share::SCN scn_;
};
struct CheckNodeInSameWriterSeqIncLogicOp {
  CheckNodeInSameWriterSeqIncLogicOp(MdsCtx &ctx) : ctx_(ctx) {}
  template <typename K, typename V>
  int operator()(const UserMdsNode<K, V> &node) {
    int ret = OB_SUCCESS;
    if (node.get_writer_() != ctx_.get_writer()) {
      ret = OB_ITER_END;
    } else if (!node.seq_no_.is_valid()) {
      ret = OB_ITER_END;
      MDS_LOG(WARN, "node's seq_no in row is invalid, maybe in compat scenario?", K(node), K_(ctx));
    } else if (node.seq_no_ >= ctx_.get_seq_no()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG(ERROR, "node's seq_no in row is not less than mds ctx", K(node), K_(ctx));
    } else {// check safe, no need check next node
      ret = OB_ITER_END;
    }
    return ret;
  }
  MdsCtx &ctx_;
};
template <typename K, typename V>
template <typename DATA>
int MdsRow<K, V>::construct_insert_record_user_mds_node_(MdsRowBase<K, V> *mds_row,
                                                         DATA &&data,
                                                         const MdsNodeType node_type,
                                                         const share::SCN scn,
                                                         MdsCtx &ctx)
{
  #define PRINT_WRAPPER KR(ret), K(typeid(V).name()), K(obj_to_string(node_type)), K(scn), K(ctx),\
                        K(*this), KP(new_node)
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);
  bool need_rollback_node = false;
  UserMdsNode<K, V> *new_node = nullptr;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_0_0) {
    if (!scn.is_max() && !ctx.get_seq_no().is_valid()) {
      ctx.set_seq_no(transaction::ObTxSEQ::MIN_VAL());
      MDS_LOG_SET(WARN, "seq no on mds ctx is invalid, maybe meet old version CLOG, convert to min scn");
    }
    if (scn.is_max()) {// write operation on leader after upgrade
      if (OB_FAIL(sorted_list_.reverse_for_each_node(CheckNodeInSameWriterSeqIncLogicOp(ctx)))) {// can not assert here, cause some modules maynot adapt this logic yet
        MDS_LOG_SET(WARN, "seq_no is not satisfied inc logic");
      }
    }
  }
#ifndef UNITTEST_DEBUG
  set_mds_mem_check_thread_local_info(mds_row->p_mds_unit_->p_mds_table_->ls_id_,
                                      mds_row->p_mds_unit_->p_mds_table_->tablet_id_,
                                      typeid(new_node).name());
#endif
  // if this is an insert action, check is same scn mds node exists
  if (OB_FAIL(ret)) {
  } else if (scn != share::SCN::max_scn() && MDS_FAIL(sorted_list_.for_each_node(CheckNodeExistOp(scn)))) {
    MDS_LOG_SET(WARN, "scn is not max, this is an insert action, but node with same scn exists");
  } else if (MDS_FAIL_FLAG(MdsFactory::create(new_node,
                                              mds_row,
                                              node_type,
                                              ctx.get_writer().writer_type_,
                                              ctx.get_writer().writer_id_,
                                              ctx.get_seq_no()),
                           need_rollback_node)) {
    MDS_LOG_SET(WARN, "construct new node failed");
  } else if (MDS_FAIL(common::meta::move_or_copy_or_assign(std::forward<DATA>(data),
                                                           new_node->user_data_,
                                                           MdsAllocator::get_instance()))) {
    MDS_LOG_SET(WARN, "move_or_copy_or_assign user data failed");
  } else {
    CLICK();
    new_node->on_user_data_set_();
    if (!scn.is_max()) {// replay
      new_node->on_redo_(scn);
    }
    sorted_list_.insert(new_node);
    new_node->mds_ctx_ = &ctx;
    CLICK();
    ctx.record_written_node(new_node);
    if (scn == share::SCN::max_scn()) {
      report_event_("WRITE_NODE", *new_node);
    } else {
      report_event_("REPLAY_NODE", *new_node);
    }
  }
  // rollback logic
  if (OB_FAIL(ret)) {
    if (need_rollback_node) {
      MdsFactory::destroy(new_node);
      new_node = nullptr;
    }
    if (OB_ENTRY_EXIST == ret) {// example: repeat replay same CLOG more than one time
      ret = OB_SUCCESS;
    }
  }
  reset_mds_mem_check_thread_local_info();
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename DATA>
int MdsRow<K, V>::set(DATA &&data,
                      MdsCtx &ctx,
                      const RetryParam &retry_param,
                      const bool is_for_remove)
{
  #define PRINT_WRAPPER KR(ret), K(typeid(V).name()), K(ctx), K(retry_param), K(*this)
  int ret = OB_SUCCESS;
  bool write_conflict = true;
  MDS_TG(5_ms);
  if (!ctx.can_write()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_SET(WARN, "invalid mds ctx");
  } else {
    MdsWLockGuard lg(MdsRowBase<K, V>::lock_);// lock row
    if (sorted_list_.empty()) {// for now, all writting data are in memory
      write_conflict = false;
    } else {
      if (OB_LIKELY(sorted_list_.get_head().is_decided_())) {
        write_conflict = false;
      } else if (sorted_list_.get_head().get_writer_() == ctx.get_writer()) {
        write_conflict = false;
      }
    }
    UserMdsNode<K, V> *new_node = nullptr;
    if (OB_UNLIKELY(write_conflict)) {
      ret = OB_EAGAIN;
      if (retry_param.check_reach_print_interval_and_update()) {
        MDS_LOG_SET(INFO, "mds row write conflict");
      }
    } else if (MDS_FAIL(construct_insert_record_user_mds_node_(this,
                                                               std::forward<DATA>(data),
                                                               is_for_remove ? MdsNodeType::DELETE : MdsNodeType::SET,
                                                               share::SCN::max_scn(),
                                                               ctx))) {
      MDS_LOG_SET(WARN, "execute write op failed");
    } else {
      MDS_LOG_SET(DEBUG, "MdsRow set node success");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename DATA>
int MdsRow<K, V>::replay(DATA &&data,
                         MdsCtx &ctx,
                         const share::SCN scn,
                         const bool is_for_remove)
{
  #define PRINT_WRAPPER KR(ret), K(typeid(V).name()), K(ctx), K(scn), K(is_for_remove), K(*this)
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);
  if (!ctx.can_write() || !scn.is_valid() || scn.is_max()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_SET(ERROR, "invalid argument");
  } else {
    MdsWLockGuard lg(MdsRowBase<K, V>::lock_);
    if (MDS_FAIL(construct_insert_record_user_mds_node_(this,
                                                        std::forward<DATA>(data),
                                                        is_for_remove ? MdsNodeType::DELETE : MdsNodeType::SET,
                                                        scn,
                                                        ctx))) {
      MDS_LOG_SET(WARN, "fail to constrtuct data node");
      if (OB_ENTRY_NOT_EXIST == ret) {// may replay multi times
        ret = OB_SUCCESS;
        MDS_LOG_SET(INFO, "this node has been replayed");
      }
    } else {
      MDS_LOG_SET(TRACE, "MdsRow replay node success");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename READ_OP, typename CHECK_NODE_OP>
struct CheckAndReadOP {
  CheckAndReadOP(bool &read_op_applied,
                 READ_OP &read_op,
                 CHECK_NODE_OP &check_node_op)
  : read_op_applied_(read_op_applied),
  read_op_(read_op),
  check_node_op_(check_node_op) {}
  template <typename K, typename V>
  int operator()(const UserMdsNode<K, V> &node) {
    #define PRINT_WRAPPER KR(ret), K(typeid(V).name())
    int ret = OB_SUCCESS;
    bool can_read = false;
    MDS_ASSERT(!node.is_aborted_());// should not meet aborted node, cause aborted node deleted immediately
    if (OB_FAIL(check_node_op_(node, can_read))) {
      MDS_LOG_GET(WARN, "check node can read meet failed");
    } else if (can_read) {
      if (node.get_node_type() == MdsNodeType::DELETE) {
        ret = OB_ENTRY_NOT_EXIST;
        MDS_LOG_GET(INFO, "data is marked removed, return not exist");
      } else if (node.get_node_type() == MdsNodeType::SET) {
        read_op_applied_ = true;
        if (OB_FAIL(read_op_(node))) {
          MDS_LOG_GET(WARN, "call user read op failed");
        }
      } else {
        MDS_LOG_GET(ERROR, "meet unknown mds node type");
      }
      if (OB_SUCC(ret)) {
        ret = OB_ITER_END;// break for_each
      }
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  bool &read_op_applied_;
  READ_OP &read_op_;
  CHECK_NODE_OP &check_node_op_;
};
template <typename K, typename V>
template <typename READ_OP, typename CHECK_NODE_OP>
int MdsRow<K, V>::get_with_read_wrapper_(READ_OP &&read_operation,
                                         CHECK_NODE_OP &&check_node_op) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(typeid(V).name())
  int ret = OB_SUCCESS;
  bool read_op_applied = false;
  if (OB_FAIL(sorted_list_.for_each_node(CheckAndReadOP<READ_OP, CHECK_NODE_OP>(read_op_applied,
                                                                                read_operation,
                                                                                check_node_op)))) {
    MDS_LOG_GET(WARN, "fail to do for_each_node");
  } else if (!read_op_applied) {// no node satisfied
    ret = OB_SNAPSHOT_DISCARDED;
    MDS_LOG_GET(DEBUG, "there is no node can be read");
  }
  return ret;
  #undef PRINT_WRAPPER
}

struct CheckSnapshotOP {
  CheckSnapshotOP(const share::SCN snapshot, const RetryParam &retry_param)
  : snapshot_(snapshot),
  retry_param_(retry_param) {}
  template <typename K, typename V>
  int operator()(const UserMdsNode<K, V> &node, bool &can_read) {
    return check_node_snapshot(node, snapshot_, retry_param_, can_read);
  }
  const share::SCN snapshot_;
  const RetryParam &retry_param_;
};
template <typename K, typename V>
template <typename READ_OP>
int MdsRow<K, V>::get_snapshot(READ_OP &&read_operation,
                               const share::SCN snapshot,
                               const RetryParam &retry_param) const
{
  #define PRINT_WRAPPER KR(ret), K(snapshot), K(retry_param), K(*this)
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);
  MdsRLockGuard lg(MdsRowBase<K, V>::lock_);
  if (MDS_FAIL(get_with_read_wrapper_(read_operation, CheckSnapshotOP(snapshot, retry_param)))) {
    if (OB_EAGAIN != ret && OB_SNAPSHOT_DISCARDED != ret) {
      MDS_LOG_GET(WARN, "MdsRow get_snapshot failed");
    }
  } else {
    MDS_LOG_GET(DEBUG, "MdsRow get_snapshot success");
  }
  return ret;
  #undef PRINT_WRAPPER
}

struct CheckLatestOP {
  template <typename K, typename V>
  int operator()(const UserMdsNode<K, V> &, bool &can_read) { can_read = true; return OB_SUCCESS; }
};
template <typename K, typename V>
template <typename READ_OP>
int MdsRow<K, V>::get_latest(READ_OP &&read_operation) const
{
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);
  MdsRLockGuard lg(MdsRowBase<K, V>::lock_);
  if (MDS_FAIL(get_with_read_wrapper_(read_operation, CheckLatestOP()))) {
    if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
      MDS_LOG_GET(WARN, "MdsRow get_latest failed");
    }
  } else {
    MDS_LOG_GET(TRACE, "MdsRow get_latest success");
  }
  return ret;
  #undef PRINT_WRAPPER
}

struct CheckNodeSnapshotAndSeqNoOP {
  CheckNodeSnapshotAndSeqNoOP(const MdsWriter &writer,
                              const share::SCN snapshot,
                              const transaction::ObTxSEQ read_seq,
                              const RetryParam &retry_param)
  : writer_(writer),
  snapshot_(snapshot),
  read_seq_(read_seq),
  retry_param_(retry_param) {}
  template <typename K, typename V>
  int operator()(const UserMdsNode<K, V> &node, bool &can_read) {
    int ret = OB_SUCCESS;
    if (node.get_writer_() != writer_) {// check snapshot
      ret = check_node_snapshot(node, snapshot_, retry_param_, can_read);
    } else {// check seq_no
      if (node.seq_no_ < read_seq_) {
        can_read = true;
      }// otherwise, keep looking older node
    }
    return ret;
  }
  const MdsWriter &writer_;
  const share::SCN snapshot_;
  const transaction::ObTxSEQ read_seq_;
  const RetryParam &retry_param_;
};
template <typename K, typename V>
template <typename READ_OP>
int MdsRow<K, V>::get_by_writer(READ_OP &&read_operation,
                                const MdsWriter &writer,
                                const share::SCN snapshot,
                                const transaction::ObTxSEQ read_seq,
                                const RetryParam &retry_param) const
{
  #define PRINT_WRAPPER KR(ret), K(writer), K(snapshot), K(read_seq), K(retry_param), K(*this)
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);
  MdsRLockGuard lg(MdsRowBase<K, V>::lock_);
  if (MDS_FAIL(get_with_read_wrapper_(read_operation,
                                      CheckNodeSnapshotAndSeqNoOP(writer,
                                                                  snapshot,
                                                                  read_seq,
                                                                  retry_param)))) {
    if (OB_UNLIKELY(OB_EAGAIN != ret && OB_SNAPSHOT_DISCARDED != ret)) {
      MDS_LOG_GET(WARN, "MdsRow get_by_writer failed");
    }
  } else {
    MDS_LOG_GET(DEBUG, "MdsRow get_by_writer success");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V, typename DUMP_OP>
struct DumpNodeOP {
  DumpNodeOP(const MdsRow<K, V> &row,
             DUMP_OP &dump_op,
             const uint8_t mds_table_id,
             const uint8_t mds_unit_id,
             MdsDumpKV &dump_kv,
             const share::SCN flush_scn,
             const bool for_flush,
             const ScanNodeOrder order)
  : row_(row),
  dump_op_(dump_op),
  mds_table_id_(mds_table_id),
  mds_unit_id_(mds_unit_id),
  dump_kv_(dump_kv),
  flush_scn_(flush_scn),
  for_flush_(for_flush),
  order_(order),
  state_() {}
  TO_STRING_KV(K_(dump_kv), K_(flush_scn), K_(for_flush), K_(order));
  struct ScanState {
    enum STATE {
      FILTERING = 0,
      DOING,
      FINISHING,
    };
    ScanState() : state_(STATE::FILTERING) {}
    int advance(STATE new_state) {
      int ret = OB_SUCCESS;
      MDS_ASSERT(new_state >= state_);
      if (new_state < state_) {
        ret = OB_STATE_NOT_MATCH;
      } else {
        state_ = new_state;
      }
      return ret;
    }
    STATE state_;
  };
  int dump_node_(const UserMdsNode<K, V> &node) {
    #define PRINT_WRAPPER KR(ret), K(*this)
    int ret = OB_SUCCESS;
    MDS_TG(1_ms);
    if (MDS_FAIL(dump_kv_.v_.init(mds_table_id_,
                                  mds_unit_id_,
                                  node,
                                  MdsAllocator::get_instance()))) {
      MDS_LOG_SCAN(WARN, "failt to convert user mds node to dump node", K(node));
    } else if (MDS_FAIL(dump_op_(dump_kv_))) {
      MDS_LOG_SCAN(WARN, "failt to apply op on dump node", K(node));
    } else if (for_flush_) {
      row_.report_event_("DUMP_NODE_FOR_FLUSH", node);
    } else {
      // report_event_("DUMP_NODE", node);
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  int scan_from_new_to_old_(const UserMdsNode<K, V> &node) {
    int ret = OB_SUCCESS;
    if (!check_node_scn_beflow_flush(node, flush_scn_)) {// skip
    } else if (!node.is_dumped_()) {// need do dump action
      if (OB_FAIL(state_.advance(ScanState::STATE::DOING))) {
      } else {
        ret = dump_node_(node);
      }
    } else {// need break or keep doing
      if (OB_FAIL(state_.advance(ScanState::STATE::FINISHING))) {
      } else if (for_flush_) {// just break
        ret = OB_ITER_END;
      } else {// need keep doing scan more
        ret = dump_node_(node);
      }
    }
    return ret;
  }
  int scan_from_old_to_new_(const UserMdsNode<K, V> &node) {
    int ret = OB_SUCCESS;
    if (node.is_dumped_()) {// skip
    } else if (check_node_scn_beflow_flush(node, flush_scn_)) {// need do dump action
      if (OB_FAIL(state_.advance(ScanState::STATE::DOING))) {
      } else {
        ret = dump_node_(node);
      }
    } else {// need break or keep doing
      if (OB_FAIL(state_.advance(ScanState::STATE::FINISHING))) {
      } else if (for_flush_) {// just break
        ret = OB_ITER_END;
      } else {// need keep doing scan more
        ret = dump_node_(node);
      }
    }
    return ret;
  }
  int operator()(const UserMdsNode<K, V> &node) {
    int ret = OB_SUCCESS;
    if (ScanNodeOrder::FROM_NEW_TO_OLD == order_) {
      ret = scan_from_new_to_old_(node);
    } else if (ScanNodeOrder::FROM_OLD_TO_NEW == order_) {
      ret = scan_from_old_to_new_(node);
    } else {
      MDS_ASSERT(false);
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  const MdsRow<K, V> &row_;
  DUMP_OP &dump_op_;
  const uint8_t mds_table_id_;
  const uint8_t mds_unit_id_;
  MdsDumpKV &dump_kv_;
  const share::SCN flush_scn_;
  const bool for_flush_;
  const ScanNodeOrder order_;
  ScanState state_;
};
template <typename K, typename V>
template <typename DUMP_OP>
int MdsRow<K, V>::scan_nodes_to_dump(DUMP_OP &&op,
                                     const uint8_t mds_table_id,
                                     const uint8_t mds_unit_id,
                                     MdsDumpKV &dump_kv,
                                     const share::SCN &flush_scn,
                                     const bool for_flush,
                                     const ScanNodeOrder order) const
{
  #define PRINT_WRAPPER KR(ret), K(dump_kv), K(flush_scn), K(for_flush)
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);
  DumpNodeOP<K, V, DUMP_OP> dump_node_op(*this, op, mds_table_id, mds_unit_id, dump_kv, flush_scn, for_flush, order);
  MdsRLockGuard lg(MdsRowBase<K, V>::lock_);
  if (ScanNodeOrder::FROM_NEW_TO_OLD == order) {
    ret = sorted_list_.for_each_node(dump_node_op);
  } else if (ScanNodeOrder::FROM_OLD_TO_NEW == order) {
    ret = sorted_list_.reverse_for_each_node(dump_node_op);
  } else {
    MDS_ASSERT(false);
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_FAIL(ret)) {
    MDS_LOG_SCAN(WARN, "fail to apply op on mds node", K(*this));
  } else {
    MDS_LOG_SCAN(TRACE, "scan row", K(*this));
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
void MdsRow<K, V>::node_abort_callback_(ListNodeBase *node)
{
  #define PRINT_WRAPPER KR(ret), KPC(node)
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  UserMdsNode<K, V> *cast_node = ((UserMdsNode<K, V>*)(ListNode<UserMdsNode<K, V>>*)(node));
  if (OB_ISNULL(cast_node)) {
    MDS_LOG_GC(ERROR, "down cast failed!", K(*this), KP(cast_node));
  } else if (!cast_node->is_aborted_()) {
    MDS_LOG_GC(ERROR, "node state is not aborted", K(*this), KP(cast_node));
  } else {
    sorted_list_.del(node);
    MdsFactory::destroy(cast_node);
  }
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
int64_t MdsRow<K, V>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  MdsTableBase *p_mds_table = nullptr;
  if (MdsRowBase<K, V>::p_mds_unit_) {
    p_mds_table = MdsRowBase<K, V>::p_mds_unit_->p_mds_table_;
  }
  databuff_printf(buf, buf_len, pos, "sorted_list={%s}, ", to_cstring(sorted_list_));
  databuff_printf(buf, buf_len, pos, "mds_table={%s}, ", p_mds_table ? to_cstring(*p_mds_table) : "NULL");
  databuff_printf(buf, buf_len, pos, "key={%s}", MdsRowBase<K, V>::key_ ? to_cstring(* MdsRowBase<K, V>::key_) : "NULL");
  return pos;
}

struct FillVirtualInfoOP {
  FillVirtualInfoOP(ObString key_str,
                    ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array,
                    const int64_t unit_id)
  : key_str_(key_str),
  idx_(0),
  mds_node_info_array_(mds_node_info_array),
  unit_id_(unit_id) {}
  template <typename K, typename V>
  int operator()(const UserMdsNode<K, V> &node) {
    #define PRINT_WRAPPER KR(ret), K_(key_str), K_(idx), K_(unit_id), K_(mds_node_info_array)
    int ret = OB_SUCCESS;
    MDS_TG(5_ms);
    if (MDS_FAIL(mds_node_info_array_.push_back(MdsNodeInfoForVirtualTable()))) {
      MDS_LOG_SCAN(WARN, "fail to push new element to array");
    } else {
      MdsNodeInfoForVirtualTable &virtual_info = mds_node_info_array_.at(mds_node_info_array_.count() - 1);
      if (MDS_FAIL(virtual_info.user_key_.assign(key_str_))) {
        MDS_LOG_SCAN(WARN, "fail to construct ObStringHolder");
      } else if (MDS_FAIL(node.fill_virtual_info(virtual_info))) {
        MDS_LOG_SCAN(WARN, "fail to fill virtual info from mds node");
      } else {
        virtual_info.version_idx_ = idx_++;
        virtual_info.unit_id_ = unit_id_;
      }
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  ObString key_str_;
  int64_t idx_;
  ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array_;
  const int64_t unit_id_;
};
template <typename K, typename V>
template <typename Key>
int MdsRow<K, V>::fill_virtual_info(const Key &key,
                                    ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array,
                                    const int64_t unit_id) const
{
  #define PRINT_WRAPPER KR(ret), K(key), KPC(this)
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);
  MdsRLockGuard lg(MdsRowBase<K, V>::lock_);
  CLICK();
  constexpr int64_t buffer_size = 512_B;
  char stack_buffer[buffer_size] = { 0 };
  int64_t pos = 0;
  if (FALSE_IT(databuff_printf(stack_buffer, buffer_size, pos, "%s", to_cstring(key)))) {
  } else if (OB_FAIL(sorted_list_.for_each_node(FillVirtualInfoOP(ObString(pos, stack_buffer), mds_node_info_array, unit_id)))) {
    MDS_LOG_SCAN(WARN, "failed to fill virtual info");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <int N>
void MdsRow<K, V>::report_event_(const char (&event_str)[N],
                                 const UserMdsNode<K, V> &node,
                                 const char *file,
                                 const uint32_t line,
                                 const char *function_name) const
{
  int ret = OB_SUCCESS;
  constexpr int64_t buffer_size = 1_KB;
  char stack_buffer[buffer_size] = { 0 };
  observer::MdsEvent event;
  if (nullptr == MdsRowBase<K, V>::p_mds_unit_ ||
      nullptr == MdsRowBase<K, V>::p_mds_unit_->p_mds_table_) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(ERROR, "not expected ptr", K(*this));
  } else if (OB_FAIL(node.fill_event_(event, event_str, stack_buffer, buffer_size))) {
    MDS_LOG(WARN, "fail fill mds event", K(*this));
  } else {
    observer::MdsEventKey key(MTL_ID(),
                              MdsRowBase<K, V>::p_mds_unit_->p_mds_table_->ls_id_,
                              MdsRowBase<K, V>::p_mds_unit_->p_mds_table_->tablet_id_);
    observer::ObMdsEventBuffer::append(key, event, MdsRowBase<K, V>::p_mds_unit_->p_mds_table_, file, line, function_name);
  }
}

}
}
}
#endif