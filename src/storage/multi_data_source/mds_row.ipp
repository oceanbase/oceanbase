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

#include "share/ob_errno.h"
#include "storage/multi_data_source/mds_table_base.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
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
MdsRow<K, V>::~MdsRow()// all mds nodes lived with RAII, owned by MdsRow
{
  MdsWLockGuard lg(MdsRowBase<K, V>::lock_);
  if (!sorted_list_.empty()) {
    MDS_LOG_RET(WARN, OB_SUCCESS, "release mds mode when mds row destructed", K(*this));
    sorted_list_.for_each_node_from_tail_to_head_until_true(
      [this](const UserMdsNode<K, V> &node) {
        UserMdsNode<K, V> &cast_node = const_cast<UserMdsNode<K, V> &>(node);
        sorted_list_.del((ListNodeBase*)(ListNode<UserMdsNode<K, V>>*)&cast_node);
        MdsFactory::destroy(&cast_node);
        return false;
      }
    );
  }
}

template <typename K, typename V>
template <typename OPERATION>
int MdsRow<K, V>::for_each_node_(OPERATION &&op) const
{
  int ret = 0;
  sorted_list_.for_each_node_from_head_to_tail_until_true(
    [&ret, &op](const UserMdsNode<K, V> &node) {
      ret = op(node);
      return OB_SUCCESS != ret;
    }
  );
  return ret;
}

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
#ifndef UNITTEST_DEBUG
  set_mds_mem_check_thread_local_info(mds_row->p_mds_unit_->p_mds_table_->ls_id_,
                                      mds_row->p_mds_unit_->p_mds_table_->tablet_id_,
                                      typeid(new_node).name());
#endif
  // if this is an insert action, check is same scn mds node exists
  if (scn != share::SCN::max_scn() && MDS_FAIL(for_each_node_(
    [scn](const UserMdsNode<K, V> &node) {
      int ret = OB_SUCCESS;
      if (scn == node.redo_scn_) {
        ret = OB_ENTRY_EXIST;
      }
      return ret;
    }
  ))) {
    MDS_LOG_SET(WARN, "scn is not max, this is an insert action, but node with same scn exists");
  } else if (MDS_FAIL_FLAG(MdsFactory::create(new_node,
                                              mds_row,
                                              node_type,
                                              ctx.get_writer().writer_type_,
                                              ctx.get_writer().writer_id_),
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
    if (OB_ENTRY_EXIST == ret) {
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

// this function must be called from for_each_lock_ method with lock protection.
template <typename K, typename V>
int MdsRow<K, V>::check_node_snapshot_(const UserMdsNode<K, V> &node,
                                       const share::SCN snapshot,
                                       const RetryParam &retry_param,
                                       bool &can_read) const
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
template <typename READ_OP, typename CHECK_NODE_OP>
int MdsRow<K, V>::get_with_read_wrapper_(READ_OP &&read_operation,
                                         CHECK_NODE_OP &&check_node_op) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(typeid(V).name())
  int ret = OB_SUCCESS;
  bool read_op_applied = false;
  auto read_op_wrapper = [&read_operation, &read_op_applied](const UserMdsNode<K, V> &data) -> int {
    read_op_applied = true;
    return read_operation(data);
  };
  for_each_node_([&](const UserMdsNode<K, V> &node) {
    bool can_read = false;
    MDS_ASSERT(!node.is_aborted_());// should not meet aborted node, cause aborted node deleted immediately
    if (OB_FAIL(check_node_op(node, can_read))) {
      MDS_LOG_GET(WARN, "check node can read meet failed");
    } else if (can_read) {
      if (node.get_node_type() == MdsNodeType::DELETE) {
        ret = OB_ENTRY_NOT_EXIST;
        MDS_LOG_GET(INFO, "data is marked removed, return not exist");
      } else if (node.get_node_type() == MdsNodeType::SET) {
        if (OB_FAIL(read_op_wrapper(node))) {
          MDS_LOG_GET(WARN, "call user read op failed");
        }
      } else {
        MDS_LOG_GET(ERROR, "meet unknown mds node type");
      }
    }
    return OB_FAIL(ret) || can_read;// either fail or read already, stop iter
  });
  if (OB_SUCC(ret)) {
    if (!read_op_applied) {// no node satisfied
      ret = OB_SNAPSHOT_DISCARDED;
      MDS_LOG_GET(DEBUG, "there is no node can be read");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename READ_OP>
int MdsRow<K, V>::get_snapshot(READ_OP &&read_operation,
                               const share::SCN snapshot,
                               const int64_t read_seq,
                               const RetryParam &retry_param) const
{
  #define PRINT_WRAPPER KR(ret), K(snapshot), K(read_seq), K(retry_param), K(*this)
  int ret = OB_SUCCESS;
  UNUSED(read_seq);
  MDS_TG(5_ms);
  MdsRLockGuard lg(MdsRowBase<K, V>::lock_);
  if (MDS_FAIL(get_with_read_wrapper_(read_operation,
    [this, snapshot, &retry_param](const UserMdsNode<K, V> &node, bool &can_read) -> int {
      return check_node_snapshot_(node, snapshot, retry_param, can_read);
    }
  ))) {
    if (OB_EAGAIN != ret && OB_SNAPSHOT_DISCARDED != ret) {
      MDS_LOG_GET(WARN, "MdsRow get_snapshot failed");
    }
  } else {
    MDS_LOG_GET(DEBUG, "MdsRow get_snapshot success");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename READ_OP>
int MdsRow<K, V>::get_latest(READ_OP &&read_operation, const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(read_seq), K(*this)
  int ret = OB_SUCCESS;
  UNUSED(read_seq);
  MDS_TG(5_ms);
  MdsRLockGuard lg(MdsRowBase<K, V>::lock_);
  if (MDS_FAIL(get_with_read_wrapper_(read_operation,
    [&](const UserMdsNode<K, V> &, bool &can_read) -> int {
      can_read = true;
      return OB_SUCCESS;
    }
  ))) {
    if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
      MDS_LOG_GET(WARN, "MdsRow get_latest failed");
    }
  } else {
    MDS_LOG_GET(TRACE, "MdsRow get_latest success");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename READ_OP>
int MdsRow<K, V>::get_by_writer(READ_OP &&read_operation,
                                const MdsWriter &writer,
                                const share::SCN snapshot,
                                const int64_t read_seq,
                                const RetryParam &retry_param) const
{
  #define PRINT_WRAPPER KR(ret), K(writer), K(snapshot), K(read_seq), K(retry_param), K(*this)
  int ret = OB_SUCCESS;
  UNUSED(read_seq);
  MDS_TG(5_ms);
  MdsRLockGuard lg(MdsRowBase<K, V>::lock_);
  if (MDS_FAIL(get_with_read_wrapper_(read_operation,
    [this, snapshot, &writer, &retry_param](const UserMdsNode<K, V> &node, bool &can_read) -> int {
      int ret = OB_SUCCESS;
      if (node.get_writer_() == writer) {
        can_read = true;
      } else {
        ret = check_node_snapshot_(node, snapshot, retry_param, can_read);
      }
      return ret;
    }
  ))) {
    if (OB_UNLIKELY(OB_EAGAIN != ret && OB_SNAPSHOT_DISCARDED != ret)) {
      MDS_LOG_GET(WARN, "MdsRow get_by_writer failed");
    }
  } else {
    MDS_LOG_GET(DEBUG, "MdsRow get_by_writer success");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename DUMP_OP>
int MdsRow<K, V>::scan_dump_node_from_tail_to_head(DUMP_OP &&op,
                                                   const uint8_t mds_table_id,
                                                   const uint8_t mds_unit_id,
                                                   MdsDumpKV &dump_kv,
                                                   const share::SCN &flush_scn,
                                                   const bool for_flush) const
{
  #define PRINT_WRAPPER KR(ret), K(dump_kv), K(flush_scn), K(for_flush)
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);
  MdsRLockGuard lg(MdsRowBase<K, V>::lock_);
  CLICK();
  bool has_meet_undump_node = false;
  sorted_list_.for_each_node_from_tail_to_head_until_true(// from old to new
    [this, &op, &ret, &dump_kv, flush_scn, for_flush, mds_table_id, mds_unit_id, &has_meet_undump_node](const UserMdsNode<K, V> &node) {
      bool need_break = false;
      MDS_TG(5_ms);
      MDS_ASSERT(!node.is_aborted_());// should not see aborted node, cause it is deleted immediatly
      if (!node.is_dumped_()) {
        has_meet_undump_node = true;// this is a barrier, mark it to defense
      }
      if (!node.is_committed_()) {
        if (node.redo_scn_.is_valid() && !flush_scn.is_max()) {
          MDS_ASSERT(node.redo_scn_ > flush_scn);// defense
        }
        need_break = true;
      } else if (node.is_dumped_() && for_flush) {// just skip it
        // all nodes before first undumped node should be dumped status,(reverse scan order)
        // and all nodes after first undumped node should be undemped status(reverse scan order)
        MDS_ASSERT(has_meet_undump_node == false);// defense
      } else if (!check_node_scn_beflow_flush(node, flush_scn)) {
        need_break = true;
      } else if (MDS_FAIL(dump_kv.v_.init(mds_table_id,
                                          mds_unit_id,
                                          node,
                                          DefaultAllocator::get_instance()))) {
        MDS_LOG_SCAN(WARN, "failt to convert user mds node to dump node", K(node));
      } else if (MDS_FAIL(op(dump_kv))) {
        MDS_LOG_SCAN(WARN, "failt to apply op on dump node", K(node));
      } else if (for_flush) {
        report_event_("DUMP_NODE_FOR_FLUSH", node);
      } else {
        // report_event_("DUMP_NODE", node);
      }
      return need_break || OB_SUCCESS != ret;
    }
  );
  if (OB_FAIL(ret)) {
    MDS_LOG_SCAN(WARN, "failt to apply op on mds node", K(*this));
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
  UserMdsNode<K, V> *cast_node = dynamic_cast<UserMdsNode<K, V>*>(node);
  if (OB_ISNULL(cast_node)) {
    MDS_LOG_GC(ERROR, "down cast failed!", K(*this), KP(cast_node));
  } else if (!cast_node->is_aborted_()) {
    MDS_LOG_GC(ERROR, "node state is not aborted", K(*this), KP(cast_node));
  } else {
    bool has_meet_input_arg_node = false;
    sorted_list_.for_each_node_from_head_to_tail_until_true([this,
                                                             cast_node,
                                                             &has_meet_input_arg_node]
                                                             (const UserMdsNode<K, V> &node) {
      bool ret = false;
      if (&node == cast_node) {
        has_meet_input_arg_node = true;
        sorted_list_.del((ListNodeBase*)(ListNode<UserMdsNode<K, V>>*)&node);
        MdsFactory::destroy(const_cast<UserMdsNode<K, V>*>(&node));
        ret = true;
      }
      return ret;
    });
    MDS_ASSERT(has_meet_input_arg_node);
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
  int idx = 0;
  constexpr int64_t buffer_size = 512_B;
  char stack_buffer[buffer_size] = { 0 };
  int64_t pos = 0;
  if (FALSE_IT(databuff_printf(stack_buffer, buffer_size, pos, "%s", to_cstring(key)))) {
  } else {
    ObString key_str(pos, stack_buffer);
    sorted_list_.for_each_node_from_head_to_tail_until_true([&ret, &key, &idx, &mds_node_info_array, &key_str, unit_id, this](const UserMdsNode<K, V> &node) {
      MDS_TG(5_ms);
      if (MDS_FAIL(mds_node_info_array.push_back(MdsNodeInfoForVirtualTable()))) {
        MDS_LOG_SCAN(WARN, "fail to push new element to array");
      } else {
        MdsNodeInfoForVirtualTable &virtual_info = mds_node_info_array.at(mds_node_info_array.count() - 1);
        if (MDS_FAIL(virtual_info.user_key_.assign(key_str))) {
          MDS_LOG_SCAN(WARN, "fail to construct ObStringHolder");
        } else if (MDS_FAIL(node.fill_virtual_info(virtual_info))) {
          MDS_LOG_SCAN(WARN, "fail to fill virtual info from mds node");
        } else {
          virtual_info.version_idx_ = idx++;
          virtual_info.unit_id_ = unit_id;
        }
      }
      return OB_SUCCESS != ret;
    });
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
    observer::ObMdsEventBuffer::append(key, event, file, line, function_name);
  }
}

}
}
}
#endif