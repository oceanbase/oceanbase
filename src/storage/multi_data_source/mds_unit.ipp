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
#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_UNIT_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_UNIT_IPP

#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "ob_clock_generator.h"
#include "observer/virtual_table/ob_mds_event_buffer.h"
#include "share/ob_errno.h"
#include "runtime_utility/mds_retry_control.h"
#include <type_traits>
#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_UNIT_H_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_UNIT_H_IPP
#include "mds_unit.h"
#endif

namespace oceanbase
{
namespace storage
{
namespace mds
{
/************************************MULTI ROW METHOD DEFINATION***********************************/

template <typename K, typename V>
template <typename ValueType>
struct MdsUnit<K, V>::IteratorBase {// Bidirectional iterators
  // iterator traits
  using difference_type = int64_t;
  using value_type = ValueType;
  using pointer = ValueType *;
  using reference = ValueType &;
  using iterator_category = std::bidirectional_iterator_tag;
  using row_type = Row<K, V>;// extra
  // method needed define
  IteratorBase() : p_kv_(nullptr) {}
  IteratorBase(ValueType *val) : p_kv_(val) {}
  IteratorBase(const IteratorBase<ValueType> &rhs) : p_kv_(rhs.p_kv_) {}
  IteratorBase<ValueType> &operator=(const IteratorBase<ValueType> &rhs) { p_kv_ = rhs.p_kv_; return *this; }
  bool operator==(const IteratorBase<ValueType> &rhs) { return p_kv_ == rhs.p_kv_; }
  bool operator!=(const IteratorBase<ValueType> &rhs) { return p_kv_ != rhs.p_kv_; }
  ValueType &operator*() { return *p_kv_; }
  ValueType *operator->() { return p_kv_; }
  KvPair<K, Row<K, V>> *p_kv_;
};

template <typename K, typename V>
template <typename ValueType>
struct MdsUnit<K, V>::NormalIterator : public IteratorBase<ValueType> {
  NormalIterator() : IteratorBase<ValueType>() {}
  NormalIterator(const NormalIterator &rhs) : IteratorBase<ValueType>(rhs) {}
  NormalIterator(ValueType *val) : IteratorBase<ValueType>(val) {}
  NormalIterator<ValueType> &operator++() {
    IteratorBase<ValueType>::p_kv_ = (KvPair<K, Row<K, V>> *)IteratorBase<ValueType>::p_kv_->next();
    return *this;
  }
  NormalIterator<ValueType> operator++(int) {
    NormalIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
  NormalIterator<ValueType> &operator--() {
    IteratorBase<ValueType>::p_kv_ = (KvPair<K, Row<K, V>> *)IteratorBase<ValueType>::p_kv_->prev();
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
struct MdsUnit<K, V>::ReverseIterator : public IteratorBase<ValueType> {
  ReverseIterator() : IteratorBase<ValueType>() {}
  ReverseIterator(const ReverseIterator &rhs) : IteratorBase<ValueType>(rhs) {}
  ReverseIterator(ValueType *val) : IteratorBase<ValueType>(val) {}
  ReverseIterator<ValueType> &operator++() {
    IteratorBase<ValueType>::p_kv_ = (KvPair<K, Row<K, V>> *)IteratorBase<ValueType>::p_kv_->prev();
    return *this;
  }
  ReverseIterator<ValueType> operator++(int) {
    ReverseIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
  ReverseIterator<ValueType> &operator--() {
    IteratorBase<ValueType>::p_kv_ = (KvPair<K, Row<K, V>> *)IteratorBase<ValueType>::p_kv_->next();
    return *this;
  }
  ReverseIterator<ValueType> operator--(int) {
    ReverseIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
};

template <typename K, typename V>
typename MdsUnit<K, V>::iterator MdsUnit<K, V>::begin()
{ return iterator((KvPair<K, Row<K, V>>*)multi_row_list_.list_head_); }
template <typename K, typename V>
typename MdsUnit<K, V>::iterator MdsUnit<K, V>::end()
{ return iterator(nullptr); }
template <typename K, typename V>
typename MdsUnit<K, V>::const_iterator MdsUnit<K, V>::cbegin()
{ return const_iterator((KvPair<K, Row<K, V>>*)multi_row_list_.list_head_); }
template <typename K, typename V>
typename MdsUnit<K, V>::const_iterator MdsUnit<K, V>::cend()
{ return const_iterator(nullptr); }
template <typename K, typename V>
typename MdsUnit<K, V>::reverse_iterator MdsUnit<K, V>::rbegin()
{ return reverse_iterator((KvPair<K, Row<K, V>>*)multi_row_list_.list_tail_); }
template <typename K, typename V>
typename MdsUnit<K, V>::reverse_iterator MdsUnit<K, V>::rend()
{ return reverse_iterator(nullptr); }
template <typename K, typename V>
typename MdsUnit<K, V>::const_reverse_iterator MdsUnit<K, V>::crbegin()
{ return const_reverse_iterator((KvPair<K, Row<K, V>>*)multi_row_list_.list_tail_); }
template <typename K, typename V>
typename MdsUnit<K, V>::const_reverse_iterator MdsUnit<K, V>::crend()
{ return const_reverse_iterator(nullptr); }

template <typename K, typename V>
MdsUnit<K, V>::MdsUnit() {}

template <typename K, typename V>
MdsUnit<K, V>::~MdsUnit()
{
  #define PRINT_WRAPPER KR(ret)
  int ret = OB_SUCCESS;
  if (!multi_row_list_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_DESTROY(WARN, "there are still valid data in this unit, they will be detroyed here");
    multi_row_list_.for_each_node_from_head_to_tail_until_true(
      [&ret, this](const KvPair<K, Row<K, V>> &kv_pair) {
      MDS_LOG_DESTROY(WARN, "destroy row when unit destroy", K(kv_pair));
      KvPair<K, Row<K, V>> &kv = const_cast<KvPair<K, Row<K, V>> &>(kv_pair);
      ListNode<KvPair<K, Row<K, V>>> *p_kv = static_cast<ListNode<KvPair<K, Row<K, V>>>*>(&kv);
      multi_row_list_.del(p_kv);
      MdsFactory::destroy(p_kv);
      return false;
    });
  }
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename OP>
int MdsUnit<K, V>::for_each_node_on_row(OP &&op) const
{
  #define PRINT_WRAPPER KR(ret)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsWLockGuard lg(lock_);
  CLICK();
  multi_row_list_.for_each_node_from_head_to_tail_until_true(
    [&op, &ret](const KvPair<K, Row<K, V>> &kv_row) {
    MDS_TG(1_ms);
    const K *p_k = &kv_row.k_;
    const Row<K, V> &row = kv_row.v_;
    if (MDS_FAIL(row.for_each_node_from_tail_to_head(std::forward<OP>(op)))) {
      MDS_LOG_SCAN(WARN, "fail to scan node on row", KPC(p_k), K(kv_row));
    }
    return OB_SUCCESS != ret;// keep scanning until meet failure
  });
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename OP>
int MdsUnit<K, V>::for_each_row(FowEachRowAction action_type, OP &&op)// node maybe recycled in this function
{
  #define PRINT_WRAPPER KR(ret)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsWLockGuard lg(lock_);
  CLICK();
  multi_row_list_.for_each_node_from_head_to_tail_until_true(
    [action_type, &op, &ret, this](const KvPair<K, Row<K, V>> &kv_row) mutable {
      MDS_TG(1_ms);
      const K *p_k = &kv_row.k_;
      const Row<K, V> &row = kv_row.v_;
      if (MDS_FAIL(op(row))) {// node maybe recycled inside op
        MDS_LOG_SCAN(WARN, "fail to scan row", KPC(p_k));
      }
      // CAUTIONS: not every path scan need recycle empty row, or maybe result some problem unexpected, for example:
      // CALCULATE_REC_SCN operation will lock rows inner op, but will not release locks after op executed done.
      // (to resolve replay out of order problem, if repaly concurrent happened with calculate rec_scn, without lock's protection, will finally get a wrong rec_scn)
      // but destroy mds_row will add row's lock inner destruction, which will resulting deadlock in same thread.
      // so only operations logic behaves like gc should recycle empty row.
      if (FowEachRowAction::RECYCLE == action_type || FowEachRowAction::RESET == action_type) {
        erase_kv_from_list_if_empty_(&const_cast<KvPair<K, Row<K, V>> &>(kv_row));
      }
      return OB_SUCCESS != ret;// keep scanning until meet failure
  });
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
KvPair<K, Row<K, V>> *MdsUnit<K, V>::get_row_from_list_(const K &key) const
{
  KvPair<K, Row<K, V>> *p_kv = nullptr;
  multi_row_list_.for_each_node_from_head_to_tail_until_true(
    [&p_kv, &key](const KvPair<K, Row<K, V>> &kv_pair) {
      if (kv_pair.k_ == key) {
        p_kv = &const_cast<KvPair<K, Row<K, V>> &>(kv_pair);
      }
      return nullptr != p_kv;
    }
  );
  return p_kv;
}

template <typename K, typename V>
int MdsUnit<K, V>::insert_empty_kv_to_list_(const K &key, KvPair<K, Row<K, V>> *&p_kv, MdsTableBase *p_mds_table)
{
  #define PRINT_WRAPPER KR(ret), K(key), K(typeid(K).name()), K(typeid(V).name())
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  set_mds_mem_check_thread_local_info(p_mds_table->ls_id_,
                                      p_mds_table->tablet_id_,
                                      typeid(p_kv).name());
  if (MDS_FAIL(MdsFactory::create(p_kv))) {
    MDS_LOG_SET(WARN, "MdsUnit create kv pair failed");
  } else if (FALSE_IT(p_kv->v_.p_mds_unit_ = this)) {
  } else if (MDS_FAIL(common::meta::copy_or_assign(key, p_kv->k_))) {
    MDS_LOG_SET(ERROR, "copy user key failed");
  } else {
    p_kv->v_.key_ = &(p_kv->k_);
    multi_row_list_.insert(p_kv);
  }
  reset_mds_mem_check_thread_local_info();
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
void MdsUnit<K, V>::erase_kv_from_list_if_empty_(KvPair<K, Row<K, V>> *p_kv)
{
  int ret = OB_SUCCESS;
  Row<K, V> &mds_row = p_kv->v_;
  if (mds_row.sorted_list_.empty()) {
    if (!multi_row_list_.check_node_exist(p_kv)) {
      MDS_LOG(ERROR, "mds row is not record in list", KPC(p_kv));
    } else {
      multi_row_list_.del(p_kv);
      MdsFactory::destroy(p_kv);
    }
  }
  return;
}

template <typename K, typename V>
int MdsUnit<K, V>::SetOP::operator()() {
  #define PRINT_WRAPPER KR(ret), K_(key), K_(value), K_(ctx), K_(retry_param), K_(is_for_remove)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  KvPair<K, Row<K, V>> *p_kv = this_->get_row_from_list_(key_);
  if (OB_ISNULL(p_kv)) {
    if (MDS_FAIL(this_->insert_empty_kv_to_list_(key_, p_kv, p_mds_table_))) {
      MDS_LOG_SET(WARN, "insert new key to unit failed");
    } else {
      MDS_LOG_SET(INFO, "insert new key to unit");
    }
  }
  if (OB_SUCC(ret)) {
    if (is_lvalue_) {
      ret = p_kv->v_.set(value_, ctx_, retry_param_, is_for_remove_);
    } else {
      ret = p_kv->v_.set(std::move(value_), ctx_, retry_param_, is_for_remove_);
    }
    if (MDS_FAIL(ret)) {
      this_->erase_kv_from_list_if_empty_(p_kv);// rollback
      MDS_LOG_SET(WARN, "MdsUnit set value failed");
    } else {
      MDS_LOG_SET(TRACE, "MdsUnit set value success");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}
template <typename K, typename V>
template <typename Value>
int MdsUnit<K, V>::set(MdsTableBase *p_mds_table,
                       const K &key,
                       Value &&value,
                       MdsCtx &ctx,
                       const int64_t lock_timeout_us,
                       const bool is_for_remove)
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  bool is_lvalue = std::is_lvalue_reference<decltype(value)>::value;
  RetryParam retry_param(lock_timeout_us);
  SetOP op(this, is_lvalue, p_mds_table, key, value, ctx, is_for_remove, retry_param);
  if (MDS_FAIL(retry_release_lock_with_op_until_timeout<LockMode::WRITE>(lock_, retry_param, op))) {
    if (OB_TIMEOUT == ret) {
      ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    }
  }
  return ret;
}

template <typename K, typename V>
template <typename Value>
int MdsUnit<K, V>::replay(MdsTableBase *p_mds_table,
                          const K &key,
                          Value &&value,
                          MdsCtx &ctx,
                          const share::SCN scn,
                          const bool is_for_remove)
{
  #define PRINT_WRAPPER KR(ret), K(key), K(value), K(ctx), K(scn), K(is_for_remove)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsWLockGuard lg(lock_);
  CLICK();
  KvPair<K, Row<K, V>> *p_kv = get_row_from_list_(key);
  if (OB_ISNULL(p_kv)) {
    if (MDS_FAIL(insert_empty_kv_to_list_(key, p_kv, p_mds_table))) {
      MDS_LOG_SET(WARN, "insert new key to unit failed");
    } else {
      MDS_LOG_SET(INFO, "insert new key to unit");
    }
  }
  if (OB_SUCC(ret)) {
    if (MDS_FAIL(p_kv->v_.replay(std::forward<Value>(value),
                                 ctx,
                                 scn,
                                 is_for_remove))) {
      erase_kv_from_list_if_empty_(p_kv);// rollback
      MDS_LOG_SET(WARN, "MdsUnit set value failed");
    } else {
      MDS_LOG_SET(TRACE, "MdsUnit set value success");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename OP>
int MdsUnit<K, V>::GetSnapShotOp<OP>::operator()() {
  #define PRINT_WRAPPER KR(ret), K_(key), K(typeid(OP).name()), K_(snapshot), K_(read_seq), K_(retry_param)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  KvPair<K, Row<K, V>> *p_kv = this_->get_row_from_list_(key_);
  if (OB_ISNULL(p_kv)) {
    ret = OB_ENTRY_NOT_EXIST;
    MDS_LOG_GET(WARN, "row key not exist");
  } else if (MDS_FAIL(p_kv->v_.get_snapshot(read_op_,
                                            snapshot_,
                                            read_seq_,
                                            retry_param_))) {
    if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret && OB_EAGAIN != ret)) {
      MDS_LOG_GET(WARN, "MdsUnit get_snapshot failed");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}
template <typename K, typename V>
template <typename OP>
int MdsUnit<K, V>::get_snapshot(const K &key,
                                OP &&read_op,
                                const share::SCN snapshot,
                                const int64_t read_seq,
                                const int64_t lock_timeout_us) const
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  RetryParam retry_param(lock_timeout_us);
  GetSnapShotOp<typename std::remove_reference<OP>::type> op(this, key, read_op, snapshot, read_seq, retry_param);
  if (MDS_FAIL(retry_release_lock_with_op_until_timeout<LockMode::READ>(lock_, retry_param, op))) {
    if (OB_TIMEOUT == ret) {
      ret = OB_ERR_SHARED_LOCK_CONFLICT;
    }
  }
  return ret;
}

template <typename K, typename V>
template <typename OP>
int MdsUnit<K, V>::GetByWriterOp<OP>::operator()() {
  #define PRINT_WRAPPER KR(ret), K_(key), K(typeid(OP).name()), K_(writer), K_(snapshot), K_(read_seq), K_(retry_param)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  KvPair<K, Row<K, V>> *p_kv = this_->get_row_from_list_(key_);
  if (OB_ISNULL(p_kv)) {
    ret = OB_ENTRY_NOT_EXIST;
    MDS_LOG_GET(WARN, "row key not exist");
  } else if (MDS_FAIL(p_kv->v_.get_by_writer(read_op_,
                                             writer_,
                                             snapshot_,
                                             read_seq_,
                                             retry_param_))) {
    if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
      MDS_LOG_GET(WARN, "MdsUnit get_by_writer failed");
    }
  } else {
    MDS_LOG_GET(TRACE, "MdsUnit get_by_writer success");
  }
  return ret;
  #undef PRINT_WRAPPER
}
template <typename K, typename V>
template <typename OP>
int MdsUnit<K, V>::get_by_writer(const K &key,
                                 OP &&read_op,
                                 const MdsWriter &writer,
                                 const share::SCN snapshot,
                                 const int64_t read_seq,
                                 const int64_t lock_timeout_us) const
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  RetryParam retry_param(lock_timeout_us);
  GetByWriterOp<typename std::remove_reference<OP>::type> op(this, key, read_op, writer, snapshot, read_seq, retry_param);
  if (MDS_FAIL(retry_release_lock_with_op_until_timeout<LockMode::READ>(lock_, retry_param, op))) {
    if (OB_TIMEOUT == ret) {
      ret = OB_ERR_SHARED_LOCK_CONFLICT;
    }
  }
  return ret;
}

template <typename K, typename V>
template <typename OP>
int MdsUnit<K, V>::get_latest(const K &key, OP &&read_op, const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(key), K(typeid(OP).name()), K(read_seq)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsRLockGuard lg(lock_);
  CLICK();
  KvPair<K, Row<K, V>> *p_kv = get_row_from_list_(key);
  if (OB_ISNULL(p_kv)) {
    ret = OB_ENTRY_NOT_EXIST;
    MDS_LOG_GET(WARN, "row key not exist");
  } else if (MDS_FAIL(p_kv->v_.get_latest(std::forward<OP>(read_op), read_seq))) {
    if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
      MDS_LOG_GET(WARN, "MdsUnit get_latest failed");
    }
  } else {
    MDS_LOG_GET(TRACE, "MdsUnit get_latest success");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename DUMP_OP>
int MdsUnit<K, V>::scan_KV_row(DUMP_OP &&op,
                               share::SCN &flush_scn,
                               const uint8_t mds_table_id,
                               const uint8_t mds_unit_id,
                               const bool for_flush) const
{
  #define PRINT_WRAPPER KR(ret), K(flush_scn), K(for_flush)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsRLockGuard lg(lock_);
  CLICK();
  MdsDumpKV kv;
  multi_row_list_.for_each_node_from_head_to_tail_until_true(
    [&op, flush_scn, &ret, &kv, mds_table_id, mds_unit_id, for_flush](const KvPair<K, Row<K, V>> &kv_row) {
      MDS_TG(10_ms);
      if (MDS_FAIL(kv.k_.init(mds_table_id,
                              mds_unit_id,
                              kv_row.k_,
                              DefaultAllocator::get_instance()))) {
        MDS_LOG_SCAN(WARN, "fail to init MdsDumpKey");
      } else if (MDS_FAIL(kv_row.v_.scan_dump_node_from_tail_to_head(op,
                                                                     mds_table_id,
                                                                     mds_unit_id,
                                                                     kv,
                                                                     flush_scn,
                                                                     for_flush))) {
        MDS_LOG_SCAN(WARN, "fail to scan dump node on row");
      }
      return OB_SUCCESS != ret;// keep scanning until meet failure
    }
  );
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
int MdsUnit<K, V>::fill_virtual_info(ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array, const int64_t unit_id) const
{
  #define PRINT_WRAPPER KR(ret)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsRLockGuard lg(lock_);
  CLICK();
  multi_row_list_.for_each_node_from_head_to_tail_until_true(
    [&ret, &mds_node_info_array, unit_id](const KvPair<K, Row<K, V>> &kv_row) {
      MDS_TG(10_ms);
      if (MDS_FAIL(kv_row.v_.fill_virtual_info(kv_row.k_, mds_node_info_array, unit_id))) {
        MDS_LOG_SCAN(WARN, "fail to fill virtual info");
      }
      return OB_SUCCESS != ret;// keep scanning until meet failure
    }
  );
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <int N>
void MdsUnit<K, V>::report_event_(const char (&event_str)[N],
                                  const K &key,
                                  const char *file,
                                  const uint32_t line,
                                  const char *function_name) const
{
  int ret = OB_SUCCESS;
  observer::MdsEvent event;
  constexpr int64_t buffer_size = 1_KB;
  char stack_buffer[buffer_size] = { 0 };
  int64_t pos = 0;
  if (FALSE_IT(databuff_printf(stack_buffer, buffer_size, pos, "%s", to_cstring(key)))) {
  } else {
    event.key_str_.assign(stack_buffer, pos);
    event.event_ = event_str;
    observer::MdsEventKey key(MTL_ID(),
                             MdsUnitBase<K, V>::p_mds_table_->ls_id_,
                             MdsUnitBase<K, V>::p_mds_table_->tablet_id_);
    observer::ObMdsEventBuffer::append(key, event, file, line, function_name);
  }
}

/************************************SIGNLE ROW METHOD DEFINATION**********************************/

template <typename V>
MdsUnit<DummyKey, V>::MdsUnit()
{
  single_row_.v_.key_ = &(single_row_.k_);
  single_row_.v_.p_mds_unit_ = this;
}

template <typename V>
template <typename ValueType>
struct MdsUnit<DummyKey, V>::IteratorBase {// Bidirectional iterators
  // iterator traits
  using difference_type = int64_t;
  using value_type = ValueType;
  using pointer = ValueType *;
  using reference = ValueType &;
  using iterator_category = std::bidirectional_iterator_tag;
  using row_type = Row<DummyKey, V>;// extra
  // method needed define
  IteratorBase() : p_kv_(nullptr) {}
  IteratorBase(ValueType *val) : p_kv_(val) {}
  IteratorBase(const IteratorBase<ValueType> &rhs) : p_kv_(rhs.p_kv_) {}
  IteratorBase<ValueType> &operator=(const IteratorBase<ValueType> &rhs) { p_kv_ = rhs.p_kv_; }
  bool operator==(const IteratorBase<ValueType> &rhs) { return p_kv_ == rhs.p_kv_; }
  bool operator!=(const IteratorBase<ValueType> &rhs) { return p_kv_ != rhs.p_kv_; }
  ValueType &operator*() { return *p_kv_; }
  ValueType *operator->() { return p_kv_; }
  KvPair<DummyKey, Row<DummyKey, V>> *p_kv_;
};

template <typename V>
template <typename ValueType>
struct MdsUnit<DummyKey, V>::NormalIterator : public IteratorBase<ValueType> {
  NormalIterator() : IteratorBase<ValueType>() {}
  NormalIterator(const NormalIterator &rhs) : IteratorBase<ValueType>(rhs) {}
  NormalIterator(ValueType *val) : IteratorBase<ValueType>(val) {}
  NormalIterator<ValueType> &operator++() {
    IteratorBase<ValueType>::p_kv_ = (KvPair<DummyKey, Row<DummyKey, V>> *)IteratorBase<ValueType>::p_kv_->next();
    return *this;
  }
  NormalIterator<ValueType> operator++(int) {
    NormalIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
  NormalIterator<ValueType> &operator--() {
    IteratorBase<ValueType>::p_kv_ = (KvPair<DummyKey, Row<DummyKey, V>> *)IteratorBase<ValueType>::p_kv_->prev();
    return *this;
  }
  NormalIterator<ValueType> operator--(int) {
    NormalIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
};

template <typename V>
template <typename ValueType>
struct MdsUnit<DummyKey, V>::ReverseIterator : public IteratorBase<ValueType> {
  ReverseIterator() : IteratorBase<ValueType>() {}
  ReverseIterator(const ReverseIterator &rhs) : IteratorBase<ValueType>(rhs) {}
  ReverseIterator(ValueType *val) : IteratorBase<ValueType>(val) {}
  ReverseIterator<ValueType> &operator++() {
    IteratorBase<ValueType>::p_kv_ = (KvPair<DummyKey, Row<DummyKey, V>> *)IteratorBase<ValueType>::p_kv_->prev();
    return *this;
  }
  ReverseIterator<ValueType> operator++(int) {
    ReverseIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
  ReverseIterator<ValueType> &operator--() {
    IteratorBase<ValueType>::p_kv_ = (KvPair<DummyKey, Row<DummyKey, V>> *)IteratorBase<ValueType>::p_kv_->next();
    return *this;
  }
  ReverseIterator<ValueType> operator--(int) {
    ReverseIterator<ValueType> ret_val = *this;
    ++(*this);
    return ret_val;
  }
};

template <typename V>
typename MdsUnit<DummyKey, V>::iterator MdsUnit<DummyKey, V>::begin()
{ return iterator(&single_row_); }
template <typename V>
typename MdsUnit<DummyKey, V>::iterator MdsUnit<DummyKey, V>::end()
{ return iterator(nullptr); }
template <typename V>
typename MdsUnit<DummyKey, V>::const_iterator MdsUnit<DummyKey, V>::cbegin()
{ return const_iterator(&single_row_); }
template <typename V>
typename MdsUnit<DummyKey, V>::const_iterator MdsUnit<DummyKey, V>::cend()
{ return const_iterator(nullptr); }
template <typename V>
typename MdsUnit<DummyKey, V>::reverse_iterator MdsUnit<DummyKey, V>::rbegin()
{ return reverse_iterator(&single_row_); }
template <typename V>
typename MdsUnit<DummyKey, V>::reverse_iterator MdsUnit<DummyKey, V>::rend()
{ return reverse_iterator(nullptr); }
template <typename V>
typename MdsUnit<DummyKey, V>::const_reverse_iterator MdsUnit<DummyKey, V>::crbegin()
{ return const_reverse_iterator(&single_row_); }
template <typename V>
typename MdsUnit<DummyKey, V>::const_reverse_iterator MdsUnit<DummyKey, V>::crend()
{ return const_reverse_iterator(nullptr); }

template <typename V>
int MdsUnit<DummyKey, V>::SetOP::operator()() {
  #define PRINT_WRAPPER KR(ret), K_(value), K_(ctx), K_(retry_param), K_(is_lvalue)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  if (is_lvalue_) {
    ret = this_->single_row_.v_.set(value_, ctx_, retry_param_);
  } else {
    ret = this_->single_row_.v_.set(std::move(value_), ctx_, retry_param_);
  }
  if (MDS_FAIL(ret)) {
    MDS_LOG_SET(WARN, "MdsUnit set value failed");
  } else {
    MDS_LOG_SET(TRACE, "MdsUnit set value success");
  }
  return ret;
  #undef PRINT_WRAPPER
}
template <typename V>
template <typename Value>
int MdsUnit<DummyKey, V>::set(MdsTableBase *p_mds_table,
                              Value &&value,
                              MdsCtx &ctx,
                              const int64_t lock_timeout_us)
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  RetryParam retry_param(lock_timeout_us);
  bool is_lvalue = std::is_lvalue_reference<decltype(value)>::value;
  SetOP op(this, is_lvalue, value, ctx, retry_param);
  if (MDS_FAIL(retry_release_lock_with_op_until_timeout<LockMode::WRITE>(lock_, retry_param, op))) {
    if (OB_TIMEOUT == ret) {
      ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    }
  }
  return ret;
}

template <typename V>
template <typename Value>
int  MdsUnit<DummyKey, V>::replay(MdsTableBase *p_mds_table,
                                  Value &&value,
                                  MdsCtx &ctx,
                                  const share::SCN scn)
{
  #define PRINT_WRAPPER KR(ret), K(value), K(ctx), K(scn)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsWLockGuard lg(lock_);
  CLICK();
  if (MDS_FAIL(single_row_.v_.replay(std::forward<Value>(value), ctx, scn))) {
    MDS_LOG_SET(WARN, "MdsUnit replay value failed");
  } else {
    MDS_LOG_SET(TRACE, "MdsUnit replay value success");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename V>
template <typename OP>
int MdsUnit<DummyKey, V>::GetSnapShotOp<OP>::operator()() {
  #define PRINT_WRAPPER KR(ret), K(typeid(OP).name()), K_(read_seq), K_(retry_param)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  if (MDS_FAIL(this_->single_row_.v_.get_snapshot(read_op_,
                                                  snapshot_,
                                                  read_seq_,
                                                  retry_param_))) {
    if (OB_SNAPSHOT_DISCARDED != ret) {
      MDS_LOG_GET(WARN, "MdsUnit get_snapshot failed");
    }
  } else {
    MDS_LOG_GET(TRACE, "MdsUnit get_snapshot success");
  }
  return ret;
  #undef PRINT_WRAPPER
}
template <typename V>
template <typename OP>
int MdsUnit<DummyKey, V>::get_snapshot(OP &&read_op,
                                       const share::SCN snapshot,
                                       const int64_t read_seq,
                                       const int64_t lock_timeout_us) const
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  RetryParam retry_param(lock_timeout_us);
  GetSnapShotOp<typename std::remove_reference<OP>::type> op(this, read_op, snapshot, read_seq, retry_param);
  if (MDS_FAIL(retry_release_lock_with_op_until_timeout<LockMode::READ>(lock_, retry_param, op))) {
    if (OB_TIMEOUT == ret) {
      ret = OB_ERR_SHARED_LOCK_CONFLICT;
    }
  }
  return ret;
}

template <typename V>
template <typename OP>
int MdsUnit<DummyKey, V>::GetByWriterOp<OP>::operator()() {
  #define PRINT_WRAPPER KR(ret), K(typeid(OP).name()), K_(writer), K_(snapshot), K_(read_seq), K_(retry_param)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  if (MDS_FAIL(this_->single_row_.v_.get_by_writer(read_op_,
                                                   writer_,
                                                   snapshot_,
                                                   read_seq_,
                                                   retry_param_))) {
    if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
      MDS_LOG_GET(WARN, "MdsUnit get_by_writer failed");
    }
  } else {
    MDS_LOG_GET(TRACE, "MdsUnit get_by_writer success");
  }
  return ret;
  #undef PRINT_WRAPPER
}
template <typename V>
template <typename OP>
int MdsUnit<DummyKey, V>::get_by_writer(OP &&read_op,
                                        const MdsWriter &writer,
                                        const share::SCN snapshot,
                                        const int64_t read_seq,
                                        const int64_t lock_timeout_us) const
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  RetryParam retry_param(lock_timeout_us);
  GetByWriterOp<typename std::remove_reference<OP>::type> op(this, read_op, writer, snapshot, read_seq, retry_param);
  if (MDS_FAIL(retry_release_lock_with_op_until_timeout<LockMode::READ>(lock_, retry_param, op))) {
    if (OB_TIMEOUT == ret) {
      ret = OB_ERR_SHARED_LOCK_CONFLICT;
    }
  }
  return ret;
}

template <typename V>
template <typename OP>
int MdsUnit<DummyKey, V>::get_latest(OP &&read_op, const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(typeid(OP).name()), K(read_seq)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsRLockGuard lg(lock_);
  CLICK();
  if (MDS_FAIL(single_row_.v_.get_latest(std::forward<OP>(read_op), read_seq))) {
    if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
      MDS_LOG_GET(WARN, "MdsUnit get_latest failed");
    }
  } else {
    MDS_LOG_GET(TRACE, "MdsUnit get_latest success");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename V>
template <typename DUMP_OP>
int MdsUnit<DummyKey, V>::scan_KV_row(DUMP_OP &&op,
                                      share::SCN &flush_scn,
                                      const uint8_t mds_table_id,
                                      const uint8_t mds_unit_id,
                                      const bool for_flush) const
{
  #define PRINT_WRAPPER KR(ret), K(kv), K(typeid(op).name()), K(flush_scn), K(for_flush)
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  MdsRLockGuard lg(lock_);
  CLICK();
  MdsDumpKV kv;
  if (MDS_FAIL(kv.k_.init(mds_table_id,
                          mds_unit_id,
                          DummyKey(),
                          DefaultAllocator::get_instance()))) {
    MDS_LOG_SCAN(WARN, "init key failed");
  } else if (MDS_FAIL(single_row_.v_.scan_dump_node_from_tail_to_head(std::forward<DUMP_OP>(op),
                                                                      mds_table_id,
                                                                      mds_unit_id,
                                                                      kv,
                                                                      flush_scn,
                                                                      for_flush))) {
    MDS_LOG_SCAN(WARN, "scan single row to dump failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename V>
template <typename OP>
int MdsUnit<DummyKey, V>::for_each_node_on_row(OP &&op) const {
  #define PRINT_WRAPPER KR(ret)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsWLockGuard lg(lock_);
  CLICK();
  if (MDS_FAIL(single_row_.v_.for_each_node_from_tail_to_head(std::forward<OP>(op)))) {
    MDS_LOG_SCAN(WARN, "fail to scan node on single row");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename V>
template <typename OP>
int MdsUnit<DummyKey, V>::for_each_row(FowEachRowAction action_type, OP &&op) const {
  UNUSED(action_type);
  #define PRINT_WRAPPER KR(ret)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsWLockGuard lg(lock_);
  CLICK();
  if (MDS_FAIL(op(single_row_.v_))) {
    MDS_LOG_SCAN(WARN, "fail to scan single row");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename V>
int MdsUnit<DummyKey, V>::fill_virtual_info(ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array, const int64_t unit_id) const
{
  #define PRINT_WRAPPER KR(ret)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsRLockGuard lg(lock_);
  CLICK();
  if (MDS_FAIL(single_row_.v_.fill_virtual_info(DummyKey(), mds_node_info_array, unit_id))) {
    MDS_LOG_SCAN(WARN, "fail to fill_virtual_info");
  }
  return ret;
  #undef PRINT_WRAPPER
}

}
}
}
#endif