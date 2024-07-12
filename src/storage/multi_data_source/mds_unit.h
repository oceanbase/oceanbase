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
#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_UNIT_H
#define STORAGE_MULTI_DATA_SOURCE_MDS_UNIT_H

#include "lib/lock/ob_small_spin_lock.h"
#include "lib/ob_errno.h"
#include "lib/utility/utility.h"
#include "mds_row.h"
#include "share/scn.h"
#include "storage/multi_data_source/mds_node.h"
#include "storage/multi_data_source/runtime_utility/list_helper.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include <type_traits>
#include <utility>
#include "mds_table_base.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
template <typename K, typename V>
using Row = MdsRow<K, V>;
template <typename K, typename V>
struct KvPair : public ListNode<KvPair<K, V>>
{
  KvPair() = default;
  TO_STRING_KV(K_(k), K_(v));
  int mds_serialize(char *buf, const int64_t buf_len, int64_t &pos) const {
    return k_.mds_serialize(buf, buf_len, pos);
  }
  int mds_deserialize(const char *buf, const int64_t buf_len, int64_t &pos) {
    return k_.mds_deserialize(buf, buf_len, pos);
  }
  int64_t mds_get_serialize_size() const {
    return k_.mds_get_serialize_size();
  }
  K k_;
  V v_;
};
// multi row defination
template <typename K, typename V>
class MdsUnit final : public MdsUnitBase<K, V>
{
  struct SetOP {
    SetOP() = delete;
    SetOP(const SetOP &) = delete;
    SetOP &operator=(const SetOP &) = delete;
    SetOP(MdsUnit<K, V> *p_this,
          bool is_lvalue,
          MdsTableBase *p_mds_table,
          const K &key,
          V &value,
          MdsCtx &ctx,
          bool is_for_remove,
          RetryParam &retry_param) :
    this_(p_this),
    is_lvalue_(is_lvalue),
    p_mds_table_(p_mds_table),
    key_(key),
    value_(value),
    ctx_(ctx),
    is_for_remove_(is_for_remove),
    retry_param_(retry_param) {}
    int operator()();
    MdsUnit<K, V> *this_;
    bool is_lvalue_;
    MdsTableBase *p_mds_table_;
    const K &key_;
    V &value_;
    MdsCtx &ctx_;
    bool is_for_remove_;
    RetryParam &retry_param_;
  };
  template <typename OP>
  struct GetSnapShotOp {
    GetSnapShotOp() = delete;
    GetSnapShotOp(const GetSnapShotOp &) = delete;
    GetSnapShotOp &operator=(const GetSnapShotOp &) = delete;
    GetSnapShotOp(const MdsUnit<K, V> *p_this,
                  const K &key,
                  OP &read_op,
                  share::SCN snapshot,
                  RetryParam &retry_param)
    : this_(p_this),
    key_(key),
    read_op_(read_op),
    snapshot_(snapshot),
    retry_param_(retry_param) {}
    int operator()();
    const MdsUnit<K, V> *this_;
    const K &key_;
    OP &read_op_;
    share::SCN snapshot_;
    RetryParam &retry_param_;
  };
  template <typename OP>
  struct GetByWriterOp {
    GetByWriterOp() = delete;
    GetByWriterOp(const GetByWriterOp &) = delete;
    GetByWriterOp &operator=(const GetByWriterOp &) = delete;
    GetByWriterOp(const MdsUnit<K, V> *p_this,
                  const K &key,
                  OP &read_op,
                  const MdsWriter &writer,
                  share::SCN snapshot,
                  transaction::ObTxSEQ read_seq,
                  RetryParam &retry_param)
    : this_(p_this),
    key_(key),
    read_op_(read_op),
    writer_(writer),
    snapshot_(snapshot),
    read_seq_(read_seq),
    retry_param_(retry_param) {}
    int operator()();
    const MdsUnit<K, V> *this_;
    const K &key_;
    OP &read_op_;
    const MdsWriter &writer_;
    share::SCN snapshot_;
    transaction::ObTxSEQ read_seq_;
    RetryParam &retry_param_;
  };
public:
  typedef K key_type;
  typedef V value_type;
public:// iterator defination
  template <typename ValueType>
  struct IteratorBase;
  template <typename ValueType>
  struct NormalIterator;
  template <typename ValueType>
  struct ReverseIterator;
  using iterator = NormalIterator<KvPair<K, Row<K, V>>>;
  using const_iterator = NormalIterator<const KvPair<K, Row<K, V>>>;
  using reverse_iterator = ReverseIterator<KvPair<K, Row<K, V>>>;
  using const_reverse_iterator = ReverseIterator<const KvPair<K, Row<K, V>>>;
  iterator begin();
  iterator end();
  const_iterator cbegin();
  const_iterator cend();
  reverse_iterator rbegin();
  reverse_iterator rend();
  const_reverse_iterator crbegin();
  const_reverse_iterator crend();
public:
  MdsUnit();
  ~MdsUnit();
public:
  template <typename Value>
  int set(MdsTableBase *p_mds_table,
          const K &key,
          Value &&value,
          MdsCtx &ctx,
          const int64_t lock_timeout_us,
          const bool is_for_remove = false);
  template <typename Value>
  int replay(MdsTableBase *p_mds_table,
             const K &key,
             Value &&value,
             MdsCtx &ctx,
             const share::SCN scn,
             const bool is_for_remove = false);
  template <typename OP>
  int get_snapshot(const K &key,
                   OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t timeout_us) const;
  template <typename OP>
  int get_by_writer(const K &key,
                    OP &&op,
                    const MdsWriter &writer,
                    const share::SCN snapshot,
                    const transaction::ObTxSEQ read_seq,
                    const int64_t timeout_us) const;
  template <typename OP>
  int get_latest(const K &key, OP &&op) const;
  template <typename DUMP_OP>
  int scan_KV_row(DUMP_OP &&op,
                  const share::SCN flush_scn,
                  const uint8_t mds_table_id,
                  const uint8_t mds_unit_id,
                  const bool for_flush,
                  const ScanRowOrder scan_row_order,
                  const ScanNodeOrder scan_node_order) const;
  template <typename OP>
  int for_each_node_on_row(OP &&op) const;
  template <typename OP>
  int for_each_row(FowEachRowAction action_type, OP &&op);
  void lock() const { lock_.wrlock(); }
  void unlock() const { lock_.unlock(); }
  int fill_virtual_info(ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array, const int64_t unit_id) const;
  TO_STRING_KV(K_(multi_row_list));
public:
  template <int N>
  void report_event_(const char (&event_str)[N],
                     const K &key,
                     const char *file = __builtin_FILE(),
                     const uint32_t line = __builtin_LINE(),
                     const char *function_name = __builtin_FUNCTION()) const;
  int get_row_from_list_(const K &key, KvPair<K, Row<K, V>> *&p_kv) const;
  int insert_empty_kv_to_list_(const K &key, KvPair<K, Row<K, V>> *&p_kv, MdsTableBase *p_mds_table);
  void erase_kv_from_list_if_empty_(KvPair<K, Row<K, V>> *p_kv);
  SortedList<KvPair<K, Row<K, V>>, SORT_TYPE::ASC> multi_row_list_;
  mutable MdsLock lock_;
};

// signal row defination
template <typename V>
class MdsUnit<DummyKey, V> final : public MdsUnitBase<DummyKey, V>
{
  struct SetOP {
    SetOP() = delete;
    SetOP(const SetOP &) = delete;
    SetOP &operator=(const SetOP &) = delete;
    SetOP(MdsUnit<DummyKey, V> *p_this,
          bool is_lvalue,
          V &value,
          MdsCtx &ctx,
          RetryParam &retry_param) :
    this_(p_this),
    is_lvalue_(is_lvalue),
    value_(value),
    ctx_(ctx),
    retry_param_(retry_param) {}
    int operator()();
    MdsUnit<DummyKey, V> *this_;
    bool is_lvalue_;
    V &value_;
    MdsCtx &ctx_;
    RetryParam &retry_param_;
  };
  template <typename OP>
  struct GetSnapShotOp {
    GetSnapShotOp() = delete;
    GetSnapShotOp(const GetSnapShotOp &) = delete;
    GetSnapShotOp &operator=(const GetSnapShotOp &) = delete;
    GetSnapShotOp(const MdsUnit<DummyKey, V> *p_this,
                  OP &read_op,
                  share::SCN snapshot,
                  RetryParam &retry_param)
    : this_(p_this),
    read_op_(read_op),
    snapshot_(snapshot),
    retry_param_(retry_param) {}
    int operator()();
    const MdsUnit<DummyKey, V> *this_;
    OP &read_op_;
    share::SCN snapshot_;
    RetryParam &retry_param_;
  };
  template <typename OP>
  struct GetByWriterOp {
    GetByWriterOp() = delete;
    GetByWriterOp(const GetByWriterOp &) = delete;
    GetByWriterOp &operator=(const GetByWriterOp &) = delete;
    GetByWriterOp(const MdsUnit<DummyKey, V> *p_this,
                  OP &read_op,
                  const MdsWriter &writer,
                  share::SCN snapshot,
                  transaction::ObTxSEQ read_seq,
                  RetryParam &retry_param)
    : this_(p_this),
    read_op_(read_op),
    writer_(writer),
    snapshot_(snapshot),
    read_seq_(read_seq),
    retry_param_(retry_param) {}
    int operator()();
    const MdsUnit<DummyKey, V> *this_;
    OP &read_op_;
    const MdsWriter &writer_;
    share::SCN snapshot_;
    transaction::ObTxSEQ read_seq_;
    RetryParam &retry_param_;
  };
public:
  typedef DummyKey key_type;
  typedef V value_type;
public:// iterator defination
  template <typename ValueType>
  struct IteratorBase;
  template <typename ValueType>
  struct NormalIterator;
  template <typename ValueType>
  struct ReverseIterator;
  using iterator = NormalIterator<KvPair<DummyKey, Row<DummyKey, V>>>;
  using const_iterator = NormalIterator<const KvPair<DummyKey, Row<DummyKey, V>>>;
  using reverse_iterator = ReverseIterator<KvPair<DummyKey, Row<DummyKey, V>>>;
  using const_reverse_iterator = ReverseIterator<const KvPair<DummyKey, Row<DummyKey, V>>>;
  iterator begin();
  iterator end();
  const_iterator cbegin();
  const_iterator cend();
  reverse_iterator rbegin();
  reverse_iterator rend();
  const_reverse_iterator crbegin();
  const_reverse_iterator crend();
public:
  MdsUnit();
  template <typename Value>
  int set(MdsTableBase *p_mds_table, Value &&value, MdsCtx &ctx, const int64_t lock_timeout_us);
  template <typename Value>
  int replay(MdsTableBase *p_mds_table, Value &&value, MdsCtx &ctx, const share::SCN scn);
  template <typename OP>
  int get_snapshot(OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t timeout_us) const;
  template <typename OP>
  int get_by_writer(OP &&op,
                    const MdsWriter &writer,
                    const share::SCN snapshot,
                    const transaction::ObTxSEQ read_seq,
                    const int64_t timeout_us) const;
  template <typename OP>
  int get_latest(OP &&op) const;
  template <typename DUMP_OP>
  int scan_KV_row(DUMP_OP &&op,
                  const share::SCN flush_scn,
                  const uint8_t mds_table_id,
                  const uint8_t mds_unit_id,
                  const bool for_flush,
                  const ScanRowOrder scan_row_order,
                  const ScanNodeOrder scan_node_order) const;
  template <typename OP>
  int for_each_node_on_row(OP &&op) const;
  template <typename OP>
  int for_each_row(FowEachRowAction action_type, OP &&op) const;
  void lock() const { lock_.wrlock(); }
  void unlock() const { lock_.unlock(); }
  int fill_virtual_info(ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array, const int64_t unit_id) const;
  TO_STRING_KV(K_(single_row));
public:
  KvPair<DummyKey, Row<DummyKey, V>> single_row_;
  mutable MdsLock lock_;
};
}
}
}

#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_UNIT_H_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_UNIT_H_IPP
#include "mds_unit.ipp"
#endif

#endif