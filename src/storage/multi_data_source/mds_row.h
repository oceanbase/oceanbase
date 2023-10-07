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
#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_ROW_H
#define STORAGE_MULTI_DATA_SOURCE_MDS_ROW_H

#include "lib/lock/ob_small_spin_lock.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_errno.h"
#include "lib/utility/utility.h"
#include "share/ob_errno.h"
#include "share/scn.h"
#include "runtime_utility/mds_factory.h"
#include "runtime_utility/common_define.h"
#include "mds_node.h"
#include "mds_ctx.h"
#include "common/ob_clock_generator.h"
#include "storage/multi_data_source/mds_table_base.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include <utility>
#include "deps/oblib/src/common/meta_programming/ob_meta_copy.h"
#include "runtime_utility/mds_retry_control.h"

namespace oceanbase
{
namespace unittest
{
class TestMdsRowAndMdsCtx;
}
namespace storage
{
namespace mds
{

template <typename K, typename V>
class MdsUnit;
template <typename K, typename V>
class MdsRow : public MdsRowBase<K, V>// Row OWNS all UserMdsNode in list
{
  template <typename MdsTableType>
  friend class MdsTableImpl;
  friend class unittest::TestMdsRowAndMdsCtx;
  template <typename UnitK, typename UnitV>
  friend class MdsUnit;
  template <typename UnitK, typename UnitV>
  friend class ObMdsNodeScanIterator;
public:// iterator defination
  template <typename ValueType>
  struct IteratorBase;
  template <typename ValueType>
  struct NormalIterator;
  template <typename ValueType>
  struct ReverseIterator;
  using iterator = NormalIterator<UserMdsNode<K, V>>;
  using const_iterator = NormalIterator<const UserMdsNode<K, V>>;
  using reverse_iterator = ReverseIterator<UserMdsNode<K, V>>;
  using const_reverse_iterator = ReverseIterator<const UserMdsNode<K, V>>;
  iterator begin();
  iterator end();
  const_iterator cbegin();
  const_iterator cend();
  reverse_iterator rbegin();
  reverse_iterator rend();
  const_reverse_iterator crbegin();
  const_reverse_iterator crend();
public:
  MdsRow() : MdsRowBase<K, V>() {}
  ~MdsRow();
  template <typename DATA>
  int set(DATA &&data,
          MdsCtx &ctx,
          const RetryParam &retry_param,
          const bool is_for_remove = false);
  template <typename DATA>
  int replay(DATA &&data,
             MdsCtx &ctx,
             const share::SCN scn,
             const bool is_for_remove = false);
  template <typename READ_OP>
  int get_snapshot(READ_OP &&read_operation,
                   const share::SCN snapshot,
                   const int64_t read_seq,
                   const RetryParam &retry_param) const;
  template <typename READ_OP>
  int get_latest(READ_OP &&read_operation, const int64_t read_seq) const;
  template <typename READ_OP>
  int get_by_writer(READ_OP &&read_operation,
                    const MdsWriter &writer,
                    const share::SCN snapshot,
                    const int64_t read_seq,
                    const RetryParam &retry_param) const;
  template <typename DUMP_OP>
  int scan_dump_node_from_tail_to_head(DUMP_OP &&op,
                                       const uint8_t mds_table_id,
                                       const uint8_t mds_unit_id,
                                       MdsDumpKV &dump_kv,
                                       const share::SCN &flush_scn,
                                       const bool for_flush) const;
  // template <typename OP>
  // int for_each_node_from_tail_to_head(OP &&op) const;
  template <typename Key>
  int fill_virtual_info(const Key &key, ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array, const int64_t unit_id) const;
  // if a node aborted, delete it immediately.
  virtual void node_abort_callback_(ListNodeBase *node) override;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  // TO_STRING_KV(K_(sorted_list), KPC_(MdsRowBase<K, V>::p_mds_table), KPC_(MdsRowBase<K, V>::key));
public:
  template <typename OPERATION>
  int for_each_node_(OPERATION &&op) const;
  template <typename READ_OP, typename SPECIFIED_GET_LOGIC>
  int get_with_read_wrapper_(READ_OP &&read_operation, SPECIFIED_GET_LOGIC &&specified_logic) const;
  template <typename DATA>
  int construct_insert_record_user_mds_node_(MdsRowBase<K, V> *p_mds_row,
                                             DATA &&data,
                                             const MdsNodeType node_type,
                                             const share::SCN scn,
                                             MdsCtx &ctx);
  int check_node_snapshot_(const UserMdsNode<K, V> &node,
                           const share::SCN snapshot,
                           const RetryParam &retry_param,
                           bool &can_read) const;
  template <int N>
  void report_event_(const char (&event_str)[N],
                     const UserMdsNode<K, V> &node,
                     const char *file = __builtin_FILE(),
                     const uint32_t line = __builtin_LINE(),
                     const char *function_name = __builtin_FUNCTION()) const;
public:
  // define this structure cause C++11 not support perfect-forwarding capture lambda
  struct ForwardWrapper
  {
    ForwardWrapper(const V &data) : is_constructed_from_lvalue_(true), data_(data) {}
    ForwardWrapper(V &&data) : is_constructed_from_lvalue_(false), data_(data) {}
    bool is_constructed_from_lvalue_;
    const V &data_;
  };
public:
  SortedList<UserMdsNode<K, V>,
             SORT_TYPE::DESC> sorted_list_;
};
}
}
}

#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_ROW_H_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_ROW_H_IPP
#include "mds_row.ipp"
#endif

#endif