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
#ifndef STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_ITERATOR_H
#define STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_ITERATOR_H

#include "lib/ob_errno.h"
#include "mds_table_impl.h"
#include "lib/guard/ob_light_shared_gaurd.h"
#include "mds_table_handle.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObTabletBasePointer;
namespace mds
{

template <typename UnitKey, typename UnitValue>
using FilterFunction = ObFunction<int(mds::UserMdsNode<UnitKey, UnitValue>&, bool &)>;

inline void construct_lock_guard(mds::MdsRLockGuard &guard, mds::MdsLock &lock) {
  guard.~MdsRLockGuard();
  new (&guard) mds::MdsRLockGuard(lock);
}

template <typename UnitKey, typename UnitValue>
struct ObMdsKvRowScanIterator {// will lock unit when init, but won't lock row when get_next_kv_row()
  using KvRowIter = typename mds::MdsUnit<UnitKey, UnitValue>::iterator;
  using KvRow = mds::KvPair<UnitKey, mds::Row<UnitKey, UnitValue>>;
  ObMdsKvRowScanIterator();
  int init(mds::MdsTableHandle &mds_table_handle);
  int get_next_kv_row(KvRow *&p_kv_row);
  TO_STRING_KV(KP(this), K_(is_inited), K_(is_first_scan),\
               K(typeid(UnitKey).name()), K(typeid(UnitValue).name()))
private:
  bool is_inited_;
  bool is_first_scan_;
  mds::MdsUnit<UnitKey, UnitValue> *p_mds_unit_;
  KvRowIter kv_row_iter_;
  mds::MdsRLockGuard unit_guard_;
};

template <typename UnitKey, typename UnitValue>
struct ObMdsNodeScanIterator {// will lock row when init, but won't lock node when get_next()
  using KvRow = mds::KvPair<UnitKey, mds::Row<UnitKey, UnitValue>>;
  using KvRowIter = typename mds::MdsUnit<UnitKey, UnitValue>::iterator;
  using NodeIter = typename KvRowIter::row_type::iterator;
  ObMdsNodeScanIterator(FilterFunction<UnitKey, UnitValue> &filter_function);
  int init(KvRow *&p_kv_rowm);
  int get_next_kv_node(UnitKey &key, mds::UserMdsNode<UnitKey, UnitValue> *&p_node);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(KP(this), K_(is_inited), K_(is_first_scan),\
               K(typeid(UnitKey).name()), K(typeid(UnitValue).name()))
private:
  bool is_inited_;
  bool is_first_scan_;
  KvRow *p_mds_kv_row_;
  NodeIter node_iter_;
  FilterFunction<UnitKey, UnitValue> &filter_function_;
  mds::MdsRLockGuard row_guard_;
};

/* About Filter:
 * Initializing the iterator with a filter causes get_next() to return only specified nodes.
 * The filter's input is an MDS node in the specified unit,
 * and a 'need_skip' flag to indicate whether this node should be skipped when calling get_next().
 * If the filter returns a value other than OB_SUCCESS, the iterator will propagate the error and will not advance.
 *
 * Cautions:
 * 0. All nodes on the MDS table will be iterated over, regardless of their state.
 *    (Aborted nodes are not visible due to the immediate deletion principle.)
 * 1. get_next() provides a key (value semantic) and a p_node (pointer to a real node).
 *    You may access this node until:
 *    a. The next call to get_next(), **or**
 *    b. The iterator is destroyed.
 *    **You do not own the node!** Copy it if you need to access it after the iterator is done.
 * 2. The iterator holds the MDS unit lock (in read mode) and row locks (in read mode) during iteration.
 *    **Do not hold the iterator for long periods**:
 *    - Holding the unit lock blocks new KV rows from being written to the unit.
 *    - Holding a row lock blocks state transitions for nodes in that row.
 * 3. The filter should be stateless. If state preservation is required, carefully consider other cautions.
 * 4. A filter may return OB_EAGAIN, which causes the iterator to release the current row lock and retry the same row.
 *    (e.g., when reading with snapshot requirements, and the state of a BEFORE_PREPARE node cannot be compared.)
 * 5. Due to point 4, a node may be processed by the filter multiple times, and its state may change between iterations.
 * 6. If a filter returns OB_EAGAIN, it **must** provide a valid `timeout_us`.
 *    If retries exceed the timeout, the iterator returns OB_TIMEOUT.
 */
template <typename UnitKey, typename UnitValue>
struct ObMdsUnitRowNodeScanIterator {// will add mds table ref when init to make sure inner iter safe
  using KvRow = mds::KvPair<UnitKey, mds::Row<UnitKey, UnitValue>>;
  using KvRowIter = typename mds::MdsUnit<UnitKey, UnitValue>::iterator;
  using NodeIter = typename KvRowIter::row_type::iterator;
  struct DummyFilter {
    int operator()(mds::UserMdsNode<UnitKey, UnitValue> &, bool &need_skip) { need_skip = false; return OB_SUCCESS; }
  };
  ObMdsUnitRowNodeScanIterator();
  int init(mds::MdsTableHandle &mds_table_handle,
           const FilterFunction<UnitKey, UnitValue> &filter = DummyFilter(),
           const int64_t timeout_ts = INT64_MAX);
  int get_next(UnitKey &key, mds::UserMdsNode<UnitKey, UnitValue> *&p_node);
  TO_STRING_KV(KP(this), K_(is_inited), K_(is_first_scan), K_(mds_table_handle),\
               K(typeid(UnitKey).name()), K(typeid(UnitValue).name()), K(row_scan_cache_.count()),\
               K_(row_output_idx), K_(retry_param))
private:
  int cache_all_nodes_in_row_(KvRow *p_kv_row);
private:
  bool is_inited_;
  bool is_first_scan_;
  mds::MdsTableHandle mds_table_handle_;
  FilterFunction<UnitKey, UnitValue> filter_function_;
  ObMdsKvRowScanIterator<UnitKey, UnitValue> row_scan_iter_;
  ObMdsNodeScanIterator<UnitKey, UnitValue> node_scan_iter_;
  UnitKey row_key_;
  ObArray<UserMdsNode<UnitKey, UnitValue>> row_scan_cache_;
  int64_t row_output_idx_;
  RetryParam retry_param_;
};

}
}
}

#ifndef STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_ITERATOR_H_IPP
#define STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_ITERATOR_H_IPP
#include "mds_table_iterator.ipp"
#endif

#endif