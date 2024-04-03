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
class ObTabletPointer;
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

/* about filter: init iterator with a filter makes get_next() only get specified nodes.
 * filter's input is mds node in specified unit, and a flag 'need_skip' to indicate wether this node should be seen when call get_next().
 * if filter's return value is not OB_SUCCESS, iterator will stop, every time call get_next() will return ITER_STOP.
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
  int init(mds::MdsTableHandle &mds_table_handle, const FilterFunction<UnitKey, UnitValue> &filter = DummyFilter());
  int get_next(UnitKey &key, mds::UserMdsNode<UnitKey, UnitValue> *&p_node);
  TO_STRING_KV(KP(this), K_(is_inited), K_(is_first_scan), K_(mds_table_handle),\
               K(typeid(UnitKey).name()), K(typeid(UnitValue).name()))
private:
  bool is_inited_;
  bool is_first_scan_;
  mds::MdsTableHandle mds_table_handle_;
  FilterFunction<UnitKey, UnitValue> filter_function_;
  ObMdsKvRowScanIterator<UnitKey, UnitValue> row_scan_iter_;
  ObMdsNodeScanIterator<UnitKey, UnitValue> node_scan_iter_;
};

}
}
}

#ifndef STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_ITERATOR_H_IPP
#define STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_ITERATOR_H_IPP
#include "mds_table_iterator.ipp"
#endif

#endif