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
#ifndef STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_ITERATOR_IPP
#define STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_ITERATOR_IPP

#ifndef STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_ITERATOR_H_IPP
#define STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_ITERATOR_H_IPP
#include "mds_table_iterator.h"
#endif

namespace oceanbase
{
namespace storage
{
namespace mds
{

template <typename UnitKey, typename UnitValue>
ObMdsKvRowScanIterator<UnitKey, UnitValue>::ObMdsKvRowScanIterator()
: is_inited_(false),
is_first_scan_(false),
p_mds_unit_(nullptr),
kv_row_iter_(),
unit_guard_() {}

template <typename UnitKey, typename UnitValue>
int ObMdsKvRowScanIterator<UnitKey, UnitValue>::init(mds::MdsTableHandle &mds_table_handle) {
  #define PRINT_WRAPPER KR(ret), K(mds_table_handle), K(typeid(UnitKey).name()),\
                        K(typeid(UnitValue).name())
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    MDS_LOG_NONE(WARN, "ObMdsKvRowScanIterator init twice");
  } else if (!mds_table_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_NONE(WARN, "try iterate invalid mds table");
  } else if (OB_FAIL(mds_table_handle.get_mds_unit(p_mds_unit_))) {
    MDS_LOG_NONE(WARN, "fail to find unit in this mds table");
  } else {
    construct_lock_guard(unit_guard_, p_mds_unit_->lock_);// lock unit to make sure get kv_row safe
    is_inited_ = true;
    is_first_scan_ = true;
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename UnitKey, typename UnitValue>
int ObMdsKvRowScanIterator<UnitKey, UnitValue>::get_next_kv_row(KvRow *&p_kv_row) {
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    MDS_LOG_NONE(WARN, "ObMdsKvRowScanIterator not init");
  } else if (is_first_scan_) {
    is_first_scan_ = false;
    kv_row_iter_ = p_mds_unit_->begin();
  }
  if (OB_SUCC(ret)) {
    if (kv_row_iter_ == p_mds_unit_->end()) {
      ret = OB_ITER_END;
    } else {
      p_kv_row = &(*(kv_row_iter_++));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename UnitKey, typename UnitValue>
ObMdsNodeScanIterator<UnitKey, UnitValue>::ObMdsNodeScanIterator(FilterFunction<UnitKey, UnitValue> &filter_function)
: is_inited_(false),
is_first_scan_(true),
p_mds_kv_row_(nullptr),
node_iter_(),
filter_function_(filter_function),
row_guard_() {}

template <typename UnitKey, typename UnitValue>
int ObMdsNodeScanIterator<UnitKey, UnitValue>::init(KvRow *&p_kv_row) {
  #define PRINT_WRAPPER KR(ret), K(*this), KP(p_kv_row)
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    MDS_LOG_NONE(WARN, "ObMdsNodeScanIterator init twice");
  } else if (OB_ISNULL(p_kv_row)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_NONE(WARN, "p_kv_row is NULL");
  } else {
    p_mds_kv_row_ = p_kv_row;
    construct_lock_guard(row_guard_, p_mds_kv_row_->v_.lock_);// lock unit to make sure get kv_row safe
    is_inited_ = true;
    is_first_scan_ = true;
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename UnitKey, typename UnitValue>
int ObMdsNodeScanIterator<UnitKey, UnitValue>::get_next_kv_node(UnitKey &key,
                                                                mds::UserMdsNode<UnitKey, UnitValue> *&p_node) {
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    MDS_LOG_NONE(WARN, "ObMdsNodeScanIterator not init");
  } else if (is_first_scan_) {
    is_first_scan_ = false;
    node_iter_ = p_mds_kv_row_->v_.begin();
  }
  bool need_filter = false;
  while (OB_SUCC(ret)) {
    need_filter = false;
    if (node_iter_ == p_mds_kv_row_->v_.end()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(filter_function_(*node_iter_, need_filter))) {
      MDS_LOG_NONE(WARN, "scan node failed", K(*p_node));
    } else if (need_filter) {
      node_iter_++;
    } else {
      key = p_mds_kv_row_->k_;
      p_node = &(*node_iter_++);
      break;
      MDS_LOG_NONE(TRACE, "scan node", K(*p_node));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename UnitKey, typename UnitValue>
bool ObMdsNodeScanIterator<UnitKey, UnitValue>::is_valid() const { return is_inited_; }

template <typename UnitKey, typename UnitValue>
void ObMdsNodeScanIterator<UnitKey, UnitValue>::reset() {
  FilterFunction<UnitKey, UnitValue> &filter_function = filter_function_;
  this->~ObMdsNodeScanIterator();
  new (this) ObMdsNodeScanIterator(filter_function);
}

template <typename UnitKey, typename UnitValue>
ObMdsUnitRowNodeScanIterator<UnitKey, UnitValue>::ObMdsUnitRowNodeScanIterator()
: is_inited_(false),
is_first_scan_(true),
mds_table_handle_(),
filter_function_(),
row_scan_iter_(),
node_scan_iter_(filter_function_) {}

template <typename UnitKey, typename UnitValue>
int ObMdsUnitRowNodeScanIterator<UnitKey, UnitValue>::init(mds::MdsTableHandle &mds_table_handle,
                                                           const FilterFunction<UnitKey, UnitValue> &filter) {
  #define PRINT_WRAPPER KR(ret), K(*this), K(mds_table_handle)
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    MDS_LOG_NONE(WARN, "ObMdsUnitRowNodeScanIterator init twice");
  } else if (!mds_table_handle.is_valid() || !filter.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_NONE(WARN, "mds_table_handle invalid");
  } else if (OB_FAIL(filter_function_.assign(filter))) {
    MDS_LOG_NONE(WARN, "failed to init filter function");
  } else {
    mds_table_handle_ = mds_table_handle;
    is_inited_ = true;
    is_first_scan_ = true;
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename UnitKey, typename UnitValue>
int ObMdsUnitRowNodeScanIterator<UnitKey, UnitValue>::get_next(UnitKey &key,
                                                               mds::UserMdsNode<UnitKey, UnitValue> *&p_node) {
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  bool node_meet_end = false;
  bool row_mmet_end = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    MDS_LOG_NONE(WARN, "ObMdsUnitRowNodeScanIterator not init");
  } else if (is_first_scan_) {
    is_first_scan_ = false;
    if (OB_FAIL(row_scan_iter_.init(mds_table_handle_))) {
      MDS_LOG_NONE(WARN, "fail to init row_scan_iter_");
    }
  }
  while (OB_SUCC(ret) &&
         (!node_scan_iter_.is_valid() || // first time to scan
         OB_ITER_END == (ret = node_scan_iter_.get_next_kv_node(key, p_node)))) {// every time scan row end
    node_scan_iter_.reset();
    KvRow *p_kv_row = nullptr;
    if (OB_FAIL(row_scan_iter_.get_next_kv_row(p_kv_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        MDS_LOG_NONE(WARN, "fail to get kv row");
      }
    } else if (OB_FAIL(node_scan_iter_.init(p_kv_row))) {
      MDS_LOG_NONE(WARN, "fail to init node_scan_iter_");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

}
}
}

#endif