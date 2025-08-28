/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#ifndef _OB_TABLE_MERGE_FILTERS_H
#define _OB_TABLE_MERGE_FILTERS_H 1

#include "ob_table_filter.h"
#include "observer/table/common/ob_hbase_common_struct.h"

namespace oceanbase
{
namespace table
{

/**
 * -------------------------------------- ObRowIteratorator ----------------------------------------
 */

template <typename Row, typename... ReScanParam>
class ObTableRowIterator
{
public:
  ObTableRowIterator() = default;
  virtual ~ObTableRowIterator() = default;
public:
  virtual int next_row() = 0;
  virtual Row *get_cur_row() = 0; 
  virtual const Row *get_cur_row() const = 0; 
  virtual int rescan(ReScanParam&... rescan_param) = 0;

  bool valid() const { return valid_; }
  void set_valid(bool valid) { valid_ = valid; }
protected:
  bool valid_ = true;
};

// cache query result row to adapt table merge row iter
class ObTableResultRowIter : public ObTableRowIterator<common::ObNewRow>
{
public:
  ObTableResultRowIter(ObTableQueryIResultIterator *result_iter)
    : cur_row_(),
      iterable_result_(nullptr),
      result_iter_(result_iter)
  {}

  virtual ~ObTableResultRowIter()
  {
    if (OB_NOT_NULL(result_iter_)) {
      result_iter_->~ObTableQueryIResultIterator();
      result_iter_ = nullptr;
    }
  }
  int next_row();
  common::ObNewRow *get_cur_row() {return &cur_row_; }
  const common::ObNewRow *get_cur_row() const {return &cur_row_; }
  int rescan() { return OB_NOT_IMPLEMENT; }
  TO_STRING_KV(K_(iterable_result));
private:
  // current row
  common::ObNewRow cur_row_;
  // cache some row
  ObTableQueryIterableResult *iterable_result_;
  // internal iterator
  ObTableQueryIResultIterator* result_iter_;
};

/**
 * -------------------------------------- ObTableMergeRowIterator ----------------------------------------
 */

template <typename Row, typename Compare, typename RowIterator>
class ObTableMergeRowIterator
{
public:
  ObTableMergeRowIterator();
  virtual ~ObTableMergeRowIterator()
  {
    inner_row_iters_.reset();
    allocator_.reset();
  }

  int init(Compare* compare);
  virtual int get_next_row(Row *&row);
  int seek(const ObString& key);
  bool has_more_rows() const { return binary_heap_.count() != 0; }
  template<typename Iterator>
  int assign_inner_row_iters(common::ObIArray<Iterator *> &row_iters)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_iters.count(); i++) {
      RowIterator *row_iter = row_iters.at(i);
      if (OB_FAIL(inner_row_iters_.push_back(row_iter))) {
        LOG_WARN("fail to add result iter", K(ret));
      }
    }
    return ret;
  }
  template<typename... ReScanParam>
  int rescan(ReScanParam&... rescan_param);

  void reset() {
    binary_heap_.reset();
    last_pop_iter_ = nullptr;
  }

  TO_STRING_KV(K_(binary_heap), K_(inner_row_iters));

private:
  int build_heap(const common::ObIArray<RowIterator*>& inner_row_iters);
  int iter_and_push_iterator(RowIterator &iter);
  int direct_push_iterator(RowIterator &iter);

private:
  struct HeapCompare
  {
    HeapCompare() : compare_(nullptr), error_code_(common::OB_SUCCESS) {}
    bool operator()(const RowIterator *lhs, const RowIterator *rhs);
    int get_error_code() const { return error_code_; }
    Compare *compare_;
    int error_code_;
  };
private:
  ObArenaAllocator allocator_;
  HeapCompare compare_;
  common::ObBinaryHeap<RowIterator*, HeapCompare, 64> binary_heap_;
  common::ObSEArray<RowIterator *, 8> inner_row_iters_;
  bool is_inited_;
  RowIterator *last_pop_iter_; 
};

template <typename Row, typename Compare, typename RowIterator>
ObTableMergeRowIterator<Row, Compare, RowIterator>::ObTableMergeRowIterator()
  : allocator_("TableMrgRowAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    compare_(),
    binary_heap_(compare_),
    is_inited_(false),
    last_pop_iter_(nullptr)
{
  inner_row_iters_.set_attr(ObMemAttr(MTL_ID(), "MergeInnerIters"));
}

template <typename Row, typename Compare, typename RowIterator>
bool ObTableMergeRowIterator<Row, Compare, RowIterator>::HeapCompare::operator() (const RowIterator *lhs, const RowIterator *rhs)
{
  int ret = common::OB_SUCCESS;
  bool bret = false;
  if (OB_ISNULL(compare_) || OB_ISNULL(lhs) || OB_ISNULL(lhs->get_cur_row()) ||
      OB_ISNULL(rhs) || OB_ISNULL(rhs->get_cur_row())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC_(compare), KPC(lhs), KPC(rhs));
  } else {
    bret = !compare_->operator()(*lhs->get_cur_row(), *rhs->get_cur_row());
  }
  if (OB_FAIL(ret)) {
    error_code_ = ret;
  } else if (OB_FAIL(compare_->get_error_code())) {
    error_code_ = compare_->get_error_code();
  }
  return bret;
}

template <typename Row, typename Compare, typename RowIterator>
int ObTableMergeRowIterator<Row, Compare, RowIterator>::init(Compare* compare)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(compare)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    compare_.compare_ = compare;
    if (inner_row_iters_.count() >= 1 && OB_FAIL(build_heap(inner_row_iters_))) {
      LOG_WARN("fail to build heap", K(ret), K(inner_row_iters_.count()));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename Row, typename Compare, typename RowIterator>
int ObTableMergeRowIterator<Row, Compare, RowIterator>::build_heap(const ObIArray<RowIterator*>& inner_row_iters)
{
  int ret = common::OB_SUCCESS;
  ObSEArray<RowIterator *, 8> valid_row_iters;
  valid_row_iters.set_attr(ObMemAttr(MTL_ID(), "MergeValidIters"));
  for (int i = 0; OB_SUCC(ret) && i < inner_row_iters.count(); i++) {
    RowIterator *row_iter = inner_row_iters.at(i);
    if (OB_ISNULL(row_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null row iterator", K(ret));
    } else if (OB_FAIL(row_iter->next_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to iterate to next", K(ret));
      } else {
        // row_iter valid -> false
        LOG_DEBUG("no valid row iter", KPC(row_iter));
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(valid_row_iters.push_back(row_iter))) {
      LOG_WARN("fail to push valid row iter", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(inner_row_iters_.assign(valid_row_iters))) {
    LOG_WARN("fail to assign inner row iters", K(ret), K(valid_row_iters.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inner_row_iters_.count(); ++i) {
      RowIterator *row_iter = inner_row_iters_.at(i);
      if (OB_ISNULL(row_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null row iterator", K(ret));
      } else if (OB_FAIL(direct_push_iterator(*row_iter))) {
        LOG_WARN("fail to direct push iterator", K(ret));
      }
    }
  }
  return ret;
}

template <typename Row, typename Compare, typename RowIterator>
int ObTableMergeRowIterator<Row, Compare, RowIterator>::iter_and_push_iterator(RowIterator &iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iter.next_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to iterate to next", K(ret));
    } else {
      ret = OB_SUCCESS;
      LOG_DEBUG("one cache row iterator finished", K(ret), K(iter));
    }
  } else if (OB_FAIL(binary_heap_.push(&iter))) {
    LOG_WARN("fail to push iter", K(ret));
  } else if (OB_FAIL(compare_.get_error_code())) {
    LOG_WARN("fail to compare iter", K(ret));
  } else {}
  return ret;
}

template <typename Row, typename Compare, typename RowIterator>
int ObTableMergeRowIterator<Row, Compare, RowIterator>::direct_push_iterator(RowIterator &iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!iter.valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try to push invalid iter to heap", K(iter), K(ret));
  } else if (OB_FAIL(binary_heap_.push(&iter))) {
    LOG_WARN("fail to push iter", K(iter), K(ret));
  } else if (OB_FAIL(compare_.get_error_code())) {
    LOG_WARN("fail to compare iter", K(iter), K(ret));
  } else {}
  return ret;
}


template <typename Row, typename Compare, typename RowIterator>
int ObTableMergeRowIterator<Row, Compare, RowIterator>::get_next_row(Row *&row)
{
  int ret = OB_SUCCESS;
  if (binary_heap_.empty() && OB_ISNULL(last_pop_iter_)) {
    ret = OB_ITER_END;
    LOG_DEBUG("ObTableMergeRowIterator iter end", K(ret));
  } else if (OB_NOT_NULL(last_pop_iter_) && OB_FAIL(iter_and_push_iterator(*last_pop_iter_))) {
    LOG_WARN("fail to iter and push_iterator", K(ret));
  } else if (binary_heap_.empty()) {
    ret = OB_ITER_END;
    LOG_DEBUG("ObTableMergeRowIterator iter end after iter last pop iter", K(ret));
  } else {
    RowIterator* cache_iterator = binary_heap_.top();
    if (OB_ISNULL(cache_iterator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iterator", K(ret));
    } else if (OB_FAIL(binary_heap_.pop())) {
      LOG_WARN("fail to pop items", K(ret));
    } else if (OB_FAIL(compare_.get_error_code())) {
      LOG_WARN("fail to compare items", K(ret));
    } else {
      last_pop_iter_ = cache_iterator;
      row = cache_iterator->get_cur_row();
    }
  }
  return ret;
}

template <typename Row, typename Compare, typename RowIterator>
int ObTableMergeRowIterator<Row, Compare, RowIterator>::seek(const ObString& key)
{
  int ret = OB_SUCCESS;
  int64_t heap_size = binary_heap_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < heap_size; ++i) {
    RowIterator* cache_iter = binary_heap_.top();
    if (OB_ISNULL(cache_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iterator", K(ret));
    } else if (OB_FAIL(binary_heap_.pop())) {
      LOG_WARN("fail to pop items", K(ret));
    } else if (OB_FAIL(compare_.get_error_code())) {
      LOG_WARN("fail to compare items", K(ret));
    } else {
      const Row *current_row = cache_iter->get_cur_row();
      while (OB_SUCC(ret) && current_row->get_cell(0).get_string().compare(key) < 0) {
        if (OB_FAIL(cache_iter->next_row())) {
          if (OB_ITER_END == ret) {
            // No more rows in this iterator
            LOG_DEBUG("No more results available", K(ret));
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          current_row = cache_iter->get_cur_row();
        }
      }
      if (OB_FAIL(ret)) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(binary_heap_.push(cache_iter))) {
        LOG_WARN("fail to push", K(ret));
      } else if (OB_FAIL(compare_.get_error_code())) {
        LOG_WARN("fail to compare items", K(ret));
      }
    }
  }

  return ret;
}

template <typename Row, typename Compare, typename RowIterator>
template<typename... ReScanParam>
int ObTableMergeRowIterator<Row, Compare, RowIterator>::rescan(ReScanParam&... rescan_param)
{
  int ret = common::OB_SUCCESS;
  reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < inner_row_iters_.count(); ++i) {
    RowIterator *row_iter = inner_row_iters_.at(i);
    if (OB_ISNULL(row_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null row iterator", K(ret));
    } else if (OB_FAIL(row_iter->rescan(rescan_param...))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to rescan", K(ret));
      } else {
        ret = OB_SUCCESS;
        LOG_DEBUG("fail to rescan", K(ret), K(row_iter));
      }
    } else if (OB_FAIL(iter_and_push_iterator(*row_iter))) {
      LOG_WARN("fail to iter and push iterator", K(ret));
    }
  }

  return ret;
}

/**
 * -------------------------------------- ObMergeTableQueryResultIterator ----------------------------------------
 */

class ObMergeTableQueryResultIterator : public ObTableQueryResultIterator,
      public ObTableMergeRowIterator<common::ObNewRow, ObTableMergeFilterCompare, ObTableResultRowIter>
{
public:
  ObMergeTableQueryResultIterator(const ObTableQuery& query, table::ObTableQueryResult &one_result);
  ObMergeTableQueryResultIterator(const ObTableQuery& query, table::ObTableQueryIterableResult &one_result);
  ObMergeTableQueryResultIterator(const ObTableQuery& query);
  TO_STRING_KV(KP_(serlize_result), KP_(iterable_result));
  virtual ~ObMergeTableQueryResultIterator()
  {
    result_row_iters_.reset();
    allocator_.reset();
  }
  virtual int get_next_result(ObTableQueryResult *&one_result) override;
  virtual int get_next_result(ObTableQueryIterableResult *&one_result) override;

  virtual int get_next_result(ObTableQueryResult &one_result);
  virtual int get_next_result(ObTableQueryIterableResult &one_result);
  virtual bool has_more_result() const override { return has_more_rows(); }

  virtual void set_query(const ObTableQuery *query) override { query_ = query; };

  virtual void set_one_result(ObTableQueryResult *result) { serlize_result_ = result; }
  ObIAllocator& get_allocator() { return allocator_; }
  template<typename ResultIterType>
  int assign_inner_result_iters(common::ObIArray<ResultIterType *> &result_iters)
  {
    int ret = OB_SUCCESS;
    ObTableResultRowIter *tmp_result_row_iter = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < result_iters.count(); i++) {
      ResultIterType *result_iter = result_iters.at(i);
      if (OB_ISNULL(tmp_result_row_iter = OB_NEWx(ObTableResultRowIter, &allocator_, result_iter))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObTableResultRowIter))); 
      } else if (OB_FAIL(result_row_iters_.push_back(tmp_result_row_iter))) {
        LOG_WARN("fail to add result row iter", K(ret));
        tmp_result_row_iter->~ObTableResultRowIter();
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(assign_inner_row_iters(result_row_iters_))) {
      LOG_WARN("fail to assign inner row iteators", K(ret));
    }
    return ret;
  }

private:
  virtual void set_scan_result(table::ObTableApiScanRowIterator* scan_result) override { UNUSED(scan_result); }
  virtual ObTableQueryResult *get_one_result() { return nullptr; }
  virtual void set_query_async() override { is_query_async_ = true; }
  virtual void set_filter(hfilter::Filter *filter) override { UNUSED(filter); }
  virtual hfilter::Filter *get_filter() const override { return nullptr; }

private:
  common::ObArenaAllocator allocator_;
  table::ObTableQueryResult* serlize_result_;
  table::ObTableQueryIterableResult* iterable_result_;
  common::ObSEArray<ObTableResultRowIter *, 4> result_row_iters_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_MERGE_FILTERS_H */
