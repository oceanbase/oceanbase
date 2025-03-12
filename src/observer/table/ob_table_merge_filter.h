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

namespace oceanbase
{
namespace table
{

template <typename Row, typename Compare>
class ObMergeTableQueryResultIterator : public ObTableQueryResultIterator
{
public:
  ObMergeTableQueryResultIterator(const ObTableQuery& query, table::ObTableQueryResult &one_result);
  ObMergeTableQueryResultIterator(const ObTableQuery& query, table::ObTableQueryIterableResult &one_result);
  TO_STRING_KV(KP_(serlize_result), KP_(iterable_result), K_(binary_heap));
  virtual ~ObMergeTableQueryResultIterator()
  {
    for (int i = 0; i < binary_heap_.count(); i++) {
      if (OB_NOT_NULL(binary_heap_.at(i))) {
        binary_heap_.at(i)->~ObRowCacheIterator();
        binary_heap_.at(i) = nullptr;
      }
    }
    for (int j = 0; j < inner_result_iters_.count(); j++) {
      ObTableQueryUtils::destroy_result_iterator(inner_result_iters_.at(j));
    }
    allocator_.reset();
  }

  int init(Compare* compare);
  virtual int get_next_result(ObTableQueryResult *&one_result) override;
  virtual int get_next_result(ObTableQueryIterableResult *&one_result) override;
  virtual bool has_more_result() const override { return binary_heap_.count() != 0; }

  virtual void set_query(const ObTableQuery *query) override { query_ = query; };

  virtual void set_one_result(ObTableQueryResult *result) { serlize_result_ = result; }

  int seek(const ObString& key);
  common::ObIArray<ObTableQueryResultIterator *>& get_inner_result_iterators() { return inner_result_iters_; }

  ObIAllocator& get_allocator() { return allocator_; }

private:
  virtual void set_scan_result(table::ObTableApiScanRowIterator* scan_result) override { UNUSED(scan_result); }
  virtual ObTableQueryResult *get_one_result() { return nullptr; }
  virtual void set_query_async() override { /* is_query_async_ = true; */ }
  virtual void set_filter(hfilter::Filter *filter) override { UNUSED(filter); }
  virtual hfilter::Filter *get_filter() const override { return nullptr; }
private:
  int build_heap(const common::ObIArray<ObTableQueryResultIterator*>& result_iters);

private:
  struct ObRowCacheIterator
  {
    ObRowCacheIterator()
      : allocator_("RowCacheIterAlc", OB_MALLOC_BIG_BLOCK_SIZE, MTL_ID()),
        row_(),
        iterable_result_(nullptr),
        result_iter_(nullptr) {}

    ~ObRowCacheIterator()
    {
      if (OB_NOT_NULL(iterable_result_)) {
        iterable_result_->~ObTableQueryIterableResult();
      }
      if (OB_NOT_NULL(result_iter_)) {
        result_iter_->~ObTableQueryResultIterator();
        result_iter_ = nullptr;
      }
      allocator_.reset();
    }
    int init(ObTableQueryResultIterator *result_iter);
    int get_next_row(Row &row);
    TO_STRING_KV(KP_(iterable_result), KP_(result_iter));

    ObArenaAllocator allocator_;
    // current row
    Row row_;
    // cache some row
    ObTableQueryIterableResult* iterable_result_;
    // internal iterator
    ObTableQueryResultIterator* result_iter_;
  };
  struct HeapCompare
  {
    HeapCompare() : compare_(nullptr), error_code_(common::OB_SUCCESS) {}
    bool operator()(const ObRowCacheIterator *lhs, const ObRowCacheIterator *rhs);
    int get_error_code() const { return error_code_; }
    Compare *compare_;
    int error_code_;
  };
private:
  ObArenaAllocator allocator_;
  HeapCompare compare_;

  table::ObTableQueryResult* serlize_result_;
  table::ObTableQueryIterableResult* iterable_result_;


  common::ObBinaryHeap<ObRowCacheIterator*, HeapCompare, 64> binary_heap_;
  common::ObSEArray<ObTableQueryResultIterator *, 8> inner_result_iters_;

  bool is_inited_;
  int64_t row_count_;
};

template <typename Row, typename Compare>
ObMergeTableQueryResultIterator<Row, Compare>::ObMergeTableQueryResultIterator(const ObTableQuery& query,
                                                                              table::ObTableQueryResult &one_result)
  : ObTableQueryResultIterator(&query),
    allocator_("TableMrgFltAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    serlize_result_(&one_result),
    iterable_result_(nullptr),
    binary_heap_(compare_),
    is_inited_(false)
{
  inner_result_iters_.set_attr(ObMemAttr(MTL_ID(), "MergeInnerIters"));
}

template <typename Row, typename Compare>
ObMergeTableQueryResultIterator<Row, Compare>::ObMergeTableQueryResultIterator(const ObTableQuery& query,
                                                                              table::ObTableQueryIterableResult &one_result)
  : ObTableQueryResultIterator(&query),
    allocator_("TableMrgFltAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    serlize_result_(nullptr),
    iterable_result_(&one_result),
    binary_heap_(compare_),
    is_inited_(false),
    row_count_(0)
{
  inner_result_iters_.set_attr(ObMemAttr(MTL_ID(), "MergeInnerIters"));
}

template <typename Row, typename Compare>
bool ObMergeTableQueryResultIterator<Row, Compare>::HeapCompare::operator()(const ObRowCacheIterator *lhs,
                                                                            const ObRowCacheIterator *rhs)
{
  int ret = common::OB_SUCCESS;
  bool bret = false;
  if (OB_ISNULL(compare_)) {
    ret = common::OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(compare_));
  } else {
    bret = !compare_->operator()(lhs->row_, rhs->row_);
  }
  if (OB_FAIL(ret)) {
    error_code_ = ret;
  } else if (OB_FAIL(compare_->get_error_code())) {
    error_code_ = compare_->get_error_code();
  }
  return bret;
}

template <typename Row, typename Compare>
int ObMergeTableQueryResultIterator<Row, Compare>::init(Compare* compare)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
  } else if (OB_ISNULL(compare)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    compare_.compare_ = compare;
    if (inner_result_iters_.count() >= 1 && OB_FAIL(build_heap(inner_result_iters_))) {
      SERVER_LOG(WARN, "fail to build heap", K(ret), K(inner_result_iters_.count()));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename Row, typename Compare>
int ObMergeTableQueryResultIterator<Row, Compare>::build_heap(const ObIArray<ObTableQueryResultIterator*>& result_iters)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < result_iters.count(); ++i) {
    bool in_heap = false;
    ObRowCacheIterator* cache_iterator = OB_NEWx(ObRowCacheIterator, &allocator_);
    ObTableQueryResultIterator* iter = result_iters.at(i);
    if (OB_ISNULL(cache_iterator)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc cache iter", K(ret));
    } else if (OB_FAIL(cache_iterator->init(iter))) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN, "fail to init cache_iter", K(ret));
      }
    } else if (OB_FAIL(binary_heap_.push(cache_iterator, in_heap))) {
      SERVER_LOG(WARN, "fail to push heap", K(ret));
    } else if (OB_FAIL(compare_.get_error_code())) {
      SERVER_LOG(WARN, "fail to compare items", K(ret));
    }

    if (in_heap == false && OB_NOT_NULL(cache_iterator)) {
      cache_iterator->~ObRowCacheIterator();
      allocator_.free(cache_iterator);
    }
  }
  return ret;
}


template <typename Row, typename Compare>
int ObMergeTableQueryResultIterator<Row, Compare>::get_next_result(ObTableQueryResult *&one_result)
{
  int ret = OB_SUCCESS;
  one_result = serlize_result_;
  if (binary_heap_.empty()) {
    ret = OB_ITER_END;
  } else {
    int limit = query_->get_batch() <= 0 ?
                static_cast<int64_t>(ObTableQueryResult::get_max_packet_buffer_length() - 1024)
                : query_->get_batch();
    while (!binary_heap_.empty() && serlize_result_->get_row_count() < limit) {
      ObRowCacheIterator* cache_iterator = binary_heap_.top();
      bool in_heap = true;
      if (OB_FAIL(binary_heap_.pop())) {
        SERVER_LOG(WARN, "fail to pop items", K(ret));
      } else if (FALSE_IT(in_heap = false)) {
      } else if (OB_FAIL(compare_.get_error_code())) {
        SERVER_LOG(WARN, "fail to compare items", K(ret));
      } else if (OB_FAIL(serlize_result_->add_row(cache_iterator->row_))) {
        SERVER_LOG(WARN, "fail to add row", K(ret));
      } else {
        // Clean up rows that have already been serialized to response
        cache_iterator->allocator_.reuse();
        Row row;
        if (OB_FAIL(cache_iterator->get_next_row(row))) {
          if (ret != OB_ITER_END) {
            SERVER_LOG(WARN, "fail to get next row from ObRowCacheIterator");
          } else if (row.is_valid()) {
            ret = OB_SUCCESS;
            if (OB_FAIL(ob_write_row(cache_iterator->allocator_, row, cache_iterator->row_))) {
              SERVER_LOG(WARN, "fail to copy row", K(ret));
            } else if (OB_FAIL(binary_heap_.push(cache_iterator, in_heap))) {
              SERVER_LOG(WARN, "fail to push items", K(ret));
            } else if (OB_FAIL(compare_.get_error_code())) {
              SERVER_LOG(WARN, "fail to compare items", K(ret));
            }
          }
        } else {
          if (OB_FAIL(ob_write_row(cache_iterator->allocator_, row, cache_iterator->row_))) {
            SERVER_LOG(WARN, "fail to copy row", K(ret));
          } else if (OB_FAIL(binary_heap_.push(cache_iterator, in_heap))) {
            SERVER_LOG(WARN, "fail to push items", K(ret));
          } else if (OB_FAIL(compare_.get_error_code())) {
            SERVER_LOG(WARN, "fail to compare items", K(ret));
          }
        }
      }
      if (!in_heap && OB_NOT_NULL(cache_iterator)) {
        cache_iterator->~ObRowCacheIterator();
        allocator_.free(cache_iterator);
      }
    }
  }
  return ret;
}

template <typename Row, typename Compare>
int ObMergeTableQueryResultIterator<Row, Compare>::get_next_result(ObTableQueryIterableResult *&one_result)
{
  int ret = OB_SUCCESS;
  one_result = iterable_result_;
  if (binary_heap_.empty()) {
    ret = OB_ITER_END;
  } else {
    int limit = query_->get_batch() <= 0 ?
                static_cast<int64_t>(ObTableQueryResult::get_max_packet_buffer_length() - 1024)
                : query_->get_batch();
    while (!binary_heap_.empty() && iterable_result_->get_row_count() < limit) {
      ObRowCacheIterator* cache_iterator = binary_heap_.top();
      bool in_heap = true;
      if (OB_FAIL(binary_heap_.pop())) {
        SERVER_LOG(WARN, "fail to pop items", K(ret));
      } else if (FALSE_IT(in_heap = false)) {
      } else if (OB_FAIL(compare_.get_error_code())) {
        SERVER_LOG(WARN, "fail to compare items", K(ret));
      } else if (OB_FAIL(iterable_result_->add_row(cache_iterator->row_))) {
        SERVER_LOG(WARN, "fail to add row", K(ret));
      } else {
        // Clean up rows that have already been serialized to response
        cache_iterator->allocator_.reuse();
        Row row;
        if (OB_FAIL(cache_iterator->get_next_row(row))) {
          if (ret != OB_ITER_END) {
            SERVER_LOG(WARN, "fail to get next row from ObRowCacheIterator");
          } else if (row.is_valid()) {
            ret = OB_SUCCESS;
            if (OB_FAIL(ob_write_row(cache_iterator->allocator_, row, cache_iterator->row_))) {
              SERVER_LOG(WARN, "fail to copy row", K(ret));
            } else if (OB_FAIL(binary_heap_.push(cache_iterator, in_heap))) {
              SERVER_LOG(WARN, "fail to push items", K(ret));
            } else if (OB_FAIL(compare_.get_error_code())) {
              SERVER_LOG(WARN, "fail to compare items", K(ret));
            }
          }
        } else {
          if (OB_FAIL(ob_write_row(cache_iterator->allocator_, row, cache_iterator->row_))) {
            SERVER_LOG(WARN, "fail to copy row", K(ret));
          } else if (OB_FAIL(binary_heap_.push(cache_iterator, in_heap))) {
            SERVER_LOG(WARN, "fail to push items", K(ret));
          } else if (OB_FAIL(compare_.get_error_code())) {
            SERVER_LOG(WARN, "fail to compare items", K(ret));
          }
        }
      }
      if (!in_heap && OB_NOT_NULL(cache_iterator)) {
        cache_iterator->~ObRowCacheIterator();
        allocator_.free(cache_iterator);
      }
    }
  }
  return ret;
}

template <typename Row, typename Compare>
int ObMergeTableQueryResultIterator<Row, Compare>::ObRowCacheIterator::init(ObTableQueryResultIterator* iter)
{
  int ret = OB_SUCCESS;

  ObTableQueryIterableResult* iterable_result = nullptr;
  Row first_row;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "result iter is nullptr", K(ret));
  } else if (OB_FAIL(iter->get_next_result(iterable_result))) {
    if (ret == OB_ITER_END) {
      ret = OB_ITER_END;
    } else {
      SERVER_LOG(WARN, "fail to get iterable result", K(ret));
    }
  } else if (OB_FAIL(iterable_result->get_row(first_row))) {
    if (ret == OB_ARRAY_OUT_OF_RANGE) {
      ret = OB_ITER_END;
    } else {
      SERVER_LOG(WARN, "fail to get_next_entity", K(ret));
    }
  } else {
    iterable_result_ = iterable_result;
    result_iter_ = iter;
    if (OB_FAIL(ob_write_row(allocator_, first_row, row_))) {
      SERVER_LOG(WARN, "fail to copy row", K(ret));
    }
  }
  return ret;
}


template <typename Row, typename Compare>
int ObMergeTableQueryResultIterator<Row, Compare>::ObRowCacheIterator::ObRowCacheIterator::get_next_row(Row& row)
{
 int ret = OB_SUCCESS;
  Row new_row;
  if (OB_FAIL(iterable_result_->get_row(new_row))) {
    if (ret == OB_ARRAY_OUT_OF_RANGE) { // The buffer is exhausted; need to fetch more from the iterator
      if (OB_FAIL(result_iter_->get_next_result(iterable_result_))) {
        if (OB_ITER_END == ret) {
          // No more results available
          SERVER_LOG(DEBUG, "No more results available", K(ret));
        } else {
          SERVER_LOG(WARN, "fail to get_next_result", K(ret));
        }
      } else {
        if (OB_FAIL(iterable_result_->get_row(new_row))) {
          if (ret == OB_ARRAY_OUT_OF_RANGE) {
            SERVER_LOG(WARN, "fail to get_row after refreshing iterable_result_ OB_ITER_END", K(ret));
            ret = OB_ITER_END;
          } else {
            SERVER_LOG(WARN, "fail to get_row after refreshing iterable_result_", K(ret));
          }
        } else {
          row = new_row;
        }
      }
    } else {
      SERVER_LOG(WARN, "unexpect error ", K(ret));
    }
  } else {
    row = new_row;
  }
  return ret;
}

template <typename Row, typename Compare>
int ObMergeTableQueryResultIterator<Row, Compare>::seek(const ObString& key)
{
  int ret = OB_SUCCESS;
  int64_t heap_size = binary_heap_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < heap_size; ++i) {
    ObRowCacheIterator* cache_iter = binary_heap_.top();
    if (OB_FAIL(binary_heap_.pop())) {
      SERVER_LOG(WARN, "fail to pop items", K(ret));
    } else if (OB_FAIL(compare_.get_error_code())) {
      SERVER_LOG(WARN, "fail to compare items", K(ret));
    }

    Row &current_row = cache_iter->row_;
    while (OB_SUCC(ret) && current_row.get_cell(0).get_string().compare(key) < 0) {
      Row next_row;
      if (OB_FAIL(cache_iter->get_next_row(next_row))) {
        if (OB_ITER_END == ret) {
          // No more rows in this iterator
          SERVER_LOG(DEBUG, "No more results available", K(ret));
        } else {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        current_row = next_row;
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(ob_write_row(cache_iter->allocator_, current_row, cache_iter->row_))) {
      SERVER_LOG(WARN, "fail to copy row", K(ret));
    } else if (OB_FAIL(binary_heap_.push(cache_iter))) {
      SERVER_LOG(WARN, "fail to push", K(ret));
    } else if (OB_FAIL(compare_.get_error_code())) {
      SERVER_LOG(WARN, "fail to compare items", K(ret));
    }
  }

  return ret;
}

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_MERGE_FILTERS_H */
