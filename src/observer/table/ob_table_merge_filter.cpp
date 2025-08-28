/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_merge_filter.h"

namespace oceanbase
{
namespace table
{

int ObTableResultRowIter::next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null result iter", K(ret));
  } else if (OB_ISNULL(iterable_result_) && OB_FAIL(result_iter_->get_next_result(iterable_result_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next result", K(ret));
    }
  } else if (OB_FAIL(iterable_result_->get_row(cur_row_))) {
    if (ret == OB_ARRAY_OUT_OF_RANGE) { // The buffer is exhausted; need to fetch more from the iterator
      if (OB_FAIL(result_iter_->get_next_result(iterable_result_))) {
        if (OB_ITER_END == ret) {
          // No more results available
          SERVER_LOG(DEBUG, "No more results available", K(ret));
        } else {
          SERVER_LOG(WARN, "fail to get_next_result", K(ret));
        }
      } else {
        if (OB_FAIL(iterable_result_->get_row(cur_row_))) {
          if (ret == OB_ARRAY_OUT_OF_RANGE) {
            SERVER_LOG(WARN, "fail to get_row after refreshing iterable_result_ OB_ITER_END", K(ret));
            ret = OB_ITER_END;
          } else {  
            SERVER_LOG(WARN, "fail to get_row after refreshing iterable_result_", K(ret));
          }
        }
      }
    } else {
      SERVER_LOG(WARN, "fail to get row", K(ret));  
    }
  }
  return ret;
}

ObMergeTableQueryResultIterator::ObMergeTableQueryResultIterator(const ObTableQuery& query,
                                                                 table::ObTableQueryResult &one_result)
  : ObTableQueryResultIterator(&query),
    allocator_("TableMrgFltAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    serlize_result_(&one_result),
    iterable_result_(nullptr)
{
}

ObMergeTableQueryResultIterator::ObMergeTableQueryResultIterator(const ObTableQuery& query)
  : ObTableQueryResultIterator(&query),
    allocator_("TableMrgFltAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    serlize_result_(nullptr),
    iterable_result_(nullptr)
{
}

ObMergeTableQueryResultIterator::ObMergeTableQueryResultIterator(const ObTableQuery& query,
                                                                 table::ObTableQueryIterableResult &one_result)
  : ObTableQueryResultIterator(&query),
    allocator_("TableMrgFltAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    serlize_result_(nullptr),
    iterable_result_(&one_result)
{
}

int ObMergeTableQueryResultIterator::get_next_result(ObTableQueryResult *&one_result)
{
  int ret = OB_SUCCESS;
  one_result = serlize_result_;
  if (OB_ISNULL(serlize_result_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected null serlize_result_", K(ret));
  } else if (OB_FAIL(get_next_result(*serlize_result_))) {
    SERVER_LOG(WARN, "fail to get next result", K(ret));
  } else {}
  return ret;
}

int ObMergeTableQueryResultIterator::get_next_result(ObTableQueryResult &one_result)
{
  int ret = OB_SUCCESS;
  int limit = query_->get_batch() <= 0 ? 
              static_cast<int64_t>(ObTableQueryResult::get_max_packet_buffer_length() - 1024)
              : query_->get_batch();
  while (OB_SUCC(ret) && one_result.get_row_count() < limit) {
    ObNewRow *row = nullptr; 
    if (OB_FAIL(get_next_row(row))) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (OB_FAIL(one_result.add_row(*row))) {
      SERVER_LOG(WARN, "fail to add row", K(ret));
    }
  }
  return ret;
}

int ObMergeTableQueryResultIterator::get_next_result(ObTableQueryIterableResult *&one_result)
{
  int ret = OB_SUCCESS;
  one_result = iterable_result_;
  if (OB_FAIL(get_next_result(*iterable_result_))) {
    SERVER_LOG(WARN, "fail to get next result", K(ret));
  }
  return ret;
}

int ObMergeTableQueryResultIterator::get_next_result(ObTableQueryIterableResult &one_result)
{
  int ret = OB_SUCCESS;
  int limit = query_->get_batch() <= 0 ? 
              static_cast<int64_t>(ObTableQueryResult::get_max_packet_buffer_length() - 1024)
              : query_->get_batch();
  while (OB_SUCC(ret) && one_result.get_row_count() < limit) {
    ObNewRow *row = nullptr; 
    if (OB_FAIL(get_next_row(row))) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (OB_FAIL(one_result.add_row(*row))) {
      SERVER_LOG(WARN, "fail to add row", K(ret));
    }
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
