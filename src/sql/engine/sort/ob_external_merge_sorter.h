/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_EXTERNAL_MERGE_SORTER_H_
#define OCEANBASE_SQL_ENGINE_SORT_OB_EXTERNAL_MERGE_SORTER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_heap.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/sort/ob_sort_vec_op_chunk.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief 公共外排类，提供通用的外部归并排序功能
 *
 * @tparam Compare 比较器类型
 * @tparam Store_Row 存储行类型
 * @tparam has_addon 是否有addon字段
 */
template <typename Compare, typename Store_Row, bool has_addon>
class ObExternalMergeSorter
{
public:
  static const int64_t MAX_MERGE_WAYS = 256;
  typedef ObSortVecOpChunk<Store_Row, has_addon> ChunkType;
  typedef common::ObBinaryHeap<ChunkType *, Compare, MAX_MERGE_WAYS> MergeHeap;

  ObExternalMergeSorter(common::ObIAllocator &allocator, Compare &comp);
  virtual ~ObExternalMergeSorter();
  void reset();

  int init(common::ObDList<ChunkType> &chunks, const int64_t merge_ways);
  int get_next_row(const Store_Row *&sk_row, const Store_Row *&addon_row);
  bool is_inited() const { return is_inited_; }

private:
  int heap_next(ChunkType *&chunk);

private:
  common::ObIAllocator &allocator_;
  Compare &comp_;
  MergeHeap *heap_;
  bool heap_iter_begin_;
  bool is_inited_;
};


template <typename Compare, typename Store_Row, bool has_addon>
ObExternalMergeSorter<Compare, Store_Row, has_addon>::ObExternalMergeSorter(
    common::ObIAllocator &allocator, Compare &comp)
  : allocator_(allocator),
    comp_(comp),
    heap_(nullptr),
    heap_iter_begin_(false),
    is_inited_(false)
{
}

template <typename Compare, typename Store_Row, bool has_addon>
ObExternalMergeSorter<Compare, Store_Row, has_addon>::~ObExternalMergeSorter()
{
  reset();
}

template <typename Compare, typename Store_Row, bool has_addon>
void ObExternalMergeSorter<Compare, Store_Row, has_addon>::reset()
{
  if (nullptr != heap_) {
    heap_->~MergeHeap();
    allocator_.free(heap_);
    heap_ = nullptr;
  }

  heap_iter_begin_ = false;
  is_inited_ = false;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObExternalMergeSorter<Compare, Store_Row, has_addon>::init(
    common::ObDList<ChunkType> &chunks, const int64_t merge_ways)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SQL_ENG_LOG(WARN, "already initialized", K(ret));
  } else if (chunks.get_size() < 2 || merge_ways < 2 || merge_ways > chunks.get_size()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(chunks.get_size()), K(merge_ways), K(chunks.get_size()));
  } else if (OB_ISNULL(heap_ = OB_NEWx(MergeHeap, (&allocator_), comp_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
  } else {
    // 初始化指定数量的chunk迭代器并添加到堆中
    ChunkType *chunk = chunks.get_first();
    for (int64_t i = 0; i < merge_ways && OB_SUCC(ret); i++) {
      chunk->reset_row_iter();
      if (OB_FAIL(chunk->init_row_iter())) {
        SQL_ENG_LOG(WARN, "init iterator failed", K(ret));
      } else if (OB_FAIL(chunk->get_next_row()) || nullptr == chunk->sk_row_) {
        if (OB_ITER_END == ret || OB_SUCCESS == ret) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "row store is not empty, iterate end is unexpected", K(ret),
                      KP(chunk->sk_row_));
        }
        SQL_ENG_LOG(WARN, "get next row failed", K(ret));
      } else if (OB_FAIL(heap_->push(chunk))) {
        SQL_ENG_LOG(WARN, "heap push failed", K(ret));
      } else {
        chunk = chunk->get_next();
      }
    }

    // 初始化堆
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      heap_iter_begin_ = false;
      SQL_ENG_LOG(DEBUG, "external merge sorter init success",
                 K(chunks.get_size()), K(merge_ways));
    }
  }

  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObExternalMergeSorter<Compare, Store_Row, has_addon>::heap_next(ChunkType *&chunk)
{
  int ret = OB_SUCCESS;
  chunk = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not initialized", K(ret));
  } else if (OB_ISNULL(heap_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "heap is null", K(ret));
  } else {
    if (heap_iter_begin_) {
      if (!heap_->empty()) {
        ChunkType *it = heap_->top();
        bool is_end = false;

        // 获取下一行
        if (OB_FAIL(it->get_next_row())) {
          if (OB_ITER_END == ret) {
            is_end = true;
            ret = OB_SUCCESS;
          } else {
            SQL_ENG_LOG(WARN, "get next row failed", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (is_end) {
            if (OB_FAIL(heap_->pop())) {
              SQL_ENG_LOG(WARN, "heap pop failed", K(ret));
            }
          } else {
            if (OB_FAIL(heap_->replace_top(it))) {
              SQL_ENG_LOG(WARN, "heap replace failed", K(ret));
            }
          }
        }
      }
    } else {
      heap_iter_begin_ = true;
    }

    if (OB_SUCC(ret)) {
      if (heap_->empty()) {
        ret = OB_ITER_END;
      } else {
        chunk = heap_->top();
      }
    }
  }

  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObExternalMergeSorter<Compare, Store_Row, has_addon>::get_next_row(
    const Store_Row *&sk_row, const Store_Row *&addon_row)
{
  int ret = OB_SUCCESS;
  sk_row = nullptr;
  addon_row = nullptr;

  ChunkType *chunk = nullptr;
  if (OB_FAIL(heap_next(chunk))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "heap next failed", K(ret));
    }
  } else if (OB_ISNULL(chunk)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "chunk is null", K(ret));
  } else {
    sk_row = chunk->sk_row_;
    if (has_addon) {
      addon_row = chunk->addon_row_;
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase


#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_EXTERNAL_MERGE_SORTER_H_ */
