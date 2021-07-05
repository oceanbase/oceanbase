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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_IN_MEMORY_TOPN_SORT_
#define OCEANBASE_SQL_ENGINE_SORT_OB_IN_MEMORY_TOPN_SORT_

#include "lib/container/ob_heap.h"
#include "lib/string/ob_string.h"
#include "lib/charset/ob_charset.h"
#include "common/object/ob_object.h"
#include "common/row/ob_row.h"
#include "common/row/ob_row_store.h"
#include "sql/engine/sort/ob_base_sort.h"

namespace oceanbase {

namespace sql {

class ObInMemoryTopnSort : public ObBaseSort {
public:
  ObInMemoryTopnSort();
  virtual ~ObInMemoryTopnSort();
  virtual void reset();
  virtual void reuse();
  virtual int add_row(const common::ObNewRow& row, bool& need_sort);
  virtual int sort_rows();
  virtual int get_next_row(common::ObNewRow& row);
  virtual int64_t get_row_count() const override;
  virtual int64_t get_used_mem_size() const override;
  virtual int get_next_compact_row(common::ObString& compact_row);
  virtual int set_sort_columns(const common::ObIArray<ObSortColumn>& sort_columns, const int64_t preifx_pos);
  inline void set_fetch_with_ties(bool is_fetch_with_ties)
  {
    is_fetch_with_ties_ = is_fetch_with_ties;
  }
  inline void set_iter_end()
  {
    iter_end_ = true;
  }
  inline bool is_iter_end()
  {
    return iter_end_;
  }
  // TO_STRING_KV(K_(sort_array_pos));
private:
  // Optimize mem usage/performance of top-n sort:
  // Record buf_len of each allocated row. When old row pop-ed out of the heap
  // and has enough space for new row, use the space of old row to store new row
  // instead of allocating space for new row.
  // Note that this is not perfect solution, it cannot handle the case that row size
  // keeps going up. However, this can cover most cases.
  struct RowWrapper {
    RowWrapper() : row_(), buf_len_(0)
    {}

    TO_STRING_KV(K_(row), K_(buf_len));
    common::ObNewRow row_;
    int64_t buf_len_;
  };

  struct RowComparer {
    explicit RowComparer(const common::ObIArray<ObSortColumn>* sort_columns);
    bool operator()(const RowWrapper* r1, const RowWrapper* r2);
    inline bool operator()(const common::ObNewRow& r1, const common::ObNewRow& r2);
    int get_error_code()
    { /* never fail */
      return common::OB_SUCCESS;
    }
    const common::ObIArray<ObSortColumn>* sort_columns_;
  };

private:
  int adjust_topn_heap(const common::ObNewRow& row);

private:
  // data members
  int64_t topn_sort_array_pos_;
  bool is_fetch_with_ties_;  // for fetch with ties
  bool iter_end_;
  common::ObNewRow* last_row_;
  RowComparer cmp_;
  common::ObBinaryHeap<RowWrapper*, RowComparer> heap_;
  DISALLOW_COPY_AND_ASSIGN(ObInMemoryTopnSort);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_IN_MEMORY_TOPN_SORT_ */
