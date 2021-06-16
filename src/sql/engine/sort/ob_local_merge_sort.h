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

#ifndef OCEANBASE_SQL_ENGINE_SORT_LOCAL_MERGE_SORT_H_
#define OCEANBASE_SQL_ENGINE_SORT_LOCAL_MERGE_SORT_H_

#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/sort/ob_base_sort.h"

namespace oceanbase {
namespace sql {
class ObExecContext;

class ObLMSRowCompare : public ObRowComparer {
public:
  bool operator()(int64_t row_idx1, int64_t row_idx2);
};

class ObLocalMergeSort : public ObBaseSort {
private:
  class InputReader {
  public:
    InputReader(int64_t start_pos, int64_t end_pos) : start_pos_(start_pos), end_pos_(end_pos), cur_pos_(start_pos)
    {}
    ~InputReader()
    {}
    int get_row(const common::ObNewRow*& row, common::ObArray<const ObBaseSort::StrongTypeRow*>& sort_array);

    TO_STRING_KV(K_(start_pos), K_(end_pos));

  private:
    int64_t start_pos_;
    int64_t end_pos_;
    int64_t cur_pos_;
  };

public:
  ObLocalMergeSort();
  virtual ~ObLocalMergeSort();

  virtual void reset();
  virtual void reuse();
  virtual int add_row(const common::ObNewRow& row, bool& need_sort);
  virtual int sort_rows();
  virtual int get_next_row(common::ObNewRow& row);
  virtual int64_t get_used_mem_size() const override;
  virtual int inner_dump(ObIMergeSort& merge_sort, bool dump_last);
  virtual int dump(ObIMergeSort& merge_sort);
  virtual int final_dump(ObIMergeSort& merge_sort);
  virtual int set_sort_columns(const common::ObIArray<ObSortColumn>& sort_columns, const int64_t preifx_pos);
  virtual void set_cur_input(int64_t nth_input)
  {
    cur_input_ = nth_input;
  }

private:
  virtual int cleanup();
  virtual int save_last_row();
  virtual int inner_add_row(const common::ObNewRow& row);
  virtual int heap_get_next_row(common::ObNewRow& row);
  virtual void set_finish(bool is_finish)
  {
    is_finish_ = is_finish;
  }
  virtual void set_heap_get_row(bool is_heap_get_row)
  {
    is_heap_get_row_ = is_heap_get_row;
  }
  virtual int alloc_reader(int64_t& start_pos, int64_t end_pos);

private:
  bool is_finish_;
  bool is_heap_get_row_;
  int64_t cur_input_;
  // the end pos of last group
  int64_t last_group_end_pos_;
  bool is_last_row_same_group_;
  common::ObNewRow* last_row_;
  // split N inputs
  common::ObArray<InputReader*> input_readers_;

  // for inner merge sort
  ObRowHeap<ObLMSRowCompare> row_heap_;

  // for compare last_row_ and cur_row
  common::ObArray<const common::ObNewRow*> cmp_row_arr_;
  ObLMSRowCompare cmp_fun_;

  DISALLOW_COPY_AND_ASSIGN(ObLocalMergeSort);
};

}  // namespace sql
}  // end namespace oceanbase

#endif
