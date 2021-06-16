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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_MERGE_SORT_H_
#define OCEANBASE_SQL_ENGINE_SORT_OB_MERGE_SORT_H_

#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "common/row/ob_row.h"
#include "sql/engine/sort/ob_run_file.h"
#include "sql/engine/sort/ob_sort_round.h"
#include "sql/engine/sort/ob_base_sort.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}

namespace sql {
// on-disk merge sort, used by ObSort
class ObMergeSort : public ObIMergeSort, public common::ObOuterRowIterator {
  static const int64_t MEM_LIMIT_SIZE = 128 * 1024 * 1024;  // 128M
  static const int64_t FILE_BUF_SIZE = 2 * 1024 * 1024;     // 2M
  static const int64_t EXPIRE_TIMESTAMP = 0;                // no time limit
public:
  ObMergeSort(common::ObIAllocator& allocator);
  virtual ~ObMergeSort();
  virtual void reset();
  virtual void reuse();
  int clean_up();
  int init(
      const common::ObIArray<ObSortColumn>& sort_columns, const common::ObNewRow& cur_row, const uint64_t tenant_id);
  void set_sort_columns(const common::ObIArray<ObSortColumn>& sort_columns);
  void set_merge_run_count(const int64_t merge_run_count)
  {
    merge_run_count_ = merge_run_count;
  }

  // dump run
  int dump_base_run(ObOuterRowIterator& row_iterator, bool build_fragment = true);
  // dump merge run
  int dump_merge_run();
  // build heap for cur merge run
  int build_merge_heap(const int64_t column_count, const int64_t start_idx, const int64_t end_idx);
  /// @pre build_heap()
  virtual int get_next_row(common::ObNewRow& row);
  int do_get_next_row(common::ObNewRow& row, const bool clear_on_end);
  int do_merge_sort(const int64_t column_count);
  int do_one_round(const int64_t column_count);
  virtual int build_cur_fragment();
  int attach_row(const common::ObNewRow& src, common::ObNewRow& dst);

private:
  // types and constants
  struct RowRun {
    RowRun() : id_(0), row_(NULL)
    {}
    TO_STRING_KV("run_id", id_);
    int64_t id_;
    const common::ObNewRow* row_;
  };

  struct HeapComparer {
    explicit HeapComparer(const common::ObIArray<ObSortColumn>* sort_columns, int& ret);
    bool operator()(const RowRun& run1, const RowRun& run2);
    int get_error_code()
    {
      return ret_;
    }
    void set_sort_columns(const common::ObIArray<ObSortColumn>* sort_columns)
    {
      sort_columns_ = sort_columns;
    }

  private:
    const common::ObIArray<ObSortColumn>* sort_columns_;
    int& ret_;
  };

private:
  // data members
  ObSortRun sort_run_[2];
  ObSortRun* cur_round_;
  ObSortRun* next_round_;
  int64_t merge_run_count_;  // merge run count each merge
  int64_t last_run_idx_;
  common::ObNewRow cur_row_;
  int comp_ret_;
  HeapComparer comparer_;
  common::ObBinaryHeap<RowRun, HeapComparer> heap_;
  common::ObIAllocator& allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObMergeSort);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_MERGE_SORT_H_ */
