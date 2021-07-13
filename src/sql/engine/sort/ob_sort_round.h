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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_SORT_ROUND_H_
#define OCEANBASE_SQL_ENGINE_SORT_OB_SORT_ROUND_H_

#include "storage/ob_parallel_external_sort.h"

namespace oceanbase {
namespace sql {
class ObSortRun {
  friend class ObMergeSort;

public:
  typedef storage::ObFragmentWriterV2<common::ObNewRow> FragmentWriter;
  typedef storage::ObFragmentReaderV2<common::ObNewRow> FragmentReader;
  typedef common::ObArray<FragmentReader*> FragmentIteratorList;
  ObSortRun();
  virtual ~ObSortRun();
  void reset();
  int clean_up();
  int reuse();
  int init(const int64_t buf_size, const int64_t expire_timestamp, common::ObIAllocator* allocator,
      const uint64_t tenant_id);
  int prefetch(FragmentIteratorList& iters);
  int add_item(const common::ObNewRow& item);
  int build_fragment();
  bool is_inited() const
  {
    return is_inited_;
  }

protected:
  FragmentWriter writer_;
  FragmentIteratorList iters_;

private:
  bool is_inited_;
  int64_t buf_size_;
  int64_t expire_timestamp_;
  common::ObIAllocator* allocator_;
  uint64_t tenant_id_;
  int64_t dir_id_;
  bool is_writer_opened_;
};
}  // namespace sql
}  // namespace oceanbase

#endif
