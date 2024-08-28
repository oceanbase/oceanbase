/** * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "sql/engine/basic/ob_temp_row_store.h"

namespace oceanbase
{
namespace sql
{

class ObEvalCtx;
class ObExpr;

class ObPartitionStore
{
public:
  ObPartitionStore(int64_t tenant_id, common::ObIAllocator &alloc)
      : tenant_id_(tenant_id), alloc_this_from_(&alloc), row_store_(&alloc)
  {}
  virtual ~ObPartitionStore();

  int init(const ObExprPtrIArray &exprs, const int64_t max_batch_size,
           const ObCompressorType compressor_type, uint32_t extra_size);
  int open();
  void close();
  int rescan();
  inline int add_row(const common::ObIArray<ObExpr*> &exprs,
              ObEvalCtx &eval_ctx,
              ObCompactRow *&stored_row) {
    return row_store_.add_row(exprs, eval_ctx, stored_row);
  }

  inline int add_row(const ObCompactRow *src, ObCompactRow *&stored_row) {
    return row_store_.add_row(reinterpret_cast<const ObCompactRow *>(src), stored_row);
  }

  int add_batch(const common::IVectorPtrs &vectors, const uint16_t selector[], const int64_t size,
                ObCompactRow **stored_rows = nullptr);

  int dump(bool all_dump, int64_t dumped_size);
  int finish_dump(bool memory_need_dump);
  int finish_add_row(bool need_dump);

  int get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                     ObEvalCtx &ctx,
                     const int64_t max_rows,
                     int64_t &read_rows,
                     const ObCompactRow **stored_rows);
  int get_next_batch(const ObCompactRow **stored_rows,
                     const int64_t max_rows,
                     int64_t &read_rows);

  inline bool has_next() { return store_iter_.has_next(); }
  int begin_iterator();

  inline int64_t get_cur_row_count() { return n_get_rows_; }
  inline int64_t get_row_count_in_memory() { return row_store_.get_row_cnt_in_memory(); }
  inline int64_t get_row_count_on_disk() { return row_store_.get_row_cnt_on_disk(); }

  int64_t get_last_buffer_mem_size() { return row_store_.get_last_buffer_mem_size(); }
  int64_t get_size_in_memory() { return row_store_.get_mem_used(); }
  int64_t get_size_on_disk() { return row_store_.get_file_size(); }

  void set_iteration_age(sql::ObTempRowStore::IterationAge &age) {
    store_iter_.set_iteration_age(&age);
  }

  inline  bool has_switch_block() { return row_store_.get_block_list_cnt() > 1; }
  inline  void set_memory_limit(int64_t limit) { row_store_.set_mem_limit(limit); }
  inline  bool is_dumped() { return 0 < row_store_.get_file_size(); }
  inline  int64_t get_dump_size() { return row_store_.get_file_size(); }
  inline  ObTempRowStore &get_row_store() { return row_store_; }

  inline const common::ObIAllocator *get_alloc_this_from() const { return alloc_this_from_; }
  TO_STRING_KV(K(n_get_rows_), K(extra_size_));

protected:
  uint64_t tenant_id_;
  const common::ObIAllocator *alloc_this_from_;
  ObTempRowStore row_store_;
  ObTempRowStore::Iterator store_iter_;
  int64_t n_get_rows_{0};
  uint32_t extra_size_{0};
};

}
} // namespace oceanbase
