/**
 * Copyright (c) 2025 OceanBase
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

#include "storage/direct_load/ob_direct_load_batch_rows.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadPXBatchRows
{
public:
  ObTableLoadPXBatchRows();
  ~ObTableLoadPXBatchRows();
  void reset();
  void reuse();
  int init(const common::ObIArray<share::schema::ObColDesc> &col_descs,
           const common::ObIArray<common::ObAccuracy> &col_accuracys,
           const sql::ObBitVector *col_nullables,
           const ObDirectLoadRowFlag &row_flag,
           const int64_t max_batch_size,
           // 为了老路径farm能过
           const bool need_reshape);

  // 深拷贝
  int append_batch(const IVectorPtrs &vectors, const int64_t offset, const int64_t size);
  int append_batch(const ObIArray<ObDatumVector> &datum_vectors, const int64_t offset,
                   const int64_t size);
  int append_selective(const IVectorPtrs &vectors, const uint16_t *selector, int64_t size);
  int append_selective(const ObIArray<ObDatumVector> &datum_vectors, const uint16_t *selector,
                       int64_t size);
  int append_row(const ObDirectLoadDatumRow &datum_row);

  // 浅拷贝
  int shallow_copy(const IVectorPtrs &vectors, const int64_t batch_size);
  int shallow_copy(const ObIArray<ObDatumVector> &datum_vectors, const int64_t batch_size);

  storage::ObDirectLoadBatchRows &get_batch_rows() { return batch_rows_; }

  inline int64_t get_column_count() const { return batch_rows_.get_vectors().count(); }
  inline int64_t size() const { return batch_rows_.size(); }
  inline int64_t remain_size() const { return batch_rows_.remain_size(); }
  inline bool empty() const { return batch_rows_.empty(); }
  inline bool full() const { return batch_rows_.full(); }

  // Total memory usage
  inline int64_t memory_usage() const { return batch_rows_.memory_usage(); }
  // Rows bytes usage
  inline int64_t bytes_usage() const { return batch_rows_.bytes_usage(); }

  TO_STRING_KV(K_(batch_rows), K_(is_inited));

private:
  ObArray<ObColDesc> col_descs_;
  ObArray<ObAccuracy> col_accuracys_;
  ObArenaAllocator reshape_allocator_;
  storage::ObDirectLoadBatchRows batch_rows_;
  bool need_reshape_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
