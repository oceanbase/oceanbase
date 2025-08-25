/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_vector.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObColDesc;
} // namespace schema
} // namespace share
namespace storage
{
class ObDirectLoadDatumRow;

class ObDirectLoadBatchRows
{
public:
  ObDirectLoadBatchRows();
  ~ObDirectLoadBatchRows();
  void reset();
  void reuse();
  int init(const common::ObIArray<share::schema::ObColDesc> &col_descs,
           const sql::ObBitVector *col_nullables, const int64_t max_batch_size,
           const ObDirectLoadRowFlag &row_flag);

  // 深拷贝
  int append_row(const ObDirectLoadDatumRow &datum_row);
  int append_batch(const IVectorPtrs &vectors, const int64_t offset, const int64_t size);
  int append_batch(const ObIArray<ObDatumVector> &datum_vectors, const int64_t offset,
                   const int64_t size);
  int append_selective(const ObDirectLoadBatchRows &src, const uint16_t *selector,
                       const int64_t size);
  int append_selective(const IVectorPtrs &vectors, const uint16_t *selector, int64_t size);
  int append_selective(const ObIArray<ObDatumVector> &datum_vectors, const uint16_t *selector,
                       int64_t size);

  // 浅拷贝
  int shallow_copy(const IVectorPtrs &vectors, const int64_t batch_size);
  int shallow_copy(const ObIArray<ObDatumVector> &datum_vectors, const int64_t batch_size);

  int get_datum_row(const int64_t batch_idx, ObDirectLoadDatumRow &datum_row) const;

  const ObIArray<ObDirectLoadVector *> &get_vectors() const { return vectors_; }
  int64_t get_max_batch_size() const { return max_batch_size_; }
  const ObDirectLoadRowFlag &get_row_flag() const { return row_flag_; }
  void set_row_flag(const ObDirectLoadRowFlag &row_flag) { row_flag_ = row_flag; }
  inline int64_t get_column_count() const
  {
    return row_flag_.uncontain_hidden_pk_ ? vectors_.count() - 1 : vectors_.count();
  }

  inline void set_size(const int64_t size) { size_ = size; }
  inline int64_t size() const { return size_; }
  inline int64_t remain_size() const { return max_batch_size_ - size_; }
  inline bool empty() const { return 0 == size_; }
  inline bool full() const { return size_ == max_batch_size_; }

  // Total memory usage
  int64_t memory_usage() const;
  // Rows bytes usage
  int64_t bytes_usage() const;

  TO_STRING_KV(K_(vectors), K_(max_batch_size), K_(row_flag), K_(size), K_(is_inited));

private:
  int init_vectors(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                   const sql::ObBitVector *col_nullables, int64_t max_batch_size);

private:
  common::ObArenaAllocator allocator_; // 常驻内存分配器
  ObArray<ObDirectLoadVector *> vectors_;
  int64_t max_batch_size_;
  ObDirectLoadRowFlag row_flag_;
  int64_t size_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
