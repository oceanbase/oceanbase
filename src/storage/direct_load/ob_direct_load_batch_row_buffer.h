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

#include "share/vector/ob_i_vector.h"

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
class ObDirectLoadRowFlag;

class ObDirectLoadBatchRowBuffer
{
public:
  ObDirectLoadBatchRowBuffer();
  ~ObDirectLoadBatchRowBuffer();
  void reset();
  void reuse();
  int init(const common::ObIArray<share::schema::ObColDesc> &col_descs,
           const int64_t max_batch_size);
  int append_row(const ObDirectLoadDatumRow &datum_row,
                 const ObDirectLoadRowFlag &row_flag,
                 bool &is_full);
  int append_row(const IVectorPtrs &vectors,
                 const int64_t row_idx,
                 const ObDirectLoadRowFlag &row_flag,
                 bool &is_full);
  int get_batch(const IVectorPtrs *&vectors, int64_t &row_count);
  bool empty() const { return 0 == row_count_; }
  const IVectorPtrs &get_vectors() const { return vectors_; }
  int64_t get_max_batch_size() const { return max_batch_size_; }
  int64_t get_row_count() const { return row_count_; }
  TO_STRING_KV(K_(vectors), K_(max_batch_size), K_(row_count), K_(is_inited));

private:
  int init_vectors(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                   int64_t max_batch_size);
  int to_vector(const ObDatum &datum, ObIVector *vector, const int64_t batch_idx,
                ObIAllocator *allocator);
  int to_vector(ObIVector *src_vector, const int64_t src_batch_idx, ObIVector *vector,
                const int64_t batch_idx, ObIAllocator *allocator);

private:
  common::ObArenaAllocator allocator_; // 常驻内存分配器
  ObArray<common::ObArenaAllocator *> vector_allocators_; // 动态内存分配器
  ObArray<ObIVector *> vectors_;
  int64_t max_batch_size_;
  int64_t row_count_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
