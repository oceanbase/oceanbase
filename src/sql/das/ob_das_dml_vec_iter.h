/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_DAS_DML_VEC_ITER_H
#define OCEANBASE_DAS_DML_VEC_ITER_H

#include "src/sql/das/ob_das_domain_utils.h"

namespace oceanbase
{
namespace sql
{

class ObVecIndexDMLIterator final : public ObDomainDMLIterator
{
public:
  static constexpr char* VEC_DELTA_INSERT = const_cast<char*>("I");
  static constexpr char* VEC_DELTA_DELETE = const_cast<char*>("D");
  ObVecIndexDMLIterator(
      common::ObIAllocator &allocator,
      const IntFixedArray *row_projector,
      ObDASWriteBuffer::Iterator &write_iter,
      const ObDASDMLBaseCtDef *das_ctdef,
      const ObDASDMLBaseCtDef *main_ctdef)
    : ObDomainDMLIterator(allocator, row_projector, write_iter, das_ctdef, main_ctdef),
      is_old_row_(das_ctdef_->op_type_ == ObDASOpType::DAS_OP_TABLE_UPDATE)
  {}
  virtual ~ObVecIndexDMLIterator() = default;
  INHERIT_TO_STRING_KV("ObDomainDMLIterator", ObDomainDMLIterator, K_(is_old_row));
protected:
  int get_vec_data(
      const ObChunkDatumStore::StoredRow *store_row,
      const int64_t vec_id_idx,
      const int64_t vector_idx,
      int64_t &vec_id,
      ObString &vector);
  int get_vec_data_for_update(
      const ObChunkDatumStore::StoredRow *store_row,
      const int64_t vec_id_idx,
      const int64_t vector_idx,
      int64_t &vec_id,
      ObString &vector);
private:
  virtual int generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row) override;
  int generate_vec_delta_buff_row(common::ObIAllocator &allocator,
    const ObChunkDatumStore::StoredRow *store_row,
    const int64_t vec_id_idx,
    const int64_t type_idx,
    const int64_t vector_idx,
    const int64_t &vec_id,
    ObString &vector,
    ObDomainIndexRow &rows);
  int get_vector_index_column_idxs(int64_t &vec_id_idx, int64_t &type_idx, int64_t &vector_idx);
private:
  bool is_old_row_;
};

class ObSparseVecIndexDMLIterator final : public ObDomainDMLIterator
{
public:
  ObSparseVecIndexDMLIterator(
      common::ObIAllocator &allocator,
      const IntFixedArray *row_projector,
      ObDASWriteBuffer::Iterator &write_iter,
      const ObDASDMLBaseCtDef *das_ctdef,
      const ObDASDMLBaseCtDef *main_ctdef)
    : ObDomainDMLIterator(allocator, row_projector, write_iter, das_ctdef, main_ctdef) {}

  virtual ~ObSparseVecIndexDMLIterator() = default;
private:
  virtual int generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row) override;
  int get_sparse_vector_index_column_idxs(int64_t &sparse_vec_idx, int64_t &dim_idx, int64_t &docid_idx, int64_t &value_idx);

  int get_sparse_vec_data(
    const ObChunkDatumStore::StoredRow *store_row,
    const int64_t docid_idx,
    const int64_t sparse_vec_idx,
    ObDatum &docid,
    ObString &sparse_vec);
  int get_sparse_vec_data_for_update(
    const ObChunkDatumStore::StoredRow *store_row,
    const int64_t docid_idx,
    const int64_t sparse_vec_idx,
    ObDatum &docid,
    ObString &sparse_vec);
  int generate_sparse_vec_index_row(common::ObIAllocator &allocator,
    const ObChunkDatumStore::StoredRow *store_row,
    const int64_t dim_idx,
    const int64_t docid_idx,
    const int64_t value_idx,
    const int64_t vec_idx,
    const ObDatum &docid,
    ObString &sparse_vec,
    ObDomainIndexRow &rows);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_DAS_DML_VEC_ITER_H
