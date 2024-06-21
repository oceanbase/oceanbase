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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/join/hash_join/ob_hj_partition.h"

namespace oceanbase
{
namespace sql
{

ObHJPartition::~ObHJPartition()
{
  close();
}

int ObHJPartition::rescan()
{
  int ret = OB_SUCCESS;
  store_iter_.reset();
  return ret;
}

int ObHJPartition::finish_dump(bool memory_need_dump)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_store_.finish_add_row(memory_need_dump))) {
    LOG_WARN("failed to finish chunk row store", K(ret));
  } else if (memory_need_dump && 0 != row_store_.get_mem_used()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect memroy is 0", K(ret), K(row_store_.get_mem_used()));
  }
  return ret;
}

int ObHJPartition::dump(bool all_dump, int64_t dumped_size)
{
  int ret = OB_SUCCESS;
  if (0 >= row_store_.get_mem_used()) {
    // ret = OB_ERR_UNEXPECTED;
    // LOG_WARN("unexpect chunk row store use memory", K(ret), K(row_store_.get_mem_used()));
  } else if (OB_FAIL(row_store_.dump(all_dump, dumped_size))) {
    LOG_WARN("failed to dump data to chunk row store", K(ret));
  }
  return ret;
}

int ObHJPartition::add_batch(const common::IVectorPtrs &vectors,
                             const uint16_t selector[],
                             const int64_t size,
                             ObHJStoredRow **stored_rows /*= nullptr*/)
{
  n_add_rows_ += size;
  return row_store_.add_batch(vectors, selector, size,
                              reinterpret_cast<ObCompactRow **>(stored_rows));
}

int ObHJPartition::get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                                  ObEvalCtx &ctx,
                                  const int64_t max_rows,
                                  int64_t &read_rows,
                                  const ObHJStoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  const ObCompactRow **inner_stored_row = reinterpret_cast<const ObCompactRow **>(stored_row);
  read_rows = 0;
  while (OB_SUCC(ret) && 0 == read_rows) {
    if (OB_FAIL(store_iter_.get_next_batch(exprs, ctx, max_rows, read_rows,
                                           inner_stored_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (n_get_rows_ != row_store_.get_row_cnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Got row count is not match with row count of chunk row store", K(ret),
          K(n_get_rows_), K(row_store_.get_row_cnt()));
      }
    } else {
      n_get_rows_ += read_rows;
    }
  }

  return ret;
}

int ObHJPartition::get_next_batch(const ObHJStoredRow **stored_row,
                                  const int64_t max_rows,
                                  int64_t &read_rows)
{
  int ret = OB_SUCCESS;

  const ObCompactRow **inner_stored_row = reinterpret_cast<const ObCompactRow **>(stored_row);
  read_rows = 0;
  // here read empty once after iter end
  while (OB_SUCC(ret) && 0 == read_rows) {
    ret = store_iter_.get_next_batch(max_rows, read_rows, inner_stored_row);
    if (OB_FAIL(ret)) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (n_get_rows_ != row_store_.get_row_cnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Got row count is not match with row count of chunk row store", K(ret),
          K(n_get_rows_), K(row_store_.get_row_cnt()));
      }
    } else {
      n_get_rows_ += read_rows;
    }
  }

  return ret;
}

int ObHJPartition::init_progressive_iterator()
{
  int ret = OB_SUCCESS;
  store_iter_.reset();
  if (OB_FAIL(row_store_.begin(store_iter_))) {
    LOG_WARN("failed to set row store iterator", K(ret));
  }
  n_get_rows_ = 0;
  return ret;
}

// 可能会读多次，所以每次都应该set iterator，同时reset
int ObHJPartition::begin_iterator()
{
  int ret = OB_SUCCESS;
  store_iter_.reset();
  if (OB_FAIL(row_store_.begin(store_iter_))) {
    LOG_WARN("failed to set row store iterator", K(ret));
  }
  n_get_rows_ = 0;
  return ret;
}

int ObHJPartition::init(const ObExprPtrIArray &exprs, const int64_t max_batch_size,
                        const ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(tenant_id_, common::ObModIds::OB_ARENA_HASH_JOIN, ObCtxIds::WORK_AREA);
  if (OB_FAIL(row_store_.init(exprs,
                              max_batch_size,
                              mem_attr,
                              0/*mem limit*/,
                              true,
                              8/*extra_size*/,
                              compressor_type))) {
    LOG_WARN("failed to init chunk row store", K(ret));
  }
  return ret;
}

int ObHJPartition::open()
{
  return begin_iterator();
}

void ObHJPartition::close()
{
  store_iter_.reset();
  row_store_.reset();
}

} // end namespace sql
} // end namespace oceanbase
