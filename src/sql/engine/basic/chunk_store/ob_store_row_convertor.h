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

#ifndef OCEANBASE_BASIC_OB_STORE_ROW_CONVERTOR_H_
#define OCEANBASE_BASIC_OB_STORE_ROW_CONVERTOR_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "src/share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_compact_row.h"

namespace oceanbase
{
namespace sql
{

class ObStoreRowConvertor final
{
public:
  ObStoreRowConvertor() : store_(nullptr), row_meta_(nullptr), crow_buf_(nullptr),
                          srow_buf_(nullptr), crow_size_(0), srow_size_(0) {}
  int init(ObTempBlockStore *store, const RowMeta *row_meta);
  int stored_row_to_compact_row(const ObChunkDatumStore::StoredRow *srow, const ObCompactRow *&crow);
  int compact_row_to_stored_row(const ObCompactRow *crow, const ObChunkDatumStore::StoredRow *&srow);
  int get_compact_row_from_expr(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, const ObCompactRow *&crow);
private:
  int calc_conv_row_size(const ObChunkDatumStore::StoredRow *srow, int64_t &row_size);
  int calc_conv_row_size(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, int64_t &row_size);
  int calc_conv_row_size(const ObCompactRow *crow, int64_t &row_size);
  int alloc_compact_row_buf(const int64_t size)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(crow_buf_)) {
      crow_buf_ = static_cast<char *>(store_->alloc(size));
      crow_size_ = size;
    } else if (crow_size_ < size) {
      store_->free(crow_buf_, crow_size_);
      crow_buf_ = static_cast<char *>(store_->alloc(size));
      crow_size_ = size;
    }
    if (OB_ISNULL(crow_buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "fail to alloc memory", K(ret));
    }
    return ret;
  }
  int alloc_stored_row_buf(const int64_t size)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(srow_buf_)) {
      srow_buf_ = static_cast<char *>(store_->alloc(size));
      srow_size_ = size;
    } else if (srow_size_ < size) {
      store_->free(srow_buf_, srow_size_);
      srow_buf_ = static_cast<char *>(store_->alloc(size));
      srow_size_ = size;
    }
    if (OB_ISNULL(srow_buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "fail to alloc memory", K(ret));
    }
    return ret;
  }

private:
  ObTempBlockStore *store_;
  const RowMeta *row_meta_;
  char *crow_buf_; // buf to store compact row;
  char *srow_buf_; // buf to store stored row;
  int64_t crow_size_;
  int64_t srow_size_;
};

}
}
#endif // OCEANBASE_BASIC_OB_STORE_ROW_CONVERTOR_H_