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

#ifndef OCEANBASE_BASIC_OB_DEFAULT_BLOCK_WRITER_H_
#define OCEANBASE_BASIC_OB_DEFAULT_BLOCK_WRITER_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "src/share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/chunk_store/ob_chunk_block.h"
#include "sql/engine/basic/chunk_store/ob_block_iwriter.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_temp_block_store.h"

namespace oceanbase
{
namespace sql
{

class ObDefaultBlockWriter final : public ObBlockIWriter
{
public:
  ObDefaultBlockWriter(ObTempBlockStore *store) : ObBlockIWriter(store) {};
  ~ObDefaultBlockWriter() { reset(); };

  void reset() {}
  int add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, ObChunkDatumStore::StoredRow **stored_row = nullptr) override;
  int add_row(const ObChunkDatumStore::StoredRow &src_sr, ObChunkDatumStore::StoredRow **dst_sr = nullptr) override;
  int add_row(const blocksstable::ObStorageDatum *storage_datums, const ObStorageColumnGroupSchema &cg_schema,
              const int64_t extra_size, ObChunkDatumStore::StoredRow **stored_row) override;
  int close() override;
  int add_batch(const common::ObDatum **datums, const common::ObIArray<ObExpr *> &exprs,
                const uint16_t selector[], const int64_t size,
                ObChunkDatumStore::StoredRow **stored_rows, BatchCtx *batch_ctx) override;
  void set_meta(const ChunkRowMeta *row_meta) override {};
  int prepare_blk_for_write(ObTempBlockStore::Block *blk) final override;

  inline int get_last_stored_row(const ObChunkDatumStore::StoredRow *&sr)
  {
    int ret = OB_SUCCESS;
    sr = reinterpret_cast<const ObChunkDatumStore::StoredRow*>(get_last_row());
    return ret;
  }
private:
  /*
   * before add_row we should call ensure_write
   * 1. if the cur_blk could hold the next row:
   * 2. if the write buffer couldn't hold next row:
   *    2.1 if the current row's size <= DEFAULT_BUF_SIZE (64KB), then alloc a block which size if 64KB.
   *    2.2 if current row's size > 64KB, then the size of alloced block is current row's size.
   */
  int ensure_write(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx);
  int ensure_write(const ObChunkDatumStore::StoredRow &stored_row);
  int ensure_write(const blocksstable::ObStorageDatum *storage_datums,
                   const ObStorageColumnGroupSchema &cg_schema,
                   const int64_t extra_size);

  // before dump the block we need to unswizzling each row;
  int block_unswizzling(ObTempBlockStore::Block *blk);

  int ensure_write(const int64_t size);
  // get the stored size in writer buffer for a row.
  int get_row_stored_size(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, uint64_t &size);
  int get_row_stored_size(const blocksstable::ObStorageDatum *storage_datums,
                          const ObStorageColumnGroupSchema &cg_schema,
                          const int64_t extra_size, uint64_t &size);
  inline int ensure_init()
  {
    int ret = OB_SUCCESS;
    if (!inited_) {
      inited_ = true;
    }
    return ret;
  }
  int inner_add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, ObChunkDatumStore::StoredRow **stored_row = nullptr);
  int inner_add_row(const blocksstable::ObStorageDatum *storage_datums, const ObStorageColumnGroupSchema &cg_schema,
                    const int64_t extra_size, ObChunkDatumStore::StoredRow **dst_sr);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_DEFAULT_BLOCK_WRITER_H_
