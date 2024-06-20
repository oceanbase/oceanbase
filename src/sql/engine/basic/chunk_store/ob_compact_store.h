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
#ifndef OCEANBASE_BASIC_OB_COMPACT_STORE_H_
#define OCEANBASE_BASIC_OB_COMPACT_STORE_H_
#include "share/ob_define.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "lib/alloc/alloc_struct.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/chunk_store/ob_chunk_block.h"
#include "sql/engine/basic/chunk_store/ob_block_ireader.h"
#include "sql/engine/basic/chunk_store/ob_block_iwriter.h"
#include "sql/engine/basic/chunk_store/ob_chunk_block.h"
#include "src/share/ob_ddl_common.h"

namespace oceanbase
{
namespace storage {
  class ObColumnSchemaItem;
}
namespace sql
{

class ObCompactStore final : public ObTempBlockStore
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObCompactStore(common::ObIAllocator *alloc = NULL) : ObTempBlockStore(alloc),
                        writer_(nullptr), reader_(nullptr), batch_ctx_(nullptr),
                        row_meta_(*allocator_), row_cnt_(0), block_reader_(), start_iter_(false),
                        cur_blk_id_(0)
  {
  };
  virtual ~ObCompactStore() {reset();};
  void reset();
  void rescan();
  int init(const int64_t mem_limit,
           const uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
           const int64_t mem_ctx_id = common::ObCtxIds::DEFAULT_CTX_ID,
           const char *label = common::ObModIds::OB_SQL_ROW_STORE,
           const bool enable_dump = true,
           const uint32_t row_extra_size = 0,
           const bool enable_trunc = true,
           const ObCompressorType compress_type = NONE_COMPRESSOR,
           const ExprFixedArray *exprs = nullptr);

  int init(const int64_t mem_limit,
           const ObIArray<storage::ObColumnSchemaItem> &col_array,
           const uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
           const int64_t mem_ctx_id = common::ObCtxIds::DEFAULT_CTX_ID,
           const char *label = common::ObModIds::OB_SQL_ROW_STORE,
           const bool enable_dump = true,
           const uint32_t row_extra_size = 0,
           const bool enable_trunc = true,
           const ObCompressorType compress_type = NONE_COMPRESSOR);
  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                const ObBitVector &skip, const int64_t batch_size,
                const uint16_t selector[], const int64_t size,
                ObChunkDatumStore::StoredRow **stored_rows = nullptr);
  // called in sort_op
  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                const ObBitVector &skip, const int64_t batch_size,
                int64_t &stored_rows_count,
                ObChunkDatumStore::StoredRow **stored_rows = nullptr,
                const int64_t start_pos = 0);
  int add_row(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx, ObChunkDatumStore::StoredRow **stored_row = nullptr);
  int add_row(const ObChunkDatumStore::StoredRow &src_sr, ObChunkDatumStore::StoredRow **dst_sr = nullptr);
  // for chunkslicestore.
  int add_row(const blocksstable::ObDatumRow &datum_row, const ObStorageColumnGroupSchema &cg_schema,
              const int64_t extra_size, ObChunkDatumStore::StoredRow **stored_row = nullptr);
  int get_next_row(const ObChunkDatumStore::StoredRow *&sr);

  int finish_write();
  // for ChunkSliceStore, get the last row to split range.
  int get_last_stored_row(const ObChunkDatumStore::StoredRow *&sr);
  ObBlockIReader* get_reader() { return reader_; }
  ObTempBlockStore::BlockReader* get_block_reader() { return &block_reader_; }
  ObBlockIWriter* get_writer() { return writer_; }
  int64_t get_row_cnt() const { return row_cnt_; }

  int has_next(bool &has_next);
  ChunkRowMeta *get_row_meta() { return &row_meta_; }
  void set_meta(ChunkRowMeta *row_meta) { writer_->set_meta(row_meta); reader_->set_meta(row_meta); }
  void set_blk_holder(ObTempBlockStore::BlockHolder *blk_holder) { block_reader_.set_blk_holder(blk_holder); }
protected:
  int prepare_blk_for_write(Block *) final override;
  int prepare_blk_for_read(Block *) final override;

private:
  int init_writer_reader();
  int init_batch_ctx(const int64_t col_cnt, const int64_t max_batch_size);

  int inner_add_batch(const common::ObDatum **datums, const common::ObIArray<ObExpr *> &exprs,
                      const uint16_t selector[], const int64_t size,
                      ObChunkDatumStore::StoredRow **stored_rows);

  int add_batch_fallback(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                         const ObBitVector &skip, const int64_t batch_size,
                         const uint16_t selector[], const int64_t size,
                         ObChunkDatumStore::StoredRow **stored_rows);
  int inner_get_next_row(const ObChunkDatumStore::StoredRow *&sr);

private:
  ObBlockIWriter *writer_;
  ObBlockIReader *reader_;
  BatchCtx *batch_ctx_;

  ChunkRowMeta row_meta_;
  int64_t row_cnt_;
  ObTempBlockStore::BlockReader block_reader_;
  bool start_iter_;
  int64_t cur_blk_id_;
}
;

} // end namespace sql
} // end namespace oceanbase
#endif // OCEANBASE_BASIC_OB_COMPACT_STORE_H_