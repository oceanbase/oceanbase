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

#ifndef OCEANBASE_BASIC_OB_BLOCK_IWRITER_H_
#define OCEANBASE_BASIC_OB_BLOCK_IWRITER_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "src/share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/chunk_store/ob_chunk_block.h"
#include "sql/engine/basic/chunk_store/ob_chunk_block.h"

namespace oceanbase
{
namespace sql
{

class ObCompactStore;

class ObBlockIWriter
{
public:
  const static int64_t DEFAULT_BUF_SIZE = 64L * 1024L;
  ObBlockIWriter(ObTempBlockStore *store = nullptr) : store_(store), inited_(false), cur_blk_(nullptr), last_row_pos_(nullptr) {};
  ~ObBlockIWriter() { reset(); };

  void reset()
  {
    inited_ = false;
    store_ = nullptr;
    cur_blk_ = nullptr;
    last_row_pos_ = nullptr;
  };

  virtual int add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, ObChunkDatumStore::StoredRow **stored_row = nullptr) = 0;
  virtual int add_row(const ObChunkDatumStore::StoredRow &src_sr, ObChunkDatumStore::StoredRow **dst_sr = nullptr) = 0;
  virtual int add_row(const blocksstable::ObStorageDatum *storage_datums, const ObStorageColumnGroupSchema &cg_schema,
              const int64_t extra_size, ObChunkDatumStore::StoredRow **stored_row) = 0;

  virtual int add_batch(const common::ObDatum **datums, const common::ObIArray<ObExpr *> &exprs,
                        const uint16_t selector[], const int64_t size,
                        ObChunkDatumStore::StoredRow **stored_rows, BatchCtx *batch_ctx) = 0;
  virtual int close() = 0;

  virtual void set_meta(const ChunkRowMeta* row_meta) = 0;
  virtual int prepare_blk_for_write(ObTempBlockStore::Block *blk) = 0;
  virtual int get_last_stored_row(const ObChunkDatumStore::StoredRow *&sr) = 0;

protected:
  inline int64_t get_size()
  {
    int64_t size = 0;
    if (OB_NOT_NULL(cur_blk_)) {
      size = store_->get_blk_buf().head_size();
    }
    return size;
  }
  inline int64_t get_remain()
  {
    int64_t remain_size = 0;
    if (OB_NOT_NULL(cur_blk_)) {
      remain_size = store_->get_blk_buf().remain();
    }
    return remain_size;
  }

  inline char *get_cur_buf()
  {
    char *res = nullptr;
    if (OB_NOT_NULL(cur_blk_)) {
      res = store_->get_blk_buf().head();
    }
    return res;
  }
  inline int advance(int64_t size)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(cur_blk_)) {
      last_row_pos_ = store_->get_blk_buf().head();
      cur_blk_->cnt_ += 1;
      store_->inc_block_id_cnt(1);
      ret = store_->get_blk_buf().advance(size);
    }
    return ret;
  }

  inline bool is_overflow(int64_t size)
  {
    return size > get_remain() - static_cast<int64_t>(sizeof(ObTempBlockStore::Block));
  }

  inline const char *get_last_row() { return last_row_pos_; }

protected:
  ObTempBlockStore *store_;
  bool inited_;
  ObTempBlockStore::Block* cur_blk_;
  const char *last_row_pos_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_BLOCK_IWRITER_H_
