/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_BITMAP_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_BITMAP_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"

namespace oceanbase
{
namespace sql
{

class ObFastBitmap
{
 public:
  static const uint64_t CHUNK_SHIFT = 16;
  static const uint64_t CHUNK_SIZE = 1 << CHUNK_SHIFT;
  static const uint64_t CHUNK_MASK = CHUNK_SIZE - 1;
  static const uint64_t WORD_SHIFT = 6;
  static const uint64_t WORD_BITS = 64;
  static const uint64_t WORDS_PER_CHUNK = CHUNK_SIZE / WORD_BITS;
  static const int64_t MAX_BITMAP_RESERVED_MEM_SIZE = 10 * 1024 * 1024; // 10MB

  class ChunkInfo
  {
   public:
    ChunkInfo() : min_val_(UINT16_MAX), max_val_(0), chunk_() {}
    ~ChunkInfo() {}

    TO_STRING_KV(K_(chunk), K_(min_val), K_(max_val));

    uint16_t min_val_;
    uint16_t max_val_;
    uint64_t chunk_[WORDS_PER_CHUNK];
  };

  class Iterator
  {
   public:
    Iterator()
      : bm_(nullptr),
        cur_val_(-1)
    { }

    int init(const ObFastBitmap *bm);
    void reuse();
    void reset();
    bool is_inited() const { return bm_ != nullptr; }
    int next_id(uint64_t &val);
    int advance_to(const uint64_t target, uint64_t &val);

   private:
    const ObFastBitmap *bm_;
    uint64_t cur_val_;
  };

  friend class Iterator;

  ObFastBitmap(common::ObIAllocator &allocator)
    : cardinality_(0),
      chunks_(),
      allocator_(allocator),
      info_allocator_(common::ObMemAttr(MTL_ID(), "DASFastBitmap")),
      chunk_buf_(nullptr),
      chunk_buf_count_(0),
      chunk_buf_idx_(0)
  { }

  ~ObFastBitmap();

  void reset();
  void reuse();
  int64_t cardinality() const { return cardinality_; }
  int init(int64_t max_value_count);
  int add(uint64_t val);

 private:
  int alloc_chunk(uint64_t chunk_idx, ChunkInfo *&chunk_info);

 private:
  int64_t cardinality_;
  ObSEArray<ChunkInfo*, 32> chunks_;
  common::ObIAllocator &allocator_;
  common::ObArenaAllocator info_allocator_;
  ChunkInfo *chunk_buf_;
  int64_t chunk_buf_count_;
  int64_t chunk_buf_idx_;
};

class ObDASBitmapOpParam : public ObIDASSearchOpParam
{
public:
  ObDASBitmapOpParam(ObIDASSearchOp *child)
  : ObIDASSearchOpParam(DAS_SEARCH_OP_BITMAP),
    child_(child)
  { }
  virtual ~ObDASBitmapOpParam() {}
  OB_INLINE ObIDASSearchOp *get_child() const { return child_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
private:
  ObIDASSearchOp *child_;
};

class ObDASBitmapOp : public ObIDASSearchOp
{
public:
  ObDASBitmapOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    exhausted_(true),
    bitmap_built_(false),
    bitmap_(nullptr),
    bitmap_iter_(),
    rowid_expr_(nullptr),
    scan_op_(nullptr)
  { }
  virtual ~ObDASBitmapOp() {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;

private:
  int build_bitmap();

private:
  bool exhausted_;
  bool bitmap_built_;
  ObFastBitmap *bitmap_;
  ObFastBitmap::Iterator bitmap_iter_;
  ObExpr *rowid_expr_;
  ObDASScalarScanOp *scan_op_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_BITMAP_OP_H_