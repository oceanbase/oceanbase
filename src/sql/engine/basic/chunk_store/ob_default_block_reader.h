/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BASIC_OB_DEFAULT_BLOCK_READER_H_
#define OCEANBASE_BASIC_OB_DEFAULT_BLOCK_READER_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "src/share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/chunk_store/ob_block_ireader.h"

namespace oceanbase
{
namespace sql
{

class ObCompactStore;
class StoredRow;
class ObDefaultBlockReader final : public ObBlockIReader
{
public:
  ObDefaultBlockReader(ObTempBlockStore *store) : ObBlockIReader(store), cur_pos_in_blk_(0), cur_row_in_blk_(0) {};
  virtual ~ObDefaultBlockReader() { reset(); };
  void reuse()
  {
    cur_pos_in_blk_ = 0;
    cur_row_in_blk_ = 0;
    cur_blk_ = nullptr;
  }
  void reset()
  {
    cur_pos_in_blk_ = 0;
    cur_row_in_blk_ = 0;
    cur_blk_ = nullptr;
  }
  virtual int get_row(const ObChunkDatumStore::StoredRow *&sr) override;
  inline bool blk_has_next_row() { return cur_blk_ != NULL && cur_blk_->cnt_ > cur_row_in_blk_; }
  void set_meta(const ChunkRowMeta *row_meta) override {};
  int prepare_blk_for_read(ObTempBlockStore::Block *blk) final override;

private:
  int64_t cur_pos_in_blk_;
  int64_t cur_row_in_blk_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_DEFAULT_BLOCK_READER_H_
