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

#ifndef OCEANBASE_BASIC_OB_BLOCK_IREADER_H_
#define OCEANBASE_BASIC_OB_BLOCK_IREADER_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "src/share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/chunk_store/ob_chunk_block.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "sql/engine/basic/chunk_store/ob_chunk_block.h"

namespace oceanbase
{
namespace sql
{

class ObBlockIReader
{
public:
  explicit ObBlockIReader(ObTempBlockStore *store) : store_(store), cur_blk_(nullptr) {};
  virtual ~ObBlockIReader() { reset(); };

  void reset()
  {
    cur_blk_ = nullptr;
    store_ = nullptr;
  }
  virtual void reuse() = 0;
  virtual int get_row(const ObChunkDatumStore::StoredRow *&sr) = 0;
  virtual void set_meta(const ChunkRowMeta* row_meta) = 0;
  void set_block(const ObTempBlockStore::Block *blk) { cur_blk_ = blk; }
  const ObTempBlockStore::Block *get_block() {return cur_blk_; }
  virtual int prepare_blk_for_read(ObTempBlockStore::Block *blk)  = 0;

protected:
  ObTempBlockStore *store_;
  const ObTempBlockStore::Block* cur_blk_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_BLOCK_IREADER_H_
