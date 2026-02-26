/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_DAG_MICRO_BLOCK_ITERATOR_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_DAG_MICRO_BLOCK_ITERATOR_H_
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "storage/ddl/ob_cg_block_tmp_file.h"
#include "storage/blocksstable/ob_simplified_sstable_macro_block_header.h"

namespace oceanbase
{
namespace blocksstable
{

class ObDagMicroBlockIterator final : public ObMicroBlockBareIterator
{
public:
  ObDagMicroBlockIterator();
  virtual ~ObDagMicroBlockIterator();
  void reset();
  int open_cg_block(ObCGBlock *cg_block);
  int update_cg_block_offset_and_micro_idx();

  TO_STRING_KV(KP_(macro_block_buf), K_(macro_block_buf_size), K_(simplified_macro_header),
       K_(begin_idx), K_(end_idx), K_(iter_idx), K_(read_pos), K_(need_deserialize),
       K_(is_inited), K_(cg_block));

private:
  int open(const char *macro_block_buf,
           const int64_t macro_block_buf_size,
           const bool need_deserialize);
  int get_index_block(ObMicroBlockData &micro_block, const bool force_deserialize);
  int fast_locate_micro_block(const int64_t cg_block_offset, const int64_t micro_block_idx);
private:
  ObCGBlock *cg_block_;
  ObSimplifiedSSTableMacroBlockHeader simplified_macro_header_;
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif