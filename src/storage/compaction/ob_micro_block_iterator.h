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

#ifndef OCEANBASE_COMPACTION_OB_MICRO_BLOCK_ITERATOR_H_
#define OCEANBASE_COMPACTION_OB_MICRO_BLOCK_ITERATOR_H_

#include "share/ob_define.h"
#include "lib/io/ob_io_manager.h"
#include "common/ob_range.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_index_mgr.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"

namespace oceanbase {
namespace blocksstable {
class ObMacroBlockMeta;
class ObMicroBlockIndexTransformer;
}  // namespace blocksstable

namespace compaction {
// Load macro block buf from disk and offer interfaces of raw data.
class ObMacroBlockLoader {
public:
  ObMacroBlockLoader();
  virtual ~ObMacroBlockLoader();
  int init(const char* macro_block_buf, const int64_t macro_block_buf_size,
      const blocksstable::ObFullMacroBlockMeta* meta, const common::ObStoreRange* range = NULL);
  int next_micro_block(blocksstable::ObMicroBlock& micro_block);
  void reset();

  // for storage perf test
  OB_INLINE common::ObIArray<blocksstable::ObMicroBlockInfo>& get_micro_block_infos()
  {
    return micro_block_infos_;
  }
  OB_INLINE common::ObIArray<common::ObStoreRowkey>& get_end_keys()
  {
    return end_keys_;
  }
  OB_INLINE bool is_left_border()
  {
    return 0 == cur_micro_cursor_;
  }
  OB_INLINE bool is_right_border()
  {
    return micro_block_infos_.count() - 1 == cur_micro_cursor_;
  }
  OB_INLINE int64_t get_micro_index() const
  {
    return cur_micro_cursor_;
  }

private:
  int transform_block_header(
      const blocksstable::ObSSTableMacroBlockHeader*& block_header, const common::ObObjMeta*& column_types);

private:
  bool is_inited_;
  const char* macro_buf_;
  int64_t macro_buf_size_;
  common::ObStoreRange range_;
  common::ObArray<blocksstable::ObMicroBlockInfo> micro_block_infos_;
  common::ObArray<common::ObStoreRowkey> end_keys_;
  common::ObArenaAllocator allocator_;  // used to allocate memory for end_keys_
  int64_t cur_micro_cursor_;
};

class ObMicroBlockIterator {
public:
  ObMicroBlockIterator();
  virtual ~ObMicroBlockIterator();

  int init(const uint64_t table_id, const common::ObIArray<share::schema::ObColDesc>& columns,
      const common::ObStoreRange& range, const blocksstable::ObMacroBlockCtx& macro_block_ctx,
      blocksstable::ObStorageFile* pg_file);
  void reset();
  int next(const blocksstable::ObMicroBlock*& micro_block);

  // for storage perf test
  OB_INLINE common::ObIArray<blocksstable::ObMicroBlockInfo>& get_micro_block_infos()
  {
    return loader_.get_micro_block_infos();
  }
  OB_INLINE common::ObIArray<common::ObStoreRowkey>& get_end_keys()
  {
    return loader_.get_end_keys();
  }
  OB_INLINE bool is_left_border()
  {
    return loader_.is_left_border();
  }
  OB_INLINE bool is_right_border()
  {
    return loader_.is_right_border();
  }

private:
  bool inited_;
  uint64_t table_id_;
  const common::ObIArray<share::schema::ObColDesc>* columns_;
  common::ObStoreRange range_;
  blocksstable::ObMicroBlock micro_block_;
  ObMacroBlockLoader loader_;
  blocksstable::ObColumnMap column_map_;
  common::ObArenaAllocator column_map_allocator_;  // allocator for column map, will not reset util dtor
  blocksstable::ObFullMacroBlockMeta full_meta_;
  blocksstable::ObMacroBlockHandle macro_handle_;

  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockIterator);
};

}  // end namespace compaction
}  // end namespace oceanbase
#endif  // OCEANBASE_COMPACTION_OB_MICRO_BLOCK_ITERATOR_H_
