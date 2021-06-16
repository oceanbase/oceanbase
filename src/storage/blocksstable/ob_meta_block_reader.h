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

#ifndef OB_META_BLOCK_READER_H_
#define OB_META_BLOCK_READER_H_
#include "lib/io/ob_io_manager.h"
#include "ob_block_sstable_struct.h"
#include "ob_macro_block_common_header.h"

namespace oceanbase {
namespace blocksstable {

// used for STORE_FILE_SYSTEM_LOCAL or STORE_FILE_SYSTEM_RAID
class ObMetaBlockReader {
public:
  ObMetaBlockReader();
  virtual ~ObMetaBlockReader();
  int read(const ObSuperBlockV2::MetaEntry& meta_entry);

  // only used for read second index of ob 1.x
  int read_old_macro_block(const int64_t entry_block);
  virtual void reset();
  OB_INLINE const common::ObIArray<MacroBlockId>& get_macro_blocks() const
  {
    return macro_blocks_;
  }

protected:
  virtual int parse(const ObMacroBlockCommonHeader& common_header, const ObLinkedMacroBlockHeader& linked_header,
      const char* buf, const int64_t buf_len) = 0;

private:
  int get_meta_blocks(const int64_t entry_block);
  int init(const int64_t file_id, const int64_t file_size);

private:
  common::ObArenaAllocator allocator_;
  common::ObIODesc io_desc_;
  const char* buf_;
  const ObLinkedMacroBlockHeader* linked_header_;
  common::ObArray<MacroBlockId> macro_blocks_;
  ObStoreFileCtx file_ctx_;
  int64_t sstable_macro_block_count_;  // for 1.4 or 2.0, it is 0
  bool is_inited_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_META_BLOCK_READER_H_ */
