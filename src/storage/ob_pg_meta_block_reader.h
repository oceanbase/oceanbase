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

#ifndef OB_PG_META_BLOCK_READER_H_
#define OB_PG_META_BLOCK_READER_H_

#include "blocksstable/ob_store_file.h"
#include "blocksstable/ob_store_file_system.h"
#include "blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
namespace storage {

class ObPGMetaBlockReader final {
public:
  ObPGMetaBlockReader();
  ~ObPGMetaBlockReader() = default;
  int init(const blocksstable::MacroBlockId& entry_block, blocksstable::ObStorageFileHandle& file_handle);
  int read_block(char*& buf, int64_t& buf_len);
  void reset();
  ObIArray<blocksstable::MacroBlockId>& get_meta_block_list();

private:
  int get_meta_blocks(const blocksstable::MacroBlockId& entry_block);
  int prefetch_block();
  int check_data_checksum(const char* buf, const int64_t buf_len);
  int get_previous_block_id(const char* buf, const int64_t buf_len, blocksstable::MacroBlockId& previous_block_id);

private:
  static const int64_t HANDLE_CNT = 2;
  bool is_inited_;
  common::ObIODesc io_desc_;
  blocksstable::ObMacroBlockHandle handles_[HANDLE_CNT];
  int64_t handle_pos_;
  blocksstable::ObMacroBlocksHandle macros_handle_;
  int64_t prefetch_macro_block_idx_;
  int64_t read_macro_block_idx_;
  blocksstable::ObStorageFileHandle file_handle_;
};

struct ObPGMetaItemBuffer final {
public:
  ObPGMetaItemBuffer() : buf_(nullptr), buf_len_(0), item_type_(0)
  {}
  ~ObPGMetaItemBuffer() = default;
  TO_STRING_KV(KP_(buf), K_(buf_len), K_(item_type));
  void reset()
  {
    buf_ = nullptr;
    buf_len_ = 0;
    item_type_ = 0;
  }

public:
  const char* buf_;
  int64_t buf_len_;
  int16_t item_type_;
};

class ObPGMetaItemReader final {
public:
  ObPGMetaItemReader();
  ~ObPGMetaItemReader() = default;
  int init(const blocksstable::MacroBlockId& entry_block, blocksstable::ObStorageFileHandle& file_handle);
  int get_next_item(ObPGMetaItemBuffer& item);
  void reset();
  common::ObIArray<blocksstable::MacroBlockId>& get_meta_block_list();

private:
  int read_item_block();
  int parse_item(ObPGMetaItemBuffer& item);

private:
  static const int64_t HANDLE_CNT = 2;
  bool is_inited_;
  const blocksstable::ObMacroBlockCommonHeader* common_header_;
  const blocksstable::ObLinkedMacroBlockHeaderV2* linked_header_;
  ObPGMetaBlockReader reader_;
  char* buf_;
  int64_t buf_pos_;
  int64_t buf_len_;
  common::ObArenaAllocator allocator_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_META_BLOCK_READER_H_
