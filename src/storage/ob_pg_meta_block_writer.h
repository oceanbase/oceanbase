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

#ifndef OB_PG_META_BLOCK_WRITER_H_
#define OB_PG_META_BLOCK_WRITER_H_

#include "lib/io/ob_io_manager.h"
#include "blocksstable/ob_macro_block_common_header.h"
#include "blocksstable/ob_store_file.h"
#include "blocksstable/ob_store_file_system.h"

namespace oceanbase {
namespace storage {
enum ObPGMetaItemType {
  PG_MACRO_META = 0,
  PG_META = 1,
  PG_SUPER_BLOCK = 2,
  TENANT_CONFIG_META = 3,
  TENANT_FILE_INFO = 4,
};

class ObIPGMetaItem {
public:
  ObIPGMetaItem() = default;
  virtual ~ObIPGMetaItem() = default;
  virtual int serialize(const char*& buf, int64_t& pos) = 0;
  virtual int16_t get_item_type() const = 0;
};

class ObIPGWriteMetaItemCallback {
public:
  ObIPGWriteMetaItemCallback();
  virtual ~ObIPGWriteMetaItemCallback();
  virtual int process() = 0;
};

class ObIPGMetaItemIterator {
public:
  ObIPGMetaItemIterator() = default;
  virtual ~ObIPGMetaItemIterator() = default;
  virtual int get_next_item(ObIPGMetaItem*& meta_item) = 0;
};

class ObPGMetaBlockWriter final {
public:
  ObPGMetaBlockWriter();
  ~ObPGMetaBlockWriter() = default;
  int init(blocksstable::ObStorageFileHandle& file_handle);
  int write_block(const char* buf, const int64_t buf_len, blocksstable::ObMacroBlockCommonHeader& common_header,
      blocksstable::ObLinkedMacroBlockHeaderV2& linked_header);
  int close();
  const blocksstable::MacroBlockId& get_entry_block() const;
  ObIArray<blocksstable::MacroBlockId>& get_meta_block_list();
  void reset();

private:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  common::ObIODesc io_desc_;
  blocksstable::ObMacroBlocksWriteCtx write_ctx_;
  blocksstable::ObMacroBlockHandle handle_;
  blocksstable::ObStorageFileHandle file_handle_;
  blocksstable::MacroBlockId macro_block_id_;
  blocksstable::ObMacroBlockMetaV2 meta_;
  blocksstable::ObMacroBlockSchemaInfo schema_;
};

struct ObPGMetaItemHeader {
public:
  ObPGMetaItemHeader() : type_(0), reserved_(0), size_(0)
  {}
  ~ObPGMetaItemHeader() = default;
  int16_t type_;
  int16_t reserved_;
  int32_t size_;
};

class ObPGMetaItemWriter final {
public:
  ObPGMetaItemWriter();
  ~ObPGMetaItemWriter() = default;
  int init(blocksstable::ObStorageFileHandle& file_handle);
  int write_item(ObIPGMetaItem* item, ObIPGWriteMetaItemCallback* callback = nullptr);
  int get_entry_block_index(blocksstable::MacroBlockId& entry_block) const;
  common::ObIArray<blocksstable::MacroBlockId>& get_meta_block_list();
  int close();
  void reset();

private:
  int write_block();
  int write_item_header(const ObIPGMetaItem* item, const int64_t item_len);
  int write_item_content(const char* item_buf, const int64_t item_buf_len, int64_t& item_pos);

private:
  static const int64_t HEADER_SIZE =
      sizeof(blocksstable::ObMacroBlockCommonHeader) + sizeof(blocksstable::ObLinkedMacroBlockHeaderV2);
  bool is_inited_;
  ObIPGMetaItemIterator* iter_;
  common::ObArenaAllocator allocator_;
  ObPGMetaBlockWriter writer_;

  // buf for write io
  char* io_buf_;
  int64_t io_buf_size_;
  int64_t io_buf_pos_;

  // macro block header
  blocksstable::ObMacroBlockCommonHeader* common_header_;
  blocksstable::ObLinkedMacroBlockHeaderV2* linked_header_;
  DISALLOW_COPY_AND_ASSIGN(ObPGMetaItemWriter);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_META_BLOCK_WRITER_H_
