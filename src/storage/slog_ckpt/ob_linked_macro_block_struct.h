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

#ifndef OB_STORAGE_CKPT_LINKED_MARCO_BLOCK_STRUCT_H_
#define OB_STORAGE_CKPT_LINKED_MARCO_BLOCK_STRUCT_H_

#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_macro_block_handle.h"

namespace oceanbase
{
namespace storage
{

struct ObLinkedMacroBlockHeader final
{
  static const int32_t LINKED_MACRO_BLOCK_HEADER_VERSION = 1;
  static const int32_t LINKED_MACRO_BLOCK_HEADER_MAGIC = 10000;

  ObLinkedMacroBlockHeader()
  {
    reset();
  }
  ~ObLinkedMacroBlockHeader() = default;
  bool is_valid() const;
  const blocksstable::MacroBlockId get_previous_block_id() const
  {
    return previous_macro_block_id_;
  }
  void set_previous_block_id(const blocksstable::MacroBlockId &block_id)
  {
    previous_macro_block_id_ = block_id;
  }

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  static int64_t get_serialize_size()
  {
    return sizeof(ObLinkedMacroBlockHeader);
  }

  void reset()
  {
    version_ = LINKED_MACRO_BLOCK_HEADER_VERSION;
    magic_ = LINKED_MACRO_BLOCK_HEADER_MAGIC;
    item_count_ = 0;
    fragment_offset_ = 0;
    previous_macro_block_id_.reset();
  }

  TO_STRING_KV(
    K_(version), K_(magic), K_(item_count), K_(fragment_offset), K_(previous_macro_block_id));

  int32_t version_;
  int32_t magic_;
  int32_t item_count_;
  int32_t fragment_offset_;
  blocksstable::MacroBlockId previous_macro_block_id_;
};

struct ObLinkedMacroBlockItemHeader final
{
  static const int32_t LINKED_MACRO_BLOCK_ITEM_HEADER_VERSION = 1;
  static const int32_t LINKED_MACRO_BLOCK_ITEM_MAGIC = 10001;

  ObLinkedMacroBlockItemHeader()
    : version_(LINKED_MACRO_BLOCK_ITEM_HEADER_VERSION), magic_(LINKED_MACRO_BLOCK_ITEM_MAGIC),
      payload_size_(0), payload_crc_(0)
  {
  }
  ~ObLinkedMacroBlockItemHeader() = default;

  bool is_valid() const
  {
    return LINKED_MACRO_BLOCK_ITEM_HEADER_VERSION == version_ &&
      LINKED_MACRO_BLOCK_ITEM_MAGIC == magic_;
  }

  TO_STRING_KV(K_(version), K_(magic), K_(payload_size), K_(payload_crc));

  int32_t version_;
  int32_t magic_;
  int32_t payload_size_;
  int32_t payload_crc_;
};

class ObMetaBlockListHandle final
{
public:
  ObMetaBlockListHandle();
  ~ObMetaBlockListHandle();
  int add_macro_blocks(const common::ObIArray<blocksstable::MacroBlockId> &block_list);
  void reset();
  int reserve(const int64_t block_count);
  const common::ObIArray<blocksstable::MacroBlockId> &get_meta_block_list() const;
private:
  void switch_handle();
  void reset_new_handle();
private:
  static const int64_t META_BLOCK_HANDLE_CNT = 2;
  blocksstable::ObMacroBlocksHandle meta_handles_[META_BLOCK_HANDLE_CNT];
  int64_t cur_handle_pos_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_CKPT_LINKED_MARCO_BLOCK_STRUCT_H_
