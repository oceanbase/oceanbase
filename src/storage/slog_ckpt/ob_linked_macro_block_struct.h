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
namespace blocksstable
{
  class ObSSTableMacroInfo;
  class ObIMacroBlockFlushCallback;
}
namespace storage
{

struct ObTabletPersisterParam;

enum ObLinkedMacroBlockWriteType : int8_t
{
  PRIV_SLOG_CKPT          = 0,
  PRIV_MACRO_INFO         = 1,
  SHARED_MAJOR_MACRO_INFO = 2,
  SHARED_INC_MACRO_INFO   = 3,
  LMI_MAX_TYPE            = 4,
};

struct ObLinkedMacroBlockHeader final
{
  static const int32_t LINKED_MACRO_BLOCK_HEADER_VERSION_V1 = 1;
  static const int32_t LINKED_MACRO_BLOCK_HEADER_VERSION_V2 = 2;
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
  int64_t get_serialize_size()
  {
    return sizeof(version_) + sizeof(magic_) + sizeof(item_count_) + sizeof(fragment_offset_)
        + previous_macro_block_id_.get_serialize_size();
  }

  void reset()
  {
    version_ = LINKED_MACRO_BLOCK_HEADER_VERSION_V2;
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
  blocksstable::ObStorageObjectsHandle meta_handles_[META_BLOCK_HANDLE_CNT];
  int64_t cur_handle_pos_;
};

class ObLinkedMacroInfoWriteParam final
{
public:
  ObLinkedMacroInfoWriteParam()
    : type_(ObLinkedMacroBlockWriteType::LMI_MAX_TYPE),
      ls_id_(),
      tablet_id_(),
      tablet_private_transfer_epoch_(-1),
      start_macro_seq_(-1),
      reorganization_scn_(-1),
      op_id_(0),
      write_callback_(nullptr)
  {}
  ~ObLinkedMacroInfoWriteParam() = default;
  bool is_valid() const;
  int build_linked_marco_info_param(
      const ObTabletPersisterParam &persist_param,
      const int64_t cur_macro_seq);
  void reset();
  blocksstable::ObIMacroBlockFlushCallback *write_callback() const { return write_callback_; }
  TO_STRING_KV(K_(type),
               K_(ls_id),
               K_(tablet_id),
               K_(tablet_private_transfer_epoch),
               K_(start_macro_seq),
               K_(reorganization_scn),
               K_(op_id),
               KP_(write_callback));
private:
  friend class ObLinkedMacroBlockWriter;
private:
  ObLinkedMacroBlockWriteType type_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  int32_t tablet_private_transfer_epoch_;
  int64_t start_macro_seq_;
  int64_t reorganization_scn_;
  uint64_t op_id_;
  blocksstable::ObIMacroBlockFlushCallback *write_callback_;
};

class ObSlogCheckpointFdDispenser final
{
public:
  ObSlogCheckpointFdDispenser() : min_file_id_(0), max_file_id_(0) {}
  explicit ObSlogCheckpointFdDispenser(const int64_t cur_file_id)
  {
    min_file_id_ = max_file_id_ = cur_file_id + 1;
  }
  ~ObSlogCheckpointFdDispenser() = default;
  void set_cur_max_file_id(const int64_t cur_file_id) {  min_file_id_ = max_file_id_ = cur_file_id + 1; }
  int64_t get_min_file_id() const { return min_file_id_; }
  int64_t get_max_file_id() const { return max_file_id_; }
  int64_t acquire_new_file_id() { return ATOMIC_FAA(&max_file_id_, 1); }

  TO_STRING_KV(K_(min_file_id), K_(max_file_id));
private :
  int64_t min_file_id_;
  int64_t max_file_id_;

  DISALLOW_COPY_AND_ASSIGN(ObSlogCheckpointFdDispenser);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_CKPT_LINKED_MARCO_BLOCK_STRUCT_H_
