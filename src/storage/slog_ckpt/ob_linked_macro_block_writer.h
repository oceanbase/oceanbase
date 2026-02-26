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

#ifndef OB_STORAGE_CKPT_LINKED_MARCO_BLOCK_WRITER_H_
#define OB_STORAGE_CKPT_LINKED_MARCO_BLOCK_WRITER_H_

#include "share/io/ob_io_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"

namespace oceanbase
{
namespace storage
{

// This class is used to write macro block without item awareness
class ObLinkedMacroBlockWriter final
{
public:
  ObLinkedMacroBlockWriter();
  ~ObLinkedMacroBlockWriter() = default;
  ObLinkedMacroBlockWriter(const ObLinkedMacroBlockWriter &) = delete;
  ObLinkedMacroBlockWriter &operator=(const ObLinkedMacroBlockWriter &) = delete;

  int init_for_slog_ckpt(
      const uint64_t tenant_id,
      const int64_t tenant_epoch_id,
      ObSlogCheckpointFdDispenser *fd_dispenser);
  int init_for_macro_info(
      const ObLinkedMacroInfoWriteParam &param);
  int write_block(
      char *buf, const int64_t buf_len,
      blocksstable::ObMacroBlockCommonHeader &common_header,
      ObLinkedMacroBlockHeader &linked_header, blocksstable::MacroBlockId &pre_block_id,
      blocksstable::ObIMacroBlockFlushCallback *redo_callback_ = nullptr);
  int close(blocksstable::ObIMacroBlockFlushCallback *redo_callback, blocksstable::MacroBlockId &pre_block_id);
  const blocksstable::MacroBlockId &get_entry_block() const;
  ObIArray<blocksstable::MacroBlockId> &get_meta_block_list();
  int64_t get_meta_block_cnt() const;

  int64_t get_last_macro_seq() const { return macro_info_param_.start_macro_seq_; }
  void reset();
  void reuse_for_next_round();

private:
  typedef ObLinkedMacroBlockWriteType WriteType;
  bool is_inited_;
  WriteType type_;
  blocksstable::ObMacroBlocksWriteCtx write_ctx_;
  blocksstable::ObStorageObjectHandle handle_;
  blocksstable::MacroBlockId entry_block_id_;
  uint64_t tenant_id_;
  int64_t tenant_epoch_id_;
  ObSlogCheckpointFdDispenser *fd_dispenser_;
  ObLinkedMacroInfoWriteParam macro_info_param_;
};

class ObLinkedMacroBlockItemWriter final
{
public:
  ObLinkedMacroBlockItemWriter();
  ~ObLinkedMacroBlockItemWriter() = default;
  ObLinkedMacroBlockItemWriter(const ObLinkedMacroBlockItemWriter &) = delete;
  ObLinkedMacroBlockItemWriter &operator=(const ObLinkedMacroBlockItemWriter &) = delete;
  // used for writing macro_info both in shared_nothing and shared_storage
  int init_for_macro_info(const ObLinkedMacroInfoWriteParam &param);
  // only used for slog checkpoint in shared nothing and shared storage
  int init_for_slog_ckpt(
      const uint64_t tenant_id,
      const int64_t tenant_epoch_id,
      const ObMemAttr &mem_attr,
      ObSlogCheckpointFdDispenser *fd_dispenser);
  int write_item(const char *item_buf, const int64_t item_buf_len, int64_t *item_idx = nullptr);
  int close();
  inline bool is_closed() const { return is_closed_; };
  void reset();
  void reuse_for_next_round();

  int get_entry_block(blocksstable::MacroBlockId &entry_block) const;
  common::ObIArray<blocksstable::MacroBlockId> &get_meta_block_list();
  int64_t get_last_macro_seq() const { return block_writer_.get_last_macro_seq(); }
  int64_t get_written_macro_cnt() const { return block_writer_.get_meta_block_cnt(); }
private:
  void inner_reset();
  int write_block();
  int write_item_header(const char *item_buf, const int64_t item_buf_len);
  int write_item_content(const char *item_buf, const int64_t item_buf_len, int64_t &item_pos);

private:
  bool is_inited_;
  bool is_closed_;
  int64_t written_items_cnt_;

  common::ObArenaAllocator allocator_;
  ObLinkedMacroBlockWriter block_writer_;

  // buf for write io
  char *io_buf_;
  int64_t io_buf_size_;
  int64_t io_buf_pos_;

  // macro block header
  blocksstable::ObMacroBlockCommonHeader common_header_;
  ObLinkedMacroBlockHeader linked_header_;
  blocksstable::ObIMacroBlockFlushCallback *write_callback_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_META_BLOCK_WRITER_H_
