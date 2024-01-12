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
#include "storage/blocksstable/ob_block_manager.h"
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

  int init();
  int write_block(const char *buf, const int64_t buf_len,
    blocksstable::ObMacroBlockCommonHeader &common_header,
    ObLinkedMacroBlockHeader &linked_header, blocksstable::MacroBlockId &pre_block_id);
  int close(blocksstable::MacroBlockId &pre_block_id);
  const blocksstable::MacroBlockId &get_entry_block() const;
  ObIArray<blocksstable::MacroBlockId> &get_meta_block_list();
  void reset();
  void reuse_for_next_round();

private:
  bool is_inited_;
  common::ObIOFlag io_desc_;
  blocksstable::ObMacroBlocksWriteCtx write_ctx_;
  blocksstable::ObMacroBlockHandle handle_;
  blocksstable::MacroBlockId entry_block_id_;
};

class ObLinkedMacroBlockItemWriter final
{
public:
  ObLinkedMacroBlockItemWriter();
  ~ObLinkedMacroBlockItemWriter() = default;
  ObLinkedMacroBlockItemWriter(const ObLinkedMacroBlockItemWriter &) = delete;
  ObLinkedMacroBlockItemWriter &operator=(const ObLinkedMacroBlockItemWriter &) = delete;

  int init(const bool need_disk_addr, const ObMemAttr &mem_attr);
  int write_item(const char *item_buf, const int64_t item_buf_len, int64_t *item_idx = nullptr);
  int close();
  void reset();
  void reuse_for_next_round();

  int get_entry_block(blocksstable::MacroBlockId &entry_block) const;
  common::ObIArray<blocksstable::MacroBlockId> &get_meta_block_list();
  int64_t get_item_disk_addr(const int64_t item_idx, ObMetaDiskAddr &addr) const;

private:
  void inner_reset();
  int write_block();
  int write_item_header(const char *item_buf, const int64_t item_buf_len);
  int write_item_content(const char *item_buf, const int64_t item_buf_len, int64_t &item_pos);
  int record_inflight_item(const int64_t item_buf_len, int64_t *item_idx);
  int set_pre_block_inflight_items_addr(const blocksstable::MacroBlockId &pre_block_id);

private:
  static const int64_t BLOCK_HEADER_SIZE =
    sizeof(blocksstable::ObMacroBlockCommonHeader) + sizeof(ObLinkedMacroBlockHeader);
  bool is_inited_;
  bool is_closed_;
  int64_t written_items_cnt_;

  bool need_disk_addr_;
  int64_t first_inflight_item_idx_;        // first item idx which still in the iobuf
  int64_t pre_block_inflight_items_cnt_;   // item count of pre block
  int64_t curr_block_inflight_items_cnt_;  // item count of current block

  common::ObArenaAllocator allocator_;
  ObLinkedMacroBlockWriter block_writer_;

  // the arr index represent item_idx
  common::ObArray<int64_t> item_size_arr_;
  common::ObArray<ObMetaDiskAddr> item_disk_addr_arr_;

  // buf for write io
  char *io_buf_;
  int64_t io_buf_size_;
  int64_t io_buf_pos_;

  // macro block header
  blocksstable::ObMacroBlockCommonHeader *common_header_;
  ObLinkedMacroBlockHeader *linked_header_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_META_BLOCK_WRITER_H_
