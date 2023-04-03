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

#ifndef OB_STORAGE_CKPT_LINKED_MARCO_BLOCK_READER_H_
#define OB_STORAGE_CKPT_LINKED_MARCO_BLOCK_READER_H_

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_handle.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"

namespace oceanbase
{
namespace storage
{

// This class is used to read macro block without item awareness
class ObLinkedMacroBlockReader final
{
public:
  ObLinkedMacroBlockReader();
  ~ObLinkedMacroBlockReader() = default;
  ObLinkedMacroBlockReader(const ObLinkedMacroBlockReader &) = delete;
  ObLinkedMacroBlockReader &operator=(const ObLinkedMacroBlockReader &) = delete;

  int init(const blocksstable::MacroBlockId &entry_block);
  int iter_read_block(char *&buf, int64_t &buf_len, blocksstable::MacroBlockId &block_id);
  void reset();
  ObIArray<blocksstable::MacroBlockId> &get_meta_block_list();

  static int pread_block(const ObMetaDiskAddr &addr, blocksstable::ObMacroBlockHandle &handler);
  static int read_block_by_id(
    const blocksstable::MacroBlockId &block_id, blocksstable::ObMacroBlockHandle &handler);

private:
  int get_meta_blocks(const blocksstable::MacroBlockId &entry_block);
  int prefetch_block();
  int get_previous_block_id(
    const char *buf, const int64_t buf_len, blocksstable::MacroBlockId &previous_block_id);

  static int check_data_checksum(const char *buf, const int64_t buf_len);

private:
  bool is_inited_;
  blocksstable::ObMacroBlockHandle handles_[2];
  int64_t handle_pos_;
  blocksstable::ObMacroBlocksHandle macros_handle_;
  int64_t prefetch_macro_block_idx_;
  int64_t read_macro_block_cnt_;
};

class ObLinkedMacroBlockItemReader final
{
public:
  ObLinkedMacroBlockItemReader();
  ~ObLinkedMacroBlockItemReader() = default;
  ObLinkedMacroBlockItemReader(const ObLinkedMacroBlockItemReader &) = delete;
  ObLinkedMacroBlockItemReader &operator=(const ObLinkedMacroBlockItemReader &) = delete;

  int init(const blocksstable::MacroBlockId &entry_block);
  int get_next_item(char *&item_buf, int64_t &item_buf_len, ObMetaDiskAddr &addr);
  void reset();
  common::ObIArray<blocksstable::MacroBlockId> &get_meta_block_list();

  static int get_next_block_id(const common::ObIArray<blocksstable::MacroBlockId> &block_list,
    const blocksstable::MacroBlockId &block_id, blocksstable::MacroBlockId &next_block_id);
  static int read_item(const common::ObIArray<blocksstable::MacroBlockId> &block_list,
    const ObMetaDiskAddr &addr, char *item_buf, int64_t &item_buf_len);

private:
  int read_item_block();
  int parse_item(char *&item_buf, int64_t &item_buf_len, ObMetaDiskAddr &addr);

  static int check_item_crc(const int32_t crc, const char *item_buf, const int64_t item_buf_len);
  static int read_large_item(const common::ObIArray<blocksstable::MacroBlockId> &block_list,
    const ObMetaDiskAddr &addr, char *item_buf, int64_t &item_buf_len);

private:
  bool is_inited_;
  const blocksstable::ObMacroBlockCommonHeader *common_header_;
  const ObLinkedMacroBlockHeader *linked_header_;
  ObLinkedMacroBlockReader block_reader_;

  // record the macro block that the buf belongs to,
  // if it is a big buf for a large item, only record the first block
  blocksstable::MacroBlockId buf_block_id_;
  char *buf_;
  int64_t buf_pos_;
  int64_t buf_len_;
  common::ObArenaAllocator allocator_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_CKPT_LINKED_MARCO_BLOCK_READER_H_
