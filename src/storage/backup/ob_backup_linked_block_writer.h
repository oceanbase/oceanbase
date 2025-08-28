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

#ifndef _OCEANBASE_BACKUP_OB_BACKUP_LINKED_BLOCK_WRITER_H_
#define _OCEANBASE_BACKUP_OB_BACKUP_LINKED_BLOCK_WRITER_H_

#include "common/ob_tablet_id.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "share/backup/ob_backup_struct.h"
#include "common/storage/ob_io_device.h"
#include "storage/backup/ob_backup_file_writer_ctx.h"
#include "lib/allocator/page_arena.h"
#include "ob_backup_linked_item.h"

namespace oceanbase
{
namespace backup
{

class ObBackupLinkedBlockWriter final
{
public:
  ObBackupLinkedBlockWriter();
  ~ObBackupLinkedBlockWriter();
  
  int init(const ObBackupLinkedBlockAddr &entry_block_addr,
      ObBackupFileWriteCtx &file_write_ctx, int64_t &file_offset);
  int write_block(const blocksstable::ObBufferReader &buffer_reader,
      ObBackupLinkedBlockAddr &block_addr);
  int close();

private:
  int flush_block_to_dest_(const blocksstable::ObBufferReader &buffer_reader);
  int calc_next_block_addr_();
  
private:
  bool is_inited_;
  int64_t *file_offset_;
  ObBackupFileWriteCtx *write_ctx_;
  ObBackupLinkedBlockAddr current_block_addr_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLinkedBlockWriter);
};

class ObBackupLinkedBlockItemWriter final
{
public:
  ObBackupLinkedBlockItemWriter();
  ~ObBackupLinkedBlockItemWriter();

  int init(const ObLSBackupDataParam &param, const common::ObTabletID &tablet_id,
      const storage::ObITable::TableKey &table_key, const int64_t file_id,
      ObBackupFileWriteCtx &file_write_ctx, int64_t &file_offset);
  int write(const ObBackupLinkedItem &link_item);
  int close();
  int get_root_block_id(ObBackupLinkedBlockAddr &block_addr, int64_t &total_block_count);

public:
  const common::ObArray<ObBackupLinkedBlockAddr> &get_written_block_list() const { return written_block_list_; }

private:
  int get_entry_block_addr_(const ObLSBackupDataParam &param, const int64_t file_id,
      const int64_t file_offset, ObBackupLinkedBlockAddr &block_id);
  int construct_common_header_(char *header_buf, const int64_t data_length,
      const int64_t align_length, share::ObBackupCommonHeader *&common_header);
  int construct_linked_header_(char *header_buf);
  int check_remain_size_enough_(const ObBackupLinkedItem &item, bool &is_enough);
  int write_single_item_(const ObBackupLinkedItem &item);
  int close_and_write_block_();
  int close_block_();
  int write_block_();

private:
  static const int64_t BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE;

private:
  bool is_inited_;
  bool is_closed_;
  common::ObTabletID tablet_id_;
  storage::ObITable::TableKey table_key_;
  int64_t buf_size_;
  ObBackupLinkedBlockAddr prev_block_addr_;
  ObBackupLinkedBlockWriter block_writer_;
  share::ObBackupCommonHeader common_header_;
  ObBackupLinkedBlockHeader linked_header_;
  blocksstable::ObSelfBufferWriter buffer_writer_;
  int64_t cur_block_write_item_cnt_;
  int64_t total_block_write_item_cnt_;
  int64_t total_block_cnt_;
  ObBackupFileWriteCtx *write_ctx_;
  bool has_prev_;
  common::ObArray<ObBackupLinkedBlockAddr> written_block_list_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLinkedBlockItemWriter);
};

}
}

#endif
