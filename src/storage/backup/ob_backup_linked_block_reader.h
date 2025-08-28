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

#ifndef _OCEANBASE_BACKUP_OB_BACKUP_LINKED_BLOCK_READER_H_
#define _OCEANBASE_BACKUP_OB_BACKUP_LINKED_BLOCK_READER_H_

#include "ob_backup_linked_item.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#include "common/ob_tablet_id.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace backup
{

class ObBackupLinkedBlockReader final
{
public:
  ObBackupLinkedBlockReader();
  ~ObBackupLinkedBlockReader();
  
  int init(const share::ObBackupDest &backup_set_dest,
      const ObBackupLinkedBlockAddr &entry_block_id, const int64_t total_block_count,
      const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key,
      const ObMemAttr &mem_attr, const ObStorageIdMod &mod);
  int get_next_block(blocksstable::ObBufferReader &buffer_reader,
      ObBackupLinkedBlockAddr &block_id, bool &has_prev, int64_t &item_count);

private:
  int pread_block_(const ObBackupLinkedBlockAddr &block_addr, blocksstable::ObBufferReader &buffer_reader);
  int get_backup_file_path_(const ObBackupLinkedBlockAddr &block_addr, share::ObBackupPath &backup_path);
  int parse_block_header_(blocksstable::ObBufferReader &buffer_reader);
  int decode_common_header_(blocksstable::ObBufferReader &buffer_reader);
  int decode_linked_header_(blocksstable::ObBufferReader &buffer_reader);

private:
  static const int64_t BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE;

private:
  bool is_inited_;
  char *buf_;
  int64_t buf_len_;
  int64_t total_block_cnt_;
  int64_t read_block_cnt_;
  ObArenaAllocator allocator_;
  ObBackupLinkedBlockAddr next_block_addr_;
  share::ObBackupCommonHeader common_header_;
  ObBackupLinkedBlockHeader linked_header_;
  share::ObBackupDest backup_set_dest_;
  ObStorageIdMod mod_;
  common::ObTabletID tablet_id_;
  storage::ObITable::TableKey table_key_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLinkedBlockReader);
};

class ObBackupLinkedBlockItemReader final
{
public:
  ObBackupLinkedBlockItemReader();
  ~ObBackupLinkedBlockItemReader();

  int init(const share::ObBackupDest &backup_set_dest,
      const ObBackupLinkedBlockAddr &entry_block_id, const int64_t total_block_count,
      const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key,
      const ObMemAttr &mem_attr, const ObStorageIdMod &mod);
  int get_next_item(ObBackupLinkedItem &link_item);

public:
  const common::ObArray<ObBackupLinkedBlockAddr> &get_read_block_list() const { return read_block_list_; }

private:
  int check_need_fetch_new_block_(bool &need_fetch_new);
  int read_item_block_();
  int parse_item_list_(const int64_t item_count,
      blocksstable::ObBufferReader &buffer_reader);
  int inner_get_next_item_(ObBackupLinkedItem &link_item);

private:
  bool is_inited_;
  bool has_prev_;
  int64_t item_idx_;
  int64_t read_item_count_;
  int64_t read_block_count_;
  common::ObArray<ObBackupLinkedItem> block_item_list_;
  ObBackupLinkedBlockReader block_reader_;
  common::ObArray<ObBackupLinkedBlockAddr> read_block_list_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLinkedBlockItemReader);
};

}
}

#endif