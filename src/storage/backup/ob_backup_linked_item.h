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

#ifndef _OCEANBASE_BACKUP_OB_BACKUP_LINKED_BLOCK_ITEM_H_
#define _OCEANBASE_BACKUP_OB_BACKUP_LINKED_BLOCK_ITEM_H_

#include "common/ob_tablet_id.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "common/storage/ob_io_device.h"
#include "storage/backup/ob_backup_file_writer_ctx.h"
#include "lib/allocator/page_arena.h"
#include "storage/ob_parallel_external_sort.h"

namespace oceanbase
{
namespace backup
{

struct ObBackupLinkedBlockHeader
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t VERSION = 0;
  static const int64_t MAGIC = 1;
public:
  ObBackupLinkedBlockHeader();
  ~ObBackupLinkedBlockHeader();
  void set_previous_block_id(const ObBackupLinkedBlockAddr &physical_id) { prev_block_addr_ = physical_id; }

  TO_STRING_KV(K_(version), K_(magic), K_(item_count), K_(tablet_id), K_(table_key), K_(prev_block_addr), K_(has_prev));
  int32_t version_;
  int32_t magic_;
  int64_t item_count_;
  common::ObTabletID tablet_id_;
  storage::ObITable::TableKey table_key_;
  ObBackupLinkedBlockAddr prev_block_addr_;
  bool has_prev_;
};

struct ObBackupLinkedItem
{
  OB_UNIS_VERSION(1);
public:
  ObBackupLinkedItem();
  ~ObBackupLinkedItem();
  void reset();
  bool is_valid() const;
  bool operator==(const ObBackupLinkedItem &other) const;
  bool operator!=(const ObBackupLinkedItem &other) const;
  TO_STRING_KV(K_(macro_id), K_(backup_id));
  blocksstable::MacroBlockId macro_id_;
  ObBackupPhysicalID backup_id_; // TODO(yanfeng): change type when quick restore code merge
};

}
}

#endif
