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

#ifndef OCEANBASE_STORAGE_BACKUP_OB_PARTITION_BACKUP_STRUCT_H_
#define OCEANBASE_STORAGE_BACKUP_OB_PARTITION_BACKUP_STRUCT_H_

#include "share/scheduler/ob_sys_task_stat.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_partition_service.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
namespace storage {

struct ObBackupSSTableMetaInfo {
  OB_UNIS_VERSION(1);

public:
  ObBackupSSTableMetaInfo();
  virtual ~ObBackupSSTableMetaInfo() = default;
  int assign(const ObBackupSSTableMetaInfo& sstable_meta_info);
  bool is_valid() const;
  void reset();
  int set_sstable_meta_info(const blocksstable::ObSSTableBaseMeta& sstable_meta, const ObITable::TableKey& table_key);

  TO_STRING_KV(K_(sstable_meta), K_(table_key));

  common::ObArenaAllocator allocator_;
  // OB_SERIALIZE_MEMBER
  blocksstable::ObSSTableBaseMeta sstable_meta_;
  ObITable::TableKey table_key_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupSSTableMetaInfo);
};

struct ObBackupPartitionStoreMetaInfo {
  OB_UNIS_VERSION(1);

public:
  ObBackupPartitionStoreMetaInfo();
  virtual ~ObBackupPartitionStoreMetaInfo() = default;
  int assign(const ObBackupPartitionStoreMetaInfo& partition_store_meta_info);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(partition_store_meta), K_(sstable_meta_info_array));
  ObPGPartitionStoreMeta partition_store_meta_;
  common::ObSArray<ObBackupSSTableMetaInfo> sstable_meta_info_array_;
};

struct ObBackupPGMetaInfo {
  OB_UNIS_VERSION(1);

public:
  ObBackupPGMetaInfo();
  virtual ~ObBackupPGMetaInfo() = default;
  int assign(const ObBackupPGMetaInfo& pg_meta_info);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(pg_meta), K_(partition_store_meta_info_array));
  ObPartitionGroupMeta pg_meta_;
  common::ObSArray<ObBackupPartitionStoreMetaInfo> partition_store_meta_info_array_;
};

// 3.1 and later use this struct
// one file for each pg of each backup_set, flush file to disk after every subtask finished
struct ObBackupTableMacroIndex {
  static const int64_t TABLE_MACRO_INDEX_VERSION = 1;
  OB_UNIS_VERSION(TABLE_MACRO_INDEX_VERSION);

public:
  ObBackupTableMacroIndex();
  virtual ~ObBackupTableMacroIndex() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(sstable_macro_index), K_(data_version), K_(data_seq), K_(backup_set_id), K_(sub_task_id), K_(offset),
      K_(data_length), KP_(table_key_ptr));

  // need serialize
  int64_t sstable_macro_index_;
  int64_t data_version_;
  int64_t data_seq_;
  int64_t backup_set_id_;
  int64_t sub_task_id_;
  int64_t offset_;
  int64_t data_length_;  //=ObBackupDataHeader(header_length_+macro_meta_length_ + macro_data_length_)
  // no need serialize
  const ObITable::TableKey* table_key_ptr_;
};

struct ObBackupTableKeyInfo {
  static const int64_t BACKUP_TABLE_KEY_META_VERSION = 1;
  OB_UNIS_VERSION(BACKUP_TABLE_KEY_META_VERSION);

public:
  ObBackupTableKeyInfo();
  virtual ~ObBackupTableKeyInfo() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(table_key), K_(total_macro_block_count));

  // need serialize
  ObITable::TableKey table_key_;
  int64_t total_macro_block_count_;
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_PARTITION_BACKUP_H_
