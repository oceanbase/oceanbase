/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#include "observer/table_load/backup/ob_table_load_backup_partition_scanner.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/ob_define.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

// 用于2x物理备份
class ObTableLoadPhysicalBackupPartScannerV1 : public ObTableLoadBackupPartScanner
{
public:
  ObTableLoadPhysicalBackupPartScannerV1();
  virtual ~ObTableLoadPhysicalBackupPartScannerV1();
  int init(const ObTableLoadBackupVersion &backup_version,
           const share::ObBackupStorageInfo &storage_info,
           const ObSchemaInfo &schema_info,
           const ObString &data_path,
           const ObString &backup_set_id,
           const int64_t backup_table_id,
           const int64_t subpart_count,
           const int64_t subpart_idx);
  void reset() override;
  TO_STRING_KV(K_(storage_info),
               K_(backup_version),
               K_(data_path),
               K_(backup_set_id),
               K_(backup_table_id),
               K_(block_idx),
               K_(block_start_idx),
               K_(block_end_idx),
               K_(data_macro_block_index),
               K_(lob_macro_block_index));
private:
  struct ObBackupMacroIndex
  {
    static const int64_t MACRO_INDEX_VERSION = 1;
    OB_UNIS_VERSION(MACRO_INDEX_VERSION);
  public:
    ObBackupMacroIndex();
    void reset();
    int check_valid() const;
    TO_STRING_KV(
      K_(table_id),
      K_(partition_id),
      K_(index_table_id),
      K_(sstable_macro_index),
      K_(data_version),
      K_(data_seq),
      K_(backup_set_id),
      K_(sub_task_id),
      K_(offset),
      K_(data_length));

    uint64_t table_id_;
    int64_t partition_id_;
    uint64_t index_table_id_;
    int64_t sstable_macro_index_;
    int64_t data_version_;
    int64_t data_seq_;
    int64_t backup_set_id_;
    int64_t sub_task_id_;
    int64_t offset_;
    int64_t data_length_; //=ObBackupDataHeader(header_length_+macro_meta_length_ + macro_data_length_)
  };
private:
  int init_macro_block_index(const int64_t subpart_count,
                             const int64_t subpart_idx) override;
  int read_macro_block_data(const int64_t block_idx,
                            const bool is_lob_block,
                            char *&data_buf,
                            int64_t &read_size) override;
private:
  ObString data_path_;
  ObString backup_set_id_;
  int64_t backup_table_id_;
  ObArray<ObBackupMacroIndex> data_macro_block_index_;
  ObArray<ObBackupMacroIndex> lob_macro_block_index_;
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
