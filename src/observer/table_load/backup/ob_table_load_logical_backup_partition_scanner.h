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

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObTableLoadLogicalBackupPartScanner : public ObTableLoadBackupPartScanner
{
public:
  ObTableLoadLogicalBackupPartScanner();
  virtual ~ObTableLoadLogicalBackupPartScanner();
  int init(const ObTableLoadBackupVersion &backup_version,
           const share::ObBackupStorageInfo &storage_info,
           const ObSchemaInfo &schema_info,
           const ObIArray<int64_t> &column_ids,
           const ObString &data_path,
           const ObString &meta_path,
           const ObString &backup_set_id,
           const int64_t subpart_count,
           const int64_t subpart_idx);
  void reset() override;
  TO_STRING_KV(K(storage_info_),
               K(backup_version_),
               K(column_ids_),
               K(data_path_),
               K(block_idx_),
               K(block_start_idx_),
               K(block_end_idx_),
               K(data_macro_block_index_),
               K(lob_macro_block_index_));
private:
  int init_macro_block_index(const int64_t subpart_count,
                             const int64_t subpart_idx) override;
  int read_macro_block_data(const int64_t block_idx,
                            const bool is_lob_block,
                            char *&data_buf,
                            int64_t &read_size) override;
private:
  ObString data_path_;
  ObString meta_path_;
  ObString backup_set_id_;
  ObArray<ObString> data_macro_block_index_;
  ObArray<ObString> lob_macro_block_index_;
};

} // namespace table_load_backup
} // namespace observer
} // namespace oceanbase
