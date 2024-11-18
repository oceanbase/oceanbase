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
#include "observer/table_load/backup/ob_table_load_backup_table.h"
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_block_sstable_struct.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{

class ObTableLoadBackupTable_V_3_X : public ObTableLoadBackupTable
{
public:
  ObTableLoadBackupTable_V_3_X();
  virtual ~ObTableLoadBackupTable_V_3_X() {}
  int init(
      const share::ObBackupStorageInfo *storage_info,
      const ObString &path,
      const share::schema::ObTableSchema *table_schema) override;
  int scan(
      int64_t part_idx,
      ObNewRowIterator *&iter,
      ObIAllocator &allocator,
      int64_t subpart_count = 1,
      int64_t subpart_idx = 0) override;
  int64_t get_partition_count() const override { return part_list_.count(); }
  TO_STRING_KV(
      K_(storage_info),
      K_(schema_info),
      K_(data_path),
      K_(meta_path),
      K_(backup_set_id),
      K_(backup_table_id),
      K_(part_list));
private:
  int parse_path(const ObString &path);
  int check_support_for_tenant();
  int get_partitions();
  int init_schema_info(const share::schema::ObTableSchema *table_schema);
private:
  ObArenaAllocator allocator_;
  share::ObBackupStorageInfo storage_info_;
  ObSchemaInfo schema_info_;
  ObString data_path_;
  ObString meta_path_;
  int64_t backup_table_id_;
  ObString backup_set_id_;
  ObArray<ObString> part_list_;
  bool is_inited_;
};

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
