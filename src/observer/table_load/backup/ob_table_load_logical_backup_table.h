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
#include "observer/table_load/backup/ob_table_load_logical_backup_partition_scanner.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObTableLoadLogicalBackupTable : public ObTableLoadBackupTable
{
public:
  ObTableLoadLogicalBackupTable();
  virtual ~ObTableLoadLogicalBackupTable() = default;
  int init(const ObTableLoadBackupVersion &backup_version,
           const share::ObBackupStorageInfo *storage_info,
           const ObString &path,
           const share::schema::ObTableSchema *table_schema) override;
  int scan(int64_t part_idx,
           ObNewRowIterator *&iter,
           ObIAllocator &allocator,
           int64_t subpart_count = 1,
           int64_t subpart_idx = 0) override;
  int64_t get_partition_count() const override { return part_list_.count(); }
  TO_STRING_KV(K_(backup_version),
               K_(backup_table_id),
               K_(backup_tenant_id),
               K_(backup_set_id),
               K_(data_path),
               K_(meta_path),
               K_(column_ids),
               K_(part_list));
private:
  int check_support_for_tenant();
  int init_schema_info(const share::schema::ObTableSchema *table_schema);
  int parse_path(const ObString &path);
  int get_column_ids();
  int get_partitions();
  int get_column_ids_from_create_table_sql(const ObString &sql, ObIArray<int64_t> &column_ids);
private:
  ObArenaAllocator allocator_;
  ObTableLoadBackupVersion backup_version_;
  share::ObBackupStorageInfo storage_info_;
  ObSchemaInfo schema_info_;
  ObString backup_table_id_;
  ObString backup_tenant_id_;
  ObString backup_set_id_;
  ObString data_path_;
  ObString meta_path_;
  ObArray<int64_t> column_ids_;
  ObArray<ObString> part_list_;
  bool is_inited_;
};

} // namespace table_load_backup
} // namespace observer
} // namespace oceanbase
