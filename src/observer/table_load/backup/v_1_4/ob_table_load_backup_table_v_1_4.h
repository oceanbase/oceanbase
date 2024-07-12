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

namespace oceanbase
{
namespace observer
{

class ObTableLoadBackupTable_V_1_4 : public ObTableLoadBackupTable
{
public:
  ObTableLoadBackupTable_V_1_4()
    : allocator_("TLD_BT_V_1_4"),
      is_inited_(false)
  {
    allocator_.set_tenant_id(MTL_ID());
    column_ids_.set_tenant_id(MTL_ID());
    part_list_.set_tenant_id(MTL_ID());
  }
  virtual ~ObTableLoadBackupTable_V_1_4() {}
  int init(const share::ObBackupStorageInfo *storage_info, const ObString &path) override;
  int scan(int64_t part_idx, ObNewRowIterator *&iter, ObIAllocator &allocator,
           int64_t subpart_count = 1, int64_t subpart_idx = 0) override;
  bool is_valid() const override;
  int64_t get_column_count() const override { return column_ids_.count(); }
  int64_t get_partition_count() const override { return part_list_.count(); }
  TO_STRING_KV(K(table_id_), K(data_path_), K(meta_path_), K(column_ids_), K(part_list_));
private:
  int parse_path(const ObString &path);
  int get_column_ids();
  int get_partitions();
private:
  ObArenaAllocator allocator_;
  share::ObBackupStorageInfo storage_info_;
  ObString table_id_;
  ObString data_path_;
  ObString meta_path_;
  ObArray<int64_t> column_ids_;
  ObArray<ObString> part_list_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
