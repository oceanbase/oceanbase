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
#include "observer/table_load/backup/ob_table_load_backup_imicro_block_reader.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObTableLoadBackupMicroBlockScanner
{
public:
  ObTableLoadBackupMicroBlockScanner();
  ~ObTableLoadBackupMicroBlockScanner();
  int init(const ObMicroBlockData *micro_block_data,
           const ObTableLoadBackupVersion &backup_version,
           const ObIColumnMap *column_map);
  void reset();
  void reuse();
  bool is_valid() const;
  int get_next_row(common::ObNewRow *&row);
private:
  int init_row();
private:
  common::ObArenaAllocator allocator_;
  ObIMicroBlockReader *reader_;
  const ObIColumnMap *column_map_;
  common::ObNewRow row_;
  int64_t row_idx_;
  bool is_inited_;
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
