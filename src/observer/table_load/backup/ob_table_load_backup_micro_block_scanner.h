/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
