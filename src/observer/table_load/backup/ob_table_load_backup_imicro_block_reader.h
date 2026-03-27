/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once
#include "observer/table_load/backup/ob_table_load_backup_irow_reader.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
class ObIMicroBlockReader
{
public:
  ObIMicroBlockReader() {}
  virtual ~ObIMicroBlockReader() = default;
  virtual int init(const ObMicroBlockData *micro_block_data,
                   const ObTableLoadBackupVersion &backup_version,
                   const ObIColumnMap *column_map) = 0;
  virtual void reset() = 0;
  virtual int get_row(const int64_t idx, common::ObNewRow &row) = 0;
};

} // namespace table_load_backup
} // namespace observer
} // namespace oceanbase
