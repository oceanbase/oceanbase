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
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_imicro_block_reader.h"
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_row_reader.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{

class ObMicroBlockReader : public ObIMicroBlockReader
{
public:
  ObMicroBlockReader();
  virtual ~ObMicroBlockReader();
  int init(const ObMicroBlockData *micro_block_data, const ObColumnMap *column_map) override;
  void reset() override;
  int get_row(const int64_t idx, common::ObNewRow &row) override;
protected:
  common::ObArenaAllocator allocator_;
  const ObColumnMap *column_map_;
  const ObMicroBlockHeader *block_header_;
  const char *data_begin_;
  const char *data_end_;
  const int32_t *index_data_;
  ObFlatRowReader flat_row_reader_;
  bool is_inited_;
};

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
