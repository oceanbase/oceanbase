/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/allocator/page_arena.h"
#include "share/vector/ob_i_vector.h"

namespace oceanbase
{
namespace table
{
class ObTableLoadSqlStatistics;
} // namespace table
namespace blocksstable
{
class ObDatumRow;
class ObBatchDatumRows;
} // namespace blocksstable
namespace storage
{
class ObDirectLoadInsertTabletContext;
class ObDirectLoadLobBuilder;
class ObDirectLoadDatumRow;
class ObDirectLoadRowFlag;
class ObDirectLoadBatchRows;

class ObDirectLoadInsertTableRowHandler
{
public:
  ObDirectLoadInsertTableRowHandler();
  ~ObDirectLoadInsertTableRowHandler();
  void reset();
  int init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
           common::ObIAllocator *lob_allocator = nullptr);
  // 包含多版本列的完整行
  int handle_row(blocksstable::ObDatumRow &datum_row, const bool skip_lob);
  int handle_batch(blocksstable::ObBatchDatumRows &datum_rows);
  // 中间过程数据
  int handle_row(ObDirectLoadDatumRow &datum_row,
                 const ObDirectLoadRowFlag &row_flag);
  int handle_batch(const ObDirectLoadBatchRows &batch_rows,
                   const uint16_t *selector,
                   const int64_t size);
  int close();

private:
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  table::ObTableLoadSqlStatistics *sql_statistics_;
  ObDirectLoadLobBuilder *lob_builder_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
