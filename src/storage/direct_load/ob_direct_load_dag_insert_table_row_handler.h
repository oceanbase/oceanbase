/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/allocator/page_arena.h"

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
class ObDirectLoadDagLobBuilder;
class ObDirectLoadDatumRow;

class ObDirectLoadDagInsertTableRowHandler
{
public:
  ObDirectLoadDagInsertTableRowHandler();
  ~ObDirectLoadDagInsertTableRowHandler();
  int init(ObDirectLoadInsertTabletContext *insert_tablet_ctx);
  int switch_slice(const int64_t slice_idx);
  int handle_row(blocksstable::ObDatumRow &datum_row);
  int handle_batch(blocksstable::ObBatchDatumRows &datum_rows);
  int close();

private:
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  table::ObTableLoadSqlStatistics *sql_statistics_;
  ObDirectLoadDagLobBuilder *lob_builder_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
