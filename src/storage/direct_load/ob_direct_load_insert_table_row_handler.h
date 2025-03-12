/**
 * Copyright (c) 2024 OceanBase
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
  int handle_row(const IVectorPtrs &vectors,
                 const int64_t row_idx,
                 const ObDirectLoadRowFlag &row_flag);
  int close();

private:
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  table::ObTableLoadSqlStatistics *sql_statistics_;
  ObDirectLoadLobBuilder *lob_builder_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
