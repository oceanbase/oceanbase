/**
 * Copyright (c) 2021 OceanBase
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

#include "storage/blocksstable/ob_datum_row.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_handler.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace sql
{
class ObLoadDataStat;
} // namespace sql
namespace table
{
class ObTableLoadSqlStatistics;
} // namespace table
namespace storage
{
class ObDirectLoadInsertTabletContext;
class ObDirectLoadIStoreRowIterator;
class ObDirectLoadDMLRowHandler;

class ObDirectLoadInsertTableRowIterator final : public ObIDirectLoadRowIterator
{
public:
  ObDirectLoadInsertTableRowIterator();
  virtual ~ObDirectLoadInsertTableRowIterator();
  int init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
           const ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
           ObDirectLoadDMLRowHandler *dml_row_handler, sql::ObLoadDataStat *job_stat,
           bool is_delete_full_row);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
  int get_next_row(const bool skip_lob, const blocksstable::ObDatumRow *&row) override;
  int close();
private:
  int inner_get_next_row(blocksstable::ObDatumRow *&datum_row);

protected:
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  const ObIArray<ObDirectLoadIStoreRowIterator *> *row_iters_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
  sql::ObLoadDataStat *job_stat_;
  ObDirectLoadInsertTableRowHandler row_handler_;
  blocksstable::ObDatumRow insert_datum_row_;
  blocksstable::ObDatumRow delete_datum_row_;
  bool is_delete_full_row_;
  int64_t rowkey_column_count_;
  int64_t column_count_;
  int64_t pos_;
  ObDirectLoadInsertTableResult insert_table_result_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
