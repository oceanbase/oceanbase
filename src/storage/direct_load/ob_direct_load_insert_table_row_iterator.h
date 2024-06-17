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

#include "storage/access/ob_store_row_iterator.h"

namespace oceanbase
{
namespace table
{
class ObTableLoadSqlStatistics;
} // namespace table
namespace storage
{
class ObDirectLoadInsertTabletContext;
class ObDirectLoadLobBuilder;

class ObDirectLoadInsertTableRowIterator : public ObIStoreRowIterator
{
public:
  ObDirectLoadInsertTableRowIterator();
  virtual ~ObDirectLoadInsertTableRowIterator();
  int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
  int get_next_row(const bool skip_lob, const blocksstable::ObDatumRow *&row);
protected:
  int inner_init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
                 table::ObTableLoadSqlStatistics *sql_statistics,
                 ObDirectLoadLobBuilder &lob_builder);
  virtual int inner_get_next_row(blocksstable::ObDatumRow *&datum_row) = 0;
private:
  int handle_lob(blocksstable::ObDatumRow &datum_row);
protected:
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  table::ObTableLoadSqlStatistics *sql_statistics_;
  ObDirectLoadLobBuilder *lob_builder_;
  common::ObArenaAllocator lob_allocator_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
