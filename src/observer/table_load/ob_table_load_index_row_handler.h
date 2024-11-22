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
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreTableCtx;

class ObTableLoadIndexRowHandler : public ObDirectLoadDMLRowHandler
{
public:
  ObTableLoadIndexRowHandler();
  virtual ~ObTableLoadIndexRowHandler();
  int handle_insert_row(const ObTabletID tablet_id, const blocksstable::ObDatumRow &row);
  int handle_insert_batch(const ObTabletID &tablet_id, const blocksstable::ObBatchDatumRows &datum_rows) override
  {
    return OB_ERR_UNEXPECTED;
  }
  int handle_delete_row(const ObTabletID tablet_id, const blocksstable::ObDatumRow &row);
  int handle_insert_row_with_multi_version(const ObTabletID tablet_id, const blocksstable::ObDatumRow &row)
  {
    return OB_ERR_UNEXPECTED;
  }
  int handle_insert_batch_with_multi_version(const ObTabletID &tablet_id, const blocksstable::ObBatchDatumRows &datum_rows)
  {
    return OB_ERR_UNEXPECTED;
  }
  int handle_update_row(const blocksstable::ObDatumRow &row) override { return OB_ERR_UNEXPECTED; };
  int handle_update_row(common::ObArray<const ObDirectLoadExternalRow *> &rows,
                        const ObDirectLoadExternalRow *&row) override
  {
    return OB_ERR_UNEXPECTED;
  };
  int handle_update_row(common::ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
                        const ObDirectLoadMultipleDatumRow *&row) override
  {
    return OB_ERR_UNEXPECTED;
  };
  int handle_update_row(const ObTabletID tablet_id,
                        const blocksstable::ObDatumRow &old_row,
                        const blocksstable::ObDatumRow &new_row,
                        const blocksstable::ObDatumRow *&result_row) override
  {
    return OB_ERR_UNEXPECTED;
  };
  TO_STRING_EMPTY();
};

} // namespace observer
} // namespace oceanbase