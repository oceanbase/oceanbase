/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "storage/direct_load/ob_direct_load_dml_row_handler.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadLobRowDeleteHandler : public storage::ObDirectLoadDMLRowHandler
{
public:
  ObTableLoadLobRowDeleteHandler() = default;
  virtual ~ObTableLoadLobRowDeleteHandler() = default;

  /**
   * handle rows direct insert into sstable
   */
  int handle_insert_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override
  {
    return OB_ERR_UNEXPECTED;
  }
  int handle_delete_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override
  {
    // do nothing
    return OB_SUCCESS;
  }
  int handle_insert_row(const ObTabletID &tablet_id,
                        const blocksstable::ObDatumRow &datum_row) override
  {
    return OB_ERR_UNEXPECTED;
  }
  int handle_insert_batch(const ObTabletID &tablet_id,
                          const blocksstable::ObBatchDatumRows &datum_rows) override
  {
    return OB_ERR_UNEXPECTED;
  }

  /**
   * handle rows with the same primary key in the imported data
   */
  int handle_update_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override
  {
    return OB_ERR_UNEXPECTED;
  }
  int handle_update_row(const ObTabletID &tablet_id,
                        common::ObArray<const storage::ObDirectLoadExternalRow *> &rows,
                        const storage::ObDirectLoadExternalRow *&row) override
  {
    return OB_ERR_UNEXPECTED;
  }
  int handle_update_row(common::ObArray<const storage::ObDirectLoadMultipleDatumRow *> &rows,
                        const storage::ObDirectLoadMultipleDatumRow *&row) override
  {
    return OB_ERR_UNEXPECTED;
  }

  /**
   * handle rows with the same primary key between the imported data and the original data
   */
  int handle_update_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &old_row,
                        const storage::ObDirectLoadDatumRow &new_row,
                        const storage::ObDirectLoadDatumRow *&result_row) override
  {
    return OB_ERR_UNEXPECTED;
  }

  /**
   * handle insert row conflict with delete row
   */
  int handle_insert_delete_conflict(const ObTabletID &tablet_id,
                                   const storage::ObDirectLoadDatumRow &datum_row) override
  {
    return OB_ERR_UNEXPECTED;
  }

  TO_STRING_EMPTY();
};

} // namespace observer
} // namespace oceanbase
