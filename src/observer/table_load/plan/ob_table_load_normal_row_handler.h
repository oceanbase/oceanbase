/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "observer/table_load/plan/ob_table_load_dml_row_handler.h"
#include "sql/engine/cmd/ob_load_data_utils.h"

namespace oceanbase
{
namespace table
{
class ObTableLoadResultInfo;
} // namespace table
namespace observer
{
class ObTableLoadErrorRowHandler;

class ObTableLoadNormalRowHandler : public ObTableLoadTableDMLRowHandler
{
public:
  ObTableLoadNormalRowHandler(ObTableLoadTableOp *table_op);
  virtual ~ObTableLoadNormalRowHandler() = default;
  int init() override;

  /**
   * handle rows direct insert into sstable
   */
  int handle_insert_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override;
  int handle_insert_row(const ObTabletID &tablet_id,
                        const blocksstable::ObDatumRow &datum_row) override;
  int handle_insert_batch(const ObTabletID &tablet_id,
                          const blocksstable::ObBatchDatumRows &datum_rows) override;
  int handle_delete_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override;

  /**
   * handle rows with the same primary key in the imported data
   */
  int handle_update_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override;
  int handle_update_row(const ObTabletID &tablet_id,
                        common::ObArray<const storage::ObDirectLoadExternalRow *> &rows,
                        const storage::ObDirectLoadExternalRow *&row) override;
  int handle_update_row(common::ObArray<const storage::ObDirectLoadMultipleDatumRow *> &rows,
                        const storage::ObDirectLoadMultipleDatumRow *&row) override;

  /**
   * handle rows with the same primary key between the imported data and the original data
   */
  int handle_update_row(const ObTabletID &tablet_id, const storage::ObDirectLoadDatumRow &old_row,
                        const storage::ObDirectLoadDatumRow &new_row,
                        const storage::ObDirectLoadDatumRow *&result_row) override;

  /**
   * handle insert row conflict with delete row
   */
  int handle_insert_delete_conflict(const ObTabletID &tablet_id,
                                     const storage::ObDirectLoadDatumRow &datum_row) override;

  TO_STRING_EMPTY();
};

} // namespace observer
} // namespace oceanbase
