/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "sql/engine/cmd/ob_load_data_utils.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadErrorRowHandler;

class ObTableLoadUniqueIndexRowHandler : public storage::ObDirectLoadDMLRowHandler
{
public:
  ObTableLoadUniqueIndexRowHandler();
  virtual ~ObTableLoadUniqueIndexRowHandler();
  int init(ObTableLoadStoreCtx *store_ctx);

  /**
   * handle rows direct insert into sstable
   */
  int handle_insert_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override;
  int handle_delete_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override
  {
    return OB_ERR_UNEXPECTED;
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
                        const storage::ObDirectLoadDatumRow &datum_row) override;
  int handle_update_row(const ObTabletID &tablet_id,
                        common::ObArray<const storage::ObDirectLoadExternalRow *> &rows,
                        const storage::ObDirectLoadExternalRow *&row) override;
  int handle_update_row(common::ObArray<const storage::ObDirectLoadMultipleDatumRow *> &rows,
                        const storage::ObDirectLoadMultipleDatumRow *&row) override;

  /**
   * handle rows with the same primary key between the imported data and the original data
   */
  int handle_update_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &old_row,
                        const storage::ObDirectLoadDatumRow &new_row,
                        const storage::ObDirectLoadDatumRow *&result_row) override;

  /**
   * handle insert row conflict with delete row
   */
  int handle_insert_delete_conflict(const ObTabletID &tablet_id,
                                   const storage::ObDirectLoadDatumRow &datum_row) override
  {
    return OB_ERR_UNEXPECTED;
  }

  TO_STRING_KV(KP_(store_ctx),
               KP_(error_row_handler),
               KP_(result_info),
               K_(dup_action),
               K_(is_inited));

private:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadErrorRowHandler *error_row_handler_;
  table::ObTableLoadResultInfo *result_info_;
  sql::ObLoadDupActionType dup_action_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase