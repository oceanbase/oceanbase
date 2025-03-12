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

#include "sql/engine/cmd/ob_load_data_utils.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"

namespace oceanbase
{
namespace table
{
class ObTableLoadResultInfo;
} // namespace table
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadErrorRowHandler;

class ObTableLoadDataRowInsertHandler : public storage::ObDirectLoadDMLRowHandler
{
public:
  ObTableLoadDataRowInsertHandler();
  virtual ~ObTableLoadDataRowInsertHandler();
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
                        const blocksstable::ObDatumRow &datum_row) override;
  int handle_insert_batch(const ObTabletID &tablet_id,
                          const blocksstable::ObBatchDatumRows &datum_rows) override;

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

  TO_STRING_KV(KP_(store_ctx),
               KP_(error_row_handler),
               K_(result_info),
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