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

#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "storage/blocksstable/ob_datum_row.h"
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
class ObTableLoadErrorRowHandler;
class ObTableLoadParam;
class ObTableLoadStoreTableCtx;

class ObTableLoadDataRowHandler : public ObDirectLoadDMLRowHandler
{
private:
  static bool external_row_compare(const ObDirectLoadExternalRow *lhs,
                                   const ObDirectLoadExternalRow *rhs)
  {
    return lhs->seq_no_ < rhs->seq_no_;
  }
  static bool multiple_external_row_compare(const ObDirectLoadMultipleDatumRow *lhs,
                                            const ObDirectLoadMultipleDatumRow *rhs)
  {
    return lhs->seq_no_ < rhs->seq_no_;
  }

public:
  ObTableLoadDataRowHandler();
  virtual ~ObTableLoadDataRowHandler();
  int init(const ObTableLoadParam &param, table::ObTableLoadResultInfo &result_info,
           ObTableLoadErrorRowHandler *error_row_handler,
           ObArray<ObTableLoadStoreTableCtx *> *index_store_table_ctxs);
  int handle_insert_row(const ObTabletID tablet_id, const blocksstable::ObDatumRow &row) override;
  int handle_insert_batch(const ObTabletID &tablet_id, const blocksstable::ObBatchDatumRows &datum_rows) override;
  int handle_delete_row(const ObTabletID tablet_id, const blocksstable::ObDatumRow &row)
  {
    return OB_ERR_UNEXPECTED;
  }
  int handle_insert_row_with_multi_version(const ObTabletID tablet_id, const blocksstable::ObDatumRow &row) override;
  int handle_insert_batch_with_multi_version(const ObTabletID &tablet_id, const blocksstable::ObBatchDatumRows &datum_rows) override;
  int handle_update_row(const blocksstable::ObDatumRow &row);
  int handle_update_row(common::ObArray<const ObDirectLoadExternalRow *> &rows,
                        const ObDirectLoadExternalRow *&row);
  int handle_update_row(common::ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
                        const ObDirectLoadMultipleDatumRow *&row) override;
  int handle_update_row(const ObTabletID tablet_id,
                        const blocksstable::ObDatumRow &old_row,
                        const blocksstable::ObDatumRow &new_row,
                        const blocksstable::ObDatumRow *&result_row);
  TO_STRING_KV(KPC_(error_row_handler), K_(result_info), K_(dup_action), K_(is_inited));
private:
  ObTableLoadErrorRowHandler * error_row_handler_;
  ObArray<ObTableLoadStoreTableCtx *> *index_store_table_ctxs_;
  table::ObTableLoadResultInfo *result_info_;
  sql::ObLoadDupActionType dup_action_;
  bool is_inited_;
};

}
}