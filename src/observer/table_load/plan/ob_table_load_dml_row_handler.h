/**
 * Copyright (c) 2025 OceanBase
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

#include "storage/direct_load/ob_direct_load_dml_row_handler.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableOp;
class ObTableLoadTableChannel;

class ObTableLoadTableDMLRowHandler : public storage::ObDirectLoadDMLRowHandler
{
public:
  ObTableLoadTableDMLRowHandler(ObTableLoadTableOp *table_op);
  virtual ~ObTableLoadTableDMLRowHandler() = default;
  virtual int init() = 0;

  VIRTUAL_TO_STRING_KV(KP_(table_op), KP_(output_channels), K_(is_inited));

protected:
  int push_insert_row(const ObTabletID &tablet_id, const storage::ObDirectLoadDatumRow &datum_row);
  int push_insert_row(const ObTabletID &tablet_id, const blocksstable::ObDatumRow &datum_row);
  int push_insert_batch(const ObTabletID &tablet_id,
                        const blocksstable::ObBatchDatumRows &datum_rows);
  int push_delete_row(const ObTabletID &tablet_id, const storage::ObDirectLoadDatumRow &datum_row);
  int push_update_row(const ObTabletID &tablet_id, const storage::ObDirectLoadDatumRow &datum_row);
  int push_update_row(const ObTabletID &tablet_id,
                      common::ObArray<const storage::ObDirectLoadExternalRow *> &rows,
                      const storage::ObDirectLoadExternalRow *result_row);
  int push_update_row(common::ObArray<const storage::ObDirectLoadMultipleDatumRow *> &rows,
                      const storage::ObDirectLoadMultipleDatumRow *result_row);
  int push_update_row(const ObTabletID &tablet_id, const storage::ObDirectLoadDatumRow &old_row,
                      const storage::ObDirectLoadDatumRow &new_row,
                      const storage::ObDirectLoadDatumRow *result_row);
  int push_insert_delete_conflict(const ObTabletID &tablet_id,
                                  const storage::ObDirectLoadDatumRow &datum_row);

protected:
  ObTableLoadTableOp *table_op_;
  const ObIArray<ObTableLoadTableChannel *> *output_channels_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
