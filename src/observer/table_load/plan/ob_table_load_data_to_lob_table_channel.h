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

#include "observer/table_load/plan/ob_table_load_data_channel.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadDataToLobTableChannel : public ObTableLoadTableChannel
{
public:
  ObTableLoadDataToLobTableChannel(ObTableLoadTableOp *up_table_op,
                                   ObTableLoadTableOp *down_table_op)
    : ObTableLoadTableChannel(up_table_op, down_table_op)
  {
  }
  virtual ~ObTableLoadDataToLobTableChannel() = default;
  int handle_insert_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override;
  int handle_insert_row(const ObTabletID &tablet_id,
                        const blocksstable::ObDatumRow &datum_row) override;
  int handle_insert_batch(const ObTabletID &tablet_id,
                          const blocksstable::ObBatchDatumRows &datum_rows) override;
  int handle_delete_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override;
  int handle_update_row(const ObTabletID &tablet_id,
                        const storage::ObDirectLoadDatumRow &datum_row) override;
  int handle_update_row(const ObTabletID &tablet_id,
                        ObIArray<const storage::ObDirectLoadExternalRow *> &rows,
                        const storage::ObDirectLoadExternalRow *result_row) override;
  int handle_update_row(ObArray<const storage::ObDirectLoadMultipleDatumRow *> &rows,
                        const storage::ObDirectLoadMultipleDatumRow *result_row) override;
  int handle_update_row(const ObTabletID &tablet_id, const storage::ObDirectLoadDatumRow &old_row,
                        const storage::ObDirectLoadDatumRow &new_row,
                        const storage::ObDirectLoadDatumRow *result_row) override;
  int handle_insert_delete_conflict(const ObTabletID &tablet_id,
                                    const storage::ObDirectLoadDatumRow &datum_row) override;
private:
  int create_row_projector() override { return OB_SUCCESS; }
  ObDirectLoadTableType::Type get_table_type() override
  {
    return ObDirectLoadTableType::EXTERNAL_TABLE;
  }
};

} // namespace observer
} // namespace oceanbase
