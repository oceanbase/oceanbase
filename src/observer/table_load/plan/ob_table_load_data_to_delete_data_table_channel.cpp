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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_data_to_delete_data_table_channel.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace storage;

int ObTableLoadDataToDeleteDataTableChannel::handle_insert_row(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToDeleteDataTableChannel::handle_insert_row(const ObTabletID &tablet_id,
                                                               const ObDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToDeleteDataTableChannel::handle_insert_batch(const ObTabletID &tablet_id,
                                                                 const ObBatchDatumRows &datum_rows)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToDeleteDataTableChannel::handle_delete_row(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToDeleteDataTableChannel::handle_update_row(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToDeleteDataTableChannel::handle_update_row(
  const ObTabletID &tablet_id, ObIArray<const ObDirectLoadExternalRow *> &rows,
  const ObDirectLoadExternalRow *result_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToDeleteDataTableChannel::handle_update_row(
  ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
  const ObDirectLoadMultipleDatumRow *result_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToDeleteDataTableChannel::handle_update_row(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &old_row,
  const ObDirectLoadDatumRow &new_row, const ObDirectLoadDatumRow *result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataToIndexTableChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected channel is closed", KR(ret), K(is_closed_));
  } else if (result_row == &new_row) {
    ObTableLoadTableBuilder *table_builder = nullptr;
    if (OB_FAIL(table_builder_mgr_.get_table_builder(table_builder))) {
      LOG_WARN("fail to get table builder", KR(ret));
    } else {
      ObDirectLoadDatumRow &delete_datum_row = table_builder->get_delete_datum_row();
      delete_datum_row.storage_datums_ = old_row.storage_datums_;
      delete_datum_row.seq_no_ = old_row.seq_no_;
      if (OB_FAIL(table_builder->append_row(tablet_id, delete_datum_row))) {
        LOG_WARN("fail to append row", KR(ret), K(tablet_id), K(delete_datum_row));
      }
    }
  }
  return ret;
}

int ObTableLoadDataToDeleteDataTableChannel::handle_insert_delete_conflict(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

} // namespace observer
} // namespace oceanbase
