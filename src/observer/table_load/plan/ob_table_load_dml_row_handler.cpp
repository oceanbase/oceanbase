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

#include "observer/table_load/plan/ob_table_load_dml_row_handler.h"
#include "observer/table_load/plan/ob_table_load_data_channel.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace storage;

/**
 * ObTableLoadTableDMLRowHandler
 */

ObTableLoadTableDMLRowHandler::ObTableLoadTableDMLRowHandler(ObTableLoadTableOp *table_op)
  : table_op_(table_op), output_channels_(&table_op->get_output_channels()), is_inited_(false)
{
}

int ObTableLoadTableDMLRowHandler::push_insert_row(const ObTabletID &tablet_id,
                                                   const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_->count(); ++i) {
    ObTableLoadTableChannel *table_channel = output_channels_->at(i);
    if (OB_FAIL(table_channel->handle_insert_row(tablet_id, datum_row))) {
      LOG_WARN("fail to handle insert row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadTableDMLRowHandler::push_insert_row(const ObTabletID &tablet_id,
                                                   const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_->count(); ++i) {
    ObTableLoadTableChannel *table_channel = output_channels_->at(i);
    if (OB_FAIL(table_channel->handle_insert_row(tablet_id, datum_row))) {
      LOG_WARN("fail to handle insert row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadTableDMLRowHandler::push_insert_batch(const ObTabletID &tablet_id,
                                                     const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_->count(); ++i) {
    ObTableLoadTableChannel *table_channel = output_channels_->at(i);
    if (OB_FAIL(table_channel->handle_insert_batch(tablet_id, datum_rows))) {
      LOG_WARN("fail to handle insert batch", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadTableDMLRowHandler::push_delete_row(const ObTabletID &tablet_id,
                                                   const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_->count(); ++i) {
    ObTableLoadTableChannel *table_channel = output_channels_->at(i);
    if (OB_FAIL(table_channel->handle_delete_row(tablet_id, datum_row))) {
      LOG_WARN("fail to handle delete row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadTableDMLRowHandler::push_update_row(const ObTabletID &tablet_id,
                                                   const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_->count(); ++i) {
    ObTableLoadTableChannel *table_channel = output_channels_->at(i);
    if (OB_FAIL(table_channel->handle_update_row(tablet_id, datum_row))) {
      LOG_WARN("fail to handle update row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadTableDMLRowHandler::push_update_row(const ObTabletID &tablet_id,
                                                   ObArray<const ObDirectLoadExternalRow *> &rows,
                                                   const ObDirectLoadExternalRow *result_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_->count(); ++i) {
    ObTableLoadTableChannel *table_channel = output_channels_->at(i);
    if (OB_FAIL(table_channel->handle_update_row(tablet_id, rows, result_row))) {
      LOG_WARN("fail to handle update row", KR(ret));
    }
  }
  return ret;
}
int ObTableLoadTableDMLRowHandler::push_update_row(
  ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
  const ObDirectLoadMultipleDatumRow *result_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_->count(); ++i) {
    ObTableLoadTableChannel *table_channel = output_channels_->at(i);
    if (OB_FAIL(table_channel->handle_update_row(rows, result_row))) {
      LOG_WARN("fail to handle update row", KR(ret));
    }
  }
  return ret;
}
int ObTableLoadTableDMLRowHandler::push_update_row(const ObTabletID &tablet_id,
                                                   const ObDirectLoadDatumRow &old_row,
                                                   const ObDirectLoadDatumRow &new_row,
                                                   const ObDirectLoadDatumRow *result_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_->count(); ++i) {
    ObTableLoadTableChannel *table_channel = output_channels_->at(i);
    if (OB_FAIL(table_channel->handle_update_row(tablet_id, old_row, new_row, result_row))) {
      LOG_WARN("fail to handle update row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadTableDMLRowHandler::push_insert_delete_conflict(const ObTabletID &tablet_id,
                                                               const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_->count(); ++i) {
    ObTableLoadTableChannel *table_channel = output_channels_->at(i);
    if (OB_FAIL(table_channel->handle_insert_delete_conflict(tablet_id, datum_row))) {
      LOG_WARN("fail to handle insert delete conflict", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
