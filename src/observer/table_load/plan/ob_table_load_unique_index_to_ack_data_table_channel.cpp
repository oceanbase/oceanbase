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

#include "observer/table_load/plan/ob_table_load_unique_index_to_ack_data_table_channel.h"
#include "observer/table_load/ob_table_load_row_projector.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace storage;

int ObTableLoadUniqueIndexToAckDataTableChannel::create_row_projector()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_projector_ =
                  OB_NEWx(ObTableLoadUniqueIndexToMainRowkeyProjector, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new row projector", KR(ret));
  } else if (OB_FAIL(row_projector_->init(
               up_table_op_->op_ctx_->store_table_ctx_->schema_->table_id_,
               down_table_op_->op_ctx_->store_table_ctx_->schema_->table_id_))) {
    LOG_WARN("fail to init row projector", KR(ret));
  }
  return ret;
}

int ObTableLoadUniqueIndexToAckDataTableChannel::handle_insert_row(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadUniqueIndexToAckDataTableChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected channel is closed", KR(ret));
  } else if (datum_row.is_ack_) {
    ObTableLoadTableBuilder *table_builder = nullptr;
    if (OB_FAIL(table_builder_mgr_.get_table_builder(table_builder))) {
      LOG_WARN("fail to get table builder", KR(ret));
    } else {
      ObDirectLoadDatumRow &insert_datum_row = table_builder->get_insert_datum_row();
      insert_datum_row.seq_no_ = datum_row.seq_no_;
      ObTabletID data_tablet_id;
      if (OB_FAIL(
            row_projector_->projector(tablet_id, datum_row, data_tablet_id, insert_datum_row))) {
        LOG_WARN("fail to project row", KR(ret));
      } else if (OB_FAIL(table_builder->append_row(data_tablet_id, insert_datum_row))) {
        LOG_WARN("fail to append row", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadUniqueIndexToAckDataTableChannel::handle_insert_row(const ObTabletID &tablet_id,
                                                                   const ObDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadUniqueIndexToAckDataTableChannel::handle_insert_batch(
  const ObTabletID &tablet_id, const ObBatchDatumRows &datum_rows)
{
  return OB_SUCCESS;
}

int ObTableLoadUniqueIndexToAckDataTableChannel::handle_delete_row(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadUniqueIndexToAckDataTableChannel::handle_update_row(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadUniqueIndexToAckDataTableChannel::handle_update_row(
  const ObTabletID &tablet_id, ObIArray<const ObDirectLoadExternalRow *> &rows,
  const ObDirectLoadExternalRow *result_row)
{
  return OB_SUCCESS;
}

int ObTableLoadUniqueIndexToAckDataTableChannel::handle_update_row(
  ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
  const ObDirectLoadMultipleDatumRow *result_row)
{
  return OB_SUCCESS;
}

int ObTableLoadUniqueIndexToAckDataTableChannel::handle_update_row(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &old_row,
  const ObDirectLoadDatumRow &new_row, const ObDirectLoadDatumRow *result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadUniqueIndexToAckDataTableChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected channel is closed", KR(ret));
  } else {
    ObTableLoadTableBuilder *table_builder = nullptr;
    if (OB_FAIL(table_builder_mgr_.get_table_builder(table_builder))) {
      LOG_WARN("fail to get table builder", KR(ret));
    } else {
      ObDirectLoadDatumRow &insert_datum_row = table_builder->get_insert_datum_row();
      insert_datum_row.seq_no_ = result_row->seq_no_;
      ObTabletID data_tablet_id;
      if (OB_FAIL(
            row_projector_->projector(tablet_id, *result_row, data_tablet_id, insert_datum_row))) {
        LOG_WARN("fail to project row", KR(ret));
      } else if (OB_FAIL(table_builder->append_row(data_tablet_id, insert_datum_row))) {
        LOG_WARN("fail to append row", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadUniqueIndexToAckDataTableChannel::handle_insert_delete_conflict(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

} // namespace observer
} // namespace oceanbase
