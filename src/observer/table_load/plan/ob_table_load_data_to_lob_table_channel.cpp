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

#include "observer/table_load/plan/ob_table_load_data_to_lob_table_channel.h"
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

int ObTableLoadDataToLobTableChannel::handle_insert_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_insert_row(const ObTabletID &tablet_id,
                                                        const ObDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_insert_batch(const ObTabletID &tablet_id,
                                                          const ObBatchDatumRows &datum_rows)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_delete_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataToLobTableChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected channel is closed", KR(ret));
  } else {
    ObTableLoadTableBuilder *table_builder = nullptr;
    if (OB_FAIL(table_builder_mgr_.get_table_builder(table_builder))) {
      LOG_WARN("fail to get table builder", KR(ret));
    } else {
      ObDirectLoadDatumRow &delete_datum_row = table_builder->get_delete_datum_row();
      delete_datum_row.seq_no_ = datum_row.seq_no_;
      ObTabletID lob_tablet_id;
      if (OB_FAIL(
            (static_cast<ObTableLoadStoreLobTableCtx *>(down_table_op_->op_ctx_->store_table_ctx_))
              ->get_tablet_id(tablet_id, lob_tablet_id))) {
        LOG_WARN("fail to get tablet id", KR(ret));
      }
      common::ObArray<int64_t> &lob_column_idxs =
        up_table_op_->op_ctx_->store_table_ctx_->schema_->lob_column_idxs_;
      for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs.count(); i++) {
        int64_t lob_idx = lob_column_idxs.at(i);
        ObStorageDatum &datum = datum_row.storage_datums_[lob_idx];
        const ObLobCommon &lob_common = datum.get_lob_data();
        if (!lob_common.in_row_) {
          const ObLobId &lob_id = reinterpret_cast<const ObLobData *>(lob_common.buffer_)->id_;
          delete_datum_row.storage_datums_[0].set_string(reinterpret_cast<const char *>(&lob_id),
                                                         sizeof(ObLobId));
          if (OB_FAIL(table_builder->append_row(lob_tablet_id, delete_datum_row))) {
            LOG_WARN("fail to append row", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadDataToLobTableChannel::handle_update_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_update_row(
  const ObTabletID &tablet_id, ObIArray<const ObDirectLoadExternalRow *> &rows,
  const ObDirectLoadExternalRow *result_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_update_row(
  ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
  const ObDirectLoadMultipleDatumRow *result_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_update_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &old_row,
                                                        const ObDirectLoadDatumRow &new_row,
                                                        const ObDirectLoadDatumRow *result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataToLobTableChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected channel is closed", KR(ret));
  } else if (result_row == &new_row) {
    ObTableLoadTableBuilder *table_builder = nullptr;
    if (OB_FAIL(table_builder_mgr_.get_table_builder(table_builder))) {
      LOG_WARN("fail to get table builder", KR(ret));
    } else {
      ObDirectLoadDatumRow &delete_datum_row = table_builder->get_delete_datum_row();
      delete_datum_row.seq_no_ = old_row.seq_no_;
      ObTabletID lob_tablet_id;
      if (OB_FAIL(
            (static_cast<ObTableLoadStoreLobTableCtx *>(down_table_op_->op_ctx_->store_table_ctx_))
              ->get_tablet_id(tablet_id, lob_tablet_id))) {
        LOG_WARN("fail to get tablet id", KR(ret));
      }
      common::ObArray<int64_t> &lob_column_idxs =
        up_table_op_->op_ctx_->store_table_ctx_->schema_->lob_column_idxs_;
      for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs.count(); i++) {
        int64_t lob_idx = lob_column_idxs.at(i);
        ObStorageDatum &datum = old_row.storage_datums_[lob_idx];
        const ObLobCommon &lob_common = datum.get_lob_data();
        if (!lob_common.in_row_) {
          const ObLobId &lob_id = reinterpret_cast<const ObLobData *>(lob_common.buffer_)->id_;
          delete_datum_row.storage_datums_[0].set_string(reinterpret_cast<const char *>(&lob_id),
                                                         sizeof(ObLobId));
          if (OB_FAIL(table_builder->append_row(lob_tablet_id, delete_datum_row))) {
            LOG_WARN("fail to append row", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadDataToLobTableChannel::handle_insert_delete_conflict(const ObTabletID &tablet_id,
                                                                     const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

} // namespace observer
} // namespace oceanbase
