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

#define USING_LOG_PREFIX SHARE

#include "ob_vector_index_aux_table_handler.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "sql/das/ob_das_dml_vec_iter.h"

namespace oceanbase
{
namespace share
{

ObVectorIndexDeltaTableHandler::~ObVectorIndexDeltaTableHandler()
{
  ObAccessService *oas = MTL(ObAccessService *);
  if (OB_NOT_NULL(oas)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(delta_table_iter_)) {
      tmp_ret = oas->revert_scan_iter(delta_table_iter_);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN_RET(tmp_ret, "revert delta_table_iter failed");
      }
    }
    delta_table_iter_ = nullptr;
    if (OB_NOT_NULL(index_table_iter_)) {
      tmp_ret = oas->revert_scan_iter(index_table_iter_);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN_RET(tmp_ret, "revert index_table_iter failed");
      }
    }
    index_table_iter_ = nullptr;
  }
}

int ObVectorIndexDeltaTableHandler::init(
    ObPluginVectorIndexAdaptor *adaptor,
    const ObLSID &ls_id, const uint64_t data_table_id,
    const uint64_t inc_index_table_id, const uint64_t vbitmap_table_id,
    const ObTabletID &inc_index_tablet_id, const ObTabletID &vbitmap_tablet_id,
    const share::SCN &target_scn)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *delta_table_schema = nullptr;
  const ObTableSchema *index_table_schema = nullptr;
  ls_id_ = ls_id;
  data_table_id_ = data_table_id;
  inc_index_table_id_ = inc_index_table_id;
  vbitmap_table_id_ = vbitmap_table_id;
  inc_index_tablet_id_ = inc_index_tablet_id;
  vbitmap_tablet_id_ = vbitmap_tablet_id;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, tenant_schema_version_))) {
    LOG_WARN("failed to get schema version", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, inc_index_table_id, delta_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(inc_index_table_id));
  } else if (OB_ISNULL(delta_table_schema) || delta_table_schema->is_in_recyclebin()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index table not exist", K(ret), K(tenant_id_), K(inc_index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, vbitmap_table_id, index_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(vbitmap_table_id));
  } else if (OB_ISNULL(index_table_schema) || index_table_schema->is_in_recyclebin()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index table not exist", K(ret), K(tenant_id_), K(vbitmap_table_id));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                adaptor,
                                target_scn,
                                INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL,
                                allocator_,
                                allocator_,
                                delta_scan_param_,
                                delta_table_param_,
                                delta_table_iter_,
                                &delta_dml_column_ids_,
                                true))) {
    LOG_WARN("failed to read vid id table local tablet.", K(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                      adaptor,
                                      target_scn,
                                      INDEX_TYPE_VEC_INDEX_ID_LOCAL,
                                      allocator_,
                                      allocator_,
                                      index_scan_param_,
                                      index_table_param_,
                                      index_table_iter_,
                                      &index_dml_column_ids_,
                                      true))) {
    LOG_WARN("failed to read data table local tablet.", K(ret));
  } else if (OB_FAIL(inc_table_dml_param_.convert(delta_table_schema, delta_table_schema->get_schema_version(), delta_dml_column_ids_))) {
    LOG_WARN("failed to convert table dml param.", K(ret));
  } else if (OB_FAIL(vbitmap_table_dml_param_.convert(index_table_schema, index_table_schema->get_schema_version(), index_dml_column_ids_))) {
    LOG_WARN("failed to convert table dml param.", K(ret));
  } else {
    inc_table_schema_version_ = delta_table_schema->get_schema_version();
    vbitmap_table_schema_version_ = index_table_schema->get_schema_version();
  }
  return ret;
}

int ObVectorIndexDeltaTableHandler::delete_incr_table_data(
    transaction::ObTxReadSnapshot &snapshot,
    transaction::ObTxDesc *tx_desc, const int64_t timeout,
    const share::SCN& frozen_scn, const ObVectorIndexRoaringBitMap *bitmap)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  storage::ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  if (OB_ISNULL(tx_desc) || OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
  } else {
    ObTableScanIterator *delta_scan_iter = static_cast<ObTableScanIterator *>(delta_table_iter_);
    ObTableScanIterator *index_scan_iter = static_cast<ObTableScanIterator *>(index_table_iter_);
    if (OB_ISNULL(delta_scan_iter) || OB_ISNULL(index_scan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table scan iter", K(ret));
    } else if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, inc_table_dml_param_, inc_table_schema_version_, timeout))) {
      LOG_WARN("prepare dml param fail", K(ret));
    // } else if (OB_FAIL(delete_tablet_data(inc_index_tablet_id_, dml_param, tx_desc, delta_scan_iter, delta_dml_column_ids_))) {
    //   LOG_WARN("failed to delete delta table data", K(ret));
    } else if (FALSE_IT(dml_param.schema_version_ = vbitmap_table_schema_version_)) {
    } else if (FALSE_IT(dml_param.table_param_ = &vbitmap_table_dml_param_)) {
    } else if (OB_FAIL(delete_tablet_data(vbitmap_tablet_id_, dml_param, tx_desc, index_scan_iter, index_dml_column_ids_, frozen_scn, bitmap))) {
      LOG_WARN("failed to delete index table data", K(ret));
    }
  }
  return ret;
}

int ObVectorIndexDeltaTableHandler::prepare_dml_param(
    ObDMLBaseParam &dml_param, storage::ObStoreCtxGuard &store_ctx_guard,
    transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    share::schema::ObTableDMLParam &table_dml_param, const int64_t schema_version,
    const uint64_t timeout)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  if (schema_version <= 0 || ! snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_version), K(snapshot.is_valid()));
  } else if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed", K(ret), KP(oas));
  } else if (OB_ISNULL(tx_desc)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null", K(ret), KP(tx_desc));
  } else {
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.write_flag_.reset();
    dml_param.write_flag_.set_is_insert_up();
    dml_param.table_param_ = &table_dml_param;
    dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
    dml_param.timeout_ = timeout;
    dml_param.branch_id_ = 0;
    dml_param.store_ctx_guard_ = &store_ctx_guard;
    dml_param.schema_version_ = schema_version;
    dml_param.tenant_schema_version_ = tenant_schema_version_;
    dml_param.dml_allocator_ = &allocator_;
    if (OB_FAIL(dml_param.snapshot_.assign(snapshot))) {
      LOG_WARN("assign snapshot fail", K(ret), K(snapshot));
    } else if (OB_FAIL(oas->get_write_store_ctx_guard(ls_id_, timeout, *tx_desc, snapshot, 0, dml_param.write_flag_, store_ctx_guard))) {
      LOG_WARN("failed to get write store context guard", K(ret));
    }
  }
  return ret;
}

int ObVectorIndexDeltaTableHandler::delete_tablet_data(
    ObTabletID& tablet_id,
    ObDMLBaseParam &dml_param,
    transaction::ObTxDesc *tx_desc,
    ObTableScanIterator *table_scan_iter,
    ObIArray<uint64_t> &dml_column_ids,
    const share::SCN& frozen_scn,
    const ObVectorIndexRoaringBitMap *bitmap)
{
  static const int BATCH_CNT = 2000; // 8M / 4(sizeof(float)) / 1000(dim)
  int ret = OB_SUCCESS;
  int64_t loop_cnt = 0;
  bool delete_unfinish = true;
  int64_t delta_table_affected_rows = 0;
  int64_t skip_rows = 0;
  storage::ObValueRowIterator row_iter;
  ObAccessService *oas = MTL(ObAccessService *);
  const roaring::api::roaring64_bitmap_t *insert_bitmap = nullptr == bitmap ? nullptr : bitmap->insert_bitmap_;
  const roaring::api::roaring64_bitmap_t *delete_bitmap = nullptr == bitmap ? nullptr : bitmap->delete_bitmap_;
  int64_t read_num = 0;
  SCN read_scn = SCN::min_scn();
  int64_t vid = 0;
  ObString op;
  if (OB_ISNULL(tx_desc) || OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
  } else if (OB_ISNULL(table_scan_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table scan iter", K(ret));
  }
  while (OB_SUCC(ret) && delete_unfinish) {
    int cur_row_count = 0;
    if (OB_FAIL(row_iter.init())) {
      LOG_WARN("fail to init row iter", K(ret));
    }
    while (OB_SUCC(ret) && cur_row_count <= BATCH_CNT) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row invalid.", K(ret));
      } else if (OB_FALSE_IT(read_num = datum_row->storage_datums_[0].get_int())) {
        LOG_WARN("failed to get read scn.", K(ret));
      } else if (OB_FAIL(read_scn.convert_for_gts(read_num))) {
        LOG_WARN("failed to convert from ts.", K(ret), K(read_num));
      } else if (OB_FALSE_IT(vid = datum_row->storage_datums_[1].get_int())) {
        LOG_WARN("failed to get vid.", K(ret));
      } else if (OB_FALSE_IT(op = datum_row->storage_datums_[2].get_string())) {
        LOG_WARN("failed to get op.", K(ret));
      } else if (op.length() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid op length.", K(ret), K(op));
      } else if (read_scn > frozen_scn) {
        ++skip_rows;
        LOG_INFO("skip delete not include record", K(read_scn), K(frozen_scn), K(read_num), K(vid), K(op), KPC(datum_row));
      } else if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_DELETE[0]
          && (OB_ISNULL(delete_bitmap) || ! roaring::api::roaring64_bitmap_contains(delete_bitmap, vid))) {
        LOG_WARN("delete uninclude D record", K(vid), K(op), KPC(datum_row));
      } else if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_INSERT[0]
          && (OB_ISNULL(insert_bitmap) || ! roaring::api::roaring64_bitmap_contains(insert_bitmap, vid))
          && (OB_ISNULL(delete_bitmap) || ! roaring::api::roaring64_bitmap_contains(delete_bitmap, vid))) {
        LOG_WARN("skip delete uninclude I record", K(vid), K(op), KPC(datum_row));
      } else if (OB_FAIL(row_iter.add_row(*datum_row))) {
        LOG_WARN("failed to add row to iter", K(ret));
      } else {
        cur_row_count += 1;
        LOG_TRACE("delete record ", KPC(datum_row));
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
    } else if (cur_row_count == 0) {
      delete_unfinish = false;
    } else if (OB_FAIL(oas->delete_rows(ls_id_, tablet_id, *tx_desc, dml_param, dml_column_ids, &row_iter, delta_table_affected_rows))) {
      LOG_WARN("failed to delete rows from delta table", K(ret), K(tablet_id));
    } else {
      LOG_INFO("delete rows success", K(tx_desc->get_tx_id()), K(ls_id_), K(tablet_id), K(cur_row_count), K(delta_table_affected_rows), K(skip_rows));
    }
    delta_table_affected_rows = 0;
    row_iter.reset();
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::init(
    const ObLSID &ls_id,
    const uint64_t data_table_id,
    const uint64_t snapshot_table_id,
    const ObTabletID &snapshot_tablet_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *snapshot_table_schema = nullptr;
  ls_id_ = ls_id;
  data_table_id_ = data_table_id;
  snapshot_table_id_ = snapshot_table_id;
  snapshot_tablet_id_ = snapshot_tablet_id;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, tenant_schema_version_))) {
    LOG_WARN("failed to get schema version", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, data_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(data_table_id), K(snapshot_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, snapshot_table_id, snapshot_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema) || data_table_schema->is_in_recyclebin()
          || OB_ISNULL(snapshot_table_schema) || snapshot_table_schema->is_in_recyclebin()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema not exist", K(ret), K(data_table_id), K(snapshot_table_id),
      KPC(snapshot_table_schema), KPC(data_table_schema));
  } else if (OB_FAIL(prepare_snapshot_table_column_info(*data_table_schema, *snapshot_table_schema,
      all_column_ids_, dml_column_ids_, extra_column_idxs_, data_col_cs_type_, vector_key_col_idx_,
      vector_data_col_idx_, vector_vid_col_idx_, vector_col_idx_, vector_visible_col_idx_,
      key_col_id_, data_col_id_, visible_col_id_))) {
    LOG_WARN("prepare_snapshot_table_column_info fail", K(ret), KPC(snapshot_table_schema), KPC(data_table_schema));
  } else if (OB_FAIL(table_dml_param_.convert(snapshot_table_schema, snapshot_table_schema->get_schema_version(), dml_column_ids_))) {
    LOG_WARN("failed to convert table dml param.", K(ret));
  } else {
    schema_version_ = snapshot_table_schema->get_schema_version();
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::prepare_snapshot_table_column_info(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &snapshot_table_schema,
    ObIArray<uint64_t> &all_column_ids,
    ObIArray<uint64_t> &dml_column_ids,
    ObIArray<uint64_t> &extra_column_idxs,
    ObCollationType &data_col_cs_type,
    int64_t &vector_key_col_idx,
    int64_t &vector_data_col_idx,
    int64_t &vector_vid_col_idx,
    int64_t &vector_col_idx,
    int64_t &vector_visible_col_idx,
    int64_t &key_col_id,
    int64_t &data_col_id,
    int64_t &visible_col_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(snapshot_table_schema.get_all_column_ids(all_column_ids))) {
    LOG_WARN("fail to get all column ids", K(ret), K(data_table_schema));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < all_column_ids.count(); i++) {
    const ObColumnSchemaV2 *column_schema;
    if (OB_ISNULL(column_schema = data_table_schema.get_column_schema(all_column_ids.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column schema", K(ret), K(all_column_ids.at(i)));
    } else if (column_schema->is_vec_hnsw_vid_column()) {
      vector_vid_col_idx = i;
      if (!column_schema->is_nullable()) {
        ObString index_name;
        if (OB_FAIL(snapshot_table_schema.get_index_name(index_name))) {
          LOG_WARN("failed to get index name", K(ret));
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_INFO("vector index created before 4.3.5.2 do not support vector index optimize task, please rebuild vector index.", K(ret), K(index_name));
        }
      } else if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_hidden_pk_column_id(all_column_ids.at(i))) {
      vector_vid_col_idx = i;
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_vector_column()) {
      vector_col_idx = i;
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_key_column()) {
      vector_key_col_idx = i;
      key_col_id = all_column_ids.at(i);
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_data_column()) {
      vector_data_col_idx = i;
      data_col_id = all_column_ids.at(i);
      data_col_cs_type = column_schema->get_collation_type();
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_visible_column()) {
      vector_visible_col_idx = i;
      visible_col_id = all_column_ids.at(i);
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else { // set extra column id
      if (OB_FAIL(extra_column_idxs.push_back(i))) {
        LOG_WARN("failed to push back extra column idx", K(ret), K(i));
      } else if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    }
  } // end for.
  if (OB_SUCC(ret)) {
    if (vector_vid_col_idx == -1 || vector_col_idx == -1 || vector_key_col_idx == -1 || vector_data_col_idx == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get valid vector index col idx", K(ret), K(vector_col_idx), K(vector_vid_col_idx),
              K(vector_key_col_idx), K(vector_data_col_idx), K(all_column_ids));
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::prepare_insert_iter(
    ObIArray<ObVecIdxSnapshotBlockData> &data_blocks, const int64_t snapshot_version,
    const ObVectorIndexAlgorithmType index_type, storage::ObValueRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(blocksstable::ObDatumRow, datum_row, tenant_id_) {
    const int64_t new_snapshot_column_cnt = snapshot_column_count_ + extra_column_idxs_.count();
    if (OB_FAIL(datum_row.init(new_snapshot_column_cnt))) {
      LOG_WARN("init datum row fail", K(ret), K(new_snapshot_column_cnt), K(snapshot_column_count_), K(extra_column_idxs_.count()));
    } else if (OB_FAIL(row_iter.init())) {
      LOG_WARN("fail to init row iter", K(ret));
    }
    for (int64_t row_id = 0; row_id < data_blocks.count() && OB_SUCC(ret); row_id++) {
      ObString key;
      const ObVecIdxSnapshotBlockData& block = data_blocks.at(row_id);
      if (block.is_meta_block()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("block should not be meta block", K(ret), K(block), K(row_id));
      } else if (OB_FAIL(ObVectorIndexSegmentMeta::get_segment_persist_key(index_type, snapshot_tablet_id_, snapshot_version, row_id, allocator_, key))) {
        LOG_WARN("generate segment persist key fail", K(ret), K(snapshot_tablet_id_), K(snapshot_version));
      } else if (key.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr key_str", K(ret), KP(key.ptr()), K(key.length()));
      } else {
        datum_row.storage_datums_[vector_key_col_idx_].set_string(key);
        datum_row.storage_datums_[vector_data_col_idx_].set_string(block.get_data());
        datum_row.storage_datums_[vector_data_col_idx_].set_has_lob_header();
        datum_row.storage_datums_[vector_vid_col_idx_].set_null();
        datum_row.storage_datums_[vector_col_idx_].set_null();
        datum_row.storage_datums_[vector_visible_col_idx_].set_true();
        // set extra column default value
        if (extra_column_idxs_.count() > 0) {
          for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs_.count(); i++) {
            if (extra_column_idxs_.at(i) == vector_key_col_idx_ ||
                extra_column_idxs_.at(i) == vector_data_col_idx_ ||
                extra_column_idxs_.at(i) == vector_vid_col_idx_ ||
                extra_column_idxs_.at(i) == vector_col_idx_ ||
                extra_column_idxs_.at(i) == vector_visible_col_idx_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected extra column idx", K(i), K(extra_column_idxs_.at(i)),
                  K_(vector_key_col_idx), K_(vector_data_col_idx), K_(vector_vid_col_idx), K_(vector_col_idx), K_(vector_visible_col_idx));
            } else {
              datum_row.storage_datums_[extra_column_idxs_.at(i)].set_null();
            }
          }
        }
        LOG_DEBUG("[VECTOR INDEX SNAP] print snap table column ids", K(ret),
            K_(vector_key_col_idx), K_(vector_data_col_idx), K_(vector_vid_col_idx),
            K_(vector_col_idx), K_(vector_visible_col_idx), K_(extra_column_idxs));
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(row_iter.add_row(datum_row))) {
          LOG_WARN("failed to add row to iter", K(ret));
        }
        datum_row.reuse();
      }
    } // end for
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::prepare_dml_param(
    ObDMLBaseParam &dml_param, storage::ObStoreCtxGuard &store_ctx_guard,
    transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    const uint64_t timeout)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  if (schema_version_ <= 0 || ! snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_version_), K(snapshot.is_valid()));
  } else if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed", K(ret), KP(oas));
  } else if (OB_ISNULL(tx_desc)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null", K(ret), KP(tx_desc));
  } else {
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.write_flag_.reset();
    dml_param.write_flag_.set_is_insert_up();
    dml_param.table_param_ = &table_dml_param_;
    dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
    dml_param.timeout_ = timeout;
    dml_param.branch_id_ = 0;
    dml_param.store_ctx_guard_ = &store_ctx_guard;
    dml_param.schema_version_ = schema_version_;
    dml_param.tenant_schema_version_ = tenant_schema_version_;
    dml_param.dml_allocator_ = &allocator_;
    if (OB_FAIL(dml_param.snapshot_.assign(snapshot))) {
      LOG_WARN("assign snapshot fail", K(ret), K(snapshot));
    } else if (OB_FAIL(oas->get_write_store_ctx_guard(ls_id_, timeout, *tx_desc, snapshot, 0, dml_param.write_flag_, store_ctx_guard))) {
      LOG_WARN("failed to get write store context guard", K(ret));
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::do_insert(
    storage::ObValueRowIterator &row_iter, transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObAccessService *oas = MTL(ObAccessService *);
  storage::ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, timeout))) {
    LOG_WARN("assign snapshot fail", K(ret), K(snapshot));
  } else if (OB_FAIL(oas->insert_rows(ls_id_, snapshot_tablet_id_, *tx_desc, dml_param, dml_column_ids_, &row_iter, affected_rows))) {
    LOG_WARN("failed to insert rows to snapshot table", K(ret), K(ls_id_), K(snapshot_tablet_id_), K(dml_param));
  } else {
    LOG_INFO("insert rows success", K(ls_id_), K(snapshot_tablet_id_), K(affected_rows), KPC(tx_desc));
  }

  store_ctx_guard.reset();
  row_iter.reset();
  return ret;
}

int ObVectorIndexSnapTableHandler::insertup_meta_row(
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  const int64_t retry_interval_us = 1000l * 1000l; // 1s
  if (OB_ISNULL(txs) || OB_ISNULL(tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(txs), KP(tx_desc));
  } else {
    int64_t retry_cnt = 0;
    bool need_retry = false;
    oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_LOCAL_RETRY);
    do {
      transaction::ObTxReadSnapshot snapshot;
      if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id_, timeout_ts, snapshot))) {
        LOG_WARN("fail to refresh snapshot during retry", K(ret));
      } else if (OB_FAIL(inner_insertup_meta_row(adaptor, tx_desc, snapshot, timeout_ts))) {
        LOG_WARN("add segment to meta fail, check need retry", KR(ret), K(retry_cnt));
        need_retry = false;
        if (IS_INTERRUPTED()) {
          LOG_INFO("[VECTOR INDEX RETRY] Retry is interrupted by worker interrupt signal", KR(ret), K(retry_cnt));
        } else if (lib::Worker::WS_OUT_OF_THROTTLE == THIS_WORKER.check_wait()) {
          ret = OB_KILLED_BY_THROTTLING;
          LOG_INFO("[VECTOR INDEX RETRY] Retry is interrupted by worker check wait", KR(ret), K(retry_cnt));
        } else if (retry_cnt >= ObVectorIndexSnapTableHandler::MAX_RETRY_COUNT) {
          LOG_WARN("max retry count reached", K(retry_cnt));
        } else if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
          need_retry = true;
          ++retry_cnt;
          LOG_INFO("[VECTOR INDEX RETRY] lock conflict, retry again", K(ret), K(retry_cnt));
        } else if (OB_TRANSACTION_SET_VIOLATION == ret) {
          need_retry = true;
          ++retry_cnt;
          LOG_INFO("[VECTOR INDEX RETRY] transaction set changed during the execution, retry again", K(ret), K(retry_cnt));
        } else if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          need_retry = true;
          ++retry_cnt;
          LOG_INFO("[VECTOR INDEX RETRY] insert conflict, retry again", K(ret), K(retry_cnt));
        } else {
          // 其他错误，不重试
          LOG_WARN("fail to add segment to meta, no retry", K(ret));
        }
      } else {
        need_retry = false;
      }
      if (need_retry) {
        ++retry_cnt;
        LOG_INFO("[VECTOR INDEX RETRY] retry again", K(ret), K(retry_cnt));
        ob_usleep(retry_interval_us);
      }
    } while (need_retry);
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::inner_insertup_meta_row(
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  common::ObNewRowIterator *snap_data_iter = nullptr;
  ObVectorIndexMeta old_meta;
  ObVectorIndexMeta new_meta;
  ObString new_meta_data;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "VISnapSegOp"));
  const int64_t snapshot_version = snapshot.version().get_val_for_sql();
  blocksstable::ObDatumRow old_row;
  blocksstable::ObDatumRow new_row;
  storage::ObTableScanParam snap_scan_param;
  schema::ObTableParam snap_table_param(allocator);
  bool row_exist = false;
  if (OB_ISNULL(adaptor) || OB_ISNULL(tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(adaptor), KP(tx_desc));
  } else if (OB_FAIL(old_row.init(get_dml_column_cnt()))) {
    LOG_WARN("fail to init old tmp row", K(ret), K(old_row));
  } else if (OB_FAIL(new_row.init(get_dml_column_cnt()))) {
    LOG_WARN("fail to init new tmp row", K(ret), K(old_row));
  } else if (OB_FAIL(scan_and_lock_meta_row(adaptor, tx_desc, snapshot,
      snap_data_iter, snap_table_param, snap_scan_param, timeout_ts, old_row, row_exist))) {
    LOG_WARN("fail to scan snapshot meta row", K(ret));
  } else if (! row_exist) {
    if (OB_FAIL(prepare_meta_for_insert(new_meta, snapshot_version))) {
      LOG_WARN("init new meta fail", K(ret));
    } else if (OB_FAIL(new_meta.serialize(allocator, new_meta_data))) {
      LOG_WARN("serialize meta fail", K(ret), K(new_meta));
    } else if (OB_FAIL(prepare_insert_meta_row(new_row, new_meta_data))) {
      LOG_WARN("prepare insert meta row fail", K(ret));
    } else if (OB_FAIL(insert_meta_data_row(tx_desc, snapshot, new_row, timeout_ts))) {
      LOG_WARN("do insert fail", K(ret), K(new_row));
    } else {
      LOG_INFO("insert meta row success", K(new_meta));
    }
  } else {
    if (OB_FAIL(read_meta_from_row(allocator, old_row, old_meta))) {
      LOG_WARN("fail to read old meta", K(ret));
    } else if (OB_FAIL(prepare_meta_for_update(new_meta, old_meta, snapshot_version))) {
      LOG_WARN("init new meta fail", K(ret));
    } else if (OB_FAIL(new_meta.serialize(allocator, new_meta_data))) {
      LOG_WARN("serialize meta fail", K(ret));
    } else if (OB_FAIL(new_row.deep_copy(old_row, allocator_))) {
      LOG_WARN("fail to copy old_row", K(ret));
    } else if (OB_FALSE_IT(new_row.storage_datums_[get_vector_data_col_idx()].set_string(new_meta_data))) {
    } else if (OB_FAIL(update_meta_data_row(tx_desc, snapshot, old_row, new_row, timeout_ts))) {
      LOG_WARN("update_meta_data_row fail", K(ret));
    } else {
      LOG_INFO("update meta row success", K(new_meta), K(old_meta));
    }
  }
  if (OB_NOT_NULL(oas)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snap_data_iter)) {
      tmp_ret = oas->revert_scan_iter(snap_data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    snap_data_iter = nullptr;
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::insert_segment_data(
    ObIArray<ObVecIdxSnapshotBlockData> &data_blocks, transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    const int64_t snapshot_version, const ObVectorIndexAlgorithmType index_type, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  storage::ObValueRowIterator row_iter;
  if (OB_FAIL(prepare_insert_iter(data_blocks, snapshot_version, index_type, row_iter))) {
    LOG_WARN("prepare_insert_iter fail", K(ret));
  } else if (OB_FAIL(do_insert(row_iter, tx_desc, snapshot, timeout))) {
    LOG_WARN("do insert fail", K(ret), K(row_iter));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::prepare_insert_meta_row(blocksstable::ObDatumRow &row, const ObString &meta_data)
{
  int ret = OB_SUCCESS;
  ObString key;
  if (OB_FAIL(ObVectorIndexSegmentMeta::get_vector_index_meta_key(allocator_, snapshot_tablet_id_, key))) {
    LOG_WARN("fail to build vec snapshot key str", K(ret));
  } else {
    row.storage_datums_[vector_key_col_idx_].set_string(key);
    row.storage_datums_[vector_data_col_idx_].set_string(meta_data);
    row.storage_datums_[vector_data_col_idx_].set_has_lob_header();
    row.storage_datums_[vector_vid_col_idx_].set_null();
    row.storage_datums_[vector_col_idx_].set_null();
    row.storage_datums_[vector_visible_col_idx_].set_true();

    // set extra column default value
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs_.count(); i++) {
      if (extra_column_idxs_.at(i) == vector_key_col_idx_ ||
          extra_column_idxs_.at(i) == vector_data_col_idx_ ||
          extra_column_idxs_.at(i) == vector_vid_col_idx_ ||
          extra_column_idxs_.at(i) == vector_col_idx_ ||
          extra_column_idxs_.at(i) == vector_visible_col_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected extra column idx", K(i), K(extra_column_idxs_.at(i)),
          K_(vector_key_col_idx), K_(vector_data_col_idx),
          K_(vector_vid_col_idx), K_(vector_col_idx), K_(vector_visible_col_idx));
      } else {
        row.storage_datums_[extra_column_idxs_.at(i)].set_null();
      }
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::insert_meta_data_row(
    transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    blocksstable::ObDatumRow &new_row, const uint64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  storage::ObValueRowIterator row_iter;
  ObAccessService *oas = MTL(ObAccessService*);
  storage::ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  if (OB_FAIL(row_iter.init())) {
    LOG_WARN("failed to init row iter", K(ret));
  } else if (OB_FAIL(row_iter.add_row(new_row))) {
    LOG_WARN("failed to add row to iter", K(ret));
  } else if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, timeout))) {
    LOG_WARN("assign snapshot fail", K(ret), K(snapshot));
  } else if (OB_FAIL(oas->insert_rows(ls_id_, snapshot_tablet_id_,
      *tx_desc, dml_param, dml_column_ids_, &row_iter, affected_rows))) {
    LOG_WARN("fail to update_rows", K(ret), K(dml_param));
  } else {
    LOG_INFO("print insert rows count", K(affected_rows));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::construct_old_row(blocksstable::ObDatumRow *input, blocksstable::ObDatumRow &output)
{
  int ret = OB_SUCCESS;
  const int64_t in_key_col_idx = 0;
  const int64_t in_data_col_idx = 1;
  const int64_t in_visible_col_idx = 2;
  if (OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(input));
  } else {
    output.storage_datums_[vector_key_col_idx_].set_string(input->storage_datums_[in_key_col_idx].get_string());
    output.storage_datums_[vector_data_col_idx_].set_string(input->storage_datums_[in_data_col_idx].get_string());
    output.storage_datums_[vector_data_col_idx_].set_has_lob_header();
    output.storage_datums_[vector_vid_col_idx_].set_null();
    output.storage_datums_[vector_col_idx_].set_null();
    output.storage_datums_[vector_visible_col_idx_].shallow_copy_from_datum(input->storage_datums_[in_visible_col_idx]);
    LOG_INFO("construct old row", K(output), KPC(input));
    // set extra column default value
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs_.count(); i++) {
      if (extra_column_idxs_.at(i) == vector_key_col_idx_ ||
          extra_column_idxs_.at(i) == vector_data_col_idx_ ||
          extra_column_idxs_.at(i) == vector_vid_col_idx_ ||
          extra_column_idxs_.at(i) == vector_col_idx_ ||
          extra_column_idxs_.at(i) == vector_visible_col_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected extra column idx", K(i), K(extra_column_idxs_.at(i)),
          K_(vector_key_col_idx), K_(vector_data_col_idx),
          K_(vector_vid_col_idx), K_(vector_col_idx), K_(vector_visible_col_idx));
      } else {
        output.storage_datums_[extra_column_idxs_.at(i)].set_null();
      }
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::update_meta_data_row(
    transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    blocksstable::ObDatumRow &old_row, blocksstable::ObDatumRow &new_row, const uint64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObVecIndexATaskUpdIterator upd_row_iter;
  ObAccessService *oas = MTL(ObAccessService*);
  ObDMLBaseParam dml_param;
  storage::ObStoreCtxGuard store_ctx_guard;
  ObSEArray<uint64_t, 1> upd_column_ids;
  if (OB_FAIL(upd_row_iter.init())) {
    LOG_WARN("failed to init row iter", K(ret));
  } else if (OB_FAIL(upd_row_iter.add_row(old_row, new_row))) {
    LOG_WARN("failed to add row to iter", K(ret));
  } else if (OB_FAIL(upd_column_ids.push_back(data_col_id_))) {
    LOG_WARN("fail to push back update column id", K(ret), K(data_col_id_));
  } else if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, timeout))) {
    LOG_WARN("fail to prepare lob meta dml", K(ret));
  } else if (OB_FAIL(oas->update_rows(ls_id_, snapshot_tablet_id_,
      *tx_desc, dml_param, dml_column_ids_, upd_column_ids, &upd_row_iter, affected_rows))) {
    LOG_WARN("fail to update_rows", K(ret), K(dml_column_ids_), K(dml_param));
  } else {
    LOG_INFO("print update rows count", K(affected_rows));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::read_meta_from_row(
    ObIAllocator &allocator,
    const blocksstable::ObDatumRow &row,
    ObVectorIndexMeta &meta)
{
  int ret = OB_SUCCESS;
  ObString meta_data = row.storage_datums_[vector_data_col_idx_].get_string();
  int64_t pos = 0;
  if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, ObLongTextType, true, meta_data, nullptr))) {
    LOG_WARN("read real string data fail for old meta", K(ret), K(meta_data.length()));
  } else if (OB_FAIL(meta.deserialize(meta_data.ptr(), meta_data.length(), pos))) {
    LOG_WARN("fail to deserialize old meta", K(ret), K(meta_data.length()), K(pos));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::lock_row(
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    blocksstable::ObDatumRow &row,
    const int64_t timeout_ts,
    const int64_t abs_lock_timeout)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  storage::ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  if (OB_ISNULL(tx_desc) || OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or access service, get nullptr", K(ret), KP(tx_desc), KP(oas));
  } else if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, timeout_ts))) {
    LOG_WARN("prepare dml param fail", K(ret));
  } else if (OB_FAIL(oas->lock_row(ls_id_ , snapshot_tablet_id_, *tx_desc, dml_param, abs_lock_timeout, row, LF_WRITE))) {
    LOG_WARN("failed to lock row", K(ret), K(ls_id_), K(snapshot_tablet_id_));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::scan_and_lock_meta_row(
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    ObNewRowIterator *&snap_data_iter,
    schema::ObTableParam &snap_table_param,
    storage::ObTableScanParam &snap_scan_param,
    const uint64_t timeout_ts,
    blocksstable::ObDatumRow &old_row,
    bool &row_exist)
{
  int ret = OB_SUCCESS;
  ObTableScanIterator *table_scan_iter = nullptr;
  blocksstable::ObDatumRow *datum_row = nullptr;
  row_exist = false;
  if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
      adaptor,
      snapshot.version(),
      INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
      allocator_,
      allocator_,
      snap_scan_param,
      snap_table_param,
      snap_data_iter))) {
    LOG_WARN("fail to read data table local tablet.", K(ret));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan iter is null", K(ret), KP(snap_data_iter));
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get old meta row fail", K(ret));
    } else {
      ret = OB_SUCCESS;
      row_exist = false;
      LOG_INFO("there is no vector index meta row");
    }
  } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (datum_row->get_column_count() < 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (!datum_row->storage_datums_[vector_key_col_idx_].get_string().suffix_match("_meta_data")) {
    ret = OB_SUCCESS;
    row_exist = false;
    LOG_INFO("there is no vector index meta row", KPC(datum_row));
  } else if (OB_FAIL(construct_old_row(datum_row, old_row))) {
    LOG_WARN("construct vector row fail", K(ret));
  } else if (OB_FAIL(lock_row(tx_desc, snapshot, old_row, timeout_ts, timeout_ts))) {
    LOG_WARN("fail to lock row", K(ret));
  } else {
    row_exist = true;
    LOG_INFO("get and lock meta row success", K(old_row), KPC(datum_row));
  }
  return ret;
}

int ObVecIdxSnapTableSegAddOp::preprea_meta(ObVectorIndexSegmentMeta &seg_meta, ObVectorIndexMeta &fake_old_meta)
{
  int ret = OB_SUCCESS;
  new_seg_meta_ = &seg_meta;
  fake_old_meta_ = &fake_old_meta;
  return ret;
}

int ObVecIdxSnapTableSegAddOp::prepare_meta_for_insert(ObVectorIndexMeta &new_meta, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_seg_meta_) || OB_ISNULL(fake_old_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta is null", KP(new_seg_meta_), KP(fake_old_meta_));
  } else if (!fake_old_meta_->is_persistent_ && fake_old_meta_->bases_.count() > 0) {
    if (fake_old_meta_->bases_.count() > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected bases count", K(fake_old_meta_->bases_.count()), KPC(fake_old_meta_));
    } else if (OB_FAIL(new_meta.bases_.push_back(fake_old_meta_->bases_.at(0)))) {
      LOG_WARN("push back seg meta fail", K(ret));
    } else {
      FLOG_INFO("merge old version segment meta", KPC(fake_old_meta_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_meta.incrs_.push_back(*new_seg_meta_))) {
    LOG_WARN("push back seg meta fail", K(ret));
  } else {
    new_meta.is_persistent_ = true;
    new_meta.header_.scn_ = snapshot_version;
    LOG_INFO("prepare meta for insert success", K(new_meta));
  }
  return ret;
}

int ObVecIdxSnapTableSegAddOp::prepare_meta_for_update(ObVectorIndexMeta &new_meta, const ObVectorIndexMeta &old_meta, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_seg_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new seg meta is null", KPC(new_seg_meta_));
  } else if (OB_FAIL(new_meta.assign(old_meta))) {
    LOG_WARN("copy meta fail", K(ret), K(old_meta));
  } else if (OB_FAIL(new_meta.incrs_.push_back(*new_seg_meta_))) {
    LOG_WARN("push back seg meta fail", K(ret));
  } else {
    new_meta.header_.scn_ = snapshot_version;
    new_meta.is_persistent_ = true;
    LOG_INFO("prepare meta for update success", K(new_meta), K(old_meta));
  }
  return ret;
}

int ObVecIdxSnapTableSegMergeOp::preprea_meta(ObVectorIndexSegmentMeta *seg_meta, ObIArray<const ObVectorIndexSegmentMeta *> &merge_segments)
{
  int ret = OB_SUCCESS;
  new_seg_meta_ = seg_meta;
  merge_segments_ = &merge_segments;
  return ret;
}

int ObVecIdxSnapTableSegMergeOp::prepare_meta_for_insert(ObVectorIndexMeta &new_meta, const int64_t snapshot_version)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("not supported", K(ret));
  return ret;
}

int ObVecIdxSnapTableSegMergeOp::prepare_meta_for_update(ObVectorIndexMeta &new_meta, const ObVectorIndexMeta &old_meta, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  int64_t real_delete_cnt = 0;
  new_meta.header_.scn_ = snapshot_version;
  new_meta.is_persistent_ = true;
  new_meta.bases_.reset();
  new_meta.incrs_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.bases_.count(); ++i) {
    const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.bases_.at(i);
    bool should_delete = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < merge_segments_->count() && !should_delete; ++j) {
      if (0 == merge_segments_->at(j)->start_key_.compare(old_seg_meta.start_key_)) {
        should_delete = true;
        ++real_delete_cnt;
      }
    }
    if (!should_delete && OB_FAIL(new_meta.bases_.push_back(old_seg_meta))) {
      LOG_WARN("push back seg meta to bases fail", K(ret), K(old_seg_meta), K(new_meta));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.incrs_.count(); ++i) {
    const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.incrs_.at(i);
    bool should_delete = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < merge_segments_->count() && !should_delete; ++j) {
      if (0 == merge_segments_->at(j)->start_key_.compare(old_seg_meta.start_key_)) {
        should_delete = true;
        ++real_delete_cnt;
      }
    }
    if (!should_delete && OB_FAIL(new_meta.incrs_.push_back(old_seg_meta))) {
      LOG_WARN("push back seg meta to incrs fail", K(ret), K(old_seg_meta), K(new_meta));
    }
  }
  if (OB_SUCC(ret)) {
    if (real_delete_cnt != merge_segments_->count()) {
      ret =  OB_CANCELED;
      LOG_WARN("may be conflict with other task, need cancal", K(old_meta));
    } else if (OB_ISNULL(new_seg_meta_)) {
      LOG_INFO("new_seg_meta is empty, so skip push", KPC_(new_seg_meta), K(new_meta));
    } else if (ObVectorIndexSegmentType::BASE == new_seg_meta_->seg_type_) {
      if (OB_FAIL(new_meta.bases_.push_back(*new_seg_meta_))) {
        LOG_WARN("push back new seg meta to bases fail", K(ret), KPC_(new_seg_meta), K(new_meta));
      }
    } else {
      if (OB_FAIL(new_meta.incrs_.push_back(*new_seg_meta_))) {
        LOG_WARN("push back new seg meta to incrs fail", K(ret), KPC_(new_seg_meta), K(new_meta));
      }
    }
  }
  if (OB_SUCC(ret) && !new_meta.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new meta is not valid after merge", K(ret), K(new_meta), K(old_meta));
  } else {
    LOG_INFO("prepare meta for merge success", K(new_meta), K(old_meta));
  }
  return ret;
}

int ObVecIdxSnapTableSegReplaceOp::preprea_meta(ObVectorIndexMeta &meta)
{
  int ret = OB_SUCCESS;
  new_meta_ = &meta;
  return ret;
}

int ObVecIdxSnapTableSegReplaceOp::prepare_meta_for_insert(ObVectorIndexMeta &new_meta, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new meta is null", KPC(new_meta_));
  } else if (OB_FAIL(new_meta.assign(*new_meta_))) {
    LOG_WARN("push back seg meta fail", K(ret));
  } else {
    new_meta.header_.scn_ = snapshot_version;
    new_meta.is_persistent_ = true;
    LOG_INFO("prepare meta for replace success", K(new_meta));
  }
  return ret;
}

int ObVecIdxSnapTableSegReplaceOp::prepare_meta_for_update(ObVectorIndexMeta &new_meta, const ObVectorIndexMeta &old_meta, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new meta is null", KPC(new_meta_));
  } else if (OB_FAIL(new_meta.assign(*new_meta_))) {
    LOG_WARN("push back seg meta fail", K(ret));
  } else {
    new_meta.header_.scn_ = snapshot_version;
    new_meta.is_persistent_ = true;
    LOG_INFO("prepare meta for replace success", K(new_meta));
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
