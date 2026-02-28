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
#ifndef OCEANBASE_SHARE_VECTOR_INDEX_AUX_TABLE_HANDLER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_AUX_TABLE_HANDLER_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/ob_value_row_iterator.h"

namespace oceanbase
{
namespace share
{
class ObPluginVectorIndexAdaptor;

class ObVectorIndexDeltaTableHandler
{
public:
  ObVectorIndexDeltaTableHandler(const uint64_t tenant_id):
    allocator_(ObMemAttr(tenant_id, "VIDelta")),
    tenant_id_(tenant_id), tenant_schema_version_(-1),
    inc_table_schema_version_(-1), vbitmap_table_schema_version_(-1), ls_id_(),
    data_table_id_(0), inc_index_table_id_(0), vbitmap_table_id_(0),
    inc_index_tablet_id_(0), vbitmap_tablet_id_(),
    delta_table_param_(allocator_), index_table_param_(allocator_),
    delta_table_iter_(nullptr), index_table_iter_(nullptr),
    inc_table_dml_param_(allocator_), vbitmap_table_dml_param_(allocator_)
  {}
  ~ObVectorIndexDeltaTableHandler();

public:
  int init(ObPluginVectorIndexAdaptor *adaptor,
      const ObLSID &ls_id, const uint64_t data_table_id,
      const uint64_t inc_index_table_id, const uint64_t vbitmap_table_id,
      const ObTabletID &inc_index_tablet_id, const ObTabletID &vbitmap_tablet_id,
      const share::SCN &target_scn);
  int delete_incr_table_data(transaction::ObTxReadSnapshot &snapshot,
      transaction::ObTxDesc *tx_desc, const int64_t timeout,
      const share::SCN& frozen_scn, const ObVectorIndexRoaringBitMap *bitmap);

private:
  int prepare_dml_param(
      ObDMLBaseParam &dml_param, storage::ObStoreCtxGuard &store_ctx_guard,
      transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
      share::schema::ObTableDMLParam &table_dml_param, const int64_t schema_version,
      const uint64_t timeout);
  int delete_tablet_data(
      ObTabletID& tablet_id,
      ObDMLBaseParam &dml_param,
      transaction::ObTxDesc *tx_desc,
      ObTableScanIterator *table_scan_iter,
      ObIArray<uint64_t> &dml_column_ids,
      const share::SCN& frozen_scn,
      const ObVectorIndexRoaringBitMap *bitmap);

private:
  ObArenaAllocator allocator_;
  uint64_t tenant_id_;
  int64_t tenant_schema_version_;
  int64_t inc_table_schema_version_;
  int64_t vbitmap_table_schema_version_;
  ObLSID ls_id_;
  uint64_t data_table_id_;
  uint64_t inc_index_table_id_;
  uint64_t vbitmap_table_id_;
  ObTabletID inc_index_tablet_id_;
  ObTabletID vbitmap_tablet_id_;

  ObSEArray<uint64_t, 4> delta_dml_column_ids_;
  ObSEArray<uint64_t, 4> index_dml_column_ids_;

  schema::ObTableParam delta_table_param_;
  schema::ObTableParam index_table_param_;
  storage::ObTableScanParam delta_scan_param_;
  storage::ObTableScanParam index_scan_param_;

  common::ObNewRowIterator *delta_table_iter_;
  common::ObNewRowIterator *index_table_iter_;
  share::schema::ObTableDMLParam inc_table_dml_param_;
  share::schema::ObTableDMLParam vbitmap_table_dml_param_;

};

class ObVectorIndexSnapTableHandler
{
public:
  const static int64_t SNAPSHOT_TABLE_COL_CNT = 5;
  static const int64_t MAX_RETRY_COUNT = 100;

public:
  ObVectorIndexSnapTableHandler(const uint64_t tenant_id):
    allocator_(ObMemAttr(tenant_id, "VISnap")),
    tenant_id_(tenant_id), tenant_schema_version_(-1), schema_version_(-1), ls_id_(),
    data_table_id_(0), snapshot_table_id_(0), snapshot_tablet_id_(), snapshot_column_count_(SNAPSHOT_TABLE_COL_CNT),
    vector_key_col_idx_(-1), vector_data_col_idx_(-1), vector_vid_col_idx_(-1),
    vector_col_idx_(-1), vector_visible_col_idx_(-1),
    key_col_id_(-1), data_col_id_(-1), visible_col_id_(-1),
    data_col_cs_type_(CS_TYPE_INVALID), table_dml_param_(allocator_)
  {}
  ~ObVectorIndexSnapTableHandler() {}

  int init(
      const ObLSID &ls_id, const uint64_t data_table_id,
      const uint64_t snapshot_table_id, const ObTabletID &snapshot_tablet_id);
  int64_t get_lob_inrow_threshold() const { return table_dml_param_.get_data_table().get_lob_inrow_threshold(); }
  int64_t get_tenant_schema_version() const { return tenant_schema_version_; }
  int64_t get_vector_data_col_idx() const { return vector_data_col_idx_; }
  int64_t get_dml_column_cnt() const { return snapshot_column_count_ + extra_column_idxs_.count(); }

  int insertup_meta_row(
      ObPluginVectorIndexAdaptor *adaptor, transaction::ObTxDesc *tx_desc, const int64_t timeout_ts);
  int insert_segment_data(
      ObIArray<ObVecIdxSnapshotBlockData> &data_blocks, transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
      const int64_t snapshot_version, const ObVectorIndexAlgorithmType index_type, const int64_t timeout);

private:
  static int prepare_snapshot_table_column_info(
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
      int64_t &visible_col_id);

  int prepare_insert_iter(
      ObIArray<ObVecIdxSnapshotBlockData> &data_blocks, const int64_t snapshot_version,
      const ObVectorIndexAlgorithmType index_type, storage::ObValueRowIterator &row_iter);
  int do_insert(
      storage::ObValueRowIterator &row_iter, transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot, const int64_t timeout);
  int inner_insertup_meta_row(
      ObPluginVectorIndexAdaptor *adaptor,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot,
      const int64_t timeout_ts);
  int read_meta_from_row(ObIAllocator &allocator, const blocksstable::ObDatumRow &row, ObVectorIndexMeta &meta);
  int prepare_insert_meta_row(blocksstable::ObDatumRow &row, const ObString &meta_data);
  int insert_meta_data_row(
      transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
      blocksstable::ObDatumRow &new_row, const uint64_t timeout);
  int update_meta_data_row(
      transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
      blocksstable::ObDatumRow &old_row, blocksstable::ObDatumRow &new_row, const uint64_t timeout);
  int scan_and_lock_meta_row(
      ObPluginVectorIndexAdaptor *adaptor,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot,
      ObNewRowIterator *&snap_data_iter,
      schema::ObTableParam &snap_table_param,
      storage::ObTableScanParam &snap_scan_param,
      const uint64_t timeout_ts,
      blocksstable::ObDatumRow &old_row,
      bool &row_exist);
  int lock_row(
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot,
      blocksstable::ObDatumRow &row,
      const int64_t timeout,
      const int64_t abs_lock_timeout);
  int construct_old_row(blocksstable::ObDatumRow *input, blocksstable::ObDatumRow &output);

  int prepare_dml_param(
      ObDMLBaseParam &dml_param, storage::ObStoreCtxGuard &store_ctx_guard,
      transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
      const uint64_t timeout);

protected:
  virtual int prepare_meta_for_insert(ObVectorIndexMeta &new_meta, const int64_t snapshot_version) { return OB_NOT_SUPPORTED; }
  virtual int prepare_meta_for_update(ObVectorIndexMeta &new_meta, const ObVectorIndexMeta &old_meta, const int64_t snapshot_version) { return OB_NOT_SUPPORTED; }

public:
  TO_STRING_KV(KP(this), K_(tenant_id), K_(tenant_schema_version), K_(schema_version),
      K_(ls_id), K_(data_table_id), K_(snapshot_table_id), K_(snapshot_tablet_id),
      K_(snapshot_column_count), K_(vector_key_col_idx), K_(vector_data_col_idx),
      K_(vector_vid_col_idx), K_(vector_col_idx), K_(vector_visible_col_idx),
      K_(key_col_id), K_(data_col_id), K_(visible_col_id), K_(data_col_cs_type));

private:
  ObArenaAllocator allocator_;
  uint64_t tenant_id_;
  int64_t tenant_schema_version_;
  int64_t schema_version_;
  ObLSID ls_id_;
  uint64_t data_table_id_;
  uint64_t snapshot_table_id_;
  ObTabletID snapshot_tablet_id_;
  int64_t snapshot_column_count_;
  int64_t vector_key_col_idx_;
  int64_t vector_data_col_idx_;
  int64_t vector_vid_col_idx_;
  int64_t vector_col_idx_;
  int64_t vector_visible_col_idx_;
  int64_t key_col_id_;
  int64_t data_col_id_;
  int64_t visible_col_id_;
  ObCollationType data_col_cs_type_;

  ObSEArray<uint64_t, 6> all_column_ids_;
  ObSEArray<uint64_t, 6> dml_column_ids_;
  ObSEArray<uint64_t, 6> extra_column_idxs_;

  share::schema::ObTableDMLParam table_dml_param_;
};

class ObVecIdxSnapTableSegAddOp : public ObVectorIndexSnapTableHandler
{
public:
  ObVecIdxSnapTableSegAddOp(const uint64_t tenant_id):
    ObVectorIndexSnapTableHandler(tenant_id)
  {}
  virtual ~ObVecIdxSnapTableSegAddOp() = default;
  int preprea_meta(ObVectorIndexSegmentMeta &seg_meta, ObVectorIndexMeta &fake_old_meta);
protected:
  int prepare_meta_for_insert(ObVectorIndexMeta &new_meta, const int64_t snapshot_version);
  int prepare_meta_for_update(ObVectorIndexMeta &new_meta, const ObVectorIndexMeta &old_meta, const int64_t snapshot_version);

public:
  ObVectorIndexSegmentMeta *new_seg_meta_;
  // old version does have meta data, but have one base segment
  // so fake a meta, and need merge this for freeze
  ObVectorIndexMeta *fake_old_meta_;
};

class ObVecIdxSnapTableSegMergeOp : public ObVectorIndexSnapTableHandler
{
public:
  ObVecIdxSnapTableSegMergeOp(const uint64_t tenant_id):
    ObVectorIndexSnapTableHandler(tenant_id),
    new_seg_meta_(nullptr),
    merge_segments_(nullptr)
  {}
  virtual ~ObVecIdxSnapTableSegMergeOp() = default;
  int preprea_meta(ObVectorIndexSegmentMeta *seg_meta, ObIArray<const ObVectorIndexSegmentMeta *> &merge_segments);
protected:
  int prepare_meta_for_insert(ObVectorIndexMeta &new_meta, const int64_t snapshot_version);
  int prepare_meta_for_update(ObVectorIndexMeta &new_meta, const ObVectorIndexMeta &old_meta, const int64_t snapshot_version);

public:
  ObVectorIndexSegmentMeta *new_seg_meta_;
  ObIArray<const ObVectorIndexSegmentMeta *> *merge_segments_;
};

class ObVecIdxSnapTableSegReplaceOp : public ObVectorIndexSnapTableHandler
{
public:
  ObVecIdxSnapTableSegReplaceOp(const uint64_t tenant_id):
    ObVectorIndexSnapTableHandler(tenant_id),
    new_meta_(nullptr)
  {}
  virtual ~ObVecIdxSnapTableSegReplaceOp() = default;
  int preprea_meta(ObVectorIndexMeta &meta);
protected:
  int prepare_meta_for_insert(ObVectorIndexMeta &new_meta, const int64_t snapshot_version);
  int prepare_meta_for_update(ObVectorIndexMeta &new_meta, const ObVectorIndexMeta &old_meta, const int64_t snapshot_version);

public:
  ObVectorIndexMeta *new_meta_;
};

}  // namespace share
}  // namespace oceanbase

#endif