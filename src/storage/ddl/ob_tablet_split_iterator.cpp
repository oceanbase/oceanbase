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

#define USING_LOG_PREFIX STORAGE
#include "ob_tablet_split_iterator.h"
#include "storage/ddl/ob_tablet_split_sstable_helper.h"
#include "storage/ddl/ob_tablet_split_task.h"

namespace oceanbase
{
using namespace share::schema;
using namespace share;
using namespace common;
using namespace storage;
using namespace blocksstable;

namespace storage
{

ObSplitIterator::ObSplitIterator()
  : allocator_("SplitIterator", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    is_inited_(false),
    ctx_(),
    access_ctx_(),
    table_read_info_(nullptr),
    access_param_()
{
}

ObSplitIterator::~ObSplitIterator()
{
  access_ctx_.reset();
  ctx_.reset();
  access_param_.reset();
  destroy_split_object(allocator_, table_read_info_);
  allocator_.reset();
  is_inited_ = false;
}

int ObSplitIterator::build_rowkey_read_info(
    const ObSplitScanParam &param,
    const ObSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    // For rowkey cg, param.storage_schema is not a clipped one, use schema_column_cnt from storage schema will
    // scan more columns unexpectedly.
    ObSSTableMetaHandle meta_handle;
    ObRowkeyReadInfo *rowkey_read_info = nullptr;
    ObSEArray<share::schema::ObColDesc, 16> cols_desc;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("init twice", K(ret));
    } else if (OB_UNLIKELY(!param.is_valid() || sstable.is_cg_sstable())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(param), K(sstable));
    } else if (OB_FAIL(param.storage_schema_->get_mulit_version_rowkey_column_ids(cols_desc))) {
      LOG_WARN("fail to get rowkey column ids", K(ret), KPC(param.storage_schema_));
    } else if (OB_FAIL(sstable.get_meta(meta_handle))) {
      LOG_WARN("fail to get meta", K(ret), K(sstable));
    } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta is invalid", K(ret), K(sstable));
    } else if (OB_ISNULL(rowkey_read_info = OB_NEWx(ObRowkeyReadInfo, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem failed", K(ret));
    } else if (OB_FAIL(rowkey_read_info->init(allocator_,
                                              meta_handle.get_sstable_meta().get_schema_column_count()/*schema_column_count*/,
                                              param.storage_schema_->get_rowkey_column_num(),
                                              param.storage_schema_->is_oracle_mode(),
                                              cols_desc,
                                              false /*is_cg_sstable*/,
                                              false /*use_default_compat_version*/,
                                              false/*is_cs_replica_compat*/))) {
      LOG_WARN("fail to init rowkey read info", K(ret), KPC(param.storage_schema_));
    } else {
      table_read_info_ = rowkey_read_info;
    }
    if (OB_FAIL(ret)) {
      destroy_split_object(allocator_, rowkey_read_info);
    }
    return ret;
  }

  int ObSplitIterator::build_normal_cg_read_info(
      const ObSplitScanParam &param,
      const ObSSTable &sstable)
  {
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_arena("BltCgReadInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const uint16_t cg_idx = sstable.get_column_group_id();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !sstable.is_cg_sstable()
      || cg_idx >= param.storage_schema_->get_column_groups().count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(cg_idx), "cgs_cnt", param.storage_schema_->get_column_groups().count(), K(param), K(sstable));
  } else {
    ObArray<ObColDesc> multi_version_cols_desc;
    int64_t column_idx = 0;
    int64_t column_id = 0;
    ObColumnParam *column_param = nullptr;
    const ObStorageColumnSchema *column_schema = nullptr;
    ObTableReadInfo *table_read_info = nullptr;
    const ObStorageColumnGroupSchema &cg_schema = param.storage_schema_->get_column_groups().at(cg_idx);
    if (OB_UNLIKELY(!cg_schema.is_single_column_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column group", K(ret), K(cg_schema), K(sstable));
    } else if (OB_FAIL(param.storage_schema_->get_multi_version_column_descs(multi_version_cols_desc))) {
      LOG_WARN("get multi version column descs failed", K(ret));
    } else if (OB_FALSE_IT(column_idx = cg_schema.get_column_idx(0/*column idx*/))) {
    } else if (OB_UNLIKELY(column_idx < 0 || column_idx >= multi_version_cols_desc.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), K(column_idx), "multi_version_cols_desc", multi_version_cols_desc);
    } else if (OB_FALSE_IT(column_id = multi_version_cols_desc.at(column_idx).col_id_)) {
    } else if (OB_ISNULL(column_schema = param.storage_schema_->get_column_schema(column_id))) {
      ret = OB_ERR_SYS;
      LOG_WARN("get column schema failed", K(ret), K(column_id));
    } else if (OB_FAIL(ObTableParam::alloc_column(tmp_arena, column_param))) {
      LOG_WARN("alloc column param failed", K(ret));
    } else if (OB_FAIL(column_schema->construct_column_param(param.data_format_version_, *column_param))) {
      LOG_WARN("convert column schema to param failed", K(ret), KPC(column_schema));
    } else if (OB_ISNULL(table_read_info = OB_NEWx(ObTableReadInfo, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem failed", K(ret));
    } else if (OB_FAIL(ObTenantCGReadInfoMgr::construct_cg_read_info(allocator_/*will deep copy column_param*/,
                                                                     param.storage_schema_->is_oracle_mode(),
                                                                     multi_version_cols_desc.at(column_idx),
                                                                     column_param,
                                                                     *table_read_info))) {
      LOG_WARN("construct cg read info failed", K(ret), KPC(column_schema));
    } else {
      table_read_info_ = table_read_info;
    }

    if (OB_FAIL(ret)) {
      destroy_split_object(allocator_, table_read_info);
    }
  }
  return ret;
}

//construct table access param
int ObSplitIterator::construct_access_param(
  const ObSplitScanParam &param,
  const ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  const bool is_cg_sstable = sstable.is_cg_sstable();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param));
  } else if (!is_cg_sstable && OB_FAIL(build_rowkey_read_info(param, sstable))) {
    LOG_WARN("build rowkey read info failed", K(ret), K(param));
  } else if (is_cg_sstable && OB_FAIL(build_normal_cg_read_info(param, sstable))) {
    LOG_WARN("build normal cg read info failed", K(ret), K(param));
  } else if (OB_FAIL(access_param_.init_merge_param(
        param.table_id_, param.src_tablet_.get_tablet_meta().tablet_id_, *table_read_info_, false/*is_multi_version_minor_merge*/))) {
    LOG_WARN("init table access param failed", K(ret), KPC(table_read_info_), K(param));
  }
  LOG_INFO("construct table access param finished", K(ret), K(access_param_), K(sstable));
  return ret;
}

// construct version range and ctx
int ObSplitIterator::construct_access_ctx(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const int64_t snapshot_version = INT64_MAX;
  ObQueryFlag query_flag(ObQueryFlag::Forward,
                        true, /*is daily merge scan*/
                        true, /*is read multiple macro block*/
                        true, /*sys task scan, read one macro block in single io*/
                        false /*is full row scan?*/,
                        false,
                        false);
  query_flag.is_bare_row_scan_ = true; // output mult-version rows without do_compact.
  common::ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = snapshot_version;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id));
  } else if (OB_FAIL(ctx_.init_for_read(ls_id,
                                        tablet_id,
                                        INT64_MAX,
                                        -1,
                                        SCN::max_scn()))) {
    LOG_WARN("fail to init store ctx", K(ret), K(ls_id));
  } else if (OB_FAIL(access_ctx_.init(query_flag,
                                      ctx_,
                                      allocator_,
                                      allocator_,
                                      trans_version_range))) {
    LOG_WARN("fail to init accesss ctx", K(ret));
  }
  LOG_INFO("construct access ctx finished", K(ret), K(access_ctx_));
  return ret;
}


ObRowScan::ObRowScan() :
  ObSplitIterator(), row_iter_(nullptr)
{}

ObRowScan::~ObRowScan()
{
  if (OB_NOT_NULL(row_iter_)) {
    row_iter_->~ObSSTableRowWholeScanner();
    row_iter_ = nullptr;
  }
}

int ObRowScan::init(
    const ObSplitScanParam &param,
    ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), K(sstable));
  } else if (OB_FAIL(construct_access_param(param, sstable))) {
    LOG_WARN("construct access param failed", K(ret), K(param));
  } else if (OB_FAIL(construct_access_ctx(param.src_tablet_.get_tablet_meta().ls_id_, param.src_tablet_.get_tablet_meta().tablet_id_))) {
    LOG_WARN("construct access param failed", K(ret), K(param));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableRowWholeScanner)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mem failed", K(ret));
  } else if (FALSE_IT(row_iter_ = new(buf)ObSSTableRowWholeScanner())) {
  } else if (OB_FAIL(row_iter_->init(access_param_.iter_param_,
                                     access_ctx_,
                                     &sstable,
                                     param.query_range_))) {
    LOG_WARN("construct iterator failed", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != row_iter_) {
      row_iter_->~ObSSTableRowWholeScanner();
      row_iter_ = nullptr;
    }
    if (nullptr != buf) {
      allocator_.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObRowScan::init(
    const ObSplitScanParam &param,
    const blocksstable::ObMacroBlockDesc &macro_desc,
    ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !macro_desc.is_valid() || !sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), K(macro_desc), K(sstable));
  } else if (OB_FAIL(construct_access_param(param, sstable))) {
    LOG_WARN("construct access param failed", K(ret), K(param));
  } else if (OB_FAIL(construct_access_ctx(param.src_tablet_.get_tablet_meta().ls_id_, param.src_tablet_.get_tablet_meta().tablet_id_))) {
    LOG_WARN("construct access param failed", K(ret), K(param));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableRowWholeScanner)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mem failed", K(ret));
  } else if (FALSE_IT(row_iter_ = new(buf)ObSSTableRowWholeScanner())) {
  } else if (OB_FAIL(row_iter_->open(access_param_.iter_param_,
     access_ctx_, *param.query_range_, macro_desc, sstable))) {
    LOG_WARN("constrtuct stored row iterator failed", K(ret), K(access_param_), K(access_ctx_), K(macro_desc), K(sstable));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != row_iter_) {
      row_iter_->~ObSSTableRowWholeScanner();
      row_iter_ = nullptr;
    }
    if (nullptr != buf) {
      allocator_.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObRowScan::get_next_row(const ObDatumRow *&tmp_row)
{
  int ret = OB_SUCCESS;
  tmp_row = nullptr;
  const ObDatumRow *row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(row_iter_->get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_UNLIKELY(nullptr == row || !row->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected datum row", K(ret), KPC(row));
  } else {
    tmp_row = row;
  }
  return ret;
}

ObSnapshotRowScan::ObSnapshotRowScan() : is_inited_(false),
  allocator_("SplitSnapScan", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
  snapshot_version_(0), range_(), read_info_(), write_row_(), out_cols_projector_(), access_param_(), ctx_(), access_ctx_(), get_table_param_(), scan_merge_(nullptr)
{
  reset();
}

ObSnapshotRowScan::~ObSnapshotRowScan()
{
  reset();
}

void ObSnapshotRowScan::reset()
{
  is_inited_ = false;
  if (OB_NOT_NULL(scan_merge_)) {
    scan_merge_->~ObMultipleScanMerge();
    allocator_.free(scan_merge_);
    scan_merge_ = nullptr;
  }
  get_table_param_.reset();
  access_ctx_.reset();
  ctx_.reset();
  access_param_.reset();
  out_cols_projector_.reset();
  write_row_.reset();
  read_info_.reset();
  range_.reset();
  snapshot_version_ = 0;
  allocator_.reset();
}

int ObSnapshotRowScan::init(
    const ObSplitScanParam &param,
    const ObIArray<ObColDesc> &schema_store_col_descs,
    const int64_t schema_column_cnt,
    const int64_t schema_rowkey_cnt,
    const bool is_oracle_mode,
    const ObTabletHandle &tablet_handle,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    const ObTabletID &tablet_id = param.src_tablet_.get_tablet_meta().tablet_id_;
    const ObLSID &ls_id = param.src_tablet_.get_tablet_meta().ls_id_;
    ObQueryFlag query_flag(ObQueryFlag::Forward,
        false, /* daily merge*/
        true,  /* use *optimize */
        true,  /* use whole macro scan*/
        false, /* not full row*/
        false, /* not index_back*/
        false);/* query stat */
    query_flag.disable_cache();
    ObTabletTableIterator table_iter;
    snapshot_version_ = snapshot_version;
    schema_rowkey_cnt_ = schema_rowkey_cnt;
    range_.set_whole_range();
    if (OB_FAIL(table_iter.set_tablet_handle(tablet_handle))) {
      LOG_WARN("failed to set tablet handle", K(ret));
    } else if (OB_FAIL(table_iter.refresh_read_tables_from_tablet(snapshot_version,
                                                                  false/*allow_no_ready_read*/,
                                                                  false/*major_sstable_only*/,
                                                                  false/*need_split_src_table*/,
                                                                  false/*need_split_dst_table*/))) {
      LOG_WARN("failed to get read tables", K(ret), K(param));
    } else if (OB_FAIL(read_info_.init(allocator_,
                                       schema_column_cnt,
                                       schema_rowkey_cnt,
                                       is_oracle_mode,
                                       schema_store_col_descs,
                                       nullptr/*storage_cols_index*/))) {
      LOG_WARN("failed to init read info", K(ret), K(schema_column_cnt), K(schema_rowkey_cnt), K(schema_store_col_descs));
    } else if (OB_FAIL(write_row_.init(allocator_, read_info_.get_columns_desc().count() + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("Fail to init write row", K(ret));
    } else if (OB_FALSE_IT(write_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT))) {
    } else if (OB_FAIL(construct_access_param(param.table_id_, tablet_id, read_info_))) {
      LOG_WARN("failed to init access param", K(ret), K(param));
    } else if (OB_FAIL(construct_range_ctx(query_flag, ls_id))) {
      LOG_WARN("failed to init access ctx", K(ret));
    } else if (OB_FAIL(construct_multiple_scan_merge(table_iter, range_))) {
      LOG_WARN("failed to init scan merge", K(ret), K(table_iter));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSnapshotRowScan::construct_access_param(
    const uint64_t table_id,
    const common::ObTabletID &tablet_id,
    const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  const int64_t column_cnt = read_info.get_columns_desc().count();
  out_cols_projector_.reset();
  if (OB_FAIL(out_cols_projector_.prepare_allocate(column_cnt, 0))) {
    LOG_WARN("failed to prepare allocate", K(ret), K(column_cnt));
  } else {
    for (int64_t i = 0; i < out_cols_projector_.count(); i++) {
      out_cols_projector_[i] = i;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(access_param_.init_merge_param(table_id,
                                                    tablet_id,
                                                    read_info,
                                                    false/*is_multi_version_minor_merge*/))) {
    LOG_WARN("failed to init access param", K(ret));
  } else {
    access_param_.iter_param_.out_cols_project_ = &out_cols_projector_;
  }
  return ret;
}

int ObSnapshotRowScan::construct_range_ctx(
    ObQueryFlag &query_flag,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  common::ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = snapshot_version_;
  trans_version_range.multi_version_start_ = snapshot_version_;
  trans_version_range.base_version_ = 0;
  SCN tmp_scn;
  if (OB_FAIL(tmp_scn.convert_for_tx(snapshot_version_))) {
    LOG_WARN("convert fail", K(ret), K(ls_id), K_(snapshot_version));
  } else if (OB_FAIL(ctx_.init_for_read(ls_id,
                                        access_param_.iter_param_.tablet_id_,
                                        INT64_MAX,
                                        -1,
                                        tmp_scn))) {
    LOG_WARN("fail to init store ctx", K(ret), K(ls_id));
  } else if (OB_FAIL(access_ctx_.init(query_flag, ctx_, allocator_, allocator_, trans_version_range))) {
    LOG_WARN("fail to init accesss ctx", K(ret));
  } else if (OB_NOT_NULL(access_ctx_.lob_locator_helper_)) {
    access_ctx_.lob_locator_helper_->update_lob_locator_ctx(access_param_.iter_param_.table_id_,
                                                            access_param_.iter_param_.tablet_id_.id(),
                                                            0/*tx_id*/);
  }
  return ret;
}

int ObSnapshotRowScan::construct_multiple_scan_merge(
    const ObTabletTableIterator &table_iter,
    const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_FAIL(get_table_param_.tablet_iter_.assign(table_iter))) {
    LOG_WARN("fail to assign tablet iterator", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMultipleScanMerge)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObMultipleScanMerge", K(ret));
  } else if (FALSE_IT(scan_merge_ = new(buf)ObMultipleScanMerge())) {
  } else if (OB_FAIL(scan_merge_->init(access_param_, access_ctx_, get_table_param_))) {
    LOG_WARN("fail to init scan merge", K(ret), K(access_param_), K(access_ctx_));
  } else if (OB_FAIL(scan_merge_->open(range))) {
    LOG_WARN("fail to open scan merge", K(ret), K(access_param_), K(access_ctx_), K(range));
  } else {
    scan_merge_->disable_padding();
    scan_merge_->disable_fill_virtual_column();
  }
  return ret;
}

int ObSnapshotRowScan::add_extra_rowkey(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_column_count = schema_rowkey_cnt_;
  const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (OB_UNLIKELY(write_row_.get_capacity() < row.count_ + extra_rowkey_cnt ||
                  row.count_ < rowkey_column_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected row", K(ret), K(write_row_), K(row.count_), K(rowkey_column_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; i++) {
      if (i < rowkey_column_count) {
        write_row_.storage_datums_[i] = row.storage_datums_[i];
      } else {
        write_row_.storage_datums_[i + extra_rowkey_cnt] = row.storage_datums_[i];
      }
    }
    write_row_.storage_datums_[rowkey_column_count].set_int(-snapshot_version_);
    write_row_.storage_datums_[rowkey_column_count + 1].set_int(0);
    write_row_.count_ = row.count_ + extra_rowkey_cnt;
  }
  return ret;
}

int ObSnapshotRowScan::get_next_row(const ObDatumRow *&out_row)
{
  int ret = OB_SUCCESS;
  out_row = nullptr;
  ObDatumRow *row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(scan_merge_->get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_UNLIKELY(nullptr == row || !row->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected datum row", K(ret), KPC(row));
  } else if (OB_FAIL(add_extra_rowkey(*row))) {
    LOG_WARN("failed to add extra rowkey", K(ret));
  } else {
    out_row = static_cast<const ObDatumRow *>(&write_row_);
  }
  return ret;
}

ObUncommittedRowScan::ObUncommittedRowScan()
  : row_scan_(), row_scan_end_(false), next_row_(nullptr),
    major_snapshot_version_(OB_INVALID_TIMESTAMP), trans_version_col_idx_(0), row_queue_(),
    row_queue_allocator_("SplitRowQueue", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    row_queue_has_unskippable_row_(false)
{
}

ObUncommittedRowScan::~ObUncommittedRowScan()
{
}

int ObUncommittedRowScan::init(
    const ObSplitScanParam param,
    ObSSTable &src_sstable,
    const int64_t major_snapshot_version,
    const int64_t schema_column_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_scan_.init(param, src_sstable))) {
    LOG_WARN("failed to init", K(ret));
  } else {
    row_scan_end_ = false;
    next_row_ = nullptr;
    major_snapshot_version_ = major_snapshot_version;
    trans_version_col_idx_ = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
        row_scan_.get_rowkey_read_info()->get_schema_rowkey_count(), true);
    if (OB_FAIL(row_queue_.init(schema_column_cnt + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("failed to init row queue", K(ret));
    } else {
      row_queue_allocator_.reset();
      row_queue_has_unskippable_row_ = false;
    }
  }
  return ret;
}

int ObUncommittedRowScan::get_next_row(const ObDatumRow *&res_row)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row = nullptr;
  res_row = nullptr;
  while (OB_SUCC(ret) && !row_queue_.has_next()) {
    if (OB_FAIL(get_next_rowkey_rows())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next rowkey rows", K(ret));
      }
    } else if (!row_queue_has_unskippable_row_) {
      row_queue_reuse();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_queue_.get_next_row(row))) {
    LOG_WARN("failed to get next row", K(ret));
  } else {
    res_row = row;
  }
  return ret;
}

int ObUncommittedRowScan::get_next_rowkey_rows()
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row = nullptr;
  if (OB_UNLIKELY(row_queue_.has_next())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("previous rowkey rows still exist", K(ret));
  }

  // collect rows from next_row_ to row_queue_
  if (OB_SUCC(ret) && OB_NOT_NULL(next_row_)) {
    if (OB_FAIL(row_queue_add(*next_row_))) {
      LOG_WARN("failed to add row", K(ret));
    } else {
      next_row_ = nullptr;
    }
  }

  // collect rows from row_scan_ to row_queue_ or next_row_
  while (OB_SUCC(ret) && !row_scan_end_ && nullptr == next_row_) {
    if (OB_FAIL(row_scan_.get_next_row(row))) {
      if (OB_ITER_END == ret) {
        row_scan_end_ = true;
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row", K(ret));
    } else if (OB_FAIL(row_queue_add(*row))) {
      if (OB_SIZE_OVERFLOW == ret) {
        next_row_ = row;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to add row", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !row_queue_.has_next()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObUncommittedRowScan::row_queue_add(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool rowkey_changed = false;
  if (row_queue_.has_next()) {
    const int64_t schema_rowkey_cnt = row_scan_.get_rowkey_read_info()->get_schema_rowkey_count();
    const blocksstable::ObStorageDatumUtils &datum_utils = row_scan_.get_rowkey_read_info()->get_datum_utils();
    const ObDatumRow *last_row_in_queue = row_queue_.get_last();
    ObDatumRowkey cur_key;
    ObDatumRowkey last_key;
    int compare_result = 0;
    if (OB_ISNULL(last_row_in_queue)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected last row is nullptr", K(ret), K(row_queue_));
    } else if (OB_FAIL(last_key.assign(last_row_in_queue->storage_datums_, schema_rowkey_cnt))) {
      LOG_WARN("failed to assign qu rowkey", K(ret));
    } else if (OB_FAIL(cur_key.assign(row.storage_datums_, schema_rowkey_cnt))) {
      LOG_WARN("failed to assign cur key", K(ret));
    } else if (OB_FAIL(cur_key.compare(last_key, datum_utils, compare_result))) {
      LOG_WARN("failed to compare last key", K(ret), K(cur_key), K(last_key));
    } else if (OB_UNLIKELY(compare_result < 0)) {
      ret = OB_ROWKEY_ORDER_ERROR;
      LOG_ERROR("input rowkey is less then last rowkey", K(ret), K(cur_key), K(last_key), K(ret));
    } else if (compare_result > 0) {
      rowkey_changed = true;
    }
  }

  if (OB_SUCC(ret) && !rowkey_changed) {
    bool can_skip = false;
    if (OB_FAIL(row_queue_.add_row(row, row_queue_allocator_))) {
      LOG_WARN("failed to add row", K(ret));
    } else if (OB_FAIL(check_can_skip(row, can_skip))) {
      LOG_WARN("failed to check can skip", K(ret));
    } else if (!can_skip) {
      row_queue_has_unskippable_row_ = true;
    }
  }

  if (OB_SUCC(ret) && rowkey_changed) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_SIZE_OVERFLOW == ret) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("change errcode", K(ret), "real_ret", OB_SIZE_OVERFLOW);
  }
  return ret;
}

void ObUncommittedRowScan::row_queue_reuse()
{
  row_queue_.reuse();
  row_queue_allocator_.reuse();
  row_queue_has_unskippable_row_ = false;
  return;
}

int ObUncommittedRowScan::check_can_skip(const ObDatumRow &row, bool &can_skip)
{
  int ret = OB_SUCCESS;
  SCN scn_commit_trans_version = SCN::max_scn();
  int64_t commit_version = OB_INVALID_VERSION;
  if (OB_INVALID_TIMESTAMP == major_snapshot_version_) {
    can_skip = false;
  } else if (row.mvcc_row_flag_.is_uncommitted_row()) {
    SCN major_snapshot_scn;
    storage::ObTxTableGuards &tx_table_guards = row_scan_.get_tx_table_guards();
    bool can_read = false;
    int64_t state = ObTxData::MAX_STATE_CNT;
    const transaction::ObTransID &read_trans_id = row.trans_id_;
    if (OB_FAIL(major_snapshot_scn.convert_for_tx(major_snapshot_version_))) {
      LOG_WARN("failed to convert major snapshot version", K(ret), K(major_snapshot_version_));
    } else if (OB_FAIL(tx_table_guards.get_tx_state_with_scn(
        read_trans_id, major_snapshot_scn, state, scn_commit_trans_version))) {
      LOG_WARN("get transaction status failed", K(ret), K(read_trans_id), K(state));
    } else if (ObTxData::RUNNING == state) {
      can_skip = false;
    } else if (ObTxData::COMMIT == state || ObTxData::ELR_COMMIT == state || ObTxData::ABORT == state) {
      can_skip = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected row state", K(ret));
    }
  } else {
    if (OB_UNLIKELY(trans_version_col_idx_ >= row.get_column_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trans version column index out of range", K(ret), K(trans_version_col_idx_), K(row));
    } else {
      const int64_t row_commit_version = -(row.storage_datums_[trans_version_col_idx_].get_int());
      can_skip = row_commit_version <= major_snapshot_version_;
    }
  }
  if (OB_SUCC(ret) && can_skip) {
    LOG_DEBUG("skip row", K(row));
  }
  return ret;
}

ObSplitReuseBlockIter::ObSplitReuseBlockIter()
  : ObSplitIterator(),
  cur_cmp_tablet_index_(0),
  is_macro_block_opened_(false),
  is_micro_block_opened_(false),
  cur_rowid_in_sstable_(-1),
  scan_param_(nullptr),
  split_helper_(nullptr),
  macro_meta_iter_(),
  cur_macro_block_desc_(nullptr),
  micro_block_iter_(),
  cur_micro_block_(nullptr),
  micro_row_scanner_(nullptr),
  macro_reader_()
{
}

ObSplitReuseBlockIter::~ObSplitReuseBlockIter()
{
  cur_cmp_tablet_index_ = 0;
  is_macro_block_opened_ = false;
  is_micro_block_opened_ = false;
  cur_rowid_in_sstable_ = -1;
  scan_param_ = nullptr;
  split_helper_ = nullptr;
  destroy_split_object(allocator_, micro_row_scanner_);
  cur_micro_block_ = nullptr;
  micro_block_iter_.reset();
  cur_macro_block_desc_ = nullptr;
  macro_meta_iter_.reset();
}

int ObSplitReuseBlockIter::inner_open_cur_macro_block()
{
  int ret = OB_SUCCESS;
  micro_block_iter_.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == cur_macro_block_desc_ || !cur_macro_block_desc_->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KPC(cur_macro_block_desc_), KPC(this));
  } else if (OB_FAIL(micro_block_iter_.init(
      *cur_macro_block_desc_,
      *split_helper_->get_index_read_info(),
      macro_meta_iter_.get_micro_index_infos(),
      macro_meta_iter_.get_micro_endkeys(),
      static_cast<ObRowStoreType>(cur_macro_block_desc_->row_store_type_),
      split_helper_->get_sstable()))) {
    LOG_WARN("init micro_block_iter failed", K(ret), K(macro_meta_iter_));
  } else {
    is_macro_block_opened_ = true;
    cur_rowid_in_sstable_ -= cur_macro_block_desc_->row_count_;
  }
  return ret;
}

int ObSplitReuseBlockIter::inner_open_cur_micro_block()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == micro_row_scanner_
      || !is_macro_block_opened_
      || is_micro_block_opened_
      || nullptr == cur_micro_block_
      || !cur_micro_block_->range_.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KP(micro_row_scanner_), K(is_macro_block_opened_), K(is_micro_block_opened_), KPC(cur_micro_block_));
  } else {
    bool is_compressed = false;
    ObMicroBlockData decompressed_data;
    ObMicroBlockDesMeta micro_des_meta;
    const ObMicroIndexInfo *micro_index_info = cur_micro_block_->micro_index_info_;
    micro_row_scanner_->reuse();
    cur_rowid_in_sstable_ -= cur_micro_block_->header_.row_count_;
    if (OB_UNLIKELY(nullptr == micro_index_info || !micro_index_info->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected micro block", K(ret), KPC(micro_index_info), KPC(cur_micro_block_));
    } else if (OB_FAIL(micro_index_info->row_header_->fill_micro_des_meta(false, micro_des_meta))) {
      LOG_WARN("Fail to fill micro block deserialize meta", K(ret), KPC(micro_index_info));
    } else if (OB_FAIL(macro_reader_.decrypt_and_decompress_data(
        micro_des_meta,
        cur_micro_block_->data_.get_buf(),
        cur_micro_block_->data_.get_buf_size(),
        decompressed_data,
        is_compressed))) {
      LOG_WARN("Failed to decrypt and decompress data", K(ret), KPC_(cur_micro_block));
    } else if (split_helper_->get_sstable()->is_cg_sstable()) { // not including co sstable.
      ObCSRange range;
      range.start_row_id_ = 0;
      range.end_row_id_ = cur_micro_block_->header_.row_count_ - 1;
      if (OB_FAIL(micro_row_scanner_->open_column_block(cur_macro_block_desc_->macro_block_id_,
                                            decompressed_data,
                                            range))) {
        LOG_WARN("failed to open column block", K(ret), KPC(cur_macro_block_desc_), K(decompressed_data), K(range));
      }
    } else {
      if (OB_FAIL(micro_row_scanner_->set_range(cur_micro_block_->range_))) {
        LOG_WARN("Failed to init micro scanner", K(ret));
      } else if (OB_FAIL(micro_row_scanner_->open(
          cur_macro_block_desc_->macro_block_id_,
          decompressed_data,
          micro_block_iter_.is_left_border(),
          micro_block_iter_.is_right_border()))) {
        LOG_WARN("Failed to open micro scanner", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      is_micro_block_opened_ = true;
    }
  }
  return ret;
}

int ObSplitReuseBlockIter::inner_init_micro_row_scanner(
    const ObSplitScanParam &param,
    const ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), K(sstable));
  } else if (OB_FAIL(construct_access_param(param, sstable))) {
    LOG_WARN("construct access param failed", K(ret), K(param), K(sstable));
  } else if (OB_FAIL(construct_access_ctx(param.src_tablet_.get_tablet_meta().ls_id_, param.src_tablet_.get_tablet_meta().tablet_id_))) {
    LOG_WARN("construct access param failed", K(ret), K(param));
  } else if (OB_ISNULL(micro_row_scanner_ = OB_NEWx(ObMicroBlockRowDirectScanner, (&allocator_), allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate mem for micro row scanner", K(ret));
  } else if (OB_FAIL(micro_row_scanner_->init(access_param_.iter_param_,
                                              access_ctx_,
                                              &sstable))) {
    LOG_WARN("Failed to init micro row scanner", K(ret), K(access_param_), K(access_ctx_));
  }
  if (OB_FAIL(ret)) {
    destroy_split_object(allocator_, micro_row_scanner_);
  }
  return ret;
}

int ObSplitReuseBlockIter::init(
    ObIAllocator &allocator,
    const ObSplitScanParam &scan_param,
    const ObSSTableSplitWriteHelper *split_helper)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!scan_param.is_valid()
      || nullptr == split_helper
      || nullptr == split_helper->get_sstable()
      || nullptr == split_helper->get_index_read_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(scan_param), KPC(split_helper));
  } else if (OB_FAIL(macro_meta_iter_.open(
      *split_helper->get_sstable(),
      *scan_param.query_range_,
      *split_helper->get_index_read_info(),
      allocator))) {
    LOG_WARN("open dual macro meta iter failed", K(ret), KPC(split_helper));
  } else if (OB_FAIL(inner_init_micro_row_scanner(scan_param, *split_helper->get_sstable()))) {
    LOG_WARN("init micro row scanner failed", K(ret), K(scan_param), KPC(split_helper));
  } else {
    scan_param_ = &scan_param;
    split_helper_ = split_helper;
    is_macro_block_opened_ = false;
    is_micro_block_opened_ = false;
    cur_rowid_in_sstable_ = -1;
    is_inited_ = true;
  }
  return ret;
}

int ObSplitReuseBlockIter::check_can_reuse_macro_block(
    int64_t &dest_tablet_index,
    bool &can_reuse) const
{
  int ret = OB_SUCCESS;
  dest_tablet_index = OB_INVALID_INDEX;
  can_reuse = false;
  int cmp_ret = 0;
  ObDatumRowkey macro_end_key;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (split_helper_->get_sstable()->is_small_sstable()) {
    can_reuse = false;
    dest_tablet_index = OB_INVALID_INDEX;
  } else if (OB_ISNULL(cur_macro_block_desc_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KPC(this));
  } else if (OB_FAIL(cur_macro_block_desc_->macro_meta_->get_rowkey(macro_end_key))) {
    LOG_WARN("get macro block end key failed", K(ret), KPC(cur_macro_block_desc_));
  } else if (OB_FAIL(compare(macro_end_key, cmp_ret))) {
    LOG_WARN("compare failed", K(ret), K(cur_cmp_tablet_index_), K(cur_rowid_in_sstable_), K(macro_end_key));
  } else if (cmp_ret < 0) {
    can_reuse = true;
    dest_tablet_index = cur_cmp_tablet_index_;
  }
  LOG_TRACE("get and compare macro block", K(ret), K(can_reuse), K(cur_cmp_tablet_index_),
    KPC(cur_macro_block_desc_), KPC(split_helper_));
  return ret;
}

int ObSplitReuseBlockIter::check_can_reuse_micro_block(
    int64_t &dest_tablet_index,
    bool &can_reuse) const
{
  int ret = OB_SUCCESS;
  dest_tablet_index = OB_INVALID_INDEX;
  can_reuse = false;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == cur_macro_block_desc_
    || nullptr == cur_macro_block_desc_->macro_meta_
    || nullptr == cur_micro_block_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KPC(cur_macro_block_desc_), KPC(cur_micro_block_), KPC(this));
  } else if (!split_helper_->get_sstable()->is_major_sstable()
    || cur_macro_block_desc_->macro_meta_->get_meta_val().progressive_merge_round_ != scan_param_->storage_schema_->get_progressive_merge_round()) {
    can_reuse = false;
    dest_tablet_index = OB_INVALID_INDEX;
  } else if (OB_FAIL(compare(cur_micro_block_->range_.get_end_key(), cmp_ret))) {
    LOG_WARN("compare failed", K(ret), K(cur_cmp_tablet_index_), K(cur_rowid_in_sstable_), KPC(cur_micro_block_));
  } else if (cmp_ret < 0) {
    can_reuse = true;
    dest_tablet_index = cur_cmp_tablet_index_;
  }
  LOG_TRACE("get and compare micro block", K(ret), K(can_reuse), K(cur_cmp_tablet_index_),
    K(cur_rowid_in_sstable_), KPC(cur_macro_block_desc_), KPC(cur_micro_block_), KPC(split_helper_));
  return ret;
}

int ObSplitReuseBlockIter::compare(
    const ObDatumRowkey &rs_rowkey/*row_store_rowkey*/, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(split_helper_->get_sstable())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KPC(split_helper_));
  } else if (split_helper_->get_sstable()->is_column_store_sstable()) {
    const ObIArray<ObCSRowId> &end_partkey_rowids = static_cast<const ObColSSTableSplitWriteHelper *>(split_helper_)->get_end_partkey_rowids();
    if (cur_cmp_tablet_index_ < 0 || cur_cmp_tablet_index_ >= end_partkey_rowids.count()) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys", K(ret), K(cur_cmp_tablet_index_), K(end_partkey_rowids));
    } else if (cur_rowid_in_sstable_ < end_partkey_rowids.at(cur_cmp_tablet_index_)) {
      cmp_ret = -1;
    } else if (cur_rowid_in_sstable_ == end_partkey_rowids.at(cur_cmp_tablet_index_)) {
      cmp_ret = 0;
    } else {
      cmp_ret = 1;
    }
  } else {
    const ObIArray<ObDatumRowkey> &end_partkeys = static_cast<const ObRowSSTableSplitWriteHelper *>(split_helper_)->get_end_partkeys();
    if (cur_cmp_tablet_index_ < 0 || cur_cmp_tablet_index_ >= end_partkeys.count()) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys", K(ret), K(cur_cmp_tablet_index_), K(end_partkeys));
    } else if (OB_FAIL(rs_rowkey.compare(end_partkeys.at(cur_cmp_tablet_index_), table_read_info_->get_datum_utils(), cmp_ret))) {
      LOG_WARN("compare failed", K(ret), K(rs_rowkey), K(cur_cmp_tablet_index_), K(end_partkeys), KPC(table_read_info_));
    }
  }
  return ret;
}

int ObSplitReuseBlockIter::get_next_macro_block(
    ObMacroBlockDesc &block_desc,
    const ObMicroBlockData *&clustered_micro_block_data,
    int64_t &dest_tablet_index,
    bool &can_reuse)
{
  int ret = OB_SUCCESS;
  clustered_micro_block_data = nullptr;
  dest_tablet_index = OB_INVALID_INDEX;
  can_reuse = false;
  ObDatumRowkey macro_end_key;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(is_macro_block_opened_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("macro block already opened", K(ret));
  } else if (OB_FAIL(macro_meta_iter_.get_next_macro_block(block_desc))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next macro block failed", K(ret));
    }
  } else if (FALSE_IT(cur_macro_block_desc_ = &block_desc)) {
  } else if (FALSE_IT(cur_rowid_in_sstable_ += block_desc.row_count_)) {
  } else if (OB_FAIL(check_can_reuse_macro_block(dest_tablet_index, can_reuse))) {
    LOG_WARN("pre check can reuse macro block failed", K(ret));
  } else if (can_reuse) {
    if (block_desc.is_clustered_index_tree_ && OB_FAIL(macro_meta_iter_.get_current_clustered_index_info(clustered_micro_block_data))) {
      LOG_WARN("get micro data failed", K(ret));
    }
  }
  LOG_TRACE("get and compare macro block", K(ret), K(can_reuse), K(cur_cmp_tablet_index_),
    K(cur_rowid_in_sstable_), K(macro_end_key), K(block_desc), KPC(split_helper_));
  return ret;
}

int ObSplitReuseBlockIter::get_next_micro_block(
    const ObMicroBlock *&micro_block,
    int64_t &dest_tablet_index,
    bool &can_reuse)
{
  int ret = OB_SUCCESS;
  micro_block = nullptr;
  dest_tablet_index = OB_INVALID_INDEX;
  can_reuse = false;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_macro_block_opened_ && OB_FAIL(inner_open_cur_macro_block())) {
    LOG_WARN("failed to open curr macro block", K(ret));
  } else if (OB_FAIL(micro_block_iter_.next(cur_micro_block_))) {
    if (OB_ITER_END == ret) {
      is_macro_block_opened_ = false;
    } else {
      LOG_WARN("get next micro block failed", K(ret));
    }
  } else if (OB_ISNULL(cur_micro_block_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("unexpected null micro block", K(ret));
  } else if (FALSE_IT(cur_rowid_in_sstable_ += cur_micro_block_->header_.row_count_)) {
  } else if (OB_FAIL(check_can_reuse_micro_block(dest_tablet_index, can_reuse))) {
    LOG_WARN("pre check can reuse micro block failed", K(ret));
  } else if (can_reuse) {
    micro_block = cur_micro_block_;
  }
  LOG_TRACE("get and compare micro block", K(ret), K(can_reuse), K(cur_cmp_tablet_index_),
    K(cur_rowid_in_sstable_), KPC(cur_macro_block_desc_), KPC(cur_micro_block_), KPC(split_helper_));
  return ret;
}

int ObSplitReuseBlockIter::get_next_row(
    const ObDatumRow *&datum_row,
    int64_t &dest_tablet_index)
{
  int ret = OB_SUCCESS;
  datum_row = nullptr;
  dest_tablet_index = OB_INVALID_INDEX;
  ObDatumRowkey rowkey;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_micro_block_opened_ && OB_FAIL(inner_open_cur_micro_block())) {
    LOG_WARN("failed to open curr micro block", K(ret));
  } else if (OB_FAIL(micro_row_scanner_->get_next_row(datum_row))) {
    if (OB_ITER_END == ret) {
      is_micro_block_opened_ = false;
    } else {
      LOG_WARN("get next row failed", K(ret));
    }
  } else if (OB_ISNULL(datum_row)) {
    ret = OB_ERR_SYS;
    LOG_WARN("unexpected null row", K(ret), KPC(cur_micro_block_));
  } else if (FALSE_IT(cur_rowid_in_sstable_ += 1)) {
  } else if (OB_FAIL(rowkey.assign(datum_row->storage_datums_, datum_row->get_column_count()))) {
    LOG_WARN("assign rowkey failed", K(ret), KPC(datum_row));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(compare(rowkey, cmp_ret))) {
        LOG_WARN("compare failed", K(ret), K(rowkey));
      } else if (cmp_ret < 0) {
        dest_tablet_index = cur_cmp_tablet_index_;
        break;
      } else {
        cur_cmp_tablet_index_ += 1;
      }
    }
  }
  LOG_TRACE("get and compare row", K(ret), K(cur_cmp_tablet_index_), K(rowkey),
    K(cur_rowid_in_sstable_), KPC(cur_macro_block_desc_), KPC(cur_micro_block_), KPC(split_helper_));
  return ret;
}

} //storage
} //oceanbase
