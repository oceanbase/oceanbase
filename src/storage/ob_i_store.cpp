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

#include "ob_i_store.h"
#include "ob_sstable.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/container/ob_array_iterator.h"
#include "share/schema/ob_schema_struct.h"
#include "storage/ob_dml_param.h"
#include "storage/ob_relative_table.h"
#include "blocksstable/ob_storage_cache_suite.h"
#include "blocksstable/ob_lob_data_reader.h"
#include "storage/ob_sstable_rowkey_helper.h"
#include "storage/transaction/ob_trans_define.h"
#include "storage/ob_partition_service.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_operator.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase {
using namespace transaction;
namespace common {
OB_SERIALIZE_MEMBER(ObQueryFlag, flag_);
}

namespace storage {
using namespace common;

bool ObMultiVersionRowkeyHelpper::is_valid_multi_version_macro_meta(const blocksstable::ObMacroBlockMeta& meta)
{
  bool bret = true;
  int extra_col_cnt = get_extra_rowkey_col_cnt();
  for (int i = 0; bret && i < extra_col_cnt; ++i) {
    const int64_t col_store_idx = meta.rowkey_column_number_ - extra_col_cnt + i;
    IS_META_TYPE_FUNC type_func = OB_MULTI_VERSION_EXTRA_ROWKEY[i].is_meta_type_func_;
    bret = OB_MULTI_VERSION_EXTRA_ROWKEY[i].column_index_ == meta.column_id_array_[col_store_idx] &&
           (meta.column_type_array_[col_store_idx].*type_func)();
  }
  return bret;
}

int ObMultiVersionRowkeyHelpper::convert_multi_version_iter_param(const ObTableIterParam& param, const bool is_get,
    ObColDescArray& store_out_cols, const int64_t multi_version_rowkey_cnt,
    ObIArray<int32_t>* store_projector /*= NULL*/)
{
  int ret = OB_SUCCESS;
  const int64_t schema_rowkey_col_cnt = param.rowkey_cnt_;
  const ObColDescIArray* out_cols = nullptr;
  const ObIArray<int32_t>* projector = nullptr;

  share::schema::ObColDesc desc;
  int32_t i = 0;
  // add schema rowkey columns
  if (OB_FAIL(param.get_out_cols(is_get, out_cols))) {
    STORAGE_LOG(WARN, "fail to get out cols", K(ret));
  } else if (OB_FAIL(param.get_projector(is_get, projector))) {
    STORAGE_LOG(WARN, "fail to get projector", K(ret));
  }
  const bool need_build_store_projector = NULL != projector && NULL != store_projector;
  for (; OB_SUCC(ret) && i < schema_rowkey_col_cnt; i++) {
    desc = out_cols->at(i);
    if (OB_FAIL(store_out_cols.push_back(desc))) {
      STORAGE_LOG(WARN, "add store utput columns failed", K(ret));
    } else if (need_build_store_projector && OB_FAIL(store_projector->push_back(projector->at(i)))) {
      STORAGE_LOG(WARN, "store_projector push back failed", K(ret), K(i));
    }
  }
  // add multi version extra rowkey columns
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_extra_rowkey_cols(
            store_out_cols, i, need_build_store_projector, multi_version_rowkey_cnt, store_projector))) {
      STORAGE_LOG(WARN, "fail to add extra rowkey columns", K(ret), K(i));
    }
  }
  // add rest columns
  int32_t idx = 0;
  for (; OB_SUCC(ret) && i < out_cols->count() + multi_version_rowkey_cnt; i++) {
    desc = out_cols->at(i - multi_version_rowkey_cnt);
    if (OB_FAIL(store_out_cols.push_back(desc))) {
      STORAGE_LOG(WARN, "add store utput columns failed", K(ret));
    } else if (need_build_store_projector) {
      idx = projector->at(i - multi_version_rowkey_cnt) == OB_INVALID_INDEX
                ? OB_INVALID_INDEX
                : projector->at(i - multi_version_rowkey_cnt) + multi_version_rowkey_cnt;
      if (OB_FAIL(store_projector->push_back(idx))) {
        STORAGE_LOG(WARN, "store_projector push back failed", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(ObColDescIArray& store_out_cols, int32_t& index,
    const bool need_build_store_projector, const int64_t multi_version_rowkey_cnt, ObIArray<int32_t>* store_projector)
{
  int ret = OB_SUCCESS;
  if (index < 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "add extra rowkey cols index should NOT small than 0", K(index));
  } else {
    share::schema::ObColDesc desc;
    int32_t i = 0;
    for (; OB_SUCC(ret) && i < multi_version_rowkey_cnt; ++i) {
      const ObMultiVersionExtraRowkey& mv_ext_rowkey = OB_MULTI_VERSION_EXTRA_ROWKEY[i];
      desc.col_id_ = mv_ext_rowkey.column_index_;
      (desc.col_type_.*mv_ext_rowkey.set_meta_type_func_)();
      // by default the trans_version column value would be multiplied by -1
      // so in effect we store the lastest version first
      desc.col_order_ = ObOrderType::ASC;
      if (OB_FAIL(store_out_cols.push_back(desc))) {
        STORAGE_LOG(WARN, "add store utput columns failed", K(ret));
      } else if (need_build_store_projector && OB_FAIL(store_projector->push_back(index + i))) {
        STORAGE_LOG(WARN, "store_projector push back failed", K(ret), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      index += i;
    }
  }
  return ret;
}

int ObStoreCtx::get_snapshot_info(ObTransSnapInfo& snap_info) const
{
  int ret = OB_SUCCESS;
  if (snapshot_info_.is_valid()) {
    snap_info = snapshot_info_;
  } else if (NULL == mem_ctx_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "memtable context is NULL", K(ret), K(*this));
  } else {
    snap_info.set_snapshot_version(mem_ctx_->get_read_snapshot());
    snap_info.set_read_sql_no(stmt_min_sql_no_);
  }
  return ret;
}

int ObStoreCtx::init_trans_ctx_mgr(const ObPartitionKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_ctx_) || OB_ISNULL(mem_ctx_->get_trans_table_guard())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "memtable ctx is null, unexpected error", K(ret), KP(mem_ctx_));
  } else {
    trans_table_guard_ = mem_ctx_->get_trans_table_guard();
    ObTransService* trans_service = nullptr;
    if (OB_ISNULL(trans_service = ObPartitionService::get_instance().get_trans_service())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trans_service is null", K(ret));
    } else if (OB_FAIL(trans_service->get_part_trans_ctx_mgr().get_partition_trans_ctx_mgr_with_ref(
                   pg_key, *trans_table_guard_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get_partition_trans_ctx_mgr", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

ObTableAccessStat::ObTableAccessStat()
    : row_cache_hit_cnt_(0),
      row_cache_miss_cnt_(0),
      row_cache_put_cnt_(0),
      bf_filter_cnt_(0),
      bf_access_cnt_(0),
      empty_read_cnt_(0),
      block_cache_hit_cnt_(0),
      block_cache_miss_cnt_(0),
      sstable_bf_filter_cnt_(0),
      sstable_bf_empty_read_cnt_(0),
      sstable_bf_access_cnt_(0),
      fuse_row_cache_hit_cnt_(0),
      fuse_row_cache_miss_cnt_(0),
      fuse_row_cache_put_cnt_(0),
      rowkey_prefix_(0)
{}

void ObTableAccessStat::reset()
{
  row_cache_hit_cnt_ = 0;
  row_cache_miss_cnt_ = 0;
  row_cache_put_cnt_ = 0;
  bf_filter_cnt_ = 0;
  bf_access_cnt_ = 0;
  empty_read_cnt_ = 0;
  block_cache_hit_cnt_ = 0;
  block_cache_miss_cnt_ = 0;
  sstable_bf_filter_cnt_ = 0;
  sstable_bf_empty_read_cnt_ = 0;
  sstable_bf_access_cnt_ = 0;
  fuse_row_cache_hit_cnt_ = 0;
  fuse_row_cache_miss_cnt_ = 0;
  fuse_row_cache_put_cnt_ = 0;
  rowkey_prefix_ = 0;
}

void ObGetTableParam::reset()
{
  partition_store_ = NULL;
  frozen_version_ = -1;
  sample_info_.reset();
  tables_handle_ = NULL;
}

ObTableIterParam::ObTableIterParam()
    : table_id_(0),
      schema_version_(0),
      rowkey_cnt_(0),
      out_cols_(NULL),
      cols_id_map_(NULL),
      projector_(NULL),
      full_projector_(NULL),
      out_cols_project_(NULL),
      out_cols_param_(NULL),
      full_out_cols_param_(NULL),
      is_multi_version_minor_merge_(false),
      full_out_cols_(NULL),
      full_cols_id_map_(NULL),
      need_scn_(false),
      iter_mode_(OIM_ITER_FULL)
{}

ObTableIterParam::~ObTableIterParam()
{}

int ObTableIterParam::get_out_cols(const bool is_get, const ObColDescIArray*& out_cols) const
{
  int ret = OB_SUCCESS;
  if (is_get) {
    out_cols = nullptr == full_out_cols_ ? out_cols_ : full_out_cols_;
  } else {
    out_cols = out_cols_;
  }
  if (nullptr != out_cols) {
    STORAGE_LOG(DEBUG, "get out cols", K(*out_cols), K(common::lbt()));
  }
  return ret;
}

int ObTableIterParam::get_column_map(const bool is_get, const share::schema::ColumnMap*& column_map) const
{
  int ret = OB_SUCCESS;
  if (is_get) {
    column_map = nullptr == full_out_cols_ ? cols_id_map_ : full_cols_id_map_;
  } else {
    column_map = cols_id_map_;
  }
  return ret;
}

int ObTableIterParam::get_projector(const bool is_get, const ObIArray<int32_t>*& projector) const
{
  int ret = OB_SUCCESS;
  if (is_get) {
    projector = nullptr == full_out_cols_ ? projector_ : full_projector_;
  } else {
    projector = projector_;
  }
  if (nullptr != projector) {
    STORAGE_LOG(DEBUG, "get projector", K(*projector), K(common::lbt()));
  }
  return ret;
}

int ObTableIterParam::get_out_cols_param(
    const bool is_get, const ObIArray<share::schema::ObColumnParam*>*& out_cols_param) const
{
  int ret = OB_SUCCESS;
  if (is_get) {
    out_cols_param = nullptr == full_out_cols_ ? out_cols_param_ : full_out_cols_param_;
  } else {
    out_cols_param = out_cols_param_;
  }
  if (nullptr != out_cols_param) {
    STORAGE_LOG(DEBUG, "get out col params", K(*out_cols_param), K(common::lbt()));
  }
  return ret;
}

void ObTableIterParam::reset()
{
  table_id_ = 0;
  schema_version_ = 0;
  rowkey_cnt_ = 0;
  out_cols_ = NULL;
  cols_id_map_ = NULL;
  projector_ = NULL;
  full_projector_ = NULL;
  out_cols_project_ = NULL;
  out_cols_param_ = NULL;
  full_out_cols_param_ = NULL;
  is_multi_version_minor_merge_ = false;
  full_out_cols_ = NULL;
  full_cols_id_map_ = NULL;
  need_scn_ = false;
  iter_mode_ = OIM_ITER_FULL;
}

bool ObTableIterParam::is_valid() const
{
  return OB_INVALID_ID != table_id_ && OB_INVALID_VERSION != schema_version_ && rowkey_cnt_ > 0 &&
         OB_NOT_NULL(out_cols_) && out_cols_->count() > 0;
}

int ObTableIterParam::has_lob_column_out(const bool is_get, bool& has_lob_column) const
{
  int ret = OB_SUCCESS;
  const ObColDescIArray* out_cols = nullptr;
  has_lob_column = false;
  if (OB_FAIL(get_out_cols(is_get, out_cols))) {
    STORAGE_LOG(WARN, "failt to get out cols", K(ret));
  } else if (OB_NOT_NULL(out_cols)) {
    for (int64_t i = 0; !has_lob_column && i < out_cols->count(); i++) {
      has_lob_column = out_cols->at(i).col_type_.is_lob();
    }
  }
  return ret;
}

bool ObTableIterParam::enable_fuse_row_cache() const
{
  bool bret = nullptr != full_out_cols_;
  if (bret) {
    for (int64_t i = 0; bret && i < full_out_cols_->count(); i++) {
      bret = !full_out_cols_->at(i).col_type_.is_lob();
    }
  }
  bret = bret && !need_scn_;
  return bret;
}

bool ObTableIterParam::need_build_column_map(int64_t schema_version) const
{
  return NULL == projector_ || schema_version_ != schema_version || 0 == schema_version;
}

int ObColDescArrayParam::init(const ObColDescIArray* ref)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    is_local_ = nullptr == ref;
    ref_cols_ = ref;
    if (!is_local_) {
      local_cols_.reset();
      col_cnt_ = ref->count();
    } else {
      col_cnt_ = 0;
    }
    is_inited_ = true;
  }
  return ret;
}

void ObColDescArrayParam::reset()
{
  local_cols_.reuse();
  ref_cols_ = nullptr;
  col_cnt_ = 0;
  is_local_ = false;
  is_inited_ = false;
}

int ObColDescArrayParam::assign(const ObColDescIArray& other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_local_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref column array shouldn't be assigned", K(ret), K(is_local_), KP(ref_cols_));
  } else if (OB_FAIL(local_cols_.assign(other))) {
    LOG_WARN("assign fail", K(ret), K(is_local_), K(local_cols_));
  } else {
    col_cnt_ = local_cols_.count();
  }
  return ret;
}

int ObColDescArrayParam::push_back(const share::schema::ObColDesc& desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_local_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref column array shouldn't be assigned", K(ret), K(is_local_), KP(ref_cols_));
  } else if (OB_FAIL(local_cols_.push_back(desc))) {
    LOG_WARN("push back fail", K(ret), K(is_local_), K(local_cols_), K(desc));
  } else {
    col_cnt_ = local_cols_.count();
  }
  return ret;
}

const ObColDescIArray& ObColDescArrayParam::get_col_descs() const
{
  if (IS_NOT_INIT) {
    LOG_WARN("not init");
    return local_cols_;
  } else if (!is_local_) {
    if (nullptr == ref_cols_) {
      LOG_WARN("ref columns is null");
      return local_cols_;
    } else {
      return *ref_cols_;
    }
  } else {
    return local_cols_;
  }
}

ObTableAccessParam::ObTableAccessParam()
    : iter_param_(),
      reserve_cell_cnt_(0),
      out_col_desc_param_(),
      full_out_cols_(nullptr),
      out_cols_param_(NULL),
      index_back_project_(NULL),
      padding_cols_(NULL),
      filters_(NULL),
      virtual_column_exprs_(NULL),
      index_projector_(NULL),
      projector_size_(0),
      output_exprs_(NULL),
      op_(NULL),
      op_filters_(NULL),
      row2exprs_projector_(NULL),
      join_key_project_(NULL),
      right_key_project_(NULL),
      fast_agg_project_(NULL),
      enable_fast_skip_(false),
      need_fill_scale_(false),
      col_scale_info_()
{}

ObTableAccessParam::~ObTableAccessParam()
{}

void ObTableAccessParam::reset()
{
  iter_param_.reset();
  reserve_cell_cnt_ = 0;
  out_col_desc_param_.reset();
  full_out_cols_ = nullptr;
  out_cols_param_ = NULL;
  index_back_project_ = NULL;
  padding_cols_ = NULL;
  filters_ = NULL;
  virtual_column_exprs_ = NULL;
  index_projector_ = NULL;
  projector_size_ = 0;
  output_exprs_ = NULL;
  op_ = NULL;
  op_filters_ = NULL;
  row2exprs_projector_ = NULL;
  join_key_project_ = NULL;
  right_key_project_ = NULL;
  fast_agg_project_ = NULL;
  enable_fast_skip_ = false;
  need_fill_scale_ = false;
  col_scale_info_.reset();
}

int ObTableAccessParam::init(ObTableScanParam& scan_param, const bool is_mv)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableParam& table_param = *scan_param.table_param_;
  if (OB_FAIL(out_col_desc_param_.init(&table_param.get_col_descs()))) {
    LOG_WARN("init out cols fail", K(ret));
  } else {
    iter_param_.reset();
    iter_param_.table_id_ = sql::ObSQLMockSchemaUtils::is_mock_index(table_param.get_table_id())
                                ? sql::ObSQLMockSchemaUtils::get_baseid_from_rowid_index_id(table_param.get_table_id())
                                : table_param.get_table_id();
    iter_param_.schema_version_ = table_param.get_schema_version();
    iter_param_.rowkey_cnt_ = table_param.get_main_rowkey_cnt();
    iter_param_.cols_id_map_ = &(table_param.get_column_map());
    iter_param_.full_cols_id_map_ = &(table_param.get_full_column_map());
    iter_param_.projector_ = is_mv ? iter_param_.projector_ : &table_param.get_projector();
    iter_param_.out_cols_project_ = &table_param.get_output_projector();
    iter_param_.out_cols_ = &out_col_desc_param_.get_col_descs();
    iter_param_.full_projector_ = &table_param.get_full_projector();
    iter_param_.need_scn_ = scan_param.need_scn_;
    iter_param_.out_cols_param_ = &table_param.get_columns();
    iter_param_.full_out_cols_param_ = &table_param.get_full_columns();

    reserve_cell_cnt_ = scan_param.reserved_cell_count_;
    filters_ = 0 == scan_param.filters_.count() ? NULL : &scan_param.filters_;
    padding_cols_ = &table_param.get_pad_col_projector();
    out_cols_param_ = &table_param.get_columns();
    virtual_column_exprs_ = &scan_param.virtual_column_exprs_;
    index_projector_ = scan_param.index_projector_;
    projector_size_ = scan_param.projector_size_;

    output_exprs_ = scan_param.output_exprs_;
    op_ = scan_param.op_;
    op_filters_ = scan_param.op_filters_;
    row2exprs_projector_ = scan_param.row2exprs_projector_;
    enable_fast_skip_ = false;

    if (is_mv) {
      join_key_project_ = &table_param.get_join_key_projector();
      right_key_project_ = &table_param.get_right_key_projector();
    }
    if (0 == table_param.get_columns().count()) {
      STORAGE_LOG(WARN, "The Column count is 0, ", K(table_param), K(lbt()));
    }

    // todo for fast agg scan, init iter_param_.fast_agg_project_
    if (OB_SUCC(ret)) {
      full_out_cols_ = &table_param.get_full_col_descs();
      iter_param_.full_out_cols_ = full_out_cols_->count() > 0 ? full_out_cols_ : nullptr;
      if (OB_FAIL(init_column_scale_info())) {
        LOG_WARN("init column scale info failed", K(ret));
      }
    }
  }

  return ret;
}

int ObTableAccessParam::init(const uint64_t table_id, const int64_t schema_version, const int64_t rowkey_column_num,
    const ObIArray<share::schema::ObColDesc>& column_ids, const bool is_multi_version_minor_merge,
    share::schema::ObTableParam* table_param, const bool is_mv_right_table)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(out_col_desc_param_.init())) {
    LOG_WARN("init out cols failed", K(ret), KP(table_param));
  } else if (OB_FAIL(out_col_desc_param_.assign(column_ids))) {
    LOG_WARN("assign out cols failed", K(ret), KP(table_param));
  } else {
    iter_param_.reset();
    iter_param_.table_id_ = table_id;
    iter_param_.schema_version_ = schema_version;
    iter_param_.rowkey_cnt_ = rowkey_column_num;
    iter_param_.is_multi_version_minor_merge_ = is_multi_version_minor_merge;
    iter_param_.out_cols_ = &out_col_desc_param_.get_col_descs();
    iter_param_.full_out_cols_ = nullptr != full_out_cols_ && full_out_cols_->count() > 0 ? full_out_cols_ : nullptr;

    if (NULL != table_param) {
      iter_param_.cols_id_map_ = &table_param->get_column_map();
      iter_param_.full_cols_id_map_ = &table_param->get_full_column_map();
      iter_param_.out_cols_project_ = &table_param->get_output_projector();
      iter_param_.full_projector_ = &table_param->get_full_projector();
      iter_param_.out_cols_param_ = &table_param->get_columns();
      iter_param_.full_out_cols_param_ = &table_param->get_full_columns();
      out_cols_param_ = &table_param->get_columns();
      enable_fast_skip_ = false;
      if (is_mv_right_table) {
        join_key_project_ = &table_param->get_join_key_projector();
        right_key_project_ = &table_param->get_right_key_projector();
      }
    }
  }
  return ret;
}

int ObTableAccessParam::init_basic_param(const uint64_t table_id, const int64_t schema_version,
    const int64_t rowkey_column_num, const ObIArray<share::schema::ObColDesc>& column_ids,
    const ObIArray<int32_t>* out_cols_index)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out_col_desc_param_.init(nullptr))) {
    LOG_WARN("init out cols fail", K(ret));
  } else if (OB_FAIL(out_col_desc_param_.assign(column_ids))) {
    LOG_WARN("assign out cols fail", K(ret), K(column_ids));
  } else {
    enable_fast_skip_ = false;
    iter_param_.reset();
    iter_param_.table_id_ = table_id;
    iter_param_.schema_version_ = schema_version;
    iter_param_.rowkey_cnt_ = rowkey_column_num;
    iter_param_.out_cols_project_ = out_cols_index;
    iter_param_.out_cols_ = &out_col_desc_param_.get_col_descs();
    iter_param_.full_out_cols_ = nullptr;
  }
  return ret;
}

int ObTableAccessParam::init_index_back(ObTableScanParam& scan_param)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableParam& table_param = *scan_param.table_param_;
  if (OB_FAIL(out_col_desc_param_.init(&table_param.get_index_col_descs()))) {
    LOG_WARN("init out cols fail", K(ret));
  } else {
    iter_param_.table_id_ = table_param.get_index_id();
    iter_param_.schema_version_ = table_param.get_index_schema_version();
    iter_param_.rowkey_cnt_ = table_param.get_index_rowkey_cnt();
    iter_param_.cols_id_map_ = &(table_param.get_index_column_map());
    iter_param_.full_cols_id_map_ = &(table_param.get_index_column_map());
    iter_param_.out_cols_project_ = &table_param.get_index_output_projector();
    iter_param_.projector_ = &table_param.get_index_projector();
    iter_param_.out_cols_ = &out_col_desc_param_.get_col_descs();
    iter_param_.out_cols_param_ = &table_param.get_index_columns();
    iter_param_.full_out_cols_param_ = &table_param.get_full_columns();

    reserve_cell_cnt_ = scan_param.reserved_cell_count_;
    index_back_project_ = &table_param.get_index_back_projector();
    if (scan_param.filters_before_index_back_.count() > 0) {
      filters_ = &scan_param.filters_before_index_back_;
    }
    out_cols_param_ = &table_param.get_index_columns();

    output_exprs_ = scan_param.output_exprs_;
    op_ = scan_param.op_;
    op_filters_ = scan_param.op_filters_before_index_back_;
    row2exprs_projector_ = scan_param.row2exprs_projector_;
    enable_fast_skip_ = false;

    if (OB_SUCC(ret)) {
      iter_param_.full_out_cols_ = nullptr;
      iter_param_.full_projector_ = &table_param.get_full_projector();
    }
  }
  return ret;
}

int ObTableAccessParam::init_column_scale_info()
{
  int ret = OB_SUCCESS;
  need_fill_scale_ = false;
  col_scale_info_.reset();
  const ObIArray<share::schema::ObColumnParam*>* out_col_param = NULL;
  const ObIArray<int32_t>* out_col_project = NULL;
  if (OB_ISNULL(out_cols_param_) || OB_ISNULL(out_col_project = iter_param_.out_cols_project_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The out cols param is NULL, ", K(ret), K(out_col_param));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < out_col_project->count(); ++i) {
      share::schema::ObColumnParam* col_param = NULL;
      int32_t idx = out_col_project->at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= out_cols_param_->count())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid project idx", K(ret), K(idx), K(out_cols_param_->count()));
      } else if (OB_ISNULL(col_param = out_cols_param_->at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "column param is null", K(ret), K(idx));
      } else if (col_param->get_meta_type().is_interval_type()) {
        need_fill_scale_ = true;
        if (OB_FAIL(col_scale_info_.push_back(std::make_pair(i, col_param->get_accuracy().get_scale())))) {
          STORAGE_LOG(WARN, "array push back failed", K(ret));
        }
      }
    }
    STORAGE_LOG(DEBUG,
        "check need fill scale",
        K(ret),
        K(need_fill_scale_),
        K(col_scale_info_),
        KPC(out_col_project),
        KPC(out_cols_param_));
  }
  return ret;
}

ObLobLocatorHelper::ObLobLocatorHelper()
    : table_id_(OB_INVALID_ID),
      snapshot_version_(0),
      rowid_version_(ObURowIDData::INVALID_ROWID_VERSION),
      rowid_project_(nullptr),
      rowid_objs_(),
      locator_allocator_(),
      is_inited_(false)
{}

ObLobLocatorHelper::~ObLobLocatorHelper()
{}

void ObLobLocatorHelper::reset()
{
  table_id_ = OB_INVALID_ID;
  snapshot_version_ = 0;
  rowid_version_ = ObURowIDData::INVALID_ROWID_VERSION;
  rowid_project_ = nullptr;
  rowid_objs_.reset();
  locator_allocator_.reset();
  is_inited_ = false;
}

int ObLobLocatorHelper::init(const share::schema::ObTableParam& table_param, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLobLocatorHelper init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(!table_param.use_lob_locator() || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObLobLocatorHelper", K(ret), K(table_param), K(snapshot_version));
  } else if (OB_UNLIKELY(!share::is_oracle_mode() || is_sys_table(table_param.get_table_id()))) {
    // only oracle mode and user table support lob locator
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected tenant mode to init ObLobLocatorHelper", K(ret), K(table_param));
  } else if (table_param.get_rowid_version() != ObURowIDData::INVALID_ROWID_VERSION &&
             table_param.get_rowid_projector().empty()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected empty rowid projector", K(ret), K(table_param));
  } else {
    rowid_version_ = table_param.get_rowid_version();
    rowid_project_ = &table_param.get_rowid_projector();
    table_id_ = table_param.get_table_id();
    snapshot_version_ = snapshot_version;
    is_inited_ = true;
  }

  return ret;
}

int ObLobLocatorHelper::fill_lob_locator(
    common::ObNewRow& row, bool is_projected_row, const ObTableAccessParam& access_param)
{
  int ret = OB_SUCCESS;
  const ObColDescIArray* col_descs = access_param.iter_param_.out_cols_;
  const common::ObIArray<int32_t>* out_project = access_param.iter_param_.out_cols_project_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobLocatorHelper is not init", K(ret), K(*this));
  } else if (OB_UNLIKELY(nullptr == col_descs || nullptr == out_project)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to fill lob locator", K(ret), KP(col_descs), KP(out_project));
  } else if (!share::is_oracle_mode() || is_sys_table(access_param.iter_param_.table_id_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Only oracle mode need build lob locator", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "start to fill lob locator", K(row));
    // ObLobLocatorHelper is inited, we always cound find a lob cell in projected row
    locator_allocator_.reuse();
    common::ObString rowid;
    if (OB_FAIL(build_rowid_obj(row, rowid, is_projected_row, *out_project))) {
      STORAGE_LOG(WARN, "Failed to build rowid obj", K(ret), K(rowid));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < out_project->count(); i++) {
        int64_t obj_idx = is_projected_row ? i : out_project->at(i);
        if (obj_idx < 0 || obj_idx >= row.count_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN,
              "Unexpected out project idx",
              K(ret),
              K(i),
              K(obj_idx),
              K(is_projected_row),
              KPC(out_project),
              K(row));
        } else {
          ObObj& obj = row.cells_[obj_idx];
          if (obj.is_lob()) {
            int32_t idx = out_project->at(i);
            if (idx < 0 || idx >= col_descs->count()) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "Unexpected out project idx", K(ret), K(i), K(idx), KPC(col_descs));
            } else if (OB_FAIL(build_lob_locator(obj, col_descs->at(idx).col_id_, rowid))) {
              STORAGE_LOG(WARN, "Failed to build lob locator", K(ret), K(obj), K(rowid));
            } else {
              STORAGE_LOG(DEBUG, "succ to fill lob locator", K(obj));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObLobLocatorHelper::build_rowid_obj(common::ObNewRow& row, common::ObString& rowid_str, bool is_projected_row,
    const common::ObIArray<int32_t>& out_project)
{
  int ret = OB_SUCCESS;
  rowid_objs_.reset();
  rowid_str.reset();
  if (rowid_version_ == ObURowIDData::INVALID_ROWID_VERSION) {
    // use empty string for rowid
    //
  } else if (OB_ISNULL(rowid_project_) || rowid_project_->empty()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null or empty rowid project", K(ret), K(*this));
  } else {
    if (is_projected_row) {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowid_project_->count(); i++) {
        int64_t idx = rowid_project_->at(i);
        if (OB_UNLIKELY(idx < 0 || idx >= row.count_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpecte column project idx", K(ret), K(idx));
        } else if (OB_FAIL(rowid_objs_.push_back(row.cells_[idx]))) {
          STORAGE_LOG(WARN, "Failed to push back rowid object", K(ret), K(idx), K(row));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowid_project_->count(); i++) {
        int64_t idx = rowid_project_->at(i);
        if (OB_UNLIKELY(idx < 0 || idx >= out_project.count())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpecte column project idx", K(ret), K(idx), K(out_project));
        } else if (FALSE_IT(idx = out_project.at(idx))) {
        } else if (OB_UNLIKELY(idx < 0 || idx >= row.count_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpecte column project idx", K(ret), K(idx), K(row));
        } else if (OB_FAIL(rowid_objs_.push_back(row.cells_[idx]))) {
          STORAGE_LOG(WARN, "Failed to push back rowid object", K(ret), K(idx), K(row));
        }
      }
    }

    if (OB_SUCC(ret)) {
      common::ObURowIDData rowid;
      int64_t rowid_buf_size = 0;
      int64_t rowid_str_size = 0;
      char* rowid_buf = nullptr;
      if (OB_FAIL(rowid.set_rowid_content(rowid_objs_, rowid_version_, locator_allocator_))) {
        STORAGE_LOG(WARN, "Failed to set rowid content", K(ret), K(*this));
      } else if (FALSE_IT(rowid_buf_size = rowid.needed_base64_buffer_size())) {
      } else if (OB_ISNULL(rowid_buf = reinterpret_cast<char*>(locator_allocator_.alloc(rowid_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for rowid base64 str", K(ret), K(rowid_buf_size));
      } else if (OB_FAIL(rowid.get_base64_str(rowid_buf, rowid_buf_size, rowid_str_size))) {
        STORAGE_LOG(WARN, "Failed to get rowid base64 string", K(ret));
      } else {
        rowid_str.assign_ptr(rowid_buf, rowid_str_size);
      }
    }
  }
  STORAGE_LOG(DEBUG, "build rowid for lob locator", K(ret), K(rowid_str));

  return ret;
}

int ObLobLocatorHelper::build_lob_locator(
    common::ObObj& lob_obj, const uint64_t column_id, const common::ObString& rowid_str)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!lob_obj.is_lob_inrow() || !is_valid_id(column_id))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build lob locator", K(ret), K(lob_obj), K(column_id));
  } else {
    int64_t locator_size = 0;
    char* buf = nullptr;
    ObLobLocator* locator = nullptr;
    ObString payload;
    if (OB_FAIL(lob_obj.get_string(payload))) {
      STORAGE_LOG(WARN, "Failed to get string from lob object", K(ret), K(lob_obj));
    } else if (FALSE_IT(locator_size = sizeof(ObLobLocator) + payload.length() + rowid_str.length())) {
    } else if (OB_ISNULL(buf = reinterpret_cast<char*>(locator_allocator_.alloc(locator_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory for lob locator", K(ret), K(locator_size));
    } else if (FALSE_IT(MEMSET(buf, 0, locator_size))) {
    } else if (FALSE_IT(locator = reinterpret_cast<ObLobLocator*>(buf))) {
    } else if (OB_FAIL(locator->init(table_id_, column_id, snapshot_version_, LOB_DEFAULT_FLAGS, rowid_str, payload))) {
      STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(*this), K(rowid_str), K(lob_obj));
    } else {
      lob_obj.set_lob_locator(*locator);
      STORAGE_LOG(DEBUG, "succ to build lob locator", K(lob_obj), KPC(locator));
    }
  }

  return ret;
}

ObTableAccessContext::ObTableAccessContext()
    : is_inited_(false),
      timeout_(0),
      pkey_(),
      query_flag_(),
      sql_mode_(0),
      store_ctx_(NULL),
      expr_ctx_(NULL),
      limit_param_(NULL),
      stmt_allocator_(NULL),
      allocator_(NULL),
      stmt_mem_(NULL),
      scan_mem_(NULL),
      table_scan_stat_(NULL),
      block_cache_ws_(NULL),
      access_stat_(),
      out_cnt_(0),
      is_end_(false),
      trans_version_range_(),
      row_filter_(NULL),
      use_fuse_row_cache_(false),
      fq_ctx_(nullptr),
      need_scn_(false),
      fuse_row_cache_hit_rate_(0),
      block_cache_hit_rate_(0),
      is_array_binding_(false),
      range_array_pos_(nullptr),
      range_array_cursor_(0),
      merge_log_ts_(INT_MAX),
      read_out_type_(MAX_ROW_STORE),
      lob_locator_helper_(nullptr)
{}

ObTableAccessContext::~ObTableAccessContext()
{
  if (OB_NOT_NULL(lob_locator_helper_)) {
    lob_locator_helper_->~ObLobLocatorHelper();
    if (OB_NOT_NULL(stmt_allocator_)) {
      stmt_allocator_->free(lob_locator_helper_);
    }
  }
}

int ObTableAccessContext::build_lob_locator_helper(
    ObTableScanParam& scan_param, const ObVersionRange& trans_version_range)
{
  int ret = OB_SUCCESS;
  void* buf = nullptr;

  if (OB_UNLIKELY(nullptr == scan_param.table_param_ || nullptr == stmt_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build lob locator helper", K(ret), K(scan_param), KP_(stmt_allocator));
  } else if (!scan_param.table_param_->use_lob_locator()) {
    // TODO: use_lob_locator flag will be set only when there are lob columns in out cols projector
    lob_locator_helper_ = nullptr;
  } else if (!share::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected tenant mode", K(ret));
  } else if (OB_ISNULL(buf = stmt_allocator_->alloc(sizeof(ObLobLocatorHelper)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for ObLobLocatorHelper", K(ret));
  } else if (FALSE_IT(lob_locator_helper_ = new (buf) ObLobLocatorHelper())) {
  } else if (OB_FAIL(lob_locator_helper_->init(*scan_param.table_param_, trans_version_range.snapshot_version_))) {
    STORAGE_LOG(
        WARN, "Failed to init lob locator helper", K(ret), KPC(scan_param.table_param_), K(trans_version_range));
  } else if (!lob_locator_helper_->is_valid()) {
    STORAGE_LOG(DEBUG, "destory invalid lob locator helper", KPC(lob_locator_helper_));
    lob_locator_helper_->~ObLobLocatorHelper();
    stmt_allocator_->free(buf);
    lob_locator_helper_ = nullptr;
  } else {
    STORAGE_LOG(DEBUG, "succ to init lob locator helper", KPC(lob_locator_helper_));
  }

  return ret;
}

int ObTableAccessContext::init(ObTableScanParam& scan_param, const ObStoreCtx& ctx,
    blocksstable::ObBlockCacheWorkingSet& block_cache_ws, const ObVersionRange& trans_version_range,
    const ObIStoreRowFilter* row_filter, const bool is_index_back)
{
  int ret = OB_SUCCESS;

  lib::MemoryContext& current_mem =
      (NULL == scan_param.iterator_mementity_) ? CURRENT_CONTEXT : *scan_param.iterator_mementity_;
  lib::ContextParam param;
  param
      .set_mem_attr(
          scan_param.pkey_.get_tenant_id(), common::ObModIds::OB_TABLE_SCAN_ITER, common::ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!scan_param.pkey_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "pkey", scan_param.pkey_);
  } else if (NULL != scan_mem_) {
    // reused, do nothing.
  } else if (OB_FAIL(current_mem.CREATE_CONTEXT(scan_mem_, param))) {
    LOG_WARN("fail to create entity", K(ret));
  }
  if (OB_SUCC(ret)) {
    stmt_mem_ = &current_mem;
    stmt_allocator_ = &stmt_mem_->get_arena_allocator();
    allocator_ = &scan_mem_->get_arena_allocator();
    pkey_ = scan_param.pkey_;
    query_flag_ = scan_param.scan_flag_;
    sql_mode_ = scan_param.sql_mode_;
    timeout_ = scan_param.timeout_;
    store_ctx_ = &ctx;
    if (!is_index_back) {  // for main table ctx
      table_scan_stat_ = &scan_param.main_table_scan_stat_;
      expr_ctx_ = &scan_param.expr_ctx_;
      limit_param_ = scan_param.limit_param_.is_valid() ? &scan_param.limit_param_ : NULL;
      if (scan_param.is_index_limit()) {
        limit_param_ = NULL;
      }
      if (scan_param.scan_flag_.is_index_back()) {

        query_flag_.scan_order_ = ObQueryFlag::Forward;
      }
    } else {  // for index table ctx
      table_scan_stat_ = &scan_param.idx_table_scan_stat_;
      if (scan_param.filters_before_index_back_.count() > 0) {
        expr_ctx_ = &scan_param.expr_ctx_;
      }
      if (scan_param.is_index_limit()) {
        limit_param_ = scan_param.limit_param_.is_valid() ? &scan_param.limit_param_ : NULL;
      }
    }
    table_scan_stat_->reset();
    block_cache_ws_ = &block_cache_ws;
    trans_version_range_ = trans_version_range;
    row_filter_ = row_filter;
    need_scn_ = scan_param.need_scn_;
    is_array_binding_ = scan_param.range_array_pos_.count() > 1;
    range_array_pos_ = &scan_param.range_array_pos_;
    is_inited_ = true;
    if (!is_index_back && OB_FAIL(build_lob_locator_helper(scan_param, trans_version_range))) {
      STORAGE_LOG(WARN, "Failed to build lob locator helper", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableAccessContext::init(const common::ObQueryFlag& query_flag, const ObStoreCtx& ctx,
    ObArenaAllocator& allocator, ObArenaAllocator& stmt_allocator, blocksstable::ObBlockCacheWorkingSet& block_cache_ws,
    const ObVersionRange& trans_version_range)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    query_flag_ = query_flag;
    store_ctx_ = &ctx;
    allocator_ = &allocator;
    stmt_allocator_ = &stmt_allocator;
    block_cache_ws_ = &block_cache_ws;
    trans_version_range_ = trans_version_range;
    pkey_ = ctx.cur_pkey_;
    lob_locator_helper_ = nullptr;
    is_inited_ = true;
  }
  return ret;
}
int ObTableAccessContext::init(const common::ObQueryFlag& query_flag, const ObStoreCtx& ctx,
    common::ObArenaAllocator& allocator, const common::ObVersionRange& trans_version_range)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    query_flag_ = query_flag;
    store_ctx_ = &ctx;
    allocator_ = &allocator;
    stmt_allocator_ = &allocator;
    block_cache_ws_ = NULL;
    trans_version_range_ = trans_version_range;
    pkey_ = ctx.cur_pkey_;
    lob_locator_helper_ = nullptr;
    is_inited_ = true;
  }
  return ret;
}

void ObTableAccessContext::reset()
{
  is_inited_ = false;
  timeout_ = 0;
  pkey_.reset();
  query_flag_.reset();
  sql_mode_ = 0;
  store_ctx_ = NULL;
  expr_ctx_ = NULL;
  limit_param_ = NULL;
  if (OB_NOT_NULL(lob_locator_helper_)) {
    lob_locator_helper_->~ObLobLocatorHelper();
    lob_locator_helper_ = nullptr;
    if (OB_NOT_NULL(stmt_allocator_)) {
      stmt_allocator_->free(lob_locator_helper_);
    }
  }
  stmt_allocator_ = NULL;
  allocator_ = NULL;
  stmt_mem_ = NULL;
  if (NULL != scan_mem_) {
    DESTROY_CONTEXT(scan_mem_);
    scan_mem_ = NULL;
  }
  table_scan_stat_ = NULL;
  block_cache_ws_ = NULL;
  access_stat_.reset();
  out_cnt_ = 0;
  is_end_ = false;
  trans_version_range_.reset();
  row_filter_ = NULL;
  use_fuse_row_cache_ = false;
  fq_ctx_ = nullptr;
  fuse_row_cache_hit_rate_ = 0;
  block_cache_hit_rate_ = 0;
  is_array_binding_ = false;
  range_array_pos_ = nullptr;
  range_array_cursor_ = 0;
  read_out_type_ = MAX_ROW_STORE;
}

void ObTableAccessContext::reuse()
{
  is_inited_ = false;
  timeout_ = 0;
  pkey_.reset();
  query_flag_.reset();
  sql_mode_ = 0;
  store_ctx_ = NULL;
  expr_ctx_ = NULL;
  limit_param_ = NULL;
  if (OB_NOT_NULL(lob_locator_helper_)) {
    lob_locator_helper_->~ObLobLocatorHelper();
    lob_locator_helper_ = nullptr;
    if (OB_NOT_NULL(stmt_allocator_)) {
      stmt_allocator_->free(lob_locator_helper_);
    }
  }
  stmt_allocator_ = NULL;
  allocator_ = NULL;
  stmt_mem_ = NULL;
  if (NULL != scan_mem_) {
    scan_mem_->reuse_arena();
  }
  table_scan_stat_ = NULL;
  block_cache_ws_ = NULL;
  out_cnt_ = 0;
  is_end_ = false;
  trans_version_range_.reset();
  row_filter_ = NULL;
  use_fuse_row_cache_ = false;
  fq_ctx_ = nullptr;
  fuse_row_cache_hit_rate_ = 0;
  block_cache_hit_rate_ = 0;
  is_array_binding_ = false;
  range_array_pos_ = nullptr;
  range_array_cursor_ = 0;
}

void ObStoreRowLockState::reset()
{
  is_locked_ = false;
  trans_version_ = 0;
  lock_trans_id_.reset();
}

OB_DEF_SERIALIZE(ObStoreRow)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      row_val_,
      flag_,
      dml_,
      is_get_,
      scan_index_,
      from_base_,
      row_type_flag_.flag_,
      first_dml_,
      is_sparse_row_);
  if (OB_SUCC(ret) && is_sparse_row_) {
    if (OB_ISNULL(column_ids_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "sparse row's column id is null", K(ret));
    } else {
      OB_UNIS_ENCODE_ARRAY(column_ids_, row_val_.count_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObStoreRow)
{
  int ret = OB_SUCCESS;
  int col_id_count = 0;
  LST_DO_CODE(OB_UNIS_DECODE,
      row_val_,
      flag_,
      dml_,
      is_get_,
      scan_index_,
      from_base_,
      row_type_flag_.flag_,
      first_dml_,
      is_sparse_row_);
  if (OB_SUCC(ret) && is_sparse_row_) {
    OB_UNIS_DECODE(col_id_count);
    if (OB_SUCC(ret)) {
      if (row_val_.count_ != col_id_count) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "column id count is not equal", K(ret), K(col_id_count), K(row_val_.count_));
      } else {
        OB_UNIS_DECODE_ARRAY(column_ids_, row_val_.count_);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObStoreRow)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      row_val_,
      flag_,
      dml_,
      is_get_,
      scan_index_,
      from_base_,
      row_type_flag_.flag_,
      first_dml_,
      is_sparse_row_);
  if (is_sparse_row_) {
    OB_UNIS_ADD_LEN_ARRAY(column_ids_, row_val_.count_);
  }
  return len;
}

void ObStoreRow::reset()
{
  row_val_.reset();
  flag_ = -1;
  capacity_ = 0;
  reset_dml();
  is_get_ = false;
  from_base_ = false;
  row_pos_flag_.reset();
  scan_index_ = 0;
  row_type_flag_.reset();
  is_sparse_row_ = false;
  column_ids_ = NULL;
  snapshot_version_ = 0;
  range_array_idx_ = 0;
  trans_id_ptr_ = NULL;
  fast_filter_skipped_ = false;
  last_purge_ts_ = 0;
}

int ObStoreRow::deep_copy(const ObStoreRow& src, char* buf, const int64_t len, int64_t& pos)
{
  int ret = common::OB_SUCCESS;

  if (NULL == buf || len <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(len), KP(buf));
  } else if (!src.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "src is not valid", K(ret));
  } else {
    flag_ = src.flag_;
    dml_ = src.dml_;
    first_dml_ = src.first_dml_;
    capacity_ = src.capacity_;
    is_get_ = src.is_get_;
    from_base_ = src.from_base_;
    row_pos_flag_ = src.row_pos_flag_;
    scan_index_ = src.scan_index_;
    row_type_flag_ = src.row_type_flag_;
    is_sparse_row_ = src.is_sparse_row_;
    trans_id_ptr_ = src.trans_id_ptr_;
    fast_filter_skipped_ = src.fast_filter_skipped_;
    if (OB_FAIL(row_val_.deep_copy(src.row_val_, buf, len, pos))) {
      STORAGE_LOG(WARN, "failed to deep copy row_val", K(ret));
    } else if (is_sparse_row_) {
      if (src.get_column_id_size() + pos > len) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        column_ids_ = reinterpret_cast<uint16_t*>(buf + pos);
        MEMCPY(column_ids_, src.column_ids_, src.get_column_id_size());
        pos += src.get_column_id_size();
      }
    }
  }
  return ret;
}

int64_t ObStoreRow::to_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;

  if (NULL != buffer && length >= 0) {

    common::databuff_printf(buffer, length, pos, "flag=%ld ", flag_);
    common::databuff_printf(buffer, length, pos, "first_dml=%d ", first_dml_);
    common::databuff_printf(buffer, length, pos, "dml=%d ", dml_);
    common::databuff_printf(buffer, length, pos, "capacity_=%ld ", capacity_);
    common::databuff_printf(buffer, length, pos, "is_get=%d ", is_get_);
    common::databuff_printf(buffer, length, pos, "from_base=%d ", from_base_);
    common::databuff_printf(buffer, length, pos, "trans_id=");
    common::databuff_print_obj(buffer, length, pos, trans_id_ptr_);
    common::databuff_printf(buffer, length, pos, " ");
    common::databuff_printf(buffer, length, pos, "row_pos_flag=%d ", row_pos_flag_.flag_);
    common::databuff_printf(buffer, length, pos, "scan_index=%ld ", scan_index_);
    common::databuff_printf(buffer, length, pos, "multi_version_row_flag=%d ", row_type_flag_.flag_);
    pos += row_type_flag_.to_string(buffer + pos, length - pos);
    common::databuff_printf(buffer, length, pos, "row_val={count=%ld,", row_val_.count_);
    common::databuff_printf(buffer, length, pos, "cells=[");
    if (NULL != row_val_.cells_) {
      for (int64_t i = 0; i < row_val_.count_; ++i) {
        common::databuff_print_obj(buffer, length, pos, row_val_.cells_[i]);
        common::databuff_printf(buffer, length, pos, ",");
      }
    }
    common::databuff_printf(buffer, length, pos, "] ");
    common::databuff_printf(buffer, length, pos, "is_sparse_row=%d ", is_sparse_row_);
    common::databuff_printf(buffer, length, pos, "snapshot_version=%ld ", snapshot_version_);
    common::databuff_printf(buffer, length, pos, "fast_filtered=%d ", fast_filter_skipped_);
    common::databuff_printf(buffer, length, pos, "range_array_idx=%ld ", range_array_idx_);
    common::databuff_printf(buffer, length, pos, "last_purge_ts=%ld ", last_purge_ts_);
    if (is_sparse_row_ && NULL != column_ids_) {
      common::databuff_printf(buffer, length, pos, "column_id=[");
      for (int64_t i = 0; i < row_val_.count_; ++i) {
        common::databuff_printf(buffer, length, pos, "%u,", column_ids_[i]);
      }
      common::databuff_printf(buffer, length, pos, "] ");
    }
  }
  return pos;
}

int ObMagicRowManager::is_magic_row(const storage::ObMultiVersionRowFlag& flag, bool& is_magic_row)
{
  int ret = OB_SUCCESS;
  is_magic_row = false;
  if (flag.is_magic_row()) {
    is_magic_row = true;
    if (OB_UNLIKELY(!flag.is_last_multi_version_row())) {
      ret = OB_ERR_UNEXPECTED;
      FLOG_ERROR("magic row should only be last row", K(ret), K(flag));
    }
  }
  return ret;
}

int ObMagicRowManager::make_magic_row(
    const int64_t sql_sequence_col_idx, const ObQueryFlag& query_flag, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((!query_flag.is_sstable_cut() &&
                      (!row.row_type_flag_.is_uncommitted_row() || OB_ISNULL(row.trans_id_ptr_))) ||
                  !row.row_type_flag_.is_last_multi_version_row() || row.row_val_.count_ < sql_sequence_col_idx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(row), K(sql_sequence_col_idx));
  } else {
    row.dml_ = T_DML_UNKNOWN;
    row.flag_ = ObActionFlag::OP_ROW_EXIST;
    row.row_type_flag_.reset();
    row.row_type_flag_.set_magic_row(true);
    row.row_type_flag_.set_last_multi_version_row(true);
    row.trans_id_ptr_ = NULL;
    row.row_val_.cells_[sql_sequence_col_idx - 1].set_int(MAGIC_NUM);
    row.row_val_.cells_[sql_sequence_col_idx].set_int(MAGIC_NUM);
    if (OB_UNLIKELY(row.is_sparse_row_)) {
      row.row_val_.count_ = sql_sequence_col_idx + 1;
    } else {  // flat row
      for (int i = sql_sequence_col_idx + 1; i < row.row_val_.count_; ++i) {
        row.row_val_.cells_[i].set_nop_value();
      }
    }
  }
  return ret;
}

void ObWorkRow::reset()
{
  ObStoreRow::reset();
  row_val_.cells_ = objs;
  row_val_.count_ = common::OB_ROW_MAX_COLUMNS_COUNT;
  flag_ = common::ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  capacity_ = common::OB_ROW_MAX_COLUMNS_COUNT;
}

ObRowsInfo::ExistHelper::ExistHelper() : table_iter_param_(), table_access_context_(), is_inited_(false)
{}

ObRowsInfo::ExistHelper::~ExistHelper()
{}

int ObRowsInfo::ExistHelper::init(const ObStoreCtx& store_ctx,
    const common::ObIArray<share::schema::ObColDesc>& col_descs, uint64_t table_id, int64_t rowkey_column_cnt,
    common::ObArenaAllocator& allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObExistPrefixScanHelper init twice", K(ret));
  } else if (OB_UNLIKELY(rowkey_column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObExistPrefixScanHelper", K(rowkey_column_cnt), K(ret));
  } else {
    common::ObQueryFlag query_flag;
    common::ObVersionRange trans_version_range;
    query_flag.read_latest_ = ObQueryFlag::OBSF_MASK_READ_LATEST;
    query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;

    trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;

    if (OB_FAIL(table_access_context_.init(query_flag, store_ctx, allocator, trans_version_range))) {
      LOG_WARN("failed to init table access ctx", K(ret));
    } else {
      table_iter_param_.table_id_ = table_id;
      table_iter_param_.schema_version_ = 0;
      table_iter_param_.rowkey_cnt_ = rowkey_column_cnt;
      table_iter_param_.out_cols_project_ = NULL;
      table_iter_param_.out_cols_ = &col_descs;
      is_inited_ = true;
    }
  }

  return ret;
}

ObRowsInfo::RowsCompare::RowsCompare(ObIArray<ObRowkeyObjComparer*>* cmp_funcs, ObStoreRowkey& dup_key,
    const bool check_dup, int16_t& max_prefix_length, int& ret)
    : cmp_funcs_(cmp_funcs), dup_key_(dup_key), check_dup_(check_dup), max_prefix_length_(max_prefix_length), ret_(ret)
{}

int32_t ObRowsInfo::RowsCompare::compare(const ObStoreRowkey& left, const ObStoreRowkey& right)
{
  int32_t cmp_ret = 0;
  const ObObj* obj1 = left.get_obj_ptr();
  const ObObj* obj2 = right.get_obj_ptr();
  // rowkey column count always equal
  int64_t cmp_cnt = left.get_obj_cnt();
  int64_t i = 0;
  if (!check_dup_) {
    if (left.is_max() || right.is_min()) {
      cmp_ret = common::ObObjCmpFuncs::CR_GT;
    } else if (left.is_min() || right.is_max()) {
      cmp_ret = common::ObObjCmpFuncs::CR_LT;
    }
  }
  for (; i < cmp_cnt && 0 == cmp_ret; i++) {
    if (OB_UNLIKELY(common::ObObjCmpFuncs::CR_OB_ERROR == (cmp_ret = cmp_funcs_->at(i)->compare(obj1[i], obj2[i])))) {
      STORAGE_LOG(
          ERROR, "failed to compare obj1 and obj2 using collation free", K(left), K(right), K(obj1[i]), K(obj2[i]));
      right_to_die_or_duty_to_live();
    }
  }
  if (check_dup_) {
    max_prefix_length_ = MIN(max_prefix_length_, i - (0 == cmp_ret ? 0 : 1));
  }
  return cmp_ret;
}

ObRowsInfo::ObRowsInfo()
    : rows_(nullptr),
      row_count_(0),
      ext_rowkeys_(),
      exist_helper_(),
      prefix_rowkey_(),
      table_id_(OB_INVALID_ID),
      min_key_(),
      scan_mem_(nullptr),
      delete_count_(0),
      collation_free_transformed_(false),
      rowkey_column_num_(0),
      max_prefix_length_(INT16_MAX),
      is_inited_(false)
{
  min_key_.set_max();
}

ObRowsInfo::~ObRowsInfo()
{
  if (NULL != scan_mem_) {
    DESTROY_CONTEXT(scan_mem_);
    scan_mem_ = NULL;
  }
}

// TODO  col_desc need to be removed, now use full col_descs, also can be reduced by rowkeycol
int ObRowsInfo::init(
    const ObRelativeTable& table, const ObStoreCtx& store_ctx, const ObIArray<share::schema::ObColDesc>& col_descs)
{
  int ret = OB_SUCCESS;
  lib::MemoryContext& current_mem = CURRENT_CONTEXT;

  lib::ContextParam param;
  param
      .set_mem_attr(
          store_ctx.cur_pkey_.get_tenant_id(), common::ObModIds::OB_STORE_ROW_EXISTER, common::ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObRowsinfo init twice", K(ret));
  } else if (OB_FAIL(current_mem.CREATE_CONTEXT(scan_mem_, param))) {
    LOG_WARN("fail to create entity", K(ret));
  } else if (OB_ISNULL(scan_mem_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null scan mem", K(ret));
  } else if (OB_FAIL(exist_helper_.init(store_ctx,
                 col_descs,
                 table.get_table_id(),
                 table.get_rowkey_column_num(),
                 scan_mem_->get_arena_allocator()))) {
    STORAGE_LOG(WARN, "Failed to init exist helper", K(ret));
  } else {
    table_id_ = table.get_table_id();
    rowkey_column_num_ = table.get_rowkey_column_num();
    is_inited_ = true;
  }
  return ret;
}

int ObRowsInfo::ensure_space(const int64_t row_count)
{
  int ret = OB_SUCCESS;

  if (row_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(row_count), K(ret));
  } else if (row_count > ext_rowkeys_.count()) {
    if (OB_FAIL(ext_rowkeys_.prepare_allocate(row_count))) {
      STORAGE_LOG(WARN, "Failed to reservob_resolver_define.he memory for ext_rowkeys", K(row_count), K(ret));
    }
  } else if (row_count < ext_rowkeys_.count()) {
    while (ext_rowkeys_.count() > row_count) {
      ext_rowkeys_.pop_back();
    }
  }
  if (OB_SUCC(ret)) {
    min_key_.set_max();
    rows_ = NULL;
    row_count_ = row_count;
    delete_count_ = 0;
    collation_free_transformed_ = false;
    max_prefix_length_ = INT16_MAX;
    prefix_rowkey_.reset();
    scan_mem_->get_arena_allocator().reuse();
  }

  return ret;
}

// not only checking duplicate, but also assign rowkeys
int ObRowsInfo::check_duplicate(ObStoreRow* rows, const int64_t row_count, const ObRelativeTable& table)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObExtStoreRowkey ext_rowkey;
  ObSSTable* sstable = nullptr;
  ObIArray<ObRowkeyObjComparer*>* rowkey_cmp_funcs = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObRowsInfo not init", K(ret));
  } else if (OB_ISNULL(rows) || row_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid parameter", K(rows), K(row_count), K(ret));
  } else if (OB_FAIL(ensure_space(row_count))) {
    STORAGE_LOG(WARN, "Failed to ensure space for rowsinfo", K(row_count), K(ret));
  } else if (ext_rowkeys_.count() != row_count) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "Unexpected extrowkey count",
        K(row_count),
        "rowkeys_count",
        ext_rowkeys_.count(),
        K_(ext_rowkeys),
        K(ret));
  } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = table.tables_handle_.get_first_sstable(sstable)))) {
    if (OB_ENTRY_NOT_EXIST != tmp_ret) {
      STORAGE_LOG(WARN, "Failed to get last major sstable", K(ret), K(table));
    }
    rowkey_cmp_funcs = nullptr;
  } else if (OB_ISNULL(sstable) || OB_UNLIKELY(!sstable->get_rowkey_helper().is_valid())) {
    rowkey_cmp_funcs = nullptr;
  } else if (OB_UNLIKELY(sstable->get_rowkey_helper().get_rowkey_column_num() < table.get_rowkey_column_num())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "Unexpected invalid sstable rowkey column number",
        K(ret),
        K(sstable->get_rowkey_helper()),
        K(table),
        K(*sstable));
  } else {
    rowkey_cmp_funcs = &sstable->get_rowkey_helper().get_compare_funcs();
  }
  if (OB_SUCC(ret)) {
    rows_ = rows;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      if (OB_UNLIKELY(!rows[i].is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid argument", K(rows_[i]), K(ret));
      } else {
        ext_rowkeys_.at(i).reset();
        ext_rowkeys_.at(i).get_store_rowkey().assign(rows_[i].row_val_.cells_, rowkey_column_num_);
      }
    }
    if (OB_SUCC(ret)) {
      if (row_count > 1) {
        RowsCompare rows_cmp(rowkey_cmp_funcs, min_key_, true, max_prefix_length_, ret);
        std::sort(ext_rowkeys_.begin(), ext_rowkeys_.end(), rows_cmp);
      } else {
        max_prefix_length_ = 0;
      }
      if (OB_SUCC(ret)) {
        min_key_ = ext_rowkeys_.at(0).get_store_rowkey();
        if (row_count > 2 && max_prefix_length_ > 0 && max_prefix_length_ < rowkey_column_num_ &&
            max_prefix_length_ > rowkey_column_num_ / 2) {
          // use prefix rowkey optimization with large prefix length
          prefix_rowkey_.get_store_rowkey().assign(
              ext_rowkeys_.at(0).get_store_rowkey().get_obj_ptr(), max_prefix_length_);
          if (OB_FAIL(prefix_rowkey_.to_collation_free_on_demand_and_cutoff_range(scan_mem_->get_arena_allocator()))) {
            STORAGE_LOG(WARN, "Failed to transform collation free and cutoff range", K(ret));
          }
        } else {
          max_prefix_length_ = 0;
        }
        STORAGE_LOG(DEBUG,
            "Duplicate check complete",
            K(ret),
            K(max_prefix_length_),
            K(row_count),
            K_(rowkey_column_num),
            KP(rowkey_cmp_funcs));
      }
    }
  }

  return ret;
}

int ObRowsInfo::refine_ext_rowkeys(ObSSTable* sstable)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRowkeyObjComparer*>* rowkey_cmp_funcs = nullptr;

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected not init rowsinfo", K_(row_count), K_(delete_count), KP_(rows), K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to refine ext rowkeys", KP(sstable), K(ret));
  } else if (row_count_ == 0 || ext_rowkeys_.count() == delete_count_ || ext_rowkeys_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected ext_rowkey status", K_(row_count), K_(delete_count), K_(ext_rowkeys), K(ret));
  } else if (OB_UNLIKELY(!sstable->get_rowkey_helper().is_valid())) {
    rowkey_cmp_funcs = nullptr;
  } else if (OB_UNLIKELY(sstable->get_rowkey_helper().get_rowkey_column_num() < rowkey_column_num_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "Unexpected invalid sstable rowkey column number", K(ret), K(sstable->get_rowkey_helper()), K(*sstable));
  } else {
    rowkey_cmp_funcs = &sstable->get_rowkey_helper().get_compare_funcs();
  }
  if (OB_SUCC(ret)) {
    if (delete_count_ > 0) {
      RowsCompare rows_cmp(rowkey_cmp_funcs, min_key_, false, max_prefix_length_, ret);
      std::sort(ext_rowkeys_.begin(), ext_rowkeys_.end(), rows_cmp);
      if (OB_SUCC(ret)) {
        while (ext_rowkeys_.count() > 0) {
          if (ext_rowkeys_.at(ext_rowkeys_.count() - 1).get_store_rowkey().is_max()) {
            ext_rowkeys_.pop_back();
          } else {
            break;
          }
        }
        min_key_ = ext_rowkeys_.at(0).get_store_rowkey();
        delete_count_ = 0;
      } else {
        STORAGE_LOG(WARN, "Unexpected duplicate rowkeys", K_(ext_rowkeys), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!collation_free_transformed_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < ext_rowkeys_.count(); i++) {
          if (OB_FAIL(
                  ext_rowkeys_.at(i).to_collation_free_on_demand_and_cutoff_range(scan_mem_->get_arena_allocator()))) {
            STORAGE_LOG(WARN, "Failed to transform collation free rowkey", K(ext_rowkeys_.at(i)), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          collation_free_transformed_ = true;
        }
      }
    }
  }

  return ret;
}

int ObRowsInfo::clear_found_rowkey(const int64_t rowkey_idx)
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected not init rowsinfo", K_(row_count), K_(delete_count), KP_(rows), K(ret));
  } else if (rowkey_idx < 0 || rowkey_idx >= ext_rowkeys_.count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to clear found rowkey", K(rowkey_idx), K_(ext_rowkeys), K(ret));
  } else if (ext_rowkeys_.at(rowkey_idx).get_store_rowkey().is_max()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Clear found rowkey twice", K(rowkey_idx), K(ret));
  } else {
    ext_rowkeys_.at(rowkey_idx).get_store_rowkey().set_max();
    delete_count_++;
  }

  return ret;
}

ObPartitionEst::ObPartitionEst() : logical_row_count_(0), physical_row_count_(0)
{}

int ObPartitionEst::add(const ObPartitionEst& pe)
{
  int ret = common::OB_SUCCESS;

  logical_row_count_ += pe.logical_row_count_;
  physical_row_count_ += pe.physical_row_count_;

  return ret;
}

int ObPartitionEst::deep_copy(const ObPartitionEst& src)
{
  int ret = common::OB_SUCCESS;

  logical_row_count_ = src.logical_row_count_;
  physical_row_count_ = src.physical_row_count_;

  return ret;
}

ObQueryRowIteratorAdapter::ObQueryRowIteratorAdapter(const ObQRIterType type, ObIStoreRowIterator& iter)
    : ObQueryRowIterator(type), iter_(iter), row_(), allocator_(NULL), cells_(NULL), cell_cnt_(0)
{}

ObQueryRowIteratorAdapter::~ObQueryRowIteratorAdapter()
{
  if (NULL != allocator_ && NULL != cells_) {
    allocator_->free(cells_);
  }
}

int ObQueryRowIteratorAdapter::init(ObIAllocator& allocator, const int64_t cell_cnt)
{
  int ret = OB_SUCCESS;
  if (NULL != allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (cell_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cell_cnt));
  } else if (NULL == (cells_ = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * cell_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else {
    allocator_ = &allocator;
    cell_cnt_ = cell_cnt;
  }
  return ret;
}

int ObQueryRowIteratorAdapter::get_next_row(ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* crow = NULL;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(iter_.get_next_row(crow))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  } else if (crow->row_val_.count_ > cell_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid row cnt", K(ret), K_(cell_cnt), "const row", *crow);
  } else {
    row_ = *crow;
    row_.row_val_.cells_ = cells_;
    MEMCPY(cells_, crow->row_val_.cells_, sizeof(*cells_) * crow->row_val_.count_);
    row = &row_;
  }
  return ret;
}

void ObQueryRowIteratorAdapter::reset()
{
  if (NULL != allocator_ && NULL != cells_) {
    allocator_->free(cells_);
  }
  cells_ = NULL;
  cell_cnt_ = 0;
  allocator_ = NULL;
}

/*************************ObVPCompactIter*************************************/

ObVPCompactIter::ObVPCompactIter() : scan_iter_(nullptr), rowkey_cnt_(0), is_inited_(false)
{}

ObVPCompactIter::~ObVPCompactIter()
{
  if (nullptr != scan_iter_) {
    scan_iter_->~ObIStoreRowIterator();
    scan_iter_ = nullptr;
  }
  rowkey_cnt_ = 0;
  is_inited_ = false;
}

int ObVPCompactIter::init(const int64_t rowkey_cnt, ObIStoreRowIterator* scan_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init ObVPCompactIter twice", K(ret));
  } else if (OB_UNLIKELY(0 >= rowkey_cnt || nullptr == scan_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "failed to init ObVPCompactIter, invalid argument", K(ret), K(rowkey_cnt), KP(scan_iter));
  } else {
    scan_iter_ = scan_iter;
    rowkey_cnt_ = rowkey_cnt;
    is_inited_ = true;
    STORAGE_LOG(DEBUG, "ObVPCompactIter is inited", K(rowkey_cnt));
  }
  return ret;
}

int ObVPCompactIter::get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  bool output = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObVPCompactIter is not inited", K(ret));
  } else {
    while (OB_SUCC(ret) && !output) {
      if (OB_FAIL(scan_iter_->get_next_row(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "failed to get next row from scan_iter", K(ret));
        }
      } else if (ObActionFlag::OP_DEL_ROW == row->flag_) {
        output = true;
      } else if (ObActionFlag::OP_ROW_EXIST == row->flag_) {
        for (int64_t i = rowkey_cnt_; !output && i < row->row_val_.count_; ++i) {
          if (!row->row_val_.cells_[i].is_nop_value()) {
            output = true;
          }
        }
      }
      if (OB_SUCC(ret) && nullptr != row) {
        STORAGE_LOG(DEBUG, "iter one row", K(*row));
      }
    }
    if (OB_SUCC(ret) && nullptr != row) {
      STORAGE_LOG(DEBUG, "got one row", K(*row));
    }
  }
  return ret;
}

const char* merge_type_to_str(const ObMergeType& merge_type)
{
  const char* str = "";
  switch (merge_type) {
    case MAJOR_MERGE:
      str = "major merge";
      break;
    case MINOR_MERGE:
      str = "minor merge";
      break;
    case MINI_MERGE:
      str = "mini merge";
      break;
    case MINI_MINOR_MERGE:
      str = "mini minor merge";
      break;
    case RESERVED_MINOR_MERGE: {
      str = "reserved minor merge";
      break;
    }
    case RESERVED_MAJOR_MERGE: {
      str = "reserved major merge";
      break;
    }
    case HISTORY_MINI_MINOR_MERGE: {
      str = "history mini minor merge";
      break;
    }
    default:
      str = "invalid merge type";
  }
  return str;
}

}  // namespace storage
}  // namespace oceanbase
