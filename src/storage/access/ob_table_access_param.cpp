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

#include "ob_table_access_param.h"
#include "ob_dml_param.h"
#include "storage/ob_relative_table.h"
#include "storage/tablet/ob_tablet.h"
#include "share/schema/ob_table_dml_param.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

ObTableIterParam::ObTableIterParam()
    : table_id_(0),
      tablet_id_(),
      read_info_(nullptr),
      full_read_info_(nullptr),
      out_cols_project_(NULL),
      agg_cols_project_(NULL),
      pushdown_filter_(nullptr),
      tablet_handle_(),
      is_multi_version_minor_merge_(false),
      need_scn_(false),
      is_same_schema_column_(false),
      vectorized_enabled_(false),
      has_virtual_columns_(false),
      limit_prefetch_(false),
      pd_storage_flag_(0)
{
}

ObTableIterParam::~ObTableIterParam()
{
  tablet_handle_.reset();
}

void ObTableIterParam::reset()
{
  table_id_ = 0;
  tablet_id_.reset();
  tablet_handle_.reset();
  read_info_ = nullptr;
  full_read_info_ = nullptr;
  out_cols_project_ = NULL;
  agg_cols_project_ = NULL;
  is_multi_version_minor_merge_ = false;
  need_scn_ = false;
  is_same_schema_column_ = false;
  pd_storage_flag_ = 0;
  pushdown_filter_ = nullptr;
  vectorized_enabled_ = false;
  has_virtual_columns_ = false;
  limit_prefetch_ = false;
}

bool ObTableIterParam::is_valid() const
{
  return (OB_INVALID_ID != table_id_ || tablet_id_.is_valid()) // TODO: use tablet id replace table id
      && OB_NOT_NULL(read_info_) && read_info_->is_valid()
      && (nullptr == full_read_info_ || full_read_info_->is_valid_full_read_info());
}

int ObTableIterParam::check_read_info_valid()
{
  int ret = OB_SUCCESS;

  if (nullptr != full_read_info_ && nullptr != read_info_) {
    if (read_info_->is_oracle_mode() != full_read_info_->is_oracle_mode()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "Unexpected compat mode", K(ret), K(lib::is_oracle_mode()), KPC(read_info_), KPC(full_read_info_));
    } else if (read_info_->get_schema_rowkey_count() != full_read_info_->get_schema_rowkey_count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "Unexpected rowkey count in read info", K(ret), KPC(read_info_), KPC(full_read_info_));
    }
  }

  return ret;
}

int ObTableIterParam::has_lob_column_out(const bool is_get, bool &has_lob_column) const
{
  int ret = OB_SUCCESS;
  const ObTableReadInfo *read_info = nullptr;
  has_lob_column = false;
  if (OB_ISNULL(read_info = get_read_info(is_get))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null read info", K(ret));
  } else {
    const ObColDescIArray &out_cols = read_info->get_columns_desc();
    for (int64_t i = 0; !has_lob_column && i < out_cols.count(); i++) {
      has_lob_column = (out_cols.at(i).col_type_.is_lob_v2());
    }
  }
  return ret;
}

bool ObTableIterParam::enable_fuse_row_cache(const ObQueryFlag &query_flag) const
{
  bool bret = is_x86() && query_flag.is_use_fuse_row_cache() && !query_flag.is_read_latest() &&
      nullptr != full_read_info_ && !has_virtual_columns_ && !need_scn_ && is_same_schema_column_;
  const ObColDescIArray &col_descs = full_read_info_->get_columns_desc();
  for (int64_t i = 0; bret && i < col_descs.count(); i++) {
    bret = !(col_descs.at(i).col_type_.is_lob_v2());
  }
  return bret;
}

DEF_TO_STRING(ObTableIterParam)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_id),
       K_(tablet_id),
       KPC_(read_info),
       KPC_(full_read_info),
       KPC_(out_cols_project),
       KPC_(pushdown_filter),
       K_(is_multi_version_minor_merge),
       K_(need_scn),
       K_(is_same_schema_column),
       K_(pd_storage_flag),
       K_(vectorized_enabled),
       K_(has_virtual_columns),
       K_(limit_prefetch));
  J_OBJ_END();
  return pos;
}

ObTableAccessParam::ObTableAccessParam()
    : iter_param_(),
      padding_cols_(NULL),
      projector_size_(0),
      output_exprs_(NULL),
      aggregate_exprs_(NULL),
      op_(NULL),
      op_filters_(NULL),
      row2exprs_projector_(NULL),
      output_sel_mask_(NULL),
      fast_agg_project_(NULL),
      is_inited_(false)
{
}

ObTableAccessParam::~ObTableAccessParam()
{
}

void ObTableAccessParam::reset()
{
  iter_param_.reset();
  padding_cols_ = NULL;
  projector_size_ = 0;
  output_exprs_ = NULL;
  op_ = NULL;
  op_filters_ = NULL;
  row2exprs_projector_ = NULL;
  output_sel_mask_ = NULL;
  fast_agg_project_ = NULL;
  is_inited_ = false;
}

int ObTableAccessParam::init(
    const ObTableScanParam &scan_param,
    const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableParam &table_param = *scan_param.table_param_;
  if(IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableAccessParam init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid tablet handle", K(ret), K(tablet_handle));
  } else {
    iter_param_.table_id_ = table_param.get_table_id();
    iter_param_.tablet_id_ = scan_param.tablet_id_;
    iter_param_.read_info_ = &table_param.get_read_info();
    iter_param_.out_cols_project_ = &table_param.get_output_projector();
    iter_param_.agg_cols_project_ = &table_param.get_aggregate_projector();
    iter_param_.need_scn_ = scan_param.need_scn_;
    padding_cols_ = &table_param.get_pad_col_projector();
    projector_size_ = scan_param.projector_size_;

    output_exprs_ = scan_param.output_exprs_;
    aggregate_exprs_ = scan_param.aggregate_exprs_;
    op_ = scan_param.op_;
    op_filters_ = scan_param.op_filters_;
    row2exprs_projector_ = scan_param.row2exprs_projector_;
    output_sel_mask_ = &table_param.get_output_sel_mask();

    iter_param_.tablet_handle_ = tablet_handle;
    iter_param_.full_read_info_ = &tablet_handle.get_obj()->get_full_read_info();
    iter_param_.is_same_schema_column_ =
        iter_param_.read_info_->get_schema_column_count() == iter_param_.full_read_info_->get_schema_column_count();

    iter_param_.pd_storage_flag_ = scan_param.pd_storage_flag_;
    iter_param_.pushdown_filter_ = scan_param.pd_storage_filters_;
     //disable blockscan if scan order is KeepOrder(for iterator iterator and table api)
    if (OB_UNLIKELY(ObQueryFlag::KeepOrder == scan_param.scan_flag_.scan_order_)) {
      iter_param_.disable_blockscan();

    }
    if (scan_param.need_switch_param_) {
      iter_param_.set_use_iter_pool_flag();
    }
    iter_param_.has_virtual_columns_ = table_param.has_virtual_column();
    // vectorize requires blockscan is enabled(_pushdown_storage_level > 0)
    iter_param_.vectorized_enabled_ = nullptr != op_ && op_->is_vectorized();
    iter_param_.limit_prefetch_ = (nullptr == op_filters_ || op_filters_->empty());

    if (OB_FAIL(iter_param_.check_read_info_valid())) {
      STORAGE_LOG(WARN, "Failed to check read info valdie", K(ret), K(iter_param_));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableAccessParam::init_merge_param(
    const uint64_t table_id,
    const common::ObTabletID &tablet_id,
    const ObTableReadInfo &read_info,
    const bool is_multi_version_minor_merge)
{
  int ret = OB_SUCCESS;

  if(IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableAccessParam init twice", K(ret), KPC(this));
  } else {
    iter_param_.table_id_ = table_id;
    iter_param_.tablet_id_ = tablet_id;
    iter_param_.is_multi_version_minor_merge_ = is_multi_version_minor_merge;
    iter_param_.read_info_ = &read_info;
    iter_param_.full_read_info_ = &read_info;
    is_inited_ = true;
  }
  return ret;
}

int ObTableAccessParam::init_dml_access_param(
    const ObRelativeTable &table,
    const ObTableReadInfo &full_read_info,
    const share::schema::ObTableSchemaParam &schema_param,
    const ObIArray<int32_t> *out_cols_project)
{
  int ret = OB_SUCCESS;
  const ObTablet *tablet = nullptr;
  if(IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTableAccessParam init twice", K(ret), K(*this));
  } else {
    iter_param_.table_id_ = table.get_table_id();
    iter_param_.tablet_id_ = table.get_tablet_id();
    iter_param_.read_info_ = &schema_param.get_read_info();
    iter_param_.full_read_info_ = &full_read_info;
    iter_param_.is_same_schema_column_ =
        iter_param_.read_info_->get_schema_column_count() == iter_param_.full_read_info_->get_schema_column_count();
    iter_param_.out_cols_project_ = out_cols_project;
    for (int64_t i = 0; i < schema_param.get_columns().count(); i++) {
      if (schema_param.get_columns().at(i)->is_virtual_gen_col()) {
        iter_param_.has_virtual_columns_ = true;
        break;
      }
    }
    if (OB_FAIL(iter_param_.check_read_info_valid())) {
      STORAGE_LOG(WARN, "Failed to check read info valdie", K(ret), K(iter_param_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

DEF_TO_STRING(ObTableAccessParam)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(iter_param),
      KPC_(padding_cols),
      K_(projector_size),
      KPC_(output_exprs),
      KP_(op),
      KP_(op_filters),
      KP_(row2exprs_projector),
      KPC_(output_sel_mask),
      KP_(fast_agg_project),
      K_(is_inited));
  J_OBJ_END();
  return pos;
}

int set_row_scn(
    const ObTableIterParam &iter_param,
    const ObSSTable &sstable,
    const ObDatumRow *store_row)
{
  int ret = OB_SUCCESS;
  const ObColDescIArray *out_cols = nullptr;
  const ObTableReadInfo *read_info = iter_param.get_read_info();
  if (OB_UNLIKELY(nullptr == read_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null read info", K(ret));
  } else {
    int64_t trans_idx = read_info->get_trans_col_index();
    if (OB_UNLIKELY(trans_idx < 0 || trans_idx >= store_row->count_ ||
                    store_row->storage_datums_[trans_idx].is_nop())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected trans_idx", K(ret), KPC(store_row), KPC(read_info));
    } else {
      int64_t version = -store_row->storage_datums_[trans_idx].get_int();
      if (version > 0) {
        store_row->storage_datums_[trans_idx].reuse();
        store_row->storage_datums_[trans_idx].set_int(version);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scn should be greater than 0", K(ret), K(version), KPC(store_row), KPC(read_info));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
