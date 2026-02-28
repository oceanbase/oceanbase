/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_cg_iter_param_pool.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/ob_micro_block_format_version_helper.h"

namespace oceanbase
{
namespace storage
{

ObCGIterParamPool::ObCGIterParamPool(common::ObIAllocator &alloc)
  : alloc_(alloc),
   iter_params_()
{}

void ObCGIterParamPool::reset()
{
  for (int64_t i = 0; i < iter_params_.count(); ++i) {
    free_iter_param(iter_params_.at(i));
  }
  iter_params_.reset();
}

int ObCGIterParamPool::get_iter_param(
    const int32_t cg_idx,
    const ObTableIterParam &row_param,
    sql::ObExpr *expr,
    ObTableIterParam *&iter_param)
{
  int ret = OB_SUCCESS;
  iter_param = nullptr;
  if (OB_UNLIKELY(0 > cg_idx || !row_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(cg_idx), K(row_param));
  } else {
    common::ObSEArray<sql::ObExpr*, 1> exprs;
    if (nullptr != expr && OB_FAIL(exprs.push_back(expr))) {
      LOG_WARN("Fail to push back", K(ret));
    } else if (OB_FAIL(get_iter_param(cg_idx, row_param, exprs, iter_param))) {
      LOG_WARN("Fail to get cg iter param", K(ret));
    }
  }
  return ret;
}

int ObCGIterParamPool::get_iter_param(
    const int32_t cg_idx,
    const ObTableIterParam &row_param,
    const common::ObIArray<sql::ObExpr*> &output_exprs,
    ObTableIterParam *&iter_param,
    const common::ObIArray<sql::ObExpr*> *agg_exprs)
{
  int ret = OB_SUCCESS;
  iter_param = nullptr;
  const bool is_aggregate = nullptr != agg_exprs;
  if (OB_UNLIKELY(0 > cg_idx || !row_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(cg_idx), K(row_param), KP(agg_exprs));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_params_.count(); ++i) {
      ObTableIterParam* tmp_param = iter_params_.at(i);
      const common::ObIArray<sql::ObExpr*> &exprs = is_aggregate ? *agg_exprs : output_exprs;
      if (OB_ISNULL(tmp_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null iter param", K(ret));
      } else if (tmp_param->can_be_reused(cg_idx, exprs, is_aggregate)) {
        iter_param = tmp_param;
        iter_param->tablet_handle_ = row_param.tablet_handle_;
        break;
      }
    }
  }
  if (OB_FAIL(ret) || OB_NOT_NULL(iter_param)) {
  } else if (OB_FAIL(new_iter_param(cg_idx, row_param, output_exprs, iter_param, agg_exprs))) {
    LOG_WARN("Fail to new cg iter param", K(ret));
  }
  return ret;
}

int ObCGIterParamPool::put_iter_param(ObTableIterParam *iter_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret));
  } else if (OB_FAIL(iter_params_.push_back(iter_param))) {
    LOG_WARN("Fail to push new iter param", K(ret), KPC(iter_param));
  }
  return ret;
}

int ObCGIterParamPool::new_iter_param(
    const int32_t cg_idx,
    const ObTableIterParam &row_param,
    const common::ObIArray<sql::ObExpr*> &output_exprs,
    ObTableIterParam *&iter_param,
    const common::ObIArray<sql::ObExpr*> *agg_exprs)
{
  int ret = OB_SUCCESS;
  iter_param = nullptr;
  if (OB_UNLIKELY(0 > cg_idx || !row_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(cg_idx), K(row_param), KP(agg_exprs));
  } else if (OB_ISNULL(iter_param = OB_NEWx(ObTableIterParam, (&alloc_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc new iter param", K(ret), K(cg_idx));
  } else if (is_virtual_cg(cg_idx)) {
    if (OB_FAIL(fill_virtual_cg_iter_param(row_param, cg_idx, output_exprs, *iter_param, agg_exprs))) {
      LOG_WARN("Fail to fill dummy cg iter param", K(ret), K(row_param), K(cg_idx), K(output_exprs), KPC(agg_exprs));
    }
  } else if (OB_FAIL(fill_cg_iter_param(row_param, cg_idx, output_exprs, *iter_param, agg_exprs))) {
    LOG_WARN("Failed to mock cg iter param", K(ret), K(row_param), K(cg_idx), K(output_exprs), KPC(agg_exprs));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(put_iter_param(iter_param))) {
      LOG_WARN("Fail to put iter param", K(ret));
    } else if (nullptr == agg_exprs) {
      iter_param->disable_pd_aggregate();
    }
  } else if (nullptr != iter_param) {
    free_iter_param(iter_param);
    iter_param = nullptr;
  }
  return ret;
}

int ObCGIterParamPool::build_read_info_for_rowkey_cg(const ObTableIterParam &row_param,
                                                     const ObITableReadInfo *&rowkey_cg_read_info)
{
  int ret = OB_SUCCESS;

  ObTableReadInfo *table_read_info = nullptr;
  common::ObSEArray<ObColDesc, 8> rowkey_col_descs;
  common::ObSEArray<ObColumnParam*, 8> rowkey_col_params;
  common::ObSEArray<int32_t, 8> storage_cols_index;
  common::ObSEArray<ObColExtend, 8> column_extends;
  common::ObSEArray<int32_t, 8> cg_idxs;

  if (OB_UNLIKELY(!row_param.is_valid() || nullptr == row_param.read_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_param));
  } else {
    const ObITableReadInfo *read_info = row_param.read_info_;
    const int64_t schema_rowkey_cnt = read_info->get_schema_rowkey_count();
    const int64_t schema_column_count = read_info->get_schema_column_count();
    const common::ObIArray<ObColDesc> &all_col_descs = read_info->get_columns_desc();
    const common::ObIArray<ObColumnParam *> *all_col_params = read_info->get_columns();
    const common::ObIArray<ObColExtend> *all_col_extends = read_info->get_columns_extend();

    // TODO(menglan): only add trans version column
    for (int64_t i = 0; OB_SUCC(ret) && i < all_col_descs.count(); i++) {
      if (i < schema_rowkey_cnt || all_col_descs.at(i).col_id_ == common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID) {
        if (OB_FAIL(rowkey_col_descs.push_back(all_col_descs.at(i)))) {
          LOG_WARN("Fail to push back rowkey col desc", K(ret), K(i));
        } else if (all_col_params != nullptr && OB_FAIL(rowkey_col_params.push_back(all_col_params->at(i)))) {
          LOG_WARN("Fail to push back rowkey col param", K(ret), K(i));
        } else if (OB_FAIL(storage_cols_index.push_back(i < schema_rowkey_cnt ? i : OB_INVALID_INDEX))) {
          LOG_WARN("Fail to push back storage col index", K(ret), K(i));
        } else if (OB_FAIL(cg_idxs.push_back(0))) {
          LOG_WARN("Fail to push back cg idx", K(ret), K(schema_rowkey_cnt));
        } else if (all_col_extends != nullptr && OB_FAIL(column_extends.push_back(all_col_extends->at(i)))) {
          LOG_WARN("Fail to push back column extend", K(ret), K(all_col_extends->at(i)));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (rowkey_col_descs.count() != schema_rowkey_cnt + 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected, ora_rowscn is not in TableReadInfo", K(ret), K(rowkey_col_descs.count()));
    } else if (OB_ISNULL(table_read_info = OB_NEWx(ObTableReadInfo, (&alloc_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc table read info", K(ret));
    } else if (OB_FAIL(table_read_info->init(
        alloc_,
        schema_rowkey_cnt + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(),
        schema_rowkey_cnt,
        read_info->is_oracle_mode(),
        rowkey_col_descs,
        &storage_cols_index,
        &rowkey_col_params,
        &cg_idxs,
        &column_extends,
        false /*has_all_column_group*/,
        false /*is_cg_sstable*/,
        false /*need_truncate_filter*/,
        false /*is_delete_insert_table*/,
        ObMicroBlockFormatVersionHelper::DEFAULT_VERSION,
        read_info->has_ttl_definition()))) {
      LOG_WARN("Fail to init table read info", K(ret), K(schema_rowkey_cnt), K(schema_column_count));
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != table_read_info) {
      table_read_info->~ObTableReadInfo();
      alloc_.free(table_read_info);
      table_read_info = nullptr;
    }
  } else {
    rowkey_cg_read_info = table_read_info;
  }

  return ret;
}

int ObCGIterParamPool::fill_cg_iter_param(
    const ObTableIterParam &row_param,
    const int32_t cg_idx,
    const common::ObIArray<sql::ObExpr*> &output_exprs,
    ObTableIterParam &cg_param,
    const common::ObIArray<sql::ObExpr*> *agg_exprs)
{
  int ret = OB_SUCCESS;
  sql::ExprFixedArray *param_output_exprs = nullptr;
  sql::ExprFixedArray *param_agg_exprs = nullptr;
  ColumnsIndex *out_cols_project = nullptr;
  if (OB_FAIL(copy_param_exprs(output_exprs, param_output_exprs))) {
    LOG_WARN("Failed to copy output exprs", K(ret), K(output_exprs));
  } else if (nullptr != agg_exprs && OB_FAIL(copy_param_exprs(*agg_exprs, param_agg_exprs))) {
    LOG_WARN("Failed to copy aggregate exprs", K(ret), KPC(agg_exprs));
  } else if (OB_ISNULL(out_cols_project = OB_NEWx(ColumnsIndex, (&alloc_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc out cols projector", K(ret));
  } else if (OB_FAIL(out_cols_project->init(1, alloc_))) {
    LOG_WARN("Fail to init out cols projector", K(ret));
  } else if (OB_FAIL(out_cols_project->push_back(cg_idx == 0 && row_param.is_normal_cgs_at_the_end() ? row_param.get_schema_rowkey_count() : 0))) {
    LOG_WARN("Fail to push back out cols project", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != out_cols_project) {
      out_cols_project->~ColumnsIndex();
      alloc_.free(out_cols_project);
      out_cols_project = nullptr;
    }
  } else if (OB_FAIL(generate_for_column_store(row_param, param_output_exprs, param_agg_exprs, out_cols_project, cg_idx, cg_param))) {
    LOG_WARN("Fail to generate cg iter param", K(ret));
  }
  return ret;
}

int ObCGIterParamPool::copy_param_exprs(
  const common::ObIArray<sql::ObExpr*> &exprs,
  sql::ExprFixedArray *&param_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_exprs = OB_NEWx(sql::ExprFixedArray, (&alloc_), alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc output exprs", K(ret));
  } else if (OB_FAIL(param_exprs->init(exprs.count()))) {
    LOG_WARN("Fail to init output exprs", K(ret));
  } else if (OB_FAIL(param_exprs->assign(exprs))) {
    LOG_WARN("Fail to assign exprs", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != param_exprs) {
      param_exprs->~ObFixedArray();
      alloc_.free(param_exprs);
      param_exprs = nullptr;
    }
  }
  return ret;
}

int ObCGIterParamPool::fill_virtual_cg_iter_param(
    const ObTableIterParam &row_param,
    const int32_t cg_idx,
    const common::ObIArray<sql::ObExpr*> &output_exprs,
    ObTableIterParam &cg_param,
    const common::ObIArray<sql::ObExpr*> *agg_exprs)
{
  int ret = OB_SUCCESS;
  sql::ExprFixedArray *param_output_exprs = nullptr;
  sql::ExprFixedArray *param_agg_exprs = nullptr;
  if (OB_FAIL(copy_param_exprs(output_exprs, param_output_exprs))) {
    LOG_WARN("Failed to copy output exprs", K(ret), K(output_exprs));
  } else if (nullptr != agg_exprs && OB_FAIL(copy_param_exprs(*agg_exprs, param_agg_exprs))) {
    LOG_WARN("Failed to copy aggregate exprs", K(ret), KPC(agg_exprs));
  } else {
    cg_param.cg_idx_ = cg_idx;
    cg_param.output_exprs_ = param_output_exprs;
    if (nullptr != agg_exprs) {
      cg_param.aggregate_exprs_ = param_agg_exprs;
    }
    cg_param.op_ = row_param.op_;
    cg_param.pd_storage_flag_ = row_param.pd_storage_flag_;
    cg_param.table_scan_opt_ = row_param.table_scan_opt_;
    cg_param.tablet_handle_ = row_param.tablet_handle_;
    cg_param.vectorized_enabled_ = row_param.vectorized_enabled_;
    cg_param.plan_enable_rich_format_ = row_param.plan_enable_rich_format_;
  }
  return ret;
}

int ObCGIterParamPool::generate_for_column_store(const ObTableIterParam &row_param,
                                                 const sql::ExprFixedArray *output_exprs,
                                                 const sql::ExprFixedArray *agg_exprs,
                                                 const ObIArray<int32_t> *out_cols_project,
                                                 const int32_t cg_idx,
                                                 ObTableIterParam &cg_param)
{
  int ret = OB_SUCCESS;
  int64_t cg_pos = -1;

  if (OB_UNLIKELY(!row_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(row_param));
  } else { // get cg col desc from row_param, which contains read cols desc
    const ObITableReadInfo *read_info = row_param.get_read_info();
    const common::ObIArray<ObColDesc> &col_descs = read_info->get_columns_desc();
    const common::ObIArray<ObColumnParam *> *column_params = read_info->get_columns();
    const common::ObIArray<int32_t> *access_cgs = read_info->get_cg_idxs();

    if (OB_UNLIKELY(nullptr == access_cgs || nullptr == column_params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null cg indexs", K(ret), KPC(read_info));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < access_cgs->count(); i++) {
      if (cg_idx == access_cgs->at(i)) {
        cg_pos = i;
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(0 > cg_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected cg_pos or read info", K(ret), K(cg_pos), K(cg_idx), KPC(access_cgs));
    } else {
      cg_param.table_id_ = row_param.table_id_;
      cg_param.tablet_id_ = row_param.tablet_id_;
      cg_param.cg_idx_ = cg_idx;
      cg_param.tablet_handle_ = row_param.tablet_handle_;
      cg_param.cg_col_param_ = column_params->at(cg_pos);
      cg_param.out_cols_project_ = out_cols_project;
      cg_param.agg_cols_project_ = nullptr;
      cg_param.pushdown_filter_ = nullptr;
      cg_param.op_ = row_param.op_;
      cg_param.sstable_index_filter_ = nullptr;
      cg_param.output_exprs_ = output_exprs;
      cg_param.aggregate_exprs_ = agg_exprs;
      cg_param.output_sel_mask_ = nullptr;
      cg_param.is_multi_version_minor_merge_ = row_param.is_multi_version_minor_merge_;
      cg_param.need_scn_ = row_param.need_scn_;
      cg_param.is_same_schema_column_ = row_param.need_scn_;
      cg_param.vectorized_enabled_ = row_param.vectorized_enabled_;
      cg_param.has_virtual_columns_ = row_param.has_virtual_columns_;
      cg_param.has_lob_column_out_ = col_descs.at(cg_pos).col_type_.is_lob_storage();
      cg_param.is_for_foreign_check_ = row_param.is_for_foreign_check_;
      cg_param.limit_prefetch_ = row_param.limit_prefetch_;
      cg_param.is_delete_insert_ = false;
      //cg_param.ss_rowkey_prefix_cnt_ = 0;
      cg_param.pd_storage_flag_ = row_param.pd_storage_flag_;
      cg_param.table_scan_opt_ = row_param.table_scan_opt_;
      cg_param.is_column_replica_table_ = row_param.is_column_replica_table_;
      cg_param.plan_enable_rich_format_ = row_param.plan_enable_rich_format_;
      if (cg_idx == 0 && row_param.is_normal_cgs_at_the_end()) {
        if (OB_FAIL(build_read_info_for_rowkey_cg(row_param, cg_param.read_info_))) {
          LOG_WARN("Fail to build read info for rowkey cg", K(ret), K(row_param));
        }
      } else if (nullptr != row_param.cg_read_infos_) {
        if (OB_UNLIKELY(nullptr == row_param.cg_read_infos_->at(cg_pos))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null cg read info", K(ret), K(cg_pos), K(row_param));
        } else {
          cg_param.read_info_ = row_param.cg_read_infos_->at(cg_pos);
        }
      } else if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_cg_read_info(col_descs.at(cg_pos), column_params->at(cg_pos), row_param.tablet_id_, cg_param.cg_read_info_handle_))) {
        LOG_WARN("Fail to get cg read info",  K(ret), K(cg_idx), K(row_param));
      } else if (OB_UNLIKELY(!cg_param.cg_read_info_handle_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpect null cg read info", K(ret), KPC(cg_param.cg_read_info_handle_.get_read_info()));
      } else {
        cg_param.read_info_ = cg_param.cg_read_info_handle_.get_read_info();
      }
    }
  }
  return ret;
}


void ObCGIterParamPool::free_iter_param(ObTableIterParam *iter_param)
{
  if (OB_NOT_NULL(iter_param)) {
    if (OB_NOT_NULL(iter_param->output_exprs_)) {
      const_cast<sql::ObExprPtrIArray*>(iter_param->output_exprs_)->reset();
      alloc_.free((void*)iter_param->output_exprs_);
    }
    if (OB_NOT_NULL(iter_param->out_cols_project_)) {
      const_cast<ObIArray<int32_t> *>(iter_param->out_cols_project_)->reset();
      alloc_.free((void*)iter_param->out_cols_project_);
    }
    if (OB_NOT_NULL(iter_param->read_info_) && iter_param->cg_idx_ == 0) {
      const ObTableReadInfo *table_read_info = dynamic_cast<const ObTableReadInfo*>(iter_param->read_info_);
      if (nullptr != table_read_info) {
        const_cast<ObTableReadInfo*>(table_read_info)->~ObTableReadInfo();
        alloc_.free((void*)table_read_info);
      } else {
        int ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null table read info", K(ret), KPC(iter_param), KPC(iter_param->read_info_));
      }
    }
    iter_param->~ObTableIterParam();
    alloc_.free(iter_param);
  }
}

}
}
