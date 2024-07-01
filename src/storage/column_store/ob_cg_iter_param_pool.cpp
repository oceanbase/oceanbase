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
#include "ob_column_store_util.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/tablet/ob_tablet.h"

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
    const common::ObIArray<sql::ObExpr*> &exprs,
    ObTableIterParam *&iter_param,
    const bool is_aggregate)
{
  int ret = OB_SUCCESS;
  iter_param = nullptr;
  if (OB_UNLIKELY(0 > cg_idx || !row_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(cg_idx), K(row_param));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_params_.count(); ++i) {
      ObTableIterParam* tmp_param = iter_params_.at(i);
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
  } else if (OB_FAIL(new_iter_param(cg_idx, row_param, exprs, iter_param, is_aggregate))) {
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
    const common::ObIArray<sql::ObExpr*> &exprs,
    ObTableIterParam *&iter_param,
    const bool is_aggregate)
{
  int ret = OB_SUCCESS;
  iter_param = nullptr;
  if (OB_UNLIKELY(0 > cg_idx || !row_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(cg_idx), K(row_param));
  } else if (OB_ISNULL(iter_param = OB_NEWx(ObTableIterParam, (&alloc_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc new iter param", K(ret), K(cg_idx));
  } else if (is_virtual_cg(cg_idx)) {
    if (OB_FAIL(fill_virtual_cg_iter_param(row_param, cg_idx, exprs, *iter_param))) {
      LOG_WARN("Fail to fill dummy cg iter param", K(ret));
    }
  } else if (OB_FAIL(fill_cg_iter_param(row_param, cg_idx, exprs, *iter_param))) {
    STORAGE_LOG(WARN, "Failed to mock cg iter param", K(ret), K(row_param), K(cg_idx), K(exprs));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(put_iter_param(iter_param))) {
      LOG_WARN("Fail to put iter param", K(ret));
    } else if (!is_aggregate) {
      iter_param->disable_pd_aggregate();
    }
  } else if (nullptr != iter_param) {
    free_iter_param(iter_param);
    iter_param = nullptr;
  }
  return ret;
}

int ObCGIterParamPool::fill_cg_iter_param(
    const ObTableIterParam &row_param,
    const int32_t cg_idx,
    const common::ObIArray<sql::ObExpr*> &exprs,
    ObTableIterParam &cg_param)
{
  int ret = OB_SUCCESS;

  sql::ExprFixedArray *output_exprs = nullptr;
  ColumnsIndex *out_cols_project = nullptr;
  if (OB_ISNULL(output_exprs = OB_NEWx(sql::ExprFixedArray, (&alloc_), alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc output exprs", K(ret));
  } else if (OB_FAIL(output_exprs->init(exprs.count()))) {
    LOG_WARN("Fail to init output exprs", K(ret));
  } else if (OB_FAIL(output_exprs->assign(exprs))) {
    LOG_WARN("Fail to assign exprs", K(ret));
  } else if (OB_ISNULL(out_cols_project = OB_NEWx(ColumnsIndex, (&alloc_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc out cols projector", K(ret));
  } else if (OB_FAIL(out_cols_project->init(1, alloc_))) {
    LOG_WARN("Fail to init out cols projector", K(ret));
  } else if (OB_FAIL(out_cols_project->push_back(0))) {
    LOG_WARN("Fail to push back out cols project", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != output_exprs) {
      output_exprs->reset();
      alloc_.free(output_exprs);
      output_exprs = nullptr;
    }
    if (nullptr != out_cols_project) {
      out_cols_project->reset();
      alloc_.free(out_cols_project);
      out_cols_project = nullptr;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_for_column_store(row_param, output_exprs, out_cols_project, cg_idx, cg_param))) {
    LOG_WARN("Fail to generate cg iter param", K(ret));
  }

  return ret;
}

int ObCGIterParamPool::fill_virtual_cg_iter_param(
    const ObTableIterParam &row_param,
    const int32_t cg_idx,
    const common::ObIArray<sql::ObExpr*> &exprs,
    ObTableIterParam &cg_param)
{
  int ret = OB_SUCCESS;
  sql::ExprFixedArray *output_exprs = nullptr;
  if (OB_ISNULL(output_exprs = OB_NEWx(sql::ExprFixedArray, (&alloc_), alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc output exprs", K(ret));
  } else if (OB_FAIL(output_exprs->init(exprs.count()))) {
    LOG_WARN("Fail to init output exprs", K(ret));
  } else if (OB_FAIL(output_exprs->assign(exprs))) {
    LOG_WARN("Fail to assign exprs", K(ret));
  } else {
    cg_param.cg_idx_ = cg_idx;
    cg_param.output_exprs_ = output_exprs;
    cg_param.op_ = row_param.op_;
    cg_param.pd_storage_flag_ = row_param.pd_storage_flag_;
    cg_param.table_scan_opt_ = row_param.table_scan_opt_;
    cg_param.tablet_handle_ = row_param.tablet_handle_;
  }
  if (OB_FAIL(ret) && nullptr != output_exprs) {
    output_exprs->reset();
    alloc_.free(output_exprs);
  }
  return ret;
}

int ObCGIterParamPool::generate_for_column_store(const ObTableIterParam &row_param,
                                                 const sql::ExprFixedArray *exprs,
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
      cg_param.output_exprs_ = exprs;
      cg_param.aggregate_exprs_ = nullptr;
      cg_param.output_sel_mask_ = nullptr;
      cg_param.is_multi_version_minor_merge_ = row_param.is_multi_version_minor_merge_;
      cg_param.need_scn_ = row_param.need_scn_;
      cg_param.is_same_schema_column_ = row_param.need_scn_;
      cg_param.vectorized_enabled_ = row_param.vectorized_enabled_;
      cg_param.has_virtual_columns_ = row_param.has_virtual_columns_;
      cg_param.has_lob_column_out_ = col_descs.at(cg_pos).col_type_.is_lob_storage();
      cg_param.is_for_foreign_check_ = row_param.is_for_foreign_check_;
      cg_param.limit_prefetch_ = row_param.limit_prefetch_;
      //cg_param.ss_rowkey_prefix_cnt_ = 0;
      cg_param.pd_storage_flag_ = row_param.pd_storage_flag_;
      cg_param.table_scan_opt_ = row_param.table_scan_opt_;
      if (nullptr != row_param.cg_read_infos_) {
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
    iter_param->~ObTableIterParam();
    alloc_.free(iter_param);
  }
}

}
}
