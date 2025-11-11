/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/iter/ob_das_domain_id_merge_iter.h"
#include "sql/das/iter/ob_das_iter_define.h"
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "share/domain_id/ob_domain_id.h"
#include "share/vector_index/ob_vector_index_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObDASDomainIdMergeIterParam::ObDASDomainIdMergeIterParam()
  : ObDASIterParam(DAS_ITER_DOMAIN_ID_MERGE),
    rowkey_domain_ls_id_(),
    rowkey_domain_tablet_ids_(),
    data_table_iter_(nullptr),
    data_table_ctdef_(nullptr),
    data_table_rtdef_(nullptr),
    rowkey_domain_table_iters_(),
    rowkey_domain_ctdefs_(),
    rowkey_domain_rtdefs_(),
    trans_desc_(nullptr),
    snapshot_(nullptr)
{}

ObDASDomainIdMergeIterParam::~ObDASDomainIdMergeIterParam()
{}

bool ObDASDomainIdMergeIterParam::is_valid() const
{
  bool bret = true;
  bret = rowkey_domain_ls_id_.is_valid() &&
         data_table_iter_ != nullptr &&
         data_table_ctdef_ != nullptr &&
         data_table_rtdef_ != nullptr &&
         snapshot_ != nullptr &&
         rowkey_domain_tablet_ids_.count() == rowkey_domain_table_iters_.count() &&
         rowkey_domain_tablet_ids_.count() == rowkey_domain_ctdefs_.count() &&
         rowkey_domain_tablet_ids_.count() == rowkey_domain_rtdefs_.count();
  for (int64_t i = 0; i < rowkey_domain_tablet_ids_.count() && bret; i++) {
    bret = bret && rowkey_domain_tablet_ids_.at(i).is_valid() &&
                    rowkey_domain_table_iters_.at(i) != nullptr &&
                    rowkey_domain_ctdefs_.at(i) != nullptr &&
                    rowkey_domain_rtdefs_.at(i) != nullptr;
  }
  return bret;
}

ObDASDomainIdMergeIter::ObDASDomainIdMergeIter()
  : ObDASIter(),
    need_filter_rowkey_domain_(true),
    is_no_sample_(true),
    rowkey_domain_scan_params_(),
    rowkey_domain_iters_(),
    data_table_iter_(nullptr),
    rowkey_domain_ctdefs_(),
    data_table_ctdef_(nullptr),
    rowkey_domain_rtdefs_(),
    data_table_rtdef_(nullptr),
    rowkey_domain_tablet_ids_(),
    rowkey_domain_ls_id_(),
    merge_memctx_(),
    is_need_multi_get_(false)
{}

ObDASDomainIdMergeIter::~ObDASDomainIdMergeIter()
{}

int ObDASDomainIdMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, data table iter is nullptr", K(ret), KP(data_table_iter_));
  } else if (OB_FAIL(build_rowkey_domain_range())) {
    LOG_WARN("fail to build rowkey domain range", K(ret));
  } else if (OB_FAIL(data_table_iter_->do_table_scan())) {
    LOG_WARN("fail to do table scan for data table", K(ret), KPC(data_table_iter_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
      if (OB_ISNULL(rowkey_domain_iters_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpeted error, rowkey domain table iter is nullptr", K(ret), KP(rowkey_domain_iters_.at(i)));
      } else if (OB_FAIL(rowkey_domain_iters_.at(i)->do_table_scan())) {
        LOG_WARN("fail to do table scan for rowkey domain", K(ret), K(i), KPC(rowkey_domain_iters_.at(i)));
      }
    }
  }
  LOG_INFO("do table scan", K(ret), K(data_table_iter_->get_scan_param()));
  return ret;
}

int ObDASDomainIdMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, data table iter is nullptr", K(ret), KP(data_table_iter_));
  } else if (OB_FAIL(build_rowkey_domain_range())) {
    LOG_WARN("fail to build rowkey domain range", K(ret));
  } else if (OB_FAIL(data_table_iter_->rescan())) {
    LOG_WARN("fail to rescan data table iter", K(ret), KPC(data_table_iter_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
      rowkey_domain_scan_params_.at(i)->ls_id_ = rowkey_domain_ls_id_;
      rowkey_domain_scan_params_.at(i)->tablet_id_ = rowkey_domain_tablet_ids_.at(i);
      if (OB_ISNULL(rowkey_domain_iters_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpeted error, rowkey domain table iter is nullptr", K(ret), KP(rowkey_domain_iters_.at(i)));
      } else if (OB_FAIL(rowkey_domain_iters_.at(i)->rescan())) {
        LOG_WARN("fail to rescan rowkey domain iter", K(ret), KPC(rowkey_domain_iters_.at(i)));
      }
    }
  }
  LOG_INFO("rescan", K(ret), K(data_table_iter_->get_scan_param()));
  return ret;
}

void ObDASDomainIdMergeIter::clear_evaluated_flag()
{
  for (int64_t i = 0; i < rowkey_domain_iters_.count(); i++) {
    if (OB_NOT_NULL(rowkey_domain_iters_.at(i))) {
      rowkey_domain_iters_.at(i)->clear_evaluated_flag();
    }
  }
  if (OB_NOT_NULL(data_table_iter_)) {
    data_table_iter_->clear_evaluated_flag();
  }
}

int ObDASDomainIdMergeIter::set_scan_rowkey(ObEvalCtx *eval_ctx,
                                            const ObIArray<ObExpr *> &rowkey_exprs,
                                            const ObDASScanCtDef *lookup_ctdef,
                                            ObIAllocator *alloc,
                                            int64_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_data_table_iter()->set_scan_rowkey(eval_ctx, rowkey_exprs, lookup_ctdef, alloc, group_id))) {
    LOG_WARN("failed to set scan rowkey of data table iter", K(ret));
  }
  return ret;
}

int ObDASDomainIdMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_DOMAIN_ID_MERGE != param.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(ret), K(param));
  } else {
    ObDASDomainIdMergeIterParam &merge_param = static_cast<ObDASDomainIdMergeIterParam &>(param);
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), "DomainIdMerge", ObCtxIds::DEFAULT_CTX_ID).set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(merge_memctx_, param))) {
      LOG_WARN("failed to create merge memctx", K(ret));
    } else {
      common::ObArenaAllocator& alloc = get_arena_allocator();
      for (int64_t i = 0; OB_SUCC(ret) && i < merge_param.rowkey_domain_table_iters_.count(); i++) {
        ObTableScanParam *rowkey_scan_param = nullptr;
        if (OB_ISNULL(rowkey_scan_param = OB_NEWx(ObTableScanParam, &alloc))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to new rowkey scan param", K(sizeof(ObTableScanParam)), K(ret));
        } else if (OB_FAIL(init_rowkey_domain_scan_param(
            merge_param.rowkey_domain_tablet_ids_.at(i),
            merge_param.rowkey_domain_ls_id_,
            merge_param.rowkey_domain_ctdefs_.at(i),
            merge_param.rowkey_domain_rtdefs_.at(i),
            merge_param.trans_desc_, merge_param.snapshot_,
            *rowkey_scan_param))) {
          LOG_WARN("fail to init rowkey domain scan param", K(ret), K(merge_param));
        } else if (OB_FAIL(rowkey_domain_scan_params_.push_back(rowkey_scan_param))) {
          LOG_WARN("fail to push back scan param", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rowkey_domain_iters_.assign(merge_param.rowkey_domain_table_iters_))) {
      LOG_WARN("fail to assign domain iter array", K(ret));
    } else if (OB_FAIL(rowkey_domain_ctdefs_.assign(merge_param.rowkey_domain_ctdefs_))) {
      LOG_WARN("fail to assign domain ctdef array", K(ret));
    } else if (OB_FAIL(rowkey_domain_rtdefs_.assign(merge_param.rowkey_domain_rtdefs_))) {
      LOG_WARN("fail to assign domain rtdef array", K(ret));
    } else if (OB_FAIL(rowkey_domain_tablet_ids_.assign(merge_param.rowkey_domain_tablet_ids_))) {
      LOG_WARN("fail to assign domain tablet id array", K(ret));
    } else {
      data_table_iter_  = merge_param.data_table_iter_;
      data_table_ctdef_ = merge_param.data_table_ctdef_;
      data_table_rtdef_ = merge_param.data_table_rtdef_;
      rowkey_domain_ls_id_ = merge_param.rowkey_domain_ls_id_;
      need_filter_rowkey_domain_ = true;
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::set_domain_id_merge_related_ids(
    const ObDASRelatedTabletID &tablet_ids,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || tablet_ids.domain_tablet_ids_.count() != rowkey_domain_tablet_ids_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", K(ret), K(ls_id), K(tablet_ids.domain_tablet_ids_), K(rowkey_domain_tablet_ids_));
  } else {
    rowkey_domain_ls_id_ = ls_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.domain_tablet_ids_.count(); i++) {
      if (OB_UNLIKELY(!tablet_ids.domain_tablet_ids_.at(i).is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet id", K(ret), K(tablet_ids.domain_tablet_ids_.at(i)));
      } else {
        rowkey_domain_tablet_ids_.at(i) = tablet_ids.domain_tablet_ids_.at(i);
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(data_table_iter_) && OB_FAIL(data_table_iter_->reuse())) {
    LOG_WARN("fail to reuse data table iter", K(ret));
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
      const ObTabletID old_tablet_id = rowkey_domain_scan_params_.at(i)->tablet_id_;
      const bool tablet_id_changed = old_tablet_id.is_valid() && old_tablet_id != rowkey_domain_tablet_ids_.at(i);
      rowkey_domain_scan_params_.at(i)->need_switch_param_ = rowkey_domain_scan_params_.at(i)->need_switch_param_ || (tablet_id_changed ? true : false);
      if (OB_FAIL(rowkey_domain_iters_.at(i)->reuse())) {
        LOG_WARN("fail to reuse rowkey domain iter", K(ret));
      }
    }
  }
  // if (OB_SUCC(ret) && OB_NOT_NULL(merge_memctx_)) {
  //   merge_memctx_->reset_remain_one_page();
  // }
  return ret;
}

int ObDASDomainIdMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < rowkey_domain_scan_params_.count(); i++) {
    rowkey_domain_scan_params_.at(i)->destroy_schema_guard();
    rowkey_domain_scan_params_.at(i)->snapshot_.reset();
    rowkey_domain_scan_params_.at(i)->destroy();
    rowkey_domain_scan_params_.at(i)->~ObTableScanParam();
  }
  if (OB_NOT_NULL(merge_memctx_)) {
    DESTROY_CONTEXT(merge_memctx_);
    merge_memctx_ = nullptr;
  }

  rowkey_domain_scan_params_.reset();
  rowkey_domain_iters_.reset();
  rowkey_domain_ctdefs_.reset();
  rowkey_domain_rtdefs_.reset();
  rowkey_domain_tablet_ids_.reset();
  data_table_iter_ = nullptr;
  need_filter_rowkey_domain_ = true;
  is_no_sample_ = true;
  return ret;
}

int ObDASDomainIdMergeIter::get_domain_id_count(const ObDASScanCtDef *ctdef, int64_t &domain_id_count)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_ID;
  domain_id_count = 0;
  if (OB_ISNULL(ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ctdef));
  } else if (!has_exist_in_array(data_table_ctdef_->domain_tids_, ctdef->ref_table_id_, &idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected domain tid", K(ret), K(ctdef->ref_table_id_), K(data_table_ctdef_->domain_tids_));
  } else if (idx < 0 || idx > data_table_ctdef_->domain_id_idxs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("undexpect idx", K(ret), K(ctdef->ref_table_id_), K(idx), K(data_table_ctdef_->domain_id_idxs_.count()));
  } else if (FALSE_IT(domain_id_count = data_table_ctdef_->domain_id_idxs_.at(idx).count())) {
  }
  return ret;
}

int ObDASDomainIdMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data table iter is nullptr", K(ret), KP(data_table_iter_));
  } else if (is_need_multi_get_) {
    if (OB_FAIL(multi_get_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to multi get data table and rowkey domain row", K(ret));
      }
    }
  } else if (!need_filter_rowkey_domain_) {
    if (OB_FAIL(concat_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to concat data table and rowkey domain row", K(ret));
      }
    }
  } else if (OB_FAIL(sorted_merge_join_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to sorted merge join data table and rowkey domain row", K(ret));
    }
  }
  LOG_TRACE("inner get next row", K(ret));
  return ret;
}

int ObDASDomainIdMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data table is nullptr", K(ret), KP(data_table_iter_));
  } else if (is_need_multi_get_) {
    if (OB_FAIL(multi_get_rows(count, capacity))) {
      LOG_WARN("fail to multi get data table and rowkey domain rows", K(ret));
    }
  } else if (!need_filter_rowkey_domain_) {
    if (OB_FAIL(concat_rows(count, capacity))) {
      LOG_WARN("fail to concat data table and rowkey domain rows", K(ret));
    }
  } else if (OB_FAIL(sorted_merge_join_rows(count, capacity))) {
    LOG_WARN("fail to sorted merge join data table and rowkey domain rows", K(ret));
  }
  LOG_TRACE("inner get next rows", K(ret), K(count), K(capacity));
  return ret;
}

int ObDASDomainIdMergeIter::build_rowkey_domain_range()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_iter_) || OB_ISNULL(data_table_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, data table iter or ctdef is nullptr", K(ret), KP(data_table_iter_), KP(data_table_ctdef_));
  } else {
    const common::ObIArray<common::ObNewRange> &key_ranges = data_table_iter_->get_scan_param().key_ranges_;
    const common::ObIArray<common::ObNewRange> &ss_key_ranges = data_table_iter_->get_scan_param().ss_key_ranges_;
    for (int64_t k = 0; OB_SUCC(ret) && k < rowkey_domain_scan_params_.count(); k++) {
      bool is_emb_vec = false;
      bool use_rowkey_vid_tbl = false;
      if (OB_ISNULL(rowkey_domain_scan_params_.at(k))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpeted error, rowkey domain scan param is nullptr", K(ret), K(k));
      } else {
        storage::ObTableScanParam& scan_param = *rowkey_domain_scan_params_.at(k);
        if (OB_FAIL(check_is_emb_vec_domain_by_table_id(scan_param.index_id_, is_emb_vec))) {
          LOG_WARN("fail to check is emb vec domain", K(ret), K(scan_param.index_id_));
        } else if (OB_FAIL(check_use_rowkey_vid_tbl_by_table_id(data_table_iter_->get_scan_param().index_id_, use_rowkey_vid_tbl))) {
          LOG_WARN("fail to check use rowkey vid", K(ret), K(data_table_iter_->get_scan_param().index_id_));
        }

        for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
          ObNewRange key_range = key_ranges.at(i);
          key_range.table_id_ = scan_param.index_id_;
          if (is_emb_vec && use_rowkey_vid_tbl) {
            // range [rowkey][vid]
            int64_t extend_start_key_obj_cnt = key_range.start_key_.get_obj_cnt() + 1;
            int64_t extend_end_key_obj_cnt = key_range.end_key_.get_obj_cnt() + 1;
            if (extend_start_key_obj_cnt != extend_end_key_obj_cnt) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error, extend start key obj cnt != extend end key obj cnt", K(ret),
                                                                                               K(extend_start_key_obj_cnt),
                                                                                               K(extend_end_key_obj_cnt),
                                                                                               K(key_range.start_key_),
                                                                                               K(key_range.end_key_));
            } else {
              ObObj *extend_start_key_obj_ptr = static_cast<ObObj *>(get_arena_allocator().alloc(sizeof(ObObj) * extend_start_key_obj_cnt));
              ObObj *extend_end_key_obj_ptr = static_cast<ObObj *>(get_arena_allocator().alloc(sizeof(ObObj) * extend_end_key_obj_cnt));
              if (OB_ISNULL(extend_start_key_obj_ptr) || OB_ISNULL(extend_end_key_obj_ptr)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to allocate memory for extend start key obj ptr", K(ret), K(extend_start_key_obj_cnt),
                                                                                              K(extend_end_key_obj_cnt));
              } else {
                for (int64_t i = 0; OB_SUCC(ret) && i < key_range.start_key_.get_obj_cnt(); i++) {
                  extend_start_key_obj_ptr[i] = key_range.start_key_.get_obj_ptr()[i];
                }
                if (key_range.border_flag_.inclusive_start() || key_range.start_key_.is_min_row()) {
                  extend_start_key_obj_ptr[key_range.start_key_.get_obj_cnt()].set_min_value();
                } else {
                  extend_start_key_obj_ptr[key_range.start_key_.get_obj_cnt()].set_max_value();
                }
                key_range.start_key_.assign(extend_start_key_obj_ptr, extend_start_key_obj_cnt);

                for (int64_t i = 0; OB_SUCC(ret) && i < key_range.end_key_.get_obj_cnt(); i++) {
                  extend_end_key_obj_ptr[i] = key_range.end_key_.get_obj_ptr()[i];
                }
                if (key_range.border_flag_.inclusive_end() || key_range.end_key_.is_max_row()) {
                  extend_end_key_obj_ptr[key_range.end_key_.get_obj_cnt()].set_max_value();
                } else {
                  extend_end_key_obj_ptr[key_range.end_key_.get_obj_cnt()].set_min_value();
                }
                key_range.end_key_.assign(extend_end_key_obj_ptr, extend_end_key_obj_cnt);
              }
            }
          }
          ObVectorIndexSyncIntervalType sync_interval_type = ObVectorIndexSyncIntervalType::VSIT_MAX;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(scan_param.key_ranges_.push_back(key_range))) {
            LOG_WARN("fail to push back key range for rowkey domain scan param", K(ret), K(key_range));
          } else if (is_emb_vec) { // hybrid vector index mode
            if (OB_FAIL(get_sync_interval_type(scan_param.index_id_, sync_interval_type))) {
              LOG_WARN("fail to get sync interval type", K(ret), K(scan_param.index_id_));
            } else if (sync_interval_type == ObVectorIndexSyncIntervalType::VSIT_MAX) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("hybrid vector index should have sync interval type", K(ret), K(scan_param.index_id_), K(sync_interval_type));
            } else {
              is_need_multi_get_ = is_need_multi_get_ == true ? true : sync_interval_type != ObVectorIndexSyncIntervalType::VSIT_IMMEDIATE;
              const ObExprPtrIArray *op_filters = data_table_iter_->get_scan_param().op_filters_;
              if (OB_NOT_NULL(op_filters) && !op_filters->empty()) {
                is_need_multi_get_ = true;
              }
            }
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < ss_key_ranges.count(); ++i) {
          ObNewRange ss_key_range = ss_key_ranges.at(i);
          ss_key_range.table_id_ = scan_param.index_id_;
          if (OB_FAIL(scan_param.ss_key_ranges_.push_back(ss_key_range))) {
            LOG_WARN("fail to push back ss key range for rowkey domain scan param", K(ret), K(ss_key_range));
          }
        }
        if (OB_SUCC(ret)) {
          scan_param.tablet_id_ = rowkey_domain_tablet_ids_.at(k);
          scan_param.ls_id_ = rowkey_domain_ls_id_;
          scan_param.sample_info_ = data_table_iter_->get_scan_param().sample_info_;
          scan_param.scan_flag_.scan_order_ = data_table_iter_->get_scan_param().scan_flag_.scan_order_;
          scan_param.enable_new_false_range_ = data_table_iter_->get_scan_param().enable_new_false_range_;
          if (!data_table_iter_->get_scan_param().need_switch_param_) {
            scan_param.need_switch_param_ = false;
          }
          is_no_sample_ = (scan_param.sample_info_.method_ == common::SampleInfo::NO_SAMPLE);
          LOG_INFO("build rowkey domain range", K(ret), K(scan_param.key_ranges_), K(scan_param.ss_key_ranges_), K(scan_param.sample_info_));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObExprPtrIArray *op_filters = data_table_iter_->get_scan_param().op_filters_;
    if (OB_ISNULL(op_filters) || (OB_NOT_NULL(op_filters) && op_filters->empty())) {
      need_filter_rowkey_domain_ = false;
    } else {
      need_filter_rowkey_domain_ = true;
    }
  }
  LOG_INFO("finish build rowkey domain ranges", K(ret), K(need_filter_rowkey_domain_), K(is_no_sample_), K(is_need_multi_get_));
  return ret;
}

int ObDASDomainIdMergeIter::init_rowkey_domain_scan_param(
    const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    transaction::ObTxDesc *trans_desc,
    transaction::ObTxReadSnapshot *snapshot,
    storage::ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  scan_param.tenant_id_ = tenant_id;
  scan_param.key_ranges_.set_attr(ObMemAttr(tenant_id, "SParamKR"));
  scan_param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "SParamSSKR"));
  if (OB_UNLIKELY(!tablet_id.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef or rtdef", K(ret), KPC(ctdef), KPC(rtdef));
  } else {
    scan_param.tablet_id_ = tablet_id;
    scan_param.ls_id_ = ls_id;
    scan_param.scan_allocator_ = &rtdef->scan_allocator_;
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = ctdef->is_get_;
    scan_param.is_for_foreign_check_ = rtdef->is_for_foreign_check_;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.force_refresh_lc_ = rtdef->force_refresh_lc_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.ext_file_column_exprs_ = &(ctdef->pd_expr_spec_.ext_file_column_exprs_);
    scan_param.ext_mapping_column_exprs_ = &(ctdef->pd_expr_spec_.ext_mapping_column_exprs_);
    scan_param.ext_mapping_column_ids_ = &(ctdef->pd_expr_spec_.ext_mapping_column_ids_);
    scan_param.ext_column_dependent_exprs_ = &(ctdef->pd_expr_spec_.ext_column_convert_exprs_);
    scan_param.ext_enable_late_materialization_ = ctdef->pd_expr_spec_.ext_enable_late_materialization_;
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.limit_param_ = rtdef->limit_param_;
    scan_param.need_scn_ = rtdef->need_scn_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.fb_snapshot_ = rtdef->fb_snapshot_;
    scan_param.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    if (rtdef->is_for_foreign_check_) {
      scan_param.trans_desc_ = trans_desc;
    }
    if (OB_NOT_NULL(snapshot)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null snapshot", K(ret), KPC(ctdef), KPC(rtdef));
    }
    if (OB_NOT_NULL(trans_desc)) {
      scan_param.tx_id_ = trans_desc->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_FAIL(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to assign column ids", K(ret));
    }
    if (rtdef->sample_info_ != nullptr) {
      scan_param.sample_info_ = *rtdef->sample_info_;
      is_no_sample_ = (scan_param.sample_info_.method_ == common::SampleInfo::NO_SAMPLE);
    }
  }

  LOG_INFO("init rowkey domain table scan param finished", K(scan_param), K(ret));
  return ret;
}

int ObDASDomainIdMergeIter::concat_row()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DomainIDCR"));
  if (OB_FAIL(data_table_iter_->get_next_row())) {
    if (OB_ITER_END == ret && is_no_sample_) {
      int tmp_ret = ret;
      ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
        if (OB_ISNULL(rowkey_domain_iters_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null domain iter", K(ret), K(i));
        } else if (OB_FAIL(rowkey_domain_iters_.at(i)->get_next_row())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next rows", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          common::ObRowkey rowkey;
          if (OB_FAIL(get_rowkey(allocator, rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), rowkey))) {
            LOG_WARN("fail to process_data_table_rowkey", K(ret));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row count isn't equal between data table and rowkey domain", K(ret), K(rowkey),
                K(rowkey_domain_iters_.at(i)->get_scan_param()), K(data_table_iter_->get_scan_param()));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    } else if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
      if (is_no_sample_) {
        if (OB_ISNULL(rowkey_domain_iters_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null domain iter", K(ret), K(i));
        } else if (OB_FAIL(rowkey_domain_iters_.at(i)->get_next_row())) {
          LOG_WARN("fail to get next row", K(ret));
          int tmp_ret = OB_SUCCESS;
          common::ObRowkey rowkey;
          if (OB_TMP_FAIL(get_rowkey(allocator, data_table_ctdef_, data_table_rtdef_, rowkey))) {
            LOG_WARN("fail to process_data_table_rowkey", K(ret), K(tmp_ret));
          } else {
            LOG_WARN("data table rowkey", K(ret), K(rowkey), K(rowkey_domain_iters_.at(i)->get_scan_param()),
                K(data_table_iter_->get_scan_param()));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_and_fill_domain_id_in_data_table(rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), allocator))) {
        LOG_WARN("fail to get and fill domain id", K(ret));
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::concat_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DomainIDCRS"));
  int64_t data_row_cnt = 0;
  int64_t rowkey_domain_row_cnt = 0;
  ObArray<share::ObDomainIdUtils::DomainIds> domain_ids;
  if (OB_FAIL(data_table_iter_->get_next_rows(data_row_cnt, capacity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
  } else {
    const bool expect_iter_end = (OB_ITER_END == ret);
    ret = OB_SUCCESS; // recover ret from iter end
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
      domain_ids.reset();
      int64_t real_cap = (data_row_cnt > 0 && !expect_iter_end) ? data_row_cnt : capacity;
      if (!is_no_sample_) {
        int64_t domain_id_count = 0;
        share::ObDomainIdUtils::DomainIds tmp_domain_id;
        if (OB_FAIL(get_domain_id_count(rowkey_domain_ctdefs_.at(i), domain_id_count))) {
          LOG_WARN("fail to get domain id count");
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < domain_id_count; ++j) {
            if (OB_FAIL(tmp_domain_id.push_back(ObString()))) {
              LOG_WARN("fail to push back domain id", K(ret));
            }
          }
          for (int64_t j = 0; j < data_row_cnt && OB_SUCC(ret); j++) {
            if (OB_FAIL(domain_ids.push_back(tmp_domain_id))) {
              LOG_WARN("fail to push back mock domain id into array", K(ret), K(j), K(data_row_cnt));
            }
          }
        }

        if (OB_SUCC(ret)) {
          ret = expect_iter_end ? OB_ITER_END : OB_SUCCESS;
        }
      } else {
        while (OB_SUCC(ret) && (real_cap > 0 || expect_iter_end)) {
          rowkey_domain_row_cnt = 0;
          if (OB_FAIL(rowkey_domain_iters_.at(i)->get_next_rows(rowkey_domain_row_cnt, real_cap))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("fail to get next row", K(ret), K(data_row_cnt), K(real_cap), K(domain_ids));
            }
          }
          if (OB_FAIL(ret) && OB_ITER_END != ret) {
          } else if (rowkey_domain_row_cnt > 0) {
            const int tmp_ret = ret;
            if (OB_FAIL(get_domain_ids(rowkey_domain_row_cnt,
                                      rowkey_domain_ctdefs_.at(i),
                                      rowkey_domain_rtdefs_.at(i),
                                      allocator,
                                      domain_ids))) {
              LOG_WARN("fail to get domain ids", K(ret), K(count));
            } else {
              ret = tmp_ret;
            }
          }
          real_cap -= rowkey_domain_row_cnt;
        }
      }
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
      } else if (expect_iter_end && OB_ITER_END != ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row count isn't equal between data table and rowkey domain", K(ret), K(capacity), K(rowkey_domain_row_cnt),
            K(data_row_cnt));
      } else if (OB_UNLIKELY(data_row_cnt != domain_ids.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The row count of data table isn't equal to rowkey domain", K(ret), K(data_row_cnt),
          K(domain_ids), K(data_table_iter_->get_scan_param()), K(rowkey_domain_iters_.at(i)->get_scan_param()));
      } else {
        count = data_row_cnt;
        if (count > 0) {
          const int tmp_ret = ret;
          if (OB_FAIL(fill_domain_ids_in_data_table((rowkey_domain_ctdefs_.at(i))->ref_table_id_, domain_ids))) {
            LOG_WARN("fail to fill domain ids in data table", K(ret), K(tmp_ret), K((rowkey_domain_ctdefs_.at(i))->ref_table_id_), K(domain_ids));
          } else {
            ret = tmp_ret;
          }
        }
      }
      // recover ret from iter end when not last one
      if (ret == OB_ITER_END && i < rowkey_domain_iters_.count() - 1) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::sorted_merge_join_row()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DomainIDMR"));
  common::ObRowkey data_table_rowkey;
  if (OB_FAIL(data_table_iter_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next data table row", K(ret));
  } else if (OB_ITER_END == ret) {
    if (is_no_sample_) {
      for (int64_t i = 0; i < rowkey_domain_iters_.count(); i++) {
        while (OB_SUCC(rowkey_domain_iters_.at(i)->get_next_row()));
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next rowkey domain row", K(ret), K(i), KPC(rowkey_domain_iters_.at(i)));
        }
      }
    }
  } else if (is_no_sample_ && OB_FAIL(get_rowkey(allocator, data_table_ctdef_, data_table_rtdef_, data_table_rowkey))) {
    LOG_WARN("fail to get data table rowkey", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
      bool is_found = false;
      while (OB_SUCC(ret) && !is_found && is_no_sample_) {
        common::ObRowkey rowkey_domain_rowkey;
        if (OB_FAIL(rowkey_domain_iters_.at(i)->get_next_row())) {
          LOG_WARN("fail to get next rowkey domain row", K(ret));
        } else if (OB_FAIL(get_rowkey(allocator, rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), rowkey_domain_rowkey))) {
          LOG_WARN("fail to get rowkey domain rowkey");
        } else if (rowkey_domain_rowkey.equal(data_table_rowkey, is_found)) {
          LOG_WARN("fail to equal rowkey between data table and rowkey", K(ret));
        }
        LOG_TRACE("compare one row in rowkey domain", K(ret), "need_skip=", !is_found, K(data_table_rowkey),
            K(rowkey_domain_rowkey));
      }
      if (OB_FAIL(ret)) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, The row count of data table isn't equal to rowkey domain", K(ret));
        }
      } else if (OB_FAIL(get_and_fill_domain_id_in_data_table(rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), allocator))) {
        LOG_WARN("fail to get domain id", K(ret));
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::sorted_merge_join_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DomainIdMRs"));
  common::ObArray<common::ObRowkey> rowkeys_in_data_table;
  common::ObArray<share::ObDomainIdUtils::DomainIds> domain_ids;
  bool is_iter_end = false;
  int64_t data_table_cnt = 0;
  if (OB_FAIL(data_table_iter_->get_next_rows(data_table_cnt, capacity)) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next data table rows", K(ret), K(data_table_cnt), K(capacity), KPC(data_table_iter_));
  } else if (0 == data_table_cnt && OB_ITER_END == ret) {
    count = 0;
  } else if (OB_UNLIKELY(0 == data_table_cnt && OB_SUCCESS == ret)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data table row count is 0, but ret code is success", K(ret), KPC(data_table_iter_));
  } else if (OB_ITER_END == ret && FALSE_IT(is_iter_end = true)) {
  } else if (is_no_sample_ && OB_FAIL(get_rowkeys(data_table_cnt, allocator, data_table_ctdef_, data_table_rtdef_,
          rowkeys_in_data_table))) {
    LOG_WARN("fail to get data table rowkeys", K(ret), K(data_table_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
      domain_ids.reset();
      int64_t remain_cnt = data_table_cnt;
      int64_t rowkey_domain_cnt = 0;
      if (is_no_sample_) {
        while (OB_SUCC(ret) && remain_cnt > 0) {
          common::ObArray<common::ObRowkey> rowkeys_in_rowkey_domain;
          common::ObArray<share::ObDomainIdUtils::DomainIds> domain_ids_in_rowkey_domain;
          const int64_t batch_size = remain_cnt;
          if (OB_FAIL(rowkey_domain_iters_.at(i)->get_next_rows(rowkey_domain_cnt, batch_size)) && OB_ITER_END != ret) {
            LOG_WARN("fail to get next rowkey domain rows", K(ret), K(remain_cnt),  K(batch_size), K(rowkey_domain_iters_.at(i)));
          } else if (OB_UNLIKELY(OB_ITER_END == ret && (!is_iter_end || 0 == rowkey_domain_cnt))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, iter end is reached at rowkey domain, but not at data table", K(ret), K(is_iter_end),
                K(rowkey_domain_cnt));
          } else if (OB_FAIL(get_rowkeys_and_domain_ids(rowkey_domain_cnt, allocator, rowkey_domain_ctdefs_.at(i),
                  rowkey_domain_rtdefs_.at(i), rowkeys_in_rowkey_domain, domain_ids_in_rowkey_domain))) {
            LOG_WARN("fail to get rowkey domain rowkeys", K(ret), K(rowkey_domain_cnt));
          } else {
            for (int64_t k = data_table_cnt - remain_cnt, j = 0;
              OB_SUCC(ret) && k < data_table_cnt && j < rowkeys_in_rowkey_domain.count();
              ++j) {
              bool is_equal = false;
              LOG_TRACE("compare one row in rowkey domain", K(ret), K(k), K(j), K(rowkeys_in_data_table.at(k)),
                  K(rowkeys_in_rowkey_domain.at(j)));
              if (rowkeys_in_rowkey_domain.at(j).equal(rowkeys_in_data_table.at(k), is_equal)) {
                LOG_WARN("fail to equal rowkey between data table and rowkey", K(ret));
              } else if (is_equal) {
                if (OB_FAIL(domain_ids.push_back(domain_ids_in_rowkey_domain.at(j)))) {
                  LOG_WARN("fail to push back domain id", K(ret), K(j), K(rowkeys_in_rowkey_domain));
                } else {
                  --remain_cnt;
                  ++k;
                  LOG_TRACE("find domain id in rowkey domain", K(rowkeys_in_rowkey_domain.at(j)), K(remain_cnt), K(k), K(data_table_cnt));
                }
              }
            }
          }
        }
      } else {
        int64_t domain_id_count = 0;
        share::ObDomainIdUtils::DomainIds tmp_domain_id;
        if (OB_FAIL(get_domain_id_count(rowkey_domain_ctdefs_.at(i), domain_id_count))) {
          LOG_WARN("fail to get domain id count", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < domain_id_count; ++j) {
            if (OB_FAIL(tmp_domain_id.push_back(ObString()))) {
              LOG_WARN("fail to push back domain id", K(ret));
            }
          }
          for (int64_t j = 0; j < data_table_cnt && OB_SUCC(ret); j++) {
            if (OB_FAIL(domain_ids.push_back(tmp_domain_id))) {
              LOG_WARN("fail to push back mock domain id into array", K(ret), K(j), K(data_table_cnt));
            }
          }
        }
      }
      if (FAILEDx(fill_domain_ids_in_data_table((rowkey_domain_ctdefs_.at(i))->ref_table_id_, domain_ids))) {
        LOG_WARN("fail to fill domain ids in data table", K(ret), K((rowkey_domain_ctdefs_.at(i))->ref_table_id_), K(domain_ids));
      }
    }
    if (OB_SUCC(ret)) {
      count = data_table_cnt;
      ret = is_iter_end ? OB_ITER_END : ret;
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::get_rowkey(
    common::ObIAllocator &allocator,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ctdef), KP(rtdef));
  } else {
    const int64_t rowkey_cnt = ctdef->table_param_.get_read_info().get_schema_rowkey_count();
    const int64_t output_cnt = ctdef->pd_expr_spec_.access_exprs_.count();
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate rowkey obj buffer", K(ret), K(rowkey_cnt));
    } else {
      ObObj *obj_ptr = new (buf) ObObj[rowkey_cnt];
      int64_t j = 0;
      for (int64_t i = 0; OB_SUCC(ret) && j < rowkey_cnt && i < output_cnt; ++i) {
        ObExpr *expr = ctdef->pd_expr_spec_.access_exprs_.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, expr is nullptr", K(ret), K(i), K(j), KPC(ctdef));
        } else if (T_PSEUDO_GROUP_ID == expr->type_ || T_PSEUDO_ROW_TRANS_INFO_COLUMN == expr->type_) {
          // nothing to do.
          LOG_TRACE("skip expr", K(i), K(j), KPC(expr));
        } else {
          ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
          if (OB_FAIL(datum.to_obj(obj_ptr[j], expr->obj_meta_, expr->obj_datum_map_))) {
            LOG_WARN("fail to convert datum to obj", K(ret));
          } else {
            ++j;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(j < rowkey_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, outputs is less than rowkey count", K(ret), K(output_cnt), K(j),
            K(rowkey_cnt), KPC(ctdef));
      } else {
        rowkey.assign(obj_ptr, rowkey_cnt);
        LOG_TRACE("get one rowkey", K(rowkey), K(output_cnt), K(j), K(rowkey_cnt));
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::get_rowkeys(
    const int64_t size,
    common::ObIAllocator &allocator,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObIArray<common::ObRowkey> &rowkeys)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*rtdef->eval_ctx_);
  batch_info_guard.set_batch_size(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    batch_info_guard.set_batch_idx(i);
    common::ObRowkey rowkey;
    if (OB_FAIL(get_rowkey(allocator, ctdef, rtdef, rowkey))) {
      LOG_WARN("fail to process_data_table_rowkey", K(ret), K(i));
    } else if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("fail to push back rowkey", K(ret), K(rowkey));
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::get_domain_id(
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObIAllocator &allocator,
    share::ObDomainIdUtils::DomainIds &domain_id)
{
  int ret = OB_SUCCESS;
  int64_t domain_type = ObDomainIdUtils::ObDomainIDType::MAX;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef) || OB_ISNULL(data_table_ctdef_) || OB_ISNULL(data_table_rtdef_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ctdef), KP(rtdef), KP(data_table_ctdef_), KP(data_table_rtdef_));
  } else {
    int64_t idx = OB_INVALID_ID;
    if (!has_exist_in_array(data_table_ctdef_->domain_tids_, ctdef->ref_table_id_, &idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected domain tid", K(ret), K(ctdef->ref_table_id_), K(data_table_ctdef_->domain_tids_));
    } else if (idx < 0 || idx > data_table_ctdef_->domain_id_idxs_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx", K(ret), K(ctdef->ref_table_id_), K(idx), K(data_table_ctdef_->domain_id_idxs_.count()));
    } else if (FALSE_IT(domain_type = data_table_ctdef_->domain_types_.at(idx))) {
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t rowkey_cnt = ctdef->table_param_.get_read_info().get_schema_rowkey_count();
    int64_t part_key_num = 0;
    ObExpr *expr = nullptr;
    int64_t expect_result_output_cnt = rowkey_cnt + 1;
    expect_result_output_cnt = OB_NOT_NULL(ctdef->trans_info_expr_) ? (expect_result_output_cnt + 1) : expect_result_output_cnt;
    expect_result_output_cnt = (domain_type == ObDomainIdUtils::IVFPQ_CID) ? (expect_result_output_cnt + 1) : expect_result_output_cnt;
    // when partition and heap organization table, use hybrid vector index; the dml return all_columns as output, but tsc only use
    // rowkey and embeded_vec; the dml has more columns that is partition key.
    if (domain_type != ObDomainIdUtils::EMB_VEC) {
      // skip, do nothing
    } else if (OB_FAIL(check_table_need_add_part_key(data_table_iter_->get_scan_param().index_id_, part_key_num, ctdef))) {
      LOG_WARN("fail to check embedded table add part key ", K(ret), K(data_table_iter_->get_scan_param().index_id_), K(ctdef->ref_table_id_));
    } else {
      expect_result_output_cnt = expect_result_output_cnt + part_key_num;
    }
    // When the defensive check level is set to 2 (strict defensive check), the transaction information of the current
    // row is recorded for 4377 diagnosis. Then, it will add pseudo_trans_info_expr into result output of das scan.
    //
    // just skip it if trans info expr in ctdef isn't nullptr.
    if (OB_UNLIKELY(ctdef->result_output_.count() != expect_result_output_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected result output column count", K(ret), K(rowkey_cnt), K(ctdef->result_output_.count()));
    }

    int domain_id_num = (domain_type == ObDomainIdUtils::IVFPQ_CID) ? 2 : 1;
    for (int i = 0; OB_SUCC(ret) && i < domain_id_num; ++i) {
      if (OB_ISNULL(expr = ctdef->result_output_.at(rowkey_cnt + part_key_num + i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, domain id expr is nullptr", K(ret), K(rowkey_cnt), K(ctdef->result_output_));
      } else {
        ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
        if (datum.get_string().length() == 0) {
          if (OB_FAIL(domain_id.push_back(ObString()))) {
            LOG_WARN("failed to push back domain id", K(ret));
          }
        } else {
          void *buf = allocator.alloc(datum.get_string().length());
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret), KP(buf), K(domain_type), K(ctdef->ref_table_id_), K(datum.get_string().length()));
          } else {
            memcpy(buf, datum.get_string().ptr(), datum.get_string().length());
            ObString tmp_domain_id;
            tmp_domain_id.assign_ptr(reinterpret_cast<char*>(buf), datum.get_string().length());
            if (OB_FAIL(domain_id.push_back(tmp_domain_id))) {
              LOG_WARN("failed to push back domain id", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::get_and_fill_domain_id_in_data_table(
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef) || OB_ISNULL(data_table_ctdef_) || OB_ISNULL(data_table_rtdef_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ctdef), KP(rtdef), KP(data_table_ctdef_), KP(data_table_rtdef_));
  } else {
    share::ObDomainIdUtils::DomainIds domain_id;
    if (is_no_sample_ && OB_FAIL(get_domain_id(ctdef, rtdef, allocator, domain_id))) {
      LOG_WARN("fail to get domain id",K(ret));
    } else {
      int64_t domain_type = ObDomainIdUtils::ObDomainIDType::MAX;
      DomainIdxs domain_id_idxs;
      int64_t idx = OB_INVALID_ID;
      ObExpr *domain_id_expr = nullptr;

      if (!has_exist_in_array(data_table_ctdef_->domain_tids_, ctdef->ref_table_id_, &idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain tid", K(ret), K(ctdef->ref_table_id_), K(data_table_ctdef_->domain_tids_));
      } else if (idx < 0 || idx > data_table_ctdef_->domain_id_idxs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("undexpect idx", K(ret), K(ctdef->ref_table_id_), K(idx), K(data_table_ctdef_->domain_id_idxs_.count()));
      } else if (FALSE_IT(domain_type = data_table_ctdef_->domain_types_.at(idx))) {
      } else if (FALSE_IT(domain_id_idxs = data_table_ctdef_->domain_id_idxs_.at(idx))) {
      } else if (is_no_sample_ && domain_id.count() != domain_id_idxs.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id count", K(ret), K(domain_id), K(domain_id_idxs));
      } else {
        ObString empty_str;
        for (int64_t i = 0; OB_SUCC(ret) && i < domain_id_idxs.count(); ++i) {
          const int64_t domain_id_idx = domain_id_idxs.at(i);
          const ObString& domain_id_str = is_no_sample_ ? domain_id.at(i) : empty_str;
          domain_id_expr = nullptr;
          if (domain_id_idx == -1) { // do nothing
          } else if (domain_id_idx < 0 || domain_id_idx >= data_table_ctdef_->result_output_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get valid domain id idx", K(ret), K(data_table_ctdef_->domain_id_idxs_));
          } else if (OB_ISNULL(domain_id_expr = data_table_ctdef_->result_output_.at(domain_id_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted error, domain id expr is nullptr", K(ret), K(domain_id_idx), KPC(data_table_ctdef_));
          } else if (OB_FAIL(ObDomainIdUtils::fill_domain_id_datum(static_cast<ObDomainIdUtils::ObDomainIDType>(domain_type),
              domain_id_expr, data_table_rtdef_->eval_ctx_, domain_id_str))) {
            LOG_WARN("fail to fill domain id datum", K(ret), K(domain_type));
          } else {
            domain_id_expr->set_evaluated_projected(*data_table_rtdef_->eval_ctx_);
            LOG_TRACE("Domain id merge fill a domain id", K(domain_type), KP(domain_id_expr), KPC(domain_id_expr));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::get_domain_ids(
      const int64_t size,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObIAllocator &allocator,
      common::ObIArray<share::ObDomainIdUtils::DomainIds> &domain_ids)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*rtdef->eval_ctx_);
  batch_info_guard.set_batch_size(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    batch_info_guard.set_batch_idx(i);
    share::ObDomainIdUtils::DomainIds domain_id;
    if (OB_FAIL(get_domain_id(ctdef, rtdef, allocator, domain_id))) {
      LOG_WARN("fail to get domain id", K(ret), K(i));
    } else if (OB_FAIL(domain_ids.push_back(domain_id))) {
      LOG_WARN("fail to push back domain id", K(ret), K(domain_id));
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::fill_domain_ids_in_data_table(
    const uint64_t domain_tid,
    const common::ObIArray<share::ObDomainIdUtils::DomainIds> &domain_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == domain_ids.count() || OB_INVALID_ID == domain_tid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(domain_ids), K(domain_tid));
  } else if (OB_ISNULL(data_table_ctdef_) || OB_ISNULL(data_table_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, data table ctdef is nullptr", K(ret), KP(data_table_ctdef_), KP(data_table_rtdef_));
  } else {
    int64_t domain_type = OB_INVALID_ID;
    ObExpr *domain_id_expr = nullptr;
    DomainIdxs domain_id_idxs;
    int64_t idx = OB_INVALID_ID;

    if (!has_exist_in_array(data_table_ctdef_->domain_tids_, domain_tid, &idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected domain tid", K(ret), K(domain_tid), K(data_table_ctdef_->domain_tids_));
    } else if (idx < 0 || idx > data_table_ctdef_->domain_id_idxs_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("undexpect idx", K(ret), K(domain_tid), K(idx), K(data_table_ctdef_->domain_id_idxs_.count()));
    } else if (FALSE_IT(domain_type = data_table_ctdef_->domain_types_.at(idx))) {
    } else if (FALSE_IT(domain_id_idxs = data_table_ctdef_->domain_id_idxs_.at(idx))) {
    } else if (domain_ids.at(0).count() != domain_id_idxs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected domain id count", K(ret), K(domain_id_idxs));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < domain_id_idxs.count(); ++i) {
        const int64_t domain_id_idx = domain_id_idxs.at(i);
        domain_id_expr = nullptr;
        if (domain_id_idx == -1) { // do nothing
        } else if (domain_id_idx < 0 || domain_id_idx >= data_table_ctdef_->result_output_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get valid domain id idx", K(ret), K(data_table_ctdef_->domain_id_idxs_));
        } else if (OB_ISNULL(domain_id_expr = data_table_ctdef_->result_output_.at(domain_id_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, domain id expr is nullptr", K(ret), K(domain_id_idx), KPC(data_table_ctdef_));
        } else if (OB_FAIL(ObDomainIdUtils::fill_batch_domain_id_datum(static_cast<ObDomainIdUtils::ObDomainIDType>(domain_type),
            domain_id_expr, data_table_rtdef_->eval_ctx_, domain_ids, i))) {
          LOG_WARN("fail to fill domain id datum", K(ret), K(domain_type));
        } else {
          domain_id_expr->set_evaluated_projected(*data_table_rtdef_->eval_ctx_);
          LOG_TRACE("Domain id merge fill a domain id", K(domain_type), KP(domain_id_expr), KPC(domain_id_expr));
        }
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::get_rowkeys_and_domain_ids(
    const int64_t size,
    common::ObIAllocator &allocator,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObIArray<common::ObRowkey> &rowkeys,
    common::ObIArray<share::ObDomainIdUtils::DomainIds> &domain_ids)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*rtdef->eval_ctx_);
  batch_info_guard.set_batch_size(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    batch_info_guard.set_batch_idx(i);
    common::ObRowkey rowkey;
    share::ObDomainIdUtils::DomainIds domain_id;
    if (OB_FAIL(get_rowkey(allocator, ctdef, rtdef, rowkey))) {
      LOG_WARN("fail to process_data_table_rowkey", K(ret), K(i));
    } else if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("fail to push back rowkey", K(ret), K(rowkey));
    } else if (OB_FAIL(get_domain_id(ctdef, rtdef, allocator, domain_id))) {
      LOG_WARN("fail to get domain id", K(ret), K(i));
    } else if (OB_FAIL(domain_ids.push_back(domain_id))) {
      LOG_WARN("fail to push back domain id", K(ret), K(domain_id));
    }
  }
  return ret;
}


int ObDASDomainIdMergeIter::multi_get_row()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DomainIDMGR"));
  if (OB_FAIL(data_table_iter_->get_next_row())) {
    if (OB_ITER_END == ret && is_no_sample_) {
      int tmp_ret = ret;
      ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
        if (OB_ISNULL(rowkey_domain_iters_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null domain iter", K(ret), K(i));
        } else if (OB_FAIL(rowkey_domain_iters_.at(i)->get_next_row())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next rows", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          common::ObRowkey rowkey;
          if (OB_FAIL(get_rowkey(allocator, rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), rowkey))) {
            LOG_WARN("fail to process_data_table_rowkey", K(ret));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row count isn't equal between data table and rowkey domain", K(ret), K(rowkey),
                K(rowkey_domain_iters_.at(i)->get_scan_param()), K(data_table_iter_->get_scan_param()));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    } else if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
      if (is_no_sample_) {
        if (OB_ISNULL(rowkey_domain_iters_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null domain iter", K(ret), K(i));
        } else {
          common::ObRowkey data_table_rowkey;
          if (OB_FAIL(get_rowkey(allocator, data_table_ctdef_, data_table_rtdef_, data_table_rowkey))) {
            LOG_WARN("fail to get data table rowkey", K(ret));
          } else if (OB_FAIL(reset_rowkey_domain_iter_scan_range(i, data_table_rowkey))) {
            LOG_WARN("fail to reset rowkey domain iter scan range", K(ret), K(i));
          } else {
            bool is_found = false;
            while (OB_SUCC(ret) && !is_found) {
              common::ObRowkey rowkey_domain_rowkey;
              if (OB_FAIL(rowkey_domain_iters_.at(i)->get_next_row())) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  break;
                } else {
                  LOG_WARN("fail to get next rowkey domain row", K(ret));
                }
              } else if (OB_FAIL(get_rowkey(allocator, rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), rowkey_domain_rowkey))) {
                LOG_WARN("fail to get rowkey domain rowkey", K(ret));
              } else if (rowkey_domain_rowkey.equal(data_table_rowkey, is_found)) {
                LOG_WARN("fail to equal rowkey between data table and rowkey", K(ret));
              }
              LOG_TRACE("compare one row in rowkey domain", K(ret), "need_skip=", !is_found,
                       K(data_table_rowkey), K(rowkey_domain_rowkey));
            }

            if (OB_FAIL(ret)) {
              if (OB_ITER_END == ret) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected error, The row count of data table isn't equal to rowkey domain", K(ret));
              }
            } else {
              if (is_found) {
                if (OB_FAIL(get_and_fill_domain_id_in_data_table(rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), allocator))) {
                  LOG_WARN("fail to get domain id", K(ret));
                }
              } else {
                bool is_emb_vec_domain = false;
                if (OB_FAIL(check_is_emb_vec_domain(i, is_emb_vec_domain))) {
                  LOG_WARN("fail to check is emb_vec domain", K(ret), K(i));
                } else if (is_emb_vec_domain) {
                  if (OB_FAIL(fill_null_domain_id_in_data_table(rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), allocator))) {
                    LOG_WARN("fail to fill null domain id for emb_vec", K(ret));
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected error, The row count of data table isn't equal to rowkey domain for non-emb_vec type", K(ret), K(i));
                }
              }
            }
          }
        }
      } else {
        if (OB_FAIL(get_and_fill_domain_id_in_data_table(rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), allocator))) {
          LOG_WARN("fail to get and fill domain id", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::multi_get_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DomainIDMGRS"));
  int64_t data_row_cnt = 0;
  ObArray<share::ObDomainIdUtils::DomainIds> domain_ids;

  if (OB_FAIL(data_table_iter_->get_next_rows(data_row_cnt, capacity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }

  if (OB_FAIL(ret) && OB_ITER_END != ret) {
  } else {
    const bool expect_iter_end = (OB_ITER_END == ret);
    ret = OB_SUCCESS; // recover ret from iter end

    common::ObArray<common::ObRowkey> data_table_rowkeys;
    if (is_no_sample_ && data_row_cnt > 0) {
      if (OB_FAIL(get_rowkeys(data_row_cnt, allocator, data_table_ctdef_, data_table_rtdef_, data_table_rowkeys))) {
        LOG_WARN("fail to get data table rowkeys", K(ret), K(data_row_cnt));
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_domain_iters_.count(); i++) {
        bool is_emb_vec_domain = false;
        bool use_rowkey_vid_tbl = false;
        domain_ids.reset();
        if (OB_FAIL(check_is_emb_vec_domain(i, is_emb_vec_domain))) {
          LOG_WARN("fail to check is emb_vec domain", K(ret), K(i));
        } else if (OB_FAIL(check_use_rowkey_vid_tbl_by_table_id(data_table_iter_->get_scan_param().index_id_, use_rowkey_vid_tbl))) {
          LOG_WARN("fail to check use rowkey vid", K(ret), K(data_table_iter_->get_scan_param().index_id_));
        } else if (!is_no_sample_) {
          int64_t domain_id_count = 0;
          share::ObDomainIdUtils::DomainIds tmp_domain_id;
          if (OB_FAIL(get_domain_id_count(rowkey_domain_ctdefs_.at(i), domain_id_count))) {
            LOG_WARN("fail to get domain id count");
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < domain_id_count; ++j) {
              if (OB_FAIL(tmp_domain_id.push_back(ObString()))) {
                LOG_WARN("fail to push back domain id", K(ret));
              }
            }
            for (int64_t j = 0; j < data_row_cnt && OB_SUCC(ret); j++) {
              if (OB_FAIL(domain_ids.push_back(tmp_domain_id))) {
                LOG_WARN("fail to push back mock domain id into array", K(ret), K(j), K(data_row_cnt));
              }
            }
          }

          if (OB_SUCC(ret)) {
            ret = expect_iter_end ? OB_ITER_END : OB_SUCCESS;
          }
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < data_row_cnt; j++) {
            common::ObRowkey data_table_rowkey = data_table_rowkeys.at(j);
            bool is_found = false;
            common::ObRowkey rowkey_domain_rowkey;
            if (OB_FAIL(reset_rowkey_domain_iter_scan_range(i, data_table_rowkey))) {
              LOG_WARN("fail to reset rowkey domain iter scan range for row", K(ret), K(i), K(j));
            } else if (OB_FAIL(rowkey_domain_iters_.at(i)->get_next_row())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to get next rowkey domain row", K(ret));
              }
            } else if (OB_FAIL(get_rowkey(allocator, rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), rowkey_domain_rowkey))) {
              LOG_WARN("fail to get rowkey domain rowkey", K(ret));
            } else {
              if (is_emb_vec_domain && use_rowkey_vid_tbl) {
                // [rowkey] [vid] -> [rowkey]
                ObObj *extend_end_key_obj_ptr = static_cast<ObObj *>(get_arena_allocator().alloc(sizeof(ObObj) * (data_table_rowkey.get_obj_cnt())));
                if (OB_ISNULL(extend_end_key_obj_ptr)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fail to allocate memory for extend end key", K(ret));
                } else {
                  for (int64_t j = 0; OB_SUCC(ret) && j < data_table_rowkey.get_obj_cnt(); j++) {
                    extend_end_key_obj_ptr[j] = rowkey_domain_rowkey.get_obj_ptr()[j];
                  }
                  rowkey_domain_rowkey.assign(extend_end_key_obj_ptr, data_table_rowkey.get_obj_cnt());
                }
              }
            }

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(rowkey_domain_rowkey.equal(data_table_rowkey, is_found))) {
              LOG_WARN("fail to equal rowkey between data table and rowkey domain", K(ret));
            } else if (is_found) {
              share::ObDomainIdUtils::DomainIds domain_id;
              if (OB_FAIL(get_domain_id(rowkey_domain_ctdefs_.at(i), rowkey_domain_rtdefs_.at(i), allocator, domain_id))) {
                LOG_WARN("fail to get domain id", K(ret), K(j));
              } else if (OB_FAIL(domain_ids.push_back(domain_id))) {
                LOG_WARN("fail to push back domain id", K(ret), K(j));
              }
            } else {
              if (is_emb_vec_domain) {
                share::ObDomainIdUtils::DomainIds empty_domain_id;
                int64_t domain_id_count = 0;
                if (OB_FAIL(get_domain_id_count(rowkey_domain_ctdefs_.at(i), domain_id_count))) {
                  LOG_WARN("fail to get domain id count for emb_vec", K(ret), K(i));
                } else if (OB_FAIL(empty_domain_id.push_back(ObString()))) {
                  LOG_WARN("fail to push back empty domain id string", K(ret));
                } else if (OB_FAIL(domain_ids.push_back(empty_domain_id))) {
                  LOG_WARN("fail to push back empty domain id for emb_vec", K(ret), K(j));
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected error, The row count of data table isn't equal to rowkey domain for non-emb_vec type", K(ret), K(i), K(j), K(data_table_rowkey), K(rowkey_domain_rowkey));
                break;
              }
            }
          }

          if (OB_FAIL(ret) && OB_ITER_END != ret) {
          } else if (OB_UNLIKELY(data_row_cnt != domain_ids.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("The row count of data table isn't equal to rowkey domain", K(ret), K(data_row_cnt),
              K(domain_ids), K(data_table_iter_->get_scan_param()), K(rowkey_domain_iters_.at(i)->get_scan_param()));
          } else {
            count = data_row_cnt;
            if (count > 0) {
              const int tmp_ret = ret;
              if (OB_FAIL(fill_domain_ids_in_data_table((rowkey_domain_ctdefs_.at(i))->ref_table_id_, domain_ids))) {
                LOG_WARN("fail to fill domain ids in data table", K(ret), K(tmp_ret), K((rowkey_domain_ctdefs_.at(i))->ref_table_id_), K(domain_ids));
              } else {
                ret = tmp_ret;
              }
            } else {
              ret = OB_ITER_END;
            }
          }
          // recover ret from iter end when not last one
          if (ret == OB_ITER_END && i < rowkey_domain_iters_.count() - 1) {
          ret = OB_SUCCESS;
          }
        }
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::check_is_emb_vec_domain(int64_t iter_idx, bool &is_emb_vec)
{
  int ret = OB_SUCCESS;
  is_emb_vec = false;

  if (iter_idx < 0 || iter_idx >= rowkey_domain_iters_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter index", K(ret), K(iter_idx), K(rowkey_domain_iters_.count()));
  } else if (OB_ISNULL(data_table_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table ctdef is null", K(ret));
  } else {
    const uint64_t current_domain_tid = rowkey_domain_ctdefs_.at(iter_idx)->ref_table_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < data_table_ctdef_->domain_tids_.count(); i++) {
      if (data_table_ctdef_->domain_tids_.at(i) == current_domain_tid) {
        if (i < data_table_ctdef_->domain_types_.count()) {
          is_emb_vec = (data_table_ctdef_->domain_types_.at(i) == ObDomainIdUtils::ObDomainIDType::EMB_VEC);
        }
        break;
      }
    }
    LOG_DEBUG("check is emb_vec domain", K(ret), K(iter_idx), K(current_domain_tid), K(is_emb_vec));
  }
  return ret;
}

int ObDASDomainIdMergeIter::check_is_emb_vec_domain_by_table_id(int64_t table_id, bool &is_emb_vec)
{
  int ret = OB_SUCCESS;
  is_emb_vec = false;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(MTL_ID(), table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_id));
  } else {
    is_emb_vec = table_schema->is_hybrid_vec_index_embedded_type();
  }
  return ret;
}

int ObDASDomainIdMergeIter::check_use_rowkey_vid_tbl_by_table_id(int64_t table_id, bool &use_rowkey_vid_tbl)
{
  int ret = OB_SUCCESS;
  use_rowkey_vid_tbl = false;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(MTL_ID(), table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_id));
  } else {
    use_rowkey_vid_tbl = !table_schema->is_table_with_hidden_pk_column();
  }
  return ret;
}

int ObDASDomainIdMergeIter::check_table_need_add_part_key(int64_t table_id, int64_t &part_key_num, const ObDASScanCtDef *ctdef)
{
  int ret = OB_SUCCESS;
  part_key_num = 0;
  share::schema::ObSchemaGetterGuard schema_guard;
  ObIArray<uint64_t> *column_ids;
  const ObTableSchema *table_schema = nullptr;
  const ObTableSchema *domain_table_schema = nullptr;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), ctdef->ref_table_id_, domain_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(ctdef->ref_table_id_));
  } else if (OB_ISNULL(domain_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_id));
  } else if (table_schema->is_table_with_hidden_pk_column() && table_schema->is_partitioned_table()) {
    if (ctdef->result_output_.count() == domain_table_schema->get_column_count()) {
      part_key_num += table_schema->get_part_level();
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::fill_null_domain_id_in_data_table(
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef) || OB_ISNULL(data_table_ctdef_) || OB_ISNULL(data_table_rtdef_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ctdef), KP(rtdef), KP(data_table_ctdef_), KP(data_table_rtdef_));
  } else {
    int64_t domain_type = ObDomainIdUtils::ObDomainIDType::MAX;
    DomainIdxs domain_id_idxs;
    int64_t idx = OB_INVALID_ID;
    ObExpr *domain_id_expr = nullptr;

    if (!has_exist_in_array(data_table_ctdef_->domain_tids_, ctdef->ref_table_id_, &idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected domain tid", K(ret), K(ctdef->ref_table_id_), K(data_table_ctdef_->domain_tids_));
    } else if (idx < 0 || idx > data_table_ctdef_->domain_id_idxs_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("undexpect idx", K(ret), K(ctdef->ref_table_id_), K(idx), K(data_table_ctdef_->domain_id_idxs_));
    } else if (FALSE_IT(domain_type = data_table_ctdef_->domain_types_.at(idx))) {
    } else if (FALSE_IT(domain_id_idxs = data_table_ctdef_->domain_id_idxs_.at(idx))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < domain_id_idxs.count(); ++i) {
        const int64_t domain_id_idx = domain_id_idxs.at(i);
        domain_id_expr = nullptr;

        if (domain_id_idx == -1) {
        } else if (domain_id_idx < 0 || domain_id_idx >= data_table_ctdef_->result_output_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get valid domain id idx", K(ret), K(data_table_ctdef_->domain_id_idxs_));
        } else if (OB_ISNULL(domain_id_expr = data_table_ctdef_->result_output_.at(domain_id_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, domain id expr is nullptr", K(ret), K(domain_id_idx), KPC(data_table_ctdef_));
        } else {
          ObDatum &datum = domain_id_expr->locate_expr_datum(*data_table_rtdef_->eval_ctx_);
          datum.set_null();
          domain_id_expr->set_evaluated_projected(*data_table_rtdef_->eval_ctx_);
          LOG_TRACE("Domain id merge fill null domain id", K(domain_type), KP(domain_id_expr), KPC(domain_id_expr));
        }
      }
    }
  }
  return ret;
}

int ObDASDomainIdMergeIter::get_sync_interval_type(int64_t table_id, ObVectorIndexSyncIntervalType &sync_interval_type)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  ObVectorIndexParam vec_index_param;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_id));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(table_schema->get_index_params(), ObVectorIndexType::VIT_HNSW_INDEX, vec_index_param))) {
    LOG_WARN("fail to parser params from string", K(ret), K(table_schema->get_index_params()));
  } else {
    sync_interval_type = vec_index_param.sync_interval_type_;
  }
  return ret;
}


int ObDASDomainIdMergeIter::reset_rowkey_domain_iter_scan_range(int64_t iter_idx, const common::ObRowkey &data_table_rowkey)
{
  int ret = OB_SUCCESS;

  if (iter_idx < 0 || iter_idx >= rowkey_domain_scan_params_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter index", K(ret), K(iter_idx), K(rowkey_domain_scan_params_.count()));
  } else if (OB_ISNULL(data_table_iter_) || OB_ISNULL(data_table_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data table iter or ctdef is nullptr", K(ret), KP(data_table_iter_), KP(data_table_ctdef_));
  } else {
    storage::ObTableScanParam& scan_param = *rowkey_domain_scan_params_.at(iter_idx);
    ObDASScanIter *iter = rowkey_domain_iters_.at(iter_idx);
    bool is_emb_vec_domain = false;
    bool use_rowkey_vid_tbl = false;
    if (OB_FAIL(iter->reuse())) {
      LOG_WARN("fail to reuse rowkey domain iter", K(ret), K(iter_idx));
    } else if (OB_FAIL(check_is_emb_vec_domain(iter_idx, is_emb_vec_domain))) {
      LOG_WARN("fail to check is emb_vec domain", K(ret), K(iter_idx));
    } else if (OB_FAIL(check_use_rowkey_vid_tbl_by_table_id(data_table_iter_->get_scan_param().index_id_, use_rowkey_vid_tbl))) {
      LOG_WARN("fail to check use rowkey vid", K(ret), K(data_table_iter_->get_scan_param().index_id_));
    } else {
      ObNewRange key_range;
      key_range.table_id_ = scan_param.index_id_;
      key_range.border_flag_.set_inclusive_start();
      key_range.border_flag_.set_inclusive_end();

      if (is_emb_vec_domain && use_rowkey_vid_tbl) {
        ObObj *extend_start_key_obj_ptr = static_cast<ObObj *>(get_arena_allocator().alloc(sizeof(ObObj) * (data_table_rowkey.get_obj_cnt() + 1)));
        if (OB_ISNULL(extend_start_key_obj_ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for extend start key", K(ret));
        } else {
          for (int64_t j = 0; j < data_table_rowkey.get_obj_cnt(); j++) {
            extend_start_key_obj_ptr[j] = data_table_rowkey.get_obj_ptr()[j];
          }
          extend_start_key_obj_ptr[data_table_rowkey.get_obj_cnt()].set_min_value();
          key_range.start_key_.assign(extend_start_key_obj_ptr, data_table_rowkey.get_obj_cnt() + 1);

          ObObj *extend_end_key_obj_ptr = static_cast<ObObj *>(get_arena_allocator().alloc(sizeof(ObObj) * (data_table_rowkey.get_obj_cnt() + 1)));
          if (OB_ISNULL(extend_end_key_obj_ptr)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory for extend end key", K(ret));
          } else {
            for (int64_t j = 0; j < data_table_rowkey.get_obj_cnt(); j++) {
              extend_end_key_obj_ptr[j] = data_table_rowkey.get_obj_ptr()[j];
            }
            extend_end_key_obj_ptr[data_table_rowkey.get_obj_cnt()].set_max_value();
            key_range.end_key_.assign(extend_end_key_obj_ptr, data_table_rowkey.get_obj_cnt() + 1);

            if (OB_FAIL(scan_param.key_ranges_.push_back(key_range))) {
              LOG_WARN("fail to push back key range for emb_vec scan param", K(ret), K(key_range));
            }
          }
        }
      } else {
        key_range.start_key_ = data_table_rowkey;
        key_range.end_key_ = data_table_rowkey;
        if (OB_FAIL(scan_param.key_ranges_.push_back(key_range))) {
          LOG_WARN("fail to push back key range for scan param", K(ret), K(key_range));
        }
      }
    }
    if (OB_SUCC(ret)) {
      scan_param.tablet_id_ = rowkey_domain_tablet_ids_.at(iter_idx);
      scan_param.ls_id_ = rowkey_domain_ls_id_;
      scan_param.sample_info_ = data_table_iter_->get_scan_param().sample_info_;
      scan_param.scan_flag_.scan_order_ = data_table_iter_->get_scan_param().scan_flag_.scan_order_;
      if (!data_table_iter_->get_scan_param().need_switch_param_) {
        scan_param.need_switch_param_ = false;
      }
      if (OB_FAIL(iter->rescan())) {
        LOG_WARN("fail to rescan rowkey domain iter", K(ret), K(iter_idx));
      }
      LOG_INFO("reset domain iter scan range", K(ret), K(iter_idx), K(is_emb_vec_domain),
                K(scan_param.key_ranges_), K(data_table_rowkey));
    }
  }

  return ret;
}

}
}
