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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_group_scan_op.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
namespace sql
{
ObDASGroupScanOp::ObDASGroupScanOp(ObIAllocator &op_alloc)
  : ObDASScanOp(op_alloc),
    group_lookup_op_(NULL),
    iter_(),
    result_iter_(&iter_),
    is_exec_remote_(false),
    cur_group_idx_(0),
    group_size_(0)
{
}

ObDASGroupScanOp::~ObDASGroupScanOp()
{
  if (nullptr != group_lookup_op_) {
    group_lookup_op_->~ObGroupLookupOp();
    //Memory of lookupop come from op_alloc,We do not need free,just set ptr to null.
    group_lookup_op_ = nullptr;
  }
}

int ObDASGroupScanOp::rescan()
{
  int &ret = errcode_;
  if (OB_FAIL(ObDASScanOp::rescan())) {
    LOG_WARN("rescan the table iterator failed", K(ret));
  } else {
    iter_.init_group_range(cur_group_idx_, group_size_);
    if (group_lookup_op_ != nullptr) {
      group_lookup_op_->reset();
      ObGroupScanIter *group_iter = NULL;
      group_iter = static_cast<ObGroupScanIter *>(group_lookup_op_->get_lookup_iter());
      if (NULL == group_iter) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(group_iter), K(*group_lookup_op_), K(ret));
      } else {
        group_iter->init_group_range(iter_.get_cur_group_idx(), iter_.get_group_size());
      }
    }
  }
  return ret;
}

int ObDASGroupScanOp::release_op()
{
  int ret = OB_SUCCESS;
  int group_ret = OB_SUCCESS;

  if (OB_FAIL(ObDASScanOp::release_op())) {
    LOG_WARN("release das scan op fail", K(ret));
  }

  if (nullptr != group_lookup_op_) {
    group_ret = group_lookup_op_->revert_iter();
    if (OB_SUCCESS != group_ret) {
      LOG_WARN("revert lookup iterator failed", K(group_ret));
    }
    //Only cover ret code when DASScanOp release success.
    //In current implement group lookup op revert always return OB_SUCCESS.
    if (OB_SUCCESS == ret) {
      ret = group_ret;
    }
  }
  group_lookup_op_ = NULL;
  iter_.reset();
  result_iter_ = &iter_;
  return ret;
}

int ObDASGroupScanOp::open_op()
{
  int ret = OB_SUCCESS;
  int64_t max_size = scan_rtdef_->eval_ctx_->is_vectorized()
                     ? scan_rtdef_->eval_ctx_->max_batch_size_
                     :1;

  ObMemAttr attr = scan_rtdef_->stmt_allocator_.get_attr();
  iter_.init_group_range(cur_group_idx_, group_size_);
  if (OB_FAIL(iter_.init_row_store(scan_ctdef_->result_output_,
                                   *scan_rtdef_->eval_ctx_,
                                   scan_rtdef_->stmt_allocator_,
                                   max_size,
                                   scan_ctdef_->group_id_expr_,
                                   &this->get_scan_result(),
                                   scan_rtdef_->need_check_output_datum_,
                                   attr))) {
    LOG_WARN("fail to init iter", K(ret));
  } else if (OB_FAIL(ObDASScanOp::open_op())) {
    LOG_WARN("fail to open op", K(ret));
  }
  LOG_DEBUG("group scan open op", K(*this), K(this));
  return ret;
}

int ObDASGroupScanOp::do_local_index_lookup()
{
  int ret = OB_SUCCESS;
  int64_t size = sizeof(ObGroupLookupOp);
  void *buf = op_alloc_.alloc(size);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("lookup op buf allocated failed", K(ret));
  } else {
    ObGroupLookupOp *op = new(buf) ObGroupLookupOp();
    op->set_rowkey_iter(&this->iter_);
    group_lookup_op_ = op;
    result_iter_ = group_lookup_op_;
    OZ(op->init(get_lookup_ctdef(),
             get_lookup_rtdef(),
             scan_ctdef_,
             scan_rtdef_,
             trans_desc_,
             snapshot_));
    if (OB_SUCC(ret)) {
      op->set_tablet_id(related_tablet_ids_.at(0));
      op->set_ls_id(ls_id_);
      op->set_is_group_scan(true);
      OZ(op->init_group_scan_iter(cur_group_idx_,
                                  group_size_,
                                  scan_ctdef_->group_id_expr_));
    }
  }
  return ret;
}

void ObDASGroupScanOp::init_group_range(int64_t cur_group_idx, int64_t group_size)
{
  cur_group_idx_ = cur_group_idx;
  group_size_ = group_size;
  LOG_DEBUG("init group range", K(group_size), K(cur_group_idx));
}

int ObDASGroupScanOp::switch_scan_group()
{
  int ret = OB_SUCCESS;
  ObLocalIndexLookupOp *lookup_op = get_lookup_op();
  // for lookup group scan, switch lookup group scan iter
  if (NULL != lookup_op) {
    ret = lookup_op->switch_lookup_scan_group();
  } else {
    ret = iter_.switch_scan_group();
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    LOG_WARN("switch scan group failed", K(ret), KP(lookup_op), K(iter_));
  }
  LOG_DEBUG("switch scan group", K(iter_), K(ret), KP(lookup_op));
  return ret;
}

int ObDASGroupScanOp::set_scan_group(int64_t group_id)
{
  int ret = OB_SUCCESS;
  ObLocalIndexLookupOp *lookup_op = get_lookup_op();
  // for lookup group scan, switch lookup group scan iter
  if (NULL != lookup_op) {
    ret = lookup_op->set_lookup_scan_group(group_id);
  } else {
    ret = iter_.set_scan_group(group_id);
  }

  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    LOG_WARN("set scan group failed", K(ret), KP(lookup_op), K(iter_));
  }
  return ret;
}

ObNewRowIterator *ObDASGroupScanOp::get_storage_scan_iter()
{
  ObNewRowIterator *iter = NULL;
  if (related_ctdefs_.empty()) {
    iter = result_;
  } else { // has lookup
    if (NULL == group_lookup_op_ || NULL == group_lookup_op_->get_rowkey_iter()) {
      iter = NULL;
    } else {
      iter = static_cast<ObGroupScanIter *>(group_lookup_op_->get_rowkey_iter())->get_iter();
    }
  }
  return iter;
}

int ObDASGroupScanOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
  if (NULL == group_lookup_op_) {
    result_iter_ = result_;
  } else {
    result_iter_ = group_lookup_op_;
    set_is_exec_remote(true);
  }
  if (OB_FAIL(ObDASScanOp::fill_task_result(task_result, has_more, memory_limit))) {
    LOG_WARN("fail to fill task result", K(ret));
  }

  return ret;
}

// init some thing before das_op->get_next_row() in local server for remote das
int ObDASGroupScanOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
  // init result_ by ObDASScanResult
  if (OB_FAIL(ObDASScanOp::decode_task_result(task_result))) {
    LOG_WARN("fail to decode task result", K(ret));
  } else {
    // because for remote das, local server not call open_op, so here we need to init
    // something for group scan op
    //
    // init group_lookup_op_ for lookup
    if (NULL != get_lookup_ctdef() && NULL == group_lookup_op_) {
      if (OB_FAIL(do_local_index_lookup())) {
        LOG_WARN("do local index lookup failed", K(ret));
      }
    }
    // init scan iter_
    int64_t max_size = scan_rtdef_->eval_ctx_->is_vectorized()
                       ? scan_rtdef_->eval_ctx_->max_batch_size_
                       :1;
    if (OB_SUCC(ret) && NULL == iter_.get_group_id_expr()) {
      ObMemAttr attr = scan_rtdef_->stmt_allocator_.get_attr();
      iter_.init_group_range(cur_group_idx_, group_size_);
      if (OB_FAIL(iter_.init_row_store(scan_ctdef_->result_output_,
                                       *scan_rtdef_->eval_ctx_,
                                       scan_rtdef_->stmt_allocator_,
                                       max_size,
                                       scan_ctdef_->group_id_expr_,
                                       &this->get_scan_result(),
                                       scan_rtdef_->need_check_output_datum_,
                                       attr))) {
        LOG_WARN("fail to init iter", K(ret));
      }
    }

    // set ObDASScanResult as the input of ObGroupScanIter
    if (OB_FAIL(ret)) {
    } else if (NULL == group_lookup_op_) {
      iter_.get_iter() = result_;
      result_iter_ = &iter_;
    } else {
      static_cast<ObGroupScanIter *>(group_lookup_op_->get_lookup_iter())->get_iter() = result_;
      result_iter_ = group_lookup_op_->get_lookup_iter();
    }
  }
  return ret;
}
ObGroupLookupOp::~ObGroupLookupOp()
{
}

int ObGroupLookupOp::init_group_range(int64_t cur_group_idx, int64_t group_size)
{
  int ret = OB_SUCCESS;
  ObGroupScanIter *group_iter = static_cast<ObGroupScanIter*>(lookup_iter_);
  if (NULL == group_iter) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    group_iter->init_group_range(cur_group_idx, group_size);
    LOG_DEBUG("set group info",
              "scan_range", scan_param_.key_ranges_,
              K(*group_iter));
  }

  return ret;
}

int ObGroupLookupOp::init_group_scan_iter(int64_t cur_group_idx,
                                          int64_t group_size,
                                          ObExpr *group_id_expr)
{
  int ret = OB_SUCCESS;
  bool is_vectorized = lookup_rtdef_->p_pd_expr_op_->is_vectorized();

  int64_t max_row_store_size = is_vectorized ? lookup_rtdef_->eval_ctx_->max_batch_size_: 1;
  ObMemAttr attr = lookup_rtdef_->stmt_allocator_.get_attr();
  group_iter_.init_group_range(cur_group_idx, group_size);
  OZ(group_iter_.init_row_store(lookup_ctdef_->result_output_,
                                *lookup_rtdef_->eval_ctx_,
                                lookup_rtdef_->stmt_allocator_,
                                max_row_store_size,
                                group_id_expr,
                                &group_iter_.get_result_tmp_iter(),
                                lookup_rtdef_->need_check_output_datum_,
                                attr));

  return ret;
}

bool ObGroupLookupOp::need_next_index_batch() const
{
  bool bret = false;
  if (lookup_group_cnt_ >= index_group_cnt_) {
    bret = !index_end_;
  }
  return bret;
}

ObNewRowIterator *&ObGroupLookupOp::get_lookup_storage_iter()
{
  return static_cast<ObGroupScanIter *>(lookup_iter_)->get_iter();
}

int ObGroupLookupOp::switch_lookup_scan_group()
{
  int ret = OB_SUCCESS;
  state_ = OUTPUT_ROWS;
  ObGroupScanIter *group_iter = NULL;
  group_iter = static_cast<ObGroupScanIter *>(get_lookup_iter());
  if (NULL == group_iter) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(group_iter), K(ret));
  } else {
    ret = group_iter->switch_scan_group();
    ++lookup_group_cnt_;
  }

  return ret;
}

int ObGroupLookupOp::set_lookup_scan_group(int64_t group_id)
{
  int ret = OB_SUCCESS;
  state_ = OUTPUT_ROWS;
  ObGroupScanIter *group_iter = NULL;
  group_iter = static_cast<ObGroupScanIter *>(get_lookup_iter());
  if (NULL == group_iter) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(group_iter), K(ret));
  } else {
    ret = group_iter->set_scan_group(group_id);
    if(-1 == group_id) {
      ++lookup_group_cnt_;
    } else {
      lookup_group_cnt_ = group_id + 1;
    }

    if(lookup_group_cnt_ >= index_group_cnt_ && OB_ITER_END == ret && !index_end_) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObGroupLookupOp::revert_iter()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLocalIndexLookupOp::revert_iter())) {
    LOG_WARN("revert local index lookup iter from group fail.", K(ret));
  }
  group_iter_.reset();
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASGroupScanOp, ObDASScanOp), iter_, cur_group_idx_, group_size_);

}  // namespace sql
}  // namespace oceanbase
