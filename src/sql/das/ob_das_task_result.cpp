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
#include "ob_das_task_result.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
namespace oceanbase
{
using namespace common;
using namespace share;

namespace sql
{

ObDASTCB::ObDASTCB()
  : task_id_(0),
    expire_ts_(0),
    packet_cnt_(0),
    is_reading_(false),
    is_exiting_(false),
    is_vectorized_(false),
    enable_rich_format_(false),
    read_rows_(0),
    max_batch_size_(0),
    stored_row_(NULL),
    stored_row_arr_(NULL),
    datum_store_("DASTaskResMgr"),
    result_iter_(),
    vec_stored_row_(NULL),
    vec_stored_row_arr_(NULL),
    vec_row_store_(),
    vec_result_iter_(),
    tcb_lock_()
{
}

int ObDASTCB::register_reading()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(tcb_lock_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("acquire tcb lock failed", KR(ret), K(task_id_));
  } else if (OB_UNLIKELY(is_exiting_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("das tcb is exiting", KR(ret), K(task_id_));
  } else if (OB_UNLIKELY(is_reading_)) {
    ret = OB_EAGAIN;
    LOG_TRACE("someone else is reading tcb, try again later", KR(ret), K(task_id_));
  } else {
    is_reading_ = true;
  }
  return ret;
}

int ObDASTCB::unregister_reading()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(tcb_lock_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("acquire tcb lock failed", KR(ret), K(task_id_));
  } else if (OB_UNLIKELY(is_exiting_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("das tcb is exiting", KR(ret), K(task_id_));
  } else if (OB_UNLIKELY(!is_reading_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tcb is not reading", KR(ret), K(task_id_));
  } else {
    is_reading_ = false;
  }
  return ret;
}

int ObDASTCB::register_exiting(bool &is_already_exiting)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(tcb_lock_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("acquire tcb lock failed", KR(ret), K(task_id_));
  } else if (OB_UNLIKELY(is_reading_)) {
    ret = OB_EAGAIN;
    LOG_TRACE("someone else is reading tcb, try again later", KR(ret), K(task_id_));
  } else if (OB_UNLIKELY(is_exiting_)) {
    is_already_exiting = true;
  } else {
    is_exiting_ = true;
    is_already_exiting = false;
  }
  return ret;
}

ObDASTaskResultMgr::ObDASTaskResultMgr()
  : tcb_map_(),
    gc_()
{
}

ObDASTaskResultMgr::~ObDASTaskResultMgr()
{
  tcb_map_.~DASTCBMap();
  gc_.~ObDASTaskResultGC();
}

int ObDASTaskResultMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tcb_map_.init())) {
    LOG_WARN("init tcb map failed", KR(ret));
  } else {
    gc_.task_result_mgr_ = this;
  }
  return ret;
}

int ObDASTaskResultMgr::save_task_result(int64_t task_id,
                                         const ExprFixedArray *output_exprs,
                                         ObEvalCtx *eval_ctx,
                                         common::ObNewRowIterator &result,
                                         int64_t read_rows,
                                         const ObDASScanCtDef *scan_ctdef,
                                         const ObDASScanRtDef * scan_rtdef,
                                         ObDASScanOp & scan_op)
{
  int ret = OB_SUCCESS;
  DASTCBInfo tcb_info(task_id);
  ObDASTCB *tcb = NULL;
  if (OB_ISNULL(output_exprs) || OB_ISNULL(eval_ctx) ||
      OB_ISNULL(scan_ctdef) || OB_ISNULL(scan_rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(output_exprs), KP(eval_ctx),
             KP(scan_ctdef), KP(scan_rtdef));
  } else if (OB_FAIL(tcb_map_.contains_key(tcb_info))) {
    if (OB_ENTRY_EXIST == ret) {
      LOG_WARN("das task id already exist", KR(ret), K(task_id));
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("check contains key failed", KR(ret), K(task_id));
    } else {
      ret = OB_SUCCESS;
      int64_t expire_ts = scan_rtdef->timeout_ts_;
      int64_t max_batch_size = scan_ctdef->pd_expr_spec_.max_batch_size_;
      bool is_vectorized = scan_rtdef->p_pd_expr_op_->is_vectorized();
      bool enable_rich_format = scan_rtdef->enable_rich_format();
      if (OB_ISNULL(tcb = op_alloc(ObDASTCB))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc das tcb failed", KR(ret));
      } else {
        tcb->task_id_ = task_id;
        tcb->expire_ts_ = expire_ts;
        tcb->packet_cnt_ = 0;
        tcb->is_reading_ = false;
        tcb->is_exiting_ = false;
        tcb->is_vectorized_ = is_vectorized;
        tcb->enable_rich_format_ = enable_rich_format;
        tcb->read_rows_ = 0;
        tcb->stored_row_ = NULL;
        tcb->stored_row_arr_ = NULL;
        tcb->vec_stored_row_ = NULL;
        tcb->vec_stored_row_arr_ = NULL;
        tcb->max_batch_size_ = max_batch_size;
        bool iter_end = false;
        if (enable_rich_format) {
          OZ (save_task_result_by_vector(read_rows, output_exprs, eval_ctx,
                                         tcb, scan_op, result, scan_rtdef));
        } else {
          ObChunkDatumStore &datum_store = tcb->datum_store_;
          bool added = false;
          if (is_vectorized) {
            void *buf = NULL;
            if (OB_ISNULL(buf = ob_malloc(sizeof(ObChunkDatumStore::StoredRow *) * max_batch_size, "DASTaskResMgr"))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc stored row array failed", KR(ret));
            } else {
              tcb->stored_row_arr_ = static_cast<const ObChunkDatumStore::StoredRow **>(buf);
            }
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(datum_store.init(4 * 1024 * 1024, // 4MB
                                       MTL_ID(),
                                       common::ObCtxIds::DEFAULT_CTX_ID,
                                       "DASTaskResMgr",
                                       true))) {
            LOG_WARN("init datum store failed", KR(ret));
          } else if (OB_FAIL(datum_store.alloc_dir_id())) {
            LOG_WARN("datum store alloc dir id failed", KR(ret));
          }
          // add rows already read but not added to datum_store
          if (OB_FAIL(ret) || 0 == read_rows) {
            // do nothing
          } else if (!is_vectorized) {
            if (OB_FAIL(datum_store.try_add_row(*output_exprs,
                                                eval_ctx,
                                                INT64_MAX,
                                                added))) {
              LOG_WARN("try add row to datum store failed", KR(ret));
            } else if (!added) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("row wasn't added to datum store", KR(ret));
            }
          } else {
            if (OB_FAIL(datum_store.try_add_batch(*output_exprs,
                                                  eval_ctx,
                                                  read_rows,
                                                  INT64_MAX,
                                                  added))) {
              LOG_WARN("try add batch to datum store failed", KR(ret));
            } else if (!added) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("batch wasn't added to datum store", KR(ret));
            }
          }
          while (OB_SUCC(ret)) {
            added = false;
            if (!is_vectorized) {
              scan_rtdef->p_pd_expr_op_->clear_evaluated_flag();
              if (OB_FAIL(result.get_next_row())) {
                if (OB_ITER_END != ret) {
                  LOG_WARN("get next row from result failed", KR(ret));
                }
              } else {
                if (OB_FAIL(datum_store.try_add_row(*output_exprs,
                                                    eval_ctx,
                                                    INT64_MAX,
                                                    added))) {
                  LOG_WARN("try add row to datum store failed", KR(ret));
                } else if (!added) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("row wasn't added to datum store", KR(ret));
                }
              }
            } else {
              scan_rtdef->p_pd_expr_op_->clear_evaluated_flag();
              read_rows = 0;
              if (iter_end) {
                ret = OB_ITER_END;
              } else if (OB_FAIL(result.get_next_rows(read_rows, max_batch_size))) {
                if (OB_ITER_END != ret) {
                  LOG_WARN("get next rows from result failed", KR(ret));
                } else {
                  iter_end = true;
                  ret = OB_SUCCESS;
                }
              }
              if (OB_FAIL(ret) || 0 == read_rows) {
                // do nothing
              } else if (OB_FAIL(datum_store.try_add_batch(*output_exprs,
                                                           eval_ctx,
                                                           read_rows,
                                                           INT64_MAX,
                                                           added))) {
                LOG_WARN("try add batch to datum store failed", KR(ret));
              } else if (!added) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("batch wasn't added to datum store", KR(ret));
              }
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(datum_store.finish_add_row())) {
              LOG_WARN("datum store finish add row failed", KR(ret));
            } else if (OB_FAIL(tcb->result_iter_.init(&tcb->datum_store_))) {
              LOG_WARN("init das tcb result iter failed", KR(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tcb_map_.insert_and_get(tcb_info, tcb))) {
          LOG_WARN("insert das tcb failed", KR(ret));
        } else {
          tcb_map_.revert(tcb);
        }
      }
      if (OB_FAIL(ret) && NULL != tcb) {
        tcb->result_iter_.reset();
        tcb->datum_store_.reset();
        tcb->vec_result_iter_.reset();
        tcb->vec_row_store_.reset();
        if (NULL != tcb->stored_row_arr_) {
          ob_free(tcb->stored_row_arr_);
        }
        if (NULL != tcb->vec_stored_row_arr_) {
          ob_free(tcb->vec_stored_row_arr_);
        }
        tcb->~ObDASTCB();
        op_free(tcb);
        tcb = NULL;
      }
    }
  }
  return ret;
}

int ObDASTaskResultMgr::save_task_result_by_vector(int64_t &read_rows,
                                                   const ExprFixedArray *output_exprs,
                                                   ObEvalCtx *eval_ctx,
                                                   ObDASTCB *tcb,
                                                   ObDASScanOp &scan_op,
                                                   common::ObNewRowIterator &result,
                                                   const ObDASScanRtDef *scan_rtdef)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  bool added = false;
  ObTempRowStore &vec_row_store = tcb->vec_row_store_;
  int64_t max_batch_size = static_cast<const ObDASScanCtDef *>(scan_op.get_ctdef())
                           ->pd_expr_spec_.max_batch_size_;
  if (OB_ISNULL(buf = ob_malloc(sizeof(ObCompactRow *) * max_batch_size, "DASTaskResMgr"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc stored row array failed", KR(ret));
  } else {
    tcb->vec_stored_row_arr_ = static_cast<const ObCompactRow **>(buf);
  }
  ObMemAttr mem_attr(MTL_ID(), "DASTaskResMgr", ObCtxIds::DEFAULT_CTX_ID);
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(vec_row_store.init(*output_exprs,
                                         max_batch_size,
                                         mem_attr,
                                         4 * 1024 * 1024, // 4MB
                                         true, /*enable_dump*/
                                         0, /*row_extra_size*/
                                         NONE_COMPRESSOR,
                                         false/*reorder_fixed_expr*/))) {
    LOG_WARN("init vec row store failed", K(ret));
  } else if (OB_FAIL(vec_row_store.alloc_dir_id())) {
    LOG_WARN("datum store alloc dir id failed", KR(ret));
  }
  // add rows already read but not added to store
  if (OB_FAIL(ret) || 0 == read_rows) {
    // do nothing
  } else if (OB_FAIL(vec_row_store.try_add_batch(*output_exprs,
                                                  eval_ctx,
                                                  read_rows,
                                                  INT64_MAX,
                                                  added))) {
    LOG_WARN("try add batch to datum store failed", KR(ret));
  } else if (!added) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch wasn't added to datum store", KR(ret));
  }
  bool iter_end = false;
  while (OB_SUCC(ret)) {
    added = false;
    scan_rtdef->p_pd_expr_op_->clear_evaluated_flag();
    read_rows = 0;
    if (iter_end) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(result.get_next_rows(read_rows, max_batch_size))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next rows from result failed", KR(ret));
      } else {
        iter_end = true;
        ret = OB_SUCCESS;
      }
    }
    if (OB_FAIL(ret) || 0 == read_rows) {
      // do nothing
    } else if (OB_FAIL(vec_row_store.try_add_batch(*output_exprs,
                                                    eval_ctx,
                                                    read_rows,
                                                    INT64_MAX,
                                                    added))) {
      LOG_WARN("try add batch to datum store failed", KR(ret));
    } else if (!added) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch wasn't added to datum store", KR(ret));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(vec_row_store.finish_add_row())) {
      LOG_WARN("datum store finish add row failed", KR(ret));
    } else if (OB_FAIL(tcb->vec_result_iter_.init(&tcb->vec_row_store_))) {
      LOG_WARN("init das tcb result iter failed", KR(ret));
    }
  }

  return ret;
}

int ObDASTaskResultMgr::erase_task_result(int64_t task_id)
{
  int ret = OB_SUCCESS;
  DASTCBInfo tcb_info(task_id);
  ObDASTCB *tcb = NULL;
  if (OB_FAIL(tcb_map_.get(tcb_info, tcb))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("das tcb was already removed", K(task_id));
    } else {
      LOG_WARN("get das tcb failed", KR(ret), K(task_id));
    }
  } else if (OB_ISNULL(tcb)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das tcb is null", KR(ret), K(task_id));
  } else if (OB_UNLIKELY(tcb->is_exiting_)) {
    // tcb is already erased, remove from map failed last time
  } else if (OB_UNLIKELY(tcb->is_reading_)) {
    ret = OB_EAGAIN;
    LOG_TRACE("someone else is reading tcb, try again later", KR(ret), K(task_id));
  } else {
    bool is_already_exiting = false;
    if (OB_FAIL(tcb->register_exiting(is_already_exiting))) {
      LOG_WARN("register exiting failed", KR(ret), K(task_id));
    } else {
      if (!is_already_exiting) {
        // we have registered exiting flag for the first time, need to reset tcb
        tcb->result_iter_.reset();
        tcb->datum_store_.reset();
        tcb->vec_result_iter_.reset();
        tcb->vec_row_store_.reset();
        if (NULL != tcb->stored_row_arr_) {
          ob_free(tcb->stored_row_arr_);
          tcb->stored_row_arr_ = NULL;
        }
        if (NULL != tcb->vec_stored_row_arr_) {
          ob_free(tcb->vec_stored_row_arr_);
          tcb->vec_stored_row_arr_ = NULL;
        }
        if (OB_FAIL(tcb_map_.del(tcb_info))) {
          LOG_WARN("delete tcb failed", KR(ret), K(task_id));
        }
      } else {
        // tcb was already exiting, no need to reset anything
      }
    }
  }
  if (NULL != tcb) {
    tcb_map_.revert(tcb);
  }
  return ret;
}

int ObDASTaskResultMgr::iterator_task_result(ObDASDataFetchRes &res)
{
  int ret = OB_SUCCESS;
  bool has_more = false;
  int64_t task_id = res.get_task_id();
  DASTCBInfo tcb_info(task_id);
  ObDASTCB *tcb = NULL;
  if (OB_FAIL(tcb_map_.get(tcb_info, tcb))) {
    LOG_WARN("get das tcb failed", KR(ret), K(task_id));
  } else if (OB_ISNULL(tcb)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das tcb is null", KR(ret), K(task_id));
  } else if (tcb->is_exiting_) {
    // The background GC thread is already cleaning up this tcb,
    // and it will be removed from the hash map shortly.
    // This happens when the task result has expired meaning the
    // SQL has timed out.
    ret = OB_TIMEOUT;
    LOG_WARN("das tcb is exiting", KR(ret), K(task_id));
  } else if (tcb->is_reading_) {
    ret = OB_EAGAIN;
    LOG_TRACE("someone else is reading tcb, try again later", KR(ret), K(task_id));
  } else {
    if (OB_FAIL(tcb->register_reading())) {
      LOG_WARN("unregister reading tcb failed", KR(ret), K(task_id));
    } else {
      if (tcb->enable_rich_format_) {
        OZ (fetch_result_by_vector(tcb, res, has_more));
      } else {
        OZ (fetch_result_by_normal(tcb, res, has_more));
      }
      if (OB_SUCC(ret)) {
        tcb->packet_cnt_++;
      }
      int save_ret = ret;
      if (OB_FAIL(tcb->unregister_reading())) {
        LOG_WARN("unregister tcb reading failed", KR(ret), K(task_id));
      } else if (!has_more && OB_FAIL(erase_task_result(task_id))) {
        LOG_WARN("erase task result failed", KR(ret), K(task_id));
      }
      ret = save_ret;
    }
  }
  if (NULL != tcb) {
    tcb_map_.revert(tcb);
  }
  if (OB_SUCC(ret)) {
    res.set_has_more(has_more);
  }
  return ret;
}

int ObDASTaskResultMgr::fetch_result_by_normal(ObDASTCB *tcb,
                                               ObDASDataFetchRes &res,
                                               bool &has_more)
{
  int ret = OB_SUCCESS;
  int64_t task_id = res.get_task_id();
  ObChunkDatumStore &datum_store = res.get_datum_store();
  if (!datum_store.is_inited()
      && OB_FAIL(datum_store.init(INT64_MAX,
                                  MTL_ID(),
                                  common::ObCtxIds::DEFAULT_CTX_ID,
                                  "DASTaskResMgr",
                                  false))) {
    LOG_WARN("init datum store failed", KR(ret));
  } else {
    bool added = false;
    ObChunkDatumStore::Iterator &iter = tcb->result_iter_;
    int64_t &read_rows = tcb->read_rows_;
    while (OB_SUCC(ret) && !has_more) {
      int64_t memory_limit = das::OB_DAS_MAX_PACKET_SIZE;
      added = false;
      if (datum_store.get_row_cnt() == 0) {
        // make sure RPC response contains at least one row or one batch
        memory_limit = INT64_MAX;
      }
      if (!tcb->is_vectorized_) {
        const ObChunkDatumStore::StoredRow *&sr = tcb->stored_row_;
        if (NULL == sr) {
          if (OB_FAIL(iter.get_next_row(sr))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next row from iter failed", KR(ret), K(task_id));
            }
            sr = NULL;
          } else if (OB_ISNULL(sr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("stored row is null", KR(ret), K(task_id), KP(sr));
          }
        } else {
          // sr was read from last RPC, but wasn't included in the RPC response
          // due to response size limit.
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(datum_store.try_add_row(*sr, memory_limit, added))) {
          LOG_WARN("try add row to datum store failed", KR(ret), K(task_id));
        } else if (!added) {
          has_more = true;
          // sr is saved at tcb->stored_row_, and will be included in next RPC.
        } else {
          sr = NULL;
        }
      } else {
        const ObChunkDatumStore::StoredRow **srs = tcb->stored_row_arr_;
        if (0 == read_rows) {
          if (OB_FAIL(iter.get_next_batch(srs, tcb->max_batch_size_, read_rows))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next batch from iter failed", KR(ret), K(task_id));
            }
            read_rows = 0;
          } else if (0 == read_rows) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no rows were read", KR(ret), K(task_id), K(read_rows));
          }
        } else {
          // srs were read from last RPC, but weren't included in the RPC response
          // due to response size limit.
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(datum_store.try_add_batch(srs,
                                                     read_rows,
                                                     memory_limit,
                                                     added))) {
          LOG_WARN("try add batch to datum store failed", KR(ret), K(task_id));
        } else if (!added) {
          has_more = true;
        } else {
          read_rows = 0;
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  LOG_DEBUG("fecth result by normal", K(has_more), K(res), K(ret));
  return ret;
}

int ObDASTaskResultMgr::fetch_result_by_vector(ObDASTCB *tcb,
                                               ObDASDataFetchRes &res,
                                               bool &has_more)
{
  int ret = OB_SUCCESS;
  int64_t task_id = res.get_task_id();
  ObTempRowStore &vec_row_store = res.get_vec_row_store();
  ObMemAttr mem_attr(MTL_ID(), "DASTaskResMgr", ObCtxIds::DEFAULT_CTX_ID);
  if (!vec_row_store.is_inited()
      && OB_FAIL(vec_row_store.init(tcb->vec_row_store_.get_row_meta(),
                                    tcb->vec_row_store_.get_max_batch_size(),
                                    mem_attr,
                                    INT64_MAX, /* mem_limit */
                                    false, /* enable_dump */
                                    NONE_COMPRESSOR /* compressor_type */))) {
    LOG_WARN("init datum store failed", KR(ret));
  } else {
    bool added = false;
    ObTempRowStore::Iterator &iter = tcb->vec_result_iter_;
    int64_t &read_rows = tcb->read_rows_;
    while (OB_SUCC(ret) && !has_more) {
      int64_t memory_limit = das::OB_DAS_MAX_PACKET_SIZE;
      added = false;
      if (vec_row_store.get_row_cnt() == 0) {
        // make sure RPC response contains at least one row or one batch
        memory_limit = INT64_MAX;
      }
      const ObCompactRow  **srs = tcb->vec_stored_row_arr_;
      if (0 == read_rows) {
        if (OB_FAIL(iter.get_next_batch(tcb->max_batch_size_, read_rows, srs))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next batch from iter failed", KR(ret), K(task_id));
          }
          read_rows = 0;
        } else if (0 == read_rows) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("no rows were read", KR(ret), K(task_id), K(read_rows));
        }
      } else {
        // srs were read from last RPC, but weren't included in the RPC response
        // due to response size limit.
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(vec_row_store.try_add_batch(srs,
                                                     read_rows,
                                                     memory_limit,
                                                     added))) {
        LOG_WARN("try add batch to datum store failed", KR(ret), K(task_id));
      } else if (!added) {
        has_more = true;
      } else {
        read_rows = 0;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  LOG_DEBUG("fecth result by vector", K(has_more), K(res), K(ret));

  return ret;
}

int ObDASTaskResultMgr::remove_expired_results()
{
  int ret = OB_SUCCESS;
  gc_.set_current_time();
  if (OB_FAIL(tcb_map_.remove_if(gc_))) {
    LOG_WARN("remove expired results from tcb map failed", KR(ret));
  }
  return ret;
}

bool ObDASTaskResultGC::operator() (const DASTCBInfo &tcb_info, ObDASTCB *tcb)
{
  int ret = OB_SUCCESS;
  bool remove = false;
  if (OB_ISNULL(tcb)) {
    ret = OB_ERR_UNEXPECTED;
    remove = false;
    LOG_WARN("tcb is null", KR(ret), K(tcb_info), KP(tcb));
  } else if (tcb->expire_ts_ < cur_time_) {
    if (OB_FAIL(task_result_mgr_->erase_task_result(tcb_info.get_value()))) {
      remove = false;
      LOG_WARN("erase task result failed", KR(ret), K(tcb_info));
    } else {
      remove = true;
    }
  }
  return remove;
}

ObDASTaskResultGCRunner& ObDASTaskResultGCRunner::get_instance()
{
  static ObDASTaskResultGCRunner gc_runner;
  return gc_runner;
}

int ObDASTaskResultGCRunner::schedule_timer_task()
{
  int ret = OB_SUCCESS;
  ObDASTaskResultGCRunner& gc_runner = get_instance();
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, gc_runner,
                          ObDASTaskResultGCRunner::REFRESH_INTERVAL, true))) {
    LOG_WARN("schedule das task result gc runner failed", K(ret));
  }
  return ret;
}

void ObDASTaskResultGCRunner::runTimerTask()
{
  int ret = OB_SUCCESS;
  omt::TenantIdList all_tenants;
  GCTX.omt_->get_tenant_ids(all_tenants);
  for (int i = 0; OB_SUCC(ret) && i < all_tenants.size(); ++i) {
    uint64_t tenant_id = all_tenants[i];
    if (is_virtual_tenant_id(tenant_id)) {
      // skip virtual tenant
    } else {
      MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
      if (OB_FAIL(guard.switch_to(tenant_id))) {
        LOG_WARN("tenant switch failed during das task result gc", KR(ret), K(tenant_id));
      } else {
        ObDataAccessService * das = NULL;
        if (OB_ISNULL(das = MTL(ObDataAccessService *))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("das is null", K(ret), KP(das));
        } else if (OB_FAIL(das->get_task_res_mgr().remove_expired_results())) {
          LOG_WARN("remove expired results failed", KR(ret), K(tenant_id));
        }
      }
    }
    // ignore error code
    ret = OB_SUCCESS;
  }
}
} // namespace sql
} // namespace oceanbase
