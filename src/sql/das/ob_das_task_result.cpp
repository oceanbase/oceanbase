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
#include "sql/das/ob_data_access_service.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_util.h"
#include "share/detect/ob_detect_manager_utils.h"
namespace oceanbase
{
using namespace common;
using namespace share;

namespace sql
{

ObDASTCB::ObDASTCB()
  : is_inited_(false),
    task_id_(0),
    table_id_(0),
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
    datum_store_(NULL),
    result_iter_(),
    vec_stored_row_(NULL),
    vec_stored_row_arr_(NULL),
    vec_row_store_(),
    vec_result_iter_(),
    io_read_bytes_(0),
    ssstore_read_bytes_(0),
    ssstore_read_row_cnt_(0),
    memstore_read_row_cnt_(0),
    mem_profile_key_(),
    tcb_lock_()
{
}

int ObDASTCB::init(int64_t task_id, const ObDASScanRtDef *scan_rtdef,
                   const ObDASScanCtDef *scan_ctdef, const ExprFixedArray *output_exprs,
                   ObDASTCBMemProfileKey &mem_profile_key, ObDASMemProfileInfo *mem_profile_info,
                   const ObDASTCBInterruptInfo &interrupt_info)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = common::OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(*this));
  } else if (OB_ISNULL(scan_rtdef) || OB_ISNULL(mem_profile_info) ||
             OB_ISNULL(scan_ctdef) || OB_ISNULL(scan_rtdef->p_pd_expr_op_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer", K(ret));
  } else if (OB_UNLIKELY(!mem_profile_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem profile key is invalid", K(ret), K(mem_profile_key));
  } else {
    is_inited_ = true;
    task_id_ = task_id;
    table_id_ = scan_ctdef->ref_table_id_;
    expire_ts_ = scan_rtdef->timeout_ts_;
    max_batch_size_ = scan_ctdef->pd_expr_spec_.max_batch_size_;
    packet_cnt_ = 0;
    is_reading_ = false;
    is_exiting_ = false;
    is_vectorized_ = scan_rtdef->p_pd_expr_op_->is_vectorized();
    read_rows_ = 0;
    stored_row_ = NULL;
    stored_row_arr_ = NULL;
    vec_stored_row_ = NULL;
    vec_stored_row_arr_ = NULL;
    enable_rich_format_ = scan_rtdef->enable_rich_format();
    mem_profile_key_.init(mem_profile_key);
    interrupt_info_ = interrupt_info;

    void *store_buf = NULL;
    void *store_arr_buf = NULL;
    common::ObFIFOAllocator &allocator = mem_profile_info->allocator_;
    ObSqlMemMgrProcessor &sql_mem_processor = mem_profile_info->sql_mem_processor_;

    if (enable_rich_format_) {
      ObMemAttr mem_attr(MTL_ID(), "DASTaskResMgr", common::ObCtxIds::WORK_AREA);
      if (OB_ISNULL(store_arr_buf = allocator.alloc(sizeof(ObCompactRow *) * max_batch_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc stored row array failed", KR(ret));
      } else if (OB_ISNULL(vec_stored_row_arr_ = static_cast<const ObCompactRow **>(store_arr_buf))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("construct stored row array failed", K(ret));
      } else if (OB_ISNULL(store_buf = allocator.alloc(sizeof(ObTempRowStore)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc tcb vec store failed", K(ret));
      } else if (OB_ISNULL(vec_row_store_ = new(store_buf) ObTempRowStore())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("construct vec stored row array failed", K(ret));
      } else if (OB_FAIL(vec_row_store_->init(*output_exprs,
                                              max_batch_size_,
                                              mem_attr,
                                              sql_mem_processor.is_auto_mgr() ? INT64_MAX : 4 * 1024 * 1024,
                                              true, /*enable_dump*/
                                              0, /*row_extra_size*/
                                              NONE_COMPRESSOR,
                                              false/*reorder_fixed_expr*/))) {
          LOG_WARN("init vec row store failed", K(ret));
      } else {
        vec_row_store_->set_dir_id(sql_mem_processor.get_dir_id());
        vec_row_store_->set_allocator(allocator);
        vec_row_store_->set_callback(mem_profile_info);
      }
    } else {
      if (OB_ISNULL(store_buf = allocator.alloc(sizeof(ObChunkDatumStore)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc tcb datum store failed", K(ret));
      } else if (OB_ISNULL(datum_store_ = new(store_buf) ObChunkDatumStore("DASTaskResMgr"))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("construct datum store failed", K(ret));
      } else if (OB_FAIL(datum_store_->init(sql_mem_processor.is_auto_mgr() ? INT64_MAX : 4 * 1024 * 1024,
                                            MTL_ID(),
                                            common::ObCtxIds::WORK_AREA,
                                            "DASTaskResMgr",
                                            true))) {
        LOG_WARN("init datum store failed", KR(ret));
      } else if (FALSE_IT(datum_store_->set_dir_id(sql_mem_processor.get_dir_id()))) {
        // replaces alloc_dir_id in the datum store.
      } else if (FALSE_IT(datum_store_->set_allocator(allocator))) {
        // used to alloc memory within the store
      } else if (FALSE_IT(datum_store_->set_callback(mem_profile_info))) {
        // memory used is reflected in automatic memory management
      } else if (is_vectorized_) {
        if (OB_ISNULL(store_arr_buf = allocator.alloc(sizeof(ObChunkDatumStore::StoredRow *) * max_batch_size_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc stored row array failed", KR(ret));
        } else if (OB_ISNULL(stored_row_arr_ = static_cast<const ObChunkDatumStore::StoredRow **>(store_arr_buf))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("construct stored row array failed", K(ret));
        }
      }
    }

    LOG_DEBUG("save_task_result: ", K(task_id_),
                                    K(enable_rich_format_),
                                    K(scan_ctdef->ref_table_id_),
                                    K(scan_rtdef->scan_op_id_),
                                    K(scan_rtdef->scan_rows_size_),
                                    KPC(mem_profile_info));
  }
  return ret;
}

void ObDASTCB::destory(ObDASMemProfileInfo *mem_profile_info)
{
  if (is_inited_) {
    if (OB_NOT_NULL(mem_profile_info)) {
      if (OB_NOT_NULL(stored_row_arr_)) {
        mem_profile_info->allocator_.free(stored_row_arr_);
        stored_row_arr_ = NULL;
      }

      if (OB_NOT_NULL(datum_store_)) {
        mem_profile_info->update_row_count(-datum_store_->get_row_cnt_in_memory());
        result_iter_.reset();
        datum_store_->reset();
        datum_store_->~ObChunkDatumStore();
        mem_profile_info->allocator_.free(datum_store_);
        datum_store_ = NULL;
      }

      if (OB_NOT_NULL(vec_stored_row_)) {
        // no use now
      }

      if (OB_NOT_NULL(vec_row_store_)) {
        mem_profile_info->update_row_count(-vec_row_store_->get_row_cnt_in_memory());
        vec_result_iter_.reset();
        vec_row_store_->reset();
        vec_row_store_->~ObTempRowStore();
        mem_profile_info->allocator_.free(vec_row_store_);
        vec_row_store_ = NULL;
      }

      if (OB_NOT_NULL(vec_stored_row_arr_)) {
        mem_profile_info->allocator_.free(vec_stored_row_arr_);
        vec_stored_row_arr_ = NULL;
      }
    }
  }
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
  destory();
}

int ObDASTaskResultMgr::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t cpu_quota_concurrency = tenant_config->cpu_quota_concurrency;
  ObMemAttr mem_profile_hash_buck_attr(tenant_id, "DASMemHashBuck");

  if (OB_FAIL(tcb_map_.init())) {
    LOG_WARN("init tcb map failed", KR(ret));
  } else if (OB_FAIL(mem_profile_map_.create(static_cast<int64_t>(MTL_CPU_COUNT() * cpu_quota_concurrency * 2),
                                             mem_profile_hash_buck_attr, mem_profile_hash_buck_attr))) {
    LOG_WARN("create mem profile hash table failed", K(ret));
  } else {
    gc_.task_result_mgr_ = this;
  }
  return ret;
}

void ObDASTaskResultMgr::destory()
{
  tcb_map_.~DASTCBMap();
  gc_.~ObDASTaskResultGC();

  lib::ObMutexGuard guard(mem_map_mutex_);
  if (!mem_profile_map_.empty()) {
    bool it_end = false;
    int64_t mem_profile_map_size = mem_profile_map_.size();
    MemProfileMap::bucket_iterator bucket_it = mem_profile_map_.bucket_begin();
    while (bucket_it != mem_profile_map_.bucket_end()) {
      it_end = false;
      while (!it_end) {
        MemProfileMap::hashtable::bucket_lock_cond blc(*bucket_it);
        MemProfileMap::hashtable::readlocker locker(blc.lock());
        MemProfileMap::hashtable::hashbucket::const_iterator node_it = bucket_it->node_begin();
        if (node_it == bucket_it->node_end()) {
          it_end = true;
        } else {
          destroy_mem_profile(node_it->first);
        }
      }
      ++ bucket_it;
    }
    LOG_TRACE("clear mem profile map", K(MTL_ID()), K(mem_profile_map_size));
  }
  mem_profile_map_.destroy();
}

int ObDASTaskResultMgr:: check_mem_profile_key(ObDASTCBMemProfileKey &key) {
  int ret = OB_SUCCESS;
  // for regular table scan op, the key is guaranteed to be valid at inner open
  // for rpc send by old versions of observer, ObForeignKeyChecker, ObConflictChecker, etc.
  // the key is invalid, so let them use the default profile, which is shared by the current thread's task
  if (!key.is_valid()) {
    int64_t thread_id = GETTID();
    key.init(INT64_MAX, thread_id, INT64_MAX);
    LOG_DEBUG("use a defalut key", K(key));
  }
  return ret;
}

int ObDASTaskResultMgr::save_task_result(int64_t task_id,
                                         const ObDASTCBInterruptInfo &interrupt_info,
                                         const ExprFixedArray *output_exprs,
                                         ObEvalCtx *eval_ctx,
                                         common::ObNewRowIterator &result,
                                         int64_t read_rows,
                                         const ObDASScanCtDef *scan_ctdef,
                                         ObDASScanRtDef *scan_rtdef,
                                         ObDASScanOp &scan_op)
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
    ObDASMemProfileInfo *mem_profile_info = NULL;
    ObDASTCBMemProfileKey &mem_profile_key = scan_rtdef->das_tasks_key_;

    if (OB_ENTRY_EXIST == ret) {
      LOG_WARN("das task id already exist", KR(ret), K(task_id));
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("check contains key failed", KR(ret), K(task_id));
    } else if (FALSE_IT(ret = OB_SUCCESS)) {
    } else if (OB_FAIL(check_mem_profile_key(mem_profile_key))) {
      LOG_WARN("check mem profile key failed", K(ret), K(mem_profile_key));
    } else if (OB_FAIL(get_mem_profile(mem_profile_key, mem_profile_info, &eval_ctx->exec_ctx_, scan_rtdef))) {
      LOG_WARN("check contains key failed", KR(ret), K(task_id), K(mem_profile_key));
    } else if (OB_ISNULL(mem_profile_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem profile info is null", KR(ret), K(task_id), K(mem_profile_key));
    } else {
      bool enable_rich_format = scan_rtdef->enable_rich_format();

      if (OB_ISNULL(tcb = op_alloc(ObDASTCB))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc das tcb failed", KR(ret));

        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(dec_mem_profile_ref_count(mem_profile_key, mem_profile_info))) {
          LOG_WARN("dec mem profile info ref count failed", K(tmp_ret), K(mem_profile_key));
        }
      } else if (OB_FAIL(tcb->init(task_id, scan_rtdef, scan_ctdef, output_exprs, mem_profile_key,
                                   mem_profile_info, interrupt_info))) {
        LOG_WARN("tcb init failed", KR(ret));
      } else if (enable_rich_format) {
        OZ(save_task_result_by_vector(read_rows, output_exprs, eval_ctx,
                                          tcb, result, scan_rtdef, mem_profile_info));
      } else {
        OZ(save_task_result_by_normal(read_rows, output_exprs, eval_ctx,
                                          tcb, result, scan_rtdef, mem_profile_info));
      }

      if (OB_SUCC(ret)) {
        if (OB_NOT_NULL(scan_rtdef->tsc_monitor_info_)) {
          tcb->io_read_bytes_ += *scan_rtdef->tsc_monitor_info_->io_read_bytes_;
          tcb->ssstore_read_bytes_ += *scan_rtdef->tsc_monitor_info_->ssstore_read_bytes_;
          tcb->ssstore_read_row_cnt_ += *scan_rtdef->tsc_monitor_info_->ssstore_read_row_cnt_;
          tcb->memstore_read_row_cnt_ += *scan_rtdef->tsc_monitor_info_->memstore_read_row_cnt_;
          scan_rtdef->tsc_monitor_info_->reset_stat();
        }

        LOG_DEBUG("save_task_result: ", KPC(tcb), KPC(mem_profile_info), K(mem_profile_info->profile_.get_mem_used()),
                                        K(mem_profile_info->sql_mem_processor_.get_sql_mem_mgr()->get_workarea_hold_size()),
                                        K(mem_profile_info->sql_mem_processor_.get_sql_mem_mgr()->get_workarea_count()),
                                        K(mem_profile_info->sql_mem_processor_.get_sql_mem_mgr()->get_total_mem_used()));

        /**
          This tcb has not yet been inserted into the tcb map, and is not considered to exist elsewhere,
          when interrupts start at this point, there may be concurrency issues.
          There are two operations in detect callback, one is to set the interrupt state, and the other is to read the tcb map,
          we need to make sure that it is not interrupted before saving the tcb, and if it is interrupted,
          need to make sure that the resources are cleaned up.
        */
        if (OB_FAIL(check_interrupt())) {
          LOG_WARN("interrupted before inserting result map", K(ret));
        } else if (OB_FAIL(tcb_map_.insert_and_get(tcb_info, tcb))) {
          LOG_WARN("insert das tcb failed", K(ret));
        } else if (FALSE_IT(tcb_map_.revert(tcb))) {
        } else if (OB_FAIL(check_interrupt())) {
          LOG_WARN("interrupted after inserting result map", K(ret));

          // the tcb is already in the map, concurrency needs to be considered
          int tmp_ret =  OB_SUCCESS;
          if (OB_TMP_FAIL(erase_task_result(task_id, false))) {
            LOG_WARN("erase task result failed", KR(tmp_ret), K(task_id));
          }
          tcb = NULL;
        }
      }

      if (OB_FAIL(ret) && NULL != tcb) {
        LOG_TRACE("save_task_result: ", KPC(tcb), K(mem_profile_map_.size()), K(tcb_map_.size()));

        tcb->destory(mem_profile_info);
        tcb->~ObDASTCB();
        op_free(tcb);
        tcb = NULL;

        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(dec_mem_profile_ref_count(mem_profile_key, mem_profile_info))) {
          LOG_WARN("dec mem profile info ref count failed", K(tmp_ret), K(mem_profile_key));
        }
      }
    }
  }
  return ret;
}

int ObDASTaskResultMgr::save_task_result_by_normal(int64_t &read_rows,
                                                   const ExprFixedArray *output_exprs,
                                                   ObEvalCtx *eval_ctx,
                                                   ObDASTCB *tcb,
                                                   common::ObNewRowIterator &result,
                                                   const ObDASScanRtDef *scan_rtdef,
                                                   ObDASMemProfileInfo *mem_profile_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tcb) || OB_ISNULL(output_exprs) ||
      OB_ISNULL(eval_ctx) || OB_ISNULL(scan_rtdef) || OB_ISNULL(mem_profile_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer", KR(ret), KP(output_exprs), KP(eval_ctx), KP(scan_rtdef), KP(tcb));
  } else if (OB_ISNULL(tcb->datum_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tcb's datum store is null", KR(ret));
  } else {
    ObChunkDatumStore &datum_store = *tcb->datum_store_;
    ObSqlMemMgrProcessor &sql_mem_processor = mem_profile_info->sql_mem_processor_;
    bool iter_end = false;
    bool added = false;
    int64_t loop_times = 0;
    int64_t mem_limit = INT64_MAX;
    bool is_vectorized = scan_rtdef->p_pd_expr_op_->is_vectorized();
    int64_t max_batch_size = tcb->max_batch_size_;
    int64_t row_count_in_memory_before = datum_store.get_row_cnt_in_memory();
    int64_t simulate_scan_hang_on = - EVENT_CALL(EventTable::EN_DAS_SIMULATE_DAS_TASK_EXEC_HANG_ON);

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

    if (OB_SUCC(ret) && OB_FAIL(process_dump(tcb, mem_profile_info))) {
      LOG_WARN("process dump failed", K(ret), K(tcb));
    }
    mem_profile_info->update_row_count(datum_store.get_row_cnt_in_memory() - row_count_in_memory_before);

    while (OB_SUCC(ret)) {
      // hard code to check interrupt
      if ((loop_times ++ & (DAS_TCB_CHECK_INTERRUPT_INTERVAL - 1)) == 0 && OB_FAIL(check_interrupt())) {
        LOG_WARN("saving result is interrupted",  K(tcb->task_id_), KR(ret));
      } else if (OB_UNLIKELY(simulate_scan_hang_on > 0 && tcb->table_id_ > 500000)) {
        ob_usleep(simulate_scan_hang_on);
        LOG_WARN("simulate scan hang on", K(simulate_scan_hang_on), K(loop_times));
      } else {
        added = false;
        row_count_in_memory_before = datum_store.get_row_cnt_in_memory();
        scan_rtdef->p_pd_expr_op_->clear_evaluated_flag();
        if (sql_mem_processor.is_auto_mgr()) {
          mem_limit = sql_mem_processor.get_mem_bound();
          datum_store.set_mem_limit(INT64_MAX);
        } else {
          mem_limit = INT64_MAX;
          datum_store.set_mem_limit(4 * 1024 * 1024);
        }
        if (!is_vectorized) {
          if (OB_FAIL(result.get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next row from result failed", KR(ret));
            }
          } else {
            if (OB_FAIL(datum_store.try_add_row(*output_exprs,
                                                eval_ctx,
                                                mem_limit,
                                                added))) {
              LOG_WARN("try add row to datum store failed", KR(ret));
            } else if (!added) {
              if (OB_FAIL(process_dump(tcb, mem_profile_info))) {
                LOG_WARN("process dump failed", K(ret), K(tcb));
              } else if (OB_FAIL(datum_store.try_add_row(*output_exprs,
                                                         eval_ctx,
                                                         INT64_MAX,
                                                         added))) {
                LOG_WARN("try add row to datum store failed", KR(ret));
              } else if (!added) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("row wasn't added to datum store", KR(ret));
              }
            }
          }
        } else {
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
                                                       mem_limit,
                                                       added))) {
            LOG_WARN("try add batch to datum store failed", KR(ret));
          } else if (!added) {
            if (OB_FAIL(process_dump(tcb, mem_profile_info))) {
              LOG_WARN("process dump failed", K(ret), K(tcb));
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
        mem_profile_info->update_row_count(datum_store.get_row_cnt_in_memory() - row_count_in_memory_before);
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      row_count_in_memory_before = datum_store.get_row_cnt_in_memory();
      if (OB_FAIL(process_dump(tcb, mem_profile_info))) {
        LOG_WARN("process dump failed");
      } else if (!sql_mem_processor.is_auto_mgr() && OB_FAIL(datum_store.finish_add_row())) {
        LOG_WARN("datum store finish add row failed", KR(ret));
      } else if (OB_FAIL(tcb->result_iter_.init(&datum_store))) {
        LOG_WARN("init das tcb result iter failed", KR(ret));
      }
      mem_profile_info->update_row_count(datum_store.get_row_cnt_in_memory() - row_count_in_memory_before);
    }
  }
  return ret;
}

int ObDASTaskResultMgr::save_task_result_by_vector(int64_t &read_rows,
                                                   const ExprFixedArray *output_exprs,
                                                   ObEvalCtx *eval_ctx,
                                                   ObDASTCB *tcb,
                                                   common::ObNewRowIterator &result,
                                                   const ObDASScanRtDef *scan_rtdef,
                                                   ObDASMemProfileInfo *mem_profile_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tcb) || OB_ISNULL(output_exprs) ||
      OB_ISNULL(eval_ctx) || OB_ISNULL(scan_rtdef) || OB_ISNULL(mem_profile_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer", KR(ret), KP(output_exprs), KP(eval_ctx), KP(scan_rtdef), KP(tcb));
  } else if (OB_ISNULL(tcb->vec_row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tcb's vec store is null", KR(ret));
  } else {
    ObTempRowStore &vec_row_store = *tcb->vec_row_store_;
    ObSqlMemMgrProcessor &sql_mem_processor = mem_profile_info->sql_mem_processor_;
    bool added = false;
    bool iter_end = false;
    int64_t loop_times = 0;
    int64_t mem_limit = INT64_MAX;
    int64_t max_batch_size = tcb->max_batch_size_;
    int64_t row_count_in_memory_before = vec_row_store.get_row_cnt_in_memory();
    int64_t simulate_scan_hang_on = - EVENT_CALL(EventTable::EN_DAS_SIMULATE_DAS_TASK_EXEC_HANG_ON);

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

    if (OB_SUCC(ret) && OB_FAIL(process_dump(tcb, mem_profile_info))) {
      LOG_WARN("process dump failed", K(ret), K(tcb));
    }
    mem_profile_info->update_row_count(vec_row_store.get_row_cnt_in_memory() - row_count_in_memory_before);

    while (OB_SUCC(ret)) {
      // hard code to check interrupt
      if ((loop_times ++ & (DAS_TCB_CHECK_INTERRUPT_INTERVAL - 1)) == 0 && OB_FAIL(check_interrupt())) {
        LOG_WARN("saving result is interrupted",  K(tcb->task_id_), KR(ret), K(loop_times));
      } else if (OB_UNLIKELY(simulate_scan_hang_on > 0 && tcb->table_id_ > 500000)) {
        ob_usleep(simulate_scan_hang_on);
        LOG_WARN("vector simulate scan hang on", K(simulate_scan_hang_on), K(loop_times));
      } else {
        added = false;
        read_rows = 0;
        row_count_in_memory_before = vec_row_store.get_row_cnt_in_memory();
        scan_rtdef->p_pd_expr_op_->clear_evaluated_flag();
        if (sql_mem_processor.is_auto_mgr()) {
          mem_limit = sql_mem_processor.get_mem_bound();
          vec_row_store.set_mem_limit(INT64_MAX);
        } else {
          mem_limit = INT64_MAX;
          vec_row_store.set_mem_limit(4 * 1024 * 1024);
        }

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
                                                       mem_limit,
                                                       added))) {
          LOG_WARN("try add batch to datum store failed", KR(ret));
        } else if (!added) {
          if (OB_FAIL(process_dump(tcb, mem_profile_info))) {
            LOG_WARN("process dump failed", K(ret), K(tcb));
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
        mem_profile_info->update_row_count(vec_row_store.get_row_cnt_in_memory() - row_count_in_memory_before);
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      row_count_in_memory_before = vec_row_store.get_row_cnt_in_memory();
      if (OB_FAIL(process_dump(tcb, mem_profile_info))) {
        LOG_WARN("process dump failed");
      } else if (!sql_mem_processor.is_auto_mgr() && OB_FAIL(vec_row_store.finish_add_row())) {
        LOG_WARN("datum store finish add row failed", KR(ret));
      } else if (OB_FAIL(tcb->vec_result_iter_.init(&vec_row_store))) {
        LOG_WARN("init das tcb result iter failed", KR(ret));
      }
      mem_profile_info->update_row_count(vec_row_store.get_row_cnt_in_memory() - row_count_in_memory_before);
    }
  }
  return ret;
}

int ObDASTaskResultMgr::erase_task_result(int64_t task_id, bool need_unreg_dm)
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
        if (need_unreg_dm) {
          ObDetectManagerUtils::das_task_unregister_check_item_from_dm(tcb->interrupt_info_.detectable_id_, tcb->interrupt_info_.check_node_sequence_id_);
        }
        ObDASTCBMemProfileKey &mem_profile_key = tcb->mem_profile_key_;
        ObDASMemProfileInfo *mem_profile_info = NULL;
        if (OB_UNLIKELY(!mem_profile_key.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mem profile key is invalid when destory tcb, it should not happen", K(ret), K(mem_profile_key));
        } else if (OB_FAIL(mem_profile_map_.get_refactored(mem_profile_key,
                                                           mem_profile_info))) {
          LOG_WARN("get mem_profile failed", K(ret), K(mem_profile_key));
        } else if (OB_ISNULL(mem_profile_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mem profile is null when destory tcb, it should not happen", K(ret), K(mem_profile_key));
        } else {
          // we have registered exiting flag for the first time, need to reset tcb
          tcb->destory(mem_profile_info);
          if (OB_FAIL(dec_mem_profile_ref_count(mem_profile_key, mem_profile_info))) {
            LOG_WARN("dec mem profile info ref count failed", K(ret), K(mem_profile_key));
          } else if (OB_FAIL(tcb_map_.del(tcb_info))) {
            LOG_WARN("delete tcb failed", KR(ret), K(task_id));
          }
        }
      } else {
        // tcb was already exiting, no need to reset anything
      }
    }
  }
  if (NULL != tcb) {
    if (OB_UNLIKELY(ret == OB_EAGAIN)) {
      ObInterruptCode int_code(OB_RPC_CONNECT_ERROR,
                               GETTID(),
                               tcb->interrupt_info_.self_addr_,
                               "Task Result Mgr interrupt das task of reading");
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObGlobalInterruptManager::getInstance()->interrupt(tcb->interrupt_info_.interrupt_id_, int_code))) {
        LOG_WARN("interrupt reading failed", K(tmp_ret), K(ret), KPC(tcb));
      }
    }
    tcb_map_.revert(tcb);
  }
  return ret;
}

int ObDASTaskResultMgr::iterator_task_result(ObDASDataFetchRes &res,
                                             int64_t &io_read_bytes,
                                             int64_t &ssstore_read_bytes,
                                             int64_t &ssstore_read_row_cnt,
                                             int64_t &memstore_read_row_cnt)
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
    bool need_unset_interrupt = false;
    ObDASTCBMemProfileKey &mem_profile_key = tcb->mem_profile_key_;
    ObDASMemProfileInfo *mem_profile_info = NULL;
    if ((tcb->interrupt_info_.interrupt_id_.first_ != 0 || tcb->interrupt_info_.interrupt_id_.last_ != 0) &&
         OB_FAIL(SET_INTERRUPTABLE(tcb->interrupt_info_.interrupt_id_)) &&
         FALSE_IT(need_unset_interrupt = true)) {
      LOG_WARN("register interrupt failed", KR(ret));
    } else if (OB_FAIL(tcb->register_reading())) {
      LOG_WARN("unregister reading tcb failed", KR(ret), K(task_id));
    } else if (OB_UNLIKELY(!mem_profile_key.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem profile key is invalid when fetch tcb result, it should not happen", K(ret), K(mem_profile_key));
    } else if (OB_FAIL(mem_profile_map_.get_refactored(mem_profile_key,
                                                       mem_profile_info))) {
      LOG_WARN("get mem_profile failed", K(ret), K(mem_profile_key));
    } else if (OB_ISNULL(mem_profile_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem profile is null when fetch tcb result, it should not happen", K(ret), K(mem_profile_key));
    } else {
      if (tcb->enable_rich_format_) {
        int64_t row_cnt_before = tcb->vec_row_store_->get_row_cnt_in_memory();
        OZ (fetch_result_by_vector(tcb, res, has_more));
        mem_profile_info->update_row_count(tcb->vec_row_store_->get_row_cnt_in_memory() - row_cnt_before);
      } else {
        int64_t row_cnt_before = tcb->datum_store_->get_row_cnt_in_memory();
        OZ (fetch_result_by_normal(tcb, res, has_more));
        mem_profile_info->update_row_count(tcb->datum_store_->get_row_cnt_in_memory() - row_cnt_before);
      }
      if (OB_SUCC(ret)) {
        if (tcb->packet_cnt_ == 0) {
          io_read_bytes += tcb->io_read_bytes_;
          ssstore_read_bytes += tcb->ssstore_read_bytes_;
          ssstore_read_row_cnt += tcb->ssstore_read_row_cnt_;
          memstore_read_row_cnt += tcb->memstore_read_row_cnt_;
        }
        tcb->packet_cnt_++;
      }
      int save_ret = ret;
      if (OB_FAIL(tcb->unregister_reading())) {
        LOG_WARN("unregister tcb reading failed", KR(ret), K(task_id));
      } else if (!has_more || OB_FAIL(check_interrupt())) {
        ret = OB_SUCCESS;
        if (OB_FAIL(erase_task_result(task_id, true))) {
          LOG_WARN("erase task result failed", KR(ret), K(task_id));
        }
      }
      ret = save_ret;
    }

    if (need_unset_interrupt) {
      UNSET_INTERRUPTABLE(tcb->interrupt_info_.interrupt_id_);
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
  if (!datum_store.is_inited() && OB_FAIL(datum_store.init(INT64_MAX,
                                                           MTL_ID(),
                                                           common::ObCtxIds::DEFAULT_CTX_ID,
                                                           "DASTaskResMgr",
                                                           false))) {
    LOG_WARN("init datum store failed", KR(ret));
  } else {
    bool added = false;
    ObChunkDatumStore::Iterator &iter = tcb->result_iter_;
    int64_t &read_rows = tcb->read_rows_;
    int64_t loop_times = 0;
    int64_t simulate_iter_hang_on = - EVENT_CALL(EventTable::EN_DAS_SIMULATE_DAS_TASK_ITER_HANG_ON);

    while (OB_SUCC(ret) && !has_more) {
      if ((loop_times ++ & (DAS_TCB_CHECK_INTERRUPT_INTERVAL - 1)) == 0 && OB_FAIL(check_interrupt())) {
        LOG_WARN("fetching result is interrupted",  K(tcb->task_id_), KR(ret));
      } else if (OB_UNLIKELY(simulate_iter_hang_on > 0 && tcb->table_id_ > 500000)) {
        ob_usleep(simulate_iter_hang_on);
        LOG_WARN("simulate iter hang on", K(simulate_iter_hang_on), K(loop_times));
      } else {
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
  if (!vec_row_store.is_inited() && OB_FAIL(vec_row_store.init(tcb->vec_row_store_->get_row_meta(),
                                            tcb->vec_row_store_->get_max_batch_size(),
                                            mem_attr,
                                            INT64_MAX, /* mem_limit */
                                            false, /* enable_dump */
                                            NONE_COMPRESSOR /* compressor_type */))) {
    LOG_WARN("init datum store failed", KR(ret));
  } else {
    bool added = false;
    ObTempRowStore::Iterator &iter = tcb->vec_result_iter_;
    int64_t &read_rows = tcb->read_rows_;
    int64_t loop_times = 0;
    int64_t simulate_iter_hang_on = - EVENT_CALL(EventTable::EN_DAS_SIMULATE_DAS_TASK_ITER_HANG_ON);

    while (OB_SUCC(ret) && !has_more) {
      if ((loop_times ++ & (DAS_TCB_CHECK_INTERRUPT_INTERVAL - 1)) == 0 && OB_FAIL(check_interrupt())) {
        LOG_WARN("fetching result is interrupted",  K(tcb->task_id_), KR(ret));
      } else if (OB_UNLIKELY(simulate_iter_hang_on > 0 && tcb->table_id_ > 500000)) {
        ob_usleep(simulate_iter_hang_on);
        LOG_WARN("vector simulate iter hang on", K(simulate_iter_hang_on), K(loop_times));
      } else {
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

int ObDASTaskResultMgr::get_mem_profile(ObDASTCBMemProfileKey &key, ObDASMemProfileInfo *&info, ObExecContext *exec_ctx, const ObDASScanRtDef *scan_rtdef)
{
  int ret = OB_SUCCESS;
  ObAtomicGetMemProfileCall call;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key is invalid", K(ret), K(key));
  } else if (OB_FAIL(mem_profile_map_.atomic_refactored(key, call))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      if (OB_FAIL(init_mem_profile(key, info, exec_ctx, scan_rtdef))) {
        LOG_WARN("init mem_profile failed", K(ret), K(key));
      }
    } else {
      LOG_WARN("atomic get mem profile from mem profile map failed", K(ret), K(key));
    }
  } else if (OB_FAIL(call.ret_)) {
    LOG_WARN("atomic get mem profile from mem profile map failed", K(call.ret_), K(key));
  } else {
    info = call.mem_profile_info_;
  }
  return ret;
}

int ObDASTaskResultMgr::init_mem_profile(ObDASTCBMemProfileKey &key, ObDASMemProfileInfo *&info, ObExecContext *exec_ctx, const ObDASScanRtDef *scan_rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key is invalid", K(ret), K(key));
  } else if (OB_UNLIKELY(info != NULL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem profile info is not null when init", K(ret), K(key), KPC(info));
  } else if (OB_ISNULL(exec_ctx) || OB_ISNULL(scan_rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer", K(ret), K(key));
  } else {
    lib::ObMutexGuard guard(mem_map_mutex_);

    ObAtomicGetMemProfileCall call;
    if (OB_FAIL(mem_profile_map_.atomic_refactored(key, call))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        void *info_buf = nullptr;
        ObMemAttr mem_info_attr(MTL_ID(), "DASMemInfo", common::ObCtxIds::EXECUTE_CTX_ID);
        ObMemAttr allocator_attr(MTL_ID(), "DASTaskResMgr", common::ObCtxIds::WORK_AREA);
        int64_t op_id = scan_rtdef->scan_op_id_;
        int64_t cache_size = scan_rtdef->scan_rows_size_;

        // this op id is brought from the ctrl svr in the rt of the das scan task,
        // there may be old versions of observer, ObForeignKeyChecker, ObConflictChecker, etc.
        // where the op id is not filled in.
        if (op_id == OB_INVALID_ID) {
          LOG_WARN("the sender did not enter the correct op id", K(op_id));
          op_id = 0; // by design
        }

        // cache_size <= 0, probability is that the optimizer estimated imprecisely
        // cache_size < 0, it will be reinitialized to the default size of 2M inside the memory manager
        // so we use a default size
        if (cache_size <= 0 || cache_size > ObDASMemProfileInfo::CACHE_SIZE) {
          cache_size = ObDASMemProfileInfo::CACHE_SIZE;
        }

        if (OB_ISNULL(info_buf = ob_malloc(sizeof(ObDASMemProfileInfo), mem_info_attr))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem_profile_info", K(ret));
        } else if (OB_ISNULL(info = new(info_buf) ObDASMemProfileInfo(MTL_ID()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("construct mem profile info failed", K(ret));
        } else if (OB_FAIL(info->allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                 OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                 allocator_attr))) {
          LOG_WARN("init fifo allocator failed", K(ret));
          info->allocator_.reset();
          ob_free(info);
          info = NULL;
        } else {
          ObSqlProfileExecInfo profile_exec_info(exec_ctx);
          // all tasks in an op share a common profile, if the tasks come from different rpc,
          // the session in exec_ctx will be invalidated at this time
          profile_exec_info.reset_my_session();
          if (OB_FAIL(info->sql_mem_processor_.init(&info->allocator_,
                                                    MTL_ID(),
                                                    cache_size,
                                                    PHY_TABLE_SCAN,
                                                    op_id,
                                                    profile_exec_info))) {
            LOG_WARN("init sql memory manager processor failed", K(ret));
          } else if (OB_FAIL(mem_profile_map_.set_refactored(key, info))) {
            LOG_WARN("set mem profile in map failed", K(ret));
          } else {
            inc_mem_profile_ref_count(info);
          }
          if (OB_FAIL(ret)) {
            free_mem_profile(info);
          }
        }
      } else {
        LOG_WARN("atomic get mem profile from mem profile map failed", K(ret), K(key));
      }
    } else if (OB_FAIL(call.ret_)) {
      LOG_WARN("atomic get mem profile from mem profile map failed", K(call.ret_), K(key));
    } else {
      info = call.mem_profile_info_;
    }
  }
  return ret;
}


void ObAtomicGetMemProfileCall::operator() (common::hash::HashMapPair<ObDASTCBMemProfileKey, ObDASMemProfileInfo *> &entry)
{
  int &ret = ret_;
  if (OB_NOT_NULL(entry.second)) {
    mem_profile_info_ = entry.second;
    ATOMIC_INC(&(mem_profile_info_->ref_count_));
  } else {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("mem profile info is null", K(ret_), K(entry.first));
  }
}

bool ObDASMemProfileErase::operator() (common::hash::HashMapPair<ObDASTCBMemProfileKey, ObDASMemProfileInfo *> &entry)
{
  if (OB_NOT_NULL(entry.second)) {
    return ATOMIC_LOAD(&(entry.second->ref_count_)) <= 0;
  }
  return true;
}

void ObDASTaskResultMgr::inc_mem_profile_ref_count(ObDASMemProfileInfo *info)
{
  if (OB_NOT_NULL(info)) {
    ATOMIC_INC(&info->ref_count_);
  }
}

int ObDASTaskResultMgr::dec_mem_profile_ref_count(const ObDASTCBMemProfileKey &key,
                                                  ObDASMemProfileInfo *&info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(info)) {
    int64_t ref_count = ATOMIC_SAF(&info->ref_count_, 1);
    if (OB_UNLIKELY(ref_count < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref count of mem_profile < 0", K(ref_count), K(key));
    } else if (ref_count == 0) {
      lib::ObMutexGuard guard(mem_map_mutex_);
      if (OB_FAIL(destroy_mem_profile(key))) {
        LOG_WARN("destroy mem_profile failed", K(ret), K(key));
      }
    }
  }
  return ret;
}

int ObDASTaskResultMgr::destroy_mem_profile(const ObDASTCBMemProfileKey& key)
{
  int ret = OB_SUCCESS;
  ObDASMemProfileInfo *info = NULL;
  ObDASMemProfileErase call;
  bool is_erased = false;
  if (OB_FAIL(mem_profile_map_.erase_if(key, call, is_erased, &info))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("erase mem profile failed", K(ret), K(key));
    }
  } else if (is_erased) {
    free_mem_profile(info);
  }
  return ret;
}

void ObDASTaskResultMgr::free_mem_profile(ObDASMemProfileInfo *&info)
{
  if (OB_NOT_NULL(info)) {
    info->sql_mem_processor_.unregister_profile();
    info->allocator_.reset();
    ob_free(info);
    info = NULL;
  }
}

int ObDASTaskResultMgr::process_dump(ObDASTCB *tcb, ObDASMemProfileInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tcb) || OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret));
  } else {
    bool updated = false;
    bool dumped = false;
    ObSqlMemMgrProcessor &sql_mem_processor = info->sql_mem_processor_;
    common::ObFIFOAllocator &allocator = info->allocator_;

    {
      lib::ObMutexGuard guard(info->mutex_);

      ObDASTaskResultCheckUpdateMem check_update(info->row_count_);
      ObDASTaskResultCheckDump check_dump(sql_mem_processor.get_data_size());
      if (OB_FAIL(sql_mem_processor.update_max_available_mem_size_periodically(&allocator,
                                                                               check_update,
                                                                               updated))) {
        LOG_WARN("update max available memory size periodically failed", K(ret));
      } else if (need_dump(info) &&
                 OB_FAIL(sql_mem_processor.extend_max_memory_size(&allocator,
                                                                  check_dump,
                                                                  dumped,
                                                                  sql_mem_processor.get_data_size()))) {
        LOG_WARN("extend max memory size failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && dumped) {
      int64_t dump_begin_time = common::ObTimeUtility::current_time();
      LOG_DEBUG("begin dump das remote result", K(sql_mem_processor.get_data_size()),
                                                K(sql_mem_processor.get_mem_bound()));

      if (tcb->enable_rich_format_) {
        ObTempRowStore *vec_row_store = tcb->vec_row_store_;
        if (OB_ISNULL(vec_row_store)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vec row store is null");
        } else {
          if (OB_FAIL(vec_row_store->dump(true))) {
            LOG_WARN("dump row store failed", K(ret));
          } else if (OB_FAIL(vec_row_store->finish_add_row())) {
            LOG_WARN("finish add row in store failed", K(ret));
          } else {
            info->set_number_pass(1);
          }
        }
      } else {
        ObChunkDatumStore *datum_store = tcb->datum_store_;
        if (OB_ISNULL(datum_store)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datum store is null");
        } else {
          if (OB_FAIL(datum_store->dump(false, true))) {
            LOG_WARN("dump row store failed", K(ret));
          } else if (OB_FAIL(datum_store->finish_add_row())) {
            LOG_WARN("finish add row in store failed", K(ret));
          } else {
            info->set_number_pass(1);
          }
        }
      }
      int64_t dump_cost = common::ObTimeUtility::current_time() - dump_begin_time;
      LOG_DEBUG("dump das remote result cost(us)", K(dump_cost), K(ret), KPC(tcb),
                                                   K(sql_mem_processor.get_data_size()),
                                                   K(sql_mem_processor.get_mem_bound()));
    }
  }
  return ret;
}

int ObDASTaskResultMgr::check_interrupt() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(IS_INTERRUPTED())) {
    ObInterruptCode &code = GET_INTERRUPT_CODE();
    ret = code.code_;
    LOG_WARN("interrupted", K(ret), K(code.info_));
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
    if (OB_FAIL(task_result_mgr_->erase_task_result(tcb_info.get_value(), true))) {
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
