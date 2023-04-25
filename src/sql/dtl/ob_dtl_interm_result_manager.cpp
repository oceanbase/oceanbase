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

#define USING_LOG_PREFIX SQL_DTL
#include "ob_dtl_interm_result_manager.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "observer/virtual_table/ob_all_virtual_dtl_interm_result_monitor.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_msg_type.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace oceanbase::sql::dtl;



void ObDTLIntermResultGC::runTimerTask()
{
  int ret = OB_SUCCESS;
  cur_time_ = oceanbase::common::ObTimeUtility::current_time();
  expire_keys_.reset();

  // 之前放到了foreach里面，也就是每个元素会++一次，那这样，
  // 如果缓存是一个比较大的质数，则可能需要好久，而且每次都在变，所以可能基本上走不进去,bigbig bug
  ++dump_count_;
  interm_cnt_ = 0;
  // dump 每隔10秒 && 未超时 && 未使用 的row_store
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDTLIntermResultManager::getInstance().dump_result_info(*this))) {
      LOG_WARN("fail to for each row store", K(ret));
    } else {
      int64_t dump_cost = oceanbase::common::ObTimeUtility::current_time() - cur_time_;
      LOG_INFO("dump dtl interm result cost(us)", K(dump_cost), K(ret),
        "interm count", interm_cnt_, "dump count", dump_count_);
    }
  }

  clean_cnt_ = 0;
  interm_cnt_ = 0;
  cur_time_ = oceanbase::common::ObTimeUtility::current_time();
  // 清理超时row_store
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDTLIntermResultManager::getInstance().clear_timeout_result_info(*this))) {
      LOG_WARN("fail to for each row store", K(ret));
    } else {
      int64_t clear_cost = oceanbase::common::ObTimeUtility::current_time() - cur_time_;
      LOG_INFO("clear dtl interm result cost(us)", K(clear_cost), K(ret),
        K(expire_keys_.count()), "dump count", dump_count_,
        "interm count", interm_cnt_, "clean count", clean_cnt_);
    }
  }
}

void ObDTLIntermResultGC::reset()
{
  expire_keys_.reset();
}

int ObDTLIntermResultGC::operator() (common::hash::HashMapPair<ObDTLIntermResultKey, ObDTLIntermResultInfo *> &entry)
{
  int ret = OB_SUCCESS;
  int64_t dis = cur_time_ - entry.first.time_us_;
  ++interm_cnt_;
  if (DUMP == gc_type_) {
    if (!entry.second->is_read_ &&
        OB_SUCCESS == entry.second->ret_ &&
        cur_time_ - entry.first.start_time_ > DUMP_TIME_THRESHOLD &&
        dis < 0) {
      if (NULL != tenant_guard_) {
        tenant_guard_->switch_to(entry.second->datum_store_->get_tenant_id());
      }
      int64_t dump_time = oceanbase::common::ObTimeUtility::current_time();
      if (OB_FAIL(DTL_IR_STORE_DO(*entry.second, dump, false, true))) {
        LOG_WARN("fail to dump interm row store", K(ret));
      } else if (OB_FAIL(DTL_IR_STORE_DO(*entry.second, finish_add_row, true))) {
        LOG_WARN("fail to finish add row in interm store", K(ret));
      } else {
        LOG_DEBUG("dump interm result success", K(entry.first.channel_id_));
      }
      int64_t dump_cost = oceanbase::common::ObTimeUtility::current_time() - dump_time;
      entry.second->dump_time_ = 0 == entry.second->dump_time_ ? dump_time : entry.second->dump_time_;
      entry.second->dump_cost_ += dump_cost;
    }
    if (OB_SUCCESS != ret) {
      entry.second->ret_ = ret;
      // free interm result info datum store in advance for memory optimization.
      ObDTLIntermResultManager::getInstance().free_interm_result_info_store(entry.second);
    }
  }

  if (CLEAR == gc_type_ && 0 == dump_count_ % 10) {
    // 如果已经dump已经做过10次了, 则认为可以清理了.
    // 按照设计, dump每隔10秒触发一次, 清理则每隔100秒.
    // hard code 超过超时时间30秒.
    if (dis > CLEAR_TIME_THRESHOLD) {
      ++clean_cnt_;
      ret = expire_keys_.push_back(entry.first);
    }
  }

  // The Story Behind Return Code:
  //   We change the interface for this because of supporting that iterations encounter an error
  //   to return immediately, yet for all the existing logics there, they don't care the return
  //   code and wants to continue iteration anyway. So to keep the old behavior and makes everyone
  //   else happy, we have to return OB_SUCCESS here. And we only make this return code thing
  //   affects the behavior in tenant meta manager washing tablet. If you want to change the
  //   behavior in such places, please consult the individual file owners to fully understand the
  //   needs there.
  return common::OB_SUCCESS;
}

void ObAtomicGetIntermResultInfoCall::operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry)
{
  if (entry.second->is_eof_) {
    entry.second->is_read_ = true;
    result_info_guard_.set_result_info(*entry.second);
  } else {
    ret_ = OB_HASH_NOT_EXIST;
  }
  LOG_DEBUG("debug start read", K(entry.second->is_read_), K(entry.first));
}

void ObAtomicAppendBlockCall::operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry)
{
  if (OB_NOT_NULL(block_buf_)) {
    if (OB_SUCCESS != entry.second->ret_) {
      ret_ = entry.second->ret_;
    } else if (OB_UNLIKELY(!entry.second->is_store_valid())) {
      ret_ = OB_INVALID_ARGUMENT;
    } else {
      ret_ = DTL_IR_STORE_DO(*entry.second, append_block, block_buf_, size_, true);
      if (is_eof_) {
        entry.second->is_eof_ = is_eof_;
      }
    }
  }
}

void ObAtomicAppendPartBlockCall::operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry)
{
  if (OB_NOT_NULL(block_buf_)) {
    if (OB_SUCCESS != entry.second->ret_) {
      ret_ = entry.second->ret_;
    } else if (OB_UNLIKELY(!entry.second->is_store_valid())) {
      ret_ = OB_INVALID_ARGUMENT;
    } else {
      ret_ = DTL_IR_STORE_DO(*entry.second, append_block_payload, block_buf_ + start_pos_, length_, rows_, true);
      if (is_eof_) {
        entry.second->is_eof_ = is_eof_;
      }
    }
  }
}

int ObEraseTenantIntermResultInfo::operator() (common::hash::HashMapPair<ObDTLIntermResultKey, ObDTLIntermResultInfo *> &entry)
{
  int ret = OB_SUCCESS;
  if (entry.second->tenant_id_ == tenant_id_) {
    if (OB_FAIL(expire_keys_.push_back(entry.first))) {
      LOG_WARN("push back failed", K(ret));
      ret_ = ret;
    }
  }
  return OB_SUCCESS;
}

ObDTLIntermResultManager &ObDTLIntermResultManager::getInstance()
{
  static ObDTLIntermResultManager the_ir_manager;
  return the_ir_manager;
}


int ObDTLIntermResultManager::init()
{
  int ret = OB_SUCCESS;
  auto attr = SET_USE_500("HashBuckDTLINT");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(map_.create(BUCKET_NUM,
                                 attr, attr))) {
    LOG_WARN("create hash table failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, gc_,
      ObDTLIntermResultGC::REFRESH_INTERVAL, true))) {
    LOG_WARN("schedule interm result gc failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObDTLIntermResultManager::destroy()
{
  if (IS_INIT) {
    map_.destroy();
  }
}

int ObDTLIntermResultManager::get_interm_result_info(ObDTLIntermResultKey &key,
    ObDTLIntermResultInfo &result_info)
{
  int ret = OB_SUCCESS;
  ObDTLIntermResultInfo *tmp_result_info = NULL;
  if (OB_FAIL(map_.get_refactored(key, tmp_result_info))) {
    LOG_TRACE("fail to get row store in result manager", K(ret), K(key.channel_id_));
  } else {
    result_info = *tmp_result_info;
  }
  return ret;
}

int ObDTLIntermResultManager::create_interm_result_info(ObMemAttr &attr,
    ObDTLIntermResultInfoGuard &result_info_guard,
    const ObDTLIntermResultMonitorInfo &monitor_info)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObDTLIntermResultInfo *result_info = NULL;
  void *result_info_buf = NULL;
  const int64_t size = sizeof(ObChunkDatumStore);
  if (OB_ISNULL(result_info_buf =
      static_cast<ObDTLIntermResultInfo *>(ob_malloc(sizeof(ObDTLIntermResultInfo), attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc dtl interm result info", K(ret));
  } else if (OB_ISNULL(ptr = ob_malloc(size, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ob chunk row store ret", K(ret));
  } else {
    result_info = new(result_info_buf) ObDTLIntermResultInfo();
    result_info->datum_store_ = new(ptr) ObChunkDatumStore();
    result_info->is_read_ = false;
    result_info->trace_id_ = *ObCurTraceId::get_trace_id();
    result_info->monitor_info_ = monitor_info;
    result_info->tenant_id_ = attr.tenant_id_;
    result_info_guard.set_result_info(*result_info);
  }
  if (OB_FAIL(ret)) {
    if (NULL != result_info_buf) {
      ob_free(result_info_buf);
    }
    if (NULL != ptr) {
      ob_free(ptr);
    }
  }
  return ret;
}

int ObDTLIntermResultManager::insert_interm_result_info(ObDTLIntermResultKey &key,
    ObDTLIntermResultInfo *&result_info)
{
  int ret = OB_SUCCESS;
  if (!result_info->is_store_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row store is null", K(ret));
  } else if (-1 == dir_id_ && OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id_))) {
    LOG_WARN("allocate file directory failed", K(ret));
  } else if (FALSE_IT(DTL_IR_STORE_DO(*result_info, set_dir_id, dir_id_))) {
  } else if (FALSE_IT(result_info->inc_ref_count())) {
  } else if (OB_FAIL(map_.set_refactored(key, result_info))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
    dec_interm_result_ref_count(result_info);
  } else {
    LOG_DEBUG("debug create interm result info", K(key));
  }
  return ret;
}

void ObDTLIntermResultManager::free_interm_result_info_store(ObDTLIntermResultInfo *result_info)
{
  if (NULL != result_info) {
    if (result_info->is_store_valid()) {
      DTL_IR_STORE_DO(*result_info, reset);
      if (NULL != result_info->datum_store_) {
        result_info->datum_store_->~ObChunkDatumStore();
        ob_free(result_info->datum_store_);
        result_info->datum_store_ = NULL;
      }
    }
  }
}

void ObDTLIntermResultManager::free_interm_result_info(ObDTLIntermResultInfo *result_info)
{
  if (NULL != result_info) {
    free_interm_result_info_store(result_info);
    ob_free(result_info);
  }
}

int ObDTLIntermResultManager::erase_interm_result_info(ObDTLIntermResultKey &key)
{
  int ret = OB_SUCCESS;
  ObDTLIntermResultInfo *result_info = NULL;
  if (OB_FAIL(map_.erase_refactored(key, &result_info))) {
    LOG_TRACE("fail to get row store in result manager", K(key), K(ret));
  } else {
    dec_interm_result_ref_count(result_info);
  }
  return ret;
}

int ObDTLIntermResultManager::clear_timeout_result_info(ObDTLIntermResultGC &gc)
{
  int ret = OB_SUCCESS;
  gc.gc_type_ = ObDTLIntermResultGC::CLEAR;
  if (OB_FAIL(map_.foreach_refactored(gc))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      for (int i = 0; i < gc.expire_keys_.count(); ++i) {
        ObDTLIntermResultKey &key = gc.expire_keys_.at(i);
        if (OB_FAIL(ObDTLIntermResultManager::getInstance().erase_interm_result_info(key))) {
          LOG_WARN("fail to erase row store", K(key), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDTLIntermResultManager::dump_result_info(ObDTLIntermResultGC &gc)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  gc.gc_type_ = ObDTLIntermResultGC::DUMP;
  gc.tenant_guard_ = &guard;
  if (OB_FAIL(map_.foreach_refactored(gc))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  }
  gc.tenant_guard_ = NULL;
  return ret;
}

int ObDTLIntermResultManager::atomic_get_interm_result_info(ObDTLIntermResultKey &key,
    ObDTLIntermResultInfoGuard &guard)
{
  int ret = OB_SUCCESS;
  ObAtomicGetIntermResultInfoCall call(guard);
  if (OB_FAIL(map_.atomic_refactored(key, call))) {
    LOG_TRACE("fail to get row store in result manager", K(ret));
  } else if (OB_SUCCESS != call.ret_) {
    ret = call.ret_;
    LOG_TRACE("fail to get row store in result manager", K(ret));
  }
  return ret;
}

int ObDTLIntermResultManager::atomic_append_block(ObDTLIntermResultKey &key, ObAtomicAppendBlockCall &call)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.atomic_refactored(key, call))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else {
    LOG_DEBUG("debug append block to interm result info", K(key));
  }
  return ret;
}

int ObDTLIntermResultManager::atomic_append_part_block(ObDTLIntermResultKey &key, ObAtomicAppendPartBlockCall &call)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.atomic_refactored(key, call))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else {
    LOG_DEBUG("debug append part block to interm result info", K(key));
  }
  return ret;
}

 ObDTLIntermResultManager::ObDTLIntermResultManager()
  : map_(), is_inited_(false), dir_id_(-1), gc_()
{
}

 ObDTLIntermResultManager::~ObDTLIntermResultManager()
{
  destroy();
}

int ObDTLIntermResultManager::generate_monitor_info_rows(observer::ObDTLIntermResultMonitorInfoGetter &monitor_info_getter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.foreach_refactored(monitor_info_getter))) {
    LOG_WARN("fail to generate monitor info array from map", K(ret));
  }
  LOG_TRACE("generate monitor info rows", K(ret), K(map_.size()));
  return ret;
}

int ObDTLIntermResultManager::erase_tenant_interm_result_info(int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObEraseTenantIntermResultInfo eraser;
  eraser.tenant_id_ = tenant_id;
  if (OB_FAIL(map_.foreach_refactored(eraser))) {
    LOG_WARN("fail to get tenant result info in result manager", K(ret), K(tenant_id));
  } else {
    ret = eraser.ret_;
    for (int i = 0; i < eraser.expire_keys_.count(); ++i) {
      ObDTLIntermResultKey &key = eraser.expire_keys_.at(i);
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = ObDTLIntermResultManager::getInstance().erase_interm_result_info(key))) {
        if (OB_HASH_NOT_EXIST != tmp_ret) {
          LOG_WARN("fail to erase result info", K(key), K(ret));
          ret = tmp_ret;
        }
      }
    }
    if (eraser.expire_keys_.count() < 100) {
      LOG_INFO("erase_tenant_interm_result_info", K(tenant_id), K(eraser.expire_keys_));
    } else {
      LOG_INFO("erase_tenant_interm_result_info", K(tenant_id), K(eraser.expire_keys_.count()));
    }
  }
  return ret;
}

int ObDTLIntermResultManager::process_interm_result(ObDtlLinkedBuffer *buffer, int64_t channel_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to process buffer", K(ret));
  } else if (buffer->is_batch_info_valid()) {
    const ObSArray<ObDtlBatchInfo> &infos = buffer->get_batch_info();
    for (int64_t i = 0; i < infos.count() && OB_SUCC(ret); i++) {
      ObDTLIntermResultKey key;
      const ObDtlBatchInfo &batch_info = infos.at(i);
      key.time_us_ = buffer->timeout_ts();
      key.batch_id_ = batch_info.batch_id_;
      key.channel_id_ = channel_id;
      const int64_t start_pos = batch_info.start_;
      const int64_t length = batch_info.end_ - start_pos;
      const int64_t rows = batch_info.rows_;
      const bool is_eof = infos.count() - 1 == i ? buffer->is_eof() : true;
      if (OB_FAIL(process_interm_result_inner(*buffer, key, start_pos, length, rows, is_eof, false))) {
        LOG_WARN("process interm result inner", K(ret));
      }
    }
    LOG_TRACE("process interm result", K(buffer->size()), K(buffer->get_batch_info().count()),
               K(buffer->get_batch_info()));
  } else {
    ObDTLIntermResultKey key;
    key.time_us_ = buffer->timeout_ts();
    key.batch_id_ = buffer->get_batch_id();
    key.channel_id_ = channel_id;
    if (OB_FAIL(process_interm_result_inner(*buffer, key, 0, buffer->size(), 0, buffer->is_eof(), true))) {
      LOG_WARN("process interm result inner", K(ret));
    }
  }
  return ret;
}

int ObDTLIntermResultManager::process_interm_result_inner(ObDtlLinkedBuffer &buffer,
                                                                 ObDTLIntermResultKey &key,
                                                                 int64_t start_pos,
                                                                 int64_t length,
                                                                 int64_t rows,
                                                                 bool is_eof,
                                                                 bool append_whole_block)
{
  int ret = OB_SUCCESS;
  ObDTLIntermResultInfo result_info;
  if (OB_FAIL(ObDTLIntermResultManager::getInstance().get_interm_result_info(key,
        result_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ObDTLIntermResultInfoGuard result_info_guard;
      ObMemAttr attr(buffer.tenant_id(), "DtlIntermRes", common::ObCtxIds::EXECUTE_CTX_ID);
      key.start_time_ = oceanbase::common::ObTimeUtility::current_time();
      ret = OB_SUCCESS;
      if (OB_FAIL(ObDTLIntermResultManager::getInstance().create_interm_result_info(attr,
            result_info_guard,
            ObDTLIntermResultMonitorInfo(buffer.get_dfo_key().qc_id_,
                buffer.get_dfo_id(), buffer.get_sqc_id())))) {
        LOG_WARN("fail to create chunk row store", K(ret));
      } else if (OB_FAIL(DTL_IR_STORE_DO(
                  *result_info_guard.result_info_, init,
                  0, buffer.tenant_id(), common::ObCtxIds::EXECUTE_CTX_ID, "DtlIntermRes"))) {
        LOG_WARN("fail to init buffer", K(ret));
      } else if (OB_FAIL(ObDTLIntermResultManager::getInstance().insert_interm_result_info(key, result_info_guard.result_info_))) {
        LOG_WARN("fail to insert row store", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(length < 0 || start_pos + length > buffer.size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("out of index", K(ret), K(buffer.size()), K(start_pos), K(length));
    } else if (append_whole_block) {
      ObAtomicAppendBlockCall call(buffer.buf(), length, is_eof);
      if (OB_FAIL(ObDTLIntermResultManager::getInstance().atomic_append_block(key, call))) {
        if (OB_HASH_NOT_EXIST == ret &&
            oceanbase::common::ObTimeUtility::current_time() > key.time_us_) {
          ret = OB_TIMEOUT;
        }
        LOG_WARN("fail to append block", K(ret), K(key.batch_id_));
      } else {
        ret = call.ret_;
        if (OB_SUCCESS != ret) {
          LOG_WARN("fail to append block", K(ret), K(key));
        }
      }
    } else {
      ObAtomicAppendPartBlockCall call(buffer.buf(), start_pos, length, rows, is_eof);
      if (OB_FAIL(ObDTLIntermResultManager::getInstance().atomic_append_part_block(key, call))) {
        if (OB_HASH_NOT_EXIST == ret &&
            oceanbase::common::ObTimeUtility::current_time() > key.time_us_) {
          ret = OB_TIMEOUT;
        }
        LOG_WARN("fail to append block", K(ret), K(key.batch_id_));
      } else {
        ret = call.ret_;
        if (OB_SUCCESS != ret) {
          LOG_WARN("fail to append block", K(ret), K(key));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // 注意这里理论上也不会有并发问题，因为channel是点对点且串行发送的
    // 所以这个接收到了，肯定没有其他线程给这个channel发送
    // 尝试先从hash table中释放(尽早释放内存，其实也可以让timer来清理)
    ObDTLIntermResultManager::getInstance().erase_interm_result_info(key);
  }
  return ret;
}

void ObDTLIntermResultManager::inc_interm_result_ref_count(ObDTLIntermResultInfo *result_info)
{
  if (OB_NOT_NULL(result_info)) {
    result_info->inc_ref_count();
  }
}

void ObDTLIntermResultManager::dec_interm_result_ref_count(ObDTLIntermResultInfo *&result_info)
{
  if (OB_NOT_NULL(result_info)) {
    int64_t ref_count = result_info->dec_ref_count();
    if (ref_count <= 0) {
      if (OB_UNLIKELY(ref_count < 0)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ref count of interm result < 0", K(ref_count), KPC(result_info));
      }
      free_interm_result_info(result_info);
      result_info = NULL;
    }
  }
}

void ObDTLIntermResultInfoGuard::set_result_info(ObDTLIntermResultInfo &result_info)
{
  if (&result_info != result_info_) {
    reset();
    ObDTLIntermResultManager::inc_interm_result_ref_count(&result_info);
    result_info_ = &result_info;
  }
}

void ObDTLIntermResultInfoGuard::reset()
{
  if (NULL != result_info_) {
    ObDTLIntermResultManager::dec_interm_result_ref_count(result_info_);
    result_info_ = NULL;
  }
}
