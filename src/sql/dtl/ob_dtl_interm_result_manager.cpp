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
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "ob_dtl_interm_result_manager.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "observer/virtual_table/ob_all_virtual_dtl_interm_result_monitor.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "share/detect/ob_detect_manager_utils.h"
#include "deps/oblib/src/lib/hash/ob_hashtable.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace oceanbase::sql::dtl;

int ObDTLIntermResultGC::operator() (common::hash::HashMapPair<ObDTLIntermResultKey, ObDTLIntermResultInfo *> &entry)
{
  int ret = OB_SUCCESS;
  int64_t dis = cur_time_ - entry.first.timeout_ts_;
  if (dis > CLEAR_TIME_THRESHOLD) {
    ++clean_cnt_;
    if (OB_FAIL(expire_keys_.push_back(entry.first))) {
      LOG_WARN("clear interm res, but push back failed.", K(ret), K(expire_keys_.count()));
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
    result_info_guard_.set_result_info(*entry.second, interm_res_manager_);
  } else {
    ret_ = OB_HASH_NOT_EXIST;
  }
  LOG_DEBUG("debug start read", K(entry.second->is_read_), K(entry.first));
}

void ObAtomicAppendBlockCall::operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(block_buf_)) {
    if (OB_SUCCESS != entry.second->ret_) {
      ret = entry.second->ret_;
    } else if (OB_UNLIKELY(!entry.second->is_store_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("store of result info is invalid.", K(ret));
    } else {
      int row_cnt_before_append = DTL_IR_STORE_DO(*(entry.second), get_row_cnt_in_memory);
      if (OB_FAIL(DTL_IR_STORE_DO_APPEND_BLOCK(*entry.second, block_buf_, size_, true))) {
        LOG_WARN("store of result info append_block failed", K(ret));
      } else if (OB_FAIL(interm_res_manager_->process_dump(*(entry.second), mem_profile_info_))) {
        LOG_WARN("process_dump failed", K(ret));
      } else {
        int64_t row_cnt_after_append = DTL_IR_STORE_DO(*(entry.second), get_row_cnt_in_memory);
        mem_profile_info_->update_row_count(row_cnt_after_append - row_cnt_before_append);
      }
      if (is_eof_) {
        entry.second->is_eof_ = is_eof_;
      }
    }
  }
  ret_ = ret;
}

void ObAtomicAppendPartBlockCall::operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(block_buf_)) {
    if (OB_SUCCESS != entry.second->ret_) {
      ret = entry.second->ret_;
    } else if (OB_UNLIKELY(!entry.second->is_store_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("store of result info is invalid.", K(ret));
    } else {
      int64_t row_cnt_before_append = DTL_IR_STORE_DO(*(entry.second), get_row_cnt_in_memory);
      if (OB_FAIL(DTL_IR_STORE_DO_APPEND_BLOCK_PAYLOAD(*entry.second,
                                  block_buf_ + start_pos_, length_, rows_, true))) {
        LOG_WARN("store of result info append_block_payload failed", K(ret));
      } else if (OB_FAIL(interm_res_manager_->process_dump(*(entry.second), mem_profile_info_))) {
        LOG_WARN("process_dump failed", K(ret));
      } else {
        int64_t row_cnt_after_append = DTL_IR_STORE_DO(*(entry.second), get_row_cnt_in_memory);
        mem_profile_info_->update_row_count(row_cnt_after_append - row_cnt_before_append);
      }
      if (is_eof_) {
        entry.second->is_eof_ = is_eof_;
      }
    }
  }
  ret_ = ret;
}

int ObDTLIntermResultManager::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t tenant_mem_limit = lib::get_tenant_memory_limit(tenant_id);
  double mem_factor = static_cast<double>(tenant_mem_limit) / lib::get_memory_limit();
  // less memory for meta tenant
  if (is_meta_tenant(tenant_id)) {
    mem_factor = mem_factor * 0.01;
  }

  ObMemAttr interm_res_hash_buck_attr(tenant_id, "HashBuckDTLINT");
  ObMemAttr mem_profile_hash_buck_attr(tenant_id, "IRMMemHashBuck");
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t cpu_quota_concurrency = tenant_config->cpu_quota_concurrency;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(interm_res_map_.create(static_cast<int64_t>(DEFAULT_BUCKET_NUM * mem_factor),
                                 interm_res_hash_buck_attr, interm_res_hash_buck_attr))) {
    LOG_WARN("create interm_res hash table failed", K(ret));
  } else if (OB_FAIL(mem_profile_map_.create(
                        static_cast<int64_t>(MTL_CPU_COUNT() * cpu_quota_concurrency * 2),
                        mem_profile_hash_buck_attr, mem_profile_hash_buck_attr))) {
    LOG_WARN("create mem_profile hash table failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDTLIntermResultManager::mtl_init(ObDTLIntermResultManager *&dtl_interm_result_manager)
{
  return dtl_interm_result_manager->init();
}

void ObDTLIntermResultManager::destroy()
{
  if (IS_INIT) {
    erase_tenant_interm_result_info();
    interm_res_map_.destroy();
    // In theory, mem_profile_map_ should be empty at this point.
    // This is defensive programming.
    clear_mem_profile_map();
    // Used to handle scenarios where destroy is called multiple times.
    is_inited_ = false;
  }
}

int ObDTLIntermResultManager::clear_mem_profile_map()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mem_profile_mutex_);
  if (!mem_profile_map_.empty()) {
    int64_t mem_profile_map_size = mem_profile_map_.size();
    MemProfileMap::bucket_iterator bucket_it = mem_profile_map_.bucket_begin();
    while (bucket_it != mem_profile_map_.bucket_end()) {
      while (true) {
        ObDTLMemProfileKey key;
        {
          MemProfileMap::hashtable::bucket_lock_cond blc(*bucket_it);
          MemProfileMap::hashtable::readlocker locker(blc.lock());
          MemProfileMap::hashtable::hashbucket::const_iterator node_it = bucket_it->node_begin();
          if (node_it == bucket_it->node_end()) {
            break;
          } else {
            key = node_it->first;
          }
        }
        destroy_mem_profile(key);
      }
      ++bucket_it;
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("clear_mem_profile_map", K(MTL_ID()), K(mem_profile_map_size));
    }
  }
  mem_profile_map_.destroy();

  return ret;
}

void ObDTLIntermResultManager::mtl_destroy(ObDTLIntermResultManager *&dtl_interm_result_manager)
{
  if (nullptr != dtl_interm_result_manager) {
    ob_delete(dtl_interm_result_manager);
    dtl_interm_result_manager = nullptr;
  }
}

int ObDTLIntermResultManager::get_interm_result_info(ObDTLIntermResultKey &key,
    ObDTLIntermResultInfo &result_info)
{
  int ret = OB_SUCCESS;
  ObDTLIntermResultInfo *tmp_result_info = NULL;
  if (OB_FAIL(interm_res_map_.get_refactored(key, tmp_result_info))) {
    LOG_TRACE("fail to get row store in result manager", K(ret), K(key.channel_id_));
  } else {
    result_info = *tmp_result_info;
  }
  return ret;
}

int ObDTLIntermResultManager::create_interm_result_info(ObMemAttr &attr,
    ObDTLIntermResultInfoGuard &result_info_guard,
    const ObDTLIntermResultMonitorInfo &monitor_info,
    bool use_rich_format)
{
  int ret = OB_SUCCESS;
  void *result_info_buf = NULL;
  void *store_buf = NULL;
  SET_IGNORE_MEM_VERSION(attr);
  const int64_t store_size = use_rich_format ? sizeof(ObTempColumnStore) : sizeof(ObChunkDatumStore);
  if (OB_ISNULL(result_info_buf = ob_malloc(sizeof(ObDTLIntermResultInfo), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc dtl interm result info", K(ret));
  } else if (OB_ISNULL(store_buf = ob_malloc(store_size, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc store", K(ret));
  } else {
    ObDTLIntermResultInfo *result_info = new(result_info_buf) ObDTLIntermResultInfo();
    if (use_rich_format) {
      result_info->col_store_ = new(store_buf) ObTempColumnStore();
    } else {
      result_info->datum_store_ = new(store_buf) ObChunkDatumStore("DtlIntermRes");
    }
    result_info->use_rich_format_ = use_rich_format;
    result_info->is_read_ = false;
    result_info->trace_id_ = *ObCurTraceId::get_trace_id();
    result_info->monitor_info_ = monitor_info;
    result_info_guard.set_result_info(*result_info, this);
  }
  if (OB_FAIL(ret)) {
    if (NULL != result_info_buf) {
      ob_free(result_info_buf);
    }
    if (NULL != store_buf) {
      ob_free(store_buf);
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
  } else if (-1 == dir_id_) {
    // The code here is mainly for the use of the temp_table.
    // For the px module,
    // the dir_id has already been set in the previous access_mem_profile.
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id_))) {
      LOG_WARN("allocate file directory failed", K(ret));
    } else {
      DTL_IR_STORE_DO(*result_info, set_dir_id, dir_id_);
    }
  }

  if (OB_SUCC(ret)) {
    inc_interm_result_ref_count(result_info);
    if (OB_FAIL(interm_res_map_.set_refactored(key, result_info))) {
      LOG_WARN("fail to set row store in result manager", K(ret));
      dec_interm_result_ref_count(result_info);
    } else {
      LOG_DEBUG("debug insert interm result info", K(key));
    }
  }
  return ret;
}

void ObDTLIntermResultManager::free_interm_result_info_store(ObDTLIntermResultInfo *result_info)
{
  if (NULL != result_info && result_info->is_store_valid()) {
    if (NULL != result_info->datum_store_) {
      result_info->datum_store_->reset();
      result_info->datum_store_->~ObChunkDatumStore();
      ob_free(result_info->datum_store_);
      result_info->datum_store_ = NULL;
    } else if (NULL != result_info->col_store_) {
      result_info->col_store_->reset();
      result_info->col_store_->~ObTempColumnStore();
      ob_free(result_info->col_store_);
      result_info->col_store_ = NULL;
    }
  }
}

int ObDTLIntermResultManager::free_interm_result_info(ObDTLIntermResultInfo *result_info)
{
  int ret = OB_SUCCESS;
  if (NULL != result_info) {
    // For the px module that accesses automatic memory management,
    // the mem_profile_key at this point is valid
    // and enters the erase logic of the automatic memory management component.
    // For the temp_table module that does not access it, this logic is skipped.
    ObDTLMemProfileKey mem_profile_key(result_info->mem_profile_key_);
    ObDTLMemProfileInfo *mem_profile_info = nullptr;
    if (mem_profile_key.is_valid()) {
      if (OB_FAIL(mem_profile_map_.get_refactored(mem_profile_key,
                                                  mem_profile_info))) {
        LOG_WARN("get mem_profile failed", K(ret), K(mem_profile_key));
      } else if (OB_NOT_NULL(mem_profile_info)) {
        mem_profile_info->update_row_count(-DTL_IR_STORE_DO(*result_info, get_row_cnt_in_memory));
      }
    }
    free_interm_result_info_store(result_info);
    ob_free(result_info);
    if (mem_profile_key.is_valid()) {
      if (OB_FAIL(dec_mem_profile_ref_count(mem_profile_key, mem_profile_info))) {
        LOG_WARN("dec mem_profile ref_count failed", K(ret), K(mem_profile_key));
      }
    }
  }
  return ret;
}

int ObDTLIntermResultManager::erase_interm_result_info(const ObDTLIntermResultKey &key,
    bool need_unregister_check_item_from_dm)
{
  int ret = OB_SUCCESS;
  ObDTLIntermResultInfo *result_info = NULL;
  if (OB_FAIL(interm_res_map_.erase_refactored(key, &result_info))) {
    LOG_TRACE("fail to get row store in result manager", K(key), K(ret));
  } else {
    if (need_unregister_check_item_from_dm) {
      ObDetectManagerUtils::intern_result_unregister_check_item_from_dm(result_info);
    }
    dec_interm_result_ref_count(result_info);
  }
  return ret;
}

int ObDTLIntermResultManager::clear_timeout_result_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(interm_res_map_.foreach_refactored(gc_))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else {
    for (int i = 0; i < gc_.expire_keys_.count(); ++i) {
      ObDTLIntermResultKey &key = gc_.expire_keys_.at(i);
      if (OB_FAIL(erase_interm_result_info(key))) {
        LOG_WARN("fail to erase row store", K(key), K(ret));
      }
    }
  }
  return ret;
}

int ObDTLIntermResultManager::atomic_get_interm_result_info(ObDTLIntermResultKey &key,
    ObDTLIntermResultInfoGuard &guard)
{
  int ret = OB_SUCCESS;
  ObAtomicGetIntermResultInfoCall call(guard, this);
  if (OB_FAIL(interm_res_map_.atomic_refactored(key, call))) {
    LOG_TRACE("fail to get row store in result manager", K(ret));
  } else if (OB_SUCCESS != call.ret_) {
    ret = call.ret_;
    LOG_TRACE("fail to get row store in result manager", K(ret));
  }
  return ret;
}

int ObDTLIntermResultManager::atomic_append_block(ObDTLIntermResultKey &key,
                                              ObAtomicAppendBlockCall &call)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(interm_res_map_.atomic_refactored(key, call))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else if (OB_FAIL(call.ret_)) {
    LOG_WARN("ObAtomicAppendBlockCall fail", K(ret));
  } else {
    LOG_DEBUG("debug append block to interm result info", K(key));
  }
  return ret;
}

int ObDTLIntermResultManager::atomic_append_part_block(ObDTLIntermResultKey &key,
                                              ObAtomicAppendPartBlockCall &call)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(interm_res_map_.atomic_refactored(key, call))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else if (OB_FAIL(call.ret_)) {
    LOG_WARN("ObAtomicAppendPartBlockCall fail", K(ret));
  } else {
    LOG_DEBUG("debug append part block to interm result info", K(key));
  }
  return ret;
}

ObDTLIntermResultManager::ObDTLIntermResultManager()
  : interm_res_map_(), is_inited_(false), dir_id_(-1), gc_(),
    mem_profile_mutex_(common::ObLatchIds::SQL_MEMORY_MGR_MUTEX_LOCK)
{
}

ObDTLIntermResultManager::~ObDTLIntermResultManager()
{
  destroy();
}

int ObDTLIntermResultManager::generate_monitor_info_rows(
    observer::ObDTLIntermResultMonitorInfoGetter &monitor_info_getter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(interm_res_map_.foreach_refactored(monitor_info_getter))) {
    LOG_WARN("fail to generate monitor info array from map",
              K(ret), K(interm_res_map_.size()));
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_SUCCESS;
    }
  }
  LOG_TRACE("generate monitor info rows", K(ret), K(interm_res_map_.size()));
  return ret;
}

int ObDTLIntermResultManager::erase_tenant_interm_result_info()
{
  int ret = OB_SUCCESS;
  IntermResMap::bucket_iterator bucket_it = interm_res_map_.bucket_begin();
  while (bucket_it != interm_res_map_.bucket_end()) {
    while (true) {
      ObDTLIntermResultKey key;
      {
        IntermResMap::hashtable::bucket_lock_cond blc(*bucket_it);
        IntermResMap::hashtable::readlocker locker(blc.lock());
        IntermResMap::hashtable::hashbucket::const_iterator node_it = bucket_it->node_begin();
        if (node_it == bucket_it->node_end()) {
          break;
        } else {
          key = node_it->first;
        }
      }
      erase_interm_result_info(key);
    }
    ++bucket_it;
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("erase_tenant_interm_result_info", K(MTL_ID()), K(interm_res_map_.size()));
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
    for (int64_t i = 0; i < infos.count() && OB_SUCC(ret); ++i) {
      ObDTLIntermResultKey key;
      const ObDtlBatchInfo &batch_info = infos.at(i);
      key.timeout_ts_ = buffer->timeout_ts();
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
    key.timeout_ts_ = buffer->timeout_ts();
    key.batch_id_ = buffer->get_batch_id();
    key.channel_id_ = channel_id;
    if (OB_FAIL(process_interm_result_inner(*buffer, key, 0, buffer->size(), 0, buffer->is_eof(), true))) {
      LOG_WARN("process interm result inner", K(ret));
    }
    LOG_TRACE("process interm result", K(buffer->size()), K(buffer->get_batch_info().count()),
              K(buffer->get_batch_info()));
  }

  return ret;
}

int ObDTLIntermResultManager::process_interm_result_inner(ObDtlLinkedBuffer &buffer,
                                                          ObDTLIntermResultKey &interm_res_key,
                                                          int64_t start_pos,
                                                          int64_t length,
                                                          int64_t rows,
                                                          bool is_eof,
                                                          bool append_whole_block)
{
  int ret = OB_SUCCESS;
  ObDTLIntermResultInfo interm_res_info;
  ObDTLIntermResultInfoGuard result_info_guard;
  ObDTLMemProfileKey mem_profile_key(buffer.get_px_sequence_id(), buffer.get_dfo_id());
  ObDTLMemProfileInfo *mem_profile_info = NULL;
  if (OB_FAIL(get_interm_result_info(interm_res_key, interm_res_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ObMemAttr attr(buffer.tenant_id(), "DtlIntermRes", common::ObCtxIds::EXECUTE_CTX_ID);
      interm_res_key.start_time_ = oceanbase::common::ObTimeUtility::current_time();
      if (OB_FAIL(create_interm_result_info(attr,
            result_info_guard,
            ObDTLIntermResultMonitorInfo(buffer.get_dfo_key().qc_id_,
                buffer.get_dfo_id(), buffer.get_sqc_id())))) {
        LOG_WARN("fail to create chunk row store", K(ret));
      } else if (result_info_guard.result_info_->use_rich_format_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the store type of interm_res_info is unexpected.", K(result_info_guard.result_info_));
      } else if (OB_FAIL(result_info_guard.result_info_->datum_store_->init(
                 0, buffer.tenant_id(), common::ObCtxIds::EXECUTE_CTX_ID, "DtlIntermRes"))) {
        LOG_WARN("fail to init buffer", K(ret));
      } else if (OB_FAIL(access_mem_profile(mem_profile_key,
                                           mem_profile_info,
                                           *result_info_guard.result_info_,
                                           buffer))) {
        LOG_WARN("fail to insert access mem_profile", K(ret));
      } else if (OB_FAIL(insert_interm_result_info(interm_res_key, result_info_guard.result_info_))) {
        LOG_WARN("fail to insert row store", K(ret));
      } else {
        int reg_dm_ret = ObDetectManagerUtils::single_dfo_register_check_item_into_dm(
            buffer.get_register_dm_info(), interm_res_key, result_info_guard.result_info_);
        if (OB_SUCCESS != reg_dm_ret) {
          LOG_WARN("[DM] single dfo fail to register_check_item_into_dm",
                   K(reg_dm_ret), K(buffer.get_register_dm_info()), K(interm_res_key));
        }
        LOG_TRACE("register_check_item_into_dm", K(reg_dm_ret),
            K(buffer.get_register_dm_info()), K(interm_res_key),
            K(result_info_guard.result_info_->unregister_dm_info_.node_sequence_id_));
      }
    } else {
      LOG_WARN("fail to get interm_result_info", K(ret), K(interm_res_key));
    }
  } else if (OB_FAIL(mem_profile_map_.get_refactored(mem_profile_key, mem_profile_info))) {
    LOG_WARN("The interm result already exists, and fail to get mem_profile_info.",
              K(ret), K(interm_res_key), K(mem_profile_key));
  }
  // append block
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(length < 0 || start_pos + length > buffer.size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("out of index", K(ret), K(buffer.size()), K(start_pos), K(length));
    } else if (append_whole_block) {
      ObAtomicAppendBlockCall call(buffer.buf(), length, is_eof, this, mem_profile_info);
      if (OB_FAIL(atomic_append_block(interm_res_key, call))) {
        if (OB_HASH_NOT_EXIST == ret &&
            oceanbase::common::ObTimeUtility::current_time() > interm_res_key.timeout_ts_) {
          ret = OB_TIMEOUT;
        }
        LOG_WARN("fail to append block", K(ret), K(interm_res_key.batch_id_));
      } else {
        ret = call.ret_;
        if (OB_SUCCESS != ret) {
          LOG_WARN("fail to append block", K(ret), K(interm_res_key));
        }
      }
    } else {
      ObAtomicAppendPartBlockCall call(buffer.buf(), start_pos, length, rows,
                                       is_eof, this, mem_profile_info);
      if (OB_FAIL(atomic_append_part_block(interm_res_key, call))) {
        if (OB_HASH_NOT_EXIST == ret &&
            oceanbase::common::ObTimeUtility::current_time() > interm_res_key.timeout_ts_) {
          ret = OB_TIMEOUT;
        }
        LOG_WARN("fail to append part block", K(ret), K(interm_res_key.batch_id_));
      } else {
        ret = call.ret_;
        if (OB_SUCCESS != ret) {
          LOG_WARN("fail to append part block", K(ret), K(interm_res_key));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // 注意这里理论上也不会有并发问题，因为channel是点对点且串行发送的
    // 所以这个接收到了，肯定没有其他线程给这个channel发送
    // 尝试先从hash table中释放(尽早释放内存)
    erase_interm_result_info(interm_res_key);
  }
  return ret;
}

void ObDTLIntermResultManager::inc_interm_result_ref_count(ObDTLIntermResultInfo *result_info)
{
  if (OB_NOT_NULL(result_info)) {
    result_info->inc_ref_count();
  }
}

int ObDTLIntermResultManager::dec_interm_result_ref_count(ObDTLIntermResultInfo *&result_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(result_info)) {
    int64_t ref_count = result_info->dec_ref_count();
    if (ref_count <= 0) {
      if (OB_UNLIKELY(ref_count < 0)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ref count of interm result < 0", K(ref_count), KPC(result_info));
      }
      if (OB_FAIL(free_interm_result_info(result_info))) {
        LOG_WARN("fail to free interm_result_info", K(ret));
      } else {
        result_info = NULL;
      }
    }
  }
  return ret;
}

void ObDTLIntermResultManager::runTimerTask()
{
  int ret = OB_SUCCESS;
  gc_.cur_time_ = oceanbase::common::ObTimeUtility::current_time();
  gc_.expire_keys_.reset();
  gc_.clean_cnt_ = 0;
  // Cleaning up expired row_store
  if (OB_FAIL(clear_timeout_result_info())) {
    LOG_WARN("fail to for each row store", K(ret));
  } else {
    int64_t clear_cost = oceanbase::common::ObTimeUtility::current_time() - gc_.cur_time_;
    LOG_INFO("clear dtl interm result cost(us)", K(clear_cost), K(ret),
      K(gc_.expire_keys_.count()), "dump count", gc_.dump_count_,
      "clean count", gc_.clean_cnt_);
  }
}

void ObDTLIntermResultInfoGuard::set_result_info(ObDTLIntermResultInfo &result_info,
                                                 ObDTLIntermResultManager *interm_res_manager)
{
  if (&result_info != result_info_) {
    reset();
    ObDTLIntermResultManager::inc_interm_result_ref_count(&result_info);
    result_info_ = &result_info;
    interm_res_manager_ = interm_res_manager;
  }
}

void ObDTLIntermResultInfoGuard::reset()
{
  if (NULL != result_info_) {
    interm_res_manager_->dec_interm_result_ref_count(result_info_);
    result_info_ = NULL;
  }
}

int ObDTLIntermResultManager::mtl_start(ObDTLIntermResultManager *&dtl_interm_result_manager)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr != dtl_interm_result_manager)) {
    if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(), dtl_interm_result_manager->get_gc_task(),
                            ObDTLIntermResultGCTask::REFRESH_INTERVAL, true))) {
      LOG_WARN("failed to scheduler flush all task", K(ret));
    } else {
      dtl_interm_result_manager->get_gc_task().disable_timeout_check();
      dtl_interm_result_manager->get_gc_task().dtl_interm_result_manager_ = dtl_interm_result_manager;
      dtl_interm_result_manager->get_gc_task().is_start_ = true;
    }
  }
  return ret;
}

void ObDTLIntermResultManager::mtl_stop(ObDTLIntermResultManager *&dtl_interm_result_manager)
{
  if (OB_LIKELY(nullptr != dtl_interm_result_manager) &&
      dtl_interm_result_manager->get_gc_task().is_start_) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), dtl_interm_result_manager->get_gc_task());
  }
}

void ObDTLIntermResultManager::mtl_wait(ObDTLIntermResultManager *&dtl_interm_result_manager)
{
  if (OB_LIKELY(nullptr != dtl_interm_result_manager &&
      dtl_interm_result_manager->get_gc_task().is_start_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), dtl_interm_result_manager->get_gc_task());
  }
}

void ObDTLIntermResultGCTask::runTimerTask()
{
  dtl_interm_result_manager_->runTimerTask();
}

int ObDTLIntermResultManager::process_dump(ObDTLIntermResultInfo &result_info,
                                           ObDTLMemProfileInfo *mem_profile_info)
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  if (OB_ISNULL(mem_profile_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_profile_info is null", K(ret), K(result_info.mem_profile_key_));
  } else {
    {
      lib::ObMutexGuard guard(mem_profile_info->mutex_);
      ObDTLIntermResultCheckUpdateMem check_update_mem(mem_profile_info->row_count_);
      ObDTLIntermResultCheckDump check_dump(mem_profile_info->sql_mem_processor_.get_data_size());
      if (OB_FAIL(mem_profile_info->sql_mem_processor_.update_max_available_mem_size_periodically(
                  &mem_profile_info->allocator_,
                  check_update_mem,
                  updated))) {
        LOG_WARN("failed to update max available memory size periodically", K(ret));
      } else if (need_dump(mem_profile_info) && GCONF.is_sql_operator_dump_enabled()
                && OB_FAIL(mem_profile_info->sql_mem_processor_.extend_max_memory_size(
                &mem_profile_info->allocator_,
                check_dump,
                dumped, mem_profile_info->sql_mem_processor_.get_data_size()))) {
        LOG_WARN("failed to extend max memory size", K(ret));
      }
    }
    // dump logic
    if (OB_SUCC(ret) && dumped) {
      // For the sake of performance, this log section does not have lock protection,
      // so the information printed may not be accurate.
      LOG_DEBUG("begin dump dtl interm result", K(ret),
                K(mem_profile_info->sql_mem_processor_.get_data_size()),
                K(mem_profile_info->sql_mem_processor_.get_mem_bound()));
      int64_t dump_begin_time = oceanbase::common::ObTimeUtility::current_time();
      ++gc_.dump_count_;
      if (OB_FAIL(DTL_IR_STORE_DO_DUMP(result_info, false, true))) {
        LOG_WARN("fail to dump interm row store", K(ret));
        // Used to forcefully write buffer data to disk
        // to prevent errors of not being able to read the remaining data.
      } else if (OB_FAIL(DTL_IR_STORE_DO(result_info, finish_add_row, true))) {
        LOG_WARN("fail to finish add row in interm store", K(ret));
      } else {
        mem_profile_info->set_number_pass(1);
        int64_t dump_cost = oceanbase::common::ObTimeUtility::current_time() - dump_begin_time;
        LOG_DEBUG("dump dtl interm result cost(us)", K(dump_cost), K(ret),
          "dump count", gc_.dump_count_,
          "mem profile", result_info.mem_profile_key_,
          K(mem_profile_info->sql_mem_processor_.get_data_size()),
          K(mem_profile_info->sql_mem_processor_.get_mem_bound()));
      }
    }
  }

  return ret;
}

// Currently, only interm results of parallel execution (px)
// are integrated with automatic memory management.
int ObDTLIntermResultManager::access_mem_profile(const ObDTLMemProfileKey &mem_profile_key,
                                                 ObDTLMemProfileInfo *&mem_profile_info,
                                                 ObDTLIntermResultInfo &interm_res_info,
                                                 ObDtlLinkedBuffer &buffer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mem_profile_map_.get_refactored(mem_profile_key, mem_profile_info))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      if (OB_FAIL(init_mem_profile(mem_profile_key, mem_profile_info, buffer))) {
        LOG_WARN("fail to init mem_profile", K(ret), K(mem_profile_key));
      }
    } else {
      LOG_WARN("fail to get mem_profile", K(ret), K(mem_profile_key));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(mem_profile_info)) {
    inc_mem_profile_ref_count(mem_profile_info);
    DTL_IR_STORE_DO(interm_res_info, set_allocator, mem_profile_info->allocator_);
    DTL_IR_STORE_DO(interm_res_info, set_callback, mem_profile_info);
    DTL_IR_STORE_DO(interm_res_info, set_dir_id, mem_profile_info->sql_mem_processor_.get_dir_id());
    interm_res_info.mem_profile_key_ = mem_profile_key;
    LOG_DEBUG("Current situation of accessing intermediate results in the profile.",
              K(mem_profile_info->ref_count_), K(mem_profile_key), K(buffer));
  }
  return ret;
}

int ObDTLIntermResultManager::init_mem_profile(const ObDTLMemProfileKey &key,
                                              ObDTLMemProfileInfo *&info,
                                              ObDtlLinkedBuffer &buffer)
{
  int ret = OB_SUCCESS;
  if (buffer.seq_no() > 1) {  // seq_no begin from 1
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The buffer is not the first packet, \
            but the corresponding mem_profile does not exist.",
            K(buffer.seq_no()));
  }
  lib::ObMutexGuard guard(mem_profile_mutex_);
  // Possible scenario: Multiple interm results accessing the same mem_profile.
  // Through lock control, when the first one is initialized, the second one tries to initialize again.
  // At this point, by calling get_refactored,
  // it detects that the other end has already initialized the current mem_profile,
  // and it directly exits.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(mem_profile_map_.get_refactored(key, info))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      void *info_buf = nullptr;
      ObMemAttr mem_info_attr(MTL_ID(), "IRMMemInfo", common::ObCtxIds::EXECUTE_CTX_ID);
      ObMemAttr allocator_attr(MTL_ID(), "DtlIntermRes", common::ObCtxIds::WORK_AREA);
      int64_t cache_size = buffer.get_input_rows() * buffer.get_input_width();
      if (cache_size <= 0 || cache_size > ObDTLMemProfileInfo::CACHE_SIZE) {
        cache_size = ObDTLMemProfileInfo::CACHE_SIZE;
      }
      if (OB_ISNULL(info_buf = ob_malloc(sizeof(ObDTLMemProfileInfo), mem_info_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc mem_profile_info", K(ret));
      } else {
        info = new(info_buf) ObDTLMemProfileInfo(MTL_ID());
        if (OB_FAIL(info->allocator_.init(
                    lib::ObMallocAllocator::get_instance(),
                    OB_MALLOC_NORMAL_BLOCK_SIZE,
                    allocator_attr))) {
          LOG_WARN("failed to init fifo allocator", K(ret));
          info->allocator_.reset();
          ob_free(info);
          info = NULL;
        } else if (OB_FAIL(info->sql_mem_processor_.init(
                          &info->allocator_,
                          MTL_ID(),
                          cache_size,
                          PHY_PX_FIFO_RECEIVE,
                          buffer.get_op_id(),
                          &buffer))) {
          LOG_WARN("failed to init sql memory manager processor", K(ret));
        } else if (OB_FAIL(mem_profile_map_.set_refactored(key, info))) {
          LOG_WARN("fail to set row store in result manager", K(ret));
        }
        if (OB_FAIL(ret)) {
          free_mem_profile(info);
        }
      }
    } else {
      LOG_WARN("fail to get mem_profile", K(ret), K(key));
    }
  }
  return ret;
}

int ObDTLIntermResultManager::destroy_mem_profile(const ObDTLMemProfileKey &key)
{
  int ret = OB_SUCCESS;
  ObDTLMemProfileInfo *info = nullptr;
  if (OB_FAIL(mem_profile_map_.erase_refactored(key, &info))) {
    // The reason for the nonexistence of the corresponding mem_profile is as follows:
    // 1. The ref_cnt of the current thread is decremented (which becomes 1).
    // 2. The ref_cnt of another thread is decremented, and it executes destroy.
    // 3. The current thread checks the ref_cnt (which is now 0) to execute destroy,
    //    but it finds that the mem_profile has already been released at this time.
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("erase mem_profile failed", K(ret), K(key));
    }
  } else {
    free_mem_profile(info);
  }
  return ret;
}

void ObDTLIntermResultManager::inc_mem_profile_ref_count(ObDTLMemProfileInfo *info)
{
  if (OB_NOT_NULL(info)) {
    ATOMIC_INC(&info->ref_count_);
  }
}

int ObDTLIntermResultManager::dec_mem_profile_ref_count(const ObDTLMemProfileKey &key,
                                                        ObDTLMemProfileInfo *&info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(info)) {
    int64_t ref_count = ATOMIC_SAF(&info->ref_count_, 1);
    if (ref_count <= 0) {
      if (OB_UNLIKELY(ref_count < 0)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ref count of mem_profile < 0", K(ref_count), K(key));
      }
      lib::ObMutexGuard guard(mem_profile_mutex_);
      if (OB_FAIL(destroy_mem_profile(key))) {
        LOG_WARN("destroy mem_profile failed!", K(ret), K(key));
      }
    }
  }
  return ret;
}

void ObDTLIntermResultManager::free_mem_profile(ObDTLMemProfileInfo *&info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(info)) {
    info->sql_mem_processor_.unregister_profile();
    info->allocator_.reset();
    ob_free(info);
    info = NULL;
  }
}
