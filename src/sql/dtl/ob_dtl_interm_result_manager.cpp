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

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace oceanbase::sql::dtl;

void ObDTLIntermResultGC::runTimerTask()
{
  int ret = OB_SUCCESS;
  cur_time_ = oceanbase::common::ObTimeUtility::current_time();
  expire_keys_.reset();

  ++dump_count_;
  interm_cnt_ = 0;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDTLIntermResultManager::getInstance().dump_result_info(*this))) {
      LOG_WARN("fail to for each row store", K(ret));
    } else {
      int64_t dump_cost = oceanbase::common::ObTimeUtility::current_time() - cur_time_;
      LOG_INFO("dump dtl interm result cost(us)",
          K(dump_cost),
          K(ret),
          "interm count",
          interm_cnt_,
          "dump count",
          dump_count_);
    }
  }

  clean_cnt_ = 0;
  interm_cnt_ = 0;
  cur_time_ = oceanbase::common::ObTimeUtility::current_time();
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDTLIntermResultManager::getInstance().clear_timeout_result_info(*this))) {
      LOG_WARN("fail to for each row store", K(ret));
    } else {
      int64_t clear_cost = oceanbase::common::ObTimeUtility::current_time() - cur_time_;
      LOG_INFO("clear dtl interm result cost(us)",
          K(clear_cost),
          K(ret),
          K(expire_keys_.count()),
          "dump count",
          dump_count_,
          "interm count",
          interm_cnt_,
          "clean count",
          clean_cnt_);
    }
  }
}

void ObDTLIntermResultGC::reset()
{
  expire_keys_.reset();
}

void ObDTLIntermResultGC::operator()(common::hash::HashMapPair<ObDTLIntermResultKey, ObDTLIntermResultInfo>& entry)
{
  int ret = OB_SUCCESS;
  int64_t dis = cur_time_ - entry.first.time_us_;
  ++interm_cnt_;
  if (DUMP == gc_type_) {
    if (!entry.second.is_read_ && cur_time_ - entry.first.start_time_ > DUMP_TIME_THRESHOLD && dis < 0) {
      if (OB_FAIL(DTL_IR_STORE_DO(entry.second, dump, false, true))) {
        LOG_WARN("fail to dump interm row store", K(ret));
      } else if (OB_FAIL(DTL_IR_STORE_DO(entry.second, finish_add_row, true))) {
        LOG_WARN("fail to finish add row in interm store", K(ret));
      } else {
        LOG_DEBUG("dump interm result success", K(entry.first.channel_id_));
      }
    }
  }

  if (CLEAR == gc_type_ && 0 == dump_count_ % 10) {
    if (dis > CLEAR_TIME_THRESHOLD) {
      ++clean_cnt_;
      ret = expire_keys_.push_back(entry.first);
    }
  }
}

void ObAtomicGetIntermResultInfoCall::operator()(
    common::hash::HashMapPair<ObDTLIntermResultKey, ObDTLIntermResultInfo>& entry)
{
  entry.second.is_read_ = true;
  result_info_ = entry.second;
  LOG_DEBUG("debug start read", K(entry.second.is_read_), K(entry.first));
}

void ObAtomicAppendBlockCall::operator()(common::hash::HashMapPair<ObDTLIntermResultKey, ObDTLIntermResultInfo>& entry)
{
  if (OB_NOT_NULL(block_buf_)) {
    ret_ = DTL_IR_STORE_DO(entry.second, append_block, block_buf_, size_, true);
  }
}

ObDTLIntermResultManager& ObDTLIntermResultManager::getInstance()
{
  static ObDTLIntermResultManager the_ir_manager;
  return the_ir_manager;
}

int ObDTLIntermResultManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(map_.create(BUCKET_NUM, "HashBuckDTLINT", "HashNodeDTLINT"))) {
    LOG_WARN("create hash table failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, gc_, ObDTLIntermResultGC::REFRESH_INTERVAL, true))) {
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

int ObDTLIntermResultManager::get_interm_result_info(ObDTLIntermResultKey& key, ObDTLIntermResultInfo& result_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.get_refactored(key, result_info))) {
    LOG_WARN("fail to get row store in result manager", K(ret), K(key.channel_id_));
  }
  return ret;
}

int ObDTLIntermResultManager::create_interm_result_info(ObMemAttr& attr, ObDTLIntermResultInfo& result_info)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  const int64_t size = result_info.is_datum_ ? sizeof(ObChunkDatumStore) : sizeof(ObChunkRowStore);
  if (OB_ISNULL(ptr = ob_malloc(size, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ob chunk row store ret", K(ret));
  } else {
    if (result_info.is_datum_) {
      result_info.datum_store_ = new (ptr) ObChunkDatumStore();
    } else {
      result_info.row_store_ = new (ptr) ObChunkRowStore();
    }
    result_info.is_read_ = false;
  }
  return ret;
}

int ObDTLIntermResultManager::insert_interm_result_info(ObDTLIntermResultKey& key, ObDTLIntermResultInfo& result_info)
{
  int ret = OB_SUCCESS;
  if (!result_info.is_store_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row store is null", K(ret));
  } else if (-1 == dir_id_ && OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id_))) {
    LOG_WARN("allocate file directory failed", K(ret));
  } else if (FALSE_IT(DTL_IR_STORE_DO(result_info, set_dir_id, dir_id_))) {
  } else if (OB_FAIL(map_.set_refactored(key, result_info))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else {
    LOG_DEBUG("debug create interm result info", K(key));
  }
  return ret;
}

void ObDTLIntermResultManager::free_interm_result_info(ObDTLIntermResultInfo& result_info)
{
  if (result_info.is_store_valid()) {
    if (!result_info.is_read_) {
      DTL_IR_STORE_DO(result_info, finish_add_row, true);
    }
    DTL_IR_STORE_DO(result_info, reset);
    if (NULL != result_info.row_store_) {
      result_info.row_store_->~ObChunkRowStore();
      ob_free(result_info.row_store_);
      result_info.row_store_ = NULL;
    } else if (NULL != result_info.datum_store_) {
      result_info.datum_store_->~ObChunkDatumStore();
      ob_free(result_info.datum_store_);
      result_info.datum_store_ = NULL;
    }
  }
}

int ObDTLIntermResultManager::erase_interm_result_info(ObDTLIntermResultKey& key)
{
  int ret = OB_SUCCESS;
  ObDTLIntermResultInfo result_info;
  if (OB_FAIL(map_.erase_refactored(key, &result_info))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else {
    free_interm_result_info(result_info);
  }
  return ret;
}

int ObDTLIntermResultManager::clear_timeout_result_info(ObDTLIntermResultGC& gc)
{
  int ret = OB_SUCCESS;
  gc.gc_type_ = ObDTLIntermResultGC::CLEAR;
  if (OB_FAIL(map_.foreach_refactored(gc))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      for (int i = 0; i < gc.expire_keys_.count(); ++i) {
        ObDTLIntermResultKey& key = gc.expire_keys_.at(i);
        if (OB_FAIL(ObDTLIntermResultManager::getInstance().erase_interm_result_info(key))) {
          LOG_WARN("fail to erase row store", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDTLIntermResultManager::dump_result_info(ObDTLIntermResultGC& gc)
{
  int ret = OB_SUCCESS;
  gc.gc_type_ = ObDTLIntermResultGC::DUMP;
  if (OB_FAIL(map_.foreach_refactored(gc))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  }
  return ret;
}

int ObDTLIntermResultManager::atomic_get_interm_result_info(
    ObDTLIntermResultKey& key, ObDTLIntermResultInfo& result_info)
{
  int ret = OB_SUCCESS;
  ObAtomicGetIntermResultInfoCall call;
  if (OB_FAIL(map_.atomic_refactored(key, call))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else {
    result_info = call.result_info_;
    LOG_DEBUG("debug create interm result info", K(key));
  }
  return ret;
}

int ObDTLIntermResultManager::atomic_append_block(ObDTLIntermResultKey& key, ObAtomicAppendBlockCall& call)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.atomic_refactored(key, call))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else {
    LOG_DEBUG("debug append block to interm result info", K(key));
  }
  return ret;
}

ObDTLIntermResultManager::ObDTLIntermResultManager() : map_(), is_inited_(false), dir_id_(-1), gc_()
{}

ObDTLIntermResultManager::~ObDTLIntermResultManager()
{
  destroy();
}
