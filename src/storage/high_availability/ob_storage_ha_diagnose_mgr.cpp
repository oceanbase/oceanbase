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
#include "ob_storage_ha_diagnose_mgr.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
using namespace share;

namespace storage
{
/**
 * ------------------------------ObStorageHADiagIterator---------------------
 */
ObStorageHADiagIterator::ObStorageHADiagIterator()
  : keys_(),
    cnt_(0),
    cur_idx_(0),
    is_opened_(false)
{
}

int ObStorageHADiagIterator::open(const ObStorageHADiagType &type)
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObStorageHADiagIterator has been opened", K(ret));
  } else if (type >= ObStorageHADiagType::MAX_TYPE
    || type < ObStorageHADiagType::ERROR_DIAGNOSE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", K(ret), K(type));
  } else {
    ObStorageHADiagMgr *mgr = nullptr;
    if (OB_ISNULL(mgr = MTL(ObStorageHADiagMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObStorageHADiagMgr from MTL", K(ret));
    } else if (OB_FAIL(mgr->get_iter_array(type, keys_))) {
      LOG_WARN("failed to get iter info", K(ret), K(type));
    } else {
      cur_idx_ = 0;
      cnt_ = keys_.count();
      is_opened_ = true;
    }
  }
  return ret;
}

int ObStorageHADiagIterator::get_next_info(ObStorageHADiagInfo &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (cur_idx_ >= cnt_) {
    ret = OB_ITER_END;
  } else if (cnt_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array count", K(ret), K(cnt_));
  } else {
    ObStorageHADiagMgr *mgr = nullptr;
    if (OB_ISNULL(mgr = MTL(ObStorageHADiagMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObStorageHADiagMgr from MTL", K(ret));
    } else {
      while (OB_SUCC(ret) && cur_idx_ <= cnt_) {
        if (cur_idx_ == cnt_) {
          ret = OB_ITER_END;
          break;
        } else if (OB_FAIL(mgr->get(keys_.at(cur_idx_++), info))) {
          //overwrite ret
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("info has already been cleaned", K(ret), "key", keys_.at(cur_idx_ - 1));
          } else {
            LOG_WARN("fail to get info", K(ret), "key", keys_.at(cur_idx_ - 1));
          }
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

bool ObStorageHADiagIterator::is_valid() const
{
  return cur_idx_ >= 0 && cnt_ >= 0 && !keys_.empty();
}

void ObStorageHADiagIterator::reset()
{
  is_opened_= false;
  keys_.reset();
  cnt_ = 0;
  cur_idx_ = 0;
}

int ObStorageHADiagIterator::get_cur_key(ObStorageHADiagTaskKey &key) const
{
  int ret = OB_SUCCESS;
  key.reset();
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (0 == keys_.count() || cur_idx_ >= cnt_) {
      ret = OB_ITER_END;
    } else {
      key = keys_.at(cur_idx_);
      if (!key.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("key is invalid", K(ret), K(key));
      }
    }
  }
  return ret;
}

bool ObStorageHADiagIterator::is_opened() const
{
  return is_opened_;
}
/**
 * ------------------------------ObStorageHADiagMgr---------------------
 */
ObStorageHADiagMgr::ObStorageHADiagMgr()
  : is_inited_(false),
    allocator_(),
    task_list_(),
    service_(nullptr),
    lock_()
{
}

int ObStorageHADiagMgr::mtl_init(ObStorageHADiagMgr *&storage_ha_diag_mgr)
{
  return storage_ha_diag_mgr->init(MTL_ID(), INFO_PAGE_SIZE, INFO_MAX_SIZE);
}

int ObStorageHADiagMgr::init(
        const uint64_t tenant_id,
        const int64_t page_size,
        const int64_t max_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObStorageHADiagMgr has already been inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
    || page_size <= 0
    || max_size <= 0
    || max_size < page_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(page_size), K(max_size));
  } else {
    int64_t upper_align_size = upper_align(max_size, page_size);
    if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(),
                                    page_size,
                                    lib::ObMemAttr(tenant_id, "SHADiagMgr"),
                                    0/*init_size*/,
                                    upper_align_size,
                                    upper_align_size))) {
      LOG_WARN("failed to init allocator", K(ret), K(page_size), K(tenant_id), K(upper_align_size));
    }
  }
  if (OB_SUCC(ret)) {
    service_ = &(ObStorageHADiagService::instance());
    is_inited_ = true;
  } else {
    clear();
  }
  return ret;
}

void ObStorageHADiagMgr::clear()
{
  is_inited_ = false;
  allocator_.reset();
  task_list_.reset();
  service_ = nullptr;
}

void ObStorageHADiagMgr::destroy()
{
  is_inited_ = false;
  common::SpinWLockGuard guard(lock_);
  clear_with_no_lock_();
  allocator_.reset();
  service_ = nullptr;
}

void ObStorageHADiagMgr::clear_with_no_lock_()
{
  DLIST_FOREACH_REMOVESAFE_NORET(iter, task_list_) {
    ObStorageHADiagTask *tmp_task = task_list_.remove(iter);
    free_(tmp_task);
  }
  task_list_.clear();
}

int ObStorageHADiagMgr::add_or_update(
    const ObStorageHADiagTaskKey &task_key,
    const ObStorageHADiagInfo &input_info,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  ObStorageHADiagInfo *info = nullptr;
  ObStorageHADiagTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!task_key.is_valid() || !input_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(input_info));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(get_task_(task_key, info))) {
      //overwrite ret
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(add_task_(task_key, input_info, task))) {
          LOG_WARN("failed to add task", K(ret), K(task_key), K(input_info));
        }
      } else {
        LOG_WARN("failed to check task exist", K(ret), K(task_key));
      }
    } else {
      if (OB_FAIL(info->update(input_info))) {
        LOG_WARN("failed to update info", K(ret), K(input_info));
      }
    }
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (is_report) {
    if (OB_FAIL(service_->add_task(task_key))) {
      LOG_WARN("failed to add task to service", K(ret), K(task_key));
    } else if (REACH_TENANT_TIME_INTERVAL(1 * 100 * 1000L)) {
      service_->wakeup();
    }
  }
  return ret;
}

int ObStorageHADiagMgr::add_task_(
    const ObStorageHADiagTaskKey &task_key,
    const ObStorageHADiagInfo &input_info,
    ObStorageHADiagTask *&out_task)
{
  int ret = OB_SUCCESS;
  out_task = nullptr;
  if (!task_key.is_valid() || !input_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(input_info));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(create_task_(task_key, input_info, out_task))) {
        if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          // overwrite ret
          if (!task_list_.is_empty() && OB_FAIL(purge_task_(task_list_.get_first()))) {
            LOG_WARN("failed to purge task", K(ret), K(task_list_));
          }
        } else {
          LOG_WARN("failed to create task", K(ret), K(task_key), K(input_info));
        }
      } else {
        break;
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (!task_list_.add_last(out_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add list", K(ret), KPC(out_task));
    }
  }
  return ret;
}
//TODO(zhixing.yh) modify task key relationship with info, info include task key
int ObStorageHADiagMgr::create_task_(
    const ObStorageHADiagTaskKey &task_key,
    const ObStorageHADiagInfo &input_info,
    ObStorageHADiagTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  void *buf = nullptr;
  ObStorageHADiagInfo *info = nullptr;
  ObStorageHADiagTask *tmp_task = nullptr;
  if (!task_key.is_valid() || !input_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(input_info));
  } else if (OB_FAIL(input_info.deep_copy(allocator_, info))) {
    LOG_WARN("failed to alloc memory for diagnose info", K(ret), K(input_info));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageHADiagTask)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for ObStorageHADiagTask failed", K(ret));
  } else if (OB_ISNULL(tmp_task = new (buf) ObStorageHADiagTask())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new storage ha monitor info is nullptr", K(ret));
  } else {
    buf = nullptr;
    if (OB_FAIL(tmp_task->key_.assign(task_key))) {
      LOG_WARN("failed to assign task key", K(ret), K(task_key));
    } else {
      tmp_task->val_ = info;
      info->task_ = tmp_task;
      info = nullptr;
    }
    if (OB_NOT_NULL(info)) {
      tmp_task->~ObStorageHADiagTask();
      allocator_.free(tmp_task);
      tmp_task = nullptr;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(info)) {
      free_info_(info);
    }
    if (OB_NOT_NULL(buf)) {
      allocator_.free(buf);
      buf = nullptr;
    }
  } else {
    task = tmp_task;
  }

  return ret;
}

int ObStorageHADiagMgr::purge_task_(ObStorageHADiagTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is nullptr", K(ret));
  } else {
    ObStorageHADiagTask *tmp_task = task_list_.remove(task);
    free_(tmp_task);
  }
  return ret;
}

int ObStorageHADiagMgr::del(const ObStorageHADiagTaskKey &task_key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!task_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task key is invalid", K(ret), K(task_key));
  } else {
    common::SpinWLockGuard guard(lock_);
    DLIST_FOREACH_X(iter, task_list_, OB_SUCC(ret)) {
      if (iter->key_ == task_key) {
        if (OB_FAIL(purge_task_(iter))) {
          LOG_WARN("failed to purge task", K(ret), K(task_key));
        }
        break;
      }
    }
  }

  return ret;
}

int ObStorageHADiagMgr::get_iter_array(
    const ObStorageHADiagType &type,
    ObIArray<ObStorageHADiagTaskKey> &array) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (type < ObStorageHADiagType::ERROR_DIAGNOSE
             || type >= ObStorageHADiagType::MAX_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else {
    common::SpinRLockGuard guard(lock_);
    DLIST_FOREACH_X(it, task_list_, OB_SUCC(ret)) {
      if (type == it->key_.diag_type_) {
        if (OB_FAIL(array.push_back(it->key_))) {
          LOG_WARN("failed to push back array.", K(ret), K(it->key_));
        }
      }
    }
  }
  return ret;
}

int ObStorageHADiagMgr::get(const ObStorageHADiagTaskKey &key, ObStorageHADiagInfo &info) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(copy_with_nolock_(key, info))) {
      LOG_WARN("failed to copy info", K(ret), K(key));
    }
  }
  return ret;
}

int ObStorageHADiagMgr::get_task_(
    const ObStorageHADiagTaskKey &key,
    ObStorageHADiagInfo *&info) const
{
  int ret = OB_SUCCESS;
  info = nullptr;
  bool found_info = false;
  DLIST_FOREACH_X(iter, task_list_, OB_SUCC(ret)) {
    if (iter->key_ == key) {
      info = iter->val_;
      found_info = true;
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (!found_info) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObStorageHADiagMgr::copy_with_nolock_(const ObStorageHADiagTaskKey &key, ObStorageHADiagInfo &info) const
{
  int ret = OB_SUCCESS;
  ObStorageHADiagInfo *task_info = nullptr;
  if (OB_FAIL(get_task_(key, task_info))) {
    LOG_WARN("fail to check task exist", K(ret), K(key));
  } else if (OB_FAIL(info.assign(*task_info))) {
    LOG_WARN("fail to copy info", K(ret), KPC(task_info));
  } else if (ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE == key.module_
      && OB_FAIL(static_cast<ObTransferPerfDiagInfo &>(info).copy_item_list(
      static_cast<ObTransferPerfDiagInfo &>(*task_info)))) {
    LOG_WARN("fail to copy item list to info", K(ret), KPC(task_info));
  }
  return ret;
}

void ObStorageHADiagMgr::free_info_(ObStorageHADiagInfo *&info)
{
  if (OB_NOT_NULL(info)) {
    info->~ObStorageHADiagInfo();
    allocator_.free(info);
    info = nullptr;
  }
}

void ObStorageHADiagMgr::free_task_(ObStorageHADiagTask *&task)
{
  if (OB_NOT_NULL(task)) {
    task->~ObStorageHADiagTask();
    allocator_.free(task);
    task = nullptr;
  }
}

void ObStorageHADiagMgr::free_(ObStorageHADiagTask *&task)
{
  if (OB_NOT_NULL(task)) {
    free_info_(task->val_);
    free_task_(task);
  }
}
//TODO(zhixing.yh) modify task key relationship with info, info include task key
//can not reset info, perf diagnose info perhaps already init
int ObStorageHADiagMgr::construct_diagnose_info(
    const share::ObTransferTaskID task_id, const share::ObLSID &ls_id,
    const share::ObStorageHADiagTaskType type, const int64_t retry_id,
    const int result_code, const share::ObStorageHADiagModule module,
    share::ObStorageHADiagInfo &info)
{
  int ret = OB_SUCCESS;
  if (!task_id.is_valid()
      || !ls_id.is_valid()
      || type >= ObStorageHADiagTaskType::MAX_TYPE
      || type < ObStorageHADiagTaskType::TRANSFER_START
      || module >= ObStorageHADiagModule::MAX_MODULE
      || module < ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE
      || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(ls_id), K(type), K(module), K(retry_id), K(result_code));
  } else {
    info.task_id_ = task_id.id();
    info.ls_id_ = ls_id;
    info.module_ = module;
    info.type_ = type;
    info.retry_id_ = retry_id;
    info.result_code_ = result_code;
  }
  return ret;
}

int ObStorageHADiagMgr::construct_error_diagnose_info(share::ObTransferErrorDiagInfo &info)
{
  int ret = OB_SUCCESS;
  info.timestamp_ = ObTimeUtility::current_time();
  info.trace_id_ = *ObCurTraceId::get_trace_id();
  info.thread_id_ = GETTID();
  return ret;
}

int ObStorageHADiagMgr::construct_diagnose_info_key(
    const share::ObTransferTaskID task_id,
    const share::ObStorageHADiagModule module,
    const share::ObStorageHADiagTaskType type,
    const share::ObStorageHADiagType diag_type,
    const int64_t retry_id,
    const common::ObTabletID &tablet_id,
    share::ObStorageHADiagTaskKey &key)
{
  int ret = OB_SUCCESS;
  key.reset();
  // tablet_id do not check, it is only effective in backfill
  if (!task_id.is_valid()
      || module >= ObStorageHADiagModule::MAX_MODULE
      || module < ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE
      || type >= ObStorageHADiagTaskType::MAX_TYPE
      || type < ObStorageHADiagTaskType::TRANSFER_START
      || diag_type >= ObStorageHADiagType::MAX_TYPE
      || diag_type < ObStorageHADiagType::ERROR_DIAGNOSE
      || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(module), K(type),
        K(diag_type), K(retry_id));
  } else {
    key.tenant_id_ = MTL_ID();
    key.task_id_ = task_id.id();
    key.module_ = module;
    key.type_ = type;
    key.retry_id_ = retry_id;
    key.diag_type_ = diag_type;
    key.tablet_id_ = tablet_id;
  }
  return ret;
}

//TODO(zhixing.yh) add result_msg
int ObStorageHADiagMgr::add_info_to_mgr(
    const ObStorageHADiagInfo &info,
    const ObStorageHADiagTaskKey &key,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  ObStorageHADiagMgr *mgr = nullptr;
  if (!info.is_valid() || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info), K(key));
  } else if (OB_ISNULL(mgr = MTL(ObStorageHADiagMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get ObStorageHADiagMgr from mtl", K(ret));
  } else if (OB_FAIL(mgr->add_or_update(key, info, is_report))) {
    STORAGE_LOG(WARN, "failed to add transfer diag info", K(ret), K(key), K(info));
  } else {
    STORAGE_LOG(DEBUG, "success to add transfer diag info", K(ret), K(key), K(info));
  }
  return ret;
}

int ObStorageHADiagMgr::append_perf_diagnose_info(
    const common::ObTabletID &tablet_id,
    share::ObStorageHADiagInfo &info)
{
  int ret = OB_SUCCESS;
  //do not check tablet_id invalid
  info.tablet_id_ = tablet_id;
  return ret;
}

int ObStorageHADiagMgr::append_error_diagnose_info(
    const common::ObTabletID &tablet_id,
    const share::ObStorageHACostItemName result_msg,
    share::ObStorageHADiagInfo &info)
{
  int ret = OB_SUCCESS;
  // MAX_NAME also is valid
  if (result_msg < share::ObStorageHACostItemName::TRANSFER_START_BEGIN
      || result_msg > share::ObStorageHACostItemName::MAX_NAME) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(result_msg));
  } else {
    //do not check tablet_id invalid
    info.result_msg_ = result_msg;
    info.tablet_id_ = tablet_id;
  }
  return ret;
}

int ObStorageHADiagMgr::add_transfer_error_diagnose_info(
    const share::ObTransferTaskID task_id, const share::ObLSID &ls_id,
    const share::ObStorageHADiagTaskType type, const int64_t retry_id,
    const int result_code, const common::ObTabletID &tablet_id, const share::ObStorageHACostItemName result_msg)
{
  int ret = OB_SUCCESS;
  share::ObTransferErrorDiagInfo info;
  share::ObStorageHADiagTaskKey key;
  if (!task_id.is_valid()
      || !ls_id.is_valid()
      || type >= ObStorageHADiagTaskType::MAX_TYPE
      || type < ObStorageHADiagTaskType::TRANSFER_START
      || retry_id < 0
      || OB_SUCCESS == result_code) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(ls_id), K(type), K(retry_id), K(result_code));
  } else if (OB_FAIL(construct_diagnose_info(task_id, ls_id, type, retry_id, result_code,
      ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE, info))) {
    LOG_WARN("failed to construct error diagnose info", K(ret), K(task_id), K(ls_id), K(type), K(retry_id), K(result_code));
  } else if (OB_FAIL(append_error_diagnose_info(tablet_id, result_msg, info))) {
    LOG_WARN("fail to append diagnose info", K(ret), K(tablet_id), K(result_msg));
  } else if (OB_FAIL(construct_error_diagnose_info(info))) {
    LOG_WARN("failed to construct error diagnose info", K(ret));
  } else if (OB_FAIL(construct_diagnose_info_key(task_id, ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE,
      type, ObStorageHADiagType::ERROR_DIAGNOSE, retry_id, tablet_id, key))) {
    LOG_WARN("failed to construct error diagnose info key", K(ret), K(task_id), K(type), K(retry_id), K(tablet_id));
  } else if (OB_FAIL(add_info_to_mgr(info, key, true/*is_report*/))) {
    LOG_WARN("failed to add error diagnose info", K(ret), K(info), K(key));
  }
  return ret;
}

int ObStorageHADiagMgr::add_transfer_error_diagnose_info(
    const share::ObTransferTaskID task_id, const share::ObLSID &ls_id,
    const share::ObStorageHADiagTaskType type, const int64_t retry_id,
    const int result_code, const share::ObStorageHACostItemName result_msg)
{
  int ret = OB_SUCCESS;
  common::ObTabletID unused_tablet_id;
  if (OB_FAIL(add_transfer_error_diagnose_info(task_id, ls_id, type, retry_id,
      result_code, unused_tablet_id, result_msg))) {
    LOG_WARN("fail to add transfer error diagnose info", K(ret), K(task_id), K(ls_id), K(type),
        K(retry_id), K(result_code), K(unused_tablet_id), K(result_msg));
  }
  return ret;
}

int ObStorageHADiagMgr::construct_perf_diagnose_info(
    const int64_t tablet_count,
    const int64_t start_timestamp,
    share::ObTransferPerfDiagInfo &info)
{
  int ret = OB_SUCCESS;
  if (start_timestamp < 0 || tablet_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_timestamp));
  } else {
    info.timestamp_ = start_timestamp;
    info.end_timestamp_ = ObTimeUtility::current_time();
    info.tablet_count_ = tablet_count;
  }
  return ret;
}

int ObStorageHADiagMgr::add_transfer_perf_diagnose_info(
    const share::ObStorageHADiagTaskKey &key,
    const int64_t start_timestamp, const int64_t tablet_count,
    const bool is_report, share::ObTransferPerfDiagInfo &info)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid() || start_timestamp < 0 || tablet_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(start_timestamp), K(tablet_count));
  } else if (OB_FAIL(construct_perf_diagnose_info(tablet_count, start_timestamp, info))) {
    LOG_WARN("failed to construct perf diagnose info", K(ret), K(tablet_count), K(start_timestamp));
  } else if (OB_FAIL(add_info_to_mgr(info, key, is_report))) {
    LOG_WARN("failed to add perf diagnose info", K(ret), K(info), K(key), K(is_report));
  }
  return ret;
}

int ObStorageHADiagMgr::add_key_to_service_(const ObIArray<ObStorageHADiagTaskKey> &task_keys)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_keys.count(); i++) {
    const ObStorageHADiagTaskKey &key = task_keys.at(i);
    if (OB_FAIL(service_->add_task(key))) {
      LOG_WARN("failed to add task to service", K(ret), K(key));
    }
  }
  return ret;
}

int ObStorageHADiagMgr::add_task_key_to_report_array_(
    const share::ObTransferTaskID &task_id,
    ObIArray<ObStorageHADiagTaskKey> &task_keys) const
{
  int ret = OB_SUCCESS;
  task_keys.reset();
  if (!task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id));
  } else {
    common::SpinRLockGuard guard(lock_);
    DLIST_FOREACH_X(iter, task_list_, OB_SUCC(ret)) {
      if ((ObStorageHADiagType::PERF_DIAGNOSE == iter->key_.diag_type_
          && iter->key_.task_id_ == task_id.id()
          && (ObStorageHADiagTaskType::TRANSFER_START == iter->key_.type_
          || ObStorageHADiagTaskType::TRANSFER_DOING == iter->key_.type_
          || ObStorageHADiagTaskType::TRANSFER_ABORT == iter->key_.type_))) {
        if (OB_FAIL(task_keys.push_back(iter->key_))) {
          LOG_WARN("failed to add task", K(ret), K(iter->key_));
        }
      }
    }
  }
  return ret;
}

int ObStorageHADiagMgr::report_task(const share::ObTransferTaskID &task_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObStorageHADiagTaskKey> task_keys;
  if (!task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id));
  } else if (OB_FAIL(add_task_key_to_report_array_(task_id, task_keys))) {
    LOG_WARN("failed to add task to report array", K(ret), K(task_id));
  } else if (OB_FAIL(add_key_to_service_(task_keys))) {
    LOG_WARN("failed to add task to service", K(ret), K(task_keys));
  }
  return ret;
}

}
}
