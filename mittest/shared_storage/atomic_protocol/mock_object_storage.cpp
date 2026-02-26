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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST

#include "mock_object_storage.h"
#include "test_ss_atomic_util.h"
#include <string.h>

namespace oceanbase
{

namespace blocksstable
{
int ObObjectManager::read_object(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  bool is_init = ATOMIC_LOAD(&OB_MOCK_OBJ_SRORAGE.tenant_) != NULL;
  if (is_init) {
    return OB_MOCK_OBJ_SRORAGE.read_object(read_info, object_handle);
  } else {
    if (OB_FAIL(ObObjectManager::async_read_object(read_info, object_handle))) {
      LOG_WARN("fail to sync read object", K(ret), K(read_info));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("Fail to wait io finish", K(ret), K(read_info));
    }
  }
  return ret;
}

int ObObjectManager::write_object(
    const ObStorageObjectOpt &opt,
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  bool is_init = ATOMIC_LOAD(&OB_MOCK_OBJ_SRORAGE.tenant_) != NULL;
  if (is_init) {
    return OB_MOCK_OBJ_SRORAGE.write_object(opt, const_cast<ObStorageObjectWriteInfo&>(write_info), object_handle);
  } else {
    if (OB_FAIL(ObObjectManager::async_write_object(opt, write_info, object_handle))) {
      LOG_WARN("fail to sync write block", K(ret), K(write_info), K(object_handle));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("fail to wait io finish", K(ret), K(write_info));
    }
  }
  return ret;
}
} // blocksstable


namespace storage
{
static int64_t lease_epoch = 1;

void mock_switch_sswriter()
{
  ATOMIC_INC(&lease_epoch);
  LOG_INFO("mock switch sswriter", K(lease_epoch));
}

int ObSSWriterService::check_lease(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  is_sswriter = true;
  epoch = ATOMIC_LOAD(&lease_epoch);
  return OB_SUCCESS;
}

unordered_map<ObInjectErrKey, ObInjectedErr*> ObMockObjectManager::inject_err_map;
ObLinkQueue ObMockObjectManager::req_queue_;

int ObMockObjectManager::mtl_init(ObMockObjectManager *&mock_obj_mgr)
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObMockObjectManager mtl init");
  return mock_obj_mgr->init();
}

int ObMockObjectManager::init()
{
  int ret = OB_SUCCESS;
  LOG_INFO("MockObjectManager.init", K(ret));
  return ret;
}

int ObMockObjectManager::start()
{
  int ret = OB_SUCCESS;
  ATOMIC_SET(&stop_, false);
  LOG_INFO("MockObjectManager.start", K(ret));
  return ret;
}

void ObMockObjectManager::stop()
{
  int ret = OB_SUCCESS;
  ATOMIC_SET(&stop_, true);
  LOG_INFO("MockObjectManager.stop");
}

void ObMockObjectManager::destroy() {}

void ObMockObjectManager::run1()
{
  // lib::set_thread_name("MockObjMgr");
  // LOG_INFO("req consumer run!", KPC(this));
  // while (!has_set_stop()) {
  //   ObLink *e = NULL;
  //   req_queue_.pop(e);
  //   if (e) {
  //     process_ss_req_(static_cast<ObMockSSReq*>(e));
  //   } else if (ATOMIC_BCAS(&is_sleeping_, false, true)) {
  //     LOG_INFO("to sleeping", KPC(this));
  //     auto key = sleep_cond_.get_key();
  //     sleep_cond_.wait(key, 2000);
  //     LOG_INFO("wakeup", KPC(this));
  //   }
  // }
}

int ObMockObjectManager::read_object(
  const ObStorageObjectReadInfo &read_info,
  ObStorageObjectHandle &object_handle)
{
  // 1. push to request queue
  // 2. wait for request finish
  int ret = OB_SUCCESS;
  ATOMIC_INC(&read_cnt_);
  void *read_req_ptr = ob_malloc(sizeof(ObMockSSReadReq), ObNewModIds::TEST);
  ObStorageObjectReadInfo *my_read_info_ptr = (ObStorageObjectReadInfo*)ob_malloc(sizeof(ObStorageObjectReadInfo), ObNewModIds::TEST);
  ObStorageObjectReadInfo *my_read_info = new(my_read_info_ptr) ObStorageObjectReadInfo;
  ObStorageObjectHandle *my_handler_ptr = (ObStorageObjectHandle*)ob_malloc(sizeof(ObStorageObjectHandle), ObNewModIds::TEST);
  ObStorageObjectHandle *my_handler = new(my_handler_ptr) ObStorageObjectHandle;
  inc_alloc_cnt();
  *my_read_info = read_info;
  my_read_info->buf_ = (char*)ob_malloc(read_info.size_, ObNewModIds::TEST);
  my_read_info->size_ = read_info.size_;
  ObMockSSReadReq *read_req = nullptr;
  bool need_wait = true;
  bool need_free = false;
  if (nullptr == read_req_ptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc read req memery", K(ret), K(read_info));
  } else if (FALSE_IT(read_req = new(read_req_ptr) ObMockSSReadReq(my_read_info, my_handler))) {
  } else if (FALSE_IT(inject_err_(read_req))) {
  } else {
    if (!read_req->injected_err_->is_timeout_) {
      read_req->read_info_ = &read_info;
      read_req->object_handle_ = &object_handle;
      need_free = true;
    }
    if (OB_FAIL(handle_and_push_req_(read_req, need_wait))) {
      LOG_WARN("Fail to wait io finish", K(ret), KPC(read_req));
    } else if (need_wait) {
      int cnt = 0;
      while (ATOMIC_LOAD(&read_req->has_done_) != true) {
        cnt++;
        // not consumered by queue
        ob_usleep(100);
        if (cnt % 1000 == 0) {
          LOG_WARN("wait too much time", K(ret), KPC(read_req));
        }
      }
      ret = read_req->ret_;
      if (OB_FAIL(ret)) {
        LOG_WARN("Fail to do read req", K(ret), KPC(read_req));
      } else {
        LOG_INFO("wait io finish", K(ret), KPC(read_req),
          KP(read_req->read_info_), KP(my_read_info), KP(&read_info), KP(read_req->object_handle_), KP(&object_handle), KP(my_read_info->buf_));
      }
    } else {
      LOG_INFO("no need to wait io finish", K(ret), KPC(read_req), K(read_info), KPC(my_handler));
    }
  }
  if (need_free) {
    inc_free_cnt();
    ob_free(read_req->injected_err_);
    ob_free(read_req_ptr);
    ob_free(my_read_info->buf_);
    ob_free(my_read_info);
    ob_free(my_handler);
  }
  return ret;
}

int ObMockObjectManager::write_object(
  const ObStorageObjectOpt &opt,
  ObStorageObjectWriteInfo &write_info,
  ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ATOMIC_INC(&write_cnt_);
  void *write_req_ptr = ob_malloc(sizeof(ObMockSSWriteReq), ObNewModIds::TEST);
  ObStorageObjectOpt *my_opt_ptr = (ObStorageObjectOpt*)ob_malloc(sizeof(ObStorageObjectOpt), ObNewModIds::TEST);
  ObStorageObjectOpt *my_opt = new(my_opt_ptr) ObStorageObjectOpt;
  ObStorageObjectWriteInfo *my_write_info_ptr = (ObStorageObjectWriteInfo*)ob_malloc(sizeof(ObStorageObjectWriteInfo), ObNewModIds::TEST);
  ObStorageObjectWriteInfo *my_write_info = new(my_write_info_ptr) ObStorageObjectWriteInfo;
  ObStorageObjectHandle *my_handler_ptr = (ObStorageObjectHandle*)ob_malloc(sizeof(ObStorageObjectHandle), ObNewModIds::TEST);
  ObStorageObjectHandle *my_handler = new(my_handler_ptr) ObStorageObjectHandle;
  inc_alloc_cnt();
  *my_opt = opt;
  *my_write_info = write_info;
  char *tmp_buf = (char*)ob_malloc(write_info.size_, ObNewModIds::TEST);
  my_write_info->buffer_ = tmp_buf;
  memcpy(tmp_buf, write_info.buffer_, write_info.size_);
  my_write_info->size_ = write_info.size_;
  ObMockSSWriteReq *write_req = nullptr;
  bool need_wait = true;
  bool need_free = false;
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(write_info));
  } else if (nullptr == write_req_ptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc write req memery", K(ret), K(opt), K(write_info));
  } else if (FALSE_IT(write_req = new(write_req_ptr) ObMockSSWriteReq(my_opt, my_write_info, my_handler))) {
  } else if (FALSE_IT(inject_err_(write_req))) {
  } else {
    if (!write_req->injected_err_->is_timeout_) {
      write_req->opt_ = &opt;
      write_req->write_info_ = &write_info;
      write_req->object_handle_ = &object_handle;
      need_free = true;
    }
    if (OB_FAIL(handle_and_push_req_(write_req, need_wait))) {
      LOG_WARN("Fail to wait io finish", K(ret), K(write_req));
    } else if (need_wait) {
      need_free = true;
      int cnt = 0;
      LOG_INFO("need to wait io finish", K(ret), KPC(write_req), K(opt), K(write_info), KPC(my_handler));
      while (ATOMIC_LOAD(&write_req->has_done_) != true) {
        cnt++;
        // not consumered by queue
        ob_usleep(100);
        if (cnt % 1000 == 0) {
          LOG_WARN("wait too much time", K(ret), KPC(write_req));
        }
      }
      ret = write_req->ret_;
      if (OB_FAIL(ret)) {
        LOG_WARN("Fail to do write req", K(ret), KPC(write_req));
      } else {
        LOG_INFO("wait io finish", K(ret), KPC(write_req));
      }
    } else {
      LOG_INFO("no need to wait io finish", K(ret), KPC(write_req), K(opt), K(write_info), KPC(my_handler));
    }
  }
  if (need_free) {
    inc_free_cnt();
    ob_free(tmp_buf);
    ob_free(my_opt);
    ob_free(my_write_info);
    ob_free(my_handler);
    ob_free(write_req->injected_err_);
    ob_free(write_req_ptr);
  }
  return ret;
}

void ObMockObjectManager::inject_err_to_object_type(ObInjectErrKey err_key,
                                                    ObInjectedErr *err)
{
  ObLatchWGuard guard(map_lock_, common::ObLatchIds::DEFAULT_SPIN_RWLOCK);
  inject_err_map[err_key] = err;
  LOG_INFO("inject err to object type", K(err_key), KPC(err));
  if (inject_err_map.find(err_key) != inject_err_map.end()) {
    LOG_INFO("inject err", K(err_key), KPC(err));
  }
}

// private fun
int ObMockObjectManager::inject_err_(ObMockSSReq *req)
{
  int ret = OB_SUCCESS;
  ObInjectErrKey err_key;
  ObStorageObjectType obj_type = ObStorageObjectType::MAX;
  if (ObMockSSReqType::WRITE_REQ == req->type_) {
    ObMockSSWriteReq *w_req = static_cast<ObMockSSWriteReq*>(req);
    obj_type = w_req->opt_->object_type_;
  } else if (ObMockSSReqType::READ_REQ == req->type_) {
    ObMockSSReadReq *r_req = static_cast<ObMockSSReadReq*>(req);
    obj_type = static_cast<ObStorageObjectType>(r_req->read_info_->macro_block_id_.storage_object_type_);
  }
  if (process_point_for_write_) {
    if (ObMockSSReqType::WRITE_REQ == req->type_) {
      ++process_point_;
      LOG_INFO("inc point for write", K(err_key), K(process_point_));
    }
  } else {
    ++process_point_;
    LOG_INFO("inc point for total", K(err_key), K(process_point_));
  }
  err_key = process_point_;
  if (inject_err_map.find(err_key) != inject_err_map.end()) {
    ObLatchWGuard guard(map_lock_, common::ObLatchIds::DEFAULT_SPIN_RWLOCK);
    ObInjectedErr *injected_err = inject_err_map[err_key];
    LOG_INFO("find injected err", K(err_key), KPC(injected_err));
    req->injected_err_ = inject_err_map[err_key];
    inject_err_map.erase(err_key);
  } else {
    ObInjectedErr *ptr = (ObInjectedErr*)ob_malloc(sizeof(ObInjectedErr), ObNewModIds::TEST);
    ObInjectedErr *injected_err = new(ptr) ObInjectedErr();
    req->injected_err_ = injected_err;
  }
  LOG_INFO("handle req and inject err", K(err_key), KPC(req));
  return ret;
}

int ObMockObjectManager::handle_and_push_req_(ObMockSSReq *req, bool &need_wait)
{
  int ret = OB_SUCCESS;
  LOG_INFO("handle and push req", KPC(req), KP(req));
  need_wait = true;
  if (req->injected_err_->is_sswriter_change_) {
    mock_switch_sswriter();
    LOG_INFO("inject switch sswriter", KPC(req));
  }
  if (req->injected_err_->is_succ_) {
    if (OB_FAIL(push_to_queue_(req))) {
      LOG_WARN("Fail to wait io finish", K(ret), KPC(req));
    }
  }
  if (req->injected_err_->is_timeout_) {
    LOG_INFO("inject req timeout", KPC(req->injected_err_));
    need_wait = false;
    // if (ObMockSSReqType::WRITE_REQ == req->type_) {
    //   ObMockSSWriteReq *w_req = static_cast<ObMockSSWriteReq*>(req);
    //   ob_usleep(1000);
    // } else if (ObMockSSReqType::READ_REQ == req->type_) {
    //   ObMockSSReadReq *r_req = static_cast<ObMockSSReadReq*>(req);
    //   ob_usleep(1000);
    // }
    ret = OB_TIMEOUT;
  }
  return ret;
}

int ObMockObjectManager::push_to_queue_(ObMockSSReq *req)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(req_queue_.push(req))) {
    LOG_WARN("Fail to wait io finish", K(ret), K(req));
  } else if (ATOMIC_BCAS(&is_sleeping_, true, false)) {
    sleep_cond_.signal();
  }
  return ret;
}


void ObMockObjectManager::process_ss_req_(ObMockSSReq *req)
{
  int ret = OB_SUCCESS;
  int64_t curr_ts = ObTimeUtility::current_time();
  if (req->injected_err_->is_shuffled_) {
    // shuffle the request
    // 1. re-push the request to the queue
    req->injected_err_->is_shuffled_ = false;
    LOG_INFO("shuffle the request", K(ret), KPC(req));
    push_to_queue_(req);
  } else if (req->injected_err_->get_need_wait()
          && ((curr_ts - req->init_ts_) <= 2 * 1000 * 1000)
          && !stop_) {
    LOG_INFO("the request need wait to process", K(ret), KPC(req->injected_err_));
    push_to_queue_(req);
  } else if (req->injected_err_->is_need_wake_waiting_) {
    req->injected_err_->need_wakeup_err_->reset_need_wait();
    req->injected_err_->is_need_wake_waiting_ = false;
    LOG_INFO("the request wake up anthoer request", K(ret), KP(req));
    push_to_queue_(req);
  } else if (ObMockSSReqType::WRITE_REQ == req->type_) {
    ObMockSSWriteReq *w_req = static_cast<ObMockSSWriteReq*>(req);
    LOG_INFO("handle write req", K(ret), KPC(w_req));
    if (OB_FAIL(ObObjectManager::async_write_object(*w_req->opt_, *w_req->write_info_, *w_req->object_handle_))) {
      LOG_WARN("fail to sync write block", K(ret), KPC(w_req));
    } else if (OB_FAIL(w_req->object_handle_->wait())) {
      LOG_WARN("Fail to wait io finish", K(ret), KPC(w_req));
    }
    ATOMIC_SET(&req->ret_, ret);
    ATOMIC_SET(&req->has_done_, true);
    if (req->injected_err_->is_timeout_) {
      // no need wait, should help it free
      inc_free_cnt();
      w_req->opt_->~ObStorageObjectOpt();
      ob_free((void*)w_req->opt_);
      ob_free((void*)w_req->write_info_->buffer_);
      w_req->write_info_->~ObStorageObjectWriteInfo();
      ob_free((void*)w_req->write_info_);
      w_req->object_handle_->~ObStorageObjectHandle();
      ob_free((void*)w_req->object_handle_);
      ob_free(req->injected_err_);
      ob_free(req);
    }
  } else if (ObMockSSReqType::READ_REQ == req->type_) {
    ObMockSSReadReq *r_req = static_cast<ObMockSSReadReq*>(req);
    LOG_INFO("handle read req", K(ret), KP(r_req), KP(r_req->read_info_));
    if (OB_FAIL(ObObjectManager::async_read_object(*r_req->read_info_, *r_req->object_handle_))) {
      LOG_WARN("fail to sync read object", K(ret), KPC(r_req));
    } else if (OB_FAIL(r_req->object_handle_->wait())) {
      LOG_WARN("Fail to wait io finish", K(ret), KPC(r_req));
    }
    ATOMIC_SET(&req->ret_, ret);
    ATOMIC_SET(&req->has_done_, true);
    if (req->injected_err_->is_timeout_) {
      // no need wait, should help it free
      inc_free_cnt();
      ob_free((void*)(r_req->read_info_->buf_));
      r_req->read_info_->~ObStorageObjectReadInfo();
      ob_free((void*)r_req->read_info_);
      r_req->object_handle_->~ObStorageObjectHandle();
      ob_free((void*)req->object_handle_);
      ob_free(req->injected_err_);
      ob_free(req);
    }
  }
}

}  // namespace storage
}  // namespace oceanbase
#endif