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

#include "ob_ddl_redo_log_writer.h"
#include "logservice/archiveservice/ob_archive_service.h"
#include "logservice/ob_log_handler.h"
#include "logservice/ob_log_service.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tablet/ob_tablet.h"
#include "rootserver/ddl_task/ob_ddl_task.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::archive;
using namespace oceanbase::blocksstable;
using namespace oceanbase::logservice;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

int ObDDLCtrlSpeedItem::init(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", K(ret), K(ls_id));
  } else {
    ls_id_ = ls_id;
    next_available_write_ts_ = ObTimeUtility::current_time();
    if (OB_FAIL(refresh())) {
      LOG_WARN("fail to init write speed and clog disk used threshold", K(ret));
    } else {
      is_inited_ = true;
      LOG_INFO("succeed to init ObDDLCtrlSpeedItem", K(ret), K(is_inited_), K(ls_id_),
        K(next_available_write_ts_), K(write_speed_), K(disk_used_stop_write_threshold_));
    }
  }
  return ret;
}

// refrese ddl clog write speed and disk used threshold on tenant level.
int ObDDLCtrlSpeedItem::refresh()
{
  int ret = OB_SUCCESS;
  int64_t archive_speed = 0;
  int64_t refresh_speed = 0;
  bool ignore = false;
  bool force_wait = false;
  int64_t total_used_space = 0; // for current tenant, used bytes.
  int64_t total_disk_space = 0; // for current tenant, limit used bytes.
  ObLSHandle ls_handle;
  palf::PalfOptions palf_opt;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
  ObArchiveService *archive_service = MTL(ObArchiveService*);
  if (OB_ISNULL(log_service) || OB_ISNULL(archive_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, nullptr found", K(ret), KP(log_service), KP(archive_service));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      // log stream may be removed during timer refresh task.
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get ls", K(ret), K(ls_id_), K(MTL_ID()));
    }
  } else if (OB_FAIL(archive_service->get_ls_archive_speed(ls_id_,
                                                           archive_speed,
                                                           force_wait /* force_wait, which is unused */,
                                                           ignore))) {
    LOG_WARN("fail to get archive speed for ls", K(ret), K(ls_id_));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ignore = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(log_service->get_palf_options(palf_opt))) {
    LOG_WARN("fail to get palf_options", K(ret));
  } else if (OB_FAIL(log_service->get_palf_disk_usage(total_used_space, total_disk_space))) {
    STORAGE_LOG(WARN, "failed to get the disk space that clog used", K(ret));
  } else if (OB_ISNULL(GCTX.bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, bandwidth throttle is null", K(ret), KP(GCTX.bandwidth_throttle_));
  } else if (OB_FAIL(GCTX.bandwidth_throttle_->get_rate(refresh_speed))) {
    LOG_WARN("fail to get rate", K(ret), K(refresh_speed));
  } else {
    // archive is not on if ignore = true.
    write_speed_ = ignore ? std::max(refresh_speed, 1 * MIN_WRITE_SPEED) : std::max(archive_speed, 1 * MIN_WRITE_SPEED);
    disk_used_stop_write_threshold_ = min(palf_opt.disk_options_.log_disk_utilization_threshold_,
                                       palf_opt.disk_options_.log_disk_utilization_limit_threshold_);
    need_stop_write_ = 100.0 * total_used_space / total_disk_space >= disk_used_stop_write_threshold_ ? true : false;
  }
  LOG_DEBUG("current ddl clog write speed", K(ret), K(need_stop_write_), K(ls_id_), K(archive_speed), K(write_speed_),
    K(total_used_space), K(total_disk_space), K(disk_used_stop_write_threshold_), K(refresh_speed));
  return ret;
}

// calculate the sleep time for the input bytes, and return next available write timestamp.
int ObDDLCtrlSpeedItem::cal_limit(const int64_t bytes, int64_t &next_available_ts)
{
  int ret = OB_SUCCESS;
  next_available_ts = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (bytes < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input bytes.", K(ret), K(bytes));
  } else if (write_speed_ < MIN_WRITE_SPEED) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected write speed", K(ret), K(write_speed_));
  }
  if (OB_SUCC(ret)) {
    const int64_t need_sleep_us = static_cast<int64_t>(1.0 * bytes / (write_speed_ * 1024 * 1024) * 1000 * 1000);
    int64_t tmp_us = 0;
    do {
      tmp_us = next_available_write_ts_;
      next_available_ts = std::max(ObTimeUtility::current_time(), next_available_write_ts_ + need_sleep_us);
    } while (!ATOMIC_BCAS(&next_available_write_ts_, tmp_us, next_available_ts));
  }
  return ret;
}

int ObDDLCtrlSpeedItem::check_cur_node_is_leader(bool &is_leader)
{
  int ret = OB_SUCCESS;
  is_leader = true;
  ObRole role = INVALID_ROLE;
  ObLS *ls = nullptr;
  ObLSHandle handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_svr is nullptr", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id_, handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls handle", K(ret), K_(ls_id));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is nullptr", K(ret));
  } else if (OB_FAIL(ls->get_ls_role(role))) {
    LOG_WARN("get ls role failed", K(ret));
  } else if (role != ObRole::LEADER) {
    is_leader = false;
  }
  return ret;
}

int ObDDLCtrlSpeedItem::do_sleep(
  const int64_t next_available_ts,
  const uint64_t tenant_id,
  const int64_t task_id,
  ObDDLKvMgrHandle &ddl_kv_mgr_handle,
  int64_t &real_sleep_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  real_sleep_us = 0;
  bool is_exist = true;

  bool is_need_stop_write = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (next_available_ts <= 0 || OB_INVALID_TENANT_ID == tenant_id || task_id == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(next_available_ts), K(tenant_id), K(task_id));
  } else if (OB_TMP_FAIL(check_need_stop_write(ddl_kv_mgr_handle, is_need_stop_write))) {
    LOG_WARN("fail to check need stop write", K(tmp_ret), K(ddl_kv_mgr_handle));
  }
  if (OB_FAIL(ret)) {
  } else if (is_need_stop_write) /*clog disk used exceeds threshold*/ {
    int64_t loop_cnt = 0;
    ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    while (OB_SUCC(ret) && is_need_stop_write) {
      // TODO YIREN (FIXME-20221017), exit when task is canceled, etc.
      ob_usleep(SLEEP_INTERVAL);
      if (0 == loop_cnt % 100) {
        if (OB_TMP_FAIL(rootserver::ObDDLTaskRecordOperator::check_task_id_exist(*sql_proxy, tenant_id, task_id, is_exist))) {
          is_exist = true;
          LOG_WARN("check task id exist failed", K(tmp_ret), K(task_id));
        } else {
          if (!is_exist) {
            LOG_INFO("task is not exist", K(task_id));
            break;
          }
        }
      }
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        ObTaskController::get().allow_next_syslog();
        FLOG_INFO("stop write ddl clog", K(ret), K(ls_id_),
          K(write_speed_), K(need_stop_write_), K(ref_cnt_),
          K(disk_used_stop_write_threshold_));
      }
      if (OB_TMP_FAIL(check_need_stop_write(ddl_kv_mgr_handle, is_need_stop_write))) {
        LOG_WARN("fail to check need stop write", K(tmp_ret), K(ddl_kv_mgr_handle));
      }
      loop_cnt++;
    }
  }
  if (OB_SUCC(ret) && is_exist) {
    real_sleep_us = std::max(0L, next_available_ts - ObTimeUtility::current_time());
    ob_usleep(real_sleep_us);
  }
  return ret;
}

int ObDDLCtrlSpeedItem::check_need_stop_write(ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                                              bool &is_need_stop_write)
{
  int ret = OB_SUCCESS;
  is_need_stop_write = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool is_leader = true;
    if (OB_FAIL(check_cur_node_is_leader(is_leader))) {
      LOG_WARN("check cur node is leader failed", K(ret));
    } else {
      if (is_leader) {
        int64_t ddl_kv_count = ddl_kv_mgr_handle.get_obj()->get_count();
        is_need_stop_write = (ddl_kv_count >= ObTabletDDLKvMgr::MAX_DDL_KV_CNT_IN_STORAGE - 1);
        is_need_stop_write = (is_need_stop_write || need_stop_write_);
      } else {
        is_need_stop_write = false;
      }
    }
  }
  return ret;
}

// calculate the sleep time for the input bytes, sleep.
int ObDDLCtrlSpeedItem::limit_and_sleep(
  const int64_t bytes,
  const uint64_t tenant_id,
  const int64_t task_id,
  ObDDLKvMgrHandle &ddl_kv_mgr_handle,
  int64_t &real_sleep_us)
{
  int ret = OB_SUCCESS;
  real_sleep_us = 0;
  int64_t next_available_ts = 0;
  int64_t transmit_sleep_us = 0; // network related.
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if ((disk_used_stop_write_threshold_ <= 0
      || disk_used_stop_write_threshold_ > 100) || bytes < 0 || OB_INVALID_TENANT_ID == tenant_id || 0 == task_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(disk_used_stop_write_threshold_), K(bytes), K(tenant_id), K(task_id));
  } else if (OB_FAIL(cal_limit(bytes, next_available_ts))) {
    LOG_WARN("fail to calculate sleep time", K(ret), K(bytes), K(next_available_ts));
  } else if (OB_ISNULL(GCTX.bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, bandwidth throttle is null", K(ret), KP(GCTX.bandwidth_throttle_));
  } else if (OB_FAIL(GCTX.bandwidth_throttle_->limit_out_and_sleep(bytes,
                                                                   ObTimeUtility::current_time(),
                                                                   INT64_MAX,
                                                                   &transmit_sleep_us))) {
    LOG_WARN("fail to limit out and sleep", K(ret), K(bytes), K(transmit_sleep_us));
  } else if (OB_FAIL(do_sleep(next_available_ts, tenant_id, task_id, ddl_kv_mgr_handle, real_sleep_us))) {
    LOG_WARN("fail to sleep", K(ret), K(next_available_ts), K(real_sleep_us));
  } else {/* do nothing. */}
  return ret;
}

int ObDDLCtrlSpeedHandle::ObDDLCtrlSpeedItemHandle::set_ctrl_speed_item(
    ObDDLCtrlSpeedItem *item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err, item is nullptr", K(ret));
  } else {
    item->inc_ref();
    item_ = item;
  }
  return ret;
}

int ObDDLCtrlSpeedHandle::ObDDLCtrlSpeedItemHandle::get_ctrl_speed_item(
    ObDDLCtrlSpeedItem *&item) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(item_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, speed handle item is nullptr", K(ret));
  } else {
    item = item_;
  }
  return ret;
}

void ObDDLCtrlSpeedHandle::ObDDLCtrlSpeedItemHandle::reset()
{
  if (nullptr != item_) {
    if (0 == item_->dec_ref()) {
      item_->~ObDDLCtrlSpeedItem();
    }
    item_ = nullptr;
  }
}

ObDDLCtrlSpeedHandle::ObDDLCtrlSpeedHandle()
  : is_inited_(false), speed_handle_map_(), allocator_(SET_USE_500("DDLClogCtrl")),
    bucket_lock_(), refreshTimerTask_()
{
}

ObDDLCtrlSpeedHandle::~ObDDLCtrlSpeedHandle()
{
  bucket_lock_.destroy();
  if (speed_handle_map_.created()) {
    speed_handle_map_.destroy();
  }
  allocator_.reset();
}

ObDDLCtrlSpeedHandle &ObDDLCtrlSpeedHandle::get_instance()
{
  static ObDDLCtrlSpeedHandle instance;
  return instance;
}

int ObDDLCtrlSpeedHandle::init()
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "DDLSpeedCtrl");
  SET_USE_500(attr);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else if (OB_UNLIKELY(speed_handle_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, speed handle map is created", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(MAP_BUCKET_NUM))) {
    LOG_WARN("init bucket lock failed", K(ret));
  } else if (OB_FAIL(speed_handle_map_.create(MAP_BUCKET_NUM, attr, attr))) {
    LOG_WARN("fail to create speed handle map", K(ret));
  } else {
    is_inited_ = true;
    if (OB_FAIL(refreshTimerTask_.init(lib::TGDefIDs::ServerGTimer))) {
      LOG_WARN("fail to init refreshTimerTask", K(ret));
    } else {
      LOG_INFO("succeed to init ObDDLCtrlSpeedHandle", K(ret));
    }
  }
  return ret;
}

int ObDDLCtrlSpeedHandle::limit_and_sleep(const uint64_t tenant_id,
                                          const share::ObLSID &ls_id,
                                          const int64_t bytes,
                                          const int64_t task_id,
                                          ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                                          int64_t &real_sleep_us)
{
  int ret = OB_SUCCESS;
  SpeedHandleKey speed_handle_key;
  ObDDLCtrlSpeedItem *speed_handle_item = nullptr;
  ObDDLCtrlSpeedItemHandle item_handle;
  item_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if(OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid() || bytes < 0 || 0 == task_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(task_id), K(ls_id), K(bytes));
  } else if (FALSE_IT(speed_handle_key.tenant_id_ = tenant_id)) {
  } else if (FALSE_IT(speed_handle_key.ls_id_ = ls_id)) {
  } else if (OB_UNLIKELY(!speed_handle_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("speed handle map is not created", K(ret));
  } else if (OB_FAIL(add_ctrl_speed_item(speed_handle_key, item_handle))) {
    LOG_WARN("add speed item failed", K(ret));
  } else if (OB_FAIL(item_handle.get_ctrl_speed_item(speed_handle_item))) {
    LOG_WARN("get speed handle item failed", K(ret));
  } else if (OB_ISNULL(speed_handle_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err, ctrl speed item is nullptr", K(ret), K(speed_handle_key));
  } else if (OB_FAIL(speed_handle_item->limit_and_sleep(bytes,
                                                        tenant_id,
                                                        task_id,
                                                        ddl_kv_mgr_handle,
                                                        real_sleep_us))) {
    LOG_WARN("fail to limit and sleep", K(ret), K(bytes), K(task_id), K(real_sleep_us));
  }
  return ret;
}

// add entry in speed_handle_map if it does not exist.
// set entry in ctrl_speed_item_handle.
int ObDDLCtrlSpeedHandle::add_ctrl_speed_item(
    const SpeedHandleKey &speed_handle_key,
    ObDDLCtrlSpeedItemHandle &item_handle)
{
  int ret = OB_SUCCESS;
  common::ObBucketHashWLockGuard guard(bucket_lock_, speed_handle_key.hash());
  char *buf = nullptr;
  ObDDLCtrlSpeedItem *speed_handle_item = nullptr;
  item_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!speed_handle_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", K(ret), K(speed_handle_key));
  } else if (OB_UNLIKELY(!speed_handle_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected, speed handle map is not created", K(ret));
  } else if (nullptr != speed_handle_map_.get(speed_handle_key)) {
    // do nothing, speed handle item has already exist.
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(sizeof(ObDDLCtrlSpeedItem))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    speed_handle_item = new (buf) ObDDLCtrlSpeedItem();
    if (OB_ISNULL(speed_handle_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret));
    } else if (OB_FAIL(speed_handle_item->init(speed_handle_key.ls_id_))) {
        LOG_WARN("fail to init new speed handle item", K(ret), K(speed_handle_key));
    } else if (OB_FAIL(speed_handle_map_.set_refactored(speed_handle_key, speed_handle_item))) {
      LOG_WARN("fail to add speed handle item", K(ret), K(speed_handle_key));
    } else {
      speed_handle_item->inc_ref();
    }
  }

  // set entry for ctrl_speed_item_handle.
  if (OB_SUCC(ret)) {
    ObDDLCtrlSpeedItem *curr_speed_handle_item = nullptr;
    if (OB_FAIL(speed_handle_map_.get_refactored(speed_handle_key, curr_speed_handle_item))) {
      LOG_WARN("get refactored failed", K(ret), K(speed_handle_key));
    } else if (OB_ISNULL(curr_speed_handle_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err, speed handle item is nullptr", K(ret), K(speed_handle_key));
    } else if (OB_FAIL(item_handle.set_ctrl_speed_item(curr_speed_handle_item))) {
      LOG_WARN("set ctrl speed item failed", K(ret), K(speed_handle_key));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != speed_handle_item) {
      speed_handle_item->~ObDDLCtrlSpeedItem();
      speed_handle_item = nullptr;
    }
    if (nullptr != buf) {
      allocator_.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

// remove entry from speed_handle_map.
int ObDDLCtrlSpeedHandle::remove_ctrl_speed_item(const ObIArray<SpeedHandleKey> &remove_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < remove_items.count(); i++) {
    const SpeedHandleKey &speed_handle_key = remove_items.at(i);
    common::ObBucketHashWLockGuard guard(bucket_lock_, speed_handle_key.hash());
    char *buf = nullptr;
    ObDDLCtrlSpeedItem *speed_handle_item = nullptr;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_UNLIKELY(!speed_handle_key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ls id is invalid", K(ret), K(speed_handle_key));
    } else if (OB_UNLIKELY(!speed_handle_map_.created())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected, speed handle map is not created", K(ret));
    } else if (OB_FAIL(speed_handle_map_.get_refactored(speed_handle_key, speed_handle_item))) {
      LOG_WARN("get refactored failed", K(ret), K(speed_handle_key));
    } else if (OB_FAIL(speed_handle_map_.erase_refactored(speed_handle_key))) {
      LOG_WARN("fail to erase_refactored", K(ret), K(speed_handle_key));
    } else if (OB_ISNULL(speed_handle_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, speed handle item is nullptr", K(ret), K(speed_handle_key));
    } else {
      if (0 == speed_handle_item->dec_ref()) {
        speed_handle_item->~ObDDLCtrlSpeedItem();
        speed_handle_item = nullptr;
      }
    }
  }
  return ret;
}

// refresh speed_handle_map, including
// 1. remove speed_handle_item whose ls/tenant does not exist;
// 2. refresh write_speed_ and refresh disk_used_stop_write_threshold_.
int ObDDLCtrlSpeedHandle::refresh()
{
  int ret = OB_SUCCESS;
  // 1. remove speed_handle_item whose ls/tenant does not exist;
  GetNeedRemoveItemsFn get_need_remove_items_fn;
  if (OB_FAIL(speed_handle_map_.foreach_refactored(get_need_remove_items_fn))) {
    LOG_WARN("foreach refactored failed", K(ret));
  } else if (OB_FAIL(remove_ctrl_speed_item(get_need_remove_items_fn.remove_items_))) {
    LOG_WARN("remove ctrl speed item failed", K(ret), "to_remove_items", get_need_remove_items_fn.remove_items_);
  }
  // 2. update speed and disk config.
  if (OB_SUCC(ret)) {
    UpdateSpeedHandleItemFn update_speed_handle_item_fn;
    if (OB_FAIL(speed_handle_map_.foreach_refactored(update_speed_handle_item_fn))) {
      LOG_WARN("update write speed and disk config failed", K(ret));
    }
  }
  return ret;
}

// UpdateSpeedHandleItemFn update ddl clog write speed and disk used config
int ObDDLCtrlSpeedHandle::UpdateSpeedHandleItemFn::operator() (
    hash::HashMapPair<SpeedHandleKey, ObDDLCtrlSpeedItem*> &entry)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(entry.first.tenant_id_) {
    if (OB_ISNULL(entry.second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nulptr", K(ret), K(entry.first));
    } else if (OB_FAIL(entry.second->refresh())) {
      LOG_WARN("refresh speed and disk config failed", K(ret), K(entry));
    }
  } else if (OB_TENANT_NOT_IN_SERVER == ret || OB_IN_STOP_STATE == ret) { // tenant deleted or on deleting
    if (OB_ISNULL(entry.second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nulptr", K(ret), K(entry.first));
    } else {
      entry.second->reset_need_stop_write();
      ret = OB_SUCCESS;
    }
  } else {
    LOG_WARN("switch tenant id failed", K(ret), K(MTL_ID()), K(entry));
  }
  return ret;
}

int ObDDLCtrlSpeedHandle::GetNeedRemoveItemsFn::operator() (
  hash::HashMapPair<SpeedHandleKey, ObDDLCtrlSpeedItem*> &entry)
{
  int ret = OB_SUCCESS;
  bool erase = false;
  const SpeedHandleKey &speed_handle_key = entry.first;
  if (OB_UNLIKELY(!speed_handle_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(speed_handle_key));
  } else {
    MTL_SWITCH(speed_handle_key.tenant_id_) {
      ObLSHandle ls_handle;
      if (OB_FAIL(MTL(ObLSService *)->get_ls(speed_handle_key.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
        if (OB_LS_NOT_EXIST == ret) {
          erase = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get ls", K(ret), K(speed_handle_key));
        }
      }
    } else {
      if (OB_TENANT_NOT_IN_SERVER == ret || OB_IN_STOP_STATE == ret) { // tenant deleted or on deleting
        ret = OB_SUCCESS;
        erase = true;
      } else {
        LOG_WARN("fail to switch tenant id", K(ret), K(MTL_ID()), K(speed_handle_key));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (erase && OB_FAIL(remove_items_.push_back(speed_handle_key))) {
    LOG_WARN("add remove item failed", K(ret));
  }
  return ret;
}

// RefreshSpeedHandle Timer Task
ObDDLCtrlSpeedHandle::RefreshSpeedHandleTask::RefreshSpeedHandleTask()
  : is_inited_(false) {}

ObDDLCtrlSpeedHandle::RefreshSpeedHandleTask::~RefreshSpeedHandleTask()
{
  is_inited_ = false;
}

int ObDDLCtrlSpeedHandle::RefreshSpeedHandleTask::init(int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    is_inited_ = true;
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, REFRESH_INTERVAL, true /* schedule repeatedly */))) {
      LOG_WARN("fail to schedule RefreshSpeedHandle Timer Task", K(ret));
    }
  }
  return ret;
}

void ObDDLCtrlSpeedHandle::RefreshSpeedHandleTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("RefreshSpeedHandleTask not init", K(ret));
  } else if (OB_FAIL(ObDDLCtrlSpeedHandle::get_instance().refresh())) {
    LOG_WARN("fail to refresh SpeedHandleMap", K(ret));
  }
}

ObDDLRedoLogWriter::ObDDLRedoLogWriter() : is_inited_(false), bucket_lock_()
{
}

ObDDLRedoLogWriter::~ObDDLRedoLogWriter()
{
}

ObDDLRedoLogWriter &ObDDLRedoLogWriter::get_instance()
{
  static ObDDLRedoLogWriter instance;
  return instance;
}

int ObDDLRedoLogWriter::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 10243L;
  if (is_inited_) {
  } else if (OB_FAIL(bucket_lock_.init(bucket_num))) {
    LOG_WARN("init bucket lock failed", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDDLRedoLogWriter::write(
  ObTabletHandle &tablet_handle,
  ObDDLKvMgrHandle &ddl_kv_mgr_handle,
  const ObDDLRedoLog &log,
  const uint64_t tenant_id,
  const int64_t task_id,
  const share::ObLSID &ls_id,
  ObLogHandler *log_handler,
  const blocksstable::MacroBlockId &macro_block_id,
  char *buffer,
  ObDDLRedoLogHandle &handle)
{
  int ret = OB_SUCCESS;
  const enum ObReplayBarrierType replay_barrier_type = ObReplayBarrierType::NO_NEED_BARRIER;
  logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::DDL_LOG_BASE_TYPE,
                                          replay_barrier_type);
  ObDDLClogHeader ddl_header(ObDDLClogType::DDL_REDO_LOG);
  const int64_t buffer_size = base_header.get_serialize_size()
                              + ddl_header.get_serialize_size()
                              + log.get_serialize_size();
  int64_t pos = 0;
  ObDDLMacroBlockClogCb *cb = nullptr;
  ObDDLRedoLog tmp_log;
  int64_t log_start_pos = 0;

  palf::LSN lsn;
  const bool need_nonblock= false;
  SCN base_scn = SCN::min_scn();
  SCN scn;
  uint32_t lock_tid = 0;
  int64_t real_sleep_us = 0;
  int tmp_ret = OB_SUCCESS;
  if (!log.is_valid() || nullptr == log_handler || !ls_id.is_valid()
                      || OB_INVALID_TENANT_ID == tenant_id
                      || nullptr == buffer || 0 == task_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(log), K(ls_id), K(tenant_id), KP(buffer));
  } else if (OB_TMP_FAIL(ObDDLCtrlSpeedHandle::get_instance().limit_and_sleep(tenant_id, ls_id, buffer_size, task_id, ddl_kv_mgr_handle, real_sleep_us))) {
    LOG_WARN("fail to limit and sleep", K(tmp_ret), K(tenant_id), K(task_id), K(ls_id), K(buffer_size), K(real_sleep_us));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->rdlock(ObDDLRedoLogHandle::DDL_REDO_LOG_TIMEOUT, lock_tid))) {
    LOG_WARN("failed to rdlock", K(ret));
  } else if (ddl_kv_mgr_handle.get_obj()->get_commit_scn(tablet_handle.get_obj()->get_tablet_meta()).is_valid_and_not_min()) {
    ret = OB_TRANS_COMMITED;
    LOG_WARN("already commit", K(ret));
  } else if (ddl_kv_mgr_handle.get_obj()->get_start_scn() != log.get_redo_info().start_scn_) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("restarted", K(ret));
  } else if (OB_ISNULL(cb = op_alloc(ObDDLMacroBlockClogCb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(base_header.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("failed to serialize log base header", K(ret));
  } else if (OB_FAIL(ddl_header.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("fail to seriaize ddl redo log", K(ret));
  } else if (FALSE_IT(log_start_pos = pos)) {
  } else if (OB_FAIL(log.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("fail to seriaize ddl redo log", K(ret));
  } else if (OB_FAIL(tmp_log.deserialize(buffer, buffer_size, log_start_pos))) {
    LOG_WARN("fail to deserialize ddl redo log", K(ret));
  /* use the ObString data_buffer_ in tmp_log.redo_info_, do not rely on the macro_block_buf in original log*/
  } else if (OB_FAIL(cb->init(ls_id, tmp_log.get_redo_info(), macro_block_id, tablet_handle, ddl_kv_mgr_handle))) {
    LOG_WARN("init ddl clog callback failed", K(ret));
  } else if (OB_FAIL(log_handler->append(buffer,
                                         buffer_size,
                                         base_scn,
                                         need_nonblock,
                                         cb,
                                         lsn,
                                         scn))) {
    LOG_WARN("fail to submit ddl redo log", K(ret), K(buffer), K(buffer_size));
  } else {
    handle.cb_ = cb;
    cb = nullptr;
    handle.scn_ = scn;
    LOG_INFO("submit ddl redo log succeed", K(lsn), K(base_scn), K(scn));
  }
  if (0 != lock_tid) {
    ddl_kv_mgr_handle.get_obj()->unlock(lock_tid);
  }
  if (OB_FAIL(ret)) {
    if (nullptr != cb) {
      op_free(cb);
      cb = nullptr;
    }
  }
  return ret;
}

int ObDDLRedoLogWriter::write_ddl_start_log(ObLSHandle &ls_handle,
                                            ObTabletHandle &tablet_handle,
                                            ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                                            const ObDDLStartLog &log,
                                            ObLogHandler *log_handler,
                                            SCN &start_scn)
{
  int ret = OB_SUCCESS;
  start_scn.set_min();
  const enum ObReplayBarrierType replay_barrier_type = ObReplayBarrierType::STRICT_BARRIER;
  logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::DDL_LOG_BASE_TYPE,
                                          replay_barrier_type);
  ObDDLClogHeader ddl_header(ObDDLClogType::DDL_START_LOG);
  const int64_t buffer_size = base_header.get_serialize_size()
                              + ddl_header.get_serialize_size()
                              + log.get_serialize_size();
  char buffer[buffer_size];
  int64_t pos = 0;
  ObDDLStartClogCb *cb = nullptr;

  palf::LSN lsn;
  const bool need_nonblock= false;
  SCN scn = SCN::min_scn();
  bool is_external_consistent = false;
  ObBucketHashWLockGuard guard(bucket_lock_, log.get_table_key().get_tablet_id().hash());
  uint32_t lock_tid = 0;
  if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->wrlock(ObDDLRedoLogHandle::DDL_REDO_LOG_TIMEOUT, lock_tid))) {
    LOG_WARN("failed to wrlock", K(ret));
  } else if (ddl_kv_mgr_handle.get_obj()->is_execution_id_older(log.get_execution_id())) {
    ret = OB_TASK_EXPIRED;
    LOG_INFO("receive a old execution id, don't do ddl start", K(ret), K(log));
  } else if (ddl_kv_mgr_handle.get_obj()->get_commit_scn(tablet_handle.get_obj()->get_tablet_meta()).is_valid_and_not_min()) {
    start_scn = ddl_kv_mgr_handle.get_obj()->get_start_scn();
    if (!start_scn.is_valid_and_not_min()) {
      start_scn = tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_;
    }
    if (!start_scn.is_valid_and_not_min()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("start scn must be valid after commit", K(ret), K(start_scn));
    } else {
      LOG_INFO("already committed, use previous start scn", K(ret), K(tablet_handle.get_obj()->get_tablet_meta()));
    }
  } else if (OB_ISNULL(cb = op_alloc(ObDDLStartClogCb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(cb->init(log.get_table_key(), log.get_data_format_version(), log.get_execution_id(), lock_tid, ddl_kv_mgr_handle))) {
    LOG_WARN("failed to init cb", K(ret));
  } else if (OB_FAIL(base_header.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("failed to serialize log base header", K(ret));
  } else if (OB_FAIL(ddl_header.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("fail to seriaize ddl start log", K(ret));
  } else if (OB_FAIL(log.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("fail to seriaize ddl start log", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_ddl_log_handler()->add_tablet(log.get_table_key().get_tablet_id()))) {
    LOG_WARN("add tablet failed", K(ret));
  } else if (OB_FAIL(log_handler->append(buffer,
                                         buffer_size,
                                         SCN::min_scn(),
                                         need_nonblock,
                                         cb,
                                         lsn,
                                         scn))) {
    LOG_WARN("fail to submit ddl start log", K(ret), K(buffer_size));
    if (ObDDLUtil::need_remote_write(ret)) {
      ret = OB_NOT_MASTER;
      LOG_INFO("overwrite return to OB_NOT_MASTER");
    }
  } else {
    ObDDLStartClogCb *tmp_cb = cb;
    cb = nullptr;
    lock_tid = 0;
    bool finish = false;
    const int64_t start_time = ObTimeUtility::current_time();
    while (OB_SUCC(ret) && !finish) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (tmp_cb->is_success()) {
        finish = true;
      } else if (tmp_cb->is_failed()) {
        ret = OB_NOT_MASTER;
      }
      if (OB_SUCC(ret) && !finish) {
        const int64_t current_time = ObTimeUtility::current_time();
        if (current_time - start_time > ObDDLRedoLogHandle::DDL_REDO_LOG_TIMEOUT) {
          ret = OB_TIMEOUT;
          LOG_WARN("write ddl start log timeout", K(ret), K(current_time), K(start_time));
        } else {
          if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) { //10s
            LOG_INFO("wait ddl start log callback", K(ret), K(finish), K(current_time), K(start_time));
          }
          ob_usleep(ObDDLRedoLogHandle::CHECK_DDL_REDO_LOG_FINISH_INTERVAL);
        }
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t saved_snapshot_version = log.get_table_key().get_snapshot_version();
      start_scn = scn;
      // remove ddl sstable if exists and flush ddl start log ts and snapshot version into tablet meta
      if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->update_tablet(*tablet_handle.get_obj(), start_scn, saved_snapshot_version, log.get_data_format_version(), log.get_execution_id(), start_scn))) {
        LOG_WARN("clean up ddl sstable failed", K(ret), K(log));
      }
      FLOG_INFO("start ddl kv mgr finished", K(ret), K(start_scn), K(log));
    }
    tmp_cb->try_release(); // release the memory no matter succ or not
  }
  if (OB_FAIL(ret)) {
    if (nullptr != cb) {
      op_free(cb);
      cb = nullptr;
    }
  }
  if (0 != lock_tid) {
    ddl_kv_mgr_handle.get_obj()->unlock(lock_tid);
  }
  return ret;
}

template <typename T>
int ObDDLRedoLogWriter::write_ddl_commit_log(ObTabletHandle &tablet_handle,
                                             ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                                             const T &log,
                                             const ObDDLClogType clog_type,
                                             const share::ObLSID &ls_id,
                                             ObLogHandler *log_handler,
                                             ObDDLCommitLogHandle &handle)
{
  int ret = OB_SUCCESS;
  const enum ObReplayBarrierType replay_barrier_type = ObReplayBarrierType::PRE_BARRIER;
  DEBUG_SYNC(BEFORE_WRITE_DDL_PREPARE_LOG);
  logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::DDL_LOG_BASE_TYPE,
                                          replay_barrier_type);
  ObDDLClogHeader ddl_header(clog_type);
  char *buffer = nullptr;
  const int64_t buffer_size = base_header.get_serialize_size()
                              + ddl_header.get_serialize_size()
                              + log.get_serialize_size();
  int64_t pos = 0;
  ObDDLCommitClogCb *cb = nullptr;

  palf::LSN lsn;
  const bool need_nonblock= false;
  SCN base_scn = SCN::min_scn();
  SCN scn = SCN::min_scn();
  bool is_external_consistent = false;
  uint32_t lock_tid = 0;
  if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->wrlock(ObDDLRedoLogHandle::DDL_REDO_LOG_TIMEOUT, lock_tid))) {
    LOG_WARN("failed to wrlock", K(ret));
  } else if (ddl_kv_mgr_handle.get_obj()->get_start_scn() != log.get_start_scn()) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("restarted", K(ret));
  } else if (ddl_kv_mgr_handle.get_obj()->get_commit_scn(tablet_handle.get_obj()->get_tablet_meta()).is_valid_and_not_min()) {
    ret = OB_TRANS_COMMITED;
    LOG_WARN("already committed", K(ret), K(log));
  } else if (OB_ISNULL(buffer = static_cast<char *>(ob_malloc(buffer_size, ObMemAttr(MTL_ID(), "DDL_COMMIT_LOG"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_ISNULL(cb = op_alloc(ObDDLCommitClogCb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(cb->init(ls_id, log.get_table_key().tablet_id_, log.get_start_scn(), lock_tid, ddl_kv_mgr_handle))) {
    LOG_WARN("init ddl commit log callback failed", K(ret), K(ls_id), K(log));
  } else if (OB_FAIL(base_header.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("failed to serialize log base header", K(ret));
  } else if (OB_FAIL(ddl_header.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("fail to seriaize ddl commit log", K(ret));
  } else if (OB_FAIL(log.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("fail to seriaize ddl commit log", K(ret));
  } else if (OB_FAIL(OB_TS_MGR.get_ts_sync(MTL_ID(), ObDDLRedoLogHandle::DDL_REDO_LOG_TIMEOUT, base_scn, is_external_consistent))) {
    LOG_WARN("fail to get gts sync", K(ret), K(log));
  } else if (OB_FAIL(log_handler->append(buffer,
                                         buffer_size,
                                         base_scn,
                                         need_nonblock,
                                         cb,
                                         lsn,
                                         scn))) {
    LOG_WARN("fail to submit ddl commit log", K(ret), K(buffer), K(buffer_size));
  } else {
    ObDDLCommitClogCb *tmp_cb = cb;
    cb = nullptr;
    lock_tid = 0;
    bool need_retry = true;
    while (need_retry) {
      if (OB_FAIL(OB_TS_MGR.wait_gts_elapse(MTL_ID(), scn))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("fail to wait gts elapse", K(ret), K(log));
        } else {
          ob_usleep(1000);
        }
      } else {
        need_retry = false;
      }
    }
    if (OB_SUCC(ret)) {
      handle.cb_ = tmp_cb;
      handle.commit_scn_ = scn;
      LOG_INFO("submit ddl commit log succeed", K(lsn), K(base_scn), K(scn));
    } else {
      tmp_cb->try_release(); // release the memory
    }
  }
  if (nullptr != buffer) {
    ob_free(buffer);
    buffer = nullptr;
  }
  if (0 != lock_tid) {
    ddl_kv_mgr_handle.get_obj()->unlock(lock_tid);
  }
  if (OB_FAIL(ret)) {
    if (nullptr != cb) {
      op_free(cb);
      cb = nullptr;
    }
  }
  return ret;
}

ObDDLRedoLogHandle::ObDDLRedoLogHandle()
  : cb_(nullptr), scn_(SCN::min_scn())
{
}

ObDDLRedoLogHandle::~ObDDLRedoLogHandle()
{
  reset();
}

void ObDDLRedoLogHandle::reset()
{
  if (nullptr != cb_) {
    cb_->try_release();
    cb_ = nullptr;
  }
}

int ObDDLRedoLogHandle::wait(const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cb_)) {
  } else {
    bool finish = false;
    const int64_t start_time = ObTimeUtility::current_time();
    while (OB_SUCC(ret) && !finish) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (cb_->is_success()) {
        finish = true;
      } else if (cb_->is_failed()) {
        ret = OB_NOT_MASTER;
      }
      if (OB_SUCC(ret) && !finish) {
        const int64_t current_time = ObTimeUtility::current_time();
        if (current_time - start_time > timeout) {
          ret = OB_TIMEOUT;
          LOG_WARN("write ddl redo log timeout", K(ret), K(current_time), K(start_time));
        } else {
          if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) { //10s
            LOG_INFO("wait ddl redo log callback", K(ret), K(finish), K(current_time), K(start_time));
          }
          ob_usleep(ObDDLRedoLogHandle::CHECK_DDL_REDO_LOG_FINISH_INTERVAL);
        }
      }
    }
  }
  return ret;
}

ObDDLCommitLogHandle::ObDDLCommitLogHandle()
  : cb_(nullptr), commit_scn_(SCN::min_scn())
{
}

ObDDLCommitLogHandle::~ObDDLCommitLogHandle()
{
  reset();
}

int ObDDLCommitLogHandle::wait(const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cb_)) {
  } else {
    bool finish = false;
    const int64_t start_time = ObTimeUtility::current_time();
    while (OB_SUCC(ret) && !finish) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (cb_->is_success()) {
        finish = true;
        ret = cb_->get_ret_code();
        if (OB_FAIL(ret)) {
          LOG_WARN("ddl commit log callback execute failed", K(ret), KPC(cb_));
        }
      } else if (cb_->is_failed()) {
        ret = OB_NOT_MASTER;
      }
      if (OB_SUCC(ret) && !finish) {
        const int64_t current_time = ObTimeUtility::current_time();
        if (current_time - start_time > timeout) {
          ret = OB_TIMEOUT;
          LOG_WARN("write ddl commit log timeout", K(ret), K(current_time), K(start_time));
        } else {
          if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) { //10s
            LOG_INFO("wait ddl commit log callback", K(ret), K(finish), K(current_time), K(start_time));
          }
          ob_usleep(ObDDLRedoLogHandle::CHECK_DDL_REDO_LOG_FINISH_INTERVAL);
        }
      }
    }
  }
  return ret;
}

void ObDDLCommitLogHandle::reset()
{
  int tmp_ret = OB_SUCCESS;
  if (nullptr != cb_) {
    cb_->try_release();
    cb_ = nullptr;
  }
}


int ObDDLMacroBlockRedoWriter::write_macro_redo(ObTabletHandle &tablet_handle,
                                                ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                                                const ObDDLMacroBlockRedoInfo &redo_info,
                                                const share::ObLSID &ls_id,
                                                const int64_t task_id,
                                                logservice::ObLogHandler *log_handler,
                                                const blocksstable::MacroBlockId &macro_block_id,
                                                char *buffer,
                                                ObDDLRedoLogHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!redo_info.is_valid()
                  || nullptr == log_handler
                  || nullptr == buffer
                  || 0 == task_id
                  || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(redo_info), KP(log_handler), KP(buffer), K(task_id), K(ls_id));
  } else {
    ObDDLRedoLog log;
    int64_t tmp_ret = OB_SUCCESS;
    const uint64_t tenant_id = MTL_ID();
    if (OB_FAIL(log.init(redo_info))) {
      LOG_WARN("fail to init DDLRedoLog", K(ret), K(redo_info));
    } else if (OB_FAIL(ObDDLRedoLogWriter::get_instance().write(tablet_handle,
                                                                ddl_kv_mgr_handle,
                                                                log, tenant_id,
                                                                task_id, ls_id,
                                                                log_handler,
                                                                macro_block_id, buffer,
                                                                handle))) {
      LOG_WARN("fail to write ddl redo log item", K(ret));
    }
  }
  return ret;
}

int ObDDLMacroBlockRedoWriter::remote_write_macro_redo(const int64_t task_id,
                                                       const ObAddr &leader_addr,
                                                       const ObLSID &leader_ls_id,
                                                       const ObDDLMacroBlockRedoInfo &redo_info)
{
  int ret = OB_SUCCESS;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  if (OB_UNLIKELY(!redo_info.is_valid() || 0 == task_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(redo_info));
  } else if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("srv rpc proxy is null", K(ret), KP(srv_rpc_proxy));
  } else {
    obrpc::ObRpcRemoteWriteDDLRedoLogArg arg;
    if (OB_FAIL(arg.init(MTL_ID(), leader_ls_id, redo_info, task_id))) {
      LOG_WARN("fail to init ObRpcRemoteWriteDDLRedoLogArg", K(ret));
    } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr).by(MTL_ID()).remote_write_ddl_redo_log(arg))) {
      LOG_WARN("fail to remote write ddl redo log", K(ret), K(leader_addr), K(arg));
    }
  }
  return ret;
}

ObDDLSSTableRedoWriter::ObDDLSSTableRedoWriter()
  : is_inited_(false), remote_write_(false), start_scn_(SCN::min_scn()),
    ls_id_(), tablet_id_(), ddl_redo_handle_(), leader_addr_(), leader_ls_id_(), buffer_(nullptr)
{
}

int ObDDLSSTableRedoWriter::init(const ObLSID &ls_id, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLSSTableRedoWriter has been inited twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLSSTableRedoWriter::start_ddl_redo(const ObITable::TableKey &table_key,
                                           const int64_t execution_id,
                                           const int64_t data_format_version,
                                           ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObDDLStartLog log;
  ddl_kv_mgr_handle.reset();
  SCN tmp_scn;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLSSTableRedoWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || execution_id < 0 || data_format_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key), K(execution_id), K(data_format_version));
  } else if (OB_FAIL(log.init(table_key, data_format_version, execution_id))) {
    LOG_WARN("fail to init DDLStartLog", K(ret), K(table_key), K(execution_id), K(data_format_version));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), K(table_key));
  } else if (OB_FAIL(ls->get_tablet(tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
    LOG_WARN("create ddl kv mgr failed", K(ret));
  } else if (OB_FAIL(ObDDLRedoLogWriter::get_instance().write_ddl_start_log(ls_handle, tablet_handle, ddl_kv_mgr_handle, log, ls->get_log_handler(), tmp_scn))) {
    LOG_WARN("fail to write ddl start log", K(ret), K(table_key));
  } else if (FALSE_IT(set_start_scn(tmp_scn))) {
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->register_to_tablet(get_start_scn(), ddl_kv_mgr_handle))) {
    LOG_WARN("register ddl kv mgr to tablet failed", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    ddl_kv_mgr_handle.get_obj()->reset_commit_success(); // releated issue:
  }
  return ret;
}

int ObDDLSSTableRedoWriter::end_ddl_redo_and_create_ddl_sstable(
    const share::ObLSID &ls_id,
    const ObITable::TableKey &table_key,
    const uint64_t table_id,
    const int64_t execution_id,
    const int64_t ddl_task_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  const ObTabletID &tablet_id = table_key.tablet_id_;
  ObLS *ls = nullptr;
  SCN ddl_start_scn = get_start_scn();
  SCN commit_scn = SCN::min_scn();
  bool is_remote_write = false;
  bool commit_by_this_execution = false;
  if (OB_UNLIKELY(!ls_id.is_valid() || !table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls", K(ret), K(ls_id), K(table_key));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle))) {
    LOG_WARN("get tablet failed", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("get ddl kv manager failed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(write_commit_log(tablet_handle, ddl_kv_mgr_handle, true, table_key, commit_scn, is_remote_write))) {
    if (OB_TASK_EXPIRED == ret) {
      LOG_INFO("ddl task expired", K(ret), K(table_key), K(table_id), K(execution_id), K(ddl_task_id));
    } else {
      LOG_WARN("fail write ddl commit log", K(ret), K(table_key));
    }
  } else {
    commit_by_this_execution = true;
  }

  if (OB_TRANS_COMMITED == ret) {
    commit_scn = ddl_kv_mgr_handle.get_obj()->get_commit_scn(tablet_handle.get_obj()->get_tablet_meta());
    if (!commit_scn.is_valid_and_not_min()) {
      ret = OB_EAGAIN;
      LOG_WARN("committed on leader but not committed on me, retry", K(ret), K(ddl_start_scn), K(commit_scn), K(table_id), K(execution_id), K(ddl_task_id));
    } else {
      ret = OB_SUCCESS;
      ddl_start_scn = ddl_kv_mgr_handle.get_obj()->get_start_scn();
      set_start_scn(ddl_start_scn);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_remote_write) {
    LOG_INFO("ddl commit log is written in remote, need wait replay", K(ddl_task_id), K(tablet_id), K(ddl_start_scn), K(commit_scn));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->ddl_commit(*tablet_handle.get_obj(), ddl_start_scn, commit_scn))) {
    if (OB_TASK_EXPIRED == ret) {
      LOG_INFO("ddl task expired", K(ret), K(ls_id), K(tablet_id),
          K(ddl_start_scn), "new_ddl_start_scn", ddl_kv_mgr_handle.get_obj()->get_start_scn());
    } else {
      LOG_WARN("failed to do ddl kv commit", K(ret), K(ddl_start_scn), K(commit_scn));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->wait_ddl_merge_success(*tablet_handle.get_obj(), ddl_start_scn, commit_scn))) {
    if (OB_TASK_EXPIRED == ret) {
      LOG_INFO("ddl task expired, but return success", K(ret), K(ls_id), K(tablet_id),
          K(ddl_start_scn), "new_ddl_start_scn",
          ddl_kv_mgr_handle.get_obj()->get_start_scn());
    } else {
      LOG_WARN("failed to wait ddl merge", K(ret), K(ddl_start_scn));
    }
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet handle is null", K(ret), K(ls_id), K(tablet_id));
  } else {
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    ObSSTableMetaHandle sst_meta_hdl;
    const ObSSTable *first_major_sstable = nullptr;
    if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
            LOG_WARN("fail to fetch table store", K(ret));
    } else if (OB_FALSE_IT(first_major_sstable = static_cast<const ObSSTable *>(
        table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)))) {
    } else if (OB_ISNULL(first_major_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no major after wait merge success", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(first_major_sstable->get_meta(sst_meta_hdl))) {
      LOG_WARN("fail to get sstable meta handle", K(ret));
    } else if (commit_by_this_execution && OB_UNLIKELY(first_major_sstable->get_key() != table_key)) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("ddl major sstable dropped, snapshot holding may have bug", K(ret), KPC(first_major_sstable), K(table_key), K(tablet_id), K(execution_id), K(ddl_task_id));
    } else {
      for (int64_t retry_cnt = 10; retry_cnt > 0; retry_cnt--) { // overwrite ret
        if (OB_FAIL(ObTabletDDLUtil::report_ddl_checksum(ls_id,
                                                         tablet_id,
                                                         table_id,
                                                         execution_id,
                                                         ddl_task_id,
                                                         sst_meta_hdl.get_sstable_meta().get_col_checksum(),
                                                         sst_meta_hdl.get_sstable_meta().get_col_checksum_cnt()))) {
          LOG_WARN("report ddl column checksum failed", K(ret), K(ls_id), K(tablet_id), K(execution_id), K(ddl_task_id));
        } else {
          break;
        }
        ob_usleep(100L * 1000L);
      }
    }
  }
  return ret;
}

int ObDDLSSTableRedoWriter::write_redo_log(const ObDDLMacroBlockRedoInfo &redo_info,
                                           const blocksstable::MacroBlockId &macro_block_id,
                                           const bool allow_remote_write,
                                           const int64_t task_id,
                                           ObTabletHandle &tablet_handle,
                                           ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const int64_t BUF_SIZE = 2 * 1024 * 1024 + 16 * 1024;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLSSTableRedoWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!redo_info.is_valid() || 0 == task_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (nullptr == buffer_ && OB_ISNULL(buffer_ = static_cast<char *>(ob_malloc(BUF_SIZE, ObMemAttr(MTL_ID(), "DDL_REDO_LOG"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(BUF_SIZE));
  } else if (!remote_write_) {
    if (OB_FAIL(ObDDLMacroBlockRedoWriter::write_macro_redo(tablet_handle, ddl_kv_mgr_handle, redo_info, ls->get_ls_id(), task_id, ls->get_log_handler(), macro_block_id, buffer_, ddl_redo_handle_))) {
      if (ObDDLUtil::need_remote_write(ret) && allow_remote_write) {
        if (OB_FAIL(switch_to_remote_write())) {
          LOG_WARN("fail to switch to remote write", K(ret));
        }
      } else {
        LOG_WARN("fail to write ddl redo clog", K(ret), K(MTL_GET_TENANT_ROLE_CACHE()));
      }
    }
  }

  if (OB_SUCC(ret) && remote_write_) {
    if (OB_FAIL(retry_remote_write_ddl_clog( [&]() { return remote_write_macro_redo(task_id, redo_info); }))) {
      LOG_WARN("remote write redo failed", K(ret), K(task_id));
    }
  }
  return ret;
}

int ObDDLSSTableRedoWriter::wait_redo_log_finish(const ObDDLMacroBlockRedoInfo &redo_info,
                                                 const blocksstable::MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLSSTableRedoWriter has not been inited", K(ret));
  } else if (remote_write_) {
    // remote write no need to wait local handle
  } else if (OB_UNLIKELY(!ddl_redo_handle_.is_valid())) {
    // no redo log has been written yet
  } else if (OB_FAIL(ddl_redo_handle_.wait())) {
    LOG_WARN("fail to wait io finish", K(ret));
  } else if (OB_FAIL(ddl_redo_handle_.cb_->get_ret_code())) {
    LOG_WARN("ddl redo callback executed failed", K(ret));
  }
  ddl_redo_handle_.reset();
  return ret;
}

int ObDDLSSTableRedoWriter::write_commit_log(ObTabletHandle &tablet_handle,
                                             ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                                             const bool allow_remote_write,
                                             const ObITable::TableKey &table_key,
                                             SCN &commit_scn,
                                             bool &is_remote_write)

{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("storage_ddl", "before_write_prepare_log",
                        "table_key", table_key);
  DEBUG_SYNC(BEFORE_DDL_WRITE_PREPARE_LOG);
#endif
  commit_scn.set_min();
  is_remote_write = false;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObDDLCommitLog log;
  ObDDLCommitLogHandle handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLSSTableRedoWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || !start_scn_.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key), K(start_scn_));
  } else if (OB_FAIL(log.init(table_key, get_start_scn()))) {
    LOG_WARN("fail to init DDLCommitLog", K(ret), K(table_key), K(start_scn_));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), K(table_key));
  } else if (!remote_write_) {
    if (OB_FAIL(ObDDLRedoLogWriter::get_instance().write_ddl_commit_log(tablet_handle, ddl_kv_mgr_handle, log, ObDDLClogType::DDL_COMMIT_LOG, ls_id_, ls->get_log_handler(), handle))) {
      if (ObDDLUtil::need_remote_write(ret) && allow_remote_write) {
        if (OB_FAIL(switch_to_remote_write())) {
          LOG_WARN("fail to switch to remote write", K(ret), K(table_key));
        }
      } else {
        LOG_WARN("fail to write ddl commit log", K(ret), K(table_key));
      }
    } else if (OB_FAIL(handle.wait())) {
      LOG_WARN("wait ddl commit log finish failed", K(ret), K(table_key));
    } else {
      commit_scn = handle.get_commit_scn();
    }
  }
  if (OB_SUCC(ret) && remote_write_) {
    obrpc::ObRpcRemoteWriteDDLCommitLogArg arg;
    if (OB_FAIL(arg.init(MTL_ID(), leader_ls_id_, table_key, get_start_scn()))) {
      LOG_WARN("fail to init ObRpcRemoteWriteDDLCommitLogArg", K(ret));
    } else if (OB_FAIL(retry_remote_write_ddl_clog( [&]() { return remote_write_commit_log(arg, commit_scn); }))) {
      LOG_WARN("remote write ddl commit log failed", K(ret), K(arg));
    } else {
      is_remote_write = !(leader_addr_ == GCTX.self_addr());
    }
  }
  return ret;
}

int ObDDLSSTableRedoWriter::switch_to_remote_write()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  share::ObLocationService *location_service = nullptr;
  bool is_cache_hit = false;
  if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("location service is null", K(ret), KP(location_service));
  } else if (OB_FAIL(location_service->get(tenant_id,
                                           tablet_id_,
                                           INT64_MAX/*expire_renew_time*/,
                                           is_cache_hit,
                                           leader_ls_id_))) {
    LOG_WARN("fail to get log stream id", K(ret), K_(tablet_id));
  } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id,
                                                  tenant_id,
                                                  leader_ls_id_,
                                                  true, /*force_renew*/
                                                  leader_addr_))) {
      LOG_WARN("get leader failed", K(ret), K(leader_ls_id_));
  } else {
    remote_write_ = true;
    LOG_INFO("switch to remote write", K(ret), K_(tablet_id));
  }
  return ret;
}

template <typename T>
int ObDDLSSTableRedoWriter::retry_remote_write_ddl_clog(T function)
{
  int ret = OB_SUCCESS;
  int retry_cnt = 0;
  const int64_t MAX_REMOTE_WRITE_RETRY_CNT = 800;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(switch_to_remote_write())) {
      LOG_WARN("flush ls leader location failed", K(ret));
    } else if (OB_FAIL(function())) {
      if (OB_NOT_MASTER == ret && retry_cnt++ < MAX_REMOTE_WRITE_RETRY_CNT) {
        ob_usleep(10 * 1000); // 10 ms.
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("remote write macro redo failed", K(ret), K_(leader_ls_id), K_(leader_addr));
      }
    } else {
      break; // remote write ddl clog successfully.
    }
  }
  return ret;
}

int ObDDLSSTableRedoWriter::remote_write_macro_redo(const int64_t task_id, const ObDDLMacroBlockRedoInfo &redo_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLMacroBlockRedoWriter::remote_write_macro_redo(task_id,
                                                                leader_addr_,
                                                                leader_ls_id_,
                                                                redo_info))) {
    LOG_WARN("remote write macro redo failed", K(ret));
  }
  return ret;
}

int ObDDLSSTableRedoWriter::remote_write_commit_log(const obrpc::ObRpcRemoteWriteDDLCommitLogArg &arg, SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObSrvRpcProxy *srv_rpc_proxy = GCTX.srv_rpc_proxy_;
  obrpc::Int64 log_ns;
  int retry_cnt = 0;
  if (OB_ISNULL(srv_rpc_proxy)) {
    ret = OB_ERR_SYS;
    LOG_WARN("srv rpc proxy or location service is null", K(ret), KP(srv_rpc_proxy));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr_).by(MTL_ID()).remote_write_ddl_commit_log(arg, log_ns))) {
    LOG_WARN("remote write macro redo failed", K(ret), K_(leader_ls_id), K_(leader_addr));
  } else if (OB_FAIL(commit_scn.convert_for_tx(log_ns))) {
    LOG_WARN("convert for tx failed", K(ret));
  }
  return ret;
}

ObDDLSSTableRedoWriter::~ObDDLSSTableRedoWriter()
{
  if (nullptr != buffer_) {
    ob_free(buffer_);
    buffer_ = nullptr;
  }
}

ObDDLRedoLogWriterCallback::ObDDLRedoLogWriterCallback()
  : is_inited_(false), redo_info_(), table_key_(), macro_block_id_(), ddl_writer_(nullptr), block_buffer_(nullptr), task_id_(0)
{
}

ObDDLRedoLogWriterCallback::~ObDDLRedoLogWriterCallback()
{
  (void)wait();
  if (nullptr != block_buffer_) {
    ob_free(block_buffer_);
    block_buffer_ = nullptr;
  }
}

int ObDDLRedoLogWriterCallback::init(const ObDDLMacroBlockType block_type,
                                     const ObITable::TableKey &table_key,
                                     const int64_t task_id,
                                     ObDDLSSTableRedoWriter *ddl_writer,
                                     ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSService *ls_service = nullptr;
  bool is_cache_hit = false;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLSSTableRedoWriter has been inited twice", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || nullptr == ddl_writer || DDL_MB_INVALID_TYPE == block_type || 0 == task_id || !ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key), K(block_type), K(task_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ddl_kv_mgr_handle.get_obj()->get_ls_id(), ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), KPC(ddl_kv_mgr_handle.get_obj()));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               ddl_kv_mgr_handle.get_obj()->get_tablet_id(),
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), KPC(ddl_kv_mgr_handle.get_obj()));
  } else {
    block_type_ = block_type;
    table_key_ = table_key;
    ddl_writer_ = ddl_writer;
    task_id_ = task_id;
    tablet_handle_ = tablet_handle;
    ddl_kv_mgr_handle_ = ddl_kv_mgr_handle;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLRedoLogWriterCallback::write(const ObMacroBlockHandle &macro_handle,
                                      const ObLogicMacroBlockId &logic_id,
                                      char *buf,
                                      const int64_t buf_len,
                                      const int64_t data_seq)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogWriterCallback is not inited", K(ret));
  } else if (OB_FAIL(prepare_block_buffer_if_need())) {
    LOG_WARN("prepare block buffer failed", K(ret));
  } else {
    macro_block_id_ = macro_handle.get_macro_id();
    redo_info_.table_key_ = table_key_;
    redo_info_.data_buffer_.assign(buf, buf_len);
    redo_info_.block_type_ = block_type_;
    redo_info_.logic_id_ = logic_id;
    redo_info_.start_scn_ = ddl_writer_->get_start_scn();
    if (OB_FAIL(ddl_writer_->write_redo_log(redo_info_, macro_block_id_, true/*allow remote write*/, task_id_, tablet_handle_, ddl_kv_mgr_handle_))) {
      LOG_WARN("fail to write ddl redo log", K(ret));
    }
  }
  return ret;
}

int ObDDLRedoLogWriterCallback::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogWriterCallback is not inited", K(ret));
  } else if (OB_FAIL(ddl_writer_->wait_redo_log_finish(redo_info_, macro_block_id_))) {
    LOG_WARN("fail to wait redo log finish", K(ret));
  }
  return ret;
}

int ObDDLRedoLogWriterCallback::prepare_block_buffer_if_need()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(block_buffer_)) {
    block_buffer_ = static_cast<char *>(ob_malloc(OB_SERVER_BLOCK_MGR.get_macro_block_size(), ObMemAttr(MTL_ID(), "DDL_REDO_CB")));
    if (nullptr == block_buffer_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for block bufffer failed", K(ret));
    }
  }
  return ret;
}
