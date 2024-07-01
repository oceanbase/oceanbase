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
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tablet/ob_tablet.h"
#include "share/ob_ddl_sim_point.h"
#include "share/ob_ddl_common.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::archive;
using namespace oceanbase::blocksstable;
using namespace oceanbase::logservice;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;

bool ObDDLFullNeedStopWriteChecker::check_need_stop_write()
{
  return ddl_kv_mgr_handle_.get_obj()->get_count() >= ObTabletDDLKvMgr::MAX_DDL_KV_CNT_IN_STORAGE - 1;
}

bool ObDDLIncNeedStopWriteChecker::check_need_stop_write()
{
  return tablet_.get_memtable_count() >= common::MAX_MEMSTORE_CNT - 1;
}

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
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
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
  } else if (OB_FAIL(ls_svr->get_ls(ls_id_, handle, ObLSGetMod::DDL_MOD))) {
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
  ObDDLNeedStopWriteChecker &checker,
  int64_t &real_sleep_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  real_sleep_us = 0;

  bool is_need_stop_write = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (next_available_ts <= 0 || OB_INVALID_TENANT_ID == tenant_id || task_id == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(next_available_ts), K(tenant_id), K(task_id));
  } else if (OB_FAIL(DDL_SIM(MTL_ID(), task_id, DDL_REDO_WRITER_SPEED_CONTROL_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(MTL_ID()), K(task_id));
  } else if (OB_TMP_FAIL(check_need_stop_write(checker, is_need_stop_write))) {
    LOG_WARN("fail to check need stop write", K(tmp_ret));
  }
  if (OB_FAIL(ret)) {
  } else if (is_need_stop_write) /*clog disk used exceeds threshold*/ {
    int64_t loop_cnt = 0;
    while (OB_SUCC(ret) && is_need_stop_write) {
      ob_usleep(SLEEP_INTERVAL);
      if (0 == loop_cnt % 100) {
        uint64_t unused_data_format_version = 0;
        int64_t unused_snapshot_version = 0;
        share::ObDDLTaskStatus task_status = share::ObDDLTaskStatus::PREPARE;
        if (OB_TMP_FAIL(ObDDLUtil::get_data_information(tenant_id, task_id, unused_data_format_version,
            unused_snapshot_version, task_status))) {
          if (OB_ITER_END == tmp_ret) {
            is_need_stop_write = false;
            LOG_INFO("exit due to ddl task exit", K(tenant_id), K(task_id));
          } else if (loop_cnt >= 100 * 1000) { // wait_time = 100 * 1000 * SLEEP_INTERVAL = 100s.
            is_need_stop_write = false;
            LOG_INFO("exit due to sql exceeds time limit", K(tmp_ret), K(tenant_id), K(task_id));
          } else {
            if (REACH_COUNT_INTERVAL(1000L)) {
              LOG_WARN("get ddl task info failed", K(tmp_ret), K(tenant_id), K(task_id));
            }
          }
        } else if (!is_replica_build_ddl_task_status(task_status)) {
          is_need_stop_write = false;
          LOG_INFO("exit due to mismatched status", K(tenant_id), K(task_id));
        }
      }
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        ObTaskController::get().allow_next_syslog();
        FLOG_INFO("stop write ddl clog", K(ret), K(ls_id_),
          K(write_speed_), K(need_stop_write_), K(ref_cnt_),
          K(disk_used_stop_write_threshold_));
      }
      if (is_need_stop_write && OB_TMP_FAIL(check_need_stop_write(checker, is_need_stop_write))) {
        LOG_WARN("fail to check need stop write", K(tmp_ret));
      }
      loop_cnt++;
    }
  }
  if (OB_SUCC(ret)) {
    real_sleep_us = std::max(0L, next_available_ts - ObTimeUtility::current_time());
    ob_usleep(real_sleep_us);
  }
  return ret;
}

int ObDDLCtrlSpeedItem::check_need_stop_write(ObDDLNeedStopWriteChecker &checker,
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
        is_need_stop_write = (checker.check_need_stop_write() || need_stop_write_);
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
  ObDDLNeedStopWriteChecker &checker,
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
  } else if (OB_FAIL(do_sleep(next_available_ts, tenant_id, task_id, checker, real_sleep_us))) {
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
                                          ObDDLNeedStopWriteChecker &checker,
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
  } else if (OB_FAIL(DDL_SIM(tenant_id, task_id, WRITE_DUPLICATED_DDL_REDO_LOG))) {
    LOG_WARN("ddl sim remote write", K(ret), K(tenant_id), K(task_id));
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
                                                        checker,
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
      if (OB_FAIL(MTL(ObLSService *)->get_ls(speed_handle_key.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
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

ObDDLRedoLock::ObDDLRedoLock() : is_inited_(false), bucket_lock_()
{
}

ObDDLRedoLock::~ObDDLRedoLock()
{
}

ObDDLRedoLock &ObDDLRedoLock::get_instance()
{
  static ObDDLRedoLock instance;
  return instance;
}

int ObDDLRedoLock::init()
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

int ObDDLRedoLogWriter::local_write_ddl_macro_redo(
    const ObDDLMacroBlockRedoInfo &redo_info,
    const share::ObLSID &ls_id,
    const int64_t task_id,
    logservice::ObLogHandler *log_handler,
    const blocksstable::MacroBlockId &macro_block_id,
    char *buffer,
    ObDDLRedoLogHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObDDLRedoLog log;
  const enum ObReplayBarrierType replay_barrier_type = ObReplayBarrierType::NO_NEED_BARRIER;
  logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::DDL_LOG_BASE_TYPE,
                                          replay_barrier_type);
  ObDDLClogHeader ddl_header(ObDDLClogType::DDL_REDO_LOG);
  int64_t buffer_size = 0;
  int64_t pos = 0;
  ObDDLMacroBlockClogCb *cb = nullptr;
  ObDDLRedoLog tmp_log;
  int64_t log_start_pos = 0;

  palf::LSN lsn;
  const bool need_nonblock= false;
  const bool allow_compression = false;
  SCN base_scn = SCN::min_scn();
  SCN scn;
  int64_t real_sleep_us = 0;
  int tmp_ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ddl_kv_mgr_handle.reset();
  if (OB_UNLIKELY(!redo_info.is_valid()
                  || nullptr == log_handler
                  || OB_INVALID_TENANT_ID == tenant_id
                  || nullptr == buffer
                  || 0 == task_id
                  || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(redo_info), KP(log_handler), KP(buffer), K(task_id), K(ls_id), K(tenant_id));
  } else if (OB_FAIL(log.init(redo_info))) {
    LOG_WARN("fail to init DDLRedoLog", K(ret), K(redo_info));
  } else if (FALSE_IT(buffer_size = base_header.get_serialize_size()
                                    + ddl_header.get_serialize_size()
                                    + log.get_serialize_size())) {
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (OB_FAIL(ls->get_tablet(log.get_redo_info().table_key_.tablet_id_, tablet_handle, ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(log.get_redo_info()));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("create ddl kv mgr failed", K(ret));
  } else {
    ObDDLFullNeedStopWriteChecker checker(ddl_kv_mgr_handle);
    if (OB_TMP_FAIL(ObDDLCtrlSpeedHandle::get_instance().limit_and_sleep(tenant_id, ls_id, buffer_size, task_id, checker, real_sleep_us))) {
      LOG_WARN("fail to limit and sleep", K(tmp_ret), K(tenant_id), K(task_id), K(ls_id), K(buffer_size), K(real_sleep_us));
    }
  }
  if (OB_FAIL(ret)) {
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
  } else if (OB_FAIL(cb->init(ls_id, tmp_log.get_redo_info(), macro_block_id, tablet_handle))) {
    LOG_WARN("init ddl clog callback failed", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id, task_id, DDL_REDO_WRITER_WRITE_MACRO_LOG_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(log_handler->append(buffer,
                                         buffer_size,
                                         base_scn,
                                         need_nonblock,
                                         allow_compression,
                                         cb,
                                         lsn,
                                         scn))) {
    LOG_WARN("fail to submit ddl redo log", K(ret), K(buffer), K(buffer_size));
  } else {
    handle.cb_ = cb;
    cb = nullptr;
    handle.scn_ = scn;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != cb) {
      op_free(cb);
      cb = nullptr;
    }
  }
  return ret;
}

int ObDDLRedoLogWriter::local_write_ddl_start_log(
    const ObDDLStartLog &log,
    ObLSHandle &ls_handle,
    ObLogHandler *log_handler,
    ObDDLKvMgrHandle &ddl_kv_mgr_handle,
    ObDDLKvMgrHandle &lob_kv_mgr_handle,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    uint32_t &lock_tid,
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
  const bool allow_compression = false;
  SCN scn = SCN::min_scn();
  bool is_external_consistent = false;
  ObDDLRedoLockGuard guard(log.get_table_key().get_tablet_id().hash());
  if (OB_ISNULL(cb = op_alloc(ObDDLStartClogCb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(cb->init(log.get_table_key(), log.get_data_format_version(), log.get_execution_id(),
    ddl_kv_mgr_handle, lob_kv_mgr_handle, direct_load_mgr_handle, lock_tid))) {
    LOG_WARN("failed to init cb", K(ret));
  } else if (OB_FAIL(base_header.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("failed to serialize log base header", K(ret));
  } else if (OB_FAIL(ddl_header.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("fail to seriaize ddl start log", K(ret));
  } else if (OB_FAIL(log.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("fail to seriaize ddl start log", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_ddl_log_handler()->add_tablet(log.get_table_key().get_tablet_id()))) {
    LOG_WARN("add tablet failed", K(ret), "tablet_id", log.get_table_key().get_tablet_id());
  } else if (lob_kv_mgr_handle.is_valid() && OB_FAIL(ls_handle.get_ls()->get_ddl_log_handler()->add_tablet(lob_kv_mgr_handle.get_obj()->get_tablet_id()))) {
    LOG_WARN("add lob tablet failed", K(ret), "lob_tablet_id", lob_kv_mgr_handle.get_obj()->get_tablet_id());
  } else if (OB_FAIL(log_handler->append(buffer,
                                         buffer_size,
                                         SCN::min_scn(),
                                         need_nonblock,
                                         allow_compression,
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
    start_scn = scn;
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
    tmp_cb->try_release(); // release the memory no matter succ or not
  }
  if (OB_FAIL(ret)) {
    if (nullptr != cb) {
      op_free(cb);
      cb = nullptr;
    }
  }
  return ret;
}

int ObDDLRedoLogWriter::local_write_ddl_commit_log(
    const ObDDLCommitLog &log,
    const ObDDLClogType clog_type,
    const share::ObLSID &ls_id,
    ObLogHandler *log_handler,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    ObTabletDirectLoadMgrHandle &lob_direct_load_mgr_handle,
    ObDDLCommitLogHandle &handle,
    uint32_t &lock_tid)
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
  const bool allow_compression = false;
  SCN base_scn = SCN::min_scn();
  SCN scn = SCN::min_scn();
  bool is_external_consistent = false;
if (OB_ISNULL(buffer = static_cast<char *>(ob_malloc(buffer_size, ObMemAttr(MTL_ID(), "DDL_COMMIT_LOG"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_ISNULL(cb = op_alloc(ObDDLCommitClogCb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(cb->init(ls_id, log.get_table_key().tablet_id_, log.get_start_scn(), lock_tid, direct_load_mgr_handle, lob_direct_load_mgr_handle))) {
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
                                         allow_compression,
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
    } else {
      tmp_cb->try_release(); // release the memory
    }
  }
  if (nullptr != buffer) {
    ob_free(buffer);
    buffer = nullptr;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != cb) {
      op_free(cb);
      cb = nullptr;
    }
  }
  return ret;
}

bool ObDDLRedoLogWriter::need_retry(int ret_code)
{
  return OB_NOT_MASTER == ret_code;
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

int ObDDLRedoLogWriter::remote_write_ddl_macro_redo(
    const int64_t task_id,
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
    if (OB_FAIL(arg.init(MTL_ID(), leader_ls_id_, redo_info, task_id))) {
      LOG_WARN("fail to init ObRpcRemoteWriteDDLRedoLogArg", K(ret));
    } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr_).by(MTL_ID()).remote_write_ddl_redo_log(arg))) {
      LOG_WARN("fail to remote write ddl redo log", K(ret), K_(leader_addr), K(arg));
    }
  }
  return ret;
}

ObDDLRedoLogWriter::ObDDLRedoLogWriter()
  : is_inited_(false), remote_write_(false),
    ls_id_(), tablet_id_(), ddl_redo_handle_(), leader_addr_(), leader_ls_id_(), buffer_(nullptr)
{
}

int ObDDLRedoLogWriter::init(const ObLSID &ls_id, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ddl redo log writer has been inited twice", K(ret));
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

void ObDDLRedoLogWriter::reset()
{
  is_inited_ = false;
  remote_write_ = false;
  ls_id_.reset();
  tablet_id_.reset();
  ddl_redo_handle_.reset();
  leader_addr_.reset();
  leader_ls_id_.reset();
}

int ObDDLRedoLogWriter::write_start_log(
    const ObITable::TableKey &table_key,
    const int64_t execution_id,
    const uint64_t data_format_version,
    const ObDirectLoadType direct_load_type,
    ObDDLKvMgrHandle &ddl_kv_mgr_handle,
    ObDDLKvMgrHandle &lob_kv_mgr_handle,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    uint32_t &lock_tid,
    SCN &start_scn)
{
  int ret = OB_SUCCESS;
  ObDDLStartLog log;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  start_scn.set_min();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl redo log writer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || execution_id < 0 || data_format_version <= 0 || !is_valid_direct_load(direct_load_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key), K(execution_id), K(data_format_version), K(direct_load_type));
  } else if (OB_FAIL(log.init(table_key, data_format_version, execution_id, direct_load_type,
          lob_kv_mgr_handle.is_valid() ? lob_kv_mgr_handle.get_obj()->get_tablet_id() : ObTabletID()))) {
    LOG_WARN("fail to init DDLStartLog", K(ret), K(table_key), K(execution_id), K(data_format_version));
  }  else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), K(table_key));
  /*} else if (OB_FAIL(DDL_SIM(MTL_ID(), ddl_task_id, DDL_REDO_WRITER_WRITE_START_LOG_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(MTL_ID()), K(ddl_task_id));*/
  } else if (OB_FAIL(local_write_ddl_start_log(log, ls_handle, ls->get_log_handler(),
      ddl_kv_mgr_handle, lob_kv_mgr_handle, direct_load_mgr_handle, lock_tid, start_scn))) {
    LOG_WARN("fail to write ddl start log", K(ret), K(table_key));
  } else {
  /*SERVER_EVENT_ADD("ddl", "ddl write start log",
    "tenant_id", MTL_ID(),
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "task_id", ddl_task_id,
    "tablet_id", tablet_id_,
    "start_scn", start_scn);
    LOG_INFO("write ddl start log", K(ret), K(table_key), K(start_scn));*/
  }
  return ret;
}

int ObDDLRedoLogWriter::write_macro_block_log(
    const ObDDLMacroBlockRedoInfo &redo_info,
    const blocksstable::MacroBlockId &macro_block_id,
    const bool allow_remote_write,
    const int64_t task_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const int64_t BUF_SIZE = 2 * 1024 * 1024 + 16 * 1024;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl redo log writer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!redo_info.is_valid() || 0 == task_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(redo_info), K(task_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (nullptr == buffer_ && OB_ISNULL(buffer_ = static_cast<char *>(ob_malloc(BUF_SIZE, ObMemAttr(MTL_ID(), "DDL_REDO_LOG"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(BUF_SIZE));
  } else if (!remote_write_) {
    if (OB_FAIL(local_write_ddl_macro_redo(redo_info, ls->get_ls_id(), task_id, ls->get_log_handler(), macro_block_id, buffer_, ddl_redo_handle_))) {
      if (ObDDLUtil::need_remote_write(ret) && allow_remote_write) {
        if (OB_FAIL(switch_to_remote_write())) {
          LOG_WARN("fail to switch to remote write", K(ret));
        }
      } else {
        LOG_WARN("fail to write ddl redo clog", K(ret), K(MTL_GET_TENANT_ROLE_CACHE()));
      }
    } else {
      LOG_INFO("local write redo log of macro block", K(redo_info), K(macro_block_id));
    }
  }

  if (OB_SUCC(ret) && remote_write_) {
    if (OB_FAIL(retry_remote_write_macro_redo(task_id, redo_info))) {
      LOG_WARN("remote write redo failed", K(ret), K(task_id));
    } else {
      LOG_INFO("remote write redo log of macro block", K(redo_info), K(macro_block_id));
    }
  }
  return ret;
}

int ObDDLRedoLogWriter::wait_macro_block_log_finish(
    const ObDDLMacroBlockRedoInfo &redo_info,
    const blocksstable::MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  int64_t wait_timeout_us = MAX(ObDDLRedoLogHandle::DDL_REDO_LOG_TIMEOUT, GCONF._data_storage_io_timeout * 1);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl redo log writer has not been inited", K(ret));
  } else if (remote_write_) {
    // remote write no need to wait local handle
  } else if (OB_UNLIKELY(!ddl_redo_handle_.is_valid())) {
    // no redo log has been written yet
  } else if (OB_FAIL(ddl_redo_handle_.wait(wait_timeout_us))) {
    LOG_WARN("fail to wait io finish", K(ret));
  } else if (OB_FAIL(ddl_redo_handle_.cb_->get_ret_code())) {
    LOG_WARN("ddl redo callback executed failed", K(ret));
  } else {
    DEBUG_SYNC(AFTER_MACRO_BLOCK_WRITER_DDL_CALLBACK_WAIT);
  }
  ddl_redo_handle_.reset();
  return ret;
}

int ObDDLRedoLogWriter::write_commit_log_with_retry(
    const bool allow_remote_write,
    const ObITable::TableKey &table_key,
    const share::SCN &start_scn,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    ObTabletHandle &tablet_handle,
    SCN &commit_scn,
    bool &is_remote_write,
    uint32_t &lock_tid)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  const int64_t timeout_us = ObDDLRedoLogWriter::DEFAULT_RETRY_TIMEOUT_US;
  int64_t retry_count = 0;
  do {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(write_commit_log(allow_remote_write, table_key, start_scn, direct_load_mgr_handle, tablet_handle, commit_scn, is_remote_write, lock_tid))) {
      LOG_WARN("write ddl commit log failed", K(ret));
    }
    if (ObDDLRedoLogWriter::need_retry(ret)) {
      usleep(1000L * 1000L); // 1s
      ++retry_count;
      LOG_INFO("retry write ddl commit log", K(ret), K(table_key), K(retry_count));
    } else {
      break;
    }
  } while (ObTimeUtility::fast_current_time() - start_ts < timeout_us);
  return ret;
}

int ObDDLRedoLogWriter::write_commit_log(
    const bool allow_remote_write,
    const ObITable::TableKey &table_key,
    const share::SCN &start_scn,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    ObTabletHandle &tablet_handle,
    SCN &commit_scn,
    bool &is_remote_write,
    uint32_t &lock_tid)
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
  ObTabletBindingMdsUserData ddl_data;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl redo log writer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || !start_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key), K(start_scn));
  } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
    LOG_WARN("failed to get ddl data from tablet", K(ret), K(tablet_handle));
  } else if (OB_FAIL(log.init(table_key, start_scn, ddl_data.lob_meta_tablet_id_))) {
    LOG_WARN("fail to init DDLCommitLog", K(ret), K(table_key), K(start_scn), K(ddl_data.lob_meta_tablet_id_));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), K(table_key));
  } else if (start_scn != direct_load_mgr_handle.get_obj()->get_start_scn()) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("current task is restarted", K(ret), K(start_scn), "current_start_scn", direct_load_mgr_handle.get_obj()->get_start_scn());
  } else if (direct_load_mgr_handle.get_obj()->get_commit_scn(tablet_handle.get_obj()->get_tablet_meta()).is_valid_and_not_min()) {
    commit_scn = direct_load_mgr_handle.get_obj()->get_commit_scn(tablet_handle.get_obj()->get_tablet_meta());
    LOG_WARN("already committed", K(ret), K(start_scn), K(commit_scn), K(direct_load_mgr_handle.get_obj()->get_start_scn()), K(log));
  } else if (!remote_write_) {
    ObTabletBindingMdsUserData ddl_data;
    ObTabletDirectLoadMgrHandle lob_direct_load_mgr_handle;
    if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
      LOG_WARN("failed to get ddl data from tablet", K(ret), K(tablet_handle));
    } else if (ddl_data.lob_meta_tablet_id_.is_valid()) {
      bool is_lob_major_sstable_exist = false;
      if (OB_FAIL(MTL(ObTenantDirectLoadMgr *)->get_tablet_mgr_and_check_major(ls_id_, ddl_data.lob_meta_tablet_id_,
              true/* is_full_direct_load */, lob_direct_load_mgr_handle, is_lob_major_sstable_exist))) {
        if (OB_ENTRY_NOT_EXIST == ret && is_lob_major_sstable_exist) {
          ret = OB_SUCCESS;
          LOG_INFO("lob meta tablet exist major sstable, skip", K(ret), K(ddl_data.lob_meta_tablet_id_));
        } else {
          LOG_WARN("get tablet mgr failed", K(ret), K(ddl_data.lob_meta_tablet_id_));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(local_write_ddl_commit_log(
      log, ObDDLClogType::DDL_COMMIT_LOG, ls_id_, ls->get_log_handler(), direct_load_mgr_handle, lob_direct_load_mgr_handle, handle, lock_tid))) {
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
      LOG_INFO("local write ddl commit log", K(ret), K(table_key), K(commit_scn));
    }
  }
  if (OB_SUCC(ret) && remote_write_) {
    obrpc::ObRpcRemoteWriteDDLCommitLogArg arg;
    if (OB_FAIL(arg.init(MTL_ID(), leader_ls_id_, table_key, start_scn))) {
      LOG_WARN("fail to init ObRpcRemoteWriteDDLCommitLogArg", K(ret));
    } else if (OB_FAIL(retry_remote_write_commit_clog(arg, commit_scn))) {
      LOG_WARN("remote write ddl commit log failed", K(ret), K(arg));
    } else {
      is_remote_write = !(leader_addr_ == GCTX.self_addr());
      LOG_INFO("remote write ddl commit log", K(ret), K(table_key), K(commit_scn), K(is_remote_write));
    }
  }
  SERVER_EVENT_ADD("ddl", "ddl write commit log",
    "tenant_id", MTL_ID(),
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "start_scn", direct_load_mgr_handle.get_obj()->get_start_scn(),
    "tablet_id", tablet_id_,
    "commit_scn", commit_scn,
    is_remote_write);
  LOG_INFO("ddl write commit log", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}


int ObDDLRedoLogWriter::switch_to_remote_write()
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
    LOG_INFO("switch to remote write", K(ret), K_(tablet_id), K_(leader_ls_id), K_(leader_addr));
  }
  return ret;
}

int ObDDLRedoLogWriter::retry_remote_write_macro_redo(
    const int64_t task_id,
    const storage::ObDDLMacroBlockRedoInfo &redo_info)
{
  int ret = OB_SUCCESS;
  int retry_cnt = 0;
  const int64_t MAX_REMOTE_WRITE_RETRY_CNT = 800;
  if (OB_UNLIKELY(!redo_info.is_valid() || 0 == task_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(redo_info));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(switch_to_remote_write())) {
        LOG_WARN("flush ls leader location failed", K(ret));
      } else if (OB_FAIL(remote_write_ddl_macro_redo(task_id, redo_info))) {
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
  }
  return ret;
}

int ObDDLRedoLogWriter::retry_remote_write_commit_clog(
    const obrpc::ObRpcRemoteWriteDDLCommitLogArg &arg,
    share::SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  int retry_cnt = 0;
  const int64_t MAX_REMOTE_WRITE_RETRY_CNT = 800;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(switch_to_remote_write())) {
        LOG_WARN("flush ls leader location failed", K(ret));
      } else if (OB_FAIL(remote_write_ddl_commit_redo(arg, commit_scn))) {
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
  }
  return ret;
}


int ObDDLRedoLogWriter::remote_write_ddl_commit_redo(const obrpc::ObRpcRemoteWriteDDLCommitLogArg &arg, SCN &commit_scn)
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

ObDDLRedoLogWriter::~ObDDLRedoLogWriter()
{
  ddl_redo_handle_.reset();
  if (nullptr != buffer_) {
    ob_free(buffer_);
    buffer_ = nullptr;
  }
}

ObDDLRedoLogWriterCallback::ObDDLRedoLogWriterCallback()
  : is_inited_(false), redo_info_(), block_type_(ObDDLMacroBlockType::DDL_MB_INVALID_TYPE),
    table_key_(), macro_block_id_(), task_id_(0), data_format_version_(0),
    direct_load_type_(DIRECT_LOAD_INVALID), row_id_offset_(-1)
{
}

ObDDLRedoLogWriterCallback::~ObDDLRedoLogWriterCallback()
{
  (void)wait();
}

int ObDDLRedoLogWriterCallback::init(const share::ObLSID &ls_id,
                                     const ObTabletID &tablet_id,
                                     const ObDDLMacroBlockType block_type,
                                     const ObITable::TableKey &table_key,
                                     const int64_t task_id,
                                     const share::SCN &start_scn,
                                     const uint64_t data_format_version,
                                     const ObDirectLoadType direct_load_type,
                                     const int64_t row_id_offset/*=-1*/)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSService *ls_service = nullptr;
  bool is_cache_hit = false;
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ddl redo log writer has been inited twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !table_key.is_valid() ||
                         DDL_MB_INVALID_TYPE == block_type || 0 == task_id || data_format_version < 0 ||
                         !is_valid_direct_load(direct_load_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id), K(table_key), K(block_type), K(data_format_version),
        K(direct_load_type), K(task_id));
  } else if (OB_UNLIKELY(table_key.is_column_store_sstable() && row_id_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument of column group data", K(ret), K(table_key), K(row_id_offset));
  } else if (OB_FAIL(ddl_writer_.init(ls_id, tablet_id))) {
    LOG_WARN("fail to init ddl_writer_", K(ret), K(ls_id), K(tablet_id));
  } else {
    block_type_ = block_type;
    table_key_ = table_key;
    task_id_ = task_id;
    start_scn_ = start_scn;
    data_format_version_ = data_format_version;
    direct_load_type_ = direct_load_type;
    row_id_offset_ = row_id_offset;
    is_inited_ = true;
  }
  return ret;
}

void ObDDLRedoLogWriterCallback::reset()
{
  is_inited_ = false;
  redo_info_.reset();
  block_type_ = ObDDLMacroBlockType::DDL_MB_INVALID_TYPE;
  table_key_.reset();
  macro_block_id_.reset();
  ddl_writer_.reset();
  task_id_ = 0;
  start_scn_.reset();
  data_format_version_ = 0;
  direct_load_type_ = DIRECT_LOAD_INVALID;
  row_id_offset_ = -1;
}

bool ObDDLRedoLogWriterCallback::is_column_group_info_valid() const
{
  return table_key_.is_column_store_sstable() && row_id_offset_ >= 0;
}

int ObDDLRedoLogWriterCallback::write(ObMacroBlockHandle &macro_handle,
                                      const ObLogicMacroBlockId &logic_id,
                                      char *buf,
                                      const int64_t buf_len,
                                      const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogWriterCallback is not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_handle.is_valid() || !logic_id.is_valid() || nullptr == buf || row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_handle), K(logic_id), KP(buf), K(row_count));
  } else {
    macro_block_id_ = macro_handle.get_macro_id();
    redo_info_.table_key_ = table_key_;
    redo_info_.data_buffer_.assign(buf, buf_len);
    redo_info_.block_type_ = block_type_;
    redo_info_.logic_id_ = logic_id;
    redo_info_.start_scn_ = start_scn_;
    redo_info_.data_format_version_ = data_format_version_;
    redo_info_.type_ = direct_load_type_;
    if (is_column_group_info_valid()) {
      redo_info_.end_row_id_ = row_id_offset_ + row_count - 1;
      row_id_offset_ += row_count;
    }
    if (OB_FAIL(ddl_writer_.write_macro_block_log(redo_info_, macro_block_id_, true/*allow remote write*/, task_id_))) {
      LOG_WARN("fail to write ddl redo log", K(ret));
      if (ObDDLRedoLogWriter::need_retry(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(retry(ObDDLRedoLogWriter::DEFAULT_RETRY_TIMEOUT_US))) {
          LOG_WARN("retry wirte ddl macro redo log failed", K(ret), K(tmp_ret), K(task_id_), K(table_key_));
        } else {
          ret = OB_SUCCESS; // overwrite the return code
        }
      }
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
  } else if (OB_FAIL(ddl_writer_.wait_macro_block_log_finish(redo_info_, macro_block_id_))) {
    LOG_WARN("fail to wait redo log finish", K(ret));
  }
  return ret;
}

int ObDDLRedoLogWriterCallback::retry(const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogWriterCallback is not inited", K(ret));
  } else if (timeout_us <= 0) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeout less than 0", K(ret), K(timeout_us));
  } else if (OB_UNLIKELY(!macro_block_id_.is_valid() || !redo_info_.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("macro block id or redo info not valid", K(ret), K(macro_block_id_), K(redo_info_));
  } else {
    int64_t start_ts = ObTimeUtility::fast_current_time();
    while (ObTimeUtility::fast_current_time() - start_ts < timeout_us) { // ignore ret
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(ddl_writer_.write_macro_block_log(redo_info_, macro_block_id_, true/*allow remote write*/, task_id_))) {
        LOG_WARN("fail to write ddl redo log", K(ret));
      } else if (OB_FAIL(ddl_writer_.wait_macro_block_log_finish(redo_info_, macro_block_id_))) {
        LOG_WARN("wait ddl redo log finish failed", K(ret));
      } else {
        FLOG_INFO("retry write ddl macro redo success", K(ret), K(table_key_), K(macro_block_id_));
      }
      if (ObDDLRedoLogWriter::need_retry(ret)) {
        usleep(1000L * 1000L); // 1s
        ++retry_count;
        LOG_INFO("retry write ddl macro redo log", K(ret), K(table_key_), K(retry_count));
      } else {
        break;
      }
    }
  }
  return ret;
}

