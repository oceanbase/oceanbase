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

#include "palf_callback_wrapper.h"
#include "lib/ob_errno.h"
#include "logservice/palf/palf_callback.h"
#include "lib/net/ob_addr.h"
namespace oceanbase
{
namespace palf
{
PalfFSCbWrapper::PalfFSCbWrapper() : list_() {}
PalfFSCbWrapper::~PalfFSCbWrapper() {}

int PalfFSCbWrapper::add_cb_impl(PalfFSCbNode *cb_impl)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (false == list_.add_last(cb_impl)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    PALF_LOG(INFO, "PalfFSCbWrapper add_cb_impl success");
  }
  return ret;
}

void PalfFSCbWrapper::del_cb_impl(PalfFSCbNode *cb_impl)
{
  ObSpinLockGuard guard(lock_);
  (void)list_.remove(cb_impl);
}

int PalfFSCbWrapper::update_end_lsn(int64_t id, const LSN &end_lsn, const share::SCN &end_scn, const int64_t proposal_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(true == list_.is_empty())) {
    PALF_LOG(TRACE, "the block size callback list is empty", K(id), K(end_lsn));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObSpinLockGuard guard(lock_);
    DLIST_FOREACH(node, list_) {
      PalfFSCb *cb = node->fs_cb_;
      if (NULL == cb) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "PalfFSCb is NULL, unexpect error", KPC(node));
      } else if (OB_SUCCESS != (tmp_ret = cb->update_end_lsn(id, end_lsn, end_scn, proposal_id))) {
        PALF_LOG(ERROR, "update_end_lsn failed", K(tmp_ret), K(id), K(end_lsn), K(end_scn), K(proposal_id), KPC(node));
      }
    }
  }
  return ret;
}

PalfRoleChangeCbWrapper::PalfRoleChangeCbWrapper() : list_() {}
PalfRoleChangeCbWrapper::~PalfRoleChangeCbWrapper() {}

int PalfRoleChangeCbWrapper::add_cb_impl(PalfRoleChangeCbNode *cb_impl)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (false == list_.add_last(cb_impl)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    PALF_LOG(INFO, "PalfRoleChangeCbWrapper add_cb_impl success");
  }
  return ret;
}

void PalfRoleChangeCbWrapper::del_cb_impl(PalfRoleChangeCbNode *cb_impl)
{
  ObSpinLockGuard guard(lock_);
  if (NULL == list_.remove(cb_impl)) {
  } else {
    PALF_LOG(INFO, "PalfRoleChangeCbWrapper del_cb_impl success");
  }
}

int PalfRoleChangeCbWrapper::on_role_change(int64_t id)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(true == list_.is_empty())) {
    PALF_LOG(INFO, "the role change callback list is empty", K(id));
  } else {
    ObSpinLockGuard guard(lock_);
    DLIST_FOREACH(node, list_) {
      PalfRoleChangeCb *rc_cb = node->rc_cb_;
      if (NULL == rc_cb) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "PalfRoleChangeCb is NULL, unexpect error", K(ret), KPC(node));
      } else if (OB_FAIL(rc_cb->on_role_change(id))) {
        PALF_LOG(INFO, "on_role_change failed", K(ret), K(id), KPC(node));
      }
    }
  }
  return ret;
}

int PalfRoleChangeCbWrapper::on_need_change_leader(const int64_t id, const ObAddr &dest_addr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(true == list_.is_empty())) {
    PALF_LOG(INFO, "the role change callback list is empty", K(id), K(dest_addr));
  } else {
    ObSpinLockGuard guard(lock_);
    DLIST_FOREACH(node, list_) {
      PalfRoleChangeCb *rc_cb = node->rc_cb_;
      if (NULL == rc_cb) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "PalfRoleChangeCb is NULL, unexpect error", K(ret), KPC(node), K(id), K(dest_addr));
      } else if (OB_FAIL(rc_cb->on_need_change_leader(id, dest_addr))) {
        PALF_LOG(WARN, "on_need_change_leader failed", K(ret), K(id), KPC(node), K(dest_addr));
      }
    }
  }
  return ret;
}

PalfRebuildCbWrapper::PalfRebuildCbWrapper() : list_() {}
PalfRebuildCbWrapper::~PalfRebuildCbWrapper() {}

int PalfRebuildCbWrapper::add_cb_impl(PalfRebuildCbNode *cb_impl)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (false == list_.add_last(cb_impl)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    PALF_LOG(INFO, "PalfRebuildCbWrapper add_cb_impl success");
  }
  return ret;
}

void PalfRebuildCbWrapper::del_cb_impl(PalfRebuildCbNode *cb_impl)
{
  ObSpinLockGuard guard(lock_);
  if (NULL == list_.remove(cb_impl)) {
  } else {
    PALF_LOG(INFO, "PalfRebuildCbWrapper del_cb_impl success");
  }
}

int PalfRebuildCbWrapper::on_rebuild(const int64_t id, const LSN &lsn)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(true == list_.is_empty())) {
    PALF_LOG(INFO, "the role change callback list is empty", K(id));
  } else {
    ObSpinLockGuard guard(lock_);
    DLIST_FOREACH(node, list_) {
      PalfRebuildCb *rebuild_cb = node->rebuild_cb_;
      if (NULL == rebuild_cb) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "PalfRebuildCb is NULL, unexpect error", K(ret), KPC(node));
      } else if (OB_FAIL(rebuild_cb->on_rebuild(id, lsn))) {
        PALF_LOG(INFO, "on_rebuild failed", K(ret), K(id), K(lsn), KPC(node));
      }
    }
  }
  return ret;
}


LogPlugins::LogPlugins()
  : loc_lock_(),
    loc_cb_(NULL),
    palf_monitor_lock_(),
    palf_monitor_(NULL),
    palflite_monitor_lock_(),
    palflite_monitor_(NULL),
    locality_cb_lock_(),
    locality_cb_(NULL) { }

LogPlugins::~LogPlugins()
{
  destroy();
}

void LogPlugins::destroy()
{
  {
    common::RWLock::WLockGuard guard(loc_lock_);
    loc_cb_ = NULL;
  }
  {
    common::RWLock::WLockGuard guard(palf_monitor_lock_);
    palf_monitor_ = NULL;
  }
  {
    common::RWLock::WLockGuard guard(palflite_monitor_lock_);
    palflite_monitor_ = NULL;
  }
  {
    common::RWLock::WLockGuard guard(locality_cb_lock_);
    locality_cb_ = NULL;
  }
}

template<>
int LogPlugins::add_plugin(PalfLocationCacheCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(loc_lock_);
  if (OB_ISNULL(plugin)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "Palf plugin is NULL", KP(plugin));
  } else if (OB_NOT_NULL(loc_cb_)) {
    ret = OB_OP_NOT_ALLOW;
    PALF_LOG(INFO, "Palf plugin is not NULL", KP(plugin), KP_(loc_cb));
  } else {
    loc_cb_ = plugin;
    PALF_LOG(INFO, "add_plugin success", KP(plugin));
  }
  return ret;
}

template<>
int LogPlugins::del_plugin(PalfLocationCacheCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(loc_lock_);
  if (OB_NOT_NULL(loc_cb_)) {
    PALF_LOG(INFO, "del_plugin success", KP_(loc_cb));
    loc_cb_ = NULL;
  }
  return ret;
}

template<>
int LogPlugins::add_plugin(PalfMonitorCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(palf_monitor_lock_);
  if (OB_ISNULL(plugin)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "Palf plugin is NULL", KP(plugin));
  } else if (OB_NOT_NULL(palf_monitor_)) {
    ret = OB_OP_NOT_ALLOW;
    PALF_LOG(INFO, "Palf plugin is not NULL", KP(plugin), KP_(palf_monitor));
  } else {
    palf_monitor_ = plugin;
    PALF_LOG(INFO, "add_plugin success", KP(plugin));
  }
  return ret;
}

template<>
int LogPlugins::del_plugin(PalfMonitorCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(palf_monitor_lock_);
  if (OB_NOT_NULL(palf_monitor_)) {
    PALF_LOG(INFO, "del_plugin success", KP_(palf_monitor));
    palf_monitor_ = NULL;
  }
  return ret;
}

template<>
int LogPlugins::add_plugin(PalfLiteMonitorCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(palf_monitor_lock_);
  if (OB_ISNULL(plugin)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "Palf plugin is NULL", KP(plugin));
  } else if (OB_NOT_NULL(palflite_monitor_)) {
    ret = OB_OP_NOT_ALLOW;
    PALF_LOG(INFO, "Palf plugin is not NULL", KP(plugin), KP_(loc_cb));
  } else {
    palflite_monitor_ = plugin;
    PALF_LOG(INFO, "add_plugin success", KP(plugin));
  }
  return ret;
}

template<>
int LogPlugins::del_plugin(PalfLiteMonitorCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(palf_monitor_lock_);
  if (OB_NOT_NULL(palflite_monitor_)) {
    PALF_LOG(INFO, "del_plugin success", KP_(palflite_monitor));
    palflite_monitor_ = NULL;
  }
  return ret;
}

template<>
int LogPlugins::add_plugin(PalfLocalityInfoCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(locality_cb_lock_);
  if (OB_ISNULL(plugin)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "Palf plugin is NULL", KP(plugin));
  } else if (OB_NOT_NULL(locality_cb_)) {
    ret = OB_OP_NOT_ALLOW;
    PALF_LOG(INFO, "Palf plugin is not NULL", KP(plugin), KP_(locality_cb));
  } else {
    locality_cb_ = plugin;
    PALF_LOG(INFO, "add_plugin success", KP(plugin));
  }
  return ret;
}

template<>
int LogPlugins::del_plugin(PalfLocalityInfoCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(locality_cb_lock_);
  if (OB_NOT_NULL(locality_cb_)) {
    PALF_LOG(INFO, "del_plugin success", KP_(locality_cb));
    locality_cb_ = NULL;
  }
  return ret;
}

}; // end namespace palf
}; // end namespace oceanbase
