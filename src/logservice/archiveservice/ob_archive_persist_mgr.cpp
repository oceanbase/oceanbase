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

#include "ob_archive_persist_mgr.h"
#include <cstdint>
#include "common/ob_role.h"                     // ObRole
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"     // ObMySQLProxy
#include "lib/ob_errno.h"
#include "lib/string/ob_sql_string.h"           // ObSqlString
#include "lib/ob_define.h"                      // gen_meta_tenant_id
#include "lib/guard/ob_shared_guard.h"          // ObShareGuard
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_archive_define.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_archive_struct.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_ls_id.h"                     // ObLSID
#include "logservice/ob_log_service.h"          // ObLogService
#include "share/backup/ob_tenant_archive_mgr.h" // ObTenantArchiveMgr
#include "storage/tx_storage/ob_ls_map.h"       // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h"   // ObLSService
#include "ob_ls_mgr.h"                          // ObArchiveLSMgr
#include "ob_archive_round_mgr.h"               // ObArchiveRoundMgr
#include "ob_archive_util.h"                    // GET_LS_TASK_CTX
#include "storage/tx_storage/ob_ls_handle.h"    // ObLSHandle
#include "share/ls/ob_ls_recovery_stat_operator.h"         // ObLSRecoveryStatOperator

#define STAT(level, fmt, args...) ARCHIVE_LOG(level, "[LOG_STREAM] [ARCHIVE_PROGRESS] " fmt, ##args);
#define STAT_RET(level, errcode, fmt, args...) ARCHIVE_LOG_RET(level, errcode, "[LOG_STREAM] [ARCHIVE_PROGRESS] " fmt, ##args);
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define WSTAT(fmt, args...) STAT(WARN, fmt, ##args)
#define WSTAT_RET(errcode, fmt, args...) STAT_RET(WARN, errcode, fmt, ##args)
#define TSTAT(fmt, args...) STAT(TRACE, fmt, ##args)

namespace oceanbase
{
namespace archive
{
using namespace oceanbase::palf;
using namespace oceanbase::share;
using namespace oceanbase::storage;

class ObArchivePersistMgr::DeleteStaleLSFunctor
{
  public:
    DeleteStaleLSFunctor(ObLSService *ls_svr) : ls_svr_(ls_svr) {}
    bool operator()(const ObLSID &id, ObArchivePersistValue *value)
    {
      int ret = OB_SUCCESS;
      bool bret = false;
      ObLSHandle handle;
      if (OB_FAIL(ls_svr_->get_ls(id, handle, ObLSGetMod::ARCHIVE_MOD))) {
        ARCHIVE_LOG(WARN, "get_ls failed, remove archive persist value", K(ret), K(id));
        bret = true;
      }
      return true;
    }
  private:
    ObLSService *ls_svr_;
};

ObArchivePersistMgr::ObArchivePersistMgr() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  tenant_key_(),
  dest_no_(-1),
  state_(),
  state_rwlock_(common::ObLatchIds::ARCHIVE_PERSIST_MGR_LOCK),
  proxy_(NULL),
  ls_svr_(NULL),
  ls_mgr_(NULL),
  round_mgr_(NULL),
  last_update_ts_(OB_INVALID_TIMESTAMP),
  map_()
{}

ObArchivePersistMgr::~ObArchivePersistMgr()
{
  destroy();
}

int ObArchivePersistMgr::init(const uint64_t tenant_id,
    common::ObMySQLProxy *proxy,
    storage::ObLSService *ls_svr,
    ObArchiveLSMgr *ls_mgr,
    ObArchiveRoundMgr *round_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "archive persist mgr init twice", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(proxy)
      || OB_ISNULL(ls_mgr)
      || OB_ISNULL(round_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "archive persist mgr init failed", K(ret), K(tenant_id),
        K(proxy), K(ls_svr), K(ls_mgr), K(round_mgr));
  } else if (OB_FAIL(table_operator_.init(tenant_id))) {
    ARCHIVE_LOG(WARN, "table operator init failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(map_.init("ArcPersistMap"))) {
    ARCHIVE_LOG(WARN, "map init failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    proxy_ = proxy;
    ls_svr_ = ls_svr;
    ls_mgr_ = ls_mgr;
    round_mgr_ = round_mgr;
    inited_ = true;
  }
  return ret;
}

void ObArchivePersistMgr::destroy()
{
  if (inited_) {
    map_.destroy();
    tenant_id_ = OB_INVALID_TENANT_ID;
    tenant_key_.reset();
    last_update_ts_ = OB_INVALID_TIMESTAMP;
    proxy_ = NULL;
    ls_svr_ = NULL;
    ls_mgr_ = NULL;
    round_mgr_ = NULL;
    inited_ = false;
  }
}

int ObArchivePersistMgr::get_ls_archive_progress(const ObLSID &id, LSN &lsn, SCN &scn, bool &force, bool &ignore)
{
  int ret = OB_SUCCESS;
  ArchiveKey key;
  ObArchivePersistValue *value = NULL;
  ObLSArchivePersistInfo info;
  bool is_madatory = false;
  int64_t unused_speed = 0;
  ignore = false;
  force = false;
  ObArchiveRoundState state;
  {
    RLockGuard guard(state_rwlock_);
    key.incarnation_ = tenant_key_.incarnation_;;
    key.dest_id_ = tenant_key_.dest_id_;
    key.round_ = tenant_key_.round_;
    state = state_;
  }
  if (OB_UNLIKELY(! id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(id));
  } else if (! state_in_archive_(state)) {
    // archive not in beginning or doing or suspend state, just ignore it
    ignore = true;
  } else if (OB_FAIL(map_.get(id, value))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // archive progress not exist, need retry
      ret = OB_EAGAIN;
      ARCHIVE_LOG(WARN, "archive progress not exist, need retry", K(ret), K(id));
    } else {
      ARCHIVE_LOG(WARN, "get archive progress failed", K(ret), K(id));
    }
  } else if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "archive progress value is NULL", K(ret), K(id), K(value));
  } else {
    value->get(is_madatory, unused_speed, info);
    if (ArchiveKey(info.incarnation_, info.key_.dest_id_, info.key_.round_id_) != key) {
      ret = OB_EAGAIN;
      ARCHIVE_LOG(WARN, "ls archive progress not match with tenant, need retry",
          K(key), K(is_madatory), K(info));
    } else if (info.state_.is_interrupted()) {
      ignore = true;
      ARCHIVE_LOG(WARN, "ls archive progress is interrupted, skip it", K(id), K(info));
    } else {
      lsn = palf::LSN(info.lsn_);
      scn = info.checkpoint_scn_;
      force = is_madatory;
    }
    map_.revert(value);
  }
  return ret;
}

int ObArchivePersistMgr::get_ls_archive_speed(const ObLSID &id, int64_t &speed, bool &force, bool &ignore)
{
  int ret = OB_SUCCESS;
  ObArchivePersistValue *value = NULL;
  ObLSArchivePersistInfo info;
  bool is_madatory = false;
  ArchiveKey key;
  ignore = false;
  force = false;
  ObArchiveRoundState state;
  {
    RLockGuard guard(state_rwlock_);
    key.incarnation_ = tenant_key_.incarnation_;;
    key.dest_id_ = tenant_key_.dest_id_;
    key.round_ = tenant_key_.round_;
    state = state_;
  }
  speed = 0;
  if (OB_UNLIKELY(! id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(id));
  } else if (! state_in_archive_(state)) {
    // archive not in beginning or doing or suspend state, just ignore it
    ignore = true;
  } else if (OB_FAIL(map_.get(id, value))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // archive progress not exist, need retry
      ret = OB_EAGAIN;
    } else {
      ARCHIVE_LOG(WARN, "get archive progress failed", K(ret), K(id));
    }
  } else if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "archive progress value is NULL", K(ret), K(id), K(value));
  } else {
    value->get(is_madatory, speed, info);
    if (ArchiveKey(info.incarnation_, info.key_.dest_id_, info.key_.round_id_) != key) {
      ret = OB_EAGAIN;
      ARCHIVE_LOG(WARN, "ls archive progress not match with tenant, need retry",
          K(key), K(is_madatory), K(speed), K(info));
    } else {
      force = is_madatory;
    }
    map_.revert(value);
  }
  return ret;
}

int ObArchivePersistMgr::get_archive_persist_info(const ObLSID &id,
    const ArchiveKey &key,
    ObLSArchivePersistInfo &info)
{
  int ret = OB_SUCCESS;
  bool record_exist = false;
  if (OB_UNLIKELY(! id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(id));
  } else if (OB_FAIL(load_ls_archive_progress_(id, key, info, record_exist))) {
    ARCHIVE_LOG(WARN, "load ls archive progress failed", K(ret), K(id));
  } else if (! record_exist) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ARCHIVE_LOG(INFO, "get archive persist info succ", K(info));
  }
  return ret;
}

int ObArchivePersistMgr::check_tenant_in_archive(bool &in_archive)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr attr;
  in_archive = true;
  if (OB_FAIL(load_archive_round_attr(attr))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      in_archive = false;
    } else {
      ARCHIVE_LOG(WARN, "load archive round attr failed", K(ret));
    }
  } else if (OB_UNLIKELY(! attr.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "tenant archive round attr is invalid", K(ret), K(attr));
  } else {
    in_archive = attr.state_.is_beginning()
      || attr.state_.is_doing()
      || attr.state_.is_prepare()
      || attr.state_.is_suspending()
      || attr.state_.is_suspend();
  }
  return ret;
}

int ObArchivePersistMgr::check_and_get_piece_persist_continuous(const ObLSID &id,
    ObLSArchivePersistInfo &info)
{
  int ret = OB_SUCCESS;
  ObArchivePersistValue *value = NULL;
  if (OB_UNLIKELY(! id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(id));
  } else if (OB_FAIL(map_.get(id, value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get failed", K(ret), K(id));
    }
  } else if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    info = value->info_;
    map_.revert(value);
  }
  return ret;
}

int ObArchivePersistMgr::load_archive_round_attr(ObTenantArchiveRoundAttr &attr)
{
  int ret = OB_SUCCESS;
  share::ObArchivePersistHelper helper;
  common::ObSEArray<std::pair<int64_t, int64_t>, 1> dest_array;
  if (OB_FAIL(helper.init(tenant_id_))) {
    ARCHIVE_LOG(WARN, "archive persist helper init failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(helper.get_valid_dest_pairs(*proxy_, dest_array))) {
    ARCHIVE_LOG(WARN, "get valid dest pair failed", K(ret), K(tenant_id_));
  } else {
    if (dest_array.count() > 1) {
      ret = OB_NOT_SUPPORTED;
      ARCHIVE_LOG(ERROR, "archive dest count more than one, not support", K(ret), K(tenant_id_), K(dest_array));
    } else if (dest_array.count() == 0) {
      ret = OB_ENTRY_NOT_EXIST;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
        ARCHIVE_LOG(INFO, "no archive dest exist, just skip", K(ret), K(tenant_id_));
      }
    } else {
      const int64_t dest_id = dest_array.at(0).second;
      const int64_t need_lock = true;
      if (OB_FAIL(helper.get_round_by_dest_id(*proxy_, dest_id, need_lock, attr))) {
        ARCHIVE_LOG(WARN, "get dest round failed", K(ret), K(tenant_id_), K(dest_id));
      } else {
        WLockGuard guard(state_rwlock_);
        tenant_key_.incarnation_ = attr.incarnation_;
        tenant_key_.dest_id_ = attr.dest_id_;
        tenant_key_.round_ = attr.round_id_;
        dest_no_ = dest_array.at(0).first;
        state_ = attr.state_;
        ARCHIVE_LOG(TRACE, "get dest round succ", K(tenant_id_), K(dest_id), K(attr));
      }
    }
  }
  return ret;
}

void ObArchivePersistMgr::persist_and_load()
{
  persist_archive_progress_();
  load_archive_progress_(tenant_key_);
}

int ObArchivePersistMgr::persist_archive_progress_()
{
  int ret = OB_SUCCESS;
  ArchiveKey key;
  ObArchiveRoundState state;
  ObTenantArchiveRoundAttr attr;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  bool need_do = false;

  if (OB_FAIL(load_archive_round_attr(attr))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "load archive round attr failed", K(ret), K(attr));
    }
  } else if (OB_UNLIKELY(! attr.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "round attr is not valid", K(ret), K(attr));
  } else if (OB_FAIL(check_round_state_if_do_persist_(attr, key, state, need_do))) {
    // just skip it
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      ARCHIVE_LOG(INFO, "server is in diff round", K(key), K(attr));
    }
  } else if (! need_do) {
  } else if (OB_FAIL(ls_svr_->get_ls_iter(guard, ObLSGetMod::ARCHIVE_MOD))) {
    ARCHIVE_LOG(WARN, "get ls iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "iter is NULL", K(ret), K(iter));
  } else {
    ObLS *ls = NULL;
    ObLSArchivePersistInfo info;
    bool exist = false;
    while (OB_SUCC(ret) && ! lib::Thread::current().has_set_stop()) {
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          ARCHIVE_LOG(WARN, "get next ls failed", K(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "ls is NULL", K(ret), K(ls));
      } else if (! check_persist_authority_(ls->get_ls_id())) {
        // do nothing
      } else if (OB_FAIL(build_ls_archive_info_(ls->get_ls_id(), key, info, exist))) {
        ARCHIVE_LOG(WARN, "build ls archive info failed", K(ret));
      } else if (OB_FAIL(do_persist_(ls->get_ls_id(), exist, info))) {
        ARCHIVE_LOG(WARN, "do persist failed", KPC(ls), K(info));
        ret = OB_SUCCESS;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

bool ObArchivePersistMgr::check_persist_authority_(const ObLSID &id) const
{
  int ret = OB_SUCCESS;
  bool bret = false;
  common::ObRole role;
  int64_t epoch = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(id, role, epoch))) {
     ARCHIVE_LOG(WARN, "get role failed", K(ret), K(id));
  } else {
    bret = is_strong_leader(role);
  }
  return bret;
}

int ObArchivePersistMgr::check_round_state_if_do_persist_(const ObTenantArchiveRoundAttr &attr,
    ArchiveKey &key,
    ObArchiveRoundState &state,
    bool &need_do)
{
  int ret = OB_SUCCESS;
  need_do = true;
  if (FALSE_IT(round_mgr_->get_archive_round_info(key, state))) {
  } else if (key != ArchiveKey(attr.incarnation_, attr.dest_id_, attr.round_id_)) {
    need_do = false;
    ARCHIVE_LOG(INFO, "server in diff round, just skip", K(key), K(attr));
  } else if (! need_persist_(attr.state_)) {
    need_do = false;
  }
  return ret;
}

int ObArchivePersistMgr::build_ls_archive_info_(const ObLSID &id,
    const ArchiveKey &key,
    ObLSArchivePersistInfo &info,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  info.reset();
  GET_LS_TASK_CTX(ls_mgr_, id) {
    if (OB_FAIL(ls_archive_task->get_max_archive_info(key, info))) {
      ARCHIVE_LOG(WARN, "get max archive info failed", K(ret), K(key), KPC(ls_archive_task));
    } else if (OB_UNLIKELY(! info.is_valid())) {
      ARCHIVE_LOG(WARN, "max archive info is invalid, just skip it", K(id), K(key), K(info));
    } else {
      exist = true;
    }
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObArchivePersistMgr::check_ls_archive_progress_advance_(const ObLSID &id,
    const ObLSArchivePersistInfo &info,
    bool &advanced)
{
  int ret = OB_SUCCESS;
  ObArchivePersistValue *value = NULL;
  advanced = false;
  if (OB_FAIL(map_.get(id, value))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ARCHIVE_LOG(INFO, "archive progress not exist in local", K(id));
      ret = OB_SUCCESS;
      advanced = true;
    } else {
      ARCHIVE_LOG(WARN, "map get failed", K(ret), K(id));
    }
  } else if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "value is NULL", K(ret), K(id), K(value));
  } else {
    advanced = value->info_.incarnation_ != info.incarnation_
      || value->info_.key_.round_id_ != info.key_.round_id_
      || value->info_.checkpoint_scn_ < info.checkpoint_scn_
      || value->info_.state_.status_ != info.state_.status_;
    map_.revert(value);
  }
  return ret;
}

int ObArchivePersistMgr::load_archive_progress_(const ArchiveKey &key)
{
  int ret = OB_SUCCESS;
  bool is_madatory = false;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  if (! key.is_valid()) {
    // just skip
  } else if (OB_FAIL(ls_svr_->get_ls_iter(guard, ObLSGetMod::ARCHIVE_MOD))) {
    ARCHIVE_LOG(WARN, "get ls iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "iter is NULL", K(ret), K(iter));
  } else if (OB_FAIL(load_dest_mode_(is_madatory))) {
    ARCHIVE_LOG(WARN, "load dest mode failed", K(ret));
  } else {
    ObLS *ls = NULL;
    ObLSArchivePersistInfo info;
    bool record_exist = false;
    while (OB_SUCC(ret) && ! lib::Thread::current().has_set_stop()) {
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          ARCHIVE_LOG(WARN, "get next ls failed", K(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "ls is NULL", K(ret), K(ls));
      } else if (OB_FAIL(load_ls_archive_progress_(
              ls->get_ls_id(), key, info, record_exist))) {
        ARCHIVE_LOG(WARN, "load ls archive progress failed", K(ret), K(key), KPC(ls));
      } else if (! record_exist) {
      } else if (OB_FAIL(update_local_archive_progress_(ls->get_ls_id(), is_madatory, info))) {
        ARCHIVE_LOG(WARN, "update local archive progress failed", K(ret), K(info));
      }
    } // while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObArchivePersistMgr::load_dest_mode_(bool &is_madatory)
{
  int ret = OB_SUCCESS;
  const bool need_lock = false;
  share::ObArchivePersistHelper helper;
  share::ObLogArchiveDestAtrr::Binding binding;
  if (OB_FAIL(helper.init(tenant_id_))) {
    ARCHIVE_LOG(WARN, "archive persist helper init failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(helper.get_binding(*proxy_, need_lock, dest_no_, binding))) {
    ARCHIVE_LOG(WARN, "get archive dest binding failed", K(ret));
  } else {
    is_madatory = binding == share::ObLogArchiveDestAtrr::Binding::MANDATORY;
  }
  return ret;
}

int ObArchivePersistMgr::clear_stale_ls_()
{
  int ret = OB_SUCCESS;
  DeleteStaleLSFunctor functor(ls_svr_);
  if (OB_FAIL(map_.remove_if(functor))) {
    ARCHIVE_LOG(WARN, "remove_if failed", K(ret));
  } else {
    ARCHIVE_LOG(TRACE, "clear stale ls succ");
  }
  return ret;
}

bool ObArchivePersistMgr::state_in_archive_(const share::ObArchiveRoundState &state) const
{
  return state.is_beginning() || state.is_doing() || state.is_suspending() || state.is_suspend();
}

int ObArchivePersistMgr::load_ls_archive_progress_(const ObLSID &id,
    const ArchiveKey &key,
    ObLSArchivePersistInfo &info,
    bool &record_exist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_operator_.get_latest_ls_archive_progress(*proxy_, key.dest_id_,
    key.round_, id, info, record_exist))) {
    ARCHIVE_LOG(WARN, "get ls archive progress failed", K(ret), K(id));
  }
  return ret;
}

int ObArchivePersistMgr::update_local_archive_progress_(const ObLSID &id,
    const bool is_madatory, const ObLSArchivePersistInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(info));
  } else if (OB_ENTRY_EXIST == (ret = map_.contains_key(id))) {
    ret = OB_SUCCESS;
    ObArchivePersistValue *value = NULL;
    if (OB_FAIL(map_.get(id, value))) {
      ARCHIVE_LOG(WARN, "get info failed", K(ret), K(id));
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "value is NULL", K(ret), K(id), K(value));
    } else if (OB_FAIL(value->set(is_madatory, info))) {
      ARCHIVE_LOG(WARN, "persist value set failed", K(ret), K(id), K(info));
    } else {
      map_.revert(value);
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    ObArchivePersistValue *value = NULL;
    if (OB_FAIL(map_.alloc_value(value))) {
      ARCHIVE_LOG(WARN, "alloc_value fail", K(ret), K(id));
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(WARN, "value is NULL", K(ret), K(id), K(value));
    } else if (OB_FAIL(value->set(is_madatory, info))) {
      ARCHIVE_LOG(WARN, "value set failed", K(ret), K(id), K(info));
    } else if (OB_FAIL(map_.insert_and_get(id, value))) {
      ARCHIVE_LOG(WARN, "insert and get failed", K(ret), K(id), K(value));
    } else {
      // insert_and_get必须与revert成对使用
      map_.revert(value);
      value = NULL;
    }

    if (OB_FAIL(ret) && NULL != value) {
      map_.del(id);
      map_.free_value(value);
      value = NULL;
    }
  } else {
    ARCHIVE_LOG(WARN, "contains_key failed", K(ret), K(id));
  }
  return ret;
}

int ObArchivePersistMgr::do_persist_(const ObLSID &id, const bool exist, ObLSArchivePersistInfo &info)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  int64_t begin_ts = ObTimeUtility::current_time();
  const bool need_lock = true;     // select for update
  bool need_commit = false;
  ObMySQLTransaction trans;
  ObTenantArchiveRoundAttr current_round;
  if (need_wipe_suspend_status_(state_) && OB_FAIL(do_wipe_suspend_status_(id))) {
    // tenant in beginning state, try wipe suspend state in all pieces for this ls
    ARCHIVE_LOG(WARN, "do wipe suspend state failed", K(ret), K(id));
  } else if (OB_FAIL(trans.start(proxy_, gen_meta_tenant_id(tenant_id_)))) {
    ARCHIVE_LOG(WARN, "trans start failed", K(ret));
  } else if (OB_FAIL(table_operator_.get_round_by_dest_id(trans, tenant_key_.dest_id_, need_lock, current_round))) {
    ARCHIVE_LOG(WARN, "get log archive round status failed", K(ret));
  } else if (! need_persist_(current_round.state_)) {
    // do nothing
  } else if (! need_stop_status_(current_round.state_)
      && ! need_suspend_status_(current_round.state_)) {
    // 1. 当租户归档状态处于BEGINNING / DOING, 有日志流归档进度则持久化该进度
    bool record_exist = false;
    ObLSArchivePersistInfo persist_info;
    if (! exist) {
      // skip
    } else if (OB_FAIL(table_operator_.get_latest_ls_archive_progress(trans, current_round.dest_id_,
      current_round.round_id_, id, persist_info, record_exist))) {
      ARCHIVE_LOG(WARN, "get ls archive progress failed", K(ret), K(id));
    } else if (! record_exist || ! persist_info.is_valid()
        || persist_info.key_.piece_id_ < info.key_.piece_id_
        || (persist_info.key_.piece_id_ == info.key_.piece_id_ && persist_info.lsn_ < info.lsn_)
        || (info.state_.is_interrupted() && ! persist_info.state_.is_interrupted())) {
      // 需要更新归档进度场景
      // 1. 不存在已持久化归档进度
      // 2. 已持久化归档进度无效
      // 3. 已持久化归档进度piece比当前值小
      // 4. piece相同, 已持久化归档进度piece比当前值小
      // 5. 当前归档断流, 而持久化归档进度非断流状态
      if (exist && OB_FAIL(table_operator_.insert_ls_archive_progress(trans, info, affected_rows))) {
        ARCHIVE_LOG(WARN, "insert ls archive progress failed", K(ret), K(id), K(info));
      } else {
        need_commit = true;
      }
    }
  } else if (need_stop_status_(current_round.state_)) {
    // 2. 归档状态处于STOPPING状态, 则设置该日志流所有piece归档状态为STOP
    if (OB_FAIL(table_operator_.set_ls_archive_stop(trans, current_round.dest_id_,
      current_round.round_id_, id, affected_rows))) {
      ARCHIVE_LOG(WARN, "set_ls_archive_stop failed", K(ret), K(id), K(current_round));
    } else {
      need_commit = true;
      ARCHIVE_LOG(INFO, "set_ls_archive_stop succ", K(ret), K(id), K(current_round), K(affected_rows));
    }
  } else {
    // 3. tenant archive in SUSPENDING status, set all piece status SUSPEND for this ls
    if (OB_FAIL(table_operator_.set_ls_archive_suspend(trans, current_round.dest_id_,
            current_round.round_id_, id, affected_rows))) {
      ARCHIVE_LOG(WARN, "set_ls_archive_suspend failed", K(ret), K(id), K(current_round));
    } else {
      need_commit = true;
      ARCHIVE_LOG(INFO, "set_ls_archive_suspend succ", K(ret), K(id), K(current_round), K(affected_rows));
    }
  }

  if (OB_SUCC(ret) && need_commit) {
    if (OB_FAIL(trans.end(true))) {
      ARCHIVE_LOG(WARN, "trans commit failed", K(ret), K(id), K(info));
    } else {
      int64_t total_time = ObTimeUtility::current_time() - begin_ts;
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
        ISTAT("persist archive progress succ", K(info), K(total_time));
      }
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      ARCHIVE_LOG(WARN, "failed to end trans", K(tmp_ret), K(id), K(info));
    } else {
      ARCHIVE_LOG(TRACE, "trans rollback succ");
    }
  }
  return ret;
}

int ObArchivePersistMgr::do_wipe_suspend_status_(const ObLSID &id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  bool need_commit = false;
  ObMySQLTransaction trans;
  ObTenantArchiveRoundAttr current_round;
  const bool need_lock = true;     // select for update
  ObLSArchivePersistInfo persist_info;
  bool record_exist = false;
  if (OB_FAIL(trans.start(proxy_, gen_meta_tenant_id(tenant_id_)))) {
    ARCHIVE_LOG(WARN, "trans start failed", K(ret));
  } else if (OB_FAIL(table_operator_.get_round_by_dest_id(trans, tenant_key_.dest_id_, need_lock, current_round))) {
    ARCHIVE_LOG(WARN, "get log archive round status failed", K(ret));
  } else if (! need_wipe_suspend_status_(current_round.state_)) {
    //do nothing
  } else if (OB_FAIL(table_operator_.get_latest_ls_archive_progress(trans, current_round.dest_id_,
          current_round.round_id_, id, persist_info, record_exist))) {
    ARCHIVE_LOG(WARN, "get ls archive progress failed", K(ret), K(id));
  } else if (! persist_info.state_.is_suspend()) {
    // do nothing
  } else if (OB_FAIL(table_operator_.set_ls_archive_doing(trans, current_round.dest_id_,
          current_round.round_id_, id, affected_rows))) {
    ARCHIVE_LOG(WARN, "insert ls archive progress failed", K(ret), K(id), K(persist_info));
  } else {
    need_commit = true;
  }

  if (OB_SUCC(ret) && need_commit) {
    if (OB_FAIL(trans.end(true))) {
      ARCHIVE_LOG(WARN, "trans commit failed", K(ret), K(id));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      ARCHIVE_LOG(WARN, "failed to end trans", K(tmp_ret), K(id));
    }
  }
  return ret;
}

bool ObArchivePersistMgr::need_persist_(const ObArchiveRoundState &state) const
{
  return state.is_beginning() || state.is_doing() || state.is_stopping() || state.is_interrupted() || state.is_suspending();
}

bool ObArchivePersistMgr::need_stop_status_(const ObArchiveRoundState &state) const
{
  return state.is_stopping();
}

bool ObArchivePersistMgr::need_suspend_status_(const ObArchiveRoundState &state) const
{
  return state.is_suspending();
}

bool ObArchivePersistMgr::need_wipe_suspend_status_(const ObArchiveRoundState &state) const
{
  return state.is_beginning();
}

int ObArchivePersistMgr::get_ls_create_scn(const ObLSID &id, SCN &scn)
{
  int ret = OB_SUCCESS;
  share::ObLSRecoveryStat stat;
  const bool need_for_update = false;
  share::ObLSRecoveryStatOperator op;
  if (OB_FAIL(op.get_ls_recovery_stat(tenant_id_, id, need_for_update, stat, *proxy_))) {
    ARCHIVE_LOG(WARN, "get ls reocvery stat failed", K(ret));
  } else if (OB_UNLIKELY(! stat.is_valid())) {
    ret = OB_EAGAIN;
    ARCHIVE_LOG(WARN, "ls recovery stat is not valid, just wait retry", K(id), K(stat), K(ret));
  } else {
    scn = stat.get_create_scn();
  }
  return ret;
}

void ObArchivePersistValue::get(bool &is_madatory,
    int64_t &speed,
    ObLSArchivePersistInfo &info)
{
  RLockGuard guard(rwlock_);
  is_madatory = is_madatory_;
  speed = speed_;
  info = info_;
}

int ObArchivePersistValue::set(const bool is_madatory, const ObLSArchivePersistInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(info));
  } else {
    WLockGuard guard(rwlock_);
    const int64_t cur_time = ObTimeUtility::current_time();
    if (OB_INVALID_TIMESTAMP == last_update_ts_) {
    } else {
      speed_ = static_cast<int64_t>(info.lsn_ - info_.lsn_) / std::max(1L, cur_time - last_update_ts_);
    }
    is_madatory_ = is_madatory;
    info_ = info;
    last_update_ts_ = cur_time;
  }
  return ret;
}
} // namespace archive
} // namespace oceanbas
