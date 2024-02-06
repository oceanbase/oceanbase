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

#include "ob_ls_mgr.h"
#include "lib/guard/ob_shared_guard.h"        // ObShareGuard
#include "lib/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "share/backup/ob_backup_struct.h"    // ObBackupPathString
#include "share/ob_debug_sync.h"              // DEBUG
#include "storage/ls/ob_ls.h"                 // ObLS
#include "storage/tx_storage/ob_ls_map.h"     // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "logservice/ob_log_service.h"        // ObLogService
#include "logservice/palf_handle_guard.h"     // PalfHandleGuard
#include "ob_archive_allocator.h"             // ObArchiveAllocator
#include "ob_archive_sequencer.h"             // ObArchiveSequencer
#include "ob_archive_persist_mgr.h"           // ObArchivePersistMgr
#include "ob_archive_util.h"                  // GET_LS_TASK_CTX
#include "ob_archive_round_mgr.h"             // ObArchiveRoundMgr
#include <stdint.h>

namespace oceanbase
{
namespace archive
{
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::palf;

int ObArchiveLSMgr::iterate_ls(const std::function<int (const ObLSArchiveTask &)> &func)
{
  auto get_ls_archive_stat_func = [&func](const ObLSID &id, ObLSArchiveTask *task) -> bool {
    int ret = OB_SUCCESS;
    bool bret = true;
    if (OB_FAIL(func(*task))) {
      bret = false;
      ARCHIVE_LOG(WARN, "iter ls archive stat failed", K(ret));
    }
    return bret;
  };
  return ls_map_.for_each(get_ls_archive_stat_func);
}

// 检查是否需要做归档
// 按照强leader 或者 backup zone配置
bool check_ls_need_do_archive(const common::ObRole role)
{
  return is_strong_leader(role);
}

class ObArchiveLSMgr::CheckDeleteFunctor
{
public:
  CheckDeleteFunctor(const ArchiveKey &key,
      const bool is_in_archive,
      ObLSService *ls_svr,
      ObLogService *log_service) :
    key_(key),
    is_in_archive_(is_in_archive),
    ls_svr_(ls_svr),
    log_service_(log_service) {}

  bool operator()(const ObLSID &id, ObLSArchiveTask *task)
  {
    int ret = OB_SUCCESS;
    bool bret = false;
    palf::PalfHandleGuard palf_handle_guard;
    ArchiveWorkStation station;
    common::ObRole role;
    int64_t epoch = OB_INVALID_TIMESTAMP;

    if (! is_in_archive_) {
      bret = true;
      ARCHIVE_LOG(INFO, "tenant not in archive, need gc ls archive task", K(id));
    } else if (OB_ISNULL(ls_svr_) || OB_ISNULL(log_service_)) {
      bret = true;
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "ls_svr_ or log_service_ is NULL", K(ret), K(ls_svr_), K(log_service_));
    } else if (OB_FAIL(log_service_->open_palf(id, palf_handle_guard))) {
      bret = true;
      ARCHIVE_LOG(WARN, "open ls failed, gc ls archive task", K(ret), K(id));
    } else if (OB_FAIL(palf_handle_guard.get_role(role, epoch))) {
      ARCHIVE_LOG(WARN, "get role failed", K(ret), K(id));
    } else if (! check_ls_need_do_archive(role)) {
      bret = true;
      ARCHIVE_LOG(INFO, "check_ls_need_do_archive return false, gc ls archive task", K(id), K(epoch), K(role));
    } else {
      // TODO: fake lease
      ObArchiveLease lease(epoch, 0, 0);
      ArchiveWorkStation station(key_, lease);
      bret = ! task->check_task_valid(station);
    }
    return bret;
  }

private:
  ArchiveKey key_;
  bool is_in_archive_;
  ObLSService *ls_svr_;
  ObLogService *log_service_;
};

class ObArchiveLSMgr::ClearArchiveTaskFunctor
{
public:
  ClearArchiveTaskFunctor() = default;
  ~ClearArchiveTaskFunctor() {}
public:
  bool operator()(const ObLSID &id, ObLSArchiveTask *task)
  {
    int ret = OB_SUCCESS;
    bool bret = true;
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "task is NULL", KR(ret), K(id), K(task));
    } else {
      task->destroy();
      ARCHIVE_LOG(INFO, "destroy ls archive task succ", K(id));
    }
    return bret;
  }
};

ObArchiveLSMgr::ObArchiveLSMgr() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  round_start_scn_(),
  ls_map_(),
  last_print_ts_(OB_INVALID_TIMESTAMP),
  allocator_(NULL),
  round_mgr_(NULL),
  log_service_(NULL),
  persist_mgr_(NULL),
  cond_()
{}

ObArchiveLSMgr::~ObArchiveLSMgr()
{
  destroy();
  ARCHIVE_LOG(INFO, "ObArchiveLSMgr destroy");
}

int ObArchiveLSMgr::init(const uint64_t tenant_id,
    ObLogService *log_service,
    storage::ObLSService *ls_svr,
    ObArchiveAllocator *allocator,
    ObArchiveSequencer *sequencer,
    ObArchiveRoundMgr *round_mgr,
    ObArchivePersistMgr *persist_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "ls archive mgr init twice", K(ret));
  } else if (OB_ISNULL(log_service_ = log_service)
      || OB_ISNULL(ls_svr_ = ls_svr)
      || OB_ISNULL(allocator_ = allocator)
      || OB_ISNULL(sequencer_ = sequencer)
      || OB_ISNULL(round_mgr_ = round_mgr)
      || OB_ISNULL(persist_mgr_ = persist_mgr)
      || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(log_service),
        K(ls_svr), K(allocator), K(sequencer), K(round_mgr), K(persist_mgr));
  } else if (OB_FAIL(ls_map_.init("ArchiveLSMap"))) {
    ARCHIVE_LOG(WARN, "ls_map init failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
  }
  return ret;
}

int ObArchiveLSMgr::start()
{
  int ret = OB_SUCCESS;
  ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(! inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveLSMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ObThreadPool::start())) {
    ARCHIVE_LOG(WARN, "ObArchiveLSMgr start fail", K(ret));
  } else {
    ARCHIVE_LOG(INFO, "ObArchiveLSMgr start succ");
  }
  return ret;
}

void ObArchiveLSMgr::stop()
{
  ARCHIVE_LOG(INFO, "ObArchiveLSMgr stop");
  ObThreadPool::stop();
}

void ObArchiveLSMgr::wait()
{
  ARCHIVE_LOG(INFO, "ObArchiveLSMgr wait");
  ObThreadPool::wait();
}


int ObArchiveLSMgr::set_archive_info(
    const SCN &round_start_scn,
    const int64_t piece_interval_us,
    const SCN &genesis_scn,
    const int64_t base_piece_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!round_start_scn.is_valid()
        || piece_interval_us <= 0
        || !genesis_scn.is_valid()
        || base_piece_id < 1)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(round_start_scn),
        K(piece_interval_us), K(genesis_scn), K(base_piece_id));
  } else {
    round_start_scn_ = round_start_scn;
    piece_interval_ = piece_interval_us;
    genesis_scn_ = genesis_scn;
    base_piece_id_ = base_piece_id;
    ARCHIVE_LOG(INFO, "ls mgr set archive info succ", K(round_start_scn),
        K(piece_interval_us), K(genesis_scn), K(base_piece_id));
  }
  return ret;
}

void ObArchiveLSMgr::clear_archive_info()
{
    round_start_scn_.reset();
    piece_interval_ = 0;
    genesis_scn_.reset();
    base_piece_id_ = 0;
    ARCHIVE_LOG(INFO, "ls mgr clear info succ");
}

void ObArchiveLSMgr::notify_start()
{
  cond_.signal();
  ARCHIVE_LOG(INFO, "ls mgr notify start succ");
}

void ObArchiveLSMgr::notify_stop()
{
  ARCHIVE_LOG(INFO, "ls mgr notify stop succ");
}

void ObArchiveLSMgr::destroy()
{
  inited_ = false;
  stop();
  wait();
  reset_task();
  tenant_id_ = OB_INVALID_TENANT_ID;
  round_start_scn_.reset();
  ls_map_.destroy();
  allocator_ = NULL;
  log_service_ = NULL;
  ls_svr_ = NULL;
  persist_mgr_ = NULL;
}

int ObArchiveLSMgr::revert_ls_task(ObLSArchiveTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ls archive mgr not init", K(ret));
  } else if (NULL != task) {
    ls_map_.revert(task);
  }
  return ret;
}

int ObArchiveLSMgr::get_ls_guard(const ObLSID &id, ObArchiveLSGuard &guard)
{
  int ret = OB_SUCCESS;
  ObLSArchiveTask *task = NULL;
  if (OB_UNLIKELY(! inited_)) {
    ARCHIVE_LOG(WARN, "ObArchiveLSMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ls_map_.get(id, task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "ls_map_ get fail", KR(ret), K(id));
    }
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "task is NULL", K(id), KR(ret));
  } else {
    guard.set_ls_task(task);
  }
  return ret;
}

// 提供对外添加归档任务接口
// 单租户日志流数量有限, 不再提供额外添加日志流归档任务逻辑, 只是唤醒工作线程
int ObArchiveLSMgr::authorize_ls_archive_task(const ObLSID &id,
    const int64_t epoch,
    const SCN &start_scn)
{
  UNUSED(id);
  UNUSED(epoch);
  UNUSED(start_scn);
  cond_.signal();
  return OB_SUCCESS;
}

void ObArchiveLSMgr::run1()
{
  ARCHIVE_LOG(INFO, "ObArchiveLSMgr thread start");
  lib::set_thread_name("LSArchiveMgr");
  ObCurTraceId::init(GCONF.self_addr_);

  if (OB_UNLIKELY(! inited_)) {
    ARCHIVE_LOG_RET(ERROR, OB_NOT_INIT, "LSArchiveMgr not init");
  } else {
    while (! has_set_stop()) {
      int64_t begin_tstamp = ObTimeUtility::current_time();
      do_thread_task_();
      int64_t end_tstamp = ObTimeUtility::current_time();
      int64_t wait_interval = THREAD_RUN_INTERVAL - (end_tstamp - begin_tstamp);
      if (wait_interval > 0) {
        cond_.timedwait(wait_interval);
      }
    }
  }
}

// 添加归档任务
// 清理非法的归档任务, 其中一期非leader, 二期非leader授权为非法归档任务
void ObArchiveLSMgr::do_thread_task_()
{
  ArchiveKey key;
  share::ObArchiveRoundState state;
  round_mgr_->get_archive_round_info(key, state);
  // not only in doing state but also in interrupt or suspend state, ls mgr should work,
  // otherwise ls archive state can not persist in table successfully
  const bool is_in_doing = state.is_doing() || state.is_interrupted() || state.is_suspend();
  gc_stale_ls_task_(key, is_in_doing);

  DEBUG_SYNC(BEFORE_ARCHIVE_ADD_LS_TASK);

  if (is_in_doing) {
    add_ls_task_();
  }
}

// 提供添加归档任务(一期非backup zone模式)
void ObArchiveLSMgr::add_ls_task_()
{
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLSMgr not init", KR(ret));
  } else if (OB_FAIL(ls_svr_->get_ls_iter(guard, ObLSGetMod::ARCHIVE_MOD))) {
    ARCHIVE_LOG(WARN, "get ls iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "iter is NULL", K(ret), K(iter));
  } else {
    ObLS *ls = NULL;
    bool is_valid = false;
    int64_t ls_count = 0;
    bool is_add = true;
    ArchiveKey key;
    common::ObRole role;
    int64_t epoch = OB_INVALID_TIMESTAMP;
    share::ObArchiveRoundState state;
    round_mgr_->get_archive_round_info(key, state);
    while (OB_SUCC(ret) && ! has_set_stop()) {
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          ARCHIVE_LOG(WARN, "iter get next failed", K(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "ls is NULL", K(ret), K(ls));
      } else {
        palf::PalfHandleGuard palf_handle_guard;
        const ObLSID &ls_id = ls->get_ls_id();
        //只有普通租户非私有日志流需要做归档
        if (OB_FAIL(log_service_->open_palf(ls_id, palf_handle_guard))) {
          ARCHIVE_LOG(WARN, "open ls failed", K(ret), K(ls_id));
        } else if (OB_FAIL(palf_handle_guard.get_role(role, epoch))) {
          ARCHIVE_LOG(WARN, "get role failed", K(ret), KPC(ls));
        } else if (check_ls_need_do_archive(role)) {
          if (OB_FAIL(check_ls_archive_task_valid_(key, ls, is_valid))) {
            ARCHIVE_LOG(WARN, "check_ls_task_exist_ fail", KR(ret), KPC(ls));
          } else if (is_valid) {
            // just ship it
          } else if (OB_FAIL(add_task_(ls_id, key, epoch))) {
            ARCHIVE_LOG(WARN, "add_task_ fail", KR(ret), K(ls_id));
          } else {
            sequencer_->signal();
          }
        } else {
          ARCHIVE_LOG(INFO, "ls not need do archive on this server", K(ls_id));
        }
      }
    } // while
    // 迭代完全部日志流
    if (OB_ITER_END != ret) {
      ret = OB_SUCCESS;
    }
  }
}

//TODO 由于epoch未来会被lease替换 此时暂时重复获取get_role
int ObArchiveLSMgr::check_ls_archive_task_valid_(const ArchiveKey &key,
    ObLS *ls,
    bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObArchiveLSGuard guard(this);
  ObLSArchiveTask *task = NULL;
  const ObLSID &id = ls->get_ls_id();
  common::ObRole role;
  int64_t epoch = OB_INVALID_TIMESTAMP;
  is_valid = true;

  if (OB_FAIL(get_ls_guard(id, guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_ls_archive_task_guard fail", KR(ret), K(id));
    } else {
      is_valid = false;
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(task = guard.get_ls_task())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task is NULL", KR(ret), K(id));
  } else if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(id, role, epoch))) {
    ARCHIVE_LOG(WARN, "get role failed", K(ret), KPC(ls));
  } else {
    // TODO: 获取日志流epoch
    // fake lease后两个参数
    ObArchiveLease lease(epoch, 0, 0);
    ArchiveWorkStation station(key, lease);
    is_valid = task->check_task_valid(station);
  }
  return ret;
}

int ObArchiveLSMgr::add_task_(const ObLSID &id,
    const ArchiveKey &key,
    const int64_t epoch)
{
  int ret = OB_SUCCESS;
  const SCN &min_scn = round_start_scn_;

  //TODO fake lease
  ObArchiveLease lease(epoch, 0, 0);
  ArchiveWorkStation station(key, lease);
  StartArchiveHelper helper(id, tenant_id_, station, min_scn, piece_interval_,
      genesis_scn_, base_piece_id_, persist_mgr_);
  if (OB_FAIL(helper.handle())) {
    ARCHIVE_LOG(WARN, "start archive helper handle failed", KR(ret), K(helper));
  } else if (OB_FAIL(insert_or_update_ls_(helper))) {
    ARCHIVE_LOG(WARN, "insert or update ls task failed", KR(ret), K(helper));
  } else {
    ARCHIVE_LOG(INFO, "add ls archive task succ", K(id));
  }

  if (OB_FAIL(ret)) {
    (void)ls_map_.del(id);
  }
  return ret;
}

int ObArchiveLSMgr::insert_or_update_ls_(const StartArchiveHelper &helper)
{
  int ret = OB_SUCCESS;
  const ObLSID id = helper.get_ls_id();

  if (OB_UNLIKELY(! helper.is_valid())) {
    ARCHIVE_LOG(WARN, "helper is not valid", KR(ret), K(helper));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ENTRY_EXIST == (ret = ls_map_.contains_key(id))) {
    // update ls task info
    ret = OB_SUCCESS;
    GET_LS_TASK_CTX(this, id) {
      ls_archive_task->update_ls_task(helper);
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    ObLSArchiveTask *ls_task = NULL;
    //const bool mandatory = true;
    if (OB_FAIL(ls_map_.alloc_value(ls_task))) {
      ARCHIVE_LOG(WARN, "alloc_value fail", K(ret), K(id));
    } else if (OB_ISNULL(ls_task)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(WARN, "ls_task is NULL", KR(ret), K(id));
    } else {
      if (OB_FAIL(ls_task->init(helper, allocator_))) {
        ARCHIVE_LOG(WARN, "ls_task init fail", KR(ret), K(helper));
      } else if (OB_FAIL(ls_map_.insert_and_get(id, ls_task))) {
        ARCHIVE_LOG(WARN, "ls_map_ insert_and_get fail", KR(ret),
            K(id), KPC(ls_task));
      } else {
        // make sure to revert here, otherwise there will be a memory/ref leak
        ls_map_.revert(ls_task);
        ls_task = NULL;
      }
    }

    if (OB_FAIL(ret) && NULL != ls_task) {
      (void)ls_map_.del(id);
      // corresponding to (insert and get), add one reference
      ls_map_.free_value(ls_task);
      ls_task = NULL;
    }
  } else {
    // 其他返回码
    ARCHIVE_LOG(WARN, "ls_map_ contains_key fail", KR(ret), K(id));
  }

  return ret;
}

void ObArchiveLSMgr::gc_stale_ls_task_(const ArchiveKey &key, const bool is_in_doing)
{
  int ret = OB_SUCCESS;
  CheckDeleteFunctor functor(key, is_in_doing, ls_svr_, log_service_);

  if (OB_FAIL(ls_map_.remove_if(functor))) {
    ARCHIVE_LOG(WARN, "ls_map for each failed", KR(ret));
  } else {
    ARCHIVE_LOG(INFO, "gc stale ls task succ");
  }
}

// 释放ls_task资源, 以及从map里删除每个task
void ObArchiveLSMgr::reset_task()
{
  int ret = OB_SUCCESS;
  ClearArchiveTaskFunctor functor;

  if (OB_FAIL(ls_map_.for_each(functor))) {
    ARCHIVE_LOG(WARN, "for_each failed, memory maybe leak", KR(ret));
  }

  ls_map_.reset();
}

int ObArchiveLSMgr::mark_fatal_error(const ObLSID &id,
    const ArchiveKey &key,
    const ObArchiveInterruptReason &reason)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!id.is_valid() || ! key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguement", K(ret), K(ret), K(reason));
  } else {
    GET_LS_TASK_CTX(this, id) {
      if (OB_FAIL(ls_archive_task->mark_error(key))) {
        ARCHIVE_LOG(WARN, "ls archive task mark fatal error failed", K(ret), K(id), K(key), K(reason));
      } else if (OB_FAIL(round_mgr_->mark_fatal_error(id, key, reason))) {
        ARCHIVE_LOG(WARN, "archive round mgr  mark fatal error failed", K(ret),
            K(id), K(key), K(reason));
      }
    }
  }

  return ret;
}

int ObArchiveLSMgr::print_tasks()
{
  int ret = OB_SUCCESS;
  auto print_task_func = [&](const ObLSID &id, ObLSArchiveTask *task) -> bool {
    int ret = OB_SUCCESS;
    if (OB_FAIL(task->print_self())) {
      ARCHIVE_LOG(WARN, "print self failed", K(ret));
    }
    return true;
  };
  const int64_t cur_ts = common::ObTimeUtility::fast_current_time();
  if (cur_ts - last_print_ts_ > DEFAULT_PRINT_INTERVAL) {
    ret = ls_map_.for_each(print_task_func);
    last_print_ts_ = cur_ts;
  }
  return ret;
}

} // namespace archive
} // namespace oceanbase
