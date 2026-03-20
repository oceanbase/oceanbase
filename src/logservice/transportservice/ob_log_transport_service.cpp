/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_log_transport_service.h"
#include "logservice/ob_log_handler.h"
#include "logservice/palf/palf_iterator.h"
#include "logservice/ob_ls_adapter.h"
#include "logservice/replayservice/ob_log_replay_service.h"
#include "logservice/applyservice/ob_log_apply_service.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/thread/thread_mgr.h"
#include "logservice/palf/log_entry_header.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_server_struct.h"
#include "share/ob_log_restore_proxy.h"
#include "share/ob_sync_standby_dest_parser.h"
#include "share/ob_sync_standby_dest_operator.h"
#include "lib/random/ob_random.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/log_entry.h"
#include "share/ob_sync_standby_dest_operator.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;
using namespace lib;

namespace logservice
{
ObTransportServiceTask::ObTransportServiceTask()
  : lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
    type_(ObTransportServiceTaskType::INVALID_TRANSPORT_TASK),
    transport_status_(NULL),
    lease_()
{
}

ObTransportServiceTask::~ObTransportServiceTask()
{
  destroy();
}

void ObTransportServiceTask::reset()
{
  type_ = ObTransportServiceTaskType::INVALID_TRANSPORT_TASK;
  transport_status_ = NULL;
}

void ObTransportServiceTask::destroy()
{
  reset();
}

LogTransportStatus *ObTransportServiceTask::get_transport_status()
{
  return transport_status_;
}

bool ObTransportServiceTask::acquire_lease()
{
  return lease_.acquire();
}

bool ObTransportServiceTask::revoke_lease()
{
  return lease_.revoke();
}

ObTransportServiceTaskType ObTransportServiceTask::get_type() const
{
  return type_;
}

int ObLogTransportTask::init(void *log_buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_buf)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "log_buf is NULL", K(ret));
  }
  log_buf_ = static_cast<const char *>(log_buf);
  return ret;
}

ObTransportServiceSubmitTask::ObTransportServiceSubmitTask():
    ObTransportServiceTask(),
    base_lsn_(),
    base_scn_(),
    next_to_submit_lsn_(),
    next_to_submit_scn_(),
    iterator_()
{
  type_ = ObTransportServiceTaskType::TRANSPORT_SUBMIT_TASK;
}

ObTransportServiceSubmitTask::~ObTransportServiceSubmitTask()
{
  destroy();
}

void ObTransportServiceSubmitTask::reset()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  base_lsn_.reset();
  base_scn_.reset();
  next_to_submit_lsn_.reset();
  next_to_submit_scn_.reset();
  lease_.reset();
}

void ObTransportServiceSubmitTask::destroy()
{
  reset();
  iterator_.destroy();
  ObTransportServiceTask::destroy();
}

int ObTransportServiceSubmitTask::init(const share::ObLSID &ls_id,
                                       const palf::LSN &base_lsn,
                                       const share::SCN &base_scn,
                                       LogTransportStatus *transport_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(transport_status)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(transport_status));
  } else if (OB_UNLIKELY(!base_lsn.is_valid() || !base_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(base_lsn), K(base_scn));
  } else if (OB_FAIL(seek_log_iterator(ls_id, base_lsn, iterator_))) {
    CLOG_LOG(WARN, "seek iterator failed", KR(ret), K(ls_id), K(base_lsn));
  } else if (OB_FAIL(iterator_.set_io_context(palf::LogIOContext(MTL_ID(), ls_id.id(), palf::LogIOUser::OTHER)))) { //TODO by ziqi: LogIOUser
    CLOG_LOG(WARN, "iterator set_io_context failed", KR(ret), K(ls_id));
  } else {
    transport_status_ = transport_status;
    next_to_submit_lsn_ = base_lsn;
    next_to_submit_scn_.set_min();
    base_lsn_ = base_lsn;
    base_scn_ = base_scn;
    type_ = ObTransportServiceTaskType::TRANSPORT_SUBMIT_TASK;
    if (OB_SUCCESS != (tmp_ret = iterator_.next())) {
      // 在没有写入的情况下有可能已经到达边界
      CLOG_LOG(WARN, "iterator next failed", K(iterator_), K(tmp_ret));
    }
    CLOG_LOG(INFO, "ObTransportServiceSubmitTask init success", KR(ret), K(ls_id), K(type_), K(next_to_submit_lsn_),
             K(next_to_submit_scn_), K(base_scn), K(base_lsn), K(transport_status));
  }
  return ret;
}

//---------------ObTransportServiceSubmitTask 方法实现---------------//
int ObTransportServiceSubmitTask::get_next_to_submit_log_info(palf::LSN &lsn, share::SCN &scn) const
{
  ObLockGuard<ObSpinLock> guard(lock_);
  return get_next_to_submit_log_info_(lsn, scn);
}

int ObTransportServiceSubmitTask::get_next_to_submit_log_info_(palf::LSN &lsn, share::SCN &scn) const
{
  int ret = OB_SUCCESS;
  lsn.val_ = ATOMIC_LOAD(&next_to_submit_lsn_.val_);
  scn = next_to_submit_scn_.atomic_load();
  return ret;
}

bool ObTransportServiceSubmitTask::has_remained_submit_log(const share::SCN &transported_point,
                                                             bool &iterate_end_by_replayable_point)
{
  // next接口只在submit任务中单线程调用
  if (false == iterator_.is_valid()) {
    int ret = next_log(transported_point, iterate_end_by_replayable_point);
    CLOG_LOG(TRACE, "has_remained_submit_log", K(ret), K(transported_point), K(iterate_end_by_replayable_point), K(iterator_));
  }
  return iterator_.is_valid();
}

int ObTransportServiceSubmitTask::update_submit_log_meta_info(const palf::LSN &lsn, const share::SCN &scn)
{
  ObLockGuard<ObSpinLock> guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_next_to_submit_lsn_(lsn))
      || OB_FAIL(update_next_to_submit_scn_(SCN::scn_inc(scn)))) {
    CLOG_LOG(ERROR, "failed to update_submit_log_meta_info", KR(ret), K(lsn), K(scn),
             K(next_to_submit_lsn_), K(next_to_submit_scn_));
  } else {
    CLOG_LOG(TRACE, "update_submit_log_meta_info", KR(ret), K(lsn), K(scn),
             K(next_to_submit_lsn_), K(next_to_submit_scn_), K(iterator_));
  }
  return ret;
}

int ObTransportServiceSubmitTask::update_next_to_submit_lsn_(const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(lsn <= next_to_submit_lsn_)) {
    CLOG_LOG(WARN, "lsn is not greater than next_to_submit_lsn", K(lsn), K(next_to_submit_lsn_));
  } else {
    next_to_submit_lsn_ = lsn;
  }
  return ret;
}

int ObTransportServiceSubmitTask::update_next_to_submit_scn_(const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(scn <= next_to_submit_scn_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(scn), K(next_to_submit_scn_));
  } else {
    next_to_submit_scn_ = scn;
  }
  return ret;
}

int ObTransportServiceSubmitTask::need_skip(const share::SCN &scn, bool &need_skip)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!base_scn_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    need_skip = scn < base_scn_;
    if (need_skip) {
      CLOG_LOG(INFO, "log will be skipped due to scn < base_scn", K(scn), K(base_scn_), K(base_lsn_));
    }
  }
  return ret;
}

int ObTransportServiceSubmitTask::get_log(const char *&buffer, int64_t &nbytes,
                                          share::SCN &scn, palf::LSN &lsn, palf::LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  ipalf::IGroupEntry group_entry(GCONF.enable_logservice);
  const char *buf = nullptr;

  if (OB_FAIL(iterator_.get_entry(buf, group_entry, lsn))) {
    // skip, may return iterator end
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "buf is NULL", K(ret));
  } else if ((nbytes = group_entry.get_serialize_size(lsn)) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid serialize size", K(ret), K(nbytes));
  } else {
    // 获取完整的 group entry（包含 header）
    buffer = buf;  // buf 已经指向包含 header 的完整数据
    nbytes = group_entry.get_serialize_size(lsn);  // 包含 header 的完整大小
    scn = group_entry.get_scn();
    end_lsn = lsn + nbytes;
  }
  return ret;
}

int ObTransportServiceSubmitTask::next_log(const share::SCN &transported_point,
                                           bool &iterate_end_by_replayable_point)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SCN next_min_scn;
  if (OB_SUCCESS != (tmp_ret = iterator_.next(transported_point, next_min_scn,
                                              iterate_end_by_replayable_point))) {
    if (OB_ITER_END == tmp_ret) {
      share::SCN current_scn = next_to_submit_scn_.atomic_load();
      if (next_min_scn == current_scn
          || !transported_point.is_valid()) {
        // do nothing
      } else if (!next_min_scn.is_valid() || next_min_scn == SCN::base_scn()) {
        // should only occurs when palf has no log
        CLOG_LOG(INFO, "next_min_scn is invalid", K(transported_point),
                 K(next_min_scn), K(current_scn), K(ret), K(iterator_));
      } else if (OB_UNLIKELY(next_min_scn < current_scn)) {
        // SCN 回退场景，可能是宕机重启后的情况
        ret = OB_SUCCESS;
        CLOG_LOG(ERROR, "next_min_scn < next_to_submit_scn_, possible restart scenario",
                 K(transported_point), K(next_min_scn), K(current_scn), K(ret), K(iterator_));
      } else {
        next_to_submit_scn_.atomic_set(next_min_scn);
        CLOG_LOG(INFO, "update next_to_submit_scn_", K(transported_point),
                 K(next_min_scn), K(current_scn), K(ret), K(iterator_));
      }
    } else {
      // ignore other err ret of iterator
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObTransportServiceSubmitTask::reset_iterator(const share::ObLSID &id, const palf::LSN &begin_lsn)
{
  int ret = OB_SUCCESS;
  // 处理宕机重启场景：next_to_submit_lsn_可能小于begin_lsn

  // 检查 next_to_submit_lsn_ 的有效性
  if (!next_to_submit_lsn_.is_valid()) {
    // 如果 next_to_submit_lsn_ 无效，直接使用 begin_lsn
    next_to_submit_lsn_ = begin_lsn;
    CLOG_LOG(INFO, "next_to_submit_lsn_ is invalid, use begin_lsn",
             K(id), K(begin_lsn), K(next_to_submit_lsn_));
  } else {
    // 如果 next_to_submit_lsn_ 有效，取两者中的较大值
    next_to_submit_lsn_ = std::max(next_to_submit_lsn_, begin_lsn);
  }

  if (OB_FAIL(seek_log_iterator(id, next_to_submit_lsn_, iterator_))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "seek iterator failed", K(begin_lsn), K(next_to_submit_lsn_), K(ret));
  } else if (OB_FAIL(iterator_.next())) {
    CLOG_LOG(WARN, "iterator next failed", K(begin_lsn), K(next_to_submit_lsn_), K(ret));
  }
  return ret;
}

ObTransportServiceStatusTask::ObTransportServiceStatusTask()
 : ObTransportServiceTask(),
   total_submit_cb_cnt_(0),
   last_check_submit_cb_cnt_(0),
   total_transport_cb_cnt_(0),
   idx_(-1),
   need_batch_push_(false)
{
  type_ = ObTransportServiceTaskType::TRANSPORT_STATUS_TASK;
}

ObTransportServiceStatusTask::~ObTransportServiceStatusTask()
{
  destroy();
}

void ObTransportServiceStatusTask::reset()
{
  ObLink *link = NULL;
  int ret = OB_SUCCESS;
  int64_t drained_cnt = 0;
  while (OB_SUCCESS == (ret = queue_.pop(link)) && NULL != link) {
    ObLogTransportTask *transport_task = static_cast<ObLogTransportTask *>(link);
    ob_free(transport_task);
    transport_task = NULL;
    drained_cnt++;
  }
  if (drained_cnt > 0) {
    CLOG_LOG(WARN, "drained leftover transport tasks from queue on reset",
             K(drained_cnt), K(idx_), KPC(transport_status_));
  }
  ObTransportServiceTask::reset();
  total_submit_cb_cnt_ = 0;
  last_check_submit_cb_cnt_ = 0;
  total_transport_cb_cnt_ = 0;
  idx_ = -1;
  need_batch_push_ = false;
  type_ = ObTransportServiceTaskType::INVALID_TRANSPORT_TASK;
  transport_status_ = nullptr;
  lease_.reset();
}

void ObTransportServiceStatusTask::destroy()
{
  reset();
  ObTransportServiceTask::destroy();
}

int ObTransportServiceStatusTask::init(LogTransportStatus *transport_status,
                                       const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(transport_status)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(transport_status));
  } else {
    transport_status_ = transport_status;
    type_ = ObTransportServiceTaskType::TRANSPORT_STATUS_TASK;
    idx_ = idx;
  }
  return ret;
}

ObLink *ObTransportServiceStatusTask::top()
{
  ObLink *p = NULL;
  queue_.top(p);
  return p;
}

ObLink *ObTransportServiceStatusTask::pop()
{
  ObLink *p = NULL;
  int ret = queue_.pop(p);
  if (OB_FAIL(ret)) {
    p = NULL;
  }
  return p;
}

int ObTransportServiceStatusTask::push(Link *p)
{
  need_batch_push_ = true;  // 标记需要提交到全局队列
  return queue_.push(p);
}

void ObTransportServiceStatusTask::inc_total_submit_cb_cnt()
{
  total_submit_cb_cnt_++;
}

void ObTransportServiceStatusTask::inc_total_transport_cb_cnt()
{
  total_transport_cb_cnt_++;
}

int64_t ObTransportServiceStatusTask::get_total_submit_cb_cnt() const
{
  return total_submit_cb_cnt_;
}

int64_t ObTransportServiceStatusTask::get_total_transport_cb_cnt() const
{
  return total_transport_cb_cnt_;
}

ObTransportServiceInitTask::ObTransportServiceInitTask()
  : ObTransportServiceTask(),
    ls_id_(),
    proposal_id_(0),
    sync_mode_(palf::SyncMode::INVALID_SYNC_MODE),
    begin_lsn_()
{
  type_ = ObTransportServiceTaskType::TRANSPORT_INIT_TASK;
}

ObTransportServiceInitTask::~ObTransportServiceInitTask()
{
  reset();
}

int ObTransportServiceInitTask::init(const share::ObLSID &ls_id,
                                     const int64_t proposal_id,
                                     const palf::SyncMode &sync_mode,
                                     const palf::LSN &begin_lsn,
                                     LogTransportStatus *transport_status)
{
  int ret = OB_SUCCESS;

  if (!ls_id.is_valid() || proposal_id <= 0 || !begin_lsn.is_valid() || OB_ISNULL(transport_status)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(ls_id), K(proposal_id), K(sync_mode),
             K(begin_lsn), KP(transport_status));
  } else {
    ls_id_ = ls_id;
    proposal_id_ = proposal_id;
    sync_mode_ = sync_mode;
    begin_lsn_ = begin_lsn;
    transport_status_ = transport_status;
    type_ = ObTransportServiceTaskType::TRANSPORT_INIT_TASK;
  }

  return ret;
}

void ObTransportServiceInitTask::reset()
{
  ls_id_.reset();
  proposal_id_ = 0;
  sync_mode_ = palf::SyncMode::INVALID_SYNC_MODE;
  begin_lsn_.reset();
  lease_.reset();
  ObTransportServiceTask::reset();
}

void ObTransportServiceInitTask::destroy()
{
  reset();
  ObTransportServiceTask::destroy();
}

int ObTransportServiceInitTask::do_init()
{
  int ret = OB_SUCCESS;
  LogTransportStatus *transport_status = get_transport_status();

  if (OB_ISNULL(transport_status)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport_status is NULL", K(ret), K(ls_id_));
  } else if (OB_ISNULL(transport_status->tp_sv_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport_service is NULL", K(ret), K(ls_id_));
  } else if (proposal_id_ <= 0 || proposal_id_ < transport_status->get_proposal_id()) { //检查当前的init task是否过期，考虑并发场景
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(ls_id_), K(proposal_id_), K(transport_status->get_proposal_id()),
             K(sync_mode_));
  } else if (OB_FAIL(enable_sync_status_(transport_status))) {
    CLOG_LOG(WARN, "enable_sync_status_ failed in init task", K(ret), K(ls_id_));
  }

  if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "do_init failed in init task", K(ret), K(ls_id_), K(proposal_id_));
  } else if (transport_status->is_enabled()) {
    palf::LSN standby_end_lsn;
    share::SCN standby_end_scn;
    palf::LSN start_lsn;
    share::SCN start_scn;

    // 初始化流程按顺序：更新备库位点 -> 计算迭代日志起点 -> 初始化/提交任务
    if (OB_FAIL(update_standby_info_(transport_status, standby_end_lsn, standby_end_scn))) {
      CLOG_LOG(WARN, "update_standby_info_ failed in init task", K(ret), K(ls_id_));
    } else if (OB_FAIL(calc_start_point_(transport_status, standby_end_lsn, standby_end_scn, start_lsn, start_scn))) {
      CLOG_LOG(WARN, "calc_start_point_ failed in init task", K(ret), K(ls_id_));
    } else if (OB_FAIL(transport_status->submit_read_log_task(start_lsn, start_scn))) {
      CLOG_LOG(WARN, "submit_read_log_task failed in init task", K(ret), K(ls_id_));
    }
  }

  return ret;
}

int ObTransportServiceInitTask::enable_sync_status_(LogTransportStatus *transport_status)
{
  int ret = OB_SUCCESS;
  const bool need_enable = (sync_mode_ == palf::SyncMode::SYNC);

  if (need_enable && !transport_status->is_enabled_without_lock()) {
    // 如果是sync mode，启用强同步，获取sync_standby_dest
    if (OB_FAIL(transport_status->tp_sv_->query_sync_standby_dest(ls_id_))) {
      CLOG_LOG(WARN, "query_sync_standby_dest failed in init task", K(ret), K(ls_id_));
    // 修改is_enabled_为true，并初始化tp_submit_task_的iterator
    } else if (OB_FAIL(transport_status->enable_status(proposal_id_))) {
      CLOG_LOG(WARN, "enable_status failed in init task", K(ret), K(ls_id_), K(proposal_id_));
    } else {
      CLOG_LOG(INFO, "enable_status success in init task", K(ls_id_), K(proposal_id_));
    }
  }

  return ret;
}

int ObTransportServiceInitTask::update_standby_info_(LogTransportStatus *transport_status,
                                                     palf::LSN &standby_end_lsn,
                                                     share::SCN &standby_end_scn)
{
  int ret = OB_SUCCESS;

  // 获取备库地址，并更新到standby_status
  if (OB_FAIL(query_standby_ls_location_(transport_status))) {
    CLOG_LOG(WARN, "query_standby_ls_location_ failed in init task", K(ret), K(ls_id_));
  // 获取备库位点
  } else if (OB_FAIL(transport_status->get_standby_committed_info(standby_end_lsn, standby_end_scn))) {
    CLOG_LOG(WARN, "get_standby_committed_info failed", K(ret), K(ls_id_));
  // 更新备库位点，并通知给apply service更新备库位点
  } else if (standby_end_lsn.is_valid() && standby_end_scn.is_valid()) {
    if (OB_FAIL(transport_status->update_standby_committed_end_lsn(standby_end_lsn, standby_end_scn))) {
      if (OB_NO_NEED_UPDATE == ret) { // 备库位点未变化，跳过更新
        CLOG_LOG(WARN, "standby_end_lsn not changed, skip update", K(standby_end_lsn), K(ls_id_));
        ret = OB_SUCCESS;
      } else { // 更新备库位点失败，记录日志
        CLOG_LOG(WARN, "update_standby_committed_end_lsn failed", K(ret), K(ls_id_));
      }
    }
  } else {
    CLOG_LOG(WARN, "standby_end_lsn or standby_end_scn is invalid", K(ret), K(ls_id_), K(standby_end_lsn),
              K(standby_end_scn));
  }

  return ret;
}

int ObTransportServiceInitTask::calc_start_point_(LogTransportStatus *transport_status,
                                                  const palf::LSN &standby_end_lsn,
                                                  const share::SCN &standby_end_scn,
                                                  palf::LSN &start_lsn,
                                                  share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  palf::LSN palf_end_lsn = transport_status->get_palf_committed_end_lsn();
  share::SCN palf_end_scn = transport_status->get_palf_committed_end_scn();
  palf::LSN base_lsn;
  share::SCN base_scn;

  // 默认使用palf位点，如备库位点有效且更小则覆盖
  base_lsn = palf_end_lsn;
  base_scn = palf_end_scn;
  if (!palf_end_lsn.is_valid() || !palf_end_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "palf_end_lsn or palf_end_scn is invalid", K(ret), K(ls_id_), K(palf_end_lsn), K(palf_end_scn));
  } else if (!standby_end_lsn.is_valid() || !standby_end_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "standby_end_lsn or standby_end_scn is invalid", K(ret), K(ls_id_), K(standby_end_lsn), K(standby_end_scn));
  } else if (standby_end_lsn < palf_end_lsn && standby_end_scn < palf_end_scn) {
    base_lsn = standby_end_lsn;
    base_scn = standby_end_scn;
  }

  palf::LSN last_sent_lsn = palf::LSN(ATOMIC_LOAD(&transport_status->last_sent_lsn_.val_));
  share::SCN last_sent_scn = transport_status->last_sent_scn_.atomic_load();
  start_lsn = last_sent_lsn.is_valid() ? last_sent_lsn : base_lsn;
  start_scn = last_sent_scn.is_valid() ? last_sent_scn : base_scn;
  if (!start_scn.is_valid()) {
    start_scn.set_min();
  }

  return ret;
}

int LogTransportStatus::submit_read_log_task(const palf::LSN &start_lsn, const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  // 在写锁下初始化/复用submit任务，确保与并发提交/重置互斥
  {
    WLockGuardWithRetryInterval guard(lock_, WRLOCK_RETRY_INTERVAL_US, WRLOCK_RETRY_INTERVAL_US);
    if (OB_ISNULL(tp_submit_task_.get_transport_status())) {
      if (OB_FAIL(tp_submit_task_.init(ls_id_, start_lsn, start_scn, this))) {
        CLOG_LOG(WARN, "failed to init submit task", K(ret), K(ls_id_), K(start_lsn), K(start_scn));
      } else {
        CLOG_LOG(INFO, "init submit task success", K(ls_id_), K(start_lsn), K(start_scn));
      }
    } else if (OB_FAIL(tp_submit_task_.reset_iterator(ls_id_, start_lsn))) {
      CLOG_LOG(WARN, "reset_iterator failed", K(ret), K(ls_id_), K(start_lsn));
    }
  }

  // submit_task_to_transport_service_ 通过 lease 机制保证线程安全，不需要写锁保护
  if (OB_SUCC(ret) && OB_FAIL(submit_task_to_transport_service_(tp_submit_task_))) {
    CLOG_LOG(WARN, "submit_task_to_transport_service_ failed", K(ret), K(ls_id_));
  } else {
    CLOG_LOG(INFO, "submit task success", K(ls_id_));
  }
  return ret;
}

int ObTransportServiceInitTask::query_standby_ls_location_(LogTransportStatus *transport_status)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(transport_status)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "transport_status is NULL", K(ret), K(ls_id_));
  } else if (OB_ISNULL(transport_status->tp_sv_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport_service is NULL", K(ret), K(ls_id_));
  } else {
    // 获取备库地址（通过RPC，在后台线程中执行）
    common::ObAddr standby_addr;
    ObSyncStandbyDestStruct sync_standby_dest;
    ObLogTransportService *transport_service = transport_status->tp_sv_;

    if (OB_FAIL(transport_service->get_sync_standby_dest(sync_standby_dest))) {
      CLOG_LOG(WARN, "get_sync_standby_dest failed", K(ret), K(ls_id_));
    } else if (sync_standby_dest.is_valid()) {
      // 通过RPC获取备库地址
      share::ObLogRestoreProxyUtil restore_proxy;
      if (OB_FAIL(restore_proxy.init_with_service_attr(MTL_ID(), &sync_standby_dest.restore_source_service_attr_))) {
        CLOG_LOG(WARN, "restore_proxy init failed", K(ret), K(ls_id_));
      } else if (OB_FAIL(transport_service->query_standby_addr_(&restore_proxy,
                       sync_standby_dest.restore_source_service_attr_,
                       ls_id_, standby_addr))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          CLOG_LOG(INFO, "no standby addr found, will retry later", K(ls_id_));
          ret = OB_SUCCESS;  // 不是错误，后续会重试
        } else {
          CLOG_LOG(WARN, "query_standby_addr_ failed", K(ret), K(ls_id_));
        }
      } else if (OB_FAIL(transport_status->set_standby_addr(standby_addr))) {
        CLOG_LOG(WARN, "set_standby_addr failed", K(ret), K(ls_id_), K(standby_addr));
      }
    }
  }

  return ret;
}

bool ObTransportServiceStatusTask::is_transport_done()
{
  return queue_.is_empty();
}

int64_t ObTransportServiceStatusTask::idx() const
{
  return idx_;
}

//---------------LogTransportStatus 方法实现---------------//
LogTransportStatus::LogTransportStatus()
  : is_inited_(false),
    is_in_stop_state_(true),
    is_ls_gc_state_(false),
    is_enabled_(false),
    ls_id_(),
    role_(FOLLOWER),
    proposal_id_(),
    palf_committed_end_lsn_(),
    palf_committed_end_scn_(),
    standby_committed_end_lsn_(),
    standby_committed_end_scn_(),
    last_sent_lsn_(),
    last_acked_lsn_(),
    palf_env_(),
    palf_handle_(),
    fs_cb_(),
    ref_cnt_(0),
    lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
    retry_lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
    sync_mode_lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
    retry_iterator_(),
    retry_start_lsn_(),
    last_retry_check_time_us_(0),
    last_standby_lsn_(),
    last_standby_lsn_update_time_us_(0)
{
}

LogTransportStatus::~LogTransportStatus()
{
  destroy();
}

void LogTransportStatus::destroy()
{
  CLOG_LOG(INFO, "destroy transport status", KPC(this));
  unregister_file_size_cb();
  // 清理 tp_status_tasks_ 数组
  for (int i = 0; i < TRANSPORT_TASK_QUEUE_SIZE; ++i) {
    tp_status_tasks_[i].destroy();
  }
  tp_submit_task_.destroy();
  tp_init_task_.destroy();
  // 清理周期性重传 iterator 及相关状态，并避免与 reset/read 并发访问 palf_handle_
  {
    WLockGuard guard(retry_lock_);
    retry_iterator_.destroy();
    retry_start_lsn_.reset();
    last_retry_check_time_us_ = 0;
    last_standby_lsn_.reset();
    last_standby_lsn_update_time_us_ = 0;
    close_palf_handle();
  }
  is_inited_ = false;
  is_in_stop_state_ = true;
  ATOMIC_STORE(&is_ls_gc_state_, false);
  is_enabled_ = false;
  proposal_id_ = -1;
  role_ = FOLLOWER;
  fs_cb_.destroy();
  palf_committed_end_lsn_.reset();
  standby_committed_end_lsn_.reset();
  last_sent_lsn_.reset();
  last_acked_lsn_.reset();
  palf_env_ = nullptr;
  tp_sv_ = nullptr;
  ls_id_.reset();
}

int LogTransportStatus::init(const share::ObLSID &id,
                             ipalf::IPalfEnv *palf_env,
                             ObLogTransportService *tp_sv)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "transport status has already been inited", K(ret), K(id));
  } else if (!id.is_valid() || OB_ISNULL(palf_env) || OB_ISNULL(tp_sv)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(id), K(tp_sv), K(ret));
  } else if (OB_FAIL(palf_env->open(id.id(), palf_handle_))) {
    CLOG_LOG(ERROR, "failed to open palf handle", K(palf_env), K(id));
  } else {
    ls_id_ = id;
    proposal_id_ = -1;
    tp_sv_ = tp_sv;
    palf_env_ = palf_env;
    IGNORE_RETURN new (&fs_cb_) ObTransportFsCb(this);
    is_in_stop_state_ = false;
    // 在重启场景下，从 PALF 中恢复持久化的 sync_mode 状态
    // 如果之前是 SYNC 模式，则恢复 is_enabled_ 为 true
    if (is_sync_mode_enabled()) {
      is_enabled_ = true;
      CLOG_LOG(INFO, "restore enabled status from sync_mode", K(id));
    } else {
      is_enabled_ = false;
    }

    for (int i = 0; OB_SUCC(ret) && i < TRANSPORT_TASK_QUEUE_SIZE; ++i) {
      if (OB_FAIL(tp_status_tasks_[i].init(this, i))) {
        CLOG_LOG(ERROR, "failed to init status task", K(ret), K(i), K(id));
      }
    }
    if (OB_FAIL(ret)) {
      // 如果初始化失败，清理已初始化的任务
      for (int i = 0; i < TRANSPORT_TASK_QUEUE_SIZE; ++i) {
        tp_status_tasks_[i].destroy();
      }
    } else if (OB_FAIL(palf_handle_->register_file_size_cb(&fs_cb_))) {
      CLOG_LOG(ERROR, "failed to register cb", K(ret));
      // 清理已初始化的任务
      for (int i = 0; i < TRANSPORT_TASK_QUEUE_SIZE; ++i) {
        tp_status_tasks_[i].destroy();
      }
    } else {
      is_inited_ = true;
      palf::LSN initial_end_lsn;
      share::SCN initial_end_scn;
      if (OB_FAIL(palf_handle_->get_end_lsn(initial_end_lsn))) {
        CLOG_LOG(WARN, "failed to get initial end lsn", K(ret));
      } else if (OB_FAIL(palf_handle_->get_end_scn(initial_end_scn))) {
        CLOG_LOG(WARN, "failed to get initial end scn", K(ret));
      } else if (initial_end_lsn.is_valid() && initial_end_scn.is_valid()) {
        palf_committed_end_lsn_ = initial_end_lsn;
        palf_committed_end_scn_ = initial_end_scn;
        CLOG_LOG(INFO, "initial end lsn and scn", K(initial_end_lsn), K(initial_end_scn));
      }
      CLOG_LOG(INFO, "transport status init success", K(ret), KPC(this));
    }
  }
  if (OB_FAIL(ret) && (OB_INIT_TWICE != ret)) {
    destroy();
  }
  return ret;
}

bool LogTransportStatus::is_enabled() const
{
  RLockGuard guard(lock_);
  return is_enabled_;
}

bool LogTransportStatus::is_enabled_without_lock() const
{
  return is_enabled_;
}

int LogTransportStatus::switch_to_leader(const int64_t new_proposal_id,
                                         const palf::SyncMode &sync_mode,
                                         const palf::LSN &begin_lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "transport status has not been inited");
  } else if (is_in_stop_state_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(INFO, "transport status has been stopped");
  } else if (new_proposal_id < ATOMIC_LOAD(&proposal_id_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid proposal id", K(ret), K(new_proposal_id), KPC(this));
  } else if (OB_FAIL(submit_init_task(new_proposal_id, sync_mode, begin_lsn))) { // 异步完成强同步初始化流程
    CLOG_LOG(WARN, "failed to submit init task", K(ret), K(ls_id_), K(new_proposal_id), K(sync_mode), K(begin_lsn));
  } else {
    WLockGuardWithRetryInterval wguard(lock_, WRLOCK_RETRY_INTERVAL_US, WRLOCK_RETRY_INTERVAL_US);
    role_ = LEADER;
    ATOMIC_STORE(&proposal_id_, new_proposal_id);
  }

  CLOG_LOG(INFO, "transport status switch_to_leader end", K(ret), K(sync_mode), K(begin_lsn), KPC(this));
  return ret;
}

int LogTransportStatus::switch_to_follower()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "transport status has not been inited", K(ret));
  } else {
    // 判断是否需要等待备库同步：只有在SYNC模式且已启用时才需要等待
    bool need_wait_standby_sync = is_sync_mode_enabled()
                                  && !ATOMIC_LOAD(&is_ls_gc_state_)
                                  && is_enabled_without_lock();

    // SYNC模式下，等待备库同步完成（避免降级后数据丢失）
    // TODO by ziqi: 增加超时机制，避免无限等待，补充case测试
    if (need_wait_standby_sync) {
      const int64_t MAX_WAIT_TIME_US = 10 * 1000; // 10ms
      while (is_enabled_without_lock()) {
        {
          RLockGuard guard(lock_);
          palf::LSN palf_end_lsn = get_palf_committed_end_lsn();
          palf::LSN standby_end_lsn = get_standby_committed_end_lsn();

          if (standby_end_lsn.is_valid() && standby_end_lsn >= palf_end_lsn) {
            CLOG_LOG(INFO, "standby sync done, ready to switch to follower",
                     K_(ls_id), K(standby_end_lsn), K(palf_end_lsn));
            break;
          }
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            CLOG_LOG(INFO, "wait standby sync", K_(ls_id), K(standby_end_lsn), K(palf_end_lsn));
          }
        }
        ob_usleep(MAX_WAIT_TIME_US);
      }
    }

    // 统一更新角色和状态
    WLockGuardWithRetryInterval guard(lock_, WRLOCK_RETRY_INTERVAL_US, WRLOCK_RETRY_INTERVAL_US);
    role_ = FOLLOWER;
    is_enabled_ = is_sync_mode_enabled() ? true : false;
    standby_committed_end_lsn_.reset();
    standby_committed_end_scn_.reset();

    CLOG_LOG(INFO, "switch_to_follower success", K_(ls_id), K(need_wait_standby_sync));
  }
  return ret;
}

void LogTransportStatus::mark_ls_gc_state()
{
  ATOMIC_STORE(&is_ls_gc_state_, true);
  CLOG_LOG(INFO, "transport status mark_ls_gc_state", KPC(this));
}

bool LogTransportStatus::need_submit_log() const
{
  return (role_ == common::ObRole::LEADER) && is_enabled_without_lock() && standby_addr_valid_;
}

bool LogTransportStatus::is_sync_mode_enabled() const
{
  bool is_sync = true; //如果出现异常默认为sync，避免sync模式误判为async，后续改造接口
  int ret = OB_SUCCESS;
  if (OB_ISNULL(palf_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "palf_handle is NULL when checking sync mode", K_(ls_id));
  } else if (!palf_handle_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "palf_handle is invalid when checking sync mode", K_(ls_id));
  } else {
    ipalf::SyncMode sync_mode = ipalf::SyncMode::INVALID_SYNC_MODE;
    int ret = palf_handle_->get_sync_mode(sync_mode);
    if (OB_SUCC(ret)) {
      is_sync = (sync_mode == ipalf::SyncMode::SYNC);
    } else {
      CLOG_LOG(WARN, "failed to get sync mode from palf", K(ret), K_(ls_id));
    }
  }
  return is_sync;
}

int LogTransportStatus::push_log_transport_task(ObLogTransportTask &task)
{
  int ret = OB_SUCCESS;
  // 提交到tp_status_task_的队列
  // push() 方法会自动设置 need_batch_push_ 标志
  // 随机选择队列索引以实现负载均衡
  const int64_t idx = common::ObRandom::rand(0, TRANSPORT_TASK_QUEUE_SIZE - 1);
  return tp_status_tasks_[idx].push(&task);
}

int LogTransportStatus::get_ls_id(share::ObLSID &id) const
{
  int ret = OB_SUCCESS;
  id = ls_id_;
  return ret;
}

void LogTransportStatus::update_last_sent_lsn(const palf::LSN &lsn)
{
  // WLockGuard guard(lock_);
  ATOMIC_STORE(&last_sent_lsn_.val_, lsn.val_);
}

void LogTransportStatus::update_last_sent_scn(const share::SCN &scn)
{
  // WLockGuard guard(lock_);
  last_sent_scn_.atomic_store(scn);
}

palf::LSN LogTransportStatus::get_last_sent_lsn() const
{
  return palf::LSN(ATOMIC_LOAD(&last_sent_lsn_.val_));
}

palf::LSN LogTransportStatus::get_last_acked_lsn() const
{
  return palf::LSN(ATOMIC_LOAD(&last_acked_lsn_.val_));
}

int LogTransportStatus::register_file_size_cb()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(palf_handle_)) {
    if (OB_FAIL(palf_handle_->register_file_size_cb(&fs_cb_))) {
      CLOG_LOG(ERROR, "register_file_size_cb failed", K(ret), KPC(this));
    }
  }
  return ret;
}

int LogTransportStatus::unregister_file_size_cb()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(palf_handle_)) {
    if (OB_FAIL(palf_handle_->unregister_file_size_cb())) {
      CLOG_LOG(WARN, "unregister_file_size_cb failed", K(ret), KPC(this));
    }
  }
  return ret;
}

void LogTransportStatus::close_palf_handle()
{
  if (OB_NOT_NULL(palf_env_) && OB_NOT_NULL(palf_handle_) && palf_handle_->is_valid()) {
    palf_env_->close(palf_handle_);
  } else {
    // do nothing
  }
}

int LogTransportStatus::enable_status(const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  if (proposal_id < proposal_id_) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "proposal_id is smaller than local, skip enable_status",
             K(ret), K(ls_id_), K(proposal_id), K_(proposal_id));
  } else if (is_enabled_without_lock()) {
    CLOG_LOG(WARN, "transport status has already been enabled", K(ret));
  } else {
    palf::LSN current_end_lsn;
    share::SCN current_end_scn;

    if (!palf_committed_end_lsn_.is_valid() || !palf_committed_end_scn_.is_valid()) {
      if (OB_NOT_NULL(palf_handle_) && palf_handle_->is_valid()) {
        if (OB_FAIL(palf_handle_->get_end_lsn(current_end_lsn))) {
          CLOG_LOG(WARN, "failed to get end_lsn", K(ret));
        } else if (OB_FAIL(palf_handle_->get_end_scn(current_end_scn))) {
          CLOG_LOG(WARN, "failed to get end_scn", K(ret));
        } else if (current_end_lsn.is_valid() && current_end_scn.is_valid()) {
          CLOG_LOG(INFO, "get palf end lsn/scn in enable_status",
                    K(current_end_lsn), K(current_end_scn));
        }
      }
    }

    {
      WLockGuard guard(lock_);
      is_enabled_ = true;

      // 更新从PALF获取的位点信息
      if (current_end_lsn.is_valid() && current_end_scn.is_valid()) {
        palf_committed_end_lsn_ = current_end_lsn;
        palf_committed_end_scn_ = current_end_scn;
        CLOG_LOG(INFO, "update palf committed end lsn/scn in enable_status",
                  K(current_end_lsn), K(current_end_scn));
      }
    }
  }
  return ret;
}

int LogTransportStatus::disable_status(const int64_t new_proposal_id)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (new_proposal_id < proposal_id_) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "proposal_id is smaller than local, skip disable_status",
             K(ret), K(ls_id_), K(new_proposal_id), K_(proposal_id));
  } else {
    is_enabled_ = false;
    CLOG_LOG(INFO, "transport status disable success", KPC(this), K(new_proposal_id));
  }
  return ret;
}

int LogTransportStatus::get_standby_committed_info(palf::LSN &standby_committed_end_lsn, share::SCN &standby_committed_end_scn) const
{
  int ret = OB_SUCCESS;
  standby_committed_end_lsn.reset();
  standby_committed_end_scn.reset();

  // 首先尝试从缓存中获取备库位点（如果已通过 RPC 更新）
  palf::LSN cached_lsn = get_standby_committed_end_lsn();
  share::SCN cached_scn = get_standby_committed_end_scn();
  if (cached_lsn.is_valid() && cached_scn.is_valid()) {
    standby_committed_end_lsn = cached_lsn;
    standby_committed_end_scn = cached_scn;
    CLOG_LOG(TRACE, "get standby committed info from cache", K(ls_id_), K(standby_committed_end_lsn), K(standby_committed_end_scn));
  } else if (OB_ISNULL(tp_sv_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "tp_sv_ is NULL", K(ret), K(ls_id_));
  } else {
    // 通过 restore_proxy 查询备库位点
    ObSyncStandbyDestStruct sync_standby_dest;
    if (OB_FAIL(tp_sv_->get_sync_standby_dest(sync_standby_dest))) {
      CLOG_LOG(WARN, "failed to get sync_standby_dest", K(ret), K(ls_id_));
    } else if (!sync_standby_dest.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "sync_standby_dest is invalid", K(ret), K(ls_id_), K(sync_standby_dest));
    } else {
      share::ObLogRestoreProxyUtil restore_proxy;
      if (OB_FAIL(restore_proxy.init_with_service_attr(MTL_ID(), &sync_standby_dest.restore_source_service_attr_))) {
        CLOG_LOG(WARN, "restore_proxy init failed", K(ret), K(ls_id_));
      } else if (OB_FAIL(restore_proxy.get_tenant_committed_info(
                          sync_standby_dest.restore_source_service_attr_.user_.tenant_id_,
                          ls_id_, standby_committed_end_lsn, standby_committed_end_scn))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // 备库中该日志流不存在或没有 leader，返回空位点
          CLOG_LOG(INFO, "no leader found for log stream in standby cluster or ls not exist", K(ret), K(ls_id_));
          ret = OB_SUCCESS;  // 这种情况不算错误，只是没有位点信息
        } else {
          CLOG_LOG(WARN, "get standby committed info failed", K(ret), K(ls_id_));
        }
      } else {
        CLOG_LOG(INFO, "get standby committed info from restore_proxy", K(ls_id_), K(standby_committed_end_lsn), K(standby_committed_end_scn));
      }
    }
  }
  return ret;
}

int LogTransportStatus::update_palf_committed_end_lsn(const palf::LSN &end_lsn, const share::SCN &end_scn)
{
  int ret = OB_SUCCESS;
  RLockGuard rlock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "transport status is not init", K(ret));
  } else if (OB_UNLIKELY(!end_lsn.is_valid() || !end_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(end_lsn), K(end_scn));
  } else {
    // 外层已经持有读锁，不需要重复加锁
    palf::LSN palf_committed_end_lsn = palf::LSN(ATOMIC_LOAD(&palf_committed_end_lsn_.val_));
    share::SCN palf_committed_end_scn = palf_committed_end_scn_.atomic_load();
    if (palf_committed_end_lsn.is_valid() && end_lsn < palf_committed_end_lsn) {
      CLOG_LOG(WARN, "invalid new end_lsn", K(end_lsn), K(palf_committed_end_lsn));
    } else if (palf_committed_end_scn.is_valid() && end_scn < palf_committed_end_scn) {
      CLOG_LOG(WARN, "invalid new end_scn", K(end_scn), K(palf_committed_end_scn_));
    } else {
      ATOMIC_STORE(&palf_committed_end_lsn_.val_, end_lsn.val_);
      palf_committed_end_scn_.atomic_store(end_scn);
      if (! need_submit_log()) {
        CLOG_LOG(TRACE, "not need to submit log", K(ls_id_), K(role_), K(is_enabled_), K(standby_addr_), K(standby_addr_valid_), K(end_lsn), K(end_scn));
      } else if (OB_FAIL(submit_task_to_transport_service_(tp_submit_task_))) {
        CLOG_LOG(WARN, "failed to submit task to transport service", K(ret), K(ls_id_), K(role_), K(is_enabled_),
         K(standby_addr_), K(standby_addr_valid_), K(end_lsn), K(end_scn), K(tp_submit_task_));
      } else {
        CLOG_LOG(TRACE, "submit task to transport service success", K(ls_id_), K(role_), K(is_enabled_), K(standby_addr_), K(standby_addr_valid_), K(end_lsn), K(end_scn));
      }
    }
  }
  return ret;
}

// 更新备库已提交的日志位点
// 由 RPC 回调调用，需要处理并发更新和位点回退的情况
// @return OB_NO_NEED_UPDATE 表示位点未变化，OB_STATE_NOT_MATCH 表示位点回退
int LogTransportStatus::update_standby_committed_end_lsn(const palf::LSN &end_lsn, const share::SCN &end_scn)
{
  int ret = OB_SUCCESS;

  if (!end_lsn.is_valid() || !end_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(end_lsn), K(end_scn));
  } else {
    // 获取读锁后再检查 is_enabled_
    RLockGuard guard(lock_);

    palf::LSN current_lsn = get_standby_committed_end_lsn();
    share::SCN current_scn = get_standby_committed_end_scn();

    // no need to check whether sync_mode is enabled
    // MA mode need this

    // 检查位点是否回退（备库重启或切主可能导致位点回退）
    if (current_lsn.is_valid() && end_lsn.val_ < current_lsn.val_) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "standby committed_end_lsn rollback, skip update",
               K(end_lsn), K(current_lsn), K(end_scn), K(current_scn), K(ls_id_));
    } else if (current_lsn.is_valid() &&
               end_lsn.val_ == current_lsn.val_ &&
               end_scn == current_scn) {
      ret = OB_NO_NEED_UPDATE;
    } else {
      // 原子更新两个位点（虽然不是完全原子，但在读锁保护下是安全的）
      ATOMIC_STORE(&standby_committed_end_lsn_.val_, end_lsn.val_);
      standby_committed_end_scn_.atomic_store(end_scn);
      CLOG_LOG(TRACE, "update standby committed_end_lsn success",
               K(end_lsn), K(current_lsn), K(end_scn), K(current_scn), K(ls_id_));

      // 通知 apply service 更新备库位点
      if (OB_NOT_NULL(tp_sv_)) {
        int notify_ret = tp_sv_->notify_apply_service(ls_id_, end_lsn, end_scn);
        if (OB_SUCCESS != notify_ret) {
          // 通知失败不影响位点更新，仅记录日志
          CLOG_LOG(WARN, "notify_apply_service failed",
                   K(notify_ret), K(ls_id_), K(end_lsn), K(end_scn));
        }
      }
    }
  }
  return ret;
}

share::SCN LogTransportStatus::get_standby_committed_end_scn() const
{
  share::SCN scn = standby_committed_end_scn_.atomic_load();
  return scn;
}

void LogTransportStatus::clear_standby_committed_end_lsn()
{
  // 清除备库位点，设置为无效值
  RLockGuard guard(lock_);
  standby_committed_end_lsn_.reset();
  standby_committed_end_scn_.reset();
  CLOG_LOG(INFO, "clear standby committed end lsn", K(ls_id_));
}

// 提交任务到传输服务的工作队列
// 通过 lease 机制保证同一任务不会被重复提交
int LogTransportStatus::submit_task_to_transport_service_(ObTransportServiceTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task.get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport status is NULL", K(ret), K(task));
  } else if (task.acquire_lease()) {
    inc_ref();
    if (OB_ISNULL(tp_sv_)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "transport_sv_ is NULL", K(ret));
    } else if (OB_FAIL(tp_sv_->push_task(&task))) {
      dec_ref();
      CLOG_LOG(WARN, "push task to transport service failed", K(ret));
    }
  }
  return ret;
}

int LogTransportStatus::submit_init_task(const int64_t proposal_id,
                                         const palf::SyncMode &sync_mode,
                                         const palf::LSN &begin_lsn)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tp_sv_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "transport service is NULL", K(ret));
  } else if (proposal_id <= 0 || !begin_lsn.is_valid() || sync_mode == palf::SyncMode::INVALID_SYNC_MODE) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(ls_id_), K(proposal_id), K(begin_lsn));
  } else {
    // 在写锁下初始化/复用init任务，确保与并发提交互斥
    WLockGuardWithRetryInterval guard(lock_, WRLOCK_RETRY_INTERVAL_US, WRLOCK_RETRY_INTERVAL_US);
    // 检查任务是否已在队列中
    if (OB_NOT_NULL(tp_init_task_.get_transport_status()) && !tp_init_task_.acquire_lease()) {
      CLOG_LOG(TRACE, "init task already in queue, skip submit", K(ls_id_));
    } else {
      // 任务不在队列中，可以重置并重新初始化
      if (OB_NOT_NULL(tp_init_task_.get_transport_status())) {
        tp_init_task_.revoke_lease();
        tp_init_task_.reset();
      }
      if (OB_FAIL(tp_init_task_.init(ls_id_, proposal_id, sync_mode, begin_lsn, this))) {
        CLOG_LOG(WARN, "init_task init failed", K(ret), K(ls_id_));
      } else if (OB_FAIL(submit_task_to_transport_service_(tp_init_task_))) {
        CLOG_LOG(WARN, "submit init task failed", K(ret), K(ls_id_));
      }
    }
  }

  return ret;
}

int LogTransportStatus::batch_push_all_status_task_queue()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < TRANSPORT_TASK_QUEUE_SIZE; ++i) {
    ObTransportServiceStatusTask &status_task = tp_status_tasks_[i];
    if (!status_task.need_batch_push()) {
      // do nothing
    } else if (OB_FAIL(submit_task_to_transport_service_(status_task))) {
      CLOG_LOG(WARN, "failed to push status task to transport service",
               K(status_task), K(ret), K(i), KPC(this));
    } else {
      status_task.set_batch_push_finish();
      CLOG_LOG(TRACE, "push status task to transport service", K(status_task), K(i));
    }
  }
  return ret;
}

int LogTransportStatus::retry_all_status_task_queue()
{
  int ret = OB_SUCCESS;
  // 检查所有status_task队列，如果有任务，强制设置need_batch_push标志
  for (int i = 0; i < TRANSPORT_TASK_QUEUE_SIZE; ++i) {
    ObTransportServiceStatusTask &status_task = tp_status_tasks_[i];
    if (NULL != status_task.top()) {
      // 队列中有任务，强制设置need_batch_push标志，确保会被重新提交
      status_task.set_need_batch_push();
    }
  }

  // 批量提交所有有need_batch_push标志的status_task到全局队列
  if (OB_FAIL(batch_push_all_status_task_queue())) {
    CLOG_LOG(WARN, "failed to batch_push_all_status_task_queue in retry", K(ret), KPC(this));
  }
  return ret;
}

// 执行日志传输任务：将日志通过RPC发送到备库
// 注意：调用者需要持有 transport_status 的读锁
int LogTransportStatus::do_transport_task_(ObLogTransportTask *transport_task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "transport status not init", K(ret));
  } else if (OB_ISNULL(transport_task)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "transport_task is NULL", K(ret));
  } else if (!is_enabled_ || role_ != common::ObRole::LEADER) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "transport not enabled or not leader", K(ls_id_), K(is_enabled_), K(role_));
  } else {
    // 构造RPC请求
    ObLogTransportReq req;
    // 从sync_standby_dest结构中读取standby_cluster_id和standby_tenant_id
    if (OB_ISNULL(tp_sv_)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "tp_sv_ is NULL", K(ret));
    } else {
      ObSyncStandbyDestStruct sync_standby_dest;
      if (OB_FAIL(tp_sv_->get_sync_standby_dest(sync_standby_dest))) {
        CLOG_LOG(WARN, "failed to get sync_standby_dest", K(ret));
      } else if (!sync_standby_dest.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        CLOG_LOG(WARN, "sync_standby_dest is invalid", K(ret), K(sync_standby_dest));
      } else {
        req.standby_cluster_id_ = sync_standby_dest.restore_source_service_attr_.user_.cluster_id_;
        req.standby_tenant_id_ = sync_standby_dest.restore_source_service_attr_.user_.tenant_id_;
      }
    }

    if (OB_FAIL(ret)) {
      // 获取备库信息失败，不继续处理
    } else {
      req.ls_id_ = transport_task->ls_id_;
      req.start_lsn_ = transport_task->lsn_;
      req.end_lsn_ = transport_task->lsn_ + transport_task->log_size_;
      req.scn_ = transport_task->scn_;
      req.log_data_ = transport_task->log_buf_;
      req.log_size_ = transport_task->log_size_;

      // 注：特殊日志（如sync_mode日志）的解析调试逻辑已移除，如需调试可在此处添加

      // 获取本机地址并发送RPC
      const common::ObAddr &self_addr = GCTX.self_addr();
      if (!self_addr.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "self addr is invalid", K(ret));
      } else {
        req.src_ = self_addr;

        if (!req.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          CLOG_LOG(WARN, "invalid RPC request", K(ret), K(req));
        } else {
          const int64_t timeout_us = 3 * 1000 * 1000; // 3秒
          if (OB_FAIL(send_log_via_rpc_(req, timeout_us))) {
            CLOG_LOG(WARN, "send_log_via_rpc_ failed", K(ret), K(req), K(standby_addr_));
          } else {
            CLOG_LOG(TRACE, "do_transport_task success", K(req.ls_id_), K(req.start_lsn_),
                     K(req.end_lsn_), K(req.log_size_), K(req.scn_));
          }
        }
      }
    }
  }
  return ret;
}

int LogTransportStatus::set_standby_addr(const common::ObAddr &standby_addr)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "transport status not init", K(ret));
  } else if (!standby_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid standby addr", K(ret), K(standby_addr));
    standby_addr_valid_ = false;
  } else {
    standby_addr_ = standby_addr;
    standby_addr_valid_ = true;
    CLOG_LOG(INFO, "set standby addr success", K_(ls_id), K(standby_addr));
  }
  return ret;
}

int LogTransportStatus::get_rpc_proxy_(obrpc::ObLogTransportRpcProxy *&rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tp_sv_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "transport service is NULL", K(ret));
  } else if (OB_ISNULL(rpc_proxy = tp_sv_->get_rpc_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "rpc_proxy is NULL", K(ret));
  }
  return ret;
}

int LogTransportStatus::send_log_via_rpc_(const ObLogTransportReq &req, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  obrpc::ObLogTransportRpcProxy *rpc_proxy = NULL;
  ObLogSyncStandbyInfo resp;

  if (!req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid request", K(ret), K(req));
  } else if (!standby_addr_valid_) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid standby addr", K(ret), K(standby_addr_));
  } else if (OB_FAIL(get_rpc_proxy_(rpc_proxy))) {
    CLOG_LOG(WARN, "get_rpc_proxy_ failed", K(ret));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "rpc_proxy is NULL", K(ret));
  } else {
    // 调用异步RPC发送日志
    ObLogTransportRespCallback<obrpc::OB_LOG_TRANSPORT_REQ> cb(this);

    if (OB_FAIL(rpc_proxy->to(standby_addr_)
                       .dst_cluster_id(req.standby_cluster_id_)
                       .by(req.standby_tenant_id_)
                       .timeout(timeout_us)
                       .post_log_transport_req(req, &cb))) {
      CLOG_LOG(WARN, "post_log_transport_req failed", K(ret), K(req), K(standby_addr_), K(timeout_us));
    } else {
      // RPC发送成功，回调会在响应到达时自动调用
      CLOG_LOG(TRACE, "post_log_transport_req success", K(req), K(standby_addr_));
    }
  }

  return ret;
}

int LogTransportStatus::is_standby_sync_done(bool &is_done)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "transport status is not inited", K(ret));
  } else if (!is_enabled()) {
    is_done = true;
    CLOG_LOG(INFO, "transport is not enabled", K(is_done));
  } else if (!is_sync_mode_enabled() || ATOMIC_LOAD(&is_ls_gc_state_)) {
    is_done = true;
    CLOG_LOG(INFO, "is_standby_sync_done (non-SYNC mode)", K(ls_id_), K(is_done), K_(is_enabled));
  } else {
    // 只有在 SYNC 模式下才需要检查备库同步状态
    // 如果 sync_mode 不是 SYNC，直接返回 is_done = true，不需要等待备库位点
    RLockGuard guard(lock_);
    palf::LSN standby_lsn = palf::LSN(ATOMIC_LOAD(&standby_committed_end_lsn_.val_));
    palf::LSN palf_lsn = palf::LSN(ATOMIC_LOAD(&palf_committed_end_lsn_.val_));
    is_done = (standby_lsn.is_valid() && standby_lsn >= palf_lsn);
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      CLOG_LOG(INFO, "is_standby_sync_done (SYNC mode)", K(ls_id_), K(is_done), K(palf_lsn), K(standby_lsn), K_(is_enabled));
    }
  }
  return ret;
}

int LogTransportStatus::get_sync_end_scn(share::SCN &sync_end_scn) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "transport status is not inited", K(ret));
  } else {
    share::SCN palf_end_scn = palf_committed_end_scn_.atomic_load();

    // 只有在 SYNC 模式下才考虑 standby_end_scn
    if (is_sync_mode_enabled()) {
      share::SCN standby_end_scn = standby_committed_end_scn_.atomic_load();
      if (! standby_end_scn.is_valid()) {
        ret = OB_EAGAIN;
        CLOG_LOG(INFO, "standby_end_scn is valid", K(ret),K(ls_id_), K(standby_end_scn));
      } else if (standby_end_scn.is_valid_and_not_min() && palf_end_scn.is_valid_and_not_min()) {
        sync_end_scn = MIN(standby_end_scn, palf_end_scn);
      } else if (standby_end_scn.is_valid_and_not_min()) {
        sync_end_scn = standby_end_scn;
      } else if (palf_end_scn.is_valid_and_not_min()) {
        sync_end_scn = palf_end_scn;
      }
      CLOG_LOG(TRACE, "get sync end scn (SYNC mode)", K(ls_id_), K(standby_end_scn), K(palf_end_scn), K(sync_end_scn));
    } else {
      // 非 SYNC 模式，只返回 palf_committed_end_scn
      if (palf_end_scn.is_valid_and_not_min()) {
        sync_end_scn = palf_end_scn;
      }
      CLOG_LOG(TRACE, "get sync end scn (non-SYNC mode)", K(ls_id_), K(palf_end_scn), K(sync_end_scn));
    }
  }
  return ret;
}

int LogTransportStatus::check_has_remained_committed_log()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "transport status is not inited", K(ret));
  } else {
    palf::LSN last_sent = get_last_sent_lsn();
    palf::LSN palf_end_lsn;

    if (OB_NOT_NULL(palf_handle_) && palf_handle_->is_valid()
        && OB_SUCC(palf_handle_->get_end_lsn(palf_end_lsn))) {
      if (last_sent < palf_end_lsn) {
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "iterator found no log but palf has more, retry later",
                   K(last_sent), K(palf_end_lsn), KPC(this));
        }
        CLOG_LOG(TRACE, "iterator found no log but palf has more, retry later",
                 K(last_sent), K(palf_end_lsn), KPC(this));
      } else {
        CLOG_LOG(INFO, "no remained submit log", K(last_sent), K(palf_end_lsn), KPC(this));
      }
    } else {
      CLOG_LOG(INFO, "no remained submit log, palf handle invalid or get_end_lsn failed",
               KPC(this));
    }
  }

  return ret;
}

int LogTransportStatus::reset_retry_iterator(const palf::LSN &start_lsn)
{
  int ret = OB_SUCCESS;

  // 参数检查在锁外进行
  if (!start_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid start_lsn", K(ret), K(start_lsn));
  } else {
    WLockGuard guard(retry_lock_);

    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "transport status not init", K(ret));
    } else if (is_in_stop_state_) {
      // NB: 如果 transport_status 正在被销毁或已停止，不应该创建新的 iterator
      // 避免在 destroy() 过程中创建 iterator 导致泄漏
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "transport status in stop state, skip reset retry iterator", K(ls_id_), K(is_in_stop_state_));
    } else if (OB_ISNULL(palf_env_)) {
      // NB: palf_env_ 在 destroy() 中被设置为 nullptr，此时不应该创建新的 iterator
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "palf_env is null, skip reset retry iterator", K(ls_id_));
    } else if (OB_ISNULL(palf_handle_) || !palf_handle_->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "palf_handle is invalid", K(ret));
    } else {
      // 如果 iterator 已经初始化，先销毁
      if (retry_iterator_.is_inited()) {
        retry_iterator_.destroy();
      }

      if (OB_FAIL(seek_log_iterator(ls_id_, start_lsn, retry_iterator_))) {
        CLOG_LOG(WARN, "seek retry iterator failed", K(ret), K(start_lsn), K(ls_id_));
      } else if (OB_FAIL(retry_iterator_.set_io_context(palf::LogIOContext(MTL_ID(), ls_id_.id(), palf::LogIOUser::OTHER)))) {
        CLOG_LOG(WARN, "retry iterator set_io_context failed", K(ret), K(ls_id_));
        retry_iterator_.destroy();
      } else {
        retry_start_lsn_ = start_lsn;
        // 调用 next() 准备读取第一条日志
        int tmp_ret = retry_iterator_.next();
        if (OB_SUCCESS != tmp_ret && OB_ITER_END != tmp_ret) {
          CLOG_LOG(WARN, "retry iterator next failed", K(tmp_ret), K(start_lsn));
        }
        CLOG_LOG(INFO, "reset retry iterator success", K(ls_id_), K(start_lsn));
      }
    }
  }
  return ret;
}


int LogTransportStatus::check_need_periodic_retry_(palf::LSN &start_lsn, bool &need_reset)
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_STANDBY_SYNC_INTERVAL_US = 1 * 1000 * 1000; // 1秒
  const int64_t now_us = ObTimeUtility::fast_current_time();
  start_lsn.reset();
  need_reset = false;

  // 第一层：错误检查和前置条件
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "transport status not init", K(ret));
  } else if (!need_submit_log()) {
    // 正常情况：transport未启用或不需要提交日志，不需要重传
    CLOG_LOG(TRACE, "transport not enabled or not need submit log, skip", K(ls_id_));
  } else {
    WLockGuard guard(retry_lock_);
    if (now_us - last_retry_check_time_us_ < CHECK_STANDBY_SYNC_INTERVAL_US) {
      // 检查太频繁，返回 OB_EAGAIN
      ret = OB_EAGAIN;
      CLOG_LOG(TRACE, "check too frequent, skip", K(ls_id_));
    } else {
      // 第二层：主逻辑处理（更新检查时间，检查位点状态）
      last_retry_check_time_us_ = now_us;
      palf::LSN standby_lsn = get_standby_committed_end_lsn(); // 备库已提交的LSN
      palf::LSN palf_lsn = get_palf_committed_end_lsn(); // 主库已提交的LSN

      if (!standby_lsn.is_valid() || !palf_lsn.is_valid()) {
        // 正常情况：LSN无效，不需要重传，等待下一轮处理
        CLOG_LOG(WARN, "lsn invalid, skip", K(ls_id_), K(standby_lsn), K(palf_lsn));
      } else if (standby_lsn >= palf_lsn) {
        // 正常情况：备库已同步，清理iterator和状态
        if (retry_iterator_.is_inited()) {
          retry_iterator_.destroy();
          retry_start_lsn_.reset();
        }
        last_standby_lsn_.reset();
        last_standby_lsn_update_time_us_ = 0;
        CLOG_LOG(TRACE, "standby synced, no need retry", K(ls_id_), K(standby_lsn), K(palf_lsn));
      } else if (standby_lsn != last_standby_lsn_ ||
                !last_standby_lsn_.is_valid() ||
                last_standby_lsn_update_time_us_ == 0) {
        // 正常情况：备库位点有变化或第一次检查，更新记录
        palf::LSN old_standby_lsn = last_standby_lsn_;
        last_standby_lsn_ = standby_lsn;
        last_standby_lsn_update_time_us_ = now_us;
        CLOG_LOG(TRACE, "standby lsn changed or first check, update record",
                K(ls_id_), K(standby_lsn), K(old_standby_lsn), K(palf_lsn));
      } else if (now_us - last_standby_lsn_update_time_us_ < CHECK_STANDBY_SYNC_INTERVAL_US) {
        // 正常情况：位点未变化但时间未超过阈值
        CLOG_LOG(TRACE, "standby lsn unchanged but time not enough, skip",
                K(ls_id_), K(standby_lsn), K(palf_lsn),
                K(now_us - last_standby_lsn_update_time_us_));
      } else {
        // 主路径：备库位点不变超过1秒，需要重传
        start_lsn = standby_lsn;
        need_reset = (!retry_iterator_.is_inited() ||
                      !retry_start_lsn_.is_valid() ||
                      retry_start_lsn_ != start_lsn);
        CLOG_LOG(INFO, "standby lsn unchanged for more than 1s, need retry",
                K(ls_id_), K(standby_lsn), K(palf_lsn),
                K(now_us), K(last_standby_lsn_update_time_us_));
      }
    }
  }

  return ret;
}

// 读取并发送单条日志
int LogTransportStatus::read_and_send_single_log_()
{
  int ret = OB_SUCCESS;
  const char *buf = nullptr;
  int64_t size = 0;
  share::SCN scn;
  palf::LSN lsn;
  palf::LSN end_lsn;
  ipalf::IGroupEntry group_entry(GCONF.enable_logservice);
  char *task_buf = nullptr;
  ObLogTransportTask *task = nullptr;

  WLockGuard guard(retry_lock_);
  // 读取日志
  if (OB_FAIL(retry_iterator_.get_entry(buf, group_entry, lsn))) {
    if (OB_ITER_END == ret) {
      CLOG_LOG(TRACE, "iterator end", K(ls_id_));
    } else {
      CLOG_LOG(WARN, "get_entry failed", K(ret), K(ls_id_));
    }
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "buf is NULL", K(ret), K(ls_id_));
  } else {
    size = group_entry.get_serialize_size(lsn);
    scn = group_entry.get_scn();
    end_lsn = lsn + size;

    // 检查是否超过本地提交位点
    palf::LSN palf_lsn = get_palf_committed_end_lsn();
    if (end_lsn > palf_lsn) {
      ret = OB_ITER_END;
      CLOG_LOG(TRACE, "log exceeds palf lsn, stop", K(ls_id_), K(end_lsn), K(palf_lsn));
    } else {
      // 分配任务缓冲区
      const int64_t task_size = sizeof(ObLogTransportTask) + size;
      task_buf = static_cast<char *>(ob_malloc(task_size, "RetryTpTask"));
      if (OB_ISNULL(task_buf)) {
        ret = OB_EAGAIN;
        CLOG_LOG(WARN, "alloc task buf failed", K(ret), K(ls_id_));
      } else {
        task = new (task_buf) ObLogTransportTask(ls_id_, lsn, scn, size);
        char *log_buf = task_buf + sizeof(ObLogTransportTask);
        MEMCPY(log_buf, buf, size);

        if (OB_FAIL(task->init(log_buf))) {
          CLOG_LOG(WARN, "init task failed", K(ret), K(ls_id_));
          task->~ObLogTransportTask();
          ob_free(task_buf);
          task_buf = NULL;
          task = NULL;
        } else if (OB_FAIL(push_log_transport_task(*task))) {
          CLOG_LOG(WARN, "push task failed", K(ret), K(ls_id_));
          task->~ObLogTransportTask();
          ob_free(task_buf);
          task_buf = NULL;
          task = NULL;
        } else {
          // 主路径：推进iterator
          share::SCN next_min_scn;
          bool iterate_end = false;
          share::SCN transported_point = get_palf_committed_end_scn();
          if (OB_FAIL(retry_iterator_.next(transported_point, next_min_scn, iterate_end))) {
            if (OB_ITER_END == ret) {
              CLOG_LOG(TRACE, "iterator reached end after next", K(ls_id_));
            } else {
              CLOG_LOG(WARN, "iterator next failed", K(ret), K(ls_id_));
            }
          } else {
            CLOG_LOG(TRACE, "retry log sent", K(ls_id_), K(lsn), K(end_lsn), K(scn));
          }
        }
      }
    }
  }

  return ret;
}

int LogTransportStatus::read_and_send_logs_()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_LOGS_PER_ROUND = 10; // 每轮最多发送10条日志
  int64_t sent_count = 0;

  // 循环读取并发送日志
  while (OB_SUCC(ret) && sent_count < MAX_LOGS_PER_ROUND) {
    int tmp_ret = read_and_send_single_log_();
    if (OB_SUCCESS == tmp_ret) {
      sent_count++;
    } else if (OB_ITER_END == tmp_ret) {
      // 正常结束
      break;
    } else {
      ret = tmp_ret;
      break;
    }
  }

  // 批量提交所有有need_batch_push标志的status_task到全局队列，触发实际发送
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = batch_push_all_status_task_queue())) {
    CLOG_LOG(WARN, "failed to batch_push_all_status_task_queue in periodic retry",
              K(tmp_ret), K(ls_id_), K(sent_count));
  }

  palf::LSN standby_lsn = get_standby_committed_end_lsn();
  palf::LSN palf_lsn = get_palf_committed_end_lsn();
  CLOG_LOG(INFO, "periodic retry completed", K(ls_id_), K(sent_count),
            K(standby_lsn), K(palf_lsn));

  return ret;
}

// 错误注入点：用于验证周期性发日志功能
ERRSIM_POINT_DEF(EN_PERIODIC_RETRY_TRANSPORT_LOG);
int LogTransportStatus::periodic_retry_transport_log()
{
  int ret = OB_SUCCESS;

  // 错误注入：测试周期性发日志功能
  if (OB_UNLIKELY(EN_PERIODIC_RETRY_TRANSPORT_LOG)) {
    ret = EN_PERIODIC_RETRY_TRANSPORT_LOG;
    CLOG_LOG(WARN, "error injection: periodic retry transport log", K(ret), K(ls_id_));
  } else if (IS_NOT_INIT || is_in_stop_state_) {
    // NB: 如果 transport_status 正在被销毁或已停止，不应该创建新的 iterator
    // 避免在 destroy() 过程中创建 iterator 导致泄漏
    ret = OB_NOT_INIT;
    CLOG_LOG(TRACE, "transport status not init or in stop state, skip periodic retry", K(ls_id_), K(is_inited_), K(is_in_stop_state_));
  } else {
    // 第一步：检查是否需要重传，并确定起始LSN
    palf::LSN start_lsn;
    bool need_reset = false;

    if (OB_FAIL(check_need_periodic_retry_(start_lsn, need_reset))) {
      // OB_EAGAIN: 检查太频繁，正常情况，直接返回
      // OB_NOT_INIT: 初始化问题，返回错误
      if (OB_EAGAIN != ret) {
        CLOG_LOG(WARN, "check need periodic retry failed", KR(ret), K(ls_id_));
      }
    } else if (!start_lsn.is_valid()) {
      // start_lsn无效表示不需要重传（备库已同步、位点还在推进等），这是正常情况
      // 直接返回成功，不需要重传
      CLOG_LOG(TRACE, "start_lsn invalid, no need retry", K(ls_id_), K(start_lsn));
    } else {
      // 需要重传：重置iterator（如需要）并发送日志
      bool iterator_need_prepare = false;
      {
        RLockGuard guard(retry_lock_);
        iterator_need_prepare = (need_reset || !retry_iterator_.is_inited() || !retry_iterator_.is_valid());
      }
      if (iterator_need_prepare) {
        if (OB_FAIL(reset_retry_iterator(start_lsn))) {
          CLOG_LOG(WARN, "reset iterator failed", KR(ret), K(ls_id_), K(start_lsn));
        }
      }

      // iterator就绪后，读取并发送日志
      if (OB_SUCC(ret)) {
        bool iterator_ready = false;
        {
          RLockGuard guard(retry_lock_);
          iterator_ready = (retry_iterator_.is_inited() && retry_iterator_.is_valid());
        }
        if (!iterator_ready) {
          CLOG_LOG(WARN, "iterator not ready", K(ls_id_));
        } else {
          ret = read_and_send_logs_();
        }
      }
    }
  }

  return ret;
}

//---------------ObTransportFsCb---------------//
ObTransportFsCb::ObTransportFsCb()
  : transport_status_(nullptr)
{
}

ObTransportFsCb::ObTransportFsCb(LogTransportStatus *transport_status)
  : transport_status_(transport_status)
{
}

ObTransportFsCb::~ObTransportFsCb()
{
  destroy();
}

void ObTransportFsCb::destroy()
{
  transport_status_ = nullptr;
}

int ObTransportFsCb::update_end_lsn(int64_t id,
                                    const LSN &end_lsn,
                                    const SCN &end_scn,
                                    const int64_t proposal_id)
{
  UNUSED(id);
  UNUSED(proposal_id);

  int ret = OB_SUCCESS;
  if (OB_ISNULL(transport_status_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "transport_status is NULL", K(ret));
  } else {
    if (OB_FAIL(transport_status_->update_palf_committed_end_lsn(end_lsn, end_scn))) {
      CLOG_LOG(ERROR, "update_transport_lsn failed", K(ret), K(id), K(end_lsn), K(end_scn));
    } else {
      CLOG_LOG(TRACE, "ObTransportFsCb update_end_lsn", K(id), K(end_lsn), K(end_scn), K(proposal_id));
    }
  }
  return ret;
}

//---------------ObLogTransportService---------------//
ObLogTransportService::ObLogTransportService()
    : is_inited_(false),
      is_running_(false),
      tg_id_(-1),
      palf_env_(NULL),
      apply_service_(NULL),
      replay_service_(NULL),
      transport_status_map_(),
      rpc_proxy_(NULL),
      sync_standby_dest_lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
      sync_standby_dest_(),
      update_standby_addr_task_(*this),
      update_standby_addr_timer_(),
      periodic_retry_transport_task_(*this),
      periodic_retry_transport_timer_()
{
}

ObLogTransportService::~ObLogTransportService()
{
  destroy();
}

int ObLogTransportService::init(ipalf::IPalfEnv *palf_env,
                                ObLogApplyService *apply_service,
                                ObLogReplayService *replay_service,
                                rpc::frame::ObReqTransport *transport)
{
  int ret = OB_SUCCESS;
  const uint64_t MAP_TENANT_ID = MTL_ID();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogTransportService init twice", K(ret));
  } else if (OB_ISNULL(palf_env_ = palf_env) || OB_ISNULL(apply_service_ = apply_service) || OB_ISNULL(replay_service_ = replay_service) || OB_ISNULL(transport)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(palf_env), KP(apply_service), KP(replay_service), KP(transport));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TransportService, tg_id_))) {
    CLOG_LOG(WARN, "fail to create thread group", K(ret));
  } else if (OB_FAIL(MTL_REGISTER_THREAD_DYNAMIC(0.5, tg_id_))) {
    CLOG_LOG(WARN, "MTL_REGISTER_THREAD_DYNAMIC failed", K(ret), K(tg_id_));
  } else if (OB_FAIL(transport_status_map_.init("TRANSP_STATUS", MAP_TENANT_ID))) {
    CLOG_LOG(WARN, "transport_status_map_ init error", K(ret));
  } else {
    rpc_proxy_ = OB_NEW(obrpc::ObLogTransportRpcProxy, "LogTPRpcProxy");
    if (OB_ISNULL(rpc_proxy_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(ERROR, "allocate ObLogTransportRpcProxy failed", K(ret));
    } else if (OB_FAIL(rpc_proxy_->init(transport))) {
      CLOG_LOG(ERROR, "init ObLogTransportRpcProxy failed", K(ret), KP(transport));
      OB_DELETE(ObLogTransportRpcProxy, "LogTPRpcProxy", rpc_proxy_);
      rpc_proxy_ = nullptr;
    } else {
      is_inited_ = true;
      CLOG_LOG(INFO, "ObLogTransportService init success");
    }
  }

  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
    CLOG_LOG(WARN, "ObLogTransportService init failed", K(ret));
  }
  return ret;
}

int ObLogTransportService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogTransportService not init", KR(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    CLOG_LOG(ERROR, "start ObLogTransportService failed", K(ret));
  // } else if (OB_FAIL(TG_SET_ADAPTIVE_STRATEGY(tg_id_, adaptive_strategy))) {
  //   CLOG_LOG(WARN, "set adaptive strategy failed", K(ret));
  } else if (OB_FAIL(update_standby_addr_timer_.set_run_wrapper_with_ret(MTL_CTX()))) {
    CLOG_LOG(ERROR, "set run wrapper for update standby addr timer failed", K(ret));
  } else if (OB_FAIL(update_standby_addr_timer_.init("UpdateStandbyAddr"))) {
    CLOG_LOG(ERROR, "init update standby addr timer failed", K(ret));
  } else if (OB_FAIL(update_standby_addr_timer_.schedule(update_standby_addr_task_,
                                                         UPDATE_STANDBY_ADDR_INTERVAL_US, true))) {
    CLOG_LOG(ERROR, "schedule standby addr update task failed", K(ret));
  } else if (OB_FAIL(periodic_retry_transport_timer_.set_run_wrapper_with_ret(MTL_CTX()))) {
    CLOG_LOG(ERROR, "set run wrapper for periodic retry transport timer failed", K(ret));
  } else if (OB_FAIL(periodic_retry_transport_timer_.init("PeriodicTransportLog"))) {
    CLOG_LOG(ERROR, "init periodic retry transport timer failed", K(ret));
  } else if (OB_FAIL(periodic_retry_transport_timer_.schedule(periodic_retry_transport_task_,
                                                              PERIODIC_RETRY_TRANSPORT_INTERVAL_US, true))) {
    CLOG_LOG(ERROR, "schedule periodic retry transport task failed", K(ret));
  } else {
    ATOMIC_STORE(&is_running_, true);
    CLOG_LOG(INFO, "start ObLogTransportService success", K(ret), K(tg_id_));
  }
  return ret;
}

void ObLogTransportService::stop() {
  CLOG_LOG(INFO, "ObLogTransportService stop begin");
  ATOMIC_STORE(&is_running_, false);
  update_standby_addr_timer_.stop();
  periodic_retry_transport_timer_.stop();
  TG_STOP(tg_id_);
  CLOG_LOG(INFO, "stop ObLogTransportService finish");
}

void ObLogTransportService::wait() {
  CLOG_LOG(INFO, "ObLogTransportService wait begin");
  int64_t num = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(TG_GET_QUEUE_NUM(tg_id_, num)) && num > 0) {
    PAUSE();
  }
  if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "ObLogTransportService failed to get queue number");
  }
  TG_STOP(tg_id_);
  TG_WAIT(tg_id_);
  CLOG_LOG(INFO, "ObLogTransportService SimpleQueue destroy finish");
  CLOG_LOG(INFO, "ObLogTransportService wait finish");
  return;
}

void ObLogTransportService::destroy()
{
  CLOG_LOG(INFO, "ObLogTransportService destroy");
  ATOMIC_STORE(&is_running_, false);
  update_standby_addr_timer_.stop();
  update_standby_addr_timer_.wait();
  update_standby_addr_timer_.destroy();
  periodic_retry_transport_timer_.stop();
  periodic_retry_transport_timer_.wait();
  periodic_retry_transport_timer_.destroy();
  (void)remove_all_ls_();
  is_inited_ = false;
  {
    common::SpinWLockGuard guard(sync_standby_dest_lock_);
    sync_standby_dest_.reset();
  }
  if (-1 != tg_id_) {
    MTL_UNREGISTER_THREAD_DYNAMIC(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  palf_env_ = NULL;
  apply_service_ = NULL;
  replay_service_ = NULL;
  transport_status_map_.destroy();
  if (nullptr != rpc_proxy_) {
    rpc_proxy_->destroy();
    OB_DELETE(ObLogTransportRpcProxy, "LogTransportRpcProxy", rpc_proxy_);
    rpc_proxy_ = nullptr;
  }
  CLOG_LOG(INFO, "ObLogTransportService destroy finished");
}

int ObLogTransportService::query_sync_standby_dest(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  LogTransportStatus *transport_status = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogTransportService not init", KR(ret));
  } else if (OB_FAIL(transport_status_map_.get(ls_id, transport_status))) {
    CLOG_LOG(WARN, "failed to get transport status", KR(ret), K(ls_id));
  } else {
    // 同步获取sync_standby_dest，确保启用时就有有效值
    // 避免异步TimerTask延迟导致do_transport_task_处理时sync_standby_dest无效
    ObSyncStandbyDestStruct sync_standby_dest;
    bool is_empty = false;
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      CLOG_LOG(WARN, "sql_proxy is NULL, will rely on timer task to update sync_standby_dest", K(ls_id));
    } else if (OB_SUCCESS != (tmp_ret = ObSyncStandbyDestOperator::read_sync_standby_dest(
                    *GCTX.sql_proxy_, gen_meta_tenant_id(MTL_ID()), false, is_empty, sync_standby_dest))) {
      CLOG_LOG(WARN, "failed to read sync standby dest, will rely on timer task to update",
                KR(tmp_ret), K(ls_id), K(MTL_ID()));
    } else if (is_empty) {
      CLOG_LOG(INFO, "no standby dest found when enable sync mode, will rely on timer task to update", K(ls_id));
    } else if (OB_FAIL(set_sync_standby_dest(sync_standby_dest))) {
      CLOG_LOG(WARN, "failed to set sync standby dest, will rely on timer task to update",
                KR(ret), K(ls_id), K(sync_standby_dest));
      // 不阻止enable，定时任务会后续更新
    } else if (!sync_standby_dest.is_valid()) {
      CLOG_LOG(WARN, "sync standby dest is invalid, will rely on timer task to update", K(ls_id), K(sync_standby_dest));
    } else {
      CLOG_LOG(INFO, "get sync standby dest success when enable sync mode", K(ls_id), K(sync_standby_dest));
    }
  }
  return ret;
}

int ObLogTransportService::disable_sync_mode(const share::ObLSID &ls_id, const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  ObTpStatusGuard guard;
  LogTransportStatus *transport_status = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogTransportService not init", KR(ret));
  } else if (OB_FAIL(get_transport_status(ls_id, guard))) {
    CLOG_LOG(WARN, "failed to get transport status", KR(ret), K(ls_id));
  } else if (OB_ISNULL(transport_status = guard.get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport status is NULL", KR(ret), K(ls_id));
  } else if (OB_FAIL(transport_status->disable_status(proposal_id))) {
    CLOG_LOG(WARN, "failed to disable transport status", KR(ret), K(ls_id), K(proposal_id));
  } else {
    // 禁用强同步模式时，清除备库位点信息
    // 确保disable_sync_mode和clear_standby_info原子执行，避免状态不一致
    transport_status->clear_standby_committed_end_lsn();
    CLOG_LOG(INFO, "disable sync mode success", K(ls_id), K(proposal_id));
  }
  return ret;
}

int ObLogTransportService::clear_standby_info(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObTpStatusGuard guard;
  LogTransportStatus *transport_status = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogTransportService not init", KR(ret));
  } else if (OB_FAIL(get_transport_status(ls_id, guard))) {
    CLOG_LOG(WARN, "failed to get transport status", KR(ret), K(ls_id));
  } else if (OB_ISNULL(transport_status = guard.get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport status is NULL", KR(ret), K(ls_id));
  } else {
    transport_status->clear_standby_committed_end_lsn();
    CLOG_LOG(INFO, "clear standby committed end lsn success", K(ls_id));
  }
  return ret;
}

int ObLogTransportService::get_sync_end_scn(const share::ObLSID &ls_id, share::SCN &sync_end_scn)
{
  int ret = OB_SUCCESS;
  ObTpStatusGuard guard;
  LogTransportStatus *transport_status = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogTransportService not init", KR(ret));
  } else if (OB_FAIL(get_transport_status(ls_id, guard))) {
    CLOG_LOG(WARN, "failed to get transport status", KR(ret), K(ls_id));
  } else if (OB_ISNULL(transport_status = guard.get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport status is NULL", KR(ret), K(ls_id));
  } else if (OB_FAIL(transport_status->get_sync_end_scn(sync_end_scn))) {
    CLOG_LOG(WARN, "failed to get sync end scn", KR(ret), K(ls_id));
  }
  return ret;
}

int ObLogTransportService::get_standby_sync_scn(const share::ObLSID &ls_id,
                                                share::SCN &palf_sync_scn,
                                                share::SCN &standby_sync_scn)
{
  int ret = OB_SUCCESS;
  ObTpStatusGuard guard;
  LogTransportStatus *transport_status = NULL;
  palf_sync_scn = SCN::min_scn();
  standby_sync_scn = SCN::min_scn();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogTransportService not init", KR(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid ls_id", KR(ret), K(ls_id));
  } else if (OB_FAIL(get_transport_status(ls_id, guard))) {
    CLOG_LOG(WARN, "failed to get transport status", KR(ret), K(ls_id));
  } else if (OB_ISNULL(transport_status = guard.get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport_status is null", KR(ret), K(ls_id));
  } else {
    palf_sync_scn = transport_status->get_palf_committed_end_scn();
    standby_sync_scn = transport_status->get_standby_committed_end_scn();
  }
  return ret;
}

int ObLogTransportService::query_standby_addr_(share::ObLogRestoreProxyUtil *restore_proxy,
                                                const share::ObRestoreSourceServiceAttr &service_attr,
                                                const share::ObLSID &ls_id,
                                                common::ObAddr &standby_addr)
{
  int ret = OB_SUCCESS;
  standby_addr.reset();

  if (OB_ISNULL(restore_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "restore_proxy is NULL", K(ret), K(ls_id));
  } else if (OB_UNLIKELY(!service_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "service_attr is invalid", K(ret), K(ls_id), K(service_attr));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "srv_rpc_proxy_ is null", K(ret));
  } else if (OB_FAIL(restore_proxy->get_primary_ls_leader_addr_by_rpc(
                         service_attr, GCTX.srv_rpc_proxy_, ls_id, standby_addr))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      CLOG_LOG(INFO, "no leader found for log stream in standby cluster", K(ret), K(ls_id));
    } else {
      CLOG_LOG(WARN, "get_standby_ls_leader_addr_by_rpc failed", K(ret), K(ls_id));
    }
  }
  CLOG_LOG(INFO, "query standby addr success", K(ls_id), K(standby_addr));
  return ret;
}

void ObLogTransportService::UpdateStandbyAddrTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (! is_user_tenant(MTL_ID())) {
    CLOG_LOG(TRACE, "not user tenant, skip update standby addr", K(MTL_ID()));
    return;
  } else if (! transport_service_.is_inited_) {
    CLOG_LOG(TRACE, "transport service is not inited", K(MTL_ID()));
    return;
  } else if (false == ATOMIC_LOAD(&transport_service_.is_running_)) {
    CLOG_LOG(INFO, "transport service is not running, skip update standby addr",
             K(MTL_ID()), K(transport_service_.is_running_), K(transport_service_.is_inited_));
    return;
  }

  // STEP 1. collect append mode ls ids
  common::ObArray<share::ObLSID> ls_id_array;
  if (OB_FAIL(collect_ls_list_(ls_id_array))) {
    CLOG_LOG(WARN, "failed to collect append mode ls ids", KR(ret));
  }

  // STEP 2. read and update global sync standby dest
  ObSyncStandbyDestStruct sync_standby_dest;
  bool is_empty = false;
  bool sync_standby_dest_changed = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(read_and_update_sync_standby_dest_(sync_standby_dest, is_empty, sync_standby_dest_changed))) {
      CLOG_LOG(WARN, "failed to read and update sync standby dest", KR(ret));
    } else if (is_empty) {
      CLOG_LOG(INFO, "no standby addr found");
      return;
    }
  }

  // STEP 3. init restore proxy
  share::ObLogRestoreProxyUtil restore_proxy; //TODO by ziqi: 改成单例或私有变量
  if (OB_SUCC(ret) && !is_empty) {
    if (OB_FAIL(init_restore_proxy_(sync_standby_dest, is_empty, restore_proxy))) {
      CLOG_LOG(WARN, "failed to init restore proxy", KR(ret));
    }
  }

  // STEP 4. update all ls standby addr
  if (OB_SUCC(ret) && !is_empty) {
    if (OB_FAIL(update_all_ls_standby_addr_(ls_id_array, sync_standby_dest, restore_proxy, sync_standby_dest_changed))) {
      CLOG_LOG(WARN, "failed to update all ls standby addr", KR(ret));
    }
  }
}

int ObLogTransportService::UpdateStandbyAddrTask::collect_ls_list_(
    common::ObArray<share::ObLSID> &ls_id_array)
{
  int ret = OB_SUCCESS;
  class CollectLSIdFunctor
  {
  public:
    CollectLSIdFunctor(common::ObArray<share::ObLSID> &ls_id_array)
      : ls_id_array_(ls_id_array) {}
    bool operator()(const share::ObLSID &ls_id, LogTransportStatus *transport_status)
    {
      int ret = OB_SUCCESS;
      palf::AccessMode access_mode = palf::AccessMode::INVALID_ACCESS_MODE;
      if (OB_ISNULL(transport_status)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "transport_status is NULL", KR(ret), K(ls_id));
        return true;
      }
      ObTpStatusGuard guard;
      guard.set_transport_status_(transport_status);
      LogTransportStatus *tp_status = guard.get_transport_status();
      ipalf::IPalfHandle *palf_handle = nullptr;
      if (OB_ISNULL(palf_handle = tp_status->get_palf_handle())) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "palf_handle is NULL", KR(ret), K(ls_id));
        return true;
      } else if (OB_FAIL(palf_handle->get_access_mode(access_mode))) {
        CLOG_LOG(WARN, "failed to get access mode", KR(ret), K(ls_id));
      // } else if (access_mode != palf::AccessMode::APPEND) {
      //   CLOG_LOG(INFO, "access mode is not append, skip", K(ls_id));
      //   return true;
      } else if (OB_FAIL(ls_id_array_.push_back(ls_id))) {
        CLOG_LOG(WARN, "failed to push back ls_id", K(ret), K(ls_id));
      }
      return OB_SUCCESS == ret;
    }
  private:
    common::ObArray<share::ObLSID> &ls_id_array_;
  };

  CollectLSIdFunctor collect_functor(ls_id_array);
  transport_service_.transport_status_map_.for_each(collect_functor);
  return ret;
}

int ObLogTransportService::UpdateStandbyAddrTask::read_and_update_sync_standby_dest_(
    share::ObSyncStandbyDestStruct &sync_standby_dest,
    bool &is_empty,
    bool &sync_standby_dest_changed)
{
  int ret = OB_SUCCESS;
  is_empty = false;
  sync_standby_dest_changed = false;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "sql_proxy is NULL", KR(ret));
  } else if (OB_FAIL(ObSyncStandbyDestOperator::read_sync_standby_dest(*GCTX.sql_proxy_,
      gen_meta_tenant_id(MTL_ID()), false, is_empty, sync_standby_dest))) {
    CLOG_LOG(WARN, "failed to read sync standby dest", KR(ret), K(MTL_ID()));
  } else if (is_empty) {
    CLOG_LOG(INFO, "no standby addr found");
  } else if (!sync_standby_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "sync standby dest is invalid", KR(ret), K(sync_standby_dest));
  } else {
    CLOG_LOG(INFO, "read sync standby dest success", K(sync_standby_dest));
    // 更新transport_service_中的sync_standby_dest_
    ObSyncStandbyDestStruct old_sync_standby_dest;
    if (OB_FAIL(transport_service_.get_sync_standby_dest(old_sync_standby_dest))) {
      CLOG_LOG(WARN, "failed to get sync standby dest", KR(ret));
    } else if (!old_sync_standby_dest.is_valid() && sync_standby_dest.is_valid()) {
      sync_standby_dest_changed = true;  // 从无效变为有效
    }

    if (OB_FAIL(transport_service_.set_sync_standby_dest(sync_standby_dest))) {
      //overwrite ret
      CLOG_LOG(WARN, "failed to set sync standby dest", KR(ret), K(sync_standby_dest));
    }
  }
  return ret;
}

int ObLogTransportService::UpdateStandbyAddrTask::init_restore_proxy_(
    const share::ObSyncStandbyDestStruct &sync_standby_dest,
    bool is_empty,
    share::ObLogRestoreProxyUtil &restore_proxy)
{
  int ret = OB_SUCCESS;
  if (is_empty) {
    CLOG_LOG(INFO, "no standby addr found");
  } else if (sync_standby_dest.is_valid()) {
    if (OB_FAIL(restore_proxy.init_with_service_attr(MTL_ID(), &sync_standby_dest.restore_source_service_attr_))) {
      CLOG_LOG(WARN, "restore_proxy init failed", KR(ret));
    }
  }
  return ret;
}

int ObLogTransportService::UpdateStandbyAddrTask::update_single_ls_standby_addr_(
    const share::ObLSID &ls_id,
    const share::ObSyncStandbyDestStruct &sync_standby_dest,
    share::ObLogRestoreProxyUtil &restore_proxy,
    bool sync_standby_dest_changed)
{
  int ret = OB_SUCCESS;
  LogTransportStatus *transport_status = NULL;
  ObTpStatusGuard guard;

  // 通过get方法获取transport_status（短暂持有锁，立即释放）
  if (OB_FAIL(transport_service_.get_transport_status(ls_id, guard))) {
    CLOG_LOG(WARN, "failed to get transport status", KR(ret), K(ls_id));
  } else if (NULL == (transport_status = guard.get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport status is NULL", KR(ret), K(ls_id));
  } else if (!sync_standby_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "sync_standby_dest is invalid", KR(ret), K(ls_id));
  } else {
    common::ObAddr standby_addr;
    // 直接使用外部已经获取的sync_standby_dest，避免重复获取
    if (OB_FAIL(transport_service_.query_standby_addr_(&restore_proxy,
                           sync_standby_dest.restore_source_service_attr_,
                           ls_id, standby_addr))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        common::ObAddr invalid_addr;
        transport_status->set_standby_addr(invalid_addr);
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          CLOG_LOG(INFO, "no standby addr found, clear it", K(ls_id));
        }
        ret = OB_SUCCESS;  // OB_ENTRY_NOT_EXIST不是错误，继续处理
      } else {
        CLOG_LOG(WARN, "get standby addr failed", KR(ret), K(ls_id));
      }
    } else {
      const common::ObAddr &old_addr = transport_status->get_standby_addr();
      if (old_addr != standby_addr) {  //TODO by ziqi: 补充case
        if (OB_FAIL(transport_status->set_standby_addr(standby_addr))) {
          CLOG_LOG(WARN, "set_standby_addr failed", KR(ret), K(ls_id), K(standby_addr));
        } else {
          CLOG_LOG(INFO, "update standby addr", K(ls_id), K(old_addr), K(standby_addr));
        }
      } else {
        CLOG_LOG(TRACE, "standby addr unchanged", K(ls_id), K(standby_addr));
      }
    }

    // 如果sync_standby_dest从无效变为有效，重新提交所有有任务的status_task到全局队列
    // 这样可以重新处理之前因为sync_standby_dest无效而失败的任务
    if (OB_SUCC(ret) && sync_standby_dest_changed && transport_status->is_enabled()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = transport_status->retry_all_status_task_queue())) {
        CLOG_LOG(WARN, "failed to retry_all_status_task_queue after sync_standby_dest recovered",
                 KR(tmp_ret), K(ls_id));
      } else {
        CLOG_LOG(INFO, "retry_all_status_task_queue after sync_standby_dest recovered", K(ls_id));
      }
    }
  }
  return ret;
}

int ObLogTransportService::UpdateStandbyAddrTask::update_all_ls_standby_addr_(
    const common::ObArray<share::ObLSID> &ls_id_array,
    const share::ObSyncStandbyDestStruct &sync_standby_dest,
    share::ObLogRestoreProxyUtil &restore_proxy,
    bool sync_standby_dest_changed)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < ls_id_array.count(); ++i) {
    const share::ObLSID &ls_id = ls_id_array.at(i);
    int tmp_ret = update_single_ls_standby_addr_(ls_id, sync_standby_dest, restore_proxy, sync_standby_dest_changed);
    if (OB_FAIL(tmp_ret)) {
      CLOG_LOG(WARN, "failed to update single ls standby addr", KR(tmp_ret), K(ls_id));
      // 继续处理其他LS，不中断循环
      ret = tmp_ret;
    }
  }
  return ret;
}

void ObLogTransportService::PeriodicRetryTransportTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(MTL_ID())) {
    CLOG_LOG(TRACE, "not user tenant, skip periodic retry transport", K(MTL_ID()));
    return;
  } else if (!transport_service_.is_inited_) {
    CLOG_LOG(TRACE, "transport service is not inited, skip periodic retry transport", K(MTL_ID()));
    return;
  } else if (false == ATOMIC_LOAD(&transport_service_.is_running_)) {
    CLOG_LOG(INFO, "transport service is not running, skip periodic retry transport",
             K(MTL_ID()), K(transport_service_.is_running_), K(transport_service_.is_inited_));
    return;
  }

  // 第一步：在 for_each 中收集需要处理的ls_id
  // 避免在 for_each 中执行耗时操作，防止长时间持有 map 的锁
  common::ObArray<share::ObLSID> ls_id_array;
  class CollectLSIDFunctor
  {
  public:
    CollectLSIDFunctor(common::ObArray<share::ObLSID> &array)
      : ls_id_array_(array) {}
    bool operator()(const share::ObLSID &ls_id, LogTransportStatus *transport_status)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(transport_status)) {
        //ignore error
        CLOG_LOG(WARN, "transport_status is NULL", K(ls_id));
        return true; // 继续处理下一个
      }

      // 只收集ls_id，不执行耗时操作
      if (OB_FAIL(ls_id_array_.push_back(ls_id))) {
        CLOG_LOG(WARN, "failed to push back ls_id", KR(ret), K(ls_id));
      }
      return true;
    }
  private:
    common::ObArray<share::ObLSID> &ls_id_array_;
  };

  CollectLSIDFunctor collect_functor(ls_id_array);
  transport_service_.transport_status_map_.for_each(collect_functor);

  // 第二步：在 for_each 循环外部执行耗时操作
  for (int64_t i = 0; i < ls_id_array.count(); ++i) {
    const share::ObLSID &ls_id = ls_id_array.at(i);
    LogTransportStatus *transport_status = NULL;

    // 通过ls_id从map中获取transport_status，如果已被删除则跳过
    ObTpStatusGuard guard;
    if (OB_FAIL(transport_service_.get_transport_status(ls_id, guard))) {
      CLOG_LOG(WARN, "failed to get transport status", KR(ret), K(ls_id));
      continue;
    }
    transport_status = guard.get_transport_status();
    if (OB_NOT_NULL(transport_status)) {
      // 调用周期性重传方法（耗时操作，在 map 锁外执行）
      int tmp_ret = transport_status->periodic_retry_transport_log();
      if (OB_SUCCESS != tmp_ret && OB_EAGAIN != tmp_ret) {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          CLOG_LOG(WARN, "periodic retry transport log failed", KR(tmp_ret), K(ls_id));
        }
      }
    }
  }
}

int ObLogTransportService::add_ls(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  LogTransportStatus *transport_status = NULL;
  ObMemAttr attr(MTL_ID(), "TransportStatus");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogTransportService not init", KR(ret));
  } else if (false == ATOMIC_LOAD(&is_running_)) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(ERROR, "ObLogTransportService has been stopped", KR(ret));
  } else if (NULL == (transport_status = static_cast<LogTransportStatus *>(mtl_malloc(sizeof(LogTransportStatus), attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to alloc transport status", KR(ret), K(id));
  } else {
    new (transport_status) LogTransportStatus();
    if (OB_FAIL(transport_status->init(id, palf_env_, this))) {
      mtl_free(transport_status);
      transport_status = NULL;
      CLOG_LOG(WARN, "failed to init transport status", KR(ret), K(id));
    } else {
      transport_status->inc_ref();
      if (OB_FAIL(transport_status_map_.insert(id, transport_status))) {
        CLOG_LOG(WARN, "failed to add transport status", KR(ret), K(id));
        revert_transport_status_(transport_status);
        transport_status = NULL;
      } else {
        CLOG_LOG(INFO, "add transport status success", KR(ret), K(id));
      }
    }
  }
  return ret;
}

int ObLogTransportService::remove_ls(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  RemoveTransportStatusFunctor functor;
  if (OB_FAIL(transport_status_map_.erase_if(id, functor))) {
    CLOG_LOG(WARN, "failed to remove log stream", K(ret), K(id));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else {
    CLOG_LOG(INFO, "remove transport status success", KR(ret), K(id));
  }
  return ret;
}

//---------------ObTpStatusGuard---------------//
ObTpStatusGuard::ObTpStatusGuard()
    : transport_status_(NULL)
{}

ObTpStatusGuard::~ObTpStatusGuard()
{
  if (NULL != transport_status_) {
    if (0 == transport_status_->dec_ref()) {
      CLOG_LOG(INFO, "free transport status", KPC(transport_status_));
      transport_status_->~LogTransportStatus();
      mtl_free(transport_status_);
    }
    transport_status_ = NULL;
  }
}

LogTransportStatus *ObTpStatusGuard::get_transport_status()
{
  return transport_status_;
}

void ObTpStatusGuard::set_transport_status_(LogTransportStatus *transport_status)
{
  transport_status_ = transport_status;
  transport_status_->inc_ref();
}

int ObLogTransportService::get_transport_status(const share::ObLSID &id, ObTpStatusGuard &guard)
{
  int ret = OB_SUCCESS;
  LogTransportStatus *status = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportService not init", KR(ret), K(id));
  } else if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(id));
  } else if (OB_FAIL(transport_status_map_.get(id, status))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      CLOG_LOG(TRACE, "transport status not exist", K(id));
    } else {
      CLOG_LOG(WARN, "failed to get transport status", KR(ret), K(id));
    }
  } else if (OB_ISNULL(status)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport status is NULL", KR(ret), K(id));
  } else {
    guard.set_transport_status_(status);
    CLOG_LOG(TRACE, "get transport status success", K(id));
  }
  return ret;
}

int ObLogTransportService::remove_all_ls_()
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObLSID> ls_id_array;
  RemoveTransportStatusFunctor functor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportService not init", KR(ret));
  } else {
    class CollectLSIdFunctor
    {
    public:
      CollectLSIdFunctor(common::ObArray<share::ObLSID> &ls_id_array)
        : ls_id_array_(ls_id_array) {}
      bool operator()(const share::ObLSID &id, LogTransportStatus *transport_status)
      {
        int ret = OB_SUCCESS;
        if (OB_ISNULL(transport_status)) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "transport status is NULL", K(id), KR(ret));
        } else if (OB_FAIL(ls_id_array_.push_back(id))) {
          CLOG_LOG(WARN, "failed to push back ls_id", K(id), KR(ret));
        }
        return OB_SUCCESS == ret;
      }
    private:
      common::ObArray<share::ObLSID> &ls_id_array_;
    };

    CollectLSIdFunctor collect_functor(ls_id_array);
    if (OB_FAIL(transport_status_map_.for_each(collect_functor))) {
      CLOG_LOG(WARN, "failed to collect transport status", KR(ret));
    } else {
      for (int64_t i = 0; i < ls_id_array.count(); ++i) {
        const share::ObLSID &ls_id = ls_id_array.at(i);
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = transport_status_map_.erase_if(ls_id, functor))) {
          CLOG_LOG(WARN, "failed to remove transport status", KR(tmp_ret), K(ls_id));
          ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
        }
      }
      CLOG_LOG(INFO, "remove transport status success", KR(ret));
    }
  }
  return ret;
}

bool ObLogTransportService::RemoveTransportStatusFunctor::operator()(const share::ObLSID &id,
                                                                     LogTransportStatus *transport_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(transport_status)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport status is NULL", K(id), KR(ret));
  } else if (OB_FAIL(transport_status->disable_status(palf::INVALID_PROPOSAL_ID))) {
    // use INT64_MAX to ensure disable_status always succeeds when removing transport status
    CLOG_LOG(WARN, "failed to disable status", K(id), KR(ret));
  }
  if (OB_SUCCESS == ret && 0 == transport_status->dec_ref()) {
    CLOG_LOG(INFO, "free transport status", KPC(transport_status));
    transport_status->~LogTransportStatus();
    mtl_free(transport_status);
  }
  ret_code_ = ret;
  return OB_SUCCESS == ret;
}

int ObLogTransportService::is_standby_sync_done(const share::ObLSID &id, bool &is_done)
{
  int ret = OB_SUCCESS;
  ObTpStatusGuard guard;
  LogTransportStatus *transport_status = NULL;
  if (!is_user_tenant(MTL_ID())) {
    is_done = true;
    CLOG_LOG(INFO, "not user tenant, skip is_standby_sync_done", K(id), K(MTL_ID()));
  } else if (OB_FAIL(get_transport_status(id, guard))) {
    CLOG_LOG(WARN, "failed to get transport status", K(id), KR(ret));
  } else if (NULL == (transport_status = guard.get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport status is NULL", K(id), KR(ret));
  } else {
    ret = transport_status->is_standby_sync_done(is_done);
  }
  return ret;
}

share::SCN ObLogTransportService::inner_get_replayed_point_()
{
  int ret = OB_SUCCESS;
  share::SCN replayable_scn;
  if (OB_FAIL(replay_service_->get_replayable_point(replayable_scn))) {
    CLOG_LOG(WARN, "failed to get replayable point", KR(ret));
    replayable_scn = share::SCN::max_scn();
  }
  return replayable_scn;
}

// 读取并提交单条日志到队列
int ObLogTransportService::fetch_and_submit_single_log_(LogTransportStatus &transport_status,
                                                        ObTransportServiceSubmitTask *submit_task,
                                                        palf::LSN &cur_lsn,
                                                        share::SCN &cur_log_scn,
                                                        int64_t &log_size)
{
  int ret = OB_SUCCESS;
  const char *log_buf = NULL;
  share::ObLSID id;
  bool need_skip = false;
  bool need_iterate_next_log = false;
  palf::LSN end_lsn;
  ObLogTransportTask *transport_task = NULL;
  char *task_buf = NULL;
  const SCN &transported_point = transport_status.get_palf_committed_end_scn();

  if (OB_UNLIKELY(OB_ISNULL(submit_task))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "submit task is NULL when fetch log", K(transport_status), KPC(submit_task));
  } else if (OB_FAIL(submit_task->get_log(log_buf, log_size, cur_log_scn, cur_lsn, end_lsn))) {
    CLOG_LOG(WARN, "submit task get log value failed", K(transport_status), KPC(submit_task));
  } else if (OB_FAIL(submit_task->need_skip(cur_log_scn, need_skip))) {
    CLOG_LOG(INFO, "check need_skip failed", K(transport_status), K(cur_lsn), K(log_size), K(cur_log_scn));
  } else if (need_skip) {
    need_iterate_next_log = true;
    CLOG_LOG(TRACE, "skip current log", K(id), K(transport_status), K(cur_lsn), K(end_lsn), K(log_size), K(cur_log_scn), K(transported_point));
    // 跳过日志时需要调用next_log()推进iterator，否则会一直读取同一条日志
    // 但不需要调用update_submit_log_meta_info，因为这条日志没有被处理
  } else if (OB_FAIL(transport_status.get_ls_id(id))) {
    CLOG_LOG(WARN, "transport status get ls id failed", K(transport_status), K(id), KR(ret));
  } else {
    // 分配传输任务内存
    const int64_t task_size = sizeof(ObLogTransportTask) + log_size;
    if (OB_UNLIKELY(NULL == (task_buf = static_cast<char *>(ob_malloc(task_size, "TransportTask"))))) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        CLOG_LOG(WARN, "failed to alloc transport task buf", K(ret), K(id), K(cur_lsn));
      }
    } else {
      transport_task = new (task_buf) ObLogTransportTask(id, cur_lsn, cur_log_scn, log_size);
      char *task_log_buf = task_buf + sizeof(ObLogTransportTask);
      MEMCPY(task_log_buf, log_buf, log_size);
      if (OB_FAIL(transport_task->init(task_log_buf))) {
        CLOG_LOG(WARN, "init transport task failed", K(ret), K(id));
        transport_task->~ObLogTransportTask();
        ob_free(task_buf);
        transport_task = NULL;
        task_buf = NULL;
      }
    }
  }
  if (OB_SUCC(ret) && NULL != transport_task) {
    if (OB_SUCC(transport_status.push_log_transport_task(*transport_task))) {
      transport_status.update_last_sent_lsn(end_lsn);
      transport_status.update_last_sent_scn(cur_log_scn);
      need_iterate_next_log = true;
      CLOG_LOG(TRACE, "push_log_transport_task success", K(id), K(cur_lsn), K(end_lsn), K(log_size), K(cur_log_scn));
    } else {
      CLOG_LOG(WARN, "push_log_transport_task failed", K(ret), K(transport_status), K(cur_lsn));
      transport_task->~ObLogTransportTask();
      ob_free(task_buf);
      transport_task = NULL;
      task_buf = NULL;
    }
  }

  if (OB_SUCC(ret) && need_iterate_next_log) {
    bool unused_iterate_end_by_transported_point = false;
    // 正常处理日志时，更新meta_info并推进iterator
    if (OB_FAIL(submit_task->update_submit_log_meta_info(end_lsn, cur_log_scn))) {
      CLOG_LOG(ERROR, "failed to update_submit_log_meta_info", KR(ret), K(cur_lsn),
                K(log_size), K(cur_log_scn));
    } else if (OB_FAIL(submit_task->next_log(transported_point, unused_iterate_end_by_transported_point))) {
      CLOG_LOG(ERROR, "failed to next_log", K(transport_status), K(cur_lsn), K(log_size),
                K(cur_log_scn), K(transported_point), K(ret));
    }
  }
  CLOG_LOG(TRACE, "fetch and submit single log end", K(ret), K(id), K(cur_lsn), K(end_lsn), K(log_size), K(cur_log_scn), K(transported_point), K(need_iterate_next_log));
  return ret;
}

// 处理提交任务 - 读取日志并提交到队列
int ObLogTransportService::handle_submit_task_(ObTransportServiceSubmitTask *submit_task,
                                                bool &is_timeslice_run_out)
{
  int ret = OB_SUCCESS;
  LogTransportStatus *transport_status = NULL;
  const int64_t MAX_SUBMIT_TIME_PER_ROUND = 10 * 1000; // 10ms
  const int64_t BATCH_PUSH_TRANSPORT_TASK_COUNT_THRESOLD = 16;
  const int64_t BATCH_PUSH_TRANSPORT_TASK_SIZE_THRESOLD = 16 * 1024 * 1024;  // 16MB

  if (OB_ISNULL(submit_task)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "submit_log_task is NULL", KPC(submit_task), KR(ret));
  } else if (OB_ISNULL(transport_status = submit_task->get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "transport status is NULL", KPC(submit_task), KPC(transport_status), KR(ret));
  } else if (transport_status->try_rdlock()) {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    bool need_submit_log = true;
    bool iterate_end_by_replayable_point = false;
    int count = 0;
    palf::LSN last_batch_to_submit_lsn;

    while (OB_SUCC(ret) && need_submit_log && (!is_timeslice_run_out)) {
      int64_t log_size = 0;
      palf::LSN to_submit_lsn;
      share::SCN to_submit_scn;

      if (!transport_status->is_enabled_without_lock() || !transport_status->need_submit_log()) {
        need_submit_log = false;
        CLOG_LOG(TRACE, "transport status is not enabled or need not submit log", KPC(transport_status));
      } else {
        const SCN &transported_point = transport_status->get_palf_committed_end_scn();
        CLOG_LOG(TRACE, "transported point", K(transported_point), KPC(transport_status));
        need_submit_log = submit_task->has_remained_submit_log(transported_point,
                                                               iterate_end_by_replayable_point);
        if (!need_submit_log) {
          // 检查是否有未发送的已提交日志（通过比较 last_sent_lsn 和 palf end_lsn）
          ret = transport_status->check_has_remained_committed_log();
        } else if (OB_SUCC(fetch_and_submit_single_log_(*transport_status, submit_task,
                                                        to_submit_lsn, to_submit_scn, log_size))) {
          count++;
          // 定期批量提交 status_task 到全局队列
          if (!last_batch_to_submit_lsn.is_valid()) {
            last_batch_to_submit_lsn = to_submit_lsn;
          } else if ((0 == (count & (BATCH_PUSH_TRANSPORT_TASK_COUNT_THRESOLD - 1)))
                     || ((to_submit_lsn - last_batch_to_submit_lsn) > BATCH_PUSH_TRANSPORT_TASK_SIZE_THRESOLD)) {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = transport_status->batch_push_all_status_task_queue())) {
              CLOG_LOG(WARN, "failed to batch_push_all_status_task_queue",
                       KR(tmp_ret), KPC(transport_status));
            } else {
              last_batch_to_submit_lsn = to_submit_lsn;
            }
          }

          if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
            CLOG_LOG(INFO, "succ to submit log task to transport service",
                     K(to_submit_lsn), K(to_submit_scn), KPC(transport_status));
          }
        } else if (OB_EAGAIN == ret) {
          CLOG_LOG(TRACE, "fetch and submit single log failed, will retry",
                   K(to_submit_lsn), KPC(transport_status), K(ret));
          // do nothing, will retry
        } else {
          CLOG_LOG(WARN, "fetch and submit single log failed",
                   K(to_submit_lsn), KPC(transport_status), K(ret));
        }
      }

      int64_t used_time = ObTimeUtility::fast_current_time() - start_ts;
      if (OB_SUCC(ret) && used_time > MAX_SUBMIT_TIME_PER_ROUND) {
        is_timeslice_run_out = true;
      }
    }

    // 最后再次批量提交，确保所有任务都被提交到全局队列
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = transport_status->batch_push_all_status_task_queue())) {
      CLOG_LOG(WARN, "failed to batch_push_all_status_task_queue at end",
               KR(tmp_ret), KPC(transport_status));
    }

    transport_status->unlock();
  } else {
    // return OB_EAGAIN to avoid taking up worker threads
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(INFO, "try lock failed", "transport_status", *transport_status, K(ret));
    }
  }
  CLOG_LOG(TRACE, "handle_submit_task_ end", K(ret), KPC(submit_task), K(is_timeslice_run_out));
  return ret;
}

// 处理status任务 - 从队列取出任务并发送给备库
int ObLogTransportService::handle_status_task_(ObTransportServiceStatusTask *status_task,
                                               bool &is_timeslice_run_out)
{
  int ret = OB_SUCCESS;
  bool is_queue_empty = false;
  LogTransportStatus *transport_status = NULL;
  const int64_t MAX_TRANSPORT_TIME_PER_ROUND = 10 * 1000; // 10ms

  if (OB_ISNULL(status_task)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "status task is NULL", KPC(status_task), KR(ret));
  } else if (OB_ISNULL(transport_status = status_task->get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "transport status is NULL", KPC(status_task), KPC(transport_status), KR(ret));
  } else {
    int64_t start_ts = ObTimeUtility::fast_current_time();
    do {
      ObLink *link = NULL;
      ObLogTransportTask *transport_task = NULL;

      if (transport_status->try_rdlock()) {
        if (!transport_status->is_enabled_without_lock()) {
          is_queue_empty = true;
        } else if (NULL == (link = status_task->top())) {
          // queue is empty
          ret = OB_SUCCESS;
          is_queue_empty = true;
        } else if (OB_ISNULL(transport_task = static_cast<ObLogTransportTask *>(link))) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "transport_task is NULL", KPC(transport_status), K(ret));
        } else {
          // 发送日志到备库
          if (OB_FAIL(transport_status->do_transport_task_(transport_task))) {
            CLOG_LOG(WARN, "do_transport_task_ failed", K(ret), KPC(transport_task));
            // 发送失败时不从队列中移除，等待重试
          } else {
            // 发送成功，从队列中移除
            ObLink *link_to_destroy = NULL;
            if (OB_ISNULL(link_to_destroy = status_task->pop())) {
              ret = OB_ERR_UNEXPECTED;
              CLOG_LOG(ERROR, "failed to pop task after transport", KPC(transport_task), K(ret));
            } else if (OB_ISNULL(transport_task = static_cast<ObLogTransportTask *>(link_to_destroy))) {
              ret = OB_ERR_UNEXPECTED;
              CLOG_LOG(ERROR, "transport_task is NULL when pop after transport", K(ret));
            } else {
              // 释放传输任务内存
              ob_free(transport_task);
              transport_task = NULL;

              // 避免单个任务占用过多线程时间
              int64_t used_time = ObTimeUtility::fast_current_time() - start_ts;
              if (used_time > MAX_TRANSPORT_TIME_PER_ROUND) {
                is_timeslice_run_out = true;
              }
            }
          }
        }
        transport_status->unlock();
      } else {
        // write lock is locked, may be cleaning up
        ob_usleep(10 * 1000); // 10ms
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          CLOG_LOG(INFO, "try lock failed", KPC(transport_status), K(ret));
        }
      }
    } while (OB_SUCC(ret) && (!is_queue_empty) && (!is_timeslice_run_out));
  }
  return ret;
}

int ObLogTransportService::handle_init_task_(ObTransportServiceInitTask *init_task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(init_task)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "init_task is NULL", K(ret));
  } else if (OB_FAIL(init_task->do_init())) {
    CLOG_LOG(WARN, "do_init failed", K(ret), KPC(init_task));
    // 检查日志流是否被删除
    LogTransportStatus *transport_status = init_task->get_transport_status();
    if (OB_ISNULL(transport_status)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "transport_status is NULL", KPC(init_task), KR(ret));
    } else {
      share::ObLSID ls_id;
      if (OB_SUCC(transport_status->get_ls_id(ls_id))) {
        ObTpStatusGuard guard;
        int check_ret = get_transport_status(ls_id, guard);
        if (OB_ENTRY_NOT_EXIST == check_ret) {
          CLOG_LOG(INFO, "log stream is deleted, finish init_task", K(ls_id));
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
    }
  } else {
    CLOG_LOG(INFO, "init task execute success");
  }

  // 引用计数和 lease 管理统一由 handle() 函数处理，与其他任务保持一致

  return ret;
}

int ObLogTransportService::push_task(ObTransportServiceTask *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogTransportService not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "task is NULL", KR(ret));
  } else {
    while (OB_FAIL(TG_PUSH_TASK(tg_id_, task)) && OB_EAGAIN == ret) {
      ob_throttle_usleep(1000, ret);
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        CLOG_LOG(WARN, "failed to submit service task to transport service, queue is full, will retry", KR(ret), KPC(task));
      }
    }
  }
  return ret;
}

void ObLogTransportService::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObTransportServiceTask *task_to_handle = static_cast<ObTransportServiceTask *>(task);
  LogTransportStatus *transport_status = NULL;
  bool need_push_back = false;

  if (OB_ISNULL(task_to_handle)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "task is NULL", KP(task_to_handle));
  } else if (OB_ISNULL(transport_status = task_to_handle->get_transport_status())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "transport_status is NULL, task will be discarded", KPC(task_to_handle));
  } else if (!is_running_) {
    CLOG_LOG(WARN, "transport service has been stopped, just ignore the task",
             K(is_running_), KPC(transport_status));
    revert_transport_status_(transport_status);
    task_to_handle = NULL;
  } else {
    bool is_timeslice_run_out = false;
    ObTransportServiceTaskType task_type = task_to_handle->get_type();

    if (ObTransportServiceTaskType::TRANSPORT_INIT_TASK == task_type) {
      ObTransportServiceInitTask *init_task = static_cast<ObTransportServiceInitTask *>(task_to_handle);
      ret = handle_init_task_(init_task);
    } else if (!transport_status->is_enabled()) {
      CLOG_LOG(INFO, "transport status is disabled, just ignore the task", KPC(transport_status));
    } else if (ObTransportServiceTaskType::TRANSPORT_SUBMIT_TASK == task_type) {
      ObTransportServiceSubmitTask *submit_task = static_cast<ObTransportServiceSubmitTask *>(task_to_handle);
      ret = handle_submit_task_(submit_task, is_timeslice_run_out);
    } else if (ObTransportServiceTaskType::TRANSPORT_STATUS_TASK == task_type) {
      ObTransportServiceStatusTask *status_task = static_cast<ObTransportServiceStatusTask *>(task_to_handle);
      ret = handle_status_task_(status_task, is_timeslice_run_out);
    } else {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "invalid task_type", K(ret), K(task_type), KPC(transport_status));
    }

    //TODO by ziqi: delete log for debug
    CLOG_LOG(TRACE, "handle task end", K(ret), K(task_type), KPC(transport_status));

    if (OB_ENTRY_NOT_EXIST == ret) {
      // 日志流已被删除，任务应该完成并释放引用计数
      if (OB_NOT_NULL(task_to_handle) && task_to_handle->revoke_lease()) {
        revert_transport_status_(transport_status);
      } else {
        need_push_back = true;
      }
    } else if (OB_FAIL(ret) || is_timeslice_run_out) {
      // transport failed or timeslice is run out
      need_push_back = true;
    } else if (OB_NOT_NULL(task_to_handle) && task_to_handle->revoke_lease()) {
      // success to set state to idle, no need to push back
      revert_transport_status_(transport_status);
    } else {
      need_push_back = true;
    }
  }

  if ((OB_FAIL(ret) || need_push_back) && NULL != task_to_handle) {
    // 重新推入队列, 如果 transport_status 为 NULL，不应该重新推入，避免无限循环
    if (OB_ISNULL(task_to_handle->get_transport_status())) {
      CLOG_LOG(WARN, "task with NULL transport_status will not be pushed back");
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = push_task(task_to_handle))) {
        CLOG_LOG(WARN, "failed to push task back", K(tmp_ret), KPC(task_to_handle));
        // 只有在transport_status非空时才调用revert，避免重复释放
        if (OB_NOT_NULL(transport_status)) {
          revert_transport_status_(transport_status);
        }
      }
    }
  }
  CLOG_LOG(INFO, "handle task end", K(ret), KPC(task_to_handle), K(need_push_back), KPC(transport_status));
}

obrpc::ObLogTransportRpcProxy *ObLogTransportService::get_rpc_proxy()
{
  return rpc_proxy_;
}

int ObLogTransportService::set_sync_standby_dest(const ObSyncStandbyDestStruct &sync_standby_dest)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportService not init", KR(ret));
  } else {
    common::SpinWLockGuard guard(sync_standby_dest_lock_);
    if (OB_FAIL(sync_standby_dest_.assign(sync_standby_dest))) {
      CLOG_LOG(WARN, "failed to assign sync standby dest", KR(ret), K(sync_standby_dest));
    } else {
      CLOG_LOG(INFO, "set sync standby dest success", K(sync_standby_dest));
    }
  }
  return ret;
}

int ObLogTransportService::get_sync_standby_dest(ObSyncStandbyDestStruct &sync_standby_dest) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(sync_standby_dest_lock_);
  if (OB_FAIL(sync_standby_dest.assign(sync_standby_dest_))) {
    CLOG_LOG(WARN, "failed to assign sync standby dest", KR(ret), K_(sync_standby_dest));
  }
  return ret;
}

int ObLogTransportService::switch_to_leader(const share::ObLSID &id,
                                            const int64_t new_proposal_id,
                                            const palf::SyncMode &sync_mode,
                                            const palf::LSN &begin_lsn)
{
  int ret = OB_SUCCESS;
  ObTpStatusGuard guard;
  LogTransportStatus *transport_status = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportService not init", KR(ret), K(id));
  } else if (!id.is_valid() || new_proposal_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(id), K(new_proposal_id));
  } else if (OB_FAIL(get_transport_status(id, guard))) {
    CLOG_LOG(WARN, "get_transport_status failed", KR(ret), K(id));
  } else if (NULL == (transport_status = guard.get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport_status is NULL", KR(ret), K(id));
  } else {
    if (OB_FAIL(transport_status->switch_to_leader(new_proposal_id, sync_mode, begin_lsn))) {
      CLOG_LOG(WARN, "transport_status switch_to_leader failed", KR(ret), K(id), K(new_proposal_id),
               K(sync_mode), K(begin_lsn));
    } else {
      CLOG_LOG(INFO, "transport_service switch_to_leader success", K(id), K(new_proposal_id),
               K(sync_mode), K(begin_lsn));
    }
  }

  return ret;
}

int ObLogTransportService::switch_to_follower(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  ObTpStatusGuard guard;
  LogTransportStatus *transport_status = NULL;

  CLOG_LOG(INFO, "transport service switch_to_follower start", K(id)); //TODO by ziqi: delete

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportService not init", KR(ret), K(id));
  } else if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(id));
  } else if (OB_FAIL(get_transport_status(id, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // transport_status 不存在，可能是日志流还未创建或已删除，记录日志但不报错
      CLOG_LOG(TRACE, "transport_status not exist, skip switch_to_follower", K(id));
      ret = OB_SUCCESS;
    } else {
      CLOG_LOG(WARN, "get_transport_status failed", KR(ret), K(id));
    }
  } else if (NULL == (transport_status = guard.get_transport_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "transport_status is NULL", KR(ret), K(id));
  } else {
    if (OB_FAIL(transport_status->switch_to_follower())) {
      CLOG_LOG(WARN, "transport_status switch_to_follower failed", KR(ret), K(id));
    } else {
      CLOG_LOG(INFO, "transport_service switch_to_follower success", K(id));
    }
  }

  return ret;
}

int ObLogTransportService::mark_ls_gc_state(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  ObTpStatusGuard guard;
  LogTransportStatus *transport_status = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportService not init", KR(ret), K(id));
  } else if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(id));
  } else if (OB_FAIL(get_transport_status(id, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      CLOG_LOG(TRACE, "transport_status not exist, skip mark_offline", K(id));
    } else {
      CLOG_LOG(WARN, "get_transport_status failed", KR(ret), K(id));
    }
  } else if (NULL != (transport_status = guard.get_transport_status())) {
    transport_status->mark_ls_gc_state();
    CLOG_LOG(INFO, "transport service mark_ls_gc_state success", K(id));
  }
  return ret;
}

int ObLogTransportService::notify_apply_service(const share::ObLSID &id, const palf::LSN &standby_lsn, const share::SCN &standby_scn)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportService not init", KR(ret), K(id));
  } else if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(id), K(standby_lsn), K(standby_scn));
  } else if (OB_ISNULL(apply_service_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "apply_service_ is NULL", KR(ret), K(id));
  } else {
    // 获取 apply status
    logservice::ObApplyStatusGuard guard;
    if (OB_FAIL(apply_service_->get_apply_status(id, guard))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // apply status 不存在，可能是日志流还未创建或已删除，记录日志但不报错
        CLOG_LOG(TRACE, "apply status not exist, skip notify", K(id), K(standby_lsn));
        ret = OB_SUCCESS;
      } else {
        CLOG_LOG(WARN, "get_apply_status failed", KR(ret), K(id), K(standby_lsn));
      }
    } else {
      logservice::ObApplyStatus *apply_status = guard.get_apply_status();
      if (OB_ISNULL(apply_status)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "apply_status is NULL", KR(ret), K(id), K(standby_lsn));
      } else {
        CLOG_LOG(TRACE, "notify_apply_service standby committed end lsn",
                 K(id), K(standby_lsn), KPC(apply_status));

        //在强同步模式下，apply service需要知道备库的 committed end lsn
        apply_status->update_standby_committed_end_lsn(standby_lsn, standby_scn);
      }
    }
  }

  return ret;
}

int ObLogTransportService::revert_transport_status_(LogTransportStatus *transport_status)
{
  int ret = OB_SUCCESS;
  if (NULL != transport_status) {
    if (0 == transport_status->dec_ref()) {
      CLOG_LOG(INFO, "free transport status", KPC(transport_status));
      transport_status->~LogTransportStatus();
      mtl_free(transport_status);
    }
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase