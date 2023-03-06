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

#include "ob_archive_sender.h"
#include <algorithm>
#include <cstdint>
#include "lib/alloc/alloc_assist.h"
#include "lib/ob_errno.h"
#include "lib/restore/ob_storage.h"
#include "lib/string/ob_string.h"    // ObString
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/backup/ob_archive_piece.h"    // ObArchivePiece
#include "lib/thread/ob_thread_name.h"
#include "share/backup/ob_archive_struct.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_errno.h"
#include "share/ob_ls_id.h"          // ObLSID
#include "share/rc/ob_tenant_base.h"    // MTL_ID
#include "observer/ob_server_struct.h"                   // GCTX
#include "ob_ls_mgr.h"               // ObArchiveLSMgr
#include "ob_archive_round_mgr.h"    // ObArchiveRoundMgr
#include "ob_archive_define.h"
#include "ob_archive_allocator.h"    // ObArchiveAllocator
#include "ob_archive_define.h"       // ARCHIVE_N
#include "ob_archive_util.h"         // cal_archive_file_id
#include "ob_archive_task.h"         // ObArchiveSendTask
#include "ob_ls_task.h"              // ObLSArchiveTask
#include "ob_archive_task_queue.h"   // ObArchiveTaskStatus
#include "ob_archive_io.h"           // ObArchiveIO
#include "share/backup/ob_backup_path.h"   // ObBackupPath
#include "share/backup/ob_archive_path.h"   // ObArchivePathUtil

namespace oceanbase
{
using namespace share;
namespace archive
{
ObArchiveSender::ObArchiveSender() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  allocator_(NULL),
  ls_mgr_(NULL),
  persist_mgr_(NULL),
  round_mgr_(NULL),
  task_queue_(),
  send_cond_()
{
}

ObArchiveSender::~ObArchiveSender()
{
  ARCHIVE_LOG(INFO, "ObArchiveSender destroy");
  destroy();
}

int ObArchiveSender::init(const uint64_t tenant_id,
    ObArchiveAllocator *allocator,
    ObArchiveLSMgr *ls_mgr,
    ObArchivePersistMgr *persist_mgr,
    ObArchiveRoundMgr *round_mgr)
{
  int ret = OB_SUCCESS;
  const int64_t TASK_STATUS_LIMIT = 100 * 1000L;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "archive sender init twice", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(allocator_ = allocator)
      || OB_ISNULL(ls_mgr_ = ls_mgr)
      || OB_ISNULL(persist_mgr_ = persist_mgr)
      || OB_ISNULL(round_mgr_ = round_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(allocator), K(ls_mgr), K(round_mgr));
  } else if (OB_FAIL(task_queue_.init(TASK_STATUS_LIMIT, "ArcSenderQueue", tenant_id))) {
    ARCHIVE_LOG(WARN, "task queue init failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
  }
  return ret;
}

void ObArchiveSender::destroy()
{
  inited_ = false;
  stop();
  wait();
  tenant_id_ = OB_INVALID_TENANT_ID;
  allocator_ = NULL;
  ls_mgr_ = NULL;
  persist_mgr_ = NULL;
}

int ObArchiveSender::start()
{
  int ret = OB_SUCCESS;
  ObThreadPool::set_run_wrapper(MTL_CTX(), lib::ThreadCGroup::BACK_CGROUP);
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(INFO, "ObArchiveSender has not been initialized", KR(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    ARCHIVE_LOG(WARN, "start ObArchiveSender threads fail", KR(ret));
  } else {
    ARCHIVE_LOG(INFO, "start ObArchiveSender threads succ", KR(ret));
  }

  return ret;
}

void ObArchiveSender::stop()
{
  ObThreadPool::stop();

  ARCHIVE_LOG(INFO, "stop ObArchiveSender threads succ");
}

void ObArchiveSender::wait()
{
  ARCHIVE_LOG(INFO, "ObArchiveSender wait");
  ObThreadPool::wait();
}

void ObArchiveSender::release_send_task(ObArchiveSendTask *task)
{
  if (NULL == task || NULL == allocator_) {
    ARCHIVE_LOG(ERROR, "invalid arguments", K(task), K(allocator_));
  } else {
    allocator_->free_send_task(task);
  }
}

// PG内task需要严格保证递增，由调用者保证
// 不能存在先插入10号log，又插入8号的情况
int ObArchiveSender::submit_send_task(ObArchiveSendTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveSender not init");
  } else if (OB_ISNULL(task) || OB_UNLIKELY(! task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), KPC(task));
  } else {
    RETRY_FUNC_ON_ERROR(OB_ALLOCATE_MEMORY_FAILED, has_set_stop(), (*this), submit_send_task_, task);
    if (OB_SUCC(ret)) {
      send_cond_.signal();
    }
  }

  // 提交send_task遇到归档stop, 释放任务, 返回成功
  if (OB_IN_STOP_STATE == ret) {
    release_send_task(task);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObArchiveSender::push_task_status(ObArchiveTaskStatus *task_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_status)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", K(ret), K(task_status));
  } else if (OB_FAIL(task_queue_.push(task_status))) {
    ARCHIVE_LOG(WARN, "push fail", K(ret), KPC(task_status));
  } else {
    ARCHIVE_LOG(INFO, "push succ", KP(task_status));
  }
  return ret;
}

int64_t ObArchiveSender::get_send_task_status_count() const
{
  return task_queue_.size();
}

int ObArchiveSender::submit_send_task_(ObArchiveSendTask *task)
{
  int ret = OB_SUCCESS;
  const ObLSID &id = task->get_ls_id();
  if (OB_ISNULL(ls_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ls_mgr_ is NULL", K(ret), K(ls_mgr_));
  } else {
    GET_LS_TASK_CTX(ls_mgr_, id) {
      if (OB_FAIL(ls_archive_task->push_send_task(*task, *this))) {
        ARCHIVE_LOG(WARN, "push_send_task fail", K(ret), KPC(task));
      }
    }
  }
  return ret;
}

void ObArchiveSender::run1()
{
  ARCHIVE_LOG(INFO, "ObArchiveSender thread start", K_(tenant_id));
  lib::set_thread_name("ArcSender");
  ObCurTraceId::init(GCONF.self_addr_);

  if (OB_UNLIKELY(! inited_)) {
    ARCHIVE_LOG(ERROR, "archive sender not init");
  } else {
    while (! has_set_stop()) {
      do_thread_task_();
    }
  }
}

void ObArchiveSender::do_thread_task_()
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  const int64_t MAX_ARCHIVE_TASK_STATUS_POP_TIMEOUT = 5 * 1000 * 1000L;
  if (OB_FAIL(task_queue_.pop(data, MAX_ARCHIVE_TASK_STATUS_POP_TIMEOUT))) {
    // no task exist, just skip
    ob_usleep(1 * 1000 * 1000L);
  } else if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "data is NULL", K(ret), K(data));
  } else {
    if (OB_FAIL(handle_task_list(data))) {
      ARCHIVE_LOG(WARN, "handle task list fail", K(ret));
    }
  }
}

int ObArchiveSender::handle_task_list(void *data)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  bool task_consume = false;
  ObLink *link = NULL;
  ObArchiveSendTask *task = NULL;
  ObArchiveTaskStatus *task_status = static_cast<ObArchiveTaskStatus *>(data);

  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_SEND_NUM && ! has_set_stop(); i++) {
    task_consume = false;
    exist = false;
    task = NULL;
    if (OB_FAIL(task_status->top(link, exist))) {
      ARCHIVE_LOG(WARN, "top failed", K(ret));
    } else if (! exist) {
      break;
    } else if (OB_ISNULL(link)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "link is NULL", K(ret));
    } else if (FALSE_IT(task = static_cast<ObArchiveSendTask *>(link))) {
    } else if (OB_SUCC(handle(*task, task_consume))) {
      if (! task_consume) {
        // 有任务无法被消费, 直接跳出循环
        ob_usleep(100 * 1000L);
        break;
      }
    } else if (! is_retry_ret_code_(ret) && is_ignore_ret_code_(ret)) {
      ret = OB_SUCCESS;
    }

    // handle task
    if (OB_SUCC(ret) && NULL != task && task_consume) {
      if (OB_FAIL(task_status->pop_front(1))) {
        ARCHIVE_LOG(ERROR, "pop front failed", K(ret), K(task_status));
      } else {
        release_send_task(task);
      }
    }
  }

  if (NULL != task_status) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_retire_task_status_(*task_status))) {
      ARCHIVE_LOG(WARN, "try_retire_task_status_ fail", K(ret), KPC(task_status));
    }
  }
  return ret;
}

bool ObArchiveSender::in_normal_status_(const ArchiveKey &key) const
{
  return round_mgr_->is_in_archive_status(key);
}

// 仅有需要重试的任务返回错误码
int ObArchiveSender::handle(const ObArchiveSendTask &task, bool &task_consume)
{
  int ret = OB_SUCCESS;
  const ObLSID &id = task.get_ls_id();
  const ArchiveWorkStation &station = task.get_station();
  share::ObBackupDest backup_dest;
  task_consume = true;   // 默认task需要被消费掉, 只有补偿piece场景才不会消费
  if (OB_UNLIKELY(! task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(task));
  } else if (OB_UNLIKELY(! in_normal_status_(station.get_round()))) {
    // not in normal status, just skip
  } else if (OB_FAIL(round_mgr_->get_backup_dest(station.get_round(), backup_dest))) {
    ARCHIVE_LOG(WARN, "get backup dest failed", K(ret), K(task));
  } else {
    int64_t next_compensate_piece_id = 0;
    DestSendOperator operation = DestSendOperator::SEND;
    GET_LS_TASK_CTX(ls_mgr_, id) {
      int64_t file_id = OB_INVALID_ARCHIVE_FILE_ID;
      int64_t file_offset = OB_INVALID_ARCHIVE_FILE_OFFSET;
      ObArchiveSendDestArg arg;
      if (OB_FAIL(ls_archive_task->get_archive_send_arg(station, arg))) {
        ARCHIVE_LOG(WARN, "get archive progress failed", K(ret), K(id), K(task));
      } else if (OB_FAIL(check_can_send_task_(task, arg.tuple_))) {
        ARCHIVE_LOG(WARN, "can't send task", K(ret), K(task), KPC(ls_archive_task));
      } else if (OB_FAIL(check_piece_continuous_(task, arg.tuple_, next_compensate_piece_id, operation))) {
        ARCHIVE_LOG(WARN, "check piece continuous failed", K(ret));
      } else if (DestSendOperator::WAIT == operation) {
        // do nothing
        task_consume = false;
      } else if (DestSendOperator::COMPENSATE == operation) {
        task_consume = false;
        if (OB_FAIL(do_compensate_piece_(id, next_compensate_piece_id, station,
                                         backup_dest, *ls_archive_task))) {
          ARCHIVE_LOG(WARN, "do compensate piece failed", K(ret), K(task), KPC(ls_archive_task));
        }
      } else if (OB_FAIL(archive_log_(backup_dest, arg, task, *ls_archive_task))) {
        ARCHIVE_LOG(WARN, "archive log failed", K(ret), K(task), KPC(ls_archive_task));
      }

      if (OB_FAIL(ret)) {
        handle_archive_error_(id, station.get_round(), ret);
      } else {
        //
      }
    }
  }
  return ret;
}

// 1. 从没归档出去过数据, 可以立即归档 -> ls_archive_task没有piece记录
// 2. 未切piece, 可以立即归档 -> ls_archive_task piece与当前任务piece相同
// 3. 需要等待持久化 -> persist_mgr piece与当前任务piece不同, 并且LSN不连续
// 4. 需要补偿piece -> persist_mgr piece与当前任务piece不同, 并且LSN连续并且piece_id相差大于1
//    NOTE: 指定next_piece_id补偿，保证连续空洞piece都有机会补偿
int ObArchiveSender::check_piece_continuous_(const ObArchiveSendTask &task,
    const LogFileTuple &ls_task_tuple,
    int64_t &next_piece_id,
    DestSendOperator &operation)
{
  int ret = OB_SUCCESS;
  ObLSArchivePersistInfo info;
  const ObLSID &id = task.get_ls_id();
  const ObArchivePiece &piece = task.get_piece();
  const ArchiveWorkStation &station = task.get_station();
  if (! ls_task_tuple.get_piece().is_valid()) {
    ARCHIVE_LOG(INFO, "no log archived, no need check piece continuous", K(ls_task_tuple));
  } else if (OB_LIKELY(ls_task_tuple.get_piece() == task.get_piece())) {
  } else if (OB_FAIL(persist_mgr_->check_and_get_piece_persist_continuous(id, info))
      && OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "get persist archive info failed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // send task piece diff from ls_task, and no record persist, need wait
    ret = OB_SUCCESS;
    operation = DestSendOperator::WAIT;
    ARCHIVE_LOG(INFO, "pre piece archive progress not persist, just wait", K(info), K(task));
  } else {
    const int64_t persist_piece_id = info.key_.piece_id_;
    if (persist_piece_id != piece.get_piece_id() && info.lsn_ != task.get_start_lsn().val_) {
      // more lsn need to persist, just wait
      operation = DestSendOperator::WAIT;
      ARCHIVE_LOG(INFO, "persist lsn not equal with send task "
          "and persist piece id not equal with send task, just wait", K(info), K(task));
    } else if (piece.get_piece_id() > persist_piece_id + 1
        && info.lsn_ == task.get_start_lsn().val_) {
      operation = DestSendOperator::COMPENSATE;
      next_piece_id = persist_piece_id + 1;
      ARCHIVE_LOG(INFO, "persist lsn equal with send task and gap of persist piece id "
          "and send task piece id bigger than 1, just wait", K(info), K(task));
    }
  }
  return ret;
}

int ObArchiveSender::do_compensate_piece_(const ObLSID &id,
    const int64_t next_piece_id,
    const ArchiveWorkStation &station,
    const ObBackupDest &backup_dest,
    ObLSArchiveTask &ls_archive_task)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath prefix;
  if (OB_FAIL(share::ObArchivePathUtil::get_piece_ls_dir_path(backup_dest, station.get_round().dest_id_,
      station.get_round().round_, next_piece_id, id, prefix))) {
    ARCHIVE_LOG(WARN, "get piece ls dir path failed", K(ret), K(id), K(next_piece_id), K(station));
  } else {
    ObArchiveIO archive_io;
    if (OB_FAIL(archive_io.mkdir(prefix.get_obstr(), backup_dest.get_storage_info()))) {
      ARCHIVE_LOG(WARN, "mkdir failed", K(ret), K(id));
    } else {
      ARCHIVE_LOG(INFO, "archive dir make succ", K(ret), K(prefix));
      ret = ls_archive_task.compensate_piece(station, next_piece_id);
    }
  }
  return ret;
}

int ObArchiveSender::check_can_send_task_(const ObArchiveSendTask &task,
    const LogFileTuple &tuple)
{
  int ret = OB_SUCCESS;
  if (tuple.get_piece() > task.get_piece()) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "piece rollback", K(ret), K(tuple), K(task));
  } else if (tuple.get_lsn() != task.get_start_lsn()) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "lsn not continous", K(ret), K(tuple), K(task));
  }
  return ret;
}

int ObArchiveSender::archive_log_(const ObBackupDest &backup_dest,
    const ObArchiveSendDestArg &arg,
    const ObArchiveSendTask &task,
    ObLSArchiveTask &ls_archive_task)
{
  int ret = OB_SUCCESS;
  int64_t file_id = 0;
  int64_t file_offset = 0;
  share::ObBackupPath path;
  ObBackupPathString uri;
  const ObLSID &id = task.get_ls_id();
  const ObArchivePiece &pre_piece = arg.tuple_.get_piece();
  const ObArchivePiece &piece = task.get_piece();
  const ArchiveWorkStation &station = task.get_station();
  bool new_file = false;
  char *origin_data = NULL;
  int64_t origin_data_len = 0;
  char *filled_data = NULL;
  int64_t filled_data_len = 0;
  const int64_t start_ts = common::ObTimeUtility::current_time();
  // 1. decide archive file
  if (OB_FAIL(decide_archive_file_(task, arg.cur_file_id_, arg.cur_file_offset_,
                                   pre_piece, file_id, file_offset))) {
    ARCHIVE_LOG(WARN, "decide archive file failed", K(ret), K(task), K(ls_archive_task));
  }
  // 2. build archive preifix if needed
  else if (OB_FAIL(build_archive_prefix_if_needed_(id, station, arg.piece_dir_exist_,
                                                   pre_piece, piece, backup_dest))) {
    ARCHIVE_LOG(WARN, "build archive prefix failed", K(ret));
  }
  // 3. build archive path
  else if (OB_FAIL(build_archive_path_(id, file_id, station, piece, backup_dest, path))) {
    ARCHIVE_LOG(WARN, "build archive path failed", K(ret));
  } else if (FALSE_IT(new_file = (0 == file_offset))) {
  }
  // 4. get task origin data
  else if (OB_FAIL(task.get_buffer(origin_data, origin_data_len))) {
    ARCHIVE_LOG(WARN, "get buffer failed", K(ret), K(task));
  } else if (OB_UNLIKELY(NULL == origin_data || origin_data_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid data", K(ret), K(task), K(origin_data), K(origin_data_len));
  }
  // 5. fill archive file header if needed
  else if (new_file
      && OB_FAIL(fill_file_header_if_needed_(task.get_start_lsn(), origin_data,
          origin_data_len, filled_data, filled_data_len))) {
    ARCHIVE_LOG(WARN, "fill file header if needed failed", K(ret));
  }
  // 6. push log
  else if (OB_FAIL(push_log_(id, path.get_obstr(), backup_dest.get_storage_info(), new_file ?
          file_offset : file_offset + ARCHIVE_FILE_HEADER_SIZE,
          new_file ? filled_data : origin_data, new_file ? filled_data_len : origin_data_len))) {
    ARCHIVE_LOG(WARN, "push log failed", K(ret), K(task));
  }
  // 7. 更新日志流归档任务archive file info
  else if (OB_FAIL(update_archive_progress_(file_id, file_offset, task, ls_archive_task))) {
    ARCHIVE_LOG(WARN, "update archive file info failed", K(ret), K(file_id));
  }

  // 8. 释放filled buffer
  if (NULL != filled_data) {
    mtl_free(filled_data);
    filled_data = NULL;
  }

  // 9. 统计
  if (OB_SUCC(ret)) {
    statistic(task, common::ObTimeUtility::current_time() - start_ts);
  }
  return ret;
}

int ObArchiveSender::decide_archive_file_(const ObArchiveSendTask &task,
    const int64_t pre_file_id,
    const int64_t pre_file_offset,
    const ObArchivePiece &pre_piece,
    int64_t &file_id,
    int64_t &file_offset)
{
  int ret = OB_SUCCESS;
  const LSN &lsn = task.get_start_lsn();
  const ArchiveWorkStation &station = task.get_station();
  const ObArchivePiece &piece = task.get_piece();

  file_id = cal_archive_file_id(lsn, MAX_ARCHIVE_FILE_SIZE);
  if (file_id == pre_file_id && pre_piece == piece) {
    file_offset = pre_file_offset;
  } else {
    file_offset = 0;
  }
  return ret;
}

int ObArchiveSender::build_archive_prefix_if_needed_(const ObLSID &id,
    const ArchiveWorkStation &station,
    const bool piece_dir_exist,
    const ObArchivePiece &pre_piece,
    const ObArchivePiece &cur_piece,
    const ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupPathString uri;
  share::ObBackupPath prefix;
  if (pre_piece.is_valid() && pre_piece == cur_piece && piece_dir_exist) {
    // just skip
  } else if (OB_FAIL(share::ObArchivePathUtil::get_piece_ls_dir_path(backup_dest, station.get_round().dest_id_,
      station.get_round().round_, cur_piece.get_piece_id(), id, prefix))) {
    ARCHIVE_LOG(WARN, "get piece ls dir path failed", K(ret), K(id),
        K(cur_piece), K(station), K(backup_dest));
  } else {
    ObArchiveIO archive_io;
    if (OB_FAIL(archive_io.mkdir(prefix.get_obstr(), backup_dest.get_storage_info()))) {
      ARCHIVE_LOG(WARN, "mkdir failed", K(ret), K(id), K(uri));
    } else {
      ARCHIVE_LOG(INFO, "archive dir make succ", K(ret), K(prefix));
    }
  }
  return ret;
}

int ObArchiveSender::build_archive_path_(const ObLSID &id,
    const int64_t file_id,
    const ArchiveWorkStation &station,
    const ObArchivePiece &piece,
    const ObBackupDest &backup_dest,
    share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObArchivePathUtil::get_ls_archive_file_path(backup_dest, station.get_round().dest_id_,
            station.get_round().round_, piece.get_piece_id(), id, file_id, path))) {
    ARCHIVE_LOG(WARN, "get ls archive file path failed", K(ret));
  }
  return ret;
}

int ObArchiveSender::fill_file_header_if_needed_(const palf::LSN &lsn,
    char *origin_data,
    const int64_t origin_data_len,
    char *&filled_data,
    int64_t &filled_data_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObArchiveFileHeader file_header;
  filled_data_len = origin_data_len + ARCHIVE_FILE_HEADER_SIZE;
  if (OB_ISNULL(filled_data = (char*)mtl_malloc(filled_data_len, "ArcFile"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "alloc memory failed", K(ret));
  } else if (OB_FAIL(file_header.generate_header(lsn))) {
    ARCHIVE_LOG(WARN, "generate archive file header failed", K(ret), K(lsn));
  } else if (OB_FAIL(file_header.serialize(filled_data, filled_data_len, pos))) {
    ARCHIVE_LOG(WARN, "archive file header serialize failed", K(ret));
  } else if (OB_UNLIKELY(pos > ARCHIVE_FILE_HEADER_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "pos exceed", K(ret), K(pos));
  } else {
    MEMSET(filled_data + pos, 0, ARCHIVE_FILE_HEADER_SIZE - pos);
    MEMCPY(filled_data + ARCHIVE_FILE_HEADER_SIZE, origin_data, origin_data_len);
  }
  return ret;
}

int ObArchiveSender::push_log_(const ObLSID &id,
    const ObString &uri,
    const share::ObBackupStorageInfo *storage_info,
    const int64_t offset,
    char *data,
    const int64_t data_len)
{
  int ret = OB_SUCCESS;
  ObArchiveIO archive_io;

  if (OB_FAIL(archive_io.push_log(uri, storage_info, data, data_len, offset))) {
    ARCHIVE_LOG(WARN, "push log failed", K(ret));
  } else {
    ARCHIVE_LOG(INFO, "push log succ", K(id));
  }
  return ret;
}

int ObArchiveSender::update_archive_progress_(const int64_t file_id,
    const int64_t file_offset,
    const ObArchiveSendTask &task,
    ObLSArchiveTask &ls_archive_task)
{
  const int64_t end_offset = file_offset + task.get_buf_size();
  const ArchiveWorkStation &station = task.get_station();
  const LSN &lsn = task.get_end_lsn();
  const int64_t log_ts = task.get_max_log_ts();
  const ObArchivePiece &piece = task.get_piece();
  LogFileTuple tuple(lsn, log_ts, piece);
  return ls_archive_task.update_archive_progress(station, file_id, end_offset, tuple);
}

int ObArchiveSender::try_retire_task_status_(ObArchiveTaskStatus &task_status)
{
  int ret = OB_SUCCESS;
  bool is_queue_empty = false;
  bool is_discarded = false;

  if (OB_FAIL(task_status.retire(is_queue_empty, is_discarded))) {
    ARCHIVE_LOG(ERROR, "task_status retire fail", KR(ret), K(task_status));
  } else if (is_discarded && NULL != allocator_) {
    allocator_->free_send_task_status(&task_status);
  } else if (! is_queue_empty) {
    if (OB_FAIL(task_queue_.push(&task_status))) {
      ARCHIVE_LOG(WARN, "push fail", KR(ret), K(task_status));
    }
  }
  return ret;
}

void ObArchiveSender::handle_archive_error_(const ObLSID &id,
    const ArchiveKey &key,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  if (! in_normal_status_(key)) {
    // skip it
  } else if (is_ignore_ret_code_(ret_code)) {
  } else {
    ARCHIVE_LOG(ERROR, "archive sender encounter fatal error", K(ret), K(id), K(key), K(ret_code));
    ObArchiveInterruptReason reasaon(ObArchiveInterruptReason::Factor::SEND_ERROR, lbt(), ret_code);
    if (OB_FAIL(ls_mgr_->mark_fata_error(id, key, reasaon))) {}
  }
}

bool ObArchiveSender::is_retry_ret_code_(const int ret_code) const
{
  return is_io_error(ret_code)
    || OB_ALLOCATE_MEMORY_FAILED == ret_code
    || OB_BACKUP_DEVICE_OUT_OF_SPACE == ret_code
    || OB_BACKUP_PWRITE_OFFSET_NOT_MATCH == ret_code;
}

bool ObArchiveSender::is_ignore_ret_code_(const int ret_code) const
{
  return is_retry_ret_code_(ret_code)
    || OB_LOG_ARCHIVE_LEADER_CHANGED == ret_code
    || OB_ENTRY_NOT_EXIST == ret_code;
}

void ObArchiveSender::statistic(const ObArchiveSendTask &task, const int64_t cost_ts)
{
  static __thread int64_t SEND_LOG_LSN_SIZE;
  static __thread int64_t SEND_BUF_SIZE;
  static __thread int64_t SEND_TASK_COUNT;
  static __thread int64_t SEND_COST_TS;

  SEND_LOG_LSN_SIZE += static_cast<int64_t>((task.get_end_lsn() - task.get_start_lsn()));
  SEND_BUF_SIZE += task.get_buf_size();
  SEND_TASK_COUNT++;
  SEND_COST_TS += cost_ts;

  if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
    const int64_t total_send_log_size = SEND_LOG_LSN_SIZE;
    const int64_t total_send_buf_size = SEND_BUF_SIZE;
    const int64_t total_send_task_count = SEND_TASK_COUNT;
    const int64_t total_send_cost_ts = SEND_COST_TS;
    const int64_t avg_task_lsn_size = total_send_log_size / std::max(total_send_task_count, 1L);
    const int64_t avg_task_buf_size = total_send_buf_size / std::max(total_send_task_count, 1L);
    const int64_t avg_task_cost_ts = total_send_cost_ts / std::max(total_send_task_count, 1L);
    ARCHIVE_LOG(INFO, "archive_sender statistic in 10s",
                K(total_send_log_size),
                K(total_send_buf_size),
                K(total_send_task_count),
                K(total_send_cost_ts),
                K(avg_task_lsn_size),
                K(avg_task_buf_size),
                K(avg_task_cost_ts));
    SEND_LOG_LSN_SIZE = 0;
    SEND_BUF_SIZE = 0;
    SEND_TASK_COUNT = 0;
    SEND_COST_TS = 0;
  }
}

} // namespace archive
} // namespace oceanbase
