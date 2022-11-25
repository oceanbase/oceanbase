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

#include "ob_start_archive_helper.h"
#include "lib/ob_define.h"                  // OB_INVALID_FILE_ID
#include "lib/ob_errno.h"
#include "logservice/archiveservice/ob_archive_define.h"
#include "logservice/ob_log_handler.h"
#include "logservice/ob_log_service.h"      // ObLogService
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/palf_iterator.h"
#include "logservice/palf_handle_guard.h"   // PalfHandleGuard
#include "ob_archive_util.h"                // cal
#include "storage/tx_storage/ob_ls_map.h"
#include <cstdint>

namespace oceanbase
{
namespace archive
{
using namespace oceanbase::logservice;
using namespace oceanbase::palf;
StartArchiveHelper::StartArchiveHelper(const ObLSID &id,
    const uint64_t tenant_id,
    const ArchiveWorkStation &station,
    const int64_t min_log_ts,
    const int64_t piece_interval,
    const int64_t genesis_ts,
    const int64_t base_piece_id,
    ObArchivePersistMgr *persist_mgr)
  : id_(id),
    tenant_id_(tenant_id),
    station_(station),
    log_gap_exist_(false),
    min_log_ts_(min_log_ts),
    piece_interval_(piece_interval),
    genesis_ts_(genesis_ts),
    base_piece_id_(base_piece_id),
    start_offset_(),
    archive_file_id_(OB_INVALID_ARCHIVE_FILE_ID),
    archive_file_offset_(OB_INVALID_ARCHIVE_FILE_OFFSET),
    max_archived_ts_(OB_INVALID_TIMESTAMP),
    piece_(),
    persist_mgr_(persist_mgr)
{}

StartArchiveHelper::~StartArchiveHelper()
{
  id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  station_.reset();
  log_gap_exist_ = false;
  min_log_ts_ = OB_INVALID_TIMESTAMP;
  piece_interval_ = 0;
  genesis_ts_ = OB_INVALID_TIMESTAMP;
  base_piece_id_ = 0;
  start_offset_.reset();
  archive_file_id_ = OB_INVALID_ARCHIVE_FILE_ID;
  archive_file_offset_ = OB_INVALID_ARCHIVE_FILE_OFFSET;
  max_archived_ts_ = OB_INVALID_TIMESTAMP;
  piece_.reset();
  persist_mgr_ = NULL;
}

bool StartArchiveHelper::is_valid() const
{
  return id_.is_valid()
    && OB_INVALID_TENANT_ID != tenant_id_
    && station_.is_valid()
    && piece_.is_valid()
    && OB_INVALID_TIMESTAMP != max_archived_ts_
    && (log_gap_exist_
        || (start_offset_.is_valid()
          && OB_INVALID_ARCHIVE_FILE_ID != archive_file_id_
          && OB_INVALID_ARCHIVE_FILE_OFFSET != archive_file_offset_));
}

int StartArchiveHelper::handle()
{
  int ret = OB_SUCCESS;
  int64_t piece_id = 0;
  bool archive_progress_exist = false;

  if (OB_UNLIKELY(! id_.is_valid()
        || ! station_.is_valid()
        || min_log_ts_ == OB_INVALID_TIMESTAMP
        || NULL == persist_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argumetn", K(ret), K(id_), K(station_), K(persist_mgr_));
  } else if (OB_FAIL(fetch_exist_archive_progress_(archive_progress_exist))) {
    ARCHIVE_LOG(WARN, "fetch exist archive progress failed", K(ret), K(id_));
  } else if (archive_progress_exist) {
  } else if (OB_FAIL(locate_round_start_archive_point_())) {
    ARCHIVE_LOG(WARN, "locate round start archive point failed", K(ret));
  }

  return ret;
}

int StartArchiveHelper::fetch_exist_archive_progress_(bool &record_exist)
{
  int ret = OB_SUCCESS;
  const ArchiveKey &key = station_.get_round();
  ObLSArchivePersistInfo persist_info;
  record_exist = false;

  if (OB_FAIL(persist_mgr_->get_archive_persist_info(id_, key, persist_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ARCHIVE_LOG(INFO, "no persist archive record exist", K(ret), K(id_), K(station_));
    } else {
      ARCHIVE_LOG(WARN, "load archive persist info failed", K(ret), K(id_), K(station_));
    }
  } else if (OB_UNLIKELY(! persist_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid archive persist info", K(ret), K(persist_info), K(id_));
  } else if (key != ArchiveKey(persist_info.incarnation_, persist_info.key_.dest_id_, persist_info.key_.round_id_)) {
    ARCHIVE_LOG(INFO, "max archive persist info in different round, just skip",
        K(id_), K(station_), K(persist_info));
  } else if (OB_FAIL(cal_archive_file_id_offset_(
                                                 LSN(persist_info.lsn_),
                                                 persist_info.archive_file_id_,
                                                 persist_info.archive_file_offset_))) {
    ARCHIVE_LOG(WARN, "cal archive file id offset failed", K(ret), K(id_), K(persist_info));
  } else {
    record_exist = true;
    piece_min_lsn_ = persist_info.start_lsn_;
    start_offset_ = persist_info.lsn_;
    max_archived_ts_ = persist_info.checkpoint_scn_;
    piece_.set(persist_info.key_.piece_id_, piece_interval_, genesis_ts_, base_piece_id_);
    ARCHIVE_LOG(INFO, "fetch exist archive progress succ", KPC(this));
  }
  return ret;
}

int StartArchiveHelper::locate_round_start_archive_point_()
{
  int ret = OB_SUCCESS;
  LSN lsn;
  bool log_gap = false;
  int64_t start_ts = OB_INVALID_TIMESTAMP;

  if (OB_FAIL(get_local_base_lsn_(lsn, log_gap))) {
    ARCHIVE_LOG(WARN, "get local base lsn failed", K(ret));
  } else if (OB_FAIL(get_local_start_ts_(start_ts))) {
    ARCHIVE_LOG(WARN, "get local start ts failed", K(ret));
  } else {
    piece_ = ObArchivePiece(start_ts, piece_interval_, genesis_ts_, base_piece_id_);
    max_archived_ts_ = start_ts;
    // 缺失日志场景下, 依然以足够安全的值初始化最大归档进度
    // 这是为开启归档后创建日志流, 为归档任何日志即回收, 有足够大的piece_id
    // piece_id既是归档进度表主键, 也是统计归档进度基准不能回退
    if (log_gap) {
      ARCHIVE_LOG(ERROR, "locate round start archive point, log gap exist", K(id_), K(min_log_ts_));
      log_gap_exist_ = log_gap;
    } else if (OB_FAIL(cal_archive_file_id_offset_(lsn, OB_INVALID_ARCHIVE_FILE_ID, 0))) {
      ARCHIVE_LOG(WARN, "cal archive file id and offset failed", K(ret), K_(id));
    } else {
      piece_min_lsn_ = lsn;
      start_offset_ = lsn;
      ARCHIVE_LOG(INFO, "locate_round_start_archive_point_ succ", KPC(this));
    }
  }
  return ret;
}

// 基于日志流定位归档起点
//
// 1. OB_SUCCESS
//    可以locate到小于等于start_ts的日志, 会定位到准确block
//    其中对于写了offline日志, 并且小于归档start_ts, 会定位到最后一个block,
//    对于这种情况不依赖palf实现, gc时即便没有归档进度也依赖归档start_ts检查是否可以回收
//
// 2. OB_ENTRY_NOT_EXIST
//    对于新建日志流且未写过任何日志的日志流, 重试即可
//
// 3. OB_ERR_OUT_OF_LOWER_BOUND
//    日志流剩余所有日志都大于start_ts
//    a) 如果日志流base_lsn等于0, 说明是新建日志流, 从0开始归档
//    b) 对于日志流base_lsn大于0, 说明日志已经被回收, 需要断流
int StartArchiveHelper::get_local_base_lsn_(palf::LSN &lsn, bool &log_gap)
{
  int ret = OB_SUCCESS;
  palf::PalfHandleGuard guard;
  if (OB_FAIL(MTL(logservice::ObLogService*)->open_palf(id_, guard))) {
    ARCHIVE_LOG(WARN, "open palf failed", K(ret), KPC(this));
  } else if (OB_FAIL(guard.locate_by_ts_ns_coarsely(min_log_ts_, lsn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
      ARCHIVE_LOG(WARN, "no log bigger than min_log_ts_, wait next turn", K(ret), K_(id), K_(min_log_ts));
    } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = guard.get_begin_lsn(lsn))) {
        ARCHIVE_LOG(WARN, "get begin lsn failed", K(tmp_ret), KPC(this));
        ret = OB_EAGAIN;
      } else if (lsn == LSN(palf::PALF_INITIAL_LSN_VAL)) {
        lsn = LSN(palf::PALF_INITIAL_LSN_VAL);
        ret = OB_SUCCESS;
      } else {
        log_gap = true;
        ARCHIVE_LOG(WARN, "log gap exist, mark fatal error");
        ret = OB_SUCCESS;
      }
    } else {
      ARCHIVE_LOG(WARN, "locate by ts_ns coarsely failed", K(ret), KPC(this));
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_START_ARCHIVE_LOG_GAP) OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
    log_gap = true;
    ret = OB_SUCCESS;
  }
#endif
  return ret;
}

// 对于开启归档立刻断流的场景, 需要为其设置准确的初始piece_id
// 1. piece_id是归档进度表主键, 必须有合理值, 重新开启归档, piece_id也依次顺序递增, 不可以偏大
// 2. piece是汇总整体归档进度基准, 对于已经冻结的piece, 不可以偏小
//
// 使用开启归档时间以及日志流创建时间作为基准piece_id
int StartArchiveHelper::get_local_start_ts_(int64_t &timestamp)
{
  int ret = OB_SUCCESS;
  int64_t create_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(persist_mgr_->get_ls_create_ts(id_, create_ts))) {
    ARCHIVE_LOG(WARN, "get ls create ts failed", K(ret), K(id_));
  } else {
    timestamp = std::max(create_ts, min_log_ts_ - 1);
  }
  return ret;
}

// 由于归档独立压缩/加密, 归档数据offset无法与ob日志offset完全一致
// 仅保证归档file_id包含对应ob日志范围, 归档file_offset独自维护
int StartArchiveHelper::cal_archive_file_id_offset_(const LSN &lsn,
    const int64_t archive_file_id,
    const int64_t archive_file_offset)
{
  int ret = OB_SUCCESS;
  int64_t file_id = OB_INVALID_ARCHIVE_FILE_ID;

  if (OB_UNLIKELY(OB_INVALID_ARCHIVE_FILE_ID ==
        (file_id = cal_archive_file_id(lsn, MAX_ARCHIVE_FILE_SIZE)))) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid file id", K(ret), K(file_id), K(lsn), K(id_));
  } else {
    archive_file_id_ = file_id;
    archive_file_offset_ = file_id == archive_file_id ? archive_file_offset : 0;
  }
  return ret;
}

} // namespace archive
} // namespace oceanbase
