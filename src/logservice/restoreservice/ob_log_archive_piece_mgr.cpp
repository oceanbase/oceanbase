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

#include "ob_log_archive_piece_mgr.h"
#include "lib/container/ob_se_array.h"            // ObSEArray
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"                 // ObString
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "logservice/archiveservice/ob_archive_define.h"
#include "logservice/archiveservice/ob_archive_file_utils.h"      // ObArchiveFileUtils
#include "logservice/archiveservice/ob_archive_util.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/log_iterator_storage.h"
#include "logservice/palf/lsn.h"
#include "logservice/palf/palf_iterator.h"
#include "rootserver/restore/ob_restore_util.h"  // ObRestoreUtil
#include "share/backup/ob_backup_path.h"         // ObBackupPath
#include "share/backup/ob_backup_store.h"        // ObBackupStore
#include "share/backup/ob_archive_store.h"
#include "share/backup/ob_archive_piece.h"        // ObArchivePiece
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_archive_path.h"   // ObArchivePathUtil
#include "share/rc/ob_tenant_base.h"
#include "share/backup/ob_archive_struct.h"       // ObArchiveLSMetaType
#include "share/ob_errno.h"
#include <cstdint>

namespace oceanbase
{
namespace logservice
{
using namespace share;
void ObLogArchivePieceContext::RoundContext::reset()
{
  state_ = RoundContext::State::INVALID;
  round_id_ = 0;
  start_scn_.reset();
  end_scn_.reset();
  min_piece_id_ = 0;
  max_piece_id_ = 0;
  base_piece_id_ = 0;
  piece_switch_interval_ = 0;
  base_piece_scn_.reset();
}

bool ObLogArchivePieceContext::RoundContext::is_valid() const
{
  return ((RoundContext::State::ACTIVE == state_ && min_piece_id_ > 0 && start_scn_ != SCN::max_scn())
      || (RoundContext::State::STOP == state_ && end_scn_ != SCN::max_scn() && end_scn_ > start_scn_))
    && round_id_ > 0
    && start_scn_.is_valid()
    && base_piece_id_ > 0
    && piece_switch_interval_ > 0
    && base_piece_scn_.is_valid();
}

bool ObLogArchivePieceContext::RoundContext::is_in_stop_state() const
{
  return RoundContext::State::STOP == state_;
}

bool ObLogArchivePieceContext::RoundContext::is_in_empty_state() const
{
  return RoundContext::State::EMPTY == state_;
}

bool ObLogArchivePieceContext::RoundContext::is_in_active_state() const
{
  return RoundContext::State::ACTIVE == state_;
}

ObLogArchivePieceContext::RoundContext &ObLogArchivePieceContext::RoundContext::operator=(const RoundContext &other)
{
  state_ = other.state_;
  round_id_ = other.round_id_;
  start_scn_ = other.start_scn_;
  end_scn_ = other.end_scn_;
  min_piece_id_ = other.min_piece_id_;
  max_piece_id_ = other.max_piece_id_;
  base_piece_id_ = other.base_piece_id_;
  piece_switch_interval_ = other.piece_switch_interval_;
  base_piece_scn_ = other.base_piece_scn_;
  return *this;
}

bool ObLogArchivePieceContext::RoundContext::check_round_continuous_(const RoundContext &pre_round) const
{
  bool bret = false;
  if (!pre_round.is_valid() || !is_valid()) {
    bret = false;
  } else if (pre_round.end_scn_ >= start_scn_) {
    bret = true;
  }
  return bret;
}

void ObLogArchivePieceContext::InnerPieceContext::reset()
{
  state_ = InnerPieceContext::State::INVALID;
  piece_id_ = 0;
  round_id_ = 0;
  min_lsn_in_piece_.reset();
  max_lsn_in_piece_.reset();
  min_file_id_ = 0;
  max_file_id_ = 0;
  file_id_ = 0;
  file_offset_ = 0;
  max_lsn_.reset();
}

bool ObLogArchivePieceContext::InnerPieceContext::is_valid() const
{
  return State::EMPTY == state_
    || State::LOW_BOUND == state_
    || ((State::ACTIVE == state_ || State::FROZEN == state_ || State::GC == state_)
        && min_lsn_in_piece_.is_valid()
        && min_file_id_ > 0
        && max_file_id_ >= min_file_id_
        && min_lsn_in_piece_.is_valid());
}

int ObLogArchivePieceContext::InnerPieceContext::update_file(
    const int64_t file_id,
    const int64_t file_offset,
    const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_id > max_file_id_ || file_id < min_file_id_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(file_id), K(file_offset), K(lsn));
  } else {
    file_id_ = file_id;
    file_offset_ = file_offset;
    max_lsn_ = lsn;
  }
  return ret;
}

ObLogArchivePieceContext::InnerPieceContext &ObLogArchivePieceContext::InnerPieceContext::operator=(
    const InnerPieceContext &other)
{
  state_ = other.state_;
  piece_id_ = other.piece_id_;
  round_id_ = other.round_id_;
  min_lsn_in_piece_ = other.min_lsn_in_piece_;
  max_lsn_in_piece_ = other.max_lsn_in_piece_;
  min_file_id_ = other.min_file_id_;
  max_file_id_ = other.max_file_id_;
  file_id_ = other.file_id_;
  file_offset_ = other.file_offset_;
  max_lsn_ = other.max_lsn_;
  return *this;
}

ObLogArchivePieceContext::ObLogArchivePieceContext() :
  is_inited_(false),
  locate_round_(false),
  id_(),
  dest_id_(0),
  min_round_id_(0),
  max_round_id_(0),
  round_context_(),
  inner_piece_context_(),
  archive_dest_()
{}

ObLogArchivePieceContext::~ObLogArchivePieceContext()
{
  reset();
}

void ObLogArchivePieceContext::reset()
{
  is_inited_ = false;
  id_.reset();
  archive_dest_.reset();
  reset_locate_info();
}

int ObLogArchivePieceContext::init(const share::ObLSID &id,
    const share::ObBackupDest &archive_dest)
{
  // before piece context init with new log_restore_source, the context should be reset first
  reset();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! id.is_valid()
        || ! archive_dest.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(id), K(archive_dest));
  } else if (OB_FAIL(archive_dest_.deep_copy(archive_dest))) {
    CLOG_LOG(WARN, "root path deep copy failed", K(ret), K(id), K(archive_dest));
  } else {
    id_ = id;
    is_inited_ = true;
  }
  return ret;
}

bool ObLogArchivePieceContext::is_valid() const
{
  return is_inited_
    && locate_round_
    && id_.is_valid()
    && dest_id_ > 0
    && min_round_id_ > 0
    && max_round_id_ >= min_round_id_
    && round_context_.is_valid()
    && inner_piece_context_.is_valid()
    && archive_dest_.is_valid();
}

int ObLogArchivePieceContext::get_piece(const SCN &pre_scn,
    const palf::LSN &start_lsn,
    int64_t &dest_id,
    int64_t &round_id,
    int64_t &piece_id,
    int64_t &file_id,
    int64_t &offset,
    palf::LSN &max_lsn,
    bool &to_newest)
{
  int ret = OB_SUCCESS;
  // if piece context not valid, reset it
  if (! is_valid()) {
    reset_locate_info();
  }

  file_id = cal_archive_file_id_(start_lsn);
  if (OB_UNLIKELY(! pre_scn.is_valid() || ! start_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(pre_scn), K(start_lsn));
  } else if (OB_FAIL(get_piece_(pre_scn, start_lsn, file_id, dest_id,
          round_id, piece_id, offset, max_lsn, to_newest))) {
    CLOG_LOG(WARN, "get piece failed", K(ret));
  }
  return ret;
}

int ObLogArchivePieceContext::deep_copy_to(ObLogArchivePieceContext &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(other.archive_dest_.deep_copy(archive_dest_))) {
    CLOG_LOG(WARN, "deep copy failed", K(ret));
  } else {
    other.is_inited_ = is_inited_;
    other.locate_round_ = locate_round_;
    other.id_ = id_;
    other.dest_id_ = dest_id_;
    other.min_round_id_ = min_round_id_;
    other.max_round_id_ = max_round_id_;
    other.round_context_ = round_context_;
    other.inner_piece_context_ = inner_piece_context_;
  }
  return ret;
}

void ObLogArchivePieceContext::reset_locate_info()
{
  locate_round_ = false;
  dest_id_ = 0;
  min_round_id_ = 0;
  max_round_id_ = 0;
  round_context_.reset();
  inner_piece_context_.reset();
}

int ObLogArchivePieceContext::update_file_info(const int64_t dest_id,
    const int64_t round_id,
    const int64_t piece_id,
    const int64_t file_id,
    const int64_t file_offset,
    const palf::LSN &max_lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_id <= 0 || file_offset < 0 || ! max_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(file_id), K(file_offset), K(max_lsn));
  } else if (dest_id != dest_id_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "different dest, unexpected", K(ret), K(dest_id), K(round_id),
        K(piece_id), K(file_id), K(file_offset), K(max_lsn), KPC(this));
  } else if (OB_FAIL(inner_piece_context_.update_file(file_id, file_offset, max_lsn))) {
    CLOG_LOG(WARN, "inner_piece_context_ update file failed", K(ret), K(dest_id), K(round_id),
        K(piece_id), K(file_id), K(file_offset), K(max_lsn), KPC(this));
  }
  return ret;
}

void ObLogArchivePieceContext::get_max_file_info(int64_t &dest_id,
    int64_t &round_id,
    int64_t &piece_id,
    int64_t &max_file_id,
    int64_t &max_file_offset,
    palf::LSN &max_lsn)
{
  dest_id = dest_id_;
  round_id = round_context_.round_id_;
  piece_id = inner_piece_context_.piece_id_;
  max_file_id = inner_piece_context_.file_id_;
  max_file_offset = inner_piece_context_.file_offset_;
  max_lsn = inner_piece_context_.max_lsn_;
}

int ObLogArchivePieceContext::get_max_archive_log(palf::LSN &lsn, SCN &scn)
{
  int ret = OB_SUCCESS;
  ObLogArchivePieceContext orign_context;
  if (OB_FAIL(deep_copy_to(orign_context))) {
    CLOG_LOG(WARN, "piece context deep copy failed", KPC(this));
  } else if (OB_FAIL(get_round_range_())) {
    CLOG_LOG(WARN, "get round range failed", K(ret), KPC(this));
  } else if (OB_FAIL(get_max_archive_log_(orign_context, lsn, scn))) {
    CLOG_LOG(WARN, "get max archive log failed", K(ret), KPC(this));
  } else {
    CLOG_LOG(INFO, "get max archive log succ", K(ret), K(lsn), K(scn), KPC(this));
  }
  return ret;
}

int ObLogArchivePieceContext::seek(const SCN &scn, palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret));
  } else {
    ret = seek_(scn, lsn);
  }
  return ret;
}

int ObLogArchivePieceContext::get_ls_meta_data(
    const share::ObArchiveLSMetaType &meta_type,
      const SCN &timestamp,
      char *buf,
      const int64_t buf_size,
      int64_t &read_size,
      const bool fuzzy_match)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf
        || buf_size <= 0
        || !meta_type.is_valid()
        || !timestamp.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = get_ls_meta_data_(meta_type, timestamp, fuzzy_match, buf, buf_size, read_size);
  }
  return ret;
}

int ObLogArchivePieceContext::get_round_(const SCN &start_scn)
{
  int ret = OB_SUCCESS;
  int64_t round_id = 0;
  share::ObArchiveStore archive_store;
  if (OB_UNLIKELY(dest_id_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "invalid dest id", K(ret), K(dest_id_));
  } else if (OB_UNLIKELY(locate_round_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "already locate round", K(ret), KPC(this));
  } else if (OB_FAIL(archive_store.init(archive_dest_))) {
    CLOG_LOG(WARN, "backup store init failed", K(ret), K_(archive_dest));
  } else if (OB_FAIL(archive_store.get_round_id(dest_id_, start_scn, round_id))) {
    CLOG_LOG(WARN, "archive store get round failed", K(ret), K(dest_id_), K(start_scn));
  } else if (OB_UNLIKELY(round_id <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "invalid round id", K(ret), K(round_id), K(start_scn), K(archive_store));
  } else {
    round_context_.reset();
    round_context_.round_id_ = round_id;
    locate_round_ = true;
    CLOG_LOG(INFO, "get round succ", K(ret), K(start_scn), KPC(this));
  }
  return ret;
}

int ObLogArchivePieceContext::get_round_range_()
{
  int ret = OB_SUCCESS;
  int64_t min_round_id = 0;
  int64_t max_round_id = 0;
  share::ObArchiveStore archive_store;
  if (OB_FAIL(load_archive_meta_())) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_INVALID_BACKUP_DEST;
      CLOG_LOG(WARN, "archive meta file not exist, invalid archive dest", K(id_), K(archive_dest_));
    } else {
      CLOG_LOG(WARN, "load archive meta failed", K(id_), K(archive_dest_));
    }
  } else if (OB_FAIL(archive_store.init(archive_dest_))) {
    CLOG_LOG(WARN, "backup store init failed", K(ret), K_(archive_dest));
  } else if (OB_FAIL(archive_store.get_round_range(dest_id_, min_round_id, max_round_id))) {
    CLOG_LOG(WARN, "archive store get round failed", K(ret), K(dest_id_));
  } else if (OB_UNLIKELY(min_round_id <= 0 || max_round_id < min_round_id)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid round id", K(ret), K(min_round_id),
        K(max_round_id), K(archive_store), KPC(this));
  } else if (max_round_id_ == max_round_id && min_round_id_ == min_round_id) {
    // skip
  } else {
    min_round_id_ = min_round_id;
    max_round_id_ = max_round_id;
    CLOG_LOG(INFO, "get round range succ", K(ret), K(min_round_id), K(max_round_id), K(id_));
  }
  return ret;
}

int ObLogArchivePieceContext::load_archive_meta_()
{
  int ret = OB_SUCCESS;
  share::ObBackupStore backup_store;
  share::ObBackupFormatDesc desc;
  if (OB_FAIL(backup_store.init(archive_dest_))) {
    CLOG_LOG(WARN, "backup store init failed", K(ret), K_(archive_dest));
  } else if (OB_FAIL(backup_store.read_format_file(desc))) {
    CLOG_LOG(WARN, "read single file failed", K(ret), K(archive_dest_), K(backup_store));
  } else if (OB_UNLIKELY(! desc.is_valid())) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(WARN, "backup format desc is invalid", K(ret), K(desc), K(backup_store));
  } else {
    dest_id_ = desc.dest_id_;
    CLOG_LOG(INFO, "load archive meta succ", K(desc));
  }
  return ret;
}

// 此处假设stop状态的round, 一定都包含round end file
// 反之仅round end file持久化成功的轮次, 才可以确认是stop状态, 与归档集群状态无关
int ObLogArchivePieceContext::load_round_(const int64_t round_id, RoundContext &round_context, bool &exist)
{
  int ret = OB_SUCCESS;
  share::ObArchiveStore archive_store;
  bool start_exist = false;
  bool end_exist = false;
  share::ObRoundStartDesc start_desc;
  share::ObRoundEndDesc end_desc;
  exist = true;
  if (OB_FAIL(archive_store.init(archive_dest_))) {
    CLOG_LOG(WARN, "backup store init failed", K(ret), K_(archive_dest));
  } else if (OB_FAIL(archive_store.is_round_start_file_exist(dest_id_, round_id, start_exist))) {
    CLOG_LOG(WARN, "check round start file exist failed", K(ret), K(round_id), KPC(this));
  } else if (! start_exist) {
    exist = false;
    CLOG_LOG(INFO, "round not exist, skip it", K(round_id), KPC(this));
  } else if (OB_FAIL(archive_store.read_round_start(dest_id_, round_id, start_desc))) {
    CLOG_LOG(WARN, "read round start file failed", K(ret), K(round_id), KPC(this));
  } else if (OB_FAIL(archive_store.is_round_end_file_exist(dest_id_, round_id, end_exist))) {
    CLOG_LOG(WARN, "check round start file exist failed", K(ret), K(round_id), KPC(this));
  } else if (! end_exist) {
    CLOG_LOG(INFO, "round end file not exist, round not stop", K(round_id), KPC(this));
  } else if (OB_FAIL(archive_store.read_round_end(dest_id_, round_id, end_desc))) {
    CLOG_LOG(WARN, "check round start file exist failed", K(ret), K(round_id), KPC(this));
  }

  if (OB_FAIL(ret) || ! exist) {
  } else {
    round_context.round_id_ = start_desc.round_id_;
    round_context.start_scn_ = start_desc.start_scn_;
    round_context.base_piece_id_ = start_desc.base_piece_id_;
    round_context.base_piece_scn_ = start_desc.start_scn_;
    round_context.piece_switch_interval_ = start_desc.piece_switch_interval_;
    if (end_exist) {
      round_context.state_ = RoundContext::State::STOP;
      round_context.end_scn_ = end_desc.checkpoint_scn_;
      round_context.max_piece_id_ = cal_piece_id_(round_context.end_scn_);
    } else {
      round_context.state_ = RoundContext::State::ACTIVE;
      round_context.end_scn_.set_max();
    }
  }
  return ret;
}

int ObLogArchivePieceContext::get_piece_(const SCN &scn,
    const palf::LSN &lsn,
    const int64_t file_id,
    int64_t &dest_id,
    int64_t &round_id,
    int64_t &piece_id,
    int64_t &offset,
    palf::LSN &max_lsn,
    bool &to_newest)
{
  int ret = OB_SUCCESS;
  bool done = false;
  while (OB_SUCC(ret) && ! done) {
    if (OB_FAIL(switch_round_if_need_(scn, lsn))) {
      CLOG_LOG(WARN, "switch round if need failed", K(ret), KPC(this));
    } else if (OB_FAIL(switch_piece_if_need_(file_id, scn, lsn))) {
      CLOG_LOG(WARN, "switch piece if need", K(ret), KPC(this));
    } else {
      ret = get_(lsn, file_id, dest_id, round_id, piece_id, offset, max_lsn, done, to_newest);
    }

    // 由于场景复杂, 为避免遗漏场景导致无法跳出循环, 每次重试sleep 100ms
    if (! done) {
      ob_usleep(100 * 1000L);
    }

    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      CLOG_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "get piece cost too much time", K(scn), K(lsn), KPC(this));
    }
  }
  return ret;
}

int ObLogArchivePieceContext::switch_round_if_need_(const SCN &scn, const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  RoundOp op = RoundOp::NONE;
  RoundContext pre_round = round_context_;
  check_if_switch_round_(scn, lsn, op);
  switch (op) {
    case RoundOp::NONE:
      break;
    case RoundOp::LOAD:
      ret = load_round_info_();
      break;
    case RoundOp::LOAD_RANGE:
      ret = get_round_range_();
      break;
    case RoundOp::LOCATE:
      ret = get_round_(scn);
      break;
    case RoundOp::FORWARD:
      ret = forward_round_(pre_round);
      break;
    case RoundOp::BACKWARD:
      ret = backward_round_();
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "invalid round op", K(ret), K(op));
      break;
  }
  return ret;
}

void ObLogArchivePieceContext::check_if_switch_round_(const share::SCN &scn, const palf::LSN &lsn, RoundOp &op)
{
  op = RoundOp::NONE;
  if (min_round_id_ == 0 || max_round_id_ == 0
      || round_context_.round_id_ > max_round_id_
      || is_max_round_done_(lsn) /* 当前读取到最大round的最大值, 并且round已经STOP*/) {
    op = RoundOp::LOAD_RANGE;
  } else if (! locate_round_) {
    op = RoundOp::LOCATE;
  } else if (need_backward_round_(lsn)/*确定当前round日志全部大于需要消费日志, 并且当前round大于最小round id*/) {
    op = RoundOp::BACKWARD;
  } else if (need_forward_round_(lsn)/*确定当前round日志全部小于需要消费日志, 并且当前round小于最大round id*/) {
    op = RoundOp::FORWARD;
  } else if (need_load_round_info_(scn, lsn)/*当前round能访问到的最大piece已经STOP, 并且当前round还是ACTIVE的*/) {
    op = RoundOp::LOAD;
  }

  if (RoundOp::NONE != op) {
    CLOG_LOG(INFO, "check_if_switch_round_ op", K(op), KPC(this));
  }
}

// 检查是否所有round日志都被消费完
// 当前读取到最大round的最大值, 并且round已经STOP
bool ObLogArchivePieceContext::is_max_round_done_(const palf::LSN &lsn) const
{
  bool bret = false;
  if (max_round_id_ <= 0) {
    bret = false;
  } else if (! round_context_.is_valid()
      || round_context_.round_id_ < max_round_id_
      || ! round_context_.is_in_stop_state()) {
    bret = false;
  } else if (! inner_piece_context_.is_valid()
      || inner_piece_context_.round_id_ < max_round_id_
      || inner_piece_context_.piece_id_ < round_context_.max_piece_id_
      || ! inner_piece_context_.is_frozen_()
      || lsn < inner_piece_context_.max_lsn_in_piece_) {
    bret = false;
  } else {
    bret = true;
    CLOG_LOG(INFO, "max round consume done, need switch load round range", KPC(this));
  }
  return bret;
}

// 可以前向切round, 一定是piece已经切到最小, 并且最小piece是FROZEN状态
bool ObLogArchivePieceContext::need_backward_round_(const palf::LSN &lsn) const
{
  bool bret = false;
  if (min_round_id_ > 0
      && round_context_.is_valid()
      && round_context_.round_id_ > min_round_id_
      && (inner_piece_context_.is_frozen_() || inner_piece_context_.is_empty_() || inner_piece_context_.is_gc_())
      && round_context_.min_piece_id_ == inner_piece_context_.piece_id_
      && inner_piece_context_.min_lsn_in_piece_ > lsn) {
    bret = true;
  }
  return bret;
}

// Note: 对于空round, 默认一定是与下一个round是不连续的; 因此遇到空round, 默认只能前向查询, 不支持后向查找
//  bad case round 1、2、3, 当前为3，需要切到round 1, 中间round 2为空, 只依赖round 2信息无法判断需要切到round 1
//  遇到前向切round, 由调用者处理
bool ObLogArchivePieceContext::need_forward_round_(const palf::LSN &lsn) const
{
  bool bret = false;
  if (max_round_id_ <= 0) {
    bret = false;
  } else if (round_context_.is_in_empty_state()) {
    bret = true;
    CLOG_LOG(INFO, "empty round, only forward round supported", K(round_context_));
  } else if (round_context_.is_valid()
      && round_context_.is_in_stop_state()
      && round_context_.round_id_ < max_round_id_
      && inner_piece_context_.piece_id_ == round_context_.max_piece_id_
      && inner_piece_context_.is_valid()) {
    // 当前piece是round下最新piece, 并且round已经STOP
    if (inner_piece_context_.is_low_bound_()) {
      // 当前piece状态为LOW_BOUND, 在当前piece内日志流仍未产生
      bret = true;
    }
    else if ((inner_piece_context_.is_frozen_() || inner_piece_context_.is_empty_())
        && inner_piece_context_.max_lsn_in_piece_ <= lsn) {
      // 同时当前piece状态为FROZEN/EMPTY
      // 并且需要日志LSN大于当前piece下最大值
      bret = true;
    }
  }
  return bret;
}

// 前提: 当前round_context状态非STOP
// 1. 当前round_context信息是无效的
// 2. 当前round最大piece不包含需要消费的日志
bool ObLogArchivePieceContext::need_load_round_info_(const share::SCN &scn, const palf::LSN &lsn) const
{
  bool bret = false;
  if (round_context_.is_in_stop_state()) {
    bret = false;
  } else if (round_context_.round_id_ > max_round_id_ || round_context_.round_id_ < min_round_id_ || round_context_.round_id_ <= 0) {
    bret = false;
  } else if (! round_context_.is_valid()) {
    bret = true;
  } else if (round_context_.max_piece_id_ == inner_piece_context_.piece_id_
      && (inner_piece_context_.is_frozen_() || inner_piece_context_.is_empty_())
      && lsn >= inner_piece_context_.max_lsn_in_piece_) {
    // The piece id of current inner piece equals to the max piece id of the current round,
    // and the no more log will be added in the piece (piece state is frozen or empty),
    // and the lsn needed is larger than the max lsn of the piece.
    //
    // In this case, the piece range should be reloaded and advanced.
    bret = true;
  } else {
    bret = cal_piece_id_(scn) > round_context_.max_piece_id_;
  }
  return bret;
}

int ObLogArchivePieceContext::load_round_info_()
{
  int ret = OB_SUCCESS;
  bool round_exist = false;
  int64_t min_piece_id = 0;
  int64_t max_piece_id = 0;
  if (OB_UNLIKELY(round_context_.round_id_ > max_round_id_
        || round_context_.round_id_ < min_round_id_
        || round_context_.round_id_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "round id not valid", K(ret), K_(round_context));
  } else if (OB_FAIL(load_round_(round_context_.round_id_, round_context_, round_exist))) {
    CLOG_LOG(WARN, "load round failed", K(ret), K_(round_context));
  } else if (! round_exist) {
    ret = OB_ARCHIVE_ROUND_NOT_CONTINUOUS;
    CLOG_LOG(WARN, "round not exist, unexpected", K(ret), K(round_exist), K_(round_context));
  } else if (OB_FAIL(get_round_piece_range_(round_context_.round_id_, min_piece_id, max_piece_id))
      && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "get piece range failed", K(ret), K_(round_context));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    if (round_context_.is_in_stop_state()) {
      round_context_.state_ = RoundContext::State::EMPTY;
    }
  } else {
    round_context_.min_piece_id_ = min_piece_id;
    round_context_.max_piece_id_ = max_piece_id;
    CLOG_LOG(INFO, "load round info succ", K_(round_context));
  }
  return ret;
}

int ObLogArchivePieceContext::get_round_piece_range_(const int64_t round_id, int64_t &min_piece_id, int64_t &max_piece_id)
{
  int ret = OB_SUCCESS;
  share::ObArchiveStore archive_store;
  share::ObPieceInfoDesc desc;
  if (OB_FAIL(archive_store.init(archive_dest_))) {
    CLOG_LOG(WARN, "backup store init failed", K(ret), K_(archive_dest));
  } else if (OB_FAIL(archive_store.get_piece_range(dest_id_, round_id, min_piece_id, max_piece_id))) {
    CLOG_LOG(WARN, "get piece range failed", K(ret), K(dest_id_), K(round_id));
  }
  return ret;
}

// 由于round id可能不连续, 该函数找到一个可以消费的轮次
int ObLogArchivePieceContext::forward_round_(const RoundContext &pre_round)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(min_round_id_ <= 0
        || max_round_id_ < min_round_id_
        || round_context_.round_id_ >= max_round_id_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KPC(this));
  } else {
    bool done = false;
    int64_t round_id = round_context_.round_id_ + 1;
    while (OB_SUCC(ret) && ! done && round_id <= max_round_id_) {
      bool exist = false;
      if (OB_FAIL(check_round_exist_(round_id, exist))) {
        CLOG_LOG(WARN, "check round exist failed", K(ret));
      } else if (exist) {
        done = true;
      } else {
        round_id++;
      }
    }
    // 由于max_round_id一定存在, 预期成功一定可以done
    if (OB_SUCC(ret) && done) {
      round_context_.reset();
      round_context_.round_id_ = round_id;
      CLOG_LOG(INFO, "forward round succ", K(round_id), KPC(this));
    }

    if (OB_SUCC(ret) && done) {
      if (OB_FAIL(load_round_info_())) {
        CLOG_LOG(WARN, "load round info failed", K(ret));
      } else if (! round_context_.check_round_continuous_(pre_round)) {
        ret = OB_ARCHIVE_ROUND_NOT_CONTINUOUS;
        CLOG_LOG(WARN, "forward round not continue", K(ret), K(pre_round), K(round_context_));
      }
    }
  }
  return ret;
}

// 前向查找更加复杂, 不支持直接前向查询, 由调用者处理
int ObLogArchivePieceContext::backward_round_()
{
  return OB_ERR_OUT_OF_LOWER_BOUND;
}

int ObLogArchivePieceContext::check_round_exist_(const int64_t round_id, bool &exist)
{
  int ret = OB_SUCCESS;
  share::ObArchiveStore archive_store;
  if (OB_FAIL(archive_store.init(archive_dest_))) {
    CLOG_LOG(WARN, "backup store init failed", K(ret), K_(archive_dest));
  } else if (OB_FAIL(archive_store.is_round_start_file_exist(dest_id_, round_id, exist))) {
    CLOG_LOG(WARN, "check round start file exist failed", K(ret), K(dest_id_), K(round_id));
  }
  return ret;
}

int ObLogArchivePieceContext::switch_piece_if_need_(const int64_t file_id, const SCN &scn, const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  PieceOp op = PieceOp::NONE;
  check_if_switch_piece_(file_id, lsn, op);
  switch (op) {
    case PieceOp::NONE:
      break;
    case PieceOp::LOAD:
      ret = get_cur_piece_info_(scn);
      break;
    case PieceOp::ADVANCE:
      ret = advance_piece_();
      break;
    case PieceOp::BACKWARD:
      ret = backward_piece_();
      break;
    case PieceOp::FORWARD:
      ret = forward_piece_();
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      CLOG_LOG(ERROR, "piece op not supported", K(ret), K(op), KPC(this));
  }
  return ret;
}

void ObLogArchivePieceContext::check_if_switch_piece_(const int64_t file_id,
    const palf::LSN &lsn,
    PieceOp &op)
{
  op = PieceOp::NONE;
  if (! round_context_.is_valid()) {
    op = PieceOp::NONE;
  } else if (! inner_piece_context_.is_valid()) {
    op = PieceOp::LOAD;
  } else if (inner_piece_context_.round_id_ != round_context_.round_id_) {
    op = PieceOp::LOAD;
  }
  // 当前piece状态确定
  else if (inner_piece_context_.is_frozen_()) {
    // check forward or backward
    if (inner_piece_context_.max_lsn_in_piece_ > lsn) {
      if (inner_piece_context_.min_lsn_in_piece_ > lsn) {
        op = PieceOp::BACKWARD;
      } else {
         op = PieceOp::NONE;
      }
    } else {
      op = PieceOp::FORWARD;
    }
  }
  // 当前piece为EMPTY
  else if (inner_piece_context_.is_empty_()) {
    if (inner_piece_context_.max_lsn_in_piece_.is_valid() && inner_piece_context_.max_lsn_in_piece_ > lsn) {
      op = PieceOp::BACKWARD;
    } else {
      op = PieceOp::FORWARD;
    }
  }
  // 当前piece状态是LOW BOUND
  else if (inner_piece_context_.is_low_bound_()) {
    op = PieceOp::FORWARD;
  }
  // 当前piece内日志流GC
  else if (inner_piece_context_.is_gc_()) {
    if (inner_piece_context_.max_lsn_in_piece_ > lsn) {
      if (inner_piece_context_.min_lsn_in_piece_ > lsn) {
        op = PieceOp::BACKWARD;
      } else {
         op = PieceOp::NONE;
      }
    } else {
      // gc state, no bigger lsn exist in next pieces
      op = PieceOp::NONE;
    }
  }
  // 当前piece仍然为ACTIVE
  else {
    // 1. min_lsn > lsn --> backward
    // 2. max_file_id not bigger than file_id --> advance
    // 3. otherwise --> none
    if (inner_piece_context_.min_lsn_in_piece_ > lsn) {
      op = PieceOp::BACKWARD;
    } else if (inner_piece_context_.max_file_id_ <= file_id) {
      // if piece context is stale, load piece meta fully
      // else just advance piece simply
      if (inner_piece_context_.max_file_id_ + 1 < file_id) {
        op = PieceOp::LOAD;
      } else {
        op = PieceOp::ADVANCE;
      }
    } else {
      op = PieceOp::NONE;
    }
  }

  if (PieceOp::NONE != op) {
    CLOG_LOG(INFO, "check switch_piece_ op", K(lsn), K(file_id), K(inner_piece_context_), K(round_context_), K(op));
  }
}

// 由scn以及inner_piece_context与round_context共同决定piece_id
// 1. inner_piece_context.round_id == round_context.round_id 说明依然消费当前round, 如果当前piece_id有效, 并且该piece依然active, 则继续刷新该piece
// 2. inner_piece_context.round_id == round_context.round_id 并且piece状态为FROZEN或者EMPTY, 非预期错误
// 3. inner_piece_context.round_id != round_context.round_id, 需要由scn以及round piece范围, 决定piece id
int ObLogArchivePieceContext::get_cur_piece_info_(const SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t piece_id = 0;
  if (OB_FAIL(cal_load_piece_id_(scn, piece_id))) {
    CLOG_LOG(WARN, "cal load piece id failed", K(ret), K(scn));
  } else if (0 >= piece_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid piece id", K(ret), K(piece_id), K(scn));
  } else if (OB_FAIL(get_piece_meta_info_(piece_id))) {
    CLOG_LOG(WARN, "get piece meta info failed", K(ret), K(piece_id));
  } else if (OB_FAIL(get_piece_file_range_())) {
    CLOG_LOG(WARN, "get piece file range failed", K(ret), K(inner_piece_context_));
  } else if (OB_FAIL(get_min_lsn_in_piece_())) {
    CLOG_LOG(WARN, "get min lsn in piece failed", K(ret));
  } else {
    CLOG_LOG(INFO, "get cur piece info succ", KPC(this));
  }
  return ret;
}

int ObLogArchivePieceContext::cal_load_piece_id_(const SCN &scn, int64_t &piece_id)
{
  int ret = OB_SUCCESS;
  const int64_t base_piece_id = cal_piece_id_(scn);
  if (inner_piece_context_.round_id_ == round_context_.round_id_) {
    // 大概率被回收了, 报错处理, 后续可以优化
    if (inner_piece_context_.piece_id_ < round_context_.min_piece_id_) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "piece maybe recycle", K(ret), KPC(this));
    } else if (inner_piece_context_.piece_id_ > round_context_.max_piece_id_) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "piece id out of range", K(ret), KPC(this));
    } else {
      piece_id = inner_piece_context_.piece_id_;
    }
  } else {
    piece_id = std::max(round_context_.min_piece_id_, base_piece_id);
  }
  CLOG_LOG(INFO, "cal load piece id", K(id_), K(round_context_), K(scn), K(piece_id));
  return ret;
}

int ObLogArchivePieceContext::get_piece_meta_info_(const int64_t piece_id)
{
  int ret = OB_SUCCESS;
  const int64_t round_id = round_context_.round_id_;
  share::ObArchiveStore archive_store;
  bool piece_meta_exist = true;
  bool is_ls_in_piece = false;
  bool is_ls_gc = false;
  palf::LSN min_lsn;
  palf::LSN max_lsn;
  if (piece_id < round_context_.min_piece_id_) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "piece id out of round range lower bound", K(ret), K(piece_id), KPC(this));
  } else if (piece_id > round_context_.max_piece_id_) {
    ret = OB_ITER_END;
    CLOG_LOG(WARN, "piece id out of round range upper bound", K(ret), K(piece_id), KPC(this));
  } else if (OB_FAIL(archive_store.init(archive_dest_))) {
    CLOG_LOG(WARN, "backup store init failed", K(ret), K_(archive_dest));
  } else if (OB_FAIL(archive_store.is_single_piece_file_exist(dest_id_, round_id, piece_id, piece_meta_exist))) {
    // 不同日志流归档进度不同, 可能存在多个piece未FROZEN情况, 消费piece需要确认是否FROZEN
    CLOG_LOG(WARN, "check single piece file exist failed", K(ret), K(piece_id), KPC(this));
  } else if (! piece_meta_exist) {
    // single piece file not exist, active piece
  } else if (OB_FAIL(get_ls_inner_piece_info_(id_, dest_id_, round_id, piece_id, min_lsn, max_lsn, is_ls_in_piece, is_ls_gc))) {
    CLOG_LOG(WARN, "get ls inner piece info failed", K(ret), K(round_id), K(piece_id), K(id_));
  }

  if (OB_SUCC(ret)) {
    inner_piece_context_.round_id_ = round_id;
    inner_piece_context_.piece_id_ = piece_id;
    // 如果piece meta 存在, 那么piece已经FROZEN, 日志流在该piece状态可能为FROZEN, EMPTY和LOW_BOUND
    if (piece_meta_exist) {
      // ls在该piece内存在, 那么状态一定是FROZEN或者EMPTY
      if (is_ls_in_piece) {
        inner_piece_context_.min_lsn_in_piece_ = min_lsn;
        inner_piece_context_.max_lsn_in_piece_ = max_lsn;
        if (is_ls_gc) {
          inner_piece_context_.state_ = InnerPieceContext::State::GC;
          inner_piece_context_.min_file_id_ = cal_archive_file_id_(inner_piece_context_.min_lsn_in_piece_);
          inner_piece_context_.max_file_id_ = cal_archive_file_id_(inner_piece_context_.max_lsn_in_piece_);
        } else if (inner_piece_context_.min_lsn_in_piece_ == inner_piece_context_.max_lsn_in_piece_) {
          inner_piece_context_.state_ = InnerPieceContext::State::EMPTY;
        } else {
          inner_piece_context_.state_ = InnerPieceContext::State::FROZEN;
          inner_piece_context_.min_file_id_ = cal_archive_file_id_(inner_piece_context_.min_lsn_in_piece_);
          inner_piece_context_.max_file_id_ = cal_archive_file_id_(inner_piece_context_.max_lsn_in_piece_);
        }
      }
      // ls在该piece内不存在, 并且该piece是FROZEN状态, 那么ls一定在更大piece内
      // NOTE: restore不理解归档文件内容, 很难区分是GC或LOW_BOUND
      // 依赖zeyong提交, 标示日志流已GC, 物理恢复场景固定, 可以假设是LOW_BOUND
      else {
        inner_piece_context_.state_ = InnerPieceContext::State::LOW_BOUND;
      }
    }
    // piece meta 不存在, piece一定是ACTIVE状态的
    else {
      inner_piece_context_.state_ = InnerPieceContext::State::ACTIVE;
    }
  }
  return ret;
}

int ObLogArchivePieceContext::get_ls_inner_piece_info_(const share::ObLSID &id,
    const int64_t dest_id,
    const int64_t round_id,
    const int64_t piece_id,
    palf::LSN &min_lsn,
    palf::LSN &max_lsn,
    bool &exist,
    bool &gc)
{
  int ret = OB_SUCCESS;
  share::ObArchiveStore archive_store;
  share::ObSingleLSInfoDesc desc;
  exist = false;
  gc = false;
  if (OB_FAIL(archive_store.init(archive_dest_))) {
    CLOG_LOG(WARN, "backup store init failed", K(ret), K_(archive_dest));
  } else if (OB_FAIL(archive_store.read_single_ls_info(dest_id, round_id, piece_id, id, desc))
      && OB_BACKUP_FILE_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "get single piece file failed", K(ret), K(dest_id), K(round_id), K(piece_id), K(id));
  } else if (OB_BACKUP_FILE_NOT_EXIST == ret) {
    exist = false;
    ret = OB_SUCCESS;
    CLOG_LOG(INFO, "ls not exist in cur piece", K(dest_id), K(round_id), K(piece_id), K(id));
  } else if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(WARN, "invalid single piece file", K(ret), K(dest_id),
        K(round_id), K(piece_id), K(id), K(desc));
  } else {
    exist = true;
    gc = desc.deleted_;
    min_lsn = palf::LSN(desc.min_lsn_);
    max_lsn = palf::LSN(desc.max_lsn_);
  }
  return ret;
}

int ObLogArchivePieceContext::get_piece_file_range_()
{
  int ret = OB_SUCCESS;
  share::ObArchiveStore archive_store;
  int64_t min_file_id = 0;
  int64_t max_file_id = 0;
  if (! inner_piece_context_.is_active()) {
    // piece context is frozen or empty or low bound, file range is certain
  } else if (OB_FAIL(archive_store.init(archive_dest_))) {
    CLOG_LOG(WARN, "backup store init failed", K(ret), K_(archive_dest));
  } else if (OB_FAIL(archive_store.get_file_range_in_piece(dest_id_, inner_piece_context_.round_id_,
          inner_piece_context_.piece_id_, id_, min_file_id, max_file_id))
      && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "get file range failed", K(ret), K(inner_piece_context_), K(id_));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    CLOG_LOG(INFO, "file not exist in piece, rewrite ret code", K(ret), KPC(this));
    ret = OB_ITER_END;
  } else if (min_file_id <= 0 || max_file_id < min_file_id) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "invalid file id", K(ret), K(min_file_id),
        K(max_file_id), K(id_), K(inner_piece_context_));
  } else {
    inner_piece_context_.min_file_id_ = min_file_id;
    inner_piece_context_.max_file_id_ = max_file_id;
  }
  return ret;
}

int ObLogArchivePieceContext::advance_piece_()
{
  int ret = OB_SUCCESS;
  bool next_file_exist = false;
  bool piece_meta_exist = false;
  share::ObArchiveStore archive_store;
  if (OB_UNLIKELY(!round_context_.is_valid() || !inner_piece_context_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "round or piece context not valid", K(ret), KPC(this));
  } else if (OB_UNLIKELY(! inner_piece_context_.is_active())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "piece state not match", K(ret), KPC(this));
  } else if (OB_FAIL(archive_store.init(archive_dest_))) {
    CLOG_LOG(WARN, "backup store init failed", K(ret), K_(archive_dest));
  } else if (OB_FAIL(archive_store.is_archive_log_file_exist(dest_id_, inner_piece_context_.round_id_,
          inner_piece_context_.piece_id_, id_, inner_piece_context_.max_file_id_ + 1, next_file_exist))) {
    CLOG_LOG(WARN, "check archive log file exist failed", K(ret));
  } else if (next_file_exist) {
    // advance inner_piece max_file_id
    inner_piece_context_.max_file_id_ += 1;
  } else if (OB_FAIL(archive_store.is_single_piece_file_exist(dest_id_, inner_piece_context_.round_id_,
          inner_piece_context_.piece_id_, piece_meta_exist))) {
  } else if (! piece_meta_exist) {
    // skip
  } else if (OB_FAIL(get_piece_meta_info_(inner_piece_context_.piece_id_))) {
    // single piece exist, piece is frozen
    CLOG_LOG(WARN, "get piece meta info failed", K(ret), KPC(this));
  }
  return ret;
}

int ObLogArchivePieceContext::forward_piece_()
{
  int ret = OB_SUCCESS;
  const int64_t piece_id = inner_piece_context_.piece_id_;
  if (inner_piece_context_.is_active()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "piece context state not match", K(ret), KPC(this));
  } else if (piece_id >= round_context_.max_piece_id_) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "piece id not smaller than max piece id, can not forward piece", K(ret), KPC(this));
  } else if (inner_piece_context_.round_id_ != round_context_.round_id_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "round id not match", K(ret), KPC(this));
  } else {
    inner_piece_context_.reset();
    inner_piece_context_.round_id_ = round_context_.round_id_;
    inner_piece_context_.piece_id_ = piece_id + 1;
    CLOG_LOG(INFO, "forward piece succ", K(ret), "pre_piece_id", piece_id, KPC(this));
  }
  return ret;
}

int ObLogArchivePieceContext::backward_piece_()
{
  int ret = OB_SUCCESS;
  const int64_t piece_id = inner_piece_context_.piece_id_;
  if (piece_id <= round_context_.min_piece_id_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "piece id not bigger than min piece id, can not backward piece", K(ret), KPC(this));
  } else if (inner_piece_context_.round_id_ != round_context_.round_id_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "round id not match", K(ret), KPC(this));
  } else {
    inner_piece_context_.reset();
    inner_piece_context_.round_id_ = round_context_.round_id_;
    inner_piece_context_.piece_id_ = piece_id - 1;
    CLOG_LOG(INFO, "backward piece succ", K(ret), "pre_piece_id", piece_id, KPC(this));
  }
  return ret;
}

int64_t ObLogArchivePieceContext::cal_piece_id_(const SCN &scn) const
{
  return share::ObArchivePiece(scn, round_context_.piece_switch_interval_,
      round_context_.base_piece_scn_, round_context_.base_piece_id_)
    .get_piece_id();
}

int ObLogArchivePieceContext::get_min_lsn_in_piece_()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_size = archive::ARCHIVE_FILE_HEADER_SIZE;
  const int64_t file_offset = 0;
  int64_t read_size = 0;
  palf::LSN base_lsn;
  if (inner_piece_context_.is_empty_()
      || inner_piece_context_.is_low_bound_()
      || inner_piece_context_.min_lsn_in_piece_.is_valid()) {
    // do nothing
  } else if (OB_ISNULL(buf = (char *)mtl_malloc(buf_size, "ArcFile"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc memory failed", K(ret));
  } else if (OB_FAIL(read_part_file_(inner_piece_context_.round_id_,
          inner_piece_context_.piece_id_, inner_piece_context_.min_file_id_,
          file_offset, buf, buf_size, read_size))) {
    CLOG_LOG(WARN, "read part file failed", K(ret));
  } else if (OB_FAIL(extract_file_base_lsn_(buf, read_size, base_lsn))) {
    CLOG_LOG(WARN, "extract base_lsn failed", KPC(this));
  } else {
    inner_piece_context_.min_lsn_in_piece_ = base_lsn;
    CLOG_LOG(INFO, "get min lsn in piece succ", K(ret), K(base_lsn), KPC(this));
  }
  if (NULL != buf) {
    mtl_free(buf);
    buf = NULL;
  }
  return ret;
}

int64_t ObLogArchivePieceContext::cal_archive_file_id_(const palf::LSN &lsn) const
{
  return archive::cal_archive_file_id(lsn, palf::PALF_BLOCK_SIZE);
}

int ObLogArchivePieceContext::get_(const palf::LSN &lsn,
    const int64_t file_id,
    int64_t &dest_id,
    int64_t &round_id,
    int64_t &piece_id,
    int64_t &offset,
    palf::LSN &max_lsn,
    bool &done,
    bool &to_newest)
{
  int ret = OB_SUCCESS;
  done = false;
  to_newest = false;
  if (! inner_piece_context_.is_valid()
      || inner_piece_context_.is_empty_()
      || inner_piece_context_.is_low_bound_()) {
    // skip
  } else if (inner_piece_context_.is_frozen_()) {
    if (inner_piece_context_.min_lsn_in_piece_ <= lsn && inner_piece_context_.max_lsn_in_piece_ > lsn) {
      done = true;
    }
  } else if (inner_piece_context_.is_gc_()) {
    if (inner_piece_context_.max_lsn_in_piece_ <= lsn) {
      done = false;
      ret = OB_ITER_END;
    } else if (inner_piece_context_.min_lsn_in_piece_ <= lsn && file_id <= inner_piece_context_.max_file_id_) {
      done = true;
    }
  } else {
    if (inner_piece_context_.min_lsn_in_piece_ <= lsn && file_id <= inner_piece_context_.max_file_id_) {
      done = true;
    }
  }

  if (done) {
    dest_id = dest_id_;
    round_id = inner_piece_context_.round_id_;
    piece_id = inner_piece_context_.piece_id_;
    if (inner_piece_context_.file_id_ == file_id && lsn >= inner_piece_context_.max_lsn_) {
      offset = inner_piece_context_.file_offset_;
      max_lsn = inner_piece_context_.max_lsn_;
    } else {
      offset = 0;
    }

    // check if to the newest file, if file_id is temporary the max file, advance piece and check
    if (round_context_.round_id_ == max_round_id_
        && inner_piece_context_.round_id_ == round_context_.round_id_
        && inner_piece_context_.piece_id_ == round_context_.max_piece_id_
        && inner_piece_context_.is_active()
        && inner_piece_context_.max_file_id_ == file_id) {
      if (OB_SUCCESS != advance_piece_()) {
        CLOG_LOG(WARN, "advance piece failed", K(ret));
      }
    }
    if (inner_piece_context_.is_active() && inner_piece_context_.max_file_id_ == file_id) {
      to_newest = true;
    }
  }

  // 已消费到最大依然没有定位到该LSN, 并且当前piece包含日志范围小于该LSN, 返回OB_ITER_END
  if (OB_SUCC(ret) && ! done) {
    if (inner_piece_context_.is_valid()
        && inner_piece_context_.round_id_ == max_round_id_
        && inner_piece_context_.piece_id_ == round_context_.max_piece_id_
        && inner_piece_context_.min_lsn_in_piece_ < lsn) {
      ret = OB_ITER_END;
    }
  }

  // 该日志流在该piece已GC, 并且最大LSN小于等于需要获取的LSN, 返回OB_ITER_END
  if (OB_SUCC(ret) && ! done) {
    if (inner_piece_context_.is_valid()
        && inner_piece_context_.is_gc_()
        && inner_piece_context_.max_lsn_in_piece_ <= lsn) {
      CLOG_LOG(INFO, "ls gc in this piece, and lsn bigger than ls max lsn, iter to end", K(lsn), KPC(this));
      ret = OB_ITER_END;
    }
  }

  // 该LSN日志已被回收
  // 最小round最小piece包含的最小lsn依然大于指定LSN
  if (! done) {
    if (inner_piece_context_.is_valid()
        && round_context_.is_valid()
        && inner_piece_context_.min_lsn_in_piece_.is_valid()
        && inner_piece_context_.piece_id_ == round_context_.min_piece_id_
        && inner_piece_context_.round_id_ == min_round_id_
        && inner_piece_context_.min_lsn_in_piece_ > lsn) {
      ret = OB_ARCHIVE_LOG_RECYCLED;
      CLOG_LOG(WARN, "lsn smaller than any log exist, maybe archive log had been recycled", K(lsn), KPC(this));
    }
  }
  return ret;
}

int ObLogArchivePieceContext::get_max_archive_log_(const ObLogArchivePieceContext &origin, palf::LSN &lsn, SCN &scn)
{
  int ret = OB_SUCCESS;
  bool done = false;
  int64_t round_id = max_round_id_;
  while (!done && OB_SUCC(ret) && round_id >= min_round_id_) {
    ret = get_max_log_in_round_(origin, round_id, lsn, scn, done);
    round_id--;
  }
  if (OB_SUCC(ret) && !done) {
    ret = OB_ENTRY_NOT_EXIST;
    ARCHIVE_LOG(WARN, "no log exist", K(ret), KPC(this));
  }
  return ret;
}

int ObLogArchivePieceContext::get_max_log_in_round_(const ObLogArchivePieceContext &origin,
    const int64_t round_id,
    palf::LSN &lsn,
    SCN &scn,
    bool &exist)
{
  int ret = OB_SUCCESS;
  round_context_.reset();
  inner_piece_context_.reset();
  round_context_.round_id_ = round_id;
  common::ObTimeGuard guard("get_max_log_in_piece_", 1000 * 1000L);
  if (OB_FAIL(load_round_info_())) {
    ARCHIVE_LOG(WARN, "load round info failed", K(ret), K_(id), K(round_id));
  } else if (! round_context_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "invalid round context", K(ret), K_(id), K_(round_context));
  } else if (round_context_.is_in_empty_state()) {
    ARCHIVE_LOG(INFO, "round is empty, just skip", K(ret), K_(id), K_(round_context));
  } else if (round_context_.is_in_active_state() && round_context_.max_piece_id_ == 0) {
    ARCHIVE_LOG(INFO, "no piece exist, just skip", K(ret), K_(id), K_(round_context));
  } else {
    guard.click("load_round_info");
    int64_t piece_id = round_context_.max_piece_id_;
    while (!exist && OB_SUCC(ret) && piece_id >= round_context_.min_piece_id_) {
      ret = get_max_log_in_piece_(origin, round_id, piece_id, lsn, scn, exist);
      piece_id--;
    }
    guard.click("get_max_log_in_pieces");
  }
  return ret;
}

int ObLogArchivePieceContext::get_max_log_in_piece_(const ObLogArchivePieceContext &origin,
    const int64_t round_id,
    const int64_t piece_id,
    palf::LSN &lsn,
    SCN &scn,
    bool &exist)
{
  int ret = OB_SUCCESS;
  inner_piece_context_.reset();
  inner_piece_context_.round_id_ = round_id;
  inner_piece_context_.piece_id_ = piece_id;
  common::ObTimeGuard guard("get_max_log_in_piece_", 1000 * 1000L);
  if (OB_FAIL(get_piece_meta_info_(piece_id))) {
    ARCHIVE_LOG(WARN, "get piece meta info failed", K(ret), K_(id), K_(round_context), K(piece_id));
  } else if (OB_FAIL(get_piece_file_range_())) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      exist = false;
      // no file exist in this piece, return OB_SUCCESS
    } else {
      ARCHIVE_LOG(WARN, "get piece file range failed", K(ret));
    }
  } else if (inner_piece_context_.is_empty_() || inner_piece_context_.max_file_id_ == 0) {
    ARCHIVE_LOG(INFO, "no file exist in piece, just skip", K(ret), K_(id), K_(round_context), K_(inner_piece_context));
  } else {
    guard.click("get_piece_meta_info");
    ret = get_max_log_in_file_(origin, round_id, piece_id, inner_piece_context_.max_file_id_, lsn, scn, exist);
  }
  return ret;
}

int ObLogArchivePieceContext::get_max_log_in_file_(const ObLogArchivePieceContext &origin,
    const int64_t round_id,
    const int64_t piece_id,
    const int64_t file_id,
    palf::LSN &lsn,
    SCN &scn,
    bool &exist)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_size = archive::ARCHIVE_FILE_DATA_BUF_SIZE;
  const int64_t header_size = archive::ARCHIVE_FILE_HEADER_SIZE;
  int64_t read_size = 0;
  palf::MemoryStorage mem_storage;
  palf::MemPalfGroupBufferIterator iter;

  // if get max log context match origin piece context
  // only need to read data not restored
  const bool context_match = (dest_id_ == origin.dest_id_
      && round_id == origin.round_context_.round_id_
      && round_id == origin.inner_piece_context_.round_id_
      && piece_id == origin.inner_piece_context_.piece_id_
      && file_id == origin.inner_piece_context_.file_id_
      && origin.inner_piece_context_.file_offset_ > 0);
  const int64_t file_offset = context_match ? origin.inner_piece_context_.file_offset_ : 0;
  // if context match, use max_lsn in origin piece context
  palf::LSN base_lsn = context_match ? origin.inner_piece_context_.max_lsn_ : palf::LSN(palf::LOG_INVALID_LSN_VAL);
  common::ObTimeGuard guard("get_max_log_in_file", 1000 * 1000L);

  if (OB_ISNULL(buf = (char *)mtl_malloc(buf_size, "ArcFile"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc memory failed", K(ret), K_(id));
  } else if (OB_FAIL(read_part_file_(round_id, piece_id, file_id,
          file_offset, buf, buf_size, read_size))) {
    CLOG_LOG(WARN, "read part file failed", K(ret), K_(id));
  } else if (0 == read_size && context_match) {
    lsn = base_lsn;
    exist = true;
    CLOG_LOG(INFO, "origin piece context match and no more archive log exists to restore",
        K(context_match), K(base_lsn), K(origin), KPC(this));
  } else if (!context_match && OB_FAIL(extract_file_base_lsn_(buf, buf_size, base_lsn))) {
    // if contex not match, extract base_lsn in the archive file
    CLOG_LOG(WARN, "extract base_lsn failed", KPC(this));
  } else {
    guard.click("read_data");
    const char *log_buf = context_match ? buf : buf + header_size;
    const int64_t log_buf_size = context_match ? read_size : read_size - header_size;
    if (OB_FAIL(mem_storage.init(base_lsn))) {
      CLOG_LOG(WARN, "MemoryStorage init failed", K(ret), K(base_lsn), KPC(this));
    } else if (OB_FAIL(mem_storage.append(log_buf, log_buf_size))) {
      CLOG_LOG(WARN, "MemoryStorage append failed", K(log_buf), K(log_buf_size),
          K(context_match), K(file_id), K(file_offset), K(id_));
    } else if (OB_FAIL(iter.init(base_lsn, [](){ return palf::LSN(palf::LOG_MAX_LSN_VAL); }, &mem_storage))) {
      CLOG_LOG(WARN, "iter init failed", K(id_), K(base_lsn), K(log_buf), K(log_buf_size));
    } else {
      palf::LogGroupEntry entry;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter.next())) {
          if (OB_ITER_END != ret) {
            CLOG_LOG(WARN, "iter next failed", K(ret), KPC(this), K(iter));
          }
        } else if (OB_FAIL(iter.get_entry(entry, lsn))) {
          CLOG_LOG(WARN, "get entry failed", K(ret));
        } else if (! entry.check_integrity()) {
          ret = OB_INVALID_DATA;
          CLOG_LOG(WARN, "invalid data", K(ret), KPC(this), K(iter), K(entry));
        } else {
          lsn = lsn + entry.get_serialize_size();
          scn = entry.get_scn();
          exist = true;
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (exist) {
          CLOG_LOG(INFO, "get max archive log through log iteration from archive log", K(context_match),
              K(lsn), K(scn), K(base_lsn), K(origin), KPC(this));
        }
      }
      guard.click("iterate_log");
    }
  }
  if (NULL != buf) {
    mtl_free(buf);
    buf = NULL;
  }
  return ret;
}

int ObLogArchivePieceContext::seek_(const SCN &scn, palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  int64_t piece_id = 0;
  reset_locate_info();
  if (OB_FAIL(get_round_range_())) {
    CLOG_LOG(WARN, "get round range failed", K(ret), K_(id));
  } else if (OB_FAIL(get_round_(scn))) {
    // get the first round in which round_max_ts not smaller than scn
    CLOG_LOG(WARN, "get round failed", K(ret), KPC(this));
  } else if (OB_FAIL(load_round_info_())) {
    CLOG_LOG(WARN, "locate round failed", K(ret));
  } else if (OB_UNLIKELY(scn < round_context_.start_scn_)) {
    ret = OB_ENTRY_NOT_EXIST;
    CLOG_LOG(WARN, "scn smaller than round_start_ts", K(ret), K(scn), KPC(this));
  } else if (FALSE_IT(piece_id = cal_piece_id_(scn))) {
  } else if (OB_UNLIKELY(piece_id < round_context_.min_piece_id_ || piece_id > round_context_.max_piece_id_)) {
    ret = OB_ENTRY_NOT_EXIST;
    CLOG_LOG(WARN, "piece id not in cur round, entry not exist", K(ret), K(scn), K(piece_id), KPC(this));
  } else if (OB_FAIL(get_cur_piece_info_(scn))) {
    CLOG_LOG(WARN, "get cur piece info failed", K(ret), KPC(this));
  } else if (OB_UNLIKELY(inner_piece_context_.is_empty_())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "inner_piece_context is empty, unexpected", K(ret), K(scn), KPC(this));
  } else {
    ret = seek_in_piece_(scn, lsn);
  }
  return ret;
}

int ObLogArchivePieceContext::seek_in_piece_(const SCN &scn, palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  int64_t file_id = 0;
  if (OB_FAIL(archive::ObArchiveFileUtils::locate_file_by_scn_in_piece(archive_dest_,
          dest_id_, inner_piece_context_.round_id_, inner_piece_context_.piece_id_,
          id_, inner_piece_context_.min_file_id_,
          inner_piece_context_.max_file_id_, scn, file_id))) {
    CLOG_LOG(WARN, "locate file failed", K(ret), KPC(this));
  } else if (OB_FAIL(seek_in_file_(file_id, scn, lsn))) {
    CLOG_LOG(WARN, "seek in file failed", K(ret), K_(id), K(file_id));
  }
  return ret;
}

int ObLogArchivePieceContext::seek_in_file_(const int64_t file_id, const SCN &scn, palf::LSN &out_lsn)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_size = archive::ARCHIVE_FILE_DATA_BUF_SIZE;
  const int64_t header_size = archive::ARCHIVE_FILE_HEADER_SIZE;
  const int64_t file_offset = 0;
  int64_t read_size = 0;
  palf::LSN base_lsn;
  palf::MemoryStorage mem_storage;
  palf::MemPalfGroupBufferIterator iter;
  if (OB_ISNULL(buf = (char *)mtl_malloc(buf_size, "ArcFile"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc memory failed", K(ret));
  } else if (OB_FAIL(read_part_file_(inner_piece_context_.round_id_,
          inner_piece_context_.piece_id_, file_id, file_offset, buf, buf_size, read_size))) {
    CLOG_LOG(WARN, "read part file failed", K(ret), K(file_id), KPC(this));
  } else if (OB_FAIL(extract_file_base_lsn_(buf, buf_size, base_lsn))) {
    CLOG_LOG(WARN, "extract base_lsn failed", KPC(this));
  } else if (OB_FAIL(mem_storage.init(base_lsn))) {
    CLOG_LOG(WARN, "MemoryStorage init failed", K(ret), K_(id), K(base_lsn), KPC(this));
  } else if (OB_FAIL(mem_storage.append(buf + header_size, read_size - header_size))) {
    CLOG_LOG(WARN, "MemoryStorage append failed", K(ret));
  } else if (OB_FAIL(iter.init(base_lsn, [](){ return palf::LSN(palf::LOG_MAX_LSN_VAL); }, &mem_storage))) {
    CLOG_LOG(WARN, "iter init failed", K(ret));
  } else {
    palf::LogGroupEntry entry;
    palf::LSN lsn;
    out_lsn = base_lsn;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(WARN, "iter next failed", K(ret), KPC(this), K(iter));
        }
      } else if (OB_FAIL(iter.get_entry(entry, lsn))) {
        CLOG_LOG(WARN, "get entry failed", K(ret));
      } else if (! entry.check_integrity()) {
        ret = OB_INVALID_DATA;
        CLOG_LOG(WARN, "invalid data", K(ret), KPC(this), K(iter), K(entry));
      } else if (entry.get_scn() < scn) {
        out_lsn = lsn + entry.get_serialize_size();
        CLOG_LOG(TRACE, "entry log_ts smaller than scn", K(ret),
            K(scn), K(out_lsn), K(lsn), K(entry));
      } else if (entry.get_scn() == scn) {
        CLOG_LOG(INFO, "entry log_ts equal to scn", K(ret),
            K(scn), K(out_lsn), K(lsn), K(entry));
      } else {
        CLOG_LOG(INFO, "entry log_ts bigger than scn, just skip", K(ret),
            K(scn), K(out_lsn), K(lsn), K(entry));
        break;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      CLOG_LOG(INFO, "all log_ts in cur file smaller than scn, seek lsn in next file",
          K(ret), K(scn), K(out_lsn), K(lsn), K(entry));
    }
  }
  if (NULL != buf) {
    mtl_free(buf);
    buf = NULL;
  }
  return ret;
}

int ObLogArchivePieceContext::read_part_file_(const int64_t round_id,
    const int64_t piece_id,
    const int64_t file_id,
    const int64_t file_offset,
    char *buf,
    const int64_t buf_size,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  int64_t pos = 0;
  archive::ObArchiveFileHeader header;
  if (OB_FAIL(share::ObArchivePathUtil::get_ls_archive_file_path(archive_dest_, dest_id_,
          round_id, piece_id, id_, file_id, path))) {
    CLOG_LOG(WARN, "get ls archive file path failed", K(ret), KPC(this));
  } else if (OB_FAIL(archive::ObArchiveFileUtils::range_read(path.get_ptr(),
          archive_dest_.get_storage_info(), buf, buf_size, file_offset, read_size))) {
    CLOG_LOG(WARN, "range read failed", K(ret), K(path));
  }
  return ret;
}

int ObLogArchivePieceContext::extract_file_base_lsn_(const char *buf,
    const int64_t buf_size,
    palf::LSN &base_lsn)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  archive::ObArchiveFileHeader header;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size < archive::ARCHIVE_FILE_HEADER_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid buffer", K(buf), K(buf_size), K(id_));
  } else if (OB_FAIL(header.deserialize(buf, buf_size, pos))) {
    CLOG_LOG(WARN, "archive file header deserialize failed", K(buf), K(buf_size));
  } else if (OB_UNLIKELY(! header.is_valid())) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(WARN, "archive file header not valid", K(header), K(buf), K(buf_size));
  } else {
    base_lsn = palf::LSN(header.start_lsn_);
  }
  return ret;
}

int ObLogArchivePieceContext::get_ls_meta_data_(
    const share::ObArchiveLSMetaType &meta_type,
    const SCN &timestamp,
    const bool fuzzy_match,
    char *buf,
    const int64_t buf_size,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int64_t piece_id = 0;
  reset_locate_info();
  if (OB_FAIL(get_round_range_())) {
    CLOG_LOG(WARN, "get round range failed", K(ret), K_(id));
  } else if (OB_FAIL(get_round_(timestamp))) {
    CLOG_LOG(WARN, "get round failed", K(ret), KPC(this));
  } else if (OB_FAIL(load_round_info_())) {
    CLOG_LOG(WARN, "locate round failed", K(ret));
  } else if (FALSE_IT(piece_id = cal_piece_id_(timestamp))) {
  } else {
    piece_id = min(round_context_.max_piece_id_, piece_id);
    ret = get_ls_meta_in_piece_(meta_type, timestamp, fuzzy_match, piece_id, buf, buf_size, read_size);
  }
  return ret;
}

int ObLogArchivePieceContext::get_ls_meta_in_piece_(
    const share::ObArchiveLSMetaType &meta_type,
    const SCN &timestamp,
    const bool fuzzy_match,
    const int64_t base_piece_id,
    char *buf,
    const int64_t buf_size,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  bool done = false;
  int64_t piece_id = base_piece_id;
  int64_t file_id = 0;
  share::ObBackupPath prefix;
  ObArray<int64_t> array;
  for (; OB_SUCC(ret) && ! done && piece_id >= round_context_.min_piece_id_; piece_id--) {
    array.reset();
    if (OB_FAIL(share::ObArchivePathUtil::get_ls_meta_record_prefix(archive_dest_, dest_id_,
            round_context_.round_id_, piece_id, id_, meta_type, prefix))) {
      CLOG_LOG(WARN, "ger ls meta record prefix failed", K(ret), KPC(this));
    } else if (archive::ObArchiveFileUtils::list_files(prefix.get_obstr(),
          archive_dest_.get_storage_info(), array)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        CLOG_LOG(INFO, "no file exist, need backward", K(ret), K(prefix));
        ret = OB_SUCCESS;
      } else {
        CLOG_LOG(WARN, "list_files failed", K(ret), K(prefix));
      }
    } else if (OB_UNLIKELY(array.empty())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "list_files array is empty", K(ret), K(prefix));
    } else if (OB_FAIL(get_ls_meta_file_in_array_(timestamp, fuzzy_match, file_id, array))) {
      // only in precise match mode, OB_ENTRY_NOT_EXIST can return
      if (OB_ENTRY_NOT_EXIST != ret) {
        CLOG_LOG(WARN, "faild to get_ls_meta_file_in_array_", K(ret), K(prefix), K_(id), K(timestamp), K(array));
      }
    } else {
      done = true;
      break;
    }
  }

  if (done && OB_SUCC(ret)) {
    share::ObBackupPath path;
    if (OB_FAIL(share::ObArchivePathUtil::get_ls_meta_record_path(archive_dest_, dest_id_,
            round_context_.round_id_, piece_id, id_, meta_type, file_id, path))) {
      CLOG_LOG(WARN, "ger ls meta record prefix failed", K(ret), KPC(this));
    } else if (OB_FAIL(archive::ObArchiveFileUtils::read_file(path.get_obstr(),
            archive_dest_.get_storage_info(), buf, buf_size, read_size))) {
      CLOG_LOG(WARN, "read_file failed", K(ret), K(path), KPC(this));
    }
  }

  if (! done && OB_SUCCESS == ret) {
    ret = OB_ENTRY_NOT_EXIST;
    CLOG_LOG(WARN, "no match ls meta file exist", K(ret), K(timestamp), KPC(this));
  }
  return ret;
}

int ObLogArchivePieceContext::get_ls_meta_file_in_array_(const SCN &timestamp,
    const bool fuzzy_match,
    int64_t &file_id,
    common::ObIArray<int64_t> &array)
{
  int ret = OB_SUCCESS;
  bool done = false;
  for (int64_t i = 0; i < array.count(); i++) {
    const int64_t tmp_file_id = array.at(i);
    SCN tmp_scn;
    if (OB_UNLIKELY(tmp_file_id < 0)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "invalid tmp_file_id", K(ret), K(timestamp), K(file_id));
    } else if (OB_FAIL(tmp_scn.convert_for_logservice(tmp_file_id))) {
      CLOG_LOG(ERROR, "failed to convert_for_logservice", K(ret), K(timestamp), K(tmp_file_id));
    } else if (tmp_scn > timestamp) {
      if (0 == i) {
        CLOG_LOG(INFO, "all file bigger than timestamp, need backward", K(ret), K(id_), K(array), K(timestamp));
      }
      break;
    } else if (tmp_scn == timestamp) {
      file_id = tmp_file_id;
      done = true;
      break;
    } else if (fuzzy_match) {
      file_id = tmp_file_id;
      done = true;
    }
  }
  if (! done) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}
} // namespace logservice
} // namespace oceanbase
