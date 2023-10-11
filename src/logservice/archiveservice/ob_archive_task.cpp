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

#include "ob_archive_task.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_archive_define.h"
#include <cstdint>

namespace oceanbase
{
using namespace palf;
using namespace share;
namespace archive
{

ObArchiveLogFetchTask::ObArchiveLogFetchTask() :
  tenant_id_(OB_INVALID_TENANT_ID),
  id_(),
  station_(),
  cur_piece_(),
  next_piece_(),
  start_offset_(),
  end_offset_(),
  send_task_(NULL)
{
  max_scn_ = SCN::min_scn();
}

ObArchiveLogFetchTask::~ObArchiveLogFetchTask()
{
  if (NULL != send_task_) {
    ARCHIVE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "residual send task exist, maybe memory leak", KPC(this));
    send_task_ = NULL;
  }
  id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  station_.reset();
  cur_piece_.reset();
  next_piece_.reset();
  start_offset_.reset();
  end_offset_.reset();
  max_scn_.reset();
}

int ObArchiveLogFetchTask::init(const uint64_t tenant_id,
                                const ObLSID &id,
                                const ArchiveWorkStation &station,
                                const share::SCN &base_scn,
                                const LSN &start_lsn,
                                const LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
        || !id.is_valid()
        || !station.is_valid()
        || !base_scn.is_valid()
        || !start_lsn.is_valid()
        || ! end_lsn.is_valid()
        || end_lsn <= start_lsn)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(tenant_id),
        K(id), K(station), K(base_scn), K(start_lsn), K(end_lsn));
  } else {
    tenant_id_ = tenant_id;
    id_ = id;
    station_ = station;
    base_scn_ = base_scn;
    start_offset_ = start_lsn;
    cur_offset_ = start_lsn;
    end_offset_ = end_lsn;
  }
  return ret;
}

bool ObArchiveLogFetchTask::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
    && id_.is_valid()
    && station_.is_valid()
    && start_offset_.is_valid()
    && end_offset_ > start_offset_;
}

bool ObArchiveLogFetchTask::is_finish() const
{
  return end_offset_ == cur_offset_;
}

int ObArchiveLogFetchTask::clear_send_task()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(send_task_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "send task is NULL", KPC(this));
  } else {
    send_task_ = NULL;
  }
  return ret;
}

int ObArchiveLogFetchTask::back_fill(const ObArchivePiece &cur_piece,
    const LSN &start_offset,
    const LSN &end_offset,
    const SCN &max_scn,
    ObArchiveSendTask *send_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! cur_piece.is_valid()
        || !max_scn.is_valid()
        || (start_offset != cur_offset_ && cur_piece_ == cur_piece)
        || end_offset > end_offset_
        || max_scn < max_scn_)
      || OB_ISNULL(send_task)
      || OB_UNLIKELY(! send_task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(cur_piece), K(start_offset),
        K(end_offset), K(max_scn), KPC(send_task), KPC(this));
  } else {
    cur_piece_ = cur_piece;
    next_piece_.reset();
    cur_offset_ = end_offset;
    max_scn_ = max_scn;
    send_task_ = send_task;
    ARCHIVE_LOG(INFO, "print back fill task succ", K(cur_piece), K(start_offset),
        K(end_offset), K(max_scn), KPC(this));
  }
  return ret;
}

int ObArchiveLogFetchTask::set_next_piece(const ObArchivePiece &piece)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! piece.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(piece), KPC(this));
  } else {
    next_piece_ = piece;
    ARCHIVE_LOG(INFO, "set next piece", KPC(this));
  }
  return ret;
}

bool ObArchiveLogFetchTask::is_continuous_with(const LSN &lsn) const
{
  return (end_offset_ >lsn && start_offset_ <= lsn)
    || start_offset_ == lsn;
}

ObArchiveSendTask::ObArchiveSendTask() :
  status_(INITAL_STATUS),
  tenant_id_(OB_INVALID_TENANT_ID),
  id_(),
  station_(),
  piece_(),
  start_offset_(),
  end_offset_(),
  max_scn_(),
  file_id_(OB_INVALID_ARCHIVE_FILE_ID),
  file_offset_(OB_INVALID_ARCHIVE_FILE_OFFSET),
  data_(NULL),
  data_len_(0)
{}

ObArchiveSendTask::~ObArchiveSendTask()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  id_.reset();
  station_.reset();
  piece_.reset();
  start_offset_.reset();
  end_offset_.reset();
  max_scn_.reset();
  data_ = NULL;
  data_len_ = 0;
}

int ObArchiveSendTask::init(const uint64_t tenant_id,
                            const ObLSID &id,
                            const ArchiveWorkStation &station,
                            const ObArchivePiece &piece,
                            const LSN &start_offset,
                            const LSN &end_offset,
                            const SCN &max_scn)

{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
        || !id.is_valid()
        || !station.is_valid()
        || !piece.is_valid()
        || !start_offset.is_valid()
        || end_offset < start_offset
        || !max_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(id), K(station), K(piece),
        K(start_offset), K(end_offset), K(max_scn));
  } else {
    tenant_id_ = tenant_id;
    id_ = id;
    station_ = station;
    piece_ = piece;
    start_offset_ = start_offset;
    end_offset_ = end_offset;
    max_scn_ = max_scn;
  }
  return ret;
}

bool ObArchiveSendTask::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
    && id_.is_valid()
    && station_.is_valid()
    && piece_.is_valid()
    && start_offset_.is_valid()
    && end_offset_ > start_offset_
    && max_scn_.is_valid()
    && NULL != data_
    && data_len_ > 0;
}

int ObArchiveSendTask::get_buffer(char *&data, int64_t &data_len) const
{
  data = data_;
  data_len = data_len_;
  return OB_SUCCESS;
}

int ObArchiveSendTask::get_origin_buffer(char *&buf, int64_t &buf_size) const
{
  buf = data_ - ARCHIVE_FILE_HEADER_SIZE;
  buf_size = data_len_ + ARCHIVE_FILE_HEADER_SIZE;
  return OB_SUCCESS;
}

int ObArchiveSendTask::set_buffer(char *buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(buf), K(buf_size));
  } else {
    data_ = buf;
    data_len_ = buf_size;
  }
  return ret;
}

bool ObArchiveSendTask::is_continuous_with(const ObArchiveSendTask &pre_task) const
{
  bool bret = true;
  if (! is_valid() || ! pre_task.is_valid()) {
    bret = false;
  } else if (tenant_id_ != pre_task.tenant_id_
      || id_ != pre_task.id_
      || station_ != pre_task.station_
      || piece_ != pre_task.piece_
      || start_offset_ != pre_task.end_offset_) {
    bret = false;
  }
  return bret;
}

bool ObArchiveSendTask::issue_task()
{
  bool bret = true;
  int8_t old_flag = ATOMIC_LOAD(&status_);
  const int8_t new_flag = ISSUE_STATUS;
  if (old_flag != INITAL_STATUS) {
    bret = false;
  } else if (!ATOMIC_BCAS(&status_, old_flag, new_flag)) {
    bret = false;
  }
  return bret;
}

bool ObArchiveSendTask::finish_task()
{
  bool bret = true;
  int8_t old_flag = ATOMIC_LOAD(&status_);
  const int8_t new_flag = FINISH_STATUS;
  if (old_flag != ISSUE_STATUS) {
    bret = false;
  } else if (!ATOMIC_BCAS(&status_, old_flag, new_flag)) {
    bret = false;
  }
  return bret;
}

bool ObArchiveSendTask::retire_task_with_retry()
{
  bool bret = true;
  int8_t old_flag = ATOMIC_LOAD(&status_);
  const int8_t new_flag = INITAL_STATUS;
  if (old_flag != ISSUE_STATUS) {
    bret = false;
  } else if (!ATOMIC_BCAS(&status_, old_flag, new_flag)) {
    bret = false;
  }
  return bret;
}

void ObArchiveSendTask::mark_stale()
{
  ATOMIC_STORE(&status_, STALE_STATUS);
}

bool ObArchiveSendTask::is_task_finish() const
{
  return FINISH_STATUS == ATOMIC_LOAD(&status_);
}

bool ObArchiveSendTask::is_task_stale() const
{
  return STALE_STATUS == ATOMIC_LOAD(&status_);
}

int ObArchiveSendTask::update_file(const int64_t file_id, const int64_t file_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ARCHIVE_FILE_ID == file_id
        || OB_INVALID_ARCHIVE_FILE_OFFSET == file_offset)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(file_id), K(file_offset), KPC(this));
  } else {
    file_id_ = file_id;
    file_offset_ = file_offset;
  }
  return ret;
}

void ObArchiveSendTask::get_file(int64_t &file_id, int64_t &file_offset)
{
  file_id = file_id_;
  file_offset = file_offset_;
}

} // namespace archive
} // namespace oceanbase
