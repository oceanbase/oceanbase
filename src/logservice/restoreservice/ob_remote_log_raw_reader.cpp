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

#include "ob_remote_log_raw_reader.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/restore/ob_storage_info.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_backup_struct.h"      // ObBackupPath
#include "logservice/palf/log_define.h"
#include "logservice/ob_log_external_storage_handler.h"     // ObLogExternalStorageHandler
#include "ob_log_archive_piece_mgr.h"

namespace oceanbase
{
namespace logservice
{
ObRemoteLogRawReader::ObRemoteLogRawReader(GetSourceFunc &get_source_func,
    UpdateSourceFunc &update_source_func,
    RefreshStorageInfoFunc &refresh_storage_info_func) :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  id_(),
  pre_scn_(),
  start_lsn_(),
  cur_lsn_(),
  max_lsn_(),
  log_ext_handler_(NULL),
  source_guard_(),
  get_source_func_(get_source_func),
  update_source_func_(update_source_func),
  refresh_storage_info_func_(refresh_storage_info_func)
{}

ObRemoteLogRawReader::~ObRemoteLogRawReader()
{
  destroy();
}

int ObRemoteLogRawReader::init(const uint64_t tenant_id,
    const share::ObLSID &id,
    const share::SCN &pre_scn,
    logservice::ObLogExternalStorageHandler *log_ext_handler)
{
  int ret = OB_SUCCESS;
  ObRemoteLogParent *source = NULL;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObRemoteLogRawReader init twice", K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
        || !id.is_valid()
        || !pre_scn.is_valid()
        || NULL == log_ext_handler)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(id), K(pre_scn), K(log_ext_handler));
  } else if (OB_FAIL(get_source_func_(id, source_guard_))) {
    CLOG_LOG(WARN, "get source failed", K(ret), K(id));
  } else if (OB_ISNULL(source = source_guard_.get_source())) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "source is NULL", K(ret), K(id));
  } else if (OB_UNLIKELY(! share::is_location_log_source_type(source->get_source_type()))) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "source type not support", K(ret), K(id), KPC(source));
  } else {
    tenant_id_ = tenant_id;
    id_ = id;
    pre_scn_ = pre_scn;
    log_ext_handler_ = log_ext_handler;
    inited_ = true;
    CLOG_LOG(INFO, "ObRemoteLogRawReader init succ", K(id_), K(tenant_id_), K(pre_scn_), K(inited_));
  }
  return ret;
}

void ObRemoteLogRawReader::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  id_.reset();
  pre_scn_.reset();
  start_lsn_.reset();
  cur_lsn_.reset();
  max_lsn_.reset();
  log_ext_handler_ = NULL;
}

int ObRemoteLogRawReader::raw_read(const palf::LSN &start_lsn,
    void *buffer,
    const int64_t nbytes,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObRemoteLogRawReader not init", KPC(this));
  } else if (OB_UNLIKELY(!start_lsn.is_valid()
        || nbytes <= 0
        || NULL == buffer)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(start_lsn), K(nbytes));
  } else {
    start_lsn_ = start_lsn;
    cur_lsn_ = start_lsn;
    max_lsn_ = start_lsn_ + nbytes;
    ret = raw_read_((char*)buffer, nbytes, read_size);
  }
  return ret;
}

int ObRemoteLogRawReader::raw_read_(char *buffer, const int64_t buffer_size, int64_t &total_read_size)
{
  int ret = OB_SUCCESS;
  total_read_size = 0;
  int READ_TIMES = 0;
  while (total_read_size < max_lsn_ - start_lsn_ && OB_SUCC(ret)) {
    int64_t read_size = 0;
    if (OB_FAIL(read_once_(buffer + total_read_size, buffer_size - total_read_size, read_size))) {
      CLOG_LOG(WARN, "raw read failed", K(id_), K(total_read_size));
    } else if (read_size == 0) {
      break;
    } else {
      READ_TIMES++;
      total_read_size += read_size;
      cur_lsn_ = cur_lsn_ + read_size;
      CLOG_LOG(INFO, "RAW READ TIMES", K(READ_TIMES), K(read_size), K(total_read_size),
          K(buffer_size), K(start_lsn_), K(cur_lsn_), K(id_));
    }
  }

  // no enough log to read, just return total_read_size
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  // no data read after sweep archive
  if (OB_SUCC(ret) && 0 == total_read_size) {
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
  }
  return ret;
}

int ObRemoteLogRawReader::read_once_(char *buffer, const int64_t buffer_size, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int64_t dest_id = -1;
  int64_t round_id = -1;
  int64_t piece_id = -1;
  int64_t file_id = -1;
  int64_t file_offset = -1;
  share::ObBackupPath path;
  share::SCN unused_scn;
  share::ObBackupDest *dest = NULL;
  ObLogArchivePieceContext *piece_context = NULL;
  common::ObObjectStorageInfo storage_info_base;
  char storage_info_cstr[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {'\0'};
  ObRemoteLocationParent *source = static_cast<ObRemoteLocationParent*>(source_guard_.get_source());

  if (OB_ISNULL(source)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "source is NULL", K(source));
  } else if (FALSE_IT(source->get(dest, piece_context, unused_scn))) {
  } else if (OB_ISNULL(dest) || OB_ISNULL(piece_context)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "dest or piece_context is NULL", KPC(source), K(dest), K(piece_context));
  } else if (OB_FAIL(piece_context->get_raw_read_piece(pre_scn_, cur_lsn_, dest_id,
          round_id, piece_id, file_id, file_offset))) {
    CLOG_LOG(WARN, "get piece failed", KPC(this), KPC(piece_context));
  } else if (OB_UNLIKELY(!piece_context->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "piece_context not valid", KPC(this), KPC(piece_context));
  } else if (OB_FAIL(share::ObArchivePathUtil::get_ls_archive_file_path(
          *dest, dest_id, round_id, piece_id, id_, file_id, path))) {
    CLOG_LOG(WARN, "get ls archive file path failed", KPC(dest),
        K(dest_id), K(round_id), K(piece_id), K(file_id), K(id_));
  } else if (OB_FAIL(storage_info_base.assign(*dest->get_storage_info()))) {
    CLOG_LOG(WARN, "get storage_info failed", KPC(dest));
  } else if (OB_FAIL(storage_info_base.get_storage_info_str(
          storage_info_cstr, OB_MAX_BACKUP_STORAGE_INFO_LENGTH))) {
    CLOG_LOG(WARN, "get storage_info str failed", K(id_));
  } else {
    ObString uri(path.get_obstr());
    ObString storage_info(storage_info_cstr);
    if (OB_FAIL(log_ext_handler_->pread(uri, storage_info, file_offset,
            buffer, buffer_size, read_size))) {
      if (OB_FILE_LENGTH_INVALID == ret) {
        ret = OB_SUCCESS;
        read_size = 0;
      } else {
        CLOG_LOG(WARN, "read file failed", K(ret), K(uri), K(storage_info));
      }
    }

    if (OB_SUCC(ret) && 0 == read_size) {
      CLOG_LOG(INFO, "no data read", K(id_), K(cur_lsn_), KPC(piece_context));
    }
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
