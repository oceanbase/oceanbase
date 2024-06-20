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

#include "ob_ls_meta_recorder.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"             // ObTimeUtility
#include "lib/utility/ob_macro_utils.h"
#include "ob_archive_service.h"                   // ObArchiveService
#include "share/backup/ob_archive_piece.h"        // ObArchivePiece
#include "ob_archive_define.h"
#include "ob_archive_round_mgr.h"                 // ObArchiveRoundMgr
#include "ob_archive_file_utils.h"
#include "ob_ls_meta_record_task.h"               // *Task
#include "share/backup/ob_archive_path.h"         // get.*path
#include "share/rc/ob_tenant_base.h"              // mtl_

#define ADD_LS_RECORD_TASK(CLASS, type) \
{ \
  int ret = common::OB_SUCCESS;   \
  common::ObArray<share::ObLSID> array;   \
  share::ObArchiveLSMetaType task_type(type);  \
  CLASS t(task_type); \
  const int64_t interval = t.get_record_interval(); \
  RecordContext record_context;    \
  ArchiveKey key;                  \
  share::ObArchiveRoundState state;     \
  share::SCN archive_start_scn;         \
  round_mgr_->get_archive_round_info(key, state);     \
  if (! state.is_doing()) {    \
    ARCHIVE_LOG(TRACE, "not in doing state, just skip", K(key), K(state));   \
  } else if (OB_FAIL(round_mgr_->get_archive_start_scn(key, archive_start_scn))) {      \
    ARCHIVE_LOG(WARN, "get archive start scn failed", K(ret), K(key));                  \
  } else if (OB_UNLIKELY(! t.is_valid())) {   \
    ret = common::OB_ERR_UNEXPECTED;   \
    ARCHIVE_LOG(WARN, "invalid task", K(ret), K(task_type));   \
  } else if (OB_FAIL(check_and_get_record_context_(t.get_type(), record_context))   \
      && common::OB_ENTRY_NOT_EXIST != ret) {   \
    ARCHIVE_LOG(WARN, "check and get record failed", K(ret));    \
  } else if (common::OB_SUCCESS == ret   \
      && record_context.last_record_round_ == key.round_ \
      && common::ObTimeUtility::current_time_ns() - record_context.last_record_scn_.convert_to_ts()*1000L < interval) {   \
  } else if (OB_FAIL(t.get_ls_array(array))) {   \
    ARCHIVE_LOG(WARN, "get ls array failed", K(ret), K(task_type)); \
  } else { \
    for (int64_t i = 0; i < array.count(); i++)  \
    { \
      int64_t real_size = 0;                    \
      share::SCN scn;                          \
      share::ObBackupPath path;                 \
      const share::ObLSID &id = array.at(i);    \
      if (OB_FAIL(t.get_data(id, archive_start_scn, buf_ + COMMON_HEADER_SIZE, MAX_META_RECORD_DATA_SIZE, real_size, scn))) {  \
        ARCHIVE_LOG(WARN, "get data failed", K(ret));      \
      } else if (OB_UNLIKELY(! scn.is_valid())) {      \
        ARCHIVE_LOG(WARN, "scn is invalid", K(ret), K(task_type), K(scn));                  \
      } else if (record_context.last_record_file_ == scn.get_val_for_logservice()) {      \
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {    \
          ARCHIVE_LOG(INFO, "ls meta not refresh, no need record", K(type), K(record_context), K(scn));      \
        }                                                 \
      } else if (check_need_delay_(id, key, scn)) {    \
        ARCHIVE_LOG(INFO, "check_need_delay_ return true, just wait", K(id), K(task_type), K(scn)); \
      } else if (OB_FAIL(make_dir_(id, key, scn, task_type))) {   \
        ARCHIVE_LOG(WARN, "make_dir_ failed", K(ret), K(id), K(key), K(scn), K(task_type)); \
      } else if (OB_FAIL(build_path_(id, key, scn, task_type, scn.get_val_for_logservice(), path, record_context))) {             \
        ARCHIVE_LOG(WARN, "build path failed", K(ret), K(id), K(key), K(scn), K(task_type));      \
      } else if (OB_FAIL(generate_common_header_(buf_, COMMON_HEADER_SIZE, real_size, scn))) {   \
        ARCHIVE_LOG(WARN, "generate_common_header_ failed", K(ret), K(id), K(task_type)); \
      } else if (OB_FAIL(do_record_(key, buf_, real_size + COMMON_HEADER_SIZE, path))) {     \
        ARCHIVE_LOG(WARN, "do record failed", K(ret), K(id), K(task_type));\
      } else if (OB_FAIL(insert_or_update_record_context_(t.get_type(), record_context))) {  \
         ARCHIVE_LOG(WARN, "insert or update record context failed", K(ret), K(id), K(task_type));    \
      }  \
    }   \
  } \
}

namespace oceanbase
{
using namespace share;
namespace archive
{

RecordContext::RecordContext()
{
  reset();
}

RecordContext::~RecordContext()
{
  reset();
}

void RecordContext::reset()
{
  last_record_scn_.reset();
  last_record_round_ = 0;
  last_record_piece_ = 0;
  last_record_file_ = 0;
}

int RecordContext::set(const RecordContext &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    last_record_scn_ = other.last_record_scn_;
    last_record_round_ = other.last_record_round_;
    last_record_piece_ = other.last_record_piece_;
    last_record_file_ = other.last_record_file_;
  }
  return ret;
}

bool RecordContext::is_valid() const
{
  return last_record_scn_.is_valid()
    && last_record_round_ > 0
    && last_record_piece_ > 0
    && last_record_file_ > 0;
}

ObLSMetaRecorder::ObLSMetaRecorder() :
  inited_(false),
  round_mgr_(NULL),
  buf_(NULL),
  map_()
{}

ObLSMetaRecorder::~ObLSMetaRecorder()
{
  destroy();
}

int ObLSMetaRecorder::init(ObArchiveRoundMgr *round_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "ObLSMetaRecorder init twice", K(ret), K(inited_));
  } else if (OB_ISNULL(round_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(round_mgr));
  } else if (OB_FAIL(map_.init("RecordContext"))) {
    ARCHIVE_LOG(WARN, "map init failed", K(ret));
  } else {
    round_mgr_ = round_mgr;
    inited_ = true;
    ARCHIVE_LOG(INFO, "ObLSMetaRecorder init succ");
  }
  return ret;
}

void ObLSMetaRecorder::destroy()
{
  inited_ = false;
  round_mgr_ = NULL;
  buf_ = NULL;
  map_.destroy();
}

void ObLSMetaRecorder::handle()
{
  int ret = OB_SUCCESS;
  ArchiveKey key;
  share::ObArchiveRoundState state;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObLSMetaRecorder not init", K(ret), K(inited_));
  } else if (FALSE_IT(round_mgr_->get_archive_round_info(key, state))) {
  } else if (state.is_suspend()) {
    // do nothing
    ARCHIVE_LOG(TRACE, "archive state in suspend, just wait", K(key), K(state));
  } else if (! state.is_doing()) {
    clear_record_context_();
  } else if (OB_FAIL(prepare_())) {
    ARCHIVE_LOG(WARN, "prepare_ failed", K(ret));
  } else {
    // =============== scope to add ls record task begin =============== //

    ADD_LS_RECORD_TASK(ObArchiveLSSchemaMeta, share::ObArchiveLSMetaType::Type::SCHEMA_META);

    // =============== scope to add ls record task end =============== //

    clear_();
  }
}

int ObLSMetaRecorder::check_and_get_record_context_(const share::ObArchiveLSMetaType &type,
    RecordContext &record_context)
{
  int ret = OB_SUCCESS;
  RecordContext *value = NULL;
  // if local not exist, check and get from remote from current round and piece
  // only check local now
  if (OB_FAIL(map_.get(type, value))) {
    ARCHIVE_LOG(WARN, "get failed", K(ret), K(type));
  } else {
    if (OB_FAIL(record_context.set(*value))) {
      ARCHIVE_LOG(WARN, "set failed", K(ret), KPC(value));
    }
    // insert_and_get have to be used in pair
    map_.revert(value);
    value = NULL;
  }
  return ret;
}

int ObLSMetaRecorder::insert_or_update_record_context_(const share::ObArchiveLSMetaType &type,
    RecordContext &record_context)
{
  int ret = OB_SUCCESS;
  RecordContext *value = NULL;
  if (OB_ENTRY_EXIST == (ret = map_.contains_key(type))) {
    if (OB_FAIL(map_.get(type, value))) {
      ARCHIVE_LOG(WARN, "get failed", K(ret), K(type));
    } else {
      if (OB_FAIL(value->set(record_context))) {
        ARCHIVE_LOG(WARN, "set failed", K(ret), K(record_context));
      }
      // insert_and_get have to be used in pair
      map_.revert(value);
      value = NULL;
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    if (OB_FAIL(map_.alloc_value(value))) {
      ARCHIVE_LOG(WARN, "alloc_value fail", K(ret), K(type));
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(WARN, "value is NULL", K(ret), K(type), K(value));
    } else if (OB_FAIL(value->set(record_context))) {
       ARCHIVE_LOG(WARN, "set failed", K(ret), K(record_context));
    } else if (OB_FAIL(map_.insert_and_get(type, value))) {
      ARCHIVE_LOG(WARN, "insert and get failed", K(ret), K(type), K(value));
    } else {
      // insert_and_get have to be used in pair
      map_.revert(value);
      value = NULL;
    }

    if (OB_FAIL(ret) && NULL != value) {
      (void)map_.del(type);
      map_.free_value(value);
      value = NULL;
    }
  } else {
    ARCHIVE_LOG(ERROR, "contains_key failed", K(ret), K(type));
  }
  return ret;
}

void ObLSMetaRecorder::clear_record_context_()
{
  map_.reset();
}

int ObLSMetaRecorder::prepare_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_ = (char*)share::mtl_malloc(MAX_META_RECORD_FILE_SIZE, "LSMetaFile"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "alloc memory failed", K(ret));
  }
  return ret;
}

// persist ls meta only if the piece already exisscn
bool ObLSMetaRecorder::check_need_delay_(const share::ObLSID &id,
    const ArchiveKey &key,
    const SCN &scn)
{
  bool bret = true;
  int ret = OB_SUCCESS;
  SCN archive_scn;
  palf::LSN unused_lsn;
  bool force_wait = false;
  bool ignore = false;
  int64_t piece_switch_interval = -1;
  SCN genesis_scn;
  int64_t base_piece_id = -1;
  if (OB_FAIL(MTL(ObArchiveService*)->get_ls_archive_progress(
          id, unused_lsn, archive_scn, force_wait, ignore))) {
    ARCHIVE_LOG(WARN, "get ls arcchive progress failed", K(ret), K(id));
  } else if (OB_FAIL(round_mgr_->get_piece_info(key,
          piece_switch_interval, genesis_scn, base_piece_id))) {
    ARCHIVE_LOG(WARN, "get piece info failed", K(ret), K(key), K(id));
  } else if (OB_UNLIKELY(!archive_scn.is_valid() || !scn.is_valid())) {
    ret = OB_EAGAIN;
    ARCHIVE_LOG(WARN, "scn not valid", K(archive_scn), K(scn));
    bret = false;
  } else {
    share::ObArchivePiece archive_piece(archive_scn, piece_switch_interval, genesis_scn, base_piece_id);
    share::ObArchivePiece task_piece(scn, piece_switch_interval, genesis_scn, base_piece_id);
    if (OB_UNLIKELY(!archive_piece.is_valid() || ! task_piece.is_valid())) {
      bret = true;
      ARCHIVE_LOG(WARN, "invalid pieces", K(archive_piece), K(scn), K(archive_piece), K(task_piece));
    } else {
      bret = archive_piece.get_piece_id() < task_piece.get_piece_id();
    }
  }
  return bret;
}

int ObLSMetaRecorder::build_path_(const share::ObLSID &id,
    const ArchiveKey &key,
    const SCN &scn,
    const share::ObArchiveLSMetaType &type,
    const int64_t file_id,
    share::ObBackupPath &path,
    RecordContext &record_context)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest dest;
  int64_t piece_switch_interval = -1;
  SCN genesis_scn;
  int64_t base_piece_id = -1;
  if (OB_FAIL(round_mgr_->get_backup_dest(key, dest))) {
    ARCHIVE_LOG(WARN, "get backup dest failed", K(ret), K(key));
  } else if (OB_FAIL(round_mgr_->get_piece_info(key,
          piece_switch_interval, genesis_scn, base_piece_id))) {
    ARCHIVE_LOG(WARN, "get piece info failed", K(ret), K(key));
  } else {
    share::ObArchivePiece piece(scn, piece_switch_interval, genesis_scn, base_piece_id);
    if (OB_FAIL(share::ObArchivePathUtil::get_ls_meta_record_path(dest,
            key.dest_id_, key.round_, piece.get_piece_id(), id, type, file_id, path))) {
      ARCHIVE_LOG(WARN, "get ls meta record path failed", K(ret), K(id));
    } else {
      record_context.last_record_file_ = file_id;
      record_context.last_record_scn_ = scn;
      record_context.last_record_round_ = key.round_;
      record_context.last_record_piece_ = piece.get_piece_id();
    }
  }
  return ret;
}

int ObLSMetaRecorder::generate_common_header_(char *buf,
    const int64_t header_size,
    const int64_t data_size,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObLSMetaFileHeader header;
  const int64_t data_checksum = common::ob_crc64(buf + header_size, data_size);
  if (OB_FAIL(header.generate_header(scn, data_size))) {
    ARCHIVE_LOG(WARN, "generate meta file header failed", K(ret), K(scn));
  } else if (OB_FAIL(header.serialize(buf, header_size, pos))) {
    ARCHIVE_LOG(WARN, "meta header serialize failed", K(ret), K(header));
  } else if (OB_UNLIKELY(pos > COMMON_HEADER_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "pos exceed", K(ret), K(pos));
  } else {
    MEMSET(buf + pos, 0, header_size - pos);
  }
  return ret;
}

int ObLSMetaRecorder::do_record_(const ArchiveKey &key,
    const char *buf,
    const int64_t size,
    share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest dest;
  if (OB_FAIL(round_mgr_->get_backup_dest(key, dest))) {
    ARCHIVE_LOG(WARN, "get backup dest failed", K(ret));
  } else if (OB_FAIL(ObArchiveFileUtils::write_file(path.get_obstr(),
          dest.get_storage_info(), buf, size))) {
    ARCHIVE_LOG(WARN, "write file failed", K(ret));
  }
  return ret;
}

int ObLSMetaRecorder::make_dir_(const share::ObLSID &id,
    const ArchiveKey &key,
    const SCN &scn,
    const share::ObArchiveLSMetaType &type)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest dest;
  share::ObBackupPath prefix;
  int64_t piece_switch_interval = -1;
  SCN genesis_scn;
  int64_t base_piece_id = -1;
  if (OB_FAIL(round_mgr_->get_backup_dest(key, dest))) {
    ARCHIVE_LOG(WARN, "get backup dest failed", K(ret), K(key));
  } else if (OB_FAIL(round_mgr_->get_piece_info(key,
          piece_switch_interval, genesis_scn, base_piece_id))) {
    ARCHIVE_LOG(WARN, "get piece info failed", K(ret), K(key));
  } else {
    bool dir_exist = false;
    share::ObArchivePiece piece(scn, piece_switch_interval, genesis_scn, base_piece_id);
    if (OB_FAIL(share::ObArchivePathUtil::get_ls_meta_record_prefix(dest,
            key.dest_id_, key.round_, piece.get_piece_id(), id, type, prefix))) {
      ARCHIVE_LOG(WARN, "get ls meta record prefix failed", K(ret), K(id));
    } else if (OB_FAIL(ObArchiveFileUtils::is_exist(prefix.get_obstr(),
            dest.get_storage_info(), dir_exist))) {
      ARCHIVE_LOG(WARN, "check is_exist failed", K(ret), K(prefix));
    } else if (dir_exist) {
      // do nothing
    } else if (OB_FAIL(ObArchiveFileUtils::mkdir(prefix.get_obstr(), dest.get_storage_info()))) {
      ARCHIVE_LOG(WARN, "mkdir failed", K(ret), K(prefix));
    }
  }
  return ret;
}

void ObLSMetaRecorder::clear_()
{
  if (NULL != buf_) {
    share::mtl_free(buf_);
    buf_ = NULL;
  }
}
} // namespace archive
} // namespace oceanbase
