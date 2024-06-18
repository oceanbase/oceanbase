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

#define USING_LOG_PREFIX CLOG
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "ob_remote_data_generator.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/ob_log_service.h"                    // ObLogService
#include "logservice/palf/log_group_entry.h"              // LogGroupEntry
#include "logservice/archiveservice/ob_archive_file_utils.h"     // ObArchiveFileUtils
#include "share/backup/ob_archive_path.h"           // ObArchivePathUtil
#include "logservice/archiveservice/ob_archive_define.h"         // ObArchiveFileHeader
#include "logservice/archiveservice/ob_archive_util.h"       // ObArchiveFileUtils
#include "share/backup/ob_backup_path.h"                // ObBackupPath
#include "ob_log_restore_rpc.h"                           // proxy
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_archive_path.h"           // ObArchivePathUtil
#include "src/share/backup/ob_archive_store.h"      // ObArchiveStore

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::palf;
using namespace oceanbase::share;
using namespace oceanbase::archive;
// ============================ RemoteDataGenerator ============================ //
RemoteDataGenerator::RemoteDataGenerator(const uint64_t tenant_id,
    const ObLSID &id,
    const LSN &start_lsn,
    const LSN &end_lsn,
    const SCN &end_scn,
    ObLogExternalStorageHandler *log_ext_handler) :
  tenant_id_(tenant_id),
  id_(id),
  start_lsn_(start_lsn),
  next_fetch_lsn_(start_lsn),
  end_scn_(end_scn),
  end_lsn_(end_lsn),
  to_end_(false),
  log_ext_handler_(log_ext_handler)
{}

RemoteDataGenerator::~RemoteDataGenerator()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  id_.reset();
  start_lsn_.reset();
  next_fetch_lsn_.reset();
  end_lsn_.reset();
  log_ext_handler_ = NULL;
}

bool RemoteDataGenerator::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
    && id_.is_valid()
    && start_lsn_.is_valid()
    && end_scn_.is_valid()
    && end_lsn_.is_valid()
    && end_lsn_ > start_lsn_
    && log_ext_handler_ != NULL;
}

bool RemoteDataGenerator::is_fetch_to_end() const
{
  return next_fetch_lsn_ >= end_lsn_ || to_end_;
}

int RemoteDataGenerator::update_next_fetch_lsn_(const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(lsn), KPC(this));
  } else if (lsn < next_fetch_lsn_) {
    // advance step lsn, the start lsn of the read buffer may be smaller than the next_fetch_lsn_
    CLOG_LOG(TRACE, "fetch_lsn too small, skip it", K(ret), K(lsn), KPC(this));
  } else {
    next_fetch_lsn_ = lsn;
  }
  return ret;
}

int RemoteDataGenerator::read_file_(const ObString &base,
    const share::ObBackupStorageInfo *storage_info,
    const share::ObLSID &id,
    const int64_t file_id,
    const int64_t offset,
    char *data,
    const int64_t data_len,
    int64_t &data_size)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  if (OB_FAIL(ObArchivePathUtil::build_restore_path(base.ptr(), id, file_id, path))) {
    LOG_WARN("build restore path failed", K(ret));
  } else {
    ObString uri(path.get_obstr());
    char storage_info_cstr[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {'\0'};
    int64_t real_size = 0;
    common::ObObjectStorageInfo storage_info_base;
    if (OB_FAIL(storage_info_base.assign(*storage_info))) {
      OB_LOG(WARN, "fail to assign storage info base!", K(ret), KP(storage_info));
    } else if (OB_FAIL(storage_info_base.get_storage_info_str(storage_info_cstr, OB_MAX_BACKUP_STORAGE_INFO_LENGTH))) {
      LOG_WARN("get_storage_info_str failed", K(ret), K(uri), K(storage_info));
    } else {
      ObString storage_info_ob_str(storage_info_cstr);
      if (OB_FAIL(log_ext_handler_->pread(uri, storage_info_ob_str, offset, data, data_len, real_size))) {
        LOG_WARN("read file failed", K(ret), K(uri), K(storage_info));
      } else if (0 == real_size) {
        ret = OB_ITER_END;
        LOG_INFO("read no data, need retry", K(ret), K(uri), K(storage_info), K(offset), K(real_size));
      } else {
        data_size = real_size;
      }
    }
  }
  return ret;
}

// only handle orignal buffer without compression or encryption
// only to check incomplete LogGroupEntry
// compression and encryption will be supported in the future
//
// 仅支持备份情况下, 不需要处理归档写入不原子情况
int RemoteDataGenerator::process_origin_data_(char *origin_buf,
    const int64_t origin_buf_size,
    char *buf,
    int64_t &buf_size)
{
  UNUSED(origin_buf);
  UNUSED(origin_buf_size);
  UNUSED(buf);
  UNUSED(buf_size);
  return OB_NOT_SUPPORTED;
}
// ================================ ServiceDataGenerator ============================= //
ServiceDataGenerator::ServiceDataGenerator(const uint64_t tenant_id,
    const ObLSID &id,
    const LSN &start_lsn,
    const LSN &end_lsn,
    const SCN &end_scn,
    const ObAddr &server,
    ObLogExternalStorageHandler *log_ext_handler) :
  RemoteDataGenerator(tenant_id, id, start_lsn, end_lsn, end_scn, log_ext_handler),
  server_(server),
  result_()
{}

ServiceDataGenerator::~ServiceDataGenerator()
{
  server_.reset();
  result_.reset();
}

int ServiceDataGenerator::next_buffer(palf::LSN &lsn, char *&buf, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ServiceDataGenerator is invalid", K(ret), KPC(this));
  } else if (is_fetch_to_end()) {
    ret = OB_ITER_END;
    LOG_INFO("ServiceDataGenerator to end", K(ret), KPC(this));
  } else if (OB_FAIL(fetch_log_from_net_())) {
    LOG_WARN("fetch_log_from_net_ failed", K(ret), KPC(this));
  } else {
    lsn = start_lsn_;
    buf = result_.data_;
    buf_size = result_.data_len_;
  }
  return ret;
}

bool ServiceDataGenerator::is_valid() const
{
  return RemoteDataGenerator::is_valid()
    && server_.is_valid();
}

// 暂时仅支持单次fetch 需要连续fetch大段数据由上层控制
int ServiceDataGenerator::fetch_log_from_net_()
{
  int ret = OB_SUCCESS;
  ObLogService *log_svr = NULL;
  logservice::ObLogResSvrRpc *proxy = NULL;
  if (OB_ISNULL(log_svr = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObLogService is NULL", K(ret), K(log_svr), KPC(this));
  } else if (OB_ISNULL(proxy = log_svr->get_log_restore_service()->get_log_restore_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObLogResSvrRpc is NULL", K(ret), K(log_svr), KPC(this));
  } else {
    obrpc::ObRemoteFetchLogRequest req(tenant_id_, id_, start_lsn_, end_lsn_);
    if (OB_FAIL(proxy->fetch_log(server_, req, result_))) {
      LOG_WARN("fetch log failed", K(ret), K(req), KPC(this));
    } else if (OB_UNLIKELY(! result_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObRemoteFetchLogResponse is invalid", K(ret), K(req), KPC(this));
    } else {
      to_end_ = true;
      if (result_.is_empty()) {
        ret = OB_ITER_END;
        LOG_INFO("no log exist with the request", K(req), KPC(this));
      } else {
        next_fetch_lsn_ = result_.end_lsn_;
      }
    }
  }
  return ret;
}

// ==================================== LocationDataGenerator ============================== //
// 为加速定位起点文件，依赖LSN -> file_id 规则
static int64_t cal_lsn_to_file_id_(const LSN &lsn)
{
  return cal_archive_file_id(lsn, palf::PALF_BLOCK_SIZE);
}

static int extract_archive_file_header_(char *buf,
    const int64_t buf_size,
    palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  archive::ObArchiveFileHeader file_header;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf), K(buf_size));
  } else if (OB_FAIL(file_header.deserialize(buf, buf_size, pos))) {
    LOG_WARN("archive file header deserialize failed", K(ret));
  } else if (OB_UNLIKELY(pos > ARCHIVE_FILE_HEADER_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("archive file header size exceed threshold", K(ret), K(pos));
  } else if (OB_UNLIKELY(! file_header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid file header", K(ret), K(pos), K(file_header));
  } else {
    lsn = LSN(file_header.start_lsn_);
    LOG_INFO("extract_archive_file_header_ succ", K(pos), K(file_header));
  }
  return ret;
}

static int list_dir_files_(const ObString &base,
    const share::ObBackupStorageInfo *storage_info,
    const ObLSID &id,
    int64_t &min_file_id,
    int64_t &max_file_id)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath prefix;
  if (OB_FAIL(ObArchivePathUtil::build_restore_prefix(base.ptr(), id, prefix))) {
  } else {
    ObString uri(prefix.get_obstr());
    ret = ObArchiveFileUtils::get_file_range(uri, storage_info, min_file_id, max_file_id);
  }
  return ret;
}

LocationDataGenerator::LocationDataGenerator(const uint64_t tenant_id,
    const SCN &pre_scn,
    const ObLSID &id,
    const LSN &start_lsn,
    const LSN &end_lsn,
    const SCN &end_scn,
    share::ObBackupDest *dest,
    ObLogArchivePieceContext *piece_context,
    char *buf,
    const int64_t buf_size,
    const int64_t single_read_size,
    ObLogExternalStorageHandler *log_ext_handler) :
  RemoteDataGenerator(tenant_id, id, start_lsn, end_lsn, end_scn, log_ext_handler),
  pre_scn_(pre_scn),
  base_lsn_(),
  data_len_(0),
  buf_(buf),
  buf_size_(buf_size),
  single_read_size_(single_read_size),
  dest_(dest),
  piece_context_(piece_context),
  cur_file_()
{
  int64_t dest_id = -1;
  int64_t round_id = -1;
  int64_t piece_id = -1;
  int64_t file_id = -1;
  int64_t file_offset = -1;
  palf::LSN max_lsn;
  if (NULL != piece_context_) {
    piece_context_->get_max_file_info(dest_id, round_id, piece_id, file_id, file_offset, max_lsn);
    if (dest_id > 0 && round_id > 0 && piece_id > 0 && file_id > 0 && file_offset >= 0 && max_lsn.is_valid()) {
      cur_file_.dest_id_ = dest_id;
      cur_file_.round_id_ = round_id;
      cur_file_.piece_id_ = piece_id;
      cur_file_.file_id_ = file_id;
      cur_file_.base_file_offset_ = file_offset;
      cur_file_.base_lsn_ = max_lsn;
      cur_file_.cur_lsn_ = max_lsn;
    }
  }
  cur_file_.is_origin_data_ = true;
}

LocationDataGenerator::~LocationDataGenerator()
{
  pre_scn_.reset();
  base_lsn_.reset();
  dest_ = NULL;
  piece_context_ = NULL;
  buf_ = NULL;
  buf_size_ = 0;
  single_read_size_ = 0;
  cur_file_.reset();
}

int LocationDataGenerator::next_buffer(palf::LSN &lsn, char *&buf, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("LocationDataGenerator is invalid", K(ret), KPC(this));
  } else if (is_fetch_to_end()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(fetch_log_from_location_(buf, buf_size))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fetch log from location failed", K(ret), KPC(this));
    }
  } else {
    lsn = base_lsn_;
  }
  return ret;
}

int LocationDataGenerator::advance_step_lsn(const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (cur_file_.is_origin_data_) {
    if (OB_UNLIKELY(!lsn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(lsn));
    } else if (OB_FAIL(RemoteDataGenerator::update_next_fetch_lsn_(lsn))) {
      LOG_WARN("update_next_fetch_lsn_ failed", K(lsn), KPC(this));
    } else if (OB_FAIL(cur_file_.advance(lsn))) {
      LOG_WARN("advance failed", K(lsn));
    } else if (OB_UNLIKELY(!cur_file_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid cur_file_", KPC(this));
    } else if (OB_FAIL(piece_context_->update_file_info(cur_file_.dest_id_, cur_file_.round_id_, cur_file_.piece_id_,
            cur_file_.file_id_, cur_file_.cur_offset_, cur_file_.cur_lsn_))) {
      LOG_WARN("update_file_info failed", K(lsn), K(cur_file_));
    } else {
      LOG_TRACE("advance_step_lsn succ", KPC(this));
    }
  }
  return ret;
}

int LocationDataGenerator::update_max_lsn(const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (!cur_file_.is_origin_data_) {
    if (OB_SUCC(RemoteDataGenerator::update_next_fetch_lsn_(lsn)) && NULL != piece_context_) {
      if (OB_UNLIKELY(!cur_file_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid cur_file_", KPC(this));
      } else if (OB_FAIL(piece_context_->update_file_info(cur_file_.dest_id_, cur_file_.round_id_, cur_file_.piece_id_,
              cur_file_.file_id_, cur_file_.cur_offset_, cur_file_.cur_lsn_))) {
        LOG_WARN("piece context update file info failed", K(ret), KPC(this));
      } else {
        LOG_TRACE("update_file_info succ", KPC(piece_context_));
      }
    }
  }
  return ret;
}

bool LocationDataGenerator::is_valid() const
{
  return RemoteDataGenerator::is_valid()
    && NULL != dest_
    && NULL != piece_context_
    && NULL != buf_
    && buf_size_ > 0
    && single_read_size_ > 0;
}

int LocationDataGenerator::fetch_log_from_location_(char *&buf, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  int64_t dest_id = -1;
  int64_t round_id = -1;
  int64_t piece_id = -1;
  int64_t file_id = -1;
  int64_t file_offset = -1;
  int64_t read_size = 0;
  palf::LSN max_lsn_in_file (palf::LOG_INVALID_LSN_VAL);
  share::ObBackupPath piece_path;
  if (OB_FAIL(get_precise_file_and_offset_(dest_id, round_id, piece_id,
          file_id, file_offset, max_lsn_in_file, piece_path))) {
    LOG_WARN("get precise file and offset failed", K(ret));
  } else if (FALSE_IT(cal_read_size_(dest_id, round_id, piece_id, file_id, file_offset, read_size))) {
  } else if (OB_FAIL(read_file_(piece_path.get_ptr(), dest_->get_storage_info(), id_,
          file_id, file_offset, buf_, read_size, data_len_))) {
    if (OB_ITER_END == ret) {
      LOG_TRACE("read end of file", K(ret));
    } else {
      LOG_WARN("read file failed", K(ret));
    }
  } else if (file_offset > 0) {
    // 非第一次读文件, 不必再解析file header
    base_lsn_ = max_lsn_in_file;
    buf = buf_;
    buf_size = data_len_;
  } else if (OB_FAIL(extract_archive_file_header_(buf_, data_len_, base_lsn_))) {
    LOG_WARN("extract archive file heaeder failed", K(ret), KPC(this));
  } else {
    buf = buf_ + ARCHIVE_FILE_HEADER_SIZE;
    buf_size = data_len_ - ARCHIVE_FILE_HEADER_SIZE;
  }

  if ((OB_SUCC(ret) && base_lsn_ > next_fetch_lsn_) || OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    LOG_INFO("read file base_lsn bigger than start_lsn, reset locate info", K(base_lsn_), K(next_fetch_lsn_));
    piece_context_->reset_locate_info();
    ret = OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    cur_file_.reset();
    cur_file_.set(true /*origin_data*/, dest_id, round_id, piece_id, file_id, file_offset, file_offset + data_len_, base_lsn_);
  }

  return ret;
}

// 当前起始LSN小于piece_context已消费最大LSN, 需要relocate piece
// 因为以LSN计算file_id, 该file可能位于当前piece, 也可能位于前一个或者多个piece, 需要重新locate
int LocationDataGenerator::get_precise_file_and_offset_(int64_t &dest_id,
    int64_t &round_id,
    int64_t &piece_id,
    int64_t &file_id,
    int64_t &file_offset,
    palf::LSN &lsn,
    share::ObBackupPath &piece_path)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  if (FALSE_IT(file_id = cal_lsn_to_file_id_(next_fetch_lsn_))) {
  } else if (OB_FAIL(piece_context_->get_piece(pre_scn_, next_fetch_lsn_,
          dest_id, round_id, piece_id, file_id, file_offset, lsn, to_end_))) {
    if (OB_ARCHIVE_LOG_TO_END == ret) {
      ret = OB_ITER_END;
    } else {
      LOG_WARN("get cur piece failed", K(ret), KPC(piece_context_));
    }
  } else if (OB_FAIL(share::ObArchivePathUtil::get_piece_dir_path(*dest_,
          dest_id, round_id, piece_id, piece_path))) {
    LOG_WARN("get piece dir path failed", K(ret));
  } else if (lsn.is_valid() && lsn > next_fetch_lsn_) {
    file_offset = 0;
  }

  return ret;
}

void LocationDataGenerator::cal_read_size_(const int64_t dest_id,
    const int64_t round_id,
    const int64_t piece_id,
    const int64_t file_id,
    const int64_t file_offset,
    int64_t &size)
{
  if (cur_file_.match(dest_id, round_id, piece_id, next_fetch_lsn_)) {
    // read the pointed size or with file header
    if (file_offset == 0) {
      size = single_read_size_ + ARCHIVE_FILE_HEADER_SIZE;
    } else {
      size = single_read_size_;
    }
  } else {
    // read the max data file size
    size = buf_size_;
  }
}

bool LocationDataGenerator::FileDesc::is_valid() const
{
  return dest_id_ > 0
    && round_id_ > 0
    && piece_id_ > 0
    && file_id_ > 0
    && base_file_offset_ >= 0
    && max_file_offset_ > base_file_offset_
    && cur_offset_ >= base_file_offset_
    && base_lsn_.is_valid()
    && cur_lsn_.is_valid()
    && cur_lsn_ > base_lsn_;
}

void LocationDataGenerator::FileDesc::reset()
{
  is_origin_data_ = false;
  dest_id_ = -1;
  round_id_ = -1;
  piece_id_ = -1;
  file_id_ = -1;
  base_file_offset_ = -1;
  max_file_offset_ = -1;
  cur_offset_ = -1;
  base_lsn_.reset();
  cur_lsn_.reset();

}

bool LocationDataGenerator::FileDesc::match(const int64_t dest_id,
    const int64_t round_id,
    const int64_t piece_id,
    const palf::LSN &lsn) const
{
  return dest_id > 0
    && round_id > 0
    && piece_id > 0
    && lsn.is_valid()
    && dest_id == dest_id_
    && round_id == round_id_
    && piece_id == piece_id_
    && lsn == cur_lsn_;
}

int LocationDataGenerator::FileDesc::set(const bool origin_data,
    const int64_t dest_id,
    const int64_t round_id,
    const int64_t piece_id,
    const int64_t file_id,
    const int64_t base_file_offset,
    const int64_t max_file_offset,
    const palf::LSN &base_lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dest_id <= 0
        || round_id <= 0
        || piece_id <= 0
        || file_id <= 0
        || base_file_offset <0
        || max_file_offset <= base_file_offset
        || !base_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(origin_data), K(dest_id), K(round_id), K(piece_id),
        K(base_file_offset), K(max_file_offset), K(base_lsn));
  } else {
    is_origin_data_ = origin_data;
    dest_id_ = dest_id;
    round_id_ = round_id;
    piece_id_ = piece_id;
    file_id_ = file_id;
    base_file_offset_ = base_file_offset;
    max_file_offset_ = max_file_offset;
    base_lsn_ = base_lsn;
  }
  return ret;
}

int LocationDataGenerator::FileDesc::advance(const palf::LSN &cur_lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cur_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(cur_lsn));
  } else if (!is_origin_data_) {
    // skip
  } else if (cur_lsn - base_lsn_ + base_file_offset_ > max_file_offset_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("lsn oversize", K(cur_lsn), KPC(this));
  } else {
    cur_lsn_ = cur_lsn;
    int64_t step = static_cast<int64_t>((cur_lsn_ - base_lsn_));
    if (base_file_offset_ == 0) {
      cur_offset_ = base_file_offset_ + step + ARCHIVE_FILE_HEADER_SIZE;
    } else {
      cur_offset_ = base_file_offset_ + step;
    }
  }
  return ret;
}

// ==================================== RawPathDataGenerator ============================== //
RawPathDataGenerator::RawPathDataGenerator(const uint64_t tenant_id,
    const ObLSID &id,
    const LSN &start_lsn,
    const LSN &end_lsn,
    ObLogRawPathPieceContext *rawpath_ctx,
    const SCN &end_scn,
    ObLogExternalStorageHandler *log_ext_handler) :
  RemoteDataGenerator(tenant_id, id, start_lsn, end_lsn, end_scn, log_ext_handler),
  rawpath_ctx_(rawpath_ctx),
  data_len_(0),
  base_lsn_()
{
}

RawPathDataGenerator::~RawPathDataGenerator()
{
  rawpath_ctx_ = NULL;
  data_len_ = 0;
  base_lsn_.reset();
}

int RawPathDataGenerator::next_buffer(palf::LSN &lsn, char *&buf, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("RawPathDataGenerator is invalid", K(ret), KPC(this));
  } else if (is_fetch_to_end()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(fetch_log_from_dest_())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fetch log from dest failed", K(ret), KPC(this));
    }
  } else {
    lsn = base_lsn_;
    buf = data_ + ARCHIVE_FILE_HEADER_SIZE;
    buf_size = data_len_ - ARCHIVE_FILE_HEADER_SIZE;
    LOG_TRACE("after next_buffer", K(lsn), K(buf_size));
  }
  return ret;
}

int RawPathDataGenerator::advance_step_lsn(const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((NULL == rawpath_ctx_) || ! rawpath_ctx_->is_valid())) {
    LOG_TRACE("rawpath_ctx is invalid");
  } else if (OB_FAIL(RemoteDataGenerator::update_next_fetch_lsn_(lsn))) {
    LOG_WARN("update_next_fetch_lsn_ failed", K(lsn), KPC(this));
  } else {
    rawpath_ctx_->update_max_lsn(lsn);
    LOG_TRACE("advance_step_lsn succ", KPC(this));
  }
  return ret;
}

int RawPathDataGenerator::update_max_lsn(const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((NULL == rawpath_ctx_) || ! rawpath_ctx_->is_valid())) {
    LOG_TRACE("rawpath_ctx is invalid");
  } else if (OB_FAIL(RemoteDataGenerator::update_next_fetch_lsn_(lsn))) {
    LOG_WARN("update_next_fetch_lsn_ failed", K(lsn), KPC(this));
  } else {
    rawpath_ctx_->update_max_lsn(lsn);
    LOG_TRACE("update_file_info succ", K_(*rawpath_ctx));
  }
  return ret;
}

int RawPathDataGenerator::fetch_log_from_dest_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((NULL == rawpath_ctx_) || ! rawpath_ctx_->is_valid())) {
    LOG_WARN("rawpath_ctx_ is invalid");
  } else {
    char uri_str[OB_MAX_BACKUP_DEST_LENGTH + 1] = { 0 };
    char storage_info_str[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
    share::ObBackupStorageInfo storage_info;
    int64_t file_id = 0;

    if (OB_FAIL(rawpath_ctx_->cal_lsn_to_file_id(next_fetch_lsn_))) {     // locate file_id_ by next_fetch_lsn_
      LOG_WARN("fail to cal lsn to file id", K(ret), K_(id), K_(next_fetch_lsn));
    } else if (OB_FAIL(rawpath_ctx_->locate_precise_piece(next_fetch_lsn_))) {
      if (OB_ITER_END == ret) {
        LOG_INFO("locate precise piece to end", K(ret), K_(id), KPC(this));
      } else {
        LOG_WARN("locate precise piece failed", K(ret), KPC(this));
      }
    } else if (OB_FAIL(rawpath_ctx_->get_file_id(file_id))) {
      LOG_WARN("fail to get file id", KPC(this));
    } else if (OB_FAIL(rawpath_ctx_->get_cur_uri(uri_str, sizeof(uri_str)))) {
      LOG_WARN("fail to get cur uri ptr", K(ret));
    } else if (OB_FAIL(rawpath_ctx_->get_cur_storage_info(storage_info_str, sizeof(storage_info_str)))) {
      LOG_WARN("fail to get storage info ptr", K(ret));
    } else if (OB_FAIL(storage_info.set(uri_str, storage_info_str))) {
      LOG_WARN("failed to set storage info", K(ret));
    } else if (OB_FAIL(read_file_(uri_str, &storage_info, file_id))) {
      if (OB_ITER_END == ret) {
        LOG_TRACE("read end of file", K(ret));
      } else {
        LOG_WARN("read file failed", K(ret), K_(id), K(file_id));
      }
    } else if (OB_FAIL(extract_archive_file_header_())) {
      LOG_WARN("extract archive file header failed", K(ret));
    }
  }

  if ((OB_SUCC(ret) && base_lsn_ > next_fetch_lsn_) || OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file base_lsn bigger than next_fetch_lsn", K(base_lsn_), K(next_fetch_lsn_));
  }
  return ret;
}

int RawPathDataGenerator::read_file_(const ObString &base,
    const share::ObBackupStorageInfo *storage_info,
    const int64_t file_id)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  if (OB_FAIL(ObArchivePathUtil::build_restore_path(base.ptr(), id_, file_id, path))) {
    LOG_WARN("build restore path failed", K(ret));
  } else {
    ObString uri(path.get_obstr());
    int64_t real_size = 0;
    int64_t file_length = 0;
    if (OB_FAIL(ObArchiveFileUtils::get_file_length(uri, storage_info, file_length))) {
      LOG_WARN("read_file failed", K(ret), K(uri), KP(storage_info));
    } else if (0 == file_length) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("file_length is empty", K(ret), K(uri), KP(storage_info), K(file_length));
    } else if (OB_FAIL(ObArchiveFileUtils::read_file(uri, storage_info, data_, file_length, real_size))) {
      LOG_WARN("read file failed", K(ret), K_(id), K(file_id));
    } else if (0 == real_size) {
      ret = OB_ITER_END;
    } else {
      data_len_ = real_size;
    }
  }
  return ret;
}

int RawPathDataGenerator::extract_archive_file_header_()
{
  int ret = OB_SUCCESS;
  archive::ObArchiveFileHeader file_header;
  int64_t pos = 0;
  if (OB_FAIL(file_header.deserialize(data_, data_len_, pos))) {
    LOG_WARN("archive file header deserialize failed", K(ret), KPC(this));
  } else if (OB_UNLIKELY(pos > ARCHIVE_FILE_HEADER_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("archive file header size exceed threshold", K(ret), K(pos), KPC(this));
  } else if (OB_UNLIKELY(! file_header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid file header", K(ret), K(pos), K(file_header), KPC(this));
  } else {
    base_lsn_ = LSN(file_header.start_lsn_);
    rawpath_ctx_->update_min_lsn(base_lsn_);
    LOG_INFO("extract_archive_file_header_ succ", K(pos), K(file_header), KPC(this));
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
