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

#include "ob_remote_log_source.h"             // ObRemoteLogParent
#include "lib/net/ob_addr.h"                  // ObAddr
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/restoreservice/ob_log_archive_piece_mgr.h"
#include "ob_remote_log_source_allocator.h"    // ObResSrcAlloctor
#include "share/ob_define.h"
#include <cstdint>

namespace oceanbase
{
namespace logservice
{
// =========================== ObRemoteLogParent ==============================//
ObRemoteLogParent::ObRemoteLogParent(const ObLogArchiveSourceType &type, const share::ObLSID &ls_id) :
  ls_id_(ls_id),
  type_(type),
  upper_limit_ts_(OB_INVALID_TIMESTAMP),
  to_end_(false),
  end_fetch_log_ts_(OB_INVALID_TIMESTAMP),
  end_lsn_(),
  error_context_()
{}

ObRemoteLogParent::~ObRemoteLogParent()
{
  ls_id_.reset();
  type_ = ObLogArchiveSourceType::INVALID;
  upper_limit_ts_ = OB_INVALID_TIMESTAMP;
  to_end_ = false;
  end_fetch_log_ts_ = OB_INVALID_TIMESTAMP;
  end_lsn_.reset();
}

const char *ObRemoteLogParent::get_source_type_str(const ObLogArchiveSourceType &type) const
{
  return share::ObLogArchiveSourceItem::get_source_type_str(type);
}

void ObRemoteLogParent::set_to_end(const bool is_to_end, const int64_t timestamp)
{
  if (is_to_end && ! to_end_) {
    to_end_ = true;
    end_fetch_log_ts_ = timestamp;
    CLOG_LOG(INFO, "set_to_end succ", KPC(this));
  }
}

void ObRemoteLogParent::base_copy_to_(ObRemoteLogParent &other)
{
  other.type_ = type_;
  other.upper_limit_ts_ = upper_limit_ts_;
  other.to_end_ = to_end_;
  other.end_fetch_log_ts_ = end_fetch_log_ts_;
  other.end_lsn_ = end_lsn_;
  other.error_context_ = error_context_;
}

bool ObRemoteLogParent::is_valid_() const
{
  return share::is_valid_log_source_type(type_)
    && ls_id_.is_valid()
    && upper_limit_ts_ > 0;
}

void ObRemoteLogParent::mark_error(share::ObTaskId &trace_id, const int ret_code)
{
  if (OB_SUCCESS == error_context_.ret_code_) {
    error_context_.trace_id_.set(trace_id);
    error_context_.ret_code_ = ret_code;
  }
}

void ObRemoteLogParent::get_error_info(share::ObTaskId &trace_id, int &ret_code, bool &error_exist)
{
  if (OB_SUCCESS == error_context_.ret_code_) {
    error_exist = false;
  } else {
    error_exist = true;
    trace_id.set(error_context_.trace_id_);
    ret_code = error_context_.ret_code_;
  }
}

// =========================== ObRemoteSerivceParent ==============================//
ObRemoteSerivceParent::ObRemoteSerivceParent(const share::ObLSID &ls_id) :
  ObRemoteLogParent(ObLogArchiveSourceType::SERVICE, ls_id),
  server_()
{}

ObRemoteSerivceParent::~ObRemoteSerivceParent()
{
  server_.reset();
}

int ObRemoteSerivceParent::set(const ObAddr &addr, const int64_t end_log_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! addr.is_valid() || OB_INVALID_TIMESTAMP == end_log_ts)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(end_log_ts), K(addr));
  } else if (addr == server_ && end_log_ts == upper_limit_ts_) {
  } else {
    server_ = addr;
    upper_limit_ts_ = end_log_ts;
  }
  return ret;
}

void ObRemoteSerivceParent::get(ObAddr &addr, int64_t &end_log_ts)
{
  addr = server_;
  end_log_ts = upper_limit_ts_;
}

int ObRemoteSerivceParent::deep_copy_to(ObRemoteLogParent &other)
{
  ObRemoteSerivceParent &dst = static_cast<ObRemoteSerivceParent &>(other);
  dst.server_ = server_;
  base_copy_to_(other);
  return OB_SUCCESS;
}

bool ObRemoteSerivceParent::is_valid() const
{
  return is_valid_() && server_.is_valid();
}

// =========================== ObRemoteLocationParent ==============================//
ObRemoteLocationParent::ObRemoteLocationParent(const share::ObLSID &ls_id) :
  ObRemoteLogParent(ObLogArchiveSourceType::LOCATION, ls_id),
  root_path_(),
  piece_context_()
{}

ObRemoteLocationParent::~ObRemoteLocationParent()
{
  root_path_.reset();
  piece_context_.reset();
}

void ObRemoteLocationParent::get(share::ObBackupDest *&dest,
    ObLogArchivePieceContext *&piece_context,
    int64_t &end_log_ts)
{
  dest = &root_path_;
  piece_context = &piece_context_;
  end_log_ts = upper_limit_ts_;
}

int ObRemoteLocationParent::set(const share::ObBackupDest &dest, const int64_t end_log_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! dest.is_valid() || OB_INVALID_TIMESTAMP == end_log_ts)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(end_log_ts), K(dest));
  } else if (dest == root_path_) {
    if (end_log_ts < upper_limit_ts_ && INT64_MAX != upper_limit_ts_) {
      CLOG_LOG(WARN, "fetch log upper_limit_ts rollback", K(ret), K(end_log_ts), KPC(this));
    } else if (end_log_ts == upper_limit_ts_) {
      // skip
    } else {
      const int64_t pre_upper_log_ts = upper_limit_ts_;
      upper_limit_ts_ = end_log_ts;
      CLOG_LOG(INFO, "upper limit ts increase", K(dest), K(pre_upper_log_ts), K(end_log_ts));
    }
  } else if (OB_FAIL(root_path_.deep_copy(dest))) {
    CLOG_LOG(WARN, "root path deep copy failed", K(ret), K(dest), KPC(this));
  } else if (OB_FAIL(piece_context_.init(ls_id_, root_path_))) {
    CLOG_LOG(WARN, "piece context init failed", K(ret), KPC(this));
  } else {
    upper_limit_ts_ = end_log_ts;
    CLOG_LOG(INFO, "add location source succ", K(ret), KPC(this));
  }
  return ret;
}

int ObRemoteLocationParent::deep_copy_to(ObRemoteLogParent &other)
{
  int ret = OB_SUCCESS;
  ObRemoteLocationParent &dst = static_cast<ObRemoteLocationParent &>(other);
  if (OB_FAIL(dst.root_path_.deep_copy(root_path_))) {
    CLOG_LOG(WARN, "root path deep copy failed", K(ret), KPC(this));
  } else if (OB_FAIL(piece_context_.deep_copy_to(dst.piece_context_))) {
    CLOG_LOG(WARN, "piece context deep copy failed", K(ret), KPC(this));
  } else {
    base_copy_to_(other);
  }
  return ret;
}

bool ObRemoteLocationParent::is_valid() const
{
  return is_valid_() && root_path_.is_valid();
}

int ObRemoteLocationParent::update_locate_info(ObRemoteLogParent &source)
{
  int ret = OB_SUCCESS;
  ObRemoteLocationParent &dst = static_cast<ObRemoteLocationParent &>(source);
  if (OB_UNLIKELY(! dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid location parent", K(ret), K(dst));
  } else if (dst.root_path_ != root_path_) {
    // parent changed
    CLOG_LOG(WARN, "parent changed, just skip", K(dst), KPC(this));
  } else if (OB_FAIL(dst.piece_context_.deep_copy_to(piece_context_))) {
    CLOG_LOG(WARN, "deep copy to piece context failed", K(ret));
    piece_context_.reset_locate_info();
  } else {
    CLOG_LOG(TRACE, "update logcate info succ", KPC(this));
  }
  return ret;
}

// =========================== ObRemoteRawPathParent ============================== //
ObRemoteRawPathParent::ObRemoteRawPathParent(const share::ObLSID &ls_id) :
  ObRemoteLogParent(ObLogArchiveSourceType::RAWPATH, ls_id),
  paths_(),
  piece_index_(0),
  min_file_id_(0),
  max_file_id_(0)
{}

ObRemoteRawPathParent::~ObRemoteRawPathParent()
{
  to_end_ = false;
  upper_limit_ts_ = OB_INVALID_TIMESTAMP;
  end_fetch_log_ts_ = OB_INVALID_TIMESTAMP;
  end_lsn_.reset();
  piece_index_ = 0;
  min_file_id_ = 0;
  max_file_id_ = 0;
}

void ObRemoteRawPathParent::get(DirArray &array, int64_t &end_log_ts)
{
  array.assign(paths_);
  end_log_ts = upper_limit_ts_;
}

int ObRemoteRawPathParent::set(DirArray &array, const int64_t end_log_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(array.empty() || OB_INVALID_TIMESTAMP == end_log_ts)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    paths_.assign(array);
    upper_limit_ts_ = end_log_ts;
  }
  CLOG_LOG(INFO, "add_source dest", KPC(this));
  return ret;
}

int ObRemoteRawPathParent::deep_copy_to(ObRemoteLogParent &other)
{
  int ret = OB_SUCCESS;
  ObRemoteRawPathParent &dst = static_cast<ObRemoteRawPathParent &>(other);
  if (OB_FAIL(dst.paths_.assign(paths_))) {
    CLOG_LOG(WARN, "dir array assign failed", K(ret), KPC(this));
  } else {
    dst.piece_index_ = piece_index_;
    dst.min_file_id_ = min_file_id_;
    dst.max_file_id_ = max_file_id_;
    base_copy_to_(other);
  }
  return ret;
}

bool ObRemoteRawPathParent::is_valid() const
{
  return is_valid_log_source_type(type_)
    && OB_INVALID_TIMESTAMP != upper_limit_ts_
    && ! paths_.empty();
}

void ObRemoteRawPathParent::get_locate_info(int64_t &piece_index,
    int64_t &min_file_id,
    int64_t &max_file_id) const
{
  piece_index= piece_index_;
  min_file_id = min_file_id_;
  max_file_id = max_file_id_;
}
// =========================== ObRemoteSourceGuard ==============================//
ObRemoteSourceGuard::ObRemoteSourceGuard() :
  source_(NULL)
{}

ObRemoteSourceGuard::~ObRemoteSourceGuard()
{
  if (NULL != source_) {
    ObResSrcAlloctor::free(source_);
    source_ = NULL;
  }
}

int ObRemoteSourceGuard::set_source(ObRemoteLogParent *source)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "source is NULL", K(ret), K(source));
  } else {
    source_ = source;
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
