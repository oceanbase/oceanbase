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
#include "ob_log_archive_piece_mgr.h"
#include "ob_remote_log_source_allocator.h"    // ObResSrcAlloctor
#include "share/ob_define.h"
#include <cstdint>

namespace oceanbase
{
using namespace palf;
using namespace share;
namespace logservice
{
// =========================== ObRemoteLogParent ==============================//
ObRemoteLogParent::ObRemoteLogParent(const ObLogRestoreSourceType &type, const share::ObLSID &ls_id) :
  ls_id_(ls_id),
  type_(type),
  upper_limit_scn_(),
  to_end_(false),
  end_fetch_scn_(),
  end_lsn_(),
  error_context_()
{}

ObRemoteLogParent::~ObRemoteLogParent()
{
  ls_id_.reset();
  type_ = ObLogRestoreSourceType::INVALID;
  upper_limit_scn_.reset();
  to_end_ = false;
  end_fetch_scn_.reset();
  end_lsn_.reset();
}

const char *ObRemoteLogParent::get_source_type_str(const ObLogRestoreSourceType &type) const
{
  return share::ObLogRestoreSourceItem::get_source_type_str(type);
}

bool ObRemoteLogParent::set_to_end(const SCN &scn)
{
  if (scn >= upper_limit_scn_) {
    to_end_ = true;
    end_fetch_scn_ = scn;
    CLOG_LOG(INFO, "set_to_end succ", KPC(this));
  }
  return to_end_;
}

void ObRemoteLogParent::base_copy_to_(ObRemoteLogParent &other)
{
  other.type_ = type_;
  other.upper_limit_scn_ = upper_limit_scn_;
  other.to_end_ = to_end_;
  other.end_fetch_scn_ = end_fetch_scn_;
  other.end_lsn_ = end_lsn_;
  other.error_context_ = error_context_;
}

bool ObRemoteLogParent::is_valid_() const
{
  return share::is_valid_log_source_type(type_)
    && ls_id_.is_valid()
    && upper_limit_scn_.is_valid();
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
  ObRemoteLogParent(ObLogRestoreSourceType::SERVICE, ls_id),
  attr_()
{}

ObRemoteSerivceParent::~ObRemoteSerivceParent()
{
}

int ObRemoteSerivceParent::set(const RestoreServiceAttr &attr, const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! attr.is_valid() || !end_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(end_scn), K(attr));
  } else if (attr == attr_ && end_scn == upper_limit_scn_) {
  } else if (!(attr == attr_) && OB_FAIL(attr_.assign(attr))) {
    CLOG_LOG(WARN, "attr assign failed", K(attr));
  } else {
    upper_limit_scn_ = end_scn;
    to_end_ = end_fetch_scn_ >= upper_limit_scn_;
    CLOG_LOG(INFO, "set service parent succ", KPC(this));
  }
  return ret;
}

void ObRemoteSerivceParent::get(RestoreServiceAttr *&attr, SCN &end_scn)
{
  attr = &attr_;
  end_scn = upper_limit_scn_;
}

int ObRemoteSerivceParent::deep_copy_to(ObRemoteLogParent &other)
{
  ObRemoteSerivceParent &dst = static_cast<ObRemoteSerivceParent &>(other);
  base_copy_to_(other);
  return dst.attr_.assign(attr_);
}

bool ObRemoteSerivceParent::is_valid() const
{
  return is_valid_() && attr_.is_valid();
}

// =========================== ObRemoteLocationParent ==============================//
ObRemoteLocationParent::ObRemoteLocationParent(const share::ObLSID &ls_id) :
  ObRemoteLogParent(ObLogRestoreSourceType::LOCATION, ls_id),
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
    SCN &end_scn)
{
  dest = &root_path_;
  piece_context = &piece_context_;
  end_scn = upper_limit_scn_;
}

int ObRemoteLocationParent::set(const share::ObBackupDest &dest, const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! dest.is_valid() || ! end_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(end_scn), K(dest));
  } else if (dest == root_path_) {
    if (end_scn < upper_limit_scn_ && SCN::max_scn() != upper_limit_scn_) {
      CLOG_LOG(WARN, "fetch log upper_limit_scn rollback", K(ret), K(end_scn), KPC(this));
    } else if (end_scn == upper_limit_scn_) {
      // skip
    } else {
      const SCN pre_upper_scn = upper_limit_scn_;
      upper_limit_scn_ = end_scn;
      to_end_ = end_fetch_scn_ >= upper_limit_scn_;
      CLOG_LOG(INFO, "upper limit ts increase", K(dest), K(pre_upper_scn), K(end_scn));
    }
  } else if (OB_FAIL(root_path_.deep_copy(dest))) {
    CLOG_LOG(WARN, "root path deep copy failed", K(ret), K(dest), KPC(this));
  } else if (OB_FAIL(piece_context_.init(ls_id_, root_path_))) {
    CLOG_LOG(WARN, "piece context init failed", K(ret), KPC(this));
  } else {
    upper_limit_scn_ = end_scn;
    to_end_ = end_fetch_scn_ >= upper_limit_scn_;
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
  } else if (OB_UNLIKELY(! dst.piece_context_.is_valid())) {
    CLOG_LOG(TRACE, "piece_context not valid, just skip", K(dst));
  } else if (OB_FAIL(dst.piece_context_.deep_copy_to(piece_context_))) {
    CLOG_LOG(WARN, "deep copy to piece context failed", K(ret));
    piece_context_.reset_locate_info();
  } else {
    CLOG_LOG(TRACE, "update locate info succ", KPC(this));
  }
  return ret;
}

// =========================== ObRemoteRawPathParent ============================== //
ObRemoteRawPathParent::ObRemoteRawPathParent(const share::ObLSID &ls_id) :
  ObRemoteLogParent(ObLogRestoreSourceType::RAWPATH, ls_id),
  raw_piece_ctx_()
{}

ObRemoteRawPathParent::~ObRemoteRawPathParent()
{
  to_end_ = false;
  upper_limit_scn_.reset();
  end_fetch_scn_.reset();
  end_lsn_.reset();
  raw_piece_ctx_.reset();
}

int ObRemoteRawPathParent::set(DirArray &array, const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(array.empty() || !end_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(raw_piece_ctx_.init(ls_id_, array))) {
    CLOG_LOG(WARN, "fail to init raw piece context");
  } else {
    upper_limit_scn_ = end_scn;
    to_end_ = end_fetch_scn_ >= end_scn;
  }
  CLOG_LOG(INFO, "add_source dest", KPC(this));
  return ret;
}

void ObRemoteRawPathParent::get(ObLogRawPathPieceContext *&raw_piece_ctx, SCN &end_scn)
{
  raw_piece_ctx = &raw_piece_ctx_;
  end_scn = upper_limit_scn_;
}

int ObRemoteRawPathParent::deep_copy_to(ObRemoteLogParent &other)
{
  int ret = OB_SUCCESS;
  ObRemoteRawPathParent &dst = static_cast<ObRemoteRawPathParent &>(other);
  if (OB_FAIL(raw_piece_ctx_.deep_copy_to(dst.raw_piece_ctx_))) {
    CLOG_LOG(WARN, "dir array assign failed", K(ret), KPC(this));
  } else {
    base_copy_to_(other);
  }
  return ret;
}

bool ObRemoteRawPathParent::is_valid() const
{
  return is_valid_log_source_type(type_)
    && upper_limit_scn_.is_valid()
    && raw_piece_ctx_.is_valid();
}

int ObRemoteRawPathParent::update_locate_info(ObRemoteLogParent &source)
{
  int ret = OB_SUCCESS;
  ObRemoteRawPathParent &dst = static_cast<ObRemoteRawPathParent &>(source);
  if (OB_UNLIKELY(! dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid rawpath parent", K(ret), K(dst));
  } else if (OB_FAIL(dst.raw_piece_ctx_.deep_copy_to(raw_piece_ctx_))) {
    CLOG_LOG(WARN, "fail to deep copy rawpath parent", K(ret));
    raw_piece_ctx_.reset();
  } else {
    CLOG_LOG(TRACE, "update raw path locate info succ", KPC(this));
  }
  return ret;
}

// =========================== ObRemoteSourceGuard ==============================//
ObRemoteSourceGuard::ObRemoteSourceGuard() :
  source_(NULL)
{}

ObRemoteSourceGuard::~ObRemoteSourceGuard()
{
  reset();
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

void ObRemoteSourceGuard::reset()
{
  if (NULL != source_) {
    ObResSrcAlloctor::free(source_);
    source_ = NULL;
  }
}

} // namespace logservice
} // namespace oceanbase
