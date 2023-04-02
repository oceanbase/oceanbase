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
 *
 * Server Blacklist
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_svr_blacklist.h"

#include "share/ob_define.h"
#include "lib/string/ob_string.h"   // ObString
#include "ob_log_utils.h"           // ob_cdc_malloc

namespace oceanbase
{
namespace libobcdc
{
using namespace common;

ObLogSvrBlacklist::ObLogSvrBlacklist() :
    is_inited_(false),
    is_sql_server_(false),
    lock_(ObLatchIds::OBCDC_SVR_BLACKLIST_LOCK),
    svrs_buf_size_(0),
    cur_svr_list_idx_(0),
    svrs_()
{
  svrs_buf_[0] = NULL;
  svrs_buf_[1] = NULL;
}

ObLogSvrBlacklist::~ObLogSvrBlacklist()
{
  destroy();
}

int ObLogSvrBlacklist::init(const char *svrs_list_str,
    const bool is_sql_server)
{
  int ret = OB_SUCCESS;
  int64_t buf_size = MAX_SVR_BUF_SIZE;

  if (OB_UNLIKELY(is_inited_)) {
    LOG_ERROR("ObLogSvrBlacklist has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(svrs_list_str)) {
    LOG_ERROR("invalid argument", K(svrs_list_str));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(svrs_buf_[0] = reinterpret_cast<char*>(ob_cdc_malloc(buf_size)))) {
    LOG_ERROR("alloc svrs buffer 0 fail", KR(ret), K(buf_size));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(svrs_buf_[1] = reinterpret_cast<char*>(ob_cdc_malloc(buf_size)))) {
    LOG_ERROR("alloc svrs buffer 1 fail", KR(ret), K(buf_size));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    is_sql_server_ = is_sql_server;
    svrs_buf_size_ = MAX_SVR_BUF_SIZE;
    cur_svr_list_idx_ = 0;

    if (OB_FAIL(set_svrs_str_(svrs_list_str, cur_svr_list_idx_))) {
      LOG_ERROR("set_svrs_str_ fail", KR(ret), K(svrs_list_str), K(cur_svr_list_idx_));
    } else {
      is_inited_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    destroy();
  }

  return ret;
}

void ObLogSvrBlacklist::destroy()
{
  is_inited_ = false;
  is_sql_server_ = false;

  if (NULL != svrs_buf_[0]) {
    ob_cdc_free(svrs_buf_[0]);
    svrs_buf_[0] = NULL;
  }

  if (NULL != svrs_buf_[1]) {
    ob_cdc_free(svrs_buf_[1]);
    svrs_buf_[1] = NULL;
  }
  svrs_buf_size_ = 0;

  cur_svr_list_idx_ = 0;
  svrs_[0].reset();
  svrs_[1].reset();
}

int64_t ObLogSvrBlacklist::count() const
{
  // add read lock
  common::SpinRLockGuard RLockGuard(lock_);
  const int64_t cur_svr_list_idx = get_cur_svr_list_idx_();
  return svrs_[cur_svr_list_idx].count();
}

bool ObLogSvrBlacklist::is_exist(const common::ObAddr &svr) const
{
  // default not exist
  bool bool_ret = false;
  // add read lock
  common::SpinRLockGuard RLockGuard(lock_);
  const int64_t cur_svr_list_idx = get_cur_svr_list_idx_();
  const ExceptionalSvrArray &exceptional_svrs = svrs_[cur_svr_list_idx];
  bool has_done = false;

  for (int64_t idx = 0; ! has_done && idx < exceptional_svrs.count(); ++idx) {
    const ObAddr &exceptional_svr = exceptional_svrs.at(idx);

    if (svr == exceptional_svr) {
      bool_ret = true;
      has_done = true;
    }
  } // for

  return bool_ret;
}

void ObLogSvrBlacklist::refresh(const char *svrs_list_str)
{
  int ret = OB_SUCCESS;
  const int64_t cur_svr_list_idx = get_bak_cur_svr_list_idx_();

  if (! is_inited_) {
    // No update before initialisation
  } else {
    if (OB_FAIL(set_svrs_str_(svrs_list_str, cur_svr_list_idx))) {
      LOG_ERROR("set_svrs_str_ fail", KR(ret), K(svrs_list_str), K(cur_svr_list_idx));
    } else {
      // update successful, switch to new server blacklist
      // Add write lock
      common::SpinWLockGuard WLockGuard(lock_);
      switch_svr_list_();
    }
  }
}

int ObLogSvrBlacklist::set_svrs_str_(const char *svrs_list_str,
    const int64_t cur_svr_list_idx)
{
  int ret = OB_SUCCESS;
  ExceptionalSvrArray &exceptional_svrs = svrs_[cur_svr_list_idx];
  char *buffer = svrs_buf_[cur_svr_list_idx];
  const int64_t buffer_size = svrs_buf_size_;
  int64_t pos = 0;

  if (OB_ISNULL(svrs_list_str)) {
    LOG_ERROR("invalid argument", K(svrs_list_str));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(buffer)) {
    LOG_ERROR("buffer is NULL", K(buffer));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(databuff_printf(buffer, buffer_size, pos, "%s", svrs_list_str))) {
    LOG_ERROR("databuff_printf fail", KR(ret), K(buffer), K(buffer_size), K(pos), K(svrs_list_str));
  } else if (OB_FAIL(build_svr_patterns_(buffer, exceptional_svrs))) {
    LOG_ERROR("build_svr_patterns_ fail", KR(ret), K(cur_svr_list_idx), K(svrs_list_str),
        K(buffer), K(exceptional_svrs));
  } else {
    // succ
  }

  return ret;
}

int ObLogSvrBlacklist::build_svr_patterns_(char *svrs_buf,
    ExceptionalSvrArray &exceptional_svrs)
{
  int ret = OB_SUCCESS;
  const char pattern_delimiter = '|';
  exceptional_svrs.reset();

  if (OB_ISNULL(svrs_buf)) {
    LOG_ERROR("svrs_buf is NULL", K(svrs_buf));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObString remain(strlen(svrs_buf), svrs_buf);
    ObString cur_pattern;
    bool done = false;

    // No server blacklisting, no parsing required
    if (0 == strcmp(svrs_buf, "|")) {
      done = true;
    }

    while (OB_SUCC(ret) && ! done) {
      cur_pattern = remain.split_on(pattern_delimiter);

      if (cur_pattern.empty()) {
        cur_pattern = remain;
        done = true;
      }

      if (OB_SUCC(ret)) {
        ObString &str = cur_pattern;
        *(str.ptr() + str.length()) = '\0';
        str.set_length(1 + str.length());
      }

      if (OB_SUCC(ret)) {
        ObAddr svr;

        if (OB_FAIL(svr.parse_from_string(cur_pattern))) {
          LOG_ERROR("svr parse_from_string fail", KR(ret), K(cur_pattern));
        } else if (OB_UNLIKELY(! svr.is_valid())) {
          LOG_ERROR("svr is not valid", K(svr), K(cur_pattern));
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(exceptional_svrs.push_back(svr))) {
          LOG_ERROR("exceptional_svrs push_back svr fail", KR(ret), K(svr));
        } else {
          _LOG_INFO("[%sSERVER_BLACKLIST] [ADD] [SVR=%s] [SVR_CNT=%ld]",
              is_sql_server_ ? "SQL_": "", to_cstring(svr), exceptional_svrs.count());
        }
      }
    } // while
  }

  return ret;
}

int64_t ObLogSvrBlacklist::get_cur_svr_list_idx_() const
{
  return (ATOMIC_LOAD(&cur_svr_list_idx_)) % 2;
}

int64_t ObLogSvrBlacklist::get_bak_cur_svr_list_idx_() const
{
  return (ATOMIC_LOAD(&cur_svr_list_idx_) + 1) % 2;
}

void ObLogSvrBlacklist::switch_svr_list_()
{
  ATOMIC_INC(&cur_svr_list_idx_);
}

} // namespace libobcdc
} // namespace oceanbase
