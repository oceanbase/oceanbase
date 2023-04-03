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

#ifndef OCEANBASE_LIBOBCDC_SERVER_BLACKLIST_H_
#define OCEANBASE_LIBOBCDC_SERVER_BLACKLIST_H_

#include "lib/container/ob_array.h"     // ObArray
#include "lib/net/ob_addr.h"            // ObAddr
#include "lib/lock/ob_spin_rwlock.h"    // SpinRWLock, SpinRLockGuard, SpinWLockGuard
#include "ob_log_utils.h"               // _K_

namespace oceanbase
{
namespace libobcdc
{
class ObLogSvrBlacklist
{
public:
  ObLogSvrBlacklist();
  ~ObLogSvrBlacklist();

  int init(const char *svrs_list_str,
      const bool is_sql_server);
  void destroy();

  // get current svr count
  int64_t count() const;
  /// Determine if the server is available: if it is on the server blacklist, it is not available; otherwise it is available
  ///
  /// @param [in] svr      query server
  ///
  /// @retval true         not exist in blacklist
  /// @retval false        exist
  bool is_exist(const common::ObAddr &svr) const;
  ///  Parsing server_blacklist in the configuration file
  ///
  /// @param [in] svrs_list_str    server blacklist string
  ///
  void refresh(const char *svrs_list_str);

private:
  typedef common::ObArray<common::ObAddr> ExceptionalSvrArray;
  static const int64_t MAX_SVR_BUF_SIZE = 4 * _K_;

  int set_svrs_str_(const char *svrs_list_str,
      const int64_t cur_svr_list_idx);
  int build_svr_patterns_(char *svrs_buf,
      ExceptionalSvrArray &exceptional_svrs);

  int64_t get_cur_svr_list_idx_() const;
  int64_t get_bak_cur_svr_list_idx_() const;
  void switch_svr_list_();

private:
  bool is_inited_;
  bool is_sql_server_;
  mutable common::SpinRWLock lock_;
  char *svrs_buf_[2];
  int64_t svrs_buf_size_;

  int64_t cur_svr_list_idx_;
  ExceptionalSvrArray svrs_[2];

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSvrBlacklist);
};

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_SERVER_BLACKLIST_H_ */
