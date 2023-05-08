/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_FETCHER_PROGRESS_INFO_H_
#define OCEANBASE_LOG_FETCHER_PROGRESS_INFO_H_

#include "logservice/common_util/ob_log_ls_define.h"  // logservice::TenantLSID
#include "ob_log_ls_fetch_ctx.h"  // LSFetchCtx

namespace oceanbase
{
namespace logfetcher
{
struct LSProgressInfo
{
  LSProgressInfo() : tls_id_(), progress_(0) {}
  LSProgressInfo(const logservice::TenantLSID &tls_id, const int64_t progress) : tls_id_(tls_id), progress_(progress) {}

  logservice::TenantLSID tls_id_;
  int64_t progress_;
  TO_STRING_KV(K_(tls_id), K_(progress));
};
// Used to diagnosis and monitoring
typedef common::ObSEArray<LSProgressInfo, 16> LSProgressInfoArray;

struct FetchCtxMapHBFunc
{
  FetchCtxMapHBFunc(const bool print_ls_heartbeat_info);
  bool operator()(const logservice::TenantLSID &tls_id, LSFetchCtx *&ctx);

  bool                    print_ls_heartbeat_info_;
  int64_t                 data_progress_;
  int64_t                 ddl_progress_;
  palf::LSN               ddl_last_dispatch_log_lsn_;
  int64_t                 min_progress_;
  int64_t                 max_progress_;
  logservice::TenantLSID              min_progress_ls_;
  logservice::TenantLSID              max_progress_ls_;
  int64_t                 ls_count_;
  LSProgressInfoArray     ls_progress_infos_;

  TO_STRING_KV(K_(data_progress),
      K_(ddl_last_dispatch_log_lsn),
      K_(min_progress),
      K_(max_progress),
      K_(min_progress_ls),
      K_(max_progress_ls),
      K_(ls_count));
};

typedef FetchCtxMapHBFunc ProgressInfo;

} // namespace logfetcher
} // namespace oceanbase

#endif
