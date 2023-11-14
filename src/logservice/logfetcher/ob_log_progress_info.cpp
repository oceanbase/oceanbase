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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_progress_info.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace logfetcher
{
FetchCtxMapHBFunc::FetchCtxMapHBFunc(const bool print_ls_heartbeat_info) :
    print_ls_heartbeat_info_(print_ls_heartbeat_info),
    data_progress_(OB_INVALID_TIMESTAMP),
    ddl_progress_(OB_INVALID_TIMESTAMP),
    ddl_last_dispatch_log_lsn_(),
    min_progress_(OB_INVALID_TIMESTAMP),
    max_progress_(OB_INVALID_TIMESTAMP),
    min_progress_ls_(),
    max_progress_ls_(),
    ls_count_(0),
    ls_progress_infos_()
{
}

bool FetchCtxMapHBFunc::operator()(const logservice::TenantLSID &tls_id, LSFetchCtx *&ctx)
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;
  int64_t progress = OB_INVALID_TIMESTAMP;
  PartTransDispatchInfo dispatch_info;
  palf::LSN last_dispatch_log_lsn;

  if (NULL == ctx) {
    // ctx is invalid, not processed
  } else if (OB_FAIL(ctx->get_dispatch_progress(progress, dispatch_info))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_ERROR("get_dispatch_progress fail", KR(ret), K(tls_id), KPC(ctx));
    } else {
      ret = OB_SUCCESS;
    }
  }
  // The progress returned by the fetch log context must be valid, and its progress value must be a valid value, underlined by the fetch log progress
  else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == progress)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("LS dispatch progress is invalid", KR(ret), K(progress), K(tls_id), KPC(ctx),
        K(dispatch_info));
  } else {
    last_dispatch_log_lsn = dispatch_info.last_dispatch_log_lsn_;

    if (tls_id.is_sys_log_stream()) {
      // Assuming only one DDL partition
      // Update the DDL partition
      ddl_progress_ = progress;
      ddl_last_dispatch_log_lsn_ = last_dispatch_log_lsn;
    } else {
      // Update data progress
      if (OB_INVALID_TIMESTAMP == data_progress_) {
        data_progress_ = progress;
      } else {
        data_progress_ = std::min(data_progress_, progress);
      }

      if (OB_FAIL(ls_progress_infos_.push_back(LSProgressInfo(tls_id, progress)))) {
        LOG_ERROR("ls_progress_infos_ push_back failed", KR(ret));
      }
    }

    // Update maximum and minimum progress
    if (OB_INVALID_TIMESTAMP == max_progress_ || progress > max_progress_) {
      max_progress_ = progress;
      max_progress_ls_ = tls_id;
    }

    if (OB_INVALID_TIMESTAMP == min_progress_ || progress < min_progress_) {
      min_progress_ = progress;
      min_progress_ls_ = tls_id;
    }

    ls_count_++;

    if (print_ls_heartbeat_info_) {
      _LOG_INFO("[STAT] [FETCHER] [HEARTBEAT] TLS=%s PROGRESS=%ld DISPATCH_LOG_LSN=%lu "
          "DATA_PROGRESS=%ld DDL_PROGRESS=%ld DDL_DISPATCH_LOG_LSN=%lu", to_cstring(tls_id),
          progress, last_dispatch_log_lsn.val_, data_progress_, ddl_progress_, ddl_last_dispatch_log_lsn_.val_);
    }
  }

  return bool_ret;
}

} // namespace logfetcher
} // namespace oceanbase
