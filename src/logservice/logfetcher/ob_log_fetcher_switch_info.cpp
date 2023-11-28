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

#include "ob_log_fetcher_switch_info.h"

namespace oceanbase
{
namespace logfetcher
{
const char *print_switch_reason(const KickOutReason reason)
{
  const char *str = "NONE";
  switch (reason) {
    case FETCH_LOG_FAIL_ON_RPC:
      str = "FetchLogFailOnRpc";
      break;

    case FETCH_LOG_FAIL_ON_SERVER:
      str = "FetchLogFailOnServer";
      break;

    case MISSING_LOG_FETCH_FAIL:
      str = "MissingLogFetchFail";
      break;

    case LAGGED_FOLLOWER:
      str = "LAGGED_FOLLOWER";
      break;

    case LOG_NOT_IN_THIS_SERVER:
      str = "LOG_NOT_IN_THIS_SERVER";
      break;

    case LS_OFFLINED:
      str = "LS_OFFLINED";
      break;

    case PROGRESS_TIMEOUT:
      str = "PROGRESS_TIMEOUT";
      break;

    case NEED_SWITCH_SERVER:
      str = "NeedSwitchServer";
      break;

    case DISCARDED:
      str = "Discarded";
      break;

    case ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF:
      str = "ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF";
      break;

    default:
      str = "NONE";
      break;
  }

  return str;
}

} // namespace logfetcher
} // namespace oceanbase
