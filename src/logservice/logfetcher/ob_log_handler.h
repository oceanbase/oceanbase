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

#ifndef OCEANBASE_LOG_FETCHER_LOG_HANDLER_H_
#define OCEANBASE_LOG_FETCHER_LOG_HANDLER_H_

#include "share/ob_ls_id.h"  // ObLSID
#include "ob_log_fetcher_switch_info.h"  // KickOutInfo
#include "ob_log_fetch_stat_info.h"  // TransStatInfo

namespace oceanbase
{
namespace logfetcher
{
class ILogFetcherHandler
{
public:
  // Implementation for handling the LogGroupEntry
  //
  // @param [in]  tenant_id       Tenant ID
  // @param [in]  ls_id           LS ID
  // @param [in]  proposal_id     proposal id
  // @param [in]  group_start_lsn LogGroupEntry start LSN
  // @param [in]  group_entry     LogGroupEntry Log
  // @param [in]  buffer          Buffer of the LogGroupEntry Log
  // @param [in]  ls_fetch_ctx    LogFetcher LSCtx
  // @param [out] KickOutInfo     Set the reason for the failure, and then LogFetcher will switch the machine
  //   and try fetching log for high availability and real-time.
  // @param [out] StatInfo        Some statistics
  //
  // @retval OB_SUCCESS         Handle the GroupLogEntry successfully
  // @retval OB_NEED_RETRY      For Physical standby, return OB_NEED_RETRY uniformly when the handle group entry failed.
  // such as:
  //     OB_NOT_MASTER(the leader of LS is switched)
  //     OB_EAGAIN(the sliding window is full)
  //     OB_LOG_OUTOF_DISK_SPACE
  //
  // @retval OB_IN_STOP_STATE   Stop and exit
  // @retval Other error codes  Fail
  virtual int handle_group_entry(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t proposal_id,
      const palf::LSN &group_start_lsn,
      const palf::LogGroupEntry &group_entry,
      const char *buffer,
      void *ls_fetch_ctx,
      KickOutInfo &kick_out_info,
      TransStatInfo &tsi,
      volatile bool &stop_flag) = 0;
};

} // namespace logfetcher
} // namespace oceanbase

#endif
