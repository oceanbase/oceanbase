/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_RESTORE_LOG_FUNCTION_H_
#define OCEANBASE_LOGSERVICE_OB_RESTORE_LOG_FUNCTION_H_

#include "logservice/logfetcher/ob_log_fetch_stat_info.h"   // TransStatInfo
#include "logservice/palf/lsn.h"    // LSN
#include "logservice/palf/log_group_entry.h"   // LogGroupEntry
#include "logservice/logfetcher/ob_log_handler.h"   // ILogFetcherHandler
#include <cstdint>
namespace oceanbase
{
namespace share
{
struct ObLSID;
}
namespace storage
{
class ObLSService;
}
namespace logservice
{
class ObRestoreLogFunction : public logfetcher::ILogFetcherHandler
{
public:
  ObRestoreLogFunction();
  virtual ~ObRestoreLogFunction();
public:
  int init(const uint64_t tenant_id, storage::ObLSService *ls_svr);
  void destroy();
  void reset();

  virtual int handle_group_entry(
      const uint64_t tenant_id,
      const share::ObLSID &id,
      const int64_t proposal_id,
      const palf::LSN &group_start_lsn,
      const ipalf::IGroupEntry &group_entry,
      const char *buffer,
      void *ls_fetch_ctx,
      logfetcher::KickOutInfo &kick_out_info,
      logfetcher::TransStatInfo &tsi,
      volatile bool &stop_flag) override final;

private:
  int process_(const share::ObLSID &id,
      const int64_t proposal_id,
      const palf::LSN &lsn,
      const share::SCN &scn,
      const char *buf,
      const int64_t buf_size,
      volatile bool &stop_flag);

private:
  bool inited_;
  uint64_t tenant_id_;
  storage::ObLSService *ls_svr_;
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_RESTORE_LOG_FUNCTION_H_ */
