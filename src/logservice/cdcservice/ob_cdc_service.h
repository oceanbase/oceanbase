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

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_
#define OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_

#include "ob_cdc_req.h"                         // RPC Request and Response
#include "ob_cdc_fetcher.h"                     // ObCdcFetcher
#include "ob_cdc_start_lsn_locator.h"

namespace oceanbase
{
namespace cdc
{
class ObCdcService
{
public:
  ObCdcService();
  ~ObCdcService();
  int init(const uint64_t tenant_id,
      ObLSService *ls_service);

public:
  int start();
  void stop();
  void wait();
  void destroy();
  bool is_stoped() const { return ATOMIC_LOAD(&stop_flag_); }

public:
  // The following interface is the entrance to service CDC-Connector,
  // and the internal implementation is to call the corresponding interface
  // of the corresponding component
  int req_start_lsn_by_ts_ns(const obrpc::ObCdcReqStartLSNByTsReq &req_msg,
      obrpc::ObCdcReqStartLSNByTsResp &resp);

  int fetch_log(const obrpc::ObCdcLSFetchLogReq &req,
      obrpc::ObCdcLSFetchLogResp &resp,
      const int64_t send_ts,
      const int64_t recv_ts);

  int fetch_missing_log(const obrpc::ObCdcLSFetchMissLogReq &req,
      obrpc::ObCdcLSFetchLogResp &resp,
      const int64_t send_ts,
      const int64_t recv_ts);

  TO_STRING_KV(K_(is_inited));

private:
  void do_monitor_stat_(const int64_t start_ts,
      const int64_t end_ts,
      const int64_t send_ts,
      const int64_t recv_ts);

private:
  bool is_inited_;
  volatile bool stop_flag_ CACHE_ALIGNED;
  ObCdcStartLsnLocator locator_;
  ObCdcFetcher fetcher_;
};

} // namespace cdc
} // namespace oceanbase

#endif
