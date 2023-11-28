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

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_RPC_PROXY
#define OCEANBASE_LOGSERVICE_OB_CDC_RPC_PROXY

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "ob_cdc_req.h"
#include "ob_cdc_raw_log_req.h"

namespace oceanbase
{
namespace obrpc
{
class ObCdcReqStartLSNByTsReq;
class ObCdcReqStartLSNByTsResp;

class ObCdcLSFetchLogReq;
class ObCdcLSFetchLogResp;

// TODO deps/oblib/src/rpc/obrpc/ob_rpc_packet_list.h remove some rpc code
class ObCdcProxy : public ObRpcProxy
{
public:
  DEFINE_TO(ObCdcProxy);

  RPC_S(@PR5 req_start_lsn_by_ts, OB_LOG_REQ_START_LSN_BY_TS,
        (ObCdcReqStartLSNByTsReq), ObCdcReqStartLSNByTsResp);

  RPC_AP(@PR5 async_stream_fetch_log, OB_LS_FETCH_LOG2,
         (ObCdcLSFetchLogReq), ObCdcLSFetchLogResp);

  RPC_AP(@PR5 async_stream_fetch_miss_log, OB_LS_FETCH_MISSING_LOG,
         (ObCdcLSFetchMissLogReq), ObCdcLSFetchLogResp);

  RPC_AP(@PR5 async_stream_fetch_raw_log, OB_CDC_FETCH_RAW_LOG,
         (ObCdcFetchRawLogReq), ObCdcFetchRawLogResp);
};

} // namespace obrpc
} // namespace oceanbase

#endif
