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

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_RPC_PROCESSOR
#define OCEANBASE_LOGSERVICE_OB_CDC_RPC_PROCESSOR

#include "ob_cdc_rpc_proxy.h"
#include "ob_cdc_service.h"
#include "rpc/obrpc/ob_rpc_processor.h"

namespace oceanbase
{
namespace obrpc
{
int __get_cdc_service(uint64_t tenant_id, cdc::ObCdcService *&cdc_service);

class ObCdcLSReqStartLSNByTsP : public
  obrpc::ObRpcProcessor<obrpc::ObCdcProxy::ObRpc<obrpc::OB_LOG_REQ_START_LSN_BY_TS> >
{
public:
  ObCdcLSReqStartLSNByTsP() {}
  ~ObCdcLSReqStartLSNByTsP() {}
protected:
  int process();
};

class ObCdcLSFetchLogP : public
  obrpc::ObRpcProcessor<obrpc::ObCdcProxy::ObRpc<obrpc::OB_LS_FETCH_LOG2> >
{
public:
  ObCdcLSFetchLogP() {}
  ~ObCdcLSFetchLogP() {}
protected:
  int process();
};

class ObCdcLSFetchMissingLogP : public
  obrpc::ObRpcProcessor<obrpc::ObCdcProxy::ObRpc<obrpc::OB_LS_FETCH_MISSING_LOG> >
{
public:
  ObCdcLSFetchMissingLogP() {}
  ~ObCdcLSFetchMissingLogP() {}
protected:
  int process();
};

} // namespace obrpc
} // namespace oceanbase

#endif
