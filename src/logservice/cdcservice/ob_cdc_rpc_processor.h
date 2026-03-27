/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

class ObCdcFetchRawLogP : public
  obrpc::ObRpcProcessor<obrpc::ObCdcProxy::ObRpc<obrpc::OB_CDC_FETCH_RAW_LOG> >
{
public:
  ObCdcFetchRawLogP() {}
  ~ObCdcFetchRawLogP() {}
protected:
  int process();
};

} // namespace obrpc
} // namespace oceanbase

#endif
