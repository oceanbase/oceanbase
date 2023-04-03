// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

#pragma once

#include "observer/table/ob_table_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/table/ob_table_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{

class ObTableLoadGetStatusP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_GET_STATUS> >
{
public:
  explicit ObTableLoadGetStatusP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadGetStatusP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadGetStatusP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

class ObTableLoadGetStatusPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_GET_STATUS_PEER> >
{
public:
  explicit ObTableLoadGetStatusPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadGetStatusPeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadGetStatusPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

} // namespace observer
} // namespace oceanbase
