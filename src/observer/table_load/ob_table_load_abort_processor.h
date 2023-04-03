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

class ObTableLoadAbortP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_ABORT> >
{
public:
  explicit ObTableLoadAbortP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadAbortP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadAbortP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
  sql::ObSQLSessionInfo session_info_;
};

class ObTableLoadAbortPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_ABORT_PEER> >
{
public:
  explicit ObTableLoadAbortPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadAbortPeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadAbortPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

} // namespace observer
} // namespace oceanbase
