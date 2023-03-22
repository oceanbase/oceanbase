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

class ObTableLoadCommitP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_COMMIT> >
{
public:
  explicit ObTableLoadCommitP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadCommitP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadCommitP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
  sql::ObSQLSessionInfo session_info_;
};

class ObTableLoadCommitPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_COMMIT_PEER> >
{
public:
  explicit ObTableLoadCommitPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadCommitPeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadCommitPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

} // namespace observer
} // namespace oceanbase
