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

class ObTableLoadFinishP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_FINISH> >
{
public:
  explicit ObTableLoadFinishP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadFinishP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadFinishP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

class ObTableLoadPreMergePeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_PRE_MERGE_PEER> >
{
  typedef obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_PRE_MERGE_PEER> > ParentType;
public:
  explicit ObTableLoadPreMergePeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadPreMergePeerP() = default;

protected:
  int deserialize() override;
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadPreMergePeerP);
private:
  const ObGlobalContext &gctx_;
  common::ObArenaAllocator allocator_;
  table::ObTableApiCredential credential_;
};

class ObTableLoadStartMergePeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_START_MERGE_PEER> >
{
public:
  explicit ObTableLoadStartMergePeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadStartMergePeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadStartMergePeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

} // namespace observer
} // namespace oceanbase
