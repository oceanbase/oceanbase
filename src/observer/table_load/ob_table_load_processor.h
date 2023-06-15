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

class ObTableLoadP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD> >
{
  typedef obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD> > ParentType;
public:
  explicit ObTableLoadP(const ObGlobalContext &gctx) : gctx_(gctx), allocator_("TLD_LoadP")
  {
    allocator_.set_tenant_id(MTL_ID());
  }
  virtual ~ObTableLoadP() = default;

protected:
  int deserialize() override;
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
  common::ObArenaAllocator allocator_;
};

class ObTableLoadPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_PEER> >
{
  typedef obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_PEER> > ParentType;
public:
  explicit ObTableLoadPeerP(const ObGlobalContext &gctx) : gctx_(gctx), allocator_("TLD_PLoadP")
  {
    allocator_.set_tenant_id(MTL_ID());
  }
  virtual ~ObTableLoadPeerP() = default;

protected:
  int deserialize() override;
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
  common::ObArenaAllocator allocator_;
};

}  // namespace observer
}  // namespace oceanbase
