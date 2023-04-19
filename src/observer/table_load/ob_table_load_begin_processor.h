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
class ObTableLoadParam;
class ObTableLoadDDLParam;
class ObTableLoadTableCtx;

class ObTableLoadBeginP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_BEGIN> >
{
public:
  explicit ObTableLoadBeginP(const ObGlobalContext &gctx);
  virtual ~ObTableLoadBeginP();

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);
  int init_idx_array(const ObTableSchema *table_schema);
  int create_table_ctx(const ObTableLoadParam &param, const common::ObIArray<int64_t> &idx_array,
                       sql::ObSQLSessionInfo &session_info, ObTableLoadTableCtx *&table_ctx);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadBeginP);
private:
  const ObGlobalContext &gctx_;
  common::ObArenaAllocator allocator_;
  table::ObTableApiCredential credential_;
  common::ObArray<int64_t> idx_array_;
  ObTableLoadTableCtx *table_ctx_;
};

class ObTableLoadPreBeginPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_PRE_BEGIN_PEER> >
{
  typedef obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_PRE_BEGIN_PEER> > ParentType;
public:
  explicit ObTableLoadPreBeginPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadPreBeginPeerP() = default;

protected:
  int deserialize() override;
  int process() override;

private:
  int check_user_access(const ObString &credential_str);
  int create_table_ctx(const ObTableLoadParam &param, const ObTableLoadDDLParam &ddl_param,
                       ObTableLoadTableCtx *&table_ctx);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadPreBeginPeerP);
private:
  const ObGlobalContext &gctx_;
  common::ObArenaAllocator allocator_;
  table::ObTableApiCredential credential_;
};

class ObTableLoadConfirmBeginPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_CONFIRM_BEGIN_PEER> >
{
public:
  explicit ObTableLoadConfirmBeginPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadConfirmBeginPeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadConfirmBeginPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

} // namespace observer
} // namespace oceanbase
