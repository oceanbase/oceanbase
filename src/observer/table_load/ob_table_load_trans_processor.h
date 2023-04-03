// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "observer/table/ob_table_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/table/ob_table_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{

class ObTableLoadStartTransP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_START_TRANS> >
{
public:
  explicit ObTableLoadStartTransP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadStartTransP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadStartTransP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

class ObTableLoadPreStartTransPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_PRE_START_TRANS_PEER> >
{
public:
  explicit ObTableLoadPreStartTransPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadPreStartTransPeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadPreStartTransPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

class ObTableLoadConfirmStartTransPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_CONFIRM_START_TRANS_PEER> >
{
public:
  explicit ObTableLoadConfirmStartTransPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadConfirmStartTransPeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadConfirmStartTransPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

////////////////////////////////////////////////////////////////

class ObTableLoadFinishTransP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_FINISH_TRANS> >
{
public:
  explicit ObTableLoadFinishTransP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadFinishTransP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadFinishTransP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

class ObTableLoadPreFinishTransPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_PRE_FINISH_TRANS_PEER> >
{
public:
  explicit ObTableLoadPreFinishTransPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadPreFinishTransPeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadPreFinishTransPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

class ObTableLoadConfirmFinishTransPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_CONFIRM_FINISH_TRANS_PEER> >
{
public:
  explicit ObTableLoadConfirmFinishTransPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadConfirmFinishTransPeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadConfirmFinishTransPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

////////////////////////////////////////////////////////////////

class ObTableLoadAbandonTransP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_ABANDON_TRANS> >
{
public:
  explicit ObTableLoadAbandonTransP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadAbandonTransP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadAbandonTransP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

class ObTableLoadAbandonTransPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_ABANDON_TRANS_PEER> >
{
public:
  explicit ObTableLoadAbandonTransPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadAbandonTransPeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadAbandonTransPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

////////////////////////////////////////////////////////////////

class ObTableLoadGetTransStatusP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_GET_TRANS_STATUS> >
{
public:
  explicit ObTableLoadGetTransStatusP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadGetTransStatusP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadGetTransStatusP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

class ObTableLoadGetTransStatusPeerP : public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOAD_GET_TRANS_STATUS_PEER> >
{
public:
  explicit ObTableLoadGetTransStatusPeerP(const ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObTableLoadGetTransStatusPeerP() = default;

protected:
  int process() override;

private:
  int check_user_access(const ObString &credential_str);

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadGetTransStatusPeerP);
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
};

} // end namespace observer
} // end namespace oceanbase
