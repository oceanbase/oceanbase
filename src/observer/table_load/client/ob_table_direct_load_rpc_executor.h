/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "ob_table_direct_load_exec_context.h"
#include "ob_table_direct_load_rpc_proxy.h"
#include "share/table/ob_table_load_row_array.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadClientTaskParam;
class ObTableLoadClientTask;
class ObTableLoadTableCtx;

template <table::ObTableDirectLoadOperationType pcode>
class ObTableDirectLoadRpcExecutor
  : public ObTableLoadRpcExecutor<ObTableDirectLoadRpcProxy::ObTableDirectLoadRpc<pcode>>
{
  typedef ObTableLoadRpcExecutor<ObTableDirectLoadRpcProxy::ObTableDirectLoadRpc<pcode>> ParentType;

public:
  ObTableDirectLoadRpcExecutor(ObTableDirectLoadExecContext &ctx,
                               const table::ObTableDirectLoadRequest &request,
                               table::ObTableDirectLoadResult &result)
    : ParentType(request, result), ctx_(ctx)
  {
  }
  virtual ~ObTableDirectLoadRpcExecutor() = default;

protected:
  int deserialize() override { return this->request_.get_arg(this->arg_); }
  int set_result_header() override
  {
    this->result_.header_.operation_type_ = pcode;
    return OB_SUCCESS;
  }
  int serialize() override { return this->result_.set_res(this->res_, ctx_.get_allocator()); }

protected:
  ObTableDirectLoadExecContext &ctx_;
};

// begin
class ObTableDirectLoadBeginExecutor
  : public ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::BEGIN>
{
  typedef ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::BEGIN> ParentType;

public:
  ObTableDirectLoadBeginExecutor(ObTableDirectLoadExecContext &ctx,
                                 const table::ObTableDirectLoadRequest &request,
                                 table::ObTableDirectLoadResult &result)
    : ParentType(ctx, request, result)
  {
  }
  virtual ~ObTableDirectLoadBeginExecutor() = default;

protected:
  int check_args() override;
  int set_result_header() override;
  int process() override;

private:
  int resolve_param(ObTableLoadClientTaskParam &param);
};

// commit
class ObTableDirectLoadCommitExecutor
  : public ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::COMMIT>
{
  typedef ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::COMMIT> ParentType;

public:
  ObTableDirectLoadCommitExecutor(ObTableDirectLoadExecContext &ctx,
                                  const table::ObTableDirectLoadRequest &request,
                                  table::ObTableDirectLoadResult &result)
    : ParentType(ctx, request, result)
  {
  }
  virtual ~ObTableDirectLoadCommitExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// abort
class ObTableDirectLoadAbortExecutor
  : public ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::ABORT>
{
  typedef ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::ABORT> ParentType;

public:
  ObTableDirectLoadAbortExecutor(ObTableDirectLoadExecContext &ctx,
                                 const table::ObTableDirectLoadRequest &request,
                                 table::ObTableDirectLoadResult &result)
    : ParentType(ctx, request, result)
  {
  }
  virtual ~ObTableDirectLoadAbortExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// get_status
class ObTableDirectLoadGetStatusExecutor
  : public ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::GET_STATUS>
{
  typedef ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::GET_STATUS>
    ParentType;

public:
  ObTableDirectLoadGetStatusExecutor(ObTableDirectLoadExecContext &ctx,
                                     const table::ObTableDirectLoadRequest &request,
                                     table::ObTableDirectLoadResult &result)
    : ParentType(ctx, request, result)
  {
  }
  virtual ~ObTableDirectLoadGetStatusExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// insert
class ObTableDirectLoadInsertExecutor
  : public ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::INSERT>
{
  typedef ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::INSERT> ParentType;

public:
  ObTableDirectLoadInsertExecutor(ObTableDirectLoadExecContext &ctx,
                                  const table::ObTableDirectLoadRequest &request,
                                  table::ObTableDirectLoadResult &result)
    : ParentType(ctx, request, result)
  {
  }
  virtual ~ObTableDirectLoadInsertExecutor() = default;

protected:
  int check_args() override;
  int process() override;

private:
  static int decode_payload(const common::ObString &payload,
                            table::ObTableLoadObjRowArray &obj_row_array);
};

// heart_beat
class ObTableDirectLoadHeartBeatExecutor
  : public ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::HEART_BEAT>
{
  typedef ObTableDirectLoadRpcExecutor<table::ObTableDirectLoadOperationType::HEART_BEAT>
    ParentType;

public:
  ObTableDirectLoadHeartBeatExecutor(ObTableDirectLoadExecContext &ctx,
                                     const table::ObTableDirectLoadRequest &request,
                                     table::ObTableDirectLoadResult &result)
    : ParentType(ctx, request, result)
  {
  }
  virtual ~ObTableDirectLoadHeartBeatExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

} // namespace observer
} // namespace oceanbase
