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

#include "ob_table_load_control_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadParam;
class ObTableLoadDDLParam;
class ObTableLoadTableCtx;

template <ObDirectLoadControlCommandType pcode>
class ObTableLoadControlRpcExecutor
  : public ObTableLoadRpcExecutor<ObTableLoadControlRpcProxy::ObTableLoadControlRpc<pcode>>
{
  typedef ObTableLoadRpcExecutor<ObTableLoadControlRpcProxy::ObTableLoadControlRpc<pcode>>
    ParentType;

public:
  ObTableLoadControlRpcExecutor(common::ObIAllocator &allocator,
                                const ObDirectLoadControlRequest &request,
                                ObDirectLoadControlResult &result)
    : ParentType(request, result), allocator_(allocator)
  {
  }
  virtual ~ObTableLoadControlRpcExecutor() = default;

protected:
  int deserialize() override { return this->request_.get_arg(this->arg_); }
  int set_result_header() override
  {
    this->result_.command_type_ = pcode;
    return OB_SUCCESS;
  }
  int serialize() override { return this->result_.set_res(this->res_, allocator_); }

protected:
  common::ObIAllocator &allocator_;
};

// pre_begin
class ObDirectLoadControlPreBeginExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::PRE_BEGIN>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::PRE_BEGIN> ParentType;

public:
  ObDirectLoadControlPreBeginExecutor(common::ObIAllocator &allocator,
                                      const ObDirectLoadControlRequest &request,
                                      ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlPreBeginExecutor() = default;

private:
  int create_table_ctx(const ObTableLoadParam &param, const ObTableLoadDDLParam &ddl_param,
                       ObTableLoadTableCtx *&table_ctx);

protected:
  int deserialize() override;
  int check_args() override;
  int process() override;
};

// confirm_begin
class ObDirectLoadControlConfirmBeginExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::CONFIRM_BEGIN>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::CONFIRM_BEGIN> ParentType;

public:
  ObDirectLoadControlConfirmBeginExecutor(common::ObIAllocator &allocator,
                                          const ObDirectLoadControlRequest &request,
                                          ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlConfirmBeginExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// pre_merge
class ObDirectLoadControlPreMergeExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::PRE_MERGE>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::PRE_MERGE> ParentType;

public:
  ObDirectLoadControlPreMergeExecutor(common::ObIAllocator &allocator,
                                      const ObDirectLoadControlRequest &request,
                                      ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlPreMergeExecutor() = default;

protected:
  int deserialize() override;
  int check_args() override;
  int process() override;
};

// start_merge
class ObDirectLoadControlStartMergeExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::START_MERGE>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::START_MERGE> ParentType;

public:
  ObDirectLoadControlStartMergeExecutor(common::ObIAllocator &allocator,
                                        const ObDirectLoadControlRequest &request,
                                        ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlStartMergeExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// commit
class ObDirectLoadControlCommitExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::COMMIT>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::COMMIT> ParentType;

public:
  ObDirectLoadControlCommitExecutor(common::ObIAllocator &allocator,
                                    const ObDirectLoadControlRequest &request,
                                    ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlCommitExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// abort
class ObDirectLoadControlAbortExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::ABORT>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::ABORT> ParentType;

public:
  ObDirectLoadControlAbortExecutor(common::ObIAllocator &allocator,
                                   const ObDirectLoadControlRequest &request,
                                   ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlAbortExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// get_status
class ObDirectLoadControlGetStatusExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::GET_STATUS>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::GET_STATUS> ParentType;

public:
  ObDirectLoadControlGetStatusExecutor(common::ObIAllocator &allocator,
                                       const ObDirectLoadControlRequest &request,
                                       ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlGetStatusExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// heart_beat
class ObDirectLoadControlHeartBeatExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::HEART_BEAT>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::HEART_BEAT> ParentType;

public:
  ObDirectLoadControlHeartBeatExecutor(common::ObIAllocator &allocator,
                                       const ObDirectLoadControlRequest &request,
                                       ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlHeartBeatExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

/// trans
// pre_start_trans
class ObDirectLoadControlPreStartTransExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::PRE_START_TRANS>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::PRE_START_TRANS> ParentType;

public:
  ObDirectLoadControlPreStartTransExecutor(common::ObIAllocator &allocator,
                                           const ObDirectLoadControlRequest &request,
                                           ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlPreStartTransExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// confirm_start_trans
class ObDirectLoadControlConfirmStartTransExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::CONFIRM_START_TRANS>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::CONFIRM_START_TRANS>
    ParentType;

public:
  ObDirectLoadControlConfirmStartTransExecutor(common::ObIAllocator &allocator,
                                               const ObDirectLoadControlRequest &request,
                                               ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlConfirmStartTransExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// pre_finish_trans
class ObDirectLoadControlPreFinishTransExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::PRE_FINISH_TRANS>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::PRE_FINISH_TRANS>
    ParentType;

public:
  ObDirectLoadControlPreFinishTransExecutor(common::ObIAllocator &allocator,
                                            const ObDirectLoadControlRequest &request,
                                            ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlPreFinishTransExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// confirm_finish_trans
class ObDirectLoadControlConfirmFinishTransExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::CONFIRM_FINISH_TRANS>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::CONFIRM_FINISH_TRANS>
    ParentType;

public:
  ObDirectLoadControlConfirmFinishTransExecutor(common::ObIAllocator &allocator,
                                                const ObDirectLoadControlRequest &request,
                                                ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlConfirmFinishTransExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// abandon_trans
class ObDirectLoadControlAbandonTransExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::ABANDON_TRANS>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::ABANDON_TRANS> ParentType;

public:
  ObDirectLoadControlAbandonTransExecutor(common::ObIAllocator &allocator,
                                          const ObDirectLoadControlRequest &request,
                                          ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlAbandonTransExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// get_trans_status
class ObDirectLoadControlGetTransStatusExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::GET_TRANS_STATUS>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::GET_TRANS_STATUS>
    ParentType;

public:
  ObDirectLoadControlGetTransStatusExecutor(common::ObIAllocator &allocator,
                                            const ObDirectLoadControlRequest &request,
                                            ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlGetTransStatusExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// insert_trans
class ObDirectLoadControlInsertTransExecutor
  : public ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::INSERT_TRANS>
{
  typedef ObTableLoadControlRpcExecutor<ObDirectLoadControlCommandType::INSERT_TRANS> ParentType;

public:
  ObDirectLoadControlInsertTransExecutor(common::ObIAllocator &allocator,
                                         const ObDirectLoadControlRequest &request,
                                         ObDirectLoadControlResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadControlInsertTransExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

} // namespace observer
} // namespace oceanbase
