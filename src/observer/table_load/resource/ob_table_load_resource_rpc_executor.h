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

#include "observer/table_load/resource/ob_table_load_resource_rpc_proxy.h"
#include "observer/table_load/resource/ob_table_load_resource_service.h"

namespace oceanbase
{
namespace observer
{

template <ObDirectLoadResourceCommandType pcode>
class ObTableLoadResourceRpcExecutor
  : public ObTableLoadRpcExecutor<ObTableLoadResourceRpcProxy::ObTableLoadResourceRpc<pcode>>
{
  typedef ObTableLoadRpcExecutor<ObTableLoadResourceRpcProxy::ObTableLoadResourceRpc<pcode>> ParentType;

public:
  ObTableLoadResourceRpcExecutor(common::ObIAllocator &allocator,
                                 const ObDirectLoadResourceOpRequest &request,
                                 ObDirectLoadResourceOpResult &result)
    : ParentType(request, result), allocator_(allocator)
  {
  }
  virtual ~ObTableLoadResourceRpcExecutor() = default;

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

// apply_resource
class ObDirectLoadResourceApplyExecutor
  : public ObTableLoadResourceRpcExecutor<ObDirectLoadResourceCommandType::APPLY>
{
  typedef ObTableLoadResourceRpcExecutor<ObDirectLoadResourceCommandType::APPLY> ParentType;

public:
  ObDirectLoadResourceApplyExecutor(common::ObIAllocator &allocator,
                                    const ObDirectLoadResourceOpRequest &request,
                                    ObDirectLoadResourceOpResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadResourceApplyExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// release_resource
class ObDirectLoadResourceReleaseExecutor
  : public ObTableLoadResourceRpcExecutor<ObDirectLoadResourceCommandType::RELEASE>
{
  typedef ObTableLoadResourceRpcExecutor<ObDirectLoadResourceCommandType::RELEASE> ParentType;

public:
  ObDirectLoadResourceReleaseExecutor(common::ObIAllocator &allocator,
                                      const ObDirectLoadResourceOpRequest &request,
                                      ObDirectLoadResourceOpResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadResourceReleaseExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// update_resource
class ObDirectLoadResourceUpdateExecutor
  : public ObTableLoadResourceRpcExecutor<ObDirectLoadResourceCommandType::UPDATE>
{
  typedef ObTableLoadResourceRpcExecutor<ObDirectLoadResourceCommandType::UPDATE> ParentType;

public:
  ObDirectLoadResourceUpdateExecutor(common::ObIAllocator &allocator,
                                     const ObDirectLoadResourceOpRequest &request,
                                     ObDirectLoadResourceOpResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadResourceUpdateExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

// check_resource
class ObDirectLoadResourceCheckExecutor
  : public ObTableLoadResourceRpcExecutor<ObDirectLoadResourceCommandType::CHECK>
{
  typedef ObTableLoadResourceRpcExecutor<ObDirectLoadResourceCommandType::CHECK> ParentType;

public:
  ObDirectLoadResourceCheckExecutor(common::ObIAllocator &allocator,
                                    const ObDirectLoadResourceOpRequest &request,
                                    ObDirectLoadResourceOpResult &result)
    : ParentType(allocator, request, result)
  {
  }
  virtual ~ObDirectLoadResourceCheckExecutor() = default;

protected:
  int check_args() override;
  int process() override;
};

} // namespace observer
} // namespace oceanbase
