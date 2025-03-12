/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_i_table_struct.h"
#include "ob_table_group_execute.h"
#include "ob_table_tenant_group.h"

#define USING_LOG_PREFIX SERVER

namespace oceanbase
{

namespace table
{
int ObITableGroupValue::init(const ObTableGroupCtx &ctx)
{
  int ret = OB_SUCCESS;
  // init group info
  group_info_.client_addr_ = GCTX.self_addr();
  group_info_.tenant_id_ = MTL_ID();
  group_info_.table_id_ = ctx.table_id_;
  group_info_.ls_id_ = ctx.ls_id_;
  group_info_.schema_version_ = ctx.schema_version_;
  group_info_.queue_size_ = get_group_size();
  group_info_.batch_size_ = TABLEAPI_GROUP_COMMIT_MGR->get_group_size();
  group_info_.gmt_created_ = ObClockGenerator::getClock();
  group_info_.gmt_modified_ = group_info_.gmt_created_;
  if (OB_FAIL(group_info_.set_group_type(ctx.type_))) {
    LOG_WARN("fail to set group type", K(ret));
  }
  return ret;
}

int ObTableGroupInfo::set_group_type(const ObTableOperationType::Type op_type)
{
  int ret = OB_SUCCESS;
  switch (op_type) {
    case ObTableOperationType::Type::PUT:
      group_type_ = GroupType::PUT;
      break;
    case ObTableOperationType::Type::GET:
      group_type_ = GroupType::GET;
      break;
    case ObTableOperationType::Type::INSERT:
      group_type_ = GroupType::INSERT;
      break;
    case ObTableOperationType::Type::DEL:
      group_type_ = GroupType::DEL;
      break;
    case ObTableOperationType::Type::REPLACE:
      group_type_ = GroupType::REPLACE;
      break;
    case ObTableOperationType::Type::INSERT_OR_UPDATE:
    case ObTableOperationType::Type::UPDATE:
    case ObTableOperationType::Type::INCREMENT:
    case ObTableOperationType::Type::APPEND:
      group_type_ = GroupType::HYBIRD;
      break;
    case ObTableOperationType::REDIS:
      group_type_ = GroupType::REDIS;
      break;
    case ObTableOperationType::Type::INVALID:
      group_type_ = GroupType::FAIL;
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unknown op_type", K(ret), K(op_type));
      break;
  }
  return ret;
}

const char* ObTableGroupInfo::get_group_type_str()
{
  const char* group_type_str = "UNKNOWN";
  switch(group_type_) {
    case GroupType::PUT:
      group_type_str = "PUT";
      break;
    case GroupType::GET:
      group_type_str = "GET";
      break;
    case GroupType::INSERT:
      group_type_str = "INSERT";
      break;
    case GroupType::REPLACE:
      group_type_str = "REPLACE";
      break;
    case GroupType::DEL:
      group_type_str = "DELETE";
      break;
    case GroupType::HYBIRD:
      group_type_str = "HYBIRD";
      break;
    case GroupType::FAIL:
      group_type_str = "FAIL";
      break;
    case GroupType::REDIS:
      group_type_str = "REDIS";
    default:
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "unknown group type", K(group_type_));
      break;
  }
  return group_type_str;
}

int ObITableOpProcessor::init(ObTableGroupCtx &group_ctx, ObIArray<ObITableOp*> *ops)
{
  int ret = OB_SUCCESS;
  group_ctx_ = &group_ctx;
  if (OB_ISNULL(ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ops is NULL", K(ret));
  } else {
    ops_ = ops;
    functor_ = group_ctx.create_cb_functor_;
    is_inited_ = true;
  }
  return ret;
}

} // end namespace table
} // end namespace oceanbase
