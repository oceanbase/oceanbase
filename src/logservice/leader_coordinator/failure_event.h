/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef LOGSERVICE_COORDINATOR_FAILURE_EVENT_H
#define LOGSERVICE_COORDINATOR_FAILURE_EVENT_H

#include "lib/list/ob_dlist.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_table_access_helper.h"
#include <cstdint>
namespace oceanbase
{
namespace logservice
{
namespace coordinator
{

enum class FailureType
{
  INVALID_FAILURE = 0,
  FATAL_ERROR_OCCUR = 1,// 致命错误，通常是在防御性代码中发现了bug
  RESOURCE_NOT_ENOUGH = 2,// 资源不足，如磁盘与内存，通常为环境原因所致
  PROCESS_HANG = 3,// 流程阻塞，发现某主要流程一直不结束或者不断重试却不能成功
  MAJORITY_FAILURE = 4,// 多数派异常，如副本的网络与多数派断连
  SCHEMA_NOT_REFRESHED = 5, // sql may failed when tenant schema not refreshed yet
};

enum class FailureModule
{
  UNKNOWN_MODULE = 0,
  TENANT = 1,
  LOG = 2,
  TRANSACTION = 3,
  STORAGE = 4,
  SCHEMA = 5,
};

enum class FailureLevel
{
  UNKNOWN_LEVEL = 0,
  FATAL = 1,// 该级别的异常是非预期的，是在防御性代码中发现致命错误汇报上来的，具有比SERIOUS_FAILURE更高的切主优先级
  SERIOUS = 2,// 该级别的异常是预期内的，常为环境因素所致，出现此种异常将触发切主
  NOTICE = 3,// 该级别的异常只用作展示，在内部表中显示出来展示给用户，但是不会影响选举的优先级，也不会触发切主
};

inline const char *obj_to_cstring(FailureType type)
{
  const char *ret = "INVALID";
  switch (type) {
    case FailureType::FATAL_ERROR_OCCUR:
      ret = "FATAL ERROR OCCUR";
      break;
    case FailureType::RESOURCE_NOT_ENOUGH:
      ret = "RESOURCE NOT ENOUGH";
      break;
    case FailureType::PROCESS_HANG:
      ret = "PROCESS HANG";
      break;
    case FailureType::MAJORITY_FAILURE:
      ret = "MAJORITY FAILURE";
      break;
    case FailureType::SCHEMA_NOT_REFRESHED:
      ret = "SCHEMA NOT REFRESHED";
      break;
    default:
      break;
  }
  return ret;
}

inline const char *obj_to_cstring(FailureModule module)
{
  const char *ret = "UNKNOWN";
  switch (module) {
    case FailureModule::LOG:
      ret = "LOG";
      break;
    case FailureModule::TENANT:
      ret = "TENANT";
      break;
    case FailureModule::TRANSACTION:
      ret = "TRANSACTION";
      break;
    case FailureModule::STORAGE:
      ret = "STORAGE";
      break;
    case FailureModule::SCHEMA:
      ret = "SCHEMA";
      break;
    default:
      break;
  }
  return ret;
}

inline const char *obj_to_cstring(FailureLevel level)
{
  const char *ret = "UNKNOWN";
  switch (level) {
    case FailureLevel::FATAL:
      ret = "FATAL";
      break;
    case FailureLevel::SERIOUS:
      ret = "SERIOUS";
      break;
    case FailureLevel::NOTICE:
      ret = "NOTICE";
      break;
    default:
      break;
  }
  return ret;
}


class FailureEvent
{
  OB_UNIS_VERSION(1);
public:
  FailureEvent() :
  type_(FailureType::INVALID_FAILURE),
  module_(FailureModule::UNKNOWN_MODULE),
  level_(FailureLevel::UNKNOWN_LEVEL) {}
  FailureEvent(FailureType type, FailureModule module, FailureLevel level = FailureLevel::SERIOUS) :
  type_(type),
  module_(module),
  level_(level) {}
  FailureLevel get_failure_level() const { return level_; }
  FailureModule get_failure_module() const { return module_; }
  int set_info(const ObString &info) {
    return info_.assign(info);
  }
  int assign(const FailureEvent &rhs) {
    type_ = rhs.type_;
    module_ = rhs.module_;
    level_ = rhs.level_;
    return info_.assign(rhs.info_);
  }
  bool operator==(const FailureEvent &rhs) {
    bool ret = false;
    if (type_ == rhs.type_ && module_ == rhs.module_ && level_ == rhs.level_) {
      ret = (0 == info_.get_ob_string().case_compare(rhs.info_.get_ob_string()));
    }
    return ret;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos, "{type:%s, ", obj_to_cstring(type_));
    common::databuff_printf(buf, buf_len, pos, "module:%s, ", obj_to_cstring(module_));
    common::databuff_printf(buf, buf_len, pos, "info:%s, ", to_cstring(info_));
    common::databuff_printf(buf, buf_len, pos, "level:%s}", obj_to_cstring(level_));
    return pos;
  }
public:
  FailureType type_;
  FailureModule module_;
  FailureLevel level_;
  common::ObStringHolder info_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, FailureEvent, type_, module_, level_, info_);

}
}
}

#endif
