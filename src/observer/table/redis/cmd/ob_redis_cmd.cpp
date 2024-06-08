/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER
#include "ob_redis_cmd.h"

namespace oceanbase
{
namespace table
{

int RedisCommand::init_common(const common::ObIArray<common::ObString> &args)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    // 1. check entity
    // redis arity refers to the total length including the command itself, for example, "set key value" arity = 3.
    // but args does not include the command itself, that is, only "key" and "value ", so + 1 is required.
    int argc = args.count() + 1;
    bool is_argc_valid = (attr_.arity_ >= 0 && argc == attr_.arity_) || (attr_.arity_ < 0 && argc >= (-attr_.arity_));
    if (!is_argc_valid) {
      ret = OB_INVALID_ARGUMENT_NUM;
      LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
    }
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
