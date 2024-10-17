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

#pragma once
#include "share/table/ob_redis_common.h"
#include "cmd/ob_redis_cmd.h"

namespace oceanbase
{
namespace table
{
// CommandFactory
class ObRedisCommandFactory
{
public:
  static int gen_command(ObIAllocator &alloc,
                         const RedisCommandType &cmd_type,
                         const common::ObIArray<common::ObString> &args,
                         RedisCommand *&cmd);

  static inline bool is_registered(const RedisCommandType &cmd_type)
  {
    return cmd_type >= REDIS_COMMAND_INVALID && cmd_type < REDIS_COMMAND_MAX &&
           nullptr != G_REDIS_COMMAND_GEN_FUNCS_[cmd_type];
  }

  static int cmd_to_type(const ObString& cmd_name, RedisCommandType &cmd_type);

  using RedisCommandGenFunc = decltype(&ObRedisCommandFactory::gen_command);
  using CmdStrTypeMap = common::hash::ObHashMap<ObString, int>;

private:
  static RedisCommandGenFunc *G_REDIS_COMMAND_GEN_FUNCS_;
  static CmdStrTypeMap *G_CMD_STR_TYPE_MAP_;

};

}  // namespace table
}  // namespace oceanbase
