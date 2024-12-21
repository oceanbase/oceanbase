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
#include "ob_redis_command_factory.h"
#include "share/datum/ob_datum_util.h"
#include "ob_redis_command_reg.h"
#include "share/table/redis/ob_redis_common.h"
#include "observer/table/group/ob_table_tenant_group.h"

using namespace oceanbase::table;
using namespace oceanbase::common;

static ObRedisCommandFactory::RedisCommandGenFunc G_REDIS_COMMAND_GEN_FUNCS_ARRAY[REDIS_COMMAND_MAX];
static_assert(
    REDIS_COMMAND_MAX == ARRAYSIZEOF(G_REDIS_COMMAND_GEN_FUNCS_ARRAY),
    "redis command gen function array is too small");

ObRedisCommandFactory::RedisCommandGenFunc *ObRedisCommandFactory::G_REDIS_COMMAND_GEN_FUNCS_ =
    G_REDIS_COMMAND_GEN_FUNCS_ARRAY;

static ObRedisCommandFactory::CmdStrTypeMap G_CMD_STR_TYPE_MAP_HOLD;
ObRedisCommandFactory::CmdStrTypeMap *ObRedisCommandFactory::G_CMD_STR_TYPE_MAP_ = &G_CMD_STR_TYPE_MAP_HOLD;

template <int N>
struct RedisCommandGenInitFunc {
  static void init_array()
  {
    static constexpr int registered = ObRedisCommandTypeTraits<N>::registered_;
    static constexpr bool is_support_group = ObRedisCommandTypeTraits<N>::is_support_group_;

    G_REDIS_COMMAND_GEN_FUNCS_ARRAY[N] = &GenRedisCommandHelper<N * registered>::generate;
    int ret = OB_SUCCESS;
    if (!G_CMD_STR_TYPE_MAP_HOLD.created() &&
        OB_FAIL(G_CMD_STR_TYPE_MAP_HOLD.create(REDIS_COMMAND_MAX, "RedisTypeMap", "RedisTypeMap"))) {
      LOG_WARN("redis init cmd_str_type map failed!", K(ret), K(REDIS_COMMAND_MAX));
    } else if (OB_FAIL(G_CMD_STR_TYPE_MAP_HOLD.set_refactored(
                   ObString::make_string(ObRedisCommandTypeTraits<N>::cmd_name_),
                   {N, is_support_group}))) {
      LOG_WARN(
          "cmd_str_type map set failed!",
          K(ret),
          K(ObString::make_string(ObRedisCommandTypeTraits<N>::cmd_name_)),
          K(N));
    }
  }
};

bool G_REDIS_COMMAND_FUNC_SET = ObArrayConstIniter<REDIS_COMMAND_MAX, RedisCommandGenInitFunc>::init();

int ObRedisCommandFactory::gen_command(ObIAllocator &alloc,
                                       const ObString &cmd_name,
                                       const ObIArray<ObString> &args,
                                       ObString& fmt_err_msg,
                                       RedisCommand *&cmd)

{
  int ret = OB_SUCCESS;
  RedisCommandType cmd_type = RedisCommandType::REDIS_COMMAND_INVALID;
  if (OB_FAIL(cmd_to_type(cmd_name, cmd_type))) {
    LOG_WARN("fail to get redis command type", K(ret), K(cmd_name));
  } else if (!is_registered(cmd_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("redis command is not supported", K(ret), K(cmd_type));
  } else if (OB_FAIL(G_REDIS_COMMAND_GEN_FUNCS_[cmd_type](alloc, cmd_type, cmd_name, args, fmt_err_msg, nullptr, cmd))) {
    LOG_WARN("fail to alloc redis command", K(ret), K(cmd_type));
  } else if (OB_ISNULL(cmd)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to gen redis command", K(ret), K(cmd_type));
  }

  return ret;
}

int ObRedisCommandFactory::gen_group_command(ObRedisOp &redis_op,
                                            const ObString &cmd_name,
                                            const ObIArray<ObString> &args,
                                            ObString& fmt_err_msg,
                                            RedisCommand *&cmd)

{
  int ret = OB_SUCCESS;
  RedisCommandType cmd_type = RedisCommandType::REDIS_COMMAND_INVALID;
  if (OB_FAIL(cmd_to_type(cmd_name, cmd_type))) {
    LOG_WARN("fail to get redis command type", K(ret), K(cmd_name));
  } else if (!is_registered(cmd_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("redis command is not supported", K(ret), K(cmd_type));
  } else if (OB_FAIL(G_REDIS_COMMAND_GEN_FUNCS_[cmd_type](
      TABLEAPI_GROUP_COMMIT_MGR->get_op_allocator(), cmd_type, cmd_name, args, fmt_err_msg, &redis_op, cmd))) {
    LOG_WARN("fail to alloc redis command", K(ret), K(cmd_type));
  } else if (OB_ISNULL(cmd)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to gen redis command", K(ret), K(cmd_type));
  }

  return ret;
}

int ObRedisCommandFactory::cmd_to_type(const ObString &cmd_name, RedisCommandType &cmd_type)
{
  int ret = OB_SUCCESS;
  RedisCmdTypeEnableGroup tmp_type_enbale_group = {REDIS_COMMAND_INVALID, false};
  if (OB_ISNULL(G_CMD_STR_TYPE_MAP_) || !G_CMD_STR_TYPE_MAP_->created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("redis command factory is not init map", K(ret), K(cmd_name));
  } else if (OB_FAIL(G_CMD_STR_TYPE_MAP_->get_refactored(cmd_name, tmp_type_enbale_group))) {
    LOG_WARN("redis method do not find type", K(ret), K(cmd_name));
  } else {
    cmd_type = static_cast<RedisCommandType>(std::get<0>(tmp_type_enbale_group));
  }

  return ret;
}

int ObRedisCommandFactory::cmd_is_support_group(const ObString &cmd_name, bool& is_support_group)
{
  int ret = OB_SUCCESS;

  RedisCmdTypeEnableGroup tmp_type_enbale_group = {REDIS_COMMAND_INVALID, false};
  if (OB_ISNULL(G_CMD_STR_TYPE_MAP_) || !G_CMD_STR_TYPE_MAP_->created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("redis command factory is not init map", K(ret), K(cmd_name));
  } else if (OB_FAIL(G_CMD_STR_TYPE_MAP_->get_refactored(cmd_name, tmp_type_enbale_group))) {
    LOG_WARN("redis method do not find type", K(ret), K(cmd_name));
  } else {
    is_support_group = std::get<1>(tmp_type_enbale_group);
  }

  return ret;
}
