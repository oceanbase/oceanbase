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
#include "share/table/redis/ob_redis_common.h"
#include "cmd/ob_redis_cmd.h"
#include "observer/table/redis/ob_redis_command_reg.h"

namespace oceanbase
{
namespace table
{
template <int TYPE>
struct GenRedisCommandHelper {
  static int generate(ObIAllocator &alloc,
                      const RedisCommandType &cmd_type,
                      const ObString &cmd_name,
                      const ObIArray<ObString> &args,
                      ObString& fmt_err_msg,
                      ObRedisOp *op,
                      RedisCommand *&cmd)
  {
    int ret = OB_SUCCESS;
    typedef typename ObRedisCommandTypeTraits<TYPE>::RedisCommand CommandType;
    // alloc buf
    void *buf = nullptr;
    const int64_t cmd_size = sizeof(CommandType);
    if (OB_NOT_NULL(op)) {
      if (cmd_size <= ObRedisOp::FIX_BUF_SIZE) {
        buf = op->fix_buf_;
      } else {
        if (OB_ISNULL(buf = op->get_inner_allocator().alloc(cmd_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SERVER_LOG(WARN, "fail to alloc redis command", K(ret), K(cmd_type), K(cmd_name));
        }
      }
    } else {
      if (OB_ISNULL(buf = alloc.alloc(cmd_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "fail to alloc redis command", K(ret), K(cmd_type), K(cmd_name));
      }
    }
    if (OB_SUCC(ret)) {
      ObIAllocator &cmd_alloc = OB_ISNULL(op) ? alloc : op->get_inner_allocator();
      CommandType *cmd_ptr = new (buf) CommandType(cmd_alloc);
      cmd_ptr->set_cmd_type(cmd_type);
      cmd_ptr->set_cmd_name(cmd_name);
      if (OB_FAIL(cmd_ptr->init(args, fmt_err_msg))) {
        SERVER_LOG(WARN, "command args is invalid", K(ret), K(args));
        cmd_ptr->~CommandType();
        if (OB_NOT_NULL(op)) {
          op->get_inner_allocator().free(buf);
        } else {
          alloc.free(buf);
        }
      } else {
        cmd = cmd_ptr;
      }
    }
    return ret;
  }
};

template <>
struct GenRedisCommandHelper<0> {
  static int generate(ObIAllocator &alloc,
                      const RedisCommandType &cmd_type,
                      const ObString &cmd_name,
                      const ObIArray<ObString> &args,
                      ObString& fmt_err_msg,
                      ObRedisOp *op,
                      RedisCommand *&cmd)
  {
    int ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "command not registered", K(ret));
    return ret;
  }
};

// CommandFactory
class ObRedisCommandFactory
{
public:
  static int gen_command(ObIAllocator &alloc,
                         const ObString &cmd_name,
                         const common::ObIArray<common::ObString> &args,
                         ObString& fmt_err_msg,
                         RedisCommand *&cmd);
  static int gen_group_command(ObRedisOp &redis_op,
                              const ObString &cmd_name,
                              const ObIArray<ObString> &args,
                              ObString& fmt_err_msg,
                              RedisCommand *&cmd);

  static inline bool is_registered(const RedisCommandType &cmd_type)
  {
    return cmd_type >= REDIS_COMMAND_INVALID && cmd_type < REDIS_COMMAND_MAX &&
           nullptr != G_REDIS_COMMAND_GEN_FUNCS_[cmd_type];
  }

  static int cmd_to_type(const ObString& cmd_name, RedisCommandType &cmd_type);
  static int cmd_is_support_group(const ObString &cmd_name, bool& is_support_group);

  using RedisCommandGenFunc = decltype(&GenRedisCommandHelper<0>::generate);
  using RedisCmdTypeEnableGroup = std::tuple<int, bool>;
  using CmdStrTypeMap =
      common::hash::ObHashMap<ObString, RedisCmdTypeEnableGroup, oceanbase::common::hash::NoPthreadDefendMode>;

private:
  static RedisCommandGenFunc *G_REDIS_COMMAND_GEN_FUNCS_;
  static CmdStrTypeMap *G_CMD_STR_TYPE_MAP_;

};

}  // namespace table
}  // namespace oceanbase
