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

#include "share/table/redis/ob_redis_error.h"
#define USING_LOG_PREFIX SERVER

namespace oceanbase
{
namespace table
{
const char *ObRedisErr::EXPIRE_TIME_ERR = "-ERR invalid expire time in 'expire' command\r\n";
const char *ObRedisErr::SYNTAX_ERR = "-ERR syntax error\r\n";
const char *ObRedisErr::FLOAT_ERR = "-ERR value is not a valid float\r\n";
const char *ObRedisErr::INTEGER_ERR = "-ERR value is not an integer or out of range\r\n";
const char *ObRedisErr::NOT_POSITIVE_ERR = "-ERR value is out of range, must be positive\r\n";
const char *ObRedisErr::INCR_OVERFLOW = "-ERR increment or decrement would overflow\r\n";
const char *ObRedisErr::INTEGER_OUT_RANGE_ERR = "-ERR index out of range\r\n";
const char *ObRedisErr::BIT_INTEGER_ERR = "-ERR bit is not an integer or out of range\r\n";
const char *ObRedisErr::BIT_OFFSET_INTEGER_ERR = "-ERR bit offset is not an integer or out of range\r\n";
const char *ObRedisErr::MAXIMUM_ERR = "-ERR string exceeds maximum allowed size (1M)\r\n";
const char *ObRedisErr::WRONG_ARGS_ERR = "-ERR wrong number of arguments\r\n";
const char *ObRedisErr::WRONG_ARGS_LPOP_ERR = "-ERR wrong number of arguments for 'lpop' command\r\n";
const char *ObRedisErr::WRONG_ARGS_RPOP_ERR = "-ERR wrong number of arguments for 'rpop' command\r\n";
const char *ObRedisErr::EXPIRE_TIME_CMD_ERR = "-ERR invalid expire time in %.*s\r\n";
const char *ObRedisErr::INCR_OVERLOAD = "-ERR increment or decrement would overflow\r\n";
const char *ObRedisErr::HASH_VALUE_FLOAT_ERR = "-ERR hash value is not a float\r\n";
const char *ObRedisErr::MIN_MAX_FLOAT_ERR = "-ERR min or max is not a float\r\n";
const char *ObRedisErr::HASH_VALUE_ERR = "-ERR hash value is not an integer\r\n";
const char *ObRedisErr::INDEX_OVERFLOW_ERR = "-ERR index overflow\r\n";
const char *ObRedisErr::NO_SUCH_KEY_ERR = "-ERR no such key\r\n";
const char *ObRedisErr::OFFSET_OUT_RANGE_ERR = "-ERR offset is out of range\r\n";
const char *ObRedisErr::MSG_SIZE_OVERFLOW_ERR = "-ERR err_msg overflow\r\n";
const char *ObRedisErr::INF_ERR = "-ERR increment would produce NaN or Infinity\r\n";

void ObRedisErr::response_err_msg(
    int &res_ret,
    ObRedisResponse &response,
    const char *redis_err,
    int32_t args_count,
    ...)
{
  res_ret = OB_KV_REDIS_ERROR;
  int ret = OB_SUCCESS;
  ObString err_msg_str(redis_err);

  if (args_count > 0) {
    char buf[OB_MAX_ERROR_MSG_LEN] = {};
    va_list args;
    va_start(args, args_count);
    int64_t len = 0;
    len = vsnprintf(buf, OB_MAX_ERROR_MSG_LEN, redis_err, args);
    va_end(args);
    if (len < 0 || len >= OB_MAX_ERROR_MSG_LEN) {
      if (OB_FAIL(common::ob_write_string(response.get_allocator(), ObRedisErr::MSG_SIZE_OVERFLOW_ERR, err_msg_str))) {
        LOG_WARN("fail to write string", K(ret));
      }
    } else if (OB_FAIL(common::ob_write_string(response.get_allocator(), buf, err_msg_str, true))) {
      LOG_WARN("fail to write string", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(response.set_fmt_res(err_msg_str))) {
    LOG_WARN("fail to set_fmt_res", K(ret));
  }

  if (OB_FAIL(ret)) {
    res_ret = ret;
  }
  return;
}

void ObRedisErr::record_err_msg(int &res_ret, ObString &fmt_redis_msg, const char *redis_err)
{
  res_ret = OB_KV_REDIS_ERROR;
  fmt_redis_msg = redis_err;
}

void ObRedisErr::sprintf_err_msg(
    int &res_ret,
    ObIAllocator &alloc,
    ObString &fmt_redis_msg,
    const char *redis_err,
    int32_t args_count,
    ...)
{
  res_ret = OB_KV_REDIS_ERROR;
  int ret = OB_SUCCESS;
  ObString err_msg_str(redis_err);

  if (args_count > 0) {
    char buf[OB_MAX_ERROR_MSG_LEN] = {};
    va_list args;
    va_start(args, args_count);
    int64_t len = 0;
    len = vsnprintf(buf, OB_MAX_ERROR_MSG_LEN, redis_err, args);
    va_end(args);
    if (len < 0 || len >= OB_MAX_ERROR_MSG_LEN) {
      if (OB_FAIL(common::ob_write_string(alloc, ObRedisErr::MSG_SIZE_OVERFLOW_ERR, err_msg_str))) {
        LOG_WARN("fail to write string", K(ret));
      }
    } else if (OB_FAIL(common::ob_write_string(alloc, buf, err_msg_str, true))) {
      LOG_WARN("fail to write string", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    fmt_redis_msg = err_msg_str;
  } else {
    res_ret = ret;
  }

  return;
}
}  // end namespace table
}  // namespace oceanbase
