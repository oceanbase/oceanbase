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
#include "lib/string/ob_string.h"
#include "lib/string/ob_string_buffer.h"
#include "src/share/ob_errno.h"
#include "share/table/redis/ob_redis_util.h"
namespace oceanbase
{
namespace table
{
// Auxiliary macros used to calculate the number of passed parameters
#define COUNT_ARGS(...) COUNT_ARGS_IMPL(0, ##__VA_ARGS__, 5, 4, 3, 2, 1, 0)

// The expansion of auxiliary macros
#define COUNT_ARGS_IMPL(_0, _1, _2, _3, _4, _5, N, ...) N

#define RESPONSE_REDIS_ERROR(response, redis_err, ...)                               \
  do {                                                                               \
    int num_args = COUNT_ARGS(__VA_ARGS__);                                          \
    ObRedisErr::response_err_msg(ret, response, redis_err, num_args, ##__VA_ARGS__); \
  } while (0)

#define RECORD_REDIS_ERROR(fmt_redis_msg, redis_err)           \
  do {                                                         \
    ObRedisErr::record_err_msg(ret, fmt_redis_msg, redis_err); \
  } while (0)

#define SPRINTF_REDIS_ERROR(alloc, fmt_redis_msg, redis_err, ...)                               \
  do {                                                                                          \
    int num_args = COUNT_ARGS(__VA_ARGS__);                                                     \
    ObRedisErr::sprintf_err_msg(ret, alloc, fmt_redis_msg, redis_err, num_args, ##__VA_ARGS__); \
  } while (0)

#define COVER_REDIS_ERROR(ret) (ret == OB_KV_REDIS_ERROR) ? OB_SUCCESS : ret
// Record redis err msg
class ObRedisErr
{
public:
  static void
      response_err_msg(int &res_ret, ObRedisResponse &response, const char *redis_err, int32_t args_count = 0, ...);
  static void record_err_msg(int &res_ret, ObString &fmt_redis_msg, const char *redis_err);
  static void sprintf_err_msg(
      int &res_ret,
      ObIAllocator &alloc,
      ObString &fmt_redis_msg,
      const char *redis_err,
      int32_t args_count = 0,
      ...);

  OB_INLINE static bool is_redis_error(int ret)
  {
    return ret == OB_KV_REDIS_ERROR;
  }

public:
  static const char *EXPIRE_TIME_ERR;
  static const char *SYNTAX_ERR;
  static const char *FLOAT_ERR;
  static const char *INTEGER_ERR;
  static const char * NOT_POSITIVE_ERR;
  static const char *INCR_OVERFLOW;
  static const char *INTEGER_OUT_RANGE_ERR;
  static const char *BIT_INTEGER_ERR;
  static const char *BIT_OFFSET_INTEGER_ERR;
  static const char *MAXIMUM_ERR;
  static const char *WRONG_ARGS_LPOP_ERR;
  static const char *WRONG_ARGS_RPOP_ERR;
  static const char *WRONG_ARGS_ERR;
  static const char *EXPIRE_TIME_CMD_ERR;
  static const char *INCR_OVERLOAD;
  static const char *HASH_VALUE_FLOAT_ERR;
  static const char *MIN_MAX_FLOAT_ERR;
  static const char *HASH_VALUE_ERR;
  static const char *INDEX_OVERFLOW_ERR;
  static const char *NO_SUCH_KEY_ERR;
  static const char *OFFSET_OUT_RANGE_ERR;
  static const char *MSG_SIZE_OVERFLOW_ERR;
  static const char *INF_ERR;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisErr);
};

}  // namespace table
}  // namespace oceanbase
