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

#include "observer/table/redis/ob_redis_service.h"
#include "observer/table/redis/operator/ob_redis_operator.h"

namespace oceanbase
{
namespace table
{

enum class REDIS_LOCK_MODE {
  LOCK_FREE = 0,  // for data structure does not contain metadata, all ops does not include
                  // query_and_mutate to table
  SHARED,    // for data structure contains metadata, but does not include query_and_mutate to table
  EXCLUSIVE  // for data structure contains metadata, and include query_and_mutate to table
};

struct ObRedisAttr
{
  // is the imp of the cmd a read-only table operation
  // note: scan should set need_snapshot_ = false
  bool need_snapshot_;
  // lock mode
  REDIS_LOCK_MODE lock_mode_;
  // Number of parameters, including command name
  // e.g. "get key", arity = 2
  int arity_;
  // multi part cmd need global snapshot and das support
  bool use_dist_das_;
  // cmd name
  common::ObString cmd_name_;
  ObRedisAttr()
      : need_snapshot_(true),
        lock_mode_(REDIS_LOCK_MODE::LOCK_FREE),
        arity_(INT_MAX),
        use_dist_das_(false),
        cmd_name_("INVALID")
  {}
  TO_STRING_KV(K_(need_snapshot), K_(lock_mode), K_(arity), K_(cmd_name), K_(use_dist_das));
};

// The virtual base class of the redis command
class RedisCommand
{
public:
  static constexpr int32_t DEFAULT_BUCKET_NUM = 1024;

  RedisCommand()
      : is_inited_(false)
  {
  }
  virtual ~RedisCommand()
  {}
  // Call command execution
  virtual int apply(ObRedisCtx &redis_ctx) = 0;
  // set and check args here
  virtual int init(const common::ObIArray<common::ObString> &args) = 0;

  OB_INLINE common::ObString get_command_name() const
  {
    return attr_.cmd_name_;
  }
  OB_INLINE bool need_snapshot() const
  {
    return attr_.need_snapshot_;
  }

  OB_INLINE REDIS_LOCK_MODE get_lock_mode() const
  {
    return attr_.lock_mode_;
  }

  OB_INLINE bool use_dist_das() const
  {
    return attr_.use_dist_das_;
  }

  OB_INLINE const ObRedisAttr &get_attributes() const
  {
    return attr_;
  }

protected:
  int init_common(const common::ObIArray<common::ObString> &args);

  // property
  ObRedisAttr attr_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(RedisCommand);
};

}  // namespace table
}  // namespace oceanbase
