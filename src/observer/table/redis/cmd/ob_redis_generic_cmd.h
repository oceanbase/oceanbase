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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_GENERIC_CMD_
#define OCEANBASE_OBSERVER_OB_REDIS_GENERIC_CMD_
#include "observer/table/redis/ob_redis_service.h"
#include "ob_redis_cmd.h"

namespace oceanbase
{
namespace table
{
class GenericCommand : public RedisCommand
{
public:
  GenericCommand()
  {
    attr_.cmd_group_ = ObRedisCmdGroup::GENERIC_CMD;
  }
  virtual ~GenericCommand() = default;
  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;

private:
  DISALLOW_COPY_AND_ASSIGN(GenericCommand);
};

// EXPIRE key milliseconds
class ObRedisExpire : public GenericCommand
{
public:
  explicit ObRedisExpire(ObIAllocator &allocator)
      : expire_time_(-1), unit_(ObRedisUtil::SEC_TO_US)
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
  }
  virtual ~ObRedisExpire()
  {}
  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

protected:
  int64_t expire_time_;
  int64_t unit_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisExpire);
};

// PEXPIRE key milliseconds
class ObRedisPExpire : public ObRedisExpire
{
public:
  explicit ObRedisPExpire(ObIAllocator &allocator) : ObRedisExpire(allocator)
  {
    unit_ = ObRedisUtil::MS_TO_US;
  }
  virtual ~ObRedisPExpire()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisPExpire);
};

// PEXPIREAT key unix-time-milliseconds
class ObRedisExpireAt : public ObRedisExpire
{
public:
  explicit ObRedisExpireAt(ObIAllocator &allocator) : ObRedisExpire(allocator)
  {
  }
  virtual ~ObRedisExpireAt()
  {}
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisExpireAt);
};

// PEXPIREAT key unix-time-milliseconds
class ObRedisPExpireAt : public ObRedisExpireAt
{
public:
  explicit ObRedisPExpireAt(ObIAllocator &allocator) : ObRedisExpireAt(allocator)
  {
    unit_ = ObRedisUtil::MS_TO_US;
  }
  virtual ~ObRedisPExpireAt()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisPExpireAt);
};

// TTL key
class ObRedisTTL : public GenericCommand
{
public:
  explicit ObRedisTTL(ObIAllocator &allocator) : unit_(ObRedisUtil::SEC_TO_US)
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = true;
  }
  virtual ~ObRedisTTL()
  {}
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

protected:
  int64_t unit_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisTTL);
};

// PTTL key
class ObRedisPTTL : public ObRedisTTL
{
public:
  explicit ObRedisPTTL(ObIAllocator &allocator) : ObRedisTTL(allocator)
  {
    unit_ = ObRedisUtil::MS_TO_US;
  }
  virtual ~ObRedisPTTL()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisPTTL);
};

// EXISTS key [key ...]
class ObRedisExists : public GenericCommand
{
public:
  explicit ObRedisExists(ObIAllocator &allocator) : keys_(nullptr)
  {
    attr_.arity_ = -2;
    attr_.need_snapshot_ = false;
  }
  virtual ~ObRedisExists()
  {}
  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;
private:
  const ObIArray<ObString> *keys_;
  DISALLOW_COPY_AND_ASSIGN(ObRedisExists);
};

// type key
class ObRedisType : public GenericCommand
{
public:
  explicit ObRedisType(ObIAllocator &allocator)
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = false;
  }
  virtual ~ObRedisType()
  {}
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisType);
};

// DEL key [key ...]
class ObRedisDel : public GenericCommand
{
public:
  explicit ObRedisDel(ObIAllocator &allocator) : keys_(nullptr)
  {
    attr_.arity_ = -2;
    attr_.need_snapshot_ = false;
  }
  virtual ~ObRedisDel()
  {}
  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  const ObIArray<ObString> *keys_;
  DISALLOW_COPY_AND_ASSIGN(ObRedisDel);
};

// Persist key
class ObRedisPersist : public GenericCommand
{
public:
  explicit ObRedisPersist(ObIAllocator &allocator)
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = false;
  }
  virtual ~ObRedisPersist()
  {}
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisPersist);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_GENERIC_CMD_
