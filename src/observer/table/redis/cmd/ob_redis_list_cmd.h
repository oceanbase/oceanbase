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
#include "ob_redis_cmd.h"
#include "observer/table/redis/ob_redis_service.h"

namespace oceanbase
{
namespace table
{
class ListCommand : public RedisCommand
{
public:
  ListCommand()
  {
    // almost all list cmd include query_and_mutate operations
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~ListCommand() = default;

protected:
  common::ObString key_;
};

class Push : public ListCommand
{
public:
  explicit Push(ObIAllocator &allocator) : values_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisPUSH"))
  {
    attr_.arity_ = -3;
    attr_.need_snapshot_ = false;
  }
  virtual ~Push()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args) override;

protected:
  // args
  ObSEArray<common::ObString, 2> values_;

private:
  DISALLOW_COPY_AND_ASSIGN(Push);
};

class LPush : public Push
{
public:
  explicit LPush(ObIAllocator &allocator) : Push(allocator)
  {
    attr_.cmd_name_ = "LPUSH";
  }
  virtual ~LPush()
  {}

  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(LPush);
};

class LPushX : public Push
{
public:
  explicit LPushX(ObIAllocator &allocator) : Push(allocator)
  {
    attr_.cmd_name_ = "LPUSHX";
  }
  virtual ~LPushX()
  {}
  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(LPushX);
};

class RPush : public Push
{
public:
  explicit RPush(ObIAllocator &allocator) : Push(allocator)
  {
    attr_.cmd_name_ = "RPUSH";
  }
  virtual ~RPush()
  {}

  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(RPush);
};

class RPushX : public Push
{
public:
  explicit RPushX(ObIAllocator &allocator) : Push(allocator)
  {
    attr_.cmd_name_ = "RPUSHX";
  }
  virtual ~RPushX()
  {}
  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(RPushX);
};

class Pop : public ListCommand
{
public:
  explicit Pop()
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = false;
  }
  virtual ~Pop()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args) override;

private:
  DISALLOW_COPY_AND_ASSIGN(Pop);
};

class LPop : public Pop
{
public:
  explicit LPop(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "LPOP";
  }
  virtual ~LPop()
  {}
  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(LPop);
};

class RPop : public Pop
{
public:
  explicit RPop(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "RPOP";
  }
  virtual ~RPop()
  {}
  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(RPop);
};

class LIndex : public ListCommand
{
public:
  explicit LIndex(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "LINDEX";
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
  }
  virtual ~LIndex()
  {}
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  int64_t offset_;
  DISALLOW_COPY_AND_ASSIGN(LIndex);
};

class LSet : public ListCommand
{
public:
  explicit LSet(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "LSET";
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~LSet()
  {}
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  int64_t offset_;
  common::ObString value_;
  DISALLOW_COPY_AND_ASSIGN(LSet);
};

class LRange : public ListCommand
{
public:
  explicit LRange(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "LRANGE";
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~LRange()
  {}
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  int64_t start_;
  int64_t end_;
  DISALLOW_COPY_AND_ASSIGN(LRange);
};

class LTrim : public ListCommand
{
public:
  explicit LTrim(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "LTRIM";
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~LTrim()
  {}
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  int64_t start_;
  int64_t end_;
  DISALLOW_COPY_AND_ASSIGN(LTrim);
};

class LInsert : public ListCommand
{
public:
  explicit LInsert(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "LINSERT";
    attr_.arity_ = 5;
    attr_.need_snapshot_ = false;
  }
  virtual ~LInsert()
  {}
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  bool is_before_pivot_;
  ObString pivot_;
  ObString value_;
  DISALLOW_COPY_AND_ASSIGN(LInsert);
};

class LLen : public ListCommand
{
public:
  explicit LLen(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "LLEN";
    attr_.arity_ = 2;
    attr_.need_snapshot_ = true;
    attr_.lock_mode_ = REDIS_LOCK_MODE::LOCK_FREE;
  }
  virtual ~LLen()
  {}
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(LLen);
};

class LRem : public ListCommand
{
public:
  explicit LRem(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "LREM";
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~LRem()
  {}
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  int64_t count_;
  ObString value_;
  DISALLOW_COPY_AND_ASSIGN(LRem);
};

class RpopLpush : public ListCommand
{
public:
  explicit RpopLpush(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "RPOPLPUSH";
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
  }
  virtual ~RpopLpush()
  {}
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  ObString dest_key_;
  DISALLOW_COPY_AND_ASSIGN(RpopLpush);
};

class LDel : public ListCommand
{
public:
  explicit LDel(ObIAllocator &allocator)
  {
    attr_.cmd_name_ = "LDEL";
    attr_.arity_ = 2;
    attr_.need_snapshot_ = false;
  }
  virtual ~LDel()
  {}
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(LDel);
};

}  // namespace table
}  // namespace oceanbase
