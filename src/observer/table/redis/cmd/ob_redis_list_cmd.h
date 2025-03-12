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
    attr_.cmd_group_ = ObRedisCmdGroup::LIST_CMD;
  }
  virtual ~ListCommand() = default;

};

class Push : public ListCommand
{
public:
  explicit Push(ObIAllocator &allocator, bool is_push_left, bool need_exist)
      : is_push_left_(is_push_left),
        need_exist_(need_exist),
        values_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisPUSH"))
  {
    attr_.arity_ = -3;
    attr_.need_snapshot_ = false;
  }
  virtual ~Push()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;

public:
  OB_INLINE const common::ObIArray<common::ObString> &get_values() const
  {
    return values_;
  }

  OB_INLINE bool is_push_left() const
  {
    return is_push_left_;
  }

  OB_INLINE bool need_exist() const
  {
    return need_exist_;
  }

protected:
  // args
  bool is_push_left_;
  bool need_exist_;
  ObSEArray<common::ObString, 2> values_;
private:
  DISALLOW_COPY_AND_ASSIGN(Push);
};

class LPush : public Push
{
public:
  explicit LPush(ObIAllocator &allocator) : Push(allocator, true /*is_push_left*/, false /*need_exist*/)
  {}
  virtual ~LPush()
  {}

  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(LPush);
};

class LPushX : public Push
{
public:
  explicit LPushX(ObIAllocator &allocator) : Push(allocator, true /*is_push_left*/, true /*need_exist*/)
  {}
  virtual ~LPushX()
  {}
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(LPushX);
};

class RPush : public Push
{
public:
  explicit RPush(ObIAllocator &allocator) : Push(allocator, false /*is_push_left*/, false /*need_exist*/)
  {}
  virtual ~RPush()
  {}

  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(RPush);
};

class RPushX : public Push
{
public:
  explicit RPushX(ObIAllocator &allocator) : Push(allocator, false /*is_push_left*/, true /*need_exist*/)
  {}
  virtual ~RPushX()
  {}
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(RPushX);
};

class Pop : public ListCommand
{
public:
  explicit Pop(bool is_pop_left) : is_pop_left_(is_pop_left)
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = false;
  }
  virtual ~Pop()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  OB_INLINE bool is_pop_left() const
  {
    return is_pop_left_;
  }
private:
  bool is_pop_left_;
  DISALLOW_COPY_AND_ASSIGN(Pop);
};

class LPop : public Pop
{
public:
  explicit LPop(ObIAllocator &allocator) : Pop(true /*is_pop_left*/)
  {
  }
  virtual ~LPop()
  {}
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(LPop);
};

class RPop : public Pop
{
public:
  explicit RPop(ObIAllocator &allocator) : Pop(false /*is_pop_left*/)
  {}
  virtual ~RPop()
  {}
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(RPop);
};

class LIndex : public ListCommand
{
public:
  explicit LIndex(ObIAllocator &allocator)
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
  }
  virtual ~LIndex()
  {}
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  int64_t offset_;
  DISALLOW_COPY_AND_ASSIGN(LIndex);
};

class LSet : public ListCommand
{
public:
  explicit LSet(ObIAllocator &allocator)
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~LSet()
  {}
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

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
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~LRange()
  {}
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

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
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~LTrim()
  {}
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

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
    attr_.arity_ = 5;
    attr_.need_snapshot_ = false;
  }
  virtual ~LInsert()
  {}
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

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
    attr_.arity_ = 2;
    attr_.need_snapshot_ = true;
  }
  virtual ~LLen()
  {}
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(LLen);
};

class LRem : public ListCommand
{
public:
  explicit LRem(ObIAllocator &allocator)
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~LRem()
  {}
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

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
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
  }
  virtual ~RpopLpush()
  {}
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  ObString dest_key_;
  DISALLOW_COPY_AND_ASSIGN(RpopLpush);
};

}  // namespace table
}  // namespace oceanbase
