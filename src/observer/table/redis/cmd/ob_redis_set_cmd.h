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
#ifndef OCEANBASE_OBSERVER_OB_REDIS_SET_CMD_
#define OCEANBASE_OBSERVER_OB_REDIS_SET_CMD_
#include "ob_redis_cmd.h"
#include "observer/table/redis/ob_redis_service.h"

namespace oceanbase
{
namespace table
{
class SetCommand : public RedisCommand
{
public:
  using MemberSet = common::hash::ObHashSet<ObString, common::hash::NoPthreadDefendMode>;
  enum class AggFunc {
    DIFF,
    UNION,
    INTER,
  };

  SetCommand() : RedisCommand()
  {
    attr_.cmd_group_ = ObRedisCmdGroup::SET_CMD;
  }
  virtual ~SetCommand() = default;
};

class SetAgg : public SetCommand
{
public:
  explicit SetAgg(ObIAllocator &allocator, AggFunc agg_func) : keys_(nullptr), agg_func_(agg_func)
  {
    attr_.arity_ = -2;
    attr_.need_snapshot_ = false;
    attr_.use_dist_das_ = true;
  }
  virtual ~SetAgg()
  {}
  // set and check args here
  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

protected:
  const ObIArray<common::ObString> *keys_;
  AggFunc agg_func_;

private:
  DISALLOW_COPY_AND_ASSIGN(SetAgg);
};

// SDIFF key [key ...]
class SDiff : public SetAgg
{
public:
  explicit SDiff(ObIAllocator &allocator) : SetAgg(allocator, AggFunc::DIFF)
  {
  }
  virtual ~SDiff()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(SDiff);
};

// SINTER key [key ...]
class SInter : public SetAgg
{
public:
  explicit SInter(ObIAllocator &allocator) : SetAgg(allocator, AggFunc::INTER)
  {
  }
  virtual ~SInter()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(SInter);
};

// SUNION key [key ...]
class SUnion : public SetAgg
{
public:
  explicit SUnion(ObIAllocator &allocator) : SetAgg(allocator, AggFunc::UNION)
  {
  }
  virtual ~SUnion()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(SUnion);
};

// SADD key member [member ...]
class SAdd : public SetCommand
{
public:
  explicit SAdd(ObIAllocator &allocator) : members_()
  {
    attr_.arity_ = -3;
    attr_.need_snapshot_ = false;
  }
  virtual ~SAdd()
  {
    members_.destroy();
  }
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;
  const MemberSet &members() const { return members_; }

private:
  MemberSet members_;

private:
  DISALLOW_COPY_AND_ASSIGN(SAdd);
};

class SetAggStore : public SetCommand
{
public:
  explicit SetAggStore(ObIAllocator &allocator, AggFunc agg_func)
      : dest_(), keys_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisSetAgg")), agg_func_(agg_func)
  {
    attr_.arity_ = -3;
    attr_.need_snapshot_ = false;
    attr_.use_dist_das_ = true;
  }
  virtual ~SetAggStore()
  {}
  // set and check args here
  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

protected:
  ObString dest_;
  common::ObArray<ObString> keys_;
  AggFunc agg_func_;

private:
  DISALLOW_COPY_AND_ASSIGN(SetAggStore);
};

// SDIFFSTORE destination key [key ...]
class SDiffStore : public SetAggStore
{
public:
  explicit SDiffStore(ObIAllocator &allocator) : SetAggStore(allocator, AggFunc::DIFF)
  {
  }
  virtual ~SDiffStore()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(SDiffStore);
};

// SINTERSTORE destination key [key ...]
class SInterStore : public SetAggStore
{
public:
  explicit SInterStore(ObIAllocator &allocator) : SetAggStore(allocator, AggFunc::INTER)
  {
  }
  virtual ~SInterStore()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(SInterStore);
};

// SUNIONSTORE destination key [key ...]
class SUnionStore : public SetAggStore
{
public:
  explicit SUnionStore(ObIAllocator &allocator) : SetAggStore(allocator, AggFunc::UNION)
  {
  }
  virtual ~SUnionStore()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(SUnionStore);
};

// SCARD key
class SCard : public SetCommand
{
public:
  explicit SCard(ObIAllocator &allocator)
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = false;
  }
  virtual ~SCard()
  {
  }
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(SCard);
};

// SISMEMBER key member
class SIsMember : public SetCommand
{
public:
  explicit SIsMember(ObIAllocator &allocator)
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = true;
  }
  virtual ~SIsMember()
  {
  }
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;
private:
  DISALLOW_COPY_AND_ASSIGN(SIsMember);
};

// SMEMBERS key
class SMembers : public SetCommand
{
public:
  explicit SMembers(ObIAllocator &allocator)
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = false;
  }
  virtual ~SMembers()
  {
  }
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(SMembers);
};

// SMOVE source destination member
class SMove : public SetCommand
{
public:
  explicit SMove(ObIAllocator &allocator)
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
    attr_.use_dist_das_ = true;
  }
  virtual ~SMove()
  {}

  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  common::ObString src_;
  ObString dest_;
  ObString member_;

private:
  DISALLOW_COPY_AND_ASSIGN(SMove);
};

// SPOP key [count]
class SPop : public SetCommand
{
public:
  explicit SPop(ObIAllocator &allocator)
  {
    attr_.arity_ = -2;
    attr_.need_snapshot_ = false;
    attr_.use_dist_das_ = false;
  }
  virtual ~SPop()
  {}
  // set and check args here
  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  common::ObString count_str_;
  DISALLOW_COPY_AND_ASSIGN(SPop);
};

// SRANDMEMBER key [count]
class SRandMember : public SetCommand
{
public:
  explicit SRandMember(ObIAllocator &allocator)
  {
    attr_.arity_ = -2;
    attr_.need_snapshot_ = false;
    attr_.use_dist_das_ = false;
  }
  virtual ~SRandMember()
  {}
  // set and check args here
  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  common::ObString count_str_;
  DISALLOW_COPY_AND_ASSIGN(SRandMember);
};

// SREM key member [member ...]
class SRem : public SetCommand
{
public:
  explicit SRem(ObIAllocator &allocator)
  {
    attr_.arity_ = -3;
    attr_.need_snapshot_ = false;
    attr_.use_dist_das_ = false;
  }
  virtual ~SRem()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  MemberSet members_;

private:
  DISALLOW_COPY_AND_ASSIGN(SRem);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_SET_CMD_
