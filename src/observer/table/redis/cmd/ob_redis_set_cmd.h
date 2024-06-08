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
/*
sets table model:
    create table modis_set_table(
      db bigint not null,
      rkey varbinary(1024) not null,
      member varbinary(1024) not null,
      expire_ts timestamp(6) default null,
      primary key(db, rkey, member)) TTL(expire_ts + INTERVAL 0 SECOND)
      partition by key(db, rkey) partitions 3;
*/
class SetCommand : public RedisCommand
{
public:
  using MemberSet = common::hash::ObHashSet<ObString>;
  enum class AggFunc {
    DIFF,
    UNION,
    INTER,
  };

  SetCommand() : RedisCommand()
  {}
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
    attr_.lock_mode_ = REDIS_LOCK_MODE::SHARED;
  }
  virtual ~SetAgg()
  {}
  // set and check args here
  virtual int init(const common::ObIArray<common::ObString> &args) override;
  virtual int apply(ObRedisCtx &redis_ctx) override;

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
    attr_.cmd_name_ = "SDIFF";
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
    attr_.cmd_name_ = "SINTER";
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
    attr_.cmd_name_ = "SUNION";
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
    attr_.cmd_name_ = "SADD";
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~SAdd()
  {
    members_.destroy();
  }
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  common::ObString key_;
  MemberSet members_;

private:
  DISALLOW_COPY_AND_ASSIGN(SAdd);
};

class SetAggStore : public SetCommand
{
public:
  explicit SetAggStore(ObIAllocator &allocator, AggFunc agg_func)
      : dest_(),
        keys_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisSetAgg")),
        agg_func_(agg_func)
  {
    attr_.arity_ = -3;
    attr_.need_snapshot_ = false;
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
    attr_.use_dist_das_ = true;
  }
  virtual ~SetAggStore()
  {}
  // set and check args here
  virtual int init(const common::ObIArray<common::ObString> &args) override;
  virtual int apply(ObRedisCtx &redis_ctx) override;

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
    attr_.cmd_name_ = "SDIFFSTORE";
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
    attr_.cmd_name_ = "SINTERSTORE";
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
    attr_.cmd_name_ = "SUNIONSTORE";
  }
  virtual ~SUnionStore()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(SUnionStore);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_SET_CMD_