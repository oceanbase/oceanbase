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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_STRING_CMD_
#define OCEANBASE_OBSERVER_OB_REDIS_STRING_CMD_
#include "observer/table/redis/ob_redis_service.h"
#include "ob_redis_cmd.h"

namespace oceanbase
{
namespace table
{
/*
stringes table model:
  create table modis_string_table(
    db bigint not null,
    rkey varbinary(1024) not null,
    value varbinary(1024) not null,
    expire_ts timestamp(6) default null,
    primary key(db, rkey)) TTL(expire_ts + INTERVAL 0 SECOND) partition by key(db, rkey) partitions 3;
*/
class StringCommand : public RedisCommand
{
public:
  StringCommand() : key_()
  {}
  virtual ~StringCommand() = default;

protected:
  ObString key_;

private:
  DISALLOW_COPY_AND_ASSIGN(StringCommand);
};

// GETSET key value
class GetSet : public StringCommand
{
public:
  explicit GetSet(ObIAllocator &allocator) : StringCommand(), new_value_()
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "GETSET";
    attr_.use_dist_das_ = false;
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~GetSet()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  ObString new_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(GetSet);
};

// SETBIT key offset value
class SetBit : public StringCommand
{
public:
  explicit SetBit(ObIAllocator &allocator) : StringCommand(), value_()
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "SETBIT";
    attr_.use_dist_das_ = false;
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~SetBit()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  char value_;
  // [0, 2^32 - 1]
  int32_t offset_;

private:
  DISALLOW_COPY_AND_ASSIGN(SetBit);
};

// INCRBY key incr
class IncrBy : public StringCommand
{
public:
  explicit IncrBy(ObIAllocator &allocator) : incr_(), is_incr_(true)
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "INCRBY";
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~IncrBy()
  {}
  virtual int init(const common::ObIArray<common::ObString> &args) override;
  virtual int apply(ObRedisCtx &redis_ctx) override;

protected:
  ObString incr_;
  bool is_incr_;

private:
  DISALLOW_COPY_AND_ASSIGN(IncrBy);
};

// INCR key
class Incr : public IncrBy
{
public:
  explicit Incr(ObIAllocator &allocator) : IncrBy(allocator)
  {
    attr_.arity_ = 2;
    attr_.cmd_name_ = "INCR";
  }
  virtual ~Incr()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(Incr);
};

// DECR key
class Decr : public IncrBy
{
public:
  explicit Decr(ObIAllocator &allocator) : IncrBy(allocator)
  {
    attr_.arity_ = 2;
    attr_.cmd_name_ = "DECR";
    is_incr_ = false;
  }
  virtual ~Decr()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(Decr);
};

// DECRBY key decr
class DecrBy : public IncrBy
{
public:
  explicit DecrBy(ObIAllocator &allocator) : IncrBy(allocator)
  {
    attr_.arity_ = 3;
    attr_.cmd_name_ = "DECRBY";
    is_incr_ = false;
  }
  virtual ~DecrBy()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(DecrBy);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_STRING_CMD_
