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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_HASH_CMD_
#define OCEANBASE_OBSERVER_OB_REDIS_HASH_CMD_
#include "observer/table/redis/ob_redis_service.h"
#include "ob_redis_cmd.h"

namespace oceanbase
{
namespace table
{
/*
hashes table model:
  create table modis_hash_table(
    db bigint not null,
    rkey varbinary(1024) not null,
    field varbinary(1024) not null,
    value varbinary(1024) not null,
    expire_ts timestamp(6) default null,
    primary key(db, rkey, field)) TTL(expire_ts + INTERVAL 0 SECOND) partition by key(db, rkey) partitions 3;
*/
class HashCommand : public RedisCommand
{
public:
  using FieldValMap = common::hash::ObHashMap<ObString, ObString>;

  HashCommand() : key_()
  {}
  virtual ~HashCommand() = default;

protected:
  ObString key_;

private:
  DISALLOW_COPY_AND_ASSIGN(HashCommand);
};

// HSET key field value [field value ...]
class HSet : public HashCommand
{
public:
  explicit HSet(ObIAllocator &allocator)
  {
    attr_.arity_ = -4;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "HSET";
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~HSet() { field_val_map_.destroy(); }

  int init(const common::ObIArray<common::ObString> &args) override;
  virtual int apply(ObRedisCtx &redis_ctx) override;

protected:
  FieldValMap field_val_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(HSet);
};

// HMSET key field value [field value ...]
// note: it is deprecated after redis 4.0 and can be replaced by HSET
class HMSet : public HSet
{
public:
  explicit HMSet(ObIAllocator &allocator) : HSet(allocator)
  {
    attr_.cmd_name_ = "HMSET";
  }
  virtual ~HMSet() { field_val_map_.destroy(); }

  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(HMSet);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_HASH_CMD_
