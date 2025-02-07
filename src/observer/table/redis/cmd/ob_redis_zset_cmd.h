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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_ZSET_CMD_
#define OCEANBASE_OBSERVER_OB_REDIS_ZSET_CMD_
#include "observer/table/redis/ob_redis_service.h"
#include "observer/table/redis/cmd/ob_redis_set_cmd.h"

namespace oceanbase
{
namespace table
{
/*
zsets table model:
    create table modis_zset_table(
      db bigint not null,
      rkey varbinary(1024) not null,
      member varbinary(1024) not null,
      score bigint not null,
      expire_ts timestamp(6) default null,
      index index_score(score) local,
      primary key(db, rkey, member)) TTL(expire_ts + INTERVAL 0 SECOND)
      partition by key(db, rkey) partitions 3;
*/
class ZRangeCtx
{
public:
  explicit ZRangeCtx()
      : db_(0),
        key_(),
        offset_(0),
        limit_(-1),
        with_scores_(false),
        is_rev_(false),
        min_(DBL_MAX),
        min_inclusive_(false),
        max_(DBL_MAX),
        max_inclusive_(false),
        start_(INT_MAX),
        end_(INT_MAX)
  {}
  virtual ~ZRangeCtx()
  {}

  TO_STRING_KV(K_(db), K_(key), K_(min), K_(min_inclusive), K_(max), K_(max_inclusive), K_(offset),
               K_(limit), K_(with_scores), K_(is_rev), K_(start), K_(end));

public:
  int64_t db_;
  ObString key_;
  int64_t offset_;
  int64_t limit_;
  bool with_scores_;
  bool is_rev_;

  // by score
  double min_;
  bool min_inclusive_;
  double max_;
  bool max_inclusive_;

  // by rank
  int64_t start_;
  int64_t end_;
};

class ZSetCommand : public SetCommand
{
public:
  using MemberScoreMap = common::hash::ObHashMap<ObString, double>;
  ZSetCommand() : key_()
  {}
  virtual ~ZSetCommand() = default;

protected:
  static const ObString WITHSCORE;
  static const ObString WITHSCORES;
  static const ObString LIMIT;
  static const ObString WEIGHTS;
  static const ObString AGGREGATE;
  static const ObString POS_INF;
  static const ObString NEG_INF;
  common::ObString key_;

  int strntod_with_inclusive(const ObString &str, bool &inclusive, double &d);
  int string_to_double(const ObString &str, double &d);
};

// ZADD key score member [score member ...]
class ZAdd : public ZSetCommand
{
public:
  explicit ZAdd(ObIAllocator &allocator)
  {
    attr_.arity_ = -4;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "ZADD";
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~ZAdd()
  { mem_score_map_.destroy(); }

  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  // args
  MemberScoreMap mem_score_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ZAdd);
};

// ZCARD key
class ZCard : public ZSetCommand
{
public:
  explicit ZCard(ObIAllocator &allocator)
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "ZCARD";
  }
  virtual ~ZCard()
  {}

  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ZCard);
};

// ZREM key member [member ...]
class ZRem : public ZSetCommand
{
public:
  explicit ZRem(ObIAllocator &allocator)
      : members_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisZRem"))
  {
    attr_.arity_ = -3;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "ZREM";
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~ZRem()
  {}

  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  common::ObArray<ObString> members_;

private:
  DISALLOW_COPY_AND_ASSIGN(ZRem);
};

// ZINCRBY key increment member
class ZIncrBy : public ZSetCommand
{
public:
  explicit ZIncrBy(ObIAllocator &allocator) : increment_(0.0), member_()
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "ZINCRBY";
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~ZIncrBy()
  {}

  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  double increment_;
  ObString member_;

private:
  DISALLOW_COPY_AND_ASSIGN(ZIncrBy);
};

// ZSCORE key member
class ZScore : public ZSetCommand
{
public:
  explicit ZScore(ObIAllocator &allocator) : member_()
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = true;
    attr_.cmd_name_ = "ZSCORE";
  }
  virtual ~ZScore()
  {}

  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  ObString member_;
  DISALLOW_COPY_AND_ASSIGN(ZScore);
};

// ZRANK key member [WITHSCORE]
class ZRank : public ZSetCommand
{
public:
  explicit ZRank(ObIAllocator &allocator) : member_(), zrange_ctx_()
  {
    attr_.arity_ = -3;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "ZRANK";
    attr_.lock_mode_ = REDIS_LOCK_MODE::SHARED;
    zrange_ctx_.is_rev_ = false;
  }
  virtual ~ZRank()
  {}

  virtual int init(const common::ObIArray<common::ObString> &args) override;
  virtual int apply(ObRedisCtx &redis_ctx) override;

protected:
  ObString member_;
  ZRangeCtx zrange_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ZRank);
};

// ZREVRANK key member [WITHSCORE]
class ZRevRank : public ZRank
{
public:
  explicit ZRevRank(ObIAllocator &allocator) : ZRank(allocator)
  {
    zrange_ctx_.is_rev_ = true;
    attr_.cmd_name_ = "ZREVRANK";
  }
  virtual ~ZRevRank()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ZRevRank);
};

// ZRANGE key start stop [WITHSCORES]
class ZRange : public ZSetCommand
{
public:
  explicit ZRange(ObIAllocator &allocator) : zrange_ctx_()
  {
    attr_.arity_ = -4;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "ZRANGE";
    attr_.lock_mode_ = REDIS_LOCK_MODE::SHARED;
  }
  virtual ~ZRange()
  {}

  virtual int init(const common::ObIArray<common::ObString> &args) override;
  virtual int apply(ObRedisCtx &redis_ctx) override;

protected:
  ZRangeCtx zrange_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ZRange);
};

// ZREVRANGE key start stop [WITHSCORES]
class ZRevRange : public ZRange
{
public:
  explicit ZRevRange(ObIAllocator &allocator) : ZRange(allocator)
  {
    attr_.cmd_name_ = "ZREVRANGE";
    zrange_ctx_.is_rev_ = true;
  }
  virtual ~ZRevRange()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ZRevRange);
};

// ZREMRANGEBYRANK key start stop
class ZRemRangeByRank : public ZSetCommand
{
public:
  explicit ZRemRangeByRank(ObIAllocator &allocator) : zrange_ctx_()
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "ZREMRANGEBYRANK";
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~ZRemRangeByRank()
  {}

  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  ZRangeCtx zrange_ctx_;

  DISALLOW_COPY_AND_ASSIGN(ZRemRangeByRank);
};

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
class ZRangeByScore : public ZSetCommand
{
public:
  explicit ZRangeByScore(ObIAllocator &allocator) : zrange_ctx_()
  {
    attr_.arity_ = -4;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "ZRANGEBYSCORE";
    attr_.lock_mode_ = REDIS_LOCK_MODE::SHARED;
  }
  virtual ~ZRangeByScore()
  {}

  virtual int init(const common::ObIArray<common::ObString> &args) override;
  virtual int apply(ObRedisCtx &redis_ctx) override;

protected:
  ZRangeCtx zrange_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ZRangeByScore);
};

// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
class ZRevRangeByScore : public ZRangeByScore
{
public:
  explicit ZRevRangeByScore(ObIAllocator &allocator) : ZRangeByScore(allocator)
  {
    attr_.cmd_name_ = "ZREVRANGEBYSCORE";
    zrange_ctx_.is_rev_ = true;
  }
  virtual ~ZRevRangeByScore()
  {}

  int init(const common::ObIArray<common::ObString> &args) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ZRevRangeByScore);
};

// ZREMRANGEBYSCORE key min max
class ZRemRangeByScore : public ZSetCommand
{
public:
  explicit ZRemRangeByScore(ObIAllocator &allocator)
      : zrange_ctx_()
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "ZREMRANGEBYSCORE";
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
  }
  virtual ~ZRemRangeByScore()
  {}

  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  ZRangeCtx zrange_ctx_;

  DISALLOW_COPY_AND_ASSIGN(ZRemRangeByScore);
};

// ZCOUNT key min max
class ZCount : public ZSetCommand
{
public:
  explicit ZCount(ObIAllocator &allocator)
      : min_(0.0), max_(0.0), min_inclusive_(true), max_inclusive_(true)
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
    attr_.cmd_name_ = "ZCOUNT";
    attr_.lock_mode_ = REDIS_LOCK_MODE::SHARED;
  }
  virtual ~ZCount()
  {}

  int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

private:
  double min_;
  double max_;
  bool min_inclusive_;
  bool max_inclusive_;

  DISALLOW_COPY_AND_ASSIGN(ZCount);
};

class ZSetAggCommand : public ZSetCommand
{
public:
  enum class AggType {
    SUM,
    MIN,
    MAX,
  };

  explicit ZSetAggCommand(ObIAllocator &allocator)
      : dest_(),
        keys_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisZUnion")),
        weights_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisZUnion")),
        agg_type_(AggType::SUM)
  {
    attr_.arity_ = -4;
    attr_.need_snapshot_ = false;
    attr_.lock_mode_ = REDIS_LOCK_MODE::EXCLUSIVE;
    attr_.use_dist_das_ = true;
  }
  virtual ~ZSetAggCommand()
  {}

  virtual int init(const common::ObIArray<common::ObString> &args) override;
  int apply(ObRedisCtx &redis_ctx) override;

protected:
  ObString dest_;
  ObArray<ObString> keys_;
  ObArray<double> weights_;
  AggType agg_type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ZSetAggCommand);
};

// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN
// | MAX>]
class ZUnionStore : public ZSetAggCommand
{
public:
  explicit ZUnionStore(ObIAllocator &allocator) : ZSetAggCommand(allocator)
  {
    attr_.cmd_name_ = "ZUNIONSTORE";
  }
  virtual ~ZUnionStore()
  {}

  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ZUnionStore);
};

// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN
// | MAX>]
class ZInterStore : public ZSetAggCommand
{
public:
  explicit ZInterStore(ObIAllocator &allocator) : ZSetAggCommand(allocator)
  {
    attr_.cmd_name_ = "ZINTERSTORE";
  }
  virtual ~ZInterStore()
  {}

  int apply(ObRedisCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ZInterStore);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_ZSET_CMD_
