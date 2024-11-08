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
class StringCommand : public RedisCommand
{
public:
  StringCommand()
  {
    attr_.cmd_group_ = ObRedisCmdGroup::STRING_CMD;
  }
  virtual ~StringCommand() = default;

private:
  DISALLOW_COPY_AND_ASSIGN(StringCommand);
};

// APPEND key value
class Append : public StringCommand
{
public:
  explicit Append(ObIAllocator &allocator) : StringCommand()
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
  }
  virtual ~Append()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

  common::ObString val_;

private:
  DISALLOW_COPY_AND_ASSIGN(Append);
};

// BITCOUNT key [start end]
class BitCount : public StringCommand
{
public:
  explicit BitCount(ObIAllocator &allocator) : StringCommand()
  {
    attr_.arity_ = -2;
    attr_.need_snapshot_ = true;
  }
  virtual ~BitCount()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

  common::ObString start_;
  common::ObString end_;

private:
  DISALLOW_COPY_AND_ASSIGN(BitCount);
};

// GET key
class Get : public StringCommand
{
public:
  explicit Get(ObIAllocator &allocator) : StringCommand()
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = true;
  }
  virtual ~Get()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(Get);
};

// GETBIT key offset
class GetBit : public StringCommand
{
public:
  explicit GetBit(ObIAllocator &allocator) : StringCommand()
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = true;
  }
  virtual ~GetBit()
  {}
  // set AND check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

  ObString offset_;

private:
  DISALLOW_COPY_AND_ASSIGN(GetBit);
};

// GETRANGE key start end
class GetRange : public StringCommand
{
public:
  explicit GetRange(ObIAllocator &allocator) : StringCommand()
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = true;
  }
  virtual ~GetRange()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

  common::ObString start_;
  common::ObString end_;

private:
  DISALLOW_COPY_AND_ASSIGN(GetRange);
};

// INCRBYFLOAT key increment
class IncrByFloat : public StringCommand
{
public:
  explicit IncrByFloat(ObIAllocator &allocator) : StringCommand()
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
  }
  virtual ~IncrByFloat()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  common::ObString incr_;

private:
  DISALLOW_COPY_AND_ASSIGN(IncrByFloat);
};

// MGET key [key ...]
class MGet : public StringCommand
{
public:
  explicit MGet(ObIAllocator &allocator) : StringCommand(), keys_(nullptr)
  {
    attr_.arity_ = -2;
    attr_.need_snapshot_ = true;
    attr_.use_dist_das_ = true;
  }
  virtual ~MGet()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  const ObIArray<ObString> *keys_;

private:
  DISALLOW_COPY_AND_ASSIGN(MGet);
};

// MSET key value [key value ...]
class MSet : public StringCommand
{
public:
  explicit MSet(ObIAllocator &allocator)
  {
    attr_.arity_ = -3;
    attr_.need_snapshot_ = false;
    attr_.use_dist_das_ = true;
  }
  virtual ~MSet()
  {
    field_val_map_.destroy();
  }
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  FieldValMap field_val_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(MSet);
};

struct SetArg {
  common::ObString cmd_name_;
  common::ObString value_;
  common::ObString expire_str_;
  bool is_ms_ = false;
  bool nx_ = false;

  TO_STRING_KV(K_(value), K_(expire_str),K_(is_ms), K_(nx));
};

// SET key value
class Set : public StringCommand
{
public:
  explicit Set(ObIAllocator &allocator) : StringCommand()
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
  }
  virtual ~Set()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;
  OB_INLINE const common::ObString &value() const { return set_arg_.value_; }

protected:
  SetArg set_arg_;

private:
  DISALLOW_COPY_AND_ASSIGN(Set);
};

// PSETEX key milliseconds value
class PSetEx : public Set
{
public:
  explicit PSetEx(ObIAllocator &allocator) : Set(allocator)
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~PSetEx()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;

private:
  DISALLOW_COPY_AND_ASSIGN(PSetEx);
};

// SETEX key seconds value
class SetEx : public Set
{
public:
  explicit SetEx(ObIAllocator &allocator) : Set(allocator)
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~SetEx()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;

private:
  DISALLOW_COPY_AND_ASSIGN(SetEx);
};

// SETNX key value
class SetNx : public Set
{
public:
  explicit SetNx(ObIAllocator &allocator): Set(allocator)
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
  }
  virtual ~SetNx()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;

private:
  DISALLOW_COPY_AND_ASSIGN(SetNx);
};

// SETRANGE key offset value
class SetRange : public StringCommand
{
public:
  explicit SetRange(ObIAllocator &allocator) : StringCommand()
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~SetRange()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  common::ObString offset_;
  common::ObString value_;

private:
  DISALLOW_COPY_AND_ASSIGN(SetRange);
};

// STRLEN key
class StrLen : public StringCommand
{
public:
  explicit StrLen(ObIAllocator &allocator) : StringCommand()
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = true;
  }
  virtual ~StrLen()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(StrLen);
};

// GETSET key value
class GetSet : public StringCommand
{
public:
  explicit GetSet(ObIAllocator &allocator) : StringCommand(), new_value_()
  {
    attr_.arity_ = 3;
    attr_.need_snapshot_ = false;
    attr_.use_dist_das_ = false;
  }
  virtual ~GetSet()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

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
    attr_.use_dist_das_ = false;
  }
  virtual ~SetBit()
  {}
  // set and check args here
  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  int apply(ObRedisSingleCtx &redis_ctx) override;

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
  }
  virtual ~IncrBy()
  {}
  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;
  OB_INLINE const ObString &incr() { return incr_; }
  OB_INLINE bool is_incr() { return is_incr_; }

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
