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
class HashCommand : public RedisCommand
{
public:
  HashCommand()
  {
    attr_.cmd_group_ = ObRedisCmdGroup::HASH_CMD;
  }
  virtual ~HashCommand() = default;
private:
  DISALLOW_COPY_AND_ASSIGN(HashCommand);
};

// HSET key field value [field value ...]
class HSet : public HashCommand
{
public:
  explicit HSet(ObIAllocator &allocator) : allocator_(allocator)
  {
    attr_.arity_ = -4;
    attr_.need_snapshot_ = false;
  }
  virtual ~HSet()
  {
    field_val_map_.destroy();
  }

  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;
  const FieldValMap &field_val_map() const { return field_val_map_; }

protected:
  FieldValMap field_val_map_;
  ObIAllocator &allocator_;

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
  }
  virtual ~HMSet()
  {
    field_val_map_.destroy();
  }

  int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(HMSet);
};

// HSETNX key field value
class HSetNX : public HashCommand
{
public:
  explicit HSetNX(ObIAllocator &allocator)
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
  }
  virtual ~HSetNX()
  {}

  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

  ObString field_;
  ObString new_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(HSetNX);
};

// HEXISTS key field
class HExists : public HashCommand
{
public:
  explicit HExists(ObIAllocator &allocator)
  {
    attr_.arity_ = 3;
  }
  virtual ~HExists()
  {}

  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(HExists);
};

// HGET key field
class HGet : public HashCommand
{
public:
  explicit HGet(ObIAllocator &allocator)
  {
    attr_.arity_ = 3;
  }
  virtual ~HGet()
  {}

  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(HGet);
};

// HGETALL key
class HGetAll : public HashCommand
{
public:
  explicit HGetAll(ObIAllocator &allocator) : need_vals_(true), need_fields_(true)
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = false;
  }
  virtual ~HGetAll()
  {}

  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

protected:
  bool need_vals_;
  bool need_fields_;

private:
  DISALLOW_COPY_AND_ASSIGN(HGetAll);
};

// HKEYS key
class HKeys : public HGetAll
{
public:
  explicit HKeys(ObIAllocator &allocator) : HGetAll(allocator)
  {
    need_vals_ = false;
  }
  virtual ~HKeys()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(HKeys);
};

// HVALS key
class HVals : public HGetAll
{
public:
  explicit HVals(ObIAllocator &allocator) : HGetAll(allocator)
  {
    need_fields_ = false;
  }
  virtual ~HVals()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(HVals);
};

// HLEN key
class HLen : public HashCommand
{
public:
  explicit HLen(ObIAllocator &allocator)
  {
    attr_.arity_ = 2;
    attr_.need_snapshot_ = false;
  }
  virtual ~HLen()
  {}

  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  DISALLOW_COPY_AND_ASSIGN(HLen);
};

// HMGET key field [field ...]
class HMGet : public HashCommand
{
public:
  explicit HMGet(ObIAllocator &allocator)
      : fields_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisHMGet"))
  {
    attr_.arity_ = -2;
  }
  virtual ~HMGet()
  {}

  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  ObArray<ObString> fields_;
  DISALLOW_COPY_AND_ASSIGN(HMGet);
};

// HDEL key field [field ...]
class HDel : public HashCommand
{
public:
  explicit HDel(ObIAllocator &allocator)
  {
    attr_.arity_ = -2;
    attr_.need_snapshot_ = false;
  }
  virtual ~HDel()
  {}

  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  FieldSet field_set_;
  DISALLOW_COPY_AND_ASSIGN(HDel);
};

// HINCRBY key field increment
class HIncrBy : public HashCommand
{
public:
  explicit HIncrBy(ObIAllocator &allocator)
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
    attr_.need_snapshot_ = false;
  }
  virtual ~HIncrBy()
  {}

  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  ObString field_;
  int64_t increment_;

private:
  DISALLOW_COPY_AND_ASSIGN(HIncrBy);
};

// HINCRBYFLOAT key field increment
class HIncrByFloat : public HashCommand
{
public:
  explicit HIncrByFloat(ObIAllocator &allocator)
  {
    attr_.arity_ = 4;
    attr_.need_snapshot_ = false;
    attr_.need_snapshot_ = false;
  }
  virtual ~HIncrByFloat()
  {}

  int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) override;
  virtual int apply(ObRedisSingleCtx &redis_ctx) override;

private:
  ObString field_;
  long double increment_;

private:
  DISALLOW_COPY_AND_ASSIGN(HIncrByFloat);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_HASH_CMD_
