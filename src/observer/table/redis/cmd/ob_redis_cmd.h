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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_CMD_
#define OCEANBASE_OBSERVER_OB_REDIS_CMD_
#include "observer/table/redis/ob_redis_service.h"
#include "observer/table/redis/operator/ob_redis_operator.h"
#include "observer/table/group/ob_i_table_struct.h"

namespace oceanbase
{
namespace table
{

enum class REDIS_LOCK_MODE {
  LOCK_FREE = 0,  // for data structure does not contain metadata, all ops does not include
                  // query_and_mutate to table
  SHARED,    // for data structure contains metadata, but does not include query_and_mutate to table
  EXCLUSIVE  // for data structure contains metadata, and include query_and_mutate to table
};

enum ObRedisCmdGroup {
  STRING_CMD,
  HASH_CMD,
  SET_CMD,
  ZSET_CMD,
  LIST_CMD,
  GENERIC_CMD,
  INVALID_CMD
};

struct ObRedisAttr
{
  static const char *INVALID_NAME;

  ObRedisAttr()
      : need_snapshot_(true),
        lock_mode_(REDIS_LOCK_MODE::LOCK_FREE),
        arity_(INT_MAX),
        use_dist_das_(false),
        cmd_name_(INVALID_NAME),
        cmd_group_(ObRedisCmdGroup::INVALID_CMD),
        cmd_type_(RedisCommandType::REDIS_COMMAND_INVALID)
  {}

  TO_STRING_KV(K_(need_snapshot),
               K_(lock_mode),
               K_(arity),
               K_(cmd_name),
               K_(use_dist_das),
               K_(cmd_group),
               K_(cmd_type));

  // is the imp of the cmd a read-only table operation
  // note: scan should set need_snapshot_ = false
  bool need_snapshot_;
  // lock mode
  REDIS_LOCK_MODE lock_mode_;
  // Number of parameters, including command name
  // e.g. "get key", arity = 2
  int arity_;
  // multi part cmd need global snapshot and das support
  bool use_dist_das_;
  // cmd name
  common::ObString cmd_name_;
  // cmd type
  ObRedisCmdGroup cmd_group_;
  RedisCommandType cmd_type_;
};

// The virtual base class of the redis command
class RedisCommand
{
public:
  using FieldValMap = common::hash::ObHashMap<ObString, ObString, common::hash::NoPthreadDefendMode>;
  using FieldSet = common::hash::ObHashSet<ObString, common::hash::NoPthreadDefendMode>;
  static constexpr int32_t DEFAULT_BUCKET_NUM = 1024;

  RedisCommand()
      : is_inited_(false),
        key_(),
        sub_key_()
  {
  }
  virtual ~RedisCommand()
  {}
  // Call command execution
  virtual int apply(ObRedisSingleCtx &redis_ctx) = 0;
  // set and check args here
  virtual int init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg) = 0;

  OB_INLINE common::ObString get_command_name() const
  {
    return attr_.cmd_name_;
  }
  OB_INLINE bool need_snapshot() const
  {
    return attr_.need_snapshot_;
  }

  OB_INLINE REDIS_LOCK_MODE get_lock_mode() const
  {
    return attr_.lock_mode_;
  }

  OB_INLINE bool use_dist_das() const
  {
    return attr_.use_dist_das_;
  }

  OB_INLINE ObRedisCmdGroup cmd_group() const
  {
    return attr_.cmd_group_;
  }

  OB_INLINE const ObRedisAttr &get_attributes() const
  {
    return attr_;
  }

  OB_INLINE RedisCommandType cmd_type() const { return attr_.cmd_type_; }
  OB_INLINE void set_cmd_type(RedisCommandType cmd_type) { attr_.cmd_type_ = cmd_type; }
  OB_INLINE void set_cmd_name(const ObString& cmd_name) { attr_.cmd_name_ = cmd_name; }

  OB_INLINE const ObString &key() const { return key_; }
  OB_INLINE const ObString &sub_key() const { return sub_key_; }

protected:
  int init_common(const common::ObIArray<common::ObString> &args);

  // property
  ObRedisAttr attr_;
  bool is_inited_;
  ObString key_;
  ObString sub_key_; // member in SET/ZSET, field in HASH

private:
  DISALLOW_COPY_AND_ASSIGN(RedisCommand);
};

struct ObRedisOp : public ObITableOp
{
  ObRedisOp()
      : outer_allocator_(nullptr),
        allocator_("ObRedisOp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        result_(&allocator_),
        default_entity_factory_("ObRedisOpFac", MTL_ID()),
        db_(0),
        redis_cmd_(nullptr),
        ls_id_(ObLSID::INVALID_LS_ID),
        tablet_id_(ObTabletID::INVALID_TABLET_ID),
        response_(allocator_, result_, ObTableRequsetType::TABLE_REQUEST_INVALID)
  {}
public:
  OB_INLINE virtual ObTabletID tablet_id() const override { return tablet_id_; }
  virtual int get_result(ObITableResult *&result) override;
  virtual void set_failed_result(int ret_code,
                                 ObTableEntity &result_entity,
                                 ObTableOperationType::Type op_type) override
  {
    result_.generate_failed_result(ret_code, result_entity, op_type);
  }
  int init(ObRedisSingleCtx &redis_ctx, RedisCommand *redis_cmd, ObTableGroupType type);

  void update_outer_allocator(common::ObIAllocator *allocator)
  {
    outer_allocator_ = allocator;
    response_.set_allocator(allocator);
  }

  virtual void reset() override;
  OB_INLINE int64_t db() const { return db_; }
  OB_INLINE ObRedisResponse& response() { return response_; }
  OB_INLINE RedisCommand* cmd() { return redis_cmd_; }
  OB_INLINE const RedisCommand* cmd() const { return redis_cmd_; }
  int get_key(ObString &key) const;

  VIRTUAL_TO_STRING_KV(K_(result),
                       K_(db),
                       K_(response),
                       KP(redis_cmd_),
                       K_(ls_id));

public:
  static const int64_t FIX_BUF_SIZE = 256;

public:
  OB_INLINE common::ObIAllocator& get_allocator()
  {
    return OB_NOT_NULL(outer_allocator_) ? *outer_allocator_ : allocator_;
  }
  OB_INLINE common::ObIAllocator& get_inner_allocator()
  {
    return allocator_;
  }
private:
  common::ObIAllocator *outer_allocator_; // refer to rpc allocator in read only operation
  common::ObArenaAllocator allocator_;
  // result_ must be accessed externally through the response_ member
  ObRedisResult result_;
public:
  table::ObTableEntityFactory<table::ObTableEntity> default_entity_factory_;
  int64_t db_;
  RedisCommand *redis_cmd_;
  ObLSID ls_id_;
  ObTabletID tablet_id_;
  char fix_buf_[FIX_BUF_SIZE]; // for RedisCommand
  ObRedisResponse response_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisOp);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_CMD_
