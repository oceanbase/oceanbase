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

#ifndef _OB_REDIS_SERVICE_H
#define _OB_REDIS_SERVICE_H

#include "observer/table/ob_table_context.h"
#include "share/table/ob_table_rpc_struct.h"
#include "share/table/redis/ob_redis_common.h"
#include "share/table/redis/ob_redis_util.h"
#include "ob_redis_context.h"
#include "group/ob_redis_group_struct.h"

namespace oceanbase
{
namespace table
{
class RedisCommand;
class ObRedisOp;
class ObRedisAttr;

class ObTableRedisEndTransCb : public ObTableAPITransCb
{
public:
  ObTableRedisEndTransCb(rpc::ObRequest *req)
      : allocator_(ObMemAttr(MTL_ID(), "RedisCbAlloc")), response_sender_(req, &result_)
  {
    result_.set_type(ObTableOperationType::REDIS);
  }
  virtual ~ObTableRedisEndTransCb() = default;

  virtual void callback(int cb_param) override;
  virtual void callback(int cb_param, const transaction::ObTransID &trans_id) override
  {
    UNUSED(trans_id);
    this->callback(cb_param);
  }
  virtual const char *get_type() const override
  {
    return "ObTableRedisEndTransCallback";
  }
  virtual sql::ObEndTransCallbackType get_callback_type() const override
  {
    return sql::ASYNC_CALLBACK_TYPE;
  }
  int assign_execute_result(const ObTableOperationResult &result);

private:
  ObTableEntity result_entity_;
  common::ObArenaAllocator allocator_;
  ObTableOperationResult result_;
  obrpc::ObTableRpcResponseSender<ObTableOperationResult> response_sender_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableRedisEndTransCb);
};

class ObTableRedisCbFunctor : public ObTableCreateCbFunctor
{
public:
  ObTableRedisCbFunctor() : req_(nullptr), result_(nullptr)
  {}
  virtual ~ObTableRedisCbFunctor() = default;

public:
  int init(rpc::ObRequest *req, const ObTableOperationResult *result);
  virtual ObTableAPITransCb *new_callback() override;

private:
  rpc::ObRequest *req_;
  const ObTableOperationResult *result_;
};

class ObRedisService
{
public:
  static int execute(ObRedisSingleCtx &ctx);
  static int execute_cmd_single(ObRedisSingleCtx &ctx);
  static int execute_cmd_group(ObRedisSingleCtx &ctx);
  static int init_group_ctx(ObTableGroupCtx &group_ctx, const ObRedisSingleCtx &ctx, ObRedisOp &cmd, ObRedisCmdKey *key);
  static int start_trans(ObRedisCtx &redis_ctx, bool need_snapshot);
private:
  static int cover_to_redis_err(int ob_ret);
  static int end_trans(ObRedisSingleCtx &redis_ctx, bool need_snapshot, bool is_rollback);
  static int redis_cmd_to_proccess_type(const RedisCommandType &attr, observer::ObTableProccessType& ret_type);
  DISALLOW_COPY_AND_ASSIGN(ObRedisService);
};

} // end namespace table
} // end namespace oceanbase
#endif /* _OB_REDIS_SERVICE_H */
