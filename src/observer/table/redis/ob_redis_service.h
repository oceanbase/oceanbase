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
#include "observer/table/ob_table_trans_utils.h"
#include "share/table/ob_redis_common.h"

namespace oceanbase
{
namespace table
{
// The context of the redis service, including the server information required for table_ctx and redis command
// execution
class ObRedisCtx
{
public:
  explicit ObRedisCtx(common::ObIAllocator &allocator,
                      const ObTableOperationRequest &table_request,
                      ObTableOperationResult &table_result)
      : allocator_(allocator),
        tb_ctx_(allocator_),
        request_(allocator_, table_request),
        response_(allocator_, table_result)
  {
    reset();
  }
  virtual ~ObRedisCtx()
  {}
  TO_STRING_KV(K_(tb_ctx),
              K_(table_id),
              K_(tablet_id),
              K_(ls_id),
              K_(timeout_ts),
              K_(consistency_level),
              KPC_(credential),
              K_(request),
              K_(response));
  bool valid() const;

  void reset()
  {
    stat_event_type_ = nullptr;
    trans_param_ = nullptr;
    credential_ = nullptr;
    entity_factory_ = nullptr;
    consistency_level_ = ObTableConsistencyLevel::EVENTUAL;
    table_id_ = common::OB_INVALID_ID;
    tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    timeout_ts_ = 0;
    tb_ctx_.reset();
    rpc_req_ = nullptr;
  }
  int decode_request();

  OB_INLINE const ObITableEntity &get_entity() const
  {
    return request_.get_entity();
  }

  OB_INLINE int64_t get_request_db() const
  {
    return request_.get_db();
  }

public:
  common::ObIAllocator &allocator_;
  ObITableEntityFactory *entity_factory_;
  int32_t *stat_event_type_;
  // for init tb_ctx
  ObTableCtx tb_ctx_;
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  int64_t timeout_ts_;
  ObTableApiCredential *credential_;
  // for transaction
  ObTableTransParam *trans_param_;
  ObTableConsistencyLevel consistency_level_;
  // for redis command
  ObRedisRequest request_;
  ObRedisResponse response_;
  rpc::ObRequest *rpc_req_;  // rpc request

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisCtx);
};

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
  static int execute(ObRedisCtx &ctx);
private:
  static int start_trans(ObRedisCtx &redis_ctx, bool need_snapshot);
  static int end_trans(ObRedisCtx &redis_ctx, bool need_snapshot, bool is_rollback);
  DISALLOW_COPY_AND_ASSIGN(ObRedisService);
};

} // end namespace table
} // end namespace oceanbase
#endif /* _OB_REDIS_SERVICE_H */
