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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_CONTEXT_H_
#define OCEANBASE_OBSERVER_OB_REDIS_CONTEXT_H_

#include "observer/table/ob_table_context.h"
#include "share/table/ob_table_rpc_struct.h"
#include "share/table/redis/ob_redis_common.h"
#include "share/table/redis/ob_redis_util.h"
#include "observer/table/ob_table_trans_utils.h"
#include "observer/table/group/ob_i_table_struct.h"

namespace oceanbase
{
namespace table
{

struct ObRedisTableInfo {
  explicit ObRedisTableInfo(ObIAllocator &allocator):
    table_name_(),
    simple_schema_(nullptr),
    schema_cache_guard_(nullptr),
    tablet_ids_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "ObRedisTbIf"))
  {}
  virtual ~ObRedisTableInfo() {}
  TO_STRING_KV(K(table_name_), KPC(simple_schema_), KPC(schema_cache_guard_), K(tablet_ids_.count()));
  ObString table_name_;
  const share::schema::ObSimpleTableSchemaV2 *simple_schema_;
  ObKvSchemaCacheGuard *schema_cache_guard_;
  ObArray<ObTabletID> tablet_ids_;
};

class ObRedisCmdCtx
{
public:
  explicit ObRedisCmdCtx(ObIAllocator &allocator) :
    in_same_ls_(true),
    tb_infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "ObRedisCtx"))
  {}
  virtual ~ObRedisCmdCtx() {}

  int add_info(ObRedisTableInfo *tb_info);
  int get_info(int64_t idx, ObRedisTableInfo *&tb_info);
  OB_INLINE bool is_valid() const { return tb_infos_.count() == ObRedisInfoV1::REDIS_MODEL_NUM; }
  OB_INLINE bool get_in_same_ls() const { return in_same_ls_; }
  OB_INLINE void set_in_same_ls(bool in_same_ls) { in_same_ls_ = in_same_ls; }

private:
  bool in_same_ls_;
  ObArray<ObRedisTableInfo*> tb_infos_;
  DISALLOW_COPY_AND_ASSIGN(ObRedisCmdCtx);
};

struct ObRedisGuard {
  ObRedisGuard() { reset(); }
  ~ObRedisGuard() { reset(); }

  void reset() {
    schema_cache_guard_ = nullptr;
    schema_guard_ = nullptr;
    simple_table_schema_ = nullptr;
    sess_guard_ = nullptr;
  }

  TO_STRING_KV(KP(schema_cache_guard_), KP(schema_guard_), KP(simple_table_schema_), KP(sess_guard_));
  ObKvSchemaCacheGuard *schema_cache_guard_;
  ObSchemaGetterGuard *schema_guard_;
  const ObSimpleTableSchemaV2 *simple_table_schema_;
  ObTableApiSessGuard *sess_guard_;
};

// The context of the redis service, including the server information required for table_ctx and redis command
// execution
class ObRedisCtx
{
public:
  static const int64_t INVALID_TABLE_INDEX = -1;
protected:
  explicit ObRedisCtx(common::ObIAllocator &allocator, ObITableEntityFactory *entity_factory)
      : allocator_(allocator),
        retry_count_(0),
        cur_table_idx_(INVALID_TABLE_INDEX),
        cur_rowkey_idx_(0),
        is_cmd_support_group_(false),
        is_enable_group_op_(false)
  {
    reset();
  }

public:
  virtual ~ObRedisCtx()
  {}
  TO_STRING_KV(K_(redis_guard),
              K_(table_id),
              K_(tablet_id),
              K_(ls_id),
              K_(timeout_ts),
              K_(consistency_level),
              KPC_(credential),
              K_(cur_table_idx),
              K_(cur_rowkey_idx),
              K_(is_cmd_support_group),
              K_(is_enable_group_op),
              K_(did_async_commit),
              K_(need_dist_das));
  bool valid() const;

  void reset()
  {
    audit_ctx_ = nullptr;
    cmd_type_ = RedisCommandType::REDIS_COMMAND_INVALID;
    trans_param_ = nullptr;
    credential_ = nullptr;
    consistency_level_ = ObTableConsistencyLevel::EVENTUAL;
    table_id_ = common::OB_INVALID_ID;
    tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    timeout_ts_ = 0;
    timeout_ = 0;
    redis_guard_.reset();
    retry_count_ = 0;
    cur_table_idx_ = INVALID_TABLE_INDEX;
    cur_rowkey_idx_ = 0;
    is_cmd_support_group_ = false;
    is_enable_group_op_ = false;
    did_async_commit_ = false;
    need_dist_das_ = false;
  }
  int init_cmd_ctx(ObRowkey &cur_rowkey, const ObIArray<ObString> &keys);
  int init_cmd_ctx(int db, const ObString &table_name, const ObIArray<ObString> &keys);
  int try_get_table_info(ObRedisTableInfo *&tb_info);
  OB_INLINE void set_is_cmd_support_group(bool is_cmd_support_group) { is_cmd_support_group_ = is_cmd_support_group; }
  OB_INLINE bool is_cmd_support_group() const { return is_cmd_support_group_; }
  OB_INLINE void set_is_enable_group_op(bool is_enable_group_op) { is_enable_group_op_ = is_enable_group_op; }
  OB_INLINE bool is_enable_group_op() const { return is_enable_group_op_; }
  static int reset_objects(common::ObObj *objs, int64_t obj_cnt);
private:
  int get_tablet_id(const ObRowkey &rowkey,
                    const ObSimpleTableSchemaV2 &simple_table_schema,
                    ObTableApiSessGuard &sess_guard,
                    ObSchemaGetterGuard &schema_guard,
                    ObTabletID &tablet_id);

  int init_table_info(ObRowkey &rowkey,
                   ObSchemaGetterGuard &schema_guard,
                   ObTableApiSessGuard &sess_guard,
                   const ObString &table_name,
                   const ObIArray<ObString> &keys,
                   ObRedisTableInfo *&tb_info,
                   bool &is_in_same_ls,
                   ObLSID &last_ls_id);

public:
  common::ObIAllocator &allocator_; // refer to rpc processor
  ObITableEntityFactory *entity_factory_;
  RedisCommandType cmd_type_;
  // for init tb_ctx, save schema_guard/simple_table_schema/sess_guard/schema_cache_guard
  ObRedisGuard redis_guard_;
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  int64_t timeout_ts_;
  int64_t timeout_;
  ObTableApiCredential *credential_;
  // for transaction
  ObTableTransParam *trans_param_;
  ObTableConsistencyLevel consistency_level_;
  // for sql audit start
  int32_t retry_count_;
  common::ObAddr user_client_addr_;
  // for sql audit end
  ObTableAuditCtx *audit_ctx_;
  // for commands that may visit multi tables and multi rowkeys
  ObRedisCmdCtx *cmd_ctx_;
  int64_t cur_table_idx_;
  int64_t cur_rowkey_idx_;
  // for group op
  bool is_cmd_support_group_;
  bool is_enable_group_op_;
  bool did_async_commit_;
  bool need_dist_das_;


private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisCtx);
};


class ObRedisSingleCtx : public ObRedisCtx {
public:
  explicit ObRedisSingleCtx(
      common::ObIAllocator &allocator,
      ObITableEntityFactory *entity_factory,
      const ObITableRequest &table_request,
      ObRedisResult &table_result)
      : ObRedisCtx(allocator, entity_factory),
        request_(allocator, table_request),
        response_(allocator, table_result, table_request.get_type())
  {
    reset();
  }
  virtual ~ObRedisSingleCtx()
  {}

  void reset()
  {
    ObRedisCtx::reset();
    rpc_req_ = nullptr;
  }

  TO_STRING_KV(K_(request), K_(response));

  int decode_request();

  OB_INLINE int64_t get_request_db() const
  {
    return request_.get_db();
  }


public:
  // for redis command
  ObRedisRequest request_;
  // ObRedisResponse response_;
  rpc::ObRequest *rpc_req_;  // rpc request
  ObRedisResponse response_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisSingleCtx);
};

class ObRedisBatchCtx : public ObRedisCtx
{
public:
  explicit ObRedisBatchCtx(
    common::ObIAllocator &allocator,
    ObITableEntityFactory *entity_factory,
    ObIArray<ObITableOp *> &ops)
      : ObRedisCtx(allocator, entity_factory), ops_(ops) {}
  virtual ~ObRedisBatchCtx() {}
  OB_INLINE ObIArray<ObITableOp *> &ops() { return ops_; }

private:
  ObIArray<ObITableOp *> &ops_;
  DISALLOW_COPY_AND_ASSIGN(ObRedisBatchCtx);
};

} // end namespace table
} // end namespace oceanbase
#endif /* _OB_REDIS_SERVICE_H */
