/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_GROUP_EXECUTE_H_
#define OCEANBASE_OBSERVER_OB_TABLE_GROUP_EXECUTE_H_

#include "ob_table_group_common.h"
#include "observer/table/ob_table_batch_service.h"
#include "share/table/ob_table.h"
#include "rpc/ob_request.h"
#include "observer/table/ob_table_audit.h"

namespace oceanbase
{

namespace table
{

struct ObTableGroupCtx
{
public:
  ObTableGroupCtx()
      : retry_count_(0),
        audit_ctx_(retry_count_, user_client_addr_, false/* need_audit */)
  {
    reset();
  }
  virtual ~ObTableGroupCtx() = default;
  void reset()
  {
    key_ = nullptr;
    add_group_success_ = false;
    entity_type_ = ObTableEntityType::ET_DYNAMIC;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    is_get_ = false;
    timeout_ts_ = 0;
    trans_param_ = nullptr;
    schema_guard_ = nullptr;
    simple_schema_ = nullptr;
    sess_guard_ = nullptr;
    schema_cache_guard_ = nullptr;
    failed_groups_ = nullptr;
    group_factory_ = nullptr;
    op_factory_ = nullptr;
    retry_count_ = 0;
  }
  TO_STRING_KV(KPC_(key),
               K_(add_group_success),
               K_(entity_type),
               K_(credential),
               K_(ls_id),
               K_(is_get),
               K_(timeout_ts),
               KPC_(trans_param),
               KPC_(sess_guard),
               KPC_(schema_cache_guard),
               KPC_(failed_groups),
               KPC_(group_factory),
               KPC_(op_factory),
               K_(retry_count),
               K_(user_client_addr),
               K_(audit_ctx));
public:
  ObTableGroupCommitKey *key_;
  bool add_group_success_;
  ObTableEntityType entity_type_;
  ObTableApiCredential credential_;
  share::ObLSID ls_id_;
  bool is_get_;
  int64_t timeout_ts_;
  ObTableTransParam *trans_param_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  const schema::ObSimpleTableSchemaV2 *simple_schema_;
  ObTableApiSessGuard *sess_guard_;
  ObKvSchemaCacheGuard *schema_cache_guard_;
  ObTableFailedGroups *failed_groups_;
  ObTableGroupFactory<ObTableGroupCommitOps> *group_factory_;
  ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory_;
  // for sql audit start
  int32_t retry_count_;
  common::ObAddr user_client_addr_;
  ObTableAuditCtx audit_ctx_;
  // for sql audit end
};


class ObTableGroupCommitEndTransCb: public ObTableAPITransCb
{
public:
  explicit ObTableGroupCommitEndTransCb(ObTableGroupCommitOps &group,
                                        bool add_failed_group,
                                        ObTableFailedGroups *failed_groups,
                                        ObTableGroupFactory<ObTableGroupCommitOps> *group_factory,
                                        ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory)
      : is_inited_(false),
        allocator_("TbGroupCb", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        entity_factory_("TbGroupCbEntFac", MTL_ID()),
        group_(group),
        add_failed_group_(add_failed_group),
        failed_groups_(failed_groups),
        group_factory_(group_factory),
        op_factory_(op_factory)
  {}
  virtual ~ObTableGroupCommitEndTransCb() = default;
  virtual void callback(int cb_param) override;
  virtual void callback(int cb_param, const transaction::ObTransID &trans_id) override
  {
    UNUSED(trans_id);
    this->callback(cb_param);
  }
  virtual const char *get_type() const override { return "ObTableGroupCommitEndTransCb"; }
  virtual sql::ObEndTransCallbackType get_callback_type() const override { return sql::ASYNC_CALLBACK_TYPE; }
public:
  int init();
private:
  int add_failed_groups();
  int response();
  int response_failed_results(int ret_code);
public:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  ObTableEntity result_entity_;
  ObTableEntityFactory<ObTableEntity> entity_factory_;
  ObTableGroupCommitOps &group_;
  ResultFixedArray results_;
  bool add_failed_group_;
  ObTableFailedGroups *failed_groups_;
  ObTableGroupFactory<ObTableGroupCommitOps> *group_factory_;
  ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableGroupCommitEndTransCb);
};

class ObTableGroupCommitCreateCbFunctor : public ObTableCreateCbFunctor
{
public:
  ObTableGroupCommitCreateCbFunctor()
      : add_failed_group_(false),
        failed_groups_(nullptr),
        group_factory_(nullptr),
        op_factory_(nullptr),
        cb_(nullptr)
  {}
  virtual ~ObTableGroupCommitCreateCbFunctor() = default;
public:
  int init(ObTableGroupCommitOps *group,
           bool add_failed_group,
           ObTableFailedGroups *failed_groups,
           ObTableGroupFactory<ObTableGroupCommitOps> *group_factory,
           ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory);
  virtual ObTableAPITransCb* new_callback() override;
private:
  ObTableGroupCommitOps *group_;
  bool add_failed_group_;
  ObTableFailedGroups *failed_groups_;
  ObTableGroupFactory<ObTableGroupCommitOps> *group_factory_;
  ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory_;
  ObTableGroupCommitEndTransCb *cb_;
};

class ObTableGroupExecuteService final
{
public:
  static const int64_t DEFAULT_TRANS_TIMEOUT = 3 * 1000 * 1000L; // 3s
  static int execute(ObTableGroupCtx &ctx,
                     ObTableGroupCommitOps &group,
                     bool add_failed_group = true);
  static int execute(ObTableGroupCommitOps &group,
                     ObTableFailedGroups *failed_groups,
                     ObTableGroupFactory<ObTableGroupCommitOps> *group_factory,
                     ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory,
                     bool add_failed_group = true);
  static int response(ObTableGroupCommitOps &group,
                      ObTableGroupFactory<ObTableGroupCommitOps> *group_factory,
                      ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory,
                      common::ObIArray<ObTableOperationResult> &results);
  static int response_failed_results(int ret_code,
                                     ObTableGroupCommitOps &group,
                                     ObTableGroupFactory<ObTableGroupCommitOps> *group_factory,
                                     ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory);
  static int generate_failed_results(int ret_code,
                                     ObITableEntity &result_entity,
                                     ObTableGroupCommitOps &group,
                                     common::ObIArray<ObTableOperationResult> &results);
private:
  static int start_trans(ObTableBatchCtx &batch_ctx);
  static int end_trans(const ObTableBatchCtx &batch_ctx,
                       ObTableGroupCommitCreateCbFunctor *create_cb_functor,
                       bool is_rollback);
  static int init_table_ctx(ObTableGroupCommitOps &group, ObTableCtx &tb_ctx);
  static int init_batch_ctx(ObTableGroupCommitOps &group, ObTableBatchCtx &batch_ctx);
  static void free_ops(ObTableGroupCommitOps &group,
                       ObTableGroupFactory<ObTableGroupCommitSingleOp> &op_factory);
  static int execute_read(ObTableGroupCtx &ctx,
                          ObTableGroupCommitOps &group,
                          bool add_failed_group);
  static int execute_dml(ObTableGroupCtx &ctx,
                         ObTableGroupCommitOps &group,
                         bool add_failed_group);
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_GROUP_EXECUTE_H_ */
