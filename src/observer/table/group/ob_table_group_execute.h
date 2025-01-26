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
#include "ob_table_group_factory.h"

namespace oceanbase
{

namespace table
{
typedef common::ObFixedArray<ObTableOperation, common::ObIAllocator> OpFixedArray;
typedef common::ObFixedArray<ObTableOperationResult, common::ObIAllocator> ResultFixedArray;
struct ObTableGroupCtx
{
public:
  ObTableGroupCtx(common::ObIAllocator &allocator)
      : allocator_(allocator),
        retry_count_(0),
        audit_ctx_(retry_count_, user_client_addr_, false/* need_audit */)
  {
    reset();
  }
  virtual ~ObTableGroupCtx() = default;
  void reset()
  {
    key_ = nullptr;
    group_type_ = ObTableGroupType::TYPE_INVALID;
    type_ = ObTableOperationType::Type::INVALID;
    entity_type_ = ObTableEntityType::ET_DYNAMIC;
    table_id_ = OB_INVALID_ID;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    schema_version_ = OB_INVALID_VERSION;
    timeout_ts_ = 0;
    trans_param_ = nullptr;
    schema_guard_ = nullptr;
    simple_schema_ = nullptr;
    sess_guard_ = nullptr;
    schema_cache_guard_ = nullptr;
    retry_count_ = 0;
    create_cb_functor_ = nullptr;
  }
  TO_STRING_KV(KPC_(key),
               K_(group_type),
               K_(type),
               K_(entity_type),
               K_(credential),
               K_(table_id),
               K_(ls_id),
               K_(schema_version),
               K_(timeout_ts),
               KPC_(trans_param),
               KPC_(sess_guard),
               KPC_(schema_cache_guard),
               K_(retry_count),
               K_(user_client_addr),
               K_(audit_ctx));
public:
  common::ObIAllocator &allocator_; // refer to rpc allocator or tmp allocator in backgroup group task
  ObITableGroupKey *key_;
  ObTableGroupType group_type_;
  ObTableOperationType::Type type_;
  ObTableEntityType entity_type_;
  ObTableApiCredential credential_;
  common::ObTableID table_id_;
  share::ObLSID ls_id_;
  int64_t schema_version_;
  int64_t timeout_ts_;
  ObTableTransParam *trans_param_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  const schema::ObSimpleTableSchemaV2 *simple_schema_;
  ObTableApiSessGuard *sess_guard_;
  ObKvSchemaCacheGuard *schema_cache_guard_;
  // for sql audit start
  int32_t retry_count_;
  common::ObAddr user_client_addr_;
  ObTableAuditCtx audit_ctx_;
  // for sql audit end
  // for end trans callback
  ObTableCreateCbFunctor *create_cb_functor_;
};

class ObTableGroupCommitEndTransCb: public ObTableAPITransCb
{
public:
  explicit ObTableGroupCommitEndTransCb(ObTableGroup &group,
                                        bool add_failed_group,
                                        ObTableFailedGroups *failed_groups,
                                        ObTableGroupFactory<ObTableGroup> *group_factory,
                                        ObTableGroupOpFactory *op_factory)
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
private:
  int add_failed_groups();
  int response();
  int response_failed_results(int ret_code);
public:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  ObTableEntity result_entity_;
  ObTableEntityFactory<ObTableEntity> entity_factory_;
  ObTableGroup &group_;
  bool add_failed_group_;
  ObTableFailedGroups *failed_groups_;
  ObTableGroupFactory<ObTableGroup> *group_factory_;
  ObTableGroupOpFactory *op_factory_;
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
  int init(ObTableGroup *group,
           bool add_failed_group,
           ObTableFailedGroups *failed_groups,
           ObTableGroupFactory<ObTableGroup> *group_factory,
           ObTableGroupOpFactory *op_factory);
  virtual ObTableAPITransCb* new_callback() override;
private:
  ObTableGroup *group_;
  bool add_failed_group_;
  ObTableFailedGroups *failed_groups_;
  ObTableGroupFactory<ObTableGroup> *group_factory_;
  ObTableGroupOpFactory *op_factory_;
  ObTableGroupCommitEndTransCb *cb_;
};

struct ObTableOp : public ObITableOp
{
public:
  ObTableOp()
      : ObITableOp(ObTableGroupType::TYPE_TABLE_GROUP),
        result_(),
        op_(),
        request_entity_(),
        result_entity_(),
        ls_id_(ObLSID::INVALID_LS_ID),
        tablet_id_(ObTabletID::INVALID_TABLET_ID),
        is_insup_use_put_(false)
  {}
  virtual ~ObTableOp() {}
  VIRTUAL_TO_STRING_KV(K_(result),
                       K_(op),
                       K_(request_entity),
                       K_(result_entity),
                       K_(tablet_id),
                       K_(is_insup_use_put),
                       K_(result));
public:
  OB_INLINE virtual int get_result(ObITableResult *&result) override
  {
    result = &result_;
    return common::OB_SUCCESS;
  }
  OB_INLINE virtual ObTabletID tablet_id() const override { return tablet_id_; }
  virtual void set_failed_result(int ret_code,
                                 ObTableEntity &result_entity,
                                 ObTableOperationType::Type op_type) override
  {
    result_.generate_failed_result(ret_code, result_entity, op_type);
  }
  OB_INLINE bool is_get() const { return op_.type() == ObTableOperationType::Type::GET; }
  OB_INLINE bool is_insup_use_put() const { return is_insup_use_put_; }
  virtual void reset()
  {
    ObITableOp::reset();
    result_.reset();
    op_.reset();
    request_entity_.reset();
    result_entity_.reset();
    ls_id_ = ObLSID::INVALID_LS_ID;
    tablet_id_.reset();
    is_insup_use_put_ = false;
    result_.set_entity(nullptr);
    result_.reset();
  }
  void reuse()
  {
    reset();
  }
public:
  ObTableOperationResult result_;
  ObTableOperation op_; // single operation
  ObTableEntity request_entity_;
  ObTableEntity result_entity_; // used to be add result
  ObLSID ls_id_;
  ObTabletID tablet_id_;
  bool is_insup_use_put_;
};

struct ObTableGroupKey : public ObITableGroupKey
{
public:
  ObTableGroupKey(share::ObLSID ls_id,
                      ObTableID table_id,
                      int64_t schema_version,
                      ObTableOperationType::Type op_type)
    : ObITableGroupKey(ObTableGroupType::TYPE_TABLE_GROUP), // tmp
      ls_id_(ls_id),
      table_id_(table_id),
      schema_version_(schema_version),
      op_type_(op_type),
      is_insup_use_put_(false)
  {}

  ObTableGroupKey()
    : ObITableGroupKey(ObTableGroupType::TYPE_TABLE_GROUP), // tmp
      ls_id_(),
      table_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      op_type_(ObTableOperationType::Type::INVALID),
      is_insup_use_put_(false)
  {}
  virtual ~ObTableGroupKey() {}
  virtual uint64_t hash() const override;
  virtual int deep_copy(common::ObIAllocator &allocator, const ObITableGroupKey &other) override;
  virtual bool is_equal(const ObITableGroupKey &other) const override;
  VIRTUAL_TO_STRING_KV(K_(ls_id),
                       K_(table_id),
                       K_(schema_version),
                       K_(op_type),
                       K_(is_insup_use_put));
public:
  share::ObLSID ls_id_;
  common::ObTableID table_id_;
  int64_t schema_version_;
  ObTableOperationType::Type op_type_;
  bool is_insup_use_put_;  // just for marked the source op_type
};

class ObTableOpProcessor: public ObITableOpProcessor
{
public:
  ObTableOpProcessor()
    : ObITableOpProcessor(),
      allocator_("GrpTblPrcAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      batch_ctx_(nullptr),
      is_get_(false)
  {}
  ObTableOpProcessor(ObTableGroupType op_type,
                     ObTableGroupCtx *group_ctx,
                     ObIArray<ObITableOp *> *ops,
                     ObTableCreateCbFunctor *functor)
    : ObITableOpProcessor(op_type, group_ctx, ops, functor),
      allocator_("GrpTblPrcAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      batch_ctx_(nullptr),
      is_get_(false)
  {}
  virtual ~ObTableOpProcessor()
  {
    if (OB_NOT_NULL(batch_ctx_)) {
      batch_ctx_->~ObTableBatchCtx();
    }
  }
  virtual int init(ObTableGroupCtx &group_ctx, ObIArray<ObITableOp*> *ops);
  virtual int process() override;
private:
  int init_batch_params(ObTableBatchCtx &batch_ctx,
                        ObIArray<ObTableOperation> &batch_ops,
                        ObIArray<ObTableOperationResult> &batch_result);
  int init_table_ctx(ObTableBatchCtx &batch_ctx);
  int execute_dml();
  int execute_read();
  int dispatch_batch_result(ObIArray<ObTableOperationResult> &batch_result);
private:
  ObArenaAllocator allocator_;
  ObTableBatchCtx *batch_ctx_;
  bool is_get_;
};

class ObTableGroupExecuteService final
{
public:
  static const int64_t DEFAULT_TRANS_TIMEOUT = 3 * 1000 * 1000L; // 3s
  static int execute(ObTableGroup &group, bool add_fail_group);
  static int process_result(int ret_code,
                            ObTableGroup &group,
                            bool is_direct_execute,
                            bool add_failed_group);
  static int response(ObTableGroup &group,
                      ObTableGroupFactory<ObTableGroup> &group_factory,
                      ObTableGroupOpFactory &op_factory);
  static int response_failed_results(int ret_code,
                                     ObTableGroup &group,
                                     ObTableGroupFactory<ObTableGroup> &group_factory,
                                     ObTableGroupOpFactory &op_factory);
  static int start_trans(ObTableBatchCtx &batch_ctx);
  static int end_trans(const ObTableBatchCtx &batch_ctx,
                       ObTableCreateCbFunctor *create_cb_functor,
                       bool is_rollback);
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_GROUP_EXECUTE_H_ */
