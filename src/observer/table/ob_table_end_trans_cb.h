/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_TABLE_END_TRANS_CB_H
#define _OB_TABLE_END_TRANS_CB_H 1
#include "ob_table_rpc_response_sender.h"
#include "sql/ob_end_trans_callback.h"
#include "share/table/ob_table.h"
#include "ob_htable_lock_mgr.h"
#include "share/table/ob_table_rpc_struct.h"
#include "observer/table/group/ob_table_group_common.h"

namespace oceanbase
{
namespace table
{
class ObTableAPITransCb;
class ObTableCreateCbFunctor
{
public:
  ObTableCreateCbFunctor()
      : is_inited_(false)
  {}
  TO_STRING_KV(K_(is_inited));
  virtual ~ObTableCreateCbFunctor() = default;
public:
  virtual ObTableAPITransCb* new_callback() = 0;
protected:
  bool is_inited_;
};

class ObTableExecuteCreateCbFunctor : public ObTableCreateCbFunctor
{
public:
  ObTableExecuteCreateCbFunctor()
      : req_(nullptr),
        result_(nullptr),
        op_type_(ObTableOperationType::Type::INVALID)
  {}
  virtual ~ObTableExecuteCreateCbFunctor() = default;
public:
  int init(rpc::ObRequest *req, const ObTableOperationResult *result, ObTableOperationType::Type op_type);
  virtual ObTableAPITransCb* new_callback() override;
private:
  rpc::ObRequest *req_;
  const ObTableOperationResult *result_;
  ObTableOperationType::Type op_type_;
};

class ObTableBatchExecuteCreateCbFunctor : public ObTableCreateCbFunctor
{
public:
  ObTableBatchExecuteCreateCbFunctor()
      : req_(nullptr),
        result_(nullptr),
        op_type_(ObTableOperationType::Type::INVALID)
  {}
  virtual ~ObTableBatchExecuteCreateCbFunctor() = default;
public:
  int init(rpc::ObRequest *req, const ObTableBatchOperationResult *result, ObTableOperationType::Type op_type);
  virtual ObTableAPITransCb* new_callback() override;
private:
  rpc::ObRequest *req_;
  const ObTableBatchOperationResult *result_;
  ObTableOperationType::Type op_type_;
};

class ObTableLSExecuteEndTransCb;
class ObTableLSExecuteCreateCbFunctor : public ObTableCreateCbFunctor
{
public:
  ObTableLSExecuteCreateCbFunctor()
      : req_(nullptr),
        cb_(nullptr)
  {}
  virtual ~ObTableLSExecuteCreateCbFunctor() = default;
public:
  int init(rpc::ObRequest *req);
  virtual ObTableAPITransCb* new_callback() override;
private:
  rpc::ObRequest *req_;
  ObTableLSExecuteEndTransCb *cb_;
};

class ObTableAPITransCb: public sql::ObExclusiveEndTransCallback
{
public:
  ObTableAPITransCb();
  virtual ~ObTableAPITransCb();
  void destroy_cb_if_no_ref();
  void set_tx_desc(transaction::ObTxDesc *tx_desc) { tx_desc_ = tx_desc; }
  void set_lock_handle(ObHTableLockHandle *lock_handle);
protected:
  void check_callback_timeout();
protected:
  int64_t create_ts_;
  ObCurTraceId::TraceId trace_id_;
  transaction::ObTxDesc *tx_desc_;
  ObHTableLockHandle *lock_handle_; // hbase row lock handle
private:
  int32_t ref_count_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableAPITransCb);
};

class ObTableExecuteEndTransCb: public ObTableAPITransCb
{
public:
  ObTableExecuteEndTransCb(rpc::ObRequest *req, ObTableOperationType::Type table_operation_type)
    : allocator_(ObMemAttr(MTL_ID(), "TabelExeCbAlloc")),
      response_sender_(req, &result_)
  {
    result_.set_type(table_operation_type);
  }
  virtual ~ObTableExecuteEndTransCb() = default;

  virtual void callback(int cb_param) override;
  virtual void callback(int cb_param, const transaction::ObTransID &trans_id) override;
  virtual const char *get_type() const override { return "ObTableEndTransCallback"; }
  virtual sql::ObEndTransCallbackType get_callback_type() const override { return sql::ASYNC_CALLBACK_TYPE; }
  int assign_execute_result(const ObTableOperationResult &result);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableExecuteEndTransCb);
private:
  ObTableEntity result_entity_;
  common::ObArenaAllocator allocator_;
  ObTableOperationResult result_;
  obrpc::ObTableRpcResponseSender response_sender_;
};

class ObTableBatchExecuteEndTransCb: public ObTableAPITransCb
{
public:
  ObTableBatchExecuteEndTransCb(rpc::ObRequest *req, ObTableOperationType::Type table_operation_type)
    : allocator_(ObMemAttr(MTL_ID(), "TableBatCbAlloc")),
      entity_factory_("TableBatchCbEntFac", MTL_ID()),
      response_sender_(req, &result_),
      table_operation_type_(table_operation_type)
  {
  }
  virtual ~ObTableBatchExecuteEndTransCb() = default;

  virtual void callback(int cb_param) override;
  virtual void callback(int cb_param, const transaction::ObTransID &trans_id) override;
  virtual const char *get_type() const override { return "ObTableBatchEndTransCallback"; }
  virtual sql::ObEndTransCallbackType get_callback_type() const override { return sql::ASYNC_CALLBACK_TYPE; }
  int assign_batch_execute_result(const ObTableBatchOperationResult &result);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableBatchExecuteEndTransCb);
private:
  ObTableEntity result_entity_;
  common::ObArenaAllocator allocator_;
  ObTableEntityFactory<ObTableEntity> entity_factory_;
  ObTableBatchOperationResult result_;
  obrpc::ObTableRpcResponseSender response_sender_;
  ObTableOperationType::Type table_operation_type_;
};

class ObTableLSExecuteEndTransCb: public ObTableAPITransCb
{
public:
  ObTableLSExecuteEndTransCb(rpc::ObRequest *req)
    : allocator_(ObMemAttr(MTL_ID(), "TableLSCbAlloc")),
      entity_factory_("TableLSCbEntFac", MTL_ID()),
      response_sender_(req, &result_)
  {
  }
  virtual ~ObTableLSExecuteEndTransCb() = default;

  virtual void callback(int cb_param) override;
  virtual void callback(int cb_param, const transaction::ObTransID &trans_id) override;
  virtual const char *get_type() const override { return "ObTableLSEndTransCallback"; }
  virtual sql::ObEndTransCallbackType get_callback_type() const override { return sql::ASYNC_CALLBACK_TYPE; }
  int assign_ls_execute_result(const ObTableLSOpResult &result);
  OB_INLINE ObTableLSOpResult &get_result() { return result_; }
  OB_INLINE ObTableEntityFactory<ObTableSingleOpEntity> &get_entity_factory() { return entity_factory_; }
  OB_INLINE ObIAllocator &get_allocator() { return allocator_; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableLSExecuteEndTransCb);
private:
  common::ObArenaAllocator allocator_;
  ObTableSingleOpEntity result_entity_;
  ObTableEntityFactory<ObTableSingleOpEntity> entity_factory_;
  ObTableLSOpResult result_;
  obrpc::ObTableRpcResponseSender response_sender_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_END_TRANS_CB_H */
