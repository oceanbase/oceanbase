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
namespace oceanbase
{
namespace table
{
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
      :response_sender_(req, result_)
  {
    result_.set_type(table_operation_type);
  }
  virtual ~ObTableExecuteEndTransCb() = default;

  virtual void callback(int cb_param) override;
  virtual void callback(int cb_param, const transaction::ObTransID &trans_id) override;
  virtual const char *get_type() const override { return "ObTableEndTransCallback"; }
  virtual sql::ObEndTransCallbackType get_callback_type() const override { return sql::ASYNC_CALLBACK_TYPE; }
  int assign_execute_result(ObTableOperationResult &result);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableExecuteEndTransCb);
private:
  ObTableEntity result_entity_;
  common::ObArenaAllocator allocator_;
  ObTableOperationResult result_;
  obrpc::ObTableRpcResponseSender<ObTableOperationResult> response_sender_;
};

class ObTableBatchExecuteEndTransCb: public ObTableAPITransCb
{
public:
  ObTableBatchExecuteEndTransCb(rpc::ObRequest *req, ObTableOperationType::Type table_operation_type)
      : entity_factory_("TableBatchCbEntFac", MTL_ID()),
      response_sender_(req, result_),
      table_operation_type_(table_operation_type)
  {
  }
  virtual ~ObTableBatchExecuteEndTransCb() = default;

  virtual void callback(int cb_param) override;
  virtual void callback(int cb_param, const transaction::ObTransID &trans_id) override;
  virtual const char *get_type() const override { return "ObTableBatchEndTransCallback"; }
  virtual sql::ObEndTransCallbackType get_callback_type() const override { return sql::ASYNC_CALLBACK_TYPE; }
  int assign_batch_execute_result(ObTableBatchOperationResult &result);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableBatchExecuteEndTransCb);
private:
  ObTableEntity result_entity_;
  common::ObArenaAllocator allocator_;
  table::ObTableEntityFactory<table::ObTableEntity> entity_factory_;
  ObTableBatchOperationResult result_;
  obrpc::ObTableRpcResponseSender<ObTableBatchOperationResult> response_sender_;
  ObTableOperationType::Type table_operation_type_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_END_TRANS_CB_H */
