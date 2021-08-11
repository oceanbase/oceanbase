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

#ifndef _OB_TABLE_BATCH_EXECUTE_PROCESSOR_H
#define _OB_TABLE_BATCH_EXECUTE_PROCESSOR_H 1
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "ob_table_rpc_processor.h"
#include "ob_table_service.h"
namespace oceanbase
{
namespace observer
{
/// @see RPC_S(PR5 batch_execute, obrpc::OB_TABLE_API_BATCH_EXECUTE, (table::ObTableBatchOperationRequest), table::ObTableBatchOperationResult);
class ObTableBatchExecuteP: public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_BATCH_EXECUTE> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_BATCH_EXECUTE> > ParentType;
public:
  explicit ObTableBatchExecuteP(const ObGlobalContext &gctx);
  virtual ~ObTableBatchExecuteP() = default;

  virtual int deserialize() override;
  virtual int response(const int retcode) override;
protected:
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) override;
  virtual void audit_on_finish() override;
  virtual uint64_t get_request_checksum() override;

private:
  int check_arg2() const;
  int get_rowkeys(common::ObIArray<common::ObRowkey> &rowkeys);
  int get_partition_ids(uint64_t table_id, common::ObIArray<int64_t> &part_ids);
  int multi_insert_or_update();
  int multi_get();
  int multi_delete();
  int multi_insert();
  int multi_replace();
  int multi_update();
  int batch_execute(bool is_readonly);
private:
  static const int64_t COMMON_COLUMN_NUM = 16;
  table::ObTableEntityFactory<table::ObTableEntity> default_entity_factory_;
  table::ObTableEntity result_entity_;
  common::ObArenaAllocator allocator_;
  ObTableServiceGetCtx table_service_ctx_;
  bool need_rollback_trans_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_BATCH_EXECUTE_PROCESSOR_H */
