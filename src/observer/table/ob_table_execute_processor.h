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

#ifndef _OB_TABLE_EXECUTE_PROCESSOR_H
#define _OB_TABLE_EXECUTE_PROCESSOR_H 1
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "ob_table_rpc_processor.h"
#include "ob_table_context.h"
#include "ob_table_executor.h"
#include "ob_table_cache.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "ob_table_op_wrapper.h"

namespace oceanbase
{
namespace observer
{
/// @see RPC_S(PR5 execute, obrpc::OB_TABLE_API_EXECUTE, (table::ObTableOperationRequest), table::ObTableOperationResult);
class ObTableApiExecuteP: public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE> > ParentType;
public:
  explicit ObTableApiExecuteP(const ObGlobalContext &gctx);
  virtual ~ObTableApiExecuteP() = default;

  virtual int deserialize() override;
  virtual int process() override;
  virtual int response(const int retcode) override;
protected:
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) override;
  virtual void audit_on_finish() override;
  virtual uint64_t get_request_checksum() override;
  virtual int before_response(int error_code) override;

private:
  int init_tb_ctx();
  int check_arg2() const;
  int get_tablet_id(uint64_t table_id, const ObRowkey &rowkey, common::ObTabletID &tablet_id);
  template<int TYPE>
  int process_dml_op()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(start_trans(false, /* is_readonly */
                            sql::stmt::T_INSERT,
                            arg_.consistency_level_,
                            tb_ctx_.get_table_id(),
                            tb_ctx_.get_ls_id(),
                            get_timeout_ts()))) {
      SERVER_LOG(WARN, "fail to start trans", K(ret));
    } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
      SERVER_LOG(WARN, "fail to init trans", K(ret));
    } else if (OB_FAIL(table::ObTableOpWrapper::process_op<TYPE>(tb_ctx_, result_))) {
      SERVER_LOG(WARN, "fail to process op", K(ret));
    }

    result_.set_errno(ret);
    table::ObTableApiUtil::replace_ret_code(ret);
    int tmp_ret = ret;
    if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
      SERVER_LOG(WARN, "fail to end trans", K(ret));
    }

    ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
    return ret;
  }
  int process_get();
private:
  table::ObTableEntity request_entity_;
  table::ObTableEntity result_entity_;
  common::ObArenaAllocator allocator_;
  table::ObTableCtx tb_ctx_;
  table::ObTableEntityFactory<table::ObTableEntity> default_entity_factory_;
  bool need_rollback_trans_;
  int64_t query_timeout_ts_;
};


} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_EXECUTE_PROCESSOR_H */
