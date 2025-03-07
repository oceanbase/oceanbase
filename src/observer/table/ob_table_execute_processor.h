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
#include "group/ob_table_group_service.h"

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
  virtual int before_process() override;
  virtual int process() override;
  virtual int response(const int retcode) override;
protected:
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  virtual uint64_t get_request_checksum() override;
  virtual int before_response(int error_code) override;
  virtual table::ObTableEntityType get_entity_type() override { return arg_.entity_type_; }
  virtual bool is_kv_processor() override { return true; }
private:
  int init_tb_ctx();
  int init_group_ctx(table::ObTableGroupCtx &ctx, share::ObLSID ls_id);
  ObTableProccessType get_stat_process_type();
  int check_arg2() const;
  int process_group_commit();
  int init_group_key();
  template<int TYPE>
  int process_dml_op()
  {
    int ret = OB_SUCCESS;
    table::ObTableExecuteCreateCbFunctor functor;

    if (OB_FAIL(start_trans(false,
                            arg_.consistency_level_,
                            tb_ctx_.get_ls_id(),
                            get_timeout_ts(),
                            tb_ctx_.need_dist_das()))) {
      SERVER_LOG(WARN, "fail to start transaction", K(ret), K_(tb_ctx));
    } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
      SERVER_LOG(WARN, "fail to init trans", K(ret));
    } else if (OB_FAIL(table::ObTableOpWrapper::process_op<TYPE>(tb_ctx_, result_))) {
      SERVER_LOG(WARN, "fail to process op", K(ret));
    } else if (OB_FAIL(functor.init(req_, &result_, arg_.table_operation_.type()))) {
      SERVER_LOG(WARN, "fail to init create execute callback functor", K(ret));
    }

    int tmp_ret = ret;
    if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, &functor))) {
      SERVER_LOG(WARN, "fail to end trans", K(ret));
    }

    ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
    return ret;
  }
  int process_get();
  int process_insert()
  {
    int ret = OB_SUCCESS;
    if (!tb_ctx_.is_ttl_table()) {
      ret = process_dml_op<table::TABLE_API_EXEC_INSERT>();
    } else {
      ret = process_dml_op<table::TABLE_API_EXEC_TTL>();
    }
    return ret;
  }
  // put override all columns, so no need to attention TTL
  int process_put()
  {
    return process_dml_op<table::TABLE_API_EXEC_INSERT>();
  }
  int process_insert_up()
  {
    int ret = OB_SUCCESS;
    if (!tb_ctx_.is_ttl_table()) {
      ret = process_dml_op<table::TABLE_API_EXEC_INSERT_UP>();
    } else {
      ret = process_dml_op<table::TABLE_API_EXEC_TTL>();
    }
    return ret;
  }
  int process_incr_or_append_op();
  bool is_group_commit_enable(table::ObTableOperationType::Type op_type) const;
private:
  table::ObTableEntity request_entity_;
  table::ObTableEntity result_entity_;
  common::ObArenaAllocator allocator_;
  table::ObTableCtx tb_ctx_;
  bool is_group_commit_;
  bool is_group_trigger_;
  table::ObTableOp *group_single_op_;
};


} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_EXECUTE_PROCESSOR_H */
