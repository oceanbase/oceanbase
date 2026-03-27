/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #pragma once

 #include "rpc/obrpc/ob_rpc_proxy.h"
 #include "rpc/obrpc/ob_rpc_processor.h"
 #include "share/table/ob_table_rpc_proxy.h"
 #include "ob_table_rpc_processor.h"
 #include "sql/plan_cache/ob_cache_object_factory.h"
 #include "sql/plan_cache/ob_plan_cache.h"
 #include "ob_table_context.h"

 namespace oceanbase
 {
 namespace observer
 {
 class ObHbaseExecuteP : public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_HBASE_EXECUTE> >
 {
   typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_HBASE_EXECUTE>> ParentType;

 public:
   explicit ObHbaseExecuteP(const ObGlobalContext &gctx);
   virtual ~ObHbaseExecuteP();
   virtual int before_process();
   virtual int try_process() override;
   virtual int deserialize();
   virtual int before_response(int error_code);
   virtual int response(const int retcode);
 protected:
   virtual int check_arg() override;
   virtual void reset_ctx() override;
   virtual uint64_t get_request_checksum() override { return 0; };
   virtual table::ObTableEntityType get_entity_type() override { return table::ObTableEntityType::ET_HKV_V2; }
   virtual bool is_kv_processor() override { return true; }
   virtual bool is_new_try_process() override { return true; }
 private:
   int init_tb_ctx(table::ObHCfRows &cf_rows, table::ObTableCtx &tb_ctx);
   int lock_rows();
   int init_schema_and_calc_part(share::ObLSID &ls_id);
   int decide_use_which_table_operation(table::ObTableCtx &tb_ctx);
   int check_mode_defense();
   ObTableProccessType get_stat_process_type();
 private:
   DISALLOW_COPY_AND_ASSIGN(ObHbaseExecuteP);
 };

 }  // end namespace observer
 }  // end namespace oceanbase