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

 #define USING_LOG_PREFIX SERVER
 #include "ob_hbase_execute_processor.h"
 #include "ob_table_move_response.h"
 #include "cf_service/ob_hbase_column_family_service.h"
 #include "part_calc/ob_table_part_calc.h"
 #include "tableapi/ob_table_api_service.h"
 #include "observer/table/adapters/ob_hbase_adapter_factory.h"

 using namespace oceanbase::observer;
 using namespace oceanbase::common;
 using namespace oceanbase::table;
 using namespace oceanbase::share;
 using namespace oceanbase::sql;

 ObHbaseExecuteP::ObHbaseExecuteP(const ObGlobalContext &gctx)
     : ObTableRpcProcessor(gctx)
 {
   allocator_.set_attr(ObMemAttr(MTL_ID(), "TbHbaseExeP", ObCtxIds::DEFAULT_CTX_ID));
 }

 int ObHbaseExecuteP::deserialize()
 {
   int ret = OB_SUCCESS;
   arg_.set_deserialize_allocator(&allocator_);
   if (OB_FAIL(ParentType::deserialize())) {
     LOG_WARN("fail to deserialize parent type", K(ret));
   }
   return ret;
 }

 ObHbaseExecuteP::~ObHbaseExecuteP()
 {
  arg_.reset();
 }

 int ObHbaseExecuteP::before_process()
 {
   int ret = OB_SUCCESS;
   is_tablegroup_req_ = arg_.cf_rows_.count() > 1;
   retry_policy_.allow_route_retry_ = arg_.server_can_retry();
   if (OB_FAIL(ParentType::before_process())) {
     LOG_WARN("before process failed", K(ret));
   }

   return ret;
 }

 int ObHbaseExecuteP::check_arg()
 {
   int ret = OB_SUCCESS;
   return ret;
 }

 void ObHbaseExecuteP::reset_ctx()
 {
   ObTableApiProcessorBase::reset_ctx();
   need_retry_in_queue_ = false;
 }

 int ObHbaseExecuteP::init_schema_and_calc_part(ObLSID &ls_id)
 {
  int ret = OB_SUCCESS;
  bool is_same_ls = true;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(credential_.tenant_id_));
  }
  for (int i = 0; OB_SUCC(ret) && i < arg_.cf_rows_.count(); ++i) {
    const ObSimpleTableSchemaV2 *simple_schema = nullptr;
    ObLSID cur_ls_id(ObLSID::INVALID_LS_ID);
    ObHCfRows &same_cf_rows = arg_.cf_rows_.at(i);
    ObString &real_table_name = same_cf_rows.real_table_name_;
    if (OB_FAIL(schema_guard_.get_simple_table_schema(credential_.tenant_id_,
                                                      credential_.database_id_,
                                                      real_table_name,
                                                      false, /* is_index */
                                                      simple_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(real_table_name), K(credential_.tenant_id_), K(credential_.database_id_));
    } else if (OB_ISNULL(simple_schema) || simple_schema->get_table_id() == OB_INVALID_ID) {
      ret = OB_TABLE_NOT_EXIST;
      ObString db("");
      LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE, real_table_name.length(), real_table_name.ptr(), db.length(), db.ptr());
      LOG_WARN("table not exist", K(ret), K(credential_.tenant_id_), K(credential_.database_id_), K(real_table_name));
    } else if (simple_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_USER_ERROR(OB_ERR_OPERATION_ON_RECYCLE_OBJECT);
      LOG_WARN("table is in recycle bin, not allow to do operation", K(ret), K(credential_.tenant_id_),
                  K(credential_.database_id_), K(real_table_name));
    } else if (!schema_cache_guard_.is_inited() && OB_FAIL(schema_cache_guard_.init(credential_.tenant_id_,
                                                                                    simple_schema->get_table_id(),
                                                                                    simple_schema->get_schema_version(),
                                                                                    schema_guard_))) {
      LOG_WARN("fail to init schema cache guard", K(ret));
    } else {
      same_cf_rows.set_table_schema(simple_schema);
      ObTablePartCalculator part_calc(allocator_,
                                      sess_guard_,
                                      schema_cache_guard_,
                                      schema_guard_,
                                      simple_schema);
      if (OB_FAIL(part_calc.calc(simple_schema, same_cf_rows, is_same_ls, cur_ls_id))) {
        LOG_WARN("fail to calculate part", K(ret));
      } else {
        if (i == 0) {
          if (is_same_ls) {
            ls_id = cur_ls_id;
          }
        } else if (is_same_ls) {
          is_same_ls = (ls_id == cur_ls_id);
          if (!is_same_ls) {
            // not same ls, reset ls_id to avoid misunderstanding
            ls_id = ObLSID::INVALID_LS_ID;
          }
        }
      }
    }
  } // end for
  return ret;
 }

 ObTableProccessType ObHbaseExecuteP::get_stat_process_type()
 {
  return ObTableProccessType::TABLE_API_HBASE_PUT;
 }

 int ObHbaseExecuteP::try_process()
 {
  int ret = OB_SUCCESS;
  ObTableConsistencyLevel consistency_level = ObTableConsistencyLevel::STRONG;
  stat_process_type_ = get_stat_process_type();
  ObLSID ls_id(ObLSID::INVALID_LS_ID);
  ObTabletID route_tablet_id(ObTabletID::INVALID_TABLET_ID);
  if (OB_FAIL(init_schema_and_calc_part(ls_id))) {
    LOG_WARN("fail to init schema and calc part", K(ret));
  } else if (FALSE_IT(route_tablet_id = arg_.cf_rows_.at(0).get_cf_row(0).get_cell(0).get_tablet_id())) {
  } else if (!route_tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("route tablet id is invalid", K(ret));
  } else if (OB_FAIL(lock_rows())) {
    LOG_WARN("fail to lock rows", K(ret));
  } else if (OB_FAIL(start_trans(false,
                                 consistency_level,
                                 ls_id,
                                 get_timeout_ts(),
                                 !ls_id.is_valid() /*need_global_snapshot*/))) {
    LOG_WARN("fail to start trans", K(ret), K(ls_id));
  } else if (!trans_param_.tx_snapshot_.is_ls_snapshot()
             && OB_FAIL(check_local_execute(route_tablet_id))) {
    LOG_WARN("fail to check local execute", K(ret), K(route_tablet_id));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < arg_.cf_rows_.count(); ++i) {
      SMART_VAR(ObTableCtx, tb_ctx, allocator_) {
        ObHbaseAdapterGuard adapter_guard(allocator_);
        ObIHbaseAdapter *adapter = nullptr;
        if (OB_FAIL(adapter_guard.get_hbase_adapter(adapter, schema_cache_guard_.get_hbase_mode_type()))) {
          LOG_WARN("fail to get hbase adapter", K(ret));
        } else if (OB_FAIL(init_tb_ctx(arg_.cf_rows_.at(i), tb_ctx))) {
          LOG_WARN("fail to init tb ctx", K(ret));
        } else if (OB_FAIL(adapter->put(tb_ctx, arg_.cf_rows_.at(i)))) {
          LOG_WARN("fail to put", K(ret), K(arg_.cf_rows_.at(i)), K(i));
        }
      }
    }
  }
  result_.set_err(ret);
  result_.set_op_type(arg_.op_type_);
  ObHbaseExecuteCreateCbFunctor functor;
  if (OB_SUCC(ret) && OB_FAIL(functor.init(req_, &result_))) {
    LOG_WARN("fail to init create execute callback functor", K(ret));
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, &functor))) {
    LOG_WARN("fail to end trans", K(ret));
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

  #ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute hbase operation", K(ret), K(arg_.op_type_), K_(retry_count), K(arg_.option_flag_));
  #endif

  return ret;
 }

 int ObHbaseExecuteP::init_tb_ctx(ObHCfRows &cf_rows, ObTableCtx &tb_ctx)
 {
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
  tb_ctx.set_entity_type(ObTableEntityType::ET_HKV_V2);
  tb_ctx.set_schema_cache_guard(&schema_cache_guard_);
  tb_ctx.set_schema_guard(&schema_guard_);
  tb_ctx.set_simple_table_schema(cf_rows.get_table_schema());
  tb_ctx.set_sess_guard(&sess_guard_);
  tb_ctx.set_audit_ctx(&audit_ctx_);
  if (tb_ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_common_without_check(credential_, tablet_id, get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.table_name_));
  } else if (OB_FAIL(decide_use_which_table_operation(tb_ctx))) {
    LOG_WARN("fail to decide use which table operation", K(ret));
  } else {
    switch (tb_ctx.get_opertion_type()) {
      case ObTableOperationType::PUT:
        // init_put has duplicate check, so we use init_insert
        if (OB_FAIL(tb_ctx.init_insert())) {
          LOG_WARN("fail to init put", K(ret));
        }
        break;
      case ObTableOperationType::INSERT_OR_UPDATE:
        if (cf_rows.count() < 1 || cf_rows.get_cf_row(0).cells_.count() < 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid cf rows", K(ret), K(cf_rows));
        } else if (FALSE_IT(tb_ctx.set_entity(&cf_rows.get_cf_row(0).cells_.at(0)))) {
        } else if (OB_FAIL(tb_ctx.init_insert_up(false))) {
          LOG_WARN("fail to init insert or update", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected operation type", K(ret), K(tb_ctx.get_opertion_type()));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret));
  } else if (OB_FAIL(tb_ctx.init_trans(trans_param_.trans_desc_, trans_param_.tx_snapshot_))) {
    LOG_WARN("fail to init trans", K(ret));
  } else {
    // here we cannot determine whether the client use put or not, and we decide use which dml type in adapter
    tb_ctx.set_init_flag(true);
  }
   return ret;
 }

int ObHbaseExecuteP::decide_use_which_table_operation(ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  if (tb_ctx.get_entity_type() != ObTableEntityType::ET_HKV_V2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected entity type", K(ret), K(tb_ctx.get_entity_type()));
  } else if (tb_ctx.has_lob_column() || OB_UNLIKELY(tb_ctx.has_secondary_index())) {
    tb_ctx.set_operation_type(ObTableOperationType::INSERT_OR_UPDATE);
  } else {
    tb_ctx.set_operation_type(ObTableOperationType::PUT);
    tb_ctx.set_client_use_put(true);
  }
  return ret;
}

 int ObHbaseExecuteP::lock_rows()
 {
  int ret = OB_SUCCESS;
  ObHTableLockHandle *lock_handle = nullptr;
  if (OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(lock_handle))) {
    LOG_WARN("fail to acquire lock handle", K(ret));
  } else if (OB_ISNULL(lock_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock handle is null", K(ret));
  } else if (OB_FAIL(ObHTableUtils::lock_htable_rows(arg_.cf_rows_,
                                                     *lock_handle,
                                                     ObHTableLockMode::SHARED))) {
    LOG_WARN("fail to lock rows", K(ret), K(arg_.cf_rows_));
  } else {
    trans_param_.lock_handle_ = lock_handle;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(lock_handle)) {
    HTABLE_LOCK_MGR->release_handle(*lock_handle);
    lock_handle = nullptr;
  }

  return ret;
 }

 int ObHbaseExecuteP::before_response(int error_code)
 {
   return ObTableRpcProcessor::before_response(error_code);
 }

 int ObHbaseExecuteP::response(const int retcode)
 {
   int ret = OB_SUCCESS;
   if (!need_retry_in_queue_ && !had_do_response()) {
     ret = ObRpcProcessor::response(retcode);
   }
   return ret;
 }