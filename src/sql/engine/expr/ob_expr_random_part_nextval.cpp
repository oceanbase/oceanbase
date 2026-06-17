/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_random_part_nextval.h"

#include "observer/table_load/ob_table_load_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/ddl/ob_partition_random_distribution_helper.h"
#include "storage/direct_load/ob_direct_load_struct.h"
#include "rootserver/ob_random_partition_helper.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using share::schema::ObTableSchema;
using share::schema::ObSchemaGetterGuard;

namespace sql
{
OB_SERIALIZE_MEMBER_INHERIT(ObExprRandomPartNextval, ObExprAutoincNextval);
ObExprRandomPartNextval::ObExprRandomPartNextval(ObIAllocator &alloc)
    : ObExprAutoincNextval(alloc,
                           T_FUN_SYS_RANDOM_PART_NEXTVAL,
                           N_RANDOM_PART_NEXTVAL,
                           ZERO_OR_ONE,
                           NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  /* NextVal是一个人肉生成的FuncOp */
  disable_operand_auto_cast();
}


ObExprRandomPartNextval::ObExprRandomPartNextval(
    common::ObIAllocator &alloc,
    ObExprOperatorType type,
    const char *name,
    int32_t param_num,
    ObValidForGeneratedColFlag valid_for_generated_col,
    int32_t dimension,
    bool is_internal_for_mysql/* = false */,
    bool is_internal_for_oracle/* = false */)
  : ObExprAutoincNextval(alloc,
                         type,
                         name,
                         param_num,
                         valid_for_generated_col,
                         dimension,
                         is_internal_for_mysql,
                         is_internal_for_oracle)
{
  disable_operand_auto_cast();
}

ObExprRandomPartNextval::~ObExprRandomPartNextval()
{
}

int ObExprRandomPartNextval::get_input_value(const uint64_t sql_mode,
                                             const ObObjTypeClass *input_tc,
                                             ObDatum *input_value,
                                             share::AutoincParam &autoinc_param,
                                             bool &is_to_generate,
                                             uint64_t &casted_value)
{
  int ret = OB_SUCCESS;
  if (NULL == input_value || input_value->is_null() || input_value->is_nop()) {
    is_to_generate = true;
  } else {
    bool is_zero = false;
    if (OB_ISNULL(input_tc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid first arg type", K(ret));
    } else if (OB_FAIL(ObExprAutoincNextval::get_uint_value_with_tc(*input_tc, input_value, is_zero, casted_value))) {
      LOG_WARN("get casted unsigned int value failed", K(ret));
    } else {
      is_to_generate = is_zero && !(SMO_NO_AUTO_VALUE_ON_ZERO & sql_mode);
    }
  }
  return ret;
}

int ObExprRandomPartNextval::generate_autoinc_value(const ObRandomPartSeqUsedOutAction seq_used_out_action,
                                                    ObSchemaGetterGuard &schema_guard,
                                                    AutoincParam &param,
                                                    uint64_t &new_val)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = param.tenant_id_;
  const uint64_t table_id = param.autoinc_table_id_;
  ObIArray<ObTabletID> &active_tablet_ids = param.active_tablet_ids_;
  ObTabletID &target_tablet_id = param.cur_tablet_id_;
  if (OB_SUCC(ret)) {
    uint64_t value = 0;
    while (OB_SUCC(ret) && 0 == value) {
      if (OB_SUCC(ret) && active_tablet_ids.empty()) {
        const ObTableSchema *table_schema = nullptr;
        const ObArray<ObTabletID> inactive_tablet_ids;
        ObArray<int64_t> unused_index;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
          LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table schema is not found", K(ret), K(table_id));
        } else if (OB_FAIL(rootserver::ObRandomPartitionHelper::get_active_tablets(*table_schema, inactive_tablet_ids, unused_index, active_tablet_ids))) {
          LOG_WARN("failed to get active tablets", K(ret), K(table_id));
        }
      }

      if (OB_SUCC(ret) && !active_tablet_ids.empty() && !target_tablet_id.is_valid()) {
        share::ObLocationService *location_service = nullptr;
        if (OB_ISNULL(location_service = GCTX.location_service_)) {
          ret = OB_ERR_SYS;
          LOG_WARN("root service or location_cache is null", K(ret), KP(location_service));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < active_tablet_ids.count() && !target_tablet_id.is_valid(); i++) {
          const ObTabletID &tablet_id = active_tablet_ids.at(i);
          bool is_cache_hit = false;
          ObLSID ls_id;
          ObAddr leader_addr;
          if (OB_FAIL(location_service->get(tenant_id, tablet_id, 0/*expire_renew_time*/, is_cache_hit, ls_id))) {
            LOG_WARN("fail to get log stream id", K(ret), K(tablet_id));
          } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id, tenant_id, ls_id, false/*force_renew*/, leader_addr))) {
            LOG_WARN("get leader failed", K(ret), K(ls_id));
          } else if (GCTX.self_addr() == leader_addr) {
            target_tablet_id = tablet_id;
          }
          if (OB_FAIL(ret)) {
            if (OB_LS_LOCATION_NOT_EXIST == ret || OB_GET_LOCATION_TIME_OUT == ret || OB_LS_LOCATION_LEADER_NOT_EXIST == ret) {
              // overwrite ret
              if (OB_FAIL(THIS_WORKER.check_status())) {
                LOG_WARN("failed to check status", K(ret));
              } else {
                (void)location_service->renew_tablet_location(tenant_id, tablet_id, ret, false/*is_nonblock*/);
                ob_usleep<common::ObWaitEventIds::STORAGE_AUTOINC_FETCH_RETRY_SLEEP>(100 * 1000L); // 100ms
                i -= 1; // retry this tablet
              }
            }
          }
        }
        if (OB_SUCC(ret) && !target_tablet_id.is_valid()) {
          target_tablet_id = active_tablet_ids.at(ObTimeUtility::fast_current_time() % active_tablet_ids.count());
        }
      }

      if (OB_SUCC(ret)) {
        if (!target_tablet_id.is_valid()) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("no target tablet", K(ret), K(table_id), K(seq_used_out_action));
        } else {
          ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
          if (OB_FAIL(auto_inc.get_autoinc_seq(tenant_id, target_tablet_id, value))) {
            LOG_WARN("get_autoinc_seq fail", K(ret), K(tenant_id), K(target_tablet_id), K(seq_used_out_action));
          }
        }
        if (OB_SIZE_OVERFLOW == ret) {
          ObSEArray<ObTabletID, 1> inactive_tablet;
          ObSEArray<ObTabletID, 4> new_active_tablet_ids;
          ObSEArray<ObTabletID, 4> latest_active_tablet_ids;
          if (target_tablet_id.is_valid() && OB_FAIL(inactive_tablet.push_back(target_tablet_id))) { // overwrite ret
            LOG_WARN("failed to push back", K(ret), K(table_id));
          } else if (OB_FAIL(get_difference(active_tablet_ids, inactive_tablet, new_active_tablet_ids))) {
            LOG_WARN("failed to get difference", K(ret), K(table_id));
          } else if (ObRandomPartSeqUsedOutAction::TRY_ADD_PART_AND_RETRY_IF_NO_ACTIVE == seq_used_out_action) {
            (void)send_add_random_partition_rpc(tenant_id, table_id, inactive_tablet, 0/*specified_value*/, latest_active_tablet_ids);
            if (OB_UNLIKELY(new_active_tablet_ids.empty())) {
              ret = OB_SCHEMA_EAGAIN;
              LOG_WARN("no more active tablets, retry", K(ret), K(table_id));
            }
          } else if (ObRandomPartSeqUsedOutAction::ERROR_IF_NO_ACTIVE == seq_used_out_action) {
            if (new_active_tablet_ids.empty()) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("no more active tablets", K(ret), K(table_id));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "full direct load rows outside existing random partitions");
            }
          } else if (ObRandomPartSeqUsedOutAction::ADD_PART == seq_used_out_action) {
            // FIXME: retry
            if (OB_FAIL(send_add_random_partition_rpc(tenant_id, table_id, inactive_tablet, 0/*specified_value*/, latest_active_tablet_ids))) {
              LOG_WARN("failed to send add random partition rpc for full direct", K(ret));
            } else if (new_active_tablet_ids.empty()) {
              if (OB_FAIL(append(new_active_tablet_ids, latest_active_tablet_ids))) {
                LOG_WARN("failed to append", K(ret), K(latest_active_tablet_ids.count()));
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected action", K(ret), K(seq_used_out_action));
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(active_tablet_ids.assign(new_active_tablet_ids))) {
            LOG_WARN("failed to assign", K(ret), K(table_id));
          } else {
            target_tablet_id.reset();
            value = 0;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      new_val = value;
    }
  }
  return ret;
}

int ObExprRandomPartNextval::send_add_random_partition_rpc(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<ObTabletID> &inactive_tablet_ids,
    const uint64_t specified_value,
    ObIArray<ObTabletID> &active_tablet_ids)
{
  int ret = OB_SUCCESS;
  ObRandomPartitionArgBuilder random_part_helper;
  obrpc::ObAlterRandomPartitionRes res;
  ObArray<ObTabletID> new_active_tablets;
  SMART_VAR(obrpc::ObAlterTableArg, arg) {
  if (OB_FAIL(random_part_helper.build_arg(tenant_id, table_id, inactive_tablet_ids, specified_value, 0/*avaiable_ls_cnt*/, arg))) {
    LOG_WARN("fail to build arg", K(ret), K(table_id), K(inactive_tablet_ids));
  } else if (!arg.is_random_partition()) {
    //do nothing
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(GCTX.rs_rpc_proxy_));
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->timeout(GCONF._ob_ddl_timeout).alter_random_distribution_partition(arg, res))) {
    LOG_WARN("alter table failed", K(ret), K(arg));
  } else if (OB_FAIL(get_difference(res.active_tablets_, active_tablet_ids, new_active_tablets))) {
    LOG_WARN("failed to get difference", K(ret));
  } else if (OB_FAIL(append(active_tablet_ids, new_active_tablets))) {
    LOG_WARN("failed to append", K(ret));
  }
  }
  return ret;
}

int ObExprRandomPartNextval::cg_expr(
        ObExprCGCtx &op_cg_ctx,
        const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(0 == rt_expr.arg_cnt_ || 1 == rt_expr.arg_cnt_);
  if (OB_FAIL(ObAutoincNextvalInfo::init_autoinc_nextval_info(
          op_cg_ctx.allocator_, raw_expr, rt_expr, type_))) {
    LOG_WARN("fail to init_autoinc_nextval_info", K(ret), K(type_));
  } else {
    rt_expr.eval_func_ = eval_nextval;
  }
  return ret;
}

int ObExprRandomPartNextval::eval_nextval(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *input_value = NULL;
  ObPhysicalPlanCtx *plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
  ObSQLSessionInfo *my_session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session) || OB_ISNULL(ctx.exec_ctx_.get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no phy plan context", K(ret), K(plan_ctx), K(my_session), K(ctx.exec_ctx_.get_sql_ctx()));
  } else if (OB_FAIL(expr.eval_param_value(ctx, input_value))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else {
    const ObObjTypeClass tc = ob_obj_type_class(expr.datum_meta_.type_);
    const uint64_t sql_mode = my_session->get_sql_mode();
    const bool is_ddl_idempotent_autoinc = ctx.exec_ctx_.is_ddl_idempotent_autoinc();
    ObObjTypeClass input_tc = ObNullTC;
    const ObObjTypeClass *input_tc_ptr = nullptr;
    ObSchemaGetterGuard *schema_guard = ctx.exec_ctx_.get_sql_ctx()->schema_guard_;
    const uint64_t autoinc_table_id = static_cast<ObAutoincNextvalInfo *>(expr.extra_info_)->autoinc_table_id_;
    const uint64_t autoinc_col_id = static_cast<ObAutoincNextvalInfo *>(expr.extra_info_)->autoinc_col_id_;
    const bool is_full_direct_load = plan_ctx->get_phy_plan() != nullptr
      && plan_ctx->get_phy_plan()->get_append_table_id() == autoinc_table_id
      && plan_ctx->get_phy_plan()->get_enable_append()
      && !plan_ctx->get_phy_plan()->get_enable_inc_direct_load()
      && !plan_ctx->get_phy_plan()->get_enable_replace();
    ObIArray<AutoincParam> &autoinc_params = plan_ctx->get_autoinc_params();
    AutoincParam *autoinc_param = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      if (autoinc_table_id == autoinc_params.at(i).autoinc_table_id_ &&
          autoinc_col_id == autoinc_params.at(i).autoinc_col_id_) {
        autoinc_param = &(autoinc_params.at(i));
        break;
      }
    }
    // this column with column_index is auto-increment column
    if (OB_ISNULL(autoinc_param) || OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid autoinc param or schema guard", K(ret), K(autoinc_table_id), K(autoinc_col_id), K(autoinc_params), K(schema_guard));
    }

    if (OB_SUCC(ret) && expr.arg_cnt_ == 1) {
      if (OB_ISNULL(expr.args_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr.args_ is null", K(ret), K(autoinc_table_id), K(autoinc_col_id));
      } else {
        input_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
        input_tc_ptr = &input_tc;
      }
    }

    bool is_to_generate = false;
    uint64_t new_val = 0;
    const ObRandomPartSeqUsedOutAction seq_used_out_action = is_full_direct_load ? ObRandomPartSeqUsedOutAction::ERROR_IF_NO_ACTIVE : ObRandomPartSeqUsedOutAction::TRY_ADD_PART_AND_RETRY_IF_NO_ACTIVE;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(eval_nextval(sql_mode, is_ddl_idempotent_autoinc, seq_used_out_action, *schema_guard, *autoinc_param, input_tc_ptr, input_value, tc, expr_datum, is_to_generate, new_val))) {
      LOG_WARN("failed to eval next val", K(ret), K(autoinc_table_id), K(autoinc_col_id), K(sql_mode), K(is_ddl_idempotent_autoinc),
          KPC(autoinc_param), KPC(input_value));
    } else {
      if (is_to_generate) {
        plan_ctx->set_autoinc_id_tmp(new_val);
      }
      plan_ctx->set_autoinc_col_value(new_val);
    }
  }
  return ret;
}

// input_value and expr_datum can be at same memory
int ObExprRandomPartNextval::eval_nextval(
    const uint64_t sql_mode,
    const bool is_ddl_idempotent_autoinc,
    const ObRandomPartSeqUsedOutAction seq_used_out_action,
    ObSchemaGetterGuard &schema_guard,
    AutoincParam &autoinc_param,
    const ObObjTypeClass *input_tc,
    ObDatum *input_value,
    const ObObjTypeClass &tc,
    ObDatum &expr_datum,
    bool &is_to_generate,
    uint64_t &new_val)
{
  int ret = OB_SUCCESS;
  new_val = 0;
  if (OB_FAIL(get_input_value(sql_mode, input_tc, input_value, autoinc_param, is_to_generate, new_val))) {
    LOG_WARN("check generation failed", K(ret));
  } else if (is_to_generate) {
    if (OB_UNLIKELY(is_ddl_idempotent_autoinc)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ddl idempotent autoinc not suppoorted", K(ret), K(autoinc_param));
    } else if (OB_FAIL(generate_autoinc_value(seq_used_out_action, schema_guard, autoinc_param, new_val))) {
      LOG_WARN("generate autoinc value failed", K(ret));
    }
  } else {
    const uint64_t tenant_id = autoinc_param.tenant_id_;
    const uint64_t table_id = autoinc_param.autoinc_table_id_;
    ObArray<RandomPartSyncTabletCtx> &sync_ctxs = autoinc_param.random_part_sync_ctxs_;
    ObIArray<ObTabletID> &active_tablet_ids = autoinc_param.active_tablet_ids_;
    if (sync_ctxs.empty()) {
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table schema is not found", K(ret), K(table_id));
      } else {
        ObPartition **data_partitions = table_schema->get_part_array();
        if (OB_ISNULL(data_partitions)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data_partitions is null", KR(ret), KPC(table_schema));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_partition_num(); i++) {
            bool contain = false;
            if (OB_ISNULL(data_partitions[i])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to get tablet_ids", KR(ret), KPC(table_schema));
            } else {
              const ObRowkey &high_bound_val = data_partitions[i]->get_high_bound_val();
              RandomPartSyncTabletCtx sync_ctx;
              sync_ctx.tablet_id_ = data_partitions[i]->get_tablet_id();
              if (OB_UNLIKELY(1 != high_bound_val.get_obj_cnt())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("row key of random partitioned table must be single object", KR(ret), KPC(table_schema));
              } else if (OB_FAIL(high_bound_val.get_obj_ptr()[0].get_int(sync_ctx.high_bound_val_))) {
                LOG_WARN("fail to get int from high bound val", KR(ret), K(high_bound_val));
              } else if (OB_FAIL(sync_ctxs.push_back(sync_ctx))) {
                LOG_WARN("failed to push back", K(ret));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret) && active_tablet_ids.empty()) {
        ObArray<int64_t> unused_index;
        ObArray<ObTabletID> inactive_tablet_ids;
        if (OB_FAIL(rootserver::ObRandomPartitionHelper::get_active_tablets(*table_schema, inactive_tablet_ids, unused_index, active_tablet_ids))) {
          LOG_WARN("failed too get active tablets", K(ret), K(table_id));
        } else if (OB_UNLIKELY(active_tablet_ids.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("no active tablet", K(ret), K(table_id));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(sync_ctxs.empty() || active_tablet_ids.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected autoinc param", K(ret), K(sync_ctxs), K(active_tablet_ids));
    } else {
      ObArray<RandomPartSyncTabletCtx>::iterator target = sync_ctxs.end();
      if (OB_UNLIKELY(new_val <= INT64_MAX)) {
        RandomPartSyncTabletCtx new_val_ctx;
        RandomPartSyncTabletCtxCmp cmp;
        new_val_ctx.high_bound_val_ = static_cast<int64_t>(new_val);
        target = std::upper_bound(sync_ctxs.begin(), sync_ctxs.end(), new_val_ctx, cmp);
      }
      if (target == sync_ctxs.end()) {
        ObArray<ObTabletID> latest_active_tablet_ids;
        if (ObRandomPartSeqUsedOutAction::TRY_ADD_PART_AND_RETRY_IF_NO_ACTIVE == seq_used_out_action) {
          (void)send_add_random_partition_rpc(tenant_id, table_id, active_tablet_ids, new_val, latest_active_tablet_ids);
          ret = OB_SCHEMA_EAGAIN;
          LOG_WARN("random partition extended, retry", K(ret), K(new_val));
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "full direct load rows outside existing random partitions");
        }
      } else if (new_val > target->value_to_sync_) {
        target->value_to_sync_ = new_val;
        target->sync_flag_ = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_to_generate) {
      // keep the input datum
      if (input_value != &expr_datum) {
        expr_datum.set_datum(*input_value);
      }
    } else {
      switch (tc) {
        case ObIntTC:
        case ObUIntTC: {
          expr_datum.set_uint(new_val);
          break;
        }
        case ObFloatTC: {
          expr_datum.set_float(static_cast<float>(new_val));
          break;
        }
        case ObDoubleTC: {
          expr_datum.set_double(static_cast<double>(new_val));
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("only int/float/double types support auto increment",
                   K(ret), K(tc));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (autoinc_param.autoinc_desired_count_ > 0) {
      --autoinc_param.autoinc_desired_count_;
    }
  }
  return ret;
}

ObRandomPartSeqUsedOutAction ObExprRandomPartNextval::get_table_load_action(const observer::ObTableLoadParam &param)
{
  ObRandomPartSeqUsedOutAction action = storage::ObDirectLoadMethod::is_incremental(param.method_)
    ? ObRandomPartSeqUsedOutAction::TRY_ADD_PART_AND_RETRY_IF_NO_ACTIVE : ObRandomPartSeqUsedOutAction::ERROR_IF_NO_ACTIVE;
  return action;
}

bool RandomPartSyncTabletCtxCmp::operator()(const RandomPartSyncTabletCtx &lhs, const RandomPartSyncTabletCtx &rhs)
{
  return lhs.high_bound_val_ < rhs.high_bound_val_;
}

}//end namespace sql
}//end namespace oceanbase
