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

#define USING_LOG_PREFIX SQL_PC
#include "sql/plan_cache/ob_cache_object.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_truncated_string.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "pl/ob_pl.h"

#include <cstring>

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

void ObParamInfo::reset()
{
  flag_.reset();
  scale_ = 0;
  type_ = common::ObNullType;
  ext_real_type_ = common::ObNullType;
  is_oracle_empty_string_ = false;
  col_type_ = common::CS_TYPE_INVALID;
}

OB_SERIALIZE_MEMBER(ObParamInfo,
                    flag_,
                    scale_,
                    type_,
                    ext_real_type_,
                    is_oracle_empty_string_,
                    col_type_);

ObPlanCacheObject::ObPlanCacheObject(ObLibCacheNameSpace ns, lib::MemoryContext &mem_context)
  : ObILibCacheObject(ns, mem_context),
    tenant_schema_version_(OB_INVALID_VERSION),
    sys_schema_version_(OB_INVALID_VERSION),
    dependency_tables_(allocator_),
    outline_state_(),
    pre_calc_frames_(),
    params_info_( (ObWrapperAllocator(allocator_)) ),
    is_contain_virtual_table_(false),
    is_contain_inner_table_(false),
    fetch_cur_time_(false),
    is_ignore_stmt_(false),
    stmt_type_(stmt::T_NONE),
    need_param_(true)
{
}

int ObPlanCacheObject::set_params_info(const ParamStore &params)
{
  int ret = OB_SUCCESS;
  int64_t N = params.count();
  ObParamInfo param_info;
  if (N > 0 && OB_FAIL(params_info_.reserve(N))) {
    OB_LOG(WARN, "fail to reserve params info", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    param_info.flag_ = params.at(i).get_param_flag();
    param_info.type_ = params.at(i).get_param_meta().get_type();
    param_info.col_type_ = params.at(i).get_collation_type();
    if (ObSQLUtils::is_oracle_empty_string(params.at(i))) {
      param_info.is_oracle_empty_string_ = true;
    }
    if (params.at(i).get_param_meta().get_type() != params.at(i).get_type()) {
      LOG_TRACE("differ in set_params_info",
                K(params.at(i).get_param_meta().get_type()),
                K(params.at(i).get_type()),
                K(common::lbt()));
    }
    if (params.at(i).is_ext()) {
      ObDataType data_type;
      if (OB_FAIL(ObSQLUtils::get_ext_obj_data_type(params.at(i), data_type))) {
        LOG_WARN("fail to get ext obj data type", K(ret));
      } else {
        param_info.ext_real_type_ = data_type.get_obj_type();
        param_info.scale_ = data_type.get_scale();
      }
      LOG_DEBUG("ext params info", K(data_type), K(param_info), K(params.at(i)));
    } else {
      param_info.scale_ = params.at(i).get_scale();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(params_info_.push_back(param_info))) {
        LOG_WARN("failed to push back param info", K(ret));
      }
    }
    param_info.reset();
  }
  return ret;
}

int ObPlanCacheObject::get_base_table_version(const uint64_t table_id, int64_t &table_version) const
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(dependency_tables_, i) {
    const ObSchemaObjVersion &obj_version = dependency_tables_.at(i);
    if (obj_version.object_id_ == table_id) {
      //所有的dependency table的table_id应该是唯一的，其实判断table id就够了，这里再判断下table type，冗余性的检查下
      if (obj_version.is_base_table() || ObDependencyTableType::DEPENDENCY_VIEW == obj_version.get_type()) {
        table_version = obj_version.version_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid dependency table type", K(obj_version));
      }
      break;
    }
  }
  return ret;
}

void ObPlanCacheObject::reset()
{
  ObILibCacheObject::reset();
  tenant_schema_version_ = OB_INVALID_VERSION;
  sys_schema_version_ = OB_INVALID_VERSION;
  dependency_tables_.reset();
  outline_state_.reset();
  pre_calc_frames_.reset();
  params_info_.reset();
  is_contain_virtual_table_ = false;
  is_contain_inner_table_ = false;
  fetch_cur_time_ = false;
  is_ignore_stmt_ = false;
  stmt_type_ = stmt::T_NONE;
  need_param_ = true;
}

int ObPlanCacheObject::check_pre_calc_cons(const bool is_ignore_stmt,
                                        bool &is_match,
                                        ObPreCalcExprConstraint &pre_calc_con,
                                        ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  is_match = true;
  ObPhysicalPlanCtx *phy_plan_ctx = exec_ctx.get_physical_plan_ctx();
  ObPreCalcExprFrameInfo &pre_calc_frame = pre_calc_con.pre_calc_expr_info_;
  const PreCalcExprExpectResult expect_res = pre_calc_con.expect_result_;
  ObSEArray<ObDatumObjParam, 4> datum_params;
  if (OB_ISNULL(phy_plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid physical plan ctx", K(ret), K(phy_plan_ctx));
  } else if (OB_FALSE_IT(phy_plan_ctx->set_ignore_stmt(is_ignore_stmt))) {
  } else if (PRE_CALC_ERROR == expect_res) {
    if (OB_FAIL(pre_calc_frame.eval_expect_err(exec_ctx, is_match))) {
      LOG_WARN("failed to eval pre calc expr frame info expect error", K(ret));
    }
  } else if (OB_FAIL(pre_calc_frame.eval(exec_ctx, datum_params))) {
    LOG_TRACE("failed to eval pre calc expr frame info", K(ret));
    is_match = false;
    ret = OB_SUCCESS;
  } else {
    ObObjParam obj_param;
    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < datum_params.count(); ++i) {
      if (OB_FAIL(datum_params.at(i).to_objparam(obj_param, &exec_ctx.get_allocator()))) {
        LOG_WARN("failed to obj param", K(ret));
      } else if (OB_FAIL(pre_calc_con.check_is_match(obj_param, is_match))) {
        LOG_WARN("failed to check is match", K(ret));
      } // else end
    } // for end
  }
  return ret;
}

int ObPlanCacheObject::pre_calculation(const bool is_ignore_stmt,
                                   ObPreCalcExprFrameInfo &pre_calc_frame,
                                   ObExecContext &exec_ctx,
                                   const uint64_t calc_types) /* default PRE_CALC_DEFAULT */
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = exec_ctx.get_physical_plan_ctx();
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  ObSEArray<ObDatumObjParam, 4> datum_params;
  // datum_store will be used out of the block, use exec_ctx's allocator
  DatumParamStore datum_store(ObWrapperAllocator(exec_ctx.get_allocator()));
  // TODO [zongmei.zzm]
  // create table t (a int primary key) partition by hash(a) partitions 2;
  // select * from t where a = '1' + 1
  // 新引擎类型推导会在a上加隐式cast: select * from t where cast (a as double) = ?
  // 结果就该sql的query range在新引擎下无法抽取，而老引擎是可以抽取query range的
  if (OB_ISNULL(phy_plan_ctx) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session or phy plan ctx", K(ret), K(phy_plan_ctx), K(session));
  } else if (OB_FALSE_IT(phy_plan_ctx->set_ignore_stmt(is_ignore_stmt))) {
  } else if (pre_calc_frame.pre_calc_rt_exprs_.count() <= 0) {
    /* do nothing */
  } else if (OB_FAIL(pre_calc_frame.eval(exec_ctx, datum_params))) {
    LOG_WARN("failed to eval pre calc expr frame info", K(ret),
             K(calc_types));
  } else if (OB_FAIL(datum_store.assign(datum_params))) {
    LOG_WARN("failed to push back datum param", K(ret), K(calc_types));
  } else if (PRE_CALC_DEFAULT == calc_types &&
             OB_FAIL(phy_plan_ctx->extend_datum_param_store(datum_store))) {
    LOG_WARN("failed to extend param frame", K(ret), K(calc_types));
  } else { /* do nothing */
  }

  return ret;
}

int ObPlanCacheObject::construct_array_params(const ObSqlExpression &expr,
                                          ObIAllocator &allocator,
                                          ObExprCtx &expr_ctx,
                                          ObNewRow &row,
                                          ObObjParam &result)
{
  int ret = OB_SUCCESS;
  int64_t bind_array_count = 0;
  void *ba_buf = NULL;
  void *data_buf = NULL;
  int64_t ba_buf_size = static_cast<int64_t>(sizeof(ObSqlArrayObj));
  int64_t data_buf_size = 0;
  ObSqlArrayObj *binding_array = NULL;
  ObObj tmp_obj;
  if (OB_ISNULL(expr_ctx.phy_plan_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", K(ret));
  } else {
    const ParamStore &param_store = expr_ctx.phy_plan_ctx_->get_param_store();
    int64_t param_addr = 0;
    if (OB_UNLIKELY(expr.get_array_param_index() < 0
                    || expr.get_array_param_index() >= param_store.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array param index is invalid", K(expr.get_array_param_index()), K(param_store.count()));
    } else if (OB_UNLIKELY(!param_store.at(expr.get_array_param_index()).is_ext_sql_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param store obj is invalid", K(ret), K(param_store.at(expr.get_array_param_index())));
    } else if (OB_FAIL(param_store.at(expr.get_array_param_index()).get_ext(param_addr))) {
      LOG_WARN("get param addr failed", K(ret), K(param_store.at(expr.get_array_param_index())));
    } else {
      const ObSqlArrayObj *array_param = reinterpret_cast<const ObSqlArrayObj*>(param_addr);
      bind_array_count = array_param->count_;
      data_buf_size = static_cast<int64_t>(sizeof(ObObj) * bind_array_count);
    }
  }
  if (OB_FAIL(ret)) {
    //do thing
  } else if (OB_ISNULL(ba_buf = allocator.alloc(ba_buf_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate nested table failed", K(ret), K(ba_buf_size));
  } else if (OB_ISNULL(data_buf = allocator.alloc(data_buf_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate data buffer failed", K(ret), K(data_buf_size));
  } else {
    binding_array = new(ba_buf) ObSqlArrayObj();
    binding_array->data_ = new(data_buf) ObObjParam[bind_array_count];
    binding_array->count_ = bind_array_count;
  }
  expr_ctx.cur_array_index_ = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < bind_array_count; ++i) {
    tmp_obj.reset();
    if (OB_FAIL(expr.calc(expr_ctx, row, tmp_obj))) {
      LOG_WARN("calc array expr failed", K(ret), K(expr), K(row));
    } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, binding_array->data_[expr_ctx.cur_array_index_]))) {
      LOG_WARN("write obj failed", K(ret), K(tmp_obj));
    }
    ++expr_ctx.cur_array_index_;
  }
  if (OB_SUCC(ret)) {
    result.set_extend(reinterpret_cast<int64_t>(binding_array), T_EXT_SQL_ARRAY);
    result.set_param_meta();
  }
  return ret;
}

bool ObPlanCacheObject::has_sequence() const
{
  int ret = OB_SUCCESS;
  bool has_sequence = false;
  ARRAY_FOREACH(dependency_tables_, i) {
    const ObSchemaObjVersion &obj_version = dependency_tables_.at(i);
    if (DEPENDENCY_SEQUENCE == obj_version.object_type_) {
      has_sequence = true;
      break;
    }
  }
  return has_sequence;
}
int ObPlanCacheObject::get_audit_objects(
    common::ObIArray<share::schema::ObObjectStruct> &object_ids) const
{
  int ret = OB_SUCCESS;
  ObObjectStruct tmp_object_struct;
  ARRAY_FOREACH(dependency_tables_, i) {
    const ObSchemaObjVersion &obj_version = dependency_tables_.at(i);
    switch (obj_version.object_type_) {
      case DEPENDENCY_VIEW:
      case DEPENDENCY_TABLE: {
        tmp_object_struct.type_ = ObObjectType::TABLE;
        break;
      }
      case DEPENDENCY_PACKAGE:
      case DEPENDENCY_PACKAGE_BODY: {
        tmp_object_struct.type_ = ObObjectType::PACKAGE;
        break;
      }
      case DEPENDENCY_SEQUENCE: {
        tmp_object_struct.type_ = ObObjectType::SEQUENCE;
        break;
      }
      default: {
        //do nothing
      }
    }
    tmp_object_struct.id_ = static_cast<uint64_t>(obj_version.object_id_);
    if (OB_FAIL(object_ids.push_back(tmp_object_struct))) {
      LOG_WARN("failed to push_back table_id", K(ret));
      break;
    }
  }
  return ret;
}

int ObPlanCacheObject::check_need_add_cache_obj_stat(ObILibCacheCtx &ctx, bool &need_real_add)
{
  int ret = OB_SUCCESS;
  ObPlanCacheCtx &pc_ctx = static_cast<ObPlanCacheCtx&>(ctx);
  need_real_add = pc_ctx.need_add_obj_stat_;
  return ret;
}

int ObPlanCacheObject::type_to_name(const ObLibCacheNameSpace ns,
                                    common::ObIAllocator &allocator,
                                    common::ObString &type_name)
{
  int ret = OB_SUCCESS;
  const char* type_strs[] = {"NS_INVALID", "SQL_PLAN", "PROCEDURE", "FUNCTION", "ANONYMOUS", "TRIGGER", "PACKAGE", "NS_MAX"};
  char *buf = NULL;
  if (ns <= NS_INVALID || ns >= NS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache obj type", K(ret), K(ns));
  } else {
    int32_t str_len = (int32_t)std::strlen(type_strs[static_cast<int64_t>(ns)]);
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(str_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(str_len));
    } else {
      MEMCPY(buf, type_strs[static_cast<int64_t>(ns)], str_len);
      type_name.assign(buf, str_len);
    }
  }
  return ret;
}

void ObPlanCacheObject::dump_deleted_log_info(const bool is_debug_log /* = true */) const
{
  ObString raw_sql;
  if (is_sql_crsr()) {
    const ObPhysicalPlan *plan = dynamic_cast<const ObPhysicalPlan *>(this);
    if (OB_ISNULL(plan)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "the plan is null", K(plan), K(this));
    } else {
      raw_sql = ObTruncatedString(plan->stat_.raw_sql_, OB_MAX_SQL_LENGTH).string();
    }
  } else if (is_anon()) {
    const pl::ObPLFunction *pl_func = dynamic_cast<const pl::ObPLFunction *>(this);
    if (OB_ISNULL(pl_func)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "the pl_func is null", K(this));
    } else {
      raw_sql = ObTruncatedString(pl_func->get_stat().raw_sql_, OB_MAX_SQL_LENGTH).string();
    }
  } else {
    // do nothing
  }
  if (is_debug_log) {
    SQL_PC_LOG(DEBUG, "Dumping Cache Deleted Info",
               K(object_id_),
               K(tenant_id_),
               K(added_to_lc_),
               K(ns_),
               K(get_ref_count()),
               K(log_del_time_),
               K(raw_sql),
               K(this));
  } else {
    SQL_PC_LOG(INFO, "Dumping Cache Deleted Info",
               K(object_id_),
               K(tenant_id_),
               K(added_to_lc_),
               K(ns_),
               K(get_ref_count()),
               K(log_del_time_),
               K(raw_sql),
               K(this));
  }
}

}  // namespace sql
}  // namespace oceanbase
