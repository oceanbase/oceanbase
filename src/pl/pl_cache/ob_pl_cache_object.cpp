/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL_CACHE
#include "ob_pl_cache_object.h"
#include "lib/oblog/ob_log_module.h"
#include "pl/pl_cache/ob_pl_cache.h"
#include "src/share/ob_truncated_string.h"

namespace oceanbase
{
namespace pl
{

OB_SERIALIZE_MEMBER(ObPlParamInfo,
                    flag_,
                    scale_,
                    type_,
                    ext_real_type_,
                    is_oracle_empty_string_,
                    col_type_,
                    pl_type_,
                    udt_id_);

void ObPLCacheObject::reset()
{
  ObILibCacheObject::reset();
  tenant_schema_version_ = OB_INVALID_VERSION;
  sys_schema_version_ = OB_INVALID_VERSION;
  params_info_.reset();
  sql_expression_factory_.destroy();
  expr_operator_factory_.destroy();
  expressions_.reset();
}

int ObPLCacheObject::set_params_info(const ParamStore &params, bool is_anonymous)
{
  int ret = OB_SUCCESS;
  int64_t N = params.count();
  ObPlParamInfo param_info;
  if (N > 0 && OB_FAIL(params_info_.reserve(N))) {
    OB_LOG(WARN, "fail to reserve params info", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    param_info.flag_ = params.at(i).get_param_flag();
    param_info.type_ = params.at(i).get_param_meta().get_type();
    param_info.col_type_ = params.at(i).get_collation_type();
    if (sql::ObSQLUtils::is_oracle_empty_string(params.at(i))) {
      param_info.is_oracle_empty_string_ = true;
    }
    if (params.at(i).get_param_meta().get_type() != params.at(i).get_type()) {
      LOG_TRACE("differ in set_params_info",
                K(params.at(i).get_param_meta().get_type()),
                K(params.at(i).get_type()),
                K(common::lbt()));
    }
    if (params.at(i).is_pl_extend()) {
      ObDataType data_type;
      param_info.pl_type_ = params.at(i).get_meta().get_extend_type();
      if (param_info.pl_type_ == pl::PL_NESTED_TABLE_TYPE ||
          param_info.pl_type_ == pl::PL_ASSOCIATIVE_ARRAY_TYPE ||
          param_info.pl_type_ == pl::PL_VARRAY_TYPE ||
          param_info.pl_type_ == pl::PL_RECORD_TYPE) {
        const pl::ObPLComposite *composite =
                reinterpret_cast<const pl::ObPLComposite*>(params.at(i).get_ext());
        if (OB_ISNULL(composite)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("nested table is null", K(ret));
        } else {
          param_info.udt_id_ = composite->get_id();
          if (OB_INVALID_ID == param_info.udt_id_) { // anonymous array
            if (OB_FAIL(sql::ObSQLUtils::get_ext_obj_data_type(params.at(i), data_type))) {
              LOG_WARN("fail to get ext obj data type", K(ret));
            } else {
              param_info.ext_real_type_ = data_type.get_obj_type();
              param_info.scale_ = data_type.get_scale();
            }
          }
        }
      } else {
        if (OB_FAIL(sql::ObSQLUtils::get_ext_obj_data_type(params.at(i), data_type))) {
          LOG_WARN("fail to get ext obj data type", K(ret));
        } else {
          param_info.ext_real_type_ = data_type.get_obj_type();
          param_info.scale_ = data_type.get_scale();
        }
      }
      LOG_DEBUG("ext params info", K(data_type), K(param_info), K(params.at(i)));
    } else {
      param_info.scale_ = params.at(i).get_scale();
      if (is_anonymous) {
        ObPLFunction *func = static_cast<ObPLFunction *>(this);
        if (func->get_variables().count() > i &&
            func->get_variables().at(i).is_pl_integer_type()) {
          param_info.pl_type_ = PL_INTEGER_TYPE;
        }
      }
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

int ObPLCacheObject::update_cache_obj_stat(sql::ObILibCacheCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObPLCacheCtx &pc_ctx = static_cast<ObPLCacheCtx&>(ctx);
  PLCacheObjStat &stat = get_stat_for_update();

  stat.pl_schema_id_ = pc_ctx.key_.key_id_;
  stat.gen_time_ = ObTimeUtility::current_time();
  stat.last_active_time_ = ObTimeUtility::current_time();
  stat.hit_count_ = 0;
  stat.schema_version_ = get_tenant_schema_version();
  MEMCPY(stat.sql_id_, pc_ctx.sql_id_, (int32_t)sizeof(pc_ctx.sql_id_));

  if (OB_SUCC(ret)) {
    ObTruncatedString trunc_name_sql(stat.name_, OB_MAX_SQL_LENGTH);
    if (OB_FAIL(ob_write_string(get_allocator(),
                                trunc_name_sql.string(),
                                stat.name_))) {
      LOG_WARN("failed to write sql", K(ret));
    } else if (OB_FAIL(ob_write_string(get_allocator(),
                                       pc_ctx.key_.sys_vars_str_,
                                       stat_.sys_vars_str_))) {
      LOG_WARN("failed to write sql", K(ret));
    } else {
      stat.sql_cs_type_ = pc_ctx.session_info_->get_local_collation_connection();
    }
  }
  if (OB_SUCC(ret)) {
    if (ObLibCacheNameSpace::NS_ANON == get_ns() ||
        ObLibCacheNameSpace::NS_CALLSTMT == get_ns()) {
      ObTruncatedString trunc_raw_sql(pc_ctx.raw_sql_, OB_MAX_SQL_LENGTH);
      if (OB_FAIL(ob_write_string(get_allocator(),
                                  trunc_raw_sql.string(),
                                  stat.raw_sql_))) {
        LOG_WARN("failed to write sql", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ObLibCacheNameSpace::NS_ANON == get_ns() && OB_INVALID_ID != pc_ctx.key_.key_id_) {
      stat.ps_stmt_id_ = pc_ctx.key_.key_id_;
    }
  }
  if (OB_SUCC(ret)) {
    // Update last_active_time_ last, because last_active_time_ is used to
    // indicate whether the cache stat has been updated.
    stat.last_active_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObPLCacheObject::update_execute_time(int64_t exec_time)
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&(stat_.elapsed_time_), exec_time);
  ATOMIC_INC(&(stat_.execute_times_));
  int64_t slowest_usec = ATOMIC_LOAD(&stat_.slowest_exec_usec_);
  if (slowest_usec < exec_time) {
    ATOMIC_STORE(&(stat_.slowest_exec_usec_), exec_time);
    ATOMIC_STORE(&(stat_.slowest_exec_time_), ObClockGenerator::getClock());
  }
  return ret;
}

}
}