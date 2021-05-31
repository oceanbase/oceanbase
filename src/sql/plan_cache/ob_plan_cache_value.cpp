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
#include "ob_plan_cache_value.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/parser/parse_malloc.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_duplicate_scope_define.h"
using oceanbase::share::ObIPartitionLocationCache;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

int PCVSchemaObj::init(const ObTableSchema* schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema) || OB_ISNULL(inner_alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null argument", K(ret), K(schema), K(inner_alloc_));
  } else {
    tenant_id_ = schema->get_tenant_id();
    database_id_ = schema->get_database_id();
    schema_id_ = schema->get_table_id();
    schema_version_ = schema->get_schema_version();
    schema_type_ = TABLE_SCHEMA;
    table_type_ = schema->get_table_type();
    is_tmp_table_ = schema->is_tmp_table();
    // copy table name
    char* buf = nullptr;
    const ObString& tname = schema->get_table_name_str();
    if (nullptr == (buf = static_cast<char*>(inner_alloc_->alloc(tname.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(tname.length()));
    } else {
      MEMCPY(buf, tname.ptr(), tname.length());
      table_name_.assign_ptr(buf, tname.length());
    }
  }
  return ret;
}

bool PCVSchemaObj::operator==(const PCVSchemaObj& other) const
{
  bool ret = true;
  if (schema_type_ != other.schema_type_) {
    ret = false;
  } else if (TABLE_SCHEMA == other.schema_type_) {
    ret = tenant_id_ == other.tenant_id_ && database_id_ == other.database_id_ && schema_id_ == other.schema_id_ &&
          schema_version_ == other.schema_version_ && table_type_ == other.table_type_;
  } else {
    ret = schema_id_ == other.schema_id_ && schema_version_ == other.schema_version_;
  }
  return ret;
}

int PCVSchemaObj::init_without_copy_name(const ObSimpleTableSchemaV2* schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null argument", K(ret), K(schema));
  } else {
    tenant_id_ = schema->get_tenant_id();
    database_id_ = schema->get_database_id();
    schema_id_ = schema->get_table_id();
    schema_version_ = schema->get_schema_version();
    schema_type_ = TABLE_SCHEMA;
    table_type_ = schema->get_table_type();
    is_tmp_table_ = schema->is_tmp_table();

    table_name_ = schema->get_table_name_str();
  }
  return ret;
}

int PCVSchemaObj::init_with_version_obj(const ObSchemaObjVersion& schema_obj_version)
{
  int ret = OB_SUCCESS;
  schema_type_ = schema_obj_version.get_schema_type();
  schema_id_ = schema_obj_version.object_id_;
  schema_version_ = schema_obj_version.version_;
  return ret;
}

void PCVSchemaObj::reset()
{
  tenant_id_ = common::OB_INVALID_ID;
  database_id_ = common::OB_INVALID_ID;
  schema_id_ = common::OB_INVALID_ID;
  schema_type_ = OB_MAX_SCHEMA;
  schema_version_ = 0;
  table_type_ = MAX_TABLE_TYPE;
  is_tmp_table_ = false;
  if (inner_alloc_ != nullptr && table_name_.ptr() != nullptr) {
    inner_alloc_->free(table_name_.ptr());
    table_name_.reset();
    inner_alloc_ = nullptr;
  }
}

PCVSchemaObj::~PCVSchemaObj()
{
  reset();
}

ObPlanCacheValue::ObPlanCacheValue()
    : pcv_set_(NULL),
      pc_alloc_(NULL),
      last_plan_id_(OB_INVALID_ID),
      use_global_location_cache_(true),
      tenant_schema_version_(OB_INVALID_VERSION),
      sys_schema_version_(OB_INVALID_VERSION),
      merged_version_(OB_INVALID_ID),
      outline_state_(),
      outline_params_wrapper_(),
      sessid_(OB_INVALID_ID),
      sess_create_time_(0),
      contain_sys_name_table_(false),
      use_static_engine_(false),
      use_static_engine_conf_(false),
      stored_schema_objs_(pc_alloc_),
      stmt_type_(stmt::T_MAX)
{
  MEMSET(sql_id_, 0, sizeof(sql_id_));
  // pthread_rwlock_init(&rwlock_, NULL);
}

int ObPlanCacheValue::init(ObPCVSet* pcv_set, const ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr = ObPlanCache::get_mem_attr();
  if (OB_ISNULL(pcv_set) || OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pcv_set), K(cache_obj));
  } else if (OB_ISNULL(pc_alloc_ = pcv_set->get_pc_allocator())) {
    LOG_WARN("invalid argument", K(pcv_set->get_pc_allocator()));
  } else if (OB_FAIL(outline_params_wrapper_.set_allocator(pc_alloc_, mem_attr))) {
    LOG_WARN("fail to set outline param wrapper allocator", K(ret));
  } else if (OB_NOT_NULL(pc_ctx.exec_ctx_.get_outline_params_wrapper())) {
    if (OB_FAIL(set_outline_params_wrapper(*pc_ctx.exec_ctx_.get_outline_params_wrapper()))) {
      LOG_WARN("fail to set outline params", K(ret));
    }
  } else if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_) || OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pc_ctx.sql_ctx_.session_info_), K(pc_ctx.sql_ctx_.schema_guard_));
  }

  if (OB_SUCC(ret)) {
    pcv_set_ = pcv_set;
    use_global_location_cache_ = !cache_obj->is_contain_virtual_table();
    outline_state_ = cache_obj->get_outline_state();
    sys_schema_version_ = cache_obj->get_sys_schema_version();
    tenant_schema_version_ = cache_obj->get_tenant_schema_version();
    merged_version_ = cache_obj->get_merged_version();
    sql_traits_ = pc_ctx.sql_traits_;
    use_static_engine_ = pc_ctx.sql_ctx_.session_info_->use_static_typing_engine();
    use_static_engine_conf_ = GCONF._enable_static_typing_engine;
    stmt_type_ = cache_obj->get_stmt_type();
    MEMCPY(sql_id_, pc_ctx.sql_ctx_.sql_id_, sizeof(pc_ctx.sql_ctx_.sql_id_));
    if (OB_FAIL(not_param_index_.add_members2(pc_ctx.not_param_index_))) {
      LOG_WARN("fail to add not param index members", K(ret));
    } else if (OB_FAIL(neg_param_index_.add_members2(pc_ctx.neg_param_index_))) {
      LOG_WARN("fail to add neg param index members", K(ret));
    } else if (OB_FAIL(param_charset_type_.assign(pc_ctx.param_charset_type_))) {
      LOG_WARN("fail to assign param charset type", K(ret));
    } else if (OB_FAIL(must_be_positive_idx_.add_members2(pc_ctx.must_be_positive_index_))) {
      LOG_WARN("failed to add bitset members", K(ret));
    } else if (OB_FAIL(set_stored_schema_objs(cache_obj->get_dependency_table(), pc_ctx.sql_ctx_.schema_guard_))) {
      LOG_WARN("failed to set stored schema objs",
          K(ret),
          K(cache_obj->get_dependency_table()),
          K(pc_ctx.sql_ctx_.schema_guard_));
    } else {
      // deep copy special param raw text
      if (pc_ctx.is_ps_mode_) {
        if (OB_FAIL(not_param_var_.assign(pc_ctx.not_param_var_))) {
          LOG_WARN("fail to assign not param var", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < not_param_var_.count(); ++i) {
          if (OB_FAIL(deep_copy_obj(*pc_alloc_, not_param_var_.at(i).ps_param_, not_param_var_.at(i).ps_param_))) {
            LOG_WARN("deep_copy_obj failed", K(i), K(not_param_var_.at(i)));
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < pc_ctx.not_param_info_.count(); i++) {
          if (OB_FAIL(not_param_info_.push_back(pc_ctx.not_param_info_.at(i)))) {
            LOG_WARN("fail to push not_param_info", K(ret));
          } else if (OB_FAIL(ob_write_string(
                         *pc_alloc_, not_param_info_.at(i).raw_text_, not_param_info_.at(i).raw_text_))) {
            LOG_WARN("fail to deep copy param raw text", K(ret));
          }
        }  // for end
      }
      // deep copy constructed sql
      if (OB_SUCC(ret)) {
        ObString outline_signature_str;
        if (pc_ctx.is_ps_mode_) {
          outline_signature_str = pc_ctx.raw_sql_;
        } else {
          outline_signature_str = pc_ctx.bl_key_.constructed_sql_;
        }
        int64_t size = outline_signature_str.get_serialize_size();
        if (0 == size) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          char* buf = NULL;
          int64_t pos_s = 0;
          if (OB_UNLIKELY(NULL == (buf = (char*)pc_alloc_->alloc(size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc mem", K(ret));
          } else if (OB_FAIL(outline_signature_str.serialize(buf, size, pos_s))) {
            LOG_WARN("fail to serialize constructed_sql_", K(ret));
          } else {
            outline_signature_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(pos_s));
          }
        }
      }
      // deep copy constructed_sql_, used for baseline;
      if (OB_SUCC(ret)) {
        if (pc_ctx.is_ps_mode_ && OB_FAIL(ob_write_string(*pc_alloc_, pc_ctx.raw_sql_, constructed_sql_))) {
          LOG_WARN("fail to deep copy param raw text", K(ret));
        } else if (!pc_ctx.is_ps_mode_ &&
                   OB_FAIL(ob_write_string(*pc_alloc_, pc_ctx.bl_key_.constructed_sql_, constructed_sql_))) {
          LOG_WARN("failed to write string", K(ret));
        }
      }

      // set sessid_ if necessary
      if (OB_SUCC(ret)) {
        if (is_contain_tmp_tbl()) {
          sessid_ = pc_ctx.sql_ctx_.session_info_->get_sessid_for_table();
          sess_create_time_ = pc_ctx.sql_ctx_.session_info_->get_sess_create_time();
          pc_ctx.tmp_table_names_.reset();
          if (OB_FAIL(get_tmp_depend_tbl_names(pc_ctx.tmp_table_names_))) {
            LOG_WARN("failed to get tmp depend tbl ids", K(ret));
          } else {
            // do nothing
          }
        } else { /* do nothing */
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      table_stat_versions_.set_allocator(pc_alloc_);
      if (OB_FAIL(table_stat_versions_.assign(cache_obj->get_table_stat_versions()))) {
        LOG_WARN("assign table statistic versions failed", K(ret));
      }
    }
  }

  return ret;
}

int ObPlanCacheValue::choose_plan(
    ObPlanCacheCtx& pc_ctx, const ObIArray<PCVSchemaObj>& schema_array, ObCacheObject*& plan_out)
{
  int ret = OB_SUCCESS;
  bool is_old_version = false;
  plan_out = NULL;
  ObSQLSessionInfo* session = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  ObCacheObject* plan = NULL;
  ObIPartitionLocationCache* location_cache_used = NULL;
  int64_t outline_param_idx = 0;
  LOG_DEBUG("choose plan");
  bool enable_baseline = false;
  bool need_check_schema = (schema_array.count() != 0);
  if (OB_ISNULL(session = pc_ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(ERROR, "got session is NULL", K(ret));
  } else if (FALSE_IT(session->set_stmt_type(stmt_type_))) {
  } else if (OB_FAIL(session->get_use_plan_baseline(enable_baseline))) {
    LOG_WARN("fail to get use plan baseline", K(ret));
  } else if (enable_baseline && pc_ctx.bl_key_.constructed_sql_.empty()) {
    if (OB_FAIL(ob_write_string(pc_ctx.allocator_, constructed_sql_, pc_ctx.bl_key_.constructed_sql_))) {
      LOG_WARN("fail to deep copy constructed_sql_", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_INVALID_ID == (tenant_id = session->get_effective_tenant_id())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(ERROR, "got effective tenant id is invalid", K(ret));
  } else if (OB_FAIL(check_value_version_for_get(pc_ctx.sql_ctx_.schema_guard_,
                 need_check_schema,
                 schema_array,
                 tenant_id,
                 pc_ctx.sql_ctx_.merged_version_,
                 is_old_version))) {
    SQL_PC_LOG(WARN, "fail to check table version", K(ret));
  } else if (true == is_old_version) {
    ret = OB_OLD_SCHEMA_VERSION;
    SQL_PC_LOG(DEBUG, "view or table is old version", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_partition_location_cache(pc_ctx, location_cache_used))) {
    LOG_WARN("fail to get partition location cache", K(ret));
  } else {
    ParamStore* params = pc_ctx.fp_result_.cache_params_;
    // init param store
    if (!pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt() && OB_FAIL(resolver_params(pc_ctx,
                                                                         stmt_type_,
                                                                         param_charset_type_,
                                                                         neg_param_index_,
                                                                         not_param_index_,
                                                                         must_be_positive_idx_,
                                                                         pc_ctx.fp_result_.raw_params_,
                                                                         params))) {
      LOG_WARN("fail to resolver raw params", K(ret));
    } else if (pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt() && OB_FAIL(resolve_multi_stmt_params(pc_ctx))) {
      if (OB_BATCHED_MULTI_STMT_ROLLBACK != ret) {
        LOG_WARN("failed to resolver row params", K(ret));
      }
    } else if (OB_FAIL(get_outline_param_index(pc_ctx.exec_ctx_,
                   outline_param_idx))) {  // need use param store
      LOG_WARN("fail to judge concurrent limit sql", K(ret));
    } else {
      int64_t org_param_count = 0;
      if (OB_NOT_NULL(params)) {
        org_param_count = params->count();
      }
      DLIST_FOREACH(plan_set, plan_sets_)
      {
        plan = NULL;
        bool is_same = false;
        if (OB_ISNULL(plan_set)) {
          ret = OB_INVALID_ARGUMENT;
          SQL_PC_LOG(WARN, "invalid argument", K(plan_set), K(ret));
        } else if (OB_FAIL(plan_set->match_params_info(params, pc_ctx, outline_param_idx, is_same))) {
          SQL_PC_LOG(WARN, "fail to match params info", K(ret));
        } else if (!is_same) {  // do nothing
          LOG_TRACE("params info does not match", KPC(params));
        } else {
          if (OB_FAIL(plan_set->select_plan(location_cache_used, pc_ctx, plan))) {
            if (OB_SQL_PC_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              SQL_PC_LOG(TRACE, "failed to select plan in plan set", K(ret));
            }
          } else {
            break;
          }
        }
        if (NULL == plan && OB_NOT_NULL(params) && (params->count() > org_param_count)) {
          ObPhysicalPlanCtx* phy_ctx = NULL;
          if (session->use_static_typing_engine()) {
            phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
            if (NULL == phy_ctx) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("physical ctx is null", K(ret));
            }
          }
          for (int64_t i = params->count(); OB_SUCC(ret) && i > org_param_count; --i) {
            params->pop_back();
            if (session->use_static_typing_engine()) {
              phy_ctx->get_datum_param_store().pop_back();
            }
          }
        }
      }  // end for
    }
  }

  if (NULL == plan && OB_SUCC(ret)) {
    ret = OB_SQL_PC_NOT_EXIST;
  }

  if (OB_SUCC(ret)) {
    plan_out = plan;
    pc_ctx.sql_traits_ = sql_traits_;  // used for check read only
  }
  return ret;
}

int ObPlanCacheValue::resolver_params(ObPlanCacheCtx& pc_ctx, const stmt::StmtType stmt_type,
    const ObIArray<ObCharsetType>& param_charset_type, const ObBitSet<>& neg_param_index,
    const ObBitSet<>& not_param_index_, const ObBitSet<>& must_be_positive_idx, ObIArray<ObPCParam*>& raw_params,
    ParamStore* obj_params)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = pc_ctx.exec_ctx_.get_my_session();
  ParseNode* raw_param = NULL;
  ObObjParam value;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret), KP(session));
  } else if (obj_params != NULL) {
    ObCollationType collation_connection = static_cast<ObCollationType>(session->get_local_collation_connection());
    int64_t N = raw_params.count();
    (void)obj_params->reserve(N);
    if (N != param_charset_type.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("raw_params and param_charset_type count is different",
          K(N),
          K(param_charset_type.count()),
          K(pc_ctx.raw_sql_),
          K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      value.reset();
      if (OB_ISNULL(raw_params.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(raw_params.at(i)));
      } else if (NULL == (raw_param = raw_params.at(i)->node_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(raw_param));
      } else if (!not_param_index_.has_member(i)) {  // not param
        if (neg_param_index.has_member(i)) {
          // select -  1.2 from dual
          // "-  1.2" will be treated as a const node with neg sign
          // however, ObNumber::from("-  1.2") will throw a error, for there are spaces between neg sign and num
          // so remove spaces before resolve_const is called
          if (OB_FAIL(rm_space_for_neg_num(raw_param, pc_ctx.allocator_))) {
            SQL_PC_LOG(WARN, "fail to remove spaces for neg node", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(handle_varchar_charset(param_charset_type.at(i), pc_ctx.allocator_, raw_param))) {
          SQL_PC_LOG(WARN, "fail to handle varchar charset");
        }
        ObString literal_prefix;
        const bool is_paramlize = false;
        CHECK_COMPATIBILITY_MODE(session);
        int64_t server_collation = CS_TYPE_INVALID;
        if (OB_FAIL(ret)) {
        } else if (share::is_oracle_mode() &&
                   OB_FAIL(session->get_sys_variable(share::SYS_VAR_COLLATION_SERVER, server_collation))) {
          LOG_WARN("get sys variable failed", K(ret));
        } else if (OB_FAIL(ObResolverUtils::resolve_const(raw_param,
                       stmt_type,
                       pc_ctx.allocator_,
                       collation_connection,
                       session->get_nls_collation_nation(),
                       session->get_timezone_info(),
                       value,
                       is_paramlize,
                       literal_prefix,
                       session->get_actual_nls_length_semantics(),
                       static_cast<ObCollationType>(server_collation)))) {
          SQL_PC_LOG(WARN, "fail to resolve const", K(ret));
        } else if (FALSE_IT(value.set_raw_text_info(
                       static_cast<int32_t>(raw_param->raw_sql_offset_), static_cast<int32_t>(raw_param->text_len_)))) {
          // nothing.
        } else if (OB_FAIL(obj_params->push_back(value))) {
          SQL_PC_LOG(WARN, "fail to push item to array", K(ret));
        } else if (ob_is_numeric_type(value.get_type())) {
          if (must_be_positive_idx.has_member(i)) {
            if (share::is_oracle_mode() &&
                (value.is_negative_number() ||
                    (value.is_zero_number() && '-' == raw_param->str_value_[0]))) {  // -0 is also counted as negative
              ret = OB_NOT_SUPPORTED;
              LOG_DEBUG("param must be positive", K(ret), K(i), K(value));
              pc_ctx.should_add_plan_ = false;
            } else if (share::is_mysql_mode() && value.is_integer_type() &&
                       (value.get_int() < 0 || (0 == value.get_int() && '-' == raw_param->str_value_[0]))) {
              ret = OB_NOT_SUPPORTED;
              LOG_DEBUG("param must be positive", K(ret), K(i), K(value));
              pc_ctx.should_add_plan_ = false;
            } else {
              // do nothing
            }
          }
        }
        SQL_PC_LOG(DEBUG,
            "is_param",
            K(i),
            K(value),
            K(raw_param->type_),
            K(raw_param->value_),
            "str_value",
            ObString(raw_param->str_len_, raw_param->str_value_));
      } else {
        SQL_PC_LOG(DEBUG,
            "not_param",
            K(i),
            K(value),
            K(raw_param->type_),
            K(raw_param->value_),
            "str_value",
            ObString(raw_param->str_len_, raw_param->str_value_));
      }
    }  // for end
  }
  if (OB_SUCC(ret)) {
    if (session->use_static_typing_engine()) {
      ObPhysicalPlanCtx* phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
      if (NULL != phy_ctx) {
        if (OB_FAIL(phy_ctx->init_datum_param_store())) {
          LOG_WARN("fail to init datum param store", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPlanCacheValue::resolve_multi_stmt_params(ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  ParamStore* obj_params = pc_ctx.fp_result_.cache_params_;
  if (OB_ISNULL(obj_params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    bool is_valid = true;
    int64_t query_num = pc_ctx.multi_stmt_fp_results_.count();
    int64_t param_num = pc_ctx.fp_result_.raw_params_.count() - not_param_info_.count();
    // check whether all the values are the same
    if (!not_param_info_.empty() &&
        OB_FAIL(check_multi_stmt_not_param_value(pc_ctx.multi_stmt_fp_results_, not_param_info_, is_valid))) {
      LOG_WARN("failed to check multi stmt not param value", K(ret));
    } else if (!is_valid) {
      ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      LOG_TRACE("batched multi_stmt needs rollback", K(ret));
    } else if (OB_FAIL(create_multi_stmt_param_store(pc_ctx.allocator_, query_num, param_num, *obj_params))) {
      LOG_WARN("failed to create multi_stmt_param_store", K(ret));
    } else if (OB_FAIL(check_multi_stmt_param_type(pc_ctx,
                   stmt_type_,
                   param_charset_type_,
                   neg_param_index_,
                   not_param_index_,
                   must_be_positive_idx_,
                   *obj_params,
                   is_valid))) {
      LOG_WARN("failed to check multi stmt param type", K(ret));
    } else if (!is_valid) {
      ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      LOG_TRACE("batched multi_stmt needs rollback", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObPlanCacheValue::create_multi_stmt_param_store(
    common::ObIAllocator& allocator, int64_t query_num, int64_t param_num, ParamStore& param_store)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (OB_FAIL(param_store.reserve(param_num))) {
    LOG_WARN("failed to reserver param num", K(param_num), K(ret));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", K(query_num), K(param_store));
  }
  return ret;
}

int ObPlanCacheValue::check_multi_stmt_param_type(ObPlanCacheCtx& pc_ctx, stmt::StmtType stmt_type,
    const ObIArray<ObCharsetType>& param_charset_type, const ObBitSet<>& neg_param_index,
    const ObBitSet<>& not_param_index, const ObBitSet<>& must_be_positive_idx, ParamStore& param_store, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc;
  ParamStore temp_obj_params((ObWrapperAllocator(tmp_alloc)));
  ParamStore first_obj_params((ObWrapperAllocator(tmp_alloc)));
  int64_t query_num = pc_ctx.multi_stmt_fp_results_.count();
  int64_t param_num = pc_ctx.fp_result_.raw_params_.count() - not_param_index.num_members();
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < query_num; i++) {
    temp_obj_params.reuse();
    if (OB_FAIL(resolver_params(pc_ctx,
            stmt_type,
            param_charset_type,
            neg_param_index,
            not_param_index,
            must_be_positive_idx,
            pc_ctx.multi_stmt_fp_results_.at(i).raw_params_,
            &temp_obj_params))) {
      LOG_WARN("failed to resolve parames", K(ret), K(param_store));
    } else if (i == 0) {
      ret = first_obj_params.assign(temp_obj_params);
      ObDataType data_type;
      // set type and external type
      for (int64_t j = 0; OB_SUCC(ret) && j < param_num; j++) {
        data_type.reset();
        data_type.set_meta_type(temp_obj_params.at(j).get_meta());
        data_type.set_accuracy(temp_obj_params.at(j).get_accuracy());
      }
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < param_num; j++) {
        // need to compare accuracy and result flag ?
        if (first_obj_params.at(j).get_type() != temp_obj_params.at(j).get_type() ||
            first_obj_params.at(j).get_scale() != temp_obj_params.at(j).get_scale()) {
          is_valid = false;
          if (REACH_TIME_INTERVAL(10000000)) {
            LOG_INFO("batched multi-stmt does not have the same param type",
                K(i),
                K(first_obj_params.at(j)),
                K(temp_obj_params.at(j)));
          }
        }
      }
    }
  }
  return ret;
}

int ObPlanCacheValue::check_multi_stmt_not_param_value(const ObIArray<ObFastParserResult>& multi_stmt_fp_results,
    const ObIArray<NotParamInfo>& not_param_info, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < multi_stmt_fp_results.count(); i++) {
    if (OB_FAIL(check_not_param_value(multi_stmt_fp_results.at(i), not_param_info, is_same))) {
      LOG_WARN("failed to check not param value", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObPlanCacheValue::check_not_param_value(
    const ObFastParserResult& fp_result, const ObIArray<NotParamInfo>& not_param_info, bool& is_same)
{
  int ret = OB_SUCCESS;
  ParseNode* raw_param = NULL;
  ObPCParam* pc_param = NULL;
  is_same = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < not_param_info.count(); ++i) {
    if (OB_FAIL(fp_result.raw_params_.at(not_param_info.at(i).idx_, pc_param))) {
      LOG_WARN("fail to get raw params", K(not_param_info.at(i).idx_), K(ret));
    } else if (OB_ISNULL(pc_param)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(pc_param));
    } else if (NULL == (raw_param = pc_param->node_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(raw_param));
    } else if (0 != not_param_info.at(i).raw_text_.compare(ObString(raw_param->text_len_, raw_param->raw_text_))) {
      is_same = false;
      LOG_DEBUG("match not param info",
          "raw value",
          ObString(raw_param->text_len_, raw_param->raw_text_),
          "cached special value",
          not_param_info.at(i).raw_text_);
    } else {
      LOG_DEBUG("match param info",
          "raw value",
          ObString(raw_param->text_len_, raw_param->raw_text_),
          "cached special value",
          not_param_info.at(i).raw_text_);
    }
  }
  return ret;
}

int ObPlanCacheValue::get_outline_param_index(ObExecContext& exec_ctx, int64_t& param_idx) const
{
  int ret = OB_SUCCESS;
  param_idx = OB_INVALID_INDEX;
  int64_t param_count = outline_params_wrapper_.get_outline_params().count();
  int64_t concurrent_num = INT64_MAX;
  if (exec_ctx.get_physical_plan_ctx() != NULL) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      bool is_match = false;
      const ObMaxConcurrentParam* param = outline_params_wrapper_.get_outline_params().at(i);
      if (OB_ISNULL(param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param is NULl", K(ret));
      } else if (OB_FAIL(param->match_fixed_param(exec_ctx.get_physical_plan_ctx()->get_param_store(), is_match))) {
        LOG_WARN("fail to match", K(ret), K(i));
      } else if (is_match && param->concurrent_num_ < concurrent_num) {
        concurrent_num = param->concurrent_num_;
        param_idx = i;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObPlanCacheValue::get_partition_location_cache(
    ObPlanCacheCtx& pc_ctx, ObIPartitionLocationCache*& location_cache_used)
{
  int ret = OB_SUCCESS;
  if (use_global_location_cache_) {
    location_cache_used = pc_ctx.sql_ctx_.partition_location_cache_;
  } else {
    ObTaskExecutorCtx* task_executor_ctx = pc_ctx.exec_ctx_.get_task_executor_ctx();
    if (OB_ISNULL(task_executor_ctx)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "task_executor_ctx is null", K(task_executor_ctx), K(ret));
    } else {
      location_cache_used = task_executor_ctx->get_partition_location_cache();
    }
  }
  return ret;
}

const ObMaxConcurrentParam* ObPlanCacheValue::get_outline_param(int64_t index) const
{
  return outline_params_wrapper_.get_outline_param(index);
}

int ObPlanCacheValue::add_plan(
    ObCacheObject& cache_object, const ObIArray<PCVSchemaObj>& schema_array, ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  bool need_new_planset = true;
  bool is_old_version = false;
  int64_t outline_param_idx = OB_INVALID_INDEX;
  if (OB_FAIL(check_value_version_for_add(cache_object, schema_array, is_old_version))) {
    SQL_PC_LOG(WARN, "fail to check table version", K(ret));
  } else if (is_old_version) {
    ret = OB_OLD_SCHEMA_VERSION;
    SQL_PC_LOG(DEBUG, "view or table is old version", K(ret));
  }

  if (OB_FAIL(ret)) {  // do nothing
  } else if (OB_FAIL(get_outline_param_index(pc_ctx.exec_ctx_, outline_param_idx))) {
    LOG_WARN("fail to judge concurrent limit sql", K(ret));
  }
  if (OB_SUCC(ret)) {
    DLIST_FOREACH(cur_plan_set, plan_sets_)
    {
      bool is_same = false;
      if (OB_ISNULL(cur_plan_set)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_PC_LOG(WARN, "invalid argument", K(cur_plan_set), K(ret));
      } else if (OB_FAIL(cur_plan_set->match_params_info(
                     cache_object.get_params_info(), outline_param_idx, pc_ctx, is_same))) {
        SQL_PC_LOG(WARN, "fail to match params info", K(ret));
      } else if (false == is_same) {  // do nothing
      } else {
        SQL_PC_LOG(DEBUG, "add plan to plan set");
        need_new_planset = false;
        if (OB_FAIL(cur_plan_set->add_cache_obj(cache_object, pc_ctx, outline_param_idx))) {
          SQL_PC_LOG(DEBUG, "failed to add plan", K(ret));
        }
        break;
      }
    }  // end for

    /**
     *  Add the plan to a new allocated plan set
     *  and then add the plan set to plan cache value
     */
    if (OB_SUCC(ret) && need_new_planset) {
      SQL_PC_LOG(DEBUG, "add new plan set");
      ObPlanSet* plan_set = nullptr;
      if (OB_FAIL(
              create_new_plan_set(ObPlanSet::get_plan_set_type_by_cache_obj_type(cache_object.get_type()), plan_set))) {
        SQL_PC_LOG(WARN, "failed to create new plan set", K(ret));
      } else {
        plan_set->set_plan_cache_value(this);
        if (OB_FAIL(plan_set->init_new_set(pc_ctx, cache_object, outline_param_idx, get_pc_alloc()))) {
          LOG_WARN("init new plan set failed", K(ret));
        } else if (OB_FAIL(plan_set->add_cache_obj(cache_object, pc_ctx, outline_param_idx))) {
          SQL_PC_LOG(DEBUG, "failed to add plan to plan set", K(ret));
        } else if (!plan_sets_.add_last(plan_set)) {
          ret = OB_ERROR;
          SQL_PC_LOG(WARN, "failed to add plan set to plan cache value", K(ret));
        } else {
          ret = OB_SUCCESS;
          SQL_PC_LOG(DEBUG, "plan set added", K(ret));
        }

        // free the memory if not used
        if (OB_FAIL(ret) && NULL != plan_set) {
          free_plan_set(plan_set);
          plan_set = NULL;
        }
      }
    }
  }
  return ret;
}

/*void ObPlanCacheValue::remove_plan(ObExecContext &exec_context, ObPhysicalPlan &plan)*/
//{
// int ret = OB_SUCCESS;
// int64_t outline_param_idx = OB_INVALID_INDEX;
// if (OB_FAIL(get_outline_param_index(exec_context, outline_param_idx))) {
// LOG_WARN("fail to judge concurrent limit sql", K(ret));
//} else {
// DLIST_FOREACH(cur_plan_set, plan_sets_) {
// bool is_same = false;
// if (OB_FAIL(cur_plan_set->match_params_info(plan.get_params_info(),
// outline_param_idx,
// is_same))) {
// BACKTRACE(ERROR, true, "fail to match param info");
//} else if (true == is_same) {
// cur_plan_set->remove_plan(plan);
//}
//} // end for
//}
/*}*/

void ObPlanCacheValue::reset()
{
  // TODO TBD: do nothing to rw_lock_
  ObDLinkBase<ObPlanCacheValue>::reset();
  last_plan_id_ = OB_INVALID_ID;
  while (!plan_sets_.is_empty()) {
    ObPlanSet* plan_set = plan_sets_.get_first();
    if (OB_ISNULL(plan_set)) {
      // do nothing;
    } else {
      plan_sets_.remove(plan_set);
      plan_set->remove_plan_stat();
      plan_set->remove_all_plan();
      free_plan_set(plan_set);
      plan_set = nullptr;
    }
  }
  plan_sets_.clear();
  // free plan_cache_key
  if (NULL == pc_alloc_) {
    SQL_PC_LOG(DEBUG, "pc alloc not init, may be reset before", K(pc_alloc_));
  } else {
    for (int64_t i = 0; i < not_param_info_.count(); i++) {
      if (NULL != not_param_info_.at(i).raw_text_.ptr()) {
        pc_alloc_->free(not_param_info_.at(i).raw_text_.ptr());
        not_param_info_.at(i).raw_text_.reset();
      }
    }
    if (NULL != outline_signature_.ptr()) {
      pc_alloc_->free(outline_signature_.ptr());
      outline_signature_.reset();
    }
    if (NULL != constructed_sql_.ptr()) {
      pc_alloc_->free(constructed_sql_.ptr());
      constructed_sql_.reset();
    }
  }
  not_param_info_.reset();
  not_param_index_.reset();
  neg_param_index_.reset();
  param_charset_type_.reset();
  sql_traits_.reset();

  if (OB_SUCCESS != outline_params_wrapper_.destroy()) {
    LOG_ERROR("fail to destroy ObOutlineParamWrapper");
  }
  outline_params_wrapper_.reset_allocator();
  use_global_location_cache_ = true;
  table_stat_versions_.reset();
  tenant_schema_version_ = OB_INVALID_VERSION;
  sys_schema_version_ = OB_INVALID_VERSION;
  merged_version_ = OB_INVALID_VERSION;
  outline_state_.reset();
  sessid_ = OB_INVALID_ID;
  sess_create_time_ = 0;
  contain_sys_name_table_ = false;
  use_static_engine_ = false;
  use_static_engine_conf_ = false;
  for (int64_t i = 0; i < stored_schema_objs_.count(); i++) {
    if (OB_ISNULL(stored_schema_objs_.at(i)) || OB_ISNULL(pc_alloc_)) {
      // do nothing
    } else {
      stored_schema_objs_.at(i)->reset();
      pc_alloc_->free(stored_schema_objs_.at(i));
    }
  }
  stored_schema_objs_.reset();
  pcv_set_ = NULL;
}

int64_t ObPlanCacheValue::get_mem_size()
{
  int64_t value_mem_size = 0;
  DLIST_FOREACH_NORET(plan_set, plan_sets_)
  {
    if (OB_ISNULL(plan_set)) {
      BACKTRACE(ERROR, true, "invalid plan_set");
    } else {
      value_mem_size += plan_set->get_mem_size();
    }
  }  // end for
  return value_mem_size;
}

int ObPlanCacheValue::remove_plan_stat()
{
  int ret = OB_SUCCESS;
  ObPlanSet* plan_set = plan_sets_.get_first();
  for (; plan_set != plan_sets_.get_header(); plan_set = plan_set->get_next()) {
    if (OB_ISNULL(plan_set)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(plan_set), K(ret));
      break;
    } else {
      plan_set->remove_plan_stat();
    }
  }
  return ret;
}

int ObPlanCacheValue::check_value_version_for_add(
    const ObCacheObject& cache_obj, const ObIArray<PCVSchemaObj>& schema_array, bool& result)
{
  int ret = OB_SUCCESS;
  result = false;
  if (OB_FAIL(check_value_version(true /*need_check_schema*/,
          cache_obj.get_outline_state().outline_version_,
          schema_array,
          cache_obj.get_merged_version(),
          cache_obj.get_table_stat_versions(),
          result))) {
    LOG_WARN("failed to check pcv version", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObPlanCacheValue::check_value_version_for_get(share::schema::ObSchemaGetterGuard* schema_guard,
    bool need_check_schema, const ObIArray<PCVSchemaObj>& schema_array, const uint64_t tenant_id,
    const int64_t merged_version, bool& result)
{
  int ret = OB_SUCCESS;
  ObSchemaObjVersion local_outline_version;
  result = false;
  if (OB_ISNULL(schema_guard)) {
    int ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_guard));
  } else if (need_check_schema && OB_FAIL(get_outline_version(*schema_guard, tenant_id, local_outline_version))) {
    LOG_WARN("failed to get local outline version", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_value_version(need_check_schema,
                 local_outline_version,
                 schema_array,
                 merged_version,
                 table_stat_versions_,
                 result))) {
    LOG_WARN("failed to check value version", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPlanCacheValue::check_value_version(bool need_check_schema, const ObSchemaObjVersion& outline_version,
    const ObIArray<PCVSchemaObj>& schema_array, const int64_t merged_version, const TableStatVersions& table_stats,
    bool& is_old_version)
{
  int ret = OB_SUCCESS;
  is_old_version = false;
  UNUSED(table_stats);
  if (need_check_schema) {
    if (outline_version != outline_state_.outline_version_) {
      is_old_version = true;
    } else if (OB_FAIL(check_dep_schema_version(schema_array, stored_schema_objs_, is_old_version))) {
      LOG_WARN("failed to check schema obj versions", K(ret));
    } else {
      // do nothing
    }
  }

  // if (OB_SUCC(ret) && !is_old_version && GCONF.enable_perf_event) {
  //  is_old_version = is_old_table_stat(table_stats);
  //}

  if (OB_SUCC(ret) && !is_old_version) {
    if (outline_state_.is_plan_fixed_ || OB_INVALID_VERSION == merged_version_ ||
        0 == GCONF.merge_stat_sampling_ratio) {
      // don't need to check merged_version when is fixed plan
    } else {
      is_old_version = merged_version != merged_version_;
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPlanCacheValue::check_dep_schema_version(
    const ObIArray<PCVSchemaObj>& schema_array, const ObIArray<PCVSchemaObj*>& pcv_schema_objs, bool& is_old_version)
{
  int ret = OB_SUCCESS;
  is_old_version = false;
  int64_t table_count = pcv_schema_objs.count();

  if (schema_array.count() != pcv_schema_objs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table count do not match", K(ret), K(schema_array.count()), K(pcv_schema_objs.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_old_version && i < table_count; ++i) {
      const PCVSchemaObj* schema_obj1 = pcv_schema_objs.at(i);
      const PCVSchemaObj& schema_obj2 = schema_array.at(i);
      if (nullptr == schema_obj1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got an unexpected null table schema", K(ret), K(schema_obj1));
      } else if (*schema_obj1 == schema_obj2) {  // schema do match
        LOG_DEBUG("matched schema objs", K(*schema_obj1), K(schema_obj2), K(i));
        // do nothing
      } else {
        LOG_DEBUG("mismatched schema objs", K(*schema_obj1), K(schema_obj2), K(i));
        is_old_version = true;
      }
    }
  }

  return ret;
}

bool ObPlanCacheValue::is_old_table_stat(const common::ObIArray<common::ObOptTableStatVersion>& table_stat_versions)
{
  bool is_old = false;
  int ret = OB_SUCCESS;
  int64_t table_count = table_stat_versions.count();
  ObOptStatManager& stat_manager = ObOptStatManager::get_instance();
  ObOptTableStat stat;
  for (int64_t i = 0; OB_SUCC(ret) && !is_old && i < table_count; ++i) {
    const ObOptTableStatVersion& stat_version = table_stat_versions.at(i);
    if (OB_FAIL(stat_manager.get_table_stat(stat_version.key_, stat))) {
      LOG_WARN("get stat failed", K(ret));
    } else if (stat_version.stat_version_ < stat.get_last_analyzed()) {
      is_old = true;
    }
  }
  return is_old;
}

int ObPlanCacheValue::get_outline_version(
    ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id, ObSchemaObjVersion& local_outline_version)
{
  int ret = OB_SUCCESS;
  const ObOutlineInfo* outline_info = NULL;
  local_outline_version.reset();
  uint64_t database_id = OB_INVALID_ID;
  if (OB_ISNULL(pcv_set_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pcv_set_ is null");
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id));
  } else if (OB_INVALID_ID == (database_id = pcv_set_->get_plan_cache_key().db_id_) ||
             NS_PRCD == pcv_set_->get_plan_cache_key().namespace_ ||
             NS_PKG == pcv_set_->get_plan_cache_key().namespace_ ||
             NS_ANON == pcv_set_->get_plan_cache_key().namespace_) {
    // do nothing
  } else {
    const ObString& signature = outline_signature_;
    if (OB_FAIL(schema_guard.get_outline_info_with_signature(tenant_id, database_id, signature, outline_info))) {
      LOG_WARN("failed to get_outline_info", K(tenant_id), K(database_id), K(signature));
    } else if (NULL == outline_info) {
      if (OB_FAIL(schema_guard.get_outline_info_with_sql_id(
              tenant_id, database_id, ObString::make_string(sql_id_), outline_info))) {
        LOG_WARN("failed to get_outline_info", K(tenant_id), K(database_id), K(signature));
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL == outline_info) {
        // do nothing
      } else {
        local_outline_version.object_id_ = outline_info->get_outline_id();
        local_outline_version.version_ = outline_info->get_schema_version();
        local_outline_version.object_type_ = DEPENDENCY_OUTLINE;
      }
    }
  }
  return ret;
}

int ObPlanCacheValue::match(ObPlanCacheCtx& pc_ctx, const ObIArray<PCVSchemaObj>& schema_array, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  // compare not param
  ObSQLSessionInfo* session_info = pc_ctx.sql_ctx_.session_info_;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(session_info));
  }
  if (OB_FAIL(ret)) {
  } else if (pc_ctx.is_ps_mode_) {
    const ObObjParam* ps_param = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < not_param_var_.count(); ++i) {
      ps_param = NULL;
      if (OB_FAIL(pc_ctx.fp_result_.ps_params_.at(not_param_var_[i].idx_, ps_param))) {
        LOG_WARN("fail to get ps param", K(not_param_info_[i].idx_), K(ret));
      } else if (OB_ISNULL(ps_param)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ps_param));
      } else if (!not_param_var_[i].ps_param_.can_compare(*ps_param)) {
        is_same = false;
        LOG_WARN("can not compare", K(not_param_var_[i].ps_param_), K(*ps_param), K(i));
      } else if (not_param_var_[i].ps_param_.is_string_type() &&
                 not_param_var_[i].ps_param_.get_collation_type() != ps_param->get_collation_type()) {
        is_same = false;
        LOG_WARN("can not compare", K(not_param_var_[i].ps_param_), K(*ps_param), K(i));
      } else if (0 != not_param_var_[i].ps_param_.compare(*ps_param)) {
        is_same = false;
        LOG_WARN("match not param var", K(not_param_var_[i]), K(ps_param), K(i));
      }
      LOG_DEBUG("match", K(not_param_var_[i].idx_), K(not_param_var_[i].ps_param_), KPC(ps_param));
    }
    if (OB_SUCC(ret) && is_same) {
      if (use_static_engine_ != session_info->use_static_typing_engine() ||
          use_static_engine_conf_ != GCONF._enable_static_typing_engine) {
        is_same = false;
      }
    }
  } else {
    if (OB_FAIL(check_not_param_value(pc_ctx.fp_result_, not_param_info_, is_same))) {
      LOG_WARN("failed to check not param value", K(ret));
    } else if (is_same && (use_static_engine_ != session_info->use_static_typing_engine() ||
                              use_static_engine_conf_ != GCONF._enable_static_typing_engine)) {
      is_same = false;
    }
  }

  // check for temp tables and same table
  if (OB_SUCC(ret) && is_same && schema_array.count() != 0) {
    if (OB_FAIL(match_dep_schema(pc_ctx, schema_array, is_same))) {
      LOG_WARN("failed to check dep table schema", K(ret));
    }
  }
  return ret;
}

int ObPlanCacheValue::handle_varchar_charset(ObCharsetType charset_type, ObIAllocator& allocator, ParseNode*& node)
{
  int ret = OB_SUCCESS;
  if (T_VARCHAR == node->type_ && CHARSET_INVALID != charset_type) {
    ParseNode* charset_node = new_node(&allocator, T_CHARSET, 0);
    ParseNode* varchar_node = new_non_terminal_node(&allocator, T_VARCHAR, 2, charset_node, node);

    if (OB_ISNULL(charset_node) || OB_ISNULL(varchar_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      const char* name = NULL;
      if (CHARSET_BINARY == charset_type) {
        name = "binary";
      } else if (CHARSET_UTF8MB4 == charset_type) {
        name = "utf8mb4";
      }
      charset_node->str_value_ = parse_strdup(name, &allocator, &(charset_node->str_len_));
      if (NULL == charset_node->str_value_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        varchar_node->str_value_ = node->str_value_;
        varchar_node->str_len_ = node->str_len_;
        varchar_node->raw_text_ = node->raw_text_;
        varchar_node->text_len_ = node->text_len_;

        node = varchar_node;
      }
    }
  }

  return ret;
}

bool ObPlanCacheValue::is_contain_tmp_tbl() const
{
  bool is_contain = false;

  for (int64_t i = 0; !is_contain && i < stored_schema_objs_.count(); i++) {
    if (nullptr != stored_schema_objs_.at(i) && stored_schema_objs_.at(i)->is_tmp_table_) {
      is_contain = true;
    }
  }

  return is_contain;
}

bool ObPlanCacheValue::is_contain_sys_pl_object() const
{
  bool is_contain = false;

  for (int64_t i = 0; !is_contain && i < stored_schema_objs_.count(); i++) {
    if (nullptr != stored_schema_objs_.at(i) &&
        (PACKAGE_SCHEMA == stored_schema_objs_.at(i)->schema_type_ ||
            UDT_SCHEMA == stored_schema_objs_.at(i)->schema_type_) &&
        OB_SYS_TENANT_ID == extract_tenant_id(stored_schema_objs_.at(i)->schema_id_)) {
      is_contain = true;
    }
  }

  return is_contain;
}

int ObPlanCacheValue::get_tmp_depend_tbl_names(TmpTableNameArray& tmp_tbl_names)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < stored_schema_objs_.count(); i++) {
    if (OB_ISNULL(stored_schema_objs_.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("got unexpected null", K(ret), K(i));
    } else if (stored_schema_objs_.at(i)->is_tmp_table_ &&
               OB_FAIL(tmp_tbl_names.push_back(stored_schema_objs_.at(i)->table_name_))) {
      LOG_WARN("failed to push back tmp table name ptr", K(ret), K(i));
    }
  }
  return ret;
}

int ObPlanCacheValue::create_new_plan_set(const ObPlanSetType plan_set_type, ObPlanSet*& new_plan_set)
{
  int ret = OB_SUCCESS;
  ObIAllocator* pc_alloc = get_pc_alloc();
  void* buff = nullptr;
  new_plan_set = nullptr;

  int64_t mem_sz = plan_set_type == PST_SQL_CRSR ? sizeof(ObSqlPlanSet) : sizeof(ObPLPlanSet);

  if (OB_ISNULL(pc_alloc) || (plan_set_type < PST_SQL_CRSR || plan_set_type >= PST_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pc_alloc), K(plan_set_type));
  } else if (nullptr == (buff = pc_alloc->alloc(mem_sz))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObPlanSet", K(ret));
  } else {
    if (PST_SQL_CRSR == plan_set_type) {
      new_plan_set = new (buff) ObSqlPlanSet();
    } else {
      new_plan_set = new (buff) ObPLPlanSet();
    }
  }

  if (OB_SUCC(ret)) {
    // do nothing
  } else if (nullptr != new_plan_set && nullptr != pc_alloc) {  // cleanup
    new_plan_set->reset();
    new_plan_set->~ObPlanSet();
    pc_alloc->free(new_plan_set);
    new_plan_set = nullptr;
  }
  return ret;
}

void ObPlanCacheValue::free_plan_set(ObPlanSet* plan_set)
{
  int ret = OB_SUCCESS;
  ObIAllocator* pc_alloc = get_pc_alloc();
  if (OB_ISNULL(pc_alloc) || OB_ISNULL(plan_set)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pc_alloc), K(plan_set), K(ret));
  } else {
    plan_set->reset();
    plan_set->~ObPlanSet();
    pc_alloc->free(plan_set);
    plan_set = nullptr;
  }
}

int ObPlanCacheValue::set_stored_schema_objs(
    const DependenyTableStore& dep_table_store, share::schema::ObSchemaGetterGuard* schema_guard)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = nullptr;
  PCVSchemaObj* pcv_schema_obj = nullptr;
  void* obj_buf = nullptr;

  stored_schema_objs_.reset();
  stored_schema_objs_.set_allocator(pc_alloc_);

  if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null argument", K(ret), K(schema_guard));
  } else if (OB_FAIL(stored_schema_objs_.init(dep_table_store.count()))) {
    LOG_WARN("failed to init stored_schema_objs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dep_table_store.count(); i++) {
      const ObSchemaObjVersion& table_version = dep_table_store.at(i);
      table_schema = nullptr;
      if (table_version.get_schema_type() != TABLE_SCHEMA) {
        if (nullptr == (obj_buf = pc_alloc_->alloc(sizeof(PCVSchemaObj)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else if (FALSE_IT(pcv_schema_obj = new (obj_buf) PCVSchemaObj(pc_alloc_))) {
          // do nothing
        } else if (OB_FAIL(pcv_schema_obj->init_with_version_obj(table_version))) {
          LOG_WARN("failed to init pcv schema obj", K(ret), K(table_version));
        } else if (OB_FAIL(stored_schema_objs_.push_back(pcv_schema_obj))) {
          LOG_WARN("failed to push back array", K(ret));
        } else {
          obj_buf = nullptr;
          pcv_schema_obj = nullptr;
        }
      } else if (OB_FAIL(schema_guard->get_table_schema(table_version.get_object_id(),
                     table_schema))) {  // now deal with table schema
        LOG_WARN("failed to get table schema", K(ret), K(table_version), K(table_schema));
      } else if (nullptr == table_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get an unexpected null schema", K(ret), K(table_schema));
      } else if (table_schema->is_index_table() || table_schema->is_dropped_schema()) {
        // do nothing
      } else if (nullptr == (obj_buf = pc_alloc_->alloc(sizeof(PCVSchemaObj)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (FALSE_IT(pcv_schema_obj = new (obj_buf) PCVSchemaObj(pc_alloc_))) {
        // do nothing
      } else if (OB_FAIL(pcv_schema_obj->init(table_schema))) {
        LOG_WARN("failed to init pcv schema obj with table schema", K(ret));
      } else if (FALSE_IT(pcv_schema_obj->is_explicit_db_name_ = table_version.is_db_explicit_)) {
        // do nothing
      } else if (OB_FAIL(stored_schema_objs_.push_back(pcv_schema_obj))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (!contain_sys_name_table_) {
        if (share::is_oracle_mode()) {
          if (OB_FAIL(share::schema::ObSysTableChecker::is_sys_table_name(
                  OB_ORA_SYS_DATABASE_ID, table_schema->get_table_name(), contain_sys_name_table_))) {
            LOG_WARN("failed to check sys table", K(ret));
          }
        } else if (OB_FAIL(share::schema::ObSysTableChecker::is_sys_table_name(
                       OB_SYS_DATABASE_ID, table_schema->get_table_name(), contain_sys_name_table_))) {
          LOG_WARN("failed to check sys table", K(ret));
        } else {
          // do nothing
        }
        LOG_DEBUG("check sys table", K(table_schema->get_table_name()), K(contain_sys_name_table_));
      } else {
        // do nothing
      }
      obj_buf = nullptr;
      pcv_schema_obj = nullptr;
      table_schema = nullptr;
    }  // for end
  }
  if (OB_FAIL(ret)) {
    stored_schema_objs_.reset();
  } else {
    // do nothing
  }
  return ret;
}

int ObPlanCacheValue::get_all_dep_schema(ObPlanCacheCtx& pc_ctx, const uint64_t database_id,
    int64_t& new_schema_version, bool& need_check_schema, ObIArray<PCVSchemaObj>& schema_array)
{
  int ret = OB_SUCCESS;
  need_check_schema = false;
  if (OB_FAIL(need_check_schema_version(pc_ctx, new_schema_version, need_check_schema))) {
    LOG_WARN("failed to get need_check_schema flag", K(ret));
  } else if (!need_check_schema) {
    // do nothing
  } else if (OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pc_ctx.sql_ctx_.schema_guard_));
  } else {
    schema_array.reset();
    const ObSimpleTableSchemaV2* table_schema = nullptr;
    PCVSchemaObj tmp_schema_obj;
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_schema_objs_.count(); i++) {
      ObSchemaGetterGuard& schema_guard = *pc_ctx.sql_ctx_.schema_guard_;
      PCVSchemaObj* pcv_schema = stored_schema_objs_.at(i);
      if (OB_ISNULL(pcv_schema)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("got unexpected null", K(ret));
      } else if (TABLE_SCHEMA != pcv_schema->schema_type_) {
        // if no table schema, get schema version is enough
        int64_t new_version = 0;
        if (OB_FAIL(
                schema_guard.get_schema_version_v2(pcv_schema->schema_type_, pcv_schema->schema_id_, new_version))) {
          LOG_WARN("failed to get schema version", K(ret), K(pcv_schema->schema_type_), K(pcv_schema->schema_id_));
        } else {
          tmp_schema_obj.schema_id_ = pcv_schema->schema_id_;
          tmp_schema_obj.schema_type_ = pcv_schema->schema_type_;
          tmp_schema_obj.schema_version_ = new_version;
          if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
            LOG_WARN("failed to push back array", K(ret));
          } else {
            tmp_schema_obj.reset();
          }
        }
      } else if (share::is_oracle_mode()) {
        if (pcv_schema->is_explicit_db_name_) {
          if (OB_FAIL(schema_guard.get_table_schema(pcv_schema->schema_id_, table_schema))) {
            LOG_WARN("failed to get table schema", K(pcv_schema->schema_id_), K(ret));
          } else { /* do nothing */
          }
        } else if (OB_FAIL(schema_guard.get_simple_table_schema(
                       pcv_schema->tenant_id_, database_id, pcv_schema->table_name_, false, table_schema))) {
          LOG_WARN("failed to get table schema", K(pcv_schema->schema_id_), K(ret));
        } else if (nullptr == table_schema && OB_FAIL(schema_guard.get_simple_table_schema(pcv_schema->tenant_id_,
                                                  combine_id(pcv_schema->tenant_id_, common::OB_ORA_SYS_DATABASE_ID),
                                                  pcv_schema->table_name_,
                                                  false,
                                                  table_schema))) {
          LOG_WARN("failed to get table schema", K(ret), K(pcv_schema->tenant_id_), K(pcv_schema->table_name_));
        } else {
          // do nothing
        }
      } else if (OB_FAIL(schema_guard.get_simple_table_schema(
                     pcv_schema->tenant_id_, pcv_schema->database_id_, pcv_schema->table_name_, false, table_schema))) {
        LOG_WARN("failed to get table schema",
            K(ret),
            K(pcv_schema->tenant_id_),
            K(pcv_schema->database_id_),
            K(pcv_schema->table_name_));
      } else {
        // do nothing
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (TABLE_SCHEMA != pcv_schema->schema_type_) {  // not table schema
        tmp_schema_obj.reset();
      } else if (nullptr == table_schema) {
        ret = OB_OLD_SCHEMA_VERSION;
        LOG_DEBUG("table not exist", K(ret), K(*pcv_schema), K(table_schema));
      } else if (table_schema->is_dropped_schema()) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table is not exist", K(ret), K(table_schema));
      } else if (OB_FAIL(tmp_schema_obj.init_without_copy_name(table_schema))) {
        LOG_WARN("failed to init pcv schema obj", K(ret));
      } else if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
        LOG_WARN("failed to push back array", K(ret));
      } else {
        table_schema = nullptr;
        tmp_schema_obj.reset();
      }
    }  // for end
  }
  return ret;
}

int ObPlanCacheValue::match_dep_schema(
    const ObPlanCacheCtx& pc_ctx, const ObIArray<PCVSchemaObj>& schema_array, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  ObSQLSessionInfo* session_info = pc_ctx.sql_ctx_.session_info_;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(session_info));
  } else if (schema_array.count() != stored_schema_objs_.count()) {
    is_same = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < schema_array.count(); i++) {
      if (OB_ISNULL(stored_schema_objs_.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null table schema", K(ret), K(i), K(schema_array.at(i)), K(stored_schema_objs_.at(i)));
      } else if (schema_array.at(i).is_tmp_table_) {  // check for tmp table
        is_same = ((session_info->get_sessid_for_table() == sessid_) &&
                   (session_info->get_sess_create_time() == sess_create_time_));
      } else if (share::is_oracle_mode() && TABLE_SCHEMA == stored_schema_objs_.at(i)->schema_type_ &&
                 !stored_schema_objs_.at(i)->match_compare(schema_array.at(i))) {
        is_same = false;
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

// used for add plan
int ObPlanCacheValue::get_all_dep_schema(
    ObSchemaGetterGuard& schema_guard, const DependenyTableStore& dep_schema_objs, ObIArray<PCVSchemaObj>& schema_array)
{
  int ret = OB_SUCCESS;
  schema_array.reset();
  const ObSimpleTableSchemaV2* table_schema = nullptr;
  PCVSchemaObj tmp_schema_obj;

  for (int64_t i = 0; OB_SUCC(ret) && i < dep_schema_objs.count(); i++) {
    if (TABLE_SCHEMA != dep_schema_objs.at(i).get_schema_type()) {
      if (OB_FAIL(tmp_schema_obj.init_with_version_obj(dep_schema_objs.at(i)))) {
        LOG_WARN("failed to init pcv schema obj", K(ret));
      } else if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
        LOG_WARN("failed to push back pcv schema obj", K(ret));
      } else {
        tmp_schema_obj.reset();
      }
    } else if (OB_FAIL(schema_guard.get_table_schema(dep_schema_objs.at(i).get_object_id(), table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(dep_schema_objs.at(i)));
    } else if (nullptr == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get an unexpected null table schema", K(ret));
    } else if (table_schema->is_index_table() || table_schema->is_dropped_schema()) {
      // do nothing
    } else if (OB_FAIL(tmp_schema_obj.init_without_copy_name(table_schema))) {
      LOG_WARN("failed to init pcv schema obj", K(ret));
    } else if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
      LOG_WARN("failed to push back pcv schema obj", K(ret));
    } else {
      table_schema = nullptr;
      tmp_schema_obj.reset();
    }
  }

  if (OB_FAIL(ret)) {
    schema_array.reset();
  } else {
    LOG_DEBUG("get all dep schema", K(schema_array));
  }
  return ret;
}

int ObPlanCacheValue::need_check_schema_version(ObPlanCacheCtx& pc_ctx, int64_t& new_schema_version, bool& need_check)
{
  int ret = OB_SUCCESS;
  need_check = false;
  if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_schema_version(
          pc_ctx.sql_ctx_.session_info_->get_effective_tenant_id(), new_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret));
  } else {
    int64_t cached_tenant_schema_version = ATOMIC_LOAD(&tenant_schema_version_);
    need_check = ((new_schema_version != cached_tenant_schema_version) || is_contain_tmp_tbl() ||
                  is_contain_sys_pl_object() || contain_sys_name_table_);
    if (need_check && REACH_TIME_INTERVAL(10000000)) {
      LOG_INFO("need check schema",
          K(new_schema_version),
          K(cached_tenant_schema_version),
          K(contain_sys_name_table_),
          K(is_contain_tmp_tbl()),
          K(is_contain_sys_pl_object()),
          K(need_check),
          K(constructed_sql_));
    }
  }
  return ret;
}

int ObPlanCacheValue::lift_tenant_schema_version(int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  if (new_schema_version <= tenant_schema_version_) {
    // do nothing
  } else {
    ATOMIC_STORE(&(tenant_schema_version_), new_schema_version);
  }
  return ret;
}

int ObPlanCacheValue::rm_space_for_neg_num(ParseNode* param_node, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t pos = 0;
  int64_t idx = 0;
  if (param_node->str_len_ <= 0) {
    // do nothing
  } else if ('-' != param_node->str_value_[idx]) {
    // 'select - 1.2 from dual' and 'select 1.2 from dual' will hit the same plan, the key is
    // select ? from dual, so '- 1.2' and '1.2' will all go here, if '-' is not presented,
    // do nothing
    LOG_TRACE("rm space for neg num", K(idx), K(ObString(param_node->str_len_, param_node->str_value_)));
  } else if (OB_ISNULL(buf = (char*)allocator.alloc(param_node->str_len_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator memory", K(ret), K(param_node->str_len_));
  } else {
    buf[pos++] = '-';
    idx += 1;
    for (; idx < param_node->str_len_ && isspace(param_node->str_value_[idx]); idx++)
      ;
    int32_t len = (int32_t)(param_node->str_len_ - idx);
    if (len > 0) {
      MEMCPY(buf + pos, param_node->str_value_ + idx, len);
    }
    pos += len;
    param_node->str_value_ = buf;
    param_node->str_len_ = pos;

    LOG_DEBUG("rm space for neg num", K(idx), K(ObString(param_node->str_len_, param_node->str_value_)));
  }
  return ret;
}

}  // end of namespace sql
}  // end of namespace oceanbase
