/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL
#include "ob_dbms_lob_manager.h"
#include "share/table/ob_ttl_util.h"
#include "observer/ob_server_struct.h"
#include "pl/ob_pl.h"
#include "sql/ob_sql.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_srv_network_frame.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_parse.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/lob/lob_consistency_check/ob_lob_consistency_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "share/ob_lob_check_job_scheduler.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase {
namespace pl {
using namespace common;

// 辅助函数：检查是否存在LOB校验或恢复任务
int ObDbmsLobManager::check_lob_task_exists(uint64_t tenant_id, common::ObISQLClient &sql_proxy, bool &task_exists,
                                          int64_t table_id)
{
  int ret = OB_SUCCESS;
  task_exists = false;
  ObSqlString sql;
  SMART_VAR(ObISQLClient::ReadResult, res)
  {
    sqlclient::ObMySQLResult *result = nullptr;

    // 查询是否存在LOB校验任务（table_id = -2）或LOB恢复任务（table_id = -3）
    if (OB_FAIL(sql.assign_fmt("SELECT COUNT(*) as cnt FROM %s WHERE tenant_id = %ld AND table_id = %ld",
                               share::OB_ALL_KV_TTL_TASK_TNAME, tenant_id, table_id))) {
      LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next row", KR(ret));
    } else {
      int64_t cnt = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
      if (OB_SUCC(ret)) {
        task_exists = (cnt > 0);
      }
    }
  }

  return ret;
}

int ObDbmsLobManager::check_resource_manager_plan_exists(uint64_t tenant_id,
                                                       common::ObISQLClient &sql_proxy,
                                                       bool &resource_manager_exists)
{
  int ret = OB_SUCCESS;
  resource_manager_exists = false;
  ObSqlString sql;
  SMART_VAR(ObISQLClient::ReadResult, res)
  {
    sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt("SELECT COUNT(*) as cnt FROM %s WHERE CONSUMER_GROUP = 'LOB_CHECK'",
                               share::OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME))) {
      LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next row", KR(ret));
    } else {
      int64_t cnt = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
      if (OB_SUCC(ret)) {
        resource_manager_exists = (cnt > 0);
      }
    }
  }
  return ret;
}

// 辅助函数：解析table_ids的JSON数组（格式：[500001, 500003]）
int ObDbmsLobManager::parse_table_ids_json(ObIAllocator &allocator, ObObj &json_obj, ObJsonBuffer &table_ids_json)
{
  int ret = OB_SUCCESS;
  ObJsonNode *json_tree = nullptr;
  ObJsonInType in_type = json_obj.is_json() ? ObJsonInType::JSON_BIN : ObJsonInType::JSON_TREE;
  ObString j_str;
  if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, json_obj, j_str))) {
    LOG_WARN("fail to read real string data", K(ret), K(json_obj));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_tree(&allocator, j_str, in_type, json_tree))) {
    LOG_WARN("fail to parse json", KR(ret), K(j_str));
  } else if (OB_ISNULL(json_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json tree is null", KR(ret));
  } else if (json_tree->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json is not an array", KR(ret), K(j_str));
  } else {
    ObJsonArray *json_array = static_cast<ObJsonArray *>(json_tree);
    uint64_t array_size = json_array->element_count();
    if (array_size == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table_ids is empty", KR(ret));
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < array_size; ++i) {
      ObJsonNode *element = json_array->get_value(i);
      if (OB_ISNULL(element)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array element is null", KR(ret), K(i));
      } else if (element->json_type() != ObJsonNodeType::J_INT && element->json_type() != ObJsonNodeType::J_UINT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("array element is not an integer", KR(ret), K(i), K(element->json_type()));
      } else {
        uint64_t table_id = 0;
        if (element->json_type() == ObJsonNodeType::J_INT) {
          int64_t val = static_cast<ObJsonInt *>(element)->value();
          if (val < 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("table_id cannot be negative", KR(ret), K(val));
          } else {
            table_id = static_cast<uint64_t>(val);
          }
        } else {
          table_id = static_cast<ObJsonUint *>(element)->value();
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObJsonBinSerializer::serialize_json_value(json_array, table_ids_json))) {
        LOG_WARN("serialize json tree fail", K(ret));
      }
    }
  }

  return ret;
}

// 辅助函数：解析tablet_ids的JSON数组（格式：[200001, 200002]）
int ObDbmsLobManager::push_table_with_tablet(ObIAllocator &allocator, int64_t table_id, ObIJsonBase *tablet_ids_tree,
                                           ObJsonObject &table_with_tablet_obj)
{
  int ret = OB_SUCCESS;
  ObString table_id_str;
  uint64_t *table_id_buf = nullptr;
  if (OB_ISNULL(tablet_ids_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json tree is null", KR(ret));
  } else if (OB_ISNULL(table_id_buf = static_cast<uint64_t *>(allocator.alloc(sizeof(uint64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret));
  } else if (FALSE_IT(*table_id_buf = table_id)) {
  } else if (FALSE_IT(table_id_str.assign_ptr(reinterpret_cast<char *>(table_id_buf), sizeof(uint64_t)))) {
  } else if (tablet_ids_tree->json_type() == ObJsonNodeType::J_NULL) {
    if (OB_FAIL(table_with_tablet_obj.add(table_id_str, static_cast<ObJsonNode *>(tablet_ids_tree)))) {
      LOG_WARN("fail to add table_id to json object", KR(ret));
    }
  } else if (tablet_ids_tree->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json is not an array", KR(ret), K(tablet_ids_tree->json_type()));
  } else {
    ObJsonArray *tablet_ids_array = static_cast<ObJsonArray *>(tablet_ids_tree);
    uint64_t array_size = tablet_ids_array->element_count();
    if (array_size == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tablet_ids is empty", KR(ret));
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < array_size; ++i) {
      ObJsonNode *element = tablet_ids_array->get_value(i);
      if (OB_ISNULL(element)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array element is null", KR(ret), K(i));
      } else if (element->json_type() != ObJsonNodeType::J_INT && element->json_type() != ObJsonNodeType::J_UINT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("array element is not an integer", KR(ret), K(i), K(element->json_type()));
      } else {
        uint64_t tablet_id = 0;
        if (element->json_type() == ObJsonNodeType::J_INT) {
          int64_t val = static_cast<ObJsonInt *>(element)->value();
          if (val < 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("tablet_id cannot be negative", KR(ret), K(val));
          } else {
            tablet_id = static_cast<uint64_t>(val);
          }
        } else {
          tablet_id = static_cast<ObJsonUint *>(element)->value();
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(table_with_tablet_obj.add(table_id_str, static_cast<ObJsonNode *>(tablet_ids_tree)))) {
      LOG_WARN("fail to add table_id to json object", KR(ret));
    }
  }
  return ret;
}

// check_lob procedure begin
int ObDbmsLobManager::check_lob(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  const int64_t PARAM_NUM = 1;
  const int64_t TABLE_IDS_INDEX = 0;
  ObString table_ids_json;
  ObTTLParam param;
  param.reset();
  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  if (OB_UNLIKELY(PARAM_NUM != params.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: invalid parameters", K(ret), K(params.count()));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();

    // 检查是否已存在LOB校验或恢复任务
    bool task_exists = false;
    bool resource_manager_exists = false;
    if (tenant_id == OB_SYS_TENANT_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("lob check task is not supported in sys tenant", KR(ret), K(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "lob check task in sys tenant");
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret));
    } else if (OB_FAIL(check_resource_manager_plan_exists(tenant_id, *GCTX.sql_proxy_, resource_manager_exists))) {
      LOG_WARN("fail to check resource manager plan exists", KR(ret), K(tenant_id));
    } else if (!resource_manager_exists) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Resource manager plan LOB_CHECK does not exist, CHECK_LOB task");
      LOG_WARN("resource manager plan LOB_CHECK does not exist, please create it first", KR(ret), K(tenant_id));
    } else if (OB_FAIL(check_lob_task_exists(tenant_id, *GCTX.sql_proxy_, task_exists,
                                             ObLobConsistencyUtil::LOB_CHECK_TENANT_TASK_TABLE_ID))) {
      LOG_WARN("fail to check lob task exists", KR(ret), K(tenant_id));
    } else if (task_exists) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "CHECK_LOB task already exists");
      LOG_WARN("lob task already exists", KR(ret), K(tenant_id));
    } else {
      param.transport_ = GCTX.net_frame_->get_req_transport();
      param.type_ = obrpc::ObTTLRequestArg::LOB_CHECK_TRIGGER_TYPE;
      param.trigger_type_ = TRIGGER_TYPE::USER_TRIGGER;

      if (OB_FAIL(param.add_ttl_info(tenant_id))) {
        LOG_WARN("fail to add ttl info", KR(ret), K(tenant_id));
      } else if (!params.at(TABLE_IDS_INDEX).is_null()) {
        // 解析table_ids JSON数组
        ObJsonBuffer table_ids_json_buf(ctx.allocator_);
        // 解析JSON数组
        if (OB_FAIL(parse_table_ids_json(*ctx.allocator_, params.at(TABLE_IDS_INDEX), table_ids_json_buf))) {
          LOG_WARN("fail to parse table_ids json", KR(ret), K(params.at(TABLE_IDS_INDEX)));
        } else if (OB_FAIL(table_ids_json_buf.get_result_string(param.table_with_tablet_))) {
          LOG_WARN("fail to write json string", KR(ret), K(table_ids_json_buf));
        }
      }
      // 如果table_ids为NULL，则table_with_tablet_为空，表示扫描所有表

      if (OB_SUCC(ret)) {
        if (OB_FAIL(common::ObTTLUtil::dispatch_ttl_cmd(param))) {
          LOG_WARN("fail to dispatch ttl cmd", KR(ret), K(param));
        } else {
          LOG_INFO("check_lob success", K(tenant_id), K(param.table_with_tablet_));
        }
      }
    }
  }

  return ret;
}
// check_lob procedure end

// cancel_job procedure begin
int ObDbmsLobManager::cancel_job(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  const int64_t PARAM_NUM = 1;
  const int64_t TASK_TYPE_INDEX = 0;
  ObString task_type_str;
  obrpc::ObTTLRequestArg::TTLRequestType cmd_type = obrpc::ObTTLRequestArg::TTL_INVALID_TYPE;
  ObTTLParam param;
  param.reset();
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(PARAM_NUM != params.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: invalid parameters", K(ret), K(params.count()));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (params.at(TASK_TYPE_INDEX).is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_type cannot be null", K(ret));
  } else if (OB_FAIL(params.at(TASK_TYPE_INDEX).get_string(task_type_str))) {
    LOG_WARN("fail to get task_type", KR(ret));
  } else {
    tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();
    param.transport_ = GCTX.net_frame_->get_req_transport();
    param.trigger_type_ = TRIGGER_TYPE::USER_TRIGGER;

    if (0 == task_type_str.case_compare("check_lob")) {
      cmd_type = obrpc::ObTTLRequestArg::LOB_CHECK_CANCEL_TYPE;
    } else if (0 == task_type_str.case_compare("repair_lob")) {
      cmd_type = obrpc::ObTTLRequestArg::LOB_REPAIR_CANCEL_TYPE;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid task_type", K(ret), K(task_type_str));
    }

    if (OB_SUCC(ret)) {
      param.type_ = cmd_type;
      if (OB_FAIL(param.add_ttl_info(tenant_id))) {
        LOG_WARN("fail to add ttl info", KR(ret), K(tenant_id));
      } else if (OB_FAIL(common::ObTTLUtil::dispatch_ttl_cmd(param))) {
        LOG_WARN("fail to dispatch ttl cmd", KR(ret), K(param));
      }
    }
  }

  return ret;
}
// cancel_job procedure end

// suspend_job procedure begin
int ObDbmsLobManager::suspend_job(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  const int64_t PARAM_NUM = 1;
  const int64_t TASK_TYPE_INDEX = 0;
  ObString task_type_str;
  obrpc::ObTTLRequestArg::TTLRequestType cmd_type = obrpc::ObTTLRequestArg::TTL_INVALID_TYPE;
  ObTTLParam param;
  param.reset();
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(PARAM_NUM != params.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: invalid parameters", K(ret), K(params.count()));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (params.at(TASK_TYPE_INDEX).is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_type cannot be null", K(ret));
  } else if (OB_FAIL(params.at(TASK_TYPE_INDEX).get_string(task_type_str))) {
    LOG_WARN("fail to get task_type", KR(ret));
  } else {
    tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();
    param.transport_ = GCTX.net_frame_->get_req_transport();
    param.trigger_type_ = TRIGGER_TYPE::USER_TRIGGER;

    if (0 == task_type_str.case_compare("check_lob")) {
      cmd_type = obrpc::ObTTLRequestArg::LOB_CHECK_SUSPEND_TYPE;
    } else if (0 == task_type_str.case_compare("repair_lob")) {
      cmd_type = obrpc::ObTTLRequestArg::LOB_REPAIR_SUSPEND_TYPE;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid task_type", K(ret), K(task_type_str));
    }

    if (OB_SUCC(ret)) {
      param.type_ = cmd_type;
      if (OB_FAIL(param.add_ttl_info(tenant_id))) {
        LOG_WARN("fail to add ttl info", KR(ret), K(tenant_id));
      } else if (OB_FAIL(common::ObTTLUtil::dispatch_ttl_cmd(param))) {
        LOG_WARN("fail to dispatch ttl cmd", KR(ret), K(param));
      }
    }
  }

  return ret;
}
// suspend_job procedure end

// resume_job procedure begin
int ObDbmsLobManager::resume_job(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  const int64_t PARAM_NUM = 1;
  const int64_t TASK_TYPE_INDEX = 0;
  ObString task_type_str;
  obrpc::ObTTLRequestArg::TTLRequestType cmd_type = obrpc::ObTTLRequestArg::TTL_INVALID_TYPE;
  ObTTLParam param;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(PARAM_NUM != params.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: invalid parameters", K(ret), K(params.count()));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (params.at(TASK_TYPE_INDEX).is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_type cannot be null", K(ret));
  } else if (OB_FAIL(params.at(TASK_TYPE_INDEX).get_string(task_type_str))) {
    LOG_WARN("fail to get task_type", KR(ret));
  } else {
    tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();
    param.transport_ = GCTX.net_frame_->get_req_transport();
    param.trigger_type_ = TRIGGER_TYPE::USER_TRIGGER;

    if (0 == task_type_str.case_compare("check_lob")) {
      cmd_type = obrpc::ObTTLRequestArg::LOB_CHECK_RESUME_TYPE;
    } else if (0 == task_type_str.case_compare("repair_lob")) {
      cmd_type = obrpc::ObTTLRequestArg::LOB_REPAIR_RESUME_TYPE;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid task_type", K(ret), K(task_type_str));
    }

    if (OB_SUCC(ret)) {
      param.type_ = cmd_type;
      if (OB_FAIL(param.add_ttl_info(tenant_id))) {
        LOG_WARN("fail to add ttl info", KR(ret), K(tenant_id));
      } else if (OB_FAIL(common::ObTTLUtil::dispatch_ttl_cmd(param))) {
        LOG_WARN("fail to dispatch ttl cmd", KR(ret), K(param));
      }
    }
  }

  return ret;
}
// resume_job procedure end

int ObDbmsLobManager::get_table_id(ObObj &obj, int64_t &table_id)
{
  int ret = OB_SUCCESS;
  if (obj.get_type() == ObNumberType) {
    ObNumber number = obj.get_number();
    number.cast_to_int64(table_id);
  } else if (OB_FAIL(obj.get_int(table_id))) {
    LOG_WARN("fail to get table_id", KR(ret));
  }
  return ret;
}

// repair_lob procedure begin
// 支持两种调用方式：
// 1. call DBMS_LOB_MANAGER.REPAIR_LOB(500001, "[200001, 200002]");
// 2. call DBMS_LOB_MANAGER.REPAIR_LOB(NULL, NULL); -- 从异常结果表查询
int ObDbmsLobManager::repair_lob(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;

  ObTTLParam param;
  param.reset();
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(params.count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: invalid parameters", K(ret), K(params.count()));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    ObObj table_id = params.at(0);
    ObObj tablet_ids = params.at(1);
    tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();

    // 检查是否已存在LOB校验或恢复任务
    bool task_exists = false;
    bool resource_manager_exists = false;
    if (tenant_id == OB_SYS_TENANT_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("lob repair task is not supported in sys tenant", KR(ret), K(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "lob repair task in sys tenant");
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret));
    } else if (OB_FAIL(check_resource_manager_plan_exists(tenant_id, *GCTX.sql_proxy_, resource_manager_exists))) {
      LOG_WARN("fail to check resource manager plan exists", KR(ret), K(tenant_id));
    } else if (!resource_manager_exists) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Resource manager plan LOB_CHECK does not exist, REPAIR_LOB task");
      LOG_WARN("resource manager plan LOB_CHECK does not exist, please create it first", KR(ret), K(tenant_id));
    } else if (OB_FAIL(check_lob_task_exists(tenant_id, *GCTX.sql_proxy_, task_exists,
                                             ObLobConsistencyUtil::LOB_REPAIR_TENANT_TASK_TABLE_ID))) {
      LOG_WARN("fail to check lob task exists", KR(ret), K(tenant_id));
    } else if (task_exists) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "REPAIR_LOB task already exists");
      LOG_WARN("lob task already exists", KR(ret), K(tenant_id));
    }

    if (OB_SUCC(ret)) {
      param.transport_ = GCTX.net_frame_->get_req_transport();
      param.type_ = obrpc::ObTTLRequestArg::LOB_REPAIR_TRIGGER_TYPE;
      param.trigger_type_ = TRIGGER_TYPE::USER_TRIGGER;

      if (OB_FAIL(param.add_ttl_info(tenant_id))) {
        LOG_WARN("fail to add ttl info", KR(ret), K(tenant_id));
      } else if (table_id.is_null() && tablet_ids.is_null()) {
      } else if (table_id.is_null() && !tablet_ids.is_null()) {
        // 只填了tablet_ids，没填table_id，报错
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "table_id must be provided when tablet_ids is specified");
        LOG_WARN("table_id is required when tablet_ids is provided", KR(ret));
      } else {
        // table_id不为NULL
        int64_t table_id_value = 0;
        ObJsonBuffer tablet_ids_json_buf(ctx.allocator_);
        ObJsonObject table_with_tablet_obj(ctx.allocator_);
        ObString tablet_ids_str;
        ObJsonNode *tablet_id_tree = nullptr;
        ObJsonInType in_type = tablet_ids.is_json() ? ObJsonInType::JSON_BIN : ObJsonInType::JSON_TREE;
        if (tablet_ids.is_null()) {
          if (OB_ISNULL(tablet_id_tree = OB_NEWx(ObJsonNull, ctx.allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", KR(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(get_table_id(table_id, table_id_value))) {
          LOG_WARN("fail to get table_id", KR(ret), K(table_id.get_type()));
        } else if (table_id_value < 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("table_id is invalid", KR(ret), K(table_id_value));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "table_id is invalid");
        } else if (OB_FAIL(
                       sql::ObTextStringHelper::read_real_string_data(ctx.allocator_, tablet_ids, tablet_ids_str))) {
          LOG_WARN("fail to read real string data", KR(ret), K(tablet_ids));
        } else if (!tablet_ids.is_null() && OB_FAIL(ObJsonBaseFactory::get_json_tree(ctx.allocator_, tablet_ids_str, in_type, tablet_id_tree))) {
          LOG_WARN("fail to get tablet_ids json", KR(ret), K(tablet_ids_str));
        } else if (OB_FAIL(push_table_with_tablet(*ctx.allocator_, table_id_value, tablet_id_tree,
                                                  table_with_tablet_obj))) {
          LOG_WARN("fail to build table_with_tablet_obj", KR(ret), K(table_id_value), K(tablet_ids_str));
        } else if (OB_FAIL(ObJsonBinSerializer::serialize_json_value(static_cast<ObJsonNode *>(&table_with_tablet_obj),
                                                                     tablet_ids_json_buf))) {
          LOG_WARN("fail to serialize table_with_tablet_obj", KR(ret), K(table_with_tablet_obj));
        } else if (OB_FAIL(tablet_ids_json_buf.get_result_string(param.table_with_tablet_))) {
          LOG_WARN("fail to get result string", KR(ret), K(tablet_ids_json_buf));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(common::ObTTLUtil::dispatch_ttl_cmd(param))) {
          LOG_WARN("fail to dispatch ttl cmd", KR(ret), K(param));
        } else {
          LOG_INFO("repair_lob success", K(tenant_id), K(param.table_with_tablet_));
        }
      }
    }
  }

  return ret;
}
// repair_lob procedure end

// check_lob_inner procedure begin
int ObDbmsLobManager::check_lob_inner(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  const int64_t PARAM_NUM = 0;
  ObTTLParam param;
  param.reset();
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  LOG_INFO("check_lob_inner procedure begin", K(params));
  if (OB_UNLIKELY(PARAM_NUM != params.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: invalid parameters", K(ret), K(params.count()));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();
    param.transport_ = GCTX.net_frame_->get_req_transport();
    param.type_ = obrpc::ObTTLRequestArg::LOB_CHECK_TRIGGER_TYPE;
    param.trigger_type_ = TRIGGER_TYPE::PERIODIC_TRIGGER;  // 定时器触发
    bool resource_manager_exists = false;
    bool task_exists = false;
    if (tenant_id == OB_SYS_TENANT_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("lob check task is not supported in sys tenant", KR(ret), K(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "lob check task in sys tenant");
    } else if (OB_FAIL(param.add_ttl_info(tenant_id))) {
      LOG_WARN("fail to add ttl info", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret));
    } else if (OB_FAIL(check_resource_manager_plan_exists(tenant_id, *GCTX.sql_proxy_, resource_manager_exists))) {
      LOG_WARN("fail to check resource manager plan exists", KR(ret), K(tenant_id));
    } else if (!resource_manager_exists) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Resource manager plan LOB_CHECK does not exist, CHECK_LOB task");
      LOG_WARN("resource manager plan LOB_CHECK does not exist, please create it first", KR(ret), K(tenant_id));
    } else if (OB_FAIL(check_lob_task_exists(tenant_id, *GCTX.sql_proxy_, task_exists,
                                             ObLobConsistencyUtil::LOB_CHECK_TENANT_TASK_TABLE_ID))) {
      LOG_WARN("fail to check lob task exists", KR(ret), K(tenant_id));
    } else if (task_exists) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "CHECK_LOB task already exists");
      LOG_WARN("lob task already exists", KR(ret), K(tenant_id));
    } else if (OB_FAIL(common::ObTTLUtil::dispatch_ttl_cmd(param))) {
      LOG_WARN("fail to dispatch ttl cmd", KR(ret), K(param));
    }
  }

  return ret;
}
// check_lob_inner procedure end

// reschedule_job procedure begin
int ObDbmsLobManager::reschedule_job(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  const int64_t PARAM_NUM = 2;
  const int64_t START_DATE_INDEX = 0;
  const int64_t REPEAT_INTERVAL_INDEX = 1;
  ObString repeat_interval_str;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  bool is_oracle_mode = lib::is_oracle_mode();

  if (OB_UNLIKELY(PARAM_NUM != params.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: invalid parameters", K(ret), K(params.count()));
  } else if (OB_ISNULL(ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (params.at(START_DATE_INDEX).is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start_date cannot be null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "start_date cannot be null");
  } else if (params.at(REPEAT_INTERVAL_INDEX).is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("repeat_interval cannot be null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "repeat_interval cannot be null");
  } else if (OB_FAIL(params.at(REPEAT_INTERVAL_INDEX).get_string(repeat_interval_str))) {
    LOG_WARN("fail to get repeat_interval", KR(ret));
  } else {
    tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();

    // Check if tenant_id is valid
    if (tenant_id == OB_SYS_TENANT_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("reschedule job is not supported in sys tenant", KR(ret), K(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "reschedule job in sys tenant");
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret));
    } else {
      ObString job_name(share::LOB_CHECK_JOB_NAME);
      dbms_scheduler::ObDBMSSchedJobInfo job_info;

      if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(
              *GCTX.sql_proxy_,
              tenant_id,
              is_oracle_mode,
              job_name,
              ctx.local_expr_alloc_,
              job_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_WARN("lob_check_job does not exist", KR(ret), K(tenant_id), K(job_name));
          LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "lob_check_job does not exist");
        } else {
          LOG_WARN("fail to get dbms sched job info", KR(ret), K(tenant_id), K(job_name));
        }
      } else {
        // Get start_date timestamp
        int64_t start_date_usec = 0;
        if (is_oracle_mode) {
          start_date_usec = params.at(START_DATE_INDEX).get_int();
        } else {
          start_date_usec = params.at(START_DATE_INDEX).get_datetime();
        }

        // Validate start_date and end_date
        if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::check_is_valid_end_date(
                start_date_usec, job_info.end_date_))) {
          LOG_WARN("invalid start_date", KR(ret), K(start_date_usec), K(job_info.end_date_));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "start_date is invalid or exceeds end_date");
        } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::check_is_valid_repeat_interval(tenant_id,
                repeat_interval_str))) {
          LOG_WARN("invalid repeat_interval", KR(ret), K(repeat_interval_str));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "repeat_interval is invalid");
        } else {
          // Update start_date
          ObObj start_date_obj;
          start_date_obj.set_timestamp(start_date_usec);
          if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(
                  *GCTX.sql_proxy_, job_info, ObString("start_date"), start_date_obj))) {
            LOG_WARN("failed to update start_date", K(ret), K(tenant_id), K(job_info), K(start_date_usec));
          } else {
            // Update local job_info to prevent calculation errors
            job_info.start_date_ = start_date_usec;

            // Update repeat_interval
            ObObj repeat_interval_obj;
            repeat_interval_obj.set_string(ObVarcharType, repeat_interval_str);
            if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(
                    *GCTX.sql_proxy_, job_info, ObString("repeat_interval"), repeat_interval_obj))) {
              LOG_WARN("failed to update repeat_interval", K(ret), K(tenant_id), K(job_info), K(repeat_interval_str));
            }
          }
        }
      }
    }
  }

  return ret;
}
// reschedule_job procedure end

}  // end namespace pl
}  // namespace oceanbase