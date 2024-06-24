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

#define USING_LOG_PREFIX PL
#include "ob_dbms_limit_calculator_mysql.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_all_server_tracer.h"
#include "share/resource_limit_calculator/ob_resource_limit_calculator.h"
#include "share/ob_unit_table_operator.h"
#include "share/ob_all_server_tracer.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"//ObGetTenantResProxy
#include "share/ls/ob_ls_status_operator.h"//ObLSStatusOperator
#include "share/balance/ob_balance_job_table_operator.h"//balance_job
#include "rootserver/ob_tenant_balance_service.h"//gather_stat_primary_zone_num_and_units

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::pl;
using namespace oceanbase::sql;


namespace oceanbase
{
namespace pl
{
int ObDBMSLimitCalculator::phy_res_calculate_by_logic_res(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_RES_LEN = 512;
  char* ptr = NULL;
  int64_t pos = 0;
  ObString str_arg;
  ObUserResourceCalculateArg arg;
  ObMinPhyResourceResult res;
  const int64_t curr_tenant_id = MTL_ID();
  if (!is_sys_tenant(curr_tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("only sys tenant can do this", K(ret), K(curr_tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Only sys tenant can do this. Operator is");
  } else if (OB_UNLIKELY(2 > params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params not valid", KR(ret), K(params));
  } else if (OB_FAIL(params.at(0).get_varchar(str_arg))) {
    LOG_WARN("get parameter failed", K(ret));
  } else if (str_arg.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(str_arg));
  } else if (OB_ISNULL(ptr = static_cast<char *>(ctx.get_allocator().alloc(MAX_RES_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(MAX_RES_LEN));
  } else if (OB_FAIL(parse_dict_like_args_(to_cstring(str_arg), arg))) {
    LOG_WARN("parse argument failed", K(ret));
  } else if (OB_FAIL(MTL(ObResourceLimitCalculator *)->get_tenant_min_phy_resource_value(arg, res))) {
    LOG_WARN("get tenant min physical resource needed failed", K(ret));
  } else if (OB_FAIL(get_json_result_(res, ptr, MAX_RES_LEN, pos))) {
    LOG_WARN("get json result failed", K(ret), K(res), K(pos));
  } else {
    params.at(1).set_varchar(ptr, pos);
    LOG_INFO("phy_res_calculate_by_logic_res success", K(arg), K(res),
             K(str_arg), K(params.at(0).get_varchar()), K(params.at(1).get_varchar()));
  }
  return ret;
}

int ObDBMSLimitCalculator::phy_res_calculate_by_unit(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  int64_t tenant_id = 0;
  ObString addr_str;
  const int64_t MAX_RES_LEN = 2048;
  ObMinPhyResourceResult res;
  char* ptr = NULL;
  int64_t pos = 0;
  int64_t timeout = -1;
  bool is_active = false;
  const int64_t curr_tenant_id = MTL_ID();
  if (!is_sys_tenant(curr_tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("only sys tenant can do this", K(ret), K(curr_tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Only sys tenant can do this. Operator is");
  } else if (OB_UNLIKELY(3 > params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params not valid", KR(ret), K(params));
  } else if (FALSE_IT(tenant_id = params.at(0).get_int())) {
  } else if (FALSE_IT(addr_str = params.at(1).get_varchar())) {
  } else if (OB_FAIL(addr.parse_from_string(addr_str))) {
    LOG_WARN("parse address failed", K(ret), K(addr_str));
  } else if (OB_FAIL(SVR_TRACER.check_server_alive(addr, is_active))) {
    LOG_WARN("check server active failed", K(ret), K(addr));
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_active = false;
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_active) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "calculate physical resource needed by unit. "
                                        "The observer is not active or not in cluster.");
  } else if (OB_ISNULL(ptr = static_cast<char *>(ctx.get_allocator().alloc(MAX_RES_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(MAX_RES_LEN));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc_proxy or session is null", K(ret), K(GCTX.srv_rpc_proxy_));
  } else if (0 >= (timeout = THIS_WORKER.get_timeout_remain())) {
    ret = OB_TIMEOUT;
    LOG_WARN("query timeout is reached", K(ret), K(timeout));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(addr)
                     .timeout(timeout)
                     .by(tenant_id)
                     .phy_res_calculate_by_unit(tenant_id, res))) {
    LOG_WARN("failed to update local stat cache caused by unknow error",
             K(ret), K(addr), K(timeout), K(tenant_id));
    // rewrite the error code to make user recheck the argument and retry.
    // we should never retry here because there is something error.
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "calculate physical resource needed by unit. "
                                        "Please check the tenant id and observer address and retry.");
  } else if (OB_FAIL(get_json_result_(tenant_id, addr, res, ptr, MAX_RES_LEN, pos))) {
    LOG_WARN("get json result failed", K(ret), K(addr), K(res), K(pos), K(MAX_RES_LEN));
  } else {
    params.at(2).set_varchar(ptr, pos);
    LOG_INFO("phy_res_calculate_by_unit success", K(params.at(0).get_int()),
             K(params.at(1).get_varchar()), K(params.at(2).get_varchar()));
  }
  return ret;
}

int ObDBMSLimitCalculator::phy_res_calculate_by_standby_tenant(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  const int64_t curr_tenant_id = MTL_ID();
  if (!is_sys_tenant(curr_tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("only sys tenant can do this", K(ret), K(curr_tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Only sys tenant can do this. Operator is");
  } else if (OB_UNLIKELY(3 > params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params not valid", KR(ret), K(params));
  } else {
    const uint64_t tenant_id = params.at(0).get_int();
    const int64_t standby_unit_num = params.at(1).get_int();
    ObUserResourceCalculateArg arg;
    ObMinPhyResourceResult res;
    const int64_t MAX_RES_LEN = 512;
    int64_t pos = 0;
    char *ptr = NULL;
    if (OB_FAIL(cal_tenant_logical_res_for_standby_(tenant_id, standby_unit_num, arg))) {
      LOG_WARN("failed to calculate logical resource", KR(tenant_id), K(standby_unit_num));
    } else if (OB_ISNULL(MTL(ObResourceLimitCalculator *))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("resource limit calculator is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(MTL(ObResourceLimitCalculator *)->get_tenant_min_phy_resource_value(arg, res))) {
      LOG_WARN("get tenant min physical resource needed failed", K(ret), K(arg));
    } else if (OB_ISNULL(ptr = static_cast<char *>(ctx.get_allocator().alloc(MAX_RES_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(MAX_RES_LEN));
    } else if (OB_FAIL(get_json_result_(res, ptr, MAX_RES_LEN, pos))) {
      LOG_WARN("get json result failed", K(ret), K(res), K(pos));
    } else {
      // params: 0,1,2: primary_tenant_id, standby_tenant_unit_num, res
      params.at(2).set_varchar(ptr, pos);
    }
    LOG_DEBUG("phy_res_calculate_by_standby_tenant", K(params.at(0).get_int()),
              K(params.at(1).get_int()), K(params.at(2).get_varchar()));
  }
  return ret;
}

int ObDBMSLimitCalculator::cal_tenant_logical_res_for_standby_(
    const uint64_t tenant_id,
    const int64_t standby_unit_num,
    ObUserResourceCalculateArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || 0 >= standby_unit_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(standby_unit_num));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "calculate physical resource needed by standby tenant. "
                   "The tenant is not a user tenant, or the unit number is invalid. Please recheck it and retry.");
  } else {
    //first get primary tenant unit_group
    ObArray<ObAddr> servers;
    common::ObArray<obrpc::ObTenantLogicalRes> resources;
    int64_t logical_resource_cnt = 0;
    if (OB_FAIL(get_tenant_resource_server_for_calc_(tenant_id, servers))) {
      LOG_WARN("failed to get tenant resource server", KR(ret), K(tenant_id));
    } else if (OB_FAIL(get_server_resource_info_(servers, tenant_id, resources))) {
      //get each unit group logical resource
      LOG_WARN("failed to get server resource info", KR(ret), K(servers), K(tenant_id));
    } else if (OB_FAIL(check_server_resource_(tenant_id, resources))) {
      LOG_WARN("failed to check and sort resource", KR(ret), K(tenant_id), K(resources));
    } else if (resources.count() != servers.count() || 0 >= resources.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("resource count not match", KR(ret), K(resources), K(servers));
    } else {
      logical_resource_cnt = resources.at(0).get_arg().count();
    }

    //calculate max logical resource for standby
    if (OB_SUCC(ret)) {
      //如果备库的unit_num大于主库的unit_num，那最多备库的一台机器只需要承担主库一台机器的容量
      //小于则向上取整。
      //获取主库机器上最大server_num个数的资源
      int64_t server_num = standby_unit_num >= servers.count() ? 1
        : ceil(double(servers.count()) / standby_unit_num);
      int64_t each_server_max_ls_cnt = 0;
      int64_t max_ls_cnt = 0;
      if (FAILEDx(get_max_ls_count_of_server_(tenant_id, each_server_max_ls_cnt))) {
        LOG_WARN("failed to get max ls count of server", KR(ret), K(tenant_id));
      } else {
        //需要增加一个系统日志流的个数
        max_ls_cnt = each_server_max_ls_cnt * server_num + 1;
      }
      //选取每一种逻辑资源的TOP n 相加
      for (int64_t i = 1; OB_SUCC(ret) && i < logical_resource_cnt; ++i) {
        int64_t max_value = 0;
        if (OB_FAIL(get_max_value_of_logical_res_(i, server_num, resources, max_value))) {
          LOG_WARN("failed to get max value of logical res", KR(ret), K(i), K(server_num), K(resources));
        } else if (LOGIC_RESOURCE_LS == i) {
          max_value = max(max_value, max_ls_cnt);
        }
        if (FAILEDx(arg.set_type_value(i, max_value))) {
          LOG_WARN("failed to set type value", KR(ret), K(i), K(max_value));
        } else {
          LOG_INFO("set type value", K(i), K(max_value), K(server_num),
              K(standby_unit_num), K(max_ls_cnt));
        }
      }//end for check each resource
    }
  }
  return ret;
}

int ObDBMSLimitCalculator::get_max_value_of_logical_res_(
    const int64_t &logical_type,
    const int64_t &server_cnt,
    const common::ObIArray<obrpc::ObTenantLogicalRes> &res,
    int64_t &max_logical_value)
{
  int ret = OB_SUCCESS;
  max_logical_value = 0;
  if (OB_UNLIKELY(0 >= logical_type || logical_type >= MAX_LOGIC_RESOURCE
        || 0 >= res.count() || 0 >= server_cnt || server_cnt > res.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(logical_type), K(server_cnt), K(res));
  } else {
    ObArray<int64_t> tmp_logical_resource;
    const int64_t primary_server_cnt = res.count();
    for (int64_t j = 0; OB_SUCC(ret) && j < primary_server_cnt; ++j) {
      const ObTenantLogicalRes &server_res = res.at(j);
      int64_t value = 0;
      if (OB_FAIL(server_res.get_arg().get_type_value(logical_type, value))) {
        LOG_WARN("failed to get type value", KR(ret), K(logical_type));
      } else if (OB_FAIL(tmp_logical_resource.push_back(value))) {
        LOG_WARN("failed to push back", KR(ret), K(value));
      }
    }//end for j for get each logical resource of servers
    if (OB_SUCC(ret)) {
      lib::ob_sort(tmp_logical_resource.begin(), tmp_logical_resource.end());
      int64_t index = 0;//下标
      while (OB_SUCC(ret) && index < server_cnt) {
        const int64_t logical_index = primary_server_cnt - 1 - index;
        if (0 > logical_index || logical_index >= tmp_logical_resource.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("logical index not expected", KR(ret), K(logical_index),
              K(primary_server_cnt), K(index), K(tmp_logical_resource));
        } else {
          max_logical_value += tmp_logical_resource.at(logical_index);
        }
        index ++;
      }//end while for get max_value
    }
  }
  return ret;
}

int ObDBMSLimitCalculator::get_tenant_resource_server_for_calc_(
    const uint64_t tenant_id,
    common::ObIArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.server_tracer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.server_tracer_));
  } else {
    common::ObArray<ObUnit> units;
    ObArray<uint64_t> unit_group_ids;
    ObArray<uint64_t> server_unit_group_ids;
    ObUnitTableOperator unit_op;
    if (OB_FAIL(unit_op.init(*GCTX.sql_proxy_))) {
      LOG_WARN("failed to init unit op", KR(ret));
    } else if (OB_FAIL(unit_op.get_units_by_tenant(tenant_id, units))) {
      LOG_WARN("failed to get tenant units", KR(ret), K(tenant_id));
    } else if (0 == units.count()) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant no exist", KR(ret), K(tenant_id));
    } else {
      bool is_alive = false;
      int64_t trace_time = 0;//no use
      for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
        const ObUnit &unit = units.at(i);
        if (!unit.is_active_status()) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("unit is deleting, can not calculate resource", KR(ret), K(unit));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant is shrinking units. Operation is");
        } else if (unit.migrate_from_server_.is_valid()) {
          //不用来计算，也不报错，后面会校验是否有足够的机器来校验
          LOG_WARN("unit is migrate, can not calculate resource", KR(ret), K(unit));
        } else if (OB_FAIL(GCTX.server_tracer_->is_alive(unit.server_, is_alive, trace_time))) {
          LOG_WARN("failed to check server is alive", KR(ret), K(unit));
        } else if (!is_alive) {
          LOG_WARN("server is not alive", KR(ret), K(unit));
        } else if (has_exist_in_array(server_unit_group_ids, unit.unit_group_id_)) {
          //相同的unit_group_id不去检查了，只挑第一个可用的机器
        } else if (OB_FAIL(server_unit_group_ids.push_back(unit.unit_group_id_))) {
          LOG_WARN("failed to push back unit group id", KR(ret), K(unit));
        } else if (OB_FAIL(servers.push_back(unit.server_))) {
          LOG_WARN("failed to push back", KR(ret), K(unit));
        }
        if (OB_SUCC(ret)) {
          if (!has_exist_in_array(unit_group_ids, unit.unit_group_id_)) {
            if (OB_FAIL(unit_group_ids.push_back(unit.unit_group_id_))) {
               LOG_WARN("failed to push back", KR(ret), K(unit));
            }
          }
        }
      }//end for
      if (OB_SUCC(ret) && servers.count() != unit_group_ids.count()) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("not enough server to calculate resource", KR(ret), K(servers), K(unit_group_ids));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "All zones have units in migrating or have inactive servers. Operation is");
      }
    }
  }
  return ret;
}

int ObDBMSLimitCalculator::get_server_resource_info_(
    const common::ObIArray<ObAddr> &servers,
    const uint64_t tenant_id,
    common::ObIArray<obrpc::ObTenantLogicalRes> &resource_res)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || 0 >= servers.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(servers));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.server_tracer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.server_tracer_));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else {
    rootserver::ObGetTenantResProxy proxy(*GCTX.srv_rpc_proxy_,
        &obrpc::ObSrvRpcProxy::get_tenant_logical_resource);
    obrpc::ObGetTenantResArg arg(tenant_id);
    int tmp_ret = OB_SUCCESS;
    ObArray<int> return_code_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      const int64_t timeout = ctx.get_timeout();
      const ObAddr& addr = servers.at(i);
      if (OB_FAIL(proxy.call(addr, timeout, GCONF.cluster_id, tenant_id, arg))) {
        //When constructing the server, the server's status was already checked,
        //therefore the RPC is sent here without ignoring the error code
        LOG_WARN("failed to send rpc", KR(ret), K(addr), K(timeout), K(tenant_id));
      }
    }
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      // overwrite ret
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        ret = return_code_array.at(i);
        const obrpc::ObTenantLogicalRes *res = proxy.get_results().at(i);
        if (OB_FAIL(ret)) {
          LOG_WARN("send rpc is failed", KR(ret), K(i));
        } else if (OB_ISNULL(res)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(i));
        } else if (OB_FAIL(resource_res.push_back(*res))) {
          LOG_WARN("failed to push back", KR(ret), KPC(res));
        } else {
          LOG_INFO("success to get server resource", KPC(res));
        }
      }
    }
  }
  return ret;
}

int ObDBMSLimitCalculator::check_server_resource_(
    const uint64_t tenant_id,
    common::ObIArray<obrpc::ObTenantLogicalRes> &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || 0 >= res.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(res));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.server_tracer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.server_tracer_));
  } else {
    share::ObBalanceJob balance_job;
    int64_t start_time = 0;
    int64_t finish_time = 0;//no use
    if (OB_FAIL(share::ObBalanceJobTableOperator::get_balance_job(tenant_id,
            false, *GCTX.sql_proxy_, balance_job, start_time, finish_time))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get balance job", KR(ret), K(tenant_id));
      }
    } else {
      //当前租户有分区均衡或者日志流均衡任务，计算结果存在不准确的可能性，所以只告警，但是不报错
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("tenant has balance job", KR(ret), K(tenant_id), K(balance_job));
      LOG_USER_WARN(OB_OP_NOT_ALLOW, "Tenant is doing balance job. Operation is");
    }
  }
  if (OB_SUCC(ret)) {
    share::ObLSStatusOperator ls_op;
    ObLSStatusInfoArray ls_array;

    if (OB_FAIL(ls_op.get_all_ls_status_by_order(tenant_id, ls_array, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get all ls status", KR(ret), K(tenant_id));
    } else {
      //校验日志流个数是匹配的
      int64_t ls_count = 0;
      int64_t type = LOGIC_RESOURCE_LS;
      for (int64_t i = 0; OB_SUCC(ret) && i < res.count(); ++i) {
        const ObTenantLogicalRes &server_res = res.at(i);
        int64_t tmp_value = 0;
        if (OB_FAIL(server_res.get_arg().get_type_value(type, tmp_value))) {
          LOG_WARN("failed to get type value", KR(ret), K(type), K(i), K(server_res));
        } else {
          ls_count += tmp_value;
        }
      }//end for
      if (OB_SUCC(ret) && ls_array.count() > ls_count) {
        //由于可能存在迁移，创建或者GC中的日志流，这里的内部表和实际的日志流个数
        //都有概率匹配不上，只希望报错的是日志流缺副本的场景，但是没有办法区分
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("ls replica not enough", KR(ret), K(ls_count), K(ls_array), K(res));
        LOG_USER_WARN(OB_OP_NOT_ALLOW, "Insufficient number of LS. Operation is");
      }
    }
  }
  if (OB_OP_NOT_ALLOW == ret) {
    //为了保证可用性，在计算不一定准确的时候，告警但是允许执行
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDBMSLimitCalculator::get_max_ls_count_of_server_(
    const uint64_t tenant_id,
    int64_t &ls_count)
{
  int ret = OB_SUCCESS;
  int64_t primary_zone_num = 0;
  int64_t unit_group_num = 0;
  ObArray<share::ObSimpleUnitGroup> unit_group_array;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(rootserver::ObTenantBalanceService::gather_stat_primary_zone_num_and_units(
          tenant_id, primary_zone_num, unit_group_array))) {
    LOG_WARN("failed to gather stat of primary zone and unit", KR(ret), K(tenant_id));
  } else {
    unit_group_num = unit_group_array.count();
  }
  if (OB_SUCC(ret)) {
    //假如Unit个数为N，Primary Zone个数为P
    //稳态用户日志流个数：U = N*P,
    //考虑日志流膨胀场景，分区均衡过程中，每个日志流都会和其他所有日志流进行Transfer，需要生成新日志流.
    //这个过程中需要考虑排除掉一个Unit上日志流之间Transfer的场景，因为一个Unit里面日志流之间Transfer不需要生成新的日志流。
    //所以，额外膨胀的日志流个数是：
    //1. 每一个日志流会和其他所有日志流（包括自己）生成一个新日志流，得出的结果是：U * U
    //2. 第一步的计算结果是有冗余的：每个Unit内部有P个日志流，那么每个日志流在和P个日志流Transfer过程中不会生成新日志流，所以冗余的个数是：U * P
    //另外，4.2.3上引入了广播日志流之间的Transfer功能，所以广播日志流也会膨胀。
    //1. 广播日志流之间Transfer不需要生成新日志流
    //2. 广播日志流Transfer到用户日志流，也不需要生成新日志流
    //3. 用户日志流Transfer到广播日志流，需要生成一个新的广播日志流
    //所以，广播日志流膨胀个数等于用户日志流的稳态个数 U
    //不考虑系统日志流，一台机器上日志流的总数为
    //U + 1 + 用户日志流膨胀个数 + 广播日志流膨胀个数 = U + 2 + U*U - U*P + U = U*U - U*(P-2) + 1
    //由于广播日志流实际上是在每台机器上都存在的资源，所以最终每台机器上日志流的个数为
    //NPP-PP+2P+1
    ls_count = (unit_group_num - 1) * primary_zone_num * primary_zone_num + 2 * primary_zone_num + 1;
  }
  return ret;
}

int ObDBMSLimitCalculator::parse_dict_like_args_(
    const char* ptr,
    ObUserResourceCalculateArg &arg)
{
  int ret = OB_SUCCESS;
  char key[50] = "";
  int64_t value = 0;
  // parse the argument like: "ls: 1, tablet: 2, xxxx"
  while (OB_SUCC(ret) && sscanf(ptr, "%[^:]: %ld", key, &value) == 2) {
    int64_t type = get_logic_res_type_by_name(key);
    if (!is_valid_logic_res_type(type)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(type), K(key));
    } else if (OB_FAIL(arg.set_type_value(type, value))) {
      LOG_WARN("set type value failed", K(ret), K(type), K(value));
    }
    while (*ptr != '\0' && *ptr != ',') ptr++;
    while (*ptr != '\0' && (*ptr == ' ' || *ptr == ',')) ptr++;
  }
  return ret;
}

int ObDBMSLimitCalculator::get_json_result_(
    const ObMinPhyResourceResult &res,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t i = PHY_RESOURCE_MEMSTORE;
  int64_t value = 0;
  if (OB_FAIL(res.get_type_value(i, value))) {
    LOG_WARN("get_type_value failed", K(ret), K(get_phy_res_type_name(i)), K(value));
  } else if (OB_FAIL(databuff_printf(buf,
                                     buf_len,
                                     pos,
                                     "[{\"physical_resource_name\": \"%s\", \"min_value\": \"%ld\"}",
                                     get_phy_res_type_name(i),
                                     value))) {
    LOG_WARN("get result buffer failed", K(ret), K(pos), K(buf_len));
  } else {
    // get next type
    i++;
    for (; OB_SUCC(ret) && i < MAX_PHY_RESOURCE; i++) {
      if (OB_FAIL(res.get_type_value(i, value))) {
        LOG_WARN("get_type_value failed", K(ret), K(get_phy_res_type_name(i)), K(value));
      } else if (OB_FAIL(databuff_printf(buf,
                                         buf_len,
                                         pos,
                                         ", {\"physical_resource_name\": \"%s\", \"min_value\": \"%ld\"}",
                                         get_phy_res_type_name(i),
                                         value))) {
        LOG_WARN("get result buffer failed", K(ret), K(pos), K(buf_len));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "]"))) {
        LOG_WARN("get result buffer failed", K(ret), K(pos), K(buf_len));
      }
    }
  }
  return ret;
}

int ObDBMSLimitCalculator::get_json_result_(
    const int64_t tenant_id,
    const ObAddr &addr,
    const ObMinPhyResourceResult &res,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  char ip[MAX_IP_ADDR_LENGTH] = "";
  int64_t port = 0;
  int64_t value = 0;
  int64_t i = PHY_RESOURCE_MEMSTORE;
  if (!addr.ip_to_string(ip, MAX_IP_ADDR_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ip string failed", K(ret), K(addr));
  } else if (FALSE_IT(port = addr.get_port())) {
  } else if (OB_FAIL(res.get_type_value(i, value))) {
    LOG_WARN("get_type_value failed", K(ret), K(get_phy_res_type_name(i)), K(value));
  } else if (OB_FAIL(databuff_printf(buf,
                                     buf_len,
                                     pos,
                                     "[{\"svr_ip\": \"%s\", \"svr_port\": \"%ld\", \"tenant_id\" : \"%ld\", \"physical_resource_name\": \"%s\", \"min_value\": \"%ld\"}",
                                     ip,
                                     port,
                                     tenant_id,
                                     get_phy_res_type_name(i),
                                     value))) {
    LOG_WARN("get result buffer failed", K(ret), K(pos), K(buf_len));
  } else {
    // get next type
    i++;
    for (; OB_SUCC(ret) && i < MAX_PHY_RESOURCE; i++) {
      if (OB_FAIL(res.get_type_value(i, value))) {
        LOG_WARN("get_type_value failed", K(ret), K(get_phy_res_type_name(i)), K(value));
      } else if (OB_FAIL(databuff_printf(buf,
                                         buf_len,
                                         pos,
                                         ", {\"svr_ip\": \"%s\", \"svr_port\": \"%ld\", \"tenant_id\" : \"%ld\", \"physical_resource_name\": \"%s\", \"min_value\": \"%ld\"}",
                                         ip,
                                         port,
                                         tenant_id,
                                         get_phy_res_type_name(i),
                                         value))) {
        LOG_WARN("get result buffer failed", K(ret), K(pos), K(buf_len));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "]"))) {
        LOG_WARN("get result buffer failed", K(ret), K(pos), K(buf_len));
      }
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
