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

#define USING_LOG_PREFIX SQL_ENG

#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/cmd/ob_alter_system_executor.h"
#include "share/ob_force_print_log.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/backup/ob_backup_struct.h"
#include "observer/ob_server.h"
#include "sql/resolver/cmd/ob_bootstrap_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/rc/ob_context.h"
#include "observer/ob_server_struct.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#ifdef OB_BUILD_ARBITRATION
#include "share/arbitration_service/ob_arbitration_service_utils.h" //ObArbitrationServiceUtils
#endif
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "observer/omt/ob_tenant.h" //ObTenant
#include "rootserver/freeze/ob_major_freeze_helper.h" //ObMajorFreezeHelper
#include "share/ob_primary_standby_service.h" // ObPrimaryStandbyService
#include "rpc/obmysql/ob_sql_sock_session.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "pl/pl_cache/ob_pl_cache_mgr.h"
#include "sql/plan_cache/ob_ps_cache.h"
#include "share/restore/ob_tenant_clone_table_operator.h" //ObCancelCloneJobReason
#include "share/table/ob_ttl_util.h"
#include "rootserver/restore/ob_tenant_clone_util.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace omt;
using namespace obmysql;

namespace sql
{
int ObFreezeExecutor::execute(ObExecContext &ctx, ObFreezeStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common_rpc_proxy is null", K(ret));
  } else {
    if (!stmt.is_major_freeze()) {
      const uint64_t local_tenant_id = MTL_ID();
      bool freeze_all = (stmt.is_freeze_all() ||
                         stmt.is_freeze_all_user() ||
                         stmt.is_freeze_all_meta());
      ObRootMinorFreezeArg arg;
      if (OB_FAIL(arg.tenant_ids_.assign(stmt.get_tenant_ids()))) {
        LOG_WARN("failed to assign tenant_ids", K(ret));
      } else if (OB_FAIL(arg.server_list_.assign(stmt.get_server_list()))) {
        LOG_WARN("failed to assign server_list", K(ret));
      } else {
        arg.zone_ = stmt.get_zone();
        arg.tablet_id_ = stmt.get_tablet_id();
        arg.ls_id_ = stmt.get_ls_id();
      }
      if (OB_SUCC(ret)) {
        // get all tenants to freeze
        if (freeze_all) {
          if (OB_ISNULL(GCTX.schema_service_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid GCTX", KR(ret));
          } else {
            // if min_cluster_version < 4.2.1.0，disable all_user/all_meta,
            // and make tenant=all effective for all tenants.
            if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_0) {
              if (stmt.is_freeze_all_user() || stmt.is_freeze_all_meta()) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("all_user/all_meta are not supported when min_cluster_version is less than 4.2.1.0",
                         KR(ret), "freeze_all_user", stmt.is_freeze_all_user(),
                         "freeze_all_meta", stmt.is_freeze_all_meta());
              } else if (stmt.is_freeze_all()) {
                if (OB_FAIL(GCTX.schema_service_->get_tenant_ids(arg.tenant_ids_))) {
                  LOG_WARN("fail to get all tenant ids", KR(ret));
                }
              }
            } else {
              common::ObSArray<uint64_t> tmp_tenant_ids;
              if (OB_FAIL(GCTX.schema_service_->get_tenant_ids(tmp_tenant_ids))) {
                LOG_WARN("fail to get all tenant ids", KR(ret));
              } else {
                using FUNC_TYPE = bool (*) (const uint64_t);
                FUNC_TYPE func = nullptr;
                // caller guarantees that at most one of
                // freeze_all/freeze_all_user/freeze_all_meta is true.
                if (stmt.is_freeze_all() || stmt.is_freeze_all_user()) {
                  func = is_user_tenant;
                } else {
                  func = is_meta_tenant;
                }
                arg.tenant_ids_.reset();
                for (int64_t i = 0; OB_SUCC(ret) && (i < tmp_tenant_ids.count()); ++i) {
                  uint64_t tmp_tenant_id = tmp_tenant_ids.at(i);
                  if (func(tmp_tenant_id)) {
                    if (OB_FAIL(arg.tenant_ids_.push_back(tmp_tenant_id))) {
                      LOG_WARN("failed to push back tenant_id", KR(ret));
                    }
                  }
                }
              }
            }
          }
        // get local tenant to freeze if there is no any parameter except server_list
        } else if (arg.tenant_ids_.empty() &&
                   arg.zone_.is_empty() &&
                   !arg.tablet_id_.is_valid() &&
                   !freeze_all) {
          if (!is_sys_tenant(local_tenant_id) || arg.server_list_.empty()) {
            if (OB_FAIL(arg.tenant_ids_.push_back(local_tenant_id))) {
              LOG_WARN("failed to push back tenant_id", KR(ret));
            }
          }
        // get local tenant to freeze if there is no any parameter
        } else if (0 == arg.tenant_ids_.count() &&
                   0 == arg.server_list_.count() &&
                   arg.zone_.is_empty() &&
                   !arg.tablet_id_.is_valid() &&
                   !freeze_all) {
          if (OB_FAIL(arg.tenant_ids_.push_back(local_tenant_id))) {
            LOG_WARN("failed to push back tenant_id", KR(ret));
           }
        }
      }
      // access check:
      // not allow user_tenant to freeze other tenants
      if (OB_SUCC(ret) && !is_sys_tenant(local_tenant_id)) {
        if (arg.tenant_ids_.count() > 1 ||
            (!arg.tenant_ids_.empty() && local_tenant_id != arg.tenant_ids_[0])) {
          ret = OB_ERR_NO_PRIVILEGE;
          LOG_WARN("user_tenant cannot freeze other tenants", K(ret), K(local_tenant_id), K(arg));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t timeout = THIS_WORKER.get_timeout_remain();
        if (OB_FAIL(common_rpc_proxy->timeout(timeout).root_minor_freeze(arg))) {
          LOG_WARN("minor freeze rpc failed", K(arg), K(ret), K(timeout), "dst", common_rpc_proxy->get_server());
        }
      }
    } else if (stmt.get_tablet_id().is_valid()) {
      if (OB_UNLIKELY(1 != stmt.get_tenant_ids().count())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support schedule tablet major freeze for several tenant", K(ret), K(stmt));
      } else {
        rootserver::ObTabletMajorFreezeParam param;
        param.tenant_id_ = stmt.get_tenant_ids().at(0);
        param.tablet_id_ = stmt.get_tablet_id();
        param.is_rebuild_column_group_ = stmt.is_rebuild_column_group();
        if (OB_FAIL(rootserver::ObMajorFreezeHelper::tablet_major_freeze(param))) {
          LOG_WARN("failed to schedule tablet major freeze", K(ret), K(param));
        }
      }
    } else {
      rootserver::ObMajorFreezeParam param;
      param.freeze_all_ = stmt.is_freeze_all();
      param.freeze_all_user_ = stmt.is_freeze_all_user();
      param.freeze_all_meta_ = stmt.is_freeze_all_meta();
      param.transport_ = GCTX.net_frame_->get_req_transport();
      for (int64_t i = 0; i < stmt.get_tenant_ids().count() && OB_SUCC(ret); ++i) {
        uint64_t tenant_id = stmt.get_tenant_ids().at(i);
        if (OB_FAIL(param.add_freeze_info(tenant_id))) {
          LOG_WARN("fail to assign", KR(ret), K(tenant_id));
        }
      }
      if (OB_SUCC(ret)) {
        ObArray<int> merge_results; // save each tenant's major_freeze result, so use 'int' type
        if (OB_FAIL(rootserver::ObMajorFreezeHelper::major_freeze(param, merge_results))) {
          LOG_WARN("fail to major freeze", KR(ret), K(param), K(merge_results));
        } else if (merge_results.count() > 0) {
          bool is_frozen_exist = false;
          bool is_merge_not_finish = false;
          for (int64_t i = 0; i < merge_results.count(); ++i) {
            if (OB_FROZEN_INFO_ALREADY_EXIST == merge_results.at(i)) {
              is_frozen_exist = true;
            } else if (OB_MAJOR_FREEZE_NOT_FINISHED == merge_results.at(i)) {
              is_merge_not_finish = true;
            }
          }

          if (is_frozen_exist || is_merge_not_finish) {
            char buf[1024] = "larger frozen_scn already exist, some tenants' prev merge may not finish";
            if (merge_results.count() > 1) {
              LOG_USER_WARN(OB_FROZEN_INFO_ALREADY_EXIST, buf);
            } else {
              STRCPY(buf, "larger frozen_scn already exist, prev merge may not finish");
              LOG_USER_WARN(OB_FROZEN_INFO_ALREADY_EXIST, buf);
            }
          }
        }
        LOG_INFO("finish do major freeze", KR(ret), K(param), K(merge_results));
      }
    }
  }
  return ret;
}

int ObFlushCacheExecutor::execute(ObExecContext &ctx, ObFlushCacheStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (!stmt.is_global_) { // flush local
    int64_t tenant_num = stmt.flush_cache_arg_.tenant_ids_.count();
    int64_t db_num = stmt.flush_cache_arg_.db_ids_.count();
    common::ObString sql_id = stmt.flush_cache_arg_.sql_id_;
    switch (stmt.flush_cache_arg_.cache_type_) {
      case CACHE_TYPE_LIB_CACHE: {
        if (stmt.flush_cache_arg_.ns_type_ != ObLibCacheNameSpace::NS_INVALID) {
          ObLibCacheNameSpace ns = stmt.flush_cache_arg_.ns_type_;
          if (0 == tenant_num) { // purge in tenant level, aka. coarse-grained plan evict
            common::ObArray<uint64_t> tenant_ids;
            if (OB_ISNULL(GCTX.omt_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null of GCTX.omt_", K(ret));
            } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
              LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
            } else {
              for (int64_t i = 0; i < tenant_ids.size(); i++) {
                MTL_SWITCH(tenant_ids.at(i)) {
                  ObPlanCache* plan_cache = MTL(ObPlanCache*);
                  ret = plan_cache->flush_lib_cache_by_ns(ns);
                }
                // ignore errors at switching tenant
                ret = OB_SUCCESS;
              }
            }
          } else {
            for (int64_t i = 0; i < tenant_num; ++i) { //ignore ret
              MTL_SWITCH(stmt.flush_cache_arg_.tenant_ids_.at(i)) {
                ObPlanCache* plan_cache = MTL(ObPlanCache*);
                ret = plan_cache->flush_lib_cache_by_ns(ns);
              }
            }
          }
        } else {
          if (0 == tenant_num) { // purge in tenant level, aka. coarse-grained plan evict
            common::ObArray<uint64_t> tenant_ids;
            if (OB_ISNULL(GCTX.omt_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null of GCTX.omt_", K(ret));
            } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
              LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
            } else {
              for (int64_t i = 0; i < tenant_ids.size(); i++) {
                MTL_SWITCH(tenant_ids.at(i)) {
                  ObPlanCache* plan_cache = MTL(ObPlanCache*);
                  ret = plan_cache->flush_lib_cache();
                }
                // ignore errors at switching tenant
                ret = OB_SUCCESS;
              }
            }
          } else {
            for (int64_t i = 0; i < tenant_num; ++i) { //ignore ret
              MTL_SWITCH(stmt.flush_cache_arg_.tenant_ids_.at(i)) {
                ObPlanCache* plan_cache = MTL(ObPlanCache*);
                ret = plan_cache->flush_lib_cache();
              }
            }
          }
        }
        break;
      }
      case CACHE_TYPE_PLAN: {
        if (stmt.flush_cache_arg_.is_fine_grained_) {
          // purge in sql_id level, aka. fine-grained plan evict
          // we assume tenant_list must not be empty and this will be checked in resolve phase
          if (0 == tenant_num) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected tenant_list in fine-grained plan evict", K(tenant_num));
          } else {
            for (int64_t i = 0; i < tenant_num; i++) { // ignore ret
              int64_t t_id = stmt.flush_cache_arg_.tenant_ids_.at(i);
              MTL_SWITCH(t_id) {
                ObPlanCache* plan_cache = MTL(ObPlanCache*);
                // not specified db_name, evict all dbs
                if (db_num == 0) {
                  ret = plan_cache->flush_plan_cache_by_sql_id(OB_INVALID_ID, sql_id);
                } else { // evict db by db
                  for(int64_t j = 0; j < db_num; j++) { // ignore ret
                    ret = plan_cache->flush_plan_cache_by_sql_id(stmt.flush_cache_arg_.db_ids_.at(j), sql_id);
                  }
                }
              }
            }
          }
        } else if (0 == tenant_num) { // purge in tenant level, aka. coarse-grained plan evict
          common::ObArray<uint64_t> tenant_ids;
          if (OB_ISNULL(GCTX.omt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null of GCTX.omt_", K(ret));
          } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
            LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
          } else {
            for (int64_t i = 0; i < tenant_ids.size(); i++) {
              MTL_SWITCH(tenant_ids.at(i)) {
                ObPlanCache* plan_cache = MTL(ObPlanCache*);
                ret = plan_cache->flush_plan_cache();
              }
              // ignore errors at switching tenant
              ret = OB_SUCCESS;
            }
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < tenant_num; ++i) { //ignore ret
            MTL_SWITCH(stmt.flush_cache_arg_.tenant_ids_.at(i)) {
              ObPlanCache* plan_cache = MTL(ObPlanCache*);
              ret = plan_cache->flush_plan_cache();
            }
          }
        }
        break;
      }
      case CACHE_TYPE_SQL_AUDIT: {
        if (0 == tenant_num) {
          common::ObArray<uint64_t> tenant_ids;
          if (OB_ISNULL(GCTX.omt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null of GCTX.omt_", K(ret));
          } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
            LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
          }
          if (OB_SUCC(ret)) {
            for (int64_t i = 0; i < tenant_ids.size(); i++) { // ignore internal error
              const uint64_t tenant_id = tenant_ids.at(i);
              MTL_SWITCH(tenant_id) {
                ObMySQLRequestManager *req_mgr = MTL(ObMySQLRequestManager*);
                req_mgr->clear_queue();
              } else if (OB_TENANT_NOT_IN_SERVER == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to switch to tenant", K(ret), K(tenant_id));
              }
            }
          }
        } else {
          for (int64_t i = 0; i < tenant_num; i++) { // ignore ret
            const uint64_t tenant_id = stmt.flush_cache_arg_.tenant_ids_.at(i);
            MTL_SWITCH(tenant_id) {
              ObMySQLRequestManager *req_mgr = MTL(ObMySQLRequestManager*);
              req_mgr->clear_queue();
            } else if (OB_TENANT_NOT_IN_SERVER == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to switch to tenant", K(ret), K(tenant_id));
            }
          }
        }
        break;
      }
      case CACHE_TYPE_PL_OBJ: {
        if (stmt.flush_cache_arg_.is_fine_grained_) {
          // purge in sql_id level, aka. fine-grained plan evict
          // we assume tenant_list must not be empty and this will be checked in resolve phase
          if (0 == tenant_num) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected tenant_list in fine-grained plan evict", K(tenant_num));
          } else {
            bool is_evict_by_schema_id = common::OB_INVALID_ID != stmt.flush_cache_arg_.schema_id_;
            for (int64_t i = 0; i < tenant_num; i++) { // ignore ret
              int64_t t_id = stmt.flush_cache_arg_.tenant_ids_.at(i);
              MTL_SWITCH(t_id) {
                ObPlanCache* plan_cache = MTL(ObPlanCache*);
                // not specified db_name, evict all dbs
                if (db_num == 0) {
                  if (is_evict_by_schema_id) {
                    ret = plan_cache->flush_pl_cache_single_cache_obj<pl::ObGetPLKVEntryBySchemaIdOp>(OB_INVALID_ID, stmt.flush_cache_arg_.schema_id_);
                  } else {
                    ret = plan_cache->flush_pl_cache_single_cache_obj<pl::ObGetPLKVEntryBySQLIDOp>(OB_INVALID_ID, sql_id);
                  }
                } else { // evict db by db
                  for(int64_t j = 0; j < db_num; j++) { // ignore ret
                    if (is_evict_by_schema_id) {
                      ret = plan_cache->flush_pl_cache_single_cache_obj<pl::ObGetPLKVEntryBySchemaIdOp>(stmt.flush_cache_arg_.db_ids_.at(j), stmt.flush_cache_arg_.schema_id_);
                    } else {
                      ret = plan_cache->flush_pl_cache_single_cache_obj<pl::ObGetPLKVEntryBySQLIDOp>(stmt.flush_cache_arg_.db_ids_.at(j), sql_id);
                    }
                  }
                }
              }
            }
          }
        } else if (0 == tenant_num) {
          common::ObArray<uint64_t> tenant_ids;
          if (OB_ISNULL(GCTX.omt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null of GCTX.omt_", K(ret));
          } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
            LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
          } else {
            for (int64_t i = 0; i < tenant_ids.size(); i++) {
              MTL_SWITCH(tenant_ids.at(i)) {
                ObPlanCache* plan_cache = MTL(ObPlanCache*);
                ret = plan_cache->flush_pl_cache();
              }
              // ignore errors at switching tenant
              ret = OB_SUCCESS;
            }
          }
        } else {
          for (int64_t i = 0; i < tenant_num; i++) { // ignore internal err code
            MTL_SWITCH(stmt.flush_cache_arg_.tenant_ids_.at(i)) {
              ObPlanCache* plan_cache = MTL(ObPlanCache*);
              ret = plan_cache->flush_pl_cache();
            }
          }
        }
        break;
      }
      case CACHE_TYPE_PS_OBJ: {
        if (0 == tenant_num) {
          common::ObArray<uint64_t> tenant_ids;
          if (OB_ISNULL(GCTX.omt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null of GCTX.omt_", K(ret));
          } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
            LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
          } else {
            for (int64_t i = 0; i < tenant_ids.size(); i++) {
              MTL_SWITCH(tenant_ids.at(i)) {
                ObPsCache* ps_cache = MTL(ObPsCache*);
                if (ps_cache->is_inited()) {
                  ret = ps_cache->cache_evict_all_ps();
                }
              }
              // ignore errors at switching tenant
              ret = OB_SUCCESS;
            }
          }
        } else {
          for (int64_t i = 0; i < tenant_num; i++) { // ignore internal err code
            MTL_SWITCH(stmt.flush_cache_arg_.tenant_ids_.at(i)) {
              ObPsCache* ps_cache = MTL(ObPsCache*);
              if (ps_cache->is_inited()) {
                ret = ps_cache->cache_evict_all_ps();
              }
            }
          }
        }
        break;
      }
      //case CACHE_TYPE_BALANCE: {
      //  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
      //  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

      //  if (OB_ISNULL(task_exec_ctx)) {
      //    ret = OB_NOT_INIT;
      //    LOG_WARN("get task executor context failed");
      //  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      //    LOG_WARN("get common rpc proxy failed", K(ret));
      //  } else if (OB_ISNULL(common_rpc_proxy)) {
      //    ret = OB_ERR_UNEXPECTED;
      //    LOG_WARN("common_rpc_proxy is null", K(ret));
      //  } else if (OB_FAIL(common_rpc_proxy->flush_balance_info())) {
      //    LOG_WARN("fail to flush balance info", K(ret));
      //  }
      //  break;
      //}
      case CACHE_TYPE_ALL:
      case CACHE_TYPE_COLUMN_STAT:
      case CACHE_TYPE_BLOCK_INDEX:
      case CACHE_TYPE_BLOCK:
      case CACHE_TYPE_ROW:
      case CACHE_TYPE_BLOOM_FILTER:
      case CACHE_TYPE_CLOG:
      case CACHE_TYPE_ILOG:
      case CACHE_TYPE_SCHEMA: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("cache type not supported flush",
                 "type", stmt.flush_cache_arg_.cache_type_,
                 K(ret));
      } break;
      case CACHE_TYPE_LOCATION: {
        // TODO: @wangzhennan.wzn
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("location cache not supported to flush");
      } break;
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid cache type", "type", stmt.flush_cache_arg_.cache_type_);
      }
    }
  } else { // flush global
    ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
    obrpc::ObCommonRpcProxy *common_rpc = NULL;
    if (OB_ISNULL(task_exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
      ret = OB_NOT_INIT;
      LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
    } else if (OB_FAIL(common_rpc->admin_flush_cache(
                           stmt.flush_cache_arg_))) {
      LOG_WARN("flush cache rpc failed", K(ret), "rpc_arg", stmt.flush_cache_arg_);
    }
  }
  return ret;
}

int ObFlushKVCacheExecutor::execute(ObExecContext &ctx, ObFlushKVCacheStmt &stmt)
{
  UNUSED(stmt);
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                ctx.get_my_session()->get_effective_tenant_id(),
                schema_guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    } else {
      if (stmt.tenant_name_.is_empty() && stmt.cache_name_.is_empty()) {
        if (OB_FAIL(common::ObKVGlobalCache::get_instance().erase_cache())) {
          LOG_WARN("clear kv cache  failed", K(ret));
        } else {
          LOG_INFO("success erase all kvcache", K(ret));
        }
      } else if (!stmt.tenant_name_.is_empty() && stmt.cache_name_.is_empty()) {
        uint64_t tenant_id = OB_INVALID_ID;
        if (OB_FAIL(schema_guard.get_tenant_id(ObString::make_string(stmt.tenant_name_.ptr()), tenant_id)) ||
              OB_INVALID_ID == tenant_id) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tenant not found", K(ret));
        } else if (OB_FAIL(common::ObKVGlobalCache::get_instance().erase_cache(tenant_id))) {
          LOG_WARN("clear kv cache  failed", K(ret));
        } else {
          LOG_INFO("success erase tenant kvcache", K(ret), K(tenant_id));
        }
      } else if (!stmt.tenant_name_.is_empty() && !stmt.cache_name_.is_empty()) {
        uint64_t tenant_id = OB_INVALID_ID;
        if (OB_FAIL(schema_guard.get_tenant_id(ObString::make_string(stmt.tenant_name_.ptr()), tenant_id)) ||
              OB_INVALID_ID == tenant_id) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tenant not found", K(ret));
        } else if (OB_FAIL(common::ObKVGlobalCache::get_instance().erase_cache(tenant_id, stmt.cache_name_.ptr()))) {
          LOG_WARN("clear kv cache  failed", K(ret));
        } else {
          LOG_INFO("success erase tenant kvcache", K(ret), K(tenant_id), K(stmt.cache_name_));
        }
      } else if (stmt.tenant_name_.is_empty() && !stmt.cache_name_.is_empty()) {
        if (OB_FAIL(common::ObKVGlobalCache::get_instance().erase_cache(stmt.cache_name_.ptr()))) {
          LOG_WARN("clear kv cache  failed", K(ret));
        } else {
          LOG_INFO("success erase kvcache", K(ret), K(stmt.cache_name_));
        }
      }
    }
  }
  return ret;
}

int ObFlushIlogCacheExecutor::execute(ObExecContext &ctx, ObFlushIlogCacheStmt &stmt)
{
  UNUSEDx(ctx, stmt);
  int ret = OB_NOT_SUPPORTED;
  // ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  // obrpc::ObCommonRpcProxy *common_rpc = NULL;
  // if (OB_ISNULL(task_exec_ctx)) {
  //   ret = OB_NOT_INIT;
  //   LOG_WARN("get task executor context failed");
  // } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
  //   ret = OB_NOT_INIT;
  //   LOG_WARN("get task exec ctx error", K(ret), KP(task_exec_ctx));
  // } else {
  //   int32_t file_id = stmt.file_id_;
  //   if (file_id < 0) {
  //     ret = OB_INVALID_ARGUMENT;
  //     LOG_ERROR("invalid file_id when execute flush ilogcache", K(ret), K(file_id));
  //   } else if (NULL == GCTX.par_ser_) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_ERROR("par_ser is null", K(ret), KP(GCTX.par_ser_));
  //   } else {
  //     // flush all file if file_id is default value 0
  //     if (0 == file_id) {
  //       if (OB_FAIL(GCTX.par_ser_->admin_wash_ilog_cache())) {
  //         LOG_WARN("cursor cache wash ilog error", K(ret));
  //       }
  //     } else {
  //       if (OB_FAIL(GCTX.par_ser_->admin_wash_ilog_cache(file_id))) {
  //         LOG_WARN("cursor cache wash ilog error", K(ret), K(file_id));
  //       }
  //     }
  //   }
  // }
  return ret;
}

int ObFlushDagWarningsExecutor::execute(ObExecContext &ctx, ObFlushDagWarningsStmt &stmt)
{
  UNUSED(stmt);
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task exec ctx error", K(ret), KP(task_exec_ctx));
  } else {
    MTL(ObDagWarningHistoryManager *)->clear();
  }
  return ret;
}

int ObAdminServerExecutor::execute(ObExecContext &ctx, ObAdminServerStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  ObCommonRpcProxy *common_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor failed", K(ret));
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    ObAdminServerArg arg;
    if (OB_FAIL(arg.servers_.assign(stmt.get_server_list()))) {
      LOG_WARN("assign failed", K(ret));
    } else {
      arg.zone_ = stmt.get_zone();
    }

    if (OB_FAIL(ret)) {
      // nothing
    } else if (ObAdminServerArg::ADD == stmt.get_op()) {
      ObSrvRpcProxy *rpc_proxy = NULL;
      if (OB_ISNULL(rpc_proxy = task_exec_ctx->get_srv_rpc())) {
        ret = OB_NOT_INIT;
        LOG_WARN("get server rpc proxy failed", K(ret));
      } else if (OB_FAIL(check_server_empty_(*rpc_proxy, arg.servers_))) {
        LOG_WARN("failed to check server empty", KR(ret), K(arg));
      } else if (OB_FAIL(common_proxy->add_server(arg))) {
        LOG_WARN("common rpc proxy add server failed", K(arg), K(ret));
      }
    } else if (ObAdminServerArg::CANCEL_DELETE == stmt.get_op()) {
      if (OB_FAIL(common_proxy->cancel_delete_server(arg))) {
        LOG_WARN("common rpc proxy cancel delete server failed", K(arg), K(ret));
      }
    } else if (ObAdminServerArg::DELETE == stmt.get_op()) {
      if (OB_FAIL(common_proxy->delete_server(arg))) {
        LOG_WARN("common rpc proxy delete server failed", K(arg), K(ret), "dst", common_proxy->get_server());
      }
    } else if (ObAdminServerArg::START == stmt.get_op()) {
      if (OB_FAIL(common_proxy->start_server(arg))) {
        LOG_WARN("common rpc proxy start server failed", K(arg), K(ret));
      }
    } else if (ObAdminServerArg::STOP == stmt.get_op()
               || ObAdminServerArg::FORCE_STOP == stmt.get_op()
               || ObAdminServerArg::ISOLATE == stmt.get_op()) {
      if (ObAdminServerArg::FORCE_STOP == stmt.get_op()) {
        arg.force_stop_ = true;
        arg.op_ = ObAdminServerArg::FORCE_STOP;
      } else if (ObAdminServerArg::ISOLATE == stmt.get_op()) {
        arg.op_ = ObAdminServerArg::ISOLATE;
      } else {
        arg.force_stop_ = false;
        arg.op_ = ObAdminServerArg::STOP;
      }
      int64_t timeout = THIS_WORKER.get_timeout_remain();
      if (OB_FAIL(common_proxy->timeout(timeout).stop_server(arg))) {
        LOG_WARN("common rpc proxy stop server failed", K(arg), K(ret));
      } else if (ObAdminServerArg::STOP == stmt.get_op()
                 || ObAdminServerArg::FORCE_STOP == stmt.get_op()) {
        // check whether all leaders are switched out
        if (OB_FAIL(wait_leader_switch_out_(*(ctx.get_sql_proxy()), arg.servers_))) {
          LOG_WARN("fail to wait leader switch out", KR(ret), K(arg));
#ifdef OB_BUILD_ARBITRATION
        // check whether all 2f tenant with arb service finished degration
        } else if (OB_FAIL(ObArbitrationServiceUtils::wait_all_2f_tenants_arb_service_degration(
                               *(ctx.get_sql_proxy()),
                               arg.servers_))) {
          LOG_WARN("fail to wait degration for arb service", KR(ret), K(arg));
#endif
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected op", "type", static_cast<int64_t>(stmt.get_op()));
    }
  }
  return ret;
}

int ObAdminServerExecutor::check_server_empty_(obrpc::ObSrvRpcProxy &rpc_proxy, const obrpc::ObServerList &servers)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  int64_t timeout = 0;
  uint64_t sys_tenant_data_version = 0;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (FALSE_IT(timeout = ctx.get_timeout())) {
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("ctx time out", KR(ret), K(timeout));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_tenant_data_version))) {
    LOG_WARN("fail to get sys tenant's min data version", KR(ret));
  } else {
    Bool is_empty = false;
    const ObCheckServerEmptyArg rpc_arg(ObCheckServerEmptyArg::ADD_SERVER, sys_tenant_data_version);
    FOREACH_X(it, servers, OB_SUCC(ret)) {
      const ObAddr &addr = *it;
      is_empty = false;
      if (OB_FAIL(rpc_proxy.to(addr)
          .timeout(timeout)
          .is_empty_server(rpc_arg, is_empty))) {
        LOG_WARN("failed to check server empty", KR(ret));
      } else if (!is_empty) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("adding non-empty server is not allowed", KR(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add non-empty server");
      }
    }
  }
  return ret;
}

int ObAdminServerExecutor::wait_leader_switch_out_(
    ObISQLClient &sql_proxy,
    const obrpc::ObServerList &svr_list)
{
  int ret = OB_SUCCESS;
  const int64_t idx = 0;
  const int64_t retry_interval_us = 1000l * 1000l; // 1s
  ObSqlString sql;
  bool stop = false;
  if (OB_UNLIKELY(0 >= svr_list.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(svr_list));
  } else if (OB_FAIL(construct_wait_leader_switch_sql_(svr_list, sql))) {
    LOG_WARN("fail to construct wait leader switch sql", KR(ret), K(svr_list));
  }

  while (OB_SUCC(ret) && !stop) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      int64_t leader_cnt = 0;
      if (0 > THIS_WORKER.get_timeout_remain()) {
        ret = OB_WAIT_LEADER_SWITCH_TIMEOUT;
        LOG_WARN("wait switching out leaders from all servers timeout", KR(ret));
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("ctx check status failed", KR(ret));
      } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
        if (OB_RS_SHUTDOWN == ret || OB_RS_NOT_MASTER == ret) {
          // switching rs, sleep and retry
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("execute sql failed", KR(ret), K(sql));
        }
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is empty", KR(ret));
        } else {
          LOG_WARN("get next result failed", KR(ret));
        }
      } else if (OB_FAIL(result->get_int(idx, leader_cnt))) {
        if (OB_ERR_NULL_VALUE == ret) {
          // __all_virtual_server_stat is not ready, sleep and retry
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get sum failed", KR(ret));
        }
      } else if (0 == leader_cnt) {
        stop = true;
      } else {
        LOG_INFO("waiting switching leaders out", KR(ret), "left count", leader_cnt);
        ob_usleep(retry_interval_us);
      }
    }
  }
  return ret;
}

int ObAdminServerExecutor::construct_wait_leader_switch_sql_(
    const obrpc::ObServerList &svr_list,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= svr_list.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(svr_list));
  } else if (OB_FAIL(sql.assign_fmt(
                         "SELECT CAST(COUNT(*) AS SIGNED) FROM %s "
                         "WHERE role = 'LEADER' and (svr_ip, svr_port) IN (",
                         share::OB_CDB_OB_LS_LOCATIONS_TNAME))) {
    LOG_WARN("assign_fmt failed", KR(ret));
  } else {
    const int64_t size = svr_list.size();
    for (int64_t idx = 0; OB_SUCC(ret) && idx < size; ++idx) {
      const ObAddr &server = svr_list[idx];
      char svr_ip[MAX_IP_ADDR_LENGTH] = "\0";
      if (!server.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("addr is not vaild", KR(ret), K(server));
      } else if (!server.ip_to_string(svr_ip, sizeof(svr_ip))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("format ip str failed", KR(ret), K(server));
      } else {
        // server-zone mapping has been checked in rootservice
        if (idx == (size - 1)) {
          if (OB_FAIL(sql.append_fmt(
                  "('%s','%d'))", svr_ip, server.get_port()))) {
            LOG_WARN("append_fmt failed", KR(ret));
          }
        } else {
          if (OB_FAIL(sql.append_fmt(
                  "('%s','%d'), ", svr_ip, server.get_port()))) {
            LOG_WARN("append_fmt failed", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAdminZoneExecutor::execute(ObExecContext &ctx, ObAdminZoneStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  ObCommonRpcProxy *common_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor failed", K(ret));
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    const ObAdminZoneArg &arg = stmt.get_arg();
    ObString first_stmt;
    if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
      LOG_WARN("fail to get first stmt" , K(ret));
    } else {
      const_cast<obrpc::ObAdminZoneArg&>(arg).sql_stmt_str_ = first_stmt;
    }
    if (OB_FAIL(ret)) {
    } else if (ObAdminZoneArg::ADD == stmt.get_op()) {
      if (OB_FAIL(common_proxy->add_zone(arg))) {
        LOG_WARN("common rpc proxy add zone failed", K(arg), K(ret));
      }
    } else if (ObAdminZoneArg::DELETE == stmt.get_op()) {
      if (OB_FAIL(common_proxy->delete_zone(arg))) {
        LOG_WARN("common rpc proxy delete zone failed", K(arg), K(ret));
      }
    } else if (ObAdminZoneArg::START == stmt.get_op()) {
      if (OB_FAIL(common_proxy->start_zone(arg))) {
        LOG_WARN("common rpc proxy start zone failed", K(arg), K(ret));
      }
    } else if (ObAdminZoneArg::STOP == stmt.get_op()
               || ObAdminZoneArg::FORCE_STOP == stmt.get_op()
               || ObAdminZoneArg::ISOLATE == stmt.get_op()) {
      int64_t timeout = THIS_WORKER.get_timeout_remain();
      if (OB_FAIL(common_proxy->timeout(timeout).stop_zone(arg))) {
        LOG_WARN("common rpc proxy stop zone failed", K(arg), K(ret), "dst",
                 common_proxy->get_server());
      } else if (ObAdminZoneArg::STOP == stmt.get_op()
                 || ObAdminZoneArg::FORCE_STOP == stmt.get_op()) {
        obrpc::ObServerList server_list;
        if (OB_FAIL(construct_servers_in_zone_(*(ctx.get_sql_proxy()), arg, server_list))) {
          LOG_WARN("fail to construct servers in zone", KR(ret), K(arg));
        } else if (0 == server_list.count()) {
          // no need to wait leader election and arb-degration
        } else if (OB_FAIL(wait_leader_switch_out_(*(ctx.get_sql_proxy()), arg))) {
          // check whether all leaders are switched out
          LOG_WARN("fail to wait leader switch out", KR(ret), K(arg));
#ifdef OB_BUILD_ARBITRATION
        // check whether all 2f tenant with arb service finished degration
        } else if (OB_FAIL(ObArbitrationServiceUtils::wait_all_2f_tenants_arb_service_degration(
                               *(ctx.get_sql_proxy()),
                               server_list))) {
          LOG_WARN("fail to wait degration for arb service", KR(ret), K(arg), K(server_list));
#endif
        }
      } else {} // force stop, no need to wait leader switch
    } else if (ObAdminZoneArg::MODIFY == stmt.get_op()) {
      if (OB_FAIL(common_proxy->alter_zone(arg))) {
        LOG_WARN("common rpc proxy alter zone failed", K(arg), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected op: %ld", "type", stmt.get_op());
    }
  }
  return ret;
}

int ObAdminZoneExecutor::wait_leader_switch_out_(
    ObISQLClient &sql_proxy,
    const obrpc::ObAdminZoneArg &arg)
{
  int ret = OB_SUCCESS;
  const int64_t idx = 0;
  const int64_t retry_interval_us = 1000l * 1000l; // 1s
  bool stop = false;
  ObSqlString sql("AdminZoneExe");
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(construct_wait_leader_switch_sql_(arg, sql))) {
    LOG_WARN("fail to construct wait leader switch sql", KR(ret), K(arg));
  }

  while (OB_SUCC(ret) && !stop) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      int64_t leader_cnt = 0;
      if (0 > THIS_WORKER.get_timeout_remain()) {
        ret = OB_WAIT_LEADER_SWITCH_TIMEOUT;
        LOG_WARN("wait switching out leaders from all servers timeout", KR(ret));
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("ctx check status failed", KR(ret));
      } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
        if (OB_RS_SHUTDOWN == ret || OB_RS_NOT_MASTER == ret) {
          // switching rs, sleep and retry
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("execute sql failed", KR(ret), K(sql));
        }
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is empty", KR(ret));
        } else {
          LOG_WARN("get next result failed", KR(ret));
        }
      } else if (OB_FAIL(result->get_int(idx, leader_cnt))) {
        if (OB_ERR_NULL_VALUE == ret) {
          ret = OB_SUCCESS;
          // __all_virtual_server_stat is not ready, sleep and retry
        } else {
          LOG_WARN("get sum failed", KR(ret));
        }
      } else if (0 == leader_cnt) {
        stop = true;
      } else {
        LOG_INFO("waiting switching leaders out", KR(ret), "left count", leader_cnt);
        ob_usleep(retry_interval_us);
      }
    }
  }
  return ret;
}

int ObAdminZoneExecutor::construct_wait_leader_switch_sql_(
    const obrpc::ObAdminZoneArg &arg,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(sql.assign_fmt(
                         "SELECT CAST(COUNT(*) AS SIGNED) FROM %s "
                         "WHERE role = 'LEADER' AND zone = '%s'",
                         share::OB_CDB_OB_LS_LOCATIONS_TNAME, arg.zone_.ptr()))) {
    LOG_WARN("assign_fmt failed", KR(ret), K(arg));
  }
  return ret;
}

int ObAdminZoneExecutor::construct_servers_in_zone_(
    ObISQLClient &sql_proxy,
    const obrpc::ObAdminZoneArg &arg,
    obrpc::ObServerList &svr_list)
{
  int ret = OB_SUCCESS;
  svr_list.reset();
  share::ObServerTableOperator st_operator;
  ObArray<share::ObServerStatus> server_statuses;

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(st_operator.init(&sql_proxy))) {
    LOG_WARN("fail to init ObServerTableOperator", KR(ret));
  } else if (OB_FAIL(st_operator.get(server_statuses))) {
    LOG_WARN("build server statuses from __all_server failed", KR(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < server_statuses.count(); ++idx) {
      if (arg.zone_ == server_statuses.at(idx).zone_) {
        if (OB_FAIL(svr_list.push_back(server_statuses.at(idx).server_))) {
          LOG_WARN("fail to add server to server_list", KR(ret), K(arg), K(server_statuses));
        }
      }
    }
  }
  return ret;
}

int ObSwitchReplicaRoleExecutor::execute(ObExecContext &ctx, ObSwitchReplicaRoleStmt &stmt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session should not be null");
  } else {
    ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
    obrpc::ObCommonRpcProxy *common_rpc = NULL;
    if (OB_ISNULL(task_exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
      ret = OB_NOT_INIT;
      LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
    } else if (OB_FAIL(common_rpc->admin_switch_replica_role(
                           stmt.get_rpc_arg()))) {
      LOG_WARN("switch replica role rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
    }
  }
  return ret;
}

int ObSwitchRSRoleExecutor::execute(ObExecContext &ctx, ObSwitchRSRoleStmt &stmt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session should not be null");
  } else {
    ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
    obrpc::ObCommonRpcProxy *common_rpc = NULL;
    if (OB_ISNULL(task_exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
      ret = OB_NOT_INIT;
      LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
    } else if (OB_FAIL(common_rpc->admin_switch_rs_role(
                           stmt.get_rpc_arg()))) {
      LOG_WARN("switch rootserver role rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
    }
  }
  return ret;
}

int ObReportReplicaExecutor::execute(ObExecContext &ctx, ObReportReplicaStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_report_replica(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("report replica rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObRecycleReplicaExecutor::execute(ObExecContext &ctx, ObRecycleReplicaStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_recycle_replica(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("recycle replica rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObAdminMergeExecutor::execute(ObExecContext &ctx, ObAdminMergeStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  const obrpc::ObAdminMergeArg &arg = stmt.get_rpc_arg();
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if ((GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_0) &&
             (arg.affect_all_user_ || arg.affect_all_meta_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("all_user/all_meta are not supported when min_cluster_version is less than 4.2.1.0",
             KR(ret), "affect_all_user", arg.affect_all_user_,
             "affect_all_meta", arg.affect_all_meta_);
  } else if (OB_FAIL(common_rpc->admin_merge(arg))) {
    LOG_WARN("admin merge rpc failed", K(ret), "rpc_arg", arg);
  }
  return ret;
}

int ObAdminRecoveryExecutor::execute(ObExecContext &ctx, ObAdminRecoveryStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_recovery(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("admin merge rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObClearRoottableExecutor::execute(ObExecContext &ctx, ObClearRoottableStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_clear_roottable(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("clear roottable rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObRefreshSchemaExecutor::execute(ObExecContext &ctx, ObRefreshSchemaStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_refresh_schema(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("refresh schema rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObRefreshMemStatExecutor::execute(ObExecContext &ctx, ObRefreshMemStatStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_refresh_memory_stat(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("refresh memory stat rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObWashMemFragmentationExecutor::execute(ObExecContext &ctx, ObWashMemFragmentationStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_wash_memory_fragmentation(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("sync wash fragment rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObRefreshIOCalibraitonExecutor::execute(ObExecContext &ctx, ObRefreshIOCalibraitonStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_refresh_io_calibration(stmt.get_rpc_arg()))) {
    LOG_WARN("refresh io calibration rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObSetConfigExecutor::execute(ObExecContext &ctx, ObSetConfigStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;

  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_0) {
    const ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> all_user("all_user");
    const ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> all_meta("all_meta");
    FOREACH_X(item, stmt.get_rpc_arg().items_, OB_SUCCESS == ret) {
      if (item->tenant_name_ == all_user || item->tenant_name_ == all_meta) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("all_user/all_meta are not supported when min_cluster_version is less than 4.2.1.0",
                 KR(ret), "tenant_name", item->tenant_name_);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_set_config(stmt.get_rpc_arg()))) {
    LOG_WARN("set config rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObSetTPExecutor::execute(ObExecContext &ctx, ObSetTPStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc_proxy->admin_set_tracepoint(
                       stmt.get_rpc_arg()))) {
    LOG_WARN("set tracepoint rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }

  LOG_INFO("set tracepoint rpc", K(stmt.get_rpc_arg()));
  return ret;
}

int ObMigrateUnitExecutor::execute(ObExecContext &ctx, ObMigrateUnitStmt &stmt)
{
	int ret = OB_SUCCESS;
	ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
	obrpc::ObCommonRpcProxy *common_rpc = NULL;
	if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
		LOG_WARN("get task executor context failed");
	} else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
		ret = OB_NOT_INIT;
		LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
	} else if (OB_FAIL(common_rpc->admin_migrate_unit(
							 stmt.get_rpc_arg()))) {
		LOG_WARN("migrate unit rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
	}
	return ret;
}

int ObAddArbitrationServiceExecutor::execute(ObExecContext &ctx, ObAddArbitrationServiceStmt &stmt)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ARBITRATION
  UNUSEDx(ctx, stmt);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support in CE Version", KR(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "add arbitration service in CE version");
#else
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  bool is_compatible = false;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", KR(ret));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_arbitration_service(
                         OB_SYS_TENANT_ID, is_compatible))) {
    LOG_WARN("fail to check compat version with arbitration service", KR(ret));
  } else if (!is_compatible) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("add arbitration service with data version below 4.1 not supported", KR(ret));
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", KR(ret), K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_add_arbitration_service(stmt.get_rpc_arg()))) {
    LOG_WARN("add arbitration service rpc failed", KR(ret), "rpc_arg", stmt.get_rpc_arg());
  }
#endif
  return ret;
}

int ObRemoveArbitrationServiceExecutor::execute(ObExecContext &ctx, ObRemoveArbitrationServiceStmt &stmt)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ARBITRATION
  UNUSEDx(ctx, stmt);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support in CE Version", KR(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "remove arbitration service in CE version");
#else
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  bool is_compatible = false;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", KR(ret));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_arbitration_service(
                         OB_SYS_TENANT_ID, is_compatible))) {
    LOG_WARN("fail to check compat version with arbitration service", KR(ret));
  } else if (!is_compatible) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remove arbitration service with data version below 4.1 not supported", KR(ret));
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", KR(ret), K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_remove_arbitration_service(stmt.get_rpc_arg()))) {
    LOG_WARN("remove arbitration service rpc failed", KR(ret), "rpc_arg", stmt.get_rpc_arg());
  }
#endif
  return ret;
}

int ObReplaceArbitrationServiceExecutor::execute(ObExecContext &ctx, ObReplaceArbitrationServiceStmt &stmt)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ARBITRATION
  UNUSEDx(ctx, stmt);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support in CE Version", KR(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace arbitration service in CE version");
#else
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  bool is_compatible = false;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", KR(ret));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_arbitration_service(
                         OB_SYS_TENANT_ID, is_compatible))) {
    LOG_WARN("fail to check compat version with arbitration service", KR(ret));
  } else if (!is_compatible) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("replace arbitration service with data version below 4.1 not supported", KR(ret));
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", KR(ret), K(task_exec_ctx));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(common_rpc->admin_replace_arbitration_service(stmt.get_rpc_arg()))) {
    LOG_WARN("replace arbitration service rpc failed", KR(ret), "rpc_arg", stmt.get_rpc_arg());
  } else if (OB_FAIL(ObArbitrationServiceUtils::wait_all_tenant_with_arb_has_arb_member(
                 *GCTX.sql_proxy_,
                 stmt.get_rpc_arg().get_arbitration_service(),
                 stmt.get_rpc_arg().get_previous_arbitration_service()))) {
    LOG_WARN("fail to wait all tenant with arb service has expected arb member",
             KR(ret), "rpc_arg", stmt.get_rpc_arg());
  } else {
    // try clean cluster info from arb server
    ObRemoveClusterInfoFromArbServerArg remove_cluster_info_arg;
    int tmp_ret = OB_SUCCESS; // for remove_cluster_info operation
    if (OB_TMP_FAIL(remove_cluster_info_arg.init(stmt.get_rpc_arg().get_previous_arbitration_service()))) {
      LOG_WARN("fail to init a rpc arg", K(tmp_ret), "rpc_arg", stmt.get_rpc_arg());
    } else if (OB_TMP_FAIL(common_rpc->remove_cluster_info_from_arb_server(remove_cluster_info_arg))) {
      LOG_WARN("fail to remove cluster info from arb server", K(tmp_ret), K(remove_cluster_info_arg));
    }
    if (OB_SUCCESS != tmp_ret) {
      LOG_USER_WARN(OB_CLUSTER_INFO_MAYBE_REMAINED, stmt.get_rpc_arg().get_previous_arbitration_service().length(),
                    stmt.get_rpc_arg().get_previous_arbitration_service().ptr());
    }
  }
#endif
  return ret;
}

int ObClearLocationCacheExecutor::execute(ObExecContext &ctx, ObClearLocationCacheStmt &stmt)
{
  int ret = OB_SUCCESS;
	ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
	obrpc::ObCommonRpcProxy *common_rpc = NULL;
	if (OB_ISNULL(task_exec_ctx)) {
		ret = OB_NOT_INIT;
		LOG_WARN("get task executor context failed");
	} else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
		ret = OB_NOT_INIT;
		LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
	} else if (OB_FAIL(common_rpc->admin_clear_location_cache(
							 stmt.get_rpc_arg()))) {
		LOG_WARN("clear location cache rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
	}
	return ret;
}

int ObReloadUnitExecutor::execute(ObExecContext &ctx, ObReloadUnitStmt &stmt)
{
	int ret = OB_SUCCESS;
	UNUSED(stmt);
	ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
	if (OB_ISNULL(task_exec_ctx)) {
		ret = OB_NOT_INIT;
		LOG_WARN("get task executor context failed");
	} else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
		ret = OB_NOT_INIT;
		LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
	} else if (OB_FAIL(common_rpc->admin_reload_unit())) {
		LOG_WARN("reload unit rpc failed", K(ret));
	}
	return ret;
}

int ObReloadServerExecutor::execute(ObExecContext &ctx, ObReloadServerStmt &stmt)
{
  int ret = OB_SUCCESS;
	UNUSED(stmt);
	ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
	obrpc::ObCommonRpcProxy *common_rpc = NULL;
	if (OB_ISNULL(task_exec_ctx)) {
		ret = OB_NOT_INIT;
		LOG_WARN("get task executor context failed");
	} else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
		ret = OB_NOT_INIT;
		LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
	} else if (OB_FAIL(common_rpc->admin_reload_server())) {
		LOG_WARN("reload server rpc failed", K(ret));
	}
	return ret;
}

int ObReloadZoneExecutor::execute(ObExecContext &ctx, ObReloadZoneStmt &stmt)
{
	int ret = OB_SUCCESS;
	UNUSED(stmt);
	ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
	obrpc::ObCommonRpcProxy *common_rpc = NULL;
	if (OB_ISNULL(task_exec_ctx)) {
		ret = OB_NOT_INIT;
		LOG_WARN("get task executor context failed");
	} else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
		ret = OB_NOT_INIT;
		LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
	} else if (OB_FAIL(common_rpc->admin_reload_zone())) {
		LOG_WARN("reload zone rpc failed", K(ret));
	}
	return ret;
}

int ObClearMergeErrorExecutor::execute(ObExecContext &ctx, ObClearMergeErrorStmt &stmt)
{
	int ret = OB_SUCCESS;
	UNUSED(stmt);
	ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  const obrpc::ObAdminMergeArg &arg = stmt.get_rpc_arg();
	obrpc::ObCommonRpcProxy *common_rpc = NULL;
	if (OB_ISNULL(task_exec_ctx)) {
		ret = OB_NOT_INIT;
		LOG_WARN("get task executor context failed");
	} else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
		ret = OB_NOT_INIT;
		LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
	} else if ((GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_0) && (arg.affect_all_user_ || arg.affect_all_meta_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("all_user/all_meta are not supported when min_cluster_version is less than 4.2.1.0",
             KR(ret), "affect_all_user", arg.affect_all_user_,
             "affect_all_meta", arg.affect_all_meta_);
  } else if (OB_FAIL(common_rpc->admin_clear_merge_error(arg))) {
		LOG_WARN("clear merge error rpc failed", K(ret), "rpc_arg", arg);
	}
  return ret;
}

int ObUpgradeVirtualSchemaExecutor ::execute(
		ObExecContext &ctx, ObUpgradeVirtualSchemaStmt &stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->timeout(timeout).admin_upgrade_virtual_schema())) {
    LOG_WARN("upgrade virtual schema rpc failed", K(ret));
  }
  return ret;
}

int ObAdminUpgradeCmdExecutor::execute(ObExecContext &ctx, ObAdminUpgradeCmdStmt &stmt)
{
  int ret = OB_SUCCESS;
  obrpc::Bool upgrade = true;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else {
    if (ObAdminUpgradeCmdStmt::BEGIN == stmt.get_op()) {
      upgrade = true;
      if (OB_FAIL(common_rpc->timeout(timeout).admin_upgrade_cmd(upgrade))) {
        LOG_WARN("begin upgrade rpc failed", K(ret));
      }
    } else if (ObAdminUpgradeCmdStmt::END == stmt.get_op()) {
      upgrade = false;
      if (OB_FAIL(common_rpc->timeout(timeout).admin_upgrade_cmd(upgrade))) {
        LOG_WARN("end upgrade rpc failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAdminRollingUpgradeCmdExecutor::execute(ObExecContext &ctx, ObAdminRollingUpgradeCmdStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else {
    ObAdminRollingUpgradeArg arg;
    if (ObAdminRollingUpgradeCmdStmt::BEGIN == stmt.get_op()) {
      arg.stage_ = obrpc::OB_UPGRADE_STAGE_DBUPGRADE;
      if (OB_FAIL(common_rpc->timeout(timeout).admin_rolling_upgrade_cmd(arg))) {
        LOG_WARN("begin upgrade rpc failed", K(ret));
      }
    } else if (ObAdminRollingUpgradeCmdStmt::END == stmt.get_op()) {
      arg.stage_ = obrpc::OB_UPGRADE_STAGE_POSTUPGRADE;
      if (OB_FAIL(common_rpc->timeout(timeout).admin_rolling_upgrade_cmd(arg))) {
        LOG_WARN("end upgrade rpc failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRunJobExecutor::execute(
		ObExecContext &ctx, ObRunJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->timeout(timeout).run_job(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("run job rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObRunUpgradeJobExecutor::execute(
		ObExecContext &ctx, ObRunUpgradeJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->timeout(timeout).run_upgrade_job(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("run job rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObStopUpgradeJobExecutor::execute(
		ObExecContext &ctx, ObStopUpgradeJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->timeout(timeout).run_upgrade_job(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("run job rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObBootstrapExecutor::execute(ObExecContext &ctx, ObBootstrapStmt &stmt)
{
  int ret = OB_SUCCESS;
	const int64_t BS_TIMEOUT = 600 * 1000 * 1000;  // 10 minutes
	ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = NULL;
  obrpc::ObBootstrapArg &bootstarp_arg = stmt.bootstrap_arg_;
  int64_t rpc_timeout = BS_TIMEOUT;
  if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
    rpc_timeout = max(THIS_WORKER.get_timeout_remain(), BS_TIMEOUT);
  }
  LOG_INFO("bootstrap timeout", K(rpc_timeout));
	if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
		ret = OB_NOT_INIT;
		LOG_WARN("get task executor context failed");
	} else if (OB_ISNULL(srv_rpc_proxy = task_exec_ctx->get_srv_rpc())) {
		ret = OB_NOT_INIT;
		LOG_WARN("get common rpc proxy failed");
	} else if (OB_FAIL(srv_rpc_proxy->to(task_exec_ctx->get_self_addr()).timeout(rpc_timeout).bootstrap(bootstarp_arg))) {
		LOG_WARN("rpc proxy bootstrap failed", K(ret), K(rpc_timeout));
		BOOTSTRAP_LOG(WARN, "STEP_0.1:alter_system execute fail");
	} else {
		BOOTSTRAP_LOG(INFO, "STEP_0.1:alter_system execute success");
	}
	return ret;
}
int ObRefreshTimeZoneInfoExecutor::execute(ObExecContext &ctx, ObRefreshTimeZoneInfoStmt &stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(stmt);
  // 226改为定时刷新tz_map, RS与其他server间也不再同步tz_version
  // 所以不再需要执行refresh timezone info触发RS刷新tz map
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter system refresh time_zone_info ");

  return ret;
}

int ObEnableSqlThrottleExecutor::execute(ObExecContext &ctx, ObEnableSqlThrottleStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = ctx.get_sql_proxy();
  ObSqlString sql;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get sql proxy from ctx fail", K(ret));
  } else if (OB_FAIL(sql.assign_fmt(
                         "SET "
                         "GLOBAL sql_throttle_priority=%ld,"
                         "GLOBAL sql_throttle_rt=%.6lf,"
                         "GLOBAL sql_throttle_cpu=%.6lf,"
                         "GLOBAL sql_throttle_io=%ld,"
                         "GLOBAL sql_throttle_network=%.6lf,"
                         "GLOBAL sql_throttle_logical_reads=%ld",
                         stmt.get_priority(),
                         stmt.get_rt(),
                         stmt.get_cpu(),
                         stmt.get_io(),
                         stmt.get_queue_time(),
                         stmt.get_logical_reads()))) {
    LOG_WARN("assign_fmt failed", K(stmt), K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(sql_proxy->write(
                    GET_MY_SESSION(ctx)->get_priv_tenant_id(),
                    sql.ptr(),
                    affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(stmt), K(ret));
    }
  }
  return ret;
}

int ObDisableSqlThrottleExecutor::execute(ObExecContext &ctx, ObDisableSqlThrottleStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = ctx.get_sql_proxy();
  ObSqlString sql;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get sql proxy from ctx fail", K(ret));
  } else if (OB_FAIL(sql.assign_fmt(
                         "SET "
                         "GLOBAL sql_throttle_priority=%ld,"
                         "GLOBAL sql_throttle_rt=%.6lf,"
                         "GLOBAL sql_throttle_cpu=%.6lf,"
                         "GLOBAL sql_throttle_io=%ld,"
                         "GLOBAL sql_throttle_network=%.6lf,"
                         "GLOBAL sql_throttle_logical_reads=%ld",
                         -1L,
                         -1.0,
                         -1.0,
                         -1L,
                         -1.0,
                         -1L))) {
    LOG_WARN("assign_fmt failed", K(stmt), K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(sql_proxy->write(
                    GET_MY_SESSION(ctx)->get_priv_tenant_id(),
                    sql.ptr(),
                    affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(stmt), K(ret));
    }
  }
  return ret;
}

int ObCancelTaskExecutor::execute(ObExecContext &ctx, ObCancelTaskStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObAddr task_server;
  share::ObTaskId task_id;
  bool is_local_task = false;

	LOG_INFO("cancel sys task log",
		       K(stmt.get_task_id()), K(stmt.get_task_type()), K(stmt.get_cmd_type()));

	if (NULL == GCTX.ob_service_ || NULL == GCTX.srv_rpc_proxy_) {
		ret = OB_ERR_SYS;
		LOG_ERROR("GCTX must not inited", K(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.ob_service_));
	} else if (OB_FAIL(parse_task_id(stmt.get_task_id(), task_id))) {
		LOG_WARN("failed to parse task id", K(ret), K(stmt.get_task_id()));
	} else if (OB_FAIL(SYS_TASK_STATUS_MGR.task_exist(task_id, is_local_task))) {
		LOG_WARN("failed to check is local task", K(ret), K(task_id));
	} else if (is_local_task) {
		if (OB_FAIL(GCTX.ob_service_->cancel_sys_task(task_id))) {
		  LOG_WARN("failed to cancel sys task at local", K(ret), K(task_id));
      }
	} else {
		if (OB_FAIL(fetch_sys_task_info(ctx, stmt.get_task_id(), task_server))) {
		  LOG_WARN("failed to fetch sys task info", K(ret));
    } else if (!task_server.is_valid() || task_id.is_invalid()) {
		  ret = OB_INVALID_ARGUMENT;
		  LOG_WARN("invalid task info", K(ret), K(task_server), K(task_id));
		} else {
		  obrpc::ObCancelTaskArg rpc_arg;
		  rpc_arg.task_id_ = task_id;
		  if (OB_FAIL(GCTX.srv_rpc_proxy_->to(task_server).cancel_sys_task(rpc_arg))) {
			LOG_WARN("failed to cancel remote sys task", K(ret), K(task_server), K(rpc_arg));
		  } else {
			LOG_INFO("succeed to cancel sys task at remote", K(task_server), K(rpc_arg));
		  }
		}
	}
	return ret;
}

int ObCancelTaskExecutor::fetch_sys_task_info(
		ObExecContext &ctx,
		const common::ObString &task_id,
		common::ObAddr &task_server)
{
	int ret = OB_SUCCESS;
	SMART_VAR(ObMySQLProxy::MySQLResult, res) {
	  ObMySQLProxy *sql_proxy = ctx.get_sql_proxy();
	  sqlclient::ObMySQLResult *result_set = NULL;
	  ObSQLSessionInfo *cur_sess = ctx.get_my_session();
	  ObSqlString read_sql;
	  char svr_ip[OB_IP_STR_BUFF] = "";
	  int64_t svr_port = 0;
	  int64_t tmp_real_str_len = 0;
	  const char *sql_str = "select svr_ip, svr_port, task_type "
	  							" from oceanbase.__all_virtual_sys_task_status "
	  							" where task_id = '%.*s'";
	  char task_type_str[common::OB_SYS_TASK_TYPE_LENGTH] = "";

	  task_server.reset();

	    //execute sql
	  if (OB_ISNULL(sql_proxy) || OB_ISNULL(cur_sess)) {
	  	ret = OB_ERR_UNEXPECTED;
	  	LOG_WARN("sql proxy or session from exec context is NULL", K(ret), K(sql_proxy), K(cur_sess));
	  } else if (OB_FAIL(read_sql.append_fmt(sql_str, task_id.length(), task_id.ptr()))) {
	  	LOG_WARN("fail to generate sql", K(ret), K(read_sql), K(*cur_sess), K(task_id));
	  } else if (OB_FAIL(sql_proxy->read(res, read_sql.ptr()))) {
	  	LOG_WARN("fail to read by sql proxy", K(ret), K(read_sql));
	  } else if (OB_ISNULL(result_set = res.get_result())) {
	  	ret = OB_ERR_UNEXPECTED;
	  	LOG_WARN("result set is NULL", K(ret), K(read_sql));
	  } else if (OB_FAIL(result_set->next())) {
	  	if (OB_LIKELY(OB_ITER_END == ret)) {
	  	  ret = OB_ENTRY_NOT_EXIST;
	  	  LOG_WARN("task id not exist", K(ret), K(result_set), K(task_id));
      } else {
	  	  LOG_WARN("fail to get next row", K(ret), K(result_set));
	  	}
	  } else {
	  	EXTRACT_STRBUF_FIELD_MYSQL(*result_set, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
	  	EXTRACT_INT_FIELD_MYSQL(*result_set, "svr_port", svr_port, int64_t);
	  	EXTRACT_STRBUF_FIELD_MYSQL(*result_set, "task_type", task_type_str, OB_SYS_TASK_TYPE_LENGTH, tmp_real_str_len);
	  	UNUSED(tmp_real_str_len);
	  }

	    //set addr
	  if (OB_SUCC(ret)) {
	  	if (OB_UNLIKELY(OB_ITER_END != result_set->next())) {
	  	  ret = OB_ERR_UNEXPECTED;
	  	  LOG_WARN("more than one sessid record", K(ret), K(read_sql));
	  	} else if (OB_UNLIKELY(!task_server.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port)))) {
	  	  ret = OB_ERR_UNEXPECTED;
	  	  LOG_WARN("fail to set ip_addr", K(ret), K(svr_ip), K(svr_port));
	  	}
	  }
  }

	return ret;
}

int ObCancelTaskExecutor::parse_task_id(
    const common::ObString &task_id_str, share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  char task_id_buf[common::OB_TRACE_STAT_BUFFER_SIZE] = "";
  task_id.reset();

	int n = snprintf(task_id_buf, sizeof(task_id_buf), "%.*s",
		  task_id_str.length(), task_id_str.ptr());
	if (n < 0 || n >= sizeof(task_id_buf)) {
		ret = common::OB_BUF_NOT_ENOUGH;
		LOG_WARN("task id buf not enough", K(ret), K(n), K(task_id_str));
	} else if (OB_FAIL(task_id.parse_from_buf(task_id_buf))) {
		ret = OB_INVALID_ARGUMENT;
		LOG_WARN("invalid task id", K(ret), K(n), K(task_id_buf));
	} else {

	  // double check
	  n = snprintf(task_id_buf, sizeof(task_id_buf), "%s", to_cstring(task_id));
		if (n < 0 || n >= sizeof(task_id_buf)) {
		  ret = OB_BUF_NOT_ENOUGH;
		  LOG_WARN("invalid task id", K(ret), K(n), K(task_id), K(task_id_buf));
		} else if (0 != task_id_str.case_compare(task_id_buf)) {
		  ret = OB_INVALID_ARGUMENT;
		  LOG_WARN("task id is not valid",
			  K(ret), K(task_id_str), K(task_id_buf), K(task_id_str.length()), K(strlen(task_id_buf)));
		}
	}
	return ret;
}

int ObSetDiskValidExecutor::execute(ObExecContext &ctx, ObSetDiskValidStmt &stmt)
{
  int ret = OB_SUCCESS;
  const ObZone null_zone;
  ObArray<ObAddr> server_list;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = NULL;
  ObAddr server = stmt.server_;
  ObSetDiskValidArg arg;

  LOG_INFO("set_disk_valid", K(server));
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor failed");
  } else if (OB_ISNULL(srv_rpc_proxy = task_exec_ctx->get_srv_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get srv rpc proxy failed");
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(null_zone, server_list))) {
    LOG_WARN("get alive server failed", KR(ret), K(null_zone));
  } else if (!has_exist_in_array(server_list, server)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("server does not exist in the alive server list", K(ret), K(null_zone), K(server_list), K(server));
  } else if (OB_FAIL(srv_rpc_proxy->to(server).set_disk_valid(arg))) {
    LOG_WARN("rpc proxy set_disk_valid failed", K(ret));
  } else {
    LOG_INFO("set_disk_valid success", K(server));
  }

  return ret;
}

int ObClearBalanceTaskExecutor::execute(ObExecContext &ctx, ObClearBalanceTaskStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->timeout(timeout).admin_clear_balance_task(
                         stmt.get_rpc_arg()))) {
    LOG_WARN("send rpc for flush balance info failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

/*
 * change tenant should satisfy the following factors:
 * 0. can't change tenant by proxy.
 * 1. login tenant is sys.
 * 2. session is not in trans.
 * 3. login user has oceanbase db's access privilege.
 * 4. ObServer has target tenant's resource.
 *
 */
int ObChangeTenantExecutor::execute(ObExecContext &ctx, ObChangeTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  share::schema::ObSchemaGetterGuard schema_guard;
  uint64_t effective_tenant_id = stmt.get_tenant_id();
  uint64_t pre_effective_tenant_id = OB_INVALID_TENANT_ID;
  uint64_t login_tenant_id = OB_INVALID_TENANT_ID;
  ObString database_name(OB_SYS_DATABASE_NAME);
  share::schema::ObSessionPrivInfo session_priv;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret), K(effective_tenant_id));
  } else if (ObBasicSessionInfo::VALID_PROXY_SESSID != session_info->get_proxy_sessid()) { // case 0
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't change tenant by proxy", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation from proxy");
  } else if (OB_INVALID_TENANT_ID == effective_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(effective_tenant_id));
  } else if (OB_ISNULL(GCTX.omt_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret));
  } else if (FALSE_IT(pre_effective_tenant_id = session_info->get_effective_tenant_id())) {
  } else if (FALSE_IT(login_tenant_id = session_info->get_login_tenant_id())) {
  } else if (OB_FAIL(session_info->get_session_priv_info(session_priv))) {
    LOG_WARN("fail to get session priv info", K(ret));
  } else if (effective_tenant_id == pre_effective_tenant_id) {
    // do nothing
  } else if (OB_SYS_TENANT_ID != login_tenant_id) { //case 1
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("non-sys tenant change tenant not allowed", KR(ret),
             K(effective_tenant_id), K(login_tenant_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation from regular user tenant");
  } else if (session_info->get_in_transaction()) { //case 2
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("change tenant in transaction not allowed", KR(ret), KPC(session_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "change tenant which has active transaction");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
             pre_effective_tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", KR(ret), K(pre_effective_tenant_id));
  } else if (OB_FAIL(schema_guard.check_db_access(session_priv, database_name))) { // case 3
    LOG_WARN("fail to check db access", KR(ret), K(pre_effective_tenant_id),
             K(session_priv), K(database_name));
  } else if (session_info->get_ps_session_info_size() > 0) { // case 4
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("chang tenant is not allow when prepared stmt already opened in session", KR(ret), KPC(session_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "change tenant when has prepared stmt already opened in session");
  } else {
    observer::ObSMConnection* conn = nullptr;
    // switch connection
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(conn = session_info->get_sm_connection())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("connection is null", KR(ret), KPC(session_info));
      } else {
        omt::ObTenant* pre_effective_tenant = conn->tenant_;
        if (OB_ISNULL(pre_effective_tenant)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("effective_tenant is null", KR(ret), KPC(session_info));
        } else if (OB_FAIL(GCTX.omt_->get_tenant_with_tenant_lock(
            effective_tenant_id, *conn->tmp_handle_, conn->tenant_))) { // case 4
          LOG_WARN("fail to get tenant from omt", KR(ret),
                   K(pre_effective_tenant_id), K(effective_tenant_id));
          conn->tenant_ = pre_effective_tenant;
        } else {
          // unlock pre tenant and swap the conn->handle_ and conn->tmp_handle_
          conn->is_tenant_locked_ = true;
          ObLDHandle *pre_handle = conn->handle_;
          if (OB_FAIL(pre_effective_tenant->unlock(*pre_handle))) {
            LOG_ERROR("unlock failed, may cause ref leak", KR(ret), KPC(session_info), K(lbt()));
          } else {
            conn->handle_ = conn->tmp_handle_;
            conn->tmp_handle_ = pre_handle;
          }
        }
      }
    }
    // switch session
    if (OB_SUCC(ret)) {
      ObPCMemPctConf pc_mem_conf;
      // tenant has been locked before
      ObPlanCache *pc  = conn->tenant_->get<ObPlanCache*>();
      int64_t received_schema_version = OB_INVALID_VERSION;
      if (OB_SUCC(ret)) {
        ret = OB_E(EventTable::EN_CHANGE_TENANT_FAILED) OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(session_info->get_pc_mem_conf(pc_mem_conf))) {
        LOG_WARN("fail to get pc mem conf", KR(ret), KPC(session_info));
      } else if (OB_FAIL(GCTX.schema_service_->get_tenant_received_broadcast_version(
                 effective_tenant_id, received_schema_version))) {
        LOG_WARN("fail to get tenant received broadcast version", KR(ret), K(effective_tenant_id));
      } else if (OB_FAIL(session_info->switch_tenant(effective_tenant_id))) {
        LOG_WARN("fail to switch tenant", KR(ret), K(effective_tenant_id), K(pre_effective_tenant_id));
      } else if (OB_FAIL(session_info->set_default_database(database_name))) {
        LOG_WARN("fail to set default database", KR(ret), K(database_name));
      } else if (OB_FAIL(session_info->update_sys_variable(
                 share::SYS_VAR_OB_LAST_SCHEMA_VERSION, received_schema_version))) {
        // bugfix:
        LOG_WARN("fail to set session variable for last_schema_version", KR(ret),
                 K(effective_tenant_id), K(pre_effective_tenant_id), K(received_schema_version));
      } else if (OB_FAIL(pc->set_mem_conf(pc_mem_conf))) {
        SQL_PC_LOG(WARN, "fail to set plan cache memory conf", K(ret));
      } else {
        session_info->set_database_id(OB_SYS_DATABASE_ID);
        session_info->set_plan_cache(pc);
        session_info->set_ps_cache(NULL);

        if (OB_SUCC(ret)) {
          // System tenant's __oceanbase_inner_standby_user is used to execute remote sqls
          // (mostly are used to fetch location infos) across cluster. Such remote sqls should
          // have a higher priority than sqls from user. Otherwise, it may cause an unstable cluster status
          // while cluster/tenant is lack of rpc resource. Here, we use special tenant' rpc resource to
          // deal with remote sqls accross cluster to avoid the influence of user's sql.
          // bugfix:
          const ObString &user_name = session_info->get_user_name();
          if (0 == user_name.case_compare(OB_STANDBY_USER_NAME)) {
            // TODO: (yanmu.ztl) should use a independent special tenant
            session_info->set_rpc_tenant_id(OB_SYS_TENANT_ID);
          }
        }
      }
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        ObSQLSessionMgr *session_mgr = ctx.get_session_mgr();
        uint32_t session_id = session_info->get_sessid();
        if (OB_ISNULL(session_mgr)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session_mgr is null", KR(ret), KR(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = session_mgr->kill_session(*session_info))) {
          LOG_WARN("kill session failed", KR(ret), KR(tmp_ret), K(session_id));
        }
        if (OB_SUCCESS != tmp_ret) {
          LOG_ERROR("switch tenant failed, session info may be inconsistent",
                    KR(ret), K(tmp_ret), K(session_id), K(lbt()));
        }
      }
    }
  }
  LOG_TRACE("change tenant", KR(ret), K(login_tenant_id), K(pre_effective_tenant_id), K(effective_tenant_id));
  return ret;
}

int ObSwitchTenantExecutor::execute(ObExecContext &ctx, ObSwitchTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", KR(ret), K(stmt));
  } else {
    ObSwitchTenantArg &arg = stmt.get_arg();
    arg.set_stmt_str(first_stmt);

    //left 200ms to return result
    const int64_t remain_timeout_interval_us = THIS_WORKER.get_timeout_remain();
    const int64_t execute_timeout_interval_us = remain_timeout_interval_us - 200 * 1000; // left 200ms to return result
    const int64_t original_timeout_abs_us = THIS_WORKER.get_timeout_ts();
    if (0 < execute_timeout_interval_us) {
      THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + execute_timeout_interval_us);
    }

    // TODO support specify ALL
    if (OB_FAIL(ret)) {
    } else if (arg.get_is_verify()) {
      //do nothing
    } else if (OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.switch_tenant(arg))) {
      LOG_WARN("failed to switch_tenant", KR(ret), K(arg));
    }

    //set timeout back
    if (0 < execute_timeout_interval_us) {
      THIS_WORKER.set_timeout_ts(original_timeout_abs_us);
    }
  }
  return ret;
}

int ObRecoverTenantExecutor::execute(ObExecContext &ctx, ObRecoverTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", KR(ret), K(stmt));
  } else {
    ObRecoverTenantArg &arg = stmt.get_rpc_arg();
    arg.set_stmt_str(first_stmt);

    // TODO support specify ALL and tenant list
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.recover_tenant(arg))) {
      LOG_WARN("failed to recover_tenant", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObAddDiskExecutor::execute(ObExecContext &ctx, ObAddDiskStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(srv_rpc_proxy = task_exec_ctx->get_srv_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get server rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(srv_rpc_proxy->to(stmt.arg_.server_).add_disk(stmt.arg_))) {
    LOG_WARN("failed to send add disk rpc", K(ret), "arg", stmt.arg_);
  } else {
    FLOG_INFO("succeed to send add disk rpc", "arg", stmt.arg_);
  }
  return ret;
}

int ObDropDiskExecutor::execute(ObExecContext &ctx, ObDropDiskStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(srv_rpc_proxy = task_exec_ctx->get_srv_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get server rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(srv_rpc_proxy->to(stmt.arg_.server_).drop_disk(stmt.arg_))) {
    LOG_WARN("failed to send drop disk rpc", K(ret), "arg", stmt.arg_);
  } else {
    FLOG_INFO("succeed to send drop disk rpc", "arg", stmt.arg_);
  }
  return ret;
}

int ObArchiveLogExecutor::execute(ObExecContext &ctx, ObArchiveLogStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  common::ObCurTraceId::mark_user_request();
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common_rpc_proxy is null", K(ret));
  } else {
    FLOG_INFO("ObArchiveLogExecutor::execute", K(stmt), K(ctx));
    obrpc::ObArchiveLogArg arg;
    arg.enable_ = stmt.is_enable();
    arg.tenant_id_ = stmt.get_tenant_id();
    if (OB_FAIL(arg.archive_tenant_ids_.assign(stmt.get_archive_tenant_ids()))) {
      LOG_WARN("failed to assign archive tenant ids", K(ret), K(stmt));
    } else if (OB_FAIL(common_rpc_proxy->archive_log(arg))) {
      LOG_WARN("archive_tenant rpc failed", K(ret), K(arg), "dst", common_rpc_proxy->get_server());
    }
  }
  return ret;
}

int ObBackupDatabaseExecutor::execute(ObExecContext &ctx, ObBackupDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  ObCommonRpcProxy *common_proxy = NULL;
  common::ObCurTraceId::mark_user_request();
  ObString passwd;
  ObObj value;
  obrpc::ObBackupDatabaseArg arg;
  //rs会尝试更新冻结点的schema_version的interval 5s
  const int64_t SECOND = 1* 1000 * 1000; //1s
  const int64_t MAX_RETRY_NUM = UPDATE_SCHEMA_ADDITIONAL_INTERVAL / SECOND + 1;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info must not null", K(ret));
  } else if (!session_info->user_variable_exists(OB_BACKUP_ENCRYPTION_MODE_SESSION_STR)) {
    arg.encryption_mode_ = ObBackupEncryptionMode::NONE;
    arg.passwd_.reset();
    LOG_INFO("no backup encryption mode is specified", K(stmt));
  } else {
    if (OB_FAIL(session_info->get_user_variable_value(OB_BACKUP_ENCRYPTION_MODE_SESSION_STR,
        value))) {
      LOG_WARN("failed to get encryption mode", K(ret));
    } else if (FALSE_IT(arg.encryption_mode_ = ObBackupEncryptionMode::parse_str(value.get_varchar()))) {
    } else if (!ObBackupEncryptionMode::is_valid(arg.encryption_mode_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid mode", K(ret), K(arg), K(value));
    } else if (OB_FAIL(session_info->get_user_variable_value(OB_BACKUP_ENCRYPTION_PASSWD_SESSION_STR,
        value))) {
      LOG_WARN("failed to get passwd", K(ret));
    } else if (OB_FAIL(arg.passwd_.assign(value.get_varchar()))) {
      LOG_WARN("failed to assign passwd", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(arg.backup_dest_.assign(stmt.get_backup_dest()))) {
    LOG_WARN("failed to assign backup dest", K(ret));
  } else if (OB_FAIL(arg.backup_tenant_ids_.assign(stmt.get_backup_tenant_ids()))) {
    LOG_WARN("failed to assign backup tenant ids", K(ret));
  } else if (OB_FAIL(arg.backup_description_.assign(stmt.get_backup_description()))) {
    LOG_WARN("failed to assign backup description", K(ret));
  } else {
    arg.tenant_id_ = stmt.get_tenant_id();
    arg.is_incremental_ = stmt.get_incremental();
    arg.is_compl_log_ = stmt.get_compl_log();
    arg.initiator_tenant_id_ = stmt.get_tenant_id();
    LOG_INFO("ObBackupDatabaseExecutor::execute", K(stmt), K(arg), K(ctx));

    if (!arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(arg));
    } else {
      int32_t retry_cnt = 0;
      while (retry_cnt < MAX_RETRY_NUM) {
        ret = OB_SUCCESS;
        if (OB_FAIL(common_proxy->backup_database(arg))) {
          if (OB_EAGAIN == ret) {
            LOG_WARN("backup_database rpc failed, need retry", K(ret), K(arg),
                "dst", common_proxy->get_server(), "retry_cnt", retry_cnt);
            ob_usleep(SECOND); //1s
          } else {
            LOG_WARN("backup_database rpc failed", K(ret), K(arg), "dst", common_proxy->get_server());
            break;
          }
        } else {
          break;
        }
        ++retry_cnt;
      }
    }
  }
  return ret;
}

int ObBackupManageExecutor::execute(ObExecContext &ctx, ObBackupManageStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  ObCommonRpcProxy *common_proxy = NULL;
  common::ObCurTraceId::mark_user_request();

  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    LOG_INFO("ObBackupManageExecutor::execute", K(stmt), K(ctx));
    obrpc::ObBackupManageArg arg;
    arg.tenant_id_ = stmt.get_tenant_id();
    arg.type_ = stmt.get_type();
    arg.value_ = stmt.get_value();
    arg.copy_id_ = stmt.get_copy_id();
    if (OB_FAIL(append(arg.managed_tenant_ids_, stmt.get_managed_tenant_ids()))) {
      LOG_WARN("failed to append managed tenants", K(ret), K(stmt));
    } else if (OB_FAIL(common_proxy->backup_manage(arg))) {
      LOG_WARN("backup_manage rpc failed", K(ret), K(arg), "dst", common_proxy->get_server());
    }
  }
  return ret;
}

int ObBackupCleanExecutor::execute(ObExecContext &ctx, ObBackupCleanStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  ObCommonRpcProxy *common_proxy = NULL;
  common::ObCurTraceId::mark_user_request();

  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    LOG_INFO("ObBackupCleanExecutor::execute", K(stmt), K(ctx));
    obrpc::ObBackupCleanArg arg;
    arg.initiator_tenant_id_ = stmt.get_tenant_id();
    arg.type_ = stmt.get_type();
    arg.value_ = stmt.get_value();
    arg.dest_id_ = stmt.get_copy_id();
    if (OB_FAIL(arg.description_.assign(stmt.get_description()))) {
      LOG_WARN("set clean description failed", K(ret));
    } else if (OB_FAIL(arg.clean_tenant_ids_.assign(stmt.get_clean_tenant_ids()))) {
      LOG_WARN("set clean tenant ids failed", K(ret));
    } else if (OB_FAIL(common_proxy->backup_delete(arg))) {
      LOG_WARN("backup clean rpc failed", K(ret), K(arg), "dst", common_proxy->get_server());
    }
  }
  FLOG_INFO("ObBackupCleanExecutor::execute");
  return ret;
}

int ObDeletePolicyExecutor::execute(ObExecContext &ctx, ObDeletePolicyStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  ObCommonRpcProxy *common_proxy = NULL;
  common::ObCurTraceId::mark_user_request();

  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    LOG_INFO("ObDeletePolicyExecutor::execute", K(stmt), K(ctx));
    obrpc::ObDeletePolicyArg arg;
    arg.initiator_tenant_id_ = stmt.get_tenant_id();
    arg.type_ = stmt.get_type();
    arg.redundancy_ = stmt.get_redundancy();
    arg.backup_copies_ = stmt.get_backup_copies();
    if (OB_FAIL(databuff_printf(arg.policy_name_, sizeof(arg.policy_name_), "%s", stmt.get_policy_name()))) {
      LOG_WARN("failed to set policy name", K(ret), K(stmt));
    } else if (OB_FAIL(databuff_printf(arg.recovery_window_, sizeof(arg.recovery_window_), "%s", stmt.get_recovery_window()))) {
      LOG_WARN("failed to set recovery window", K(ret), K(stmt));
    } else if (OB_FAIL(arg.clean_tenant_ids_.assign(stmt.get_clean_tenant_ids()))) {
      LOG_WARN("set clean tenant ids failed", K(ret));
    } else if (OB_FAIL(common_proxy->delete_policy(arg))) {
      LOG_WARN("delete policy rpc failed", K(ret), K(arg), "dst", common_proxy->get_server());
    }
  }
  FLOG_INFO("ObDeletePolicyExecutor::execute");
  return ret;
}

int ObBackupKeyExecutor::execute(ObExecContext &ctx, ObBackupKeyStmt &stmt)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  if (OB_FAIL(ObMasterKeyUtil::backup_key(stmt.get_tenant_id(),
                                          stmt.get_backup_dest(),
                                          stmt.get_encrypt_key()))) {
    LOG_WARN("failed to backup master key", K(ret));
  }
#endif
  return ret;
}

int ObBackupBackupsetExecutor::execute(ObExecContext &ctx, ObBackupBackupsetStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  ObCommonRpcProxy *common_proxy = NULL;
  common::ObCurTraceId::reset();
  common::ObCurTraceId::mark_user_request();

  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task exec ctx is null", KR(ret));
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("not support now", K(ret));
  }
  return ret;
}

int ObBackupArchiveLogExecutor::execute(ObExecContext &ctx, ObBackupArchiveLogStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  common::ObCurTraceId::mark_user_request();
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common_rpc_proxy is null", K(ret));
  } else {
    FLOG_INFO("ObBackupArchiveLogExecutor::execute", K(stmt), K(ctx));
//    obrpc::ObBackupArchiveLogArg arg;
//    arg.enable_ = stmt.is_enable();

    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("not support now", K(ret));
//    if (OB_FAIL(common_rpc_proxy->backup_archive_log(arg))) {
//      LOG_WARN("archive_log rpc failed", K(ret), K(arg), "dst", common_rpc_proxy->get_server());
//    }
  }
  return ret;
}

int ObBackupBackupPieceExecutor::execute(ObExecContext &ctx, ObBackupBackupPieceStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  ObCommonRpcProxy *common_proxy = NULL;
  common::ObCurTraceId::reset();
  common::ObCurTraceId::mark_user_request();

  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task exec ctx is null", KR(ret));
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    LOG_INFO("ObBackupBackupPieceExecutor::execute", K(stmt), K(ctx));
//    obrpc::ObBackupBackupPieceArg arg;
//    arg.tenant_id_ = stmt.get_tenant_id();
//    arg.piece_id_ = stmt.get_piece_id();
//    arg.max_backup_times_ = stmt.get_max_backup_times();
//    arg.backup_all_ = stmt.is_backup_all();
//    MEMCPY(arg.backup_backup_dest_, stmt.get_backup_backup_dest().ptr(), stmt.get_backup_backup_dest().length());
//    arg.with_active_piece_ = stmt.with_active_piece();

    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("not support now", K(ret));
//    if (OB_FAIL(common_proxy->backup_backuppiece(arg))) {
//      LOG_WARN("backup archive log rpc failed", KR(ret), K(arg), "dst", common_proxy->get_server());
//    }
  }
  return ret;
}

int ObBackupSetEncryptionExecutor::execute(ObExecContext &ctx, ObBackupSetEncryptionStmt &stmt)
{
  int ret = OB_SUCCESS;
  common::ObCurTraceId::mark_user_request();
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  ObSessionVariable encryption_mode;
  ObSessionVariable encryption_passwd;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(session_info));
  } else {
    encryption_mode.value_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    encryption_mode.value_.set_varchar(ObBackupEncryptionMode::to_str(stmt.get_mode()));
    encryption_mode.meta_.set_meta(encryption_mode.value_.meta_);

    encryption_passwd.value_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    encryption_passwd.value_.set_varchar(stmt.get_passwd().ptr(), stmt.get_passwd().length());
    encryption_passwd.meta_.set_meta(encryption_passwd.value_.meta_);

    if (OB_FAIL(session_info->replace_user_variable(OB_BACKUP_ENCRYPTION_MODE_SESSION_STR, encryption_mode))) {
      LOG_WARN("failed to set encryption mode", K(ret), K(encryption_mode));
    } else if (OB_FAIL(session_info->replace_user_variable(OB_BACKUP_ENCRYPTION_PASSWD_SESSION_STR, encryption_passwd))) {
      LOG_WARN("failed to set encryption passwd", K(ret), K(encryption_passwd));
    } else {
      LOG_INFO("ObBackupSetEncryptionExecutor::execute", K(encryption_mode), K(encryption_passwd));
    }
  }

  return ret;
}

int ObBackupSetDecryptionExecutor::execute(ObExecContext &ctx, ObBackupSetDecryptionStmt &stmt)
{
  int ret = OB_SUCCESS;
  common::ObCurTraceId::mark_user_request();
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  ObSessionVariable decryption_passwd;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(session_info));
  } else {
    decryption_passwd.value_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    decryption_passwd.value_.set_varchar(stmt.get_passwd_array().ptr(), stmt.get_passwd_array().length());
    decryption_passwd.meta_.set_meta(decryption_passwd.value_.meta_);

    if (OB_FAIL(session_info->replace_user_variable(OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR, decryption_passwd))) {
      LOG_WARN("failed to set decryption passwd", K(ret), K(decryption_passwd));
    } else {
      LOG_INFO("ObBackupSetDecryptionExecutor::execute", K(decryption_passwd));
    }
  }

  return ret;
}

int ObSetRegionBandwidthExecutor::execute(ObExecContext &ctx, ObSetRegionBandwidthStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  common::ObMySQLProxy *sql_proxy = nullptr;
  ObSQLSessionInfo *session_info = NULL;
  int64_t affected_rows = 0;

  session_info = ctx.get_my_session();
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(session_info));
  } else if (OB_ISNULL(sql_proxy = ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not null", K(ret));
  } else if (OB_FAIL(sql_str.assign_fmt("replace into %s(src_region, dst_region, max_bw) values('%s', '%s', %ld)",
                                        "__all_region_network_bandwidth_limit",
                                        stmt.get_src_region(), stmt.get_dst_region(), stmt.get_max_bw()))) {
    LOG_WARN("failed to set region ratelimitor parameters", K(ret), K(sql_str));
  } else if (OB_FAIL(sql_proxy->write(session_info->get_effective_tenant_id()/*get_priv_tenant_id ???*/,
                                        sql_str.ptr(),
                                        affected_rows))) {
    LOG_WARN("failed to execute sql write", K(ret), K(sql_str));
  } else {
    LOG_INFO("ObSetRegionBandwidthExecutor::execute", K(stmt), K(ctx), K(sql_str));
  }
  return ret;
}

int ObAddRestoreSourceExecutor::execute(ObExecContext &ctx, ObAddRestoreSourceStmt &stmt)
{
  int ret = OB_SUCCESS;
  common::ObCurTraceId::mark_user_request();
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  ObObj value;
  ObSessionVariable new_value;

  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(session_info));
  } else if (!session_info->user_variable_exists(OB_RESTORE_SOURCE_NAME_SESSION_STR)) {
    LOG_INFO("no restore source specified before");
  } else {
    if (OB_FAIL(session_info->get_user_variable_value(OB_RESTORE_SOURCE_NAME_SESSION_STR, value))) {
      LOG_WARN("failed to get user variable value", KR(ret));
    } else if (OB_FAIL(stmt.add_restore_source(value.get_char()))) {
      LOG_WARN("failed to add restore source", KR(ret), K(value));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    new_value.value_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    new_value.value_.set_varchar(stmt.get_restore_source_array().ptr(), stmt.get_restore_source_array().length());
    new_value.meta_.set_meta(new_value.value_.meta_);

    if (OB_FAIL(session_info->replace_user_variable(OB_RESTORE_SOURCE_NAME_SESSION_STR, new_value))) {
      LOG_WARN("failed to set user variable", K(ret), K(new_value));
    } else {
      LOG_INFO("ObAddRestoreSourceExecutor::execute", K(stmt), K(new_value));
    }
  }

  return ret;
}

int ObClearRestoreSourceExecutor::execute(ObExecContext &ctx, ObClearRestoreSourceStmt &stmt)
{
  int ret = OB_SUCCESS;
  common::ObCurTraceId::mark_user_request();
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(session_info));
  } else {
    if (session_info->user_variable_exists(OB_RESTORE_SOURCE_NAME_SESSION_STR)) {
      if (OB_FAIL(session_info->remove_user_variable(OB_RESTORE_SOURCE_NAME_SESSION_STR))) {
        LOG_WARN("failed to remove user variable", KR(ret));
      }
    }
  }
  UNUSED(stmt);
  return ret;
}

int ObCheckpointSlogExecutor::execute(ObExecContext &ctx, ObCheckpointSlogStmt &stmt)
{
  int ret = OB_SUCCESS;
  const ObZone null_zone;
  ObArray<ObAddr> server_list;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = NULL;
  const ObAddr server = stmt.server_;
  ObCheckpointSlogArg arg;
  arg.tenant_id_ = stmt.tenant_id_;

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor failed");
  } else if (OB_ISNULL(srv_rpc_proxy = task_exec_ctx->get_srv_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get srv rpc proxy failed");
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(null_zone, server_list))) {
    LOG_WARN("get alive server failed", KR(ret), K(null_zone));
  } else if (!has_exist_in_array(server_list, server)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("server does not exist in the alive server list", K(ret), K(null_zone), K(server_list), K(server));
  } else if (OB_FAIL(srv_rpc_proxy->to(server).timeout(THIS_WORKER.get_timeout_remain()).checkpoint_slog(arg))) {
    LOG_WARN("rpc proxy checkpoint slog failed", K(ret));
  }

  LOG_INFO("checkpoint slog execute finish", K(ret), K(arg.tenant_id_), K(server));

  return ret;
}

int ObRecoverTableExecutor::execute(ObExecContext &ctx, ObRecoverTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = nullptr;
  ObCommonRpcProxy *common_proxy = nullptr;
  ObAddr server;
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor failed");
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_FAIL(common_proxy->recover_table(stmt.get_rpc_arg()))) {
    LOG_WARN("failed to send recover table rpc", K(ret));
  } else {
    const obrpc::ObRecoverTableArg &recover_table_rpc_arg = stmt.get_rpc_arg();
    LOG_INFO("send recover table rpc finish", K(recover_table_rpc_arg));
  }
  return ret;
}

int ObCancelRestoreExecutor::execute(ObExecContext &ctx, ObCancelRestoreStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = nullptr;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;
  ObSchemaGetterGuard guard;
  const ObTenantSchema *tenant_schema = nullptr;
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task exec ctx must not be null", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy must not be null", K(ret));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("failed to get sys tenant schema guard", K(ret));
  } else if (OB_FAIL(guard.get_tenant_info(stmt.get_drop_tenant_arg().tenant_name_, tenant_schema))) {
    LOG_WARN("failed to get tenant info", K(ret), K(stmt));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_USER_ERROR(OB_TENANT_NOT_EXIST, stmt.get_drop_tenant_arg().tenant_name_.length(), stmt.get_drop_tenant_arg().tenant_name_.ptr());
    LOG_WARN("tenant not exist", KR(ret), K(stmt));
  } else if (!tenant_schema->is_restore()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Cancel tenant not in restore is");
    LOG_WARN("Cancel tenant not in restore is not allowed", K(ret), K(stmt.get_drop_tenant_arg()));
  } else if (OB_FAIL(common_rpc_proxy->drop_tenant(stmt.get_drop_tenant_arg()))) {
    LOG_WARN("rpc proxy drop tenant failed", K(ret));
  } else {
    LOG_INFO("[RESTORE]succeed to cancel restore tenant", K(stmt));
  }
  return ret;
}

int ObTableTTLExecutor::execute(ObExecContext& ctx, ObTableTTLStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common_rpc_proxy is null", K(ret));
  } else {
    FLOG_INFO("ObTableTTLExecutor::execute", K(stmt), K(ctx));
    common::ObTTLParam param;
    ObSEArray<common::ObSimpleTTLInfo, 32> ttl_info_array;
    param.ttl_all_ = stmt.is_ttl_all();
    param.transport_ = GCTX.net_frame_->get_req_transport();
    param.type_ = stmt.get_type();
    for (int64_t i = 0; (i < stmt.get_tenant_ids().count()) && OB_SUCC(ret); i++) {
      uint64_t tenant_id = stmt.get_tenant_ids().at(i);
      if (OB_FAIL(param.add_ttl_info(tenant_id))) {
        LOG_WARN("fail to assign ttl info", KR(ret), K(tenant_id));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_UNLIKELY(!param.ttl_all_ && param.ttl_info_array_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(param), KR(ret));
    } else if (OB_FAIL(ObTTLUtil::dispatch_ttl_cmd(param))) {
      LOG_WARN("fail to dispatch ttl cmd", K(ret), K(param));
    }
  }
  return ret;
}

int ObResetConfigExecutor::execute(ObExecContext &ctx, ObResetConfigStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_set_config(stmt.get_rpc_arg()))) {
    LOG_WARN("set config rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObCancelCloneExecutor::execute(ObExecContext &ctx, ObCancelCloneStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  common::ObMySQLProxy *sql_proxy = nullptr;
  const ObString &clone_tenant_name = stmt.get_clone_tenant_name();
  bool clone_already_finish = false;

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", KR(ret));
  } else if (OB_ISNULL(sql_proxy = ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not be null", KR(ret));
  } else {
    ObSchemaGetterGuard guard;
    const ObTenantSchema *tenant_schema = nullptr;
    if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
      LOG_WARN("failed to get sys tenant schema guard", KR(ret));
    } else if (OB_FAIL(guard.get_tenant_info(clone_tenant_name, tenant_schema))) {
      LOG_WARN("failed to get tenant info", KR(ret), K(stmt));
    } else if (OB_ISNULL(tenant_schema)) {
      LOG_INFO("tenant not exist", KR(ret), K(clone_tenant_name));
    } else if (tenant_schema->is_normal()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("the new tenant has completed the cloning operation", KR(ret), K(clone_tenant_name));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The new tenant has completed the cloning operation, "
                                      "or this is not the name of a cloning tenant. "
                                      "Cancel cloning");
    }
  }

  if (FAILEDx(rootserver::ObTenantCloneUtil::cancel_clone_job_by_name(
                  *sql_proxy, clone_tenant_name, clone_already_finish,
                  ObCancelCloneJobReason(ObCancelCloneJobReason::CANCEL_BY_USER)))) {
    LOG_WARN("cancel clone job failed", KR(ret), K(clone_tenant_name));
  } else if (clone_already_finish) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the new tenant has completed the cloning operation", KR(ret), K(clone_tenant_name));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The new tenant has completed the cloning operation, "
                                     "or this is not the name of a cloning tenant. "
                                     "Cancel cloning");
  }

  return ret;
}

int ObTransferPartitionExecutor::execute(ObExecContext& ctx, ObTransferPartitionStmt& stmt)
{
  int ret = OB_SUCCESS;
  const rootserver::ObTransferPartitionArg &arg = stmt.get_arg();
  rootserver::ObTransferPartitionCommand command;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaid argument", KR(ret), K(arg));
  } else if (OB_FAIL(command.execute(arg))) {
    LOG_WARN("fail to execute command", KR(ret), K(arg));
  }
  return ret;
}
} // end namespace sql
} // end namespace oceanbase
