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

#include "sql/engine/cmd/ob_alter_system_executor.h"
#include "share/ob_force_print_log.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/backup/ob_backup_struct.h"
#include "observer/ob_server.h"
#include "sql/resolver/cmd/ob_bootstrap_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/plan_cache/ob_plan_cache_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "lib/allocator/page_arena.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/rc/ob_context.h"
#include "observer/ob_server_struct.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ob_dag_warning_history_mgr.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share;
using namespace omt;
using namespace obmysql;

namespace sql {
int ObFreezeExecutor::execute(ObExecContext& ctx, ObFreezeStmt& stmt)
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
    if (!stmt.is_major_freeze()) {
      ObRootMinorFreezeArg arg;
      arg.tenant_ids_ = stmt.get_tenant_ids();
      arg.partition_key_ = stmt.get_partition_key();
      arg.server_list_ = stmt.get_server_list();
      arg.zone_ = stmt.get_zone();
      if (OB_FAIL(common_rpc_proxy->root_minor_freeze(arg))) {
        LOG_WARN("minor freeze rpc failed", K(arg), K(ret), "dst", common_rpc_proxy->get_server());
      }
    } else {
      ObRootMajorFreezeArg arg;
      // set try_frozen_version to 0, rs will not check try_frozen_version
      arg.try_frozen_version_ = 0;
      arg.launch_new_round_ = true;
      arg.ignore_server_list_ = stmt.get_ignore_server_list();
      if (OB_FAIL(common_rpc_proxy->root_major_freeze(arg))) {
        LOG_WARN("major freeze rpc failed", K(arg), K(ret), "dst", common_rpc_proxy->get_server());
      }
    }
  }
  return ret;
}

int ObFlushCacheExecutor::execute(ObExecContext& ctx, ObFlushCacheStmt& stmt)
{
  int ret = OB_SUCCESS;
  if (!stmt.is_global_) {  // flush local
    int64_t tenant_num = stmt.flush_cache_arg_.tenant_ids_.count();
    switch (stmt.flush_cache_arg_.cache_type_) {
      case CACHE_TYPE_PLAN: {
        if (OB_ISNULL(ctx.get_plan_cache_manager())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("plan cache manager is null");
        } else if (0 == tenant_num) {
          ret = ctx.get_plan_cache_manager()->flush_all_plan_cache();
        } else {
          for (int64_t i = 0; i < tenant_num; ++i) {  // ignore ret
            ret = ctx.get_plan_cache_manager()->flush_plan_cache(stmt.flush_cache_arg_.tenant_ids_.at(i));
          }
        }
        break;
      }
      case CACHE_TYPE_SQL_AUDIT: {
        if (0 == tenant_num) {
          // flush all sql audit of all tenants
          TenantIdList id_list(16);
          if (OB_ISNULL(GCTX.omt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null of GCTX.omt_", K(ret));
          } else {
            GCTX.omt_->get_tenant_ids(id_list);
          }
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCC(ret)) {
            for (int64_t i = 0; i < id_list.size(); i++) {  // ignore internal error
              uint64_t t_id = id_list.at(i);
              FETCH_ENTITY(TENANT_SPACE, t_id)
              {
                ObMySQLRequestManager* req_mgr = MTL_GET(ObMySQLRequestManager*);
                if (nullptr == req_mgr) {
                  // ignore failure, maybe 500 tenant or other tenants
                  LOG_WARN("failed to get request mangaer", K(req_mgr), K(t_id));
                } else {
                  req_mgr->clear_queue();
                }
              }
              tmp_ret = ret;
            }
          }
          ret = tmp_ret;
        } else {
          int tmp_ret = OB_SUCCESS;
          for (int64_t i = 0; i < tenant_num; i++) {  // ignore ret
            FETCH_ENTITY(TENANT_SPACE, stmt.flush_cache_arg_.tenant_ids_.at(i))
            {
              ObMySQLRequestManager* req_mgr = MTL_GET(ObMySQLRequestManager*);
              if (nullptr == req_mgr) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to get request manager", K(ret), K(req_mgr));
              } else {
                req_mgr->clear_queue();
              }
            }
            tmp_ret = ret;
          }
          ret = tmp_ret;
        }
        break;
      }
      case CACHE_TYPE_PL_OBJ: {
        if (OB_ISNULL(ctx.get_plan_cache_manager())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("plan cache manager is null");
        } else if (0 == tenant_num) {
          ret = ctx.get_plan_cache_manager()->flush_all_pl_cache();
        } else {
          for (int64_t i = 0; i < tenant_num; i++) {  // ignore internal err code
            ret = ctx.get_plan_cache_manager()->flush_pl_cache(stmt.flush_cache_arg_.tenant_ids_.at(i));
          }
        }
        break;
      }
      case CACHE_TYPE_PS_OBJ: {
        if (OB_ISNULL(ctx.get_plan_cache_manager())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("plan cache manager is null");
        } else if (0 == tenant_num) {
          ret = ctx.get_plan_cache_manager()->flush_all_ps_cache();
        } else {
          for (int64_t i = 0; i < tenant_num; i++) {  // ignore internal err code
            ret = ctx.get_plan_cache_manager()->flush_ps_cache(stmt.flush_cache_arg_.tenant_ids_.at(i));
          }
        }
        break;
      }
      case CACHE_TYPE_ALL:
      case CACHE_TYPE_COLUMN_STAT:
      case CACHE_TYPE_BLOCK_INDEX:
      case CACHE_TYPE_BLOCK:
      case CACHE_TYPE_ROW:
      case CACHE_TYPE_BLOOM_FILTER:
      case CACHE_TYPE_LOCATION:
      case CACHE_TYPE_CLOG:
      case CACHE_TYPE_ILOG:
      case CACHE_TYPE_SCHEMA: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("cache type not supported flush", "type", stmt.flush_cache_arg_.cache_type_, K(ret));
      } break;
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid cache type", "type", stmt.flush_cache_arg_.cache_type_);
      }
    }
  } else {  // flush global
    ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
    obrpc::ObCommonRpcProxy* common_rpc = NULL;
    if (OB_ISNULL(task_exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
      ret = OB_NOT_INIT;
      LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
    } else if (OB_FAIL(common_rpc->admin_flush_cache(stmt.flush_cache_arg_))) {
      LOG_WARN("flush cache rpc failed", K(ret), "rpc_arg", stmt.flush_cache_arg_);
    }
  }
  return ret;
}

int ObFlushKVCacheExecutor::execute(ObExecContext& ctx, ObFlushKVCacheStmt& stmt)
{
  UNUSED(stmt);
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            ctx.get_my_session()->get_effective_tenant_id(), schema_guard))) {
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

int ObFlushIlogCacheExecutor::execute(ObExecContext& ctx, ObFlushIlogCacheStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task exec ctx error", K(ret), KP(task_exec_ctx));
  } else {
    int32_t file_id = stmt.file_id_;
    if (file_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid file_id when execute flush ilogcache", K(ret), K(file_id));
    } else if (NULL == GCTX.par_ser_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("par_ser is null", K(ret), KP(GCTX.par_ser_));
    } else {
      // flush all file if file_id is default value 0
      if (0 == file_id) {
        if (OB_FAIL(GCTX.par_ser_->admin_wash_ilog_cache())) {
          LOG_WARN("cursor cache wash ilog error", K(ret));
        }
      } else {
        if (OB_FAIL(GCTX.par_ser_->admin_wash_ilog_cache(file_id))) {
          LOG_WARN("cursor cache wash ilog error", K(ret), K(file_id));
        }
      }
    }
  }
  return ret;
}

int ObFlushDagWarningsExecutor::execute(ObExecContext& ctx, ObFlushDagWarningsStmt& stmt)
{
  UNUSED(stmt);
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task exec ctx error", K(ret), KP(task_exec_ctx));
  } else {
    storage::ObDagWarningHistoryManager::get_instance().clear();
  }
  return ret;
}

int ObLoadBaselineExecutor::execute(ObExecContext& ctx, ObLoadBaselineStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_load_baseline(stmt.load_baseline_arg_))) {
    LOG_WARN("load baseline rpc failed", K(ret), "rpc_arg", stmt.load_baseline_arg_);
  }
  return ret;
}

int ObAdminServerExecutor::execute(ObExecContext& ctx, ObAdminServerStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  ObCommonRpcProxy* common_proxy = NULL;

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
      if (OB_FAIL(common_proxy->add_server(arg))) {
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
    } else if (ObAdminServerArg::STOP == stmt.get_op() || ObAdminServerArg::FORCE_STOP == stmt.get_op() ||
               ObAdminServerArg::ISOLATE == stmt.get_op()) {
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
      } else if (ObAdminServerArg::STOP == stmt.get_op() || ObAdminServerArg::FORCE_STOP == stmt.get_op()) {
        // check whether all leaders are switched out
        ObMySQLProxy* sql_proxy = ctx.get_sql_proxy();
        const int64_t idx = 0;
        const int64_t retry_interval_us = 1000l * 1000l;  // 1s
        ObSqlString sql;
        ObCheckGtsReplicaStopServer check_gts_replica_arg;
        if (OB_FAIL(check_gts_replica_arg.init(arg.servers_))) {
          LOG_WARN("fail to init check gts replica arg", K(ret));
        } else if (OB_FAIL(sql.assign_fmt("SELECT CAST(SUM(leader_count) AS SIGNED) FROM %s "
                                          "WHERE (svr_ip, svr_port) IN (",
                       share::OB_ALL_VIRTUAL_SERVER_STAT_TNAME))) {
          LOG_WARN("assign_fmt failed", K(ret));
        } else {
          const int64_t size = arg.servers_.size();
          for (int64_t idx = 0; OB_SUCC(ret) && idx < size; ++idx) {
            const ObAddr& server = arg.servers_[idx];
            char svr_ip[MAX_IP_ADDR_LENGTH] = "\0";
            if (!server.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("addr is not vaild", K(ret), K(server));
            } else if (!server.ip_to_string(svr_ip, sizeof(svr_ip))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("format ip str failed", K(ret), K(server));
            } else {
              // server-zone mapping has been checked in rootservice
              if (idx == (size - 1)) {
                if (OB_FAIL(sql.append_fmt("('%s','%d'))", svr_ip, server.get_port()))) {
                  LOG_WARN("append_fmt failed", K(ret));
                }
              } else {
                if (OB_FAIL(sql.append_fmt("('%s','%d'), ", svr_ip, server.get_port()))) {
                  LOG_WARN("append_fmt failed", K(ret));
                }
              }
            }
          }
        }
        bool gts_replica_migrate_out_finished = false;
        bool stop = false;
        while (OB_SUCC(ret) && !stop) {
          SMART_VAR(ObMySQLProxy::MySQLResult, res)
          {
            sqlclient::ObMySQLResult* result = NULL;
            const int64_t rpc_timeout = THIS_WORKER.get_timeout_remain();
            obrpc::Bool can_stop(true /* default value */);
            int64_t leader_cnt = 0;
            if (0 > THIS_WORKER.get_timeout_remain()) {
              ret = OB_WAIT_LEADER_SWITCH_TIMEOUT;
              LOG_WARN("wait switching out leaders from all servers timeout", K(ret));
            } else if (OB_FAIL(THIS_WORKER.check_status())) {
              LOG_WARN("ctx check status failed", K(ret));
            } else if (gts_replica_migrate_out_finished) {
              // no need to check gts replica any more
            } else if (OB_FAIL(common_proxy->timeout(rpc_timeout)
                                   .check_gts_replica_enough_when_stop_server(check_gts_replica_arg, can_stop))) {
              if (OB_RS_SHUTDOWN == ret || OB_RS_NOT_MASTER == ret) {
                // switching rs, sleep and retry
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to check gts replica enough when stop server", K(ret));
              }
            } else if (!can_stop) {
              LOG_INFO("waiting gts replica migrate out", K(ret));
              usleep(retry_interval_us);
            } else {
              gts_replica_migrate_out_finished = true;
              LOG_INFO("wait gts replica migrate out finish", K(ret));
            }

            if (OB_FAIL(ret)) {
            } else if (!can_stop) {
            } else if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
              if (OB_RS_SHUTDOWN == ret || OB_RS_NOT_MASTER == ret) {
                // switching rs, sleep and retry
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("execute sql failed", K(ret), K(sql));
              }
            } else if (OB_ISNULL(result = res.get_result())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get result failed", K(ret));
            } else if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("result is empty", K(ret));
              } else {
                LOG_WARN("get next result failed", K(ret));
              }
            } else if (OB_FAIL(result->get_int(idx, leader_cnt))) {
              if (OB_ERR_NULL_VALUE == ret) {
                // __all_virtual_server_stat is not ready, sleep and retry
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("get sum failed", K(ret));
              }
            } else if (0 == leader_cnt) {
              stop = true;
            } else {
              LOG_INFO("waiting switching leaders out", K(ret), "left count", leader_cnt);
              usleep(retry_interval_us);
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected op", "type", static_cast<int64_t>(stmt.get_op()));
    }
  }
  return ret;
}

int ObAdminZoneExecutor::execute(ObExecContext& ctx, ObAdminZoneStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  ObCommonRpcProxy* common_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor failed", K(ret));
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    const ObAdminZoneArg& arg = stmt.get_arg();
    ObString first_stmt;
    if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
      LOG_WARN("fail to get first stmt", K(ret));
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
    } else if (ObAdminZoneArg::STOP == stmt.get_op() || ObAdminZoneArg::FORCE_STOP == stmt.get_op() ||
               ObAdminZoneArg::ISOLATE == stmt.get_op()) {
      int64_t timeout = THIS_WORKER.get_timeout_remain();
      if (OB_FAIL(common_proxy->timeout(timeout).stop_zone(arg))) {
        LOG_WARN("common rpc proxy stop zone failed", K(arg), K(ret), "dst", common_proxy->get_server());
      } else if (ObAdminZoneArg::STOP == stmt.get_op() || ObAdminZoneArg::FORCE_STOP == stmt.get_op()) {
        // check whether all leaders are switched out
        ObMySQLProxy* sql_proxy = ctx.get_sql_proxy();
        const int64_t idx = 0;
        const int64_t retry_interval_us = 1000l * 1000l;  // 1s
        bool gts_replica_migrate_out_finished = false;
        bool stop = false;
        while (OB_SUCC(ret) && !stop) {
          ObSqlString sql;
          SMART_VAR(ObMySQLProxy::MySQLResult, res)
          {
            sqlclient::ObMySQLResult* result = NULL;
            const int64_t rpc_timeout = THIS_WORKER.get_timeout_remain();
            const ObCheckGtsReplicaStopZone check_gts_replica_arg(arg.zone_);
            obrpc::Bool can_stop(true /* default value */);
            int64_t leader_cnt = 0;
            if (0 > THIS_WORKER.get_timeout_remain()) {
              ret = OB_WAIT_LEADER_SWITCH_TIMEOUT;
              LOG_WARN("wait switching out all leaders timeout", K(ret));
            } else if (OB_FAIL(THIS_WORKER.check_status())) {
              LOG_WARN("ctx check status failed", K(ret));
            } else if (gts_replica_migrate_out_finished) {
              // no need to check gts replica any more
            } else if (OB_FAIL(common_proxy->timeout(rpc_timeout)
                                   .check_gts_replica_enough_when_stop_zone(check_gts_replica_arg, can_stop))) {
              if (OB_RS_SHUTDOWN == ret || OB_RS_NOT_MASTER == ret) {
                // switching rs, sleep and retry
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to check gts replica enough when stop zone", K(ret));
              }
            } else if (!can_stop) {
              LOG_INFO("waiting gts replica migrate out", K(ret));
              usleep(retry_interval_us);
            } else {
              gts_replica_migrate_out_finished = true;
              LOG_INFO("wait gts replica migrate out finish", K(ret));
            }

            if (OB_FAIL(ret)) {
            } else if (!can_stop) {
            } else if (OB_FAIL(sql.assign_fmt("SELECT CAST(SUM(leader_count) AS SIGNED) FROM %s WHERE zone = '%s'",
                           share::OB_ALL_VIRTUAL_SERVER_STAT_TNAME,
                           arg.zone_.ptr()))) {
              LOG_WARN("assign_fmt failed", K(ret));
            } else if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
              if (OB_RS_SHUTDOWN == ret || OB_RS_NOT_MASTER == ret) {
                // switching rs, sleep and retry
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("execute sql failed", K(ret), K(sql));
              }
            } else if (OB_ISNULL(result = res.get_result())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get result failed", K(ret));
            } else if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("result is empty", K(ret));
              } else {
                LOG_WARN("get next result failed", K(ret));
              }
            } else if (OB_FAIL(result->get_int(idx, leader_cnt))) {
              if (OB_ERR_NULL_VALUE == ret) {
                ret = OB_SUCCESS;
                ObSqlString this_sql;
                SMART_VAR(ObMySQLProxy::MySQLResult, this_res)
                {
                  sqlclient::ObMySQLResult* this_result = NULL;
                  int64_t server_cnt = -1;
                  if (OB_FAIL(this_sql.assign_fmt(
                          "select count(*) from %s where zone = '%s'", share::OB_ALL_SERVER_TNAME, arg.zone_.ptr()))) {
                    LOG_WARN("fail to assign fmt", K(ret));
                  } else if (OB_FAIL(sql_proxy->read(this_res, this_sql.ptr()))) {
                    LOG_WARN("fail to execute sql", K(ret), K(this_sql));
                  } else if (OB_ISNULL(this_result = this_res.get_result())) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("fail to get result", K(ret));
                  } else if (OB_FAIL(this_result->next())) {
                    LOG_WARN("get result error", K(ret));
                  } else if (OB_FAIL(this_result->get_int(0L, server_cnt))) {
                    LOG_WARN("fail to get result", K(ret));
                  } else if (0 == server_cnt) {
                    // no server in this zone;
                    stop = true;
                  } else {
                    // __all_virtual_server_stat is not ready, sleep and retry
                  }
                }
              } else {
                LOG_WARN("get sum failed", K(ret));
              }
            } else if (0 == leader_cnt) {
              stop = true;
            } else {
              LOG_INFO("waiting switching leaders out", K(ret), "left count", leader_cnt);
              usleep(retry_interval_us);
            }
          }
        }
      } else {
      }  // force stop, no need to wait leader switch
    } else if (ObAdminZoneArg::MODIFY == stmt.get_op()) {
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1440) {
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "cannot alter zone during cluster updating to 143");
        LOG_INFO("alter zone during cluster upgrading to version 1.4.3");
      } else if (OB_FAIL(common_proxy->alter_zone(arg))) {
        LOG_WARN("common rpc proxy alter zone failed", K(arg), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected op: %ld", "type", stmt.get_op());
    }
  }
  return ret;
}

int ObSwitchReplicaRoleExecutor::execute(ObExecContext& ctx, ObSwitchReplicaRoleStmt& stmt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session should not be null");
  } else {
    ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
    obrpc::ObCommonRpcProxy* common_rpc = NULL;
    if (OB_ISNULL(task_exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
      ret = OB_NOT_INIT;
      LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
    } else if (OB_FAIL(common_rpc->admin_switch_replica_role(stmt.get_rpc_arg()))) {
      LOG_WARN("switch replica role rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
    }
  }
  return ret;
}

int ObSwitchRSRoleExecutor::execute(ObExecContext& ctx, ObSwitchRSRoleStmt& stmt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session should not be null");
  } else {
    ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
    obrpc::ObCommonRpcProxy* common_rpc = NULL;
    if (OB_ISNULL(task_exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
      ret = OB_NOT_INIT;
      LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
    } else if (OB_FAIL(common_rpc->admin_switch_rs_role(stmt.get_rpc_arg()))) {
      LOG_WARN("switch rootserver role rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
    }
  }
  return ret;
}

int ObChangeReplicaExecutor::execute(ObExecContext& ctx, ObChangeReplicaStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_change_replica(stmt.get_rpc_arg()))) {
    LOG_WARN("change replica rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObDropReplicaExecutor::execute(ObExecContext& ctx, ObDropReplicaStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_drop_replica(stmt.get_rpc_arg()))) {
    LOG_WARN("drop replica rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObMigrateReplicaExecutor::execute(ObExecContext& ctx, ObMigrateReplicaStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_migrate_replica(stmt.get_rpc_arg()))) {
    LOG_WARN("migrate replica rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObReportReplicaExecutor::execute(ObExecContext& ctx, ObReportReplicaStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_report_replica(stmt.get_rpc_arg()))) {
    LOG_WARN("report replica rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObRecycleReplicaExecutor::execute(ObExecContext& ctx, ObRecycleReplicaStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_recycle_replica(stmt.get_rpc_arg()))) {
    LOG_WARN("recycle replica rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObAdminMergeExecutor::execute(ObExecContext& ctx, ObAdminMergeStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_merge(stmt.get_rpc_arg()))) {
    LOG_WARN("admin merge rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObClearRoottableExecutor::execute(ObExecContext& ctx, ObClearRoottableStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_clear_roottable(stmt.get_rpc_arg()))) {
    LOG_WARN("clear roottable rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObRefreshSchemaExecutor::execute(ObExecContext& ctx, ObRefreshSchemaStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_refresh_schema(stmt.get_rpc_arg()))) {
    LOG_WARN("refresh schema rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObRefreshMemStatExecutor::execute(ObExecContext& ctx, ObRefreshMemStatStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_refresh_memory_stat(stmt.get_rpc_arg()))) {
    LOG_WARN("refresh memory stat rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObSetConfigExecutor::execute(ObExecContext& ctx, ObSetConfigStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
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

int ObSetTPExecutor::execute(ObExecContext& ctx, ObSetTPStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc()->admin_set_tracepoint(stmt.get_rpc_arg()))) {
    LOG_WARN("set tracepoint rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }

  LOG_INFO("set tracepoint rpc", K(stmt.get_rpc_arg()));
  return ret;
}

int ObMigrateUnitExecutor::execute(ObExecContext& ctx, ObMigrateUnitStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_migrate_unit(stmt.get_rpc_arg()))) {
    LOG_WARN("migrate unit rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObClearLocationCacheExecutor::execute(ObExecContext& ctx, ObClearLocationCacheStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_clear_location_cache(stmt.get_rpc_arg()))) {
    LOG_WARN("clear location cache rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObReloadGtsExecutor::execute(ObExecContext& ctx, ObReloadGtsStmt& stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_reload_gts())) {
    LOG_WARN("reload gts rpc failed", K(ret));
  }
  return ret;
}

int ObReloadUnitExecutor::execute(ObExecContext& ctx, ObReloadUnitStmt& stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
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

int ObReloadServerExecutor::execute(ObExecContext& ctx, ObReloadServerStmt& stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
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

int ObReloadZoneExecutor::execute(ObExecContext& ctx, ObReloadZoneStmt& stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
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

int ObClearMergeErrorExecutor::execute(ObExecContext& ctx, ObClearMergeErrorStmt& stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_clear_merge_error())) {
    LOG_WARN("clear merge error rpc failed", K(ret));
  }
  return ret;
}

int ObUpgradeVirtualSchemaExecutor ::execute(ObExecContext& ctx, ObUpgradeVirtualSchemaStmt& stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
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

int ObAdminUpgradeCmdExecutor::execute(ObExecContext& ctx, ObAdminUpgradeCmdStmt& stmt)
{
  int ret = OB_SUCCESS;
  obrpc::Bool upgrade = true;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
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

int ObAdminRollingUpgradeCmdExecutor::execute(ObExecContext& ctx, ObAdminRollingUpgradeCmdStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
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

int ObRunJobExecutor::execute(ObExecContext& ctx, ObRunJobStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->timeout(timeout).run_job(stmt.get_rpc_arg()))) {
    LOG_WARN("run job rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObRunUpgradeJobExecutor::execute(ObExecContext& ctx, ObRunUpgradeJobStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->timeout(timeout).run_upgrade_job(stmt.get_rpc_arg()))) {
    LOG_WARN("run job rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObStopUpgradeJobExecutor::execute(ObExecContext& ctx, ObStopUpgradeJobStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->timeout(timeout).run_upgrade_job(stmt.get_rpc_arg()))) {
    LOG_WARN("run job rpc failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObBootstrapExecutor::execute(ObExecContext& ctx, ObBootstrapStmt& stmt)
{
  int ret = OB_SUCCESS;
  const int64_t BS_TIMEOUT = 600 * 1000 * 1000;  // 10 minutes
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy = NULL;
  obrpc::ObBootstrapArg& bootstarp_arg = stmt.bootstrap_arg_;
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

int ObRefreshTimeZoneInfoExecutor::execute(ObExecContext& ctx, ObRefreshTimeZoneInfoStmt& stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(stmt);
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter system refresh time_zone_info ");

  return ret;
}

int ObEnableSqlThrottleExecutor::execute(ObExecContext& ctx, ObEnableSqlThrottleStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* sql_proxy = ctx.get_sql_proxy();
  ObSqlString sql;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get sql proxy from ctx fail", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("SET "
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
    if (OB_FAIL(sql_proxy->write(GET_MY_SESSION(ctx)->get_priv_tenant_id(), sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(stmt), K(ret));
    }
  }
  return ret;
}

int ObDisableSqlThrottleExecutor::execute(ObExecContext& ctx, ObDisableSqlThrottleStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* sql_proxy = ctx.get_sql_proxy();
  ObSqlString sql;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get sql proxy from ctx fail", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("SET "
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
    if (OB_FAIL(sql_proxy->write(GET_MY_SESSION(ctx)->get_priv_tenant_id(), sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(stmt), K(ret));
    }
  }
  return ret;
}

int ObCancelTaskExecutor::execute(ObExecContext& ctx, ObCancelTaskStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObAddr task_server;
  share::ObTaskId task_id;
  bool is_local_task = false;

  LOG_INFO("cancel sys task log", K(stmt.get_task_id()), K(stmt.get_task_type()), K(stmt.get_cmd_type()));

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
    ObExecContext& ctx, const common::ObString& task_id, common::ObAddr& task_server)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLProxy* sql_proxy = ctx.get_sql_proxy();
    sqlclient::ObMySQLResult* result_set = NULL;
    ObSQLSessionInfo* cur_sess = ctx.get_my_session();
    ObSqlString read_sql;
    char svr_ip[OB_IP_STR_BUFF] = "";
    int64_t svr_port = 0;
    int64_t tmp_real_str_len = 0;
    const char* sql_str = "select svr_ip, svr_port, task_type "
                          " from oceanbase.__all_virtual_sys_task_status "
                          " where task_id = '%.*s'";
    char task_type_str[common::OB_SYS_TASK_TYPE_LENGTH] = "";

    task_server.reset();

    // execute sql
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

    // set addr
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

int ObCancelTaskExecutor::parse_task_id(const common::ObString& task_id_str, share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  char task_id_buf[common::OB_TRACE_STAT_BUFFER_SIZE] = "";
  uint64_t task_id_value[2] = {0};
  task_id.reset();

  int n = snprintf(task_id_buf, sizeof(task_id_buf), "%.*s", task_id_str.length(), task_id_str.ptr());
  if (n < 0 || n >= sizeof(task_id_buf)) {
    ret = common::OB_BUF_NOT_ENOUGH;
    LOG_WARN("task id buf not enough", K(ret), K(n), K(task_id_str));
  } else if (2 != (n = sscanf(task_id_buf, TRACE_ID_FORMAT, &task_id_value[0], &task_id_value[1]))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task id", K(ret), K(n), K(task_id_buf));
  } else {
    task_id.set(task_id_value);

    // double check
    n = snprintf(task_id_buf, sizeof(task_id_buf), "%s", to_cstring(task_id));
    if (n < 0 || n >= sizeof(task_id_buf)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("invalid task id", K(ret), K(n), K(task_id), K(task_id_buf));
    } else if (0 != task_id_str.case_compare(task_id_buf)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("task id is not valid",
          K(ret),
          K(task_id_str),
          K(task_id_buf),
          K(task_id_str.length()),
          K(strlen(task_id_buf)));
    }
  }
  return ret;
}

int ObSetDiskValidExecutor::execute(ObExecContext& ctx, ObSetDiskValidStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy = NULL;
  ObAddr server = stmt.server_;
  ObSetDiskValidArg arg;

  LOG_INFO("set_disk_valid", K(server));
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor failed");
  } else if (OB_ISNULL(srv_rpc_proxy = task_exec_ctx->get_srv_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get srv rpc proxy failed");
  } else if (OB_FAIL(srv_rpc_proxy->to(server).set_disk_valid(arg))) {
    LOG_WARN("rpc proxy set_disk_valid failed", K(ret));
  } else {
    LOG_INFO("set_disk_valid success", K(server));
  }

  return ret;
}

int ObClearBalanceTaskExecutor::execute(ObExecContext& ctx, ObClearBalanceTaskStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->timeout(timeout).admin_clear_balance_task(stmt.get_rpc_arg()))) {
    LOG_WARN("send rpc for flush balance info failed", K(ret), "rpc_arg", stmt.get_rpc_arg());
  }
  return ret;
}

int ObChangeTenantExecutor::execute(ObExecContext& ctx, ObChangeTenantStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = ctx.get_my_session();
  share::schema::ObSchemaGetterGuard schema_guard;
  uint64_t effective_tenant_id = stmt.get_tenant_id();
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_INVALID_TENANT_ID == effective_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(effective_tenant_id));
  } else if (OB_ISNULL(GCTX.omt_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret));
  } else if (OB_SYS_TENANT_ID != session_info->get_login_tenant_id()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("non-sys tenant change tenant not allowed",
        K(ret),
        K(effective_tenant_id),
        "login_tenant_id",
        session_info->get_login_tenant_id());
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation from regular user tenant");
  } else if (session_info->get_in_transaction()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("change tenant in transaction not allowed", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "change tenant which has active transaction");
    //  } else if (!GCTX.omt_->has_tenant(effective_tenant_id)) {
    //    ret = OB_TENANT_NOT_IN_SERVER;
    //    LOG_WARN("tenant not exist in this server", K(ret), K(effective_tenant_id));
  } else {
    ObString database_name(OB_SYS_DATABASE_NAME);
    ObPlanCacheManager* plan_cache_mgr = session_info->get_plan_cache_manager();
    ObPCMemPctConf pc_mem_conf;
    share::schema::ObSessionPrivInfo session_priv;
    session_info->get_session_priv_info(session_priv);
    const uint64_t login_tenant_id = session_info->get_login_tenant_id();
    const uint64_t pre_effective_tenant_id = session_info->get_effective_tenant_id();
    int64_t received_schema_version = OB_INVALID_VERSION;
    if (OB_ISNULL(plan_cache_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan_cache_mgr is null", K(ret));
    } else if (OB_FAIL(session_info->get_pc_mem_conf(pc_mem_conf))) {
      LOG_WARN("fail to get pc mem conf", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   session_info->get_effective_tenant_id(), schema_guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.check_db_access(session_priv, database_name))) {  // condition 3
      LOG_WARN("fail to check db access", K(ret), K(database_name));
    } else if (OB_FAIL(session_info->set_default_database(database_name))) {
      LOG_WARN("fail to set default database", K(ret), K(database_name));
    } else if (OB_FAIL(session_info->switch_tenant(effective_tenant_id))) {
      LOG_WARN("fail to switch tenant", K(ret), K(effective_tenant_id), K(login_tenant_id));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_received_broadcast_version(
                   effective_tenant_id, received_schema_version))) {
      LOG_WARN("fail to get tenant received brocast version", K(ret), K(effective_tenant_id));
    } else if (OB_FAIL(
                   session_info->update_sys_variable(share::SYS_VAR_OB_LAST_SCHEMA_VERSION, received_schema_version))) {
      LOG_WARN("fail to set session variable for last_schema_version", K(ret));
    } else {
      session_info->set_database_id(combine_id(effective_tenant_id, OB_SYS_DATABASE_ID));
      session_info->set_plan_cache(plan_cache_mgr->get_or_create_plan_cache(effective_tenant_id, pc_mem_conf));
      session_info->set_ps_cache(plan_cache_mgr->get_or_create_ps_cache(effective_tenant_id, pc_mem_conf));

      if (OB_SUCC(ret)) {
        // System tenant's __oceanbase_inner_standby_user is used to execute remote sqls
        // (mostly are used to fetch location infos) across cluster. Such remote sqls should
        // have a higher priority than sqls from user. Otherwise, it may cause an unstable cluster status
        // while cluster/tenant is lack of rpc resource. Here, we use special tenant' rpc resource to
        // deal with remote sqls accross cluster to avoid the influence of user's sql.
        const ObString& user_name = session_info->get_user_name();
        if (0 == user_name.case_compare(OB_STANDBY_USER_NAME)) {
          // TODO: () should use a independent special tenant
          session_info->set_rpc_tenant_id(OB_LOC_USER_TENANT_ID);
        }
      }
      LOG_DEBUG(
          "change tenant success", K(ret), K(login_tenant_id), K(pre_effective_tenant_id), K(effective_tenant_id));
    }
  }
  return ret;
}

int ObAddDiskExecutor::execute(ObExecContext& ctx, ObAddDiskStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObSrvRpcProxy* srv_rpc_proxy = NULL;

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

int ObDropDiskExecutor::execute(ObExecContext& ctx, ObDropDiskStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObSrvRpcProxy* srv_rpc_proxy = NULL;

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

int ObArchiveLogExecutor::execute(ObExecContext& ctx, ObArchiveLogStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
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
    if (OB_FAIL(common_rpc_proxy->archive_log(arg))) {
      LOG_WARN("archive_log rpc failed", K(ret), K(arg), "dst", common_rpc_proxy->get_server());
    }
  }
  return ret;
}

int ObBackupDatabaseExecutor::execute(ObExecContext& ctx, ObBackupDatabaseStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  ObSQLSessionInfo* session_info = ctx.get_my_session();
  ObCommonRpcProxy* common_proxy = NULL;
  common::ObCurTraceId::mark_user_request();
  ObString passwd;
  ObObj value;
  obrpc::ObBackupDatabaseArg arg;
  const int64_t SECOND = 1 * 1000 * 1000;  // 1s
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
    if (OB_FAIL(session_info->get_user_variable_value(OB_BACKUP_ENCRYPTION_MODE_SESSION_STR, value))) {
      LOG_WARN("failed to get encryption mode", K(ret));
    } else if (FALSE_IT(arg.encryption_mode_ = ObBackupEncryptionMode::parse_str(value.get_varchar()))) {
    } else if (!ObBackupEncryptionMode::is_valid(arg.encryption_mode_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid mode", K(ret), K(arg), K(value));
    } else if (OB_FAIL(session_info->get_user_variable_value(OB_BACKUP_ENCRYPTION_PASSWD_SESSION_STR, value))) {
      LOG_WARN("failed to get passwd", K(ret));
    } else if (OB_FAIL(arg.passwd_.assign(value.get_varchar()))) {
      LOG_WARN("failed to assign passwd", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    arg.tenant_id_ = stmt.get_tenant_id();
    arg.is_incremental_ = stmt.get_incremental();
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
            LOG_WARN("backup_database rpc failed, need retry",
                K(ret),
                K(arg),
                "dst",
                common_proxy->get_server(),
                "retry_cnt",
                retry_cnt);
            usleep(SECOND);  // 1s
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

int ObBackupManageExecutor::execute(ObExecContext& ctx, ObBackupManageStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx);
  ObCommonRpcProxy* common_proxy = NULL;
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
    if (OB_FAIL(common_proxy->backup_manage(arg))) {
      LOG_WARN("backup_manage rpc failed", K(ret), K(arg), "dst", common_proxy->get_server());
    }
  }
  return ret;
}

int ObBackupSetEncryptionExecutor::execute(ObExecContext& ctx, ObBackupSetEncryptionStmt& stmt)
{
  int ret = OB_SUCCESS;
  common::ObCurTraceId::mark_user_request();
  common::ObMySQLProxy* sql_proxy = nullptr;
  ObSqlString set_mode_sql;
  ObSqlString set_passwd_sql;
  ObMySQLProxy::MySQLResult res;
  sqlclient::ObISQLConnection* conn = nullptr;
  observer::ObInnerSQLConnectionPool* pool = nullptr;
  ObSQLSessionInfo* session_info = ctx.get_my_session();
  int64_t affected_rows = 0;

  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(session_info));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_) || OB_ISNULL(sql_proxy->get_pool())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not null", K(ret), KP(GCTX.sql_proxy_));
  } else if (sqlclient::INNER_POOL != sql_proxy->get_pool()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool type must be inner", K(ret), "type", sql_proxy->get_pool()->get_type());
  } else if (OB_ISNULL(pool = static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool must not null", K(ret));
  } else if (OB_FAIL(set_mode_sql.assign_fmt("set @%s = '%s'",
                 OB_BACKUP_ENCRYPTION_MODE_SESSION_STR,
                 ObBackupEncryptionMode::to_str(stmt.get_mode())))) {
    LOG_WARN("failed to set mode", K(ret));
  } else if (OB_FAIL(set_passwd_sql.assign_fmt("set @%s = '%.*s'",
                 OB_BACKUP_ENCRYPTION_PASSWD_SESSION_STR,
                 stmt.get_passwd().length(),
                 stmt.get_passwd().ptr()))) {
    LOG_WARN("failed to set passwd", K(ret));
  } else if (OB_FAIL(pool->acquire(session_info, conn))) {
    LOG_WARN("failed to get conn", K(ret));
  } else if (OB_FAIL(conn->execute_write(session_info->get_effective_tenant_id(), set_mode_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to set mode", K(ret), K(set_mode_sql));
  } else if (OB_FAIL(
                 conn->execute_write(session_info->get_effective_tenant_id(), set_passwd_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to set passwd", K(ret), K(set_passwd_sql));
  } else {
    LOG_INFO("ObBackupSetEncryptionExecutor::execute", K(stmt), K(ctx), K(set_mode_sql), K(set_passwd_sql));
  }
  return ret;
}

int ObBackupSetDecryptionExecutor::execute(ObExecContext& ctx, ObBackupSetDecryptionStmt& stmt)
{
  int ret = OB_SUCCESS;
  common::ObCurTraceId::mark_user_request();
  common::ObMySQLProxy* sql_proxy = nullptr;
  ObSqlString set_passwd_sql;
  ObMySQLProxy::MySQLResult res;
  sqlclient::ObISQLConnection* conn = nullptr;
  observer::ObInnerSQLConnectionPool* pool = nullptr;
  ObSQLSessionInfo* session_info = ctx.get_my_session();
  int64_t affected_rows = 0;

  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(session_info));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_) || OB_ISNULL(sql_proxy->get_pool())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not null", K(ret), KP(GCTX.sql_proxy_));
  } else if (sqlclient::INNER_POOL != sql_proxy->get_pool()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool type must be inner", K(ret), "type", sql_proxy->get_pool()->get_type());
  } else if (OB_ISNULL(pool = static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool must not null", K(ret));
  } else if (OB_FAIL(set_passwd_sql.assign_fmt("set @%s = '%.*s'",
                 OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR,
                 stmt.get_passwd_array().length(),
                 stmt.get_passwd_array().ptr()))) {
    LOG_WARN("failed to set passwd", K(ret));
  } else if (OB_FAIL(pool->acquire(session_info, conn))) {
    LOG_WARN("failed to get conn", K(ret));
  } else if (OB_FAIL(
                 conn->execute_write(session_info->get_effective_tenant_id(), set_passwd_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to set passwd", K(ret), K(set_passwd_sql));
  } else {
    LOG_INFO("ObBackupSetEncryptionExecutor::execute", K(stmt), K(ctx), K(set_passwd_sql));
  }
  return ret;
}
}  // end namespace sql
}  // end namespace oceanbase
