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

#define USING_LOG_PREFIX SQL_EXE

#include "ob_determinate_task_spliter.h"
#include "ob_determinate_task_transmit.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase {

using namespace common;
using namespace share;

namespace sql {

template <typename T>
bool lexical_less(const T& l, const T& r)
{
  return l < r;
}

template <typename T, typename... Args>
bool lexical_less(const T& l, const T& r, Args... rest)
{
  bool less = false;
  if (l == r) {
    less = lexical_less(rest...);
  } else {
    less = l < r;
  }
  return less;
}

struct ObDeterminateTaskSpliter::SliceIDCompare {
  SliceIDCompare(int& ret) : ret_(ret)
  {}
  bool operator()(const ObSliceEvent* l, const ObSliceEvent* r)
  {
    bool less = false;
    if (OB_SUCCESS != ret_) {
    } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
      ret_ = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL slice id", K(ret_));
    } else {
      less = lexical_less(l->get_ob_slice_id().get_job_id(),
          r->get_ob_slice_id().get_job_id(),
          l->get_ob_slice_id().get_task_id(),
          r->get_ob_slice_id().get_task_id(),
          l->get_ob_slice_id().get_slice_id(),
          r->get_ob_slice_id().get_slice_id());
    }
    return less;
  }

  int& ret_;
};

ObDeterminateTaskSpliter::ObDeterminateTaskSpliter() : task_idx_(0), child_slices_fetched_(false)
{}

ObDeterminateTaskSpliter::~ObDeterminateTaskSpliter()
{}

int ObDeterminateTaskSpliter::get_next_task(ObTaskInfo*& task)
{
  int ret = OB_SUCCESS;
  ObPhyOperator* op = NULL;
  if (OB_ISNULL(job_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(op = job_->get_root_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root operator of job is NULL", K(ret));
  } else if (PHY_DETERMINATE_TASK_TRANSMIT != op->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not determinate transmit", K(ret));
  } else {
    ObDeterminateTaskTransmit* transmit = static_cast<ObDeterminateTaskTransmit*>(op);
    ObTaskID task_id;
    task_id.set_ob_job_id(job_->get_ob_job_id());
    task_id.set_task_id(task_idx_);
    task_id.set_task_cnt(transmit->get_tasks().count());
    if (task_idx_ >= transmit->get_tasks().count()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(create_task_info(task))) {
      LOG_WARN("create task info failed", K(ret));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL task", K(ret));
    } else {
      task->set_background(transmit->is_background());
      const auto& idx = transmit->get_tasks().at(task_idx_);
      const auto& range_loc = transmit->get_range_locations().at(idx.loc_idx_);
      const auto& part_loc = range_loc.part_locs_.at(idx.part_loc_idx_);
      if (OB_FAIL(task->get_range_location().part_locs_.init(1))) {
        LOG_WARN("fixed array init failed", K(ret));
      } else if (OB_FAIL(task->get_range_location().part_locs_.push_back(part_loc))) {
        LOG_WARN("fixed array push back failed", K(ret));
      } else if (OB_FAIL(fetch_child_result(task_id, *task))) {
        LOG_WARN("fetch child result failed", K(ret));
      } else {
        // send to same server if tasks in same range location
        if (task_idx_ > 0 && idx.loc_idx_ == transmit->get_tasks().at(task_idx_ - 1).loc_idx_) {
          task->get_range_location().server_ = pre_addr_;
        } else {
          if (OB_FAIL(set_task_destination(*transmit, task_id, *task))) {
            LOG_WARN("set task destination failed", K(ret));
          } else {
            pre_addr_ = task->get_range_location().server_;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObTaskLocation loc;
      loc.set_ob_task_id(task_id);
      loc.set_server(task->get_range_location().server_);
      task->set_task_location(loc);
      const auto& pkey = task->get_range_location().part_locs_.at(0).partition_key_;
      if (pkey.is_valid()) {
        const ObPhyTableLocation* phy_tlb_loc = NULL;
        if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                *exec_ctx_, pkey.get_table_id(), pkey.get_table_id(), phy_tlb_loc))) {
          LOG_WARN("get table location failed", K(ret));
        } else {
          const auto& loc_list = phy_tlb_loc->get_partition_location_list();
          int64_t idx = 0;
          for (; idx < loc_list.count(); idx++) {
            if (loc_list.at(idx).get_partition_id() == pkey.get_partition_id()) {
              break;
            }
          }
          if (idx == loc_list.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition not found in location", K(pkey), K(ret));
          } else {
            task->set_location_idx(idx);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(record_task(
              exec_ctx_->get_task_exec_ctx().get_sys_job_id(), task_id, *task, transmit->get_split_task_count()))) {
        LOG_WARN("record task failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      task_idx_++;
    }
  }
  return ret;
}

int ObDeterminateTaskSpliter::fetch_child_result(const ObTaskID& task_id, ObTaskInfo& task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(job_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!job_->has_child_job()) {
    // do nothing
  } else {
    if (!child_slices_fetched_) {
      const bool skip_empty = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < job_->get_child_count(); i++) {
        ObJob* child = NULL;
        if (OB_FAIL(job_->get_child_job(i, child))) {
          LOG_WARN("get child job failed", K(ret));
        } else if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child is NULL", K(ret));
        } else if (OB_FAIL(child->append_finished_slice_events(child_slices_, skip_empty))) {
          LOG_WARN("append finished slice events failed", K(ret));
        } else {
          std::sort(child_slices_.begin(), child_slices_.end(), SliceIDCompare(ret));
          if (OB_FAIL(ret)) {
            LOG_WARN("sort child slices failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        child_slices_fetched_ = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    auto& child_results = task.get_child_task_results();
    for (int64_t i = 0; OB_SUCC(ret) && i < job_->get_child_count(); i++) {
      ObJob* child = NULL;
      ObPhyOperator* op = NULL;
      ObTaskResultBuf res;
      if (OB_FAIL(job_->get_child_job(i, child))) {
        LOG_WARN("get child job failed", K(ret));
      } else if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is NULL", K(ret));
      } else if (OB_ISNULL(op = child->get_root_op())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get root operator failed", K(ret));
      } else if (OB_FAIL(PHY_DETERMINATE_TASK_TRANSMIT != op->get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not determinate task transmit", K(ret), KP(op));
      } else {
        const auto& result_mapping = static_cast<ObDeterminateTaskTransmit*>(op)->get_result_mapping();
        if (result_mapping.count() <= task_id.get_task_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task index exceed result mapping", K(ret));
        } else {
          const auto& mapping = result_mapping.at(task_id.get_task_id());
          FOREACH_X(slice, child_slices_, OB_SUCC(ret))
          {
            const auto& slice_id = (*slice)->get_ob_slice_id();
            if (child->get_job_id() == slice_id.get_job_id() && slice_id.get_task_id() >= mapping.task_range_.begin_ &&
                slice_id.get_task_id() < mapping.task_range_.end_ &&
                slice_id.get_slice_id() >= mapping.slice_range_.begin_ &&
                slice_id.get_slice_id() < mapping.slice_range_.end_) {
              res.reset();
              ObTaskControl* tc = NULL;
              if (OB_FAIL(child->get_task_control(*exec_ctx_, tc))) {
                LOG_WARN("fail get task ctrl", K(ret));
              } else if (tc->get_task_location(slice_id.get_task_id(), res.get_task_location())) {
                LOG_WARN("get task location failed", K(ret), K(slice_id));
              } else if (OB_FAIL(res.add_slice_event(**slice))) {
                LOG_WARN("add slice event failed", K(ret));
              } else if (OB_FAIL(child_results.push_back(res))) {
                LOG_WARN("array push back failed", K(ret));
              }
            }
          }  // end FOREACH_X
        }
      }
    }  // end for
  }

  return ret;
}

int ObDeterminateTaskSpliter::set_task_destination(
    const ObDeterminateTaskTransmit& transmit, const ObTaskID& task_id, ObTaskInfo& task)
{
  int ret = OB_SUCCESS;
  ObAddr dest;
  ObArray<ObAddr> previous;
  auto routing = transmit.get_task_routing();
  auto policy = transmit.get_task_route_policy();
  if (OB_ISNULL(job_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(routing)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL task routing", K(ret));
  } else if (OB_FAIL(task_executed_servers(exec_ctx_->get_task_exec_ctx().get_sys_job_id(), task_id, previous))) {
    LOG_WARN("get task executed servers failed",
        K(ret),
        "sys_job_id",
        exec_ctx_->get_task_exec_ctx().get_sys_job_id(),
        K(task_id));
  } else if (OB_FAIL(routing->route(policy, task, previous, dest))) {
    LOG_WARN("choose task destination failed", K(ret), K(policy), K(previous));
  } else {
    task.get_range_location().server_ = dest;
  }
  return ret;
}

int ObDeterminateTaskSpliter::task_executed_servers(
    const int64_t sys_job_id, const ObTaskID& task_id, common::ObIArray<common::ObAddr>& servers)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_ISNULL(exec_ctx_) || OB_ISNULL(exec_ctx_->get_sql_proxy())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(exec_ctx_));
  } else if (OB_FAIL(sql.assign_fmt("SELECT svr_ip, svr_port from %s WHERE job_id = %ld AND execution_id = %ld "
                                    "AND sql_job_id = %lu AND task_id = %ld",
                 OB_ALL_SQL_EXECUTE_TASK_TNAME,
                 sys_job_id,
                 task_id.get_execution_id(),
                 task_id.get_job_id(),
                 task_id.get_task_id()))) {
    LOG_WARN("assign sql failed", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(exec_ctx_->get_sql_proxy()->read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL result", K(ret));
      } else {
        char ip[OB_IP_STR_BUFF] = "";
        int32_t port = 0;
        ObAddr server;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next result failed", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
            break;
          }
          int64_t tmp = 0;
          EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip, OB_IP_STR_BUFF, tmp);
          UNUSED(tmp);  // make compiler happy
          EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int32_t);
          if (OB_FAIL(ret)) {
          } else if (!server.set_ip_addr(ip, port)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set server failed", K(ret), K(ip), K(port));
          } else if (OB_FAIL(servers.push_back(server))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDeterminateTaskSpliter::record_task(
    const int64_t sys_job_id, const ObTaskID& task_id, const ObTaskInfo& task, const int64_t slice_count)
{
  int ret = OB_SUCCESS;
  const int64_t max_task_info_len = 1 << 12;  // 4KB
  char ip[OB_IP_STR_BUFF] = "";
  ObDMLSqlSplicer dml;
  ObSqlString buf;
  if (OB_ISNULL(exec_ctx_) || OB_ISNULL(exec_ctx_->get_sql_proxy())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(exec_ctx_));
  } else if (!task.get_range_location().server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("convert address to string failed", K(ret));
  } else if (OB_FAIL(buf.reserve(max_task_info_len))) {
    LOG_WARN("string reserve failed", K(ret));
  } else {
    int64_t len = task.to_string(buf.ptr(), max_task_info_len);
    if (OB_FAIL(buf.set_length(len))) {
      LOG_WARN("set string length failed", K(ret), K(len));
    } else if (OB_FAIL(dml.add_pk_column("job_id", sys_job_id)) ||
               OB_FAIL(dml.add_pk_column("execution_id", task_id.get_execution_id())) ||
               OB_FAIL(dml.add_pk_column("sql_job_id", task_id.get_job_id())) ||
               OB_FAIL(dml.add_pk_column("task_id", task_id.get_task_id())) ||
               OB_FAIL(dml.add_pk_column("svr_ip", ip)) ||
               OB_FAIL(dml.add_pk_column("svr_port", task.get_range_location().server_.get_port())) ||
               OB_FAIL(dml.add_column("slice_count", slice_count)) || OB_FAIL(dml.add_column("task_stat", "INIT")) ||
               OB_FAIL(dml.add_column("task_result", 0)) ||
               OB_FAIL(dml.add_column(
                   "task_info", ObHexEscapeSqlStr(ObString(static_cast<int32_t>(buf.length()), buf.ptr()))))) {
      LOG_WARN("add column failed", K(ret));
    } else {
      ObDMLExecHelper exec(*exec_ctx_->get_sql_proxy(), OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert_update(OB_ALL_SQL_EXECUTE_TASK_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert on duplicate update failed", K(ret));
      }
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
