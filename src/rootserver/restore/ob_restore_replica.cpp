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

#define USING_LOG_PREFIX RS

#include "ob_restore_replica.h"
#include "common/ob_partition_key.h"
#include "share/restore/ob_restore_args.h"
#include "share/restore/ob_restore_uri_parser.h"
#include "observer/ob_restore_ctx.h"
#include "rootserver/ob_balance_info.h"
#include "rootserver/restore/ob_restore_info.h"
#include "rootserver/ob_rebalance_task_mgr.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_balance_group_container.h"
#include "share/schema/ob_schema_getter_guard.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;

ObRestoreReplica::ObRestoreReplica(ObRestoreMgrCtx& restore_ctx, RestoreJob& job_info, const volatile bool& is_stop)
    : is_stop_(is_stop), ctx_(restore_ctx), stat_(job_info, restore_ctx.sql_proxy_), job_info_(job_info)
{}

ObRestoreReplica::~ObRestoreReplica()
{}

int ObRestoreReplica::restore()
{
  int ret = OB_SUCCESS;
  LOG_INFO("restore start");
  if (!ctx_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid restore ctx", K(ret));
  } else {
    ret = do_restore();
    if (OB_CANCELED == ret) {
      ret = OB_SUCCESS;  // abort quietly while rs is switched.
      LOG_INFO("Restore process cancelled. Abort without error", K(ret));
    }
  }
  return ret;
}

int ObRestoreReplica::do_restore()
{
  int ret = OB_SUCCESS;
  int64_t task_cnt = 0;
  bool has_more = true;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(stat_.gather_stat())) {
    LOG_WARN("fail init restore stat", K(ret));
  } else if (OB_FAIL(update_tenant_stat(schema_guard))) {
    LOG_WARN("fail update tenant_stat", K(ret));
  } else if (OB_FAIL(update_progress())) {
    LOG_WARN("fail update progress", K(ret));
  } else if (OB_FAIL(check_has_more_task(has_more))) {
    LOG_WARN("fail check if has more restore task", K(ret));
  } else if (!has_more) {
    if (OB_FAIL(finish_restore())) {
      LOG_WARN("fail finish restore", K(ret));
    }
  } else if (OB_FAIL(schedule_restore_task(task_cnt))) {
    LOG_WARN("fail schedule restore task", K(ret));
  } else if (OB_FAIL(schedule_restore_follower_task(task_cnt))) {
    LOG_WARN("fail schedule follower rebuild", K(ret));
  }
  return ret;
}

int ObRestoreReplica::update_tenant_stat(share::schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  if (stat_.partition_task_.count() > 0) {
    uint64_t tenant_id = stat_.partition_task_.at(0).tenant_id_;
    TenantBalanceStat& ts = *(ctx_.tenant_stat_);
    common::ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_BALANCER);
    balancer::TenantSchemaGetter stat_finder(tenant_id);
    balancer::ObLeaderBalanceGroupContainer balance_group_container(schema_guard, stat_finder, allocator);
    ts.reuse();
    if (OB_INVALID_TENANT_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
    } else if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get schema manager failed", K(ret));
    } else if (OB_FAIL(balance_group_container.init(tenant_id))) {
      LOG_WARN("fail to init balance group container", K(ret), K(tenant_id));
    } else if (OB_FAIL(balance_group_container.build())) {
      LOG_WARN("fail to build balance index builder", K(ret));
    } else if (OB_FAIL(ts.gather_stat(tenant_id, &schema_guard, balance_group_container.get_hash_index()))) {
      LOG_WARN("gather tenant balance statistics failed", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObRestoreReplica::update_progress()
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *(ctx_.tenant_stat_);
  // restore task is finished when is_restore_ = false.
  FOREACH_X(task, stat_.partition_task_, OB_SUCC(ret))
  {
    FOREACH_X(p, ts.all_partition_, OB_SUCC(ret))
    {
      if (p->table_id_ == task->table_id_ && p->partition_id_ == task->partition_id_) {
        int64_t restored_cnt = 0;
        FOR_BEGIN_END_E(r, *p, ts.all_replica_, OB_SUCC(ret))
        {
          if (0 == r->is_restore_ && r->is_in_service() && r->is_in_member_list() &&
              ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
            restored_cnt++;
          }
        }
        // all paxos members has restored.
        if (restored_cnt >= p->schema_replica_cnt_) {
          int tmp_ret = mark_task_done(*task);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail mark task done", K(restored_cnt), K(*task), K(tmp_ret));
          } else {
            LOG_WARN("has one restore task finish", K(restored_cnt), K(*task));
          }
        }
        break;
      }
    }  // end find partition in ts
  }    // try next task

  // restore job is finished when all restore tasks are finished.
  bool job_done = true;
  FOREACH_X(task, stat_.partition_task_, OB_SUCC(ret))
  {
    if (task->job_id_ == job_info_.job_id_ && task->status_ != RESTORE_DONE) {
      job_done = false;
      break;
    }
  }
  if (job_done) {
    int tmp_ret = mark_job_done();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail mark job done", K_(job_info), K(tmp_ret));
    }
  }
  return ret;
}

int ObRestoreReplica::finish_restore()
{
  int ret = OB_SUCCESS;
  ObRestoreTableOperator restore_op;
  if (OB_FAIL(restore_op.init(ctx_.sql_proxy_))) {
    LOG_WARN("fail init restore op", K(ret));
  } else {
    if (RESTORE_DONE != job_info_.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected job status", K_(job_info), K(ret));
    } else if (OB_FAIL(restore_op.update_job_status(job_info_.job_id_, RESTORE_DONE))) {
      LOG_WARN("fail recycle finished job", K_(job_info), K(ret));
    } else if (OB_ISNULL(ctx_.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else {
      RS_JOB_COMPLETE(job_info_.job_id_, ret, *(ctx_.sql_proxy_));
    }
  }
  return ret;
}

int ObRestoreReplica::check_has_more_task(bool& has_more)
{
  int ret = OB_SUCCESS;
  has_more = false;
  if (RESTORE_DONE != stat_.job_info_.status_) {
    has_more = true;
  }
  return ret;
}

int ObRestoreReplica::schedule_restore_task(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  int64_t max_task_once = ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT;
  int64_t generated_cnt = 0;

  // update progress
  if (OB_ISNULL(ctx_.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret));
  } else if (stat_.partition_task_.count() > 0) {
    int64_t done = 0;
    FOREACH(partition, stat_.partition_task_)
    {
      if (RESTORE_DONE == partition->status_) {
        done++;
      }
    }
    int64_t job_id = stat_.job_info_.job_id_;
    int64_t progress = done * 100 / stat_.partition_task_.count();
    RS_JOB_UPDATE_PROGRESS(job_id, progress, *(ctx_.sql_proxy_));
  }

  FOREACH_X(partition, stat_.partition_task_, OB_SUCC(ret) && OB_SUCC(check_stop()) && generated_cnt < max_task_once)
  {
    bool part_need_restore = false;
    if (OB_FAIL(check_if_need_restore_partition(*partition, part_need_restore))) {
      LOG_WARN("fail check if partition need restore", K(*partition), K(ret));
    } else if (part_need_restore) {
      ObPartitionKey pkey;
      ObRestoreArgs restore_arg;
      ObRestoreTask task;
      ObRestoreTaskInfo task_info;
      common::ObArray<ObRestoreTaskInfo> task_info_array;
      OnlineReplica dst;
      TenantBalanceStat& ts = *(ctx_.tenant_stat_);
      const char* comment = balancer::RESTORE_REPLICA;
      Partition part;  // only require table_id, partition_id fields
      part.table_id_ = partition->table_id_;
      part.partition_id_ = partition->partition_id_;
      const uint64_t tenant_id = extract_tenant_id(partition->table_id_);
      if (OB_FAIL(ts.gen_partition_key(part, pkey))) {
        LOG_WARN("fail gen pkey", K(ret));
      } else if (OB_FAIL(ObRestoreURIParser::parse(partition->job_->backup_uri_, restore_arg))) {
        LOG_WARN("fail init restore arg", K(ret));
      } else if (OB_FAIL(ObRestoreURIParserHelper::set_data_version(restore_arg))) {
        LOG_WARN("fail set data version", K(restore_arg), K(ret));
      } else if (OB_FAIL(choose_restore_dest_server(pkey, dst))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_WARN("can't find available restore dest server. ignore partition for now.", K(ret), K(pkey));
          ret = OB_SUCCESS;  // ignore this partition for now
        } else {
          LOG_WARN("fail choose restore dest server", K(ret), K(pkey));
        }
      } else {
        restore_arg.backup_schema_id_ = partition->backup_table_id_;
        restore_arg.partition_id_ = partition->partition_id_;
        restore_arg.schema_id_pair_.schema_id_ = partition->table_id_;
        restore_arg.schema_id_pair_.backup_schema_id_ = partition->backup_table_id_;
        if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_version(tenant_id, restore_arg.schema_version_))) {
          LOG_WARN("fail to get tenant schema version", K(ret), K(tenant_id));
        } else if (OB_FAIL(restore_arg.set_schema_id_pairs(partition->schema_id_pairs_))) {
          STORAGE_LOG(WARN, "failed to add index id pair", K(ret));
        } else if (OB_FAIL(task_info.build(pkey, restore_arg, dst))) {
          LOG_WARN("fail build restore task", K(ret), K(pkey));
        } else if (OB_FAIL(task_info_array.push_back(task_info))) {
          LOG_WARN("fail push task info", K(pkey), K(ret));
        } else if (OB_FAIL(task.build(task_info_array, dst.get_server(), comment))) {
          LOG_WARN("fail build restore task", K(pkey), K(ret));
        } else if (OB_FAIL(ctx_.task_mgr_->add_task(task, task_cnt))) {
          LOG_WARN("fail add task to task_mgr", K(ret), K(task));
        } else {
          int tmp_ret = mark_task_doing(*partition);
          LOG_INFO("generate one partition restore task", "task", *partition, K(ret), K(tmp_ret));
          generated_cnt++;
        }
      }
    }
  }
  return ret;
}

int ObRestoreReplica::schedule_restore_follower_task(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  int64_t max_task_once = ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT;
  int64_t generated_cnt = 0;

  /*
   * Schedule follower's task when:
   * 1. leader is existed and is restored.
   * 2. follower's is_restore = 1.
   */
  Replica leader;
  TenantBalanceStat& ts = *(ctx_.tenant_stat_);
  FOREACH_X(task, stat_.partition_task_, OB_SUCC(ret) && OB_SUCC(check_stop()) && generated_cnt < max_task_once)
  {
    FOREACH_X(p, ts.all_partition_, OB_SUCC(ret))
    {
      if (p->table_id_ == task->table_id_ && p->partition_id_ == task->partition_id_) {
        bool has_leader = false;
        // use leader replica as data source
        FOR_BEGIN_END_E(r, *p, ts.all_replica_, OB_SUCC(ret))
        {
          if (r->is_strong_leader() && 0 == r->is_restore_) {
            has_leader = true;
            leader = *r;
          }
        }
        FOR_BEGIN_END_E(r, *p, ts.all_replica_, has_leader && OB_SUCC(ret))
        {
          if (is_follower(r->role_) && 1 == r->is_restore_ && r->server_->can_migrate_in()) {
            Replica& replica = *r;
            if (OB_FAIL(restore_follower_replica(*p, leader, replica, task_cnt))) {
              LOG_WARN("fail restore replica", K(replica), K(ret));
            } else {
              generated_cnt++;
            }
            break;  // For each partition, only schedule one follower restore task at most in one round.
          }
        }
        break;  // partition found,no more search
      }
    }  // end find partition in ts
  }    // try next task
  return ret;
}

int ObRestoreReplica::restore_follower_replica(Partition& part, Replica& leader, Replica& r, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;

  ObPartitionKey pkey;
  ObCopySSTableTask task;
  ObCopySSTableTaskInfo task_info;
  common::ObArray<ObCopySSTableTaskInfo> task_info_array;
  OnlineReplica dst;
  TenantBalanceStat& ts = *(ctx_.tenant_stat_);
  const char* comment = balancer::RESTORE_FOLLOWER_REPLICA;
  const ObCopySSTableType type = OB_COPY_SSTABLE_TYPE_RESTORE_FOLLOWER;
  ObReplicaMember data_src = ObReplicaMember(
      leader.server_->server_, ObTimeUtility::current_time(), leader.replica_type_, leader.get_memstore_percent());
  int64_t data_size = leader.load_factor_.get_disk_used();
  if (OB_FAIL(ts.gen_partition_key(part, pkey))) {
    LOG_WARN("fail init restore arg", K(ret));
  } else if (OB_FAIL(choose_restore_dest_server(r, dst))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("can't find available restore dest server. ignore partition for now.", K(ret), K(pkey));
      ret = OB_SUCCESS;  // ignore this partition for now
    } else {
      LOG_WARN("fail choose restore dest server", K(ret), K(pkey));
    }
  } else if (OB_FAIL(task_info.build(pkey, dst, data_src))) {
    LOG_WARN("fail build restore task", K(ret), K(pkey));
  } else if (FALSE_IT(task_info.set_transmit_data_size(data_size))) {
    // nop
  } else if (OB_FAIL(task_info_array.push_back(task_info))) {
    LOG_WARN("fail push task info", K(pkey), K(ret));
  } else if (OB_FAIL(task.build(task_info_array, type, dst.member_.get_server(), comment))) {
    LOG_WARN("fail build restore task", K(pkey), K(ret));
  } else if (OB_FAIL(ctx_.task_mgr_->add_task(task, task_cnt))) {
    LOG_WARN("fail add task to task_mgr", K(ret), K(task));
  } else {
    LOG_INFO("generate one partition restore rebuild task", K(ret));
  }
  return ret;
}

int ObRestoreReplica::check_if_need_restore_partition(const PartitionRestoreTask& task, bool& need)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *(ctx_.tenant_stat_);
  switch (task.status_) {
    case RESTORE_INIT: {
      need = true;
      break;
    }
    case RESTORE_DONE: {
      need = false;
      break;
    }
    case RESTORE_FAIL: {
      need = true;
      break;
    }
    case RESTORE_DOING: {
      need = false;
      FOREACH_X(p, ts.all_partition_, OB_SUCC(ret))
      {
        if (p->table_id_ == task.table_id_ && p->partition_id_ == task.partition_id_) {
          FOR_BEGIN_END_E(r, *p, ts.all_replica_, OB_SUCC(ret))
          {
            if (r->is_strong_leader() && r->is_restore_ && r->server_->can_migrate_in() &&
                r->server_->can_migrate_out()) {
              need = true;
              break;
            }
          }
          break;
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not reach here", K(task), K(ret));
      break;
    }
  }
  return ret;
}

int ObRestoreReplica::choose_restore_dest_server(const ObPartitionKey& pkey, OnlineReplica& dst)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *(ctx_.tenant_stat_);
  bool found = false;
  FOREACH_X(p, ts.all_partition_, OB_SUCC(ret) && !found)
  {
    if (p->table_id_ == pkey.get_table_id() && p->partition_id_ == pkey.get_partition_id()) {
      FOR_BEGIN_END_E(r, *p, ts.all_replica_, OB_SUCC(ret) && !found)
      {
        LOG_INFO("try migrate in", K(pkey), "data_version", r->data_version_, "restore", r->is_restore_);
        if (r->server_->can_migrate_in() && r->is_strong_leader()) {
          ObUnit& unit = r->unit_->info_.unit_;
          dst.unit_id_ = unit.unit_id_;
          dst.zone_ = unit.zone_;
          dst.member_ =
              ObReplicaMember(r->server_->server_, r->member_time_us_, r->replica_type_, r->get_memstore_percent());
          found = true;
        }
      }
      break;
    }
  }
  if (!found && OB_SUCC(ret)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can't find any matching replica for pkey", K(pkey), K(ret));
  }
  return ret;
}

int ObRestoreReplica::choose_restore_dest_server(const Replica& r, OnlineReplica& dst)
{
  int ret = OB_SUCCESS;
  if (r.server_->can_migrate_in() && is_follower(r.role_) && 1 == r.is_restore_) {
    ObUnit& unit = r.unit_->info_.unit_;
    dst.unit_id_ = unit.unit_id_;
    dst.zone_ = unit.zone_;
    dst.member_ = ObReplicaMember(r.server_->server_, r.member_time_us_, r.replica_type_, r.get_memstore_percent());
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can't find any matching replica", K(r), K(ret));
  }
  return ret;
}

int ObRestoreReplica::update_task_status(const PartitionRestoreTask& t, RestoreTaskStatus status)
{
  int ret = OB_SUCCESS;
  ObRestoreTableOperator restore_op;
  if (OB_FAIL(restore_op.init(ctx_.sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else if (OB_FAIL(restore_op.update_task_status(t.tenant_id_, t.table_id_, t.partition_id_, status))) {
    LOG_WARN("update task status fail", K(t), K(status), K(ret));
  }
  return ret;
}

int ObRestoreReplica::update_job_status(RestoreTaskStatus status)
{
  int ret = OB_SUCCESS;
  ObRestoreTableOperator restore_op;
  if (OB_FAIL(restore_op.init(ctx_.sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else if (OB_FAIL(restore_op.update_job_status(job_info_.job_id_, status))) {
    LOG_WARN("update job status fail", K_(job_info), K(status), K(ret));
  }
  return ret;
}

int ObRestoreReplica::mark_task_doing(PartitionRestoreTask& task)
{
  int ret = OB_SUCCESS;
  if (RESTORE_DOING == task.status_) {
    // nop
  } else if (OB_FAIL(update_task_status(task, RESTORE_DOING))) {
    LOG_WARN("fail update task status", K(task), K(ret));
  } else {
    task.status_ = RESTORE_DOING;
  }
  return ret;
}
int ObRestoreReplica::mark_task_done(PartitionRestoreTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_task_status(task, RESTORE_DONE))) {
    LOG_WARN("fail update task status", K(task), K(ret));
  } else {
    task.status_ = RESTORE_DONE;
  }
  return ret;
}

int ObRestoreReplica::mark_job_done()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_job_status(RESTORE_DONE))) {
    LOG_WARN("fail update job status", K_(job_info), K(ret));
  } else {
    job_info_.status_ = RESTORE_DONE;
  }
  return ret;
}
