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

#include "ob_tenant_snapshot_scheduler.h"
#include "lib/utility/ob_tracepoint.h" // ERRSIM_POINT_DEF
#include "src/rootserver/ob_rs_async_rpc_proxy.h"
#include "src/rootserver/tenant_snapshot/ob_tenant_snapshot_util.h"
#include "src/rootserver/restore/ob_tenant_clone_util.h"
#include "share/backup/ob_tenant_archive_mgr.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/ls/ob_ls_table_operator.h"
#include "storage/tx/ob_ts_mgr.h"

namespace oceanbase
{
using namespace transaction::tablelock;

namespace rootserver
{
using namespace oceanbase::share;

ObTenantSnapshotScheduler::ObTenantSnapshotScheduler()
  : inited_(false),
    sql_proxy_(NULL),
    idle_time_us_(1)
{
}

ObTenantSnapshotScheduler::~ObTenantSnapshotScheduler()
{
  if (!has_set_stop()) {
    stop();
    wait();
  }
}

void ObTenantSnapshotScheduler::destroy()
{
  ObTenantThreadHelper::destroy();
  inited_ = false;
}

int ObTenantSnapshotScheduler::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.sql_proxy_));
  //TODO: SimpleLSService
  } else if (OB_FAIL(ObTenantThreadHelper::create("SnapSche", lib::TGDefIDs::SimpleLSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start thread", KR(ret));
  } else {
    sql_proxy_ = GCTX.sql_proxy_;
    inited_ = true;
  }
  return ret;
}

//TODO: wakeup and idle need to be improved
void ObTenantSnapshotScheduler::wakeup()
{
  ObTenantThreadHelper::wakeup();
}

int ObTenantSnapshotScheduler::idle()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObTenantThreadHelper::idle(idle_time_us_);
    idle_time_us_ = DEFAULT_IDLE_TIME;
  }
  return ret;
}

//TODO: Synchronous deletion of snapshots to avoid delayed resource release.
//TODO: Standby tenant stop this thread
void ObTenantSnapshotScheduler::do_work()
{
  LOG_INFO("[SNAPSHOT] tenant snapshot scheduler start");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else {
    idle_time_us_ = DEFAULT_IDLE_TIME;
    bool compatibility_satisfied = false;
    bool status_satisfied = false;
    const uint64_t meta_tenant_id = MTL_ID();
    const uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
    while (!has_set_stop()) {
      ObCurTraceId::init(GCTX.self_addr());
      if (!is_meta_tenant(meta_tenant_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected tenant id", KR(ret), K(meta_tenant_id));
      } else if (OB_FAIL(ObShareUtil::check_compat_version_for_clone_tenant(user_tenant_id, compatibility_satisfied))) {
        LOG_WARN("check tenant compatibility failed", KR(ret), K(user_tenant_id));
      } else if (!compatibility_satisfied) {
        ret = OB_OP_NOT_ALLOW;
        LOG_INFO("tenant data version is below 4.3", KR(ret), K(user_tenant_id), K(compatibility_satisfied));
      } else if (OB_FAIL(ObTenantSnapshotUtil::check_tenant_status(user_tenant_id, status_satisfied))) {
        LOG_WARN("check_tenant_status failed", KR(ret), K(user_tenant_id));
      } else if (!status_satisfied) {
        LOG_INFO("tenant status is not valid", K(user_tenant_id), K(status_satisfied));
      } else {
        //*********************************************************************
        //First, check whether creation or deletion jobs exist.
        //Note: only one creation job can exist at a time, num > 1 is illegal.
        //      the num of deletion jobs is not limited.
        //*********************************************************************
        //Then, If the creation job has not yet been processed, we process it.
        //      if it has been processed, we only need to check the result.
        //Note: if the result does not pass, the rpc will be resend to observers.
        //*********************************************************************
        //Then, We continue with the deletion jobs
        //Note: we don't care about the actual results of deletion jobs
        //      in this thread.
        //*********************************************************************
        ObArray<ObCreateSnapshotJob> create_jobs;
        ObArray<ObDeleteSnapshotJob> delete_jobs;
        if (OB_FAIL(get_tenant_snapshot_jobs_(create_jobs, delete_jobs))) {
          LOG_WARN("get tenant snapshot jobs failed", KR(ret), K(user_tenant_id));
        } else {
          if (create_jobs.count() > 0) {
            if (create_jobs.count() > 1) {
              //only one creation job can exist at a time, num > 1 is illegal!
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("unexpected creation job count", KR(ret), K(create_jobs));
            } else {
              ObCreateSnapshotJob &create_job = create_jobs.at(0);
              ObTraceIdGuard trace_guard(create_job.get_trace_id());
              if (OB_FAIL(prepare_for_create_tenant_snapshot_(create_job))) {
                LOG_WARN("prepare for snapshot failed", KR(ret), K(create_job));
              } else if (OB_FAIL(process_create_tenant_snapshot_(create_job))) {
                LOG_WARN("process create snapshot failed", KR(ret), K(create_job));
              }
              idle_time_us_ = PROCESS_IDLE_TIME;
            }
          }
          //Then, we just process the deletion jobs (Do not let creation job block deletion jobs)
          ret = OB_SUCCESS;
          if (delete_jobs.count() > 0) {
            if (OB_FAIL(process_delete_tenant_snapshots_(delete_jobs))) {
              LOG_WARN("process delete snapshots failed", KR(ret), K(delete_jobs));
            }
            idle_time_us_ = PROCESS_IDLE_TIME;
          }
        }
      }
      ret = OB_SUCCESS;
      idle();
    }
  }
}

int ObTenantSnapshotScheduler::get_tenant_snapshot_jobs_(
    ObArray<ObCreateSnapshotJob> &create_jobs,
    ObArray<ObDeleteSnapshotJob> &delete_jobs)
{
  int ret = OB_SUCCESS;
  create_jobs.reset();
  delete_jobs.reset();
  ObTenantSnapshotTableOperator table_op;
  ObArray<ObTenantSnapItem> items;
  uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  ObArbitrationServiceStatus arbitration_service_status;
  int64_t paxos_replica_num = OB_INVALID_COUNT;
  int64_t clone_job_num = 0;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const ObTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(schema_guard.get_tenant_info(user_tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", KR(ret), K(user_tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema is null", KR(ret));
    } else if (FALSE_IT(arbitration_service_status = tenant_schema->get_arbitration_service_status())) {
    } else if (OB_FAIL(tenant_schema->get_paxos_replica_num(schema_guard, paxos_replica_num))) {
      LOG_WARN("failed to get paxos replica num", KR(ret), KPC(tenant_schema));
    } else if (OB_INVALID_COUNT == paxos_replica_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected paxos_replica_num", KR(ret), K(paxos_replica_num));
    }
  }

  if (FAILEDx(table_op.init(user_tenant_id, sql_proxy_))) {
    LOG_WARN("failed to init table op", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(table_op.get_all_user_tenant_snap_items(items))) {
    LOG_WARN("failed to get snapshot items", KR(ret));
  } else {
    int64_t tenant_snapshot_creation_timeout = max(SNAPSHOT_CREATION_TIMEOUT, GCONF._ob_ddl_timeout);
    ARRAY_FOREACH_N(items, i, cnt) {
      const ObTenantSnapItem& item = items.at(i);
      if (ObTenantSnapStatus::CREATING == items.at(i).get_status()
          || ObTenantSnapStatus::DECIDED == items.at(i).get_status()) {
        ObCreateSnapshotJob create_job;
        if (OB_FAIL(build_tenant_snapshot_create_job_(item, arbitration_service_status,
                                                      paxos_replica_num,
                                                      tenant_snapshot_creation_timeout,
                                                      create_job))){
          LOG_WARN("fail to build snapshot create job", KR(ret), K(item),
                                                        K(arbitration_service_status),
                                                        K(paxos_replica_num));
        } else if (OB_FAIL(create_jobs.push_back(create_job))) {
          LOG_WARN("push back failed", KR(ret), K(create_job));
        }
      } else if (ObTenantSnapStatus::DELETING == items.at(i).get_status()) {
        ObDeleteSnapshotJob delete_job;
        if (OB_FAIL(build_tenant_snapshot_delete_job_(item, delete_job))) {
          LOG_WARN("fail to build snapshot delete job", KR(ret), K(item));
        } else if (OB_FAIL(delete_jobs.push_back(delete_job))) {
          LOG_WARN("push back failed", KR(ret), K(item), K(delete_job));
        }
      } else if (ObTenantSnapStatus::CLONING == items.at(i).get_status()) {
        clone_job_num++;
      } else if (ObTenantSnapStatus::FAILED == items.at(i).get_status()) {
        // when a tenant snapshot is created failed,
        // for the normal tenant snapshot, it will be setted as DELETING and be deleted directly;
        // for the fork tenant snapshot, due to it is created in a step of clone job, it will be
        // setted as FAILED and be deleted while the clone job has been recycled
      } else if (ObTenantSnapStatus::NORMAL == items.at(i).get_status()) {
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tenant snapshot status", KR(ret), K(items));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if ((create_jobs.count() > 1)
            || (create_jobs.count() + clone_job_num > 1)) {
    //only one creation job/restoration job can exist at a time, num > 1 is illegal!
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected tenant snapshot count", KR(ret), K(create_jobs), K(clone_job_num));
  }

  return ret;
}

int ObTenantSnapshotScheduler::build_tenant_snapshot_create_job_(
                                         const ObTenantSnapItem &item,
                                         const ObArbitrationServiceStatus &arbitration_service_status,
                                         const int64_t paxos_replica_num,
                                         const int64_t timeout,
                                         ObCreateSnapshotJob &job)
{
  int ret = OB_SUCCESS;
  job.reset();
  uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  ObTenantSnapJobItem job_item;
  ObTenantSnapshotTableOperator table_op;

  if (OB_UNLIKELY(!item.is_valid() || !arbitration_service_status.is_valid() ||
                  paxos_replica_num <= 0 || timeout < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(item), K(arbitration_service_status),
                                 K(paxos_replica_num), K(timeout));
  } else if (OB_FAIL(table_op.init(user_tenant_id, sql_proxy_))) {
    LOG_WARN("failed to init table op", KR(ret));
  } else if (OB_FAIL(table_op.get_tenant_snap_job_item(item.get_tenant_snapshot_id(),
                                                       ObTenantSnapOperation::CREATE,
                                                       job_item))) {
    LOG_WARN("fail to get tenant snapshot job item", KR(ret), K(item));
  } else if (OB_UNLIKELY(!job_item.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid job item", KR(ret), K(item), K(job_item));
  } else if (OB_FAIL(job.init(item.get_create_time(),
                              item.get_create_time() + timeout,
                              paxos_replica_num, arbitration_service_status,
                              job_item))) {
    LOG_WARN("fail to init create snapshot job", KR(ret), K(item), K(paxos_replica_num),
                                                 K(arbitration_service_status),
                                                 K(job_item));
  }

  return ret;
}

int ObTenantSnapshotScheduler::build_tenant_snapshot_delete_job_(const ObTenantSnapItem &item,
                                                                 ObDeleteSnapshotJob &job)
{
  int ret = OB_SUCCESS;
  job.reset();
  uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  ObTenantSnapJobItem job_item;
  ObTenantSnapshotTableOperator table_op;

  if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(item));
  } else if (OB_FAIL(table_op.init(user_tenant_id, sql_proxy_))) {
    LOG_WARN("failed to init table op", KR(ret));
  } else if (OB_FAIL(table_op.get_tenant_snap_job_item(item.get_tenant_snapshot_id(),
                                                       ObTenantSnapOperation::DELETE,
                                                       job_item))) {
    LOG_WARN("fail to get tenant snapshot job item", KR(ret), K(item));
  } else if (OB_UNLIKELY(!job_item.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid job item", KR(ret), K(item), K(job_item));
  } else if (OB_FAIL(job.init(job_item))) {
    LOG_WARN("fail to init delete snapshot job", KR(ret), K(item), K(job_item));
  }

  return ret;
}

//*************************************************************************
//First, close ls Add/Remove tasks and transfer tasks
//*************************************************************************
//Then, generate snapshot ls items (extract ls info from other inner table)
//*************************************************************************
//Then, check whether transfer scn of each ls is consistent
//*************************************************************************
//Then, generate ls replica base info (extract from inner table)
//*************************************************************************
//Last, insert data into inner table:__all_tenant_snapshot_ls
//*************************************************************************
//Last, insert data into inner table:__all_tenant_snapshot_ls_replica
//*************************************************************************
ERRSIM_POINT_DEF(ERRSIM_PREPARE_CREATE_SNAPSHOT_ERROR);
int ObTenantSnapshotScheduler::prepare_for_create_tenant_snapshot_(
    const ObCreateSnapshotJob &create_job)
{
  int ret = OB_SUCCESS;
  uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  const ObTenantSnapshotID &tenant_snapshot_id = create_job.get_tenant_snapshot_id();
  const int64_t paxos_replica_num = create_job.get_paxos_replica_num();
  ObTenantSnapshotTableOperator first_table_op;
  ObTenantSnapLSItem snap_ls_item;
  ObArray<ObTenantSnapLSItem> snap_ls_items;
  ObArray<ObTenantSnapLSReplicaSimpleItem> ls_replica_items;

  if (OB_UNLIKELY(!create_job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(create_job));
  } else if (OB_FAIL(first_table_op.init(user_tenant_id, sql_proxy_))) {
    LOG_WARN("failed to init table op", KR(ret), K(user_tenant_id));
  } else if (OB_SUCC(first_table_op.get_tenant_snap_ls_item(tenant_snapshot_id, SYS_LS, snap_ls_item))) {
    //do nothing
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("failed to get snapshot sys ls item", KR(ret), K(tenant_snapshot_id));
  } else {
    ret = OB_SUCCESS;
    if (OB_FAIL(generate_snap_ls_items_(tenant_snapshot_id, user_tenant_id, snap_ls_items))) {
      LOG_WARN("generate snap ls items failed", KR(ret), K(tenant_snapshot_id), K(user_tenant_id));
    } else if (OB_FAIL(generate_snap_ls_replica_base_info_(tenant_snapshot_id, user_tenant_id,
                                          snap_ls_items, paxos_replica_num, ls_replica_items))) {
      LOG_WARN("generate snap ls replica base items failed", KR(ret), K(tenant_snapshot_id), K(user_tenant_id),
                                                                K(snap_ls_items), K(paxos_replica_num));
    } else {
      ObTenantSnapshotTableOperator second_table_op;
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(user_tenant_id)))) {
        LOG_WARN("failed to start trans", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(second_table_op.init(user_tenant_id, &trans))) {
        LOG_WARN("failed to init table op", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(second_table_op.insert_tenant_snap_ls_items(snap_ls_items))) {
        LOG_WARN("failed to insert snapshot ls items", KR(ret), K(snap_ls_items));
      } else if (OB_FAIL(second_table_op.insert_tenant_snap_ls_replica_simple_items(ls_replica_items))) {
        LOG_WARN("failed to insert snapshot ls replica simple items", KR(ret), K(ls_replica_items));
      } else if (OB_UNLIKELY(ERRSIM_PREPARE_CREATE_SNAPSHOT_ERROR)) {
        ret = ERRSIM_PREPARE_CREATE_SNAPSHOT_ERROR;
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, KR(tmp_ret));
          ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    // if create_job is not valid, we should not delete the tenant snapshot
    // because we don't know whether the tenant_snapshot_id the create_job contains is valid
    if (!create_job.is_valid()) {
      tmp_ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(tmp_ret), K(create_job));
    } else if (OB_TMP_FAIL(create_tenant_snapshot_fail_(create_job))) {
      LOG_WARN("failed to execute create_tenant_snapshot_fail_", KR(tmp_ret),
                                        K(tenant_snapshot_id), K(user_tenant_id));
    }
  }

  return ret;
}

//TODO: whether "create_abort" ls need to be added.
int ObTenantSnapshotScheduler::generate_snap_ls_items_(
    const ObTenantSnapshotID &tenant_snapshot_id,
    const uint64_t user_tenant_id,
    ObArray<ObTenantSnapLSItem> &snap_ls_items)
{
  int ret = OB_SUCCESS;
  snap_ls_items.reset();
  ObLSAttrArray ls_attr_array;

  if (OB_UNLIKELY(!tenant_snapshot_id.is_valid() || !is_user_tenant(user_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(user_tenant_id));
  } else {
    share::ObLSAttrOperator ls_attr_operator(user_tenant_id, sql_proxy_);
    if (OB_FAIL(ls_attr_operator.get_all_ls_by_order(ls_attr_array))) {
      LOG_WARN("fail to get all ls", KR(ret), K(user_tenant_id));
    } else {
      ARRAY_FOREACH_N(ls_attr_array, i, cnt) {
        const ObLSAttr &ls_attr = ls_attr_array.at(i);
        ObTenantSnapLSItem snap_ls_item;
        if (OB_FAIL(snap_ls_item.init(user_tenant_id, tenant_snapshot_id, ls_attr))) {
          LOG_WARN("failed to init snap_ls_item", KR(ret), K(user_tenant_id),
                                                K(tenant_snapshot_id), K(ls_attr));
        } else if (OB_FAIL(snap_ls_items.push_back(snap_ls_item))) {
          LOG_WARN("failed to push back", KR(ret), K(snap_ls_item));
        }
      }
    }
  }
  return ret;
}

int ObTenantSnapshotScheduler::generate_snap_ls_replica_base_info_(
    const ObTenantSnapshotID &tenant_snapshot_id,
    const uint64_t user_tenant_id,
    const ObArray<ObTenantSnapLSItem> &snap_ls_items,
    const int64_t paxos_replica_num,
    ObArray<ObTenantSnapLSReplicaSimpleItem> &ls_replica_items)
{
  int ret = OB_SUCCESS;
  ls_replica_items.reset();
  ObArray<ObLSInfo> tenant_ls_infos;

  if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                  || !is_user_tenant(user_tenant_id)
                  || snap_ls_items.empty())
                  || OB_INVALID_COUNT == paxos_replica_num
                  || OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(user_tenant_id),
                          K(snap_ls_items), K(paxos_replica_num), KP(GCTX.lst_operator_));
  } else if (OB_FAIL(GCTX.lst_operator_->get_by_tenant(user_tenant_id, false, tenant_ls_infos))) {
    LOG_WARN("fail to execute get_by_tenant", KR(ret), K(user_tenant_id));
  } else {
    ObTenantSnapLSReplicaSimpleItem item;
    ObArray<const ObLSReplica *> valid_replicas;
    ARRAY_FOREACH_N(snap_ls_items, i, cnt) {
      const ObLSID &ls_id = snap_ls_items.at(i).get_ls_attr().get_ls_id();
      if (OB_FAIL(get_ls_valid_replicas_(ls_id, tenant_ls_infos, valid_replicas))) {
        LOG_WARN("fail to get ls valid replicas", KR(ret), K(ls_id), K(tenant_ls_infos));
      } else if (rootserver::majority(paxos_replica_num) > valid_replicas.count()) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("ls valid replica num is not satisfied", KR(ret), K(paxos_replica_num), K(valid_replicas),
                                                        K(ls_id), K(tenant_ls_infos));
      } else {
        ARRAY_FOREACH_N(valid_replicas, j, cnt) {
          item.reset();
          const ObLSReplica *replica = valid_replicas.at(j);
          if (OB_ISNULL(replica)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected replica ptr", KR(ret), KP(replica));
          } else if (OB_UNLIKELY(!replica->is_valid()
                                 || !replica->get_server().is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("replica is not valid", KR(ret), KPC(replica));
          } else if (OB_FAIL(item.init(user_tenant_id, tenant_snapshot_id, ls_id,
                                       replica->get_server(),
                                       ObLSSnapStatus::CREATING /*status*/,
                                       replica->get_zone(),
                                       replica->get_unit_id(),
                                       SCN::invalid_scn() /*begin_interval_scn*/,
                                       SCN::invalid_scn() /*end_interval_scn*/))) {
            LOG_WARN("fail to init item", KR(ret), K(user_tenant_id), K(tenant_snapshot_id),
                                      K(ls_id), KPC(replica));
          } else if (OB_FAIL(ls_replica_items.push_back(item))) {
            LOG_WARN("fail to push back", KR(ret), K(item));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantSnapshotScheduler::get_ls_valid_replicas_(
    const ObLSID &ls_id,
    const ObArray<ObLSInfo> &tenant_ls_infos,
    ObArray<const ObLSReplica *> &valid_replicas)
{
  int ret = OB_SUCCESS;
  valid_replicas.reset();
  bool find_ls = false;

  if (OB_UNLIKELY(!ls_id.is_valid()
                  || tenant_ls_infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tenant_ls_infos));
  } else {
    ARRAY_FOREACH_N(tenant_ls_infos, i, cnt) {
      const ObLSInfo &ls_info = tenant_ls_infos.at(i);
      if (ls_id == ls_info.get_ls_id()) {
        find_ls = true;
        const ObIArray<ObLSReplica> &replicas = ls_info.get_replicas();
        ARRAY_FOREACH_N(replicas, j, cnt) {
          const ObLSReplica &replica = replicas.at(j);
          if (REPLICA_TYPE_FULL == replica.get_replica_type()
              && REPLICA_STATUS_NORMAL == replica.get_replica_status()) {
            if (OB_FAIL(valid_replicas.push_back(&replica))) {
              LOG_WARN("fail to push back", KR(ret), K(replica), KP(&replica));
            }
          }
        }
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (!find_ls) {
        ret = OB_LS_NOT_EXIST;
        LOG_WARN("fail to find ls in tenant_ls_infos", KR(ret), K(ls_id), K(tenant_ls_infos));
      }
    }
  }

  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_PROCESS_CREATE_SNAPSHOT_ERROR);
int ObTenantSnapshotScheduler::process_create_tenant_snapshot_(
    const ObCreateSnapshotJob &create_job)
{
  int ret = OB_SUCCESS;
  ObUnitTableOperator unit_operator;
  common::ObArray<ObUnit> units;
  ObArray<ObAddr> addr_array;
  uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  const ObTenantSnapshotID &tenant_snapshot_id = create_job.get_tenant_snapshot_id();
  int64_t cur_time = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!create_job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(create_job));
  } else if (cur_time > create_job.get_create_expire_ts()) {
    ret = OB_TENANT_SNAPSHOT_TIMEOUT;
    LOG_WARN("create tenant snapshot timeout", KR(ret), K(create_job), K(cur_time));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sql proxy", KR(ret));
  } else if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
    LOG_WARN("failed to init unit operator", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(user_tenant_id, units))) {
    LOG_WARN("failed to get tenant unit", KR(ret), K(user_tenant_id));
  } else {
    ARRAY_FOREACH_N(units, i, cnt) {
      const ObUnit &unit = units.at(i);
      if (OB_FAIL(addr_array.push_back(unit.server_))) {
        LOG_WARN("failed to push back addr", KR(ret), K(unit.server_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    SCN clog_start_scn = SCN::invalid_scn();
    SCN snapshot_scn = SCN::invalid_scn();
    bool need_wait_archive_finish = false;
    bool need_wait_minority = false;

    if (OB_FAIL(send_create_tenant_snapshot_rpc_(tenant_snapshot_id, user_tenant_id, addr_array))) {
      LOG_WARN("failed to send create snapshot rpc", KR(ret), K(tenant_snapshot_id),
                                                     K(user_tenant_id), K(addr_array));
    } else if (OB_FAIL(check_create_tenant_snapshot_result_(create_job, clog_start_scn, snapshot_scn,
                                                            need_wait_minority))) {
      LOG_WARN("the result does not meet the requirements or other errors", KR(ret), K(create_job));
    } else if (need_wait_minority) {
      LOG_INFO("wait for minority to create snapshot", K(create_job));
    } else if (OB_FAIL(send_flush_ls_archive_rpc_(user_tenant_id, addr_array))) {
      LOG_WARN("fail to send ls flush rpc", KR(ret), K(user_tenant_id), K(addr_array));
    } else if (OB_FAIL(check_log_archive_finish_(user_tenant_id, snapshot_scn, need_wait_archive_finish))) {
      LOG_WARN("failed to execute check_log_archive_finish", KR(ret), K(user_tenant_id), K(snapshot_scn));
    } else if (need_wait_archive_finish) {
    } else if (OB_UNLIKELY(ERRSIM_PROCESS_CREATE_SNAPSHOT_ERROR)) {
      ret = ERRSIM_PROCESS_CREATE_SNAPSHOT_ERROR;
    } else if (OB_FAIL(finish_create_tenant_snapshot_(tenant_snapshot_id, user_tenant_id,
                                                      clog_start_scn, snapshot_scn))) {
      LOG_WARN("failed to execute finish_create_tenant_snapshot", KR(ret), K(tenant_snapshot_id),
                                                                  K(user_tenant_id), K(clog_start_scn),
                                                                  K(snapshot_scn));
    }
  }

  if (OB_FAIL(ret) && OB_REPLICA_NUM_NOT_ENOUGH != ret && OB_TIMEOUT != ret) {
    int tmp_ret = OB_SUCCESS;
    // if create_job is not valid, we should not delete the tenant snapshot
    // because we don't know whether the tenant_snapshot_id it contains is valid
    if (OB_UNLIKELY(!create_job.is_valid())) {
      tmp_ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(tmp_ret), K(create_job));
    } else if (OB_TMP_FAIL(create_tenant_snapshot_fail_(create_job))) {
      LOG_WARN("failed to execute create_tenant_snapshot_fail_", KR(tmp_ret),
                                        K(tenant_snapshot_id), K(user_tenant_id));
    }
  }

  return ret;
}

int ObTenantSnapshotScheduler::send_create_tenant_snapshot_rpc_(
    const ObTenantSnapshotID &tenant_snapshot_id,
    const uint64_t user_tenant_id,
    const ObArray<ObAddr> &addr_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObArray<int> return_code_array;
  obrpc::ObInnerCreateTenantSnapshotArg arg;
  int64_t timeout = max(GCONF.rpc_timeout, DEFAULT_TIMEOUT);

  if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                  || !is_user_tenant(user_tenant_id)
                  || addr_array.empty())
                  || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(user_tenant_id),
                                                K(addr_array), KP(GCTX.srv_rpc_proxy_));
  } else {
    arg.set_tenant_snapshot_id(tenant_snapshot_id);
    arg.set_tenant_id(user_tenant_id);
    rootserver::ObTenantSnapshotCreatorProxy create_snapshot_proxy(*GCTX.srv_rpc_proxy_,
                                              &obrpc::ObSrvRpcProxy::inner_create_tenant_snapshot);
    //traverse addr_array to send RPC
    ARRAY_FOREACH_N(addr_array, i, cnt) {
      const ObAddr &addr = addr_array.at(i);
      if (OB_TMP_FAIL(create_snapshot_proxy.call(addr, timeout,
                                                 GCONF.cluster_id, user_tenant_id, arg))) {
        LOG_WARN("failed to call async rpc", KR(tmp_ret), K(addr), K(user_tenant_id), K(arg));
      }
    }

    if (OB_TMP_FAIL(create_snapshot_proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
    } else {
      //if (return_code_array.count() != create_snapshot_proxy.get_dests().count()
      //    || return_code_array.count() != create_snapshot_proxy.get_args().count()
      //    || return_code_array.count() != create_snapshot_proxy.get_results().count()) {
      //  ret = OB_ERR_UNEXPECTED;
      //  LOG_WARN("count not match", KR(ret), K(return_code_array), K(create_snapshot_proxy.get_dests()),
      //                      K(create_snapshot_proxy.get_args()), K(create_snapshot_proxy.get_results()));
      //} else {
      //  ARRAY_FOREACH_N(return_code_array, i, cnt) {
      //    int res_ret = return_code_array.at(i);
      //    const ObAddr &addr = create_snapshot_proxy.get_dests().at(i);
      //    if (OB_SUCCESS != res_ret) {
      //      LOG_WARN("rpc execute failed", KR(res_ret), K(addr));
      //    }
      //  }
      //}
    }
  }
  return ret;
}

// Check whether the num of created snapshots (for each ls) meets the majority.
// TODO This function needs to be split into multiple functions
int ObTenantSnapshotScheduler::check_create_tenant_snapshot_result_(
    const ObCreateSnapshotJob &create_job,
    SCN &clog_start_scn,
    SCN &snapshot_scn,
    bool &need_wait_minority)
{
  int ret = OB_SUCCESS;
  clog_start_scn.reset();
  snapshot_scn.reset();
  need_wait_minority = false;
  uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  ObTenantSnapshotTableOperator table_op;
  ObArray<ObTenantSnapLSItem> snap_ls_items;
  // FIXME: create_job's member functions should not be called before make sure the create_job is valid;
  const ObTenantSnapshotID &tenant_snapshot_id = create_job.get_tenant_snapshot_id();
  int64_t paxos_replica_num = create_job.get_paxos_replica_num();
  const ObArbitrationServiceStatus &arbitration_service_status = create_job.get_arbitration_service_status();
  SCN tmp_clog_start_scn = SCN::max_scn();
  SCN tmp_snapshot_scn = SCN::min_scn();
  ObArray<ObAddr> failed_addrs;
  ObArray<ObAddr> processing_addrs;
  ObTenantArchiveRoundAttr round_attr;
  const int64_t fake_incarnation = 1;
  ObTenantSnapItem tenant_snap_item;
  ObMySQLTransaction trans;

  if (OB_UNLIKELY(!create_job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(create_job));
  } else if (paxos_replica_num <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected paxos_replica_num", KR(ret), K(paxos_replica_num));
  // TODO: Whether to use arbitration service is pending.
  } else if (arbitration_service_status.is_enable_like()) {
    if (paxos_replica_num != 2 && paxos_replica_num != 4) {
      ret = OB_ERR_UNDEFINED;
      LOG_WARN("locality must be 2F or 4F", KR(ret), K(paxos_replica_num));
    } else {
      paxos_replica_num++;
    }
  }

  if (FAILEDx(trans.start(sql_proxy_, gen_meta_tenant_id(user_tenant_id)))) {
    LOG_WARN("failed to start trans", KR(ret), K(gen_meta_tenant_id(user_tenant_id)));
  } else if (OB_FAIL(table_op.init(user_tenant_id, &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(user_tenant_id));
  // We need to decide snapshot_scn and clog_start_scn while locking tenant_snapshot_item;
  // because after locking tenant_snapshot_item, the storage node can no longer report it;
  // therefore, the snapshot scn we finally determined will be correct;
  } else if (OB_FAIL(table_op.get_tenant_snap_item(tenant_snapshot_id, true /*lock*/, tenant_snap_item))) {
    LOG_WARN("fail to get_tenant_snap_item", KR(ret), K(tenant_snapshot_id));
  } else if (ObTenantSnapStatus::DECIDED == tenant_snap_item.get_status()) {
    clog_start_scn = tenant_snap_item.get_clog_start_scn();
    snapshot_scn = tenant_snap_item.get_snapshot_scn();
    need_wait_minority = false;
  } else if (OB_FAIL(table_op.get_tenant_snap_ls_items(tenant_snapshot_id, snap_ls_items))) {
    LOG_WARN("failed to get tenant snapshot ls items", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(ObTenantArchiveMgr::get_tenant_current_round(user_tenant_id, fake_incarnation, round_attr))) {
    LOG_WARN("failed to get cur log archive round", KR(ret), K(user_tenant_id));
  } else {
    ARRAY_FOREACH_N(snap_ls_items, i, cnt) {
      int32_t succ_member = 0;
      failed_addrs.reset();
      processing_addrs.reset();
      const ObLSID &ls_id = snap_ls_items.at(i).get_ls_attr().get_ls_id();
      ObArray<ObTenantSnapLSReplicaSimpleItem> simple_items;
      if (OB_FAIL(table_op.get_tenant_snap_ls_replica_simple_items(tenant_snapshot_id, ls_id, simple_items))) {
        LOG_WARN("failed to get snapshot ls replica items by ls", KR(ret), K(tenant_snapshot_id), K(ls_id));
      } else {
        ARRAY_FOREACH_N(simple_items, j, cnt) {
          //for each ls, check whether status of each replica is "normal"
          //Note: "normal" means snapshot has been created successfully
          const ObTenantSnapLSReplicaSimpleItem &item = simple_items.at(j);
          if (ObLSSnapStatus::NORMAL == item.get_status()) {
            if (!item.get_begin_interval_scn().is_valid() || !item.get_end_interval_scn().is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("scn is not valid", KR(ret), K(item));
            } else if (item.get_begin_interval_scn() < round_attr.start_scn_) {
              LOG_INFO("checkpoint scn of the ls replica is smaller than archive scn", K(item), K(round_attr));
              ObTenantSnapLSReplicaSimpleItem new_item;

              if (OB_FAIL(new_item.assign(item))) {
                LOG_WARN("fail to assign", KR(ret), K(item));
              } else if (FALSE_IT(new_item.set_status(ObLSSnapStatus::FAILED))) {
              } else if (OB_FAIL(table_op.update_tenant_snap_ls_replica_item(new_item, nullptr))) {
                LOG_WARN("fail to update tenant snapshot ls replica item", KR(ret), K(new_item));
              } else if (OB_FAIL(failed_addrs.push_back(item.get_addr()))) {
                LOG_WARN("push back failed", KR(ret), K(item.get_addr()));
              }
            } else { // item.get_begin_interval_scn() >= round_attr.start_scn_
              //clog_start_scn is the minimum value among all the begin_interval_scn
              if (item.get_begin_interval_scn() < tmp_clog_start_scn) {
                tmp_clog_start_scn = item.get_begin_interval_scn();
              }
              //snapshot_scn is the maximum value among all the end_interval_scn
              if (item.get_end_interval_scn() > tmp_snapshot_scn) {
                tmp_snapshot_scn = item.get_end_interval_scn();
              }
              succ_member++;
            }
          } else if (ObLSSnapStatus::FAILED == item.get_status()) {
            if (OB_FAIL(failed_addrs.push_back(item.get_addr()))) {
              LOG_WARN("push back failed", KR(ret), K(item.get_addr()));
            }
          } else if (ObLSSnapStatus::CREATING == item.get_status()) {
            if (OB_FAIL(processing_addrs.push_back(item.get_addr()))) {
              LOG_WARN("push back failed", KR(ret), K(item.get_addr()));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected tenant snapshot status", KR(ret), K(item));
          }
        }
      }
      if (OB_SUCC(ret)) {
        int32_t failed_member = failed_addrs.count();
        int32_t creating_member = processing_addrs.count();
        // if (arbitration_service_status.is_enable_like()) {
        //   succ_member++;
        // }
        // attention:
        // in normal case, creating_member + succ_member + failed_member == paxos_replica_num
        // in transfer case, it might happened that creating_member + succ_member + failed_member > paxos_replica_num
        if (rootserver::majority(paxos_replica_num) > creating_member + succ_member) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("no possible to reach the majority", KR(ret), K(ls_id),
                                                        K(paxos_replica_num), K(succ_member),
                                                        K(creating_member), K(failed_member),
                                                        K(processing_addrs), K(failed_addrs));
        } else if (rootserver::majority(paxos_replica_num) > succ_member) {
          ret = OB_REPLICA_NUM_NOT_ENOUGH;
          LOG_WARN("success count less than majority, creating is in progress", KR(ret),
                K(paxos_replica_num), K(succ_member), K(ls_id), K(processing_addrs), K(failed_addrs));
        } else if (paxos_replica_num > succ_member && creating_member > 0) {
          need_wait_minority = true;
          LOG_INFO("the tenant snapshot has reached majority "
                   "but some replicas are still creating snapshot", K(ls_id),
                                                                    K(paxos_replica_num), K(succ_member),
                                                                    K(creating_member), K(failed_member),
                                                                    K(processing_addrs), K(failed_addrs));
        }
      }
    } // end for
  }

  bool whether_to_commit_trans = false;
  if (OB_SUCC(ret) && ObTenantSnapStatus::CREATING == tenant_snap_item.get_status()) { // majority snapshot has created successful
    SCN snapshot_scn_to_persist;
    bool need_persist_scn = true;
    if (need_wait_minority) {
      // considering the performance of tenant cloning is affected by the missing of snapshots,
      // we will wait a small interval as long as possible to make all ls replicas create snapshots successful.
      check_need_wait_minority_create_snapshot_(create_job, need_wait_minority);
    }
    if (!need_wait_minority) {
      clog_start_scn = tmp_clog_start_scn;
      snapshot_scn = tmp_snapshot_scn;

      ObAllTenantInfo tenant_info;
      if (OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret));
      } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                           user_tenant_id, GCTX.sql_proxy_, false/*for update*/, tenant_info))) {
        LOG_WARN("fail to get tenant info", K(ret), K(user_tenant_id));
      } else if (OB_FAIL(decide_tenant_snapshot_scn_(table_op, tenant_snapshot_id, tenant_info, snapshot_scn,
                             snapshot_scn_to_persist, need_persist_scn))) {
        LOG_WARN("fail to decide snapshot scn to persist", KR(ret), K(tenant_info), K(snapshot_scn));
      } else if (need_persist_scn && OB_FAIL(table_op.update_tenant_snap_item(
                                                 tenant_snapshot_id,
                                                 ObTenantSnapStatus::CREATING,
                                                 tenant_info.is_standby()
                                                     ? ObTenantSnapStatus::CREATING/*try next turn for standby tenant*/
                                                     : ObTenantSnapStatus::DECIDED,
                                                 snapshot_scn_to_persist,
                                                 clog_start_scn))) {
        LOG_WARN("fail to update snapshot status and interval scn", KR(ret), K(tenant_info),
                 K(tenant_snapshot_id), K(clog_start_scn), K(snapshot_scn_to_persist));
      } else if (tenant_info.is_standby() && OB_FAIL(check_standby_gts_exceed_snapshot_scn_(
                     table_op, tenant_info.get_tenant_id(), tenant_snapshot_id, snapshot_scn_to_persist))) {
        LOG_WARN("fail to check standby tenant gts_scn exceed sync_scn", KR(ret), K(tenant_info));
      } else {
        whether_to_commit_trans = true;
      }
    }
    LOG_INFO("results meet the requirement",
        KR(ret), K(create_job), K(clog_start_scn), K(snapshot_scn), K(snapshot_scn_to_persist),
        K(need_wait_minority), K(whether_to_commit_trans));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(whether_to_commit_trans))) {
      LOG_WARN("trans end failed", "is_commit", whether_to_commit_trans, KR(ret), KR(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObTenantSnapshotScheduler::decide_tenant_snapshot_scn_(
    ObTenantSnapshotTableOperator &table_op,
    const ObTenantSnapshotID &tenant_snapshot_id,
    const ObAllTenantInfo &tenant_info,
    const SCN &snapshot_scn,
    SCN &output_snapshot_scn,
    bool &need_persist_scn)
{
  int ret = OB_SUCCESS;
  output_snapshot_scn.reset();
  need_persist_scn = true;

  if (OB_UNLIKELY(!tenant_info.is_valid())
      || OB_UNLIKELY(!snapshot_scn.is_valid())
      || OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_info), K(snapshot_scn), K(tenant_snapshot_id));
  } else if (tenant_info.is_primary()) {
    SCN gts_scn;
    // TODO: Currently, how to get the maximum scn in ObLSMetaPackage has not yet been solved;
    //       The end_interval_scn reported by the storage node may be smaller than the actual
    //       required; We rely on gts when local snapshots are created to determine snapshot scn;
    if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_info.get_tenant_id(), GCONF.rpc_timeout, gts_scn))) {
      LOG_WARN("fail to get gts sync", KR(ret), K(tenant_info));
    } else {
      output_snapshot_scn = MAX(snapshot_scn, gts_scn);
    }
  } else if (tenant_info.is_standby()) {
    // get snapshot_scn from inner_table
    ObTenantSnapItem item;
    if (OB_UNLIKELY(!table_op.is_inited())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument");
    } else if (OB_FAIL(table_op.get_tenant_snap_item(tenant_snapshot_id, false/*need_lock*/, item))) {
      LOG_WARN("fail to get tenant snapshot item", KR(ret), K(tenant_snapshot_id));
    } else if (item.get_snapshot_scn().is_valid()) {
      // use snapshot scn in __all_tenant_snapshot table
      output_snapshot_scn = item.get_snapshot_scn();
      need_persist_scn = false; // snapshot_scn has already persisted, no need persist again
    } else {
      output_snapshot_scn = tenant_info.get_sync_scn();
    }
  } else {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unexpected tenant role", KR(ret), K(tenant_info));
  }
  return ret;
}

int ObTenantSnapshotScheduler::check_standby_gts_exceed_snapshot_scn_(
    ObTenantSnapshotTableOperator &table_op,
    const uint64_t &tenant_id,
    const ObTenantSnapshotID &tenant_snapshot_id,
    const SCN &snapshot_scn_to_check)
{
  int ret = OB_SUCCESS;
  bool finished = false;
  SCN gts_scn;
  if (OB_UNLIKELY(!snapshot_scn_to_check.is_valid())
      || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snapshot_scn_to_check), K(tenant_id));
  } else {
    const int64_t start_check_time = ObTimeUtility::current_time();
    const int64_t check_wait_interval = 1 * 1000L * 1000L; // 1s
    const int64_t sleep_time = 100 * 1000L; // 100ms
    if (OB_SUCC(ret) && !finished && ObTimeUtility::current_time() - start_check_time < check_wait_interval) {
      if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id, GCONF.rpc_timeout, gts_scn))) {
        LOG_WARN("fail to get gts sync", KR(ret), K(tenant_id));
      } else if (gts_scn < snapshot_scn_to_check) {
        // need to wait
        finished = false;
        LOG_TRACE("standby tenant gts_scn not exceed snapshot_scn, need to wait", K(tenant_id),
                  K(snapshot_scn_to_check), K(gts_scn));
        ob_usleep(sleep_time);
      } else {
        // good, gts_scn for standby tenant has already exceed sync_scn
        finished = true;
        ObTenantSnapItem item;
        LOG_INFO("standby tenant gts_scn exceeded sync_scn", K(tenant_id), K(snapshot_scn_to_check), K(gts_scn));
        if (OB_FAIL(table_op.get_tenant_snap_item(tenant_snapshot_id, false/*need_lock*/, item))) {
          LOG_WARN("fail to get tenant snapshot item", KR(ret), K(tenant_snapshot_id));
        } else if (OB_FAIL(table_op.update_tenant_snap_item(
                               item.get_snapshot_name(),
                               ObTenantSnapStatus::CREATING/*old_status*/,
                               ObTenantSnapStatus::DECIDED/*new_status*/))) {
          LOG_WARN("fail to update snapshot status", KR(ret), K(tenant_snapshot_id), K(item));
        }
      }
    }
  }
  return ret;
}

void ObTenantSnapshotScheduler::check_need_wait_minority_create_snapshot_(
    const ObCreateSnapshotJob &create_job,
    bool &need_wait_minority)
{
  int ret = OB_SUCCESS;
  uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  SCN gts;
  const int64_t current_time = ObTimeUtility::current_time();
  need_wait_minority = false;

  if (OB_UNLIKELY(!create_job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid create job", KR(ret), K(create_job));
  } else if (create_job.get_majority_succ_time() == OB_INVALID_TIMESTAMP) {
    ObTenantSnapshotTableOperator table_op;
    if (OB_FAIL(table_op.init(user_tenant_id, sql_proxy_))) {
      LOG_WARN("failed to init table op", KR(ret));
    } else if (OB_FAIL(table_op.update_tenant_snap_job_majority_succ_time(
                                                              create_job.get_tenant_snapshot_id(),
                                                              current_time))) {
      LOG_WARN("fail to update snapshot majority succ time", KR(ret), K(create_job), K(current_time));
    } else {
      need_wait_minority = true;
    }
  } else { // create_job.get_majority_succ_time() != OB_INVALID_TIMESTAMP
    if (OB_UNLIKELY(current_time < create_job.get_majority_succ_time())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid majority succ time", KR(ret), K(create_job));
    } else if (current_time - create_job.get_majority_succ_time() < WAIT_MINORITY_CREATE_SNAPSHOT_TIME) {
      need_wait_minority = true;
    }
  }
}

int ObTenantSnapshotScheduler::send_flush_ls_archive_rpc_(const uint64_t user_tenant_id,
                                                          const ObArray<ObAddr> &addr_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<int> return_code_array;
  obrpc::ObFlushLSArchiveArg arg;
  arg.tenant_id_ = user_tenant_id;
  int64_t timeout = max(GCONF.rpc_timeout, DEFAULT_TIMEOUT);
  rootserver::ObFlushLSArchiveProxy flush_ls_archive(*GCTX.srv_rpc_proxy_,
                                                     &obrpc::ObSrvRpcProxy::flush_ls_archive);
  return_code_array.reset();

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == user_tenant_id || addr_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), K(addr_array));
  } else {
    // traverse addr_array to send RPC
    ARRAY_FOREACH_N(addr_array, i, cnt) {
      const ObAddr &addr = addr_array.at(i);
      if (OB_TMP_FAIL(flush_ls_archive.call(addr, timeout,
                                            GCONF.cluster_id, user_tenant_id, arg))) {
        LOG_WARN("failed to call async rpc", KR(tmp_ret), K(addr), K(arg), K(user_tenant_id));
      }
    }

    //wait
    if (OB_TMP_FAIL(flush_ls_archive.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
    }

    // if (OB_SUCC(ret)) {
    //   if (return_code_array.count() != flush_ls_archive.get_dests().count()) {
    //     ret = OB_ERR_UNEXPECTED;
    //     LOG_WARN("count not match", KR(ret), K(return_code_array), K(flush_ls_archive.get_dests()));
    //   } else {
    //     ARRAY_FOREACH_N(return_code_array, i, cnt) {
    //       int res_ret = return_code_array.at(i);
    //       const ObAddr &addr = flush_ls_archive.get_dests().at(i);
    //       if (OB_SUCCESS != res_ret) {
    //         ret = res_ret;
    //         LOG_WARN("rpc execute failed", KR(ret), K(addr));
    //       }
    //     }
    //   }
    // }
  }

  return ret;
}

int ObTenantSnapshotScheduler::check_log_archive_finish_(
    const uint64_t user_tenant_id,
    const SCN &snapshot_scn,
    bool &need_wait)
{
  int ret = OB_SUCCESS;
  need_wait = false;
  ObTenantArchiveRoundAttr round_attr;
  int64_t fake_incarnation = 1;
  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id) || !snapshot_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), K(snapshot_scn));
  } else if (OB_FAIL(ObTenantArchiveMgr::get_tenant_current_round(user_tenant_id, fake_incarnation, round_attr))) {
    LOG_WARN("failed to get cur log archive round", KR(ret), K(user_tenant_id));
  } else if (round_attr.checkpoint_scn_ < snapshot_scn) {
    need_wait = true;
    LOG_WARN("need wait log archive checkpoint_scn exceed snapshot_scn", KR(ret),
                  K(round_attr.checkpoint_scn_), K(snapshot_scn));
  } else {
    LOG_INFO("log archive finish", KR(ret), K(round_attr.checkpoint_scn_), K(snapshot_scn));
  }
  return ret;
}

int ObTenantSnapshotScheduler::finish_create_tenant_snapshot_(
    const share::ObTenantSnapshotID &tenant_snapshot_id,
    const uint64_t user_tenant_id,
    const SCN &clog_start_scn,
    const SCN &snapshot_scn)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObTenantSnapshotTableOperator table_op;
  uint64_t data_version = 0;
  ObTenantSnapItem global_lock;

  if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                  || !is_user_tenant(user_tenant_id)
                  || !clog_start_scn.is_valid()
                  || !snapshot_scn.is_valid()
                  || OB_ISNULL(sql_proxy_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(user_tenant_id),
                                    K(clog_start_scn), K(snapshot_scn), KP(sql_proxy_));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(user_tenant_id)))) {
    LOG_WARN("failed to start trans", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(table_op.init(user_tenant_id, &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(table_op.update_tenant_snap_item(tenant_snapshot_id,
                                                      ObTenantSnapStatus::DECIDED,
                                                      ObTenantSnapStatus::NORMAL))) {
    LOG_WARN("failed to update snapshot status", KR(ret), K(user_tenant_id), K(tenant_snapshot_id));
  } else if (OB_FAIL(table_op.delete_tenant_snap_job_item(tenant_snapshot_id))) {
    LOG_WARN("failed to delete snapshot job item", KR(ret), K(user_tenant_id), K(tenant_snapshot_id));
  } else if (OB_FAIL(table_op.get_tenant_snap_item(
                                ObTenantSnapshotID(ObTenantSnapshotTableOperator::GLOBAL_STATE_ID),
                                true /*for update*/,
                                global_lock))) {
    LOG_WARN("failed to get special tenant snapshot item", KR(ret), K(user_tenant_id));
  } else if (ObTenantSnapStatus::CLONING == global_lock.get_status()) {
    // For fork tenant (a job type of tenant cloning), the status of global_lock is set as CLONING at beginning.
    // in this case, the global_lock should be unlocked after cloning tenant is finished
  } else if (OB_FAIL(ObTenantSnapshotUtil::unlock_tenant_snapshot_simulated_mutex_from_snapshot_task(
                                                                                  trans,
                                                                                  user_tenant_id,
                                                                                  ObTenantSnapStatus::CREATING,
                                                                                  snapshot_scn))) {
    LOG_WARN("failed to unlock tenant snapshot simulated mutex", KR(ret), K(user_tenant_id), K(snapshot_scn));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, KR(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("create tenant snapshot finished", K(tenant_snapshot_id));
  }
  return ret;
}

int ObTenantSnapshotScheduler::create_tenant_snapshot_fail_(const ObCreateSnapshotJob& create_job)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObTenantSnapshotTableOperator table_op;
  ObTenantSnapItem global_lock;

  if (OB_UNLIKELY(!create_job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(create_job));
  } else {
    const ObTenantSnapshotID &tenant_snapshot_id = create_job.get_tenant_snapshot_id();
    const uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());

    if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(user_tenant_id)))) {
      LOG_WARN("failed to start trans", KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(table_op.init(user_tenant_id, &trans))) {
      LOG_WARN("failed to init table op", KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(table_op.get_tenant_snap_item(
                                ObTenantSnapshotID(ObTenantSnapshotTableOperator::GLOBAL_STATE_ID),
                                true /*for update*/,
                                global_lock))) {
      LOG_WARN("failed to get special tenant snapshot item", KR(ret), K(user_tenant_id));
    } else {
      if (ObTenantSnapStatus::CLONING == global_lock.get_status()) {
        // For fork tenant (a job type of tenant cloning), the status of global_lock is set as CLONING at beginning.
        // in this case, when creating snapshot failed,
        // the snapshot and global_lock should only be released by clone job
        if (OB_FAIL(table_op.update_tenant_snap_item(tenant_snapshot_id,
                                                     ObTenantSnapStatus::CREATING,
                                                     ObTenantSnapStatus::DECIDED,
                                                     ObTenantSnapStatus::FAILED))) {
          LOG_WARN("failed to update snapshot status", KR(ret), K(user_tenant_id), K(tenant_snapshot_id));
        }
      } else {
        if (OB_FAIL(table_op.update_tenant_snap_item(tenant_snapshot_id,
                                                     ObTenantSnapStatus::CREATING,
                                                     ObTenantSnapStatus::DECIDED,
                                                     ObTenantSnapStatus::DELETING))) {
          LOG_WARN("failed to update snapshot status", KR(ret), K(user_tenant_id), K(tenant_snapshot_id));
        } else if (OB_FAIL(ObTenantSnapshotUtil::recycle_tenant_snapshot_ls_replicas(trans, user_tenant_id,
                                                                                     tenant_snapshot_id))) {
          LOG_WARN("fail to recycle tenant snapshot ls replicas", KR(ret), K(user_tenant_id), K(tenant_snapshot_id));
        } else if (OB_FAIL(table_op.delete_tenant_snap_job_item(tenant_snapshot_id))) {
          LOG_WARN("failed to delete snapshot job item", KR(ret), K(user_tenant_id), K(tenant_snapshot_id));
        } else if (OB_FAIL(table_op.insert_tenant_snap_job_item(
                ObTenantSnapJobItem(user_tenant_id,
                                    tenant_snapshot_id,
                                    ObTenantSnapOperation::DELETE,
                                    create_job.get_trace_id())))) {
          LOG_WARN("fail to insert tenant snapshot job item", KR(ret), K(create_job));
        } else if (OB_FAIL(ObTenantSnapshotUtil::unlock_tenant_snapshot_simulated_mutex_from_snapshot_task(
                                          trans, user_tenant_id, ObTenantSnapStatus::CREATING, SCN::invalid_scn()))) {
          LOG_WARN("failed to unlock tenant snapshot simulated mutex", KR(ret), K(user_tenant_id));
        }
      }
    }

    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, KR(tmp_ret));
        ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      }
    }
  }

  return ret;
}

int ObTenantSnapshotScheduler::process_delete_tenant_snapshots_(
    const ObArray<ObDeleteSnapshotJob> &delete_jobs)
{
  int ret = OB_SUCCESS;
  uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  ObTenantSnapshotTableOperator table_op;
  ObArray<ObAddr> addr_array;

  if (OB_UNLIKELY(delete_jobs.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(delete_jobs));
  } else if (OB_FAIL(table_op.init(user_tenant_id, sql_proxy_))) {
    LOG_WARN("failed to init table op", KR(ret), K(user_tenant_id));
  } else {
    ARRAY_FOREACH_N(delete_jobs, i, cnt) {
      //process each tenant_snapshot
      const ObDeleteSnapshotJob& delete_job = delete_jobs.at(i);
      ObTraceIdGuard trace_guard(delete_job.get_trace_id());
      addr_array.reset();
      const ObTenantSnapshotID &tenant_snapshot_id = delete_job.get_tenant_snapshot_id();
      if (OB_FAIL(table_op.get_tenant_snap_related_addrs(tenant_snapshot_id, addr_array))) {
        LOG_WARN("failed to get snapshot related addrs", KR(ret), K(tenant_snapshot_id));
      } else if (addr_array.empty()) {
        //may be that creating the snapshot failed during the preparation phase.
        LOG_INFO("addr_array in __all_tenant_snapshot_ls_replica is empty", KR(ret), K(tenant_snapshot_id));
      } else if (OB_FAIL(send_delete_tenant_snapshot_rpc_(tenant_snapshot_id, user_tenant_id, addr_array))) {
        LOG_WARN("failed to send delete snapshot rpc", KR(ret), K(tenant_snapshot_id),
                                                            K(user_tenant_id), K(addr_array));
      }

      int64_t cur_time = ObTimeUtility::current_time();
      if (cur_time > delete_job.get_job_start_time() + SNAPSHOT_DELETION_TIMEOUT) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(finish_delete_tenant_snapshot_(tenant_snapshot_id, user_tenant_id))) {
          LOG_WARN("failed to execute finish_delete_tenant_snapshot_", KR(tmp_ret),
                                                  K(tenant_snapshot_id), K(user_tenant_id));
        }
      }
    }
  }

  return ret;
}

int ObTenantSnapshotScheduler::send_delete_tenant_snapshot_rpc_(
    const ObTenantSnapshotID &tenant_snapshot_id,
    const uint64_t user_tenant_id,
    const ObArray<ObAddr> &addr_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObArray<int> return_code_array;
  obrpc::ObInnerDropTenantSnapshotArg arg;
  int64_t timeout = max(GCONF.rpc_timeout, DEFAULT_TIMEOUT);

  if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                  || !is_user_tenant(user_tenant_id)
                  || addr_array.empty())
                  || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(user_tenant_id),
                                                K(addr_array), KP(GCTX.srv_rpc_proxy_));
  } else {
    arg.set_tenant_snapshot_id(tenant_snapshot_id);
    arg.set_tenant_id(user_tenant_id);
    rootserver::ObTenantSnapshotDropperProxy delete_snapshot_proxy(*GCTX.srv_rpc_proxy_,
                                            &obrpc::ObSrvRpcProxy::inner_drop_tenant_snapshot);
    //traverse addr_array to send RPC
    ARRAY_FOREACH_N(addr_array, i, cnt) {
      const ObAddr &addr = addr_array.at(i);
      if (OB_TMP_FAIL(delete_snapshot_proxy.call(addr, timeout,
                                                 GCONF.cluster_id, user_tenant_id, arg))) {
        LOG_WARN("failed to call async rpc", KR(tmp_ret), K(addr), K(user_tenant_id), K(arg));
      }
    }

    //wait
    if (OB_TMP_FAIL(delete_snapshot_proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
    } else {
      //if (return_code_array.count() != delete_snapshot_proxy.get_dests().count()
      //    || return_code_array.count() != delete_snapshot_proxy.get_args().count()
      //    || return_code_array.count() != delete_snapshot_proxy.get_results().count()) {
      //  ret = OB_ERR_UNEXPECTED;
      //  LOG_WARN("count not match", KR(ret), K(return_code_array), K(delete_snapshot_proxy.get_dests()),
      //                      K(delete_snapshot_proxy.get_args()), K(delete_snapshot_proxy.get_results()));
      //} else {
      //  ARRAY_FOREACH_N(return_code_array, i, cnt) {
      //    int res_ret = return_code_array.at(i);
      //    const ObAddr &addr = delete_snapshot_proxy.get_dests().at(i);
      //    if (OB_SUCCESS != res_ret) {
      //      LOG_WARN("rpc execute failed", KR(res_ret), K(addr));
      //    }
      //  }
      //}
    }
  }

  return ret;
}

int ObTenantSnapshotScheduler::finish_delete_tenant_snapshot_(
    const ObTenantSnapshotID &tenant_snapshot_id,
    const uint64_t user_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotTableOperator table_op;
  ObMySQLTransaction trans;

  if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                  || !is_user_tenant(user_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(user_tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(user_tenant_id)))) {
    LOG_WARN("failed to start trans", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(table_op.init(user_tenant_id, &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(table_op.delete_tenant_snap_job_item(tenant_snapshot_id))) {
    LOG_WARN("failed to delete snapshot job item",
        KR(ret), K(user_tenant_id), K(tenant_snapshot_id));
  } else if (OB_FAIL(table_op.delete_tenant_snap_item(tenant_snapshot_id))) {
    LOG_WARN("failed to delete snapshot item", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(table_op.delete_tenant_snap_ls_items(tenant_snapshot_id))) {
    LOG_WARN("failed to delete snapshot ls item", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(table_op.delete_tenant_snap_ls_replica_items(tenant_snapshot_id))) {
    LOG_WARN("failed to delete snapshot ls replica item", KR(ret), K(tenant_snapshot_id));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, KR(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("delete tenant snapshot finished", K(tenant_snapshot_id));
  }

  return ret;
}

}
}
