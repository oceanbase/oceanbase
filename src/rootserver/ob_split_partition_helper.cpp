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

#include "rootserver/ddl_task/ob_sys_ddl_util.h" // for ObSysDDLSchedulerUtil
#include "rootserver/ob_split_partition_helper.h"
#include "rootserver/ob_tenant_balance_service.h"
#include "share/tablet/ob_tablet_to_table_history_operator.h"
#include "src/share/scheduler/ob_partition_auto_split_helper.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "src/rootserver/ob_root_service.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace rootserver
{

ObSplitPartitionHelper::~ObSplitPartitionHelper()
{
  if (nullptr != tablet_creator_) {
    tablet_creator_->~ObTabletCreator();
    allocator_.free(tablet_creator_);
    tablet_creator_ = nullptr;
  }
}

int ObSplitPartitionHelper::execute(ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(upd_table_schemas_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty upd table schemas", K(ret));
  } else if (OB_FAIL(check_allow_split(schema_guard_, *upd_table_schemas_.at(0)))) {
    LOG_WARN("failed to check allow split", K(ret));
  } else if (OB_FAIL(prepare_start_args_(tenant_id_,
                                  new_table_schemas_,
                                  upd_table_schemas_,
                                  inc_table_schemas_,
                                  ls_id_,
                                  leader_addr_,
                                  src_tablet_ids_,
                                  dst_tablet_ids_,
                                  start_src_arg_,
                                  start_dst_arg_,
                                  task_id_,
                                  allocator_,
                                  trans_))) {
    LOG_WARN("failed to split start src", KR(ret));
  } else if (OB_FAIL(prepare_dst_tablet_creator_(tenant_id_,
                                                 tenant_data_version_,
                                                 ls_id_,
                                                 leader_addr_,
                                                 src_tablet_ids_,
                                                 dst_tablet_ids_,
                                                 inc_table_schemas_,
                                                 *upd_table_schemas_.at(0),
                                                 split_type_,
                                                 allocator_,
                                                 tablet_creator_,
                                                 trans_))) {
    LOG_WARN("failed to prepare dst tablet creator", K(ret));
  } else if (OB_FAIL(insert_dst_tablet_to_ls_and_table_history_(tenant_id_,
                                                                ls_id_,
                                                                dst_tablet_ids_,
                                                                inc_table_schemas_,
                                                                trans_))) {
    LOG_WARN("failed to insert dst tablet to ls and table history", K(ret));
  } else if (OB_FAIL(create_ddl_task_(tenant_id_,
                                      task_id_,
                                      split_type_,
                                      inc_table_schemas_,
                                      parallelism_,
                                      ls_id_,
                                      tenant_data_version_,
                                      allocator_,
                                      task_record,
                                      trans_))) {
    LOG_WARN("failed to create ddl task", K(ret));
  } else if (OB_FAIL(start_src_(tenant_id_,
                                ls_id_,
                                leader_addr_,
                                src_tablet_ids_,
                                start_src_arg_,
                                data_end_scn_,
                                end_autoinc_seqs_,
                                trans_))) {
    LOG_WARN("failed to split wait src end", KR(ret));
  } else if (OB_FAIL(start_dst_(tenant_id_,
                                tenant_data_version_,
                                ls_id_,
                                leader_addr_,
                                inc_table_schemas_,
                                dst_tablet_ids_,
                                data_end_scn_,
                                end_autoinc_seqs_,
                                task_id_,
                                start_dst_arg_,
                                tablet_creator_,
                                trans_))) {
    LOG_WARN("failed to split start dst", KR(ret));
  }
  return ret;
}

int ObSplitPartitionHelper::check_allow_split(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  bool is_db_in_recyclebin = false;
  const ObTenantSchema *tenant_schema = nullptr;
  common::ObArray<const ObSimpleTableSchemaV2 *> table_schemas_in_tg;
  const uint64_t tablegroup_id = table_schema.get_tablegroup_id();
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  ObArray<uint64_t> lob_col_idxs;
  if (OB_UNLIKELY(table_schema.is_in_recyclebin())) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("the table is in recyclebin.", KR(ret), K(table_schema));
  } else if (OB_FAIL(schema_guard.check_database_in_recyclebin(tenant_id,
                                                               table_schema.get_database_id(),
                                                               is_db_in_recyclebin))) {
    LOG_WARN("check database in recyclebin failed", KR(ret), K(table_schema));
  } else if (OB_UNLIKELY(is_db_in_recyclebin)) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("the database in recyclebin", KR(ret), K(table_schema));
  } else if (OB_UNLIKELY(!table_schema.is_user_table() && !table_schema.is_global_index_table())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported table type", K(ret), K(table_schema));
  } else if (OB_FAIL(ObDDLUtil::get_table_lob_col_idx(table_schema, lob_col_idxs))) {
    LOG_WARN("failed to get tabel lob col idx", K(ret), K(table_schema));
  } else if (lob_col_idxs.empty() && table_schema.has_lob_aux_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not support split table with lob aux table on gen column", K(ret), K(table_schema));
  }

  if (OB_FAIL(ret)) {
  } else if (table_schema.is_global_index_table()) {
    // global index table doesn't have columnstore replica
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", K(ret));
  } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array(zone_locality))) {
    LOG_WARN("failed to get zone replica attr array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); i++) {
      if (zone_locality[i].get_columnstore_replica_num() > 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("split with column store replica not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "split with column store replica");
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(OB_INVALID_ID == tablegroup_id)) {
    //do nothing
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, table_schemas_in_tg))) {
    LOG_WARN("failed to get table schemas in table group", K(ret), K(tablegroup_id));
  } else if (OB_UNLIKELY(table_schemas_in_tg.count() <= 1)) {
    //do nothing
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support spliting of a table in a group with multiple tables", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "spliting of a table in a group with multiple tables");
  }

  return ret;
}

int ObSplitPartitionHelper::freeze_split_src_tablet(const ObFreezeSplitSrcTabletArg &arg,
                                                    ObFreezeSplitSrcTabletRes &res,
                                                    const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else {
    MTL_SWITCH(arg.tenant_id_) {
      ObLSService *ls_service = MTL(ObLSService *);
      logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
      compaction::ObTenantTabletScheduler *tenant_tablet_scheduler = MTL(compaction::ObTenantTabletScheduler*);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      ObRole role = INVALID_ROLE;
      int64_t proposal_id = -1;
      bool has_active_memtable = false;
      const ObIArray<ObTabletID> &tablet_ids = arg.tablet_ids_;
      if (OB_ISNULL(ls_service) || OB_ISNULL(log_service) || OB_ISNULL(tenant_tablet_scheduler)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ls_service or log_service", K(ret));
      } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(arg));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls", K(ret), K(arg.ls_id_));
      } else if (OB_FAIL(ls->get_ls_role(role))) {
        LOG_WARN("get role failed", K(ret), K(MTL_ID()), K(arg.ls_id_));
      } else if (OB_UNLIKELY(ObRole::LEADER != role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("ls not leader", K(ret), K(MTL_ID()), K(arg.ls_id_));
      } else if (OB_FAIL(ls->tablet_freeze(checkpoint::INVALID_TRACE_ID, tablet_ids, true/*is_sync*/, abs_timeout_us,
              false/*need_rewrite_meta*/, ObFreezeSourceFlag::TABLET_SPLIT))) {
        LOG_WARN("batch tablet freeze failed", K(ret), K(arg));
      } else if (OB_FAIL(ls->check_tablet_no_active_memtable(tablet_ids, has_active_memtable))) {
        // safer with this check, non-mandatory
        LOG_WARN("check tablet has active memtable failed", K(ret), K(arg));
      } else if (has_active_memtable) {
        ret = OB_EAGAIN;
        LOG_WARN("tablet has active memtable need retry", K(ret), K(arg));
      }

      // the followings are workarounds, still INCORRECT in some leader switch corner cases
      // 1. wait write end for medium
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tenant_tablet_scheduler->stop_tablets_schedule_medium(tablet_ids, compaction::ObProhibitScheduleMediumMap::ProhibitFlag::SPLIT))) {
        LOG_WARN("failed to stop tablets schedule medium", K(ret), K(arg));
      } else if (OB_FAIL(tenant_tablet_scheduler->clear_tablets_prohibit_medium_flag(tablet_ids, compaction::ObProhibitScheduleMediumMap::ProhibitFlag::SPLIT))) {
        LOG_WARN("failed to clear prohibit schedule medium flag", K(ret), K(arg));
      }
      // 2. wait write end for table autoinc seq will be done in batch_get_tablet_autoinc_seq

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ls->get_log_handler()->get_max_scn(res.data_end_scn_))) {
        LOG_WARN("log_handler get_max_scn failed", K(ret), K(arg));
      }
    }
  }
  return ret;
}

// only used for create split dst tablet
int ObSplitPartitionHelper::get_split_src_tablet_id_if_any(
    const share::schema::ObTableSchema &table_schema,
    ObTabletID &split_src_tablet_id)
{
  int ret = OB_SUCCESS;
  const ObCheckPartitionMode mode = CHECK_PARTITION_MODE_NORMAL;
  bool finish = false;
  ObPartitionSchemaIter iter(table_schema, mode);
  ObPartitionSchemaIter::Info info;
  split_src_tablet_id.reset();
  while (OB_SUCC(ret) && !finish) {
    if (OB_FAIL(iter.next_partition_info(info))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        finish = true;
      } else {
        LOG_WARN("iter partition failed", KR(ret));
      }
    } else if (nullptr != info.partition_) {
      const ObTabletID &tablet_id = info.partition_->get_split_source_tablet_id();
      if (tablet_id.is_valid()) {
        if (split_src_tablet_id.is_valid() && OB_UNLIKELY(tablet_id != split_src_tablet_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("all split src tablet id must be same in schema", K(ret), K(split_src_tablet_id), KPC(info.partition_));
        } else {
          split_src_tablet_id = tablet_id;
        }
      }
      break;
    }
  }
  return ret;
}

int ObSplitPartitionHelper::check_enable_global_index_auto_split(
    const share::schema::ObTableSchema &data_table_schema,
    bool &enable_auto_split,
    int64_t &auto_part_size)
{
  int ret = OB_SUCCESS;
  enable_auto_split = false;
  auto_part_size = -1;
  if (data_table_schema.is_mysql_tmp_table() || data_table_schema.is_sys_table()) {
    // not supported table type
  } else {
    const uint64_t tenant_id = data_table_schema.get_tenant_id();
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      const ObString policy_str(tenant_config->global_index_auto_split_policy.str());
      if (0 == policy_str.case_compare("DISTRIBUTED")) {
        int64_t primary_zone_num = 0;
        int64_t unit_group_num = 0;
        ObArray<share::ObSimpleUnitGroup> unit_group_array;
        if (OB_FAIL(rootserver::ObTenantBalanceService::gather_stat_primary_zone_num_and_units(
                tenant_id, primary_zone_num, unit_group_array))) {
          LOG_WARN("failed to gather stat of primary zone and unit", KR(ret), K(tenant_id));
        } else if (primary_zone_num > 1 || unit_group_array.count() > 1) {
          enable_auto_split = true;
        }
      } else if (0 == policy_str.case_compare("ALL")) {
        enable_auto_split = true;
      }
      if (OB_SUCC(ret) && enable_auto_split) {
        const int64_t data_auto_part_size = data_table_schema.get_part_option().get_auto_part_size();
        int64_t tenant_auto_part_size = tenant_config->auto_split_tablet_size;
        const int64_t errsim_auto_part_size = OB_E(common::EventTable::EN_AUTO_SPLIT_TABLET_SIZE) 0;
        if (0 != errsim_auto_part_size) {
          tenant_auto_part_size = std::abs(errsim_auto_part_size);
        }
        auto_part_size = data_table_schema.get_part_option().is_valid_auto_part_size() ? data_auto_part_size : tenant_auto_part_size;
        LOG_INFO("enable global index auto split by tenant config", K(auto_part_size), K(data_auto_part_size), K(tenant_auto_part_size), K(errsim_auto_part_size), K(policy_str));
      }
    }
  }
  return ret;
}

int ObSplitPartitionHelper::prepare_start_args_(
    const uint64_t tenant_id,
    const ObIArray<ObTableSchema *> &new_table_schemas,
    const ObIArray<ObTableSchema *> &upd_table_schemas,
    const ObIArray<const ObTableSchema *> &inc_table_schemas,
    ObLSID &ls_id,
    ObAddr &leader_addr,
    ObIArray<ObTabletID> &src_tablet_ids,
    ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
    ObTabletSplitMdsArg &start_src_arg,
    ObTabletSplitMdsArg &start_dst_arg,
    int64_t &task_id,
    ObIAllocator &allocator,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  ObLocationService *location_service = nullptr;
  ObArray<ObLSID> ls_ids;
  ObArray<ObRowkey> dst_high_bound_vals;
  ls_id.reset();
  leader_addr.reset();
  src_tablet_ids.reset();
  dst_tablet_ids.reset();
  start_src_arg.reset();
  start_dst_arg.reset();
  task_id = 0;
  ObSEArray<ObTabletID, 1> src_data_tablet_id;
  ObRootService *root_service = GCTX.root_service_;
  ObTableLockOwnerID owner_id;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || upd_table_schemas.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(upd_table_schemas.count()));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(upd_table_schemas.at(0)->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("failed to check oracle mode", KR(ret), K(tenant_id), KPC(upd_table_schemas.at(0)));
  }

  // prepare tablet ids array for mapping between src and dst tablets later
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletSplitMdsArg::prepare_basic_args(upd_table_schemas,
                                                             inc_table_schemas,
                                                             src_tablet_ids,
                                                             dst_tablet_ids,
                                                             dst_high_bound_vals))) {
    LOG_WARN("failed to get all tablet ids", KR(ret));
  } else if (OB_FAIL(ObDDLUtil::batch_check_tablet_checksum(MTL_ID(), 0/*start index of tablet_arr*/, src_tablet_ids.count(), src_tablet_ids))) {
    LOG_WARN("verify tablet checksum error", K(ret), K(src_tablet_ids), K(tenant_id));
  }

  // lock and get ls_id
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(src_data_tablet_id.push_back(src_tablet_ids.at(0)))) {
    LOG_WARN("failed to push back src data tablet id", K(ret));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service->get_sql_proxy(), tenant_id, task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FALSE_IT(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                     task_id))) {
  } else if (OB_FAIL(ObDDLLock::lock_for_split_partition(*upd_table_schemas.at(0), nullptr/*ls_id*/, &src_data_tablet_id, dst_tablet_ids.at(0), owner_id, trans))) {
    LOG_WARN("failed to lock for split src partition", K(ret), K(src_tablet_ids), K(task_id));
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, src_tablet_ids, ls_ids))) {
    LOG_WARN("failed to batch get ls", KR(ret));
  } else if (OB_UNLIKELY(src_tablet_ids.count() != ls_ids.count() || ls_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet ids ls ids", KR(ret), K(src_tablet_ids), K(ls_ids));
  } else {
    ls_id = ls_ids.at(0);
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.count(); i++) {
      if (OB_UNLIKELY(ls_id != ls_ids.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src tablet ls mismatched", KR(ret), K(ls_id), K(ls_ids));
      }
    }
  }

  // prepare mds args
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(start_src_arg.init_split_start_src(tenant_id, is_oracle_mode, ls_id, new_table_schemas, upd_table_schemas, src_tablet_ids, dst_tablet_ids))) {
    LOG_WARN("failed to init split start src", KR(ret));
  } else if (OB_FAIL(start_dst_arg.init_split_start_dst(tenant_id, ls_id, inc_table_schemas, src_tablet_ids, dst_tablet_ids, dst_high_bound_vals))) {
    LOG_WARN("failed to init split start dst", KR(ret));
  }

  // prepare leader addr, as late as possible
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("location_cache is null", KR(ret), KP(location_service));
  } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id,
                                                  tenant_id,
                                                  ls_id,
                                                  true,
                                                  leader_addr))) {
    LOG_WARN("get leader failed", KR(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObSplitPartitionHelper::prepare_dst_tablet_creator_(
    const uint64_t tenant_id,
    const uint64_t tenant_data_version,
    const ObLSID &ls_id,
    const ObAddr &leader_addr,
    const ObIArray<ObTabletID> &src_tablet_ids,
    const ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
    const ObIArray<const ObTableSchema *> &inc_table_schemas,
    const share::schema::ObTableSchema &main_src_table_schema,
    const share::ObDDLType split_type,
    ObIAllocator &allocator,
    ObTabletCreator *&tablet_creator,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> create_commit_versions;
  void *buf = nullptr;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  if (OB_UNLIKELY(dst_tablet_ids.empty() || dst_tablet_ids.count() != inc_table_schemas.count() || nullptr != tablet_creator) || OB_ISNULL(inc_table_schemas.at(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KP(tablet_creator), K(dst_tablet_ids), K(inc_table_schemas.count()));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTabletCreator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate", K(ret));
  } else {
    // TODO(lihongqing.lhq), use scn from split source tablet rather than frozen_scn/mock base_scn.
    tablet_creator = new (buf)ObTabletCreator(tenant_id, SCN::base_scn(), trans);
    if (OB_FAIL(tablet_creator->init(true/*need_check_tablet_cnt*/))) {
      LOG_WARN("failed to init tablet creator", K(ret));
    }
  }

  // fetch split src tablet size and create_commit_versions
  if (OB_SUCC(ret)) {
    // FIXME: timeout ctx
    const int64_t timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : GCONF.rpc_timeout;
    int64_t data_tablet_size = OB_INVALID_SIZE;
    obrpc::ObFetchSplitTabletInfoArg arg;
    obrpc::ObFetchSplitTabletInfoRes res;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("srv_rpc_proxy is null", KR(ret), KP(srv_rpc_proxy));
    } else if (OB_FAIL(arg.tablet_ids_.assign(src_tablet_ids))) {
      LOG_WARN("failed to assign", KR(ret));
    } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr).timeout(timeout_us).fetch_split_tablet_info(arg, res))) {
      LOG_WARN("failed to freeze src tablet", KR(ret), K(leader_addr));
    } else if (OB_FAIL(create_commit_versions.assign(res.create_commit_versions_))) {
      LOG_WARN("failed to assign", K(ret));
    } else if (OB_FALSE_IT(data_tablet_size = res.tablet_sizes_.at(0))) {
    } else if (data_tablet_size < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data tablet size", K(ret), K(data_tablet_size));
    } else {
      const ObPartitionOption &part_option = main_src_table_schema.get_part_option();
      const int64_t cur_part_num = part_option.get_part_num();
      const int64_t auto_split_size = part_option.get_auto_part_size();
      double cur_ratio = 0;
      int64_t real_auto_split_size = 0;
      int64_t tablets_limit_per_table = lib::is_oracle_mode() ? OB_MAX_PARTITION_NUM_ORACLE : OB_MAX_PARTITION_NUM_MYSQL;
      if (cur_part_num  > tablets_limit_per_table) {
        ret = OB_TOO_MANY_PARTITIONS_ERROR;
        LOG_WARN("doesn't support splitting the tablet, when the number of tablets of the table is greater than the limit", K(ret), K(tablets_limit_per_table));
      } else if ((!is_auto_split(split_type))) {
        //manual split skip checking
      } else if (OB_FALSE_IT(cur_ratio = static_cast<double>(cur_part_num) / tablets_limit_per_table)) {
      } else if (OB_FAIL(ObServerAutoSplitScheduler::cal_real_auto_split_size(0.5/*base_ratio*/, cur_ratio, auto_split_size, real_auto_split_size))) {
        LOG_WARN("failed to calculate tablet limit penalty", K(ret), K(cur_ratio));
      } else if (data_tablet_size < real_auto_split_size) {
        ret = OB_NOT_SUPPORTED;
        LOG_DEBUG("tablet size is smaller than increased split size threshold", K(ret), K(auto_split_size), K(real_auto_split_size), K(data_tablet_size));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const ObTableSchema &data_table_schema = *inc_table_schemas.at(0);
    const int64_t split_cnt = dst_tablet_ids.at(0).count();
    const int64_t table_cnt = inc_table_schemas.count();
    bool is_oracle_mode = false;
    if (OB_FAIL(check_mem_usage_for_split_(tenant_id, split_cnt))) {
      LOG_WARN("failed to check memory usage", K(ret));
    } else if (OB_UNLIKELY(create_commit_versions.count() != table_cnt || dst_tablet_ids.count() != table_cnt)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(table_cnt), K(create_commit_versions), K(dst_tablet_ids));
    } else if (OB_FAIL(data_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle mode", KR(ret), K(data_table_schema.get_table_id()));
    } else {
      const lib::Worker::CompatMode compat_mode = is_oracle_mode ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
      ObArray<ObTabletID> tablet_ids;
      ObArray<bool> need_create_empty_majors;
      for (int64_t i = 0; OB_SUCC(ret) && i < split_cnt; i++) {
        const ObTabletID &data_tablet_id = dst_tablet_ids.at(0).at(i);
        ObTabletCreatorArg arg;
        tablet_ids.reuse();
        need_create_empty_majors.reuse();
        for (int64_t table_idx = 0; OB_SUCC(ret) && table_idx < table_cnt; table_idx++) {
          if (OB_UNLIKELY(split_cnt != dst_tablet_ids.at(table_idx).count())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid arg", K(ret), K(split_cnt), K(table_idx), K(dst_tablet_ids));
          } else if (OB_FAIL(tablet_ids.push_back(dst_tablet_ids.at(table_idx).at(i)))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(need_create_empty_majors.push_back(false))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(arg.init(tablet_ids,
                                    ls_id,
                                    data_tablet_id,
                                    inc_table_schemas,
                                    compat_mode,
                                    false/*is_create_bind_hidden_tablets*/,
                                    tenant_data_version,
                                    need_create_empty_majors,
                                    create_commit_versions,
                                    false/*has_cs_replica*/))) {
          LOG_WARN("failed to init", K(ret));
        } else if (OB_FAIL(tablet_creator->add_create_tablet_arg(arg))) {
          LOG_WARN("failed to add create tablet arg", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSplitPartitionHelper::insert_dst_tablet_to_ls_and_table_history_(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
    const ObIArray<const ObTableSchema *> &inc_table_schemas,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletToLSInfo> tablet_infos;
  if (OB_UNLIKELY(dst_tablet_ids.empty() || dst_tablet_ids.count() != inc_table_schemas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dst_tablet_ids), K(inc_table_schemas.count()));
  }
  for (int64_t table_idx = 0; OB_SUCC(ret) && table_idx < dst_tablet_ids.count(); table_idx++) {
    const ObTableSchema *table_schema = inc_table_schemas.at(table_idx);
    if (OB_ISNULL(table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid table schema", K(ret));
    } else {
      const uint64_t table_id = table_schema->get_table_id();
      const int64_t schema_version = table_schema->get_schema_version();
      ObArray<ObTabletTablePair> tablet_table_pairs;
      for (int64_t i = 0; OB_SUCC(ret) && i < dst_tablet_ids.at(table_idx).count(); i++) {
        const ObTabletID &tablet_id = dst_tablet_ids.at(table_idx).at(i);
        const ObTabletToLSInfo tablet_info(tablet_id, ls_id, table_id, 0/*transfer_seq*/);
        if (OB_FAIL(tablet_table_pairs.push_back(ObTabletTablePair(tablet_id, table_id)))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(tablet_infos.push_back(tablet_info))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObTabletToTableHistoryOperator::create_tablet_to_table_history(
              trans, tenant_id, schema_version, tablet_table_pairs))) {
        LOG_WARN("failed to create tablet to table history", K(ret), K(tenant_id), K(schema_version), K(tablet_table_pairs));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_update(trans, tenant_id, tablet_infos))) {
    LOG_WARN("fail to batch update tablet info", K(ret), K(tenant_id), K(tablet_infos));
  }
  return ret;
}

int ObSplitPartitionHelper::delete_src_tablet_to_ls_and_table_history_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &src_tablet_ids,
    const int64_t new_schema_version,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObTabletToLSTableOperator::batch_remove(trans, tenant_id, src_tablet_ids))) {
    LOG_WARN("failed to batch remove tablet to ls table", KR(ret));
  } else if (OB_FAIL(share::ObTabletToTableHistoryOperator::drop_tablet_to_table_history(
                      trans, tenant_id, new_schema_version, src_tablet_ids))) {
    LOG_WARN("failed to drop tablet to table history", KR(ret), K(tenant_id), K(new_schema_version));
  }
  return ret;
}

int ObSplitPartitionHelper::create_ddl_task_(
    const uint64_t tenant_id,
    const int64_t task_id,
    const ObDDLType split_type,
    const ObIArray<const ObTableSchema *> &inc_table_schemas,
    const int64_t parallelism,
    const share::ObLSID &ls_id,
    const uint64_t tenant_data_version,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObPartitionSplitArg split_arg;
  int64_t split_part_num = 0;
  const ObTableSchema *split_table = nullptr;
  uint64_t tenant_data_format_version = 0;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || !share::is_tablet_split(split_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(split_type), K(inc_table_schemas.count()));
  } else if (OB_ISNULL(split_table = inc_table_schemas.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret));
  } else if (OB_UNLIKELY((split_part_num = split_table->get_partition_num()) < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(split_part_num), KPC(split_table));
  } else if (OB_UNLIKELY(!split_table->is_user_table() && !split_table->is_global_index_table())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("partition split is only supported for user table or global index", K(ret), KPC(split_table));
  } else if (split_table->is_global_index_table() && inc_table_schemas.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("global index should not have aux tables", K(ret), KPC(split_table));
  } else {
    split_arg.task_type_ = split_type;
    if (MOCK_DATA_VERSION_4_3_5_3 <= tenant_data_version) {
      split_arg.src_ls_id_ = ls_id;
    }
    ObPartition** table_parts = split_table->get_part_array();
    // for main table or global_index.
    for (int64_t i = 0; OB_SUCC(ret) && i < split_part_num; i++) {
      if (OB_ISNULL(table_parts[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null part", K(ret));
      } else if (OB_FAIL(split_arg.dest_tablet_ids_.push_back(table_parts[i]->get_tablet_id()))) {
        LOG_WARN("fail to push back to array", K(ret));
      } else if (i == 0) {
        split_arg.src_tablet_id_ = table_parts[i]->get_split_source_tablet_id();
      } else if (OB_UNLIKELY(table_parts[i]->get_split_source_tablet_id() != split_arg.src_tablet_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err, split source tablet id should be same", K(ret), K(i), K(split_arg.src_tablet_id_),
                 "split_source_tablet_id", table_parts[i]->get_split_source_tablet_id());
      }
    }
    // for aux tables, containing local index table, lob table.
    for (int64_t i = 1/* 0 is main table*/; OB_SUCC(ret) && i < inc_table_schemas.count(); i++) {
      const ObTableSchema *aux_table_schema = inc_table_schemas.at(i);
      uint64_t table_id = aux_table_schema->get_table_id();
      uint64_t table_schema_version = aux_table_schema->get_schema_version();
      ObPartition** aux_table_parts = nullptr;
      if (OB_ISNULL(aux_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null schema", K(ret));
      } else if (OB_UNLIKELY(split_part_num != aux_table_schema->get_partition_num())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), K(split_part_num), KPC(aux_table_schema));
      } else if (OB_ISNULL(aux_table_parts = aux_table_schema->get_part_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), KPC(aux_table_schema));
      } else {
        ObSArray<uint64_t> *table_ids = nullptr;
        ObSArray<uint64_t> *table_schema_versions = nullptr;
        ObSArray<ObTabletID> *src_tablet_ids = nullptr;
        ObSArray<ObSArray<ObTabletID>> *dest_tablets_ids = nullptr;
        if (aux_table_schema->is_index_local_storage()) {
          table_ids = &split_arg.local_index_table_ids_;
          table_schema_versions = &split_arg.local_index_schema_versions_;
          src_tablet_ids = &split_arg.src_local_index_tablet_ids_;
          dest_tablets_ids = &split_arg.dest_local_index_tablet_ids_;
        } else if (aux_table_schema->is_aux_lob_table()) {
          table_ids = &split_arg.lob_table_ids_;
          table_schema_versions = &split_arg.lob_schema_versions_;
          src_tablet_ids = &split_arg.src_lob_tablet_ids_;
          dest_tablets_ids = &split_arg.dest_lob_tablet_ids_;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid type of aux table", K(ret), KPC(aux_table_schema));
        }

        if (OB_SUCC(ret)) {
          ObSArray<ObTabletID> dest_tablet_ids;
          for (int64_t j = 0; OB_SUCC(ret) && j < split_part_num; j++) {
            ObPartition* part = aux_table_parts[j];
            if (OB_ISNULL(part)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null part", K(ret));
            } else if (OB_FAIL(dest_tablet_ids.push_back(part->get_tablet_id()))) {
              LOG_WARN("fail to push back to array", K(ret));
            } else if (j == 0) {
              if (OB_FAIL(src_tablet_ids->push_back(part->get_split_source_tablet_id()))) {
                LOG_WARN("fail to push back to array", K(ret));
              }
            } else if (OB_UNLIKELY(part->get_split_source_tablet_id() != src_tablet_ids->at(src_tablet_ids->count() - 1))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected err, split source tablet id should be same", K(ret), K(j), K(src_tablet_ids->at(src_tablet_ids->count() - 1)),
                "split_source_tablet_id", part->get_split_source_tablet_id());
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(table_ids->push_back(table_id))) {
            LOG_WARN("push back table id failed", K(ret), K(table_id));
          } else if (OB_FAIL(table_schema_versions->push_back(table_schema_version))) {
            LOG_WARN("push back table schema version failed", K(ret),
                K(table_schema_version));
          } else if (OB_FAIL(dest_tablets_ids->push_back(dest_tablet_ids))) {
            LOG_WARN("push back split dest tablets failed", K(ret), K(dest_tablet_ids));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObCreateDDLTaskParam param(tenant_id,
                               split_type,
                               split_table,
                               nullptr,
                               split_table->get_table_id(),
                               split_table->get_schema_version(),
                               MAX(1, parallelism)/*parallelism*/,
                               split_arg.consumer_group_id_,
                               &allocator,
                               &split_arg,
                               0/*parent_task_id*/,
                               task_id);
    param.tenant_data_version_ = tenant_data_version;
    if (OB_FAIL(ObSysDDLSchedulerUtil::create_ddl_task(param, trans, task_record))) {
      LOG_WARN("submit ddl task failed", KR(ret));
    }
    LOG_TRACE("create ddl task for spliting partition", K(ret), K(param));
  }
  return ret;
}

int ObSplitPartitionHelper::start_src_(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObAddr &leader_addr,
    const ObIArray<ObTabletID> &src_tablet_ids,
    const ObTabletSplitMdsArg &start_src_arg,
    share::SCN &data_end_scn,
    ObIArray<std::pair<uint64_t, uint64_t>> &end_autoinc_seqs,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  int64_t finish_time = ObTimeUtility::current_time();
  int64_t start_time = finish_time;
  data_end_scn.reset();
  end_autoinc_seqs.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid() || !leader_addr.is_valid()
        || src_tablet_ids.empty() || !start_src_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(tenant_id), K(ls_id), K(leader_addr), K(src_tablet_ids), K(start_src_arg));
  }

  // start src
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("srv_rpc_proxy is null", KR(ret), KP(srv_rpc_proxy));
    } else if (OB_FAIL(ObTabletSplitMdsHelper::register_mds(start_src_arg, false/*need_flush_redo*/, trans))) {
      LOG_WARN("failed to register mds", KR(ret));
    }
    finish_time = ObTimeUtility::current_time();
    LOG_INFO("finish set src tablet mds", KR(ret), "cost_ts", finish_time - start_time);
    start_time = finish_time;
  }

  // wait src freeze end
  if (OB_SUCC(ret)) {
    // FIXME: timeout ctx
    const int64_t timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : GCONF.rpc_timeout;
    obrpc::ObFreezeSplitSrcTabletArg arg;
    obrpc::ObFreezeSplitSrcTabletRes res;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    if (OB_FAIL(arg.tablet_ids_.assign(src_tablet_ids))) {
      LOG_WARN("failed to assign", KR(ret));
    } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr).timeout(timeout_us).freeze_split_src_tablet(arg, res))) {
      LOG_WARN("failed to freeze src tablet", KR(ret), K(leader_addr));
    } else {
      // data_end_scn = res.data_end_scn_;
    }
    finish_time = ObTimeUtility::current_time();
    LOG_INFO("finish freeze_split_src_tablet", KR(ret), "cost_ts", finish_time - start_time, K(data_end_scn));
    start_time = finish_time;
  }

  // set src freeze flag
  if (OB_SUCC(ret)) {
    const bool need_flush_redo = true; // flush redo so that the following data_end_scn will be >= this redo scn
    ObTabletSplitMdsArg set_src_freeze_flag_arg;
    if (OB_FAIL(set_src_freeze_flag_arg.init_set_freeze_flag(tenant_id, ls_id, src_tablet_ids))) {
      LOG_WARN("failed to init split start src", KR(ret));
    } else if (OB_FAIL(ObTabletSplitMdsHelper::register_mds(set_src_freeze_flag_arg, need_flush_redo, trans))) {
      LOG_WARN("failed to register mds", KR(ret));
    }
    finish_time = ObTimeUtility::current_time();
    LOG_INFO("finish set src freeze flag", KR(ret), "cost_ts", finish_time - start_time);
    start_time = finish_time;
  }

  // get src tablet's non-empty autoinc_seq that are needed to sync to dst tablet
  if (OB_SUCC(ret)) {
    const int64_t timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : GCONF.rpc_timeout;
    ObBatchGetTabletAutoincSeqProxy proxy(*srv_rpc_proxy, &obrpc::ObSrvRpcProxy::batch_get_tablet_autoinc_seq);
    obrpc::ObBatchGetTabletAutoincSeqArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    if (OB_FAIL(arg.src_tablet_ids_.assign(src_tablet_ids))) {
      LOG_WARN("failed to assign", KR(ret));
    } else if (OB_FAIL(arg.dest_tablet_ids_.assign(src_tablet_ids))) {
      LOG_WARN("failed to assign", KR(ret));
    } else if (OB_FAIL(proxy.call(leader_addr, timeout_us, tenant_id, arg))) {
      LOG_WARN("send rpc failed", KR(ret), K(arg), K(leader_addr));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = proxy.wait())) {
      LOG_WARN("rpc proxy wait failed", K(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      const auto &result_array = proxy.get_results();
      if (OB_UNLIKELY(1 != result_array.count()) || OB_ISNULL(result_array.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result count not match", KR(ret), K(result_array.count()));
      } else {
        const auto *cur_result = result_array.at(0);
        for (int64_t j = 0; OB_SUCC(ret) && j < cur_result->autoinc_params_.count(); j++) {
          const ObMigrateTabletAutoincSeqParam &autoinc_param = cur_result->autoinc_params_.at(j);
          const int64_t table_idx = j;
          if (OB_FAIL(autoinc_param.ret_code_)) {
            LOG_WARN("failed to get autoinc", KR(ret));
          } else if (1 < autoinc_param.autoinc_seq_) { // only non-empty autoinc_seqs are needed to sync to dst
            if (OB_FAIL(end_autoinc_seqs.push_back(std::make_pair(table_idx, autoinc_param.autoinc_seq_)))) {
              LOG_WARN("failed to push back", KR(ret));
            }
          }
        }
      }
    }
    finish_time = ObTimeUtility::current_time();
    LOG_INFO("finish batch_get_tablet_autoinc_seq", KR(ret), "cost_ts", finish_time - start_time);
    start_time = finish_time;
  }

  // get data end scn finally to guarantee both mds_checkpoint_scn and clog_checkpoint_scn are less than data_end_scn
  if (OB_SUCC(ret)) {
    const int64_t timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : GCONF.rpc_timeout;
    obrpc::ObFreezeSplitSrcTabletArg arg;
    obrpc::ObFreezeSplitSrcTabletRes res;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    if (OB_FAIL(arg.tablet_ids_.assign(src_tablet_ids))) {
      LOG_WARN("failed to assign", KR(ret));
    } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr).timeout(timeout_us).freeze_split_src_tablet(arg, res))) {
      LOG_WARN("failed to freeze src tablet", KR(ret), K(leader_addr));
    } else {
      data_end_scn = res.data_end_scn_;
    }
    finish_time = ObTimeUtility::current_time();
    LOG_INFO("finish get data end scn", KR(ret), "cost_ts", finish_time - start_time, K(data_end_scn));
    start_time = finish_time;
  }
  return ret;
}

int ObSplitPartitionHelper::start_dst_(
    const uint64_t tenant_id,
    const uint64_t tenant_data_version,
    const share::ObLSID &ls_id,
    const ObAddr &leader_addr,
    const ObIArray<const ObTableSchema *> &inc_table_schemas,
    const ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
    const share::SCN &data_end_scn,
    const ObIArray<std::pair<uint64_t, uint64_t>> &end_autoinc_seqs,
    const int64_t task_id,
    ObTabletSplitMdsArg &start_dst_arg,
    ObTabletCreator *&tablet_creator,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t finish_time = ObTimeUtility::current_time();
  int64_t start_time = finish_time;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid() || !leader_addr.is_valid()
        || !start_dst_arg.is_valid() || !data_end_scn.is_valid() || inc_table_schemas.empty() || OB_ISNULL(tablet_creator))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(leader_addr), K(start_dst_arg), K(data_end_scn), K(inc_table_schemas.count()), KP(tablet_creator));
  }

  // create dst tablets
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_creator->modify_batch_args(storage::ObTabletMdsUserDataType::START_SPLIT_DST, data_end_scn, data_end_scn, true/*clear_auto_part_size*/))) {
    LOG_WARN("failed to set clog checkpoint scn of tablet creator args", KR(ret));
  } else if (OB_FAIL(tablet_creator->execute())) {
    LOG_WARN("execute create partition failed", KR(ret));
  }

  finish_time = ObTimeUtility::current_time();
  LOG_INFO("finish create split dst tablets", KR(ret), "cost_ts", finish_time - start_time);
  start_time = finish_time;

  // lock dst partition
  ObTableLockOwnerID owner_id;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                 task_id))) {
    LOG_WARN("get owner id failed", K(ret), K(task_id));
  } else if (OB_FAIL(ObDDLLock::lock_for_split_partition(*inc_table_schemas.at(0), &ls_id, nullptr/*src_tablet_ids*/, dst_tablet_ids.at(0), owner_id, trans))) {
    LOG_WARN("failed to lock for split src partition", K(ret), K(dst_tablet_ids), K(task_id));
  }

  finish_time = ObTimeUtility::current_time();
  LOG_INFO("finish lock dst partition", KR(ret), "cost_ts", finish_time - start_time);
  start_time = finish_time;

  // sync dst tablet autoinc seq
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("srv_rpc_proxy is null", KR(ret), KP(srv_rpc_proxy));
  } else if (!end_autoinc_seqs.empty()) {
    ObBatchSetTabletAutoincSeqProxy proxy(*srv_rpc_proxy, &obrpc::ObSrvRpcProxy::batch_set_tablet_autoinc_seq);
    obrpc::ObBatchSetTabletAutoincSeqArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    arg.is_tablet_creating_ = true;
    share::ObMigrateTabletAutoincSeqParam param;
    for (int64_t i = 0; OB_SUCC(ret) && i < end_autoinc_seqs.count(); i++) {
      const int64_t table_idx = end_autoinc_seqs.at(i).first;
      const uint64_t end_autoinc_seq = end_autoinc_seqs.at(i).second;
      if (OB_UNLIKELY(table_idx >= dst_tablet_ids.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index out of range", KR(ret), K(table_idx), K(dst_tablet_ids));
      } else {
        const ObIArray<ObTabletID> &dst_tablet_ids_of_this_table = dst_tablet_ids.at(table_idx);
        param.autoinc_seq_ = end_autoinc_seq;
        for (int64_t j = 0; OB_SUCC(ret) && j < dst_tablet_ids_of_this_table.count(); j++) {
          param.dest_tablet_id_ = dst_tablet_ids_of_this_table.at(j);
          if (OB_FAIL(arg.autoinc_params_.push_back(param))) {
            LOG_WARN("failed to push back", KR(ret));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (DATA_VERSION_4_3_5_0 <= tenant_data_version) {
      if (OB_FAIL(start_dst_arg.set_autoinc_seq_arg(arg))) {
        LOG_WARN("failed to set autoinc arg", K(ret), K(arg));
      }
    } else {
      const int64_t timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : GCONF.rpc_timeout;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(proxy.call(leader_addr, timeout_us, tenant_id, arg))) {
        LOG_WARN("send rpc failed", KR(ret), K(arg), K(leader_addr));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = proxy.wait())) {
        LOG_WARN("rpc proxy wait failed", K(tmp_ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      } else if (OB_SUCC(ret)) {
        const auto &result_array = proxy.get_results();
        if (OB_UNLIKELY(1 != result_array.count()) || OB_ISNULL(result_array.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result count not match", KR(ret), K(result_array.count()));
        } else {
          const auto *cur_result = result_array.at(0);
          for (int64_t j = 0; OB_SUCC(ret) && j < cur_result->autoinc_params_.count(); j++) {
            const ObMigrateTabletAutoincSeqParam &autoinc_param = cur_result->autoinc_params_.at(j);
            if (OB_FAIL(autoinc_param.ret_code_)) {
              LOG_WARN("failed to get autoinc", KR(ret));
            }
          }
        }
      }
    }

    finish_time = ObTimeUtility::current_time();
    LOG_INFO("finish sync dst tablet autoinc", KR(ret), "cost_ts", finish_time - start_time);
    start_time = finish_time;
  }

  // set dst tablet split data mds
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTabletSplitMdsHelper::register_mds(start_dst_arg, false/*need_flush_redo*/, trans))) {
      LOG_WARN("failed to register mds", KR(ret));
    }
    finish_time = ObTimeUtility::current_time();
    LOG_INFO("finish set dst tablet mds", KR(ret), "cost_ts", finish_time - start_time);
    start_time = finish_time;
  }
  return ret;
}

int ObSplitPartitionHelper::check_mem_usage_for_split_(
    const uint64_t tenant_id,
    const int64_t dst_tablets_number)
{
  int ret = OB_SUCCESS;
  share::ObUnitTableOperator unit_op;
  common::ObSEArray<share::ObResourcePool, 2> pools;
  common::ObSEArray<uint64_t, 2> unit_config_ids;
  common::ObSEArray<ObUnitConfig, 2> unit_configs;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || dst_tablets_number < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(dst_tablets_number));
  } else if (dst_tablets_number > OB_MAX_SPLIT_PER_ROUND) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("the number of destined split tablets greater than 8192 is not supported", K(ret));
    LOG_USER_WARN(OB_NOT_SUPPORTED, "the number of destined split tablets greater than 8192 is");
  } else if (OB_FAIL(unit_op.init(*GCTX.sql_proxy_))) {
    LOG_WARN("failed to init proxy", K(ret));
  } else if (OB_FAIL(unit_op.get_resource_pools(tenant_id, pools))) {
    LOG_WARN("failed to get resource pool", K(ret), K(tenant_id));
  } else if (pools.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty pool", K(ret), K(pools), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
    const share::ObResourcePool &pool = pools.at(i);
    if OB_FAIL(unit_config_ids.push_back(pool.unit_config_id_)) {
      LOG_WARN("failed to push back into unit_config_ids");
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(unit_op.get_unit_configs(unit_config_ids, unit_configs))) {
    LOG_WARN("failed to get unit configs");
  } else if (unit_configs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_configs should not be empty", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_configs.count(); ++i) {
    ObUnitConfig & u_config = unit_configs.at(i);
    const double percent_mem_for_split = 0.2;
    /*
       tenant memory | maximum num of dst tablets
            2GB                    51
            4GB                    102
                     ......
    */
    if (u_config.memory_size() * percent_mem_for_split < (dst_tablets_number * MEMORY_USAGE_SPLIT_PER_DST)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the memory usage of split greater than the memory limit for split", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "the memory usage of split greater than memory limit for split is");
    }
  }
  return ret;
}

int ObSplitPartitionHelper::clean_split_src_and_dst_tablet(
    const obrpc::ObCleanSplittedTabletArg &arg,
    const int64_t auto_part_size,
    const int64_t new_schema_version,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  ObTabletSplitMdsArg end_src_arg;
  ObTabletSplitMdsArg end_dst_arg;
  ObArray<ObTabletID> src_tablet_ids;
  ObArray<ObLSID> ls_ids;
  ObLSID ls_id;
  if (OB_FAIL(src_tablet_ids.push_back(arg.src_table_tablet_id_))) {
    LOG_WARN("failed to push back src tablet id", KR(ret));
  } else if (OB_FAIL(append(src_tablet_ids, arg.src_local_index_tablet_ids_))) {
    LOG_WARN("failed to append", KR(ret));
  } else if (OB_FAIL(append(src_tablet_ids, arg.src_lob_tablet_ids_))) {
    LOG_WARN("failed to append", KR(ret));
  } else if (OB_FAIL(delete_src_tablet_to_ls_and_table_history_(tenant_id, src_tablet_ids, new_schema_version, trans))) {
    LOG_WARN("failed to delete src tablet to ls and table history", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, arg.dest_tablet_ids_, ls_ids))) {
    LOG_WARN("failed to batch get ls", KR(ret));
  } else if (OB_UNLIKELY(arg.dest_tablet_ids_.count() != ls_ids.count() || ls_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet ids ls ids", KR(ret), K(ls_ids), K(arg.dest_tablet_ids_));
  } else {
    ls_id = ls_ids.at(0);
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.count(); i++) {
      if (OB_UNLIKELY(ls_id != ls_ids.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src tablet ls mismatched", KR(ret), K(ls_id), K(ls_ids));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(end_src_arg.init_split_end_src(tenant_id,
                                                    ls_id,
                                                    arg.src_table_tablet_id_,
                                                    arg.src_local_index_tablet_ids_,
                                                    arg.src_lob_tablet_ids_))) {
    LOG_WARN("failed to init split end src", K(ret));
  } else if (OB_FAIL(end_dst_arg.init_split_end_dst(tenant_id,
                                                    ls_id,
                                                    auto_part_size,
                                                    arg.dest_tablet_ids_,
                                                    arg.dest_local_index_tablet_ids_,
                                                    arg.dest_lob_tablet_ids_))) {
    LOG_WARN("failed to init split end dst", K(ret));
  } else if (OB_FAIL(ObTabletSplitMdsHelper::register_mds(end_src_arg, false/*need_flush_redo*/, trans))) {
    LOG_WARN("failed to register mds", K(ret));
  } else if (OB_FAIL(ObTabletSplitMdsHelper::register_mds(end_dst_arg, false/*need_flush_redo*/, trans))) {
    LOG_WARN("failed to register mds", K(ret));
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
