/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "rootserver/ob_random_partition_helper.h"
#include "share/schema/ob_schema_printer.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/scheduler/ob_partition_auto_split_helper.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_table_creator.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_tenant_balance_service.h"

namespace oceanbase
{
using namespace common;
namespace rootserver
{

int ObRandomPartitionHelper::alter_table_random_part_attr_if_need(
    const obrpc::ObAlterTableArg &alter_table_arg,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObTableSchema &table_schema,
    rootserver::ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t data_table_id = table_schema.get_table_id();
  if (!alter_table_arg.is_alter_random_partition_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("modified rando partition attr, not expected here", K(ret),
      K(alter_table_arg.is_alter_random_partition_));
  } else if (table_schema.is_in_recyclebin()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("don't support to modify random partition attr in recyclebin", K(ret), K(table_schema));
  } else if (!table_schema.is_random_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("don't support to modify random partition attr in non-random partitioned table", K(ret), K(table_schema));
  } else if (OB_FAIL(ObDDLLock::lock_for_modify_random_part_size_in_trans(tenant_id, data_table_id, trans))) {
    LOG_WARN("failed to lock data table", K(ret));
  } else {
    const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
    const ObPartitionOption &alter_part_option = alter_table_schema.get_part_option();
    ObArray<ObTabletID> alter_mds_tablet_ids;
    if (alter_part_option.is_enable_random_part()) {
      // enable auto random
      if (OB_FAIL(table_schema.get_part_option().enable_random_partition(alter_part_option.get_auto_part_size()))) {
        LOG_WARN("fail to change random partition size", K(ret), K(alter_part_option));
      }
    } else {
      /*alter table partition by random size('unlimited') */
      table_schema.forbid_auto_partition();
    }
    if (OB_SUCC(ret)) {
      /* update lob aut table  property
       * if main table is enable random partition table, lob table should
       * change random partition property too */
      if (table_schema.has_lob_aux_table() && OB_FAIL(update_lob_meta_table_random_part_attr(table_schema, schema_guard, ddl_operator, trans, alter_mds_tablet_ids))) {
        LOG_WARN("fail to get lob meta tablet id", K(ret), K(table_schema));
      } else if (OB_FAIL(ddl_operator.update_partition_option(trans, table_schema, alter_table_arg.ddl_stmt_str_))) {  // update main table
        LOG_WARN("fail to update partition option", K(ret), K(table_schema));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t abs_timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_ts() : ObTimeUtility::current_time() + GCONF.rpc_timeout;
      ObArray<ObTabletID> tablet_ids;
      if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
        LOG_WARN("failed to get tablet ids", K(ret));
      } else if (OB_FAIL(append(alter_mds_tablet_ids, tablet_ids))) {
        LOG_WARN("fail to assign alter mds tablet ids", K(ret), K(alter_mds_tablet_ids));
      } else if (OB_FAIL(ObTabletRandomMdsHelper::modify_auto_random_size(tenant_id, alter_mds_tablet_ids, alter_part_option.get_auto_part_size(), abs_timeout_us, trans))) {
        LOG_WARN("failed to modify auto part size", K(ret));
      }
    }
  }
  return ret;
}

int ObRandomPartitionHelper::update_lob_meta_table_random_part_attr(
    const ObTableSchema &table_schema,
    ObSchemaGetterGuard &schema_guard,
    rootserver::ObDDLOperator &ddl_operator,
    ObMySQLTransaction &trans,
    ObArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t lob_meta_table_id = table_schema.get_aux_lob_meta_tid();
  const ObTableSchema *lob_meta_schema = nullptr;
  const int64_t tenant_id = table_schema.get_tenant_id();
  if (OB_INVALID_ID != lob_meta_table_id) {
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, lob_meta_table_id, lob_meta_schema))) {
      LOG_WARN("fail to get lob meta schema", K(ret));
    } else if (OB_ISNULL(lob_meta_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob meta schema is null", K(ret));
    } else {
      ObArray<ObTabletID> tablet_ids;
      const ObString ddl_stmt_str("");
      if (OB_FAIL(lob_meta_schema->get_tablet_ids(tablet_ids))) {
        LOG_WARN("failed to get tablet ids", K(ret));
      } else {
        HEAP_VAR(ObTableSchema, new_aux_table_schema) {
          if (OB_FAIL(new_aux_table_schema.assign(*lob_meta_schema))) {
            LOG_WARN("assign index_schema failed", K(ret));
          } else if (OB_FAIL(new_aux_table_schema.assign_partition_schema(table_schema))) {
            LOG_WARN("fail to assign partition schema", K(table_schema), KR(ret));
          } else {
            if (OB_FAIL(ddl_operator.update_partition_option(trans, new_aux_table_schema, ddl_stmt_str))) {
              LOG_WARN("fail to update partition option.", K(ret), K(new_aux_table_schema));
            }
          }
        }
      }
    }

  }
  return ret;
}

int ObRandomPartitionHelper::get_available_ls_cnt(const uint64_t tenant_id, int64_t &ls_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObUnit> unit_array;
  ObBalanceJobDesc job_desc;
  int64_t unit_group_num = 0;
  ls_cnt = 0;
  if (OB_FAIL(rootserver::ObTenantBalanceService::gather_tenant_balance_desc(
          tenant_id, job_desc, unit_array))) {
    LOG_WARN("failed to gather stat of primary zone and unit", KR(ret), K(tenant_id));
  } else if (OB_FAIL(job_desc.get_unit_lcm_count(unit_group_num))) {
    LOG_WARN("failed to get unit lcm count", KR(ret), K(job_desc));
  } else {
    const int64_t primary_zone_num = job_desc.get_ls_cnt_in_group();
    ls_cnt = primary_zone_num * unit_group_num;
  }
  return ret;
}

int ObRandomPartitionHelper::get_active_tablets(const ObTableSchema &table_schema,
                                                const ObIArray<ObTabletID> &inactive_tablet_ids,
                                                ObIArray<int64_t> &marked_inactive_part_indexs,
                                                ObIArray<ObTabletID> &active_tablet_ids)
{
  int ret = OB_SUCCESS;
  active_tablet_ids.reuse();
  marked_inactive_part_indexs.reuse();
  ObTabletID tablet_id;
  ObPartition **data_partitions = table_schema.get_part_array();
  if (OB_ISNULL(data_partitions)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_partitions is null", KR(ret), K(table_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_partition_num(); i++) {
      bool contain = false;
      if (OB_ISNULL(data_partitions[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablet_ids", KR(ret), K(table_schema));
      } else if (ObPartitionStatus::PARTITION_STATUS_ACTIVE != data_partitions[i]->get_status()) {
        continue;
      } else if (FALSE_IT(tablet_id = data_partitions[i]->get_tablet_id())) {
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < inactive_tablet_ids.count(); j++) {
          if (tablet_id == inactive_tablet_ids.at(j)) {
            contain = true;
            break;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!contain) {
        if (OB_FAIL(active_tablet_ids.push_back(tablet_id))) {
          LOG_WARN("fail to push back tablet_id", KR(ret), K(tablet_id));
        }
      } else {
        if (OB_FAIL(marked_inactive_part_indexs.push_back(i))) {
          LOG_WARN("fail to push back part_id", KR(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObRandomPartitionHelper::mark_tablets_as_inactive(const ObTableSchema &table_schema,
                                                      const int64_t mark_inactive_num,
                                                      ObIArray<int64_t> &marked_inactive_part_indexs)
{
  int ret = OB_SUCCESS;
  ObPartition **data_partitions = nullptr;
  int64_t marked_count = 0;
  if (!table_schema.is_valid() || mark_inactive_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema), K(mark_inactive_num));
  } else if (FALSE_IT(data_partitions = table_schema.get_part_array())) {
  } else if (OB_ISNULL(data_partitions)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_partitions is null", KR(ret), K(table_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_partition_num() && marked_count < mark_inactive_num; i++) {
      if (OB_ISNULL(data_partitions[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data_partitions is null", KR(ret), K(table_schema));
      } else if (PARTITION_STATUS_ACTIVE != data_partitions[i]->get_status()) {
        continue;
      } else if (OB_FAIL(marked_inactive_part_indexs.push_back(i))) {
        LOG_WARN("fail to push back parid", KR(ret), K(i));
      } else {
        marked_count++;
      }
    }
  }
  return ret;
}

int ObRandomPartitionHelper::add_partitions(rootserver::ObDDLService &ddl_service,
                                            const uint64_t tenant_id,
                                            const int64_t tenant_data_version,
                                            const ObTableSchema &table_schema,
                                            const int64_t add_part_num,
                                            share::schema::ObMultiVersionSchemaService *schema_service,
                                            common::ObMySQLProxy *sql_proxy,
                                            ObTableSchema &new_table_schema,
                                            ObIArray<ObTabletID> &active_tablet_ids,
                                            obrpc::ObAlterTableArg &alter_table_arg,
                                            ObSchemaGetterGuard &schema_guard,
                                            ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t max_alloc_seq_id = 0;
  if (!table_schema.is_valid() || add_part_num <= 0 || OB_ISNULL(schema_service) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema), K(add_part_num), K(schema_service), K(sql_proxy));
  } else if (OB_FAIL(get_max_alloc_seq_id(table_schema, max_alloc_seq_id))) {
    LOG_WARN("fail to get max alloc seq id", KR(ret), K(table_schema));
  } else if (OB_FAIL(build_add_partition_arg(ddl_service,
                                             table_schema,
                                             add_part_num,
                                             max_alloc_seq_id,
                                             alter_table_arg.alter_random_partition_arg_.specified_value_,
                                             alter_table_arg.alter_table_schema_))) {
      LOG_WARN("fail to build add partition arg", KR(ret), K(table_schema), K(add_part_num), K(max_alloc_seq_id));
  } else if (FALSE_IT(alter_table_arg.alter_part_type_ = obrpc::ObAlterTableArg::ADD_PARTITION)) {
  } else {
    AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
    rootserver::ObDDLOperator ddl_operator(*schema_service, *sql_proxy);
    ObArenaAllocator allocator("RandDisAddPart");
    ObArray<const ObTableSchema*> orig_table_schemas;
    ObArray<ObTableSchema *> new_table_schemas;
    ObArray<AlterTableSchema *> inc_table_schemas;
    ObArray<AlterTableSchema *> del_table_schemas;
    ObArray<ObTableSchema*> upd_table_schemas;
    ObArray<const ObTableSchema *> inc_table_schema_ptrs;
    ObArray<const ObTableSchema *> del_table_schema_ptrs;
    obrpc::ObAlterTableRes res;
    ObArray<share::ObLSID> ls_id_array;
    const ObSchemaOperationType operation_type = ObDDLService::get_alter_table_schema_operation_type(alter_table_arg);
    ObSchemaGuardWrapper schema_guard_wrapper(tenant_id, schema_service);
    if (OB_FAIL(schema_guard_wrapper.init(schema_guard))) {
      LOG_WARN("fail to init schema guard wrapper", KR(ret));
    } else if (OB_FAIL(ddl_service.check_restore_point_allow(tenant_id, table_schema))) {
      LOG_WARN("check restore point allow failed,", K(ret), K(tenant_id), K(table_schema.get_table_id()));
    } else if (OB_FAIL(ddl_service.generate_tables_array(alter_table_arg,
                                             orig_table_schemas,
                                             new_table_schemas,
                                             inc_table_schemas,
                                             del_table_schemas,
                                             upd_table_schemas,
                                             table_schema,
                                             new_table_schema,
                                             alter_table_schema,
                                             schema_guard_wrapper,
                                             allocator))) {
      LOG_WARN("failed to generate tables array", KR(ret));
    } else if (OB_FAIL(ddl_service.alter_tables_partitions(alter_table_arg,
                                               orig_table_schemas,
                                               new_table_schemas,
                                               inc_table_schemas,
                                               del_table_schemas,
                                               upd_table_schemas,
                                               ddl_operator,
                                               schema_guard_wrapper,
                                               trans))) {
      LOG_WARN("alter table partitions failed", K(ret));
    } else if (orig_table_schemas.count() != new_table_schemas.count()
               || inc_table_schemas.count() != orig_table_schemas.count()
               || del_table_schemas.count() != orig_table_schemas.count()
               || inc_table_schemas.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array count is unexpected" , K(orig_table_schemas), K(new_table_schemas),
               K(inc_table_schemas), K(del_table_schemas), KR(ret));
    } else if (alter_table_schema.is_external_table()) {
      if (alter_table_arg.alter_part_type_ == ObAlterTableArg::ADD_PARTITION
          || alter_table_arg.alter_part_type_ == ObAlterTableArg::DROP_PARTITION) {
        if (OB_FAIL(ddl_service.fill_external_table_res_arg(alter_table_schema, res.res_arg_array_))) {
          LOG_WARN("fail to fill external table res arg", KR(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alter external table op type is not valid", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ddl_service.update_tables_attribute(new_table_schemas, ddl_operator, trans, operation_type, alter_table_arg.ddl_stmt_str_))) {
      LOG_WARN("failed to update tablets attribute", KR(ret), K(new_table_schema));
    } else if (OB_FAIL(ddl_service.handle_tablets_for_alter_partitions(
            alter_table_arg, inc_table_schemas, del_table_schemas, new_table_schemas, schema_guard_wrapper, trans, inc_table_schema_ptrs))) {
      LOG_WARN("fail to handle tablets for alter partitions", KR(ret));
    } else if (OB_UNLIKELY(inc_table_schema_ptrs.empty() || OB_ISNULL(inc_table_schema_ptrs.at(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected values", KR(ret), K(inc_table_schema_ptrs));
    } else if (OB_FAIL(ObTabletRandomMdsHelper::set_autoinc_seq_for_create(*inc_table_schema_ptrs.at(0), max_alloc_seq_id, trans))) {
      LOG_WARN("failed to set autoinc seq for create", KR(ret));
    } else if (OB_FAIL(inc_table_schema_ptrs.at(0)->get_tablet_ids(active_tablet_ids))) {
      LOG_WARN("failed to get new active tablet ids", K(ret), KPC(inc_table_schema_ptrs.at(0)));
    }
  }
  return ret;
}

int ObRandomPartitionHelper::get_max_alloc_seq_id(const ObTableSchema &table_schema, int64_t &max_alloc_seq_id)
{
  int ret = OB_SUCCESS;
  max_alloc_seq_id = 0;
  if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema));
  } else {
    ObPartition **data_partitions = table_schema.get_part_array();
    int64_t part_num = table_schema.get_partition_num();
    ObPartition *last_partition = nullptr;
    if (OB_ISNULL(data_partitions)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data_partitions is null", KR(ret), K(table_schema));
    } else if (FALSE_IT(last_partition = data_partitions[part_num - 1])) {
    } else if (OB_ISNULL(last_partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last partition is null", KR(ret), K(table_schema));
    } else {
      const ObRowkey &high_bound_val = last_partition->get_high_bound_val();
      if (1 != high_bound_val.get_obj_cnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row key of random partitioned table must be single object", KR(ret), K(table_schema));
      } else if (OB_FAIL(high_bound_val.get_obj_ptr()[0].get_int(max_alloc_seq_id))) {
        LOG_WARN("fail to get int from high bound val", KR(ret), K(high_bound_val));
      }
    }
  }
  return ret;
}

int ObRandomPartitionHelper::build_add_partition_arg(rootserver::ObDDLService &ddl_service,
                                                     const ObTableSchema &table_schema,
                                                     const int64_t add_part_num,
                                                     const int64_t max_alloc_seq_id,
                                                     const uint64_t specified_value,
                                                     share::schema::AlterTableSchema &alter_table_schema)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(table_schema.get_tenant_id()));
  int64_t random_partition_interval = 0;
  int64_t next_seq_id = std::max(max_alloc_seq_id, static_cast<int64_t>(specified_value));
  if (!table_schema.is_valid() || add_part_num <= 0 || max_alloc_seq_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema), K(add_part_num), K(max_alloc_seq_id));
  } else if (!tenant_config.is_valid()) {
    ret = OB_EAGAIN;
    LOG_WARN("tenant_config has not been loaded", KR(ret));
  } else if (FALSE_IT(alter_table_schema.set_part_num(add_part_num))) {
  } else if (FALSE_IT(random_partition_interval = tenant_config->_default_random_partition_interval)) {
  } else if (0 >= random_partition_interval) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(random_partition_interval));
  } else {
    const int64_t err_random_partition_interval = std::abs(OB_E(common::EventTable::EN_DEFAULT_RANDOM_PARTITION_INTERVAL) 0);
    if (0 < err_random_partition_interval) {
      random_partition_interval = err_random_partition_interval;
    }
    share::schema::ObPartition new_part;
    for (int64_t i = 0; OB_SUCC(ret) && i < add_part_num; i++) {
      if (OB_UNLIKELY(next_seq_id > INT64_MAX - random_partition_interval)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("next seq for random partition overflow", K(ret), K(table_schema.get_table_id()), K(next_seq_id),
            K(random_partition_interval), K(max_alloc_seq_id), K(specified_value));
      } else {
        next_seq_id += random_partition_interval;
        common::ObObj hign_bound_val_obj(next_seq_id);
        ObRowkey high_bound_val(&hign_bound_val_obj, 1);
        if (OB_FAIL(new_part.set_high_bound_val(high_bound_val))) {
          LOG_WARN("failed to set high_bound_val", KR(ret));
        } else {
          new_part.set_is_empty_partition_name(true);
          new_part.set_tenant_id(table_schema.get_tenant_id());
          new_part.set_table_id(table_schema.get_table_id());
          new_part.set_partition_type(PartitionType::PARTITION_TYPE_NORMAL);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(alter_table_schema.add_partition(new_part))) {
          LOG_WARN("fail to add partition", KR(ret), K(new_part));
        } else {
          new_part.reset();
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ddl_service.fill_part_name(table_schema, alter_table_schema))) {
      LOG_WARN("failed to fill part name", K(ret));
    } else {
      const int64_t part_num = alter_table_schema.get_partition_num();
      share::schema::ObPartition **part_array = alter_table_schema.get_part_array();
      if (OB_ISNULL(part_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid part array", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; i++) {
        if (OB_ISNULL(part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part is null", K(ret), K(part_array[i]));
        } else if (OB_UNLIKELY(part_array[i]->get_part_name().empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part name is empty after fill", K(ret), KPC(part_array[i]));
        } else {
          part_array[i]->set_is_empty_partition_name(false);
        }
      }
    }
  }
  return ret;
}

int ObRandomPartitionHelper::set_tablet_status_to_inactive(const ObIArray<int64_t> &marked_inactive_part_indexs,
                                                           const ObTableSchema &table_schema,
                                                           const bool need_update_table_attribute,
                                                           share::schema::ObSchemaGetterGuard &schema_guard,
                                                           share::schema::ObMultiVersionSchemaService *schema_service,
                                                           common::ObMySQLProxy *sql_proxy,
                                                           ObTableSchema &new_table_schema,
                                                           ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  ObArray<ObTabletID> inactive_data_tablet_ids;
  ObSEArray<uint64_t, 8> upd_table_ids;
  // Snapshot of the data table's part_idx for each entry in marked_inactive_part_indexs.
  // Aux tables (local indexes, lob meta, lob piece) must share the same partition
  // layout (same count and same part_idx ordering) as the data table, so we use
  // this snapshot to defensively cross-check every aux table partition we touch.
  ObSEArray<int64_t, 8> expected_part_idxs;
  const int64_t abs_timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_ts() : ObTimeUtility::current_time() + GCONF.rpc_timeout;
  if (OB_ISNULL(schema_service) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_service), K(sql_proxy));
  } else if (!table_schema.is_valid() || !new_table_schema.is_valid() || !trans.is_started()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema), K(new_table_schema), "trans_started", trans.is_started());
  } else {
    ObSEArray<ObAuxTableMetaInfo, 8> simple_index_infos;
    const uint64_t lob_meta_tid = table_schema.get_aux_lob_meta_tid();
    const uint64_t lob_piece_tid = table_schema.get_aux_lob_piece_tid();
    const uint64_t mlog_tid = table_schema.get_mlog_tid();
    if (OB_FAIL(upd_table_ids.push_back(table_schema.get_table_id()))) {
      LOG_WARN("failed to push back", KR(ret), K(table_schema.get_table_id()));
    } else if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple index infos failed", KR(ret), K(table_schema));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      if (share::schema::is_index_local_storage(simple_index_infos.at(i).index_type_)) {
        if (OB_FAIL(upd_table_ids.push_back(simple_index_infos.at(i).table_id_))) {
          LOG_WARN("push back index table id failed", KR(ret));
        } else if (share::schema::is_search_def_index(simple_index_infos.at(i).index_type_)) {
          // search_data_index hangs off search_def_index via data_table_id, not the data table.
          // Currently unreachable (search index requires heap-organized, random part forbids it),
          // but kept for when random part starts supporting heap-organized tables.
          const ObTableSchema *def_index_schema = nullptr;
          ObSEArray<ObAuxTableMetaInfo, 4> child_infos;
          if (OB_FAIL(schema_guard.get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, def_index_schema))) {
            LOG_WARN("failed to get search def index schema", KR(ret), K(tenant_id), K(simple_index_infos.at(i).table_id_));
          } else if (OB_ISNULL(def_index_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("search def index schema is null", KR(ret), K(simple_index_infos.at(i).table_id_));
          } else if (OB_FAIL(def_index_schema->get_simple_index_infos(child_infos))) {
            LOG_WARN("failed to get child infos of search def index", KR(ret), K(simple_index_infos.at(i).table_id_));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < child_infos.count(); ++j) {
              if (share::schema::is_search_data_index(child_infos.at(j).index_type_)
                  && OB_FAIL(upd_table_ids.push_back(child_infos.at(j).table_id_))) {
                LOG_WARN("push back search data index id failed", KR(ret), K(child_infos.at(j).table_id_));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_ID != lob_meta_tid && OB_FAIL(upd_table_ids.push_back(lob_meta_tid))) {
      LOG_WARN("push back lob meta tid failed", KR(ret), K(lob_meta_tid));
    }
    if (OB_SUCC(ret) && OB_INVALID_ID != lob_piece_tid && OB_FAIL(upd_table_ids.push_back(lob_piece_tid))) {
      LOG_WARN("push back lob piece tid failed", KR(ret), K(lob_piece_tid));
    }
    // mlog inherits the data table's partition layout via assign_partition_schema in ObMLogBuilder.
    if (OB_SUCC(ret) && OB_INVALID_ID != mlog_tid && OB_FAIL(upd_table_ids.push_back(mlog_tid))) {
      LOG_WARN("push back mlog tid failed", KR(ret), K(mlog_tid));
    }
    // Snapshot the data table's part_idx for each marked index. We use the
    // table_schema parameter (= the data table) directly: it's already loaded
    // by the caller and avoids one extra schema_guard.get_table_schema call.
    const int64_t data_part_num = table_schema.get_partition_num();
    ObPartition **data_part_array = table_schema.get_part_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < marked_inactive_part_indexs.count(); ++i) {
      const int64_t part_index = marked_inactive_part_indexs.at(i);
      if (OB_UNLIKELY(part_index < 0 || part_index >= data_part_num)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid part index against data table", KR(ret), K(part_index), K(data_part_num));
      } else if (OB_ISNULL(data_part_array) || OB_ISNULL(data_part_array[part_index])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data partition array or entry is null", KR(ret), K(part_index));
      } else if (OB_FAIL(expected_part_idxs.push_back(data_part_array[part_index]->get_part_idx()))) {
        LOG_WARN("failed to push back expected part_idx", KR(ret), K(part_index));
      }
    }
  }
  for (int64_t ti = upd_table_ids.count() - 1; OB_SUCC(ret) && ti >= 0; ti--) { // update_table_attribute for data table at last
    const int64_t table_id = upd_table_ids.at(ti);
    const ObTableSchema *ori_table_schema = nullptr;
    rootserver::ObDDLOperator ddl_operator(*schema_service, *sql_proxy);
    SMART_VAR(ObTableSchema, upd_table_schema) {
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, ori_table_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(table_id));
    } else if (OB_ISNULL(ori_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("failed to get table schema", KR(ret), K(table_id));
    } else if (OB_UNLIKELY(ori_table_schema->get_partition_num() != table_schema.get_partition_num())) {
      // Defensive: aux table partition count must match the data table.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition num mismatch between data table and aux table",
               KR(ret), K(table_id), "data_part_num", table_schema.get_partition_num(),
               "aux_part_num", ori_table_schema->get_partition_num());
    } else if (OB_FAIL(upd_table_schema.assign(*ori_table_schema))) {
      LOG_WARN("failed to assign", KR(ret), K(table_id));
    } else {
      upd_table_schema.reset_partition_array();
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < marked_inactive_part_indexs.count(); i++) {
      const int64_t part_num = ori_table_schema->get_partition_num();
      const int64_t part_index = marked_inactive_part_indexs.at(i);
      const ObPartition *part = nullptr;
      ObPartition upd_part;
      if (OB_UNLIKELY(0 > part_index || part_index >= part_num)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid part index", KR(ret), K(part_index), K(part_num));
      } else if (OB_ISNULL(ori_table_schema->get_part_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition_array is null", KR(ret), K(part_index));
      } else if (OB_ISNULL(part = ori_table_schema->get_part_array()[part_index])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part is null", KR(ret), K(part_index));
      } else if (OB_UNLIKELY(i >= expected_part_idxs.count()
                             || part->get_part_idx() != expected_part_idxs.at(i))) {
        // Defensive: aux table partition at part_index must share the same
        // part_idx as the data table partition at the same position. If this
        // ever fires it means the data table and an aux table have drifted
        // out of sync (e.g. partition reassign / compact split), and blindly
        // marking the aux partition INACTIVE would touch the wrong tablet.
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_idx mismatch between data table and aux table",
                 KR(ret), K(table_id), K(part_index),
                 "aux_part_idx", part->get_part_idx(),
                 "expected_part_idx", i < expected_part_idxs.count() ? expected_part_idxs.at(i) : -1,
                 KPC(part));
      } else if (ti == 0 && OB_FAIL(inactive_data_tablet_ids.push_back(part->get_tablet_id()))) {
        LOG_WARN("failed to push back tablet id", KR(ret), K(part->get_tablet_id()));
      } else if (OB_FAIL(upd_part.assign(*part))) {
        LOG_WARN("failed to assign part", KR(ret), K(table_id), KPC(part));
      } else {
        upd_part.set_status(ObPartitionStatus::PARTITION_STATUS_INACTIVE);
        if (OB_FAIL(upd_table_schema.add_partition(upd_part))) {
          LOG_WARN("failed to add partition", K(ret), K(upd_part));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", KR(ret));
    } else if (OB_FAIL(schema_service->gen_new_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("fail to gen new schema version", KR(ret), K(table_schema), K(refreshed_schema_version));
    } else if (OB_FAIL(schema_service->get_schema_service()->get_table_sql_service().update_part_info(trans,
                                                                                                      *ori_table_schema,
                                                                                                      upd_table_schema,
                                                                                                      refreshed_schema_version))) {
      LOG_WARN("update split part info failed", KR(ret), KPC(ori_table_schema), K(upd_table_schema), K(refreshed_schema_version));
    } else if (need_update_table_attribute) {
      // Intent: only bump the table's schema_version here — do NOT persist the
      // INACTIVE partition list a second time.
      //
      // The partition status changes have already been written into
      // __all_part / __all_part_history by update_part_info() above. What we
      // still need is a table-level schema_version bump so that schema refresh
      // on observers sees a fresh version and reloads partition status.
      //
      // To avoid emitting a duplicate partition delta into __all_ddl_operation
      // (which would confuse downstream consumers like CDC/OMS that derive
      // partition state from the DDL operation log), we deliberately re-assign
      // upd_table_schema from the *original* schema before calling
      // update_table_attribute. The resulting OB_DDL_SET_PARTITION_INACTIVE
      // operation record then carries only the schema_version bump, while the
      // authoritative partition-status change lives in __all_part.
      //
      // Caller side: need_update_table_attribute is true only when
      // !need_add_partition; if we are also going to add new partitions later,
      // the schema_version bump will piggy-back on add_partitions() and this
      // branch is skipped.
      if (OB_FAIL(upd_table_schema.assign(*ori_table_schema))) {
        LOG_WARN("failed to assign", KR(ret));
      } else if (OB_FAIL(ddl_operator.update_table_attribute(
                      upd_table_schema,
                      trans,
                      ObSchemaOperationType::OB_DDL_SET_PARTITION_INACTIVE,
                      nullptr))) {
        LOG_WARN("failed to update tablets attribute", K(ret), KPC(ori_table_schema));
      }
    }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletRandomMdsHelper::modify_inactive(tenant_id, inactive_data_tablet_ids, abs_timeout_us, trans))) {
    LOG_WARN("failed to modify auto part size", K(ret));
  }
  return ret;
}

int ObRandomPartitionHelper::alter_random_distribution_partition(ObDDLService &ddl_service, obrpc::ObAlterTableArg &arg, obrpc::ObAlterRandomPartitionRes &res)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = arg.alter_table_schema_;
  const uint64_t tenant_id = alter_table_schema.get_tenant_id();
  const ObTableSchema *orig_table_schema = nullptr;
  int64_t refreshed_schema_version = 0;
  uint64_t tenant_data_version = 0;
  ObDDLSQLTransaction trans(&ddl_service.get_schema_service());
  ObSchemaGetterGuard schema_guard;
  schema_guard.set_session_id(arg.session_id_);
  if (OB_UNLIKELY(!ddl_service.is_inited())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get min data version failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ddl_service.get_tenant_schema_guard_with_version_in_inner_table(tenant_id,
                                                                         schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(&ddl_service.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
    LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                   alter_table_schema.get_origin_database_name(),
                                                   alter_table_schema.get_origin_table_name(),
                                                   false /*is_index*/,
                                                   orig_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(alter_table_schema));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("NULL ptr", KR(ret), K(alter_table_schema));
  } else if (OB_UNLIKELY(is_sys_index_table(orig_table_schema->get_table_id())
                      || !orig_table_schema->is_random_part())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index type", KR(ret), KPC(orig_table_schema));
  } else {
    ObArray<ObTabletID> active_tablet_ids;
    ObArray<ObTabletID> inactive_tablet_ids;
    ObArray<int64_t> marked_inactive_part_indexs;
    bool is_balance_finished = true;
    int64_t available_ls_cnt = 0;
    bool need_add_partition = false;
    HEAP_VAR(ObTableSchema, new_table_schema) {
    if (OB_FAIL(new_table_schema.assign(*orig_table_schema))) {
      LOG_WARN("fail to assign schema", KR(ret));
    } else if (OB_FAIL(inactive_tablet_ids.assign(arg.alter_random_partition_arg_.inactive_tablets_))) {
      LOG_WARN("fail to assign inactive tablet ids", KR(ret));
    } else if (OB_FAIL(ObRandomPartitionHelper::get_active_tablets(*orig_table_schema,
                                                                   inactive_tablet_ids,
                                                                   marked_inactive_part_indexs,
                                                                   active_tablet_ids))) {
      LOG_WARN("fail to get active tablets", KR(ret));
    } else if (OB_FAIL(ObRandomPartitionHelper::get_available_ls_cnt(tenant_id, available_ls_cnt))) {
      LOG_WARN("failed to get user ls cnt", K(ret), K(tenant_id));
    } else if (active_tablet_ids.count() == available_ls_cnt) { // do nothing
    } else if (active_tablet_ids.count() > available_ls_cnt // active_tablet_ids.count() > available_ls_cnt, we need set tablet status to inactive
            && OB_FAIL(ObRandomPartitionHelper::mark_tablets_as_inactive(*orig_table_schema,
                                                                         active_tablet_ids.count() - available_ls_cnt,
                                                                         marked_inactive_part_indexs))) {
      LOG_WARN("fail to set tablet status to inactive", KR(ret));
    } else if (OB_FALSE_IT(need_add_partition = active_tablet_ids.count() < available_ls_cnt)) {
    } else if (marked_inactive_part_indexs.count() > 0
            && OB_FAIL(ObRandomPartitionHelper::set_tablet_status_to_inactive(marked_inactive_part_indexs,
                                                                              *orig_table_schema,
                                                                              !need_add_partition/*need_update_table_attribute*/,
                                                                              schema_guard,
                                                                              &ddl_service.get_schema_service(),
                                                                              &ddl_service.get_sql_proxy(),
                                                                              new_table_schema,
                                                                              trans))) {
      LOG_WARN("fail to set tablet status to inactive", KR(ret));
    } else if (need_add_partition
            && OB_FAIL(ObRandomPartitionHelper::add_partitions(ddl_service,
                                                               tenant_id,
                                                               tenant_data_version,
                                                               *orig_table_schema,
                                                               available_ls_cnt - active_tablet_ids.count(),
                                                               &ddl_service.get_schema_service(),
                                                               &ddl_service.get_sql_proxy(),
                                                               new_table_schema,
                                                               active_tablet_ids,
                                                               arg,
                                                               schema_guard,
                                                               trans))) {
      LOG_WARN("fail to add partitions", KR(ret));
    }
    // FIXME: consider new active tablets and tablets marked as inactive in this trans
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(res.active_tablets_.assign(active_tablet_ids))) {
      LOG_WARN("failed to assign", K(ret));
    }
    } // end HEAP_VAR(ObTableSchema, new_table_schema)
    const bool is_commit = OB_SUCC(ret);
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(is_commit))) {
        LOG_WARN("trans end failed", K(is_commit), K(temp_ret));
        ret = is_commit ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish_schema failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObRandomPartitionHelper::is_auto_inc_pk(const ObTableSchema &table_schema, bool &auto_inc_pk)
{
  int ret = OB_SUCCESS;
  auto_inc_pk = false;
  ObRowkeyColumn rowkey_column;
  const ObColumnSchemaV2 *col = NULL;
  if (table_schema.is_table_without_pk() || table_schema.get_rowkey_column_num() != 1) {
  } else if (OB_FAIL(table_schema.get_rowkey_info().get_column(0, rowkey_column))) {
    LOG_WARN("get column info failed", K(ret));
  } else if (NULL == (col = table_schema.get_column_schema(rowkey_column.column_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The rowkey column is NULL, ", K(ret), K(rowkey_column), K(table_schema));
  } else if (col->is_autoincrement()) {
    auto_inc_pk = true;
  }
  return ret;
}

// return OB_INVALID_ID if not a random partitioned table
int ObRandomPartitionHelper::get_random_partkey_column_id(const ObTableSchema &table_schema, uint64_t &column_id)
{
  int ret = OB_SUCCESS;
  column_id = OB_INVALID_ID;
  if (table_schema.is_random_part()) {
    if (OB_FAIL(table_schema.get_partition_key_info().get_column_id(0, column_id))) {
      LOG_WARN("failed to get random partkey column id", K(ret), K(table_schema));
    }
  }
  return ret;
}

int ObRandomPartitionHelper::check_is_random_partkey(const ObTableSchema &table_schema, const uint64_t column_id, bool &is_random_partkey)
{
  int ret = OB_SUCCESS;
  uint64_t random_partkey_column_id = OB_INVALID_ID;
  if (OB_FAIL(get_random_partkey_column_id(table_schema, random_partkey_column_id))) {
    LOG_WARN("failed to get random partkey column id", K(ret), K(column_id));
  } else {
    is_random_partkey = OB_INVALID_ID != random_partkey_column_id && column_id == random_partkey_column_id;
  }
  return ret;
}

int ObRandomPartitionHelper::check_enable_random_partition(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  uint64_t data_version = 0;
  bool is_table_column_store = false;
  bool auto_inc_pk = false;
  ObArray<ObAuxTableMetaInfo> simple_index_infos;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(is_auto_inc_pk(table_schema, auto_inc_pk))) {
    LOG_WARN("fail to get is auto inc pk");
  } else if (data_version < DATA_VERSION_4_6_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("current data version doesn't support to random partition", KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.6.1, random partitioned table is ");
  } else if (table_schema.is_in_recyclebin()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to split table in recyclebin", KR(ret), K(table_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "recyclebin table is");
  } else if (table_schema.is_table_with_pk() && (!auto_inc_pk || table_schema.is_order_auto_increment_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support to set random partition on table without primary key or table with auto increment primary key", KR(ret), K(table_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "random partition with primary key(s) is");
  } else if (!table_schema.is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to split a partition of non-user table", KR(ret), K(table_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-user table is");
  }
  return ret;
}

int ObRandomPartitionHelper::resolve_partition_random(const bool is_subpartition, ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObString func_expr_name;
  int64_t partition_num = 0;
  share::schema::ObPartitionFuncType part_func_type = share::schema::PARTITION_FUNC_TYPE_RANGE;
  common::ObSEArray<ObRawExpr*, 8> part_func_exprs;
  common::ObSEArray<ObRawExpr*, 8> range_values_exprs;
  if (OB_UNLIKELY(is_subpartition)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("such random partition not supported", K(ret));
  } else if (table_schema.is_table_without_pk() && table_schema.is_partitioned_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("already partitioned not supported", K(ret));
  } else {
    // FIXME: add check data_version
    if (OB_SUCC(ret)) {
      ObString func_expr_name;
      const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
      const ObColumnSchemaV2 *rowkey_column = nullptr;
      uint64_t rowkey_column_id = OB_INVALID_ID;
      if (OB_UNLIKELY(1 != rowkey_info.get_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid rowkey cnt", K(ret), K(rowkey_info));
      } else if (OB_FAIL(rowkey_info.get_column_id(0, rowkey_column_id))) {
        LOG_WARN("failed to get rowkey column id", K(ret), K(rowkey_info));
      } else if (OB_ISNULL(rowkey_column = table_schema.get_column_schema(rowkey_column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get rowkey column", K(ret), K(rowkey_column_id), K(table_schema));
      } else if (table_schema.is_table_without_pk()
          || (table_schema.is_index_organized_table() && ObTableAutoIncrementMode::NOORDER == table_schema.get_table_auto_increment_mode()
            && rowkey_column->is_autoincrement() && rowkey_column_id != OB_HIDDEN_PK_INCREMENT_COLUMN_ID
            && (ObIntType == rowkey_column->get_data_type() || ObUInt64Type == rowkey_column->get_data_type()))) {
        // Add initial active partition for random partitioned table
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(table_schema.get_tenant_id()));
        int64_t default_random_partition_interval = 0;
        int64_t part_num = 0;
        lib::Worker::CompatMode tenant_mode = lib::Worker::CompatMode::INVALID;
        if (OB_UNLIKELY(!tenant_config.is_valid())) {
          ret = OB_EAGAIN;
          LOG_WARN("tenant_config has not been loaded", KR(ret));
        } else if (OB_UNLIKELY(0 > (default_random_partition_interval = tenant_config->_default_random_partition_interval))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid default random partition interval", KR(ret), K(default_random_partition_interval));
        } else if (OB_ISNULL(GCTX.sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL GCTX.sql_proxy_", K(ret));
        } else if (OB_FAIL(rootserver::ObRandomPartitionHelper::get_available_ls_cnt(table_schema.get_tenant_id(), part_num))) {
          LOG_WARN("fail to get available ls", KR(ret), K(table_schema.get_tenant_id()));
        } else {
          const int64_t err_random_partition_interval = std::abs(OB_E(common::EventTable::EN_DEFAULT_RANDOM_PARTITION_INTERVAL) 0);
          if (0 < err_random_partition_interval) {
            default_random_partition_interval = err_random_partition_interval;
          }
          int64_t next_seq_id = 0;
          table_schema.set_part_level(share::schema::PARTITION_LEVEL_ONE);
          table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_RANDOM);
          table_schema.get_part_option().set_part_num(part_num);
          share::schema::ObPartition new_part;
          int64_t max_part_id = OB_MAX_PARTITION_NUM_MYSQL;
          ObString part_name_str;
          for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
            next_seq_id += default_random_partition_interval;
            common::ObObj hign_bound_val_obj(next_seq_id);
            ObRowkey high_bound_val(&hign_bound_val_obj, 1);
            if (OB_FAIL(new_part.set_high_bound_val(high_bound_val))) {
              LOG_WARN("failed to set high_bound_val", KR(ret));
            } else {
              new_part.set_tenant_id(table_schema.get_tenant_id());
              new_part.set_table_id(table_schema.get_table_id());
              new_part.set_partition_type(PartitionType::PARTITION_TYPE_NORMAL);
              new_part.set_status(ObPartitionStatus::PARTITION_STATUS_ACTIVE);
              new_part.set_part_idx(i);
              char part_name[OB_MAX_PARTITION_NAME_LENGTH];
              int64_t pos = 0;
              if (OB_FAIL(databuff_printf(part_name, OB_MAX_PARTITION_NAME_LENGTH, pos, "P%ld", max_part_id))) {
                LOG_WARN("failed to constrate partition name", K(ret), K(max_part_id));
              } else {
                part_name_str.assign(part_name, static_cast<int32_t>(pos));
                if (OB_FAIL(new_part.set_part_name(part_name_str))) {
                  LOG_WARN("failed to set partition name", K(ret), K(part_name_str));
                }
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(table_schema.add_partition(new_part))) {
              LOG_WARN("fail to add partition", KR(ret), K(new_part));
            } else {
              new_part.reset();
              max_part_id++;
            }
          }
        }

        ObString part_expr;
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(table_schema.get_allocator())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid allocator", K(ret), K(table_schema));
        } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(table_schema.get_tenant_id(), tenant_mode))) {
          LOG_WARN("fail to get tenant mode", K(ret), K(table_schema.get_tenant_id()));
        } else if (OB_FAIL(ObAutoSplitArgBuilder::print_identifier(*table_schema.get_allocator(),
                tenant_mode == lib::Worker::CompatMode::ORACLE, rowkey_column->get_column_name(), part_expr))) {
          LOG_WARN("failed to print identifier", K(ret), K(tenant_mode), KPC(rowkey_column));
        } else if (OB_FAIL(table_schema.get_part_option().set_part_expr(part_expr))) {
          LOG_WARN("fail to set part expr", KR(ret), K(table_schema));
        } else if (OB_FAIL(table_schema.add_partition_key(rowkey_column->get_column_id()))) {
          LOG_WARN("fail to add partition key", KR(ret), K(table_schema));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported random partition", K(ret), KPC(rowkey_column), K(table_schema));
      }
    }
  }
  return ret;
}

int ObRandomPartitionHelper::reset_to_initial_partitions(const int64_t target_part_num,
                                                         ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_schema.is_random_part())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not a random-partitioned table", KR(ret), K(table_schema));
  } else if (OB_UNLIKELY(target_part_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target_part_num", KR(ret), K(target_part_num), K(table_schema));
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    int64_t default_random_partition_interval = 0;
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_EAGAIN;
      LOG_WARN("tenant_config has not been loaded", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(0 >= (default_random_partition_interval = tenant_config->_default_random_partition_interval))) {
      // Mirror the boundary in build_add_partition_arg(): a zero interval would make
      // every partition share the same high_bound_val and next_seq_id, which is invalid.
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid default random partition interval", KR(ret), K(default_random_partition_interval));
    } else {
      const int64_t err_random_partition_interval = std::abs(OB_E(common::EventTable::EN_DEFAULT_RANDOM_PARTITION_INTERVAL) 0);
      if (0 < err_random_partition_interval) {
        default_random_partition_interval = err_random_partition_interval;
      }
      // drop all existing partitions and rebuild from scratch
      table_schema.reset_partition_array();
      table_schema.get_part_option().set_part_num(target_part_num);
      int64_t next_seq_id = 0;
      int64_t max_part_id = OB_MAX_PARTITION_NUM_MYSQL;
      share::schema::ObPartition new_part;
      ObString part_name_str;
      for (int64_t i = 0; OB_SUCC(ret) && i < target_part_num; ++i) {
        if (OB_UNLIKELY(next_seq_id > INT64_MAX - default_random_partition_interval)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("next seq for random partition overflow", K(ret), K(next_seq_id), K(default_random_partition_interval));
        } else {
          next_seq_id += default_random_partition_interval;
          common::ObObj high_bound_val_obj(next_seq_id);
          ObRowkey high_bound_val(&high_bound_val_obj, 1);
          if (OB_FAIL(new_part.set_high_bound_val(high_bound_val))) {
            LOG_WARN("failed to set high_bound_val", KR(ret));
          } else {
            new_part.set_tenant_id(tenant_id);
            new_part.set_table_id(table_schema.get_table_id());
            new_part.set_partition_type(PartitionType::PARTITION_TYPE_NORMAL);
            new_part.set_status(ObPartitionStatus::PARTITION_STATUS_ACTIVE);
            new_part.set_part_idx(i);
            char part_name[OB_MAX_PARTITION_NAME_LENGTH];
            int64_t pos = 0;
            if (OB_FAIL(databuff_printf(part_name, OB_MAX_PARTITION_NAME_LENGTH, pos, "P%ld", max_part_id))) {
              LOG_WARN("failed to construct partition name", KR(ret), K(max_part_id));
            } else {
              part_name_str.assign(part_name, static_cast<int32_t>(pos));
              // set_part_name() internally does deep_copy_str() on the input, so it is
              // safe to reuse the stack buffer `part_name` across loop iterations.
              if (OB_FAIL(new_part.set_part_name(part_name_str))) {
                LOG_WARN("failed to set partition name", KR(ret), K(part_name_str));
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(table_schema.add_partition(new_part))) {
          LOG_WARN("fail to add partition", KR(ret), K(new_part));
        } else {
          new_part.reset();
          max_part_id++;
        }
      }
      LOG_INFO("reset random-partitioned table to initial partitions",
               KR(ret), K(tenant_id), "table_id", table_schema.get_table_id(),
               K(target_part_num), K(default_random_partition_interval));
    }
  }
  return ret;
}

}//common
}//oceanbase
