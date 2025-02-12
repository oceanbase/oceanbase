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

#include "ob_recover_table_initiator.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "ob_restore_util.h"
#include "share/restore/ob_recover_table_persist_helper.h"
#include "share/restore/ob_import_table_persist_helper.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "share/restore/ob_import_util.h"

using namespace oceanbase;
using namespace share::schema;
using namespace common;
using namespace obrpc;
using namespace rootserver;
using namespace share;

int ObRecoverTableInitiator::init(
    share::schema::ObMultiVersionSchemaService *schema_service, common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRecoverTableInitiator init twice", K(ret));
  } else if (OB_ISNULL(schema_service) || OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema service and sql prxoy must not be null", K(ret));
  } else {
    schema_service_ = schema_service;
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObRecoverTableInitiator::initiate_recover_table(const obrpc::ObRecoverTableArg &arg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverTableInitiator is not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ObRecoverTableArg", K(ret), K(arg));
  } else if (OB_FAIL(check_before_initiate_(arg))) {
    LOG_WARN("failed to check before initiate", K(ret));
  } else if (obrpc::ObRecoverTableArg::Action::INITIATE == arg.action_) {
    if (OB_FAIL(start_recover_table_(arg))) {
      LOG_WARN("failed to start recover table", K(ret), K(arg));
    }
  } else if (obrpc::ObRecoverTableArg::Action::CANCEL == arg.action_) {
    if (OB_FAIL(cancel_recover_table_(arg))) {
      LOG_WARN("failed to cancel recover table", K(ret), K(arg));
    }
  }
  return ret;
}

int ObRecoverTableInitiator::is_recover_job_exist(const uint64_t target_tenant_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  share::ObRecoverTablePersistHelper table_op;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverTableInitiator is not init", K(ret));
  } else if (!is_user_tenant(target_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target_tenant_id", K(ret), K(target_tenant_id));
  } else if (OB_FAIL(table_op.init(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to init sys table op", K(ret));
  } else if (OB_FAIL(table_op.is_recover_table_job_exist(*sql_proxy_, target_tenant_id, is_exist))) {
    LOG_WARN("failed to check recover table job exist", K(ret), K(target_tenant_id));
  }
  return ret;
}

int ObRecoverTableInitiator::start_recover_table_(const obrpc::ObRecoverTableArg &arg)
{
  int ret = OB_SUCCESS;
  share::ObRecoverTableJob job;
  ObPhysicalRestoreJob physical_restore_job;
  if (OB_FALSE_IT(job.set_status(share::ObRecoverTableStatus::PREPARE))) {
  } else if (OB_FAIL(job.set_target_tenant_name(arg.tenant_name_))) {
    LOG_WARN("failed to set traget tenant name", K(ret));
  } else if (OB_FALSE_IT(job.set_target_tenant_id(arg.tenant_id_))) {
  } else if (OB_FAIL(job.set_description(arg.restore_tenant_arg_.description_))) {
    LOG_WARN("failed to set description", K(ret));
  } else if (OB_FAIL(fill_aux_tenant_restore_info_(arg, job, physical_restore_job))) {
    LOG_WARN("failed to fill aux tenant resetore info", K(ret), K(arg));
  } else if (OB_FAIL(fill_recover_table_arg_(arg, job))) {
    LOG_WARN("failed to fill recover table arg", K(ret));
  } else if (OB_FAIL(check_recover_table_target_schema_(job))) {
    LOG_WARN("failed to check recover table arg", K(ret), K(job));
  } else if (OB_FAIL(insert_sys_job_(job, physical_restore_job))) {
    LOG_WARN("failed to insert sys recover table job", K(ret));
  } else {
    LOG_INFO("initiate recover table succeed", K(ret), K(job));
  }
  uint64_t tenant_id = arg.tenant_id_;
  int64_t job_id = job.get_job_id();
  share::ObTaskId trace_id(*ObCurTraceId::get_trace_id());
  ROOTSERVICE_EVENT_ADD("recover_table", "start_recover_table", K(tenant_id), K(job_id), K(ret), K(trace_id));
  return ret;
}
int ObRecoverTableInitiator::cancel_recover_table_(const obrpc::ObRecoverTableArg &arg)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  share::ObRecoverTablePersistHelper recover_helper;
  share::ObImportTableJobPersistHelper import_helper;
  uint64_t exec_tenant_id = gen_meta_tenant_id(arg.tenant_id_);
  if (OB_FAIL(recover_helper.init(arg.tenant_id_))) {
    LOG_WARN("failed to init helper", K(ret), K(arg));
  } else if (OB_FAIL(import_helper.init(arg.tenant_id_))) {
    LOG_WARN("failed to init helper", K(ret), K(arg));
  } else if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
    LOG_WARN("failed to start trans", K(ret), K(exec_tenant_id));
  } else if (OB_FAIL(import_helper.force_cancel_import_job(trans))) {
    LOG_WARN("failed to force cancel import job", K(ret), K(arg));
  } else if (OB_FAIL(recover_helper.force_cancel_recover_job(trans))) {
    LOG_WARN("failed to force cancel recover job", K(ret), K(arg));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", K(ret));
    }
  }

  ROOTSERVICE_EVENT_ADD("recover_table", "cancel_recover_table", "tenant_id", arg.tenant_id_, "result", ret);
  return ret;
}

int ObRecoverTableInitiator::insert_sys_job_(
    share::ObRecoverTableJob &job, share::ObPhysicalRestoreJob &physical_restore_job)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t job_id = -1;
  if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    share::ObRecoverTablePersistHelper helper;
    if (OB_FAIL(ObLSBackupInfoOperator::get_next_job_id(trans, OB_SYS_TENANT_ID, job_id))) {
      LOG_WARN("failed to get next job_id", K(ret));
    } else if (OB_FALSE_IT(job.set_tenant_id(OB_SYS_TENANT_ID))) {
    } else if (OB_FALSE_IT(job.set_initiator_tenant_id(OB_SYS_TENANT_ID))) {
    } else if (OB_FALSE_IT(job.set_job_id(job_id))) {
    } else if (OB_FALSE_IT(job.set_initiator_job_id(0/*sys job default value*/))) {
    } else if (OB_FALSE_IT(job.set_start_ts(ObTimeUtility::current_time()))) {
    } else if (OB_FAIL(helper.init(OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to init sys table op", K(ret));
    } else if (OB_FAIL(helper.insert_recover_table_job(trans, job))) {
      LOG_WARN("failed to insert initital recover table job", K(ret), K(job));
    }

    if (FAILEDx(RS_JOB_CREATE_EXT(job_id, RESTORE_TENANT, trans, "sql_text", "restore aux tenant"))) {
      LOG_WARN("failed to get job id", K(ret));
    } else if (OB_FALSE_IT(physical_restore_job.init_restore_key(OB_SYS_TENANT_ID, job_id))) {
    } else if (OB_FALSE_IT(physical_restore_job.set_restore_start_ts(ObTimeUtility::current_time()))) {
    } else if (OB_FALSE_IT(physical_restore_job.set_initiator_job_id(job.get_job_id()))) {
    } else if (OB_FALSE_IT(physical_restore_job.set_initiator_tenant_id(OB_SYS_TENANT_ID))) {
    } else if (OB_FALSE_IT(physical_restore_job.set_recover_table(true))) {
    } else if (OB_FAIL(fill_recover_table_restore_type_(physical_restore_job))) {
    } else if (OB_FAIL(ObRestoreUtil::record_physical_restore_job(trans, physical_restore_job))) {
      LOG_WARN("failed to record physical restore job", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObRecoverTableInitiator::check_before_initiate_(const obrpc::ObRecoverTableArg &arg)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard guard;
  uint64_t target_tenant_id = 0;
  if (!is_user_tenant(arg.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "TENANT, it must be user tenant");
    LOG_WARN("invlaid tenant id, it must be user tenant", K(ret), K(arg.tenant_id_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("failed to get sys schema guard", K(ret));
  } else if (OB_FAIL(guard.get_tenant_id(arg.tenant_name_, target_tenant_id))) {
    LOG_WARN("failed to get tenant id", K(ret), K(arg.tenant_name_));
  } else if (arg.tenant_id_ != target_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg tenant id and tenant name must be couple", K(ret), K(arg));
  }
  return ret;
}

int ObRecoverTableInitiator::fill_aux_tenant_name_(share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  char aux_tenant_name[OB_MAX_TENANT_NAME_LENGTH] = "";
  if (OB_FAIL(databuff_printf(aux_tenant_name, OB_MAX_TENANT_NAME_LENGTH, "AUX_RECOVER$%ld", ObTimeUtility::current_time()))) {
    LOG_WARN("failed to generate aux tenant name", K(ret));
  } else if (OB_FAIL(job.set_aux_tenant_name(ObString(aux_tenant_name)))) {
    LOG_WARN("failed to set aux tenant name", K(ret), K(aux_tenant_name));
  }
  return ret;
}

int ObRecoverTableInitiator::fill_aux_tenant_restore_info_(
    const obrpc::ObRecoverTableArg &arg, share::ObRecoverTableJob &job, share::ObPhysicalRestoreJob &physical_restore_job)
{
  int ret = OB_SUCCESS;
  obrpc::ObPhysicalRestoreTenantArg tenant_restore_arg;
  if (OB_FAIL(fill_aux_tenant_name_(job))) {
    LOG_WARN("failed to fill aux tenant name", K(ret));
  } else if (OB_FAIL(tenant_restore_arg.assign(arg.restore_tenant_arg_))) {
    LOG_WARN("failed to assign tenant restore arg", K(ret), K(arg.restore_tenant_arg_));
  } else if (OB_FALSE_IT(tenant_restore_arg.tenant_name_ = job.get_aux_tenant_name())) {
  } else if (OB_FAIL(ObRestoreUtil::fill_physical_restore_job(1/*fake job id*/, tenant_restore_arg, physical_restore_job))) {
    LOG_WARN("failed to fill physical restore job", K(ret), K(tenant_restore_arg));
  } else if (OB_FALSE_IT(job.set_restore_scn(physical_restore_job.get_restore_scn()))) {
  } else if (OB_FAIL(job.set_restore_option(physical_restore_job.get_restore_option()))) {
    LOG_WARN("failed to set restore option", K(ret));
  } else if (OB_FAIL(job.set_backup_dest(physical_restore_job.get_backup_dest()))) {
    LOG_WARN("failed to set backup dest", K(ret));
  } else if (OB_FAIL(job.set_external_kms_info(physical_restore_job.get_kms_info()))) {
    LOG_WARN("failed to set kms info", K(ret));
  } else if (OB_FAIL(job.set_backup_passwd(physical_restore_job.get_passwd_array()))) {
    LOG_WARN("failed to set backup passwd", K(ret));
  } else if (OB_FAIL(job.get_multi_restore_path_list().assign(physical_restore_job.get_multi_restore_path_list()))) {
    LOG_WARN("faield to assign multi restore path", K(ret));
  }
  return ret;
}

int ObRecoverTableInitiator::fill_recover_database(
    const share::ObImportArg &import_arg,
    share::ObImportTableArg &import_table_arg)
{
  //TODO(zeyong) move duplicate item checking logic to ObImportArg internal later.
  int ret = OB_SUCCESS;
  const share::ObImportDatabaseArray &db_array = import_arg.get_import_database_array();
  ARRAY_FOREACH(db_array.get_items(), i) {
    const share::ObImportDatabaseItem db_item = db_array.get_items().at(i);
    if (OB_FAIL(import_table_arg.add_database(db_item))) {
      LOG_WARN("failed to add database", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed fill recover database", K(import_arg), K(db_array), K(import_table_arg.get_import_database_array()));
  }
  return ret;
}

int ObRecoverTableInitiator::fill_recover_table(
    const share::ObImportArg &import_arg,
    share::ObImportTableArg &import_table_arg)
{
  int ret = OB_SUCCESS;
  const share::ObImportTableArray &table_array = import_arg.get_import_table_array();
  bool is_dup = false;
  ObSqlString dup_item_str;
  ARRAY_FOREACH(table_array.get_items(), i) {
    const share::ObImportTableItem table_item = table_array.get_items().at(i);
    share::ObImportDatabaseItem db_item(table_item.mode_, table_item.database_name_.ptr(), table_item.database_name_.length());
    if (OB_FAIL(import_table_arg.check_database_dup(db_item, is_dup, dup_item_str))) {
      LOG_WARN("failed to check database dup", K(ret));
    } else if (is_dup) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("duplicate database", K(table_item));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, dup_item_str.ptr());
    } else if (OB_FAIL(import_table_arg.add_table(table_item))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed fill recover table", K(import_arg), K(table_array), K(import_table_arg.get_import_table_array()));
  }
  return ret;
}

int ObRecoverTableInitiator::fill_recover_partition(
    const share::ObImportArg &import_arg,
    share::ObImportTableArg &import_table_arg)
{
  int ret = OB_SUCCESS;
  bool is_dup = true;
  ObSqlString dup_item_str;
  const share::ObImportPartitionArray &partition_array = import_arg.get_import_partition_array();
  ARRAY_FOREACH(partition_array.get_items(), i) {
    const share::ObImportPartitionItem partition_item = partition_array.get_items().at(i);
    share::ObImportDatabaseItem db_item(partition_item.mode_,
                                                partition_item.database_name_.ptr(),
                                                partition_item.database_name_.length());
    share::ObImportTableItem table_item(partition_item.mode_,
                                                partition_item.database_name_.ptr(),
                                                partition_item.database_name_.length(),
                                                partition_item.table_name_.ptr(),
                                                partition_item.table_name_.length());
    if (OB_FAIL(import_table_arg.check_database_dup(db_item, is_dup, dup_item_str))) {
      LOG_WARN("failed to check database dup", K(ret));
    } else if (is_dup) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("duplicate database", K(table_item));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, dup_item_str.ptr());
    } else if (OB_FAIL(import_table_arg.check_table_dup(table_item, is_dup, dup_item_str))) {
      LOG_WARN("failed to check table dup", K(ret));
    } else if (is_dup) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("duplicate table", K(table_item));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, dup_item_str.ptr());
    } else if (OB_FAIL(import_table_arg.add_partition(partition_item))) {
      LOG_WARN("failed to add partition", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed fill recover partition", K(import_arg), K(partition_array), K(import_table_arg.get_import_partition_array()));
  }
  return ret;
}

int ObRecoverTableInitiator::fill_remap_database(
    const share::ObImportArg &import_arg,
    const share::ObImportTableArg &import_table_arg,
    share::ObImportRemapArg &import_remap_arg)
{
  int ret = OB_SUCCESS;
  bool is_dup = true;
  ObSqlString dup_item_str;
  const share::ObRemapDatabaseArray &remap_db_array = import_arg.get_remap_database_array();
  ARRAY_FOREACH(remap_db_array.get_remap_items(), i) {
    const share::ObRemapDatabaseItem remap_db_item = remap_db_array.get_remap_items().at(i);
    const share::ObImportDatabaseItem src_db_item(remap_db_item.src_.mode_,
                                                          remap_db_item.src_.name_.ptr(),
                                                          remap_db_item.src_.name_.length());
    const share::ObImportDatabaseItem target_db_item(remap_db_item.target_.mode_,
                                                             remap_db_item.target_.name_.ptr(),
                                                             remap_db_item.target_.name_.length());
    if (OB_FAIL(import_table_arg.check_database_dup(src_db_item, is_dup, dup_item_str))) {
      LOG_WARN("failed to check database dup", K(ret));
    } else if (!is_dup) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("remap not exist database", K(src_db_item));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "remap not exist recover database");
    } else if (OB_FAIL(import_table_arg.check_database_dup(target_db_item, is_dup, dup_item_str))) {
      LOG_WARN("failed to check dup", K(ret));
    } else if (is_dup) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("remap exist database", K(src_db_item));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, dup_item_str.ptr());
    } else if (OB_FAIL(import_remap_arg.add_remap_database(remap_db_item))) {
      LOG_WARN("failed to add database", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed fill remap database", K(import_arg), K(remap_db_array), K(import_remap_arg.get_remap_database_array()));
  }
  return ret;
}

int ObRecoverTableInitiator::fill_remap_table(
    const share::ObImportArg &import_arg,
    const share::ObImportTableArg &import_table_arg,
    share::ObImportRemapArg &import_remap_arg)
{
  int ret = OB_SUCCESS;
  bool is_dup = true;
  ObSqlString dup_item_str;
  const share::ObRemapTableArray &remap_table_array = import_arg.get_remap_table_array();
  ARRAY_FOREACH(remap_table_array.get_remap_items(), i) {
    const share::ObRemapTableItem remap_table_item = remap_table_array.get_remap_items().at(i);
    const share::ObImportTableItem src_table_item(remap_table_item.src_.mode_,
                                                          remap_table_item.src_.database_name_.ptr(),
                                                          remap_table_item.src_.database_name_.length(),
                                                          remap_table_item.src_.table_name_.ptr(),
                                                          remap_table_item.src_.table_name_.length());
    const share::ObImportTableItem target_table_item(remap_table_item.target_.mode_,
                                                             remap_table_item.target_.database_name_.ptr(),
                                                             remap_table_item.target_.database_name_.length(),
                                                             remap_table_item.target_.table_name_.ptr(),
                                                             remap_table_item.target_.table_name_.length());
    if (OB_FAIL(import_table_arg.check_table_dup(src_table_item, is_dup, dup_item_str))) {
      LOG_WARN("failed to check table dup", K(ret));
    } else if (!is_dup) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("remap not exist table", K(src_table_item));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "remap not exist recover table");
    } else if (OB_FAIL(import_table_arg.check_table_dup(target_table_item, is_dup, dup_item_str))) {
    } else if (is_dup) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("remap exist table", K(target_table_item));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, dup_item_str.ptr());
    } else if (OB_FAIL(import_remap_arg.add_remap_table(remap_table_item))) {
      LOG_WARN("failed to add remap table", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed fill remap table", K(import_arg), K(remap_table_array), K(import_remap_arg.get_remap_table_array()));
  }
  return ret;
}

int ObRecoverTableInitiator::fill_remap_partition(
    const share::ObImportArg &import_arg,
    const share::ObImportTableArg &import_table_arg,
    share::ObImportRemapArg &import_remap_arg)
{
  int ret = OB_SUCCESS;
  bool is_dup = true;
  ObSqlString dup_item_str;
  const share::ObRemapPartitionArray &remap_partition_array = import_arg.get_remap_partition_array();
  ARRAY_FOREACH(remap_partition_array.get_remap_items(), i) {
    const share::ObRemapPartitionItem remap_part_item = remap_partition_array.get_remap_items().at(i);
    const share::ObImportPartitionItem src_part_item(remap_part_item.src_.mode_,
                                                          remap_part_item.src_.database_name_.ptr(),
                                                          remap_part_item.src_.database_name_.length(),
                                                          remap_part_item.src_.table_name_.ptr(),
                                                          remap_part_item.src_.table_name_.length(),
                                                          remap_part_item.src_.partition_name_.ptr(),
                                                          remap_part_item.src_.partition_name_.length());
    const share::ObImportTableItem target_table_item(remap_part_item.target_.mode_,
                                                             remap_part_item.target_.database_name_.ptr(),
                                                             remap_part_item.target_.database_name_.length(),
                                                             remap_part_item.target_.table_name_.ptr(),
                                                             remap_part_item.target_.table_name_.length());
    if (OB_FAIL(import_table_arg.check_partion_dup(src_part_item, is_dup, dup_item_str))) {
      LOG_WARN("failed to check dup", K(ret));
    } else if (!is_dup) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("remap not exist partition", K(src_part_item));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "remap not exist recover partition");
    } else if (OB_FAIL(import_table_arg.check_table_dup(target_table_item, is_dup, dup_item_str))) {
      LOG_WARN("failed to check dup", K(ret));
    } else if (is_dup) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("remap exist partition", K(target_table_item));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, dup_item_str.ptr());
    } else if (OB_FAIL(import_remap_arg.add_remap_parition(remap_part_item))) {
      LOG_WARN("failed to add remap partition", K(ret));
    }
  }
  return ret;
}

int ObRecoverTableInitiator::fill_remap_tablespace(
    const share::ObImportArg &import_arg,
    share::ObImportRemapArg &import_remap_arg)
{
  int ret = OB_SUCCESS;
  const share::ObRemapTablespaceArray &remap_tablespace_array = import_arg.get_remap_tablespace_array();
  ARRAY_FOREACH(remap_tablespace_array.get_remap_items(), i) {
    const share::ObRemapTablespaceItem remap_tablespace_item = remap_tablespace_array.get_remap_items().at(i);
    if (OB_FAIL(import_remap_arg.add_remap_tablespace(remap_tablespace_item))) {
      LOG_WARN("failed to add tablespace", K(ret));
    }
  }
  return ret;
}

int ObRecoverTableInitiator::fill_remap_tablegroup(
    const share::ObImportArg &import_arg,
    share::ObImportRemapArg &import_remap_arg)
{
  int ret = OB_SUCCESS;
  const share::ObRemapTablegroupArray &remap_tablegroup_array = import_arg.get_remap_tablegroup_array();
  ARRAY_FOREACH(remap_tablegroup_array.get_remap_items(), i) {
    const share::ObRemapTablegroupItem remap_tablegroup_item = remap_tablegroup_array.get_remap_items().at(i);
    if (OB_FAIL(import_remap_arg.add_remap_tablegroup(remap_tablegroup_item))) {
      LOG_WARN("failed to add tablespace", K(ret));
    }
  }
  return ret;
}


int ObRecoverTableInitiator::fill_recover_table_arg_(
    const obrpc::ObRecoverTableArg &arg, share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  share::ObImportTableArg &import_table_arg = job.get_import_arg().get_import_table_arg();
  share::ObImportRemapArg &import_remap_arg = job.get_import_arg().get_remap_table_arg();
  LOG_INFO("succeed fill arg", K(arg), K(import_table_arg), K(import_remap_arg));
  if (arg.import_arg_.get_import_table_arg().is_import_all()) {
    import_table_arg.set_import_all();
  } else if (OB_FAIL(fill_recover_database(arg.import_arg_, import_table_arg))) {
    LOG_WARN("failed to recover database", K(ret), K(arg.import_arg_));
  } else if (OB_FAIL(fill_recover_table(arg.import_arg_, import_table_arg))) {
    LOG_WARN("failed to recover table", K(ret), K(arg.import_arg_));
  } else if (OB_FAIL(fill_recover_partition(arg.import_arg_, import_table_arg))) {
    LOG_WARN("failed to recover partition", K(ret), K(arg.import_arg_));
  } else if (OB_FAIL(fill_remap_database(arg.import_arg_, import_table_arg, import_remap_arg))) {
    LOG_WARN("failed to remap database", K(ret), K(arg.import_arg_));
  } else if (OB_FAIL(fill_remap_table(arg.import_arg_, import_table_arg, import_remap_arg))) {
    LOG_WARN("failed to remap table", K(ret), K(arg.import_arg_));
  } else if (OB_FAIL(fill_remap_partition(arg.import_arg_, import_table_arg, import_remap_arg))) {
    LOG_WARN("failed to remap partition", K(ret), K(arg.import_arg_));
  } else if (OB_FAIL(fill_remap_tablespace(arg.import_arg_, import_remap_arg))) {
    LOG_WARN("failed to remap tablespace", K(ret), K(arg.import_arg_));
  } else if (OB_FAIL(fill_remap_tablegroup(arg.import_arg_, import_remap_arg))) {
    LOG_WARN("failed to remap tablegroup", K(ret), K(arg.import_arg_));
  }
  return ret;
}

int ObRecoverTableInitiator::fill_recover_table_restore_type_(share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  /* 4.3.3 share noting mode support quick restore; 4.3.5 support mds table standby tenant read.
   * In share noting mode, backup set data_version >= 4.3.5, recover table use quick physical restore.
   * In other condition, recover table use full physical restore. */
  /* tenant_compatible_ & backup_compatible has been check in fill physical restore job */
  const uint64_t source_data_version = job.get_source_data_version();
  const ObBackupSetFileDesc::Compatible backup_compatible =
    static_cast<ObBackupSetFileDesc::Compatible>(job.get_backup_compatible());
  const bool is_allow_quick_restore = ObBackupSetFileDesc::is_allow_quick_restore(backup_compatible);
  const bool is_allow_mds_standby_read = ObTransferUtils::enable_transfer_dml_ctrl(source_data_version);
  // use quick restore should set table cnt display mode.
  if (is_allow_quick_restore && is_allow_mds_standby_read) {
    job.set_restore_type(QUICK_RESTORE_TYPE);
    job.set_progress_display_mode(TABLET_CNT_DISPLAY_MODE);
  }

  LOG_INFO("[RECOVER_TABLE] set recover table restore type", "restore_type", QUICK_RESTORE_TYPE,
    "progress_display_mode", TABLET_CNT_DISPLAY_MODE);

  return ret;
}

int ObRecoverTableInitiator::check_specified_database_remap_table_target_(const share::ObImportTableArg &import_table_arg,
    const share::ObImportRemapArg &import_remap_arg, const uint64_t target_tenant_id)
{
  int ret = OB_SUCCESS;
  const share::ObImportDatabaseArray &db_array = import_table_arg.get_import_database_array();

  // check specified remap target database
  ARRAY_FOREACH(db_array.get_items(), i) {
    const share::ObImportDatabaseItem &db_item = db_array.get_items().at(i);
    share::ObImportDatabaseItem remap_db_item;
    bool is_no_remap_database = false;
    if (OB_FAIL(import_remap_arg.get_remap_database(db_item, remap_db_item))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // no remap, set target database name the same as source.
        remap_db_item = db_item;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get remap database", K(ret), K(import_remap_arg), K(db_item));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      const ObString &target_database = remap_db_item.name_;
      bool is_exist = false;
      if (OB_FAIL(ObImportTableUtil::check_database_schema_exist(*schema_service_,
                                                                 target_tenant_id,
                                                                 target_database,
                                                                 is_exist))) {
        LOG_WARN("failed to check target database schema exist", K(ret));
      } else if (!is_exist) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_INFO("target database not exist", K(ret), K(target_database));
        // "database not exist '%.*s'"
        LOG_USER_ERROR(OB_ERR_BAD_DATABASE, target_database.length(), target_database.ptr());
      }
    }
  }

  return ret;
}

int ObRecoverTableInitiator::check_specified_table_remap_table_target_(const share::ObImportTableArg &import_table_arg,
    const share::ObImportRemapArg &import_remap_arg, const uint64_t target_tenant_id)
{
  int ret = OB_SUCCESS;
  const share::ObImportTableArray &table_array = import_table_arg.get_import_table_array();

  // check target database & target table
  ARRAY_FOREACH(table_array.get_items(), i) {
    const share::ObImportTableItem &table_item = table_array.get_items().at(i);
    share::ObImportTableItem remap_table_item;
    if (OB_FAIL(import_remap_arg.get_remap_table(table_item, remap_table_item))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // no remap, set target name the same as source.
        remap_table_item = table_item;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get remap table", K(ret), K(import_remap_arg), K(table_item));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      const ObString &target_database = remap_table_item.database_name_;
      const ObString &target_table = remap_table_item.table_name_;
      int tmp_ret = OB_SUCCESS;
      bool is_exist = false;
      if (OB_FAIL(ObImportTableUtil::check_database_schema_exist(*schema_service_,
                                                                 target_tenant_id,
                                                                 target_database,
                                                                 is_exist))) {
        LOG_WARN("failed to check target database schema exist", K(ret));
      } else if (!is_exist) {
        ret = OB_ERR_BAD_DATABASE;;
        LOG_INFO("target database not exist", K(ret), K(target_database));
        // "database not exist '%.*s'"
        LOG_USER_ERROR(OB_ERR_BAD_DATABASE, target_database.length(), target_database.ptr());
      } else if (OB_FAIL(ObImportTableUtil::check_table_schema_exist(*schema_service_,
                                                                     target_tenant_id,
                                                                     target_database,
                                                                     target_table,
                                                                     is_exist))) {
        LOG_WARN("failed to check target table schema exist", K(ret), K(target_database), K(target_table));
      } else if (is_exist) {
        ret = OB_ERR_TABLE_EXIST;
        LOG_INFO("target table exist", K(ret), K(target_database), K(target_table));
        int tmp_ret = OB_SUCCESS;
        common::ObFixedLengthString<DEFAULT_BUF_LENGTH> target_table_name;
        if (OB_TMP_FAIL(databuff_printf(target_table_name.ptr(), target_table_name.capacity(), "%.*s.%.*s",
          target_database.length(), target_database.ptr(), target_table.length(), target_table.ptr()))) {
          LOG_WARN("failed to databuff printf", K(ret));
        } else {
          // "Table '%.*s' already exists"
          LOG_USER_ERROR(OB_ERR_TABLE_EXIST, static_cast<int>(target_table_name.size()), target_table_name.ptr());
        }
      }
    }
  }

  return ret;
}

int ObRecoverTableInitiator::check_remap_table_target_(const share::ObImportTableArg &import_table_arg,
    const share::ObImportRemapArg &import_remap_arg, const uint64_t target_tenant_id)
{
  int ret = OB_SUCCESS;
  const share::ObRemapDatabaseArray &remap_db_array = import_remap_arg.get_remap_database_array();
  const share::ObRemapTableArray &remap_table_array = import_remap_arg.get_remap_table_array();

  // check all remap target database
  ARRAY_FOREACH(remap_db_array.get_remap_items(), i) {
    const share::ObRemapDatabaseItem &remap_db_item = remap_db_array.get_remap_items().at(i);
    const ObString &target_database = remap_db_item.target_.name_;
    bool is_exist = false;
    if (OB_FAIL(ObImportTableUtil::check_database_schema_exist(*schema_service_,
                                                               target_tenant_id,
                                                               target_database,
                                                               is_exist))) {
      LOG_WARN("failed to check target database schema exist", K(ret));
    } else if (!is_exist) {
      ret = OB_ERR_BAD_DATABASE;;
      LOG_INFO("target database not exist", K(ret), K(target_database));
      // "database not exist '%.*s'"
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, target_database.length(), target_database.ptr());
    }
  }

  // check all remap target table
  ARRAY_FOREACH(remap_table_array.get_remap_items(), i) {
    const share::ObRemapTableItem &remap_table_item = remap_table_array.get_remap_items().at(i);
    const ObString &target_database = remap_table_item.target_.database_name_;
    const ObString &target_table = remap_table_item.target_.database_name_;
    bool is_exist = false;
    if (OB_FAIL(ObImportTableUtil::check_database_schema_exist(*schema_service_,
                                                               target_tenant_id,
                                                               target_database,
                                                               is_exist))) {
      LOG_WARN("failed to check target database schema exist", K(ret));
    } else if (!is_exist) {
      ret = OB_ERR_BAD_DATABASE;;
      LOG_INFO("target database not exist", K(ret), K(target_database));
      // "Unknown database '%.*s'"
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, target_database.length(), target_database.ptr());
    } else if (OB_FAIL(ObImportTableUtil::check_table_schema_exist(*schema_service_,
                                                                    target_tenant_id,
                                                                    target_database,
                                                                    target_table,
                                                                    is_exist))) {
      LOG_WARN("failed to check target table schema exist", K(ret), K(target_database), K(target_table));
    } else if (is_exist) {
      ret = OB_ERR_TABLE_EXIST;
      LOG_INFO("target table exist", K(ret), K(target_database), K(target_table));
      int tmp_ret = OB_SUCCESS;
      common::ObFixedLengthString<DEFAULT_BUF_LENGTH> target_table_name;
      if (OB_TMP_FAIL(databuff_printf(target_table_name.ptr(), target_table_name.capacity(), "%.*s.%.*s",
        target_database.length(), target_database.ptr(), target_table.length(), target_table.ptr()))) {
        LOG_WARN("failed to databuff printf", K(ret));
      } else {
        // "Table '%.*s' already exists"
        LOG_USER_ERROR(OB_ERR_TABLE_EXIST, static_cast<int>(target_table_name.size()), target_table_name.ptr());
      }
    }
  }

  return ret;
}

int ObRecoverTableInitiator::check_remap_tablegroup_target_(const share::ObRemapTablegroupArray &remap_tablegroup_array, uint64_t target_tenant_id)
{
  int ret = OB_SUCCESS;

  ARRAY_FOREACH(remap_tablegroup_array.get_remap_items(), i) {
    const share::ObRemapTablegroupItem &remap_tablegroup_item = remap_tablegroup_array.get_remap_items().at(i);
    const ObString &target_tablegroup = remap_tablegroup_item.target_.name_;
    int tmp_ret = OB_SUCCESS;
    bool is_exist = false;
    if (OB_FAIL(ObImportTableUtil::check_tablegroup_exist(*schema_service_,
                                                          target_tenant_id,
                                                          target_tablegroup,
                                                          is_exist))) {
      LOG_WARN("failed to check tablegroup exist", K(ret));
    } else if (!is_exist) {
      ret = OB_TABLEGROUP_NOT_EXIST;
      LOG_INFO("remap target tablegroup not exist", K(ret), K(target_tablegroup));
      // "Tablegroup does not exist"
      LOG_USER_ERROR(OB_TABLEGROUP_NOT_EXIST);
    }
  }

  return ret;
}

int ObRecoverTableInitiator::check_remap_tablespace_target_(const share::ObRemapTablespaceArray &remap_tablespace_array,
    uint64_t target_tenant_id)
{
  int ret = OB_SUCCESS;

  // check remap target tablespace
  ARRAY_FOREACH(remap_tablespace_array.get_remap_items(), i) {
    const share::ObRemapTablespaceItem &remap_tablespace_item = remap_tablespace_array.get_remap_items().at(i);
    const ObString &target_tablespace = remap_tablespace_item.target_.name_;
    bool is_exist = false;
    int tmp_ret = OB_SUCCESS;
    if (FAILEDx(ObImportTableUtil::check_tablespace_exist(*schema_service_,
                                                          target_tenant_id,
                                                          target_tablespace,
                                                          is_exist))) {
      LOG_WARN("failed to check tablespace exist", K(ret));
    } else if (!is_exist) {
      ret = OB_TABLESPACE_NOT_EXIST;
      LOG_INFO("remap target tablespace not exist", K(ret), K(target_tablespace));
      // "Tablespace '%.*s' does not exist"
      LOG_USER_ERROR(OB_TABLESPACE_NOT_EXIST, target_tablespace.length(), target_tablespace.ptr());
    }
  }

  return ret;
}

int ObRecoverTableInitiator::check_recover_table_target_schema_(const share::ObRecoverTableJob &job)
{
  int ret = OB_SUCCESS;
  const share::ObImportTableArg &import_table_arg = job.get_import_arg().get_import_table_arg();
  const share::ObImportRemapArg &import_remap_arg = job.get_import_arg().get_remap_table_arg();
  uint64_t target_tenant_id = job.get_target_tenant_id();

  if (OB_FAIL(check_specified_database_remap_table_target_(import_table_arg, import_remap_arg, target_tenant_id))) {
    LOG_WARN("check specified dababase remap table target failed", K(ret), K(job));
  } else if (OB_FAIL(check_specified_table_remap_table_target_(import_table_arg, import_remap_arg, target_tenant_id))) {
    LOG_WARN("check specified table remap table target failed", K(ret), K(job));
  } else if (OB_FAIL(check_remap_table_target_(import_table_arg, import_remap_arg, target_tenant_id))) {
    LOG_WARN("check specified table remap table target failed", K(ret), K(job));
  } else if (OB_FAIL(check_remap_tablegroup_target_(import_remap_arg.get_remap_tablegroup_array(), target_tenant_id))) {
    LOG_WARN("check specified table remap table target failed", K(ret), K(job));
  } else if (OB_FAIL(check_remap_tablespace_target_(import_remap_arg.get_remap_tablespace_array(), target_tenant_id))) {
    LOG_WARN("check specified table remap table target failed", K(ret), K(job));
  }

  return ret;
}