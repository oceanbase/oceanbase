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

#define USING_LOG_PREFIX COMMON

#include "ob_physical_restore_table_operator.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_kv_parser.h"
#include "share/ob_cluster_version.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "share/backup/ob_backup_path.h"
#include "share/ls/ob_ls_info.h"
#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::share;

static const char* physical_restore_mod_str_array[PHYSICAL_RESTORE_MOD_MAX_NUM] = {
  "ROOTSERVICE",
  "CLOG",
  "STORAGE"
};

const char* ObPhysicalRestoreTableOperator::get_physical_restore_mod_str(
    PhysicalRestoreMod mod)
{
  const char* str = NULL;
  if (mod >= PHYSICAL_RESTORE_MOD_RS && mod < PHYSICAL_RESTORE_MOD_MAX_NUM) {
    str = physical_restore_mod_str_array[mod];
  }
  return str;
}

static const char* phy_restore_status_str_array[PHYSICAL_RESTORE_MAX_STATUS] = {
  "CREATE_TENANT",
  "RESTORE_PRE",
  "RESTORE_CREATE_INIT_LS",
  "PHYSICAL_RESTORE_WAIT_RESTORE_TO_CONSISTENT_SCN",
  "RESTORE_WAIT_LS",
  "POST_CHECK",
  "UPGRADE",
  "RESTORE_SUCCESS",
  "RESTORE_FAIL",
  "WAIT_TENANT_RESTORE_FINISH"
};

const char* ObPhysicalRestoreTableOperator::get_restore_status_str(
    PhysicalRestoreStatus status)
{
  const char* str = NULL;
  if (status >= PHYSICAL_RESTORE_CREATE_TENANT && status < PHYSICAL_RESTORE_MAX_STATUS) {
    str = phy_restore_status_str_array[status];
  }
  return str;
}

PhysicalRestoreStatus ObPhysicalRestoreTableOperator::get_restore_status(
    const common::ObString &status_str)
{
  PhysicalRestoreStatus status = PHYSICAL_RESTORE_MAX_STATUS;
  for(int i = 0; i < static_cast<int>(PHYSICAL_RESTORE_MAX_STATUS); i++) {
    if (OB_NOT_NULL(phy_restore_status_str_array[i])
        && 0 == status_str.case_compare(phy_restore_status_str_array[i])) {
      status = static_cast<PhysicalRestoreStatus>(i);
      break;
    }
  }
  return status;
}

ObPhysicalRestoreTableOperator::ObPhysicalRestoreTableOperator()
  : inited_(false),
    sql_client_(NULL), tenant_id_(OB_INVALID_TENANT_ID)
{
}

int ObPhysicalRestoreTableOperator::init(common::ObISQLClient *sql_client,
                                         const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("physical restore table operator init twice", K(ret));
  } else if (OB_ISNULL(sql_client) || is_meta_tenant(tenant_id))  {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql client is null or tenant id is invalid", KR(ret), KP(sql_client), K(tenant_id));
  } else {
    sql_client_ = sql_client;
    tenant_id_ = tenant_id;
    inited_ = true;
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::insert_job(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(exec_tenant_id));
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    // insert __all_restore_job
    if (OB_FAIL(fill_dml_splicer(dml, job_info))) {
      LOG_WARN("fail to fill dml splicer", KR(ret), K(tenant_id_), K(job_info));
    } else if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_RESTORE_JOB_TNAME, sql))) {
      LOG_WARN("splice_insert_sql failed", KR(ret));
    } else if (OB_FAIL(sql_client_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", KR(ret), K(exec_tenant_id), K(sql));
    } else if (affected_rows <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid affected rows", KR(ret), K(affected_rows), K(sql));
    }
  }
  LOG_INFO("[RESTORE] insert job", KR(ret), K(tenant_id_), K(job_info));
  return ret;
}

int ObPhysicalRestoreTableOperator::fill_dml_splicer(
    share::ObDMLSqlSplicer &dml,
    const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", K(ret));
  } else {
#define ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(JOB_INFO, COLUMN_NAME)              \
  if (OB_SUCC(ret)) {                                                          \
    if (OB_FAIL(dml.add_pk_column("job_id",                                    \
                                  JOB_INFO.get_restore_key().job_id_))) {      \
      LOG_WARN("fail to add pk column", K(ret), "job_key",                     \
               JOB_INFO.get_restore_key());                                    \
    } else if (OB_FAIL(dml.add_pk_column(                                      \
                   "tenant_id", JOB_INFO.get_restore_key().tenant_id_))) {     \
      LOG_WARN("failed to add pk column", KR(ret), "job_key",                  \
               JOB_INFO.get_restore_key());                                    \
    } else if (OB_FAIL(dml.add_pk_column("name", #COLUMN_NAME))) {             \
      LOG_WARN("fail to add pk column", K(ret), "name", #COLUMN_NAME);         \
    } else if (OB_FAIL(                                                        \
                   dml.add_column("value", (JOB_INFO).get_##COLUMN_NAME()))) { \
      LOG_WARN("fail to add column", K(ret), "value",                          \
               (JOB_INFO).get_##COLUMN_NAME());                                \
    } else if (OB_FAIL(dml.finish_row())) {                                    \
      LOG_WARN("fail to finish row", K(ret));                                  \
    }                                                                          \
  }

#define ADD_COLUMN_WITH_VALUE(JOB_INFO, COLUMN_NAME, COLUMN_VALUE)         \
  if (OB_SUCC(ret)) {                                                      \
    if (OB_FAIL(dml.add_pk_column("job_id",                                \
                                  JOB_INFO.get_restore_key().job_id_))) {  \
      LOG_WARN("fail to add pk column", K(ret), "job_key",                 \
               JOB_INFO.get_restore_key());                                \
    } else if (OB_FAIL(dml.add_pk_column(                                  \
                   "tenant_id", JOB_INFO.get_restore_key().tenant_id_))) { \
      LOG_WARN("failed to add pk column", KR(ret), "job_key",              \
               JOB_INFO.get_restore_key());                                \
    } else if (OB_FAIL(dml.add_pk_column("name", #COLUMN_NAME))) {         \
      LOG_WARN("fail to add pk column", K(ret), "name", #COLUMN_NAME);     \
    } else if (OB_FAIL(dml.add_column("value", COLUMN_VALUE))) {           \
      LOG_WARN("fail to add column", K(ret), "value", COLUMN_VALUE);       \
    } else if (OB_FAIL(dml.finish_row())) {                                \
      LOG_WARN("fail to finish row", K(ret));                              \
    }                                                                      \
  }

#define ADD_COLUMN_WITH_UINT_VALUE(JOB_INFO, COLUMN_NAME, COLUMN_VALUE)         \
  if (OB_SUCC(ret)) {                                                      \
    if (OB_FAIL(dml.add_pk_column("job_id",                                \
                                  JOB_INFO.get_restore_key().job_id_))) {  \
      LOG_WARN("fail to add pk column", K(ret), "job_key",                 \
               JOB_INFO.get_restore_key());                                \
    } else if (OB_FAIL(dml.add_pk_column(                                  \
                   "tenant_id", JOB_INFO.get_restore_key().tenant_id_))) { \
      LOG_WARN("failed to add pk column", KR(ret), "job_key",              \
               JOB_INFO.get_restore_key());                                \
    } else if (OB_FAIL(dml.add_pk_column("name", #COLUMN_NAME))) {         \
      LOG_WARN("fail to add pk column", K(ret), "name", #COLUMN_NAME);     \
    } else if (OB_FAIL(dml.add_uint64_column("value", COLUMN_VALUE))) {           \
      LOG_WARN("fail to add column", K(ret), "value", COLUMN_VALUE);       \
    } else if (OB_FAIL(dml.finish_row())) {                                \
      LOG_WARN("fail to finish row", K(ret));                              \
    }                                                                      \
  }

    char version[common::OB_CLUSTER_VERSION_LENGTH] = {0};

    ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, initiator_job_id);
    ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, initiator_tenant_id);
    ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, tenant_id);
    ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, backup_tenant_id);
    ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, restore_start_ts);
    ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, comment);

    // restore_scn
    ADD_COLUMN_WITH_UINT_VALUE(job_info, restore_scn, (job_info.get_restore_scn().get_val_for_inner_table_field()));
    // consistent_scn
    ADD_COLUMN_WITH_UINT_VALUE(job_info, consistent_scn, (job_info.get_consistent_scn().get_val_for_inner_table_field()));
    //restore_type
    ADD_COLUMN_WITH_VALUE(job_info, restore_type, (int64_t)(job_info.get_restore_type()));
    if (OB_SUCC(ret)) {
      uint64_t post_data_version = job_info.get_post_data_version();
      int64_t len = ObClusterVersion::print_version_str(
          version, common::OB_CLUSTER_VERSION_LENGTH, post_data_version);
      if (len < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid post_data_version", K(ret),
                 K(post_data_version));
      } else {
        ADD_COLUMN_WITH_VALUE(job_info, post_data_version, ObString(len, version));
      }
    }
    // status
    const char *status_str =
        ObPhysicalRestoreTableOperator::get_restore_status_str(
            job_info.get_status());
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(status_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid status", K(ret), "status", job_info.get_status());
    } else {
      ADD_COLUMN_WITH_VALUE(job_info, status, status_str);
    }

     /* uri */
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, restore_option);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, backup_dest);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, description);

     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, tenant_name);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, backup_tenant_name);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, backup_cluster_name);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, pool_list);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, primary_zone);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, locality);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, kms_encrypt);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, kms_info);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, encrypt_key);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, kms_dest);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, kms_encrypt_key);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, compat_mode);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, compatible);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, passwd_array);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, concurrency);
     ADD_COLUMN_MACRO_IN_TABLE_OPERATOR(job_info, recover_table);

     // source_cluster_version
     if (OB_SUCC(ret)) {
       uint64_t source_cluster_version = job_info.get_source_cluster_version();
       int64_t len = ObClusterVersion::print_version_str(
                     version, common::OB_CLUSTER_VERSION_LENGTH, source_cluster_version);
       if (len < 0) {
         ret = OB_INVALID_ARGUMENT;
         LOG_WARN("invalid source_cluster_version", K(ret), K(source_cluster_version));
       } else {
         ADD_COLUMN_WITH_VALUE(job_info, source_cluster_version, ObString(len, version));
       }
     }
     // source_data_version
     if (OB_SUCC(ret)) {
       uint64_t source_data_version = job_info.get_source_data_version();
       int64_t len = ObClusterVersion::print_version_str(
                     version, common::OB_CLUSTER_VERSION_LENGTH, source_data_version);
       if (len < 0) {
         ret = OB_INVALID_ARGUMENT;
         LOG_WARN("invalid source_data_version", K(ret), K(source_data_version));
       } else {
         ADD_COLUMN_WITH_VALUE(job_info, source_data_version, ObString(len, version));
       }
     }
     // while_list/b_while_list
     if (OB_SUCC(ret)) {
       ObArenaAllocator allocator("PhyWhiteList");
       const ObPhysicalRestoreWhiteList &white_list = job_info.get_white_list();
       ObString white_list_str;
       ObString b_white_list_str;
       if (OB_FAIL(white_list.get_format_str(allocator, white_list_str))) {
         LOG_WARN("fail to get format str", KR(ret), K(white_list));
       } else {
         ADD_COLUMN_WITH_VALUE(job_info, white_list, ObHexEscapeSqlStr(white_list_str));
       }

       if (OB_FAIL(ret)) {
       } else if (OB_FAIL(white_list.get_hex_str(allocator, b_white_list_str))) {
         LOG_WARN("fail to get format str", KR(ret), K(white_list));
       } else {
         ADD_COLUMN_WITH_VALUE(job_info, b_white_list, b_white_list_str);
       }
     }


    if (OB_SUCC(ret)) {
      ObArenaAllocator allocator;
      ObString backup_set_list;
      ObString backup_set_desc_list;
      ObString backup_piece_list;
      ObString log_path_list;
      const ObPhysicalRestoreBackupDestList &dest_list = job_info.get_multi_restore_path_list();
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(dest_list.get_backup_set_list_format_str(allocator, backup_set_list))) {
        LOG_WARN("fail to get format str", KR(ret), K(dest_list));
      } else if (OB_FAIL(dest_list.get_backup_set_desc_list_format_str(allocator, backup_set_desc_list))) {
        LOG_WARN("fail to get format str", KR(ret), K(dest_list));
      } else if (OB_FAIL(dest_list.get_backup_piece_list_format_str(allocator, backup_piece_list))) {
        LOG_WARN("fail to get format str", KR(ret), K(dest_list));
      } else if (OB_FAIL(dest_list.get_log_path_list_format_str(allocator, log_path_list))) {
        LOG_WARN("fail to get format str", KR(ret), K(dest_list));
      } else {
        ADD_COLUMN_WITH_VALUE(job_info, backup_set_list, backup_set_list); 
        ADD_COLUMN_WITH_VALUE(job_info, backup_set_desc_list, backup_set_desc_list); 
        ADD_COLUMN_WITH_VALUE(job_info, backup_piece_list, backup_piece_list); 
        ADD_COLUMN_WITH_VALUE(job_info, log_path_list, log_path_list);
      }
    }

#undef ADD_COLUMN_MACRO_IN_TABLE_OPERATOR
#undef ADD_COLUMN_WITH_VALUE
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::get_jobs(
    common::ObIArray<ObPhysicalRestoreJob> &jobs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    jobs.reset();
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("physical restore table operator not init", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant id is invalid", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s ORDER BY job_id, name",
                                      OB_ALL_RESTORE_JOB_TNAME))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(exec_tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      HEAP_VAR(ObPhysicalRestoreJob, current) {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          //in tenant, job_id is primary key, only check primary key
          int64_t job_id = OB_INVALID_ID;
          uint64_t tenant_id = OB_INVALID_TENANT_ID;
          const int64_t current_job_id = current.get_restore_key().job_id_;
          EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, int64_t);
          if (OB_FAIL(ret)) {
          } else if (OB_INVALID_ID != current_job_id && job_id != current_job_id) {
            if (!current.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid job", K(ret), K(current));
            } else if (OB_FAIL(jobs.push_back(current))) {
              LOG_WARN("push back job info failed", K(ret), K(current));
            } else {
              LOG_DEBUG("retrieve restore job", K(ret), "job", current);
              current.reset();
            }
          } else {
            // first or in current job_info
          }

          if (FAILEDx(current.init_restore_key(tenant_id, job_id))) {
            LOG_WARN("failed to init restore key", KR(ret), K(tenant_id), K(job_id));
          } else if (OB_FAIL(retrieve_restore_option(*result, current))) {
            LOG_WARN("fail to retrieve restore option", K(ret), K(current));
          } else {
            LOG_DEBUG("current job", K(ret), K(current));
          }
        } // end for

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          if (current.is_valid()) {
            if (OB_FAIL(jobs.push_back(current))) {
              LOG_WARN("push back job info failed", K(ret), K(current));
            } else {
              LOG_DEBUG("retrieve restore job", K(ret), "job", current);
            }
          } else {
            LOG_DEBUG("restore job is invalid", K(ret), "job", current);
          }
        } else {
          LOG_WARN("get jobs fail", K(ret));
        }
      }
    }
    LOG_INFO("[RESTORE] get restore jobs", K(ret), "job_cnt", jobs.count());
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::retrieve_restore_option(
    common::sqlclient::ObMySQLResult &result,
    ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!job.get_restore_key().is_pkey_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job", K(ret), K(job));
  } else {
    ObString name;
    ObString debug_value;
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "name", name);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", debug_value);
    LOG_DEBUG("retrieve restore option", K(ret), "name", name, "value", debug_value);

#define RETRIEVE_UINT_VALUE(COLUMN_NAME, OBJ)                        \
  if (OB_SUCC(ret)) {                                                \
    if (name == #COLUMN_NAME) {                                      \
      uint64_t current_value = 0;                                    \
      if (OB_FAIL(retrieve_uint_value(result, current_value))) {     \
        LOG_WARN("fail to retrive int value", K(ret), "column_name", \
                 #COLUMN_NAME);                                      \
      } else {                                                       \
        (OBJ).set_##COLUMN_NAME(current_value);                      \
      }                                                              \
    }                                                                \
  }

#define RETRIEVE_INT_VALUE(COLUMN_NAME, OBJ)                         \
  if (OB_SUCC(ret)) {                                                \
    if (name == #COLUMN_NAME) {                                      \
      int64_t current_value = 0;                                     \
      if (OB_FAIL(retrieve_int_value(result, current_value))) {      \
        LOG_WARN("fail to retrive int value", K(ret), "column_name", \
                 #COLUMN_NAME);                                      \
      } else {                                                       \
        (OBJ).set_##COLUMN_NAME(current_value);                      \
      }                                                              \
    }                                                                \
  }

#define RETRIEVE_STR_VALUE(COLUMN_NAME, OBJ)                       \
  if (OB_SUCC(ret)) {                                              \
    if (name == #COLUMN_NAME) {                                      \
      ObString value;                                              \
      EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "value", value, true, false, value);\
      if (FAILEDx((OBJ).set_##COLUMN_NAME(value))) {               \
        LOG_WARN("failed to set column value", KR(ret), K(value)); \
      }                                                            \
    }                                                              \
  }

    RETRIEVE_UINT_VALUE(tenant_id, job);
    RETRIEVE_UINT_VALUE(initiator_job_id, job);
    RETRIEVE_UINT_VALUE(initiator_tenant_id, job);
    RETRIEVE_UINT_VALUE(backup_tenant_id, job);
    RETRIEVE_INT_VALUE(restore_start_ts, job);
    if (OB_SUCC(ret)) {
      if (name == "restore_scn") {
        uint64_t current_value = share::OB_INVALID_SCN_VAL;
        SCN restore_scn;
        if (OB_FAIL(retrieve_uint_value(result, current_value))) {
          LOG_WARN("fail to retrive int value", K(ret), "column_name", "restore_scn");
        } else if (OB_FAIL(restore_scn.convert_for_inner_table_field(current_value))) {
          LOG_WARN("fail to set restore scn", K(ret));
        } else {
          (job).set_restore_scn(restore_scn);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == "consistent_scn") {
        uint64_t current_value = share::OB_INVALID_SCN_VAL;
        SCN consistent_scn;
        if (OB_FAIL(retrieve_uint_value(result, current_value))) {
          LOG_WARN("fail to retrive int value", K(ret), "column_name", "consistent_scn");
        } else if (OB_FAIL(consistent_scn.convert_for_inner_table_field(current_value))) {
          LOG_WARN("fail to set restore scn", K(ret));
        } else {
          (job).set_consistent_scn(consistent_scn);
        }
      }
    }

    RETRIEVE_STR_VALUE(restore_option, job);
    RETRIEVE_STR_VALUE(backup_dest, job);
    RETRIEVE_STR_VALUE(description, job);
    RETRIEVE_STR_VALUE(tenant_name, job);
    RETRIEVE_STR_VALUE(backup_tenant_name, job);
    RETRIEVE_STR_VALUE(backup_cluster_name, job);
    RETRIEVE_STR_VALUE(pool_list, job);
    RETRIEVE_STR_VALUE(primary_zone, job);
    RETRIEVE_STR_VALUE(locality, job);
    RETRIEVE_STR_VALUE(comment, job);
    RETRIEVE_STR_VALUE(passwd_array, job);
    RETRIEVE_INT_VALUE(compatible, job);
    RETRIEVE_STR_VALUE(kms_info, job);
    RETRIEVE_STR_VALUE(encrypt_key, job);
    RETRIEVE_STR_VALUE(kms_dest, job);
    RETRIEVE_STR_VALUE(kms_encrypt_key, job);
    RETRIEVE_INT_VALUE(concurrency, job);

    if (OB_SUCC(ret)) {
      if (name == "backup_dest") {
        ObString value;
        EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "value", value, true, false, value);
        if (FAILEDx((job).set_backup_dest(value))) {
          LOG_WARN("failed to set column value", KR(ret), K(value));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == "kms_encrypt") {
        int64_t kms_encrypt = 0;
        if (OB_FAIL(retrieve_int_value(result, kms_encrypt))) {
          LOG_WARN("fail to retrive int value", K(ret), "column_name", "kms_info");
        } else {
          job.set_kms_encrypt(kms_encrypt != 0);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == "recover_table") {
        int64_t recover_table = 0;
        if (OB_FAIL(retrieve_int_value(result, recover_table))) {
          LOG_WARN("fail to retrive int value", K(ret), "column_name", "recover_table");
        } else {
          job.set_recover_table(recover_table != 0);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == "restore_type") {
        uint64_t restore_type = 0;
        if (OB_FAIL(retrieve_uint_value(result, restore_type))) {
          LOG_WARN("fail to retrive int value", K(ret));
        } else {
          job.set_restore_type(share::ObRestoreType(static_cast<share::ObRestoreType::Type>(restore_type)));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == "status") {
        ObString status_str;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", status_str);
        if (OB_SUCC(ret)) {
          job.set_status(ObPhysicalRestoreTableOperator::get_restore_status(status_str));
          if (PHYSICAL_RESTORE_MAX_STATUS == job.get_status()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid status", K(ret), K(status_str));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (name == "compat_mode") {
        int64_t compat_mode = 0;
        if (OB_FAIL(retrieve_int_value(result, compat_mode))) {
          LOG_WARN("fail to retrive int value", K(ret));
        } else {
          job.set_compat_mode(static_cast<lib::Worker::CompatMode>(compat_mode));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == "post_data_version") {
        ObString version_str;
        uint64_t version = 0;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", version_str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObClusterVersion::get_version(version_str, version))) {
          LOG_WARN("fail to parser version", K(ret), K(version_str));
        } else {
          job.set_post_data_version(version);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (name == "source_cluster_version") {
        ObString version_str;
        uint64_t version = 0;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", version_str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObClusterVersion::get_version(version_str, version))) {
          LOG_WARN("fail to parser version", K(ret), K(version_str));
        } else {
          job.set_source_cluster_version(version);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == "source_data_version") {
        ObString version_str;
        uint64_t version = 0;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", version_str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObClusterVersion::get_version(version_str, version))) {
          LOG_WARN("fail to parser version", K(ret), K(version_str));
        } else {
          job.set_source_data_version(version);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == OB_STR_BACKUP_SET_LIST) {
        ObString str;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(job.get_multi_restore_path_list().backup_set_list_assign_with_format_str(str))) {
          LOG_WARN("fail to assign backup set list", KR(ret), K(str));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == "backup_set_desc_list") {
        ObString str;
        EXTRACT_VARCHAR_FIELD_MYSQL(result, "value", str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(job.get_multi_restore_path_list().backup_set_desc_list_assign_with_format_str(str))) {
          LOG_WARN("fail to assign backup set list", KR(ret), K(str));
        } 
      }
    }

    if (OB_SUCC(ret)) {
      if (name == OB_STR_BACKUP_PIECE_LIST) {
        ObString str;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(job.get_multi_restore_path_list().backup_piece_list_assign_with_format_str(str))) {
          LOG_WARN("fail to assign backup piece list", KR(ret), K(str));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == OB_STR_LOG_PATH_LIST) {
        ObString str;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(job.get_multi_restore_path_list().log_path_list_assign_with_format_str(str))) {
          LOG_WARN("fail to assign log path list", KR(ret), K(str));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (name == "b_white_list") {
        ObString str;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(job.get_white_list().assign_with_hex_str(str))) {
          LOG_WARN("fail to assign white_list", KR(ret), K(str));
        }
      }
    }

#undef RETRIEVE_UINT_VALUE
#undef RETRIEVE_INT_VALUE
#undef RETRIEVE_STR_VALUE

  }

  return ret;
}

int ObPhysicalRestoreTableOperator::retrieve_int_value(
    common::sqlclient::ObMySQLResult &result,
    int64_t &value)
{
  int ret = OB_SUCCESS;
  char value_buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = { 0 };
  int64_t real_len = 0; // not used
  EXTRACT_STRBUF_FIELD_MYSQL(result, "value", value_buf, OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_len);
  if (OB_SUCC(ret)) {
    char *endptr = NULL;
    value = strtoll(value_buf, &endptr, 0);
    if (0 == strlen(value_buf) || '\0' != *endptr) {
      ret = OB_INVALID_DATA;
      LOG_WARN("not int value", K(ret), K(value_buf));
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::retrieve_uint_value(
    common::sqlclient::ObMySQLResult &result,
    uint64_t &value)
{
  int ret = OB_SUCCESS;
  char value_buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = { 0 };
  int64_t real_len = 0; // not used
  EXTRACT_STRBUF_FIELD_MYSQL(result, "value", value_buf, OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_len);
  if (OB_SUCC(ret)) {
    char *endptr = NULL;
    value = strtoull(value_buf, &endptr, 0);
    if (0 == strlen(value_buf) || '\0' != *endptr) {
      ret = OB_INVALID_DATA;
      LOG_WARN("not uint value", K(ret), K(value_buf), K(endptr));
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::check_job_exist(
    const int64_t job_id,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("physical restore table operator not init", K(ret));
    } else if (job_id < 0 || is_user_tenant(exec_tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job_id", K(ret), K(exec_tenant_id), K(job_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT count(*) as count FROM %s WHERE job_id = %ld",
                                      OB_ALL_RESTORE_JOB_TNAME, job_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(exec_tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get row", K(ret), K(job_id), K(sql));
    } else {
      int64_t count = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int64_t);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to get cell", KR(ret), K(exec_tenant_id), K(sql));
      } else {
        exist = (count > 0);
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::get_job(
    const int64_t job_id, ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("physical restore table operator not init", K(ret));
    } else if (job_id < 0 || is_user_tenant(exec_tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job_id", K(ret), K(job_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld",
                                      OB_ALL_RESTORE_JOB_TNAME, job_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(exec_tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      int64_t idx = 0;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        int64_t tmp_job_id = OB_INVALID_ID;
        uint64_t tenant_id = OB_INVALID_TENANT_ID;
        EXTRACT_INT_FIELD_MYSQL(*result, "job_id", tmp_job_id, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get cell", KR(ret), K(exec_tenant_id), K(sql));
        } else if (job_id != tmp_job_id || tenant_id != tenant_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("job_info job is invalid", K(ret), K(tmp_job_id), K(job_id),
          K(tenant_id), K(tenant_id_));
        } else if (OB_FAIL(job_info.init_restore_key(tenant_id, job_id))) {
          LOG_WARN("failed to init restore key", KR(ret), K(tenant_id), K(job_id));
        } else if (OB_FAIL(retrieve_restore_option(*result, job_info))) {
          LOG_WARN("fail to retrieve restore option", K(ret), K(job_info));
        }
      }
      if (OB_ITER_END == ret) {
        if (!job_info.is_valid()) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("job is invalid", K(ret), K(job_id), K(job_info));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("get jobs fail", K(ret));
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::update_job_error_info(
    int64_t job_id,
    int return_ret,
    PhysicalRestoreMod mod,
    const common::ObCurTraceId::TraceId &trace_id,
    const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  const char *mod_str = get_physical_restore_mod_str(mod);
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", KR(ret));
  } else if (job_id < 0
             || OB_SUCCESS == return_ret
             || OB_ISNULL(mod_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(job_id), K(return_ret), K(mod));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    // update __all_restore_info
    if (OB_FAIL(sql.assign_fmt("UPDATE %s SET value = '%s : %s(%d) on %s with traceid %s' "
                               "WHERE job_id = %ld AND name = 'comment'",
                               OB_ALL_RESTORE_JOB_TNAME, mod_str,
                               ob_error_name(return_ret), return_ret,
                               to_cstring(addr), to_cstring(trace_id),
                               job_id))) {
      LOG_WARN("failed to set sql", K(ret), K(mod_str), K(return_ret), K(trace_id), K(addr));
    } else if (OB_FAIL(sql_client_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), KR(ret), K(exec_tenant_id));
    } else if (!is_single_row(affected_rows)
               && !is_zero_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update succeeded but affected_rows more than one", KR(ret), K(affected_rows));
    }
  }
  LOG_INFO("[RESTORE] update job error info", KR(ret), K(job_id), K(return_ret), K(mod));
  return ret;
}

int ObPhysicalRestoreTableOperator::update_job_status(
    int64_t job_id, int64_t status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);
  const char *status_str = ObPhysicalRestoreTableOperator::get_restore_status_str(
                             static_cast<PhysicalRestoreStatus>(status));
  // update __all_restore_job
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", K(ret));
  } else if (job_id < 0 || is_user_tenant(exec_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_id", K(ret), K(job_id), K(exec_tenant_id));
  } else if (OB_ISNULL(status_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(ret), K(status));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET value = '%s' WHERE job_id = %ld "
                                    "AND name = 'status' AND value != 'RESTORE_FAIL'",
                                    OB_ALL_RESTORE_JOB_TNAME, status_str, job_id))) {
    LOG_WARN("fail to assign fmt", K(ret), K(sql));
  } else if (OB_FAIL(sql_client_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret), K(exec_tenant_id));
  } else if (!is_single_row(affected_rows)
             && !is_zero_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update succeeded but affected_rows more than one", K(ret), K(affected_rows));
  }

  LOG_INFO("[RESTORE] update job status", K(ret), K(job_id), K(status));
  return ret;
}

int ObPhysicalRestoreTableOperator::remove_job(
  int64_t job_id)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", K(ret));
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    // remove from __all_restore_job
    if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
      LOG_WARN("failed to add pk column", K(ret), K(job_id));
    } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_RESTORE_JOB_TNAME, sql))) {
      LOG_WARN("splice_delete_sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret), K(exec_tenant_id));
    } else {
      // no need to check affected_rows
    }
  }
  LOG_INFO("[RESTORE] remove job", K(ret), K(job_id));
  return ret;
}

int ObPhysicalRestoreTableOperator::get_job_by_tenant_id(
    const uint64_t restore_tenant_id,
    ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_INVALID_TENANT_ID == exec_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exec_tenant_id", K(ret), K(exec_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id in (SELECT job_id "
                                    "FROM %s WHERE name = 'tenant_id' AND value = '%lu')",
                                    OB_ALL_RESTORE_JOB_TNAME,
                                    OB_ALL_RESTORE_JOB_TNAME,
                                    restore_tenant_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(get_restore_job_by_sql_(exec_tenant_id, sql, job_info))) {
    LOG_WARN("failed to get restore job by sql", KR(ret), K(exec_tenant_id), K(sql));
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::get_job_by_tenant_name(
    const ObString &tenant_name,
    ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exec_tenant_id", K(ret), K(tenant_name));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id in (SELECT job_id "
                                    "FROM %s WHERE name = 'tenant_name' AND value = '%.*s')",
                                    OB_ALL_RESTORE_JOB_TNAME,
                                    OB_ALL_RESTORE_JOB_TNAME,
                                    tenant_name.length(), tenant_name.ptr()))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_name));
  } else if (OB_FAIL(get_restore_job_by_sql_(exec_tenant_id, sql, job_info))) {
    LOG_WARN("failed to get restore job by sql", KR(ret), K(sql), K(exec_tenant_id));
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::get_job_by_restore_tenant_name(
    const ObString &tenant_name,
    ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exec_tenant_id", K(ret), K(tenant_name));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id in (SELECT job_id "
                                    "FROM %s WHERE name = 'restore_tenant_name' AND value = '%.*s')",
                                    OB_ALL_RESTORE_JOB_TNAME,
                                    OB_ALL_RESTORE_JOB_TNAME,
                                    tenant_name.length(), tenant_name.ptr()))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_name));
  } else if (OB_FAIL(get_restore_job_by_sql_(exec_tenant_id, sql, job_info))) {
    LOG_WARN("failed to get restore job by sql", KR(ret), K(sql), K(exec_tenant_id));
  }
  return ret;
}
int ObPhysicalRestoreTableOperator::get_restore_job_by_sql_(
    const uint64_t exec_tenant_id,
    const ObSqlString &sql, ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sql.empty() || OB_INVALID_TENANT_ID == exec_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql is empty", KR(ret), K(exec_tenant_id), K(sql));
  } else if (OB_ISNULL(sql_client_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_client_));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client_->read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(exec_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        int64_t idx = 0;
        int64_t job_info_id = OB_INVALID_ID;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          int64_t job_id = OB_INVALID_ID;
          uint64_t tenant_id = OB_INVALID_TENANT_ID;
          EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, int64_t);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to get job", KR(ret), K(exec_tenant_id), K(sql));
          } else if ((OB_INVALID_ID != job_info_id && job_id != job_info_id)
                     || tenant_id != tenant_id_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("exec_tenant_id has multi restore job", K(ret), K(job_info), K(sql),
            K(tenant_id), K(tenant_id_));
          } else if (FALSE_IT(job_info_id = job_id)) {
          } else if (OB_FAIL(job_info.init_restore_key(tenant_id, job_info_id))) {
            LOG_WARN("failed to init restore key", KR(ret), K(tenant_id), K(job_info_id));
          } else if (OB_FAIL(retrieve_restore_option(*result, job_info))) {
            LOG_WARN("fail to retrieve restore option", K(ret), K(job_info));
          }
        }
        if (OB_ITER_END == ret) {
          if (!job_info.is_valid()) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("job info is invalid", K(ret), K(sql), K(job_info));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          LOG_WARN("get jobs fail", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::check_finish_restore_to_consistent_scn(
    bool &is_finished, bool &is_success)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_sys_tenant(exec_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id cannot be sys", KR(ret), K_(tenant_id));
  } else {
    is_finished = true;
    is_success = true;
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select a.ls_id, b.restore_status, b.replica_status from %s as a "
              "left join %s as b on a.ls_id = b.ls_id",
              OB_ALL_LS_STATUS_TNAME, OB_ALL_LS_META_TABLE_TNAME))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (OB_FAIL(sql_client_->read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(sql));
      } else {
        int64_t ls_id = 0;
        share::ObLSRestoreStatus ls_restore_status;
        int32_t restore_status = -1;
        while (OB_SUCC(ret) && OB_SUCC(result->next())
            && !(is_finished && !is_success)) {
          EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "restore_status", restore_status, int32_t);

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ls_restore_status.set_status(restore_status))) {
            LOG_WARN("failed to set status", KR(ret), K(restore_status));
          } else if (ls_restore_status.is_restore_failed()) {
            //restore failed
            is_finished = true;
            is_success = false;
          } else {
            const ObLSRestoreStatus target_status(ObLSRestoreStatus::Status::WAIT_RESTORE_TO_CONSISTENT_SCN);
            if (ls_restore_status.get_status() < target_status.get_status()
                && !ls_restore_status.is_restore_none()
                && is_finished && is_success) {
              is_finished = false;
            }
          }
        } // while
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}