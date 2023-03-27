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
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_kv_parser.h"
#include "share/ob_cluster_version.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "share/backup/ob_backup_path.h"
#include <algorithm>
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

static const char* physical_restore_mod_str_array[PHYSICAL_RESTORE_MOD_MAX_NUM] = {"ROOTSERVICE", "CLOG", "STORAGE"};

const char* ObPhysicalRestoreTableOperator::get_physical_restore_mod_str(PhysicalRestoreMod mod)
{
  const char* str = NULL;
  if (mod >= PHYSICAL_RESTORE_MOD_RS && mod < PHYSICAL_RESTORE_MOD_MAX_NUM) {
    str = physical_restore_mod_str_array[mod];
  }
  return str;
}

static const char* phy_restore_status_str_array[PHYSICAL_RESTORE_MAX_STATUS] = {"CREATE_TENANT",
    "RESTORE_SYS_REPLICA",
    "UPGRADE_PRE",
    "UPGRADE_POST",
    "MODIFY_SCHEMA",
    "CREATE_USER_PARTITIONS",
    "RESTORE_USER_REPLICA",
    "REBUILD_INDEX",
    "POST_CHECK",
    "RESTORE_SUCCESS",
    "RESTORE_FAIL"};

const char* ObPhysicalRestoreTableOperator::get_restore_status_str(PhysicalRestoreStatus status)
{
  const char* str = NULL;
  if (status >= PHYSICAL_RESTORE_CREATE_TENANT && status < PHYSICAL_RESTORE_MAX_STATUS) {
    str = phy_restore_status_str_array[status];
  }
  return str;
}

PhysicalRestoreStatus ObPhysicalRestoreTableOperator::get_restore_status(const common::ObString& status_str)
{
  PhysicalRestoreStatus status = PHYSICAL_RESTORE_MAX_STATUS;
  for (int i = 0; i < static_cast<int>(PHYSICAL_RESTORE_MAX_STATUS); i++) {
    if (OB_NOT_NULL(phy_restore_status_str_array[i]) && 0 == status_str.case_compare(phy_restore_status_str_array[i])) {
      status = static_cast<PhysicalRestoreStatus>(i);
      break;
    }
  }
  return status;
}

ObPhysicalRestoreTableOperator::ObPhysicalRestoreTableOperator() : inited_(false), sql_client_(NULL)
{}

int ObPhysicalRestoreTableOperator::init(common::ObISQLClient* sql_client)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("physical restore table operator init twice", K(ret));
  } else if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    sql_client_ = sql_client;
    inited_ = true;
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::insert_job(const ObPhysicalRestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", KR(ret));
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    // insert __all_restore_info
    if (OB_FAIL(fill_dml_splicer(dml, job_info))) {
      LOG_WARN("fail to fill dml splicer", KR(ret), K(job_info));
    } else if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_RESTORE_INFO_TNAME, sql))) {
      LOG_WARN("splice_insert_sql failed", KR(ret));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql));
    } else if (affected_rows <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid affected rows", KR(ret), K(affected_rows), K(sql));
    }
    // insert __all_restore_progress
    if (OB_SUCC(ret)) {
      dml.reset();
      sql.reset();
      affected_rows = 0;
      const char* status_str = ObPhysicalRestoreTableOperator::get_restore_status_str(job_info.status_);
      int64_t invalid_cnt = OB_INVALID_COUNT;
      if (OB_FAIL(dml.add_pk_column("job_id", job_info.job_id_))) {
        LOG_WARN("fail to add pk column", KR(ret), "job_id", job_info.job_id_);
      } else if (OB_FAIL(dml.add_column("status", status_str)) || OB_FAIL(dml.add_column("pg_count", invalid_cnt)) ||
                 OB_FAIL(dml.add_column("finish_pg_count", invalid_cnt)) ||
                 OB_FAIL(dml.add_column("partition_count", invalid_cnt)) ||
                 OB_FAIL(dml.add_column("finish_partition_count", invalid_cnt)) ||
                 OB_FAIL(dml.add_column("macro_block_count", invalid_cnt)) ||
                 OB_FAIL(dml.add_column("finish_macro_block_count", invalid_cnt))) {
        LOG_WARN("fail to add column", KR(ret), K(job_info));
      } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_RESTORE_PROGRESS_TNAME, sql))) {
        LOG_WARN("splice_insert_sql failed", KR(ret));
      } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(sql), KR(ret), K(sql));
      } else if (affected_rows <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid affected rows", KR(ret), K(affected_rows), K(sql));
      }
    }
  }
  LOG_INFO("[RESTORE] insert job", KR(ret), K(job_info));
  return ret;
}

int ObPhysicalRestoreTableOperator::replace_job(const ObPhysicalRestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", K(ret));
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(fill_dml_splicer(dml, job_info))) {
      LOG_WARN("fail to fill dml splicer", K(ret), K(job_info));
    } else if (OB_FAIL(dml.splice_batch_replace_sql(OB_ALL_RESTORE_INFO_TNAME, sql))) {
      LOG_WARN("splice_insert_sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (affected_rows <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid affected rows", K(ret), K(affected_rows));
    }
  }
  LOG_INFO("[RESTORE] replace job", K(ret), K(job_info));
  return ret;
}

int ObPhysicalRestoreTableOperator::fill_dml_splicer(share::ObDMLSqlSplicer& dml, const ObPhysicalRestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", K(ret));
  } else {

#define DML_ADD_COLUMN(JOB_INFO, COLUMN_NAME)                                     \
  if (OB_SUCC(ret)) {                                                             \
    if (OB_FAIL(dml.add_pk_column("job_id", JOB_INFO.job_id_))) {                 \
      LOG_WARN("fail to add pk column", K(ret), "job_id", JOB_INFO.job_id_);      \
    } else if (OB_FAIL(dml.add_pk_column("name", #COLUMN_NAME))) {                \
      LOG_WARN("fail to add pk column", K(ret), "name", #COLUMN_NAME);            \
    } else if (OB_FAIL(dml.add_column("value", (JOB_INFO).COLUMN_NAME##_))) {     \
      LOG_WARN("fail to add column", K(ret), "value", (JOB_INFO).COLUMN_NAME##_); \
    } else if (OB_FAIL(dml.finish_row())) {                                       \
      LOG_WARN("fail to finish row", K(ret));                                     \
    }                                                                             \
  }

#define DML_ADD_COLUMN_WITH_VALUE(JOB_INFO, COLUMN_NAME, COLUMN_VALUE)       \
  if (OB_SUCC(ret)) {                                                        \
    if (OB_FAIL(dml.add_pk_column("job_id", JOB_INFO.job_id_))) {            \
      LOG_WARN("fail to add pk column", K(ret), "job_id", JOB_INFO.job_id_); \
    } else if (OB_FAIL(dml.add_pk_column("name", #COLUMN_NAME))) {           \
      LOG_WARN("fail to add pk column", K(ret), "name", #COLUMN_NAME);       \
    } else if (OB_FAIL(dml.add_column("value", COLUMN_VALUE))) {             \
      LOG_WARN("fail to add column", K(ret), "value", COLUMN_VALUE);         \
    } else if (OB_FAIL(dml.finish_row())) {                                  \
      LOG_WARN("fail to finish row", K(ret));                                \
    }                                                                        \
  }
    char version[common::ObClusterVersion::MAX_VERSION_ITEM] = {0};
    /* rs */
    DML_ADD_COLUMN(job_info, tenant_id);
    DML_ADD_COLUMN(job_info, restore_data_version);
    DML_ADD_COLUMN(job_info, restore_start_ts);
    DML_ADD_COLUMN(job_info, restore_schema_version);
    DML_ADD_COLUMN(job_info, rebuild_index_schema_version);
    DML_ADD_COLUMN(job_info, info);
    // pre_cluster_version
    if (OB_SUCC(ret)) {
      uint64_t pre_cluster_version = job_info.pre_cluster_version_;
      int64_t len =
          ObClusterVersion::print_version_str(version, common::ObClusterVersion::MAX_VERSION_ITEM, pre_cluster_version);
      if (len < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid pre_cluster_version", K(ret), K(pre_cluster_version));
      } else if (OB_FAIL(dml.add_pk_column("job_id", job_info.job_id_))) {
        LOG_WARN("fail to add pk column", K(ret), "job_id", job_info.job_id_);
      } else if (OB_FAIL(dml.add_pk_column("name", "pre_cluster_version"))) {
        LOG_WARN("fail to add pk column", K(ret), "name", "pre_cluster_version");
      } else if (OB_FAIL(dml.add_column("value", ObString(len, version)))) {
        LOG_WARN("fail to add column", K(ret), K(pre_cluster_version));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", K(ret));
      }
    }
    // post_cluster_version
    if (OB_SUCC(ret)) {
      uint64_t post_cluster_version = job_info.post_cluster_version_;
      int64_t len = ObClusterVersion::print_version_str(
          version, common::ObClusterVersion::MAX_VERSION_ITEM, post_cluster_version);
      if (len < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid post_cluster_version", K(ret), K(post_cluster_version));
      } else if (OB_FAIL(dml.add_pk_column("job_id", job_info.job_id_))) {
        LOG_WARN("fail to add pk column", K(ret), "job_id", job_info.job_id_);
      } else if (OB_FAIL(dml.add_pk_column("name", "post_cluster_version"))) {
        LOG_WARN("fail to add pk column", K(ret), "name", "post_cluster_version");
      } else if (OB_FAIL(dml.add_column("value", ObString(len, version)))) {
        LOG_WARN("fail to add column", K(ret), K(post_cluster_version));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", K(ret));
      }
    }
    // status
    const char* status_str = ObPhysicalRestoreTableOperator::get_restore_status_str(job_info.status_);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(status_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid status", K(ret), "status", job_info.status_);
    } else if (OB_FAIL(dml.add_pk_column("job_id", job_info.job_id_))) {
      LOG_WARN("fail to add pk column", K(ret), "job_id", job_info.job_id_);
    } else if (OB_FAIL(dml.add_pk_column("name", "status"))) {
      LOG_WARN("fail to add pk column", K(ret), "name", "status");
    } else if (OB_FAIL(dml.add_column("value", status_str))) {
      LOG_WARN("fail to add column", K(ret), "value", status_str);
    } else if (OB_FAIL(dml.finish_row())) {
      LOG_WARN("fail to finish row", K(ret));
    }

    /* uri */
    DML_ADD_COLUMN(job_info, restore_job_id);
    DML_ADD_COLUMN(job_info, restore_timestamp);
    DML_ADD_COLUMN(job_info, cluster_id);
    DML_ADD_COLUMN(job_info, restore_option);
    DML_ADD_COLUMN(job_info, backup_dest);
    DML_ADD_COLUMN(job_info, tenant_name);
    DML_ADD_COLUMN(job_info, backup_tenant_name);
    DML_ADD_COLUMN(job_info, backup_cluster_name);
    DML_ADD_COLUMN(job_info, pool_list);
    DML_ADD_COLUMN(job_info, primary_zone);
    DML_ADD_COLUMN(job_info, locality);
    /* oss */
    DML_ADD_COLUMN(job_info, backup_locality);
    DML_ADD_COLUMN(job_info, backup_primary_zone);
    DML_ADD_COLUMN_WITH_VALUE(job_info, compat_mode, static_cast<int64_t>(job_info.compat_mode_));
    DML_ADD_COLUMN(job_info, backup_tenant_id);
    DML_ADD_COLUMN(job_info, incarnation);
    DML_ADD_COLUMN(job_info, full_backup_set_id);
    DML_ADD_COLUMN(job_info, inc_backup_set_id);
    DML_ADD_COLUMN(job_info, log_archive_round);
    DML_ADD_COLUMN(job_info, snapshot_version);
    DML_ADD_COLUMN(job_info, schema_version);
    DML_ADD_COLUMN(job_info, frozen_data_version);
    DML_ADD_COLUMN(job_info, frozen_snapshot_version);
    DML_ADD_COLUMN(job_info, frozen_schema_version);
    DML_ADD_COLUMN_WITH_VALUE(job_info, passwd_array, job_info.passwd_array_);
    DML_ADD_COLUMN(job_info, compatible);
    DML_ADD_COLUMN(job_info, backup_date);
    // source_cluster_version
    if (OB_SUCC(ret)) {
      uint64_t source_cluster_version = job_info.source_cluster_version_;
      int64_t len = ObClusterVersion::print_version_str(
          version, common::ObClusterVersion::MAX_VERSION_ITEM, source_cluster_version);
      if (len < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid asource_cluster_version", K(ret), K(source_cluster_version));
      } else if (OB_FAIL(dml.add_pk_column("job_id", job_info.job_id_))) {
        LOG_WARN("fail to add pk column", K(ret), "job_id", job_info.job_id_);
      } else if (OB_FAIL(dml.add_pk_column("name", "source_cluster_version"))) {
        LOG_WARN("fail to add pk column", K(ret), "name", "source_cluster_version");
      } else if (OB_FAIL(dml.add_column("value", ObString(len, version)))) {
        LOG_WARN("fail to add column", K(ret), K(source_cluster_version));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", K(ret));
      }
    }
    // while_list/b_while_list
    if (OB_SUCC(ret)) {
      ObArenaAllocator allocator("PhyWhiteList");
      const ObPhysicalRestoreWhiteList& white_list = job_info.white_list_;
      ObString white_list_str;
      ObString b_white_list_str;
      if (OB_FAIL(white_list.get_format_str(allocator, white_list_str))) {
        LOG_WARN("fail to get format str", KR(ret), K(white_list));
      } else if (OB_FAIL(dml.add_pk_column("job_id", job_info.job_id_))) {
        LOG_WARN("fail to add pk column", KR(ret), "job_id", job_info.job_id_);
      } else if (OB_FAIL(dml.add_pk_column("name", "white_list"))) {
        LOG_WARN("fail to add pk column", KR(ret), "name", "white_list");
      } else if (OB_FAIL(dml.add_column("value", ObHexEscapeSqlStr(white_list_str)))) {
        LOG_WARN("fail to add column", KR(ret), K(white_list), K(white_list_str));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(white_list.get_hex_str(allocator, b_white_list_str))) {
        LOG_WARN("fail to get format str", KR(ret), K(white_list));
      } else if (OB_FAIL(dml.add_pk_column("job_id", job_info.job_id_))) {
        LOG_WARN("fail to add pk column", KR(ret), "job_id", job_info.job_id_);
      } else if (OB_FAIL(dml.add_pk_column("name", "b_white_list"))) {
        LOG_WARN("fail to add pk column", KR(ret), "name", "b_white_list");
      } else if (OB_FAIL(dml.add_column("value", b_white_list_str))) {
        LOG_WARN("fail to add column", KR(ret), K(b_white_list_str));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObArenaAllocator allocator;
      ObString b_backup_set_list;
      ObString b_backup_piece_list;
      const ObPhysicalRestoreBackupDestList& dest_list = job_info.multi_restore_path_list_;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(dest_list.get_backup_set_list_hex_str(allocator, b_backup_set_list))) {
        LOG_WARN("fail to get format str", KR(ret), K(dest_list));
      } else if (OB_FAIL(dml.add_pk_column("job_id", job_info.job_id_))) {
        LOG_WARN("fail to add pk column", KR(ret), "job_id", job_info.job_id_);
      } else if (OB_FAIL(dml.add_pk_column("name", "b_backup_set_list"))) {
        LOG_WARN("fail to add pk column", KR(ret), "name", "b_backup_set_list");
      } else if (OB_FAIL(dml.add_column("value", b_backup_set_list))) {
        LOG_WARN("fail to add column", KR(ret), K(b_backup_set_list));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(dest_list.get_backup_piece_list_hex_str(allocator, b_backup_piece_list))) {
        LOG_WARN("fail to get format str", KR(ret), K(dest_list));
      } else if (OB_FAIL(dml.add_pk_column("job_id", job_info.job_id_))) {
        LOG_WARN("fail to add pk column", KR(ret), "job_id", job_info.job_id_);
      } else if (OB_FAIL(dml.add_pk_column("name", "b_backup_piece_list"))) {
        LOG_WARN("fail to add pk column", KR(ret), "name", "b_backup_piece_list");
      } else if (OB_FAIL(dml.add_column("value", b_backup_piece_list))) {
        LOG_WARN("fail to add column", KR(ret), K(b_backup_piece_list));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret));
      }
    }

#undef ADD_COLUMN
#undef ADD_COLUMN_WITH_VALUE
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::get_jobs(common::ObIArray<ObPhysicalRestoreJob>& jobs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    jobs.reset();
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("physical restore table operator not init", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s ORDER BY job_id, name", OB_ALL_RESTORE_INFO_TNAME))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      ObPhysicalRestoreJob current;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        int64_t job_id = OB_INVALID_ID;
        EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
        if (OB_FAIL(ret)) {
        } else if (OB_INVALID_ID != current.job_id_ && job_id != current.job_id_) {
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

        if (OB_SUCC(ret)) {
          current.job_id_ = job_id;
          if (OB_FAIL(retrieve_restore_option(*result, current))) {
            LOG_WARN("fail to retrieve restore option", K(ret), K(current));
          } else {
            LOG_DEBUG("current job", K(ret), K(current));
          }
        }
      }  // end for

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
    LOG_INFO("[RESTORE] get restore jobs", K(ret), "job_cnt", jobs.count());
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::get_restore_infos(common::ObIArray<ObPhysicalRestoreInfo>& infos)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObPhysicalRestoreJob> jobs;
  ObPhysicalRestoreInfo info;
  infos.reset();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_jobs(jobs))) {
    LOG_WARN("failed to get jobs", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
    ObPhysicalRestoreJob& job = jobs.at(i);
    if (OB_FAIL(job.copy_to(info))) {
      LOG_WARN("failed to copy info", K(ret), K(i), K(job));
    } else if (OB_FAIL(infos.push_back(info))) {
      LOG_WARN("failed to add info", K(ret), K(i), K(info));
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::retrieve_restore_option(
    common::sqlclient::ObMySQLResult& result, ObPhysicalRestoreJob& job)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == job.job_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job", K(ret), K(job));
  } else {
    ObArenaAllocator allocator(ObModIds::RESTORE);
    char name[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = {0};
    char* value = NULL;
    int64_t len = OB_INNER_TABLE_DEFAULT_KEY_LENTH;
    int64_t real_len = 0;  // not used
    EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(result, "name", name, OB_INNER_TABLE_DEFAULT_KEY_LENTH, real_len);
    EXTRACT_LONGTEXT_FIELD_MYSQL_WITH_ALLOCATOR_SKIP_RET(result, "value", value, allocator, real_len);

#define RETRIEVE_UINT_VALUE(COLUMN_NAME, OBJ)                                       \
  if (OB_SUCC(ret)) {                                                               \
    if (0 == STRNCMP(#COLUMN_NAME, name, len)) {                                    \
      if (OB_FAIL(retrieve_uint_value(result, (OBJ).COLUMN_NAME##_))) {             \
        LOG_WARN("fail to retrive int value", K(ret), "column_name", #COLUMN_NAME); \
      }                                                                             \
    }                                                                               \
  }

#define RETRIEVE_INT_VALUE(COLUMN_NAME, OBJ)                                        \
  if (OB_SUCC(ret)) {                                                               \
    if (0 == STRNCMP(#COLUMN_NAME, name, len)) {                                    \
      if (OB_FAIL(retrieve_int_value(result, (OBJ).COLUMN_NAME##_))) {              \
        LOG_WARN("fail to retrive int value", K(ret), "column_name", #COLUMN_NAME); \
      }                                                                             \
    }                                                                               \
  }

#define RETRIEVE_STR_VALUE(COLUMN_NAME, OBJ)                                                    \
  if (OB_SUCC(ret)) {                                                                           \
    if (0 == STRNCMP(#COLUMN_NAME, name, len)) {                                                \
      EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(                                                      \
          result, "value", (OBJ).COLUMN_NAME##_, OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_len); \
    }                                                                                           \
  }

    RETRIEVE_UINT_VALUE(tenant_id, job);
    RETRIEVE_UINT_VALUE(backup_tenant_id, job);
    RETRIEVE_INT_VALUE(restore_data_version, job);
    RETRIEVE_INT_VALUE(restore_start_ts, job);
    RETRIEVE_INT_VALUE(restore_schema_version, job);
    RETRIEVE_INT_VALUE(rebuild_index_schema_version, job);
    RETRIEVE_INT_VALUE(incarnation, job);
    RETRIEVE_INT_VALUE(full_backup_set_id, job);
    RETRIEVE_INT_VALUE(inc_backup_set_id, job);
    RETRIEVE_INT_VALUE(log_archive_round, job);
    RETRIEVE_INT_VALUE(snapshot_version, job);
    RETRIEVE_INT_VALUE(schema_version, job);
    RETRIEVE_INT_VALUE(frozen_data_version, job);
    RETRIEVE_INT_VALUE(frozen_snapshot_version, job);
    RETRIEVE_INT_VALUE(frozen_schema_version, job);
    RETRIEVE_INT_VALUE(restore_job_id, job);
    RETRIEVE_INT_VALUE(restore_timestamp, job);
    RETRIEVE_INT_VALUE(cluster_id, job);
    RETRIEVE_STR_VALUE(restore_option, job);
    RETRIEVE_STR_VALUE(backup_dest, job);
    RETRIEVE_STR_VALUE(tenant_name, job);
    RETRIEVE_STR_VALUE(backup_tenant_name, job);
    RETRIEVE_STR_VALUE(backup_cluster_name, job);
    RETRIEVE_STR_VALUE(pool_list, job);
    RETRIEVE_STR_VALUE(primary_zone, job);
    RETRIEVE_STR_VALUE(locality, job);
    RETRIEVE_STR_VALUE(backup_primary_zone, job);
    RETRIEVE_STR_VALUE(backup_locality, job);
    RETRIEVE_STR_VALUE(info, job);
    RETRIEVE_STR_VALUE(passwd_array, job);
    RETRIEVE_INT_VALUE(compatible, job);
    RETRIEVE_INT_VALUE(backup_date, job);
    if (OB_SUCC(ret)) {
      if (0 == STRNCMP("status", name, len)) {
        ObString status_str;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", status_str);
        if (OB_SUCC(ret)) {
          job.status_ = ObPhysicalRestoreTableOperator::get_restore_status(status_str);
          if (PHYSICAL_RESTORE_MAX_STATUS == job.status_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid status", K(ret), K(status_str));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == STRNCMP("compat_mode", name, len)) {
        int64_t compat_mode = 0;
        if (OB_FAIL(retrieve_int_value(result, compat_mode))) {
          LOG_WARN("fail to retrive int value", K(ret));
        } else {
          job.compat_mode_ = static_cast<lib::Worker::CompatMode>(compat_mode);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == STRNCMP("pre_cluster_version", name, len)) {
        ObString version_str;
        uint64_t version = 0;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", version_str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObClusterVersion::get_version(version_str, version))) {
          LOG_WARN("fail to parser version", K(ret), K(version_str));
        } else {
          job.pre_cluster_version_ = version;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == STRNCMP("post_cluster_version", name, len)) {
        ObString version_str;
        uint64_t version = 0;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", version_str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObClusterVersion::get_version(version_str, version))) {
          LOG_WARN("fail to parser version", K(ret), K(version_str));
        } else {
          job.post_cluster_version_ = version;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == STRNCMP("source_cluster_version", name, len)) {
        ObString version_str;
        uint64_t version = 0;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", version_str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObClusterVersion::get_version(version_str, version))) {
          LOG_WARN("fail to parser version", K(ret), K(version_str));
        } else {
          job.source_cluster_version_ = version;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (0 == STRNCMP("b_backup_set_list", name, len)) {
        ObString str;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(job.multi_restore_path_list_.backup_set_list_assign_with_hex_str(str))) {
          LOG_WARN("fail to assign backup set list", KR(ret), K(str));
        } else {
          ObCompareSimpleBackupSetPath cmp;
          ObSArray<ObSimpleBackupSetPath>& simple_list = job.multi_restore_path_list_.get_backup_set_path_list();
          std::sort(simple_list.begin(), simple_list.end(), cmp);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (0 == STRNCMP("b_backup_piece_list", name, len)) {
        ObString str;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(job.multi_restore_path_list_.backup_piece_list_assign_with_hex_str(str))) {
          LOG_WARN("fail to assign backup piece list", KR(ret), K(str));
        } else {
          ObCompareSimpleBackupPiecePath cmp;
          ObSArray<ObSimpleBackupPiecePath>& simple_list = job.multi_restore_path_list_.get_backup_piece_path_list();
          std::sort(simple_list.begin(), simple_list.end(), cmp);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (0 == STRNCMP("b_white_list", name, len)) {
        ObString str;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "value", str);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(job.white_list_.assign_with_hex_str(str))) {
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

int ObPhysicalRestoreTableOperator::retrieve_int_value(common::sqlclient::ObMySQLResult& result, int64_t& value)
{
  int ret = OB_SUCCESS;
  char value_buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH];
  int64_t real_len = 0;  // not used
  EXTRACT_STRBUF_FIELD_MYSQL(result, "value", value_buf, OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_len);
  if (OB_SUCC(ret)) {
    char* endptr = NULL;
    value = strtoll(value_buf, &endptr, 0);
    if (0 == strlen(value_buf) || '\0' != *endptr) {
      ret = OB_INVALID_DATA;
      LOG_WARN("not int value", K(ret), K(value_buf));
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::retrieve_uint_value(common::sqlclient::ObMySQLResult& result, uint64_t& value)
{
  int ret = OB_SUCCESS;
  char value_buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH];
  int64_t real_len = 0;  // not used
  EXTRACT_STRBUF_FIELD_MYSQL(result, "value", value_buf, OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_len);
  if (OB_SUCC(ret)) {
    char* endptr = NULL;
    value = strtoull(value_buf, &endptr, 0);
    if (0 == strlen(value_buf) || '\0' != *endptr) {
      ret = OB_INVALID_DATA;
      LOG_WARN("not uint value", K(ret), K(value_buf));
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::check_job_exist(const int64_t job_id, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObSqlString sql;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("physical restore table operator not init", K(ret));
    } else if (job_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job_id", K(ret), K(job_id));
    } else if (OB_FAIL(sql.assign_fmt(
                   "SELECT count(*) as count FROM %s WHERE job_id = %ld", OB_ALL_RESTORE_INFO_TNAME, job_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get row", K(ret), K(job_id), K(sql));
    } else {
      int64_t count = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int64_t);
      exist = (count > 0);
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::get_job(const int64_t job_id, ObPhysicalRestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("physical restore table operator not init", K(ret));
    } else if (job_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job_id", K(ret), K(job_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld", OB_ALL_RESTORE_INFO_TNAME, job_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      int64_t idx = 0;
      job_info.job_id_ = job_id;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        int64_t job_id = OB_INVALID_ID;
        EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
        if (job_id != job_info.job_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("job_info job is invalid", K(ret), K(job_info), K(job_id));
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

int ObPhysicalRestoreTableOperator::get_job_count(int64_t& job_count)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPhysicalRestoreJob, 1> jobs;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", K(ret));
  } else if (OB_FAIL(get_jobs(jobs))) {
    LOG_WARN("fail get jobs", K(ret));
  } else {
    job_count = jobs.count();
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::update_job_error_info(int64_t job_id, int return_ret, PhysicalRestoreMod mod,
    const common::ObCurTraceId::TraceId& trace_id, const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  const char* mod_str = get_physical_restore_mod_str(mod);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", KR(ret));
  } else if (job_id < 0 || OB_SUCCESS == return_ret || OB_ISNULL(mod_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(job_id), K(return_ret), K(mod));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    // update __all_restore_info
    if (OB_FAIL(sql.assign_fmt("UPDATE %s SET value = '%s : %s(%d) on %s with traceid %s' "
                               "WHERE job_id = %ld AND name = 'info' AND value = ''",
            OB_ALL_RESTORE_INFO_TNAME,
            mod_str,
            ob_error_name(return_ret),
            return_ret,
            to_cstring(addr),
            to_cstring(trace_id),
            job_id))) {
      LOG_WARN("failed to set sql", K(ret), K(mod_str), K(return_ret), K(trace_id), K(addr));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), KR(ret));
    } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update succeeded but affected_rows more than one", KR(ret), K(affected_rows));
    }
    // update __all_retore_progress
    if (OB_SUCC(ret)) {
      sql.reset();
      affected_rows = 0;
      if (OB_FAIL(sql.assign_fmt("UPDATE %s SET info = '%s : %s(%d)' "
                                 "WHERE job_id = %ld AND info = ''",
              OB_ALL_RESTORE_PROGRESS_TNAME,
              mod_str,
              ob_error_name(return_ret),
              return_ret,
              job_id))) {
      } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update succeeded but affected_rows more than one", KR(ret), K(affected_rows));
      }
    }
  }
  LOG_INFO("[RESTORE] update job error info", KR(ret), K(job_id), K(return_ret), K(mod));
  return ret;
}

int ObPhysicalRestoreTableOperator::update_job_status(int64_t job_id, int64_t status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const char* status_str =
      ObPhysicalRestoreTableOperator::get_restore_status_str(static_cast<PhysicalRestoreStatus>(status));
  // update __all_restore_info
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", K(ret));
  } else if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_id", K(ret), K(job_id));
  } else if (OB_ISNULL(status_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(ret), K(status));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET value = '%s' WHERE job_id = %ld "
                                    "AND name = 'status' AND value != 'RESTORE_FAIL'",
                 OB_ALL_RESTORE_INFO_TNAME,
                 status_str,
                 job_id))) {
    LOG_WARN("fail to assign fmt", K(ret), K(sql));
  } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update succeeded but affected_rows more than one", K(ret), K(affected_rows));
  }
  // update __all_restore_progress
  if (OB_SUCC(ret)) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt("UPDATE %s SET status = '%s' "
                               "WHERE job_id = %ld AND status != 'RESTORE_FAIL'",
            OB_ALL_RESTORE_PROGRESS_TNAME,
            status_str,
            job_id))) {
      LOG_WARN("fail to assign fmt", K(ret), K(sql));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update succeeded but affected_rows more than one", K(ret), K(affected_rows));
    }
  }
  LOG_INFO("[RESTORE] update job status", K(ret), K(job_id), K(status));
  return ret;
}

int ObPhysicalRestoreTableOperator::recycle_job(int64_t job_id, int64_t status)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", K(ret));
  } else if (OB_FAIL(update_job_status(job_id, status))) {
    LOG_WARN("mark job done fail", K(job_id), K(ret));
  } else if (OB_FAIL(update_rs_job_status(job_id, status))) {
    LOG_WARN("remove task fail", K(job_id), K(ret));
  } else if (OB_FAIL(record_job_in_history(job_id))) {
    LOG_WARN("record job in history fail", K(job_id), K(ret));
  } else if (OB_FAIL(remove_job(job_id))) {
    LOG_WARN("fail remove job", K(job_id), K(ret));
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::update_rs_job_status(int64_t job_id, int64_t status)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", K(ret));
  } else if (job_id > 0) {
    int tmp_ret = PHYSICAL_RESTORE_SUCCESS == status ? OB_SUCCESS : OB_ERROR;
    if (OB_FAIL(RS_JOB_COMPLETE(job_id, tmp_ret, *sql_client_))) {
      LOG_ERROR("fail to complete job", K(tmp_ret), K(ret), K(job_id));
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::record_job_in_history(int64_t job_id)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreJob job;
  ObRestoreProgressInfo statistic;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", KR(ret));
  } else if (OB_FAIL(get_job(job_id, job))) {
    LOG_WARN("fail to get job", KR(ret), K(job_id));
  } else if (OB_FAIL(get_restore_progress_statistic(job, statistic))) {
    LOG_WARN("fail to get restore progress statistic", KR(ret), K(job_id));
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    const char* status_str = ObPhysicalRestoreTableOperator::get_restore_status_str(job.status_);
    int64_t invalid_cnt = OB_INVALID_COUNT;
    ObArenaAllocator allocator("PhyWhiteList");
    const ObPhysicalRestoreWhiteList& white_list = job.white_list_;
    ObString white_list_str;
    const ObPhysicalRestoreBackupDestList& dest_list = job.multi_restore_path_list_;
    ObString backup_set_list_str;
    ObString backup_piece_list_str;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(status_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid status", KR(ret), "status", job.status_);
    } else if (OB_FAIL(white_list.get_format_str(allocator, white_list_str))) {
      LOG_WARN("fail to get format str", KR(ret), K(white_list));
    } else if (OB_FAIL(dest_list.get_backup_set_list_format_str(allocator, backup_set_list_str))) {
      LOG_WARN("fail to get format str", KR(ret), K(dest_list));
    } else if (OB_FAIL(dest_list.get_backup_piece_list_format_str(allocator, backup_piece_list_str))) {
      LOG_WARN("fail to get format str", KR(ret), K(dest_list));
    } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
      LOG_WARN("failed to add pk column", KR(ret), K(job_id));
    } else if (OB_FAIL(dml.add_column("external_job_id", job.restore_job_id_)) ||
               OB_FAIL(dml.add_column("tenant_id", job.tenant_id_)) ||
               OB_FAIL(dml.add_column("tenant_name", job.tenant_name_)) ||
               OB_FAIL(dml.add_column("status", status_str)) ||
               (job.restore_start_ts_ > 0 && OB_FAIL(dml.add_time_column("start_time", job.restore_start_ts_))) ||
               OB_FAIL(dml.add_time_column("completion_time", 0 /*current time*/)) ||
               OB_FAIL(dml.add_column("pg_count", statistic.total_pg_cnt_)) ||
               OB_FAIL(dml.add_column("finish_pg_count", statistic.finish_pg_cnt_)) ||
               OB_FAIL(dml.add_column("partition_count", statistic.total_partition_cnt_)) ||
               OB_FAIL(dml.add_column("finish_partition_count", statistic.finish_partition_cnt_)) ||
               OB_FAIL(dml.add_column("macro_block_count", invalid_cnt)) ||
               OB_FAIL(dml.add_column("finish_macro_block_count", invalid_cnt)) ||
               (job.frozen_snapshot_version_ > 0 &&
                   OB_FAIL(dml.add_time_column("restore_start_timestamp", job.frozen_snapshot_version_))) ||
               (job.restore_timestamp_ > 0 &&
                   OB_FAIL(dml.add_time_column("restore_finish_timestamp", job.restore_timestamp_)))
               /*  restore_current_timestamp is null */
               || OB_FAIL(dml.add_column("restore_data_version", job.restore_data_version_)) ||
               OB_FAIL(dml.add_column("backup_dest", job.backup_dest_)) ||
               OB_FAIL(dml.add_column("restore_option", job.restore_option_)) ||
               OB_FAIL(dml.add_column("info", job.info_)) ||
               OB_FAIL(dml.add_column("backup_cluster_id", job.cluster_id_)) ||
               OB_FAIL(dml.add_column("backup_cluster_name", job.backup_cluster_name_)) ||
               OB_FAIL(dml.add_column("backup_tenant_id", job.backup_tenant_id_)) ||
               OB_FAIL(dml.add_column("backup_tenant_name", job.backup_tenant_name_)) ||
               OB_FAIL(dml.add_column("white_list", ObHexEscapeSqlStr(white_list_str))) ||
               OB_FAIL(dml.add_column("backup_set_list", ObHexEscapeSqlStr(backup_set_list_str))) ||
               OB_FAIL(dml.add_column("backup_piece_list", ObHexEscapeSqlStr(backup_piece_list_str)))) {
      LOG_WARN("fail to add column", KR(ret), K(job));
    } else if (OB_FAIL(dml.splice_replace_sql(OB_ALL_RESTORE_HISTORY_TNAME, sql))) {
      LOG_WARN("splice_delete_sql failed", KR(ret), K(sql));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), KR(ret), K(sql));
    } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows is invalid", KR(ret), K(job), K(affected_rows), K(sql));
    }
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::remove_job(int64_t job_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", K(ret));
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    // remove from __all_restore_info
    if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
      LOG_WARN("failed to add pk column", K(ret), K(job_id));
    } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_RESTORE_INFO_TNAME, sql))) {
      LOG_WARN("splice_delete_sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
      // no need to check affected_rows
    }
    // remove from __all_restore_progress
    if (OB_SUCC(ret)) {
      sql.reset();
      if (OB_FAIL(dml.splice_delete_sql(OB_ALL_RESTORE_PROGRESS_TNAME, sql))) {
        LOG_WARN("splice_delete_sql failed", K(ret));
      } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      } else if (affected_rows <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update succeeded but affected_rows is not one", K(ret), K(affected_rows));
      }
    }
  }
  LOG_INFO("[RESTORE] remove job", K(ret), K(job_id));
  return ret;
}

int ObPhysicalRestoreTableOperator::get_restore_info(const uint64_t tenant_id, ObPhysicalRestoreInfo& restore_info)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreJob job_info;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_job_by_tenant_id(tenant_id, job_info))) {
    LOG_WARN("fail to get job", K(ret), K(tenant_id));
  } else if (OB_FAIL(job_info.copy_to(restore_info))) {
    LOG_WARN("fail to copy restore info from job info", K(ret), K(tenant_id), K(job_info));
  }
  return ret;
}

int ObPhysicalRestoreTableOperator::get_job_by_tenant_id(const uint64_t tenant_id, ObPhysicalRestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql proxy is null", K(ret));
    } else if (OB_INVALID_TENANT_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id in (SELECT job_id "
                                      "FROM %s WHERE name = 'tenant_id' AND value = '%lu')",
                   OB_ALL_RESTORE_INFO_TNAME,
                   OB_ALL_RESTORE_INFO_TNAME,
                   tenant_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      int64_t idx = 0;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        int64_t job_id = OB_INVALID_ID;
        EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
        if (OB_INVALID_ID != job_info.job_id_ && job_id != job_info.job_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant_id has multi restore job", K(ret), K(job_info), K(tenant_id));
        } else if (FALSE_IT(job_info.job_id_ = job_id)) {
        } else if (OB_FAIL(retrieve_restore_option(*result, job_info))) {
          LOG_WARN("fail to retrieve restore option", K(ret), K(job_info));
        }
      }
      if (OB_ITER_END == ret) {
        if (!job_info.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("job info is invalid", K(ret), K(tenant_id), K(job_info));
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

int ObPhysicalRestoreTableOperator::init_restore_progress(const ObPhysicalRestoreJob& job)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  common::ObSqlString sql;
  int64_t affected_rows = 0;
  const char* status_str = ObPhysicalRestoreTableOperator::get_restore_status_str(job.status_);
  int64_t invalid_cnt = OB_INVALID_COUNT;
  ObArenaAllocator allocator("PhyWhiteList");
  const ObPhysicalRestoreWhiteList& white_list = job.white_list_;
  ObString white_list_str;
  const ObPhysicalRestoreBackupDestList& dest_list = job.multi_restore_path_list_;
  ObString backup_set_list_str;
  ObString backup_piece_list_str;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job.job_id_))) {
    LOG_WARN("fail to add pk column", KR(ret), "job_id", job.job_id_);
  } else if (OB_ISNULL(status_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", KR(ret), "status", job.status_);
  } else if (OB_FAIL(white_list.get_format_str(allocator, white_list_str))) {
    LOG_WARN("fail to get format str", KR(ret), K(white_list));
  } else if (OB_FAIL(dest_list.get_backup_set_list_format_str(allocator, backup_set_list_str))) {
    LOG_WARN("fail to get format str", KR(ret), K(dest_list));
  } else if (OB_FAIL(dest_list.get_backup_piece_list_format_str(allocator, backup_piece_list_str))) {
    LOG_WARN("fail to get format str", KR(ret), K(dest_list));
  } else if (OB_FAIL(dml.add_column("external_job_id", job.restore_job_id_)) ||
             OB_FAIL(dml.add_column("tenant_id", job.tenant_id_)) ||
             OB_FAIL(dml.add_column("tenant_name", job.tenant_name_)) ||
             OB_FAIL(dml.add_column("status", status_str)) ||
             (job.restore_start_ts_ > 0 && OB_FAIL(dml.add_time_column("start_time", job.restore_start_ts_)))
             /* completion_time is null */
             || OB_FAIL(dml.add_column("pg_count", invalid_cnt)) ||
             OB_FAIL(dml.add_column("finish_pg_count", invalid_cnt)) ||
             OB_FAIL(dml.add_column("partition_count", invalid_cnt)) ||
             OB_FAIL(dml.add_column("finish_partition_count", invalid_cnt)) ||
             OB_FAIL(dml.add_column("macro_block_count", invalid_cnt)) ||
             OB_FAIL(dml.add_column("finish_macro_block_count", invalid_cnt)) ||
             (job.frozen_snapshot_version_ > 0 &&
                 OB_FAIL(dml.add_time_column("restore_start_timestamp", job.frozen_snapshot_version_))) ||
             (job.restore_timestamp_ > 0 &&
                 OB_FAIL(dml.add_time_column("restore_finish_timestamp", job.restore_timestamp_)))
             /*  restore_current_timestamp is null */
             || OB_FAIL(dml.add_column("info", "")) || OB_FAIL(dml.add_column("backup_cluster_id", job.cluster_id_)) ||
             OB_FAIL(dml.add_column("backup_cluster_name", job.backup_cluster_name_)) ||
             OB_FAIL(dml.add_column("backup_tenant_id", job.backup_tenant_id_)) ||
             OB_FAIL(dml.add_column("backup_tenant_name", job.backup_tenant_name_)) ||
             OB_FAIL(dml.add_column("white_list", ObHexEscapeSqlStr(white_list_str))) ||
             OB_FAIL(dml.add_column("backup_set_list", ObHexEscapeSqlStr(backup_set_list_str))) ||
             OB_FAIL(dml.add_column("backup_piece_list", ObHexEscapeSqlStr(backup_piece_list_str)))) {
    LOG_WARN("fail to add column", KR(ret), K(job));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_RESTORE_PROGRESS_TNAME, sql))) {
    LOG_WARN("splice_insert_sql failed", KR(ret), K(job));
  } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected_rows", KR(ret), K(sql), K(affected_rows));
  }
  LOG_TRACE("[RESTORE] init restore progress", KR(ret), K(job));
  return ret;
}

int ObPhysicalRestoreTableOperator::reset_restore_progress(const ObPhysicalRestoreJob& job)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  common::ObSqlString sql;
  int64_t affected_rows = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job.job_id_))) {
    LOG_WARN("fail to add pk column", KR(ret), "job_id", job.job_id_);
  } else if (OB_FAIL(dml.add_column("pg_count", 0)) || OB_FAIL(dml.add_column("finish_pg_count", 0)) ||
             OB_FAIL(dml.add_column("partition_count", 0)) || OB_FAIL(dml.add_column("finish_partition_count", 0))) {
    LOG_WARN("fail to add column", KR(ret), K(job));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_RESTORE_PROGRESS_TNAME, sql))) {
    LOG_WARN("splice_insert_sql failed", KR(ret));
  } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(sql));
  } else {
    // no need to check affected_rows
  }
  LOG_TRACE("[RESTORE] reset restore progress", KR(ret), K(job));
  return ret;
}

int ObPhysicalRestoreTableOperator::update_restore_progress(
    const ObPhysicalRestoreJob& job, const share::ObRestoreProgressInfo& statistic)
{
  int ret = OB_SUCCESS;
  common::ObSqlString sql;
  int64_t affected_rows = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", KR(ret));
  } else if (OB_FAIL(sql.append_fmt("UPDATE %s SET "
                                    "pg_count = (CASE WHEN %ld > pg_count "
                                    "THEN %ld ELSE pg_count END), "
                                    "finish_pg_count = (CASE WHEN %ld > finish_pg_count "
                                    "THEN %ld ELSE finish_pg_count END), "
                                    "partition_count = (CASE WHEN %ld > partition_count "
                                    "THEN %ld ELSE partition_count END), "
                                    "finish_partition_count = (CASE WHEN %ld > finish_partition_count "
                                    "THEN %ld ELSE finish_partition_count END) "
                                    "WHERE job_id = %ld",
                 OB_ALL_RESTORE_PROGRESS_TNAME,
                 statistic.total_pg_cnt_,
                 statistic.total_pg_cnt_,
                 statistic.finish_pg_cnt_,
                 statistic.finish_pg_cnt_,
                 statistic.total_partition_cnt_,
                 statistic.total_partition_cnt_,
                 statistic.finish_partition_cnt_,
                 statistic.finish_partition_cnt_,
                 job.job_id_))) {
    LOG_WARN("gen sql failed", KR(ret), K(statistic));
  } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(sql));
  } else {
    // no need to check affected_rows
  }
  LOG_TRACE("[RESTORE] update restore progress", KR(ret), K(job), K(statistic));
  return ret;
}

int ObPhysicalRestoreTableOperator::get_restore_progress_statistic(
    const ObPhysicalRestoreJob& job, ObRestoreProgressInfo& statistic)
{
  int ret = OB_SUCCESS;
  statistic.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical restore table operator not init", KR(ret));
  } else if (job.job_id_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_id", KR(ret), K(job));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res)
    {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld", OB_ALL_RESTORE_PROGRESS_TNAME, job.job_id_))) {
        LOG_WARN("failed to assign sql", KR(ret), K(job));
      } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to iter next row", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "pg_count", statistic.total_pg_cnt_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_pg_count", statistic.finish_pg_cnt_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "partition_count", statistic.total_partition_cnt_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_partition_count", statistic.finish_partition_cnt_, int64_t);
        if (FAILEDx(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to iter next row", KR(ret), K(sql));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should be only one row", KR(ret), K(job), K(sql));
        }
      }
    }
  }
  LOG_TRACE("[RESTORE] get restore progress", KR(ret), K(job), K(statistic));
  return ret;
}

ObColumnStatisticRowKey::ObColumnStatisticRowKey()
    : tenant_id_(OB_INVALID_ID), table_id_(OB_INVALID_ID), partition_id_(-1), column_id_(-1)
{}

void ObColumnStatisticRowKey::reuse()
{
  tenant_id_ = 0;
  table_id_ = 0;
  partition_id_ = 0;
  column_id_ = 0;
}

void ObColumnStatisticRowKey::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  partition_id_ = -1;
  column_id_ = -1;
}

ObColumnStatistic::ObColumnStatistic()
    : row_key_(), num_distinct_(), num_null_(), llc_bitmap_size_(), version_(), last_rebuild_version_()
{}

/* ObColumnStatisticOperator */

ObColumnStatisticOperator::ObColumnStatisticOperator() : is_inited_(false), sql_client_(NULL)
{}

int ObColumnStatisticOperator::init(common::ObISQLClient *sql_client)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("physical restore table operator init twice", K(ret));
  } else if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret));
  } else {
    sql_client_ = sql_client;
    is_inited_ = true;
  }
  return ret;
}

int ObColumnStatisticOperator::update_column_statistic_version(const uint64_t tenant_id, const int64_t version)
{
  int ret = OB_SUCCESS;
  ObArray<ObColumnStatisticRowKey> row_key_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(version));
  } else if (OB_FAIL(get_batch_end_key_for_update_(tenant_id, row_key_list))) {
    LOG_WARN("failed to get batch end key for remove", KR(ret), K(tenant_id));
  } else if (1 == row_key_list.count()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_key_list.count() - 1; ++i) {
      const ObColumnStatisticRowKey &left_row_key = row_key_list.at(i);
      const ObColumnStatisticRowKey &right_row_key = row_key_list.at(i + 1);
      if (OB_FAIL(batch_update_column_statistic_version_(tenant_id, version, left_row_key, right_row_key))) {
        LOG_WARN(
            "failed to batch remove pg tasks", KR(ret), K(tenant_id), K(version), K(left_row_key), K(right_row_key));
      }
    }
  }
  return ret;
}

int ObColumnStatisticOperator::get_next_end_key_for_update_(
    const uint64_t tenant_id, const ObColumnStatisticRowKey &prev_row_key, ObColumnStatisticRowKey &next_row_key)
{
  int ret = OB_SUCCESS;
  next_row_key.reset();
  ObSqlString sql;
  ObArray<ObColumnStatistic> stat_list;
  const int64_t BATCH_SIZE = 1024;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s", OB_ALL_COLUMN_STATISTIC_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(" WHERE (tenant_id, table_id, partition_id, column_id) > (%lu, %ld, %ld, %ld)",
                 prev_row_key.tenant_id_,
                 prev_row_key.table_id_,
                 prev_row_key.partition_id_,
                 prev_row_key.column_id_))) {
    LOG_WARN("failed to append sql", KR(ret), K(prev_row_key));
  } else if (OB_FAIL(sql.append_fmt(" ORDER BY tenant_id, table_id, partition_id, column_id"
                                    " LIMIT %ld ",
                 BATCH_SIZE))) {
    LOG_WARN("failed to append sql", KR(ret));
  } else if (OB_FAIL(get_column_statistic_items_(tenant_id, sql, stat_list))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant_id), K(sql));
  } else if (stat_list.empty()) {
    ret = OB_ITER_END;
    LOG_WARN("no next end row key", KR(ret), K(sql), K(tenant_id));
  } else {
    const ObColumnStatistic &last_item = stat_list.at(stat_list.count() - 1);
    next_row_key.tenant_id_ = last_item.row_key_.tenant_id_;
    next_row_key.table_id_ = last_item.row_key_.table_id_;
    next_row_key.partition_id_ = last_item.row_key_.partition_id_;
    next_row_key.column_id_ = last_item.row_key_.column_id_;
  }
  return ret;
}

int ObColumnStatisticOperator::get_batch_end_key_for_update_(
    const uint64_t tenant_id, common::ObIArray<ObColumnStatisticRowKey> &row_key_list)
{
  int ret = OB_SUCCESS;
  row_key_list.reset();
  ObColumnStatisticRowKey prev_row_key;
  ObColumnStatisticRowKey next_row_key;
  prev_row_key.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(row_key_list.push_back(prev_row_key))) {
    LOG_WARN("failed to push back", KR(ret), K(prev_row_key));
  } else {
    do {
      next_row_key.reset();
      if (OB_FAIL(get_next_end_key_for_update_(tenant_id, prev_row_key, next_row_key))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next end key for remove", KR(ret), K(prev_row_key));
        }
      } else if (OB_FAIL(row_key_list.push_back(next_row_key))) {
        LOG_WARN("failed to push back", KR(ret), K(next_row_key));
      } else {
        prev_row_key = next_row_key;
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

int ObColumnStatisticOperator::batch_update_column_statistic_version_(const uint64_t tenant_id, const int64_t version,
    const ObColumnStatisticRowKey &left_row_key, const ObColumnStatisticRowKey &right_row_key)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET version = %ld WHERE ", OB_ALL_COLUMN_STATISTIC_TNAME, version))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.append_fmt("(tenant_id, table_id, partition_id, column_id) > (%lu, %ld, %ld, %ld)",
                 left_row_key.tenant_id_,
                 left_row_key.table_id_,
                 left_row_key.partition_id_,
                 left_row_key.column_id_))) {
    LOG_WARN("failed to append sql", KR(ret), K(left_row_key));
  } else if (OB_FAIL(sql.append_fmt(" AND "))) {
    LOG_WARN("failed to append sql", KR(ret));
  } else if (OB_FAIL(sql.append_fmt("(tenant_id, table_id, partition_id, column_id) <= (%lu, %ld, %ld, %ld)",
                 right_row_key.tenant_id_,
                 right_row_key.table_id_,
                 right_row_key.partition_id_,
                 right_row_key.column_id_))) {
    LOG_WARN("failed to append sql", KR(ret), K(right_row_key));
  } else if (OB_FAIL(sql_client_->write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(sql));
  }
  return ret;
}

int ObColumnStatisticOperator::get_column_statistic_items_(
    const uint64_t tenant_id, const common::ObSqlString &sql, common::ObIArray<ObColumnStatistic> &stat_list)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", KR(ret), K(sql));
    } else if (OB_FAIL(sql_client_->read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", KR(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObColumnStatistic item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", KR(ret));
          }
        } else if (OB_FAIL(extract_stat_item_(result, item))) {
          LOG_WARN("failed to extract item", KR(ret), K(item));
        } else if (OB_FAIL(stat_list.push_back(item))) {
          LOG_WARN("failed to push back item", KR(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObColumnStatisticOperator::extract_stat_item_(sqlclient::ObMySQLResult *result, ObColumnStatistic &item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract task item get invalid argument", KR(ret), KP(result));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.row_key_.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "table_id", item.row_key_.table_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", item.row_key_.partition_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "column_id", item.row_key_.column_id_, int64_t);
  }
  return ret;
}
