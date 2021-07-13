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

#define USING_LOG_PREFIX RS_RESTORE

#include "ob_restore_table_operator.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_kv_parser.h"
#include "rootserver/restore/ob_restore_info.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObSchemaMapSerializer::serialize(
    ObIAllocator& allocator, const common::ObIArray<ObSchemaIdPair>& id_pairs, common::ObString& schema_map_str)
{
  int ret = OB_SUCCESS;
  int64_t size = id_pairs.count() * 2 * (20 + 2);
  int64_t pos = 0;
  char* buf = NULL;
  if (0 >= size) {
    // do nothing
  } else if (NULL == (buf = static_cast<char*>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Out of memory", K(size), K(ret));
  } else {
    ARRAY_FOREACH_X(id_pairs, idx, cnt, OB_SUCC(ret))
    {
      if ((pos += snprintf(
               buf + pos, size - pos, "%lu:%lu", id_pairs.at(idx).schema_id_, id_pairs.at(idx).backup_schema_id_)) >
          size) {
        ret = OB_BUF_NOT_ENOUGH;
      } else if (idx == cnt - 1) {
        // finish, don't print ',' for last pair
      } else if ((pos += snprintf(buf + pos, size - pos, ",")) > size) {
        ret = OB_BUF_NOT_ENOUGH;
      }
    }
    if (OB_SUCC(ret)) {
      schema_map_str.assign(buf, static_cast<int32_t>(pos));
    }
  }
  return ret;
}

class SchemaMapParserCb : public ObKVMatchCb {
public:
  SchemaMapParserCb(ObIArray<ObSchemaIdPair>& id_pair) : id_pair_(id_pair)
  {}
  virtual ~SchemaMapParserCb()
  {}
  virtual int match(const char* key, const char* value)
  {
    int ret = OB_SUCCESS;
    int64_t schema_id = 0;
    int64_t backup_schema_id = 0;
    if (OB_FAIL(get_int_val(key, schema_id))) {
      LOG_WARN("fail get int val", K(key), K(ret));
    } else if (OB_FAIL(get_int_val(value, backup_schema_id))) {
      LOG_WARN("fail get int val", K(value), K(ret));
    } else {
      ObSchemaIdPair pair(static_cast<uint64_t>(schema_id), static_cast<uint64_t>(backup_schema_id));
      if (OB_FAIL(id_pair_.push_back(pair))) {
        LOG_WARN("fail push pair", K(pair), K(ret));
      }
    }
    return ret;
  }

private:
  int get_int_val(const char* nptr, int64_t& int_val)
  {
    int ret = OB_SUCCESS;
    char* end_ptr = NULL;
    int64_t ret_val = strtoll(nptr, &end_ptr, 10);
    if (ERANGE == errno) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid int value", K(nptr), K(ret));
    } else if (*nptr != '\0' && *end_ptr == '\0') {
      int_val = ret_val;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid int value", K(nptr), K(ret));
    }
    return ret;
  }

private:
  ObIArray<ObSchemaIdPair>& id_pair_;
};

int ObSchemaMapSerializer::deserialize(
    const common::ObString& schema_map_str, common::ObIArray<ObSchemaIdPair>& id_pair)
{
  int ret = OB_SUCCESS;
  SchemaMapParserCb cb(id_pair);
  ObKVParser map_parser(':', ',');
  map_parser.set_match_callback(cb);
  map_parser.set_allow_space(false);
  if (OB_FAIL(map_parser.parse(schema_map_str.ptr(), schema_map_str.length()))) {
    LOG_WARN("fail deserialize schema map", K(schema_map_str), K(ret));
  }
  return ret;
}

ObRestoreTableOperator::ObRestoreTableOperator() : inited_(false), sql_client_(NULL)
{}

int ObRestoreTableOperator::init(common::ObISQLClient* sql_client)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    sql_client_ = sql_client;
    inited_ = true;
  }
  return ret;
}

int ObRestoreTableOperator::insert_job(const RestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  RestoreJob tmp_job;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(tmp_job.assign(job_info))) {
    LOG_WARN("fail to assign job_info", K(ret), K(job_info));
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    tmp_job.backup_end_time_ = 0;
    tmp_job.recycle_end_time_ = 0;
    if (OB_FAIL(fill_dml_splicer(dml, tmp_job))) {
      LOG_WARN("fail to fill dml splicer", K(ret), K(tmp_job));
    } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_RESTORE_JOB_TNAME, sql))) {
      LOG_WARN("splice_insert_sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert succeeded but affected_rows is not one", K(ret), K(affected_rows));
    } else {
      LOG_INFO("restore job insert", K(job_info), "sql", sql.ptr());
    }
  }

  return ret;
}

int ObRestoreTableOperator::fill_dml_splicer(share::ObDMLSqlSplicer& dml, const RestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(dml.add_gmt_create(now)) || OB_FAIL(dml.add_gmt_modified(now))) {
    LOG_WARN("failed to add gmt time", K(ret), K(now));
  } else if (OB_FAIL(dml.add_column("job_id", job_info.job_id_))) {
    LOG_WARN("failed to add column", K(ret), K(job_info));
  } else if (OB_FAIL(dml.add_column("start_time", job_info.start_time_))) {
    LOG_WARN("failed to add column", K(ret), K(job_info));
  } else if (OB_FAIL(dml.add_column("backup_uri", job_info.backup_uri_))) {
    LOG_WARN("failed to add column", K(ret), K(job_info));
  } else if (OB_FAIL(dml.add_column("backup_end_time", job_info.backup_end_time_))) {
    LOG_WARN("failed to add column", K(ret), K(job_info));
  } else if (OB_FAIL(dml.add_column("recycle_end_time", job_info.recycle_end_time_))) {
    LOG_WARN("failed to add column", K(ret), K(job_info));
  } else if (OB_FAIL(dml.add_column("level", job_info.level_))) {
    LOG_WARN("failed to add column", K(ret), K(job_info));
  } else if (OB_FAIL(dml.add_column("status", job_info.status_))) {
    LOG_WARN("failed to add column", K(ret), K(job_info));
  } else if (OB_FAIL(dml.add_column("tenant_name", job_info.tenant_name_))) {
    LOG_WARN("failed to add column", K(ret), K(job_info));
  }
  return ret;
}

int ObRestoreTableOperator::get_jobs(common::ObIArray<RestoreJob>& jobs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s ORDER BY job_id", OB_ALL_RESTORE_JOB_TNAME))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      RestoreJob job_info;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        if (OB_FAIL(cons_job_info(*result, job_info))) {
          LOG_WARN("failed to construct job info", K(ret));
        } else if (OB_FAIL(jobs.push_back(job_info))) {
          LOG_WARN("fail push back job_info", K(job_info));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get jobs fail", K(ret));
      }
    }
  }
  return ret;
}

int ObRestoreTableOperator::get_job(const int64_t job_id, RestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
    } else if (job_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job_id", K(ret), K(job_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld", OB_ALL_RESTORE_JOB_TNAME, job_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      int job_cnt = 0;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        if (++job_cnt > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("job count should be 1 or 0", K(job_cnt), K(ret));
        } else if (OB_FAIL(cons_job_info(*result, job_info))) {
          LOG_WARN("failed to construct job info", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        if (0 == job_cnt) {
          ret = OB_ENTRY_NOT_EXIST;
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

int ObRestoreTableOperator::get_job_count(int64_t& job_count)
{
  int ret = OB_SUCCESS;
  ObSEArray<RestoreJob, 1> jobs;
  if (OB_FAIL(get_jobs(jobs))) {
    LOG_WARN("fail get jobs", K(ret));
  } else {
    job_count = jobs.count();
  }
  return ret;
}

int ObRestoreTableOperator::cons_job_info(const sqlclient::ObMySQLResult& res, RestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  ////////////////
  // required fields:
  ////////////////
  EXTRACT_INT_FIELD_MYSQL(res, "job_id", job_info.job_id_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(res, "tenant_name", job_info.tenant_name_);
  EXTRACT_INT_FIELD_MYSQL(res, "start_time", job_info.start_time_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "backup_end_time", job_info.backup_end_time_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "recycle_end_time", job_info.recycle_end_time_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "status", job_info.status_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "level", job_info.level_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(res, "backup_uri", job_info.backup_uri_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(job_info.assign(job_info))) {  // deep copy
      LOG_INFO("failed to deep copy job info itself", K(ret));
    }
  }
  return ret;
}

int ObRestoreTableOperator::update_job_status(int64_t job_id, int64_t status)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer pairs;
  if (OB_FAIL(pairs.add_column("status", status))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(update_job(job_id, pairs))) {
    LOG_WARN("failed to update job", K(ret), K(job_id));
  }
  LOG_INFO("[RESTORE] update job status", K(ret), K(job_id), K(status));
  return ret;
}

int ObRestoreTableOperator::update_job(int64_t job_id, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  }

  if (OB_SUCC(ret)) {
    const int64_t now = ObTimeUtility::current_time();
    if (OB_FAIL(dml.add_gmt_modified(now))) {
      LOG_WARN("failed to add gmt time", K(ret), K(now));
    } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
      LOG_WARN("failed to add pk column", K(ret), K(job_id));
    }
  }

  if (OB_SUCC(ret)) {
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.splice_update_sql(OB_ALL_RESTORE_JOB_TNAME, sql))) {
      LOG_WARN("splice_update_sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update succeeded but affected_rows is not one", K(ret), K(affected_rows));
    } else {
      LOG_INFO("restore job updated", "sql", sql.ptr());
    }
  }

  return ret;
}

int ObRestoreTableOperator::insert_task(const PartitionRestoreTask& task_info)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;

  if (!inited_) {
    ret = OB_NOT_INIT;
  }

  if (OB_SUCC(ret)) {
    const int64_t now = ObTimeUtility::current_time();
    int64_t job_id = task_info.job_id_;
    int64_t start_time = task_info.start_time_;
    int64_t tenant_id = task_info.tenant_id_;
    int64_t table_id = task_info.table_id_;
    int64_t backup_table_id = task_info.backup_table_id_;
    int64_t partition_id = task_info.partition_id_;
    int64_t status = task_info.status_;

    ObArenaAllocator allocator;
    ObString schema_map_str;
    if (OB_FAIL(ObSchemaMapSerializer::serialize(allocator, task_info.schema_id_pairs_, schema_map_str))) {
      LOG_WARN("fail serialize schema id pair", K(ret));
    }

    if (OB_FAIL(ret)) {
      // nop
    } else if (OB_FAIL(dml.add_gmt_create(now)) || OB_FAIL(dml.add_gmt_modified(now))) {
      LOG_WARN("failed to add gmt time", K(ret), K(now));
    } else if (OB_FAIL(dml.add_column("job_id", job_id))) {
      LOG_WARN("failed to add column", K(ret), K(job_id));
    } else if (OB_FAIL(dml.add_column("start_time", start_time))) {
      LOG_WARN("failed to add column", K(ret), K(start_time));
    } else if (OB_FAIL(dml.add_column("tenant_id", tenant_id))) {
      LOG_WARN("failed to add column", K(ret), K(tenant_id));
    } else if (OB_FAIL(dml.add_column("backup_table_id", backup_table_id))) {
      LOG_WARN("failed to add column", K(ret), K(backup_table_id));
    } else if (OB_FAIL(dml.add_column("table_id", table_id))) {
      LOG_WARN("failed to add column", K(ret), K(table_id));
    } else if (OB_FAIL(dml.add_column("index_map", schema_map_str))) {
      LOG_WARN("failed to add column", K(ret), K(schema_map_str));
    } else if (OB_FAIL(dml.add_column("partition_id", partition_id))) {
      LOG_WARN("failed to add column", K(ret), K(partition_id));
    } else if (OB_FAIL(dml.add_column("status", status))) {
      LOG_WARN("failed to add column", K(ret), K(status));
    }
  }

  if (OB_SUCC(ret)) {
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.splice_insert_sql(OB_ALL_RESTORE_TASK_TNAME, sql))) {
      LOG_WARN("splice_insert_sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert succeeded but affected_rows is not one", K(ret), K(affected_rows));
    } else {
      LOG_INFO("restore task insert", K(task_info), "sql", sql.ptr());
    }
  }

  return ret;
}

int ObRestoreTableOperator::insert_task(const common::ObIArray<PartitionRestoreTask>& task_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    // TODO: should insert multi rows in one sql
    for (int64_t i = 0; OB_SUCC(ret) && i < task_info.count(); ++i) {
      if (OB_FAIL(insert_task(task_info.at(i)))) {
        LOG_WARN("fail insert task", K(ret));
      }
    }
  }
  return ret;
}

int ObRestoreTableOperator::get_tasks(common::ObIArray<PartitionRestoreTask>& tasks)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s", OB_ALL_RESTORE_TASK_TNAME))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      PartitionRestoreTask task_info;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        if (OB_FAIL(cons_task_info(*result, task_info))) {
          LOG_WARN("failed to construct task info", K(ret));
        } else if (OB_FAIL(tasks.push_back(task_info))) {
          LOG_WARN("fail push back task_info", K(task_info));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get jobs fail", K(ret));
      }
    }
  }
  return ret;
}

int ObRestoreTableOperator::get_tasks(int64_t job_id, common::ObIArray<PartitionRestoreTask>& tasks)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld", OB_ALL_RESTORE_TASK_TNAME, job_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      PartitionRestoreTask task_info;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        if (OB_FAIL(cons_task_info(*result, task_info))) {
          LOG_WARN("failed to construct task info", K(ret));
        } else if (OB_FAIL(tasks.push_back(task_info))) {
          LOG_WARN("fail push back task_info", K(task_info));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get jobs fail", K(ret));
      }
    }
  }
  return ret;
}

int ObRestoreTableOperator::cons_task_info(const sqlclient::ObMySQLResult& res, PartitionRestoreTask& task_info)
{
  int ret = OB_SUCCESS;
  ObString schema_map;
  ////////////////
  // required fields:
  ////////////////
  EXTRACT_INT_FIELD_MYSQL(res, "job_id", task_info.job_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "start_time", task_info.start_time_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "status", task_info.status_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "tenant_id", task_info.tenant_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "backup_table_id", task_info.backup_table_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "table_id", task_info.table_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "partition_id", task_info.partition_id_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(res, "index_map", schema_map);

  if (OB_SUCC(ret)) {
    task_info.schema_id_pairs_.reset();
    if (!schema_map.empty() && OB_FAIL(ObSchemaMapSerializer::deserialize(schema_map, task_info.schema_id_pairs_))) {
      LOG_WARN("fail serialize schema id pair", K(ret));
    }
  }
  return ret;
}

int ObRestoreTableOperator::update_task_status(int64_t tenant_id, int64_t table_id, int64_t part_id, int64_t status)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer pairs;
  if (OB_FAIL(pairs.add_column("status", status))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(update_task(tenant_id, table_id, part_id, pairs))) {
    LOG_WARN("failed to update task", K(ret), K(tenant_id), K(table_id), K(part_id));
  }
  return ret;
}

int ObRestoreTableOperator::update_task(
    int64_t tenant_id, int64_t table_id, int64_t partition_id, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  }

  if (OB_SUCC(ret)) {
    const int64_t now = ObTimeUtility::current_time();
    if (OB_FAIL(dml.add_gmt_modified(now))) {
      LOG_WARN("failed to add gmt time", K(ret), K(now));
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))) {
      LOG_WARN("failed to add pk column", K(ret), K(tenant_id));
    } else if (OB_FAIL(dml.add_pk_column("table_id", table_id))) {
      LOG_WARN("failed to add pk column", K(ret), K(table_id));
    } else if (OB_FAIL(dml.add_pk_column("partition_id", partition_id))) {
      LOG_WARN("failed to add pk column", K(ret), K(partition_id));
    }
  }

  if (OB_SUCC(ret)) {
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.splice_update_sql(OB_ALL_RESTORE_TASK_TNAME, sql))) {
      LOG_WARN("splice_insert_sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update succeeded but affected_rows is not one", K(ret), K(affected_rows));
    } else {
      LOG_INFO("restore task updated", "sql", sql.ptr());
    }
  }

  return ret;
}

int ObRestoreTableOperator::recycle_job(int64_t job_id, int64_t status)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    if (OB_FAIL(update_job_status(job_id, status))) {
      LOG_WARN("mark job done fail", K(job_id), K(ret));
    } else if (OB_FAIL(remove_tasks(job_id))) {
      LOG_WARN("remove task fail", K(job_id), K(ret));
    } else if (OB_FAIL(record_job_in_history(job_id))) {
      LOG_WARN("fail move job to history table", K(job_id), K(ret));
    } else if (OB_FAIL(remove_job(job_id))) {
      LOG_WARN("fail remove job", K(job_id), K(ret));
    }
  }
  return ret;
}

int ObRestoreTableOperator::record_job_in_history(int64_t job_id)
{
  int ret = OB_SUCCESS;
  RestoreJob job_info;
  share::ObDMLSqlSplicer dml;
  common::ObSqlString sql;
  int64_t affected_rows = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_job(job_id, job_info))) {
    LOG_WARN("fail to get job", K(ret), K(job_id));
  } else if (OB_FAIL(fill_dml_splicer(dml, job_info))) {
    LOG_WARN("fail to fill dml splicer", K(ret), K(job_info));
  } else if (OB_FAIL(dml.splice_replace_sql(OB_ALL_RESTORE_JOB_HISTORY_TNAME, sql))) {
    LOG_WARN("splice_replace_sql failed", K(ret));
  } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replace succeeded but affected_rows is not one", K(ret), K(affected_rows));
  } else {
    LOG_INFO("reocrd restore job in history table", K(job_info), "sql", sql.ptr());
  }
  return ret;
}

int ObRestoreTableOperator::remove_job(int64_t job_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
      LOG_WARN("failed to add pk column", K(ret), K(job_id));
    } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_RESTORE_JOB_TNAME, sql))) {
      LOG_WARN("splice_delete_sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update succeeded but affected_rows is not one", K(ret), K(affected_rows));
    } else {
      LOG_INFO("remove job", "sql", sql.ptr());
    }
  }
  return ret;
}

int ObRestoreTableOperator::remove_tasks(int64_t job_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {  // job_id is not pk, cheat dml
      LOG_WARN("failed to add pk column", K(ret), K(job_id));
    } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_RESTORE_TASK_TNAME, sql))) {
      LOG_WARN("splice_delete_sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
      LOG_INFO("remove tasks", K(affected_rows), "sql", sql.ptr());
    }
  }

  return ret;
}
