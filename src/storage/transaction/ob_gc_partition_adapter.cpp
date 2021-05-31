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

#include "ob_gc_partition_adapter.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/ob_schema_status_proxy.h"
#include "share/schema/ob_schema_utils.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "lib/thread/ob_thread_name.h"

namespace oceanbase {
using namespace common;
using namespace obsys;

namespace transaction {

int ObGCPartitionAdapter::init(ObMySQLProxy* sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObGCPartitionAdapter inited twice", KR(ret));
  } else if (NULL == sql_proxy) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(sql_proxy));
  } else {
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  if (OB_SUCCESS == ret) {
    TRANS_LOG(INFO, "ObGCPartitionAdapter inited success", KP(this), KP(sql_proxy));
  } else {
    TRANS_LOG(WARN, "ObGCPartitionAdapter inited failed", KR(ret), KP(this), KP(sql_proxy));
  }
  return ret;
}

int ObGCPartitionAdapter::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObGCPartitionAdapter is not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ObGCPartitionAdapter is already running", KR(ret));
  } else if (OB_FAIL(share::ObThreadPool::start())) {
  } else {
    is_running_ = true;
  }
  if (OB_SUCCESS == ret) {
    TRANS_LOG(INFO, "ObGCPartitionAdapter start success");
  } else {
    TRANS_LOG(ERROR, "ObGCPartitionAdapter start failed", KR(ret));
  }
  return ret;
}

void ObGCPartitionAdapter::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObGCPartitionAdapter is not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObGCPartitionAdapter already has been stopped", KR(ret));
  } else {
    (void)share::ObThreadPool::stop();
    is_running_ = false;
    TRANS_LOG(INFO, "ObGCPartitionAdapter stop success");
  }
}

void ObGCPartitionAdapter::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObGCPartitionAdapter is not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ObGCPartitionAdapter is running", KR(ret));
  } else {
    (void)share::ObThreadPool::wait();
    TRANS_LOG(INFO, "ObGCPartitionAdapter wait success");
  }
}

void ObGCPartitionAdapter::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    is_inited_ = false;
    TRANS_LOG(INFO, "ObGCPartitionAdapter destroyed");
  }
}

void ObGCPartitionAdapter::run1()
{
  const int64_t RUN_INTERVAL = 50 * 1000;
  lib::set_thread_name("GCPartAdpt");
  for (int64_t loop = 0; !has_set_stop(); loop++) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(refresh_all_partition_status_())) {
      TRANS_LOG(WARN, "refresh partition status failed", KR(ret));
    }
    // rewrite ret
    ret = OB_SUCCESS;
    if (OB_FAIL(clear_obsolete_partition_())) {
      TRANS_LOG(WARN, "clear obsolete partition failed", KR(ret));
    }
    usleep(RUN_INTERVAL);
  }
}

int ObGCPartitionAdapter::check_partition_exist(const ObPartitionKey& pkey, bool& exist)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "not running", KR(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (is_sys_table(pkey.get_table_id())) {
    // System tables are not allowed to be deleted, so they always return a result of exist
    exist = true;
  } else {
    int status = GC_STATUS_UNKNOWN;
    GCInfo* info = NULL;
    if (OB_FAIL(gc_map_.get(pkey, info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "get gc partition info failed", KR(ret), K(pkey));
      } else if (NULL == (info = op_alloc(GCInfo))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc memory failed", KR(ret), K(pkey));
      } else if (OB_FAIL(gc_map_.insert_and_get(pkey, info))) {
        op_free(info);
        info = NULL;
        if (OB_ENTRY_EXIST != ret) {
          TRANS_LOG(WARN, "insert gc partition failed", KR(ret), K(pkey));
        } else {
          // If the partition already exists, simply ask the caller to try again
          // rewrite ret
          ret = OB_EAGAIN;
        }
      } else {
        gc_map_.revert(info);
        ret = OB_EAGAIN;
      }
    } else if (NULL == info) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected error, info is NULL", KR(ret), K(pkey));
    } else {
      status = info->get_status();
      info->update_access_ts();
      (void)gc_map_.revert(info);
      if (GC_STATUS_UNKNOWN == status) {
        // rewrite ret
        ret = OB_EAGAIN;
      } else if (GC_STATUS_EXIST == status) {
        exist = true;
      } else if (GC_STATUS_NOT_EXIST == status) {
        exist = false;
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected error, unknown status", KR(ret), K(status));
      }
    }
  }
  return ret;
}

ObGCPartitionAdapter& ObGCPartitionAdapter::get_instance()
{
  static ObGCPartitionAdapter gc_partition_adapter;
  return gc_partition_adapter;
}

int ObGCPartitionAdapter::refresh_all_partition_status_()
{
  int ret = OB_SUCCESS;
  ObSArray<ObPartitionKey> total_pkey_array;
  // TODO. sort unknown partition array, make batch query more effective.
  if (OB_FAIL(get_unknown_partition_array_(total_pkey_array))) {
    TRANS_LOG(WARN, "get unknown partition array failed", KR(ret));
  } else if (0 == total_pkey_array.count()) {
    // do nothing
  } else {
    ObSArray<ObPartitionKey> pkey_array;
    for (int64_t i = 0; OB_SUCCESS == ret && i < total_pkey_array.count(); i++) {
      const ObPartitionKey& pkey = total_pkey_array.at(i);
      if (pkey_array.count() > 0 && pkey_array.at(0).get_tenant_id() != pkey.get_tenant_id()) {
        if (OB_FAIL(refresh_partition_status_(pkey_array))) {
          TRANS_LOG(WARN, "refresh partition status failed", KR(ret), K(pkey_array));
        } else {
          pkey_array.reset();
        }
      }
      if (OB_SUCCESS == ret) {
        if (OB_FAIL(pkey_array.push_back(pkey))) {
          TRANS_LOG(WARN, "push back partition failed", KR(ret), K(pkey));
        } else if ((i == total_pkey_array.count() - 1) || (pkey_array.count() >= BATCH_GC_PARTITION_QUERY)) {
          if (OB_FAIL(refresh_partition_status_(pkey_array))) {
            TRANS_LOG(WARN, "refresh partition status failed", KR(ret), K(pkey_array));
          } else {
            pkey_array.reset();
          }
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObGCPartitionAdapter::refresh_partition_status_(const ObIArray<ObPartitionKey>& pkey_array)
{
  int ret = OB_SUCCESS;
  ObSArray<int> status_array;
  if (0 >= pkey_array.count()) {
    // do nothing
  } else if (OB_FAIL(query_status_(pkey_array, status_array))) {
    TRANS_LOG(WARN, "query status failed", KR(ret), K(pkey_array));
  } else if (OB_FAIL(update_partition_status_(pkey_array, status_array))) {
    TRANS_LOG(WARN, "update partition status failed", KR(ret), K(pkey_array), K(status_array));
  } else {
    // do nothing
  }
  return ret;
}

int ObGCPartitionAdapter::clear_obsolete_partition_()
{
  int ret = OB_SUCCESS;
  RemoveObsoletePartition functor;
  if (OB_FAIL(gc_map_.remove_if(functor))) {
    TRANS_LOG(WARN, "check if partition status can be remove failed", KR(ret));
  }
  return ret;
}

int ObGCPartitionAdapter::get_unknown_partition_array_(ObIArray<ObPartitionKey>& array)
{
  int ret = OB_SUCCESS;
  array.reset();
  GetUnknownPartition functor(array);
  if (OB_FAIL(gc_map_.for_each(functor))) {
    TRANS_LOG(WARN, "for each unknown partition failed", KR(ret));
  }
  return ret;
}

int ObGCPartitionAdapter::query_status_(const ObIArray<ObPartitionKey>& pkey_array, ObIArray<int>& status_array)
{
  int ret = OB_SUCCESS;
  if (0 >= pkey_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey_array));
  } else {
    status_array.reset();
    for (int64_t i = 0; OB_SUCCESS == ret && i < pkey_array.count(); i++) {
      // set partition default status not exist
      if (OB_FAIL(status_array.push_back(static_cast<int>(GC_STATUS_NOT_EXIST)))) {
        TRANS_LOG(WARN, "push status failed", KR(ret));
      }
    }
  }

  if (OB_SUCCESS == ret) {
    const uint64_t tenant_id = pkey_array.at(0).get_tenant_id();
    ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, tenant_id, share::OB_ALL_TENANT_GC_PARTITION_INFO_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      ObSqlString sql;
      if (OB_FAIL(fill_sql_(sql, pkey_array))) {
        TRANS_LOG(WARN, "fill sql failed", KR(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
        TRANS_LOG(WARN, "execute sql failed", KR(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "fail to get result", KR(ret), K(sql));
      } else {
        while (OB_SUCCESS == ret && OB_SUCCESS == (ret = result->next())) {
          int64_t tmp_tenant_id = tenant_id;
          int64_t tmp_table_id = 0;
          int64_t tmp_partition_id = 0;
          if (OB_FAIL(result->get_int(1, tmp_table_id))) {
          } else if (OB_FAIL(result->get_int(2, tmp_partition_id))) {
          } else {
            tmp_table_id = static_cast<int64_t>(combine_id(tenant_id, tmp_table_id));
            for (int64_t i = 0; i < pkey_array.count(); i++) {
              const ObPartitionKey& pkey = pkey_array.at(i);
              if (pkey.get_tenant_id() == tmp_tenant_id && pkey.get_table_id() == tmp_table_id &&
                  pkey.get_partition_id() == tmp_partition_id) {
                status_array.at(i) = GC_STATUS_EXIST;
              }
            }
          }
        }
        if (OB_ITER_END == ret) {
          // rewrite ret
          ret = OB_SUCCESS;
        }
      }
      if (REACH_TIME_INTERVAL(3 * 1000 * 1000)) {
        TRANS_LOG(INFO, "query gc partition status", KR(ret), K(sql), K(pkey_array), K(status_array));
      }
    }
  }
  return ret;
}

int ObGCPartitionAdapter::fill_sql_(ObSqlString& sql, const ObIArray<ObPartitionKey>& pkey_array)
{
  int ret = OB_SUCCESS;
  if (0 >= pkey_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey_array));
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < pkey_array.count(); i++) {
      const ObPartitionKey& pkey = pkey_array.at(i);
      const uint64_t tenant_id = pkey.get_tenant_id();
      const uint64_t table_id = pkey.get_table_id();
      const int64_t partition_id = pkey.get_partition_id();
      if (0 == i) {
        if (OB_FAIL(sql.append_fmt("SELECT tenant_id, table_id, partition_id FROM %s "
                                   "WHERE (tenant_id, table_id, partition_id) in ((%lu, %lu, %ld)",
                share::OB_ALL_TENANT_GC_PARTITION_INFO_TNAME,
                share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                partition_id))) {
          TRANS_LOG(WARN, "append sql failed", KR(ret));
        }
      } else {
        if (OB_FAIL(sql.append_fmt(", (%lu, %lu, %ld)",
                share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                partition_id))) {
          TRANS_LOG(WARN, "append sql failed", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // For compatibility reasons, to ensure that the query does not lose partition_key data,
        // additional partition_key without tenant_id needs to be passed in
        if (OB_FAIL(
                sql.append_fmt(", (%lu, %lu, %ld)", OB_INVALID_TENANT_ID, extract_pure_id(table_id), partition_id))) {
          TRANS_LOG(WARN, "append sql failed", KR(ret));
        }
      }
    }
    if (OB_SUCCESS == ret) {
      if (OB_FAIL(sql.append_fmt(")"))) {
        TRANS_LOG(WARN, "append sql failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObGCPartitionAdapter::update_partition_status_(
    const ObIArray<ObPartitionKey>& pkey_array, const ObIArray<int>& status_array)
{
  int ret = OB_SUCCESS;
  if (0 >= pkey_array.count() || 0 >= status_array.count() || pkey_array.count() != status_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey_array), K(status_array));
  } else {
    for (int64_t i = 0; i < pkey_array.count(); i++) {
      GCInfo* info = NULL;
      const ObPartitionKey& pkey = pkey_array.at(i);
      const int status = status_array.at(i);
      if (OB_FAIL(gc_map_.get(pkey, info))) {
        TRANS_LOG(WARN, "get gc partition info failed", KR(ret), K(pkey));
      } else if (NULL == info) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected error, info is NULL", KR(ret), K(pkey));
      } else {
        info->set_status(status);
        (void)gc_map_.revert(info);
      }
    }
  }
  return ret;
}

bool GetUnknownPartition::operator()(const ObPartitionKey& pkey, GCInfo* info)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid() || NULL == info) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey), KP(info));
  } else if (GC_STATUS_UNKNOWN == info->get_status() ||
             (info->get_update_ts() < info->get_access_ts() &&
                 ObTimeUtility::current_time() - info->get_update_ts() > MAX_STATUS_REFRESH_INTERVAL)) {
    if (OB_FAIL(array_.push_back(pkey))) {
      TRANS_LOG(WARN, "push back partition key failed", KR(ret), K(pkey));
    }
  } else {
    // do nothing
  }
  return true;
}

bool RemoveObsoletePartition::operator()(const ObPartitionKey& pkey, GCInfo* info)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  if (!pkey.is_valid() || NULL == info) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey), KP(info));
  } else if (ObTimeUtility::current_time() - info->get_access_ts() > STATUS_OBSOLETE_TIME) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

}  // namespace transaction
}  // namespace oceanbase
