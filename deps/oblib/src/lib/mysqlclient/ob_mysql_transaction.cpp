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

#define USING_LOG_PREFIX COMMON_MYSQLP

#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
namespace common
{
using namespace sqlclient;

ObMySQLTransaction::ObMySQLTransaction(bool enable_query_stash)
    :ObSingleConnectionProxy(),
     start_time_(0),
     in_trans_(false),
     enable_query_stash_(enable_query_stash)
{
}

ObMySQLTransaction::~ObMySQLTransaction()
{
  int ret = OB_SUCCESS;
  if (in_trans_) {
    if (OB_FAIL(end(OB_SUCCESS == get_errno()))) {
      // Although the ret cannot be transmitted here, it still plays WARN, because sometimes it is normal to fail here.
      // If it is not normal, the transaction module will log ERROR, here it is better to just log WARN
      LOG_WARN("fail to end", K(ret));
    }
  }
  if (enable_query_stash_) {
    for (auto &it : query_stash_desc_) {
      ob_delete(it.second);
    }
    query_stash_desc_.destroy();
  }
}

int ObMySQLTransaction::start_transaction(
    const uint64_t &tenant_id,
    bool with_snapshot)
{
  int ret = OB_SUCCESS;
  if (NULL == get_connection()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("conn_ is NULL", K(ret));
  } else if (OB_FAIL(get_connection()->start_transaction(tenant_id, with_snapshot))) {
    LOG_WARN("fail to start transaction", K(ret), K(tenant_id), K(with_snapshot));
  }
  return ret;
}

int ObMySQLTransaction::start(
    ObISQLClient *sql_client,
    const uint64_t tenant_id,
    bool with_snapshot/* = false*/,
    const int32_t group_id /* = 0*/)
{
  int ret = OB_SUCCESS;
  start_time_ = ::oceanbase::common::ObTimeUtility::current_time();
  if (OB_FAIL(connect(tenant_id, group_id, sql_client))) {
    LOG_WARN("failed to init", K(ret), K(tenant_id));
  } else if (enable_query_stash_ && OB_FAIL(query_stash_desc_.create(1024, "BucketQueryS", "NodeQueryS"))) {
    LOG_WARN("failed to init map", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(start_transaction(tenant_id, with_snapshot))) {
      set_errno(ret);
      close();
      LOG_WARN("failed to start transaction", K(ret), K(tenant_id), K(with_snapshot));
    } else {
      in_trans_ = true;
      LOG_DEBUG("start transaction success", K(tenant_id), K(with_snapshot));
    }
  }
  return ret;
}

int ObMySQLTransaction::start(ObISQLClient *proxy,
                              const uint64_t &tenant_id,
                              const int64_t &tenant_refreshed_schema_version,
                              bool with_snapshot)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(proxy, tenant_id, tenant_refreshed_schema_version, with_snapshot);
  return ret;
}

int ObMySQLTransaction::end_transaction(const bool commit)
{
  int ret = OB_SUCCESS;
  if (NULL != get_connection()) {
    if (commit) {
      ret = get_connection()->commit();
    } else {
      ret = get_connection()->rollback();
    }
    if (OB_SUCCESS == get_errno()) {
      set_errno(ret);
    }
  }
  return ret;
}

int ObMySQLTransaction::do_stash_query(int min_batch_cnt)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  for (hash::ObHashMap<const char*, ObSqlTransQueryStashDesc*>::iterator it = query_stash_desc_.begin();
      OB_SUCC(ret) && it != query_stash_desc_.end(); it++) {
    if (it->second->get_row_cnt() < min_batch_cnt) {
      continue;
    }
    const uint64_t start_time = ObTimeUtility::current_time();
    if (it->second->get_tenant_id() == OB_INVALID_TENANT_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("do_stash_query", K(ret));
    } else if (OB_FAIL(write(it->second->get_tenant_id(), it->second->get_stash_query().ptr(), affected_rows))) {
      LOG_ERROR("query_write", "tenant_id", it->second->get_tenant_id(), "query", it->second->get_stash_query(), K(ret));
    } else if (affected_rows != it->second->get_row_cnt()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("query_write", K(ret), K(affected_rows), "row_cnt", it->second->get_row_cnt(), "query", it->second->get_stash_query());
    } else {
      const uint64_t end_time = ObTimeUtility::current_time();
      it->second->reset();
      LOG_INFO("query_write succ", "table", it->first, "rows", affected_rows, "cost", end_time - start_time);
    }
  }
  return ret;
}

int ObMySQLTransaction::handle_trans_in_the_end(const int err_no)
{
  int ret = OB_SUCCESS;
  if (is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(end(OB_SUCCESS == err_no))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == err_no, K(tmp_ret));
      ret = OB_SUCCESS == err_no ? tmp_ret : err_no;
    } else {
      ret = err_no;
    }
  } else {
    ret = err_no;
  }
  return ret;
}

int ObMySQLTransaction::get_stash_query(uint64_t tenant_id, const char *table_name, ObSqlTransQueryStashDesc *&desc)
{
  int ret = OB_SUCCESS;
  ret = query_stash_desc_.get_refactored(table_name, desc);
  if (OB_FAIL(ret) && ret != OB_HASH_NOT_EXIST) {
    LOG_WARN("get_stash_query", K(ret), K(table_name));
  } else if (ret == OB_HASH_NOT_EXIST) {
    ret = OB_SUCCESS;
    void *ptr = ob_malloc(sizeof(ObSqlTransQueryStashDesc), "QueryStash");
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get_stash_query ob_malloc fail", K(ret));
    } else {
      desc = new(ptr) ObSqlTransQueryStashDesc();
      if (OB_FAIL(query_stash_desc_.set_refactored(table_name, desc))) {
        LOG_WARN("get_stash_query", K(ret), K(table_name));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (desc->get_stash_query().empty()) {
      desc->set_tenant_id(tenant_id);
    } else {
      if (desc->get_tenant_id() == OB_INVALID_TENANT_ID || desc->get_tenant_id() != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_stash_query", K(ret), KPC(desc), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObMySQLTransaction::end(const bool want_commit)
{
  bool commit = want_commit;
  int ret = OB_SUCCESS;
  if (in_trans_) {
    if (enable_query_stash_ && commit) {
      int tmp_ret = do_stash_query();
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("do_stash_query fail", K(tmp_ret));
        commit = false;
      }
    }
    ret = end_transaction(commit);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to end transation", K(ret));
    } else {
      LOG_DEBUG("end transaction success", K(commit));
    }
    in_trans_ = false;
  }
  close();
  return ret;
}

} // end namespace commmon
} // end namespace oceanbase
