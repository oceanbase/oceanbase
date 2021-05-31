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

#define USING_LOG_PREFIX SQL_PC

#include "ob_prepared_sql_store.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
using namespace common;

namespace sql {
int ObPreparedSqlStore::init()
{
  int ret = OB_SUCCESS;
  // init allocator
  if (OB_FAIL(allocator_.init(128))) {
    LOG_WARN("Buddy allocator init failed", K(ret));
  } else if (OB_FAIL(psmap_.create(
                 hash::cal_next_prime(512), ObModIds::OB_HASH_BUCKET_PLAN_CACHE, ObModIds::OB_HASH_NODE_PLAN_CACHE))) {
    LOG_WARN("Failed to init psmap", K(ret));
  } else {
    ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_SQL_PREPARED_SQL);
    allocator_.set_attr(attr);
  }
  return ret;
}

int ObPreparedSqlStore::store_sql(const ObString& sql, ObString& osql)
{
  int ret = OB_SUCCESS;
  ObPreparedSqlValue* value = NULL;
  ObPreparedSqlStoreAddRef op;
  do {
    ret = psmap_.atomic(sql, op);
    if (OB_HASH_EXIST == ret) {
      if (OB_SUCC(op.get_rc())) {
        osql = op.get_sql();
      }
    } else if (OB_HASH_NOT_EXIST == ret) {
      int64_t bsize = sizeof(ObPreparedSqlValue) + sql.length();
      char* buff = reinterpret_cast<char*>(allocator_.alloc(bsize));
      if (NULL != buff) {
        MEMCPY(buff, reinterpret_cast<void*>(const_cast<char*>(sql.ptr())), sql.length());
        value = new (buff + sql.length()) ObPreparedSqlValue();
        ObString nsql(sql.length(), buff);
        if (OB_SUCC(value->init())) {
          ret = psmap_.set(nsql, value);
          if (OB_HASH_EXIST == ret) {
            value->~ObPreparedSqlValue();
            allocator_.free(buff);
            ret = OB_EAGAIN;
          } else if (OB_HASH_INSERT_SUCC == ret) {
            LOG_DEBUG("Insert new prepare sql success", K(sql));
            osql = nsql;
            ret = OB_SUCCESS;
          } else {
            LOG_ERROR("unexpected error", K(ret));
            value->~ObPreparedSqlValue();
            allocator_.free(buff);
          }
        } else {
          LOG_WARN("Init ObPrepareSqlValue failed", K(ret));
          value->~ObPreparedSqlValue();
          allocator_.free(buff);
        }
      } else {
        LOG_WARN("Alloc mem for new prepare sql failed");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    } else {
      LOG_ERROR("unexpected error", K(ret));
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

int ObPreparedSqlStore::free_sql(const ObString& sql)
{
  int ret = OB_SUCCESS;
  ObPreparedSqlValue* value = NULL;
  ObPreparedSqlStoreDecRef op;
  ret = psmap_.atomic(sql, op);
  if (OB_HASH_EXIST == ret) {
    if (OB_DEC_AND_LOCK == op.get_rc()) {
      // delete sql
      value = op.get_prepared_sql_value();
      if (0 == value->ref_count_) {
        ret = psmap_.erase(sql);
        if (OB_HASH_EXIST == ret) {
          LOG_DEBUG("Erase prepared sql success", K(sql));
          value->~ObPreparedSqlValue();
          value = NULL;
          allocator_.free(reinterpret_cast<void*>(const_cast<char*>(sql.ptr())));
        } else {
          LOG_ERROR("Erase prepared sql");
        }
      }
    } else if (OB_SUCCESS == op.get_rc()) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("unexpected error", K(ret));
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    LOG_ERROR("unexpected error", K(ret));
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
