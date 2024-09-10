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
#define USING_LOG_PREFIX SHARE
#include "ob_storage_io_usage_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_server_struct.h"


using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

// query timeout 3s
#define update_storage_io_usage_sql \
       "insert  /*+ QUERY_TIMEOUT(3000000) */ \
        into %s (tenant_id, storage_id, dest_id, storage_mod, type, total) \
        values(%ld, %ld, %ld, '%s', '%s', %ld) \
        ON DUPLICATE KEY UPDATE \
        total = total + VALUES(total);"

int ObStorageIOUsageProxy::update_storage_io_usage(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const int64_t storage_id,
    const int64_t dest_id,
    const ObString &storage_mod,
    const ObString &type,
    const int64_t total)
{
    int ret = OB_SUCCESS;
    uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (!is_valid_tenant_id(tenant_id)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid config", K(ret), K(tenant_id));
    } else if (storage_id == OB_INVALID_ID && dest_id == OB_INVALID_ID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(storage_id), K(dest_id));
    } else if (total < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(total));
    } else if (0 == total ) {
      // do nothing
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, meta_tenant_id, true))){
        LOG_WARN("fail to start trans", K(ret));
    } else {
      ObSqlString sql;
      int64_t affected_rows = 0;
      uint64_t real_tenant_id = tenant_id;
      // if tenant_id is meta_tenant_id and not sys tenant, use real tenant id
      if (tenant_id == meta_tenant_id) {
        real_tenant_id = gen_user_tenant_id(tenant_id);
      }
      if (OB_FAIL(sql.append_fmt(update_storage_io_usage_sql,
                                 OB_ALL_STORAGE_IO_USAGE_TNAME,
                                 real_tenant_id,
                                 storage_id,
                                 dest_id,
                                 storage_mod.ptr(),
                                 type.ptr(),
                                 total))) {
          LOG_WARN("fail to append fmt", K(ret));
      } else if (OB_FAIL(trans.write(meta_tenant_id,
                                     sql.ptr(),
                                     affected_rows))) {
          LOG_WARN("fail to exec sql", K(ret), K(sql));
      }
      bool is_commit = true;
      // single means insert new row
      // double means insert duplicate key and update one row
      if (OB_FAIL(ret) ||
          !(is_single_row(affected_rows) || is_double_row(affected_rows))) {
        is_commit = false;
        LOG_WARN("unexpected value. expect only 1 or 2 row affected", K(ret), K(affected_rows), K(sql));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(is_commit))) {
        ret = tmp_ret;
        LOG_WARN("fail to commit/rollback trans", K(ret), K(is_commit));
      }
      LOG_DEBUG("update storage io usage", K(ret), K(sql));
    }
    return ret;
}