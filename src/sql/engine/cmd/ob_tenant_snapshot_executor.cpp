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
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_tenant_snapshot_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/cmd/ob_tenant_snapshot_stmt.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_table_operator.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_id.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

ERRSIM_POINT_DEF(ERRSIM_WAIT_SNAPSHOT_RESULT_ERROR);
int ObCreateTenantSnapshotExecutor::execute(ObExecContext &ctx, ObCreateTenantSnapshotStmt &stmt)
{
  int ret = OB_SUCCESS;
  const ObString &tenant_name = stmt.get_tenant_name();
  const ObString &tenant_snapshot_name = stmt.get_tenant_snapshot_name();
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  share::ObTenantSnapshotID tenant_snapshot_id;

  // TODO: support tenant snapshot in future
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("create tenant snapshot is not supported", KR(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "create tenant snapshot is");

  // if (OB_FAIL(rootserver::ObTenantSnapshotUtil::create_tenant_snapshot(tenant_name,
  //                                                                      tenant_snapshot_name,
  //                                                                      tenant_id,
  //                                                                      tenant_snapshot_id))) {
  //   LOG_WARN("create tenant snapshot failed", KR(ret), K(tenant_name), K(tenant_snapshot_name));
  //   if (OB_TENANT_SNAPSHOT_EXIST == ret) {
  //     LOG_USER_ERROR(OB_TENANT_SNAPSHOT_EXIST, tenant_snapshot_name.length(), tenant_snapshot_name.ptr());
  //   }
  // } else if (OB_UNLIKELY(ERRSIM_WAIT_SNAPSHOT_RESULT_ERROR)) {
  //   ret = ERRSIM_WAIT_SNAPSHOT_RESULT_ERROR;
  //   LOG_WARN("[ERRSIM CLONE] errsim wait snapshot creation finished", KR(ret));
  // } else if (OB_FAIL(wait_create_finish_(tenant_id, tenant_snapshot_id, ctx))) {
  //   LOG_WARN("wait create snapshot finish failed", KR(ret), K(tenant_id), K(tenant_snapshot_id));
  // }
  return ret;
}

int ObCreateTenantSnapshotExecutor::wait_create_finish_(const uint64_t tenant_id,
                                                        const share::ObTenantSnapshotID &tenant_snapshot_id,
                                                        ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotTableOperator table_op;
  ObTenantSnapItem item;
  const int64_t SNAPSHOT_CREATION_TIMEOUT = 300 * 1000 * 1000L;
  const int64_t TIME_DEVIATION = 10 * 1000 * 1000L;
  int64_t timeout = max(SNAPSHOT_CREATION_TIMEOUT, GCONF._ob_ddl_timeout) + TIME_DEVIATION;
  common::ObMySQLProxy *sql_proxy = nullptr;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + timeout);

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id or tenant_snapshot_id is not valid", KR(ret), K(tenant_id), K(tenant_snapshot_id));
  } else if (OB_ISNULL(sql_proxy = ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not be null", KR(ret));
  } else if (OB_FAIL(table_op.init(tenant_id, sql_proxy))) {
    LOG_WARN("fail to init table op", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      item.reset();
      ob_usleep(2 * 1000 * 1000L);  // 2s
      if (THIS_WORKER.is_timeout()) {
        // TODO: provides a view for user to query the snapshot status and tips user how to acquire it
        ret = OB_TENANT_SNAPSHOT_TIMEOUT;
        LOG_WARN("wait create tenant snapshot timeout", KR(ret), K(tenant_id), K(tenant_snapshot_id));
      } else if (OB_FAIL(table_op.get_tenant_snap_item(tenant_snapshot_id, false, item))) {
        LOG_WARN("fail to get snapshot item", KR(ret), K(tenant_snapshot_id));
        if (OB_TENANT_SNAPSHOT_NOT_EXIST == ret) {
          // if creating snapshot is failed, the according record might be deleted in the inner_table
          ret = OB_ERR_UNEXPECTED;
          LOG_USER_ERROR(OB_ERR_UNEXPECTED, "create snapshot failed");
        }
      } else if (ObTenantSnapStatus::NORMAL == item.get_status()) {
        break;
      } else if (ObTenantSnapStatus::CREATING == item.get_status()
                 || ObTenantSnapStatus::DECIDED == item.get_status()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(rootserver::ObTenantSnapshotUtil::notify_scheduler(tenant_id))) {
          LOG_WARN("fail to notify scheduler", KR(tmp_ret), K(tenant_id));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected item status", KR(ret), K(tenant_snapshot_id), K(item.get_status()));
      }
    }
  }
  return ret;
}

int ObDropTenantSnapshotExecutor::execute(ObExecContext &ctx, ObDropTenantSnapshotStmt &stmt)
{
  int ret = OB_SUCCESS;
  const ObString &tenant_name = stmt.get_tenant_name();
  const ObString &tenant_snapshot_name = stmt.get_tenant_snapshot_name();

  // TODO: support tenant snapshot in future
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("drop tenant snapshot is not supported", KR(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop tenant snapshot is");

  // if (OB_FAIL(rootserver::ObTenantSnapshotUtil::drop_tenant_snapshot(tenant_name,
  //                                                                    tenant_snapshot_name))) {
  //   LOG_WARN("drop tenant snapshot failed", KR(ret), K(tenant_name), K(tenant_snapshot_name));
  //   if (OB_TENANT_SNAPSHOT_NOT_EXIST == ret) {
  //     LOG_USER_ERROR(OB_TENANT_SNAPSHOT_NOT_EXIST, tenant_snapshot_name.length(), tenant_snapshot_name.ptr());
  //   }
  // }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
