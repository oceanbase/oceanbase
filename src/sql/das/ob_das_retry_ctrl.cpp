/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_retry_ctrl.h"
#include "sql/das/ob_das_task.h"
#include "sql/das/ob_das_ref.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql {

/**
 *
 * DAS cannot unconditionally retry for the error of tablet_location or ls_location, like -4725, -4721,
 * and needs to determine whether the real cause of the error is due to DDL operations or transfer.
 * 1. When the table, partition or tenant was dropped, which is caused by DDL, das task cannot be retried.
 * 2. When a partition was transfered and tablet location cache is not updated, tablet location cache should
 *    be updated and das task needs to be retried.
 *
 **/
void ObDASRetryCtrl::tablet_location_retry_proc(ObDASRef &das_ref,
                                                ObIDASTaskOp &task_op,
                                                bool &need_retry)
{
  need_retry = false;
  int ret = OB_SUCCESS;
  ObTableID ref_table_id = task_op.get_ref_table_id();
  ObDASLocationRouter &loc_router = DAS_CTX(das_ref.get_exec_ctx()).get_location_router();
  const ObDASTabletLoc *tablet_loc = task_op.get_tablet_loc();
  bool tablet_exist = false;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *table_schema = nullptr;
  if (OB_ISNULL(tablet_loc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet loc is nullptr", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    // tenant could be dropped
    task_op.set_errcode(ret);
    LOG_WARN("get tenant schema guard fail", KR(ret), K(MTL_ID()));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), ref_table_id, table_schema))) {
    task_op.set_errcode(ret);
    LOG_WARN("failed to get table schema", KR(ret), K(ref_table_id));
  } else if (OB_ISNULL(table_schema)) {
    // table could be dropped
    task_op.set_errcode(OB_TABLE_NOT_EXIST);
    LOG_WARN("table not exist,  maybe dropped by DDL, stop das retry", K(ref_table_id));
  } else if (table_schema->is_vir_table()) {
    // the location of the virtual table can't be refreshed,
    // so when a location exception occurs, virtual table is not retryable
  } else if (OB_FAIL(table_schema->check_if_tablet_exists(tablet_loc->tablet_id_, tablet_exist))) {
    LOG_WARN("failed to check if tablet exists", K(ret), K(tablet_loc), K(ref_table_id));
  } else if (!tablet_exist) {
    // partition could be dropped or table could be truncated, in this case we return OB_SCHEMA_EAGAIN and
    // attempt statement-level retry
    task_op.set_errcode(OB_SCHEMA_EAGAIN);
    LOG_WARN("partition not exist, maybe dropped by DDL or table was truncated", K(tablet_loc), K(ref_table_id));
  } else {
    loc_router.force_refresh_location_cache(true, task_op.get_errcode());
    need_retry = true;
    const ObDASTableLocMeta *loc_meta = tablet_loc->loc_meta_;
    LOG_INFO("[DAS RETRY] refresh tablet location cache and retry DAS task",
             "errcode", task_op.get_errcode(), KPC(loc_meta), KPC(tablet_loc));
  }
}

void ObDASRetryCtrl::tablet_nothing_readable_proc(ObDASRef &, ObIDASTaskOp &task_op, bool &need_retry)
{
  if (is_virtual_table(task_op.get_ref_table_id())) {
    need_retry = false;
  } else {
    need_retry = true;
  }
}

void ObDASRetryCtrl::task_network_retry_proc(ObDASRef &, ObIDASTaskOp &, bool &need_retry)
{
  need_retry = true;
}

}  // namespace sql
}  // namespace oceanbase
