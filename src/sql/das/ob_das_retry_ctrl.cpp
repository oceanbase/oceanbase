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

void ObDASRetryCtrl::tablet_location_retry_proc(ObDASRef &das_ref,
                                                ObIDASTaskOp &task_op,
                                                bool &need_retry)
{
  need_retry = false;
  int ret = OB_SUCCESS;
  ObTableID ref_table_id = task_op.get_ref_table_id();
  ObDASLocationRouter &loc_router = DAS_CTX(das_ref.get_exec_ctx()).get_location_router();
  const ObDASTabletLoc *tablet_loc = task_op.get_tablet_loc();
  if (is_virtual_table(ref_table_id)) {
    //the location of the virtual table can't be refreshed,
    //so when a location exception occurs, virtual table is not retryable
    need_retry = false;
  } else if (OB_ISNULL(tablet_loc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet loc is nullptr", K(ret));
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

/**
 * The storage throws 4725 to the DAS in two cases:
 * 1. When a table or partition is dropped, the tablet is recycled, which is caused by DDL and cannot be retried.
 * 2. When a partition is transfered, but the tablet location cache is not updated,
 *    the TSC operation is sent to the old server, and the storage reports 4725, this case needs to be retried.
 * The DAS cannot unconditionally retry 4725,
 * and needs to determine whether the real cause of the 4725 error is a drop table or a transfer.
 **/
void ObDASRetryCtrl::tablet_not_exist_retry_proc(ObDASRef &das_ref,
                                                 ObIDASTaskOp &task_op,
                                                 bool &need_retry)
{
  int ret = OB_SUCCESS;
  need_retry = false;
  ObTableID ref_table_id = task_op.get_ref_table_id();
  bool tablet_exist = false;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *table_schema = nullptr;
  const ObDASTabletLoc *tablet_loc = task_op.get_tablet_loc();
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(tablet_loc)) {
    LOG_WARN("invalid schema service", KR(ret), K(GCTX.schema_service_), K(tablet_loc));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    // tenant could be deleted
    task_op.set_errcode(ret);
    LOG_WARN("get tenant schema guard fail", KR(ret), K(MTL_ID()));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), ref_table_id, table_schema))) {
    task_op.set_errcode(ret);
    LOG_WARN("failed to get table schema", KR(ret));
  } else if (OB_ISNULL(table_schema)) {
    //table could be dropped
    task_op.set_errcode(OB_TABLE_NOT_EXIST);
    LOG_WARN("table not exist, fast fail das task", K(ref_table_id));
  } else if (table_schema->is_vir_table()) {
    need_retry = false;
  } else if (OB_FAIL(table_schema->check_if_tablet_exists(tablet_loc->tablet_id_, tablet_exist))) {
    LOG_WARN("check if tablet exists failed", K(ret), K(tablet_loc), K(ref_table_id));
  } else if (!tablet_exist) {
    task_op.set_errcode(OB_PARTITION_NOT_EXIST);
    LOG_WARN("partition not exist, maybe dropped by DDL", K(ret), K(tablet_loc), K(ref_table_id));
  } else {
    tablet_location_retry_proc(das_ref, task_op, need_retry);
  }
}
}  // namespace sql
}  // namespace oceanbase
