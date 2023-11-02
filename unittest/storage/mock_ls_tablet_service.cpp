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

#define USING_LOG_PREFIX STORAGE

#define private public

#include "mock_ls_tablet_service.h"

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_errno.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/ob_i_store.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tablet/ob_tablet.h"

#undef private

using namespace oceanbase::storage;

int MockInsertRowsLSTabletService::prepare_dml_running_ctx(
    const common::ObIArray<uint64_t> *column_ids,
    const common::ObIArray<uint64_t> *upd_col_ids,
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  ObRelativeTable &relative_table = run_ctx.relative_table_;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  ObStoreCtx &store_ctx = run_ctx.store_ctx_;
  auto &acc_ctx = store_ctx.mvcc_acc_ctx_;
  memtable::ObIMemtableCtx *mem_ctx = store_ctx.mvcc_acc_ctx_.get_mem_ctx();

  if (OB_UNLIKELY(!store_ctx.is_valid())
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_ISNULL(dml_param.table_param_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(mem_ctx), K(dml_param));
  } else {
    const ObTableSchemaParam &schema = dml_param.table_param_->get_data_table();
    if (OB_FAIL(relative_table.init(
        &schema,
        tablet_handle.get_obj()->get_tablet_meta().tablet_id_,
        schema.is_global_index_table() &&
        ObIndexStatus::INDEX_STATUS_UNAVAILABLE == schema.get_index_status()))) {
      LOG_WARN("failed to get relative table", K(ret), K(dml_param));
    } else if (NULL != column_ids && OB_FAIL(run_ctx.prepare_column_info(*column_ids))) {
      LOG_WARN("fail to get column descriptions and column map", K(ret), K(*column_ids));
    } else {
      relative_table.tablet_iter_.set_tablet_handle(tablet_handle);
      store_ctx.table_version_ = dml_param.schema_version_;
      run_ctx.column_ids_ = column_ids;
    }
  }

  if (OB_FAIL(ret)) {
    relative_table.destroy();
  }
  return ret;
}
