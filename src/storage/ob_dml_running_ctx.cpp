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

#include "storage/ob_dml_running_ctx.h"
#include "lib/allocator/ob_allocator.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/schema/ob_table_param.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/ob_value_row_iterator.h"
#include "storage/memtable/ob_memtable_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace blocksstable;
namespace storage
{
ObDMLRunningCtx::ObDMLRunningCtx(
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    common::ObIAllocator &allocator,
    const blocksstable::ObDmlFlag dml_flag)
  : store_ctx_(store_ctx),
    dml_param_(dml_param),
    allocator_(allocator),
    dml_flag_(dml_flag),
    relative_table_(),
    col_map_(nullptr),
    col_descs_(nullptr),
    column_ids_(nullptr),
    tbl_row_(),
    is_old_row_valid_for_lob_(false),
    schema_guard_(share::schema::ObSchemaMgrItem::MOD_RELATIVE_TABLE),
    is_inited_(false)
{
}

int ObDMLRunningCtx::init(
    const common::ObIArray<uint64_t> *column_ids,
    const common::ObIArray<uint64_t> *upd_col_ids,
    ObMultiVersionSchemaService *schema_service,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!store_ctx_.is_valid())
      || OB_UNLIKELY(!dml_param_.is_valid())
      || OB_ISNULL(dml_param_.table_param_)
      || OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(store_ctx_),
        K(dml_param_), KP(schema_service));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const uint64_t table_id = dml_param_.table_param_->get_data_table().get_table_id();
    const int64_t version = dml_param_.schema_version_;
    const int64_t tenant_schema_version = dml_param_.tenant_schema_version_;
    if (dml_param_.check_schema_version_ && OB_FAIL(check_schema_version(*schema_service, tenant_id, table_id,
        tenant_schema_version, version, tablet_handle))) {
      LOG_WARN("failed to check schema version", K(ret), K(tenant_id), K(tenant_schema_version), K(table_id), K(version));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(prepare_relative_table(
      dml_param_.table_param_->get_data_table(),
      tablet_handle,
      store_ctx_.mvcc_acc_ctx_.get_snapshot_version()))) {
    LOG_WARN("failed to get relative table", K(ret), K(dml_param_));
  } else if (NULL != column_ids && OB_FAIL(prepare_column_info(*column_ids))) {
    LOG_WARN("fail to get column descriptions and column map", K(ret), K(*column_ids));
  } else {
    store_ctx_.mvcc_acc_ctx_.mem_ctx_->set_table_version(dml_param_.schema_version_);
    store_ctx_.table_version_ = dml_param_.schema_version_;
    column_ids_ = column_ids;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    relative_table_.destroy();
  }
  return ret;
}

int ObDMLRunningCtx::prepare_column_desc(
    const common::ObIArray<uint64_t> &column_ids,
    const ObRelativeTable &table,
    ObColDescIArray &col_descs)
{
  int ret = OB_SUCCESS;
  int64_t count = column_ids.count();
  if (OB_UNLIKELY(column_ids.count() <= 0 || !table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_ids), K(table));
  } else {
    ObColDesc col_desc;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(table.get_col_desc(column_ids.at(i), col_desc))) {
        LOG_WARN("fail to get column description", "column_id", column_ids.at(i));
      } else if (OB_FAIL(col_descs.push_back(col_desc))) {
        LOG_WARN("fail to add column description", K(col_desc));
      }
    }
  }
  return ret;
}

int ObDMLRunningCtx::prepare_relative_table(
    const share::schema::ObTableSchemaParam &schema,
    ObTabletHandle &tablet_handle,
    const SCN &read_snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(relative_table_.init(&schema, tablet_handle.get_obj()->get_tablet_meta().tablet_id_,
      schema.is_storage_index_table() && !schema.can_read_index()))) {
    LOG_WARN("fail to init relative_table_", K(ret), K(tablet_handle), K(schema.get_index_status()));
  } else if (OB_FAIL(relative_table_.tablet_iter_.set_tablet_handle(tablet_handle))) {
    LOG_WARN("fail to set tablet handle to iter", K(ret), K(relative_table_.tablet_iter_));
  } else if (OB_FAIL(relative_table_.tablet_iter_.refresh_read_tables_from_tablet(
      read_snapshot.get_val_for_tx(), relative_table_.allow_not_ready()))) {
    LOG_WARN("failed to get relative table read tables", K(ret));
  }
  return ret;
}

int ObDMLRunningCtx::prepare_column_info(const common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_ids.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_ids), K(relative_table_));
  } else {
    col_descs_ = &(dml_param_.table_param_->get_col_descs());
    if (col_descs_->count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col desc is empty", K(ret));
    } else if (ObDmlFlag::DF_UPDATE == dml_flag_) {
      col_map_ = &(dml_param_.table_param_->get_col_map());
    }
  }
  return ret;
}

int ObDMLRunningCtx::check_schema_version(
    share::schema::ObMultiVersionSchemaService &schema_service,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t tenant_schema_version,
    const int64_t table_version,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  bool check_formal = !is_inner_table(table_id);
  int tmp_ret = check_tenant_schema_version(schema_service, tenant_id, table_id, tenant_schema_version);
  if (OB_SUCCESS == tmp_ret) {
    // Check tenant schema first. If not pass, then check table level schema version
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard_))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (check_formal && OB_FAIL(schema_guard_.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard_.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("failed to get schema", K(ret));
  } else if (table_version != table_schema->get_schema_version()) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("table version mismatch", K(ret), K(table_id), K(table_version), K(table_schema->get_schema_version()));
  }
  if (OB_SUCC(ret)) {
    const int64_t current_time = ObClockGenerator::getClock();
    const int64_t timeout = dml_param_.timeout_ - current_time;
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("check schema version timeout", K(ret), K(current_time), "dml_param_timeout", dml_param_.timeout_);
    } else if (OB_FAIL(tablet_handle.get_obj()->check_schema_version_with_cache(table_version, timeout))) {
      LOG_WARN("failed to check schema version", K(ret), K(table_version), K(timeout));
    }
  }
  return ret;
}

int ObDMLRunningCtx::check_tenant_schema_version(
    share::schema::ObMultiVersionSchemaService &schema_service,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t tenant_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t latest_tenant_version = -1;
  if (is_inner_table(table_id)) {
    //inner table can't skip table schema check
    ret = OB_SCHEMA_EAGAIN;
  } else if (tenant_schema_version > 0
             && OB_FAIL(schema_service.get_tenant_refreshed_schema_version(tenant_id, latest_tenant_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id), K(tenant_schema_version));
  } else if (tenant_schema_version < 0 || latest_tenant_version < 0) {
    ret = OB_SCHEMA_EAGAIN;
  } else if (!share::schema::ObSchemaService::is_formal_version(latest_tenant_version)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_INFO("local schema_version is not formal, try again", K(ret),
             K(tenant_id), K(tenant_schema_version), K(latest_tenant_version));
  } else if (latest_tenant_version > 0 && tenant_schema_version == latest_tenant_version) {
    // no schema change, do nothing
    ret = OB_SUCCESS;
  } else {
    ret = OB_SCHEMA_EAGAIN;
    LOG_INFO("need check table schema version", K(ret),
             K(tenant_id), K(tenant_schema_version), K(latest_tenant_version));
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
