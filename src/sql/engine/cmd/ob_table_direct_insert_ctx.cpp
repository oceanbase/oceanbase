/**
 * Copyright (c) 2023 OceanBase
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

#include "sql/engine/cmd/ob_table_direct_insert_ctx.h"
#include "observer/omt/ob_tenant.h"
#include "observer/table_load/ob_table_load_exec_ctx.h"
#include "observer/table_load/ob_table_load_instance.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace observer;
using namespace storage;
using namespace share;

namespace sql
{
ObTableDirectInsertCtx::~ObTableDirectInsertCtx()
{
  destroy();
}

int ObTableDirectInsertCtx::init(
    ObExecContext *exec_ctx,
    ObPhysicalPlan &phy_plan,
    const uint64_t table_id,
    const int64_t parallel,
    const bool is_incremental,
    const bool enable_inc_replace,
    const bool is_insert_overwrite,
    const double online_sample_percent)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObSQLSessionInfo *session_info = nullptr;
  ObSchemaGetterGuard *schema_guard = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableDirectInsertCtx init twice", KR(ret));
  } else if (OB_ISNULL(exec_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("exec_ctx cannot be null", KR(ret));
  } else if (OB_ISNULL(session_info = exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session info is null", KR(ret));
  } else if (OB_ISNULL(exec_ctx->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sql ctx is null", KR(ret));
  } else if (OB_ISNULL(schema_guard = exec_ctx->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected schema guard is null", KR(ret));
  } else if (OB_UNLIKELY(session_info->get_ddl_info().is_mview_complete_refresh() && enable_inc_replace)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mview complete refresh enable inc replace", KR(ret));
  } else {
    is_direct_ = true;
    if (OB_ISNULL(load_exec_ctx_ = OB_NEWx(ObTableLoadExecCtx, &exec_ctx->get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadExecCtx", KR(ret));
    } else if (OB_ISNULL(table_load_instance_ =
                           OB_NEWx(ObTableLoadInstance, &exec_ctx->get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadInstance", KR(ret));
    } else {
      load_exec_ctx_->exec_ctx_ = exec_ctx;
      const ObTableSchema *table_schema = nullptr;
      ObArray<uint64_t> column_ids;
      ObCompressorType compressor_type = ObCompressorType::NONE_COMPRESSOR;
      ObDirectLoadMethod::Type method = (is_incremental ? ObDirectLoadMethod::INCREMENTAL : ObDirectLoadMethod::FULL);
      ObDirectLoadInsertMode::Type insert_mode = ObDirectLoadInsertMode::INVALID_INSERT_MODE;
      if (session_info->get_ddl_info().is_mview_complete_refresh() || is_insert_overwrite) {
        insert_mode = ObDirectLoadInsertMode::OVERWRITE;
      } else if (enable_inc_replace) {
        insert_mode = ObDirectLoadInsertMode::INC_REPLACE;
      } else {
        insert_mode = ObDirectLoadInsertMode::NORMAL;
      }
      ObDirectLoadMode::Type load_mode = is_insert_overwrite ? ObDirectLoadMode::INSERT_OVERWRITE : ObDirectLoadMode::INSERT_INTO;
      bool is_heap_table = false;
      ObDirectLoadLevel::Type load_level = ObDirectLoadLevel::INVALID_LEVEL;
      if (OB_FAIL(get_direct_load_level(phy_plan, table_id, load_level))) {
        LOG_WARN("failed to get direct load level", KR(ret), K(phy_plan), K(table_id));
      } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(*schema_guard, tenant_id, table_id, table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret));
      } else if (OB_FAIL(ObDDLUtil::get_temp_store_compress_type(table_schema->get_compressor_type(),
                                                                 parallel,
                                                                 compressor_type))) {
        LOG_WARN("fail to get tmp store compressor type", KR(ret));
      } else if (OB_FAIL(ObTableLoadSchema::get_column_ids(table_schema, column_ids))) {
        LOG_WARN("failed to init store column idxs", KR(ret));
      } else if (OB_FAIL(ObTableLoadService::check_support_direct_load(*schema_guard,
                                                                       table_schema,
                                                                       method,
                                                                       insert_mode,
                                                                       load_mode,
                                                                       load_level,
                                                                       column_ids))) {
        LOG_WARN("fail to check support direct load", KR(ret));
      } else {
        ObTableLoadParam param;
        ObArray<ObTabletID> tablet_ids;
        tablet_ids.reset();
        param.tenant_id_ = MTL_ID();
        param.table_id_ = table_id;
        param.parallel_ = parallel;
        param.session_count_ = parallel;
        param.batch_size_ = 100;
        param.max_error_row_count_ = 0;
        param.column_count_ = column_ids.count();
        param.need_sort_ = table_schema->is_heap_table() ? phy_plan.get_direct_load_need_sort() : true;
        param.px_mode_ = true;
        param.online_opt_stat_gather_ = is_online_gather_statistics_;
        param.dup_action_ = (enable_inc_replace ? sql::ObLoadDupActionType::LOAD_REPLACE
                                                : sql::ObLoadDupActionType::LOAD_STOP_ON_DUP);
        param.method_ = method;
        param.insert_mode_ = insert_mode;
        param.load_mode_ = load_mode;
        param.compressor_type_ = compressor_type;
        param.online_sample_percent_ = online_sample_percent;
        param.load_level_ = load_level;
        if ((ObDirectLoadLevel::PARTITION == param.load_level_)
            && OB_FAIL(get_tablet_ids(*(exec_ctx->get_sql_ctx()), table_id, tablet_ids))) {
          LOG_WARN("failed to get tablet ids", KR(ret), K(table_id));
        } else if (OB_FAIL(table_load_instance_->init(param, column_ids, tablet_ids, load_exec_ctx_))) {
          LOG_WARN("failed to init direct loader", KR(ret), K(param), K(column_ids), K(tablet_ids));
        } else {
          phy_plan.set_ddl_task_id(table_load_instance_->get_table_ctx()->ddl_param_.task_id_);
          is_inited_ = true;
          LOG_DEBUG("succeeded to init direct loader", K(param));
        }
      }
    }
  }
  return ret;
}

// commit() should be called before finish()
int ObTableDirectInsertCtx::commit()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableDirectInsertCtx is not init", KR(ret));
  } else if (OB_FAIL(table_load_instance_->px_commit_data())) {
    LOG_WARN("failed to do px_commit_data", KR(ret));
  }
  return ret;
}

// finish() should be called after commit()
int ObTableDirectInsertCtx::finish()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableDirectInsertCtx is not init", KR(ret));
  } else if (OB_FAIL(table_load_instance_->px_commit_ddl())) {
    LOG_WARN("failed to do px_commit_ddl", KR(ret));
  } else {
    LOG_DEBUG("succeeded to finish direct loader");
  }
  return ret;
}

void ObTableDirectInsertCtx::destroy()
{
  if (OB_NOT_NULL(table_load_instance_)) {
    table_load_instance_->~ObTableLoadInstance();
    table_load_instance_ = nullptr;
  }
  if (OB_NOT_NULL(load_exec_ctx_)) {
    load_exec_ctx_->~ObTableLoadExecCtx();
    load_exec_ctx_ = nullptr;
  }
  is_inited_ = false;
  is_direct_ = false;
  is_online_gather_statistics_ = false;
}

int ObTableDirectInsertCtx::get_direct_load_level(
    const ObPhysicalPlan &phy_plan,
    const uint64_t table_id,
    ObDirectLoadLevel::Type &load_level)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableLocation> &table_locs = phy_plan.get_table_locations();
  load_level = ObDirectLoadLevel::TABLE;
  for (int64_t i = 0; OB_SUCC(ret) && (i < table_locs.count()); ++i) {
    const ObTableLocation &table_loc = table_locs.at(i);
    if (table_loc.get_ref_table_id() == table_id) {
      if (table_loc.get_part_hint_ids().count() > 0) {
        load_level = ObDirectLoadLevel::PARTITION;
      }
      break;
    }
  }
  return ret;
}

int ObTableDirectInsertCtx::get_tablet_ids(
    const ObSqlCtx &sql_ctx,
    const uint64_t table_id,
    ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTablePartitionInfo *table_part_info = nullptr;
  const ObIArray<ObTablePartitionInfo *> &partition_infos = sql_ctx.partition_infos_;
  for (int64_t i = 0; OB_SUCC(ret) && (i < partition_infos.count()); ++i) {
    const ObTablePartitionInfo *part_info = partition_infos.at(i);
    if (part_info->get_ref_table_id() == table_id) {
      table_part_info = part_info;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(table_part_info)) {
    const ObCandiTableLoc &loc = table_part_info->get_phy_tbl_location_info();
    const ObCandiTabletLocIArray &tablet_locs = loc.get_phy_part_loc_info_list();
    for (int64_t i = 0; OB_SUCC(ret) && (i < tablet_locs.count()); ++i) {
      const ObTabletID tablet_id = tablet_locs.at(i).get_partition_location().get_tablet_id();
      if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
        LOG_WARN("failed to add tablet id", KR(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObTableDirectInsertCtx::get_is_heap_table(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const uint64_t table_id,
    bool &is_heap_table)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", KR(ret));
  } else {
    is_heap_table = table_schema->is_heap_table();
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
