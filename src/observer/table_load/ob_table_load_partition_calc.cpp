// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_partition_calc.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "observer/table_load/ob_table_load_obj_cast.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share::schema;
using namespace sql;
using namespace table;

ObTableLoadPartitionCalc::ObTableLoadPartitionCalc()
  : session_info_(nullptr),
    is_partition_with_autoinc_(false),
    partition_with_autoinc_idx_(OB_INVALID_INDEX),
    tenant_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    is_partitioned_(false),
    allocator_("TLD_PartCalc"),
    exec_ctx_(allocator_),
    is_inited_(false)
{
}

int ObTableLoadPartitionCalc::init(uint64_t tenant_id, uint64_t table_id, sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPartitionCalc init twice", KR(ret), KP(this));
  } else {
    allocator_.set_tenant_id(tenant_id);
    sql_ctx_.schema_guard_ = &schema_guard_;
    exec_ctx_.set_sql_ctx(&sql_ctx_);
    const ObTableSchema *table_schema = nullptr;
    ObDataTypeCastParams cast_params(session_info->get_timezone_info());
    if (OB_FAIL(time_cvrt_.init(cast_params.get_nls_format(ObDateTimeType)))) {
      LOG_WARN("fail to init time converter", KR(ret));
    } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(tenant_id, table_id, schema_guard_,
                                                          table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
    } else {
      const bool is_partitioned = table_schema->is_partitioned_table();
      if (!is_partitioned) {  // 非分区表
        if (OB_FAIL(table_schema->get_tablet_and_object_id(partition_id_.tablet_id_,
                                                          partition_id_.partition_id_))) {
          LOG_WARN("fail to get tablet and object", KR(ret));
        }
      } else {  // 分区表
        // 初始化table_location_
        if (OB_FAIL(
              table_location_.init_partition_ids_by_rowkey2(exec_ctx_, *session_info, schema_guard_, table_id))) {
          LOG_WARN("fail to init table location", KR(ret));
        }
        // 获取part_key_obj_index_
        else if (OB_FAIL(init_part_key_index(table_schema, allocator_))) {
          LOG_WARN("fail to get rowkey index", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        tenant_id_ = tenant_id;
        table_id_ = table_id;
        session_info_ = session_info;
        is_partitioned_ = is_partitioned;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObTableLoadPartitionCalc::init_part_key_index(const ObTableSchema *table_schema,
                                                ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, 64> column_descs;
  if (OB_FAIL(table_schema->get_column_ids(column_descs, false))) {
    LOG_WARN("fail to get column ids", KR(ret));
  } else if (OB_UNLIKELY(column_descs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty column desc", KR(ret));
  }

  int64_t part_key_num = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
    const ObColumnSchemaV2 *column_schema =
      table_schema->get_column_schema(column_descs.at(i).col_id_);
    if (column_schema->is_tbl_part_key_column()) {
      part_key_num ++;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(part_key_obj_index_.create(part_key_num, allocator))) {
      LOG_WARN("fail to create", KR(ret));
    }
  }
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
    const ObColumnSchemaV2 *column_schema =
      table_schema->get_column_schema(column_descs.at(i).col_id_);
    if (column_schema->is_tbl_part_key_column()) {
      pos ++;
      if (OB_UNLIKELY(pos > part_key_obj_index_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected rowkey position", KR(ret), KPC(column_schema), K(pos));
      } else {
        if (table_schema->is_heap_table()) {
          abort_unless(i > 0);
          part_key_obj_index_[pos - 1].index_ = i - 1;
        } else {
          part_key_obj_index_[pos - 1].index_ = i;
        }
        if (column_schema->is_identity_column() || column_schema->is_autoincrement()) {
          is_partition_with_autoinc_ = true;
          partition_with_autoinc_idx_ = pos - 1;
        }
        part_key_obj_index_[pos - 1].column_schema_ = column_schema;
      }
    }
  }
  return ret;
}

int ObTableLoadPartitionCalc::calc(ObTableLoadPartitionCalcContext &ctx)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(calc_part_time_us);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPartitionCalc not init", KR(ret), KP(this));
  } else {
    const ObTableLoadObjRowArray &obj_rows = ctx.obj_rows_;
    const int64_t column_count = ctx.param_.column_count_;
    if (OB_UNLIKELY(obj_rows.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), K(column_count), K(obj_rows.count()));
    } else {
      const int64_t row_count = obj_rows.count();
      if (!is_partitioned_) {  // 非分区表
        for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
          if (OB_FAIL(ctx.partition_ids_.push_back(partition_id_))) {
            LOG_WARN("failed to push back partition id", KR(ret));
          }
        }
      } else {  // 分区表
        ObArray<ObNewRow> part_rows;
        part_rows.set_block_allocator(ModulePageAllocator(ctx.allocator_));
        for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
          ObNewRow part_row;
          if (OB_FAIL(
                get_row(ctx, obj_rows.at(i), column_count, part_row, ctx.allocator_))) {
            LOG_WARN("fail to get rowkey", KR(ret));
          } else if (OB_FAIL(part_rows.push_back(part_row))) {
            LOG_WARN("failed to push back partition row", KR(ret), K(part_row));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(get_partition_by_row(part_rows, ctx.partition_ids_))) {
            LOG_WARN("fail to get partition", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

// FIXME: TODO, 对于非分区表，不能进入到这里计算
int ObTableLoadPartitionCalc::get_row(ObTableLoadPartitionCalcContext &ctx, const ObTableLoadObjRow &obj_row, int32_t length, ObNewRow &part_row,
                                      ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_obj_count = part_key_obj_index_.count();
  ObObj *rowkey_objs = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * rowkey_obj_count));
  ObDataTypeCastParams cast_params(session_info_->get_timezone_info());
  ObCastCtx cast_ctx(&allocator, &cast_params, CM_NONE, ObCharset::get_system_collation());
  ObTableLoadCastObjCtx cast_obj_ctx(ctx.param_, &time_cvrt_, &cast_ctx, true);
  if (OB_ISNULL(rowkey_objs)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_obj_count; ++i) {
    const IndexAndType &index_and_type = part_key_obj_index_.at(i);
    const int64_t obj_index = index_and_type.index_;
    if (OB_UNLIKELY(obj_index >= length)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid length", KR(ret), K(obj_index), K(length));
    } else if (OB_FAIL(ObTableLoadObjCaster::cast_obj(cast_obj_ctx, index_and_type.column_schema_,
                                                    obj_row.cells_[obj_index], rowkey_objs[i]))) {
      LOG_WARN("fail to cast obj", KR(ret));
    }

  }
  if (OB_SUCC(ret)) {
    part_row.assign(rowkey_objs, rowkey_obj_count);
  }
  return ret;
}

int ObTableLoadPartitionCalc::get_partition_by_row(
  ObIArray<ObNewRow> &part_rows, ObIArray<ObTableLoadPartitionId> &partition_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> tablet_ids;
  ObArray<ObObjectID> part_ids;
  if (OB_FAIL(table_location_.calculate_partition_ids_by_rows2(
               *session_info_, schema_guard_, table_id_, part_rows, tablet_ids, part_ids))) {
    LOG_WARN("fail to calc partition id", KR(ret));
  } else if (OB_UNLIKELY(part_rows.count() != part_ids.count() ||
                         part_rows.count() != tablet_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(part_ids.count()), K(tablet_ids.count()));
  }
  for (int i = 0; OB_SUCC(ret) && i < part_rows.count(); i++) {
    if (OB_INVALID_PARTITION_ID == part_ids.at(i)
        || ObTabletID::INVALID_TABLET_ID == tablet_ids.at(i).id()) {
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      LOG_WARN("no partition matched", KR(ret), K(part_ids.at(i)), K(tablet_ids.at(i)));
    } else if (OB_FAIL(
          partition_ids.push_back(ObTableLoadPartitionId(part_ids.at(i), tablet_ids.at(i))))) {
      LOG_WARN("fail to push partition id", KR(ret));
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
