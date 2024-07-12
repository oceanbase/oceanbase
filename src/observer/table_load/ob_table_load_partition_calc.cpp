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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_partition_calc.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "observer/table_load/ob_table_load_obj_cast.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"

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
    cast_mode_(CM_NONE),
    is_partition_with_autoinc_(false),
    partition_with_autoinc_idx_(OB_INVALID_INDEX),
    param_(nullptr),
    is_partitioned_(false),
    allocator_("TLD_PartCalc"),
    exec_ctx_(allocator_),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

int ObTableLoadPartitionCalc::init(const ObTableLoadParam &param, sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPartitionCalc init twice", KR(ret), KP(this));
  } else {
    uint64_t tenant_id = param.tenant_id_;
    uint64_t table_id = param.table_id_;
    sql_ctx_.schema_guard_ = &schema_guard_;
    exec_ctx_.set_sql_ctx(&sql_ctx_);
    const ObTableSchema *table_schema = nullptr;
    ObDataTypeCastParams cast_params(session_info->get_timezone_info());
    if (OB_FAIL(time_cvrt_.init(cast_params.get_nls_format(ObDateTimeType)))) {
      LOG_WARN("fail to init time converter", KR(ret));
    } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session_info, cast_mode_))) {
      LOG_WARN("fail to get_default_cast_mode", KR(ret));
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
        param_ = &param;
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
  ObArray<ObColDesc> column_descs;
  column_descs.set_tenant_id(MTL_ID());
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

int ObTableLoadPartitionCalc::get_part_key(const table::ObTableLoadObjRow &row, common::ObNewRow &part_key) const
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, calc_part_time_us);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_key.count_ != part_key_obj_index_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part key count", KR(ret), K(part_key.count_), K(part_key_obj_index_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_key_obj_index_.count(); ++i) {
    const IndexAndType &index_and_type = part_key_obj_index_.at(i);
    const int64_t obj_index = index_and_type.index_;
    if (OB_UNLIKELY(obj_index >= row.count_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid length", KR(ret), K(obj_index), K(row.count_));
    } else {
      part_key.cells_[i] = row.cells_[obj_index];
    }
  }
  return ret;
}

int ObTableLoadPartitionCalc::cast_part_key(common::ObNewRow &part_key, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_key.count_ != part_key_obj_index_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part key count", KR(ret), K(part_key.count_), K(part_key_obj_index_.count()));
  } else {
    ObDataTypeCastParams cast_params(session_info_->get_timezone_info());
    ObCastCtx cast_ctx(&allocator, &cast_params, cast_mode_, ObCharset::get_system_collation());
    ObTableLoadCastObjCtx cast_obj_ctx(*param_, &time_cvrt_, &cast_ctx, true);
    ObObj obj;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_key_obj_index_.count(); ++i) {
      const IndexAndType &index_and_type = part_key_obj_index_.at(i);
      if (OB_FAIL(ObTableLoadObjCaster::cast_obj(cast_obj_ctx, index_and_type.column_schema_,
                                                    part_key.cells_[i], obj))) {
        LOG_WARN("fail to cast obj", KR(ret));
      } else {
        part_key.cells_[i] = obj;
      }
    }
  }
  return ret;
}

int ObTableLoadPartitionCalc::get_partition_by_row(
  ObIArray<ObNewRow> &part_rows, ObIArray<ObTableLoadPartitionId> &partition_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> tablet_ids;
  ObArray<ObObjectID> part_ids;
  tablet_ids.set_tenant_id(MTL_ID());
  part_ids.set_tenant_id(MTL_ID());
  if (OB_FAIL(table_location_.calculate_partition_ids_by_rows2(
               *session_info_, schema_guard_, param_->table_id_, part_rows, tablet_ids, part_ids))) {
    LOG_WARN("fail to calc partition id", KR(ret));
  } else if (OB_UNLIKELY(part_rows.count() != part_ids.count() ||
                         part_rows.count() != tablet_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(part_ids.count()), K(tablet_ids.count()));
  }
  for (int i = 0; OB_SUCC(ret) && i < part_rows.count(); i++) {
    if (OB_FAIL(
          partition_ids.push_back(ObTableLoadPartitionId(part_ids.at(i), tablet_ids.at(i))))) {
      LOG_WARN("fail to push partition id", KR(ret));
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
