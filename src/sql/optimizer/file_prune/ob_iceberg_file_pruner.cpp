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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_iceberg_file_pruner.h"

#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/table_format/iceberg/scan/conversions.h"
#include "sql/table_format/iceberg/scan/delete_file_index.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

#define BASE_YEAR 1970
/**
 * TODO list
 * 1. 各种类型计算 murmurhash3 的结果跟Iceberg对齐
 * 2. transform的表达式实现
 */

namespace oceanbase
{
namespace sql
{
OB_DEF_SERIALIZE(ObPartFieldBound)
{
  int ret = OB_SUCCESS;
  int64_t count = bounds_.count();
  LST_DO_CODE(OB_UNIS_ENCODE, column_id_, transform_type_, is_whole_range_, is_always_false_, count);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_ISNULL(bounds_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null field bound");
    } else {
      OB_UNIS_ENCODE(*bounds_.at(i));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPartFieldBound)
{
  int64_t len = 0;
  int64_t count = bounds_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN, column_id_, transform_type_, is_whole_range_, is_always_false_, count);
  for (int64_t i = 0; i < count; ++i) {
    if (OB_NOT_NULL(bounds_.at(i))) {
      OB_UNIS_ADD_LEN(*bounds_.at(i));
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObPartFieldBound)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  LST_DO_CODE(OB_UNIS_DECODE, column_id_, transform_type_, is_whole_range_, is_always_false_, count);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObFieldBound *bound = OB_NEWx(ObFieldBound, &allocator_);
    if (OB_ISNULL(bound)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObFieldBound");
    } else {
      OB_UNIS_DECODE(*bound);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(bounds_.push_back(bound))) {
        LOG_WARN("failed to push back bound");
      }
    }
  }
  return ret;
}

ObPartFieldBound::ObPartFieldBound(common::ObIAllocator &allocator)
: allocator_(allocator),
  column_id_(OB_INVALID_ID),
  is_whole_range_(false),
  is_always_false_(false),
  bounds_(allocator)
{}

void ObPartFieldBound::reset()
{
  column_id_ = OB_INVALID_ID;
  is_whole_range_ = false;
  is_always_false_ = false;
  for (int64_t i = 0; i < bounds_.count(); ++i) {
    if (OB_NOT_NULL(bounds_.at(i))) {
      allocator_.free(bounds_.at(i));
    }
  }
  bounds_.reset();
}

int ObPartFieldBound::assign(const ObPartFieldBound &other)
{
  int ret = OB_SUCCESS;
  if (this != &other){
    column_id_ = other.column_id_;
    is_whole_range_ = other.is_whole_range_;
    is_always_false_ = other.is_always_false_;
    if (OB_FAIL(bounds_.assign(other.bounds_))) {
      LOG_WARN("failed to assign field bound");
    }
  }
  return ret;
}

int ObPartFieldBound::deep_copy(ObPartFieldBound &src)
{
  int ret = OB_SUCCESS;
  column_id_ = src.column_id_;
  is_whole_range_ = src.is_whole_range_;
  is_always_false_ = src.is_always_false_;
  if (OB_FAIL(bounds_.init(src.bounds_.count()))) {
    LOG_WARN("failed to init fixed array");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src.bounds_.count(); ++i) {
    ObFieldBound *bound = OB_NEWx(ObFieldBound, &allocator_);
    if (OB_ISNULL(src.bounds_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null filed bound");
    } else if (OB_ISNULL(bound)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObFieldBound");
    } else if (OB_FAIL(bound->deep_copy(allocator_, *src.bounds_.at(i)))) {
      LOG_WARN("failed to deep copy field bound");
    } else if (OB_FAIL(bounds_.push_back(bound))) {
      LOG_WARN("failed to push back bound");
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObIcebergPartBound)
{
  int ret = OB_SUCCESS;
  int64_t count = part_field_bounds_.count();
  OB_UNIS_ENCODE(count);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_ISNULL(part_field_bounds_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null part field bound");
    } else {
      OB_UNIS_ENCODE(*part_field_bounds_.at(i));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObIcebergPartBound)
{
  int64_t len = 0;
  int64_t count = part_field_bounds_.count();
  OB_UNIS_ADD_LEN(count);
  for (int64_t i = 0; i < count; ++i) {
    if (OB_NOT_NULL(part_field_bounds_.at(i))) {
      OB_UNIS_ADD_LEN(*part_field_bounds_.at(i));
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObIcebergPartBound)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObPartFieldBound *field_bound = OB_NEWx(ObPartFieldBound, &allocator_, allocator_);
    if (OB_ISNULL(field_bound)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObPartFieldBound");
    } else {
      OB_UNIS_DECODE(*field_bound);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(part_field_bounds_.push_back(field_bound))) {
        LOG_WARN("failed to push back part filed bound");
      }
    }
  }
  return ret;
}

ObIcebergPartBound::ObIcebergPartBound(common::ObIAllocator &allocator)
: allocator_(allocator),
  part_field_bounds_(&allocator)
{}

void ObIcebergPartBound::reset()
{
  for (int64_t i = 0; i < part_field_bounds_.count(); ++i) {
    if (OB_NOT_NULL(part_field_bounds_.at(i))) {
      part_field_bounds_.at(i)->reset();
      allocator_.free(part_field_bounds_.at(i));
    }
  }
  part_field_bounds_.reset();
}

int ObIcebergPartBound::assign(const ObIcebergPartBound &other)
{
  int ret = OB_SUCCESS;
  if (this != &other){
    if (OB_FAIL(part_field_bounds_.assign(other.part_field_bounds_))) {
      LOG_WARN("failed to assign part field bounds");
    }
  }
  return ret;
}

int ObIcebergPartBound::deep_copy(ObIcebergPartBound &src)
{
  int ret = OB_SUCCESS;
  if (this != &src){
    if (OB_FAIL(part_field_bounds_.init(src.part_field_bounds_.count()))) {
      LOG_WARN("failed to init fixed array");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src.part_field_bounds_.count(); ++i) {
      ObPartFieldBound *part_field_bound = OB_NEWx(ObPartFieldBound, &allocator_, allocator_);
      if (OB_ISNULL(src.part_field_bounds_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null part filed bound");
      } else if (OB_ISNULL(part_field_bound)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for ObFieldBound");
      } else if (OB_FAIL(part_field_bound->deep_copy(*src.part_field_bounds_.at(i)))) {
        LOG_WARN("failed to deep copy part field bound");
      } else if (OB_FAIL(part_field_bounds_.push_back(part_field_bound))) {
        LOG_WARN("failed to push back part field bound");
      }
    }
  }
  return ret;
}

ObIcebergFilePrunner::ObIcebergFilePrunner(common::ObIAllocator &allocator)
: ObILakeTableFilePruner(allocator, PrunnerType::ICEBERG),
  part_column_descs_(allocator_),
  part_bound_(allocator_)
{}

void ObIcebergFilePrunner::reset()
{
  ObILakeTableFilePruner::reset();
  part_column_descs_.reset();
  for (int64_t i = 0; i < part_bound_.count(); ++i) {
    part_bound_.at(i).second->reset();
  }
  part_bound_.reset();
}

int ObIcebergFilePrunner::assign(const ObILakeTableFilePruner &o)
{
  int ret = OB_SUCCESS;
  if (this != &o) {
    reset();
    const ObIcebergFilePrunner &other = static_cast<const ObIcebergFilePrunner &>(o);
    if (OB_FAIL(ObILakeTableFilePruner::assign(o))) {
      LOG_WARN("failed to assign lake table file pruner");
    } else if (OB_FAIL(part_column_descs_.assign(other.part_column_descs_))) {
      LOG_WARN("failed to assign part column ids");
    } else {
      if (other.part_bound_.count() > 0) {
        OZ(part_bound_.init(other.part_bound_.count()));
        ObIcebergPartBound *bound = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < other.part_bound_.count(); i++) {
          if (OB_ISNULL(bound = OB_NEWx(ObIcebergPartBound, &allocator_, allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate part bound");
          } else {
            OZ(bound->deep_copy(*other.part_bound_.at(i).second));
            OZ(part_bound_.push_back(std::make_pair(other.part_bound_.at(i).first, bound)));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    inited_ = false;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObIcebergFilePrunner)
{
  int ret = OB_SUCCESS;
  int64_t part_bound_count = part_bound_.count();
  int64_t column_id_count = column_ids_.count();
  int64_t column_meta_count = column_metas_.count();
  int64_t part_column_desc_count = part_column_descs_.count();
  LST_DO_CODE(OB_UNIS_ENCODE, inited_, is_partitioned_, need_all_, loc_meta_, part_bound_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < part_bound_count; i++) {
    LST_DO_CODE(OB_UNIS_ENCODE, part_bound_.at(i).first, *part_bound_.at(i).second);
  }
  OB_UNIS_ENCODE_ARRAY(column_ids_, column_id_count);
  OB_UNIS_ENCODE_ARRAY(column_metas_, column_meta_count);
  OB_UNIS_ENCODE_ARRAY(part_column_descs_, part_column_desc_count);
  OB_UNIS_ENCODE(file_filter_spec_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObIcebergFilePrunner)
{
  int64_t len = 0;
  int64_t part_bound_count = part_bound_.count();
  int64_t column_id_count = column_ids_.count();
  int64_t column_meta_count = column_metas_.count();
  int64_t part_column_desc_count = part_column_descs_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN, inited_, is_partitioned_, need_all_, loc_meta_, part_bound_count);
  for (int64_t i = 0; i < part_bound_count; i++) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, part_bound_.at(i).first, *part_bound_.at(i).second);
  }
  OB_UNIS_ADD_LEN_ARRAY(column_ids_, column_id_count);
  OB_UNIS_ADD_LEN_ARRAY(column_metas_, column_meta_count);
  OB_UNIS_ADD_LEN_ARRAY(part_column_descs_, part_column_desc_count);
  OB_UNIS_ADD_LEN(file_filter_spec_);
  return len;
}

OB_DEF_DESERIALIZE(ObIcebergFilePrunner)
{
  int ret = OB_SUCCESS;
  int64_t part_bound_count = 0;
  int64_t column_id_count = 0;
  int64_t column_meta_count = 0;
  int64_t part_column_desc_count = 0;
  LST_DO_CODE(OB_UNIS_DECODE, inited_, is_partitioned_, need_all_, loc_meta_, part_bound_count);

  if (OB_SUCC(ret)) {
    if (part_bound_count == 0) {
      // do nothing
    } else if (OB_FAIL(part_bound_.init(part_bound_count))) {
      LOG_WARN("failed to prepare allocate for part bound");
    } else {
      ObIcebergPartBound *bound = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_bound_count; i++) {
        if (OB_ISNULL(bound = OB_NEWx(ObIcebergPartBound, &allocator_, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate part bound");
        } else {
          int32_t spec_id = 0;
          LST_DO_CODE(OB_UNIS_DECODE, spec_id, *bound);
          OZ(part_bound_.push_back(std::make_pair(spec_id, bound)));
        }
      }
    }
  }

  OB_UNIS_DECODE(column_id_count);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(column_ids_.prepare_allocate(column_id_count))) {
      LOG_WARN("failed to prepare allocate column ids");
    } else {
      OB_UNIS_DECODE_ARRAY(column_ids_, column_id_count);
    }
  }

  OB_UNIS_DECODE(column_meta_count);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(column_metas_.prepare_allocate(column_meta_count))) {
      LOG_WARN("failed to prepare allocate column ids");
    } else {
      OB_UNIS_DECODE_ARRAY(column_metas_, column_meta_count);
    }
  }

  OB_UNIS_DECODE(part_column_desc_count);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(part_column_descs_.prepare_allocate(part_column_desc_count))) {
      LOG_WARN("failed to prepare allocate column ids");
    } else {
      OB_UNIS_DECODE_ARRAY(part_column_descs_, part_column_desc_count);
    }
  }

  OB_UNIS_DECODE(file_filter_spec_);
  return ret;
}


int ObIcebergFilePrunner::init(ObSqlSchemaGuard *schema_guard,
                               const ObDMLStmt &stmt,
                               ObExecContext *exec_ctx,
                               const uint64_t table_id,
                               const uint64_t ref_table_id,
                               const ObIArray<iceberg::PartitionSpec *> &partition_specs,
                               const ObIArray<ObRawExpr*> &filter_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("table location init twice");
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table scehema", K(ref_table_id));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_schema), K(exec_ctx));
  } else {
    loc_meta_.table_loc_id_ = table_id;
    loc_meta_.ref_table_id_ = ref_table_id;
    loc_meta_.route_policy_ = READONLY_ZONE_FIRST;
    loc_meta_.is_external_table_ = true;
    loc_meta_.is_lake_table_ = true;
    loc_meta_.is_external_files_on_disk_ = false;
    is_partitioned_ = !partition_specs.empty();

    if (OB_FAIL(generate_column_meta_info(stmt))) {
      LOG_WARN("failed to generate column meta info");
    } else if (OB_FAIL(genearte_partition_bound(stmt, exec_ctx, table_schema,
                                                partition_specs, filter_exprs))) {
      LOG_WARN("failed to generate partition bound");
    } else if (need_all_) {
      // do nothing
    } else if (OB_FAIL(ObIcebergFileFilter::generate_pd_filter_spec(allocator_, *exec_ctx,
                                                                    &stmt, filter_exprs,
                                                                    file_filter_spec_))) {
      LOG_WARN("failed to generate pd filter spec");
    }
  }
  return ret;
}

int ObIcebergFilePrunner::genearte_partition_bound(const ObDMLStmt &stmt,
                                                   ObExecContext *exec_ctx,
                                                   const ObTableSchema *table_schema,
                                                   const ObIArray<iceberg::PartitionSpec*> &partition_specs,
                                                   const ObIArray<ObRawExpr*> &filter_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<PartColDesc, 16> part_column_descs;
  if (filter_exprs.empty()) {
    need_all_ = true;
  } else if (!is_partitioned_) {
    // do nothing
  } else if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(exec_ctx));
  } else if (OB_FAIL(part_bound_.init(partition_specs.count()))) {
    LOG_WARN("failed to create part bound");
  } else {
    ObArenaAllocator tmp_allocator("FilePrunnerTmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(exec_ctx->get_my_session());
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_specs.count(); ++i) {
      iceberg::PartitionSpec* part_spec = partition_specs.at(i);
      ObIcebergPartBound* part_bound = OB_NEWx(ObIcebergPartBound, &allocator_, allocator_);
      if (OB_ISNULL(part_spec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null part spec");
      } else if (OB_ISNULL(part_bound)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator memory for ObIcebergPartBound");
      } else if (OB_FAIL(part_bound->part_field_bounds_.init(part_spec->fields.count()))) {
        LOG_WARN("failed to init part field bounds");
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < part_spec->fields.count(); ++j) {
        ObQueryRangeArray ranges;
        iceberg::PartitionField *field = part_spec->fields.at(j);
        ObPartFieldBound *part_field_bound = OB_NEWx(ObPartFieldBound, &allocator_, allocator_);
        if (OB_ISNULL(part_field_bound)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocator memory for ObPartFieldBound");
        } else if (OB_ISNULL(field)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null partition filed");
        } else {
          part_field_bound->column_id_ = iceberg::ObIcebergUtils::get_ob_column_id(field->source_id);
          part_field_bound->transform_type_ = field->transform.transform_type;
          if (ObOptimizerUtil::find_item(column_ids_, part_field_bound->column_id_)) {
            ObSEArray<ColumnItem, 1> part_columns;
            ColumnItem *column_item = nullptr;
            tmp_allocator.reuse();
            ObPreRangeGraph pre_range_graph(tmp_allocator);
            bool dummy_single_ranges = false;
            if (OB_ISNULL(column_item = stmt.get_column_item_by_id(loc_meta_.table_loc_id_,
                                                                   part_field_bound->column_id_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null column item", K(loc_meta_), K(part_field_bound->column_id_));
            } else if (OB_FAIL(part_columns.push_back(*column_item))) {
              LOG_WARN("failed to push back column item");
            } else if (iceberg::TransformType::Identity == field->transform.transform_type &&
                      OB_FAIL(part_column_descs.push_back(PartColDesc(part_spec->spec_id, part_field_bound->column_id_, j)))) {
              LOG_WARN("failed to append var to array no dup");
            } else if (OB_FAIL(pre_range_graph.preliminary_extract_query_range(part_columns, filter_exprs, exec_ctx,
                                                                               NULL, NULL, false, true))) {
              LOG_WARN("failed to preliminary extract query range", K(part_columns), K(filter_exprs));
            } else if (OB_FAIL(pre_range_graph.get_tablet_ranges(allocator_, *exec_ctx, ranges,
                                                                 dummy_single_ranges, dtc_params))) {
              LOG_WARN("failed to get tablet ranges");
            } else if (OB_FAIL(transform_bound_by_part_type(field->transform, *column_item,
                                                            ranges, exec_ctx, table_schema))) {
              LOG_WARN("failed to transform bound by part type");
            } else if (OB_FAIL(build_field_bound_from_ranges(ranges, *part_field_bound))) {
              LOG_WARN("failed to build field bound from ranges");
            } else if (ranges.count() == 1) {
              ObNewRange *range = ranges.at(0);
              if (range->is_false_range()) {
                part_field_bound->is_always_false_ = true;
              } else if (range->is_whole_range()) {
                part_field_bound->is_whole_range_ = true;
              }
            }
          } else {
            // partition key has been deleted
            // make whole range
            part_field_bound->is_whole_range_ = true;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(part_bound->part_field_bounds_.push_back(part_field_bound))) {
            LOG_WARN("failed to push back part field bound");
          }
        }
      }
      OZ(part_bound_.push_back(std::make_pair(part_spec->spec_id, part_bound)));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(part_column_descs_.assign(part_column_descs))) {
        LOG_WARN("failed to assign part column ids");
      }
    }
  }
  return ret;
}

int ObIcebergFilePrunner::build_field_bound_from_ranges(ObIArray<ObNewRange*> &ranges,
                                                       ObPartFieldBound &part_field_bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(part_field_bound.bounds_.init(ranges.count()))) {
    LOG_WARN("failed to init fixed array");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    ObFieldBound *field_bound = OB_NEWx(ObFieldBound, &allocator_);
    if (OB_ISNULL(field_bound)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator memory for ObFieldBound");
    } else if (OB_ISNULL(ranges.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null range");
    } else if (OB_FAIL(field_bound->from_range(*ranges.at(i)))) {
      LOG_WARN("failed to init field bound from range");
    } else if (OB_FAIL(part_field_bound.bounds_.push_back(field_bound))) {
      LOG_WARN("failed to push back field bound");
    }
  }
  return ret;
}

int ObIcebergFilePrunner::transform_bound_by_part_type(iceberg::Transform &transform,
                                                      ColumnItem &column_item,
                                                      ObIArray<ObNewRange*> &part_bounds,
                                                      ObExecContext *exec_ctx,
                                                      const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = nullptr;
  if (OB_ISNULL(exec_ctx) || OB_ISNULL(session_info = exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(exec_ctx), K(session_info));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_bounds.count(); ++i)  {
    ObNewRange *range = part_bounds.at(i);
    if (OB_ISNULL(range) ||
        OB_UNLIKELY(1 != range->get_start_key().get_obj_cnt()) ||
        OB_UNLIKELY(1 != range->get_end_key().get_obj_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected range", KPC(range));
    } else if (iceberg::TransformType::Identity == transform.transform_type) {
      // do nothing for identity transform
    } else if (iceberg::TransformType::Bucket == transform.transform_type) {
      if (OB_UNLIKELY(!transform.param.has_value())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bucket transform without param");
      } else if (OB_FAIL(transform_bucket_range(*range, transform.param.value()))) {
        LOG_WARN("failed to transform bucket range");
      }
    } else if (iceberg::TransformType::Truncate == transform.transform_type) {
      if (OB_UNLIKELY(!transform.param.has_value())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("truncate transform without param");
      } else if (OB_FAIL(transform_truncate_range(*range, transform.param.value(), allocator_))) {
        LOG_WARN("failed to transform truncate range");
      }
    } else if (iceberg::TransformType::Year == transform.transform_type ||
               iceberg::TransformType::Month == transform.transform_type ||
               iceberg::TransformType::Day == transform.transform_type ||
               iceberg::TransformType::Hour == transform.transform_type) {
      if (OB_FAIL(transform_time_range(*range, transform.transform_type, session_info))) {
        LOG_WARN("failed to transform time range");
      }
    } else if (iceberg::TransformType::Void == transform.transform_type) {
      if (OB_FAIL(transform_void_range(*range))) {
        LOG_WARN("failed to transform void range");
      }
    }
  }
  return ret;
}

int ObIcebergFilePrunner::transform_bucket_range(ObNewRange &range, const int64_t N)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(N <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected N", K(N));
  } else if (range.is_false_range()) {
    // do nothing
  } else if (range.is_single_rowkey()) {
    // int, long, decimal, date, time, timestamp, timestamptz, timestamp_ns, timestamptz_ns, string, uuid, fixed, binary
    // TODO @yibo murmur hash3
    ObArenaAllocator tmp_allocator;
    ObObj &s_val = range.start_key_.get_obj_ptr()[0];

    // iceberg bucket hash,需要把 int32 变成 int64 后进行 hash
    // 目前支持 bucket 的分区列，底下使用 int32 存储的只有 ObInt32Type 和 ObDateType
    if (ObObjType::ObInt32Type == s_val.get_type() || ObObjType::ObDateType == s_val.get_type()) {
      int32_t tmp_val = s_val.get_int32();
      s_val.set_int(static_cast<int64_t>(tmp_val));
    } else if (ObObjType::ObDecimalIntType == s_val.get_type()) {
      // 以 14.20 为例，ob 存储的是 [0x8c, 0x05, 0x00, 0x00]，小端存储，且因为 DecimalInt 最小长度是
      // sizeof(int32)，所以后面两个字节为 0 但是 iceberg 计算 decimal 的时候，是按照大端计算
      // decimal，也就是 [0x00, 0x00, 0x05, 0x8c] 同时 iceberg 的 decimal 是以最小字节数存储，1420
      // 只需要两个 byte 就能表示 所以 iceberg hash 计算的实际数组是 [0x05, 0x8c] 因此我们需要把
      // ObDecimalInt 的 [0x8c, 0x05, 0x00, 0x00] 先抹去末尾的 0，再翻转(小端变大端)，再 hash
      int32_t decimal_bytes_length = s_val.get_int_bytes();
      // 从后往前数，找到第一个不为 0 的
      int32_t valid_byte_offset = decimal_bytes_length - 1;
      const int8_t *decimal_buf = reinterpret_cast<const int8_t *>(s_val.get_decimal_int());
      if (OB_ISNULL(decimal_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("decimal buf is null", K(s_val));
      } else {
        // 如果是负数，则 empty_flag = 11111111(0xFF)
        const int8_t negative_unused_bytes = 0xFF;
        const int8_t positive_unused_bytes = 0x00;
        bool is_negative = false;
        if ((decimal_buf[decimal_bytes_length - 1] >> 8) == static_cast<int8_t>(0xFF)) {
          is_negative = true;
        } else {
          is_negative = false;
        }
        if (is_negative) {
          bool is_found = false;
          for (; !is_found && valid_byte_offset > 0;) {
            // 从后往前找到了第一个有用的 byte
            if (decimal_buf[valid_byte_offset] != negative_unused_bytes) {
              is_found = true;
            } else {
              valid_byte_offset--;
            }
          }
          // 看一下当前 bytes 是不是正数，如果是正数了，说明我们需要保留前面的 0xFF
          if ((decimal_buf[valid_byte_offset] >> 8) == static_cast<int8_t>(0x00)) {
            valid_byte_offset++; // 保留前面的 0xFF
          }
        } else {
          bool is_found = false;
          for (; !is_found && valid_byte_offset > 0;) {
            // 从后往前找到了第一个有用的 byte
            if (decimal_buf[valid_byte_offset] != positive_unused_bytes) {
              is_found = true;
            } else {
              valid_byte_offset--;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        const int32_t mini_required_bytes = valid_byte_offset + 1;
        char *new_buf = NULL;
        if (OB_ISNULL(new_buf = static_cast<char *>(tmp_allocator.alloc(mini_required_bytes)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(mini_required_bytes));
        } else {
          for (int64_t i = valid_byte_offset; i >= 0; i--) {
            new_buf[valid_byte_offset - i] = decimal_buf[i];
          }
          s_val.set_binary(ObString(mini_required_bytes, new_buf));
        }
      }
    }

    if (OB_SUCC(ret)) {
      uint64_t hash_val = 0;
      if (s_val.is_null()) {
        // do nothing
      } else if (OB_FAIL(s_val.hash_murmur3_x86_32(hash_val))) {
        LOG_WARN("failed to calc murmurhash3_x86_32");
      } else if (OB_UNLIKELY(hash_val > UINT32_MAX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected hash value", K(hash_val));
      } else {
        int32_t final_hash_val = static_cast<int32_t>(hash_val);
        final_hash_val = final_hash_val & INT32_MAX; // 移除负数符号位
        int32_t hash_part = final_hash_val % N;
        s_val.set_int32(hash_part);
        range.end_key_.get_obj_ptr()[0].set_int32(hash_part);
      }
    }
  } else {
    range.set_whole_range();
  }
  return ret;
}

#define CALC_DECIMAL_INT_TRUNCATE(TYPE)               \
  case sizeof(TYPE##_t): {                            \
    const TYPE##_t &l = *(num_val->TYPE##_v_);        \
    const TYPE##_t &r = static_cast<TYPE##_t>(W);     \
    TYPE##_t truncate_val = l - (((l % r) + r) % r);  \
    res_val.from(truncate_val);                       \
    break;                                            \
  }

int ObIcebergFilePrunner::transform_truncate_range(ObNewRange &range,
                                                   const int64_t W,
                                                   ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(W <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected W", K(W));
  } else if (range.is_false_range()) {
    // do nothing
  } else if (range.is_single_rowkey()) {
    ObObj &s_val = range.start_key_.get_obj_ptr()[0];
    ObObj &e_val = range.end_key_.get_obj_ptr()[0];
    if (s_val.is_null()) {
      // do nothing
    } else if (s_val.is_int() || s_val.is_int32()) {
      int64_t int_val = s_val.get_int();
      int64_t truncate_val = int_val - (((int_val % W) + W) % W);
      s_val.set_int_value(truncate_val);
      e_val.set_int_value(truncate_val);
    } else if (s_val.is_number()) {
      //truncate_W(v) = v - (v % scaled_W);  scaled_W = decimal(W, scale(v))
      number::ObNumber num_val = s_val.get_number();
      ObScale scale = s_val.get_scale();
      number::ObNumber scaled_W;
      number::ObNumber n1;
      number::ObNumber n2;
      number::ObNumber truncate_val;
      if (OB_FAIL(scaled_W.from(W, allocator))) {
        LOG_WARN("failed to construnct number");
      } else if (OB_FAIL(wide::to_number(W, scale, allocator, scaled_W))) {
        LOG_WARN("failed to number");
      } else if (OB_FAIL(num_val.rem_v3(scaled_W, truncate_val, allocator))) {
        LOG_WARN("failed to rem numbers", K(num_val), K(scaled_W));
      } else if (!num_val.is_negative()) {
        // do nothing
      } else if (OB_FAIL(truncate_val.add_v3(scaled_W, n1, allocator))) {
        LOG_WARN("failed to add number", K(truncate_val), K(scaled_W));
      } else if (OB_FAIL(n1.rem_v3(scaled_W, n2, allocator))) {
        LOG_WARN("failed to rem numbers", K(n1), K(scaled_W));
      } else if (OB_FAIL(num_val.sub_v3(n2, truncate_val, allocator))) {
        LOG_WARN("failed to sub number");
      }
      if (OB_SUCC(ret)) {
        s_val.set_number_value(truncate_val);
        e_val.set_number_value(truncate_val);
      }
    } else if (s_val.is_decimal_int()) {
      const ObDecimalInt *num_val = s_val.get_decimal_int();
      const int32_t num_val_int_byte = s_val.get_int_bytes();
      ObDecimalIntBuilder res_val;
      switch (num_val_int_byte) {
        CALC_DECIMAL_INT_TRUNCATE(int32)
        CALC_DECIMAL_INT_TRUNCATE(int64)
        CALC_DECIMAL_INT_TRUNCATE(int128)
        CALC_DECIMAL_INT_TRUNCATE(int256)
        CALC_DECIMAL_INT_TRUNCATE(int512)
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("int_bytes is unexpected",  K(num_val_int_byte));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        s_val.set_decimal_int(res_val.get_int_bytes(), s_val.get_scale(),
                              const_cast<ObDecimalInt*>(res_val.get_decimal_int()));
        e_val.set_decimal_int(res_val.get_int_bytes(), s_val.get_scale(),
                              const_cast<ObDecimalInt*>(res_val.get_decimal_int()));
        if (OB_FAIL(ob_write_obj(allocator, s_val, s_val))) {
          LOG_WARN("failed to write obj");
        } else if (OB_FAIL(ob_write_obj(allocator, e_val, e_val))) {
          LOG_WARN("failed to write obj");
        }
      }
    } else if (s_val.is_varchar()) {
      if (W < s_val.get_val_len()) {
        // 获取不超过W bytes的合法utf8mb4字符
        ObString str_val = s_val.get_varchar();
        int64_t well_len = 0;
        if (OB_FAIL(ObCharset::well_formed_len(s_val.get_collation_type(),
                                               str_val.ptr(), W,
                                               well_len))) {
          LOG_WARN("failed to calc well formed length");
        } else {
          s_val.set_val_len(well_len);
          e_val.set_val_len(well_len);
        }
      }
    } else if (s_val.is_varbinary()) {
      if (W < s_val.get_val_len()) {
        s_val.set_val_len(W);
        e_val.set_val_len(W);
      }
    }
  } else {
    range.set_whole_range();
  }
  return ret;
}
#undef CALC_DECIMAL_INT_TRUNCATE

int ObIcebergFilePrunner::transform_time_range(ObNewRange &range,
                                              iceberg::TransformType type,
                                              ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null session info");
  } else if (!range.is_false_range()) {
    ObObj &s_val = range.start_key_.get_obj_ptr()[0];
    ObObj &e_val = range.end_key_.get_obj_ptr()[0];
    if (s_val.is_max_value() || s_val.is_min_value() || s_val.is_null()) {
      // do nothing
    } else {
      ObTime ob_time;
      if (OB_FAIL(obj_to_ob_time(s_val, ob_time, session_info))) {
        LOG_WARN("failed to get ob time from obj");
      } else if (iceberg::TransformType::Year == type) {
        int32_t year_offset = ob_time.parts_[DT_YEAR] - BASE_YEAR;
        s_val.set_int(year_offset);
      } else if (iceberg::TransformType::Month == type) {
        int32_t month_offset = (ob_time.parts_[DT_YEAR] - BASE_YEAR) * 12 + ob_time.parts_[DT_MON] - 1;
        s_val.set_int(month_offset);
      } else if (iceberg::TransformType::Day == type) {
        int32_t day_offset = ob_time.parts_[DT_DATE];
        s_val.set_int(day_offset);
      } else if (iceberg::TransformType::Hour == type) {
        int32_t hour_offset = ob_time.parts_[DT_DATE] * 24 + ob_time.parts_[DT_HOUR] - 1;
        s_val.set_int(hour_offset);
      }
      if (OB_SUCC(ret)) {
        range.border_flag_.set_inclusive_start();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (e_val.is_max_value() || e_val.is_min_value() || e_val.is_null()) {
      // do nothing
    } else {
      ObTime ob_time;
      if (OB_FAIL(obj_to_ob_time(e_val, ob_time, session_info))) {
        LOG_WARN("failed to get ob time from obj");
      } else if (iceberg::TransformType::Year == type) {
        int32_t year_offset = ob_time.parts_[DT_YEAR] - BASE_YEAR;
        e_val.set_int(year_offset);
      } else if (iceberg::TransformType::Month == type) {
        int32_t month_offset = (ob_time.parts_[DT_YEAR] - BASE_YEAR) * 12 + ob_time.parts_[DT_MON] - 1;
        e_val.set_int(month_offset);
      } else if (iceberg::TransformType::Day == type) {
        int32_t day_offset = ob_time.parts_[DT_DATE];
        e_val.set_int(day_offset);
      } else if (iceberg::TransformType::Hour == type) {
        int32_t hour_offset = ob_time.parts_[DT_DATE] * 24 + ob_time.parts_[DT_HOUR] - 1;
        e_val.set_int(hour_offset);
      }
      if (OB_SUCC(ret)) {
        range.border_flag_.set_inclusive_end();
      }
    }
  }
  return ret;
}

int ObIcebergFilePrunner::from_partition_field_summary(ObIAllocator &allocator,
                                                       ObFieldBound &field_bound,
                                                       iceberg::TransformType transform_type,
                                                       ObColumnMeta &column_meta,
                                                       iceberg::PartitionFieldSummary &summary)
{
  int ret = OB_SUCCESS;
  field_bound.contains_null_ = summary.contains_null;
  ObColumnMeta transform_column_meta = column_meta;
  if (OB_FAIL(iceberg::Transform::get_result_type(transform_type,
                                                  column_meta.type_,
                                                  transform_column_meta.type_))) {
    LOG_WARN("failed to get result type");
  }
  if (OB_SUCC(ret)) {
    if (summary.lower_bound.has_value()) {
      field_bound.include_lower_ = true;
      if (OB_FAIL(iceberg::Conversions::convert_statistics_binary_to_ob_obj(
              allocator,
              summary.lower_bound.value(),
              transform_column_meta,
              field_bound.lower_bound_))) {
        LOG_WARN("failed to convert statistic binart to obj");
      }
    } else {
      field_bound.lower_bound_.set_min_value();
    }
  }
  if (OB_SUCC(ret)) {
    if (summary.upper_bound.has_value()) {
      field_bound.include_upper_ = true;
      if (OB_FAIL(iceberg::Conversions::convert_statistics_binary_to_ob_obj(
              allocator,
              summary.upper_bound.value(),
              transform_column_meta,
              field_bound.upper_bound_))) {
        LOG_WARN("failed to convert statistic binart to obj");
      }
    } else {
      field_bound.upper_bound_.set_max_value();
    }
  }
  return ret;
}

int ObIcebergFilePrunner::obj_to_ob_time(ObObj &val,
                                           ObTime &ob_time,
                                           ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (val.is_date()) {
    if (OB_FAIL(ObTimeConverter::date_to_ob_time(val.get_date(), ob_time))) {
      LOG_WARN("failed to convert date to ob time");
    }
  } else if (val.is_mysql_date()) {
    if (OB_FAIL(ObTimeConverter::mdate_to_ob_time(val.get_mysql_date(), ob_time))) {
      LOG_WARN("failed to convert mdatetime to ob time");
    }
  } else if (val.is_datetime()) {
    if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(val.get_datetime(), nullptr, ob_time))) {
      LOG_WARN("failed to convert datetime to ob time");
    }
  } else if (val.is_mysql_datetime()) {
    if (OB_FAIL(ObTimeConverter::mdatetime_to_ob_time(val.get_mysql_datetime(), ob_time))) {
      LOG_WARN("failed to convert mdatetime to ob time");
    }
  } else if (val.is_timestamp()) {
    // const ObTimeZoneInfo *tz_info = session_info->get_timezone_info();
    if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(val.get_timestamp(), nullptr, ob_time))) {
      LOG_WARN("failed to convert datetime to ob time");
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected obj type", K(val.get_type()));
  }
  return ret;
}

int ObIcebergFilePrunner::transform_void_range(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  // void transform会把所有的值转变成NULL, 相当于没有分区, 直接当做whole range处理即可.
  if (!range.is_false_range()) {
    range.set_whole_range();
  }
  return ret;
}

int ObIcebergFilePrunner::prune_manifest_files(ObIArray<iceberg::ManifestFile*> &manifest_list,
                                               ObIArray<iceberg::ManifestFile*> &valid_manifest_list)
{
  int ret = OB_SUCCESS;
  if (need_all_ || !is_partitioned_) {
    if (OB_FAIL(valid_manifest_list.assign(manifest_list))) {
      LOG_WARN("failed to assign manifest list");
    }
  } else {
    ObArenaAllocator tmp_allocator("FilePrunnerTmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
    for (int64_t i = 0; OB_SUCC(ret) && i < manifest_list.count(); ++i) {
      iceberg::ManifestFile* manifest_file = manifest_list.at(i);
      ObIcebergPartBound* part_bound = nullptr;
      bool in_bound = false;
      if (OB_ISNULL(manifest_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null manifest file");
      } else if (OB_ISNULL(part_bound = get_part_bound_by_spec_id(manifest_file->partition_spec_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get part bound");
      } else if (OB_FAIL(check_manifest_file_in_bound(tmp_allocator, *manifest_file, *part_bound, in_bound))) {
        LOG_WARN("failed to check manifest desc in bound");
      } else if (in_bound && OB_FAIL(valid_manifest_list.push_back(manifest_file))) {
        LOG_WARN("failed to push back manifest desc");
      } else {
        tmp_allocator.reuse();
      }
    }
  }
  return ret;
}

int ObIcebergFilePrunner::check_manifest_file_in_bound(ObIAllocator &allocator,
                                                       iceberg::ManifestFile& manifest_file,
                                                       ObIcebergPartBound& part_bound,
                                                       bool &in_bound)
{
  int ret = OB_SUCCESS;
  in_bound = true;
  if (OB_UNLIKELY(part_bound.part_field_bounds_.count() != manifest_file.partitions.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected filed bounds", K(part_bound), K(manifest_file));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && in_bound && i < part_bound.part_field_bounds_.count(); ++i) {
      ObPartFieldBound *part_field_bound = part_bound.part_field_bounds_.at(i);
      iceberg::PartitionFieldSummary *part_field_summary = manifest_file.partitions.at(i);
      if (OB_ISNULL(part_field_bound) || OB_ISNULL(part_field_summary)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", KP(part_field_bound), KP(part_field_summary));
      } else if (part_field_bound->is_always_false_) {
        in_bound = false;
      } else if (part_field_bound->is_whole_range_) {
        in_bound = true;
      } else {
        in_bound = false;
        ObFieldBound manifest_field_bound;
        int64_t idx = 0;
        if (ObOptimizerUtil::find_item(column_ids_, part_field_bound->column_id_, &idx)) {
          if (OB_FAIL(from_partition_field_summary(allocator,
                                                   manifest_field_bound,
                                                   part_field_bound->transform_type_,
                                                   column_metas_.at(idx),
                                                   *part_field_summary))) {
            LOG_WARN("failed to init field bound from partition field summary");
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && !in_bound && j < part_field_bound->bounds_.count(); ++j) {
              ObFieldBound *field_bound = part_field_bound->bounds_.at(j);
              if (OB_ISNULL(field_bound)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get null field bound");
              } else {
                in_bound = field_bound->is_intersect(manifest_field_bound);
              }
            }
          }
        } else {
          in_bound = true;
        }
      }
    }
  }
  return ret;
}

int ObIcebergFilePrunner::prune_data_files(ObExecContext &exec_ctx,
                                           const ObIArray<iceberg::ManifestEntry*> &manifest_entries,
                                           const bool is_hash_aggregate,
                                           hash::ObHashMap<ObLakeTablePartKey, uint64_t> &part_key_map,
                                           ObIArray<ObIcebergFileDesc*> &file_descs)
{
  int ret = OB_SUCCESS;
  ObSEArray<const iceberg::ManifestEntry*, 16> data_entries;
  ObSEArray<const iceberg::ManifestEntry*, 16> delete_entries;
  iceberg::DeleteFileIndex delete_file_index;
  ObIcebergFileFilter file_filter(exec_ctx, file_filter_spec_, &part_column_descs_);
  uint64_t last_part_idx = 0;
  if (!need_all_ && OB_FAIL(file_filter.init(column_ids_, column_metas_))) {
    LOG_WARN("failed to init skip filter executor");
  }
  if (OB_SUCC(ret) && is_hash_aggregate) {
    if (OB_FAIL(part_key_map.create(manifest_entries.count(), "PartKetMap", "LakeTableLoc"))) {
      LOG_WARN("create range set bucket failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < manifest_entries.count(); ++i) {
    iceberg::ManifestEntry* manifest_entry = manifest_entries.at(i);
    ObIcebergPartBound* part_bound = nullptr;
    bool in_bound = false;
    if (OB_ISNULL(manifest_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null manifest entry");
    } else if (!manifest_entry->is_alive()) {
      // 忽略已经被 deleted 的 ManifestEntry
      in_bound = false;
    } else if (need_all_) {
      in_bound = true;
    } else {
      if (!is_partitioned_) {
        in_bound = true;
      } else if (OB_ISNULL(part_bound = get_part_bound_by_spec_id(manifest_entry->partition_spec_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get part bound");
      } else if (OB_FAIL(check_manifest_entry_in_bound(*manifest_entry, *part_bound, in_bound))) {
        LOG_WARN("failed to check manifest desc in bound");
      }
      if (OB_SUCC(ret) && in_bound && manifest_entry->is_data_file() && file_filter.has_pushdown_filter()) {
        bool is_filtered = false;
        if (OB_FAIL(file_filter.check_file(*manifest_entry, is_filtered))) {
          LOG_WARN("failed to check file filter reange");
        } else if (is_filtered) {
          in_bound = false;
        }
      }
    }

    if (OB_SUCC(ret) && in_bound) {
      if (manifest_entry->is_data_file()) {
        ObIcebergFileDesc *file_desc = OB_NEWx(ObIcebergFileDesc, &allocator_, allocator_);
        uint64_t part_idx = OB_INVALID_ID;
        if (OB_ISNULL(file_desc)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for ObIcebergFileDesc");
        } else {
          if (is_hash_aggregate) {
            ObLakeTablePartKey part_key;
            if (OB_UNLIKELY(!part_key_map.created())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("part_key_map is not created", K(ret));
            } else if (OB_FAIL(part_key.from_manifest_entry(manifest_entry))) {
              LOG_WARN("failed to from manifest entry");
            } else if (OB_FAIL(part_key_map.get_refactored(part_key, part_idx))) {
              if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
                part_idx = last_part_idx++;
                if (OB_FAIL(part_key_map.set_refactored(part_key, part_idx))) {
                  LOG_WARN("failed to set part id");
                }
              } else {
                LOG_WARN("failed to get part id bu part key");
              }
            }
          } else {
            // do nothing
            // 生成唯一 part id 开销很大，非必要不创建
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(file_descs.push_back(file_desc))) {
            LOG_WARN("failed to push back manifest entry");
          } else {
            file_desc->part_idx_ = part_idx;
            file_desc->entry_ = manifest_entry;
          }
        }
      } else if (manifest_entry->is_delete_file()) {
        if (OB_FAIL(delete_entries.push_back(manifest_entry))) {
          LOG_WARN("failed to add delete file");
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "vector delete");
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(delete_file_index.init(delete_entries))) {
      LOG_WARN("failed to init delete file index");
    }
  }

  if (OB_SUCC(ret) && !file_descs.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_descs.count(); ++i) {
      ObIcebergFileDesc *file_desc = file_descs.at(i);
      if (OB_ISNULL(file_desc) || OB_ISNULL(file_desc->entry_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", KP(file_desc));
      } else if (OB_FAIL(delete_file_index.match_delete_files(*file_desc->entry_,
                                                              file_desc->delete_files_))) {
        LOG_WARN("failed to match delete files");
      }
    }
  }
  return ret;
}

ObIcebergPartBound* ObIcebergFilePrunner::get_part_bound_by_spec_id(int64_t spec_id)
{
  ObIcebergPartBound *part_bound = NULL;
  for (int64_t j = 0; OB_ISNULL(part_bound) && j < part_bound_.count(); ++j) {
    if (part_bound_.at(j).first == spec_id) {
      part_bound = part_bound_.at(j).second;
    }
  }
  return part_bound;
}

int ObIcebergFilePrunner::check_manifest_entry_in_bound(iceberg::ManifestEntry& manifest_entry,
                                                       ObIcebergPartBound& part_bound,
                                                       bool &in_bound)
{
  int ret = OB_SUCCESS;
  in_bound = true;
  if (OB_UNLIKELY(part_bound.part_field_bounds_.count() != manifest_entry.data_file.partition.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected filed bounds", K(part_bound), K(manifest_entry));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && in_bound && i < part_bound.part_field_bounds_.count(); ++i) {
      ObPartFieldBound *part_field_bound = part_bound.part_field_bounds_.at(i);
      if (OB_ISNULL(part_field_bound)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", KP(part_field_bound));
      } else if (part_field_bound->is_always_false_) {
        in_bound = false;
      } else if (part_field_bound->is_whole_range_) {
        in_bound = true;
      } else {
        in_bound = false;
        ObFieldBound manifest_file_bound;
        if (OB_FAIL(manifest_file_bound.from_data_file_partition(manifest_entry.data_file.partition, i))) {
          LOG_WARN("failed to init field bound from partition field summary");
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && !in_bound && j < part_field_bound->bounds_.count(); ++j) {
            ObFieldBound *field_bound = part_field_bound->bounds_.at(j);
            if (OB_ISNULL(field_bound)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null field bound");
            } else {
              in_bound = field_bound->is_intersect(manifest_file_bound);
            }
          }
        }
      }
    }
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
