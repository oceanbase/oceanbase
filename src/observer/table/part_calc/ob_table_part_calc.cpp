/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_part_calc.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace table
{

int ObTablePartCalculator::init_tb_ctx(const ObSimpleTableSchemaV2 &simple_schema,
                                       const ObITableEntity &entity,
                                       bool need_das_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tb_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tb_ctx_ is null", K(ret));
  } else {
    tb_ctx_->set_entity(&entity);
    tb_ctx_->set_entity_type(ObTableEntityType::ET_KV);
    tb_ctx_->set_operation_type(ObTableOperationType::Type::INSERT); //expect to get an insert plan
    tb_ctx_->set_schema_cache_guard(&kv_schema_guard_);
    tb_ctx_->set_schema_guard(&schema_guard_);
    tb_ctx_->set_simple_table_schema(&simple_schema);
    tb_ctx_->set_sess_guard(&sess_guard_);
    tb_ctx_->set_need_dist_das(true); // only need_dist_das will generate calc_tablet_id expr

    const ObTableApiCredential *credetial = nullptr;
    ObTabletID tablet_id; // unused
    if (OB_FAIL(sess_guard_.get_credential(credetial))) {
      LOG_WARN("fail to get credential", K(ret));
    } else if (OB_FAIL(tb_ctx_->init_common_without_check(const_cast<ObTableApiCredential &>(*credetial), tablet_id, INT64_MAX/*timeout_ts*/))) {
      LOG_WARN("fail to init table ctx common part", K(ret), KPC(credetial));
    } else if (OB_FAIL(tb_ctx_->init_insert())) {
      LOG_WARN("fail to init insert ctx", K(ret), K_(tb_ctx));
    } else if (OB_FAIL(tb_ctx_->init_exec_ctx(need_das_ctx))) {
      LOG_WARN("fail to init exec ctx", K(ret), K_(tb_ctx), K(need_das_ctx));
    } else {
      tb_ctx_->set_init_flag(true);
    }
  }

  return ret;
}

int ObTablePartCalculator::create_plan(const ObSimpleTableSchemaV2 &simple_schema,
                                       const ObITableEntity &entity,
                                       bool need_das_ctx,
                                       ObTableApiSpec *&spec)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tb_ctx_)) {
    if (OB_ISNULL(tb_ctx_ = OB_NEWx(ObTableCtx, (&allocator_), allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc table ctx", K(ret));
    } else if (FALSE_IT(tb_ctx_->set_is_part_calc(true))) {
    } else if (OB_FAIL(init_tb_ctx(simple_schema, entity, need_das_ctx))) {
      LOG_WARN("fail to init table context", K(ret), K(need_das_ctx));
    }
  }

  ObTableApiSpec *tmp_spec = nullptr;
  ObExprFrameInfo *expr_frame_info = nullptr;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(cache_guard_.init(tb_ctx_))) {
    LOG_WARN("fail to init cache guard", K(ret));
  } else if (OB_FAIL(cache_guard_.get_expr_info(tb_ctx_, expr_frame_info))) {
    LOG_WARN("fail to get expr frame info", K(ret), KPC_(tb_ctx));
  } else if (OB_FAIL(cache_guard_.get_spec<TABLE_API_EXEC_INSERT>(tb_ctx_, tmp_spec))) {
    LOG_WARN("fail to get spec from plan", K(ret));
  } else if (OB_ISNULL(expr_frame_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_frame_info is null", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(*tb_ctx_, *expr_frame_info))) {
    LOG_WARN("fail to alloc exprs memory", K(ret));
  } else {
    spec = tmp_spec;
    tb_ctx_->set_expr_info(expr_frame_info);
  }

  return ret;
}

// 1. refresh new row frame
// 2. calculate tablet id by ObExprCalcPartitionBase::calc_part_and_tablet_id
// 3. clear evaluated flag of exprs for next eval
int ObTablePartCalculator::eval(const ObIArray<ObExpr *> &new_row,
                                ObExpr &part_id_expr,
                                const ObITableEntity &entity,
                                ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObRowkey rowkey = entity.get_rowkey();

  if (OB_ISNULL(tb_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table context is not init", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::refresh_rowkey_exprs_frame(*tb_ctx_, new_row, rowkey))) {
    LOG_WARN("fail to refresh rowkey frame", K(ret), K(entity));
  } else {
    ObEvalCtx eval_ctx(tb_ctx_->get_exec_ctx());
    ObObjectID partition_id = OB_INVALID_ID;
    if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(&part_id_expr,
                                                                 eval_ctx,
                                                                 partition_id,
                                                                 tablet_id))) {
      LOG_WARN("calc part and tablet id by expr failed", K(ret));
    } else {
      clear_evaluated_flag();
    }
  }

  return ret;
}

int ObTablePartCalculator::construct_series_entity(const ObITableEntity &entity,
                                                   ObTableEntity &series_entity)
{
  int ret = OB_SUCCESS;
  const ObRowkey &src_rowkey = entity.get_rowkey();
  const ObObj *src_obj_ptr = src_rowkey.get_obj_ptr();
  const int64_t obj_cnt = src_rowkey.get_obj_cnt();
  ObObj *tmp_objs = reinterpret_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * obj_cnt));

  if (OB_ISNULL(tmp_objs)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc tmp object", K(ret), K(obj_cnt));
  } else {
    // input is KQT,change to KTS
    tmp_objs[0] = src_obj_ptr[0]; // K
    tmp_objs[1] = src_obj_ptr[2]; // T
    tmp_objs[2].set_int(0); // S
    ObRowkey rowkey(tmp_objs, obj_cnt);
    if (OB_FAIL(series_entity.set_rowkey(rowkey))) {
      LOG_WARN("fail to set series rowkey for series_entity", K(ret), K(entity), K(rowkey));
    }
  }

  return ret;
}

void ObTablePartCalculator::clear_evaluated_flag()
{
  ObEvalCtx eval_ctx(tb_ctx_->get_exec_ctx());
  ObExprFrameInfo *expr_info = const_cast<ObExprFrameInfo *>(tb_ctx_->get_expr_frame_info());
  if (OB_NOT_NULL(expr_info)) {
    for (int64_t i = 0; i < expr_info->rt_exprs_.count(); i++) {
      expr_info->rt_exprs_.at(i).clear_evaluated_flag(eval_ctx);
    }
  }
}

// 1. get or create an insert plan for part_id_expr
// 2. eval part_id_expr to claculate tablet_id
int ObTablePartCalculator::calc(const ObSimpleTableSchemaV2 &simple_schema,
                                const ObITableEntity &entity,
                                ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  bool need_das_ctx = false;
  const ObITableEntity *calc_entity = &entity;
  ObTableEntity series_entity;
  bool is_series_mode = kv_schema_guard_.get_hbase_mode_type() == ObHbaseModeType::OB_HBASE_SERIES_TYPE;

  if (is_series_mode) {
    if (OB_FAIL(construct_series_entity(entity, series_entity))) {
      LOG_WARN("fail to construct series entity", K(ret), K(entity));
    } else {
      calc_entity = &series_entity;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(spec_) && OB_FAIL(create_plan(simple_schema, *calc_entity, need_das_ctx, spec_))) {
    LOG_WARN("fail to create plan", K(ret));
  } else if (OB_ISNULL(spec_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret), K_(tb_ctx));
  } else {
    ObTableApiInsertSpec *ins_spec = static_cast<ObTableApiInsertSpec*>(spec_);
    if (ins_spec->get_ctdefs().count() < 1) { // count of ctdef greater than 1 when there has global index
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan is unexpected", K(ret), K_(tb_ctx), K(ins_spec->get_ctdefs().count()));
    } else {
      const ObTableInsCtDef *ins_ctdef = nullptr;
      const uint64_t table_id = simple_schema.get_table_id();
      for (int i = 0; i < ins_spec->get_ctdefs().count() && OB_ISNULL(ins_ctdef); i++) {
        if (table_id == ins_spec->get_ctdefs().at(i)->das_ctdef_.table_id_) {
          ins_ctdef = ins_spec->get_ctdefs().at(i);
        }
      }
      if (OB_ISNULL(ins_ctdef) || OB_ISNULL(ins_ctdef->new_part_id_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null element", K(ret), KPC(ins_ctdef));
      } else if (OB_FAIL(eval(ins_ctdef->new_row_, *ins_ctdef->new_part_id_expr_, *calc_entity, tablet_id))) {
        LOG_WARN("fail to eval tablet id", K(ret), KPC(ins_ctdef), K(*calc_entity));
      }
    }
  }

  return ret;
}

int ObTablePartCalculator::calc(const ObTableSchema &table_schema,
                                const ObITableEntity &entity,
                                ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTableEntity series_entity;
  const ObITableEntity *calc_entity = &entity;
  bool is_series_mode = kv_schema_guard_.get_hbase_mode_type() == ObHbaseModeType::OB_HBASE_SERIES_TYPE;
  uint64_t table_id = table_schema.get_table_id();
  ObPartitionLevel part_level = table_schema.get_part_level();
  ObNewRow part_row;
  ObNewRow subpart_row;
  ObDASTabletMapper tablet_mapper;
  tablet_mapper.set_table_schema(&table_schema);
  if (ObPartitionLevel::PARTITION_LEVEL_ZERO == part_level) {
    tablet_id = table_schema.get_tablet_id();
  } else if (is_series_mode) {
    if (OB_FAIL(construct_series_entity(entity, series_entity))) {
      LOG_WARN("fail to construct series entity", K(ret), K(entity));
    } else {
      calc_entity = &series_entity;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(construct_part_row(table_schema, *calc_entity, part_row, subpart_row))) {
    LOG_WARN("fail to construct part row", K(ret), K(entity));
  } else {
    ObObjectID part_id;
    ObObjectID subpart_id;
    ObTabletID tmp_tablet_id;
    if (OB_FAIL(calc_partition_id(ObPartitionLevel::PARTITION_LEVEL_ONE,
                                        OB_INVALID_ID,
                                        part_row,
                                        tablet_mapper,
                                        tmp_tablet_id,
                                        part_id,
                                        table_schema.is_hash_like_part()))) {
      LOG_WARN("fail to calc first partiton id", K(ret), K(part_row));
    } else if (ObPartitionLevel::PARTITION_LEVEL_ONE == part_level) {
      if (tmp_tablet_id.is_valid()) {
        tablet_id = tmp_tablet_id;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet id", K(ret), K(tmp_tablet_id), K(part_row));
      }
    } else if (ObPartitionLevel::PARTITION_LEVEL_TWO == part_level) {
      if (OB_FAIL(calc_partition_id(ObPartitionLevel::PARTITION_LEVEL_TWO,
                                    part_id,
                                    subpart_row,
                                    tablet_mapper,
                                    tablet_id,
                                    subpart_id,
                                    table_schema.is_hash_like_subpart()))) {
        LOG_WARN("fail to calc second partiton id", K(ret), K(subpart_row), K(part_id));
      }
    }
  }
  LOG_DEBUG("ObTablePartCalculator::calc", K(ret), K(table_id), K(part_level),
          K(part_row), K(subpart_row), KP(tb_ctx_), K(tablet_id));

  return ret;
}

int ObTablePartCalculator::calc_partition_id(const ObPartitionLevel part_level,
                                             const ObObjectID part_id,
                                             const common::ObNewRow &row,
                                             sql::ObDASTabletMapper &tablet_mapper,
                                             common::ObTabletID &tablet_id,
                                             ObObjectID &object_id,
                                             const bool is_hash_like)
{
  int ret = OB_SUCCESS;
  if (is_hash_like) {
    ObObj hash_obj;
    ObNewRow hash_obj_row;
    if (row.get_count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row count is not expected", K(ret), K(row));
    } else if (OB_FAIL(get_hash_like_object(row.get_cell(0), hash_obj))) {
      LOG_WARN("fail to get hash like obj", K(row), K(hash_obj));
    } else if (FALSE_IT(hash_obj_row.assign(&hash_obj, 1))) {
    } else if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(part_level,
                                                              part_id,
                                                              hash_obj_row,
                                                              tablet_id,
                                                              object_id))) {
      LOG_WARN("fail to get partition id", K(ret), K(hash_obj), K(part_level));
    }
  } else if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(part_level,
                                                            part_id, row,
                                                            tablet_id,
                                                            object_id))) {
    LOG_WARN("fail to get partition id", K(ret), K(row));
  }
  return ret;
}

int ObTablePartCalculator::construct_part_row(const ObTableSchema &table_schema,
                                              const ObITableEntity &entity,
                                              ObNewRow &part_row,
                                              ObNewRow &subpart_row)
{
  int ret = OB_SUCCESS;
  const ObPartitionKeyInfo &key_info = table_schema.get_partition_key_info();
  const ObPartitionKeyInfo &sub_key_info = table_schema.get_subpartition_key_info();
  ObSEArray<uint64_t, 1> part_col_ids;
  ObSEArray<uint64_t, 1> subpart_col_ids;
  part_col_ids.set_attr(ObMemAttr(MTL_ID(), "PartColIds"));
  subpart_col_ids.set_attr(ObMemAttr(MTL_ID(), "SubPartColIds"));

  if (OB_FAIL(get_part_column_ids(key_info, part_col_ids))) {
    LOG_WARN("fail to get part key column ids", K(ret), K(key_info));
  } else if (OB_FAIL(get_part_column_ids(sub_key_info, subpart_col_ids))) {
    LOG_WARN("fail to get sub part key column ids", K(ret), K(sub_key_info));
  } else if (OB_FAIL(construct_part_row(table_schema, entity, part_col_ids, part_row))) {
    LOG_WARN("fail to construct part row", K(ret));
  } else if (OB_FAIL(construct_part_row(table_schema, entity, subpart_col_ids, subpart_row))) {
    LOG_WARN("fail to construct sub part row", K(ret));
  }
  return ret;
}

int ObTablePartCalculator::construct_part_row(const ObTableSchema &table_schema,
                                              const ObITableEntity &entity,
                                              const ObIArray<uint64_t> &col_ids,
                                              ObNewRow &part_row)
{
  int ret = OB_SUCCESS;
  const int64_t part_key_count = col_ids.count();
  if (part_key_count == 0) {
    // do nothing
  } else {
    ObObj *part_key_objs = nullptr;
    if (OB_ISNULL(part_key_objs = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * part_key_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for part key objs", K(ret), K(part_key_count));
    } else {
      part_row.assign(part_key_objs, part_key_count);
      for (int i = 0; i < part_key_count && OB_SUCC(ret); i++) {
        const uint64_t col_id = col_ids.at(i);
        const ObTableColumnInfo *col_info = nullptr;
        if (OB_FAIL(kv_schema_guard_.get_column_info(col_id, col_info))) {
          LOG_WARN("fail to get column info", K(ret), K(col_id));
        } else if (OB_ISNULL(col_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column info is null", K(ret), K(col_id));
        } else if (!col_info->is_generated_column()) {
          if (col_info->is_rowkey_column_) {
            if (col_info->col_idx_ >= entity.get_rowkey_size()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("rowkey index is out of range", K(ret), K(col_info->col_idx_), K(entity.get_rowkey_size()));
            } else if (OB_FAIL(entity.get_rowkey_value(col_info->col_idx_, part_row.get_cell(i)))) {
              LOG_WARN("fail to get rowkey value", K(ret), K(col_info->col_idx_), K(entity), K(i));
            }
          } else if (OB_FAIL(entity.get_property(col_info->column_name_, part_row.get_cell(i)))) {
            LOG_WARN("fail to get property value", K(ret), K(col_info->column_name_), K(entity), K(i));
          }
        } else { // part column is generated column
          if (OB_FAIL(calc_generated_col(table_schema, entity, *col_info, false /* need_das_ctx*/, part_row.get_cell(i)))) {
            LOG_WARN("fail to calc generated column", K(ret), K(entity), KPC(col_info));
          }
        }
        if (OB_SUCC(ret)) {
          part_row.get_cell(i).set_collation_type(col_info->type_.get_collation_type());
          part_row.get_cell(i).set_collation_level(col_info->type_.get_collation_level());
          part_row.get_cell(i).set_scale(col_info->type_.get_scale());
        }
      } // end for
    }
  }

  return ret;
}

int ObTablePartCalculator::get_simple_schema(uint64_t table_id,
                                             const ObSimpleTableSchemaV2 *&simple_schema)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *tmp_simple_schema = nullptr;

  if (OB_NOT_NULL(simple_schema_) && simple_schema_->get_table_id() == table_id) {
    simple_schema = simple_schema_;
  } else if (OB_FAIL(schema_guard_.get_simple_table_schema(MTL_ID(), table_id, tmp_simple_schema))) {
    LOG_WARN("fail to get simple schema", K(ret), K(table_id));
  } else if (OB_ISNULL(tmp_simple_schema) || tmp_simple_schema->get_table_id() == OB_INVALID_ID) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(table_id));
  } else {
    simple_schema = tmp_simple_schema;
    simple_schema_ = simple_schema;
  }

  return ret;
}

int ObTablePartCalculator::get_table_schema(uint64_t table_id,
                                            const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *tmp_table_schema = nullptr;

  if (OB_NOT_NULL(table_schema_) && table_schema_->get_table_id() == table_id) {
    table_schema = table_schema_;
  } else if (OB_FAIL(schema_guard_.get_table_schema(MTL_ID(), table_id, tmp_table_schema))) {
    LOG_WARN("fail to get simple schema", K(ret), K(table_id));
  } else if (OB_ISNULL(tmp_table_schema) || tmp_table_schema->get_table_id() == OB_INVALID_ID) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(table_id));
  } else {
    table_schema = tmp_table_schema;
    table_schema_ = table_schema;
  }

  return ret;
}

int ObTablePartCalculator::get_simple_schema(const ObString table_name,
                                             const ObSimpleTableSchemaV2 *&simple_schema)
{
  int ret = OB_SUCCESS;
  const ObTableApiCredential *credetial = nullptr;
  const ObSimpleTableSchemaV2 *tmp_simple_schema = nullptr;

  if (OB_NOT_NULL(simple_schema_) && table_name.case_compare(simple_schema_->get_table_name_str()) == 0) {
    simple_schema = simple_schema_;
  } else if (OB_FAIL(sess_guard_.get_credential(credetial))) {
    LOG_WARN("fail to get credential", K(ret));
  } else if (OB_FAIL(schema_guard_.get_simple_table_schema(credetial->tenant_id_,
                                                           credetial->database_id_,
                                                           table_name,
                                                           false, /* is_index */
                                                           tmp_simple_schema))) {
    LOG_WARN("fail to get simple schema", K(ret), KPC(credetial), K(table_name));
  } else if (OB_ISNULL(tmp_simple_schema) || tmp_simple_schema->get_table_id() == OB_INVALID_ID) {
    ret = OB_TABLE_NOT_EXIST;
    ObString db("");
    LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE, table_name.length(), table_name.ptr(), db.length(), db.ptr());
    LOG_WARN("table not exist", K(ret), KPC(credetial), K(table_name));
  } else {
    simple_schema = tmp_simple_schema;
    simple_schema_ = simple_schema;
  }

  return ret;
}

int ObTablePartCalculator::get_table_schema(const ObString table_name,
                                            const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  const ObTableApiCredential *credetial = nullptr;
  const ObTableSchema *tmp_table_schema = nullptr;

  if (OB_NOT_NULL(table_schema_) && table_name.case_compare(table_schema_->get_table_name_str()) == 0) {
    table_schema = table_schema_;
  } else if (OB_FAIL(sess_guard_.get_credential(credetial))) {
    LOG_WARN("fail to get credential", K(ret));
  } else if (OB_FAIL(schema_guard_.get_table_schema(credetial->tenant_id_,
                                                    credetial->database_id_,
                                                    table_name,
                                                    false, /* is_index */
                                                    tmp_table_schema))) {
    LOG_WARN("fail to get simple schema", K(ret), KPC(credetial), K(table_name));
  } else if (OB_ISNULL(tmp_table_schema) || tmp_table_schema->get_table_id() == OB_INVALID_ID) {
    ret = OB_TABLE_NOT_EXIST;
    ObString db("");
    LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE, table_name.length(), table_name.ptr(), db.length(), db.ptr());
    LOG_WARN("table not exist", K(ret), KPC(credetial), K(table_name));
  } else {
    table_schema = tmp_table_schema;
    table_schema_ = table_schema;
  }

  return ret;
}

int ObTablePartCalculator::calc(uint64_t table_id,
                                const ObITableEntity &entity,
                                ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (!table_schema->is_partitioned_table()) {
    tablet_id = table_schema->get_tablet_id();
  } else if (OB_FAIL(calc(*table_schema, entity, tablet_id))) {
    LOG_WARN("fail to calc part", K(ret), K(entity));
  }

  LOG_DEBUG("ObTablePartCalculator::calc", K(ret), K(table_id), K(entity), K(tablet_id),
              K(table_schema->is_partitioned_table()), K(table_schema->get_table_name()));
  return ret;
}

int ObTablePartCalculator::calc(const ObString table_name,
                                const ObITableEntity &entity,
                                ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;

  if (OB_FAIL(get_table_schema(table_name, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_name));
  } else if (!table_schema->is_partitioned_table()) {
    tablet_id = table_schema->get_tablet_id();
  } else if (OB_FAIL(calc(*table_schema, entity, tablet_id))) {
    LOG_WARN("fail to calc part", K(ret), K(entity));
  }

  return ret;
}

int ObTablePartCalculator::calc(const share::schema::ObSimpleTableSchemaV2 *simple_schema,
                                ObHCfRows &same_cf_rows,
                                bool &is_same_ls,
                                ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
  bool is_partitioned_table = true;
  bool is_secondary_table = false;
  is_same_ls = true;
  ObLSID first_ls_id(ObLSID::INVALID_LS_ID);
  if (OB_ISNULL(same_cf_rows.tablet_set_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tabelt_set", K(ret), K(same_cf_rows));
  } else if (!simple_schema->is_partitioned_table()) {
    tablet_id = simple_schema->get_tablet_id();
    is_partitioned_table = false;
    same_cf_rows.tablet_set_->emplace(tablet_id.id());
  } else if (simple_schema->get_part_level() == PARTITION_LEVEL_TWO) {
    is_secondary_table = true;
  }
  // ObHCfRows -> [ObHCfRow, ...]  ObHCfRow -> [ObHCell, ...]
  for (int i = 0; OB_SUCC(ret) && i < same_cf_rows.count(); i++) {
    ObHCfRow &cf_row = same_cf_rows.get_cf_row(i);
    if (!cf_row.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cf row is invalid", K(ret));
    } else {
      bool is_same_timestamp = cf_row.is_same_timestamp_;
      bool can_use_first_tablet_id = (is_partitioned_table && !is_secondary_table)
                                     || (is_secondary_table && is_same_timestamp);
      bool is_cache_hit = false;
      ObLSID sec_ls_id(ObLSID::INVALID_LS_ID);
      for (int j = 0; OB_SUCC(ret) && j < cf_row.cells_.count(); j++) {
        // calculate or reuse tablet_id for this cell
        bool need_get_ls_id = false;
        bool reuse_tablet_id = false;
        ObHCell &hcell = cf_row.cells_.at(j);
        if (!hcell.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("hcell is invalid", K(ret));
        } else if (!is_partitioned_table) {
          // do nothing
        } else if (j > 0 && can_use_first_tablet_id) {
          reuse_tablet_id = true;
          tablet_id = cf_row.cells_.at(0).get_tablet_id();
        } else if (OB_FAIL(calc(*simple_schema, hcell, tablet_id))) {
          LOG_WARN("fail to calc part", K(ret), K(hcell));
        } else {
          // for non-partitioned table, only one tablet has been accessed
          // for partitioned table, record tablet_id in set
          same_cf_rows.tablet_set_->emplace(tablet_id.id());
        }
        if (OB_SUCC(ret)) {
          hcell.set_tablet_id(tablet_id);
          // situations that need to get ls_id
            // 1. non-partitioned table first to get ls_id, this cf_rows will access only one tablet
            // 2. partitioned table and this cell does not reuse the tablet_id calculated before
          need_get_ls_id = is_same_ls && ((!is_partitioned_table && !first_ls_id.is_valid())
                           || (is_partitioned_table && !reuse_tablet_id));
          if (need_get_ls_id) {
            if (OB_FAIL(GCTX.location_service_->get(MTL_ID(),
                                                   tablet_id,
                                                   0, /* expire_renew_time */
                                                   is_cache_hit,
                                                   sec_ls_id))) {
              LOG_WARN("fail to get ls id", K(ret), K(MTL_ID()), K(tablet_id));
            } else {
              if (!first_ls_id.is_valid()) {
                first_ls_id = sec_ls_id;
              } else if (sec_ls_id != first_ls_id) {
                is_same_ls = false;
              }
            }
          }
        }
      } // end for
    }
  } // end for

  if (OB_SUCC(ret) && is_same_ls) {
    ls_id = first_ls_id;
  }
  return ret;
}

int ObTablePartCalculator::calc(const ObTableSchema &table_schema,
                                const ObIArray<ObITableEntity*> &entities,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
  for (int i = 0; i < entities.count() && OB_SUCC(ret); i++) {
    ObITableEntity *entity = entities.at(i);
    if (OB_ISNULL(entity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entity is null", K(ret));
    } else if (OB_FAIL(calc(table_schema, *entity, tablet_id))) {
      LOG_WARN("fail to calc part", K(ret), K(entity));
    } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids), K(tablet_id));
    }
  }

  return ret;
}

int ObTablePartCalculator::calc(const ObSimpleTableSchemaV2 &simple_schema,
                                const ObIArray<const ObITableEntity*> &entities,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
  for (int i = 0; i < entities.count() && OB_SUCC(ret); i++) {
    const ObITableEntity *entity = entities.at(i);
    if (OB_ISNULL(entity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entity is null", K(ret));
    } else if (OB_FAIL(calc(simple_schema, *entity, tablet_id))) {
      LOG_WARN("fail to calc part", K(ret), K(entity));
    } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids), K(tablet_id));
    }
  }

  return ret;
}

int ObTablePartCalculator::calc(uint64_t table_id,
                                const ObIArray<const ObITableEntity*> &entities,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  tablet_ids.reset();
  if (OB_FAIL(get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (!table_schema->is_partitioned_table()) {
    if (OB_FAIL(tablet_ids.push_back(table_schema->get_tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids));
    }
  } else if (OB_FAIL(calc(*table_schema, entities, tablet_ids))) {
    LOG_WARN("fail to calc part", K(ret), K(entities));
  }

  return ret;
}

int ObTablePartCalculator::calc(const ObString table_name,
                                const ObIArray<const ObITableEntity*> &entities,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  tablet_ids.reset();
  if (OB_FAIL(get_table_schema(table_name, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_name));
  } else if (!table_schema->is_partitioned_table()) {
    if (OB_FAIL(tablet_ids.push_back(table_schema->get_tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids));
    }
  } else if (OB_FAIL(calc(*table_schema, entities, tablet_ids))) {
    LOG_WARN("fail to calc part", K(ret), K(entities));
  }

  return ret;
}

int ObTablePartCalculator::get_part_column_ids(const ObPartitionKeyInfo &key_info,
                                               ObIArray<uint64_t> &col_ids)
{
  int ret = OB_SUCCESS;
  const ObRowkeyColumn *key_column = nullptr;

  for (int i = 0; OB_SUCC(ret) && i < key_info.get_size(); ++i) {
    if (OB_ISNULL(key_column = key_info.get_column(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("key column is NULL", K(ret), K(i));
    } else if (OB_FAIL(col_ids.push_back(key_column->column_id_))) {
      LOG_WARN("fail to push back part key column id", K(ret));
    }
  }

  return ret;
}

int ObTablePartCalculator::construct_entity(const ObObj *objs,
                                            int64_t obj_cnt,
                                            ObITableEntity &entity)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_num = 0;
  bool is_series_mode = kv_schema_guard_.get_hbase_mode_type() == ObHbaseModeType::OB_HBASE_SERIES_TYPE;

  if (OB_ISNULL(objs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs is null", K(ret), K(obj_cnt));
  } else if (OB_FAIL(kv_schema_guard_.get_rowkey_column_num(rowkey_num))) {
    LOG_WARN("fail to get rowkey num", K(ret));
  } else if (obj_cnt != rowkey_num) { // not support padding now, coming soon
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid obj count", K(ret), K(obj_cnt), K(rowkey_num));
  } else { // no need padding
    if (is_series_mode) {
      if (obj_cnt < 3) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid object count", K(ret), K(obj_cnt));
      } else {
        ObObj *tmp_objs = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * obj_cnt));
        if (OB_ISNULL(tmp_objs)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc tmp object", K(ret), K(obj_cnt));
        } else {
          // input is KQT,change to KTS
          tmp_objs[0] = objs[0]; // K
          tmp_objs[1] = objs[2]; // T
          tmp_objs[2].set_int(0); // no use
          ObRowkey rowkey(tmp_objs, obj_cnt);
          if (OB_FAIL(entity.set_rowkey(rowkey))) {
            LOG_WARN("fail to set series rowkey for entity", K(ret), K(entity), K(rowkey));
          }
        }
      }
    } else {
      ObRowkey rowkey(const_cast<ObObj *>(objs), obj_cnt);
      if (OB_FAIL(entity.set_rowkey(rowkey))) {
        LOG_WARN("fail to set rowkey for entity", K(ret), K(entity), K(rowkey));
      }
    }
  }

  return ret;
}

int ObTablePartCalculator::check_param(const ObTableColumnInfo &col_info,
                                       const ObIArray<ObExpr *> &new_row,
                                       ObRowkey &rowkey,
                                       bool &is_min,
                                       bool &is_max)
{
  int ret = false;
  is_min = false;
  is_max = false;
  const ObIArray<uint64_t> &cascaded_column_ids = col_info.cascaded_column_ids_;
  for (int64_t i = 0; i < cascaded_column_ids.count() && OB_SUCC(ret) && !is_min && !is_max; i++) {
    uint64_t col_id = cascaded_column_ids.at(i);
    const ObTableColumnInfo *col_info = nullptr;
    if (OB_FAIL(kv_schema_guard_.get_column_info(col_id, col_info))) {
      LOG_WARN("fail to get column info", K(ret), K(col_id));
    } else if (OB_ISNULL(col_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column info is null", K(ret), K(col_id));
    } else if (new_row.count() < col_info->col_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new row too short", K(ret), K(new_row), K(col_info->col_idx_));
    } else {
      ObExpr *expr = new_row.at(col_info->col_idx_);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(new_row), K(col_info->col_idx_));
      } else {
        ObDatum *tmp_datum = nullptr;
        ObEvalCtx eval_ctx(tb_ctx_->get_exec_ctx());
        if (OB_FAIL(expr->eval(eval_ctx, tmp_datum))) {
          LOG_WARN("fail to eval expr", K(ret));
        } else if (OB_ISNULL(tmp_datum)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tmp_datum is null", K(ret), K(new_row), K(col_info->col_idx_));
        } else if (col_info->col_idx_ >= rowkey.get_obj_cnt()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column idx", K(ret), K(col_info), K(rowkey));
        } else if (rowkey.get_obj_ptr()[col_info->col_idx_].is_min_value()) {
          is_min = true;
        } else if (rowkey.get_obj_ptr()[col_info->col_idx_].is_max_value()) {
          is_max = true;
        }
        clear_evaluated_flag();
      }
    }
  }

  return ret;
}

int ObTablePartCalculator::eval(const ObIArray<ObExpr *> &new_row,
                                const ObITableEntity &entity,
                                ObExpr &expr,
                                const ObTableColumnInfo &col_info,
                                ObObj &result)
{
  int ret = OB_SUCCESS;
  bool is_min = false;
  bool is_max = false;
  ObRowkey rowkey = entity.get_rowkey();

  if (OB_ISNULL(tb_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table context is not init", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::refresh_rowkey_exprs_frame(*tb_ctx_, new_row, rowkey))) {
    LOG_WARN("fail to refresh rowkey frame", K(ret), K(entity));
  } else if (OB_FAIL(check_param(col_info, new_row, rowkey, is_min, is_max))) {
    LOG_WARN("fail to check generate column param", K(ret), K(col_info), K(new_row), K(rowkey));
  } else if (is_min) {
    result.set_min_value();
  } else if (is_max) {
    result.set_max_value();
  } else {
    const ObObjMeta &meta = col_info.type_;
    ObDatum *tmp_datum = nullptr;
    ObObj tmp_obj;
    ObEvalCtx eval_ctx(tb_ctx_->get_exec_ctx());
    if (OB_FAIL(expr.eval(eval_ctx, tmp_datum))) {
      LOG_WARN("fail to eval expr", K(ret));
    } else if (OB_FAIL(tmp_datum->to_obj(tmp_obj, meta))) {
      LOG_WARN("fail to convert datum to object", K(ret), K(tmp_datum), K(meta));
    } else if (OB_FAIL(ob_write_obj(allocator_, tmp_obj, result))) {
      LOG_WARN("fail to ob_write_obj", K(ret), K(tmp_obj));
    } else {
      clear_evaluated_flag();
    }
  }

  return ret;
}

int ObTablePartCalculator::calc_generated_col(const ObSimpleTableSchemaV2 &simple_schema,
                                              const ObITableEntity &entity,
                                              const ObTableColumnInfo &col_info,
                                              bool need_das_ctx,
                                              ObObj &gen_col_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(spec_) && OB_FAIL(create_plan(simple_schema, entity, need_das_ctx, spec_))) {
    LOG_WARN("fail to create plan", K(ret));
  } else if (OB_ISNULL(spec_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret), K_(tb_ctx));
  } else {
    ObTableApiInsertSpec *ins_spec = static_cast<ObTableApiInsertSpec*>(spec_);
    if (ins_spec->get_ctdefs().count() < 1) { // count of ctdef greater than 1 when there has global index
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan is unexpected", K(ret), K_(tb_ctx), K(ins_spec->get_ctdefs().count()));
    } else {
      const ObTableInsCtDef *ins_ctdef = nullptr;
      const uint64_t table_id = simple_schema.get_table_id();
      for (int i = 0; i < ins_spec->get_ctdefs().count() && OB_ISNULL(ins_ctdef); i++) {
        if (table_id == ins_spec->get_ctdefs().at(i)->das_ctdef_.table_id_) {
          ins_ctdef = ins_spec->get_ctdefs().at(i);
        }
      }
      const ObIArray<ObExpr *> &new_row = ins_ctdef->new_row_;
      if (new_row.count() <= col_info.col_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid new row expr count", K(ret), K(new_row), K(col_info));
      } else {
        ObExpr *gen_expr = new_row.at(col_info.col_idx_);
        if (OB_ISNULL(gen_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("gen expr is null", K(ret));
        } else if (OB_FAIL(eval(new_row, entity, *gen_expr, col_info, gen_col_value))) {
          LOG_WARN("fail to eval generated expr by start_entity", K(ret), K(entity), KPC(gen_expr));
        }
      }
    }
  }
  return ret;
}


// 1. 构造 insert 执行计划
// 2. 找到生成列在 new row 中的位置
// 3. eval 生成列
int ObTablePartCalculator::calc_generated_col(const ObSimpleTableSchemaV2 &simple_schema,
                                              const ObNewRange &range,
                                              const ObTableColumnInfo &col_info,
                                              ObObj &start,
                                              ObObj &end)
{
  int ret = OB_SUCCESS;
  bool need_das_ctx = true;
  ObTableEntity start_entity;
  ObTableEntity end_entity;

  if (OB_FAIL(construct_entity(range.start_key_.get_obj_ptr(), range.start_key_.get_obj_cnt(), start_entity))) {
    LOG_WARN("fail to construnct start entity", K(ret), K(range));
  } else if (OB_FAIL(construct_entity(range.end_key_.get_obj_ptr(), range.end_key_.get_obj_cnt(), end_entity))) {
    LOG_WARN("fail to construnct end entity", K(ret), K(range));
  } else if (OB_FAIL(calc_generated_col(simple_schema, start_entity, col_info, need_das_ctx, start))) {
    LOG_WARN("fail to calc generated col", K(ret), K(start_entity), K(col_info));
  } else if (OB_FAIL(calc_generated_col(simple_schema, end_entity, col_info, need_das_ctx, end))) {
    LOG_WARN("fail to calc generated col", K(ret), K(end_entity), K(col_info));
  } else if (start.can_compare(end)) {
    if (start > end) {
      ObObj tmp = start;
      start = end;
      end = tmp;
    }
  }

  return ret;
}

// 1. 分区键是普通列，直接从 range 中获取
//   1.1 找到分区键在 range 中的位置，获取对应的 ObObj
// 2. 分区键是生成列，需要找到 new row 中的生成列进行计算
//   2.1 需要从 range 中取值刷生成列依赖的列
int ObTablePartCalculator::construct_part_range(const ObTableSchema &table_schema,
                                                const ObNewRange &range,
                                                const ObIArray<uint64_t> &col_ids,
                                                ObNewRange &part_range)
{
  int ret = OB_SUCCESS;
  const int64_t part_key_count = col_ids.count();

  if (part_key_count == 0) {
    // do nothing
  } else {
    ObObj *start = NULL;
    ObObj *end = NULL;
    if (OB_ISNULL(start = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * part_key_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for start_obj", K(ret), K(part_key_count));
    } else if (OB_ISNULL(end = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * part_key_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for end_obj", K(ret), K(part_key_count));
    } else {
      part_range.start_key_.assign(start, part_key_count);
      part_range.end_key_.assign(end, part_key_count);

      for (int i = 0; i < part_key_count && OB_SUCC(ret); i++) {
        const uint64_t col_id = col_ids.at(i);
        const ObTableColumnInfo *col_info = nullptr;
        if (OB_FAIL(kv_schema_guard_.get_column_info(col_id, col_info))) {
          LOG_WARN("fail to get column info", K(ret), K(col_id));
        } else if (OB_ISNULL(col_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column info is null", K(ret), K(col_id));
        } else if (!col_info->is_generated_column()) {
          if (range.start_key_.get_obj_cnt() <= col_info->col_idx_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid start range", K(ret), K(col_id));
          } else if (range.end_key_.get_obj_cnt() <= col_info->col_idx_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid end range", K(ret), K(col_id));
          } else {
            start[i] = range.start_key_.get_obj_ptr()[col_info->col_idx_];
            end[i] = range.end_key_.get_obj_ptr()[col_info->col_idx_];
          }
        } else { // part column is generated column
          if (OB_FAIL(calc_generated_col(table_schema, range, *col_info, start[i], end[i]))) {
            LOG_WARN("fail to calc generated column", K(ret), K(range), KPC(col_info));
          }
        }

        if (OB_SUCC(ret)) {
          start[i].set_collation_type(col_info->type_.get_collation_type());
          start[i].set_collation_level(col_info->type_.get_collation_level());
          start[i].set_scale(col_info->type_.get_scale());
          end[i].set_collation_type(col_info->type_.get_collation_type());
          end[i].set_collation_level(col_info->type_.get_collation_level());
          end[i].set_scale(col_info->type_.get_scale());
        }
      }
    }
  }

  return ret;
}

int ObTablePartCalculator::construct_part_range(const ObTableSchema &table_schema,
                                                const ObNewRange &range,
                                                ObNewRange &part_range,
                                                ObNewRange &subpart_range)
{
  int ret = OB_SUCCESS;
  ObPartitionLevel part_level = table_schema.get_part_level();

  if (OB_ISNULL(range.start_key_.get_obj_ptr()) || OB_ISNULL(range.end_key_.get_obj_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(ret), KPC(range.start_key_.get_obj_ptr()), KPC(range.end_key_.get_obj_ptr()));
  } else if (OB_UNLIKELY(range.start_key_.get_obj_cnt() != range.end_key_.get_obj_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count error", K(ret), K(range.start_key_.get_obj_cnt()), K(range.end_key_.get_obj_cnt()));
  } else {
    const ObPartitionKeyInfo &key_info = table_schema.get_partition_key_info();
    const ObPartitionKeyInfo &sub_key_info = table_schema.get_subpartition_key_info();
    ObSEArray<uint64_t, 1> part_col_ids;
    ObSEArray<uint64_t, 1> subpart_col_ids;
    part_col_ids.set_attr(ObMemAttr(MTL_ID(), "PartColIds"));
    subpart_col_ids.set_attr(ObMemAttr(MTL_ID(), "SubPartColIds"));

    if (OB_FAIL(get_part_column_ids(key_info, part_col_ids))) {
      LOG_WARN("fail to get part key column ids", K(ret), K(key_info));
    } else if (OB_FAIL(get_part_column_ids(sub_key_info, subpart_col_ids))) {
      LOG_WARN("fail to get sub part key column ids", K(ret), K(sub_key_info));
    } else if (OB_FAIL(construct_part_range(table_schema, range, part_col_ids, part_range))) {
      LOG_WARN("fail to construct part range", K(ret), K(part_col_ids), K(range));
    } else if (OB_FAIL(construct_part_range(table_schema, range, subpart_col_ids, subpart_range))) {
      LOG_WARN("fail to construct sub part range", K(ret), K(subpart_col_ids), K(range));
    } else {

    }
  }

  return ret;
}

int ObTablePartCalculator::get_hash_like_object(const ObObj &part_obj,
                                                ObObj &hash_obj)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(part_obj.get_type(),
                                                               part_obj.get_collation_type(),
                                                               part_obj.get_scale(),
                                                               is_oracle_mode,
                                                               part_obj.has_lob_header());
  ObDatum part_datum;
  uint64_t hash_value = 0;
  if (OB_FAIL(part_datum.from_obj(part_obj))) {
    LOG_WARN("fail to from obj", K(ret), K(part_obj));
  } else if (OB_ISNULL(basic_funcs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("basic_funcs is null", K(ret), K(part_obj));
  } else if (OB_FAIL(basic_funcs->murmur_hash_(part_datum, hash_value, hash_value))) {
    LOG_WARN("hash value failed", K(ret), K(part_datum));
  } else {
    int64_t result_num = static_cast<int64_t>(hash_value);
    result_num = result_num < 0 ? -result_num : result_num;
    hash_obj.set_int(result_num);
  }

  return ret;
}

int ObTablePartCalculator::get_hash_like_tablet_id(ObDASTabletMapper &tablet_mapper,
                                                   ObPartitionLevel part_level,
                                                   const ObNewRange &part_range,
                                                   ObObjectID part_id,
                                                   ObIArray<ObTabletID> &tablet_ids,
                                                   ObIArray<ObObjectID> &part_ids)
{
  int ret = OB_SUCCESS;

  if (part_range.get_start_key().get_obj_cnt() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj count is empty", K(ret));
  } else {
    const ObObj &part_obj = part_range.get_start_key().get_obj_ptr()[0];
    ObObj hash_obj;
    if (OB_FAIL(get_hash_like_object(part_obj, hash_obj))) {
      LOG_WARN("fail to get hash like object", K(ret), K(part_obj));
    } else if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(part_level,
                                                              part_id,
                                                              hash_obj,
                                                              tablet_ids,
                                                              part_ids))) {
      LOG_WARN("fail to get partition ids", K(ret), K(hash_obj), K(part_id), K(part_level));
    }
  }

  return ret;
}

int ObTablePartCalculator::calc(const ObTableSchema &table_schema,
                                const ObNewRange &part_range,
                                const ObNewRange &subpart_range,
                                ObDASTabletMapper &tablet_mapper,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObPartitionLevel part_level = table_schema.get_part_level();
  ObSEArray<ObObjectID, 4> part_ids;
  ObSEArray<ObObjectID, 4> subpart_ids;
  ObSEArray<ObTabletID, 4> tmp_tablet_ids;
  part_ids.set_attr(ObMemAttr(MTL_ID(), "PartIds"));
  subpart_ids.set_attr(ObMemAttr(MTL_ID(), "SubPartIds"));
  tmp_tablet_ids.set_attr(ObMemAttr(MTL_ID(), "TmpPartIds"));
  // TODO: In HBase scenarios, each partition level has only one partition key,
  // so we only need to check if start_key == end_key to determine if it's a single partition, no need to check border_flag
  // Later if table also goes through here, we need to push down the running mode here
  bool is_part_single = is_single_rowkey(part_range) && part_range.get_start_key().get_obj_cnt() == 1;
  bool is_subpart_single = is_single_rowkey(subpart_range) && subpart_range.get_start_key().get_obj_cnt() == 1;

  if (is_part_single && table_schema.is_hash_like_part()) {
    if (OB_FAIL(get_hash_like_tablet_id(tablet_mapper,
                                        ObPartitionLevel::PARTITION_LEVEL_ONE,
                                        part_range,
                                        OB_INVALID_ID/*part_id*/,
                                        tmp_tablet_ids,
                                        part_ids))) {
      LOG_WARN("fail to get hash like tablet id", K(ret), K(part_range));
    }
  } else if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(ObPartitionLevel::PARTITION_LEVEL_ONE,
                                                            OB_INVALID_ID,
                                                            part_range,
                                                            tmp_tablet_ids,
                                                            part_ids))) {
    LOG_WARN("fail to get first partition ids", K(ret), K(part_range));
  }

  if (OB_FAIL(ret)) {
  } else if (ObPartitionLevel::PARTITION_LEVEL_ONE == part_level) {
    if (OB_FAIL(tablet_ids.assign(tmp_tablet_ids))) {
      LOG_WARN("fail to assign tablet id", K(ret), K(tablet_ids), K(tmp_tablet_ids));
    }
  } else {
    // get sub partitions under the first partitions
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); i++) {
      ObObjectID part_id = part_ids.at(i);
      if (is_subpart_single && table_schema.is_hash_like_subpart()) {
        if (OB_FAIL(get_hash_like_tablet_id(tablet_mapper,
                                            ObPartitionLevel::PARTITION_LEVEL_TWO,
                                            subpart_range,
                                            part_id,
                                            tablet_ids,
                                            subpart_ids))) {
          LOG_WARN("fail to get sub hash like tablet id", K(ret), K(subpart_range), K(part_id));
        }
      } else if (OB_FAIL((tablet_mapper.get_tablet_and_object_id(ObPartitionLevel::PARTITION_LEVEL_TWO,
                                                                 part_id,
                                                                 subpart_range,
                                                                 tablet_ids,
                                                                 subpart_ids)))) {
        LOG_WARN("fail to get sub partition ids", K(ret), K(part_id), K(subpart_range));
      }
    }
  }

  return ret;
}

int ObTablePartCalculator::calc(const ObTableSchema &table_schema,
                                const ObNewRange &range,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = table_schema.get_table_id();
  ObPartitionLevel part_level = table_schema.get_part_level();
  ObNewRange part_range;
  part_range.border_flag_ = range.border_flag_;
  ObNewRange subpart_range;
  subpart_range.border_flag_ = range.border_flag_;
  ObDASTabletMapper tablet_mapper;
  tablet_mapper.set_table_schema(&table_schema);
  tablet_ids.reset();
  if (ObPartitionLevel::PARTITION_LEVEL_ZERO == part_level) {
    if (OB_FAIL(tablet_ids.push_back(table_schema.get_tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids));
    }
  } else if (OB_FAIL(construct_part_range(table_schema, range, part_range, subpart_range))) {
    LOG_WARN("fail to construct part range", K(ret), K(range));
  } else if (OB_NOT_NULL(tb_ctx_)) {
    if (OB_FAIL(tb_ctx_->get_exec_ctx().get_das_ctx().get_das_tablet_mapper(table_id, tablet_mapper))) {
      LOG_WARN("fail to get das tablet mapper", K(ret), K(table_id));
    } else if (OB_FAIL(calc(table_schema, part_range, subpart_range, tablet_mapper, tablet_ids))) {
      LOG_WARN("fail to calc tablet id", K(ret), K(part_range), K(subpart_range));
    }
  } else {
    SMART_VAR(ObExecContext, exec_ctx, allocator_) {
      ObSqlCtx sql_ctx;
      ObPhysicalPlanCtx phy_plan_ctx(allocator_);
      sql_ctx.schema_guard_ = &schema_guard_;
      sql_ctx.session_info_ = &sess_guard_.get_sess_info();
      exec_ctx.set_my_session(&sess_guard_.get_sess_info());
      exec_ctx.set_sql_ctx(&sql_ctx);
      exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
      exec_ctx.get_das_ctx().set_sql_ctx(&sql_ctx);
      if (OB_FAIL(exec_ctx.get_das_ctx().get_das_tablet_mapper(table_id, tablet_mapper))) {
        LOG_WARN("fail to get das tablet mapper", K(ret), K(table_id));
      } else if (OB_FAIL(calc(table_schema, part_range, subpart_range, tablet_mapper, tablet_ids))) {
        LOG_WARN("fail to calc tablet id", K(ret), K(part_range), K(subpart_range));
      }
    }
  }
  LOG_DEBUG("ObTablePartCalculator::calc", K(ret), K(table_id), K(part_level),
    K(range), K(part_range), K(subpart_range), KP(tb_ctx_), K(tablet_ids));

  return ret;
}

int ObTablePartCalculator::calc(uint64_t table_id,
                                const ObNewRange &range,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  tablet_ids.reset();
  if (OB_FAIL(get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (!table_schema->is_partitioned_table()) {
    if (OB_FAIL(tablet_ids.push_back(table_schema->get_tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids));
    }
  } else if (OB_FAIL(calc(*table_schema, range, tablet_ids))) {
    LOG_WARN("fail to calc part", K(ret), K(range));
  }

  LOG_DEBUG("ObTablePartCalculator::calc",
    K(ret), K(table_id), K(range), K(tablet_ids), K(table_schema->get_table_name()));

  return ret;
}

int ObTablePartCalculator::calc(const ObString table_name,
                                const ObNewRange &range,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  tablet_ids.reset();
  if (OB_FAIL(get_table_schema(table_name, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_name));
  } else if (!table_schema->is_partitioned_table()) {
    if (OB_FAIL(tablet_ids.push_back(table_schema->get_tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids));
    }
  } else if (OB_FAIL(calc(*table_schema, range, tablet_ids))) {
    LOG_WARN("fail to calc part", K(ret), K(range));
  } else if (OB_FAIL(clip(*table_schema, tablet_ids))) {
    LOG_WARN("fail to clip part", K(ret), K(tablet_ids));
  }

  return ret;
}

int ObTablePartCalculator::calc(uint64_t table_id,
                                const common::ObIArray<ObNewRange> &ranges,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  tablet_ids.reset();
  if (OB_FAIL(get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (!table_schema->is_partitioned_table()) {
    if (OB_FAIL(tablet_ids.push_back(table_schema->get_tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids));
    }
  } else {
    for (int i = 0; i < ranges.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(calc(*table_schema, ranges.at(i), tablet_ids))) {
        LOG_WARN("fail to calc part", K(ret), K(i), K(ranges.at(i)));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(clip(*table_schema, tablet_ids))) {
      LOG_WARN("fail to clip part", K(ret), K(tablet_ids));
    }
  }

  return ret;
}

int ObTablePartCalculator::calc(const ObString table_name,
                                const common::ObIArray<ObNewRange> &ranges,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  tablet_ids.reset();
  if (OB_FAIL(get_table_schema(table_name, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_name));
  } else if (!table_schema->is_partitioned_table()) {
    if (OB_FAIL(tablet_ids.push_back(table_schema->get_tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids));
    }
  } else {
    for (int i = 0; i < ranges.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(calc(*table_schema, ranges.at(i), tablet_ids))) {
        LOG_WARN("fail to calc part", K(ret), K(i), K(ranges.at(i)));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(clip(*table_schema, tablet_ids))) {
      LOG_WARN("fail to clip part", K(ret), K(tablet_ids));
    }
  }

  return ret;
}

bool ObTablePartCalculator::is_single_rowkey(const common::ObNewRange &range) const
{
  bool bret = false;
  ObRowkey start_key = range.get_start_key();
  ObRowkey end_key = range.get_end_key();
  if (start_key.is_min_row() || start_key.is_max_row()
      || end_key.is_min_row() || end_key.is_max_row()) {
    bret = false;
  } else if (start_key == end_key) { // ignore border flag
    bret = true;
  }
  return bret;
}

int ObTablePartCalculator::clip(const ObSimpleTableSchemaV2 &simple_schema,
                                ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;

  if (clip_type_ == ObTablePartClipType::NONE) {
    // do nothing
  } else {
    ObSEArray<ObTabletID, 32> src_tablet_ids;
    if (OB_FAIL(src_tablet_ids.assign(tablet_ids))) {
      LOG_WARN("fail to assign tablet_ids", K(ret), K(tablet_ids));
    } else {
      tablet_ids.reset();
      if (OB_FAIL(ObTablePartClipper::clip(simple_schema, clip_type_, src_tablet_ids, tablet_ids))) {
        LOG_WARN("fail to clip partition", K(ret), K_(clip_type), K(src_tablet_ids));
      }
    }
  }

  return ret;
}

} // end namespace table
} // end namespace oceanbase
