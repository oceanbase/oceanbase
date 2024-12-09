/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_table_ls_execute_processor.h"
#include "ob_table_rpc_processor_util.h"
#include "share/table/ob_table.h"
#include "ob_table_query_and_mutate_helper.h"
#include "ob_table_end_trans_cb.h"

using namespace oceanbase::observer;
using namespace oceanbase::table;
using namespace oceanbase::common;


/**
 * ---------------------------------------- ObTableHbaseMutationInfo ----------------------------------------
 */

int ObTableHbaseMutationInfo::init(const schema::ObSimpleTableSchemaV2 *table_schema,
                                   share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    simple_schema_ = table_schema;
    table_id_ = table_schema->get_table_id();
    real_table_name_ = table_schema->get_table_name_str();
    if (OB_FAIL(schema_cache_guard_.init(MTL_ID(),
                                         table_schema->get_table_id(),
                                         table_schema->get_schema_version(),
                                         schema_guard))) {
      LOG_WARN("fail to init shcema_cache_guard", K(ret));
    }
  }

  return ret;
}


/**
 * ---------------------------------------- ObTableLSExecuteP ----------------------------------------
 */

ObTableLSExecuteP::LSExecuteIter::LSExecuteIter(ObTableLSExecuteP &outer_exectute_process)
    : outer_exectute_process_(outer_exectute_process),
      tablet_ops_(),
      tablet_id_(),
      allocator_("LSExecuteIt", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      ops_timestamp_(-ObHTableUtils::current_time_millis())
{}

ObTableLSExecuteP::LSExecuteIter::~LSExecuteIter() {
  for (int i = 0; i < batch_ctxs_.count(); ++i) {
    table::ObTableBatchCtx *batch_ctx = batch_ctxs_.at(i).second;
    if (OB_NOT_NULL(batch_ctx)) {
      batch_ctx->~ObTableBatchCtx();
    }
  }
}

int ObTableLSExecuteP::LSExecuteIter::init()
{
  int ret = OB_SUCCESS;
  UNUSED(ret);
  return ret;
}

int ObTableLSExecuteP::LSExecuteIter::init_tb_ctx(ObTableSingleOp &single_op,
                                                  ObKvSchemaCacheGuard *shcema_cache_guard,
                                                  const ObSimpleTableSchemaV2 *table_schema,
                                                  ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  ObTableOperationType::Type type = single_op.get_op_type();
  tb_ctx.set_entity(&single_op.get_entities().at(0));
  tb_ctx.set_entity_type(outer_exectute_process_.arg_.entity_type_);
  tb_ctx.set_operation_type(type);
  tb_ctx.set_schema_cache_guard(shcema_cache_guard);
  tb_ctx.set_schema_guard(&outer_exectute_process_.schema_guard_);
  tb_ctx.set_simple_table_schema(table_schema);
  tb_ctx.set_sess_guard(&outer_exectute_process_.sess_guard_);
  if (tb_ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_common(outer_exectute_process_.credential_,
                                        tablet_id_,
                                        outer_exectute_process_.get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret));
  } else {
    switch (type) {
      case ObTableOperationType::GET: {
        if (OB_FAIL(tb_ctx.init_get())) {
          LOG_WARN("fail to init get ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::PUT: {
        if (OB_FAIL(tb_ctx.init_put())) {
          LOG_WARN("fail to init put ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT: {
        if (OB_FAIL(tb_ctx.init_insert())) {
          LOG_WARN("fail to init insert ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::DEL: {
        if (OB_FAIL(tb_ctx.init_delete())) {
          LOG_WARN("fail to init delete ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::UPDATE: {
        if (OB_FAIL(tb_ctx.init_update())) {
          LOG_WARN("fail to init update ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT_OR_UPDATE: {
        if (OB_FAIL(tb_ctx.init_insert_up(tablet_ops_.is_use_put()))) {
          LOG_WARN("fail to init insert up ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::REPLACE: {
        if (OB_FAIL(tb_ctx.init_replace())) {
          LOG_WARN("fail to init replace ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::APPEND: {
        if (OB_FAIL(
                tb_ctx.init_append(tablet_ops_.is_returning_affected_entity(), tablet_ops_.is_returning_rowkey()))) {
          LOG_WARN("fail to init append ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INCREMENT: {
        if (OB_FAIL(
                tb_ctx.init_increment(tablet_ops_.is_returning_affected_entity(), tablet_ops_.is_returning_rowkey()))) {
          LOG_WARN("fail to init increment ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected operation type", "type", type);
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_trans(outer_exectute_process_.get_trans_desc(),
                                       outer_exectute_process_.get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret));
  }

  return ret;
}

int ObTableLSExecuteP::LSExecuteIter::init_batch_ctx(uint64_t table_id,
                                                    ObTableSingleOp &single_op,
                                                    ObKvSchemaCacheGuard *shcema_cache_guard,
                                                    const ObSimpleTableSchemaV2 *simple_table_schema,
                                                    ObTableTabletOpResult &tablet_result,
                                                    ObTableBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  // construct batch operation result
  batch_ctx.results_ = &tablet_result;
  uint64_t real_tablet_id = OB_INVALID_ID;
  batch_ctx.result_entity_ = outer_exectute_process_.cb_->get_entity_factory().alloc();  // only use in hbase mutation
  if (OB_ISNULL(batch_ctx.result_entity_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memroy for result_entity", K(ret));
  } else if (OB_FAIL(init_tb_ctx(single_op,
                                 shcema_cache_guard,
                                 simple_table_schema,
                                 batch_ctx.tb_ctx_))) {  // init tb_ctx
    LOG_WARN("fail to init table context", K(ret));
  } else {
    // 构造batch_service需要的入参
    batch_ctx.stat_event_type_ = &outer_exectute_process_.stat_event_type_;
    batch_ctx.trans_param_ = &outer_exectute_process_.trans_param_;
    batch_ctx.entity_type_ = outer_exectute_process_.arg_.entity_type_;
    batch_ctx.consistency_level_ = outer_exectute_process_.arg_.consistency_level_;
    batch_ctx.tablet_id_ = tablet_id_;
    batch_ctx.credential_ = &outer_exectute_process_.credential_;
    batch_ctx.table_id_ = table_id;
    batch_ctx.is_atomic_ = true;    /* batch atomic always true*/
    batch_ctx.is_same_type_ = true; /* one tablet_op batch always same type */
    batch_ctx.entity_factory_ = &outer_exectute_process_.cb_->get_entity_factory();
  }
  return ret;
}

/**
 * ---------------------------------------- HTableLSExecuteIter ----------------------------------------
 */

ObTableLSExecuteP::HTableLSExecuteIter::HTableLSExecuteIter(ObTableLSExecuteP &outer_exectute_process)
    : LSExecuteIter(outer_exectute_process),
      curr_op_index_(0)
{}

int ObTableLSExecuteP::HTableLSExecuteIter::init()
{
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = outer_exectute_process_.arg_.ls_op_;
  table_group_name_ = ls_op.get_table_name();
  if (OB_FAIL(init_multi_schema_info(table_group_name_))) {
    ObString db("");
    if (ret == OB_TABLEGROUP_NOT_EXIST) {
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(db), to_cstring(table_group_name_));
    }
    LOG_WARN("fail to init schema info", K(ret), K(table_group_name_));
  } else if (hbase_infos_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hbase infos count is invalid", K(ret), K(hbase_infos_.count()));
  } else {
    const share::schema::ObSimpleTableSchemaV2 *simple_schema = NULL;
    for (int i = 0; OB_ISNULL(simple_schema) && i < hbase_infos_.count(); ++i) {
      if (hbase_infos_.at(i)->table_id_ == ls_op.get_table_id()) {
        simple_schema = hbase_infos_.at(i)->simple_schema_;
        outer_exectute_process_.schema_cache_guard_ = hbase_infos_.at(i)->schema_cache_guard_;
      }
    }
    if (OB_ISNULL(simple_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null simple_schema", K(ret), K(ls_op.get_table_id()));
    } else {
      outer_exectute_process_.simple_table_schema_ = simple_schema;
    }
  }
  return ret;
}

int ObTableLSExecuteP::HTableLSExecuteIter::init_multi_schema_info(const ObString &table_group_name)
{
  int ret = OB_SUCCESS;
  ObSEArray<const schema::ObSimpleTableSchemaV2 *, 8> table_schemas;
  uint64_t tablegroup_id = OB_INVALID_ID;

  if (outer_exectute_process_.schema_cache_guard_.is_inited()) {
    // skip
  } else if (OB_ISNULL(outer_exectute_process_.gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(outer_exectute_process_.gctx_.schema_service_->get_tenant_schema_guard(
                 outer_exectute_process_.credential_.tenant_id_, outer_exectute_process_.schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(outer_exectute_process_.credential_.tenant_id_));
  } else if (OB_FAIL(outer_exectute_process_.schema_guard_.get_tablegroup_id(
                 outer_exectute_process_.credential_.tenant_id_, table_group_name, tablegroup_id))) {
    LOG_WARN("fail to get table group id", K(ret), K(outer_exectute_process_.credential_.tenant_id_),
             K(outer_exectute_process_.credential_.database_id_), K(table_group_name));
  } else if (OB_FAIL(outer_exectute_process_.schema_guard_.get_table_schemas_in_tablegroup(
                 outer_exectute_process_.credential_.tenant_id_, tablegroup_id, table_schemas))) {
    LOG_WARN("fail to get table schema from table group", K(ret), K(outer_exectute_process_.credential_.tenant_id_),
             K(outer_exectute_process_.credential_.database_id_), K(table_group_name), K(tablegroup_id));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const schema::ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
      ObTableHbaseMutationInfo *mutation_info;
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret));
      } else if (OB_ISNULL(mutation_info = OB_NEWx(ObTableHbaseMutationInfo, &allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memroy for batch_ctx", K(ret));
      } else {
        if (OB_FAIL(mutation_info->init(table_schema, outer_exectute_process_.schema_guard_))) {
          LOG_WARN("fail to init hbase mutation info", K(ret));
        } else if (OB_FAIL(hbase_infos_.push_back(mutation_info))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to push back hbase_info", K(ret));
        }
      }
    }
  }

  return ret;
}

void ObTableLSExecuteP::HTableLSExecuteIter::reset() {
  curr_op_index_ = 0;
  tablet_ops_.reset();
  same_ctx_ops_.reset();
  table_operations_.reset();
}

int ObTableLSExecuteP::HTableLSExecuteIter::modify_htable_quailfier_and_timestamp(const ObTableSingleOp &curr_single_op,
                                                                                  ObTableOperationType::Type type,
                                                                                  int64_t now_ms)
{
  int ret = OB_SUCCESS;
  ObObj qualifier;
  if (curr_single_op.get_entities().count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is empty", K(ret), K(curr_single_op), K(type));
  } else {
    const ObITableEntity &entity = curr_single_op.get_entities().at(0);
    if (OB_FAIL(entity.get_rowkey_value(ObHTableConstants::COL_IDX_Q, qualifier))) {
      LOG_WARN("fail to get qualifier value", K(ret));
    } else if (entity.get_rowkey_size() != ObHTableConstants::HTABLE_ROWKEY_SIZE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable should be with 3 rowkey columns", K(ret), K(entity));
    } else {
      ObRowkey rowkey = entity.get_rowkey();
      ObObj *obj_ptr = rowkey.get_obj_ptr();
      if (OB_ISNULL(obj_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("obj_ptr is nullptr", K(rowkey));
      } else {
        ObObj &t_obj = const_cast<ObObj &>(obj_ptr[ObHTableConstants::COL_IDX_T]);  // column T
        ObHTableCellEntity3 htable_cell(&entity);
        bool row_is_null = htable_cell.last_get_is_null();
        int64_t timestamp = htable_cell.get_timestamp();
        bool timestamp_is_null = htable_cell.last_get_is_null();
        if (row_is_null || timestamp_is_null) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument for htable put", K(ret), K(row_is_null), K(timestamp_is_null));
        } else if (type == ObTableOperationType::INSERT_OR_UPDATE &&
                  ObHTableConstants::LATEST_TIMESTAMP == timestamp) {  // update timestamp iff LATEST_TIMESTAMP
          t_obj.set_int(now_ms);
        }
        if (OB_SUCC(ret)) {
          ObObj &q_obj = const_cast<ObObj &>(obj_ptr[ObHTableConstants::COL_IDX_Q]);  // column Q
          if (qualifier.get_string().after('.').length() == 0) {
            q_obj.set_null();
          } else {
            q_obj.set_string(ObObjType::ObVarcharType, qualifier.get_string().after('.'));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLSExecuteP::HTableLSExecuteIter::convert_batch_ctx(ObTableBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  if (same_ctx_ops_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("same_ctx_ops_ is empty", K(ret), K(same_ctx_ops_));
  } else {
    batch_ctx.is_readonly_ = same_ctx_ops_.at(0).get_op_type() == ObTableOperationType::GET;
    batch_ctx.is_same_properties_names_ = same_ctx_ops_.is_same_properties_names();
    batch_ctx.use_put_ = same_ctx_ops_.is_use_put();
    batch_ctx.returning_affected_entity_ = same_ctx_ops_.is_returning_affected_entity();
    batch_ctx.returning_rowkey_ = same_ctx_ops_.is_returning_rowkey();
    // construct batch operation
    batch_ctx.ops_ = &table_operations_;
    table_operations_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < same_ctx_ops_.count(); i++) {
      const ObTableSingleOp &single_op = same_ctx_ops_.at(i);
      ObTableOperation table_op;
      table_op.set_entity(single_op.get_entities().at(0));
      table_op.set_type(single_op.get_op_type());
      if (OB_FAIL(table_operations_.push_back(table_op))) {
        LOG_WARN("fail to push table operation", K(ret));
      }
    }
    batch_ctx.tb_ctx_.set_entity(&same_ctx_ops_.at(0).get_entities().at(0));
    batch_ctx.tablet_id_ = tablet_id_;
    batch_ctx.tb_ctx_.set_tablet_id(tablet_id_);
    batch_ctx.tb_ctx_.set_operation_type(same_ctx_ops_.at(0).get_op_type());
    batch_ctx.tb_ctx_.set_index_tablet_id(tablet_id_);
    // batch_ctx.tb_ctx_.tsc_rtdef_.scan_rtdef_.table_loc_->get_first_tablet_loc();
    ObExecContext &exec_ctx = batch_ctx.tb_ctx_.get_exec_ctx();
    ObDASCtx &das_ctx = exec_ctx.get_das_ctx();
    ObDASTableLoc *local_table_loc = das_ctx.get_table_loc_by_id(batch_ctx.tb_ctx_.get_ref_table_id(), batch_ctx.table_id_);
    ObDASTabletLoc *tablet_loc = nullptr;
    if (OB_ISNULL(local_table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local_table_loc is NULL", K(ret));
    } else if (OB_FAIL(das_ctx.extended_tablet_loc(*local_table_loc,
                                                  tablet_id_,
                                                  tablet_loc))) {
      LOG_WARN("fail to extend tablet loc", K(ret), K(tablet_id_));
    }
  }
  return ret;
}

int ObTableLSExecuteP::HTableLSExecuteIter::find_real_table_id(const ObString &family_name, uint64_t &real_table_id)
{
  int ret = OB_SUCCESS;
  real_table_id = OB_INVALID_ID;
  for (int i = 0; i < hbase_infos_.count(); ++i) {
    if (family_name == hbase_infos_.at(i)->real_table_name_.after('$')) {
      real_table_id = hbase_infos_.at(i)->table_id_;
      break;
    }
  }
  if (real_table_id == OB_INVALID_ID) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist in tablegroup", K(ret), K(real_table_id), K(family_name), K(table_group_name_));
  }
  return ret;
}

int ObTableLSExecuteP::HTableLSExecuteIter::get_family_from_op(ObTableSingleOp &curr_single_op,
                                                              ObString &family)
{
  int ret = OB_SUCCESS;
  ObObj qualifier;
  const ObTableSingleOpEntity &single_entity = curr_single_op.get_entities().at(0);
  if (OB_FAIL(single_entity.get_rowkey_value(ObHTableConstants::COL_IDX_Q, qualifier))) {
    LOG_WARN("fail to get qualifier value", K(ret));
  } else if (OB_NOT_NULL(qualifier.get_string().find('.'))) {
    family = qualifier.get_string().split_on('.');
  }
  return ret;
}

int ObTableLSExecuteP::HTableLSExecuteIter::construct_delete_family_op(const ObTableSingleOp &single_op,
                                                                      const ObTableHbaseMutationInfo &mutation_info)
{
  int ret = OB_SUCCESS;
  ObTableSingleOp new_single_op;
  tablet_ops_.set_dictionary(single_op.get_all_rowkey_names(), single_op.get_all_properties_names());
  new_single_op.set_op_query(const_cast<ObTableSingleOpQuery *>(single_op.get_query()));
  ObTableSingleOpEntity entity;
  ObSqlString qualifier;
  if (OB_FAIL(qualifier.append(mutation_info.real_table_name_.after('$')))) {
    LOG_WARN("fail to append qualifier", K(ret));
  } else if (OB_FAIL(qualifier.append("."))) {
    LOG_WARN("fail to append dot to qualifier", K(ret));
  } else if (OB_FAIL(entity.deep_copy(allocator_, single_op.get_entities().at(0)))) {
    LOG_WARN("fail to deep copy entity", K(ret));
  } else {
    ObObj q_obj;
    q_obj.set_string(ObObjType::ObVarcharType, qualifier.string());
    ObObj copy_obj;
    if (OB_FAIL(ob_write_obj(allocator_, q_obj, copy_obj))) {
      LOG_WARN("fail to write obj", K(ret), K(q_obj));
    } else if (FALSE_IT(entity.set_rowkey_value(ObHTableConstants::COL_IDX_Q, copy_obj))) {
    } else if (OB_FAIL(new_single_op.get_entities().push_back(entity))) {
      LOG_WARN("fail to push back to entity", K(ret));
    } else {
      new_single_op.set_dictionary(single_op.get_all_rowkey_names(), single_op.get_all_properties_names());
      new_single_op.set_operation_type(ObTableOperationType::DEL);
      if (OB_FAIL(tablet_ops_.add_single_op(new_single_op))) {
        LOG_WARN("fail to add single op", K(ret), K(new_single_op));
      }
    }
  }

  return ret;
}

int ObTableLSExecuteP::HTableLSExecuteIter::set_tablet_ops(ObTableTabletOp &tablet_ops)
{
  int ret = OB_SUCCESS;
  tablet_ops_.reset();
  tablet_ops_.set_tablet_id(tablet_ops.get_tablet_id());
  tablet_ops_.set_option_flag(tablet_ops.get_option_flag());
  for (int i = 0; OB_SUCC(ret) && i < tablet_ops.count(); ++i) {
    ObTableSingleOp &single_op = tablet_ops.at(i);
    ObString family;
    if (OB_FAIL(get_family_from_op(single_op, family))) {
      LOG_WARN("fail to extract family and modify entity", K(ret), K(single_op));
    } else if (single_op.get_op_type() == ObTableOperationType::DEL && OB_ISNULL(family)) {
      for (int j = 0; OB_SUCC(ret) && j < hbase_infos_.count(); ++j) {
        ObTableHbaseMutationInfo *info = hbase_infos_.at(j);
        if (OB_ISNULL(info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mutation info is NULL", K(ret));
        } else if (OB_FAIL(construct_delete_family_op(single_op, *info))) {
          LOG_WARN("fail to create delete family op", K(ret), K(single_op));
        }
      }
    } else {
      if (OB_FAIL(tablet_ops_.add_single_op(single_op))) {
        LOG_WARN("fail to add single op", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLSExecuteP::HTableLSExecuteIter::find_real_tablet_id(uint64_t arg_table_id,
                                                                uint64_t real_table_id,
                                                                ObTabletID &real_tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = OB_INVALID_INDEX;
  int64_t subpart_idx = OB_INVALID_INDEX;
  if (OB_FAIL(outer_exectute_process_.get_idx_by_table_tablet_id(arg_table_id,
                                                                tablet_ops_.get_tablet_id(),
                                                                part_idx,
                                                                subpart_idx))) {
    LOG_WARN("fail to get part idx", K(ret), K(arg_table_id), K(tablet_ops_.get_tablet_id()));
  } else if (OB_FAIL(outer_exectute_process_.get_tablet_by_idx(real_table_id,
                                                              part_idx,
                                                              subpart_idx,
                                                              real_tablet_id))) {
    LOG_WARN("failed to get tablet id by part idx", K(ret), K(real_table_id), K(real_tablet_id));
  }
  return ret;
}

int ObTableLSExecuteP::HTableLSExecuteIter::get_next_batch_ctx(ObTableTabletOpResult &tablet_result,
                                                               ObTableBatchCtx *&batch_ctx)
{
  int ret = OB_SUCCESS;
  uint64_t real_table_id = OB_INVALID_ID;
  ObTabletID real_tablet_id;
  ObString first_op_family;
  if (curr_op_index_ >= tablet_ops_.count()) {
    ret = OB_ITER_END;
    LOG_INFO("this tablet_ops_ has no more op", K(ret), K(curr_op_index_));
  } else {
    ObTableSingleOp &curr_single_op = tablet_ops_.at(curr_op_index_++);
    ObTableOperationType::Type first_op_type = curr_single_op.get_op_type();
    if (first_op_type == ObTableOperationType::APPEND ||
        first_op_type == ObTableOperationType::INCREMENT ||
        first_op_type == ObTableOperationType::GET) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support this type yet", K(ret), K(first_op_type));
    } else {
      same_ctx_ops_.reset();
      if (OB_FAIL(get_family_from_op(curr_single_op, first_op_family))) {
        LOG_WARN("fail to get family from op", K(ret), K(curr_single_op));
      } else if (OB_ISNULL(first_op_family)) {
        // get type and is table group
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls op family is null", K(ret));
      } else if (OB_FAIL(modify_htable_quailfier_and_timestamp(curr_single_op, first_op_type, ops_timestamp_))) {
        LOG_WARN("fail to modify htable quailfier and timestamp", K(ret), K(curr_single_op));
      } else if (OB_FAIL(same_ctx_ops_.add_single_op(curr_single_op))) {
        LOG_WARN("fail to add single op", K(ret));
      } else if (OB_FAIL(find_real_table_id(first_op_family, real_table_id))) {
        LOG_WARN("fail to find real table id", K(ret), K(first_op_family), K(real_table_id));
      } else if (OB_FAIL(find_real_tablet_id(outer_exectute_process_.arg_.ls_op_.get_table_id(),
                                            real_table_id,
                                            real_tablet_id))) {
        LOG_WARN("failed to find tablet id", K(ret), K(real_table_id), K(curr_single_op));
      } else if (FALSE_IT(tablet_id_ = real_tablet_id)) {
      } else {
        for (int i = 0; i < batch_ctxs_.count(); ++i) {
          if (first_op_type == batch_ctxs_.at(i).first.first && real_table_id == batch_ctxs_.at(i).first.second) {
            batch_ctx = batch_ctxs_.at(i).second;
            break;
          }
        }
        if (OB_ISNULL(batch_ctx)) {
          ObTableHbaseMutationInfo *info = nullptr;
          for (int j = 0; OB_ISNULL(info) && j < hbase_infos_.count(); ++j) {
            if (real_table_id == hbase_infos_.at(j)->table_id_) {
              info = hbase_infos_.at(j);
            }
          }
          if (OB_ISNULL(info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mutation info is NULL", K(ret));
          } else if (OB_ISNULL(batch_ctx = OB_NEWx(ObTableBatchCtx,
                                                  &allocator_,
                                                  allocator_,
                                                  outer_exectute_process_.audit_ctx_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memroy for batch_ctx", K(ret));
          } else if (OB_FAIL(init_batch_ctx(real_table_id,
                                            curr_single_op,
                                            &info->schema_cache_guard_,
                                            info->simple_schema_,
                                            tablet_result,
                                            *batch_ctx))) {
            batch_ctx->~ObTableBatchCtx();
            LOG_WARN("fail to init batch ctx", K(ret));
          } else if (OB_FAIL(batch_ctxs_.push_back(
                         std::make_pair(std::make_pair(first_op_type, real_table_id), batch_ctx)))) {
            batch_ctx->~ObTableBatchCtx();
            LOG_WARN("fail to push back batch ctx", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          ObString tmp_family;
          for (; OB_SUCC(ret) && curr_op_index_ < tablet_ops_.count(); ++curr_op_index_) {
            curr_single_op = tablet_ops_.at(curr_op_index_);
            real_tablet_id.reset();
            if (OB_FAIL(get_family_from_op(curr_single_op, tmp_family))) {
              LOG_WARN("fail to extract family and modify entity", K(ret), K(tmp_family), K(first_op_family));
            } else if (curr_single_op.get_op_type() != first_op_type || tmp_family != first_op_family) {
              break;
            } else if (OB_FAIL(find_real_tablet_id(outer_exectute_process_.arg_.ls_op_.get_table_id(), real_table_id, real_tablet_id))) {
              LOG_WARN("failed to find real tablet id ", K(ret), K(real_table_id), K(curr_single_op));
            } else if (real_tablet_id != tablet_id_) {
              break;
            } else if (OB_FAIL(modify_htable_quailfier_and_timestamp(curr_single_op, first_op_type, ops_timestamp_))) {
              LOG_WARN("fail to modify htable quailfier and timestamp", K(ret), K(curr_single_op));
            } else if (OB_FAIL(same_ctx_ops_.add_single_op(curr_single_op))) {
              LOG_WARN("fail to add single op", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(convert_batch_ctx(*batch_ctx))) {
            LOG_WARN("fail to convert batch ctx", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

/**
 * ---------------------------------------- ObTableLSExecuteP ----------------------------------------
 */

ObTableLSExecuteP::ObTableLSExecuteP(const ObGlobalContext &gctx)
    : ObTableRpcProcessor(gctx),
      allocator_("TableLSExecuteP", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      cb_(nullptr)
{}

ObTableLSExecuteP::~ObTableLSExecuteP()
{
  // cb need to be released because end_trans wouldn't release it
  // when txn is rollback
  if (OB_NOT_NULL(cb_)) {
    OB_DELETE(ObTableLSExecuteEndTransCb, "TbLsExuTnCb", cb_);
  }
}

int ObTableLSExecuteP::deserialize()
{
  arg_.ls_op_.set_deserialize_allocator(&allocator_);
  return ParentType::deserialize();
}

int ObTableLSExecuteP::before_process()
{
  int ret = OB_SUCCESS;
  ObTableLSExecuteEndTransCb *cb = nullptr;
  if (OB_SUCC(ret) && OB_FAIL(cb_functor_.init(req_))) {
    LOG_WARN("fail to init create ls callback functor", K(ret));
  } else if (OB_ISNULL(cb = static_cast<ObTableLSExecuteEndTransCb *>(cb_functor_.new_callback()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ls execute end trans callback", K(ret));
  } else {
    cb_ = cb;
    ObTableLSOpResult &cb_result = cb_->get_result();
    const ObIArray<ObString> &all_rowkey_names = arg_.ls_op_.get_all_rowkey_names();
    const ObIArray<ObString> &all_properties_names = arg_.ls_op_.get_all_properties_names();
    bool need_all_prop = arg_.ls_op_.need_all_prop_bitmap();
    if (OB_FAIL(cb_result.assign_rowkey_names(all_rowkey_names))) {
      LOG_WARN("fail to assign rowkey names", K(ret), K(all_rowkey_names));
    } else if (!need_all_prop && OB_FAIL(cb_result.assign_properties_names(all_properties_names))) {
      LOG_WARN("fail to assign properties names", K(ret), K(all_properties_names));
    } else {
      is_tablegroup_req_ = ObHTableUtils::is_tablegroup_req(arg_.ls_op_.get_table_name(), arg_.entity_type_);
      ret = ParentType::before_process();
    }
  }
  return ret;
}

int ObTableLSExecuteP::check_arg()
{
  int ret = OB_SUCCESS;
  if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
      arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "consistency level");
    LOG_WARN("some options not supported yet", K(ret), "consistency_level", arg_.consistency_level_);
  }
  return ret;
}

int ObTableLSExecuteP::check_arg_for_query_and_mutate(const ObTableSingleOp &single_op)
{
  int ret = OB_SUCCESS;
  const ObTableQuery *query = nullptr;
  const ObHTableFilter *hfilter = nullptr;
  const ObIArray<ObTableSingleOpEntity> &entities = single_op.get_entities();
  bool is_hkv = ObTableEntityType::ET_HKV == arg_.entity_type_;
  if (single_op.get_op_type() != ObTableOperationType::CHECK_AND_INSERT_UP) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "single op type is not check and insert up");
    LOG_WARN("invalid single op type", KR(ret), "single op type", single_op.get_op_type());
  } else if (OB_ISNULL(query = single_op.get_query()) || OB_ISNULL(hfilter = &(query->htable_filter()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query or htable filter is NULL", K(ret), KP(query));
  } else if (!query->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query is invalid");
    LOG_WARN("invalid table query request", K(ret), K(query));
  } else if (is_hkv && !hfilter->is_valid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ob-hbase model but not set hfilter");
    LOG_WARN("QueryAndMutate hbase model should set hfilter", K(ret));
  } else if (!is_hkv && (1 != entities.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "the count of entities must be 1 for non-hbase's single operation");
    LOG_WARN("table api single operation has unexpected entities count, expect 1", K(ret), K(entities.count()));
  } else if (entities.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the count of entities must greater than 0");
    LOG_WARN("should have at least one entities for single operation", K(ret), K(entities));
  } else {
    // these options are meaningless for QueryAndMutate users but we should control them internally
    const_cast<ObTableQuery *>(query)->set_batch(1);  // mutate for each row
    const_cast<ObTableQuery *>(query)->set_max_result_size(-1);
    const_cast<ObHTableFilter *>(hfilter)->set_max_versions(1);
    const_cast<ObHTableFilter *>(hfilter)->set_row_offset_per_column_family(0);
    const_cast<ObHTableFilter *>(hfilter)->set_max_results_per_column_family(-1);
  }
  return ret;
}

uint64_t ObTableLSExecuteP::get_request_checksum()
{
  uint64_t checksum = arg_.ls_op_.get_ls_id().id();
  int64_t table_checksum = arg_.ls_op_.get_table_id();
  checksum = ob_crc64(checksum, &table_checksum, sizeof(table_checksum));
  int64_t tablet_checksum = 0;
  uint64_t single_op_checksum = 0;
  for (int64_t i = 0; i < arg_.ls_op_.count(); i++) {
    tablet_checksum = arg_.ls_op_.at(i).get_tablet_id().id();
    checksum = ob_crc64(checksum, &tablet_checksum, sizeof(tablet_checksum));
    for (int64_t j = 0; j < arg_.ls_op_.at(i).count(); j++) {
      single_op_checksum = arg_.ls_op_.at(i).at(j).get_checksum();
      checksum = ob_crc64(checksum, &single_op_checksum, sizeof(single_op_checksum));
    }
  }
  return checksum;
}

void ObTableLSExecuteP::reset_ctx()
{
  need_retry_in_queue_ = false;
  ObTableApiProcessorBase::reset_ctx();
  if (OB_NOT_NULL(cb_)) {
    cb_->get_result().reset();
  }
}

int ObTableLSExecuteP::get_ls_id(ObLSID &ls_id, const share::schema::ObSimpleTableSchemaV2 *simple_table_schema)
{
  int ret = OB_SUCCESS;
  const ObTableLSOp &ls_op = arg_.ls_op_;
  const ObLSID &client_ls_id = arg_.ls_op_.get_ls_id();
  if (client_ls_id.is_valid()) {
    ls_id = client_ls_id;
  } else if (ls_op.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ls op count", K(ret));
  } else {
    const ObTabletID &first_tablet_id = ls_op.at(0).get_tablet_id();
    const uint64_t &first_table_id = ls_op.get_table_id();
    ObTabletID real_tablet_id;
    if (OB_FAIL(get_tablet_id(simple_table_schema, first_tablet_id, first_table_id, real_tablet_id))) {
      LOG_WARN("fail to get tablet id", K(ret), K(first_table_id), K(first_table_id));
    } else if (OB_FAIL(ParentType::get_ls_id(real_tablet_id, ls_id))) {
      LOG_WARN("fail to get ls id", K(ret), K(real_tablet_id));
    }
  }
  return ret;
}


int ObTableLSExecuteP::try_process()
{
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = arg_.ls_op_;
  ObLSID ls_id = ls_op.get_ls_id();
  const ObString &arg_table_name = ls_op.get_table_name();
  uint64_t table_id = ls_op.get_table_id();
  bool exist_global_index = false;
  bool need_all_prop = arg_.ls_op_.need_all_prop_bitmap();
  table_id_ = table_id;  // init move response need
  ObTableLSOpResult *cb_result = nullptr;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard

  if (OB_ISNULL(cb_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null callback", K(ret));
  } else if (FALSE_IT(cb_result = &cb_->get_result())) {
  } else if (is_tablegroup_req_) {
    if (OB_FAIL(execute_ls_op_tablegroup(*cb_result))) {
      LOG_WARN("fail to execute ls op", K(ret));
    }
  } else {
    if (OB_FAIL(init_schema_info(table_id, arg_table_name))) {
      if (ret == OB_TABLE_NOT_EXIST) {
        ObString db("");
        const ObString &table_name = ls_op.get_table_name();
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(db), to_cstring(table_name));
      }
      LOG_WARN("fail to init schema info", K(ret), K(table_id));
    } else if (OB_FAIL(get_ls_id(ls_id, simple_table_schema_))) {
      LOG_WARN("fail to get ls id", K(ret));
    } else if (OB_FAIL(check_table_has_global_index(exist_global_index, schema_cache_guard_))) {
      LOG_WARN("fail to check global index", K(ret), K(table_id));
    } else if (need_all_prop) {
      ObSEArray<ObString, 8> all_prop_name;
      const ObIArray<ObTableColumnInfo *> &column_info_array = schema_cache_guard_.get_column_info_array();
      if (OB_FAIL(ObTableApiUtil::expand_all_columns(column_info_array, all_prop_name))) {
        LOG_WARN("fail to expand all columns", K(ret));
      } else if (OB_FAIL(cb_result->assign_properties_names(all_prop_name))) {
        LOG_WARN("fail to assign property names to result", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                  arg_.consistency_level_,
                                  ls_id,
                                  get_timeout_ts(),
                                  exist_global_index))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(execute_ls_op(*cb_result))) {
      LOG_WARN("fail to execute ls op", K(ret));
    }
  }

  bool is_rollback = (OB_SUCCESS != ret);
  if (!is_rollback) {
    cb_ = nullptr;
  }
  int tmp_ret = ret;
  const bool use_sync = false;
  if (OB_FAIL(end_trans(is_rollback, req_, &cb_functor_, use_sync))) {
    LOG_WARN("failed to end trans", K(ret));
  }

  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute ls batch operation", K(ret), K_(retry_count));
#else
  // release mode
  LOG_TRACE("[TABLE] execute ls batch operation", K(ret), K_(retry_count), "receive_ts", get_receive_timestamp());
#endif
  return ret;
}

int ObTableLSExecuteP::execute_ls_op_tablegroup(ObTableLSOpResult &ls_result)
{
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = arg_.ls_op_;
  ObLSID ls_id = ls_op.get_ls_id();
  bool exist_global_index = false;

  SMART_VAR(HTableLSExecuteIter, htable_ls_iter, *this)
  {
    if (OB_FAIL(htable_ls_iter.init())) {
      LOG_WARN("fail to init htable_ls_iter", K(ret));
    } else if (OB_FAIL(get_ls_id(ls_id, simple_table_schema_))) {
      LOG_WARN("fail to get ls id", K(ret));
    } else if (OB_FAIL(check_table_has_global_index(exist_global_index, schema_cache_guard_))) {
      LOG_WARN("fail to check global index", K(ret), K(ls_op.get_table_name()));
    } else if (arg_.ls_op_.need_all_prop_bitmap()) {
      ObSEArray<ObString, 8> all_prop_name;
      const ObIArray<ObTableColumnInfo *> &column_info_array = schema_cache_guard_.get_column_info_array();
      if (OB_FAIL(ObTableApiUtil::expand_all_columns(column_info_array, all_prop_name))) {
        LOG_WARN("fail to expand all columns", K(ret));
      } else if (OB_FAIL(ls_result.assign_properties_names(all_prop_name))) {
        LOG_WARN("fail to assign property names to result", K(ret));
      }
    } else if (OB_FAIL(ls_result.prepare_allocate(ls_op.count()))) {
      LOG_WARN("fail to prepare_allocate ls result", K(ret));
    } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                  arg_.consistency_level_,
                                  ls_id,
                                  get_timeout_ts(),
                                  exist_global_index))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_op.count(); i++) {
        htable_ls_iter.set_tablet_ops(ls_op.at(i));
        ObTableTabletOpResult &tablet_result = ls_result.at(i);
        uint64_t affected_rows = 0;
        ObTableOperationResult op_result;
        while (OB_SUCC(ret)) {
          ObTableBatchCtx *batch_ctx = nullptr;
          ObTableTabletOpResult same_type_tablet_op_result;
          if (OB_FAIL(htable_ls_iter.get_next_batch_ctx(same_type_tablet_op_result, batch_ctx))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("fail to get next batch ctx", K(ret));
            }
          } else if (OB_FAIL(ObTableBatchService::execute(*batch_ctx))) {
            LOG_WARN("fail to execute htable batch operation", K(ret));
          } else if (OB_FAIL(add_dict_and_bm_to_result_entity(htable_ls_iter.get_same_ctx_ops(),
                                                              same_type_tablet_op_result))) {
            LOG_WARN("fail to add dictionary and bitmap", K(ret));
          } else {
            // TODO adjust the results to correspond to each single_op.
            affected_rows += same_type_tablet_op_result.at(0).get_affected_rows();
            op_result.set_entity(batch_ctx->result_entity_);
          }
        }
        if (ret == OB_ITER_END) {
          htable_ls_iter.reset();
          ret = OB_SUCCESS;
        }
        op_result.set_type(ls_op.at(i).at(0).get_op_type());
        op_result.set_affected_rows(affected_rows);
        op_result.set_err(ret);
        if (OB_SUCC(ret) && OB_FAIL(tablet_result.push_back(op_result))) {
          LOG_WARN("fail to push back op_result", K(ret), K(op_result));
        }
        // record events
        stat_row_count_ += ls_op.at(i).count();
      }
    }
  }
  return ret;
}

int ObTableLSExecuteP::execute_ls_op(ObTableLSOpResult &ls_result)
{
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = arg_.ls_op_;
  bool return_one_res = ls_op.return_one_result();
  if (!return_one_res) {
    if (OB_FAIL(ls_result.prepare_allocate(ls_op.count()))) {
      LOG_WARN("fail to prepare_allocate ls result", K(ret));
    }
  } else {
    if (OB_FAIL(ls_result.prepare_allocate(1))) {
      LOG_WARN("fail to prepare_allocate ls result", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (return_one_res) {
    int affected_rows = 0;
    ObTableTabletOpResult &tablet_result = ls_result.at(0);
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_op.count(); i++) {
      tablet_result.reuse();
      ObTableTabletOp &tablet_op = ls_op.at(i);
      if (OB_FAIL(execute_tablet_op(tablet_op,
                                    simple_table_schema_->get_table_id(),
                                    &schema_cache_guard_,
                                    simple_table_schema_,
                                    tablet_result))) {
        LOG_WARN("fail to execute tablet op", KR(ret), K(tablet_op));
      } else if (tablet_result.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet result when return one res", K(ret), K(tablet_result.count()));
      } else {
        affected_rows += tablet_result.at(0).get_affected_rows();
      }
    }
    if (OB_SUCC(ret)) {
      ObTableSingleOpResult &single_op_res = tablet_result.at(0);
      single_op_res.set_affected_rows(affected_rows);
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_op.count(); i++) {
      ObTableTabletOp &tablet_op = ls_op.at(i);
      ObTableTabletOpResult &tablet_result = ls_result.at(i);
      if (OB_FAIL(execute_tablet_op(tablet_op,
                                    simple_table_schema_->get_table_id(),
                                    &schema_cache_guard_,
                                    simple_table_schema_,
                                    tablet_result))) {
        LOG_WARN("fail to execute tablet op", KR(ret), K(tablet_op));
      }
    }
  }
  return ret;
}

int ObTableLSExecuteP::execute_tablet_op(const ObTableTabletOp &tablet_op,
                                        uint64_t table_id,
                                        ObKvSchemaCacheGuard *schema_cache_guard,
                                        const ObSimpleTableSchemaV2 *simple_table_schema,
                                        ObTableTabletOpResult &tablet_result)
{
  int ret = OB_SUCCESS;
  if (tablet_op.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet operations count is less than 1", K(ret));
  } else {
    ObTableOperationType::Type op_type = tablet_op.at(0).get_op_type();
    if (op_type == ObTableOperationType::CHECK_AND_INSERT_UP) {
      if (OB_FAIL(execute_tablet_query_and_mutate(table_id, tablet_op, tablet_result))) {
        LOG_WARN("fail to execute tablet query and mutate", K(ret));
      }
    } else {
      // other op type will check its validity in its inner logic
      if (OB_FAIL(execute_tablet_batch_ops(tablet_op,
                                          table_id,
                                          schema_cache_guard,
                                          simple_table_schema,
                                          tablet_result))) {
        LOG_WARN("fail to execute tablet batch operations", K(ret));
      }
    }
  }

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute ls batch tablet operation", K(ret), K(tablet_op), K(tablet_result), K_(retry_count));
#else
  // release mode
  LOG_TRACE("[TABLE] execute ls batch tablet operation", K(ret), K(tablet_op), K(tablet_result), K_(retry_count), "receive_ts", get_receive_timestamp());
#endif

  return ret;
}

int ObTableLSExecuteP::execute_tablet_query_and_mutate(
    const uint64_t table_id, const ObTableTabletOp &tablet_op, ObTableTabletOpResult &tablet_result)
{
  int ret = OB_SUCCESS;
  if (tablet_op.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operation count", KR(ret), K(tablet_op.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_op.count(); i++) {
      const ObTableSingleOp &single_op = tablet_op.at(i);
      const ObTableSingleOpEntity &req_entity = single_op.get_entities().at(0);
      const common::ObTabletID tablet_id = tablet_op.get_tablet_id();
      ObTableSingleOpResult single_op_result;
      single_op_result.set_errno(OB_SUCCESS);
      ObITableEntity *result_entity = cb_->get_entity_factory().alloc();

      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memroy for result_entity", K(ret));
      } else if (FALSE_IT(result_entity->set_dictionary(
                     &arg_.ls_op_.get_all_rowkey_names(), &arg_.ls_op_.get_all_properties_names()))) {
      } else if (FALSE_IT(single_op_result.set_entity(*result_entity))) {
      } else if (OB_FAIL(execute_single_query_and_mutate(table_id, tablet_id, single_op, single_op_result))) {
        LOG_WARN("fail to execute tablet op", KR(ret), K(tablet_op));
      } else if (OB_FAIL(result_entity->construct_names_bitmap(req_entity))) {
        LOG_WARN("fail to construct_names_bitmap", KR(ret), KPC(result_entity));
      } else if (OB_FAIL(tablet_result.push_back(single_op_result))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLSExecuteP::execute_tablet_batch_ops(const ObTableTabletOp &tablet_op,
                                                uint64_t table_id,
                                                ObKvSchemaCacheGuard *schema_cache_guard,
                                                const ObSimpleTableSchemaV2 *simple_table_schema,
                                                ObTableTabletOpResult &tablet_result)
{
  int ret = OB_SUCCESS;
  if (tablet_op.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operation count", KR(ret), K(tablet_op.count()));
  } else {
    ObSEArray<ObTableOperation, 16> table_operations;
    SMART_VAR(ObTableBatchCtx, batch_ctx, cb_->get_allocator(), audit_ctx_)
    {
      if (OB_FAIL(init_batch_ctx(tablet_op,
                                 table_operations,
                                 table_id,
                                 schema_cache_guard,
                                 simple_table_schema,
                                 tablet_result,
                                 batch_ctx))) {
        LOG_WARN("fail to init batch ctx", K(ret));
      } else if (OB_FAIL(ObTableBatchService::execute(batch_ctx))) {
        LOG_WARN("fail to execute batch operation", K(ret));
      } else if (OB_FAIL(add_dict_and_bm_to_result_entity(tablet_op, tablet_result))) {
        LOG_WARN("fail to add dictionary and bitmap", K(ret));
      }
    }
    // record events
    stat_row_count_ += tablet_op.count();
  }
  return ret;
}

int ObTableLSExecuteP::init_batch_ctx(const table::ObTableTabletOp &tablet_op,
                                      ObIArray<table::ObTableOperation> &table_operations,
                                      uint64_t table_id,
                                      ObKvSchemaCacheGuard *shcema_cache_guard,
                                      const ObSimpleTableSchemaV2 *simple_table_schema,
                                      table::ObTableTabletOpResult &tablet_result,
                                      table::ObTableBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = arg_.ls_op_;
  // 1. 构造batch_service需要的入参
  batch_ctx.stat_event_type_ = &stat_event_type_;
  batch_ctx.trans_param_ = &trans_param_;
  batch_ctx.entity_type_ = arg_.entity_type_;
  batch_ctx.consistency_level_ = arg_.consistency_level_;
  batch_ctx.credential_ = &credential_;
  batch_ctx.table_id_ = table_id;
  batch_ctx.tablet_id_ = tablet_op.get_tablet_id();
  batch_ctx.is_atomic_ = true; /* batch atomic always true*/
  batch_ctx.is_readonly_ = tablet_op.is_readonly();
  batch_ctx.is_same_type_ = tablet_op.is_same_type();
  batch_ctx.is_same_properties_names_ = tablet_op.is_same_properties_names();
  batch_ctx.use_put_ = tablet_op.is_use_put();
  batch_ctx.returning_affected_entity_ = tablet_op.is_returning_affected_entity();
  batch_ctx.returning_rowkey_ = tablet_op.is_returning_rowkey();
  batch_ctx.entity_factory_ = &cb_->get_entity_factory();
  batch_ctx.return_one_result_ = ls_op.return_one_result();
  // construct batch operation
  batch_ctx.ops_ = &table_operations;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_op.count(); i++) {
    const ObTableSingleOp &single_op = tablet_op.at(i);
    if (single_op.get_op_type() == ObTableOperationType::CHECK_AND_INSERT_UP) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "check_and_insertup in batch");
      LOG_WARN("invalid single op type", KR(ret), "single op type", single_op.get_op_type());
    } else {
      ObTableOperation table_op;
      table_op.set_entity(single_op.get_entities().at(0));
      table_op.set_type(single_op.get_op_type());
      if (OB_FAIL(table_operations.push_back(table_op))) {
        LOG_WARN("fail to push table operation", K(ret));
      }
    }
  }
  // construct batch operation result
  if (OB_SUCC(ret)) {
    batch_ctx.results_ = &tablet_result;
    batch_ctx.result_entity_ = cb_->get_entity_factory().alloc(); // only use in hbase mutation
    if (OB_ISNULL(batch_ctx.result_entity_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memroy for result_entity", K(ret));
    } else if (OB_FAIL(init_tb_ctx(tablet_op,
                                   table_operations.at(0),
                                   shcema_cache_guard,
                                   simple_table_schema,
                                   batch_ctx.tb_ctx_))) { // init tb_ctx
      LOG_WARN("fail to init table context", K(ret));
    }
  }

  return ret;
}

int ObTableLSExecuteP::init_tb_ctx(const table::ObTableTabletOp &tablet_op,
                                   const table::ObTableOperation &table_operation,
                                   ObKvSchemaCacheGuard *shcema_cache_guard,
                                   const ObSimpleTableSchemaV2 *table_schema,
                                   table::ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  tb_ctx.set_entity(&table_operation.entity());
  tb_ctx.set_entity_type(arg_.entity_type_);
  tb_ctx.set_operation_type(table_operation.type());
  tb_ctx.set_schema_cache_guard(shcema_cache_guard);
  tb_ctx.set_schema_guard(&schema_guard_);
  tb_ctx.set_simple_table_schema(table_schema);
  tb_ctx.set_sess_guard(&sess_guard_);
  if (tb_ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_common(credential_, tablet_op.get_tablet_id(), get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.ls_op_.get_table_id()));
  } else {
    ObTableOperationType::Type op_type = table_operation.type();
    switch (op_type) {
      case ObTableOperationType::GET: {
        if (OB_FAIL(tb_ctx.init_get())) {
          LOG_WARN("fail to init get ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::PUT: {
        if (OB_FAIL(tb_ctx.init_put())) {
          LOG_WARN("fail to init put ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT: {
        if (OB_FAIL(tb_ctx.init_insert())) {
          LOG_WARN("fail to init insert ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::DEL: {
        if (OB_FAIL(tb_ctx.init_delete())) {
          LOG_WARN("fail to init delete ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::UPDATE: {
        if (OB_FAIL(tb_ctx.init_update())) {
          LOG_WARN("fail to init update ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT_OR_UPDATE: {
        if (OB_FAIL(tb_ctx.init_insert_up(tablet_op.is_use_put()))) {
          LOG_WARN("fail to init insert up ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::REPLACE: {
        if (OB_FAIL(tb_ctx.init_replace())) {
          LOG_WARN("fail to init replace ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::APPEND: {
        if (OB_FAIL(tb_ctx.init_append(tablet_op.is_returning_affected_entity(),
                                       tablet_op.is_returning_rowkey()))) {
          LOG_WARN("fail to init append ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INCREMENT: {
        if (OB_FAIL(tb_ctx.init_increment(tablet_op.is_returning_affected_entity(),
                                          tablet_op.is_returning_rowkey()))) {
          LOG_WARN("fail to init increment ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected operation type", "type", op_type);
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret));
  }

  return ret;
}

int ObTableLSExecuteP::add_dict_and_bm_to_result_entity(const ObTableTabletOp &tablet_op,
                                                        ObTableTabletOpResult &tablet_result)
{
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = arg_.ls_op_;
  ObTableLSOpResult &ls_result = cb_->get_result();
  bool is_hkv = ObTableEntityType::ET_HKV == arg_.entity_type_;
  if (!is_hkv && !ls_op.return_one_result() && tablet_op.count() != tablet_result.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet op count is not match to tablet results", K(ret),
             "table_op_count", tablet_op.count(), "tablet_result_count", tablet_result.count());
  } else if (ls_op.return_one_result() && tablet_result.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet op count should equal to 1 when return one result", K(ret),
             "table_op_count", tablet_op.count(), "tablet_result_count", tablet_result.count());
  }

  for (int i = 0; i < tablet_result.count() && OB_SUCC(ret); i++) {
    const ObTableSingleOp &single_op = tablet_op.at(i);
    const ObTableSingleOpEntity &req_entity = single_op.get_entities().at(0);
    ObTableSingleOpEntity *result_entity = static_cast<ObTableSingleOpEntity *>(tablet_result.at(i).get_entity());
    bool need_rebuild_bitmap = arg_.ls_op_.need_all_prop_bitmap() && single_op.get_op_type() == ObTableOperationType::GET;
    result_entity->set_dictionary(&ls_result.get_rowkey_names(), &ls_result.get_properties_names());
    if (need_rebuild_bitmap) { // construct result entity bitmap based on all columns dict
      if (OB_FAIL(result_entity->construct_names_bitmap_by_dict(req_entity))) {
        LOG_WARN("fail to construct name bitmap by all columns", K(ret), K(i));
      }
    } else if (OB_FAIL(result_entity->construct_names_bitmap(req_entity))) { // directly use request bitmap as result bitmap
      LOG_WARN("fail to construct name bitmap", K(ret), K(i));
    }
  }
  return ret;
}

int ObTableLSExecuteP::execute_single_query_and_mutate(const uint64_t table_id,
                                                       const common::ObTabletID tablet_id,
                                                       const ObTableSingleOp &single_op,
                                                       ObTableSingleOpResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_arg_for_query_and_mutate(single_op))) {
    LOG_WARN("fail to check arg for query and mutate", K(ret));
  } else {
    // query is NULL has been check in check_arg_for_query_and_mutate
    const ObTableQuery *query = single_op.get_query();
    ObTableSingleOpQAM query_and_mutate(*query, true, single_op.is_check_no_exists());
    if (OB_FAIL(query_and_mutate.set_mutations(single_op))) {
      LOG_WARN("fail to set mutations", K(ret), "single_op", single_op);
    } else {
      ObTableQMParam qm_param(query_and_mutate);
      qm_param.table_id_ = table_id;
      qm_param.tablet_id_ = tablet_id;
      qm_param.timeout_ts_ = get_timeout_ts();
      qm_param.credential_ = credential_;
      qm_param.entity_type_ = arg_.entity_type_;
      qm_param.trans_desc_ = get_trans_desc();
      qm_param.tx_snapshot_ = &get_tx_snapshot();
      qm_param.single_op_result_ = &result;
      qm_param.schema_guard_ = &schema_guard_;
      qm_param.simple_table_schema_ = simple_table_schema_;
      qm_param.schema_cache_guard_ = &schema_cache_guard_;
      qm_param.sess_guard_ = &sess_guard_;
      SMART_VAR(QueryAndMutateHelper, helper, cb_->get_allocator(), qm_param, audit_ctx_) {
        if (OB_FAIL(helper.execute_query_and_mutate())) {
          LOG_WARN("fail to execute query and mutate", K(ret), K(single_op));
        } else {}
      }
    }
  }

  #ifndef NDEBUG
    // debug mode
    LOG_INFO("[TABLE] execute ls batch single operation", K(ret), K(single_op), K_(result));
  #else
    // release mode
    LOG_TRACE("[TABLE] execute ls batch single operation", K(ret), K(single_op), K_(result));
  #endif

  return ret;
}
