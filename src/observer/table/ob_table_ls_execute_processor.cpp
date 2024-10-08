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

ObTableLSExecuteP::ObTableLSExecuteP(const ObGlobalContext &gctx)
  : ObTableRpcProcessor(gctx),
    allocator_("TableLSExecuteP", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    cb_(nullptr)
{
}

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
    const ObIArray<ObString>& all_rowkey_names = arg_.ls_op_.get_all_rowkey_names();
    const ObIArray<ObString>& all_properties_names = arg_.ls_op_.get_all_properties_names();
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

int ObTableLSExecuteP::modify_htable_quailfier_and_timestamp(const ObITableEntity *entity,
                                                            const ObString& qualifier, int64_t now_ms,
                                                            ObTableOperationType::Type type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is nullptr", K(ret), K(qualifier), K(type));
  } else if (entity->get_rowkey_size() != ObHTableConstants::HTABLE_ROWKEY_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("htable should be with 3 rowkey columns", K(ret), K(entity));
  } else {
    ObRowkey rowkey = entity->get_rowkey();
    ObObj *obj_ptr = rowkey.get_obj_ptr();
    if (OB_ISNULL(obj_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj_ptr is nullptr", K(rowkey));
    } else {
      ObObj &t_obj = const_cast<ObObj&>(obj_ptr[ObHTableConstants::COL_IDX_T]);  // column T
      ObHTableCellEntity3 htable_cell(entity);
      bool row_is_null = htable_cell.last_get_is_null();
      int64_t timestamp = htable_cell.get_timestamp();
      bool timestamp_is_null = htable_cell.last_get_is_null();
      if (row_is_null || timestamp_is_null) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument for htable put", K(ret), K(row_is_null), K(timestamp_is_null));
      } else if (type == ObTableOperationType::INSERT_OR_UPDATE && ObHTableConstants::LATEST_TIMESTAMP == timestamp) { // update timestamp iff LATEST_TIMESTAMP
        t_obj.set_int(now_ms);
      }
      if (OB_SUCC(ret)) {
        ObObj &q_obj = const_cast<ObObj&>(obj_ptr[ObHTableConstants::COL_IDX_Q]);  // column Q
        q_obj.set_string(ObObjType::ObVarcharType, qualifier);
      }
    }
  }
  return ret;
}

int ObTableLSExecuteP::find_and_add_op(ObIArray<std::pair<uint64_t, ObTableTabletOp>>& tablet_ops,
                                        uint64_t real_table_id,
                                        const ObTableSingleOp& single_op,
                                        bool &is_found) {
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && !is_found && i < tablet_ops.count(); ++i) {
    std::pair<uint64_t, ObTableTabletOp>& pair = tablet_ops.at(i);
    if (pair.first == real_table_id) {
      if (OB_FAIL(pair.second.add_single_op(single_op))) {
        LOG_WARN("fail to add single_op to tablet_op", K(ret));
      } else {
        is_found = true;
      }
    }
  }
  return ret;
}

int ObTableLSExecuteP::add_new_op(ObIArray<std::pair<uint64_t, ObTableTabletOp>>& tablet_ops,
                                uint64_t real_table_id,
                                uint64_t real_tablet_id,
                                ObTableTabletOp& origin_tablet_op,
                                const ObTableSingleOp& single_op) {
  int ret = OB_SUCCESS;
  ObTableTabletOp new_op;
  new_op.set_tablet_id(real_tablet_id);
  new_op.set_dictionary(origin_tablet_op.get_all_rowkey_names(), origin_tablet_op.get_all_properties_names());
  if (OB_FAIL(new_op.add_single_op(single_op))) {
    LOG_WARN("fail to add single_op to tablet_op", K(ret));
  } else if (OB_FAIL(tablet_ops.push_back(std::make_pair(real_table_id, new_op)))) {
    LOG_WARN("fail push new tablet op", K(ret));
  }
  return ret;
}


int ObTableLSExecuteP::partition_single_op(ObTableTabletOp& origin_tablet_op, ObIArray<std::pair<uint64_t, ObTableTabletOp>>& tablet_ops)
{
  int ret = OB_SUCCESS;
  uint64_t real_table_id = OB_INVALID;
  uint64_t real_tablet_id = OB_INVALID;
  int64_t now_ms = -ObHTableUtils::current_time_millis();
  for (int32_t i = 0; OB_SUCC(ret) && i < origin_tablet_op.count(); ++i) {
    ObTableSingleOp& single_op = origin_tablet_op.at(i);
    ObString tablegroup_name = arg_.ls_op_.get_table_name();
    ObObj qualifier;
    ObTableSingleOpEntity& single_entity = single_op.get_entities().at(0);
    if (OB_FAIL(single_entity.get_rowkey_value(ObHTableConstants::COL_IDX_Q, qualifier))) {
      LOG_WARN("fail to get qualifier value", K(ret));
    } else {
      ObSqlString real_table_name;
      ObString family = qualifier.get_string().split_on('.');
      if (OB_FAIL(real_table_name.append(tablegroup_name))) {
        LOG_WARN("fail to append tablegroup name", K(ret), K(tablegroup_name));
      } else if (OB_FAIL(real_table_name.append("$"))) {
        LOG_WARN("fail to append $", K(ret));
      } else if (OB_FAIL(real_table_name.append(family))) {
        LOG_WARN("fail to append family name", K(ret), K(family));
      } else if (OB_FAIL(modify_htable_quailfier_and_timestamp(&single_entity,
                                                                qualifier.get_string().after('.'),
                                                                now_ms,
                                                                single_op.get_op_type()))) {
        LOG_WARN("fail to modify hbase entity", K(ret));
      } else if (OB_FAIL(get_table_id(real_table_name.string(), OB_INVALID, real_table_id))) {
        LOG_WARN("failed to get_table_id by table_name", K(ret), K(real_table_name), K(qualifier), K(family));
      } else if (OB_FAIL(get_tablet_id_by_rowkey(real_table_id, single_entity.get_rowkey(), real_tablet_id))) {
        LOG_WARN("failed to get_tablet_id_by_rowkey ", K(ret), K(real_table_id), K(single_entity.get_rowkey()));
      } else {
        bool is_found = false;
        if (OB_FAIL(find_and_add_op(tablet_ops, real_table_id, single_op, is_found))) {
          LOG_WARN("fail to add op to single_op", K(ret));
        } else if (!is_found && OB_FAIL(add_new_op(tablet_ops, real_table_id, real_tablet_id, origin_tablet_op, single_op))) {
          LOG_WARN("fail to push new tablet op", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableLSExecuteP::construct_delete_family_op(const ObTableSingleOp &single_op,
                                                  const ObTableHbaseMutationInfo &mutation_info,
                                                  ObTableTabletOp &tablet_op) {
  int ret = OB_SUCCESS;

  ObTableLSOp &ls_op = arg_.ls_op_;
  uint64_t real_tablet_id = OB_INVALID_ID;
  const ObRowkey& rowkey = single_op.get_entities().at(0).get_rowkey();
  if OB_FAIL(get_tablet_id_by_rowkey(mutation_info.table_id_, rowkey, real_tablet_id)) {
    LOG_WARN("fail to get tablet id", K(ret));
  } else {
    tablet_op.set_tablet_id(real_tablet_id);
    tablet_op.set_dictionary(single_op.get_all_rowkey_names(), single_op.get_all_properties_names());
    ObTableSingleOp new_single_op;
    new_single_op.set_op_query(const_cast<ObTableSingleOpQuery*>(single_op.get_query()));
    ObIArray<ObTableSingleOpEntity>& entity = new_single_op.get_entities();
    for (int i = 0; OB_SUCC(ret) && i < single_op.get_entities().count(); ++i) {
      if (OB_FAIL(entity.push_back(single_op.get_entities().at(i)))) {
        LOG_WARN("fail to push back to entity", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      new_single_op.set_dictionary(single_op.get_all_rowkey_names(), single_op.get_all_properties_names());
      new_single_op.set_operation_type(ObTableOperationType::DEL);
      if (OB_FAIL(tablet_op.add_single_op(new_single_op))) {
        LOG_WARN("fail to add single op", K(ret), K(new_single_op));
      }
    }
  }
  return ret;
}

int ObTableLSExecuteP::init_multi_schema_info(const ObString& arg_tablegroup_name) {
  int ret = OB_SUCCESS;
  ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> table_schemas;
  uint64_t tablegroup_id = OB_INVALID_ID;

  if (schema_cache_guard_.is_inited()) {
    // skip
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(credential_.tenant_id_));
  } else if (OB_FAIL(schema_guard_.get_tablegroup_id(credential_.tenant_id_, arg_tablegroup_name, tablegroup_id))) {
    LOG_WARN("fail to get table schema from table group name", K(ret), K(credential_.tenant_id_),
          K(credential_.database_id_), K(arg_tablegroup_name));
  } else if (OB_FAIL(schema_guard_.get_table_schemas_in_tablegroup(credential_.tenant_id_, tablegroup_id, table_schemas))) {
    LOG_WARN("fail to get table schema from table group", K(ret), K(credential_.tenant_id_),
          K(credential_.database_id_), K(arg_tablegroup_name), K(tablegroup_id));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const schema::ObSimpleTableSchemaV2* table_schema = table_schemas.at(i);
      ObTableHbaseMutationInfo* mutation_info = OB_NEWx(ObTableHbaseMutationInfo, &allocator_, allocator_);
      mutation_info->simple_schema_ = table_schema;
      mutation_info->table_id_ = table_schema->get_table_id();
      if (OB_FAIL(mutation_info->schema_cache_guard_.init(credential_.tenant_id_,
                                          table_schema->get_table_id(),
                                          table_schema->get_schema_version(),
                                          schema_guard_))) {
        LOG_WARN("fail to init shcema_cache_guard", K(ret));
      } else {
        hbase_infos_.push_back(mutation_info);
      }
    }
  }

  return ret;
}

int ObTableLSExecuteP::try_process()
{
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = arg_.ls_op_;
  ObLSID ls_id = ls_op.get_ls_id();
  uint64_t table_id = ls_op.get_table_id();
  bool exist_global_index = false;
  bool need_all_prop = arg_.ls_op_.need_all_prop_bitmap();
  table_id_ = table_id;  // init move response need
  ObTableLSOpResult *cb_result = nullptr;

  if (OB_ISNULL(cb_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null callback", K(ret));
  } else if (FALSE_IT(cb_result = &cb_->get_result())) {
  } else if (is_tablegroup_req_) {
    if (OB_FAIL(init_multi_schema_info(ls_op.get_table_name()))) {
      if (ret == OB_TABLEGROUP_NOT_EXIST) {
        ObString db("");
        const ObString &tablegroup_name = ls_op.get_table_name();
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(db), to_cstring(tablegroup_name));
      }
      LOG_WARN("fail to init schema info", K(ret), K(ls_op.get_table_name()));
    } else if (OB_FAIL(get_ls_id(ls_id, hbase_infos_.at(0)->simple_schema_))) {
      LOG_WARN("fail to get ls id", K(ret));
    } else if (OB_FAIL(check_table_has_global_index(exist_global_index, hbase_infos_.at(0)->schema_cache_guard_))) {
      LOG_WARN("fail to check global index", K(ret), K(ls_op.get_table_name()));
    } else if (need_all_prop) {
      ObSEArray<ObString, 8> all_prop_name;
      const ObIArray<ObTableColumnInfo *> &column_info_array = hbase_infos_.at(0)->schema_cache_guard_.get_column_info_array();
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
    } else if (OB_FAIL(execute_ls_op_tablegroup(*cb_result))) {
      LOG_WARN("fail to execute ls op", K(ret));
    }
  } else {
    if (OB_FAIL(init_schema_info(table_id))) {
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
      const ObIArray<ObTableColumnInfo *>&column_info_array = schema_cache_guard_.get_column_info_array();
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

int ObTableLSExecuteP::execute_ls_op_tablegroup(ObTableLSOpResult& ls_result) {
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = arg_.ls_op_;
  ObLSID ls_id = ls_op.get_ls_id();
  bool return_one_res = ls_op.return_one_result();
  bool exist_global_index = false;

  if (OB_FAIL(ls_result.prepare_allocate(1))) {
    LOG_WARN("fail to prepare_allocate ls result", K(ret));
  }
  ObArray<std::pair<uint64_t, ObTableTabletOp>> tablet_ops;
  tablet_ops.prepare_allocate_and_keep_count(8);
  if (ls_op.count() <= 0 || ls_op.at(0).empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_op));
  } else if (ls_op.at(0).count() == 1 && ls_op.at(0).at(0).get_op_type() == ObTableOperationType::DEL) {
    // For the case where Delete does not specify a column family, pass a single TabletOp with a single SingleOp.
    // Construct tablets under all tables, each TabletOp contains one SingleOp, and ignore the original tablet_op.
    ObTableSingleOp &single_op = ls_op.at(0).at(0);
    ObObj rowkey;
    if (OB_FAIL(single_op.get_entities().at(0).get_rowkey_value(0, rowkey))) {
      LOG_WARN("fail to get rowkey value", K(ret), K(single_op));
    }
    for (int i = 0; OB_SUCC(ret) && i < hbase_infos_.count(); ++i) {
      ObTableHbaseMutationInfo *info = hbase_infos_.at(i);
      ObTableTabletOp new_op;
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mutation info is NULL", K(ret));
      } else if (OB_FAIL(construct_delete_family_op(single_op, *info, new_op))) {
        LOG_WARN("fail to create delete family op", K(ret), K(single_op));
      } else if (OB_FAIL(tablet_ops.push_back(std::make_pair(info->table_id_, new_op)))) {
        LOG_WARN("fail to push back to tablet_ops", K(ret));
      }
    }
  } else {
    // Other cases: The client has already constructed SingleOp and placed it under the same TabletOp,
    // but the exact tablet has not been calculated. Therefore,
    // it is necessary to resolve the family of column Q to compute the associated table_id.
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_op.count(); ++i) {
      ObTableTabletOp& tablet_op = ls_op.at(i);
      if (OB_FAIL(partition_single_op(tablet_op, tablet_ops))) {
        LOG_WARN("partition_single_op fail", K(ret));
      }
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < tablet_ops.count(); ++i) {
    uint64_t table_id = tablet_ops.at(i).first;
    ObTableTabletOp& tablet_op = tablet_ops.at(i).second;
    ObTableHbaseMutationInfo *info = nullptr;
    for (int j = 0; OB_ISNULL(info) && j < hbase_infos_.count(); ++j) {
      if (table_id == hbase_infos_.at(j)->table_id_) {
        info = hbase_infos_.at(j);
      }
    }
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mutation info is NULL", K(ret));
    } else if (OB_FAIL(execute_tablet_op(tablet_op,
                                        info->table_id_,
                                        &info->schema_cache_guard_,
                                        info->simple_schema_,
                                        ls_result[0]))) {
      LOG_WARN("fail to execute_tablet_op", K(ret));
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
                                        ObTableTabletOpResult& tablet_result)
{
  int ret = OB_SUCCESS;
  if (tablet_op.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet operations count is less than 1", K(ret));
  } else {
    ObTableOperationType::Type op_type = tablet_op.at(0).get_op_type();
    if (op_type == ObTableOperationType::CHECK_AND_INSERT_UP) {
      if (OB_FAIL(execute_tablet_query_and_mutate(tablet_op, tablet_result))) {
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

int ObTableLSExecuteP::execute_tablet_query_and_mutate(const ObTableTabletOp &tablet_op, ObTableTabletOpResult &tablet_result)
{
  return execute_tablet_query_and_mutate(arg_.ls_op_.get_table_id(), tablet_op, tablet_result);
}

int ObTableLSExecuteP::execute_tablet_query_and_mutate(const uint64_t table_id, const ObTableTabletOp &tablet_op, ObTableTabletOpResult &tablet_result)
{
  int ret = OB_SUCCESS;
  if (tablet_op.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operation count", KR(ret), K(tablet_op.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_op.count(); i++) {
      const ObTableSingleOp &single_op = tablet_op.at(i);
      const ObTableSingleOpEntity& req_entity= single_op.get_entities().at(0);
      const common::ObTabletID tablet_id = tablet_op.get_tablet_id();
      ObTableSingleOpResult single_op_result;
      single_op_result.set_errno(OB_SUCCESS);
      ObITableEntity *result_entity = cb_->get_entity_factory().alloc();

      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memroy for result_entity", K(ret));
      } else if (FALSE_IT(result_entity->set_dictionary(&arg_.ls_op_.get_all_rowkey_names(),
                                                       &arg_.ls_op_.get_all_properties_names()))) {
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

int ObTableLSExecuteP::execute_tablet_batch_ops(const table::ObTableTabletOp &tablet_op,
                                                uint64_t table_id,
                                                ObKvSchemaCacheGuard *schema_cache_guard,
                                                const ObSimpleTableSchemaV2 *simple_table_schema,
                                                table::ObTableTabletOpResult&tablet_result)
{
 int ret = OB_SUCCESS;
  if (tablet_op.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operation count", KR(ret), K(tablet_op.count()));
  } else {
    ObSEArray<ObTableOperation, 16> table_operations;
    SMART_VAR(ObTableBatchCtx, batch_ctx, cb_->get_allocator(), audit_ctx_) {
      if (OB_FAIL(init_batch_ctx(batch_ctx,
                                tablet_op,
                                table_operations,
                                table_id,
                                schema_cache_guard,
                                simple_table_schema,
                                tablet_result))) {
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


int ObTableLSExecuteP::init_batch_ctx(table::ObTableBatchCtx &batch_ctx,
                  const table::ObTableTabletOp &tablet_op,
                  ObIArray<table::ObTableOperation> &table_operations,
                  uint64_t table_id,
                  ObKvSchemaCacheGuard *shcema_cache_guard,
                  const ObSimpleTableSchemaV2 *simple_table_schema,
                  table::ObTableTabletOpResult &tablet_result)
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
    } else if (OB_FAIL(init_tb_ctx(batch_ctx.tb_ctx_,
                                  tablet_op,
                                  table_operations.at(0),
                                  shcema_cache_guard,
                                  simple_table_schema))) { // init tb_ctx
      LOG_WARN("fail to init table context", K(ret));
    }
  }

  return ret;
}

int ObTableLSExecuteP::init_tb_ctx(table::ObTableCtx &tb_ctx,
                                  const table::ObTableTabletOp &tablet_op,
                                  const table::ObTableOperation &table_operation,
                                  ObKvSchemaCacheGuard *shcema_cache_guard,
                                  const ObSimpleTableSchemaV2 *table_schema)
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

int ObTableLSExecuteP::add_dict_and_bm_to_result_entity(const table::ObTableTabletOp &tablet_op,
                                                        table::ObTableTabletOpResult &tablet_result)
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
    const ObTableSingleOpEntity &req_entity= single_op.get_entities().at(0);
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
