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
#include "ob_table_query_and_mutate_processor.h"
#include "ob_table_query_and_mutate_helper.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObTableQueryAndMutateP::ObTableQueryAndMutateP(const ObGlobalContext &gctx)
    :ObTableRpcProcessor(gctx),
     allocator_("TbQaMP", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     tb_ctx_(allocator_),
     default_entity_factory_("QueryAndMutateEntFac", MTL_ID()),
     end_in_advance_(false)
{
}

int ObTableQueryAndMutateP::deserialize()
{
  arg_.query_and_mutate_.set_deserialize_allocator(&allocator_);
  arg_.query_and_mutate_.set_entity_factory(&default_entity_factory_);

  int ret = ParentType::deserialize();
  if (OB_SUCC(ret) && ObTableEntityType::ET_HKV == arg_.entity_type_) {
    // For HKV table, modify the timestamp value to be negative
    ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
    const int64_t N = mutations.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      ObITableEntity *entity = nullptr;
      table::ObTableOperation &mutation = const_cast<table::ObTableOperation&>(mutations.at(i));
      if (OB_FAIL(mutation.get_entity(entity))) {
        LOG_WARN("failed to get entity", K(ret), K(i));
      } else if (OB_FAIL(ObTableRpcProcessorUtil::negate_htable_timestamp(*entity))) {
        LOG_WARN("failed to negate timestamp value", K(ret));
      }
    } // end for
  }
  return ret;
}

int ObTableQueryAndMutateP::check_arg()
{
  int ret = OB_SUCCESS;
  ObTableQuery &query = arg_.query_and_mutate_.get_query();
  ObHTableFilter &hfilter = query.htable_filter();
  ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
  if (!query.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table query request", K(ret), K(query));
  } else if ((ObTableEntityType::ET_HKV == arg_.entity_type_) && !hfilter.is_valid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "QueryAndMutate hbase model not set hfilter");
    LOG_WARN("QueryAndMutate hbase model should set hfilter", K(ret));
  } else if ((ObTableEntityType::ET_KV == arg_.entity_type_) && (1 != mutations.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tableapi query_and_mutate unexpected mutation count, expect 1", K(ret), K(mutations.count()));
  } else if (mutations.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should have at least one mutation operation", K(ret), K(mutations));
  } else {
    // these options are meaningless for QueryAndMutate users but we should control them internally
    query.set_max_result_size(-1);

    hfilter.set_max_versions(1);
    hfilter.set_row_offset_per_column_family(0);
    hfilter.set_max_results_per_column_family(-1);
  }
  return ret;
}

uint64_t ObTableQueryAndMutateP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, arg_.table_name_.ptr(), arg_.table_name_.length());
  const uint64_t op_checksum = arg_.query_and_mutate_.get_checksum();
  checksum = ob_crc64(checksum, &op_checksum, sizeof(op_checksum));
  checksum = ob_crc64(checksum, &arg_.binlog_row_image_type_, sizeof(arg_.binlog_row_image_type_));
  return checksum;
}

void ObTableQueryAndMutateP::reset_ctx()
{
  need_retry_in_queue_ = false;
  one_result_.reset();
  ObTableApiProcessorBase::reset_ctx();
  tb_ctx_.reset();
}

int ObTableQueryAndMutateP::before_process()
{
  is_tablegroup_req_ = ObHTableUtils::is_tablegroup_req(arg_.table_name_, arg_.entity_type_);
  return ParentType::before_process();
}

int32_t ObTableQueryAndMutateP::get_stat_process_type(bool is_hkv, bool is_check_and_execute, ObTableOperationType::Type type)
{
  int32_t process_type = ObTableProccessType::TABLE_API_PROCESS_TYPE_MAX;

  if (is_hkv) {
    switch (type) {
      case table::ObTableOperationType::DEL: {
        process_type = ObTableProccessType::TABLE_API_HBASE_CHECK_AND_DELETE;
        break;
      }
      case table::ObTableOperationType::INSERT_OR_UPDATE: {
        process_type = ObTableProccessType::TABLE_API_HBASE_CHECK_AND_PUT;
        break;
      }
      case table::ObTableOperationType::INCREMENT: {
        process_type = ObTableProccessType::TABLE_API_HBASE_INCREMENT;
        break;
      }
      case table::ObTableOperationType::APPEND: {
        process_type = ObTableProccessType::TABLE_API_HBASE_APPEND;
        break;
      }
      default: {
        process_type = ObTableProccessType::TABLE_API_PROCESS_TYPE_MAX;
        break;
      }
    }
  } else { // tableapi
    if (is_check_and_execute) {
      process_type = ObTableProccessType::TABLE_API_CHECK_AND_INSERT_UP;
    } else {
      process_type = ObTableProccessType::TABLE_API_QUERY_AND_MUTATE;
    }
  }
  return process_type;
}

int ObTableQueryAndMutateP::try_process()
{
  int ret = OB_SUCCESS;
  // query_and_mutate request arg does not contain consisteny_level_
  // @see ObTableQueryAndMutateRequest
  const ObTableConsistencyLevel consistency_level = ObTableConsistencyLevel::STRONG;
  const ObTableQuery &query = arg_.query_and_mutate_.get_query();
  int64_t affected_rows = 0;
  const bool is_hkv = (ObTableEntityType::ET_HKV == arg_.entity_type_);
  ObHTableLockHandle *lock_handle = nullptr;
  ObLSID ls_id;
  bool exist_global_index = false;
  table_id_ = arg_.table_id_;
  stat_process_type_ = get_stat_process_type(is_hkv,
                                             arg_.query_and_mutate_.is_check_and_execute(),
                                             arg_.query_and_mutate_.get_mutations().at(0).type());
  if (ObTableProccessType::TABLE_API_HBASE_INCREMENT != stat_process_type_ &&
      ObTableProccessType::TABLE_API_HBASE_APPEND != stat_process_type_) {
    arg_.query_and_mutate_.get_query().set_batch(1);
  }

  if (OB_FAIL(init_schema_info(arg_.table_name_, table_id_))) {
    LOG_WARN("fail to init schema info", K(ret), K(arg_.table_name_));
  } else if (OB_FAIL(get_tablet_id(simple_table_schema_, arg_.tablet_id_, arg_.table_id_, tablet_id_))) {
    LOG_WARN("fail to get tablet id", K(ret), K(arg_.table_id_));
  } else if (OB_FAIL(get_ls_id(tablet_id_, ls_id))) {
    LOG_WARN("fail to get ls id", K(ret), K_(tablet_id));
  } else if (OB_FAIL(check_table_has_global_index(exist_global_index, schema_cache_guard_))) {
    LOG_WARN("fail to check global index", K(ret), K_(table_id));
  } else if (is_hkv && OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret));
  } else if (is_hkv && OB_FAIL(ObHTableUtils::lock_htable_row(table_id_, query, *lock_handle, ObHTableLockMode::EXCLUSIVE))) {
    LOG_WARN("fail to lock htable row", K(ret), K_(table_id), K(query));
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 consistency_level,
                                 ls_id,
                                 get_timeout_ts(),
                                 exist_global_index))) {
    LOG_WARN("fail to start readonly transaction", K(ret));
  } else if (is_hkv && FALSE_IT(lock_handle->set_tx_id(get_trans_desc()->tid()))) {
  } else {
    ObTableQMParam qm_param(arg_.query_and_mutate_);
    qm_param.table_id_ = table_id_;
    qm_param.tablet_id_ = tablet_id_;
    qm_param.timeout_ts_ = get_timeout_ts();
    qm_param.credential_ = credential_;
    qm_param.entity_type_ = arg_.entity_type_;
    qm_param.trans_desc_ = get_trans_desc();
    qm_param.tx_snapshot_ = &get_tx_snapshot();
    qm_param.query_and_mutate_result_ = &result_;
    qm_param.schema_guard_ = &schema_guard_;
    qm_param.simple_table_schema_ = simple_table_schema_;
    qm_param.schema_cache_guard_ = &schema_cache_guard_;
    qm_param.sess_guard_ = &sess_guard_;
    SMART_VAR(QueryAndMutateHelper, helper, allocator_, qm_param, audit_ctx_) {
      if (OB_FAIL(helper.execute_query_and_mutate())) {
        LOG_WARN("fail to process query and mutate", KR(ret));
      } else {
        // do nothing
      }
    }
  }

  bool need_rollback_trans = (OB_SUCCESS != ret);
  int tmp_ret = ret;
  const bool use_sync = true;
  trans_param_.lock_handle_ = lock_handle;
  if (OB_FAIL(end_trans(need_rollback_trans, req_, nullptr/* ObTableCreateCbFunctor */, use_sync))) {
    LOG_WARN("failed to end trans", K(ret), "rollback", need_rollback_trans);
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

  // record events
  stat_row_count_ = 1;

  int64_t rpc_timeout = 0;
  if (NULL != rpc_pkt_) {
    rpc_timeout = rpc_pkt_->get_timeout();
  }
  #ifndef NDEBUG
    // debug mode
    LOG_INFO("[TABLE] execute query_and_mutate", K(ret), K(rpc_timeout), K_(retry_count));
  #else
    // release mode
    LOG_TRACE("[TABLE] execute query_and_mutate", K(ret), K(rpc_timeout), K_(retry_count),
              "receive_ts", get_receive_timestamp());
  #endif
  return ret;
}
