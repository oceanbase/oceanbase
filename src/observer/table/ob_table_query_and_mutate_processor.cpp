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
#include "ob_table_rpc_processor_util.h"
#include "observer/ob_service.h"
#include "storage/ob_partition_service.h"
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "ob_htable_utils.h"
using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObTableQueryAndMutateP::ObTableQueryAndMutateP(const ObGlobalContext &gctx)
    :ObTableRpcProcessor(gctx),
     allocator_(ObModIds::TABLE_PROC),
     query_ctx_(allocator_)
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
      ObTableOperation &mutation = const_cast<ObTableOperation&>(mutations.at(i));
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
  } else if (!hfilter.is_valid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("QueryAndMutate only supports hbase model table for now", K(ret));
  } else if (mutations.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should have at least one mutation operation", K(ret), K(mutations));
  } else {
    // these options are meaningless for QueryAndMutate users but we should control them internally
    query.set_batch(1);  // mutate for each row
    query.set_max_result_size(-1);

    hfilter.set_max_versions(1);
    hfilter.set_row_offset_per_column_family(0);
    hfilter.set_max_results_per_column_family(-1);
  }
  return ret;
}

void ObTableQueryAndMutateP::audit_on_finish()
{
  audit_record_.consistency_level_ = ObConsistencyLevel::STRONG; // todo: exact consistency
  audit_record_.return_rows_ = arg_.query_and_mutate_.return_affected_entity() ? result_.affected_entity_.get_row_count() : 0;
  audit_record_.table_scan_ = true; // todo: exact judgement
  audit_record_.affected_rows_ = result_.affected_rows_;
  audit_record_.try_cnt_ = retry_count_ + 1;
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
  query_ctx_.reset_query_ctx(part_service_);
  need_retry_in_queue_ = false;
  one_result_.reset();
  ObTableApiProcessorBase::reset_ctx();
}

ObTableAPITransCb *ObTableQueryAndMutateP::new_callback(rpc::ObRequest *req)
{
  UNUSED(req);
  return nullptr;
}

int ObTableQueryAndMutateP::get_partition_ids(uint64_t table_id, ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  uint64_t partition_id = arg_.partition_id_;
  if (OB_INVALID_ID == partition_id) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("partitioned table not supported", K(ret), K(table_id));
  } else {
    if (OB_FAIL(part_ids.push_back(partition_id))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTableQueryAndMutateP::rewrite_htable_query_if_need(const ObTableOperation &mutaion, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  if (ObTableEntityType::ET_HKV == arg_.entity_type_) {
    const ObHTableFilter &htable_filter = query.get_htable_filter();
    if (htable_filter.is_valid()) {
      const ObIArray<common::ObNewRange> &key_ranges = query.get_scan_ranges();
      if (key_ranges.count() != 1) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the count of key range of increment query must be 1", K(ret));
      } else {
        const ObIArray<ObString> &columns = htable_filter.get_columns();
        if (columns.count() < 1 && ObTableOperationType::DEL != mutaion.type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("must specified at least one column qualifier except delete", K(ret));
        } else if (columns.count() == 1) {  // from tableapi java client's view, all ops are based on cq
          const ObObj *start_key_ptr = key_ranges.at(0).start_key_.get_obj_ptr();
          int64_t start_key_cnt = key_ranges.at(0).start_key_.length();
          const ObObj *end_key_ptr = key_ranges.at(0).end_key_.get_obj_ptr();
          int64_t end_key_cnt = key_ranges.at(0).end_key_.length();
          if (start_key_cnt < 2 || end_key_cnt < 2) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("the rowkey must be longer than 2", K(ret), K(start_key_cnt), K(end_key_cnt));
          } else {
            ObObj htable_filter_cq;
            htable_filter_cq.set_varbinary(columns.at(0));
            ObObj &start_key_cq = const_cast<ObObj &>(start_key_ptr[1]);
            ObObj &end_key_cq = const_cast<ObObj &>(end_key_ptr[1]);
            if (OB_FAIL(ob_write_obj(allocator_, htable_filter_cq, start_key_cq))) {
              LOG_WARN("fail to deep copy obobj", K(ret));
            } else if (OB_FAIL(ob_write_obj(allocator_, htable_filter_cq, end_key_cq))) {
              LOG_WARN("fail to deep copy obobj", K(ret));
            } else {
              if (ObTableOperationType::DEL != mutaion.type()) {  // checkAnddelete delete all version
                query.set_limit(1);                               // only lock one row
              }
            }
          }
        } else {
        }  // we have to scan additional rows to get result with multi-column
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable query and mutate must have a valid htable filter", K(ret));
    }
  }
  return ret;
}

int ObTableQueryAndMutateP::try_process()
{
  int ret = OB_SUCCESS;
  const ObTableQuery &query = arg_.query_and_mutate_.get_query();
  ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
  int64_t rpc_timeout = 0;
  if (NULL != rpc_pkt_) {
    rpc_timeout = rpc_pkt_->get_timeout();
  }
  uint64_t &table_id = query_ctx_.param_table_id();
  query_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                        false/*ignored*/,
                        arg_.entity_type_,
                        table::ObBinlogRowImageType::MINIMAL/*ignored*/);
  ObSEArray<int64_t, 1> part_ids;
  const bool is_readonly = false;
  ObTableQueryResultIterator *result_iterator = nullptr;
  int32_t result_count = 0;
  int64_t affected_rows = 0;
  const ObTableOperation &mutation = mutations.at(0);
  bool is_index_supported = true;
  if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(check_table_index_supported(table_id, is_index_supported))) {
    LOG_WARN("fail to check index supported", K(ret), K(table_id));
  } else if (OB_UNLIKELY(!is_index_supported)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("index type is not supported by table api", K(ret));
  } else if (OB_FAIL(rewrite_htable_query_if_need(mutation, const_cast<ObTableQuery &>(query)))) {
    LOG_WARN("fail to rewrite query", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (FALSE_IT(query_ctx_.param_partition_id() = part_ids.at(0))) {
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_UPDATE, table_id, part_ids,
                                 get_timeout_ts()))) {
    LOG_WARN("failed to start readonly transaction", K(ret));
  } else if (OB_FAIL(table_service_->execute_query(
                 query_ctx_, query, one_result_, result_iterator, true /* for update */))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to execute query", K(ret), K(table_id), K(arg_.entity_type_));
    }
  } else {
    // one_result references to result_
    ObTableQueryResult *one_result = nullptr;
    // htable queryAndXXX only check one row
    ret = result_iterator->get_next_result(one_result);
    if (OB_ITER_END == ret || OB_SUCC(ret)) {
      ret = OB_SUCCESS;
      one_result = &one_result_;  // empty result is OK for APPEND and INCREMENT
      switch (mutation.type()) {
        case ObTableOperationType::DEL:  // checkAndDelete
          stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_CHECK_AND_DELETE;
          if (one_result->get_row_count() > 0) {  // not empty result means check passed
            affected_rows = 1;
            int64_t deleted_cells = 0;
            ObHTableDeleteExecutor delete_executor(allocator_,
                                                   table_id,
                                                   part_ids.at(0),
                                                   get_timeout_ts(),
                                                   this,
                                                   table_service_,
                                                   part_service_);
            ret = delete_executor.htable_delete(mutations, deleted_cells);
          }
          break;
        case ObTableOperationType::INSERT_OR_UPDATE:  // checkAndPut
          stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_CHECK_AND_PUT;
          if (one_result->get_row_count() > 0) { // not empty result means check passed
            affected_rows = 1;
            int64_t put_rows = 0;
            ObHTablePutExecutor put_executor(allocator_,
                                             table_id,
                                             part_ids.at(0),
                                             get_timeout_ts(),
                                             this,
                                             table_service_,
                                             part_service_);
            ret = put_executor.htable_put(mutations, put_rows);
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("put with empty check result is not supported currently", K(ret));
          }
          break;
        case ObTableOperationType::INCREMENT:         // Increment
          stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_INCREMENT;
          if (one_result->get_row_count() > 0) {  // not empty result means check passed
            affected_rows = 1;
            ObHTableIncrementExecutor inc_executor(ObTableOperationType::INCREMENT,
                                                   allocator_,
                                                   table_id,
                                                   part_ids.at(0),
                                                   get_timeout_ts(),
                                                   this,
                                                   table_service_,
                                                   part_service_);
            int64_t put_cells = 0;
            table::ObTableQueryResult *results = NULL;
            if (arg_.query_and_mutate_.return_affected_entity()) {
              results = &result_.affected_entity_;
            }
            ret = inc_executor.htable_increment(*one_result, mutations, put_cells, results);
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("increment with empty check result is not supported currently", K(ret));
          }
          break;
        case ObTableOperationType::APPEND:             // Append
          stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_APPEND;
          if (one_result->get_row_count() > 0) {  // not empty result means check passed
            affected_rows = 1;
            ObHTableIncrementExecutor apd_executor(ObTableOperationType::APPEND,
                                                   allocator_,
                                                   table_id,
                                                   part_ids.at(0),
                                                   get_timeout_ts(),
                                                   this,
                                                   table_service_,
                                                   part_service_);
            int64_t put_cells = 0;
            table::ObTableQueryResult *results = NULL;
            if (arg_.query_and_mutate_.return_affected_entity()) {
              results = &result_.affected_entity_;
            }
            ret = apd_executor.htable_increment(*one_result, mutations, put_cells, results);
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("append with empty check result is not supported currently", K(ret));
          }
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported mutation type", K(ret), "type", mutation.type());
          break;
      }
    } else {
      LOG_WARN("failed to get one row", K(ret));
    }

    NG_TRACE_EXT(tag1, OB_ID(found_rows), result_count,
                 OB_ID(affected_rows), affected_rows);
  }
  query_ctx_.destroy_result_iterator(part_service_);
  bool need_rollback_trans = (OB_SUCCESS != ret);
  int tmp_ret = ret;
  const bool use_sync = true;
  if (OB_FAIL(end_trans(need_rollback_trans, req_, get_timeout_ts(), use_sync))) {
    LOG_WARN("failed to end trans", K(ret), "rollback", need_rollback_trans);
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  if (OB_SUCC(ret)) {
    result_.affected_rows_ = affected_rows;
  } else {
    result_.affected_rows_ = 0;
  }

  // record events
  audit_row_count_ = 1;

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute query_and_mutate", K(ret), K_(arg), K(rpc_timeout),
           K_(retry_count), K(result_count));
#else
  // release mode
  LOG_TRACE("[TABLE] execute query_and_mutate", K(ret), K_(arg),
            K(rpc_timeout), K_(retry_count),
            "receive_ts", get_receive_timestamp(), K(result_count));
#endif
  return ret;
}
