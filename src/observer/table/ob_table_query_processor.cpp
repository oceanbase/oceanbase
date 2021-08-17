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
#include "ob_table_query_processor.h"
#include "ob_table_rpc_processor_util.h"
#include "observer/ob_service.h"
#include "storage/ob_partition_service.h"
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObTableQueryP::ObTableQueryP(const ObGlobalContext &gctx)
    :ObTableRpcProcessor(gctx),
     allocator_(ObModIds::TABLE_PROC),
     table_service_ctx_(allocator_),
     result_row_count_(0)
{
  // the streaming interface may return multi packet. The memory may be freed after the first packet has been sended.
  // the deserialization of arg_ is shallow copy, so we need deep copy data to processor
  set_preserve_recv_data();
}

int ObTableQueryP::deserialize()
{
  arg_.query_.set_deserialize_allocator(&allocator_);
  return ParentType::deserialize();
}

int ObTableQueryP::check_arg()
{
  int ret = OB_SUCCESS;
  if (!arg_.query_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table query request", K(ret), "query", arg_.query_);
  } else if (arg_.consistency_level_ != ObTableConsistencyLevel::STRONG) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("some options not supported yet", K(ret),
             "consistency_level", arg_.consistency_level_);
  }
  return ret;
}

void ObTableQueryP::audit_on_finish()
{
  audit_record_.consistency_level_ = ObTableConsistencyLevel::STRONG == arg_.consistency_level_ ?
      ObConsistencyLevel::STRONG : ObConsistencyLevel::WEAK;
  audit_record_.return_rows_ = result_.get_row_count();
  audit_record_.table_scan_ = true; // todo: exact judgement
  audit_record_.affected_rows_ = result_.get_row_count();
  audit_record_.try_cnt_ = retry_count_ + 1;
}

uint64_t ObTableQueryP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, arg_.table_name_.ptr(), arg_.table_name_.length());
  checksum = ob_crc64(checksum, &arg_.consistency_level_, sizeof(arg_.consistency_level_));
  const uint64_t op_checksum = arg_.query_.get_checksum();
  checksum = ob_crc64(checksum, &op_checksum, sizeof(op_checksum));
  return checksum;
}

void ObTableQueryP::reset_ctx()
{
  table_service_ctx_.reset_query_ctx(part_service_);
  need_retry_in_queue_ = false;
  result_row_count_ = 0;
  ObTableApiProcessorBase::reset_ctx();
}

ObTableAPITransCb *ObTableQueryP::new_callback(rpc::ObRequest *req)
{
  UNUSED(req);
  return nullptr;
}

int ObTableQueryP::get_partition_ids(uint64_t table_id, ObIArray<int64_t> &part_ids)
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

int ObTableQueryP::try_process()
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = 0;
  if (NULL != rpc_pkt_) {
    rpc_timeout = rpc_pkt_->get_timeout();
  }
  const int64_t timeout_ts = get_timeout_ts();
  uint64_t &table_id = table_service_ctx_.param_table_id();
  table_service_ctx_.init_param(timeout_ts, this, &allocator_,
                                false/*ignored*/,
                                arg_.entity_type_,
                                table::ObBinlogRowImageType::MINIMAL/*ignored*/);
  ObSEArray<int64_t, 1> part_ids;
  const bool is_readonly = true;
  ObTableQueryResultIterator *result_iterator = nullptr;
  int32_t result_count = 0;
  if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (FALSE_IT(table_service_ctx_.param_partition_id() = part_ids.at(0))) {
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_SELECT, table_id, part_ids, timeout_ts))) {
    LOG_WARN("failed to start readonly transaction", K(ret));
  } else if (OB_FAIL(table_service_->execute_query(table_service_ctx_, arg_.query_,
                                                   result_, result_iterator))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to execute query", K(ret), K(table_id));
    }
  } else {
    // one_result references to result_
    ObTableQueryResult *one_result = nullptr;
    while (OB_SUCC(ret)) {
      ++result_count;
      // the last result_ does not need flush, it will be send automatically
      if (ObTimeUtility::current_time() > timeout_ts) {
        ret = OB_TRANS_TIMEOUT;
        LOG_WARN("exceed operatiton timeout", K(ret));
      } else if (OB_FAIL(result_iterator->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      } else if (result_iterator->has_more_result()) {
        if (OB_FAIL(this->flush())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to flush result packet", K(ret));
          } else {
            LOG_TRACE("user abort the stream rpc", K(ret));
          }
        } else {
          LOG_DEBUG("[yzfdebug] flush one result", K(ret), "row_count", result_.get_row_count());
          result_row_count_ += result_.get_row_count();
          result_.reset_except_property();
        }
      } else {
        // no more result
        result_row_count_ += result_.get_row_count();
        break;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    LOG_DEBUG("[yzfdebug] last result", K(ret), "row_count", result_.get_row_count());
    NG_TRACE_EXT(tag1, OB_ID(return_rows), result_count, OB_ID(arg2), result_row_count_);
  }
  table_service_ctx_.destroy_result_iterator(part_service_);
  bool need_rollback_trans = (OB_SUCCESS != ret);
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(need_rollback_trans, req_, timeout_ts))) {
    LOG_WARN("failed to end trans", K(ret), "rollback", need_rollback_trans);
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

  // record events
  stat_event_type_ = ObTableProccessType::TABLE_API_TABLE_QUERY;// table query
  
  audit_row_count_ = result_row_count_;

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute query", K(ret), K_(arg), K(rpc_timeout),
           K_(retry_count), K(result_count), K_(result_row_count));
#else
  // release mode
  LOG_TRACE("[TABLE] execute query", K(ret), K_(arg), K(rpc_timeout), K_(retry_count),
            "receive_ts", get_receive_timestamp(), K(result_count), K_(result_row_count));
#endif
  return ret;
}
