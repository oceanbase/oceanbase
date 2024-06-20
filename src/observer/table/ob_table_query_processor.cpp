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
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "ob_table_scan_executor.h"
#include "ob_table_cg_service.h"
#include "ob_htable_filter_operator.h"
#include "ob_table_query_common.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObTableQueryP::ObTableQueryP(const ObGlobalContext &gctx)
    : ObTableRpcProcessor(gctx),
      allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      tb_ctx_(allocator_),
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
  } else if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
                arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "consistency level");
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
  audit_record_.table_scan_ = tb_ctx_.is_full_table_scan();
  audit_record_.affected_rows_ = 0;
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
  need_retry_in_queue_ = false;
  result_row_count_ = 0;
  ObTableApiProcessorBase::reset_ctx();
}

ObTableAPITransCb *ObTableQueryP::new_callback(rpc::ObRequest *req)
{
  UNUSED(req);
  return nullptr;
}

int ObTableQueryP::get_tablet_ids(uint64_t table_id, ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id = arg_.tablet_id_;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  if (!tablet_id.is_valid()) {
    const uint64_t tenant_id = MTL_ID();
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id), K(table_schema));
    } else if (!table_schema->is_partitioned_table()) {
      tablet_id = table_schema->get_tablet_id();
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("partitioned table not supported", K(ret), K(table_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTableQueryP::init_tb_ctx(ObTableApiCacheGuard &cache_guard)
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  bool is_weak_read = arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL;
  tb_ctx_.set_scan(true);
  tb_ctx_.set_entity_type(arg_.entity_type_);

  if (tb_ctx_.is_init()) {
    LOG_INFO("tb ctx has been inited", K_(tb_ctx));
  } else if (OB_FAIL(tb_ctx_.init_common(credential_,
                                         arg_.tablet_id_,
                                         arg_.table_name_,
                                         get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.table_name_));
  } else if (OB_FAIL(tb_ctx_.init_scan(arg_.query_, is_weak_read))) {
    LOG_WARN("fail to init table ctx scan part", K(ret), K(arg_.table_name_), K(arg_.table_id_));
  } else if (arg_.table_id_ != tb_ctx_.get_ref_table_id()) { // todo: global_index need adapt
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("arg table id is not equal to schema table id", K(ret), K(arg_.table_id_), K(tb_ctx_.get_ref_table_id()));
  } else if (OB_FAIL(cache_guard.init(&tb_ctx_))) {
    LOG_WARN("fail to init cache guard", K(ret));
  } else if (OB_FAIL(cache_guard.get_expr_info(&tb_ctx_, expr_frame_info))) {
    LOG_WARN("fail to get expr frame info from cache", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(tb_ctx_, *expr_frame_info))) {
    LOG_WARN("fail to alloc expr memory", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx_));
  } else {
    tb_ctx_.set_init_flag(true);
    tb_ctx_.set_expr_info(expr_frame_info);
  }

  return ret;
}

int ObTableQueryP::query_and_result(ObTableApiScanExecutor *executor)
{
  int ret = OB_SUCCESS;
  ObTableQueryResultIterator *result_iter = nullptr;
  bool is_hkv = (ObTableEntityType::ET_HKV == arg_.entity_type_);
  int32_t result_count = 0;
  const int64_t timeout_ts = get_timeout_ts();
  ObTableApiScanRowIterator row_iter;

  // 1. create result iterator
  if (OB_FAIL(ObTableQueryUtils::generate_query_result_iterator(allocator_,
                                                                arg_.query_,
                                                                is_hkv,
                                                                result_,
                                                                tb_ctx_,
                                                                result_iter))) {
    LOG_WARN("fail to generate query result iterator", K(ret));
  } else if (OB_FAIL(row_iter.open(executor))) {
    LOG_WARN("fail to open scan row iterator", K(ret));
  } else {
    result_iter->set_scan_result(&row_iter);
    // hbase model, compress the result packet
    if (is_hkv) {
      ObCompressorType compressor_type = INVALID_COMPRESSOR;
      if (OB_FAIL(ObCompressorPool::get_instance().get_compressor_type(
          GCONF.tableapi_transport_compress_func, compressor_type))) {
        compressor_type = INVALID_COMPRESSOR;
      } else if (NONE_COMPRESSOR == compressor_type) {
        compressor_type = INVALID_COMPRESSOR;
      }
      this->set_result_compress_type(compressor_type);
      ret = OB_SUCCESS; // reset ret
    }
  }

  // 2. loop get row and serialize row
  if (OB_SUCC(ret)) {
    // one_result references to result_
    ObTableQueryResult *one_result = nullptr;
    while (OB_SUCC(ret)) {
      ++result_count;
      // the last result_ does not need flush, it will be send automatically
      if (ObTimeUtility::current_time() > timeout_ts) {
        ret = OB_TRANS_TIMEOUT;
        LOG_WARN("exceed operatiton timeout", K(ret));
      } else if (OB_FAIL(result_iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      } else if (result_iter->has_more_result()) {
        if (OB_FAIL(this->flush())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to flush result packet", K(ret));
          } else {
            LOG_TRACE("user abort the stream rpc", K(ret));
          }
        } else {
          LOG_DEBUG("flush one result", K(ret), "row_count", result_.get_row_count());
          result_row_count_ += result_.get_row_count();
          result_.reset_except_property();
        }
      } else {
        // no more result
        result_row_count_ += result_.get_row_count();
        break;
      }
    }

    if (OB_LIKELY(ret == OB_ITER_END)) {
      ret = OB_SUCCESS;
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = row_iter.close())) {
      LOG_WARN("fail to close row iter", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    ObTableQueryUtils::destroy_result_iterator(result_iter);

    LOG_DEBUG("last result", K(ret), "row_count", result_.get_row_count());
    NG_TRACE_EXT(tag1, OB_ID(return_rows), result_count, OB_ID(arg2), result_row_count_);
  }

  // record events
  if (is_hkv) {
    stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_QUERY; // hbase query
  } else {
    stat_event_type_ = ObTableProccessType::TABLE_API_TABLE_QUERY;// table query
  }
  audit_row_count_ = result_row_count_;

  #ifndef NDEBUG
    // debug mode
    LOG_INFO("[TABLE] execute query", K(ret), K_(arg), K(timeout_ts),
             K_(retry_count), K(result_count), K_(result_row_count));
  #else
    // release mode
    LOG_TRACE("[TABLE] execute query", K(ret), K_(arg), K(timeout_ts), K_(retry_count),
              "receive_ts", get_receive_timestamp(), K(result_count), K_(result_row_count));
  #endif

  return ret;
}

int ObTableQueryP::try_process()
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;

  if (OB_FAIL(init_tb_ctx(cache_guard))) {
    LOG_WARN("fail to init table ctx", K(ret));
  } else if (OB_FAIL(cache_guard.get_spec<TABLE_API_EXEC_SCAN>(&tb_ctx_, spec))) {
    LOG_WARN("fail to get spec from cache", K(ret));
  } else if (OB_FAIL(spec->create_executor(tb_ctx_, executor))) {
    LOG_WARN("fail to generate executor", K(ret), K(tb_ctx_));
  } else if (FALSE_IT(table_id_ = arg_.table_id_)) {
  } else if (FALSE_IT(tablet_id_ = arg_.tablet_id_)) {
  } else if (OB_FAIL(start_trans(true, /* is_readonly */
                                 sql::stmt::T_SELECT,
                                 arg_.consistency_level_,
                                 tb_ctx_.get_ref_table_id(),
                                 tb_ctx_.get_ls_id(),
                                 tb_ctx_.get_timeout_ts()))) {
    LOG_WARN("fail to start readonly transaction", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(query_and_result(static_cast<ObTableApiScanExecutor*>(executor)))) {
    LOG_WARN("fail to query and result", K(ret));
  }

  if (OB_NOT_NULL(spec)) {
    spec->destroy_executor(executor);
    tb_ctx_.set_expr_info(nullptr);
  }

  int tmp_ret = ret;
  bool need_rollback_trans = (OB_SUCCESS != ret);
  if (OB_FAIL(end_trans(need_rollback_trans, req_, tb_ctx_.get_timeout_ts()))) {
    LOG_WARN("fail to end trans", K(ret), K(need_rollback_trans));
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

  return ret;
}