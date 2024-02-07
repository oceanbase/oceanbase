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
#include "storage/tx_storage/ob_access_service.h"
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "ob_htable_utils.h"
#include "ob_table_cg_service.h"
#include "ob_htable_filter_operator.h"
#include "ob_table_filter.h"
#include "ob_table_op_wrapper.h"
#include "ob_table_query_and_mutate_helper.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObTableQueryAndMutateP::ObTableQueryAndMutateP(const ObGlobalContext &gctx)
    :ObTableRpcProcessor(gctx),
     allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
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
  audit_record_.return_rows_ = result_.affected_entity_.get_row_count();
  audit_record_.table_scan_ = tb_ctx_.is_full_table_scan();
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
  need_retry_in_queue_ = false;
  one_result_.reset();
  ObTableApiProcessorBase::reset_ctx();
}

ObTableAPITransCb *ObTableQueryAndMutateP::new_callback(rpc::ObRequest *req)
{
  UNUSED(req);
  return nullptr;
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

  if (FALSE_IT(table_id_ = arg_.table_id_)) {
  } else if (OB_FAIL(get_tablet_id(arg_.tablet_id_, arg_.table_id_, tablet_id_))) {
    LOG_WARN("fail to get tablet id", K(ret), K(arg_.table_id_));
  } else if (OB_FAIL(get_ls_id(tablet_id_, ls_id))) {
    LOG_WARN("fail to get ls id", K(ret), K_(tablet_id));
  } else if (is_hkv && OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret));
  } else if (is_hkv && OB_FAIL(ObHTableUtils::lock_htable_row(table_id_, query, *lock_handle, ObHTableLockMode::EXCLUSIVE))) {
    LOG_WARN("fail to lock htable row", K(ret), K_(table_id), K(query));
  } else if (OB_FAIL(check_table_has_global_index(table_id_, exist_global_index))) {
    LOG_WARN("fail to check global index", K(ret), K_(table_id));
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 sql::stmt::T_UPDATE,
                                 consistency_level,
                                 ls_id,
                                 get_timeout_ts(),
                                 exist_global_index))) {
    LOG_WARN("fail to start readonly transaction", K(ret));
  } else if (is_hkv && FALSE_IT(lock_handle->set_tx_id(get_trans_desc()->tid()))) {
  } else {
    SMART_VAR(QueryAndMutateHelper, helper, allocator_, arg_.query_and_mutate_,
              table_id_, tablet_id_, get_timeout_ts(), credential_, arg_.entity_type_,
              get_trans_desc(), get_tx_snapshot(), result_) {
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
  if (OB_FAIL(end_trans(need_rollback_trans, req_, get_timeout_ts(), use_sync, lock_handle))) {
    LOG_WARN("failed to end trans", K(ret), "rollback", need_rollback_trans);
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

  // record events
  audit_row_count_ = 1;

  int64_t rpc_timeout = 0;
  if (NULL != rpc_pkt_) {
    rpc_timeout = rpc_pkt_->get_timeout();
  }
  #ifndef NDEBUG
    // debug mode
    LOG_INFO("[TABLE] execute query_and_mutate", K(ret), K_(arg), K(rpc_timeout),
             K_(retry_count));
  #else
    // release mode
    LOG_TRACE("[TABLE] execute query_and_mutate", K(ret), K_(arg),
              K(rpc_timeout), K_(retry_count),
              "receive_ts", get_receive_timestamp());
  #endif
  return ret;
}
