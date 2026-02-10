//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "rootserver/compaction_ttl/ob_compaction_ttl_service.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "share/schema/ob_table_schema.h"
#include "share/table/ob_ttl_util.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/compaction_ttl/ob_compaction_ttl_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace table;
using namespace observer;
namespace rootserver
{
ERRSIM_POINT_DEF(EN_COMPACTION_TTL_SERVICE_EXECUTE);
ERRSIM_POINT_DEF(EN_COMPACTION_TTL_SERVICE_AFTER_COMMIT);
ERRSIM_POINT_DEF(EN_COMPACTION_TTL_SERVICE_TTL_TIME);

OB_SERIALIZE_MEMBER_SIMPLE(ObTTLFilterInfoArg, info_, ls_id_, tablet_id_array_, ttl_filter_info_);

/*
* ObTTLFilterInfoHelper
* */
int ObTTLFilterInfoHelper::generate_ttl_filter_info(
    const ObTableSchema &data_table_schema,
    ObTTLFilterInfo &ttl_filter_info)
{
  int ret = OB_SUCCESS;
  ObSimpleTableTTLChecker ttl_checker;
  int64_t ttl_filter_us = 0;
  if (OB_FAIL(ttl_checker.init(data_table_schema, data_table_schema.get_ttl_definition(), true/*in_full_column_order*/))) {
    LOG_WARN("failed to init ttl checker", KR(ret), K(data_table_schema));
  } else if (OB_FAIL(ttl_checker.get_ttl_filter_us(ttl_filter_us))) {
    LOG_WARN("failed to get ttl filter ts", KR(ret), K(data_table_schema));
  } else {
    ttl_filter_info.ttl_filter_col_type_ = ObTTLFilterInfo::ObTTLFilterColType::ROWSCN;
    ttl_filter_info.ttl_filter_value_ = ttl_filter_us * 1000L; // us to ns

#ifdef ERRSIM
    if (EN_COMPACTION_TTL_SERVICE_TTL_TIME) { // s -> ns
      ttl_filter_info.ttl_filter_value_ = -EN_COMPACTION_TTL_SERVICE_TTL_TIME * 1000L * 1000L * 1000L;
      LOG_INFO("ERRSIM POINT EN_COMPACTION_TTL_SERVICE_TTL_TIME", KR(ret), K(ttl_filter_info));
    }
#endif

  }
  return ret;
}
#define CHECK_ERRSIM(POINT, ret)                                               \
  if (POINT) {                                                                 \
    ret = POINT;                                                               \
    FLOG_INFO("ERRSIM POINT", KR(ret), "errsim_name", #POINT);                 \
  }                                                                            \
  if (OB_FAIL(ret)) {                                                          \
    SERVER_EVENT_SYNC_ADD("ttl_errsim", "compaction_ttl_service_execute",      \
                          "tenant_id", tenant_id_, "errsim_name", #POINT,      \
                          "ret", ret);                                         \
  }

/*
* ObCompactionTTLService
* */
int ObCompactionTTLService::execute(
  const int64_t tenant_task_start_us,
  ObMySQLProxy &sql_proxy,
  transaction::ObTransID &tx_id,
  ObTTLTaskStatus &task_status)
{
  int ret = OB_SUCCESS;
  task_status = ObTTLTaskStatus::OB_TTL_TASK_INVALID;
  // start transaction
  const int64_t start_ts = ObTimeUtility::current_time();
  ObMySQLTransaction trans;
  int64_t tablet_cnt = 0;
  int64_t timeout_us = 0;
  const int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  SMART_VARS_2((ObSchemaGetterGuard, schema_guard_before), (ObSchemaGetterGuard, schema_guard_after)) {
    const ObTableSchema *schema_before_trans = nullptr;
    int64_t schema_version_before_trans = OB_INVALID_VERSION;
    const ObTableSchema *latest_data_table_schema = nullptr;
    int64_t minimum_safe_ttl_value = max_ddl_create_snapshot_;
    if (OB_FAIL(get_latest_schema_guard(sql_proxy, schema_guard_before, schema_before_trans))) {
      if (OB_TABLE_IS_DELETED != ret) {
        LOG_WARN("failed to get latest schema guard", KR(ret));
      }
    } else if (OB_FAIL(batch_get_tablet_info_before_trans(sql_proxy, schema_guard_before, *schema_before_trans, tablet_cnt))) {
      LOG_WARN("failed to batch get tablet info before trans", KR(ret));
    } else {
      schema_version_before_trans = schema_before_trans->get_schema_version();
#ifdef ERRSIM
      SERVER_EVENT_SYNC_ADD("ttl_errsim", "compaction_ttl_service_execute", "tenant_id", tenant_id_, "table_id", data_table_id_);
      DEBUG_SYNC(COMPACTION_TTL_CHECK_META_TABLE);
#endif
    }
    timeout_us = ObTimeUtility::current_time() + tablet_cnt * TIMEOUT_PER_PARTITION_US + TRANS_TIMEOUT_US;
    if (FAILEDx(trans.start(&sql_proxy, tenant_id_))) {
      LOG_WARN("failed to start transaction", KR(ret));
    } else if (FALSE_IT(THIS_WORKER.set_timeout_ts(timeout_us))) {
    } else if (FALSE_IT(tx_id = static_cast<ObInnerSQLConnection *>(trans.get_connection())->get_session().get_tx_id())) {
    } else if (OB_FAIL(ObDDLLock::lock_for_ttl_in_trans(*schema_before_trans, trans))) {
      // TableLock on data table to avoid DML
      // OnlineDDLLock on data table to avoid online DDL/transfer/split
      LOG_WARN("failed to lock table", KR(ret), K_(data_table_id), K(task_status));
    } else if (OB_FAIL(get_latest_schema_guard(sql_proxy, schema_guard_after, latest_data_table_schema))) {
      if (OB_TABLE_IS_DELETED != ret) {
        LOG_WARN("failed to get latest schema guard", KR(ret));
      }
    } else if (OB_UNLIKELY(schema_version_before_trans != latest_data_table_schema->get_schema_version())) {
      ret = OB_EAGAIN;
      task_status = ObTTLTaskStatus::OB_TTL_TASK_PENDING;
      LOG_WARN("[COMPACTION TTL] schema version is not the same, exist DDL after check meta table", KR(ret),
        K(schema_version_before_trans), KPC(latest_data_table_schema));
    } else if (OB_FAIL(ObTTLFilterInfoHelper::generate_ttl_filter_info(*latest_data_table_schema, ttl_filter_arg_.ttl_filter_info_))) {
      LOG_WARN("failed to prepare ttl filter info", KR(ret));
    } else if (FALSE_IT(minimum_safe_ttl_value = get_minimum_safe_ttl_value(*latest_data_table_schema))) {
    } else if (OB_UNLIKELY(ttl_filter_arg_.ttl_filter_info_.is_rowscn_filter()
        && ttl_filter_arg_.ttl_filter_info_.ttl_filter_value_ <= minimum_safe_ttl_value)) {
      const int64_t tenant_task_deadline_ns = tenant_task_start_us * 1000L + TTL_SYNC_WAIT_DDL_CREATE_SNAPSHOT_INTERVAL_NS;
      const int64_t predicted_ttl_filter_value = ttl_filter_arg_.ttl_filter_info_.ttl_filter_value_
                                                + TTL_SYNC_WAIT_DDL_CREATE_SNAPSHOT_INTERVAL_NS;
      if (tenant_task_deadline_ns > minimum_safe_ttl_value
          && predicted_ttl_filter_value > minimum_safe_ttl_value) {
        ret = OB_EAGAIN;
        task_status = ObTTLTaskStatus::OB_TTL_TASK_PENDING;
        LOG_WARN("[COMPACTION TTL] ttl filter value will be valid within 1 hour, pending for retry",
          KR(ret), K(ttl_filter_arg_.ttl_filter_info_), K(minimum_safe_ttl_value), K(latest_data_table_schema->get_ttl_flag()),
          K(tenant_task_start_us), K(predicted_ttl_filter_value));
      } else {
        task_status = ObTTLTaskStatus::OB_TTL_TASK_SKIP;
        LOG_INFO("[COMPACTION TTL] filter value is too old for ddl create snapshot or being scn ttl time, sync for next TTL task execution", KR(ret),
          K(ttl_filter_arg_.ttl_filter_info_), K(minimum_safe_ttl_value), K(latest_data_table_schema->get_ttl_flag()));
      }
    } else if (OB_FAIL(lock_table_and_sync_mds(trans, *latest_data_table_schema))) {
      LOG_WARN("failed to lock table and sync mds", KR(ret), K(latest_data_table_schema));
    } else if (OB_FAIL(loop_index_table(trans, schema_guard_before, *latest_data_table_schema))) {
      if (OB_TABLE_IS_DELETED != ret) {
        LOG_WARN("failed to loop index table", KR(ret), K(task_status));
      }
    }
    THIS_WORKER.set_timeout_ts(original_timeout_us);
  }
#ifdef ERRSIM
  CHECK_ERRSIM(EN_COMPACTION_TTL_SERVICE_EXECUTE, ret);
#endif
  ret = trans.handle_trans_in_the_end(ret);
#ifdef ERRSIM
  CHECK_ERRSIM(EN_COMPACTION_TTL_SERVICE_AFTER_COMMIT, ret);
#endif
  const int64_t end_ts = ObTimeUtility::current_time();
  if (ObTTLTaskStatus::OB_TTL_TASK_INVALID == task_status) {
    if (OB_EAGAIN == ret && (end_ts > tenant_task_start_us + TTL_SYNC_WAIT_DDL_LOCK_INTERVAL_US)) {
      task_status = ObTTLTaskStatus::OB_TTL_TASK_SKIP;
    } else {
      task_status = OB_SUCCESS == ret ? ObTTLTaskStatus::OB_TTL_TASK_FINISH : ObTTLTaskStatus::OB_TTL_TASK_FAILED;
    }
  }
  LOG_INFO("compaction ttl service execute cost", KR(ret), K_(data_table_id), K(start_ts), K(end_ts), K(end_ts - start_ts), K_(sync_mds_cost_us),
    K(tablet_cnt), K(original_timeout_us), K(timeout_us), K(task_status), K(tenant_task_start_us));
  return ret;
}

int ObCompactionTTLService::batch_get_tablet_info_before_trans(
  ObMySQLProxy &sql_proxy,
  ObSchemaGetterGuard &schema_guard,
  const ObTableSchema &latest_data_table_schema,
  int64_t &tablet_cnt)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObAuxTableMetaInfo> &simple_index_infos = latest_data_table_schema.get_simple_index_infos();
  ObSEArray<ObTabletInfo, 8> tablet_infos;
  int64_t max_ddl_create_snapshot = 0;
  tablet_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    tablet_ls_pair_array_.reuse();
    tablet_infos.reuse();
    const ObAuxTableMetaInfo &index_info = simple_index_infos.at(i);
    const ObTableSchema *index_schema = nullptr;
    if (OB_UNLIKELY(ObCompactionTTLUtil::is_vec_index_for_ttl(index_info.index_type_))) { // ttl schema should not have vec index
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ttl schema should not have vec index", KR(ret), K(index_info));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_info.table_id_, index_schema))) {
      LOG_WARN("failed to get index schema", KR(ret), K_(tenant_id), K(index_info.table_id_));
    } else if (OB_UNLIKELY(nullptr == index_schema || !index_schema->is_index_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index schema", KR(ret), KPC(index_schema));
    } else if (OB_FAIL(get_tablet_ls_info(sql_proxy, *index_schema, tablet_ls_pair_array_))) {
      LOG_WARN("failed to get tablet ls info", KR(ret), K(index_schema));
    } else if (OB_FAIL(ObTabletMetaTableCompactionOperator::batch_get_tablet_info(sql_proxy, tenant_id_, tablet_ls_pair_array_, tablet_infos))) {
      if (OB_EAGAIN == ret) {
        LOG_INFO("tablet info in meta table is not ready, wait for next check", KR(ret), K_(tenant_id), K_(tablet_ls_pair_array));
      } else {
        LOG_WARN("failed to batch get tablet info", KR(ret), K_(tenant_id), K_(tablet_ls_pair_array));
      }
    } else {
      tablet_cnt += tablet_infos.count();
      for (int64_t j = 0; OB_SUCC(ret) && j < tablet_infos.count(); ++j) {
        const common::ObArray<ObTabletReplica> &tablet_replicas = tablet_infos.at(j).get_replicas();
        if (OB_UNLIKELY(tablet_replicas.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet replicas is empty", KR(ret), K(tablet_infos.at(j)));
        } else {
          max_ddl_create_snapshot = MAX(max_ddl_create_snapshot, tablet_replicas.at(0).get_ddl_create_snapshot());
        }
      } // for
    }
  } // for
  if (OB_SUCC(ret)) {
    max_ddl_create_snapshot_ = max_ddl_create_snapshot;
    tablet_cnt += latest_data_table_schema.get_all_part_num();
  }
  return ret;
}

int ObCompactionTTLService::get_latest_schema_guard(
  ObMySQLProxy &sql_proxy,
  ObSchemaGetterGuard &schema_guard,
  const ObTableSchema *&latest_data_table_schema)
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id_;
  int64_t latest_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(GCTX.schema_service_->get_schema_version_in_inner_table(
        sql_proxy, schema_status, latest_schema_version))) {
    LOG_WARN("Failed to get schema version in inner table", K(ret), K_(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->async_refresh_schema(tenant_id_, latest_schema_version))) {
    LOG_WARN("Failed to async refresh schema", K(ret), K_(tenant_id), K(latest_schema_version));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("Failed to get tenant schema guard", K(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id_, latest_data_table_schema))) {
    LOG_WARN("Failed to get table schema", K(ret), K_(tenant_id), K_(data_table_id));
  } else if (nullptr == latest_data_table_schema) {
    ret = OB_TABLE_IS_DELETED;
    LOG_INFO("table is deleted", K(ret), K_(data_table_id));
  }
  return ret;
}

int ObCompactionTTLService::get_schema_and_sync_mds(
  ObMySQLTransaction &trans,
  ObSchemaGetterGuard &schema_guard,
  const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", KR(ret), K_(tenant_id), K(table_id));
  } else if (nullptr == table_schema) {
    ret = OB_TABLE_IS_DELETED;
    LOG_INFO("table is deleted", K(ret), K_(tenant_id), K(table_id));
  } else if (OB_FAIL(lock_table_and_sync_mds(trans, *table_schema))) {
    LOG_WARN("failed to lock table and sync mds", KR(ret), K_(tenant_id), K(table_id), "table_name", table_schema->get_table_name());
  }
  return ret;
}

int ObCompactionTTLService::loop_index_table(
  ObMySQLTransaction &trans,
  ObSchemaGetterGuard &schema_guard,
  const ObTableSchema &latest_data_table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t data_table_id = latest_data_table_schema.get_table_id();
  const common::ObIArray<ObAuxTableMetaInfo> &simple_index_infos = latest_data_table_schema.get_simple_index_infos();
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    const ObAuxTableMetaInfo &index_info = simple_index_infos.at(i);
    const ObTableSchema *index_schema = nullptr;
    if (OB_UNLIKELY(ObCompactionTTLUtil::is_vec_index_for_ttl(index_info.index_type_))) { // ttl schema should not have vec index
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ttl schema should not have vec index", KR(ret), K_(tenant_id), KPC(index_schema));
    } else if (OB_FAIL(get_schema_and_sync_mds(trans, schema_guard, index_info.table_id_))) {
      LOG_WARN("failed to get index schema and sync mds", KR(ret), K_(tenant_id), K(data_table_id), K(index_info.table_id_));
    }
  } // for
  if (OB_SUCC(ret) && latest_data_table_schema.has_lob_aux_table()) {
    if (OB_FAIL(get_schema_and_sync_mds(trans, schema_guard, latest_data_table_schema.get_aux_lob_meta_tid()))) {
      LOG_WARN("failed to get lob_meta schema and sync mds", KR(ret), K_(tenant_id), K(data_table_id), K(latest_data_table_schema.get_aux_lob_meta_tid()));
    } else if (OB_FAIL(get_schema_and_sync_mds(trans, schema_guard, latest_data_table_schema.get_aux_lob_piece_tid()))) {
      LOG_WARN("failed to get lob_piece schema and sync mds", KR(ret), K_(tenant_id), K(data_table_id), K(latest_data_table_schema.get_aux_lob_piece_tid()));
    }
  }
  return ret;
}

int ObCompactionTTLService::get_tablet_ls_info(
    common::ObISQLClient &sql_proxy,
    const share::schema::ObTableSchema &schema,
    ObIArray<share::ObTabletLSPair> &tablet_ls_pair_array)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 8> tmp_index_tablet_array;
  ObSEArray<ObLSID, 8> tmp_ls_id_array;
  tablet_ls_pair_array.reuse();
  if (OB_FAIL(schema.get_tablet_ids(tmp_index_tablet_array))) {
    LOG_WARN("failed to get tablet id from schema", KR(ret), K(schema));
  } else if (tmp_index_tablet_array.empty()) {
    // do nothing
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(sql_proxy, tenant_id_, tmp_index_tablet_array, tmp_ls_id_array))) {
    // already check count of tablet_ids and ls_ids is the same
    LOG_WARN("failed to get ls id array", KR(ret), K_(tenant_id), K(tmp_index_tablet_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_index_tablet_array.count(); ++i) {
      ObTabletLSPair tablet_ls_pair(tmp_index_tablet_array.at(i), tmp_ls_id_array.at(i));
      if (OB_FAIL(tablet_ls_pair_array.push_back(tablet_ls_pair))) {
        LOG_WARN("failed to push back tablet ls pair", KR(ret), K(tablet_ls_pair));
      }
    }
  }
  return ret;
}

int ObCompactionTTLService::lock_table_and_sync_mds(
  ObMySQLTransaction &trans,
  const ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = schema.get_table_id();
  tablet_ls_pair_array_.reuse();
  // data table already locked after start transaction
  if (table_id != data_table_id_ && OB_FAIL(ObDDLLock::lock_for_sync_mds_in_trans(tenant_id_, table_id, trans))) {
    LOG_WARN("failed to lock table", KR(ret), K_(tenant_id), K(table_id));
  } else if (OB_FAIL(get_tablet_ls_info(trans, schema, tablet_ls_pair_array_))) {
    LOG_WARN("failed to get tablet ls info", KR(ret), K(schema));
  } else {
    // filter col is different in different data/index table
    ttl_filter_arg_.ttl_filter_info_.ttl_filter_col_idx_ = schema.get_rowkey_column_num();
    if (OB_FAIL(loop_to_register_mds(trans))) {
      LOG_WARN("failed to register ttl filter info", KR(ret));
    }
  }
  return ret;
}

bool compare(
    const ObTabletLSPair &lhs,
    const ObTabletLSPair &rhs)
{
  bool bret = true;
  if (lhs.get_ls_id() == rhs.get_ls_id()) {
    bret = lhs.get_tablet_id() < rhs.get_tablet_id();
  } else {
    bret = lhs.get_ls_id() < rhs.get_ls_id();
  }
  return bret;
}

int ObCompactionTTLService::loop_to_register_mds(ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *conn = nullptr;
  if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is NULL", KR(ret));
  } else {
    ttl_filter_arg_.ttl_filter_info_.key_.tx_id_ = conn->get_session().get_tx_id();
    lib::ob_sort(tablet_ls_pair_array_.begin(), tablet_ls_pair_array_.end(), compare);
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < tablet_ls_pair_array_.count(); ++idx) {
    const ObTabletLSPair &tablet_ls_pair = tablet_ls_pair_array_.at(idx);
    if (!ttl_filter_arg_.ls_id_.is_valid()) {
      ttl_filter_arg_.ls_id_ = tablet_ls_pair.get_ls_id();
      ttl_filter_arg_.tablet_id_array_.reset();
    }
    if (OB_FAIL(ttl_filter_arg_.tablet_id_array_.push_back(tablet_ls_pair.get_tablet_id()))) {
      LOG_WARN("failed to push back tablet id", KR(ret), K(tablet_ls_pair));
    } else if (idx == tablet_ls_pair_array_.count() - 1
      || ttl_filter_arg_.ls_id_ != tablet_ls_pair_array_.at(idx + 1).get_ls_id()) {
      // sync mds for same ls_id in batch
      const int64_t start_ts = ObTimeUtility::current_time();
      if (OB_FAIL(ObSyncMDSService::register_mds<ObTTLFilterInfoArg>(
          *conn, allocator_, tenant_id_, ttl_filter_arg_,
          transaction::ObTxDataSourceType::SYNC_TTL_FILTER_INFO))) {
        LOG_WARN("failed to register mds", KR(ret), K_(tenant_id), K(ttl_filter_arg_));
      } else {
        const int64_t end_ts = ObTimeUtility::current_time();
        sync_mds_cost_us_ += (end_ts - start_ts);
        ttl_filter_arg_.ls_id_.reset();
        allocator_.reuse();
      }
    }
  } // for
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
