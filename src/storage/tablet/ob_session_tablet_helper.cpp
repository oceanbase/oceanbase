/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/tablet/ob_session_tablet_helper.h"

#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_tablet_creator.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "share/ls/ob_ls_operator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/tablet/ob_tablet_to_global_temporary_table_operator.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ddl/ob_ddl_lock.h" // ObDDLLock
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "logservice/data_dictionary/ob_data_dict_storager.h"
#include "share/schema/ob_schema_guard_wrapper.h"
#include "share/tablet/ob_tablet_to_table_history_operator.h"
#include "share/ob_unit_table_operator.h"
#include "share/ob_cluster_version.h"
#include "share/tablet/ob_drop_gtt_v2_session_tablet_arg.h"
#include "storage/tablet/ob_drop_gtt_v2_session_tablet_rpc.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{
ERRSIM_POINT_DEF(EN_SESSION_TABLET_GC_FAILED);

int serialize_inc_schema(
    const uint64_t tenant_id,
    common::ObMySQLTransaction &trans,
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("STSIncSche");
  common::ObArray<const share::schema::ObTenantSchema *> tenant_schemas;
  common::ObArray<const share::schema::ObDatabaseSchema *> database_schemas;
  common::ObArray<const share::schema::ObTableSchema *> table_schemas;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transaction is not started", KR(ret));
  } else if (OB_FAIL(table_schemas.push_back(&table_schema))) {
    LOG_WARN("failed to push back table schema", KR(ret));
  } else if (OB_FAIL(datadict::ObDataDictStorage::gen_and_serialize_dict_metas(allocator,
                                                                               tenant_schemas,
                                                                               database_schemas,
                                                                               table_schemas,
                                                                               buf,
                                                                               buf_len,
                                                                               pos))) {
    LOG_WARN("fail to serialize inc schemas", KR(ret), K(table_schema));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(pos > buf_len) || OB_UNLIKELY(pos < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect valid buf", KR(ret), KP(buf), K(buf_len), K(pos));
  } else {
    observer::ObInnerSQLConnection *conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    if (OB_ISNULL(conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connection is null", KR(ret));
    } else if (OB_FAIL(conn->register_multi_data_source(tenant_id, SYS_LS, transaction::ObTxDataSourceType::DDL_TRANS, buf, pos))) {
      LOG_WARN("fail to register tx data", KR(ret), K(tenant_id), K(table_schema));
    }
  }
  return ret;
}
int ObSessionTabletCreateHelper::set_table_ids(const common::ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      if (OB_FAIL(table_ids_.push_back(table_ids.at(i)))) {
        LOG_WARN("failed to push back table id", KR(ret), K(table_ids.at(i)));
      }
    }
  }
  return ret;
}

int ObSessionTabletCreateHelper::do_work()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  const share::schema::ObTableSchema *table_schema = nullptr;
  if (OB_ISNULL(schema_service) || OB_ISNULL(schema_service->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), KP(schema_service));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id_));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    observer::ObInnerSQLConnection *conn = NULL;
    if (OB_FAIL(trans_.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to begin transaction", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(tablet_creator_.init(true/* need_check_tablet_cnt */))) {
      LOG_WARN("failed to init tablet creator", KR(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_ids_.at(0), table_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(table_ids_.at(0)));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schema is null", KR(ret), K(table_ids_.at(0)));
    } else if (OB_UNLIKELY(PARTITION_LEVEL_ZERO != table_schema->get_part_level()
                        || !(table_schema->is_oracle_tmp_table_v2() || table_schema->is_oracle_tmp_table_v2_index_table()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("partition level is zero or table is not oracle tmp table v2", KR(ret), KPC(table_schema));
    } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans_.get_connection()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connection is null", KR(ret), K(tenant_id_));
    } else {
      // 1. Acquire a ROW EXCLUSIVE table lock on the table (Table ID).
      // 2. Acquire a SHARE Online DDL lock on the table (Table ID).
      const int64_t timeout_us = MIN(THIS_WORKER.get_timeout_remain(), 10000000/* us */);
      const uint64_t table_id = table_schema->is_oracle_tmp_table_v2_index_table() ? table_schema->get_data_table_id() : table_schema->get_table_id();
      transaction::tablelock::ObLockTableRequest table_lock_arg;
      share::schema::ObLatestSchemaGuard latest_schema_guard(schema_service, tenant_id_);
      uint64_t table_id_get_from_guard = OB_INVALID_ID;
      ObTableType table_type = MAX_TABLE_TYPE;
      int64_t schema_version = OB_INVALID_VERSION;

      table_lock_arg.lock_mode_ = transaction::tablelock::ROW_EXCLUSIVE;
      table_lock_arg.timeout_us_ = timeout_us;
      table_lock_arg.table_id_ = table_id;
      table_lock_arg.op_type_ = transaction::tablelock::IN_TRANS_COMMON_LOCK;
      table_lock_arg.owner_id_.convert_from_client_sessid(conn->get_session().get_sessid_for_table(), conn->get_session().get_client_create_time());
      if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_table(tenant_id_, table_lock_arg, conn))) {
        LOG_WARN("lock table failed", KR(ret), K(table_lock_arg));
      } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id_, table_id, transaction::tablelock::EXCLUSIVE, timeout_us, trans_))) {
        LOG_WARN("lock online ddl table failed", KR(ret), K(table_lock_arg));
      } else if (OB_FAIL(latest_schema_guard.get_table_id(table_schema->get_database_id(), table_schema->get_session_id(), table_schema->get_table_name(),
                               table_id_get_from_guard, table_type, schema_version))) {
        // When ObLatestSchemaGuard::get_table_schema is invoked on a non-RS side（e.g. executer),
        // requesting a non-existent table may return error `-4002`.
        // To address this, check whether the target table exists before calling ObLatestSchemaGuard::get_table_schema.
        LOG_WARN("fail to get table id", KR(ret), K(table_schema->get_database_id()), K(table_schema->get_table_name()));
      } else if (OB_INVALID_ID == table_id_get_from_guard) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", KR(ret), K(table_schema->get_database_id()), K(table_schema->get_table_name()));
      } else {
        const share::schema::ObTableSchema *latest_table_schema = nullptr;
        const share::schema::ObTablegroupSchema *tablegroup_schema = nullptr;
        if (OB_FAIL(latest_schema_guard.get_table_schema(table_ids_.at(0), latest_table_schema))) {
          LOG_WARN("failed to get table schema", KR(ret), K(table_ids_.at(0)));
        } else if (OB_ISNULL(latest_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("latest table schema is null", KR(ret), K(table_ids_.at(0)));
        } else if (OB_INVALID_ID != latest_table_schema->get_tablegroup_id()) {
          if (OB_FAIL(latest_schema_guard.get_tablegroup_schema(latest_table_schema->get_tablegroup_id(), tablegroup_schema))) {
            LOG_WARN("failed to get tablegroup schema", KR(ret), K(latest_table_schema->get_tablegroup_id()));
          } else if (OB_ISNULL(tablegroup_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablegroup schema is null", KR(ret), K(latest_table_schema->get_tablegroup_id()), KPC(latest_table_schema));
          }
        }
        if (FAILEDx(generate_tablet_create_arg(*schema_service, latest_schema_guard, tenant_data_version, latest_table_schema, tablegroup_schema))) {
          LOG_WARN("failed to generate tablet create arg", KR(ret));
        } else if (OB_FAIL(tablet_creator_.execute())) {
          LOG_WARN("failed to execute tablet creator", KR(ret));
        } else {
          common::ObSEArray<share::ObTabletToLSInfo, 1> tablet_infos;
          common::ObSEArray<storage::ObSessionTabletInfo, 1> session_tablet_infos;
          common::ObSEArray<share::ObTabletTablePair, 1> tablet_table_pairs;
          for (int64_t i = 0; i < table_ids_.count(); i++) {
            const uint64_t table_id = table_ids_.at(i);
            const common::ObTabletID &tablet_id = tablet_ids_.at(i);
            share::ObTabletToLSInfo tablet_info(tablet_id, ls_id_, table_id, 0/*transfer_seq*/);
            storage::ObSessionTabletInfo session_tablet_info(tablet_id, ls_id_, table_id, sequence_, session_id_, 0/*transfer_seq*/);
            if (OB_FAIL(tablet_infos.push_back(tablet_info))) {
              LOG_WARN("failed to push back tablet info", KR(ret));
            } else if (OB_FAIL(session_tablet_infos.push_back(session_tablet_info))) {
              LOG_WARN("failed to push back session tablet info", KR(ret));
            } else if (OB_FAIL(tablet_table_pairs.push_back(share::ObTabletTablePair(tablet_id, table_id)))) {
              LOG_WARN("failed to push back tablet table pair", KR(ret));
            }
          }
          if (FAILEDx(share::ObTabletToLSTableOperator::batch_update(trans_, tenant_id_, tablet_infos))) {
            LOG_WARN("failed to batch update tablet info", KR(ret));
          } else if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::batch_insert(trans_, tenant_id_, session_tablet_infos))) {
            LOG_WARN("failed to batch insert tablet info", KR(ret));
          } else if (OB_FAIL(share::ObTabletToTableHistoryOperator::create_tablet_to_table_history(trans_, tenant_id_, latest_table_schema->get_schema_version(), tablet_table_pairs))) {
            LOG_WARN("failed to create tablet to table history", KR(ret));
          } else {
            FLOG_INFO("session tablet created", KR(ret), K(table_ids_), K(ls_id_), K(tablet_ids_), K(session_tablet_infos), K(tablet_table_pairs));
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(latest_table_schema)) { // serialize inc schemas for cdc
          if (OB_FAIL(serialize_inc_schema(tenant_id_, trans_, *latest_table_schema))) {
            LOG_WARN("fail to serialize inc schemas", KR(ret), K(tenant_id_), KPC(latest_table_schema));
          }
        }
      }
    }
    if (trans_.is_started()) {
      int tmp_ret = OB_SUCCESS;
      bool is_commit = (OB_SUCCESS == ret);
      if (OB_TMP_FAIL(trans_.end(is_commit))) {
        LOG_WARN("failed to end transaction", KR(ret), KR(tmp_ret));
        ret = is_commit ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObSessionTabletCreateHelper::is_ls_leader(const share::ObLSID &ls_id, bool &is_leader)
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSHandle handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObLSService should not be null", KR(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::OBSERVER_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("ls not exist, skip", KR(ret), K(ls_id));
    } else {
      LOG_WARN("get ls failed", KR(ret));
    }
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", KR(ret));
  } else if (OB_FAIL(ObDDLUtil::is_ls_leader(*ls, is_leader))) {
    LOG_WARN("is ls leader failed", KR(ret), K(ls_id));
  }
  return ret;
}

int ObSessionTabletCreateHelper::fetch_tablet_id(
    const int64_t tablet_cnt,
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  uint64_t min_tablet_id = OB_INVALID_ID;
  share::ObIDGenerator tablet_id_generator;
  if (OB_FAIL(schema_service.get_schema_service()->fetch_new_tablet_ids(tenant_id_,
                                                                        true/*gen_normal_tablet*/,
                                                                        tablet_cnt/*size*/,
                                                                        min_tablet_id))) {
    LOG_WARN("failed to fetch new tablet id", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == min_tablet_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id is invalid", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(tablet_id_generator.init(1/*step*/, min_tablet_id, min_tablet_id + tablet_cnt - 1))) {
    LOG_WARN("failed to init tablet id generator", KR(ret), K(min_tablet_id), K(min_tablet_id + tablet_cnt - 1));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; i++) {
      uint64_t tablet_id = OB_INVALID_ID;
      if (OB_FAIL(tablet_id_generator.next(tablet_id))) {
        LOG_WARN("failed to next tablet id", KR(ret), K(i));
      } else if (OB_FAIL(tablet_ids.push_back(common::ObTabletID(tablet_id)))) {
        LOG_WARN("failed to push back tablet id", KR(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObSessionTabletCreateHelper::choose_log_stream(
    share::schema::ObMultiVersionSchemaService &schema_service,
    share::schema::ObLatestSchemaGuard &lastest_schema_guard,
    const share::schema::ObTableSchema &table_schema,
    const share::schema::ObTablegroupSchema *tablegroup_schema,
    share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret));
  } else {
    ObSessionTabletInfo data_table_info;
    common::ObSEArray<share::ObLSID, 1> ls_id_array;
    share::schema::ObSchemaGuardWrapper schema_guard_wrapper(tenant_id_, GCTX.schema_service_);
    rootserver::ObNewTableTabletAllocator new_table_tablet_allocator(tenant_id_, schema_guard_wrapper, GCTX.sql_proxy_);
    const share::schema::ObTableSchema *schema = &table_schema;
    if (OB_FAIL(schema_guard_wrapper.init(schema_guard))) {
      LOG_WARN("fail to init schema guard wrapper", KR(ret));
    } else if (table_schema.is_oracle_tmp_table_v2_index_table()) {
      const uint64_t data_table_id = table_schema.get_data_table_id();
      const share::schema::ObTableSchema *data_table_schema = nullptr;
      const ObSessionTabletInfoKey data_table_key(data_table_id, sequence_, session_id_);
      if (OB_SUCC(session_tablet_map_.get_session_tablet(data_table_key, data_table_info))) {
      } else if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get data table info", KR(ret), K(data_table_key));
      } else if (OB_FAIL(lastest_schema_guard.get_table_schema(data_table_id, data_table_schema))) {
        LOG_WARN("failed to get data table schema", KR(ret), K(data_table_id));
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("data table schema is null", KR(ret), K(data_table_id));
      } else {
        schema = data_table_schema;
        const bool has_lob_column = data_table_schema->has_lob_column(true/*ignore unused column*/);
        common::ObSArray<uint64_t> table_ids;
        if (OB_FAIL(table_ids.assign(table_ids_))) {
          LOG_WARN("failed to assign table ids", KR(ret), K(table_ids_));
        } else if (has_lob_column && OB_FAIL(table_ids.push_back(data_table_schema->get_aux_lob_meta_tid()))) {
          LOG_WARN("failed to push back session tablet key", KR(ret), K(data_table_schema->get_aux_lob_meta_tid()));
        } else if (has_lob_column && OB_FAIL(table_ids.push_back(data_table_schema->get_aux_lob_piece_tid()))) {
          LOG_WARN("failed to push back session tablet key", KR(ret), K(data_table_schema->get_aux_lob_piece_tid()));
        } else if (FALSE_IT(table_ids_.reset())) {
        } else if (OB_FAIL(table_ids_.push_back(data_table_id))) {
          LOG_WARN("failed to push back data table id", KR(ret), K(data_table_id));
        } else {
          ARRAY_FOREACH(table_ids, idx) {
            if (!is_contain(table_ids_, table_ids.at(idx)) && OB_FAIL(table_ids_.push_back(table_ids.at(idx)))) {
              LOG_WARN("failed to push back table id", KR(ret), K(table_ids.at(idx)));
            }
          }
        }
      }
    }
    if (FAILEDx(new_table_tablet_allocator.init())) {
      LOG_WARN("failed to init tablet allocator", KR(ret));
    } else if (OB_FAIL(new_table_tablet_allocator.prepare_for_oracle_temp_table(trans_,
                                                                                *schema,
                                                                                data_table_info))) {
      LOG_WARN("failed to prepare tablet allocator", KR(ret), K(data_table_info), K(table_schema));
    } else if (OB_FAIL(new_table_tablet_allocator.get_ls_id_array(ls_id_array))) {
      LOG_WARN("failed to get ls id array", KR(ret));
    } else if (OB_UNLIKELY(ls_id_array.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls id array count not match", KR(ret), K(ls_id_array), K(table_schema));
    } else {
      ls_id = ls_id_array.at(0);
    }
  }
  return ret;
}

int ObSessionTabletCreateHelper::generate_tablet_create_arg(
    share::schema::ObMultiVersionSchemaService &schema_service,
    share::schema::ObLatestSchemaGuard &schema_guard,
    const uint64_t tenant_data_version,
    const share::schema::ObTableSchema *table_schema,
    const share::schema::ObTablegroupSchema *tablegroup_schema)
{
  int ret = OB_SUCCESS;
  common::ObArray<const share::schema::ObTableSchema *> table_schemas;
  common::ObArray<int64_t> create_commit_versions;
  common::ObArray<bool> need_create_empty_majors;
  rootserver::ObTabletCreatorArg create_tablet_arg;
  if (OB_FAIL(choose_log_stream(schema_service, schema_guard, *table_schema, tablegroup_schema, ls_id_))) {
    LOG_WARN("failed to choose log stream", KR(ret), KPC(table_schema), KPC(tablegroup_schema));
  } else if (OB_FAIL(fetch_tablet_id(table_ids_.count(), schema_service, tablet_ids_))) {
    LOG_WARN("failed to fetch tablet id", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids_.count(); i++) {
      const share::schema::ObTableSchema *schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(table_ids_.at(i), schema))) {
        LOG_WARN("failed to get table schema", KR(ret), K(table_ids_.at(i)));
      } else if (OB_ISNULL(schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table schema is null", KR(ret), K(table_ids_.at(i)));
      } else if (OB_FAIL(table_schemas.push_back(schema))) {
        LOG_WARN("failed to push back table schema", KR(ret));
      } else if (OB_FAIL(need_create_empty_majors.push_back(true))) {
        LOG_WARN("failed to push back need create empty majors", KR(ret));
      }
    }
  }
  if (FAILEDx(create_tablet_arg.init(tablet_ids_,
                                     ls_id_,
                                     tablet_ids_.at(0),
                                     table_schemas,
                                     lib::Worker::CompatMode::ORACLE,
                                     false/*is_create_bind_hidden_tablets*/,
                                     tenant_data_version,
                                     need_create_empty_majors,
                                     create_commit_versions,
                                     false/*has_cs_replica*/,
                                     *table_schemas.at(0)))) {
    LOG_WARN("failed to init tablet creator arg", KR(ret));
  } else if (OB_FAIL(tablet_creator_.add_create_tablet_arg(create_tablet_arg))) {
    LOG_WARN("failed to add create tablet arg", KR(ret));
  }
  return ret;
}

int ObSessionTabletDeleteHelper::delete_session_tablets_by_table_id(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSessionTabletInfo, 4> session_tablet_infos;
  ObSEArray<ObSessionTabletInfo *, 4> session_tablet_info_ptrs;
  if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::get_by_table_id(sql_proxy,
                                                                       tenant_id,
                                                                       table_id,
                                                                       session_tablet_infos))) {
    LOG_WARN("failed to get session tablet infos", K(ret), K(tenant_id), K(table_id));
  } else if (!session_tablet_infos.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < session_tablet_infos.count(); i++) {
      ObSessionTabletInfo &info = session_tablet_infos.at(i);
      info.is_creator_ = true;
      if (OB_FAIL(session_tablet_info_ptrs.push_back(&info))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      common::ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(&sql_proxy, tenant_id))) {
        LOG_WARN("failed to start transaction", K(ret));
      } else {
        ObSessionTabletDeleteHelper delete_helper(tenant_id, session_tablet_info_ptrs, trans);
        if (OB_FAIL(delete_helper.do_work())) {
          LOG_WARN("failed to delete session tablets", K(ret));
        }
        const bool is_commit = (OB_SUCCESS == ret);
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(is_commit))) {
          LOG_WARN("failed to end transaction", K(tmp_ret), K(ret), K(is_commit));
          ret = OB_SUCCESS != ret ? ret : tmp_ret;
        }
      }
    }
  }
  return ret;
}

// if the table is a related table of a data table, we need to lock the data table
// if the table was dropped and gc tasks call this function, we can delete the tablet directly
// All tablet_info.is_creator_ should be true. Also, the LS must be the same for all,
// and should be fetched from the internal table since it may change after migration.
int ObSessionTabletDeleteHelper::do_work()
{
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  int64_t new_schema_version = OB_INVALID_VERSION;

  if (OB_ISNULL(schema_service) || OB_ISNULL(schema_service->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->gen_new_schema_version(tenant_id_, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id_));
  } else {
    common::ObSEArray<const share::schema::ObTableSchema *, 1> table_schemas_for_delete;
    common::ObSEArray<common::ObTabletID, 1> tablet_ids_for_delete;
    common::ObSEArray<ObSessionTabletInfo *, 1> schema_missing_tablet_infos;
    share::schema::ObSchemaGetterGuard schema_guard;
    const bool is_atomic_batch = true;
    if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(check_and_lock_tables(is_atomic_batch,
                                             schema_guard,
                                             tablet_ids_for_delete,
                                             table_schemas_for_delete,
                                             schema_missing_tablet_infos))) {
      LOG_WARN("fail to check and lock tables", K(ret), K(is_atomic_batch));
    } else if (OB_UNLIKELY(!schema_missing_tablet_infos.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema missing tablet infos should be empty", K(ret), K(schema_missing_tablet_infos));
    }

    if (OB_SUCC(ret) && !tablet_ids_for_delete.empty()) {
      if (OB_FAIL(delete_tablets(tablet_ids_for_delete, new_schema_version))) {
        LOG_WARN("failed to delete tablets", KR(ret));
      } else {
        LOG_INFO("succeed to remove tablet", KR(ret), K(tablet_infos_), K(lbt()));
      }

      if (trans_->is_started()) {
        ARRAY_FOREACH(table_schemas_for_delete, idx) {
          const share::schema::ObTableSchema *table_schema = table_schemas_for_delete.at(idx);
          if (OB_NOT_NULL(table_schema)) { // serialize inc schemas for cdc
            if (false == table_schema->is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid table schema", KR(ret), KPC(table_schema));
            } else if (OB_FAIL(serialize_inc_schema(tenant_id_, *trans_, *table_schema))) {
              LOG_WARN("fail to serialize inc schemas", KR(ret), K(tenant_id_), KPC(table_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSessionTabletDeleteHelper::do_work_for_gc(ObSessionTabletGCTaskSummary &summary)
{
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  int64_t new_schema_version = OB_INVALID_VERSION;
  summary.failed_cnt_ += tablet_infos_.count();

  if (OB_ISNULL(schema_service) || OB_ISNULL(schema_service->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->gen_new_schema_version(tenant_id_, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id_));
  } else {
    common::ObSEArray<const share::schema::ObTableSchema *, 4> table_schemas_for_delete;
    common::ObSEArray<common::ObTabletID, 4> tablet_ids_for_delete;
    common::ObSEArray<ObSessionTabletInfo *, 4> schema_missing_tablet_infos;
    const bool is_atomic_batch = false;
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id_,
                                                        schema_guard,
                                                        schema_version_))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(check_and_lock_tables(is_atomic_batch,
                                             schema_guard,
                                             tablet_ids_for_delete,
                                             table_schemas_for_delete,
                                             schema_missing_tablet_infos))) {
      LOG_WARN("fail to check and lock tables", K(ret), K(is_atomic_batch));
    } else if (tablet_ids_for_delete.empty()) {
      LOG_INFO("tablet ids for delete is empty, nothing to do", K(ret), K(tablet_ids_for_delete));
    } else if (OB_FAIL(delete_tablets(tablet_ids_for_delete, new_schema_version))) {
      LOG_WARN("fail to delete tablets", K(ret));
    } else {
      summary.failed_cnt_ -= tablet_ids_for_delete.count();
      LOG_INFO("succeed to remove tablet", K(ret), K(tablet_ids_for_delete), K(lbt()));
    }

    if (trans_->is_started()) {
      ARRAY_FOREACH(table_schemas_for_delete, idx) {
        const share::schema::ObTableSchema *table_schema = table_schemas_for_delete.at(idx);
        if (OB_NOT_NULL(table_schema)) { // serialize inc schemas for cdc
          if (false == table_schema->is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid table schema", KR(ret), KPC(table_schema));
          } else if (OB_FAIL(serialize_inc_schema(tenant_id_, *trans_, *table_schema))) {
            LOG_WARN("fail to serialize inc schemas", KR(ret), K(tenant_id_), KPC(table_schema));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (schema_missing_tablet_infos.empty()) {
      // do nothing
    } else if (FALSE_IT(summary.schema_missing_tablets_cnt_ += schema_missing_tablet_infos.count())) {
    } else if (OB_FAIL(delete_schema_missing_tablets(schema_missing_tablet_infos, new_schema_version))) {
      LOG_WARN("fail to delete schema missing tablets", K(ret));
    }
  }
  return ret;
}

int ObSessionTabletDeleteHelper::lock_table_for_delete(
  const ObTableSchema &table_schema,
  const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = NULL;
  if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans_->get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is null", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty tablet ids", K(ret), K(tablet_ids));
  } else {
    //
    // 1. Acquire a ROW EXCLUSIVE table lock on table (Table ID).
    // 2. Acquire a ROW EXCLUSIVE Online DDL lock on table (Table ID).
    // 3. Acquire an EXCLUSIVE Online DDL lock on tablet (Tablet ID).
    // 4. Acquire an EXCLUSIVE table lock on tablet (Tablet ID).
    const uint64_t table_id = table_schema.is_oracle_tmp_table_v2_index_table() ? table_schema.get_data_table_id() : table_schema.get_table_id();
    transaction::tablelock::ObLockTableRequest table_lock_arg;
    table_lock_arg.lock_mode_ = transaction::tablelock::ROW_EXCLUSIVE;
    table_lock_arg.timeout_us_ = timeout_us_; // try lock, if not success, will be deleted by GC tasks later
    table_lock_arg.table_id_ = table_id;
    table_lock_arg.op_type_ = transaction::tablelock::IN_TRANS_COMMON_LOCK;
    table_lock_arg.owner_id_.convert_from_client_sessid(conn->get_session().get_sessid_for_table(), conn->get_session().get_client_create_time());
    if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_table(tenant_id_, table_lock_arg, conn))) {
      LOG_WARN("lock table failed", KR(ret), K(table_lock_arg));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id_, table_id, transaction::tablelock::ROW_EXCLUSIVE, timeout_us_, *trans_))) {
      LOG_WARN("lock online ddl table failed", KR(ret), K(table_lock_arg));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets_in_trans(tenant_id_, tablet_ids, transaction::tablelock::EXCLUSIVE, timeout_us_ /*try lock*/, *trans_))) {
      LOG_WARN("lock online ddl tablets failed", KR(ret), K(tablet_ids));
    } else if (OB_FAIL(ObInnerConnectionLockUtil::lock_tablet(tenant_id_, table_id, tablet_ids, transaction::tablelock::EXCLUSIVE, timeout_us_ /*try lock*/, conn))) {
      LOG_WARN("lock tablets failed", KR(ret), K(tablet_ids));
    }
  }
  return ret;
}

int ObSessionTabletDeleteHelper::check_and_lock_tables(
    const bool is_atomic_batch,
    share::schema::ObSchemaGetterGuard &schema_guard,
    /*out*/common::ObIArray<ObTabletID> &tablet_ids_for_delete,
    /*out*/common::ObIArray<const ObTableSchema *> &table_schemas_for_delete,
    /*out*/common::ObIArray<ObSessionTabletInfo *> &schema_missing_tablet_infos)
{
  int ret = OB_SUCCESS;
  tablet_ids_for_delete.reset();
  table_schemas_for_delete.reset();
  schema_missing_tablet_infos.reset();
  const int64_t bucket_cnt = 17;
  hash::ObHashSet<uint64_t> failed_data_tb_id_set;
  common::ObSEArray<common::ObTabletID, 1> tablet_ids;

  if (OB_FAIL(failed_data_tb_id_set.create(bucket_cnt))) {
    LOG_WARN("fail to create failed data table id set", K(ret), K(bucket_cnt));
  }

  ARRAY_FOREACH(tablet_infos_, idx) {
    tablet_ids.reset();
    const share::schema::ObTableSchema *table_schema = nullptr;
    ObSessionTabletInfo &tablet_info = *tablet_infos_.at(idx);
    if (!tablet_info.is_creator_) {
      ret = OB_SUCCESS;
      LOG_INFO("not creator session, skip delete from oracle temporary table", KR(ret), K(tablet_info));
    } else {
      if (OB_FAIL(ret) || OB_ISNULL(trans_) || !trans_->is_started()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid transaction", KR(ret), K(tenant_id_), K(OB_ISNULL(trans_)));
      } else if (OB_FAIL(tablet_ids.push_back(tablet_info.tablet_id_))) {
        LOG_WARN("failed to push back tablet id", KR(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, tablet_info.table_id_, table_schema))) {
        LOG_WARN("failed to get table schema", KR(ret), K(tablet_info.table_id_));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table schema is null", KR(ret), K(tablet_info.table_id_));
      } else if (OB_UNLIKELY(PARTITION_LEVEL_ZERO != table_schema->get_part_level()
                          || !(table_schema->is_oracle_tmp_table_v2() || table_schema->is_oracle_tmp_table_v2_index_table()))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("partition level is zero or table is not oracle tmp table v2", KR(ret), KPC(table_schema));
      } else if (OB_FAIL(lock_table_for_delete(*table_schema, tablet_ids))) {
        LOG_WARN("fail to lock table for delete", K(ret), KPC(table_schema), K(tablet_ids));
      }

#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        const int64_t err_tablet_id = GCONF.errsim_test_tablet_id.get_value();
        if (err_tablet_id > 0 && tablet_info.tablet_id_.id() == err_tablet_id) {
          if (OB_FAIL(EN_SESSION_TABLET_GC_FAILED ? : OB_SUCCESS)) {
            LOG_WARN("fake gc session tablet error", K(ret), K(tablet_info));
          }
        }
      }
#endif

      if (OB_FAIL(ret)) {
        if (is_atomic_batch) {
          // do nothing
        } else if (OB_ISNULL(table_schema)) {
          if (OB_TABLE_NOT_EXIST == ret) {
            /// only when @c get_table_schema returns OB_SUCCESS and table_schema is nullptr
            ret = OB_SUCCESS;
            if (OB_FAIL(schema_missing_tablet_infos.push_back(&tablet_info))) {
              LOG_WARN("fail to push back schema missing tablet info", K(ret), K(schema_missing_tablet_infos.count()));
            }
          }
        } else {
          // !is_atomic_batch && nullptr != table_schema(lock table failed): reset errcode and record primary table id
          ret = OB_SUCCESS;
          const uint64_t table_id = table_schema->is_oracle_tmp_table_v2_index_table() ? table_schema->get_data_table_id() : table_schema->get_table_id();
          if (OB_FAIL(failed_data_tb_id_set.set_refactored(table_id, /*overwrite*/1))) {
            LOG_WARN("fail to set refactored", K(ret), K(table_id));
          }
        }
      } else if (OB_FAIL(tablet_ids_for_delete.push_back(tablet_info.tablet_id_))) {
        LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids_for_delete.count()));
      } else if (OB_FAIL(table_schemas_for_delete.push_back(table_schema))) {
        LOG_WARN("fail to push back table schema", K(ret), K(table_schemas_for_delete.count()));
      }
    }
  }

  if (OB_FAIL(ret) || is_atomic_batch) {
    // do nothing
  } else if (failed_data_tb_id_set.empty()) {
    // do nothing
  } else if (OB_FAIL(remove_failed_tables(failed_data_tb_id_set, tablet_ids_for_delete, table_schemas_for_delete))) {
    LOG_WARN("failed to remove failed tables", K(ret));
  }
  return ret;
}

int ObSessionTabletDeleteHelper::remove_failed_tables(
    const hash::ObHashSet<uint64_t> &failed_data_tb_id_set,
    /*out*/common::ObIArray<ObTabletID> &tablet_ids_for_delete,
    /*out*/common::ObIArray<const ObTableSchema *> &table_schemas_for_delete)
{
  int ret = OB_SUCCESS;
  const int64_t total_cnt = tablet_ids_for_delete.count();

  if (OB_UNLIKELY(table_schemas_for_delete.count() != total_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_ids_for_delete and table_schemas_for_delete should be the same size", K(ret),
      K(total_cnt), K(table_schemas_for_delete.count()));
  } else if (0 == total_cnt) {
    // do nothing
  } else {
    int64_t cur = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
      ObTabletID tablet_id = tablet_ids_for_delete.at(i);
      const ObTableSchema *table_schema = table_schemas_for_delete.at(i);
      if (OB_UNLIKELY(!tablet_id.is_valid() || nullptr == table_schema)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet id or null table schema", K(ret), K(tablet_id),
          KPC(table_schema));
      } else {
        const uint64_t table_id = table_schema->is_oracle_tmp_table_v2_index_table() ? table_schema->get_data_table_id() : table_schema->get_table_id();
        if (OB_FAIL(failed_data_tb_id_set.exist_refactored(table_id))) {
          if (OB_HASH_EXIST == ret) {
            // skip all tablets of this table
            ret = OB_SUCCESS;
          } else if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            tablet_ids_for_delete.at(cur) = tablet_id;
            table_schemas_for_delete.at(cur) = table_schema;
            ++cur;
          } else {
            LOG_WARN("fail to do exist refactored", K(ret));
          }
        }
      }
    }
    const int64_t pop_cnt = total_cnt - cur;
    for (int64_t i = 0; OB_SUCC(ret) && i < pop_cnt; ++i) {
      tablet_ids_for_delete.pop_back();
      table_schemas_for_delete.pop_back();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(cur != tablet_ids_for_delete.count()
                           || cur != table_schemas_for_delete.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected result array's count", K(ret), "final_cnt", cur,
        K(tablet_ids_for_delete.count()), K(table_schemas_for_delete.count()));
    }
  }
  return ret;
}

// tablets must be locked before being deleted
int ObSessionTabletDeleteHelper::delete_tablets(const ObIArray<common::ObTabletID> &tablet_ids, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLSID, 1> ls_ids;
  if (OB_UNLIKELY(OB_ISNULL(trans_) || !trans_->is_started() || tablet_ids.count() == 0 || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_ids), K(schema_version), K(OB_ISNULL(trans_)));
  } else if (OB_FAIL(share::ObTabletToLSTableOperator::batch_get_ls(*trans_, tenant_id_, tablet_ids, ls_ids))) {
    LOG_WARN("failed to get ls by tablet", KR(ret), K(tablet_ids));
  } else if (OB_UNLIKELY(ls_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet ids ls ids", KR(ret), K(ls_ids), K(tablet_ids));
  } else {
    share::ObLSID &new_ls_id = ls_ids.at(0);
    ARRAY_FOREACH(ls_ids, idx) {
      if (OB_UNLIKELY(new_ls_id != ls_ids.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet ids ls ids, ls id must be the same", KR(ret), K(ls_ids), K(tablet_ids));
      }
    }
    if (FAILEDx(share::ObTabletToLSTableOperator::batch_remove(*trans_, tenant_id_, tablet_ids))) {
      LOG_WARN("failed to batch remove tablet", KR(ret), K(tablet_ids), K(tablet_infos_));
    } else if (OB_FAIL(mds_remove_tablet(tenant_id_, new_ls_id, tablet_ids, *trans_))) {
      LOG_WARN("failed to mds remove tablet", KR(ret), K(tablet_ids), K(tablet_infos_));
    } else if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::batch_remove(*trans_, tenant_id_, tablet_ids))) {
      LOG_WARN("failed to batch remove session tablet", KR(ret), K(tablet_ids), K(tablet_infos_));
    } else if (OB_FAIL(share::ObTabletToTableHistoryOperator::drop_tablet_to_table_history(*trans_, tenant_id_, schema_version, tablet_ids))) {
      LOG_WARN("failed to drop tablet to table history", KR(ret), K(tablet_ids), K(tablet_infos_));
    }
  }
  return ret;
}

int ObSessionTabletDeleteHelper::delete_schema_missing_tablets(const ObIArray<ObSessionTabletInfo *> &tablet_infos, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  if (OB_FAIL(EN_SESSION_TABLET_GC_FAILED? : OB_SUCCESS)) {
    LOG_WARN("failed to delete schema missing tablets due to fake err", K(ret));
    return ret;
  }
#endif

#ifdef OB_BUILD_PACKAGE
  const int64_t total_cnt = tablet_infos.count();
  LOG_ERROR("Prepare to delete schema missing tablets", K(ret), K(tablet_infos), K(schema_version));
  if (OB_UNLIKELY(0 == total_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty tablet infos", K(ret), K(tablet_infos));
  }
  ObFixedArray<ObTabletID, ObArenaAllocator> tablet_ids(allocator_, total_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
    const ObSessionTabletInfo *tablet_info = tablet_infos.at(i);
    if (OB_ISNULL(tablet_info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null tablet info", K(ret), KPC(tablet_info));
    } else if (OB_FAIL(tablet_ids.push_back(tablet_info->tablet_id_))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_ids.count()));
    }
  }
  // Hold OnlineDDL tablet lock(EXCLUSIVE)
  if (FAILEDx(ObOnlineDDLLock::lock_tablets_in_trans(tenant_id_, tablet_ids, transaction::tablelock::EXCLUSIVE, timeout_us_ /*try lock*/, *trans_))) {
    LOG_WARN("lock online ddl tablets failed", KR(ret), K(tablet_ids));
  } else if (OB_FAIL(delete_tablets(tablet_ids, schema_version))) {
    LOG_WARN("fail to delete tablets", K(ret));
  } else {
    FLOG_INFO("Finish to delete schema missing tablets", K(ret), K(tablet_infos), K(schema_version));
  }
#else
  ret = OB_ERR_UNEXPECTED;
  LOG_ERROR("The schema for tablets are missing, manual deletion required", K(ret), K(tablet_infos), K(schema_version));
#endif
  return ret;
}

int ObSessionTabletDeleteHelper::mds_remove_tablet(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = nullptr;
  obrpc::ObBatchRemoveTabletArg arg;
  if (OB_FAIL(arg.init(tablet_ids, ls_id))) {
    LOG_WARN("failed to init batch remove tablet arg", KR(ret));
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get connection", KR(ret));
  } else {
    ObTimeoutCtx ctx;
    const int64_t buf_len = arg.get_serialize_size();
    char *buf = (char*)allocator_.alloc(buf_len);
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", KR(ret));
    } else if (OB_FAIL(arg.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize batch remove tablet arg", KR(ret));
    } else if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("failed to set default timeout ctx", KR(ret));
    } else {
      const int64_t SLEEP_INTERVAL = 100 * 1000L; // 100ms
      do {
        if (ctx.is_timeouted()) {
          ret = OB_TIMEOUT;
          LOG_WARN("failed to set default timeout ctx", KR(ret));
        } else if (OB_FAIL(conn->register_multi_data_source(tenant_id,
                                                            ls_id,
                                                            transaction::ObTxDataSourceType::DELETE_TABLET_NEW_MDS,
                                                            buf,
                                                            buf_len))) {
          LOG_WARN("failed to register_tx_data", KR(ret), K(arg), K(buf), K(buf_len));
          if (OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_NOT_MASTER == ret) {
            LOG_INFO("fail to find leader, try again", K_(tenant_id), K(arg));
            ob_usleep(SLEEP_INTERVAL);
          }
        }
      } while (OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_NOT_MASTER == ret);
    }
  }

  return ret;
}

int ObSessionTabletGCHelper::get_local_leader_ls_ids(common::ObIArray<share::ObLSID> &local_leader_ls_ids) const
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObLSID> local_ls_id_array;
  storage::ObLSService *ls_service = MTL(storage::ObLSService*);
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", KR(ret));
  } else if (OB_FAIL(ls_service->get_ls_ids(local_ls_id_array))) {
    LOG_WARN("fail to get ls ids from MTL", KR(ret));
  } else {
    ARRAY_FOREACH(local_ls_id_array, idx) {
      const share::ObLSID &ls_id = local_ls_id_array.at(idx);
      if (!ls_id.is_user_ls()) {
        continue;
      }
      bool is_leader = false;
      int tmp_ret = storage::ObSessionTabletCreateHelper::is_ls_leader(ls_id, is_leader);
      if (OB_SUCCESS != tmp_ret) {
        // if fail to check is ls leader, continue to check next ls
        LOG_WARN("fail to check is ls leader", KR(tmp_ret), K(ls_id));
      } else if (is_leader) {
        if (OB_FAIL(local_leader_ls_ids.push_back(ls_id))) {
          LOG_WARN("failed to push back ls id", KR(ret));
        }
      }
    }
  }
  return ret;
}

// First, check if there are any leader log streams (LS) on the current node.
// Only delete temporary table tablets from leader LS; otherwise, return without doing anything.
int ObSessionTabletGCHelper::do_work()
{
  int ret = OB_SUCCESS;
  int64_t failed_count = 0;
  ObSessionTabletGCTaskSummary gc_summary;
  const int64_t start_time = ObTimeUtility::current_time();
  common::ObSEArray<storage::ObSessionTabletInfo, TABLET_GROUP_SIZE * NUM_OF_TABLET_GROUP> session_tablet_infos;
  common::ObSEArray<storage::ObSessionTabletInfo *, TABLET_GROUP_SIZE * NUM_OF_TABLET_GROUP> session_tablet_infos_for_delete;
  common::ObSEArray<common::ObSEArray<storage::ObSessionTabletInfo *, TABLET_GROUP_SIZE>, NUM_OF_TABLET_GROUP> session_tablet_infos_for_delete_grouped;
  common::ObSEArray<share::ObLSID, 4> ls_ids;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(get_local_leader_ls_ids(ls_ids))) {
    LOG_WARN("failed to get local leader ls ids", KR(ret));
  } else if (ls_ids.empty()) {
    ret = OB_SUCCESS;
    LOG_INFO("local leader ls ids is empty", KR(ret));
  } else if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::batch_get_by_ls_ids(*GCTX.sql_proxy_, tenant_id_, ls_ids, session_tablet_infos))) {
    LOG_WARN("failed to batch get session tablet by ls ids", KR(ret));
  } else if (OB_UNLIKELY(session_tablet_infos.count() == 0)) {
    ret = OB_SUCCESS;
    LOG_INFO("session tablet infos is empty", KR(ret));
  } else {
    common::ObArray<uint64_t> session_id_array;
    common::ObArray<bool> session_alive_array;

    if (OB_FAIL(session_id_array.reserve(session_tablet_infos.count()))) {
      LOG_WARN("failed to reserve session id array", KR(ret));
    } else if (OB_FAIL(session_alive_array.reserve(session_tablet_infos.count()))) {
      LOG_WARN("failed to reserve session alive array", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < session_tablet_infos.count(); i++) {
        if (OB_FAIL(session_id_array.push_back(session_tablet_infos.at(i).get_session_id()))) {
          LOG_WARN("failed to push back session id", KR(ret));
        } else if (OB_FAIL(session_alive_array.push_back(false))) {
          LOG_WARN("failed to push back session alive", KR(ret));
        }
      }
      // TODO: the local observer needs not rpc to check session alive
      if (FAILEDx(transaction::tablelock::ObTableLockDetectFuncList::batch_detect_session_alive_at_least_one(tenant_id_,
          session_id_array, nullptr, session_alive_array))) {
        LOG_WARN("failed to batch detect session alive", KR(ret));
      } else if (OB_FAIL(session_tablet_infos_for_delete.reserve(session_tablet_infos.count()))) {
        LOG_WARN("failed to reserve session tablet infos for delete", KR(ret));
      } else {
        ARRAY_FOREACH(session_tablet_infos, idx) {
          if (false == session_alive_array.at(idx)) {
            if (OB_FAIL(session_tablet_infos_for_delete.push_back(&session_tablet_infos.at(idx)))) {
              LOG_WARN("failed to push back session tablet info for delete", KR(ret));
            } else {
              session_tablet_infos.at(idx).is_creator_ = true;
            }
          }
        }
        LOG_INFO("tablet infos for delete", KR(ret), K(ls_ids), K(session_tablet_infos_for_delete.count()));
        int64_t latest_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(ret)) {
        } else if (session_tablet_infos_for_delete.empty()) {
          // do nothing
        } else if (OB_FAIL(group_by_session_and_seq(session_tablet_infos_for_delete, session_tablet_infos_for_delete_grouped))) {
          LOG_WARN("failed to group by session and sequence", KR(ret));
        }
        /// NOTE: Obtain the latest schema guard for GC.
        /// The local schema version may lag behind the cluster's.
        /// (for example after LS migration or RS leader switch over).
        /// In that case, a lookup against the locally cached schema may fail to
        /// resolve a table schema even though the table still exists. Session tablet
        /// GC must not treat a NULL table schema as proof that the table was
        /// dropped; therefore refresh to the latest schema before GC.
        else if (OB_FAIL(refresh_tenant_schema_if_need(latest_schema_version))) {
          LOG_WARN("failed to refresh tenant schema", K(ret));
        } else {
          ObSEArray<storage::ObSessionTabletInfo *, BATCH_DELETE_SESSION_TABLET_COUNT> session_tablet_infos_for_delete_batch;
          // Remove the session tablet which is not alive
          // A failure will not affect subsequent delete operations
          for (int64_t group_idx = 0; gc_summary.total_cnt_ < MAX_GC_COUNT && group_idx < session_tablet_infos_for_delete_grouped.count(); ++group_idx) {
            ret = OB_SUCCESS;
            // One group share the same session_id and sequence, must be deleted together
            ObSEArray<storage::ObSessionTabletInfo *, TABLET_GROUP_SIZE> &group = session_tablet_infos_for_delete_grouped.at(group_idx);
            // Process batch when it reaches BATCH_DELETE_SESSION_TABLET_COUNT or this is the last group
            bool is_last_group = (group_idx == session_tablet_infos_for_delete_grouped.count() - 1);
            if (OB_FAIL(append(session_tablet_infos_for_delete_batch, group))) {
              LOG_WARN("failed to append session tablet infos to batch", KR(ret));
            } else if (session_tablet_infos_for_delete_batch.count() > 0 &&
                (session_tablet_infos_for_delete_batch.count() >= BATCH_DELETE_SESSION_TABLET_COUNT || is_last_group)) {
              common::ObMySQLTransaction trans;
              if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
                LOG_WARN("failed to start transaction", KR(ret));
              } else {
                if (OB_SUCC(ret)) {
                  storage::ObSessionTabletDeleteHelper tablet_delete_helper(tenant_id_, session_tablet_infos_for_delete_batch, trans, latest_schema_version);
                  tablet_delete_helper.set_timeout_us(0/* no timeout */);
                  if (OB_FAIL(tablet_delete_helper.do_work_for_gc(gc_summary))) {
                    LOG_WARN("failed to delete session tablets in batch", KR(ret), K(session_tablet_infos_for_delete_batch));
                  } else {
                    LOG_INFO("GC succeed to remove session tablets in batch", K(tenant_id_), K(session_tablet_infos_for_delete_batch));
                  }
                  gc_summary.total_cnt_ += session_tablet_infos_for_delete_batch.count();
                }
              }
              if (trans.is_started()) {
                int tmp_ret = OB_SUCCESS;
                bool is_commit = (OB_SUCCESS == ret);
                if (OB_TMP_FAIL(trans.end(is_commit))) {
                  LOG_WARN("failed to end transaction", KR(ret), KR(tmp_ret));
                  ret = is_commit ? tmp_ret : ret;
                }
              }
              session_tablet_infos_for_delete_batch.reset();
            }
          }
        }
      }
    }
  }
  const int64_t cost_time = ObTimeUtility::current_time() - start_time;
  LOG_INFO("GC task finished", KR(ret), K(gc_summary), K(cost_time), K(ls_ids.count()));
  return ret;
}

int ObSessionTabletGCHelper::is_table_has_active_session(
  const uint64_t tenant_id,
  const uint64_t table_id,
  const int64_t schema_version,
  bool &has_active_session)
{
  int ret = OB_SUCCESS;
  has_active_session = false;
  ObSEArray<common::ObTableID, 4> table_ids;
  ObSchemaService *schema_svr = nullptr;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
    } else if (OB_ISNULL(GCTX.schema_service_)
               || OB_ISNULL(schema_svr = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", KR(ret));
  } else if (OB_FAIL(fetch_all_relative_table_ids(tenant_id,
                                                  table_id,
                                                  schema_version,
                                                  *GCTX.sql_proxy_,
                                                  *schema_svr,
                                                  /*out*/table_ids))) {
    LOG_WARN("failed to fetch all relative table ids", K(ret), K(tenant_id), K(table_id),
      K(schema_version));
  } else if (OB_FAIL(check_if_any_table_has_active_session(tenant_id,
                                                           *GCTX.sql_proxy_,
                                                           table_ids,
                                                           /*out*/has_active_session))) {
    LOG_WARN("failed to check if any table has active session", K(ret));
  }
  return ret;
}

int ObSessionTabletGCHelper::is_table_has_active_session(
  const share::schema::ObSimpleTableSchemaV2 *table_schema,
  const obrpc::ObAlterTableArg *alter_table_arg)
{
  int ret = OB_SUCCESS;
  bool has_active_session = false;
  uint64_t table_id = 0;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is null", KR(ret));
  } else if (!need_check_table(table_schema, alter_table_arg)) {
    // do nothing
  } else if (FALSE_IT(table_id = table_schema->is_oracle_tmp_table_v2_index_table() ? table_schema->get_data_table_id() : table_schema->get_table_id())) {
  } else if (OB_FAIL(is_table_has_active_session(table_schema->get_tenant_id()/* might be 0 */, table_id, table_schema->get_schema_version(), has_active_session))) {
    LOG_WARN("failed to check if table has active session", KR(ret), K(table_id), KPC(table_schema));
  } else if (has_active_session) {
    ret = OB_ERR_TEMP_TABLE_BUSY;
    LOG_WARN("table has active session", KR(ret), K(table_id), KPC(table_schema));
    LOG_USER_ERROR(OB_ERR_TEMP_TABLE_BUSY);
  } else {
    LOG_INFO("table has no active temporary tablet", KR(ret), K(table_id), KPC(table_schema));
  }
  return ret;
}

int ObSessionTabletGCHelper::is_any_table_has_active_session(
  const common::ObIArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas)
{
  int ret = OB_SUCCESS;
  const int64_t total_cnt = table_schemas.count();
  uint64_t tenant_id = OB_INVALID_TENANT_ID ;
  ObSchemaService *schema_svr = nullptr;
  bool has_active_session = false;
  common::ObSEArray<common::ObTableID, 16> table_ids;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)
             || OB_ISNULL(schema_svr = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
    const share::schema::ObSimpleTableSchemaV2 *schema = table_schemas.at(i);
    if (OB_ISNULL(schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table schema is null", K(ret));
    } else if (!need_check_table(schema, /*alter_table_arg*/nullptr)) {
      // do nothing
    } else {
      const uint64_t table_id = schema->is_oracle_tmp_table_v2_index_table() ? schema->get_data_table_id() : schema->get_table_id();
      if (OB_INVALID_TENANT_ID == tenant_id) {
        tenant_id = schema->get_tenant_id();
      } else if (schema->get_tenant_id() != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schemas' tenant id are different", K(ret), K(tenant_id), KPC(schema));
      }
      if (FAILEDx(fetch_all_relative_table_ids(tenant_id,
                                               table_id,
                                               schema->get_schema_version(),
                                               *GCTX.sql_proxy_,
                                               *schema_svr,
                                               /*out*/table_ids))) {
        LOG_WARN("failed to fetch all relative table ids", K(ret), K(tenant_id), K(table_id),
          "schema_version", schema->get_schema_version());
      }
    }

  }

  if (OB_FAIL(ret)) {
  } else if (table_ids.empty()) {
    // do nothing
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_if_any_table_has_active_session(tenant_id,
                                                           *GCTX.sql_proxy_,
                                                           table_ids,
                                                           /*out*/has_active_session))) {
    LOG_WARN("failed to check if any table has active session", K(ret));
  } else if (has_active_session) {
    ret = OB_ERR_TEMP_TABLE_BUSY;
    LOG_WARN("table has active session", K(ret), K(table_ids));
    LOG_USER_ERROR(OB_ERR_TEMP_TABLE_BUSY);
  } else {
    LOG_INFO("table has no active temporary tablet", K(ret), K(table_ids));
  }
  return ret;
}

bool ObSessionTabletGCHelper::need_check_table(
  const share::schema::ObSimpleTableSchemaV2 *schema,
  const obrpc::ObAlterTableArg *alter_table_arg)
{
  int ret = OB_SUCCESS;
  bool b_ret = true;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null schema", K(ret));
  } else if (schema->is_hidden_schema()
      || schema->get_session_id() != 0
      || (!schema->is_oracle_tmp_table_v2()
      && !schema->is_oracle_tmp_table_v2_index_table())) {
    b_ret = false;
    LOG_INFO("table is not oracle tmp table v2 or oracle tmp table v2 index table", K(ret), KPC(schema));
  } else {
    uint64_t table_id = schema->is_oracle_tmp_table_v2_index_table() ? schema->get_data_table_id() : schema->get_table_id();
    if (nullptr != alter_table_arg) {
      int64_t num_members = alter_table_arg->alter_table_schema_.alter_option_bitset_.num_members();
      if (num_members == 1) {
        // only rename and comment need not to check if table has active session
        b_ret = !alter_table_arg->alter_table_schema_.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::TABLE_NAME)
                && !alter_table_arg->alter_table_schema_.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::COMMENT);
      }
    }
    if (!b_ret) {
      LOG_INFO("no need to check if table has active session", K(ret), K(table_id));
    }
  }
  return b_ret;
}

int ObSessionTabletGCHelper::fetch_all_relative_table_ids(
  const uint64_t tenant_id,
  const uint64_t primary_table_id,
  const int64_t schema_version,
  common::ObMySQLProxy &sql_proxy,
  ObSchemaService &schema_svr,
  /*out*/common::ObIArray<common::ObTableID> &table_ids)
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  ObArray<ObAuxTableMetaInfo> related_infos;
  schema_status.tenant_id_ = MTL_ID();
  if (OB_FAIL(schema_svr.fetch_aux_tables(
        schema_status,
        tenant_id,
        primary_table_id,
        schema_version,
        sql_proxy,
        related_infos))) {
    LOG_WARN("fail to fetch_aux_tables", KR(ret), K(tenant_id),
        K(primary_table_id), K(schema_status), K(schema_version));
  } else if (OB_FAIL(table_ids.push_back(ObTableID(primary_table_id)))) {
    LOG_WARN("failed to push back table id", KR(ret));
  } else {
    ARRAY_FOREACH(related_infos, idx) {
      if (OB_FAIL(table_ids.push_back(ObTableID(related_infos.at(idx).table_id_)))) {
        LOG_WARN("failed to push back table id", KR(ret));
      }
    }
  }
  return ret;
}

int ObSessionTabletGCHelper::check_if_any_table_has_active_session(
  const uint64_t tenant_id,
  common::ObMySQLProxy &sql_proxy,
  const common::ObIArray<ObTableID> &table_ids,
  /*out*/bool &has_active_session)
{
  int ret = OB_SUCCESS;
  has_active_session = false;
  common::ObSEArray<storage::ObSessionTabletInfo, 4> session_tablet_infos;

  if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::batch_get_by_table_ids(sql_proxy, tenant_id, table_ids, session_tablet_infos))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("gtt has no active session", KR(ret), K(table_ids));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get session tablet infos", KR(ret));
    }
  } else if (OB_UNLIKELY(session_tablet_infos.count() == 0)) {
    ret = OB_SUCCESS;
    LOG_INFO("gtt has no active session", KR(ret), K(table_ids));
  } else {
    uint64_t active_tablet_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < session_tablet_infos.count(); i++) {
      if (observer::ObInnerSQLConnection::INNER_SQL_SESS_ID == session_tablet_infos.at(i).get_session_id()
          || observer::ObInnerSQLConnection::INNER_SQL_PROXY_SESS_ID == session_tablet_infos.at(i).get_session_id()) {
        // skip the inner session
      } else {
        active_tablet_count++;
      }
    }
    if (active_tablet_count > 0) {
      has_active_session = true;
      LOG_WARN("gtt has active session", KR(ret), K(table_ids));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("gtt has no active session", KR(ret), K(table_ids));
    }
  }
  return ret;
}

int ObSessionTabletGCHelper::is_table_has_active_session(const uint64_t tenant_id,
    const ObString &db_name, const ObString &table_name,
    const obrpc::ObAlterTableArg *alter_table_arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObColumnSchemaV2 *col_schema = NULL;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null");
  } else if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("failed to get tenant mode", KR(ret), K(tenant_id));
  } else if (compat_mode != lib::Worker::CompatMode::ORACLE) {
    ret = OB_SUCCESS;
    LOG_INFO("not oracle mode, skip check", KR(ret), K(tenant_id), K(db_name), K(table_name));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, db_name, table_name, false, table_schema))) {
    LOG_WARN("get table schema failed", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(db_name), K(table_name));
  } else if (OB_FAIL(is_table_has_active_session(table_schema, alter_table_arg))) {
    LOG_WARN("failed to check if table has active session", KR(ret), KPC(table_schema));
  }
  return ret;
}

int ObSessionTabletGCHelper::group_by_session_and_seq(
  const ObIArray<storage::ObSessionTabletInfo *> &session_tablet_infos_for_delete,
  ObIArray<ObSEArray<storage::ObSessionTabletInfo *, TABLET_GROUP_SIZE>> &session_tablet_infos_for_delete_grouped/* out */)
{
  int ret = OB_SUCCESS;

  // Temporary data structures for grouping: session_id -> all tablet infos for that session
  ObSEArray<hash::HashMapPair<uint64_t, int64_t>, NUM_OF_TABLET_GROUP> session_keys;  // List of discovered session IDs

  ARRAY_FOREACH(session_tablet_infos_for_delete, i) {
    storage::ObSessionTabletInfo &info = *session_tablet_infos_for_delete.at(i);
    const uint64_t session_id = info.get_session_id();
    const int64_t sequence = info.get_sequence();
    int64_t group_idx = -1;

    // Find if this group already exists
    for (int64_t j = 0; j < session_keys.count(); j++) {
      if (session_keys.at(j).first == session_id && session_keys.at(j).second == sequence) {
        group_idx = j;
        break;
      }
    }

    // If this is a new session_id and sequence, create a new group
    if (group_idx == -1) {
      ObSEArray<storage::ObSessionTabletInfo *, TABLET_GROUP_SIZE> new_group;
      hash::HashMapPair<uint64_t, int64_t> new_key;
      new_key.first = session_id;
      new_key.second = sequence;
      if (OB_FAIL(new_group.push_back(&info))) {
        LOG_WARN("fail to push back info to new group", KR(ret), K(info));
      } else if (OB_FAIL(session_keys.push_back(new_key))) {
        LOG_WARN("fail to push back session_id", KR(ret), K(session_id));
      } else if (OB_FAIL(session_tablet_infos_for_delete_grouped.push_back(new_group))) {
        LOG_WARN("fail to push back new group", KR(ret));
      }
    } else {
      // Add to existing group
      if (OB_FAIL(session_tablet_infos_for_delete_grouped.at(group_idx).push_back(&info))) {
        LOG_WARN("fail to push back info to existing group", KR(ret), K(group_idx), K(info));
      }
    }
  }

  return ret;
}

int dispatch_drop_gtt_v2_session_tablet_on_creator(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &table_ids,
    const int64_t sequence,
    const uint64_t session_id)
{
  int ret = OB_SUCCESS;
  share::ObDropGTTV2SessionTabletArg arg;
  uint64_t tenant_data_version = 0;
  if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table ids must not be empty", KR(ret), K(tenant_id));
  } else if (OB_FAIL(arg.init(tenant_id, table_ids, sequence, session_id))) {
    LOG_WARN("failed to init drop gtt v2 session tablet arg", KR(ret), K(tenant_id), K(table_ids),
             K(sequence), K(session_id));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drop gtt v2 session tablet arg", KR(ret), K(arg));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("failed to get tenant data version", KR(ret), K(tenant_id));
  } else if (!((tenant_data_version >= MOCK_DATA_VERSION_4_4_2_1
                && tenant_data_version < DATA_VERSION_4_5_0_0)
               || tenant_data_version >= DATA_VERSION_4_6_1_0)) {
    // GTT v2 + this broadcast fix ship in 4.4.2.1 (BP line) and 4.6.1.0
    // (main line). During a rolling upgrade window peers older than that do
    // not handle the new PCODE; broadcasting would surface unknown-PCODE
    // failures on those peers. Fall back to a local-only invalidation that
    // matches the pre-broadcast behavior: clean up the originator's session
    // map and (if the creator entry lives locally) execute the storage
    // delete. The cross-observer fix kicks in once every observer is upgraded.
    share::ObDropGTTV2SessionTabletRes local_res;
    if (OB_FAIL(ObRpcDropGTTV2SessionTabletP::handle_in_tenant(arg, local_res))) {
      LOG_WARN("failed to run local drop gtt v2 session tablet during compat window",
               KR(ret), K(tenant_data_version), K(arg));
    } else if (OB_SUCCESS != local_res.get_ret()) {
      ret = local_res.get_ret();
      LOG_WARN("local drop gtt v2 session tablet failed during compat window",
               KR(ret), K(tenant_data_version), K(arg));
    } else {
      LOG_INFO("ran local drop gtt v2 session tablet during compat window",
               K(tenant_data_version), K(arg), K(local_res));
    }
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global proxy is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.srv_rpc_proxy_));
  } else {
    common::ObArray<common::ObAddr> server_array;
    share::ObUnitTableOperator ut_operator;
    if (OB_FAIL(ut_operator.init(*GCTX.sql_proxy_))) {
      LOG_WARN("failed to init unit table operator", KR(ret));
    } else if (OB_FAIL(ut_operator.get_alive_servers_by_tenant(tenant_id, server_array))) {
      LOG_WARN("failed to get alive servers by tenant", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(server_array.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no alive server found for tenant", KR(ret), K(tenant_id));
    } else {
      rootserver::ObDropGTTV2SessionTabletProxy proxy(
          *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::drop_gtt_v2_session_tablet);
      // Bound the RPC by the worker's remaining time; if the worker is already
      // very close to timing out, fall back to GCONF.rpc_timeout so the RPC has
      // a sane minimum window instead of being issued with 0/negative timeout.
      const int64_t worker_remain = THIS_WORKER.get_timeout_remain();
      const int64_t default_timeout = GCONF.rpc_timeout.get_value();
      const int64_t rpc_timeout = worker_remain > 0 ? MIN(default_timeout, worker_remain) : default_timeout;
      // Dispatch to every alive server. proxy.call() failures here mean the
      // request could not even be queued (e.g. unreachable address); those
      // destinations will NOT appear in proxy.get_dests() / proxy.get_results()
      // afterwards, so result iteration cannot observe them — we track the
      // first such failure in dispatch_err for the post-loop accounting.
      int dispatch_err = OB_SUCCESS;
      ARRAY_FOREACH_X(server_array, idx, cnt, true) {
        const common::ObAddr &dest = server_array.at(idx);
        const int local_call_ret = proxy.call(dest, rpc_timeout, tenant_id, arg);
        if (OB_SUCCESS != local_call_ret) {
          LOG_WARN_RET(local_call_ret, "failed to dispatch drop gtt v2 session tablet rpc",
                       K(dest), K(rpc_timeout), K(arg));
          if (OB_SUCCESS == dispatch_err) {
            dispatch_err = local_call_ret;
          }
        }
      }
      int tmp_ret = OB_SUCCESS;
      common::ObArray<int> return_code_array;
      if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
        LOG_WARN("failed to wait all drop gtt v2 session tablet rpcs", KR(tmp_ret), K(arg));
        ret = tmp_ret;
      } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
        LOG_WARN("drop gtt v2 session tablet rpc return count mismatch", KR(ret),
                 "return_cnt", return_code_array.count(),
                 "dest_cnt", proxy.get_dests().count());
      }
      bool creator_executed = false;
      bool any_local_map_hit = false;
      int creator_ret = OB_SUCCESS;
      // Iterate over every result (don't bail on the first peer error): in a
      // mixed-version cluster some peers may reject the unknown PCODE while
      // others — including the creator — handle it correctly. Peer-level
      // failures are logged but do not fail the truncate; only the creator's
      // storage delete result is authoritative.
      ARRAY_FOREACH_X(proxy.get_results(), idx, cnt, OB_SUCC(ret)) {
        const share::ObDropGTTV2SessionTabletRes *res = proxy.get_results().at(idx);
        const common::ObAddr &dest_addr = proxy.get_dests().at(idx);
        const int peer_ret = return_code_array.at(idx);
        if (OB_SUCCESS != peer_ret) {
          LOG_WARN("drop gtt v2 session tablet rpc failed on peer; cache on that peer may be stale",
                   K(peer_ret), K(dest_addr), K(arg));
        } else if (OB_ISNULL(res)) {
          // by design: peer-level failure, not propagated (creator_ret is authoritative)
          LOG_WARN("drop gtt v2 session tablet rpc result is null", K(dest_addr), K(arg));
        } else {
          if (res->is_local_map_hit()) {
            any_local_map_hit = true;
          }
          if (res->is_executed_on_creator()) {
            creator_executed = true;
            creator_ret = res->get_ret();
            if (OB_SUCCESS != creator_ret) {
              LOG_WARN("creator failed to delete session tablets", K(creator_ret), K(dest_addr), K(arg));
            } else {
              LOG_INFO("creator finished session tablet drop", K(dest_addr), K(arg));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (creator_executed) {
          ret = creator_ret;
        } else if (any_local_map_hit) {
          // Some observer holds a stale entry for this (session, table) tuple but no observer
          // claims is_creator_=true. This typically means the creator session terminated before
          // truncate fired and its session-close path has already dropped the storage tablet;
          // the functor has invalidated the lingering caches. Log loudly so we can catch
          // unexpected leaks via observability.
          LOG_WARN("no creator observer found while stale entries exist; "
                   "creator session likely terminated, relying on session-close cleanup",
                   K(arg), K(dispatch_err));
        } else {
          LOG_INFO("no observer holds the session tablet entry, drop is a no-op",
                   K(arg), K(dispatch_err));
        }
      }
    }
  }
  return ret;
}


int ObSessionTabletGCHelper::refresh_tenant_schema_if_need(int64_t &latest_schema_version)
{
  int ret = OB_SUCCESS;
  latest_schema_version = OB_INVALID_SCHEMA_VERSION;
  ObRefreshSchemaStatus schema_stat;
  schema_stat.tenant_id_ = tenant_id_;
  ObMultiVersionSchemaService *schema_service = nullptr;

#ifdef ERRSIM
  if (OB_FAIL(EN_SESSION_TABLET_GC_FAILED? : OB_SUCCESS)) {
    LOG_INFO("skip refresh tenant schema due to fake err", K(ret));
    return OB_SUCCESS;
  }
#endif
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_ISNULL(schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null schema_service", K(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_schema_version_in_inner_table(*GCTX.sql_proxy_,
                                                                       schema_stat,
                                                                       latest_schema_version))) {
    LOG_WARN("fail to get schema version in inner table", K(ret), K(schema_stat));
  } else if (OB_FAIL(schema_service->async_refresh_schema(tenant_id_, latest_schema_version))) {
    LOG_WARN("fail to try refresh schema", K(ret), K(latest_schema_version));
  }
  return ret;
}

}  // namespace storage
} // namespace oceanbase
