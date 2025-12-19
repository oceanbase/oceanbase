/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/tablet/ob_session_tablet_helper.h"
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
#include "share/tablet/ob_tablet_to_table_history_operator.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

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
    share::schema::ObLatestSchemaGuard schema_guard(schema_service, tenant_id_);
    observer::ObInnerSQLConnection *conn = NULL;
    if (OB_FAIL(trans_.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to begin transaction", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(tablet_creator_.init(true/* need_check_tablet_cnt */))) {
      LOG_WARN("failed to init tablet creator", KR(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(table_ids_.at(0), table_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(table_ids_.at(0)));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_INVALID_ARGUMENT;
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
      const uint64_t table_id = table_schema->is_oracle_tmp_table_v2_index_table() ? table_schema->get_data_table_id() : table_schema->get_table_id();
      transaction::tablelock::ObLockTableRequest table_lock_arg;
      table_lock_arg.lock_mode_ = transaction::tablelock::ROW_EXCLUSIVE;
      table_lock_arg.timeout_us_ = MIN(THIS_WORKER.get_timeout_remain(), 10000000/* us */);
      table_lock_arg.table_id_ = table_id;
      table_lock_arg.op_type_ = transaction::tablelock::IN_TRANS_COMMON_LOCK;
      if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_table(tenant_id_, table_lock_arg, conn))) {
        LOG_WARN("lock table failed", KR(ret), K(table_lock_arg));
      } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id_, table_id, transaction::tablelock::SHARE, conn->get_ob_query_timeout(), trans_))) {
        LOG_WARN("lock online ddl table failed", KR(ret), K(table_lock_arg));
      } else {
        const share::schema::ObTablegroupSchema *tablegroup_schema = nullptr;
        if (OB_INVALID_ID != table_schema->get_tablegroup_id()) {
          if (OB_FAIL(schema_guard.get_tablegroup_schema(table_schema->get_tablegroup_id(), tablegroup_schema))) {
            LOG_WARN("failed to get tablegroup schema", KR(ret), K(table_schema->get_tablegroup_id()));
          } else if (OB_ISNULL(tablegroup_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablegroup schema is null", KR(ret), K(table_schema->get_tablegroup_id()), KPC(table_schema));
          }
        }
        if (FAILEDx(generate_tablet_create_arg(*schema_service, schema_guard, tenant_data_version, table_schema, tablegroup_schema))) {
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
          } else if (OB_FAIL(share::ObTabletToTableHistoryOperator::create_tablet_to_table_history(trans_, tenant_id_, table_schema->get_schema_version(), tablet_table_pairs))) {
            LOG_WARN("failed to create tablet to table history", KR(ret));
          } else {
            FLOG_INFO("session tablet created", KR(ret), K(table_ids_), K(ls_id_), K(tablet_ids_), K(session_tablet_infos), K(tablet_table_pairs));
          }
        }
      }
    }
    if (trans_.is_started()) {
      if (OB_SUCC(ret) && OB_NOT_NULL(table_schema)) { // serialize inc schemas for cdc
        if (OB_FAIL(serialize_inc_schema(tenant_id_, trans_, *table_schema))) {
          LOG_WARN("fail to serialize inc schemas", KR(ret), K(tenant_id_), KPC(table_schema));
        }
      }
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
    rootserver::ObNewTableTabletAllocator new_table_tablet_allocator(tenant_id_, schema_guard, GCTX.sql_proxy_);
    if (table_schema.is_oracle_tmp_table_v2_index_table()) {
      const uint64_t data_table_id = table_schema.get_data_table_id();
      const share::schema::ObTableSchema *data_table_schema = nullptr;
      const ObSessionTabletInfoKey data_table_key(data_table_id, sequence_, session_id_);
      if (OB_SUCC(session_tablet_map_.get_session_tablet(data_table_key, data_table_info))) {
      } else if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get data table info", KR(ret), K(data_table_key));
      } else if (OB_FAIL(lastest_schema_guard.get_table_schema(data_table_id, data_table_schema))) {
        LOG_WARN("failed to get data table schema", KR(ret), K(data_table_id));
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data table schema is null", KR(ret), K(data_table_id));
      } else {
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
                                                                                table_schema,
                                                                                tablegroup_schema,
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
        ret = OB_ERR_UNEXPECTED;
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
                                     false/*has_cs_replica*/))) {
    LOG_WARN("failed to init tablet creator arg", KR(ret));
  } else if (OB_FAIL(tablet_creator_.add_create_tablet_arg(create_tablet_arg))) {
    LOG_WARN("failed to add create tablet arg", KR(ret));
  }
  return ret;
}

// if the table is a related table of a data table, we need to lock the data table
// if the table was dropped and gc tasks call this function, we can delete the tablet directly
int ObSessionTabletDeleteHelper::do_work()
{
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  if (!tablet_info_.is_creator_) {
    ret = OB_SUCCESS;
    LOG_INFO("not creator session, skip delete from oracle temporary table", KR(ret), K(tablet_info_));
  } else if (OB_ISNULL(schema_service) || OB_ISNULL(schema_service->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), KP(schema_service));
  } else {
    observer::ObInnerSQLConnection *conn = NULL;
    common::ObSEArray<common::ObTabletID, 1> tablet_ids;
    const share::schema::ObTableSchema *table_schema = nullptr;
    share::schema::ObLatestSchemaGuard schema_guard(schema_service, tenant_id_);
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_service->gen_new_schema_version(tenant_id_, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(trans_.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to begin transaction", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(tablet_ids.push_back(tablet_info_.tablet_id_))) {
      LOG_WARN("failed to push back tablet id", KR(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tablet_info_.table_id_, table_schema))) {
      if (OB_ERR_SCHEMA_HISTORY_EMPTY == ret || OB_INVALID_ARGUMENT == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("table schema is empty, delete the tablet directly", KR(ret), K(tablet_info_.table_id_));
        // the table is not exist, delete the tablet directly
        if (OB_FAIL(delete_tablets(tablet_ids, new_schema_version))) {
          LOG_WARN("failed to delete tablets", KR(ret));
        }
      } else {
        LOG_WARN("failed to get table schema", KR(ret), K(tablet_info_.table_id_));
      }
    } else if (OB_ISNULL(table_schema)) {
      // the table is not exist, delete the tablet directly
      if (OB_FAIL(delete_tablets(tablet_ids, new_schema_version))) {
        LOG_WARN("failed to delete tablets", KR(ret));
      }
    } else if (OB_UNLIKELY(PARTITION_LEVEL_ZERO != table_schema->get_part_level()
                        || !(table_schema->is_oracle_tmp_table_v2() || table_schema->is_oracle_tmp_table_v2_index_table()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("partition level is zero or table is not oracle tmp table v2", KR(ret), KPC(table_schema));
    } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans_.get_connection()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connection is null", KR(ret), K(tenant_id_));
    } else {
      // 1. Acquire a ROW EXCLUSIVE table lock on table (Table ID).
      // 2. Acquire an EXCLUSIVE Online DDL lock on table (Table ID).
      // 3. Acquire an EXCLUSIVE Online DDL lock on tablet (Tablet ID).
      uint64_t table_id = table_schema->is_oracle_tmp_table_v2_index_table() ? table_schema->get_data_table_id() : table_schema->get_table_id();
      transaction::tablelock::ObLockTableRequest table_lock_arg;
      table_lock_arg.lock_mode_ = transaction::tablelock::ROW_EXCLUSIVE;
      table_lock_arg.timeout_us_ = 0; // try lock, if not success, will be deleted by GC tasks later
      table_lock_arg.table_id_ = table_id;
      table_lock_arg.op_type_ = transaction::tablelock::IN_TRANS_COMMON_LOCK;
      if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_table(tenant_id_, table_lock_arg, conn))) {
        LOG_WARN("lock table failed", KR(ret), K(table_lock_arg));
      } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id_, table_id, transaction::tablelock::EXCLUSIVE, 0 /*try lock*/, trans_))) {
        LOG_WARN("lock online ddl table failed", KR(ret), K(table_lock_arg));
      } else if (OB_FAIL(delete_tablets(tablet_ids, new_schema_version))) {
        LOG_WARN("failed to delete tablets", KR(ret));
      } else {
        LOG_INFO("succeed to remove tablet", KR(ret), K(tablet_info_), K(lbt()));
      }
    }
    if (trans_.is_started()) {
      if (OB_SUCC(ret) && OB_NOT_NULL(table_schema)) { // serialize inc schemas for cdc
        if (OB_FAIL(serialize_inc_schema(tenant_id_, trans_, *table_schema))) {
          LOG_WARN("fail to serialize inc schemas", KR(ret), K(tenant_id_), KPC(table_schema));
        }
      }
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

// tablets must be locked before being deleted
int ObSessionTabletDeleteHelper::delete_tablets(const ObIArray<common::ObTabletID> &tablet_ids, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!trans_.is_started() || tablet_ids.count() == 0 || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(trans_.is_started()), K(tablet_ids), K(schema_version));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets_in_trans(tenant_id_, tablet_ids, transaction::tablelock::EXCLUSIVE, 0 /*try lock*/, trans_))) {
    LOG_WARN("lock online ddl tablets failed", KR(ret), K(tablet_ids));
  } else if (OB_FAIL(share::ObTabletToLSTableOperator::batch_remove(trans_, tenant_id_, tablet_ids))) {
    LOG_WARN("failed to batch remove tablet", KR(ret));
  } else if (OB_FAIL(mds_remove_tablet(tenant_id_, tablet_info_.ls_id_, tablet_ids, trans_))) {
    LOG_WARN("failed to mds remove tablet", KR(ret));
  } else if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::batch_remove(trans_, tenant_id_, tablet_ids))) {
    LOG_WARN("failed to batch remove session tablet", KR(ret));
  } else if (OB_FAIL(share::ObTabletToTableHistoryOperator::drop_tablet_to_table_history(trans_, tenant_id_, schema_version, tablet_ids))) {
    LOG_WARN("failed to drop tablet to table history", KR(ret));
  }
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

bool compare_sequence(const storage::ObSessionTabletInfo &lhs, const storage::ObSessionTabletInfo &rhs)
{
  int64_t lhs_sequence = lhs.get_sequence() & ((1LL << 43) - 1LL);
  int64_t rhs_sequence = rhs.get_sequence() & ((1LL << 43) - 1LL);
  return lhs_sequence < rhs_sequence;
}

int ObSessionTabletGCHelper::do_work()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<storage::ObSessionTabletInfo, 1> session_tablet_infos;
  common::ObMySQLTransaction trans;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("failed to begin transaction", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::batch_get(trans, tenant_id_, session_tablet_infos))) {
    LOG_WARN("failed to batch get session tablet", KR(ret));
  } else if (OB_UNLIKELY(session_tablet_infos.count() == 0)) {
    ret = OB_SUCCESS;
    LOG_INFO("session tablet infos is empty", KR(ret));
  } else {
    common::ObSArray<uint32_t> session_id_array;
    common::ObSArray<bool> session_alive_array;
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
      } else {
        // remove the session tablet which is not alive
        for (int64_t i = 0; i < session_tablet_infos.count(); ++i) {
          if (false == session_alive_array.at(i)) {
            session_tablet_infos.at(i).is_creator_ = true;
            storage::ObSessionTabletDeleteHelper tablet_delete_helper(tenant_id_, session_tablet_infos.at(i));
            if (OB_FAIL(tablet_delete_helper.do_work())) {
              LOG_WARN("failed to delete session tablet", KR(ret), K(session_tablet_infos.at(i)));
            } else {
              LOG_INFO("succeed to remove session tablet", K(tenant_id_), K(session_tablet_infos.at(i)));
            }
          }
        }
      }
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
  return ret;
}

int ObSessionTabletGCHelper::is_sys_ls_leader(bool &is_leader) const
{
  int ret = OB_SUCCESS;
  is_leader = false;
  common::ObRole role= FOLLOWER;
  int64_t proposal_id = 0;
  MTL_SWITCH(tenant_id_) {
    ObLSService *ls_svr = MTL(ObLSService*);
    ObLS *ls = NULL;
    ObLSHandle handle;
    logservice::ObLogHandler *log_handler = NULL;
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mtl ObLSService should not be null", KR(ret), KP(ls_svr));
    } else if (OB_FAIL(ls_svr->get_ls(SYS_LS, handle, ObLSGetMod::OBSERVER_MOD))) {
      if (OB_LS_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("sys ls not exist, skip", KR(ret));
      } else {
        LOG_WARN("get ls failed", KR(ret));
      }
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be null", KR(ret));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log_handler is null", KR(ret), KP(log_handler));
    } else if (OB_FAIL(log_handler->get_role(role, proposal_id))) {
      LOG_WARN("fail to get role and epoch", KR(ret));
    } else {
      is_leader = is_strong_leader(role);
    }
  }
  return ret;
}


int ObSessionTabletGCHelper::is_table_has_active_session(
  const uint64_t tenant_id,
  const uint64_t table_id,
  bool &has_active_session)
{
  int ret = OB_SUCCESS;
  has_active_session = false;
  common::ObSEArray<storage::ObSessionTabletInfo, 4> session_tablet_infos;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::get_by_table_id(*GCTX.sql_proxy_, tenant_id, table_id, session_tablet_infos))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("gtt has no active session", KR(ret), K(table_id));
    } else {
      LOG_WARN("failed to get session tablet infos", KR(ret));
    }
  } else if (OB_UNLIKELY(session_tablet_infos.count() == 0)) {
    ret = OB_SUCCESS;
    LOG_INFO("gtt has no active session", KR(ret), K(table_id));
  } else {
    common::ObSArray<uint32_t> session_id_array;
    common::ObSArray<bool> session_alive_array;
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
      if (FAILEDx(transaction::tablelock::ObTableLockDetectFuncList::batch_detect_session_alive_at_least_one(tenant_id,
          session_id_array, nullptr, session_alive_array))) {
        LOG_WARN("failed to batch detect session alive", KR(ret));
      } else if (is_contain(session_alive_array, true)) {
        has_active_session = true;
        LOG_INFO("table has active session", KR(ret), K(session_tablet_infos), K(session_id_array), K(session_alive_array));
      }
    }
  }
  return ret;
}

int ObSessionTabletGCHelper::is_table_has_active_session(
  const share::schema::ObSimpleTableSchemaV2 *table_schema,
  const obrpc::ObAlterTableArg *alter_table_arg)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is null", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(table_schema->get_tenant_id(), data_version))) {
    LOG_WARN("failed to get tenant data version", KR(ret));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_4_2_0)) {
    ret = OB_SUCCESS;
    LOG_INFO("tenant data version is less than 4.4.2.0, skip check", KR(ret), K(data_version));
  } else if (table_schema->is_hidden_schema() || table_schema->get_session_id() != 0 || (!table_schema->is_oracle_tmp_table_v2() && !table_schema->is_oracle_tmp_table_v2_index_table())) {
    ret = OB_SUCCESS;
    LOG_INFO("table is not oracle tmp table v2 or oracle tmp table v2 index table", KR(ret), KPC(table_schema));
  } else {
    uint64_t table_id = table_schema->is_oracle_tmp_table_v2_index_table() ? table_schema->get_data_table_id() : table_schema->get_table_id();
    bool need_check = true;
    bool has_active_session = false;
    if (nullptr != alter_table_arg) {
      int64_t num_members = alter_table_arg->alter_table_schema_.alter_option_bitset_.num_members();
      if (num_members == 1) {
        // only rename and comment need not to check if table has active session
        need_check = !alter_table_arg->alter_table_schema_.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::TABLE_NAME) &&
                     !alter_table_arg->alter_table_schema_.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::COMMENT);
      }
    }
    if (false == need_check) {
      ret = OB_SUCCESS;
      LOG_INFO("not need to check if table has active session", KR(ret), K(table_id), KPC(table_schema));
    } else if (OB_FAIL(is_table_has_active_session(table_schema->get_tenant_id(), table_id, has_active_session))) {
      LOG_WARN("failed to check if table has active session", KR(ret), K(table_id), KPC(table_schema));
    } else if (has_active_session) {
      ret = OB_ERR_TEMP_TABLE_BUSY;
      LOG_WARN("table has active session", KR(ret), K(table_id), KPC(table_schema));
      LOG_USER_ERROR(OB_ERR_TEMP_TABLE_BUSY);
    } else {
      LOG_INFO("table has no active temporary tablet", KR(ret), K(table_id), KPC(table_schema));
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

}  // namespace storage
} // namespace oceanbase