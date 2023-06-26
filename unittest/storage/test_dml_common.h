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

#include <gtest/gtest.h>

#define protected public
#define private public

#include "lib/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_array.h"
#include "common/ob_tablet_id.h"
#include "common/ob_clock_generator.h"
#include "common/object/ob_obj_type.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_storage_format.h"
#include "share/ob_tenant_info_proxy.h"
#include "share/schema/ob_table_param.h"
#include "storage/mockcontainer/mock_ob_iterator.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_service.h"
#include "mock_ls_tablet_service.h"
#include "mock_access_service.h"
#include "init_basic_struct.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
class TestTxCallback : public transaction::ObITxCallback
{
public:
  TestTxCallback() : committed_(false) {}
public:
  virtual void callback(int ret) override { UNUSED(ret); committed_ = true; }
  bool wait() { return !committed_; }
private:
  bool committed_;
};

class TestDmlCommon
{
public:
  static int create_ls(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      ObLSHandle &ls_handle);
  static int create_data_tablet(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  static int create_data_and_index_tablets(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &data_tablet_id,
      const common::ObIArray<common::ObTabletID> &index_tablet_id_array);
  static int mock_ls_tablet_service(
      const share::ObLSID &ls_id,
      ObLSTabletService *&tablet_service);
  static void delete_mocked_ls_tablet_service(ObLSTabletService *tablet_service);
  static int mock_access_service(
      ObLSTabletService *tablet_service,
      MockObAccessService *&access_service);
  static void delete_mocked_access_service(ObAccessService *access_service);

  static int build_table_param(
      const ObTableSchema &table_schema,
      const ObIArray<uint64_t> &output_column_ids,
      share::schema::ObTableParam &table_param);
  static int build_table_scan_param(
      const uint64_t tenant_id,
      ObTxReadSnapshot &read_snapshot,
      const share::schema::ObTableParam &table_param,
      ObTableScanParam &scan_param);
  static int build_table_scan_param(
      const uint64_t tenant_id,
      ObTransID &tx_id,
      const share::schema::ObTableParam &table_param,
      ObTableScanParam &scan_param);
  static int build_table_scan_param_base_(
      const uint64_t tenant_id,
      const share::schema::ObTableParam &table_param,
      bool read_latest,
      ObTableScanParam &scan_param);
  static void build_data_table_schema(
      const uint64_t tenant_id,
      share::schema::ObTableSchema &table_schema);
  static void build_index_table_schema(
      const uint64_t tenant_id,
      share::schema::ObTableSchema &table_schema);
  static int build_tx_desc(const uint64_t tenant_id, ObTxDesc *&tx_desc);
  static void build_tx_param(ObTxParam &tx_param);
  static void release_tx_desc(ObTxDesc &tx_desc);
  static int insert_data_to_tablet();
protected:
  static int build_pure_data_tablet_arg(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &data_tablet_id,
      obrpc::ObBatchCreateTabletArg &arg);
  static int build_mixed_tablets_arg(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &data_tablet_id,
      const common::ObIArray<common::ObTabletID> &index_tablet_id_array,
      obrpc::ObBatchCreateTabletArg &arg);
public:
  static const uint64_t TX_EXPIRE_TIME_US = 120 * 1000 * 1000; // 120s
  static const int64_t TEST_LS_ID = 2;
  static const uint64_t TEST_DATA_TABLE_ID = 50;
  static const uint64_t TEST_INDEX_TABLE_ID = 51;
  static constexpr const char *data_row_str =
      "bigint  bigint   bigint  var         var        dml          \n"
      "1       62       20      Houston     Rockets    T_DML_INSERT \n"
      "2       65       17      SanAntonio  Spurs      T_DML_INSERT \n"
      "3       58       24      Dallas      Mavericks  T_DML_INSERT \n"
      "4       51       31      LosAngeles  Lakers     T_DML_INSERT \n"
      "5       57       25      Phoenix     Suns       T_DML_INSERT \n"
      "6       32       50      NewJersey   Nets       T_DML_INSERT \n"
      "7       44       38      Miami       Heats      T_DML_INSERT \n"
      "8       21       61      Chicago     Bulls      T_DML_INSERT \n"
      "9       47       35      Cleveland   Cavaliers  T_DML_INSERT \n"
      "10      59       23      Detroit     Pistons    T_DML_INSERT \n"
      "11      40       42      Utah        Jazz       T_DML_INSERT \n"
      "12      50       32      Boston      Celtics    T_DML_INSERT \n";
};

int TestDmlCommon::create_ls(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;

  ObLSService *ls_svr = MTL(ObLSService*);
  bool b_exist = false;
  ObLS *ls = nullptr;
  obrpc::ObCreateLSArg create_ls_arg;

  if (OB_FAIL(gen_create_ls_arg(tenant_id, ls_id, create_ls_arg))) {
    STORAGE_LOG(WARN, "failed to build create ls arg", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls_svr->create_ls(create_ls_arg))) {
    STORAGE_LOG(WARN, "failed to create ls", K(create_ls_arg));
  } else if (OB_FAIL(ls_svr->check_ls_exist(ls_id, b_exist))) {
    STORAGE_LOG(WARN, "failed to check ls exist", K(ls_id));
  } else if (!b_exist) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, ls does not exist", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "failed to get ls", K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), K(ls_handle));
  }

  // set member list
  if (OB_SUCC(ret)) {
    ObMemberList member_list;
    const int64_t paxos_replica_num = 1;
    (void) member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);
    GlobalLearnerList learner_list;
    if (OB_FAIL(ls->set_initial_member_list(member_list, paxos_replica_num, learner_list))) {
      STORAGE_LOG(WARN, "failed to set initial member list", K(ret),
          K(member_list), K(paxos_replica_num));
    }
  }

  // check leader
  STORAGE_LOG(INFO, "check leader");
  ObRole role;
  for (int i = 0; OB_SUCC(ret) && i < 15; i++) {
    int64_t proposal_id = 0;
    if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
      STORAGE_LOG(WARN, "failed to get role", K(ret));
    } else if (role == ObRole::LEADER) {
      break;
    }
    ::sleep(1);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(ObRole::LEADER != role)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, role is not leader", K(ret), K(role));
  }

  return ret;
}

int TestDmlCommon::create_data_tablet(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  obrpc::ObBatchCreateTabletArg arg;

  if (OB_FAIL(create_ls(tenant_id, ls_id, ls_handle))) {
    STORAGE_LOG(WARN, "failed to create ls", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(build_pure_data_tablet_arg(tenant_id, ls_id, tablet_id, arg))) {
    STORAGE_LOG(WARN, "failed to build pure data tablet arg", K(ret),
        K(tenant_id), K(ls_id), K(tablet_id));
  } else {
    ret = OB_NOT_SUPPORTED;
  }

  return ret;
}

int TestDmlCommon::create_data_and_index_tablets(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &data_tablet_id,
    const common::ObIArray<common::ObTabletID> &index_tablet_id_array)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  obrpc::ObBatchCreateTabletArg arg;

  if (OB_FAIL(create_ls(tenant_id, ls_id, ls_handle))) {
    STORAGE_LOG(WARN, "failed to create ls", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(build_mixed_tablets_arg(tenant_id, ls_id,
      data_tablet_id, index_tablet_id_array, arg))) {
    STORAGE_LOG(WARN, "failed to build pure data tablet arg", K(ret),
        K(tenant_id), K(ls_id), K(data_tablet_id), K(index_tablet_id_array));
  } else {
    ret = OB_NOT_SUPPORTED;
  }

  return ret;
}

int TestDmlCommon::mock_ls_tablet_service(
    const share::ObLSID &ls_id,
    ObLSTabletService *&tablet_service)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLS *ls = nullptr;

  if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), K(ls));
  } else {
    ObLSTabletService &svc = ls->ls_tablet_svr_;
    MockInsertRowsLSTabletService *mock_svc = OB_NEW(MockInsertRowsLSTabletService, ObModIds::TEST);
    ObArray<common::ObTabletID> tablet_ids;
    ObLSTabletService::GetAllTabletIDOperator get_all_tablet_id_op(tablet_ids);
    if (OB_ISNULL(mock_svc)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
    } else if (OB_FAIL(svc.tablet_id_set_.foreach(get_all_tablet_id_op))) {
      STORAGE_LOG(WARN, "fail to get all tablet ids from set", K(ret));
    } else {
      mock_svc->ls_ = svc.ls_;
      // ignore ObTxDataMemtableMgr and ObTxCtxMemtableMgr

      // copy hash set
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
        const common::ObTabletID &tablet_id = tablet_ids.at(i);
        if (mock_svc->tablet_id_set_.set(tablet_id)) {
          STORAGE_LOG(WARN, "failed to insert tablet id", K(tablet_id));
        }
      }

      // ignore ObBucketLock
      mock_svc->is_inited_ = svc.is_inited_;
    }

    if (OB_SUCC(ret)) {
      tablet_service = mock_svc;
    }
  }

  return ret;
}

void TestDmlCommon::delete_mocked_ls_tablet_service(ObLSTabletService *tablet_service)
{
  MockInsertRowsLSTabletService *svc = static_cast<MockInsertRowsLSTabletService*>(tablet_service);
  OB_DELETE(MockInsertRowsLSTabletService, ObModIds::TEST, svc);
}

int TestDmlCommon::mock_access_service(
    ObLSTabletService *tablet_service,
    MockObAccessService *&access_service)
{
  int ret = OB_SUCCESS;
  MockObAccessService *mock_svc = OB_NEW(MockObAccessService, ObModIds::TEST);

  if (OB_ISNULL(tablet_service)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(tablet_service));
  } else if (OB_ISNULL(mock_svc)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
  } else {
    ObAccessService *svc = MTL(ObAccessService*);
    // do copy
    mock_svc->is_inited_ = svc->is_inited_;
    mock_svc->tenant_id_ = svc->tenant_id_;
    mock_svc->ls_svr_ = svc->ls_svr_;
    mock_svc->tablet_service_ = tablet_service;

    access_service = mock_svc;
  }

  return ret;
}

void TestDmlCommon::delete_mocked_access_service(ObAccessService *access_service)
{
  MockObAccessService *svc = static_cast<MockObAccessService*>(access_service);
  OB_DELETE(MockObAccessService, ObModIds::TEST, svc);
}

int TestDmlCommon::build_table_param(
    const ObTableSchema &table_schema,
    const ObIArray<uint64_t> &output_column_ids,
    share::schema::ObTableParam &table_param)
{
  int ret = OB_SUCCESS;

  //use table schema as index schema
  if (OB_UNLIKELY(!table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(table_schema));
  } else if (OB_FAIL(table_param.convert(table_schema, output_column_ids))) {
    STORAGE_LOG(WARN, "failed to convert to table param", K(ret), K(table_schema), K(output_column_ids));
  }

  return ret;
}

/* table_scan read_latest of transaction: tx_id */
int TestDmlCommon::build_table_scan_param(
    const uint64_t tenant_id,
    ObTransID &tx_id,
    const share::schema::ObTableParam &table_param,
    ObTableScanParam &scan_param)
{
  int ret = build_table_scan_param_base_(tenant_id, table_param, true, scan_param);
  if (OB_SUCC(ret)) {
    scan_param.tx_id_ = tx_id;
  }
  return ret;
}

/* table_scan read_by_snapshot */
int TestDmlCommon::build_table_scan_param(
    const uint64_t tenant_id,
    ObTxReadSnapshot &read_snapshot,
    const share::schema::ObTableParam &table_param,
    ObTableScanParam &scan_param)
{
  int ret = build_table_scan_param_base_(tenant_id, table_param, false, scan_param);
  if (OB_SUCC(ret)) {
    scan_param.snapshot_ = read_snapshot;
  }
  return ret;
}

int TestDmlCommon::build_table_scan_param_base_(
    const uint64_t tenant_id,
    const share::schema::ObTableParam &table_param,
    bool read_latest,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  int64_t expire_time = ObTimeUtility::current_time() + TX_EXPIRE_TIME_US;
  const uint64_t table_id = TEST_DATA_TABLE_ID;

  scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + 0); // pk
  scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + 1); // c1
  scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + 2); // c2
  scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + 3); // c3
  scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + 4); // c4

  scan_param.ls_id_ = TestDmlCommon::TEST_LS_ID;
  scan_param.tablet_id_ = TEST_DATA_TABLE_ID;

  scan_param.table_param_ = &table_param;
  scan_param.index_id_ = table_id; // table id
  scan_param.is_get_ = false;
  scan_param.timeout_ = expire_time;

  ObQueryFlag query_flag(ObQueryFlag::Forward, // scan_order
                         false, // daily_merge
                         false, // optimize
                         false, // sys scan
                         false, // full_row
                         false, // index_back
                         false, // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         read_latest // read_latest
                        );
  scan_param.scan_flag_.flag_ = query_flag.flag_;

  scan_param.reserved_cell_count_ = 5;
  scan_param.allocator_ = &CURRENT_CONTEXT->get_arena_allocator();
  scan_param.for_update_ = false;
  scan_param.for_update_wait_timeout_ = expire_time;
  scan_param.sql_mode_ = SMO_DEFAULT;
  scan_param.scan_allocator_ = &CURRENT_CONTEXT->get_arena_allocator();
  scan_param.frozen_version_ = -1;
  scan_param.force_refresh_lc_ = false;
  scan_param.output_exprs_ = nullptr;
  scan_param.aggregate_exprs_ = nullptr;
  scan_param.op_ = nullptr;
  scan_param.row2exprs_projector_ = nullptr;
  scan_param.schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  scan_param.tenant_schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  scan_param.limit_param_.limit_ = -1;
  scan_param.limit_param_.offset_ = 0;
  scan_param.need_scn_ = false;
  scan_param.pd_storage_flag_ = false;
  scan_param.fb_snapshot_.reset();

  ObNewRange range;
  range.table_id_ = table_id;
  range.start_key_.set_min_row();
  range.end_key_.set_max_row();
  scan_param.key_ranges_.push_back(range);

  return ret;
}

void TestDmlCommon::build_data_table_schema(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &table_schema)
{
  const uint64_t table_id = TEST_DATA_TABLE_ID;
  const int64_t micro_block_size = 16 * 1024;

  table_schema.reset();
  table_schema.set_table_name("test_dml_common");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_schema_version(share::OB_CORE_SCHEMA_VERSION + 1);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_max_used_column_id(ObObjType::ObExtendType - 1);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ObRowStoreType::ENCODING_ROW_STORE);
  table_schema.set_storage_format_version(ObStorageFormatVersion::OB_STORAGE_FORMAT_VERSION_V4);

#define ADD_COLUMN(column_id, column_name, data_type, collation_type, is_row_key) \
  { \
    ObColumnSchemaV2 column; \
    column.set_tenant_id(tenant_id); \
    column.set_column_id(column_id); \
    column.set_column_name(column_name); \
    column.set_data_type(data_type); \
    column.set_collation_type(collation_type); \
    if (is_row_key) { \
      column.set_rowkey_position(1); \
    } \
    table_schema.add_column(column); \
  }

  // table schema
  // a(bigint)  b(bigint)  c(bigint)  d(varchar)  e(varchar)
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 0, "a", ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI, true);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 1, "b", ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI, false);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 2, "c", ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI, false);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 3, "d", ObVarcharType, CS_TYPE_UTF8MB4_BIN, false);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 4, "e", ObVarcharType, CS_TYPE_UTF8MB4_BIN, false);
#undef ADD_COLUMN
}

void TestDmlCommon::build_index_table_schema(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &table_schema)
{
  const uint64_t data_table_id = TEST_DATA_TABLE_ID;
  const uint64_t index_table_id = TEST_INDEX_TABLE_ID;
  const int64_t micro_block_size = 16 * 1024;

  table_schema.reset();
  table_schema.set_table_name("test_dml_common_index");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_data_table_id(data_table_id);
  table_schema.set_table_id(index_table_id);
  table_schema.set_index_type(ObIndexType::INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_max_used_column_id(ObObjType::ObExtendType - 1);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_row_store_type(ObRowStoreType::ENCODING_ROW_STORE);
  table_schema.set_storage_format_version(ObStorageFormatVersion::OB_STORAGE_FORMAT_VERSION_V4);

  // add index column: a
  {
    ObColumnSchemaV2 column;
    column.set_tenant_id(tenant_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID);
    column.set_column_name("index_a");
    column.set_data_type(ObIntType);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_rowkey_position(1);
    table_schema.add_column(column);
  }
}

int TestDmlCommon::build_tx_desc(const uint64_t tenant_id, ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);
  if (OB_FAIL(tx_service->acquire_tx(tx_desc, 100))) {
    STORAGE_LOG(WARN, "failed to acquire tx", K(ret));
  } else {
    STORAGE_LOG(INFO, "acquired tx desc", KPC(tx_desc));
  }
  return ret;
}

void TestDmlCommon::build_tx_param(ObTxParam &tx_param)
{
  tx_param.access_mode_ = transaction::ObTxAccessMode::RW;
  tx_param.isolation_ = transaction::ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 1221;
  tx_param.timeout_us_ = TX_EXPIRE_TIME_US;
  STORAGE_LOG(INFO, "build tx param", K(tx_param));
}

void TestDmlCommon::release_tx_desc(ObTxDesc &tx_desc)
{
  transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);
  tx_service->release_tx(tx_desc);
}

int TestDmlCommon::build_pure_data_tablet_arg(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &data_tablet_id,
    obrpc::ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;

  ObCreateTabletInfo tablet_info;
  ObArray<common::ObTabletID> tablet_id_array;
  ObArray<int64_t> tablet_schema_index_array;
  share::schema::ObTableSchema table_schema;
  build_data_table_schema(tenant_id, table_schema);

  arg.reset();
  if (OB_FAIL(tablet_id_array.push_back(data_tablet_id))) {
    STORAGE_LOG(WARN, "failed to push tablet id into array", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(tablet_schema_index_array.push_back(0))) {
    STORAGE_LOG(WARN, "failed to push index into array", K(ret));
  } else if (OB_FAIL(tablet_info.init(tablet_id_array, data_tablet_id, tablet_schema_index_array, lib::Worker::CompatMode::MYSQL, false))) {
    STORAGE_LOG(WARN, "failed to init tablet info", K(ret), K(tablet_id_array),
        K(data_tablet_id), K(tablet_schema_index_array));
  } else if (OB_FAIL(arg.init_create_tablet(ls_id, share::SCN::min_scn(), false/*need_check_tablet_cnt*/))) {
    STORAGE_LOG(WARN, "failed to init create tablet", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(arg.table_schemas_.push_back(table_schema))) {
    STORAGE_LOG(WARN, "failed to push back table schema", K(ret), K(table_schema));
  } else if (OB_FAIL(arg.tablets_.push_back(tablet_info))) {
    STORAGE_LOG(WARN, "failed to push back tablet info", K(ret), K(tablet_info));
  }

  if (OB_FAIL(ret)) {
    arg.reset();
  }

  return ret;
}

int TestDmlCommon::build_mixed_tablets_arg(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &data_tablet_id,
    const common::ObIArray<common::ObTabletID> &index_tablet_id_array,
    obrpc::ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;

  ObCreateTabletInfo tablet_info;
  ObArray<common::ObTabletID> tablet_id_array;
  ObArray<int64_t> tablet_schema_index_array;
  share::schema::ObTableSchema data_table_schema;
  share::schema::ObTableSchema index_table_schema;
  build_data_table_schema(tenant_id, data_table_schema);
  build_index_table_schema(tenant_id, index_table_schema);

  arg.reset();
  if (OB_FAIL(tablet_id_array.push_back(data_tablet_id))) {
    STORAGE_LOG(WARN, "failed to push tablet id into array", K(ret), K(data_tablet_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_tablet_id_array.count(); ++i) {
      const common::ObTabletID &index_tablet_id = index_tablet_id_array.at(i);
      if (OB_FAIL(tablet_id_array.push_back(index_tablet_id))) {
        STORAGE_LOG(WARN, "failed to push back index tablet id", K(ret), K(index_tablet_id));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_schema_index_array.push_back(0))) {
    STORAGE_LOG(WARN, "failed to push index into array", K(ret));
  } else if (OB_FAIL(tablet_schema_index_array.push_back(1))) {
    STORAGE_LOG(WARN, "failed to push index into array", K(ret));
  } else if (OB_FAIL(tablet_info.init(tablet_id_array, data_tablet_id, tablet_schema_index_array, lib::Worker::CompatMode::MYSQL, false))) {
    STORAGE_LOG(WARN, "failed to init tablet info", K(ret), K(tablet_id_array),
        K(data_tablet_id), K(tablet_schema_index_array));
  } else if (OB_FAIL(arg.init_create_tablet(ls_id, share::SCN::min_scn(), false/*need_check_tablet_cnt*/))) {
    STORAGE_LOG(WARN, "failed to init create tablet", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(arg.table_schemas_.push_back(data_table_schema))) {
    STORAGE_LOG(WARN, "failed to push back data table schema", K(ret), K(data_table_schema));
  } else if (OB_FAIL(arg.table_schemas_.push_back(index_table_schema))) {
    STORAGE_LOG(WARN, "failed to push back index table schema", K(ret), K(index_table_schema));
  } else if (OB_FAIL(arg.tablets_.push_back(tablet_info))) {
    STORAGE_LOG(WARN, "failed to push back tablet info", K(ret), K(tablet_info));
  }

  if (OB_FAIL(ret)) {
    arg.reset();
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase
