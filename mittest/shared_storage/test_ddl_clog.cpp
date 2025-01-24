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

#include <string>
#include <gtest/gtest.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "src/storage/ob_i_table.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "unittest/storage/test_tablet_helper.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

class TestDDLClogCase : public ::testing::Test
{
  public:
  TestDDLClogCase();
  virtual ~TestDDLClogCase();
  static void TearDownTestCase();
  static void SetUpTestCase();
  static void create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static void prepare_ddl_finish_log(ObTabletID &tablet_id,ObDDLFinishLog &finish_log);
  static void get_test_ls_handle();
  static void mock_sstable(ObTableHandleV2 &table_handle);

  int64_t get_next_table_id() {
    static int64_t inc_val = 0;
    inc_val += 1;
    return table_id_ + inc_val;
  }

  int64_t get_next_tablet_id() {
    static int64_t inc_val = 0;
    inc_val += 1;
    return tablet_id_ + inc_val;
  }

  void prepare_schema();

  /* basic info for mack tablet */
  static const uint64_t tenant_id_ = 1;
  static const uint64_t tablet_id_ = 300000;
  static const uint64_t table_id_ = 12345;
  static const uint64_t ls_id_ = 1001;
  static const int64_t mock_snapshot_version = 100;
  static const int64_t column_count_ = ObExtendType - 1;
  static const int64_t rowkey_count_ = 8;
  static const uint64_t mock_data_format_version = DATA_VERSION_4_3_3_0;

  static const uint64_t mock_start_scn_ = 100;

  static ObArenaAllocator allocator_;
  static ObTableSchema table_schema_;
};


ObArenaAllocator TestDDLClogCase::allocator_;
ObTableSchema TestDDLClogCase::table_schema_;


TestDDLClogCase::TestDDLClogCase()
{
}
TestDDLClogCase::~TestDDLClogCase()
{
}

void TestDDLClogCase::create_ls(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;

  ObLSService *ls_svr = MTL(ObLSService*);
  bool b_exist = false;
  ObLS *ls = nullptr;
  obrpc::ObCreateLSArg create_ls_arg;
  ASSERT_NE(nullptr, ls_svr);
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, create_ls_arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(create_ls_arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(ls_id, b_exist));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());
  ls = ls_handle.get_ls();

  // set member list
  ObMemberList member_list;
  const int64_t paxos_replica_num = 1;
  (void) member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);
  GlobalLearnerList learner_list;
  ASSERT_EQ(OB_SUCCESS, ls->set_initial_member_list(member_list, paxos_replica_num, learner_list));

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
  return ;
}

void TestDDLClogCase::mock_sstable(ObTableHandleV2 &table_handle)
{
  void *buf = nullptr;
  ObSSTable *sstable = nullptr;
  ObTabletCreateSSTableParam param;
  ObStorageSchema storage_schema;
  ObITable::TableKey table_key;

  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  table_key.version_range_.base_version_ = mock_snapshot_version;
  table_key.version_range_.snapshot_version_ = mock_snapshot_version;

  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(ObTabletID(tablet_id_), storage_schema, 100, -1, false));
  ASSERT_NE(nullptr, buf = allocator_.alloc(sizeof(ObSSTable)));
  ASSERT_NE(nullptr, sstable = new(buf)ObSSTable());
  ASSERT_EQ(OB_SUCCESS, sstable->init(param, &allocator_));
  ASSERT_EQ(OB_SUCCESS, table_handle.set_sstable(sstable, &allocator_));
}

void TestDDLClogCase::prepare_ddl_finish_log(ObTabletID &tablet_id, ObDDLFinishLog &finish_log)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObSSTable *sstable = nullptr;
  ObTabletCreateSSTableParam param;
  ObStorageSchema storage_schema;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObTabletMacroInfo macro_block_info;
  int64_t mock_start_macro_seq = 101;

  ObLSService* ls_service = MTL(ObLSService*);
  ObSharedObjectWriteInfo write_info;
  ObTabletMacroInfo tablet_macro_info;
  ObStorageObjectHandle object_handle;
  ObTabletPersisterParam persister_param(ObTabletID(tablet_id), 0 /*transfer_seq*/,mock_snapshot_version, mock_start_macro_seq);
  ObTabletPersister persister(persister_param, ObCtxIds::DEFAULT_CTX_ID);
  ObITable::TableKey table_key;
  table_key.tablet_id_ = ObTabletID(tablet_id); /*table key use new tablet id*/
  table_key.table_type_ = ObITable::MAJOR_SSTABLE;
  ObBlockInfoSet block_info_set;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObTabletSpaceUsage space_usage;
  common::ObSEArray<ObSharedObjectsWriteCtx, 16> total_write_ctxs;
  ObSArray<MacroBlockId> shared_meta_id_arr;

  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ASSERT_NE(ls_service, nullptr);
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::DDL_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ObTabletID(tablet_id_), tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));

  ASSERT_EQ(OB_SUCCESS, block_info_set.init());
  ASSERT_EQ(OB_SUCCESS, tablet_macro_info.init(allocator_, block_info_set, &linked_writer));

  ASSERT_EQ(OB_SUCCESS, persister.persist_and_fill_tablet((*tablet_handle.get_obj()), linked_writer, total_write_ctxs,
                                                          new_tablet_handle, space_usage,
                                                          macro_block_info, shared_meta_id_arr));
  ASSERT_EQ(OB_SUCCESS, persister.fill_tablet_write_info(allocator_, new_tablet_handle.get_obj(), macro_block_info,  write_info));
  blocksstable::ObStorageObjectOpt curr_opt;
  persister.build_async_write_start_opt_(curr_opt);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.alloc_object(curr_opt, object_handle));
  ASSERT_EQ(OB_SUCCESS, finish_log.init(tenant_id_, ObLSID(ls_id_), table_key, write_info.buffer_, write_info.size_, object_handle.get_macro_id(), mock_data_format_version));

  return ;
}

void set_column_type(const common::ObObjType obj_type, share::schema::ObColumnSchemaV2 &column)
{
  ObObjMeta meta_type;
  meta_type.set_type(obj_type);
  column.set_meta_type(meta_type);
  if (ob_is_string_type(obj_type) && obj_type != ObHexStringType) {
    meta_type.set_collation_level(CS_LEVEL_IMPLICIT);
    meta_type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_meta_type(meta_type);
  }
}

int generate_table(const uint64_t table_id, const int64_t column_count,
                    const int64_t rowkey_count, share::schema::ObTableSchema &table_schema)
{
  int ret = common::OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  share::schema::ObColumnSchemaV2 column;
  table_schema.reset();
  table_schema.set_table_name("test_table");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(tenant_id);
  table_schema.set_database_id(tenant_id);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_count);
  table_schema.set_max_used_column_id(column_count);
  table_schema.set_block_size(2L * 1024);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_micro_index_clustered(true);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  int64_t rowkey_pos = 0;
  for(int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    if (OB_FAIL(column.set_column_name(name))) {
      STORAGE_LOG(WARN, "set_column_name failed", K(ret));
    } else {
      set_column_type(obj_type, column);
      column.set_data_length(1);
      if (obj_type >= common::ObIntType && rowkey_pos < rowkey_count) {
        ++rowkey_pos;
        column.set_rowkey_position(rowkey_pos);
      } else {
        column.set_rowkey_position(0);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_schema.add_column(column))) {
        STORAGE_LOG(WARN, "add_column failed", K(ret), K(column));
      }
    }
  }
  return ret;
}

void TestDDLClogCase::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;

  /* mock mtl env*/
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  SERVER_STORAGE_META_SERVICE.is_started_ = true;

  /* create ls*/
  ObLSHandle ls_handle;
  create_ls(tenant_id_, ObLSID(ls_id_), ls_handle);

  /* create tablet */
  ASSERT_EQ(OB_SUCCESS, generate_table(table_id_, column_count_, rowkey_count_, table_schema_));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, ObTabletID(tablet_id_), table_schema_, allocator_));
}

void TestDDLClogCase::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestDDLClogCase, test_finish_log_write)
{
  /* preppare test*/
  ObDDLFinishLog finish_log;

  int64_t new_table_id = get_next_table_id();
  ObTabletID new_tablet_id(get_next_tablet_id());
  prepare_ddl_finish_log(new_tablet_id, finish_log);

  ObLSService* ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::DDL_MOD));

  /* create new tablet to test*/
  ObTableSchema tmp_schema;
  ASSERT_EQ(OB_SUCCESS, generate_table(new_table_id, column_count_, rowkey_count_, tmp_schema));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, ObTabletID(new_tablet_id), tmp_schema, allocator_));

  /* create ddl kv mgr*/
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObDDLKVHandle ddl_kv_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(new_tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true /*try create */));

  /* test for local write */
  ObDDLRedoLogWriter writer;
  // MockLogHandler mock_log_handler;

  // ON_CALL(mock_log_handler, append(testing::_, testing::_, testing::_,testing::_, testing::_, testing::_,testing::_))
  // .WillByDefault(testing::Return(OB_SUCCESS));


  bool is_remote_write = false;
  ASSERT_EQ(OB_SUCCESS, writer.init(ObLSID(ls_id_), new_tablet_id));
  ASSERT_EQ(OB_SUCCESS, writer.write_finish_log(false /*disable remote write*/,finish_log, is_remote_write));
}

TEST_F(TestDDLClogCase, test_finish_log_replay)
{
  ObLSHandle ls_handle;
  ObDDLFinishLog finish_log;
  ObTabletHandle tablet_handle;
  ObDDLRedoLogReplayer redo_replayer;
  int64_t new_table_id = get_next_table_id();

  ObTabletID new_tablet_id(get_next_tablet_id());
  prepare_ddl_finish_log(new_tablet_id, finish_log);

  ObLSService* ls_service  = MTL(ObLSService*);
  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::DDL_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ObTabletID(tablet_id_), tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));

  /* create new tablet to test*/
  ObTableSchema tmp_schema;
  ASSERT_EQ(OB_SUCCESS, generate_table(new_table_id, column_count_, rowkey_count_, tmp_schema));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, ObTabletID(new_tablet_id), tmp_schema, allocator_));

  /* create ddl kv mgr*/
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObDDLKVHandle ddl_kv_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(new_tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true /*try create */));

  share::SCN scn;
  scn.convert_from_ts(mock_start_scn_);
  ASSERT_EQ(OB_SUCCESS, redo_replayer.init(ls_handle.get_ls()));
  ASSERT_EQ(OB_SUCCESS, redo_replayer.replay_finish(finish_log, scn));
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -r -f ./test_ddl_clog.log*");
  OB_LOGGER.set_file_name("./test_ddl_clog.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
