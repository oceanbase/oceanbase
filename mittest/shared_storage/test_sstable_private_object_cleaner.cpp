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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define private public
#define protected public

#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "unittest/storage/test_tablet_helper.h"

namespace oceanbase
{
using namespace common;

namespace blocksstable
{

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
  table_schema.set_micro_index_clustered(false);
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  int64_t rowkey_pos = 0;
  for(int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i){
    ObObjType obj_type = ObObjType::ObIntType;
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


class TestSSTablePrivateObjectCleaner : public ::testing::Test
{
public:
  TestSSTablePrivateObjectCleaner() {}
  virtual ~TestSSTablePrivateObjectCleaner() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  int prepare_data_store_desc(ObWholeDataStoreDesc &data_desc);

private:
  static const uint64_t tenant_id_ = 1;
  static const uint64_t tablet_id_ = 300000;
  static const uint64_t table_id_ = 12345;
  static const uint64_t ls_id_ = 1001;
  static const int64_t column_count_ = 1;
  static const int64_t rowkey_count_ = 1;
  static const uint64_t mock_data_format_version = DATA_VERSION_4_3_3_0;
  static ObArenaAllocator allocator_;
  static ObTableSchema table_schema_;
};

ObArenaAllocator TestSSTablePrivateObjectCleaner::allocator_;
ObTableSchema TestSSTablePrivateObjectCleaner::table_schema_;

void TestSSTablePrivateObjectCleaner::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;

  /* mock mtl env*/
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  SERVER_STORAGE_META_SERVICE.is_started_ = true;

  /* create ls*/
  ObLSHandle ls_handle;
  create_ls(tenant_id_, ObLSID(ls_id_), ls_handle);

  /* create tablet */
  ASSERT_EQ(OB_SUCCESS, generate_table(table_id_, column_count_, rowkey_count_,
                           TestSSTablePrivateObjectCleaner::table_schema_));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(
                            ls_handle, ObTabletID(tablet_id_),
                            TestSSTablePrivateObjectCleaner::table_schema_,
                            TestSSTablePrivateObjectCleaner::allocator_));
}

void TestSSTablePrivateObjectCleaner::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSTablePrivateObjectCleaner::create_ls(
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

int TestSSTablePrivateObjectCleaner::prepare_data_store_desc(ObWholeDataStoreDesc &data_desc)
{
  int ret = OB_SUCCESS;
  ret = data_desc.init(
      false /*is_ddl*/, TestSSTablePrivateObjectCleaner::table_schema_,
      ObLSID(ls_id_), ObTabletID(tablet_id_), compaction::MAJOR_MERGE,
      ObTimeUtility::fast_current_time() /*snapshot_version*/,
      DATA_CURRENT_VERSION,
      TestSSTablePrivateObjectCleaner::table_schema_.get_micro_index_clustered(),
      0 /*transfer_seq*/);
  data_desc.get_desc().sstable_index_builder_ = nullptr;
  return ret;
}

TEST_F(TestSSTablePrivateObjectCleaner, test_cleaner)
{
  int ret = OB_SUCCESS;
  // prepare data store desc and macro block writer
  ObWholeDataStoreDesc data_desc;
  prepare_data_store_desc(data_desc);

  ObMacroBlockWriter data_writer;
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = 0;
  ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
  ObSSTablePrivateObjectCleaner cleaner;
  cleaner.is_ss_mode_ = true;
  ASSERT_EQ(OB_SUCCESS, data_writer.open(data_desc.get_desc(), 0/*parallel_idx*/, seq_param/*start_seq*/, pre_warm_param, cleaner));

  // insert 1000 rows in 100 micro blocks and 10 macro blocks.
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(3));
  ObDmlFlag flags[] = {DF_INSERT, DF_UPDATE, DF_DELETE};

  for (int64_t i = 1; OB_SUCC(ret) && i <= 1000; ++i) {
    row.storage_datums_[0].set_int(i);
    row.storage_datums_[1].set_int(-i);
    row.storage_datums_[2].set_int(-i);
    ASSERT_EQ(OB_SUCCESS, data_writer.append_row(row));
    if ((i + 1) % 10 == 0) {
      ASSERT_EQ(OB_SUCCESS, data_writer.build_micro_block());
    }
    if ((i + 1) % 100 == 0) {
      ASSERT_EQ(OB_SUCCESS, data_writer.try_switch_macro_block());
    }
  }

  ASSERT_EQ(10, cleaner.new_macro_block_ids_.count());
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sstable_private_object_cleaner.log*");
  OB_LOGGER.set_file_name("test_sstable_private_object_cleaner.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
