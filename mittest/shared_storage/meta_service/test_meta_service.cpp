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
#define USING_LOG_PREFIX STORAGETEST

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#define protected public
#define private public
#include "storage/schema_utils.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_sstablelist_op.h"
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_define.h"
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_op.h"
#include "storage/incremental/ob_shared_meta_service.h"

#include "lib/string/ob_string_holder.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "shared_storage/clean_residual_data.h"
#include "storage/test_tablet_helper.h"
#include "storage/init_basic_struct.h"
#include "storage/tx_storage/ob_ls_service.h"

#include "mittest/shared_storage/atomic_protocol/test_ss_atomic_util.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
static int64_t lease_epoch = 1;

using namespace common;

static bool global_is_sswriter = true;
void mock_switch_sswriter()
{
  ATOMIC_INC(&lease_epoch);
  LOG_INFO("mock switch sswriter", K(lease_epoch));
}

int ObSSWriterService::check_lease(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  is_sswriter = global_is_sswriter;
  epoch = ATOMIC_LOAD(&lease_epoch);
  return OB_SUCCESS;
}

int ObSSWriterService::get_sswriter_addr(
    const ObSSWriterKey &key,
    ObSSWriterAddr &sswriter_addr,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  sswriter_addr.addr_ = GCTX.self_addr();
  return ret;
}

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

static const uint64_t INITED_TABLET_ID_ = 202020;
static uint64_t init_tablet_id_ = INITED_TABLET_ID_;

class MockSSTableGenerator
{
private:
  void generate_table_key_(
                           const ObITable::TableType &type,
                           const ObTabletID &tablet_id,
                           const int64_t base_version,
                           const int64_t snapshot_version,
                           ObITable::TableKey &table_key);
public:
  int mock_sstable(
                   ObArenaAllocator &allocator,
                   const ObTableSchema &table_schema,
                   const ObITable::TableType &type,
                   const ObTabletID &tablet_id,
                   const int64_t base_version,
                   const int64_t snapshot_version,
                   ObTableHandleV2 &table_handle);
};

class TestSSMetaService : public ::testing::Test
{
public:
  TestSSMetaService() = default;
  virtual ~TestSSMetaService() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp()
  {
    const int64_t LS_ID = 1111;
    ObLSID ls_id(LS_ID);
    ls_id_ = ls_id;
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
  const ObTabletID get_next_tablet_id();
  int build_update_table_store_param_(ObArenaAllocator &allocator,
                                      const ObTableSchema &schema,
                                      const ObLSHandle &ls_handle,
                                      const ObTabletHandle &tablet_handle,
                                      const compaction::ObMergeType &merge_type,
                                      ObTableHandleV2 &table_handle, // for sstable life
                                      ObUpdateTableStoreParam &param);
public:
  ObLSID ls_id_;
};

void MockSSTableGenerator::generate_table_key_(
    const ObITable::TableType &type,
    const ObTabletID &tablet_id,
    const int64_t base_version,
    const int64_t snapshot_version,
    ObITable::TableKey &table_key)
{
  table_key.reset();
  table_key.tablet_id_ = tablet_id;
  table_key.table_type_ = type;
  table_key.version_range_.base_version_ = base_version;
  table_key.version_range_.snapshot_version_ = snapshot_version;
}

int MockSSTableGenerator::mock_sstable(
  ObArenaAllocator &allocator,
  const ObTableSchema &table_schema,
  const ObITable::TableType &type,
  const ObTabletID &tablet_id,
  const int64_t base_version,
  const int64_t snapshot_version,
  ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;

  ObITable::TableKey table_key;
  generate_table_key_(type, tablet_id, base_version, snapshot_version, table_key);

  ObTabletCreateSSTableParam param;
  ObSSTable *sstable = nullptr;

  ObStorageSchema storage_schema;
  if (OB_FAIL(storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL))) {
    LOG_WARN("failed to init storage schema", K(ret));
  } else if (OB_FAIL(param.init_for_empty_major_sstable(tablet_id, storage_schema, 100, -1, false, false))) {
    LOG_WARN("failed to build create sstable param", K(ret), K(table_key));
  } else {
    param.table_key_ = table_key;
    param.max_merged_trans_version_ = 200;
    param.filled_tx_scn_ = table_key.get_end_scn();
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, allocator, table_handle))) {
      LOG_WARN("failed to create sstable", K(param));
    }
  }

  if (FAILEDx(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_handle));
  }
  return ret;
}

const ObTabletID TestSSMetaService::get_next_tablet_id()
{
  return ObTabletID(init_tablet_id_++);
}

int TestSSMetaService::build_update_table_store_param_(ObArenaAllocator &allocator,
                                                       const ObTableSchema &schema,
                                                       const ObLSHandle &ls_handle,
                                                       const ObTabletHandle &tablet_handle,
                                                       const compaction::ObMergeType &merge_type,
                                                       ObTableHandleV2 &table_handle, // for sstable life
                                                       ObUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  ObITable::TableType table_type = merge_type == compaction::ObMergeType::MDS_MINOR_MERGE ? ObITable::TableType::MDS_MINOR_SSTABLE : ObITable::TableType::MAJOR_SSTABLE;
  MockSSTableGenerator sstable_gen;
  ObSSTable *sstable = nullptr;
  if (OB_FAIL(sstable_gen.mock_sstable(
      allocator, schema,
      table_type,
      tablet_handle.get_obj()->get_tablet_id(), 0, 200, table_handle))) {
    LOG_WARN("failed to generate new sstable", K(ret), K(schema), KPC(tablet_handle.get_obj()));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_handle));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be nullptr");
  } else {
    SCN clog_checkpoint_scn = SCN::min_scn();
    param.snapshot_version_ = tablet_handle.get_obj()->get_snapshot_version();
    param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();

    ObStorageSchema *schema_on_tablet = NULL;
    if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator, schema_on_tablet))) {
      LOG_WARN("failed to load storage schema", K(ret), K(tablet_handle));
    } else {
      param.storage_schema_ = schema_on_tablet;
    }

    param.rebuild_seq_ = ls_handle.get_ls()->get_rebuild_seq();
    param.ddl_info_.update_with_major_flag_ = false;

    param.sstable_ = sstable;
    param.allow_duplicate_sstable_ = true;

    if (FAILEDx(param.init_with_ha_info(ObHATableStoreParam(
            tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_,
            true /*need_check_transfer_seq*/)))) {
      LOG_WARN("failed to init with ha info", KR(ret));
    } else if (OB_FAIL(param.init_with_compaction_info(ObCompactionTableStoreParam(
                      merge_type,
                      clog_checkpoint_scn,
                      false /*need_report*/)))) {
      LOG_WARN("failed to init with compaction info", KR(ret));
    } else {
      LOG_INFO("success to init ObUpdateTableStoreParam", KR(ret), K(param), KPC(param.sstable_));
    }
  }
  return ret;
}

void TestSSMetaService::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMetaService::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestSSMetaService, test_create_ls)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObLSService *ls_svr = MTL(ObLSService *);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObCreateLSArg arg;
  ObLSHandle handle;
  ObLS *ls = NULL;
  ObLSID ls_id = ls_id_;

  const int64_t TABLET_ID = 49401;
  ObTabletID tablet_id(TABLET_ID);
  ObTablet *tablet =NULL;

  global_is_sswriter = false;

  // create ls
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg));
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(ls_id, ls->get_ls_id());

  // wait the ls to be leader
  ObMemberList member_list;
  int64_t paxos_replica_num = 1;
  (void) member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);
  GlobalLearnerList learner_list;
  ASSERT_EQ(OB_SUCCESS, ls->set_initial_member_list(member_list,
                                                    paxos_replica_num,
                                                    learner_list));
  for (int i=0;i<15;i++) {
    ObRole role;
    int64_t proposal_id = 0;
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
    if (role == ObRole::LEADER) {
      break;
    }
    ::sleep(1);
  }

  // get ls meta
  ObSSLSMeta ls_meta;
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, ls_meta));
  ASSERT_EQ(ls_id, ls_meta.ls_id_);

  // get inner tablet
  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  const uint64_t transfer_scn = 0;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_from_ss_(ls_id,
                                                      tablet_id,
                                                      transfer_scn,
                                                      allocator,
                                                      tablet));
  ASSERT_EQ(tablet_id, tablet->get_tablet_id());
  LOG_INFO("test_create_ls finish");
}

TEST_F(TestSSMetaService, test_update_ls_meta)
{
  int ret = OB_SUCCESS;

  LOG_INFO("test_update_ls_meta");
  uint64_t tenant_id = MTL_ID();
  ObLSService *ls_svr = MTL(ObLSService*);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObCreateLSArg arg;
  ObLSHandle handle;
  ObLS *ls = NULL;
  ObLSID ls_id = ls_id_;

  // 1. get origin ls meta
  ObSSLSMeta orig_ls_meta;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, orig_ls_meta));
  ASSERT_EQ(ls_id, orig_ls_meta.ls_id_);


  global_is_sswriter = true;
  // 2. get cache ls meta
  ObSSLSMeta *cache_ls_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta_cache_(ls_id, cache_ls_meta));
  ASSERT_NE(nullptr, cache_ls_meta);
  ASSERT_EQ(ls_id, cache_ls_meta->ls_id_);
  LOG_INFO("get ls meta", K(orig_ls_meta), KPC(cache_ls_meta));

  // 3. update ls gc state
  SCN offline_scn = SCN::base_scn();
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(ls_id,
                                                     LSGCState::LS_OFFLINE,
                                                     offline_scn));

  ASSERT_EQ(cache_ls_meta->offline_scn_, offline_scn);
  ASSERT_EQ(cache_ls_meta->gc_state_, LSGCState::LS_OFFLINE);

  // 4. clean cache and read from share storge
  cache_ls_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, meta_svr->ls_ssmeta_cache_.clear());
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta_cache_(ls_id, cache_ls_meta));
  ASSERT_EQ(nullptr, cache_ls_meta);

  ObSSLSMeta updated_ls_meta;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, updated_ls_meta));
  ASSERT_EQ(ls_id, updated_ls_meta.ls_id_);
  ASSERT_EQ(updated_ls_meta.offline_scn_, offline_scn);
  ASSERT_EQ(updated_ls_meta.gc_state_, LSGCState::LS_OFFLINE);
}

TEST_F(TestSSMetaService, test_acquire_first_created_ss_tablet_)
{
  int ret = OB_SUCCESS;
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTabletID cur_tablet_id = get_next_tablet_id();
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSID ls_id = ls_id_;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ret = TestTabletHelper::create_tablet(ls_handle, cur_tablet_id, schema, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle orig_tablet_handle;
  ret = ls->get_tablet(cur_tablet_id, orig_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObUpdateTableStoreParam update_param;
  ObTableHandleV2 table_handle;
  ret = build_update_table_store_param_(
    allocator,
    schema,
    ls_handle,
    orig_tablet_handle,
    compaction::ObMergeType::MAJOR_MERGE,
    table_handle,
    update_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle new_tablet_handle;
  ret = meta_svr->acquire_first_created_ss_tablet_(
    allocator,
    orig_tablet_handle,
    new_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestSSMetaService, test_create_tablet)
{
  int ret = OB_SUCCESS;

  LOG_INFO("test_create_tablet");
  ObLSID ls_id = ls_id_;
  ObTabletID tablet_id = get_next_tablet_id();
  ObTablet *tablet =NULL;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle handle;
  ObLS *ls = NULL;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle tablet_hdl;
  // 1. there is no ss tablet.
  const uint64_t transfer_scn = 0;
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, meta_svr->get_tablet_from_ss_(ls_id,
                                                               tablet_id,
                                                               transfer_scn,
                                                               allocator,
                                                               tablet));
  ASSERT_EQ(OB_TABLET_NOT_EXIST, meta_svr->get_tablet(ls_id,
                                                      tablet_id,
                                                      transfer_scn,
                                                      allocator,
                                                      tablet_hdl));
  // 2. create tablet while there is no ss tablet.
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  // create the tablet local
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(handle, tablet_id, table_schema, allocator));
  // create the tablet at shared storage
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id,
                                                tablet_id,
                                                transfer_scn));

  // 3. check the created tablet
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_from_ss_(ls_id,
                                                      tablet_id,
                                                      transfer_scn,
                                                      allocator,
                                                      tablet));
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id,
                                             tablet_id,
                                             transfer_scn,
                                             allocator,
                                             tablet_hdl));
  ASSERT_EQ(tablet_id, tablet_hdl.get_obj()->get_tablet_id());

  // 4. create again while there is ss tablet.
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id,
                                                tablet_id,
                                                transfer_scn));

}

// for first time upload
TEST_F(TestSSMetaService, test_update_tablet_table_store)
{
  int ret = OB_SUCCESS;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle orig_tablet_handle;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSID ls_id = ls_id_;
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ret = ls->get_tablet(ObTabletID(INITED_TABLET_ID_), orig_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema); // same as table_schema when create tablet
  ObUpdateTableStoreParam update_param;
  ObTableHandleV2 table_handle;
  ObTabletID tablet_id = ObTabletID(INITED_TABLET_ID_);
  const uint64_t transfer_scn = 0;
  ret = build_update_table_store_param_(
    allocator,
    schema,
    ls_handle,
    orig_tablet_handle,
    compaction::ObMergeType::MAJOR_MERGE,
    table_handle,
    update_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  // create the tablet at shared storage
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id_,
                                                tablet_id,
                                                transfer_scn));

  ret = meta_svr->update_tablet_table_store(
      ls_id_,
      tablet_id,
      update_param);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestSSMetaService, test_update_tablet_table_store_update)
{
  int ret = OB_SUCCESS;

  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTabletID cur_tablet_id = get_next_tablet_id();
  const uint64_t transfer_scn = 0;
  ObLSID ls_id = ls_id_;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = NULL;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ret = TestTabletHelper::create_tablet(ls_handle, cur_tablet_id, schema, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id_,
                                                cur_tablet_id,
                                                transfer_scn));

  LOG_INFO("test_meta_service debug : create tablet success", K(ret), K(ls_id_), K(cur_tablet_id));

  ObTabletHandle orig_tablet_handle;
  ret = ls->get_tablet(cur_tablet_id, orig_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObUpdateTableStoreParam update_param;
  ObTableHandleV2 table_handle;
  ret = build_update_table_store_param_(
    allocator,
    schema,
    ls_handle,
    orig_tablet_handle,
    compaction::ObMergeType::MAJOR_MERGE,
    table_handle,
    update_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("test_meta_service debug : ", K(orig_tablet_handle.get_obj()->get_pointer_handle()));
  ret = meta_svr->update_tablet_table_store(
      ls_id_,
      cur_tablet_id,
      update_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("test_meta_service debug : reg ss tablet success", K(ret), K(ls_id_), K(cur_tablet_id));

  ret = build_update_table_store_param_(
    allocator,
    schema,
    ls_handle,
    orig_tablet_handle,
    compaction::ObMergeType::MAJOR_MERGE,
    table_handle,
    update_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = meta_svr->update_tablet_table_store(
      ls_id_,
      cur_tablet_id,
      update_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = build_update_table_store_param_(
    allocator,
    schema,
    ls_handle,
    orig_tablet_handle,
    compaction::ObMergeType::MDS_MINOR_MERGE,
    table_handle,
    update_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = meta_svr->update_tablet_table_store(
      ls_id_,
      cur_tablet_id,
      update_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("test_meta_service debug : update ss tablet success", K(ret), K(ls_id_), K(cur_tablet_id));
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_meta_service.log*");
  OB_LOGGER.set_file_name("test_meta_service.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
