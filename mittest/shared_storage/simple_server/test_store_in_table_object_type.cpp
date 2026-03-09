/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif

#include <gtest/gtest.h>
#define protected public
#define private public
#include "lib/utility/ob_test_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "close_modules/shared_storage/storage/tiered_metadata_store/ob_tiered_metadata_store.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"
#undef private
#undef protected

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{

oceanbase::unittest::ObMockPalfKV PALF_KV;

int get_sslog_table_guard(const ObSSLogTableType type,
                          const int64_t tenant_id,
                          ObSSLogProxyGuard &guard)
{
  int ret = OB_SUCCESS;

  switch (type)
  {
    case ObSSLogTableType::SSLOG_TABLE: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogTableProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObSSLogTableProxy *sslog_table_proxy = new (proxy) ObSSLogTableProxy(tenant_id);
        if (OB_FAIL(sslog_table_proxy->init())) {
          SSLOG_LOG(WARN, "fail to inint", K(ret));
        } else {
          guard.set_sslog_proxy((ObISSLogProxy *)proxy);
        }
      }
      break;
    }
    case ObSSLogTableType::SSLOG_PALF_KV: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogKVProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObSSLogKVProxy *sslog_kv_proxy = new (proxy) ObSSLogKVProxy(&PALF_KV);
        guard.set_sslog_proxy((ObISSLogProxy *)proxy);
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      SSLOG_LOG(WARN, "invalid sslog type", K(type));
      break;
    }
  }

  return ret;
}
} // namespace sslog
} // namespace oceanbase

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::lib;

namespace oceanbase
{
char *shared_storage_info = nullptr;
namespace unittest
{

class ObStoreInTableObjectTypeTest : public ObSimpleClusterTestBase
{
public:
  ObStoreInTableObjectTypeTest()
      : ObSimpleClusterTestBase("test_store_in_table_object_type_dir", "20G", "20G", "20G")
  {}

  virtual void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    if (!tenant_created_) {
      OK(create_tenant("tt1", "5G", "10G", false/*oracle_mode*/, 8, "2G"));
      OK(get_tenant_id(tenant_id_));
      ASSERT_NE(0, tenant_id_);
      tenant_created_ = true;
      share::ObTenantSwitchGuard tguard;
      OK(tguard.switch_to(tenant_id_));
      OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
    }
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

protected:
  MacroBlockId build_table_meta_id(const uint64_t tablet_id,
                                   const uint64_t op_macro_seq,
                                   const uint64_t ls_id = 0) const
  {
    MacroBlockId id;
    id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META_IN_TABLE));
    id.set_second_id(tablet_id);
    id.set_third_id(op_macro_seq);
    id.set_ss_fourth_id(false/*is_inner_tablet*/, ls_id, 0/*reorg_scn*/);
    return id;
  }

  MacroBlockId build_major_meta_id(const uint64_t tablet_id,
                                   const uint64_t seq,
                                   const uint64_t reorg_scn = 0) const
  {
    MacroBlockId id;
    id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_META_MACRO));
    id.set_second_id(tablet_id);
    id.set_third_id(seq);
    id.set_reorganization_scn(reorg_scn);
    return id;
  }

  void write_macro(const MacroBlockId &id, const char *buf, const int64_t size) const
  {
    ObStorageObjectHandle handle;
    ASSERT_EQ(OB_SUCCESS, handle.set_macro_block_id(id));
    ObStorageObjectWriteInfo info;
    info.io_desc_.set_wait_event(1);
    info.buffer_ = buf;
    info.offset_ = 0;
    info.size_ = size;
    info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    info.mtl_tenant_id_ = tenant_id_;
    ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(info, handle));
  }

protected:
  static bool tenant_created_;
  static uint64_t tenant_id_;
};

bool ObStoreInTableObjectTypeTest::tenant_created_ = false;
uint64_t ObStoreInTableObjectTypeTest::tenant_id_ = 0;


TEST_F(ObStoreInTableObjectTypeTest, test_shared_tablet_sub_meta_in_table_reader_writer)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));
  int ret = OB_SUCCESS;
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_mgr);

  // 准备一个SHARED_TABLET_SUB_META_IN_TABLE宏块
  MacroBlockId table_meta_id;
  table_meta_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  table_meta_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META_IN_TABLE));
  table_meta_id.set_second_id(200001);                     // tablet_id
  table_meta_id.set_third_id((1ULL << 32) + 2);            // op_id=1, macro_seq=2
  table_meta_id.set_ss_fourth_id(false/*is_inner_tablet*/, 0/*ls_id*/, 0/*reorg_scn*/);

  const int64_t io_size = 1 * 1024;
  char write_buf[io_size] = {0};
  MEMSET(write_buf, 't', io_size);

  // 写入
  ObStorageObjectHandle write_handle;
  ASSERT_EQ(OB_SUCCESS, write_handle.set_macro_block_id(table_meta_id));
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_handle));

  // 读出（走table device异步读通路）
  ObStorageObjectHandle read_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = table_meta_id;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[io_size] = {0};
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_handle));
  ASSERT_NE(nullptr, read_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_handle.get_buffer(), io_size));

  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(table_meta_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // 删除时应将表存与非表存分流
  ObArray<MacroBlockId> macro_ids;

  // 使用 SHARED_MAJOR_META_MACRO 作为非表存类型（会写 remote）
  MacroBlockId major_meta_id = build_major_meta_id(200001, 1);
  MEMSET(write_buf, 'm', io_size);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = io_size;

  ObStorageObjectHandle major_write_handle;
  ASSERT_EQ(OB_SUCCESS, major_write_handle.set_macro_block_id(major_meta_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, major_write_handle));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(major_meta_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(table_meta_id));
  ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(major_meta_id));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_files(macro_ids));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(table_meta_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(major_meta_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(ObStoreInTableObjectTypeTest, test_list_remote_macro_ids_of_user_tablet)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTieredMetadataStore *tiered_metadata_store = MTL(ObTieredMetadataStore *);
  ASSERT_NE(nullptr, tiered_metadata_store);

  const uint64_t tablet_id = 200001;
  const uint64_t reorg_scn = 0;
  ObArray<MacroBlockId> macro_ids;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(false/*is_ls_inner_tablet*/,
                                                                 0/*ls_id*/,
                                                                 tablet_id,
                                                                 reorg_scn,
                                                                 macro_ids));
  ASSERT_EQ(0, macro_ids.count());

  const int64_t io_size = 8 * 1024;
  char buf[io_size];
  MEMSET(buf, 'a', io_size);

  MacroBlockId major_meta_id = build_major_meta_id(tablet_id, 1, reorg_scn);
  write_macro(major_meta_id, buf, io_size);

  MacroBlockId table_meta_id = build_table_meta_id(tablet_id, (1ULL << 32) + 2);
  ObString meta_value(sizeof("meta") - 1, "meta");
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->write_meta(table_meta_id, meta_value));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->is_meta_exist(table_meta_id, is_exist));
  ASSERT_TRUE(is_exist);
  macro_ids.reset();

  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(false/*is_ls_inner_tablet*/,
                                                                 0/*ls_id*/,
                                                                 tablet_id,
                                                                 reorg_scn,
                                                                 macro_ids));
  ASSERT_EQ(2, macro_ids.count());
  bool found_major = false;
  bool found_table = false;
  for (int64_t i = 0; i < macro_ids.count(); ++i) {
    if (macro_ids.at(i) == major_meta_id) {
      found_major = true;
    }
    if (macro_ids.at(i) == table_meta_id) {
      found_table = true;
    }
  }
  ASSERT_TRUE(found_major);
  ASSERT_TRUE(found_table);

  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_files(macro_ids));
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(major_meta_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(table_meta_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);

  macro_ids.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(false/*is_ls_inner_tablet*/,
                                                                 0/*ls_id*/,
                                                                 tablet_id,
                                                                 reorg_scn,
                                                                 macro_ids));
  ASSERT_EQ(0, macro_ids.count());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("20G");
  GCONF.memory_limit.set_value("10G");
  GCONF.system_memory.set_value("3G");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
