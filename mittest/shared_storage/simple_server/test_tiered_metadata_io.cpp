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
#include "close_modules/shared_storage/storage/tiered_metadata_store/ob_tiered_metadata_object_manager.h"
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

class ObTieredMetadataIOTest : public ObSimpleClusterTestBase
{
public:
  ObTieredMetadataIOTest()
      : ObSimpleClusterTestBase("test_tiered_metadata_io_dir", "50G", "50G", "50G")
  {}

  virtual void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    if (!tenant_created_) {
      OK(create_tenant("tt1", "5G", "10G", false/*oracle_mode*/, 8));
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
  MacroBlockId build_tiered_metadata_file_id(const ObStorageObjectType object_type,
                                             const uint64_t tablet_id,
                                             const uint64_t op_macro_seq,
                                             const uint64_t ls_id = 1001) const
  {
    MacroBlockId id;
    id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    id.set_storage_object_type(static_cast<uint64_t>(object_type));
    id.set_second_id(tablet_id);
    id.set_third_id((1ULL << 32) + op_macro_seq); // op_id=1
    id.set_ss_fourth_id(ObTabletID(tablet_id).is_ls_inner_tablet(), ls_id, 0/*reorg_scn*/);
    return id;
  }

  MacroBlockId inc_file_id_seq(const MacroBlockId &file_id) const
  {
    MacroBlockId res = file_id;
    res.set_third_id(file_id.third_id() + 1);
    return res;
  }

  void set_to_ss_tiered_metadata_file_id(MacroBlockId &file_id) const
  {
    file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META));
  }

  void set_to_table_tiered_metadata_file_id(MacroBlockId &file_id) const
  {
    file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META_IN_TABLE));
  }

  ObStorageObjectWriteInfo gen_one_object_write_info(const char *buf, const int64_t size) const
  {
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = size;
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_unsealed();
    write_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    write_info.set_ls_epoch_id(0);
    return write_info;
  }

  ObStorageObjectReadInfo gen_one_object_read_info(const MacroBlockId &file_id,
                                                   char *buf,
                                                   const int64_t size) const
  {
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = file_id;
    read_info.offset_ = 0;
    read_info.buf_ = buf;
    read_info.size_ = size;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ); // FIXME
    read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    read_info.io_callback_ = nullptr;
    read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    return read_info;
  }

protected:
  static bool tenant_created_;
  static uint64_t tenant_id_;
};

bool ObTieredMetadataIOTest::tenant_created_ = false;
uint64_t ObTieredMetadataIOTest::tenant_id_ = 0;

#define READ_AND_TEST(ID) \
  ObStorageObjectReadInfo read_info_##ID = gen_one_object_read_info(file_id_##ID, read_buf, read_buf_size); \
  ObStorageObjectHandle read_hd_##ID; \
  ASSERT_EQ(OB_SUCCESS, OB_TIERED_METADATA_OBJECT_MGR.read_object(read_info_##ID, read_hd_##ID)); \
  ASSERT_EQ(read_hd_##ID.get_data_size(), STRLEN(buffer_##ID)); \
  ASSERT_EQ(0, MEMCMP(read_info_##ID.buf_, buffer_##ID, STRLEN(buffer_##ID)));

#define ITERATOR_AND_TEST(ID) \
  { \
    MacroBlockId macro_id; \
    ObString macro_value; \
    ASSERT_EQ(OB_SUCCESS, iterator.get_next(macro_id, macro_value)); \
    ASSERT_EQ(file_id_##ID, macro_id); \
    ASSERT_EQ(macro_value.length(), STRLEN(buffer_##ID)); \
    ASSERT_EQ(0, MEMCMP(macro_value.ptr(), buffer_##ID, STRLEN(buffer_##ID))); \
  }

#define ITERATOR_END_AND_TEST() \
  { \
    MacroBlockId macro_id; \
    ObString macro_value; \
    ASSERT_EQ(OB_ITER_END, iterator.get_next(macro_id, macro_value)); \
  }

TEST_F(ObTieredMetadataIOTest, test_tiered_metadata_io_1)
{
  // Three SHARED_TABLET_SUB_META_IN_TABLE object IO
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));
  int ret = OB_SUCCESS;

  ObTieredMetadataStore *tiered_metadata_store = MTL(ObTieredMetadataStore *);
  ASSERT_NE(nullptr, tiered_metadata_store);

  // IO 1 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  ObArray<MacroBlockId> batch_macro_id;
  ObArray<ObStorageObjectWriteInfo> batch_write_info;
  const int64_t tablet_id = 200001;

  MacroBlockId file_id_1 = build_tiered_metadata_file_id(ObStorageObjectType::SHARED_TABLET_SUB_META_IN_TABLE,
                                                         tablet_id,
                                                         1);
  const char *buffer_1 = "aaa";
  ObStorageObjectWriteInfo write_info_1 = gen_one_object_write_info(buffer_1, STRLEN(buffer_1));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_1));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_1));

  // IO 2 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  MacroBlockId file_id_2 = inc_file_id_seq(file_id_1);
  const char *buffer_2 = "bbbb";
  ObStorageObjectWriteInfo write_info_2 = gen_one_object_write_info(buffer_2, STRLEN(buffer_2));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_2));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_2));

  // IO 3 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  MacroBlockId file_id_3 = inc_file_id_seq(file_id_2);
  const char *buffer_3 = "cccccc";
  ObStorageObjectWriteInfo write_info_3 = gen_one_object_write_info(buffer_3, STRLEN(buffer_3));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_3));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_3));

  // batch write 3 table object io
  ObTieredMetadataBatchHandle batch_hd;
  ASSERT_EQ(OB_SUCCESS, OB_TIERED_METADATA_OBJECT_MGR.batch_write_object(batch_macro_id, batch_write_info, batch_hd));


  // read and verify
  const int64_t read_buf_size = 1024;
  char read_buf[read_buf_size];
  READ_AND_TEST(1)
  READ_AND_TEST(2)
  READ_AND_TEST(3)

  // verify list
  ObArray<MacroBlockId> macro_ids;
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->list_meta_macro_files(false/*is_ls_inner_tablet*/,
                                                                     0/*ls_id*/,
                                                                     tablet_id,
                                                                     0/*reorganization_scn*/,
                                                                     macro_ids)
  );
  ASSERT_EQ(3, macro_ids.count());
  ASSERT_EQ(file_id_1, macro_ids.at(0));
  ASSERT_EQ(file_id_2, macro_ids.at(1));
  ASSERT_EQ(file_id_3, macro_ids.at(2));

  // verify delete
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->batch_del_meta(batch_macro_id));
  macro_ids.reset();
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->list_meta_macro_files(false/*is_ls_inner_tablet*/,
                                                                     0/*ls_id*/,
                                                                     tablet_id,
                                                                     0/*reorganization_scn*/,
                                                                     macro_ids)
  );
  ASSERT_EQ(0, macro_ids.count());
}

TEST_F(ObTieredMetadataIOTest, test_tiered_metadata_io_2)
{
  // Three SHARED_TABLET_SUB_META object IO
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));
  int ret = OB_SUCCESS;

  // IO 1 : SHARED_TABLET_SUB_META object IO
  ObArray<MacroBlockId> batch_macro_id;
  ObArray<ObStorageObjectWriteInfo> batch_write_info;
  const int64_t tablet_id = 200002;

  MacroBlockId file_id_1 = build_tiered_metadata_file_id(ObStorageObjectType::SHARED_TABLET_SUB_META,
                                                         tablet_id,
                                                         1);
  const char *buffer_1 = "aaa";
  ObStorageObjectWriteInfo write_info_1 = gen_one_object_write_info(buffer_1, STRLEN(buffer_1));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_1));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_1));

  // IO 2 : SHARED_TABLET_SUB_META object IO
  MacroBlockId file_id_2 = inc_file_id_seq(file_id_1);
  const char *buffer_2 = "bbbb";
  ObStorageObjectWriteInfo write_info_2 = gen_one_object_write_info(buffer_2, STRLEN(buffer_2));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_2));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_2));

  // IO 3 : SHARED_TABLET_SUB_META object IO
  MacroBlockId file_id_3 = inc_file_id_seq(file_id_2);
  const char *buffer_3 = "cccccc";
  ObStorageObjectWriteInfo write_info_3 = gen_one_object_write_info(buffer_3, STRLEN(buffer_3));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_3));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_3));

  // batch write 3 ss object io
  ObTieredMetadataBatchHandle batch_hd;
  ASSERT_EQ(OB_SUCCESS, OB_TIERED_METADATA_OBJECT_MGR.batch_write_object(batch_macro_id, batch_write_info, batch_hd));


  // read and verify
  const int64_t read_buf_size = 1024;
  char read_buf[read_buf_size];
  READ_AND_TEST(1)
  READ_AND_TEST(2)
  READ_AND_TEST(3)
}

TEST_F(ObTieredMetadataIOTest, test_tiered_metadata_io_3)
{
  // Four tiered metadata mix IO
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));
  int ret = OB_SUCCESS;

  // IO 1 : SHARED_TABLET_SUB_META object IO
  ObArray<MacroBlockId> batch_macro_id;
  ObArray<ObStorageObjectWriteInfo> batch_write_info;
  const int64_t tablet_id = 200003;

  MacroBlockId file_id_1 = build_tiered_metadata_file_id(ObStorageObjectType::SHARED_TABLET_SUB_META,
                                                         tablet_id,
                                                         1);
  const char *buffer_1 = "aaa";
  ObStorageObjectWriteInfo write_info_1 = gen_one_object_write_info(buffer_1, STRLEN(buffer_1));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_1));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_1));

  // IO 2 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  MacroBlockId file_id_2 = inc_file_id_seq(file_id_1);
  set_to_table_tiered_metadata_file_id(file_id_2);
  const char *buffer_2 = "bbbb";
  ObStorageObjectWriteInfo write_info_2 = gen_one_object_write_info(buffer_2, STRLEN(buffer_2));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_2));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_2));

  // IO 3 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  MacroBlockId file_id_3 = inc_file_id_seq(file_id_2);
  const char *buffer_3 = "cccccc";
  ObStorageObjectWriteInfo write_info_3 = gen_one_object_write_info(buffer_3, STRLEN(buffer_3));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_3));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_3));

  // IO 4 : SHARED_TABLET_SUB_META object IO
  MacroBlockId file_id_4 = inc_file_id_seq(file_id_3);
  set_to_ss_tiered_metadata_file_id(file_id_4);
  const char *buffer_4 = "ddddddddd";
  ObStorageObjectWriteInfo write_info_4 = gen_one_object_write_info(buffer_4, STRLEN(buffer_4));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_4));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_4));

  // batch write 4 mix tiered metadata object io
  ObTieredMetadataBatchHandle batch_hd;
  ASSERT_EQ(OB_SUCCESS, OB_TIERED_METADATA_OBJECT_MGR.batch_write_object(batch_macro_id, batch_write_info, batch_hd));


  // read and verify
  const int64_t read_buf_size = 1024;
  char read_buf[read_buf_size];
  READ_AND_TEST(1)
  READ_AND_TEST(2)
  READ_AND_TEST(3)
  READ_AND_TEST(4)
}

TEST_F(ObTieredMetadataIOTest, test_tiered_metadata_io_inner_tablet)
{
  // Three SHARED_TABLET_SUB_META_IN_TABLE object IO
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));
  int ret = OB_SUCCESS;

  ObTieredMetadataStore *tiered_metadata_store = MTL(ObTieredMetadataStore *);
  ASSERT_NE(nullptr, tiered_metadata_store);

  // IO 1 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  ObArray<MacroBlockId> batch_macro_id;
  ObArray<ObStorageObjectWriteInfo> batch_write_info;
  const int64_t tablet_id = ObTabletID::LS_TX_DATA_TABLET_ID;
  const int64_t ls_id = 2001;

  MacroBlockId file_id_1 = build_tiered_metadata_file_id(ObStorageObjectType::SHARED_TABLET_SUB_META_IN_TABLE,
                                                         tablet_id,
                                                         1,
                                                         ls_id);
  const char *buffer_1 = "aaa";
  ObStorageObjectWriteInfo write_info_1 = gen_one_object_write_info(buffer_1, STRLEN(buffer_1));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_1));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_1));

  // IO 2 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  MacroBlockId file_id_2 = inc_file_id_seq(file_id_1);
  const char *buffer_2 = "bbbb";
  ObStorageObjectWriteInfo write_info_2 = gen_one_object_write_info(buffer_2, STRLEN(buffer_2));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_2));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_2));

  // IO 3 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  MacroBlockId file_id_3 = inc_file_id_seq(file_id_2);
  const char *buffer_3 = "cccccc";
  ObStorageObjectWriteInfo write_info_3 = gen_one_object_write_info(buffer_3, STRLEN(buffer_3));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_3));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_3));

  // batch write 3 table object io
  ObTieredMetadataBatchHandle batch_hd;
  ASSERT_EQ(OB_SUCCESS, OB_TIERED_METADATA_OBJECT_MGR.batch_write_object(batch_macro_id, batch_write_info, batch_hd));


  // read and verify
  const int64_t read_buf_size = 1024;
  char read_buf[read_buf_size];
  READ_AND_TEST(1)
  READ_AND_TEST(2)
  READ_AND_TEST(3)

  // verify list
  ObArray<MacroBlockId> macro_ids;
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->list_meta_macro_files(true/*is_ls_inner_tablet*/,
                                                                     ls_id,
                                                                     tablet_id,
                                                                     0/*reorganization_scn*/,
                                                                     macro_ids)
  );
  ASSERT_EQ(3, macro_ids.count());
  ASSERT_EQ(file_id_1, macro_ids.at(0));
  ASSERT_EQ(file_id_2, macro_ids.at(1));
  ASSERT_EQ(file_id_3, macro_ids.at(2));

  // verify range read
  ObTieredMetadataIterator iterator;
  ASSERT_EQ(OB_INVALID_ARGUMENT, tiered_metadata_store->get_tiered_metadata_iter(file_id_3, file_id_1, iterator));
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->get_tiered_metadata_iter(file_id_1, file_id_1, iterator));
  ITERATOR_AND_TEST(1)
  ITERATOR_END_AND_TEST()

  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->get_tiered_metadata_iter(file_id_1, file_id_3, iterator));
  ITERATOR_AND_TEST(1)
  ITERATOR_AND_TEST(2)
  ITERATOR_AND_TEST(3)
  ITERATOR_END_AND_TEST()

  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->get_tiered_metadata_iter(file_id_2, file_id_3, iterator));
  ITERATOR_AND_TEST(2)
  ITERATOR_AND_TEST(3)
  ITERATOR_END_AND_TEST()


  // verify delete
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->del_meta(file_id_2));
  macro_ids.reset();
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->list_meta_macro_files(true/*is_ls_inner_tablet*/,
                                                                     ls_id,
                                                                     tablet_id,
                                                                     0/*reorganization_scn*/,
                                                                     macro_ids)
  );
  ASSERT_EQ(2, macro_ids.count());
  ASSERT_EQ(file_id_1, macro_ids.at(0));
  ASSERT_EQ(file_id_3, macro_ids.at(1));
}

TEST_F(ObTieredMetadataIOTest, test_tiered_big_metadata_io)
{
  // three SHARED_TABLET_SUB_META_IN_TABLE object IO, each size is 800KB.
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));
  int ret = OB_SUCCESS;

  ObTieredMetadataStore *tiered_metadata_store = MTL(ObTieredMetadataStore *);
  ASSERT_NE(nullptr, tiered_metadata_store);

  // IO 1 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  ObArray<MacroBlockId> batch_macro_id;
  ObArray<ObStorageObjectWriteInfo> batch_write_info;
  const int64_t tablet_id = 200004;

  MacroBlockId file_id_1 = build_tiered_metadata_file_id(ObStorageObjectType::SHARED_TABLET_SUB_META_IN_TABLE,
                                                         tablet_id,
                                                         1);
  const int64_t big_io_size = 800 * 1024L;
  char buffer_1[big_io_size] = "aaa";
  buffer_1[big_io_size - 1] = 'a';
  ObStorageObjectWriteInfo write_info_1 = gen_one_object_write_info(buffer_1, big_io_size);
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_1));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_1));

  // IO 2 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  MacroBlockId file_id_2 = inc_file_id_seq(file_id_1);
  char buffer_2[big_io_size] = "bbbb";
  ObStorageObjectWriteInfo write_info_2 = gen_one_object_write_info(buffer_2, big_io_size);
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_2));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_2));

  // IO 3 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  MacroBlockId file_id_3 = inc_file_id_seq(file_id_2);
  char buffer_3[big_io_size] = "cccccc";
  buffer_3[1024] = 'c';
  ObStorageObjectWriteInfo write_info_3 = gen_one_object_write_info(buffer_3, big_io_size);
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_3));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_3));

  // batch write 3 table object io
  ObTieredMetadataBatchHandle batch_hd;
  ASSERT_EQ(OB_SUCCESS, OB_TIERED_METADATA_OBJECT_MGR.batch_write_object(batch_macro_id, batch_write_info, batch_hd));


  // read and verify
  const int64_t read_buf_size = big_io_size;
  char read_buf[read_buf_size];

#define READ_AND_TEST_BIG_IO(ID) \
  ObStorageObjectReadInfo read_info_##ID = gen_one_object_read_info(file_id_##ID, read_buf, read_buf_size); \
  ObStorageObjectHandle read_hd_##ID; \
  ASSERT_EQ(OB_SUCCESS, OB_TIERED_METADATA_OBJECT_MGR.read_object(read_info_##ID, read_hd_##ID)); \
  ASSERT_EQ(read_hd_##ID.get_data_size(), read_buf_size); \
  ASSERT_EQ(0, MEMCMP(read_info_##ID.buf_, buffer_##ID, read_buf_size));


  READ_AND_TEST_BIG_IO(1)
  READ_AND_TEST_BIG_IO(2)
  READ_AND_TEST_BIG_IO(3)


  // verify list
  ObArray<MacroBlockId> macro_ids;
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->list_meta_macro_files(false/*is_ls_inner_tablet*/,
                                                                     0/*ls_id*/,
                                                                     tablet_id,
                                                                     0/*reorganization_scn*/,
                                                                     macro_ids)
  );
  ASSERT_EQ(3, macro_ids.count());
  ASSERT_EQ(file_id_1, macro_ids.at(0));
  ASSERT_EQ(file_id_2, macro_ids.at(1));
  ASSERT_EQ(file_id_3, macro_ids.at(2));

  // verify delete
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->batch_del_meta(batch_macro_id));
  macro_ids.reset();
  ASSERT_EQ(OB_SUCCESS, tiered_metadata_store->list_meta_macro_files(false/*is_ls_inner_tablet*/,
                                                                     0/*ls_id*/,
                                                                     tablet_id,
                                                                     0/*reorganization_scn*/,
                                                                     macro_ids)
  );
  ASSERT_EQ(0, macro_ids.count());
}

TEST_F(ObTieredMetadataIOTest, test_repeat_insert_same_key)
{
  // Three SHARED_TABLET_SUB_META_IN_TABLE object IO
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));
  int ret = OB_SUCCESS;

  // IO 1 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  ObArray<MacroBlockId> batch_macro_id;
  ObArray<ObStorageObjectWriteInfo> batch_write_info;
  const int64_t tablet_id = 200005;

  MacroBlockId file_id_1 = build_tiered_metadata_file_id(ObStorageObjectType::SHARED_TABLET_SUB_META_IN_TABLE,
                                                         tablet_id,
                                                         1);
  const char *buffer_1 = "aaa";
  ObStorageObjectWriteInfo write_info_1 = gen_one_object_write_info(buffer_1, STRLEN(buffer_1));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_1));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_1));

  // IO 2 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  MacroBlockId file_id_2 = inc_file_id_seq(file_id_1);
  const char *buffer_2 = "bbbb";
  ObStorageObjectWriteInfo write_info_2 = gen_one_object_write_info(buffer_2, STRLEN(buffer_2));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_2));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_2));

  // IO 3 : SHARED_TABLET_SUB_META_IN_TABLE object IO
  MacroBlockId file_id_3 = inc_file_id_seq(file_id_2);
  const char *buffer_3 = "cccccc";
  ObStorageObjectWriteInfo write_info_3 = gen_one_object_write_info(buffer_3, STRLEN(buffer_3));
  ASSERT_EQ(OB_SUCCESS, batch_macro_id.push_back(file_id_3));
  ASSERT_EQ(OB_SUCCESS, batch_write_info.push_back(write_info_3));

  // batch write 3 ss object io
  ObTieredMetadataBatchHandle batch_hd;
  ASSERT_EQ(OB_SUCCESS, OB_TIERED_METADATA_OBJECT_MGR.batch_write_object(batch_macro_id, batch_write_info, batch_hd));

  // rewrite file_id_1, with same value
  const char *rewrite_buffer_1 = "aaa";
  ObStorageObjectWriteInfo rewrite_info_1 = gen_one_object_write_info(rewrite_buffer_1, STRLEN(rewrite_buffer_1));
  ObStorageObjectHandle rewrite_hd_1;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.async_write_object(file_id_1, rewrite_info_1, rewrite_hd_1));
  ASSERT_EQ(OB_SUCCESS, rewrite_hd_1.wait());

  // rewrite file_id_2, with shorter value
  const char *rewrite_buffer_2 = "bbb";
  ObStorageObjectWriteInfo rewrite_info_2 = gen_one_object_write_info(rewrite_buffer_2, STRLEN(rewrite_buffer_2));
  ObStorageObjectHandle rewrite_hd_2;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.async_write_object(file_id_2, rewrite_info_2, rewrite_hd_2));
  ASSERT_EQ(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH, rewrite_hd_2.wait());

  // rewrite file_id_3, with longer value
  const char *rewrite_buffer_3 = "ccccccc";
  ObStorageObjectWriteInfo rewrite_info_3 = gen_one_object_write_info(rewrite_buffer_3, STRLEN(rewrite_buffer_3));
  ObStorageObjectHandle rewrite_hd_3;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.async_write_object(file_id_3, rewrite_info_3, rewrite_hd_3));
  ASSERT_EQ(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH, rewrite_hd_3.wait());

  // read and verify
  const int64_t read_buf_size = 1024;
  char read_buf[read_buf_size];
  READ_AND_TEST(1)
  READ_AND_TEST(2)
  READ_AND_TEST(3)
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
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
