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

#define USING_LOG_PREFIX STORAGE

#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/test_dml_common.h"
#include "unittest/storage/schema_utils.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
class TestTabletMemberLoadAndFree : public ::testing::Test
{
public:
  TestTabletMemberLoadAndFree();
  virtual ~TestTabletMemberLoadAndFree() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static int create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static int remove_ls(const share::ObLSID &ls_id);
  int create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
public:
  static const uint64_t TENANT_ID = 1;
  static const share::ObLSID LS_ID;

  common::ObArenaAllocator allocator_;
};

const share::ObLSID TestTabletMemberLoadAndFree::LS_ID(1001);

TestTabletMemberLoadAndFree::TestTabletMemberLoadAndFree()
  : allocator_()
{
}

void TestTabletMemberLoadAndFree::SetUp()
{
}

void TestTabletMemberLoadAndFree::TearDown()
{
}

void TestTabletMemberLoadAndFree::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = create_ls(TENANT_ID, LS_ID, ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTabletMemberLoadAndFree::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

int TestTabletMemberLoadAndFree::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int TestTabletMemberLoadAndFree::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id, false);
  return ret;
}

int TestTabletMemberLoadAndFree::create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = 1234567;
  share::schema::ObTableSchema table_schema;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  mds::MdsTableHandle mds_table;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(build_test_schema(table_schema, table_id))) {
    LOG_WARN("failed to build table schema");
  } else if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_))) {
    LOG_WARN("failed to create tablet", K(ret));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->inner_get_mds_table(mds_table, true/*not_exist_create*/))) {
    LOG_WARN("failed to get mds table", K(ret));
  }

  return ret;
}

TEST_F(TestTabletMemberLoadAndFree, storage_schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_allocator;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // load storage schema, memory type
  ASSERT_TRUE(tablet->storage_schema_addr_.is_memory_object());
  const ObStorageSchema *storage_schema = nullptr;
  ret = tablet->load_storage_schema(arena_allocator, storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, storage_schema);

  ASSERT_NE(0, arena_allocator.used());
  ObTablet::free_storage_schema(arena_allocator, storage_schema);
  arena_allocator.clear();
  ASSERT_EQ(0, arena_allocator.used());

  // tablet persist
  ObTabletHandle new_tablet_handle;
  ret = ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *new_tablet = new_tablet_handle.get_obj();
  ASSERT_NE(nullptr, new_tablet);

  // load storage schema, disk type
  ASSERT_TRUE(new_tablet->storage_schema_addr_.addr_.is_block());
  storage_schema = nullptr;
  ret = new_tablet->load_storage_schema(arena_allocator, storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, storage_schema);

  ASSERT_NE(0, arena_allocator.used());
  ObTablet::free_storage_schema(arena_allocator, storage_schema);
  arena_allocator.clear();
  ASSERT_EQ(0, arena_allocator.used());
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_member_load_and_free.log*");
  OB_LOGGER.set_file_name("test_tablet_member_load_and_free.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
