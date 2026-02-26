// owner: gaishun.gs
// owner group: storage

/**
 * Copyright (c) 2022 OceanBase
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

#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/test_dml_common.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_empty_shell_task.h"

using namespace oceanbase::common;

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{
class TestEmptyShellHandler : public::testing::Test
{
public:
  TestEmptyShellHandler();
  virtual ~TestEmptyShellHandler() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
public:
  static int create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static int remove_ls(const share::ObLSID &ls_id);
  int create_tablet(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &tablet_handle,
      const ObTabletStatus::Status &tablet_status = ObTabletStatus::NORMAL,
      const share::SCN &create_commit_scn = share::SCN::min_scn());
public:
  static constexpr uint64_t TENANT_ID = 1001;
  static const share::ObLSID LS_ID;

  common::ObArenaAllocator allocator_;
};

const share::ObLSID TestEmptyShellHandler::LS_ID(1001);

const int64_t checkpoint::ObEmptyShellTask::GC_EMPTY_TABLET_SHELL_INTERVAL = 3600 * 1000 * 1000L; // 3600s, 1h

TestEmptyShellHandler::TestEmptyShellHandler()
  : allocator_()
{
}

void TestEmptyShellHandler::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  SERVER_STORAGE_META_SERVICE.is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = create_ls(TENANT_ID, LS_ID, ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestEmptyShellHandler::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

void TestEmptyShellHandler::SetUp()
{
}

void TestEmptyShellHandler::TearDown()
{
}


int TestEmptyShellHandler::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int TestEmptyShellHandler::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id);
  return ret;
}

int TestEmptyShellHandler::create_tablet(
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle,
    const ObTabletStatus::Status &tablet_status,
    const share::SCN &create_commit_scn)
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
  } else if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_,
      tablet_status, create_commit_scn, tablet_handle))) {
    LOG_WARN("failed to create tablet", K(ret), K(LS_ID), K(tablet_id), K(create_commit_scn));
  } else if (OB_FAIL(tablet_handle.get_obj()->inner_get_mds_table(mds_table, true/*not_exist_create*/))) {
    LOG_WARN("failed to get mds table", K(ret));
  }

  return ret;
}

TEST_F(TestEmptyShellHandler, create_tx_abort_without_abort_scn)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  // create commit scn: 50
  share::SCN min_scn;
  min_scn.set_min();
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::NORMAL, share::SCN::plus(min_scn, 50));
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  ObTabletCreateDeleteMdsUserData user_data;
  user_data.tablet_status_ = ObTabletStatus::NORMAL;
  user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

  // ctx destructed, mock tx abort without abort scn
  {
    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(111)));
    ret = tablet->set(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObLSHandle ls_handle;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  checkpoint::ObTabletEmptyShellHandler *handler = ls->get_tablet_empty_shell_handler();
  ASSERT_NE(nullptr, handler);

  bool can_become_shell = false;
  bool need_retry = false;
  ret = handler->check_tablet_from_aborted_tx_(*tablet, can_become_shell, need_retry);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!can_become_shell);
  share::SCN rec_scn(share::SCN::base_scn());
  ret = tablet->get_mds_table_rec_scn(rec_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(rec_scn.is_max());
}

TEST_F(TestEmptyShellHandler, create_tx_abort_with_abort_scn)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  // create commit scn: 50
  share::SCN min_scn;
  min_scn.set_min();
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::NORMAL, share::SCN::plus(min_scn, 50));
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  ObTabletCreateDeleteMdsUserData user_data;
  user_data.tablet_status_ = ObTabletStatus::NORMAL;
  user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

  mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(111)));
  ret = tablet->set(user_data, ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ctx.on_redo(share::SCN::plus(min_scn, 50));
  ctx.on_abort(share::SCN::plus(min_scn, 60));

  ObLSHandle ls_handle;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  checkpoint::ObTabletEmptyShellHandler *handler = ls->get_tablet_empty_shell_handler();
  ASSERT_NE(nullptr, handler);

  bool can_become_shell = false;
  bool need_retry = false;
  ret = handler->check_tablet_from_aborted_tx_(*tablet, can_become_shell, need_retry);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(can_become_shell);
  share::SCN rec_scn(share::SCN::max_scn());
  ret = tablet->get_mds_table_rec_scn(rec_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(rec_scn.is_max());
}

TEST_F(TestEmptyShellHandler, migrate_transfer_out_deleted_tablet)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  // create commit scn: 50
  share::SCN min_scn;
  min_scn.set_min();
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::TRANSFER_OUT_DELETED, share::SCN::plus(min_scn, 50));
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  ObLSHandle ls_handle;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  checkpoint::ObTabletEmptyShellHandler *handler = ls->get_tablet_empty_shell_handler();
  ASSERT_NE(nullptr, handler);

  bool is_written = false;
  ret = tablet->check_tablet_status_written(is_written);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_written);

  bool can_become_shell = false;
  bool need_retry = false;
  ret = handler->check_candidate_tablet_(*tablet, can_become_shell, need_retry);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(can_become_shell);
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_empty_shell.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_empty_shell.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
