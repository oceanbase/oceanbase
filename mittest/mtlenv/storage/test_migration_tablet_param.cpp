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

#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include "share/rc/ob_tenant_base.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "mtlenv/storage/medium_info_helper.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/test_dml_common.h"
#include "storage/tablet/ob_tablet.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

namespace oceanbase
{
int ObClusterVersion::get_tenant_data_version(const uint64_t tenant_id, uint64_t &data_version)
{
  data_version = DATA_VERSION_4_3_2_0;
  return OB_SUCCESS;
}
namespace unittest
{
class TestMigrationTabletParam : public ::testing::Test
{
public:
  TestMigrationTabletParam() = default;
  virtual ~TestMigrationTabletParam() = default;
public:
  static constexpr uint64_t TENANT_ID = 1001;
  static const share::ObLSID LS_ID;
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static int create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static int remove_ls(const share::ObLSID &ls_id);
  int create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  static int insert_data_into_mds_data(ObArenaAllocator &allocator, mds::MdsDumpKV &kv);
  static bool is_user_data_equal(const ObTabletCreateDeleteMdsUserData &left, const ObTabletCreateDeleteMdsUserData &right);
public:
  ObArenaAllocator allocator_;
};

const share::ObLSID TestMigrationTabletParam::LS_ID(1234);

void TestMigrationTabletParam::SetUpTestCase()
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

void TestMigrationTabletParam::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

int TestMigrationTabletParam::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int TestMigrationTabletParam::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id);
  return ret;
}

int TestMigrationTabletParam::create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = 1234567;
  share::schema::ObTableSchema table_schema;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(build_test_schema(table_schema, table_id))) {
    LOG_WARN("failed to build table schema");
  } else if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_,
      ObTabletStatus::MAX, share::SCN::invalid_scn(), tablet_handle))) {
    LOG_WARN("failed to create tablet", K(ret));
  }

  return ret;
}

int TestMigrationTabletParam::insert_data_into_mds_data(ObArenaAllocator &allocator, mds::MdsDumpKV &kv)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  user_data.tablet_status_ = ObTabletStatus::NORMAL;
  const int64_t serialize_size = user_data.get_serialize_size();
  char *buffer = static_cast<char *>(allocator.alloc(serialize_size));
  int64_t pos = 0;

  if (OB_ISNULL(buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(serialize_size));
  } else if (OB_FAIL(user_data.serialize(buffer, serialize_size, pos))) {
    LOG_WARN("fail to serialize user data", K(ret));
  } else {
    kv.v_.user_data_.assign(buffer, serialize_size);
  }

  return ret;
}

bool TestMigrationTabletParam::is_user_data_equal(const ObTabletCreateDeleteMdsUserData &left, const ObTabletCreateDeleteMdsUserData &right)
{
  const bool res = left.tablet_status_ == right.tablet_status_ && left.transfer_scn_ == right.transfer_scn_
      && left.transfer_ls_id_ == right.transfer_ls_id_ && left.data_type_ == right.data_type_
      && left.create_commit_scn_ == right.create_commit_scn_
      && left.create_commit_version_ == right.create_commit_version_
      && left.delete_commit_version_ == right.delete_commit_version_
      && left.transfer_out_commit_version_ == right.transfer_out_commit_version_;
  return res;
}

TEST_F(TestMigrationTabletParam, migration)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // write data to mds table
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 50);
    ctx.on_redo(redo_scn);
    ctx.on_commit(create_commit_scn, create_commit_scn);
  }

  // {
  //   // build migration param v2
  //   ObMigrationTabletParam param_v2;
  //   ret = tablet->build_migration_tablet_param(param_v2);
  //   ASSERT_EQ(OB_NOT_SUPPORTED, ret);

  //   // insert some data into migration param
  //   ret = insert_data_into_mds_data(allocator_, param_v2.mds_data_.tablet_status_committed_kv_);
  //   ASSERT_EQ(OB_SUCCESS, ret);

  //   param_v2.version_ = ObMigrationTabletParam::PARAM_VERSION_V2;
  //   int64_t serialize_size = param_v2.get_serialize_size();
  //   char *buffer = new char[serialize_size]();
  //   int64_t pos = 0;
  //   ret = param_v2.serialize(buffer, serialize_size, pos);
  //   ASSERT_EQ(OB_SUCCESS, ret);
  //   ASSERT_EQ(serialize_size, pos);

  //   // param v2 deserialize
  //   ObMigrationTabletParam param2;
  //   pos = 0;
  //   ret = param2.deserialize(buffer, serialize_size, pos);
  //   ASSERT_EQ(OB_SUCCESS, ret);
  //   ASSERT_EQ(pos, serialize_size);
  //   ASSERT_TRUE(param2.is_valid());

  //   delete [] buffer;
  // }

  {
    // build migration param v3
    ObMigrationTabletParam param_v3;
    ret = tablet->build_migration_tablet_param(param_v3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(true, param_v3.version_ == ObMigrationTabletParam::PARAM_VERSION_V3);

    // insert some data into migration param
    ret = insert_data_into_mds_data(allocator_, param_v3.mds_data_.tablet_status_committed_kv_);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t serialize_size = param_v3.get_serialize_size();
    char *buffer = new char[serialize_size]();
    int64_t pos = 0;
    ret = param_v3.serialize(buffer, serialize_size, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(serialize_size, pos);

    // param v3 deserialize
    ObMigrationTabletParam param2;
    pos = 0;
    ret = param2.deserialize(buffer, serialize_size, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(pos, serialize_size);
    ASSERT_TRUE(param2.is_valid());

    delete [] buffer;
  }
}

TEST_F(TestMigrationTabletParam, transfer)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // write data to mds table
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
    user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
    user_data.transfer_scn_ = share::SCN::plus(share::SCN::min_scn(), 60);
    share::SCN transfer_commit_scn;
    transfer_commit_scn = share::SCN::plus(share::SCN::min_scn(), 70);

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = user_data.transfer_scn_;
    ctx.on_redo(redo_scn);
    ctx.on_commit(transfer_commit_scn, transfer_commit_scn);
  }

  // write medium info
  {
    compaction::ObMediumCompactionInfoKey key(100);
    compaction::ObMediumCompactionInfo info;
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 100);
    ASSERT_EQ(OB_SUCCESS, ret);

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(777)));
    ret = tablet->set(key, info, ctx, 1_s/*lock_timeout_us*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 110);
    ctx.on_redo(redo_scn);
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 120);
    ctx.on_commit(commit_scn, commit_scn);
  }

  {
    // build transfer tablet param v3
    ObMigrationTabletParam param_v3;
    ASSERT_EQ(false, param_v3.is_valid());
    share::ObLSID dst_ls_id(1001);
    const int64_t data_version = DATA_VERSION_4_3_2_0;
    ret = tablet->build_transfer_tablet_param(data_version, dst_ls_id, param_v3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(true, param_v3.is_valid());
    ASSERT_EQ(true, param_v3.version_ == ObMigrationTabletParam::PARAM_VERSION_V3);
    ASSERT_EQ(param_v3.last_persisted_committed_tablet_status_.data_type_, ObTabletMdsUserDataType::START_TRANSFER_IN);
    ASSERT_EQ(param_v3.last_persisted_committed_tablet_status_.tablet_status_, ObTabletStatus::TRANSFER_IN);

    int64_t serialize_size = param_v3.get_serialize_size();
    char *buffer = new char[serialize_size]();
    int64_t pos = 0;
    ret = param_v3.serialize(buffer, serialize_size, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(serialize_size, pos);

    // param v3 deserialize
    ObMigrationTabletParam param2;
    pos = 0;
    ret = param2.deserialize(buffer, serialize_size, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(pos, serialize_size);
    ASSERT_TRUE(param2.is_valid());

    ASSERT_EQ(true, is_user_data_equal(param_v3.last_persisted_committed_tablet_status_, param2.last_persisted_committed_tablet_status_));
    ObMigrationTabletParam test_param;
    test_param.assign(param_v3);
    ASSERT_EQ(true, is_user_data_equal(param_v3.last_persisted_committed_tablet_status_, test_param.last_persisted_committed_tablet_status_));
    delete [] buffer;
  }
}

TEST_F(TestMigrationTabletParam, empty_shell_transfer)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  share::schema::ObTableSchema schema;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSTabletService *ls_tablet_service  = nullptr;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else {
    ls_tablet_service = ls->get_tablet_svr();
  }
  ASSERT_NE(nullptr, ls_tablet_service);


  TestSchemaUtils::prepare_data_schema(schema);
  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_, ObTabletStatus::Status::DELETED);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ls_tablet_service->update_tablet_to_empty_shell(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle empty_shell_tablet_handle;

  ret = ls_tablet_service->get_tablet(tablet_id, empty_shell_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *empty_shell_tablet = empty_shell_tablet_handle.get_obj();
  ASSERT_NE(nullptr, empty_shell_tablet);


  {
    // build transfer tablet param v3
    ObMigrationTabletParam param_v3;
    ASSERT_EQ(false, param_v3.is_valid());
    share::ObLSID dst_ls_id(1001);
    const int64_t data_version = DATA_VERSION_4_3_2_0;
    ret = empty_shell_tablet->build_transfer_tablet_param(data_version, dst_ls_id, param_v3);
    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  }
  {
    // build migration tablet param v3
    ObMigrationTabletParam param_v3;
    ASSERT_EQ(false, param_v3.is_valid());
    ASSERT_EQ(false, param_v3.is_valid());
    ret = empty_shell_tablet->build_migration_tablet_param(param_v3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(true, param_v3.is_valid());
    ASSERT_EQ(true, param_v3.version_ == ObMigrationTabletParam::PARAM_VERSION_V3);

    ObMigrationTabletParam test_param;
    test_param.assign(param_v3);
    ASSERT_EQ(true, is_user_data_equal(param_v3.last_persisted_committed_tablet_status_, test_param.last_persisted_committed_tablet_status_));
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_migration_tablet_param.log*");
  OB_LOGGER.set_file_name("test_migration_tablet_param.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
