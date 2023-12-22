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
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tx/ob_trans_define.h"

using namespace oceanbase::common;

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{
class TestTabletStatusCache : public::testing::Test
{
public:
  TestTabletStatusCache();
  virtual ~TestTabletStatusCache() = default;
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

TestTabletStatusCache::TestTabletStatusCache()
  : allocator_()
{
}

const share::ObLSID TestTabletStatusCache::LS_ID(1001);

void TestTabletStatusCache::SetUpTestCase()
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

void TestTabletStatusCache::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

void TestTabletStatusCache::SetUp()
{
}

void TestTabletStatusCache::TearDown()
{
}

int TestTabletStatusCache::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int TestTabletStatusCache::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id);
  return ret;
}

int TestTabletStatusCache::create_tablet(
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

TEST_F(TestTabletStatusCache, weak_read)
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
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;

  mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(111)));
  ret = tablet->set(user_data, ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  share::SCN commit_scn = share::SCN::plus(min_scn, 50);
  ctx.single_log_commit(commit_scn, commit_scn);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  const ObTabletMapKey key(LS_ID, tablet_id);
  tablet_handle.reset();
  // mode is READ_READABLE_COMMITED, snapshot version is smaller than create commit version, return OB_SNAPSHOT_DISCARDED
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 20/*snapshot*/);
  ASSERT_EQ(OB_SNAPSHOT_DISCARDED, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, but snapshot is not max scn, not supported
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, 20/*snapshot*/);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_READABLE_COMMITED, snapshot version is bigger than create commit version, return OB_SUCCESS
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 60/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tablet->tablet_status_cache_.is_valid());
}

TEST_F(TestTabletStatusCache, get_transfer_out_tablet)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // set transfer scn
  // create commit scn: 50
  // transfer scn: 100
  ObTabletCreateDeleteMdsUserData user_data;
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
  share::SCN min_scn;
  min_scn.set_min();
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 100);
  user_data.transfer_ls_id_ = share::ObLSID(1010);
  user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;

  mds::MdsCtx ctx1(mds::MdsWriter(transaction::ObTransID(123)));
  ret = tablet->set(user_data, ctx1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  const ObTabletMapKey key(LS_ID, tablet_id);
  tablet_handle.reset();
  // mode is READ_READABLE_COMMITED, can not get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  //ASSERT_EQ(OB_SCHEMA_EAGAIN, ret);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, allow to get TRANSFER_OUT status tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  tablet_handle.reset();
  // read snapshot: 80. not max scn, return OB_NOT_SUPPORTED
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, 80/*snapshot*/);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // read snapshot: 80. less than transfer scn
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // check tablet status cache
  {
    SpinRLockGuard guard(tablet->mds_cache_lock_);
    const ObTabletStatusCache &tablet_status_cache = tablet->tablet_status_cache_;

    // we won't update tablet status cache if current status is TRANSFER_OUT,
    // so here the cache remains invalid
    ASSERT_EQ(ObTabletStatus::MAX, tablet_status_cache.tablet_status_);
  }

  // let transaction commit
  // commit scn: 120
  share::SCN commit_scn = share::SCN::plus(min_scn, 120);
  ctx1.single_log_commit(commit_scn, commit_scn);

  tablet_handle.reset();
  // mode is READ_READABLE_COMMITED, read snapshot is max scn, greater than transfer scn 100, not allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  tablet_handle.reset();
  // mode is READ_READABLE_COMMITED, read snapshot 80 less than transfer scn 100, allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 80/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, snapshot is not max scn, return OB_NOT_SUPPORTED
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, 80/*snapshot*/);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // begin transfer out deleted transaction
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT_DELETED;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 100);
  user_data.transfer_ls_id_ = share::ObLSID(1010);
  user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_OUT;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.delete_commit_scn_ = share::SCN::plus(min_scn, 200);
  user_data.delete_commit_version_ = 200;

  mds::MdsCtx ctx2(mds::MdsWriter(transaction::ObTransID(456)));
  ret = tablet->set(user_data, ctx2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  // mode is READ_READABLE_COMMITED, snpashot status is transfer out, not allow to get
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, allow to get tablet whose snapshot status is transfer out
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // transaction commit
  commit_scn = share::SCN::plus(min_scn, 220);
  ctx2.single_log_commit(commit_scn, commit_scn);

  // mode is READ_READABLE_COMMITED, snapshot status is transfer out deleted, not allow to get
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, snapshot status is transfer out deleted, not allow to get
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // begin transfer out deleted transaction
  user_data.tablet_status_ = ObTabletStatus::DELETED;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 100);
  user_data.transfer_ls_id_ = share::ObLSID(1010);
  user_data.data_type_ = ObTabletMdsUserDataType::REMOVE_TABLET;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.delete_commit_scn_ = share::SCN::plus(min_scn, 200);
  user_data.delete_commit_version_ = 200;

  mds::MdsCtx ctx3(mds::MdsWriter(transaction::ObTransID(789)));
  ret = tablet->set(user_data, ctx3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  ObLSHandle ls_handle;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  ret = ls->get_tablet_svr()->update_tablet_to_empty_shell(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
}

TEST_F(TestTabletStatusCache, get_transfer_deleted)
{
  int ret = OB_SUCCESS;
  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  ObLSHandle ls_handle;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN min_scn;
  min_scn.set_min();
  // set transfer scn
  // create commit scn: 50
  // transfer scn: 100
  // delete commit scn: 200
  // begin deleted transaction
  user_data.tablet_status_ = ObTabletStatus::DELETED;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 100);
  user_data.transfer_ls_id_ = share::ObLSID(1010);
  user_data.data_type_ = ObTabletMdsUserDataType::REMOVE_TABLET;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.delete_commit_scn_ = share::SCN::plus(min_scn, 200);
  user_data.delete_commit_version_ = 200;

  mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(789)));
  ret = tablet->set(user_data, ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  share::SCN commit_scn = share::SCN::plus(min_scn, 120);
  ctx.single_log_commit(commit_scn, commit_scn);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  const ObTabletMapKey key(LS_ID, tablet_id);
  tablet_handle.reset();
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  ret = ls->get_tablet_svr()->update_tablet_to_empty_shell(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());
}

TEST_F(TestTabletStatusCache, get_transfer_out_deleted)
{
  int ret = OB_SUCCESS;
  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  ObLSHandle ls_handle;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  const ObTabletMapKey key(LS_ID, tablet_id);
  tablet_handle.reset();
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tablet->tablet_status_cache_.is_valid());

  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN min_scn;
  min_scn.set_min();
  // set transfer scn
  // create commit scn: 50
  // transfer scn: 100
  // delete commit scn: 200
  // begin transfer out deleted transaction
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT_DELETED;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 100);
  user_data.transfer_ls_id_ = share::ObLSID(1010);
  user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_OUT;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.transfer_out_commit_version_ = 200;

  mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(789)));
  ret = tablet->set(user_data, ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  share::SCN commit_scn = share::SCN::plus(min_scn, 200);
  ctx.single_log_commit(commit_scn, commit_scn);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  // mock weak read
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 210/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 90/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, INT64_MAX/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());
}

TEST_F(TestTabletStatusCache, get_empty_result_tablet)
{
  int ret = OB_SUCCESS;

  // create tablet without any tablet status
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  const ObTabletMapKey key(LS_ID, tablet_id);
  ObTabletHandle tablet_handle;
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
      ObTabletStatus::Status::MAX, share::SCN::min_scn()))) {
    LOG_WARN("failed to create tablet", K(ret), K(ls->get_ls_id()), K(tablet_id));
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // test READ_WITHOUT_CHECK
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_WITHOUT_CHECK, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(tablet_handle.get_obj()->tablet_status_cache_.is_valid());

  // test READ_READABLE_COMMITED
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_FALSE(tablet_handle.get_obj()->tablet_status_cache_.is_valid());

  // test READ_ALL_COMMITED
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_FALSE(tablet_handle.get_obj()->tablet_status_cache_.is_valid());
}

TEST_F(TestTabletStatusCache, get_read_all_committed_tablet)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  const ObTabletMapKey key(LS_ID, tablet_id);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::MAX, share::SCN::invalid_scn());
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN min_scn;
  min_scn.set_min();
  share::SCN commit_scn;

  // start transfer in not commited
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_IN;
  user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_IN;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 90);

  mds::MdsCtx ctx3(mds::MdsWriter(transaction::ObTransID(2023062803)));
  ret = tablet->set(user_data, ctx3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // start transfer in commited
  commit_scn = share::SCN::plus(min_scn, 300);
  ctx3.single_log_commit(commit_scn, commit_scn);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_READABLE_COMMITED, 10/*snapshot*/);
  ASSERT_EQ(OB_SNAPSHOT_DISCARDED, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // finish transfer in not commited
  user_data.tablet_status_ = ObTabletStatus::NORMAL;
  user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_IN;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 90);

  mds::MdsCtx ctx4(mds::MdsWriter(transaction::ObTransID(2023062804)));
  ret = tablet->set(user_data, ctx4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // finish transfer in commited
  commit_scn = share::SCN::plus(min_scn, 400);
  ctx4.single_log_commit(commit_scn, commit_scn);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // start transfer out not commited
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
  user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 90);

  mds::MdsCtx ctx5(mds::MdsWriter(transaction::ObTransID(2023062805)));
  ret = tablet->set(user_data, ctx5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // start transfer out commited
  commit_scn = share::SCN::plus(min_scn, 500);
  ctx5.single_log_commit(commit_scn, commit_scn);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // finish transfer out not commited
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT_DELETED;
  user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_OUT;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  mds::MdsCtx ctx6(mds::MdsWriter(transaction::ObTransID(2023062806)));
  ret = tablet->set(user_data, ctx6);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // finish transfer out commited
  commit_scn = share::SCN::plus(min_scn, 600);
  ctx6.single_log_commit(commit_scn, commit_scn);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());
}

TEST_F(TestTabletStatusCache, read_all_committed)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  const ObTabletMapKey key(LS_ID, tablet_id);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::MAX, share::SCN::invalid_scn());
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  // get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);

  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN min_scn;
  min_scn.set_min();
  share::SCN commit_scn;

  // create not committed
  user_data.tablet_status_ = ObTabletStatus::NORMAL;
  user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

  mds::MdsCtx ctx1(mds::MdsWriter(transaction::ObTransID(123)));
  ret = tablet->set(user_data, ctx1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);

  // commit
  commit_scn = share::SCN::plus(min_scn, 100);
  ctx1.single_log_commit(commit_scn, commit_scn);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
}

// TODO(@bowen.gbw): refactor test cases to cover all scene
/*
TEST_F(TestTabletStatusCache, transfer_src_ls_read_all_committed)
{

}

TEST_F(TestTabletStatusCache, transfer_dst_ls_read_all_committed)
{

}

TEST_F(TestTabletStatusCache, transfer_src_ls_read_readable_committed)
{

}

TEST_F(TestTabletStatusCache, transfer_dst_ls_read_readable_committed)
{

}
*/

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_status_cache.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_tablet_status_cache.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
