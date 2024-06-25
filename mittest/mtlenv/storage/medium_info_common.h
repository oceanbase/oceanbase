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

#ifndef OCEANBASE_UNITTEST_MEDIUM_INFO_COMMON
#define OCEANBASE_UNITTEST_MEDIUM_INFO_COMMON

#include <gtest/gtest.h>

#define private public
#define protected public

#include "lib/oblog/ob_log.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "mtlenv/storage/medium_info_helper.h"
#include "share/rc/ob_tenant_base.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/test_dml_common.h"
#include "unittest/storage/init_basic_struct.h"
#include "unittest/storage/schema_utils.h"
#include "storage/multi_data_source/mds_table_handler.h"
#include "storage/multi_data_source/runtime_utility/mds_factory.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{
class MediumInfoCommon : public ::testing::Test
{
public:
  MediumInfoCommon() = default;
  virtual ~MediumInfoCommon() = default;
public:
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static int create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static int remove_ls(const share::ObLSID &ls_id);
  int create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  int insert_medium_info(const int64_t trans_id, const compaction::ObMediumCompactionInfoKey &key);

  static int get_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  int wait_for_mds_table_flush(const common::ObTabletID &tablet_id);
  int wait_for_all_mds_nodes_released(const common::ObTabletID &tablet_id);
public:
  static constexpr uint64_t TENANT_ID = 1001;
  static const share::ObLSID LS_ID;

  mds::MdsTableHandle mds_table_;
  common::ObArenaAllocator allocator_;
};

const share::ObLSID MediumInfoCommon::LS_ID(1234);

void MediumInfoCommon::SetUp()
{
}

void MediumInfoCommon::TearDown()
{
}

void MediumInfoCommon::SetUpTestCase()
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

void MediumInfoCommon::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

int MediumInfoCommon::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int MediumInfoCommon::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id);
  return ret;
}

int MediumInfoCommon::create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = 1234567;
  share::schema::ObTableSchema table_schema;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  mds_table_.reset();

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
  } else if (OB_FAIL(tablet_handle.get_obj()->inner_get_mds_table(mds_table_, true/*not_exist_create*/))) {
    LOG_WARN("failed to get mds table", K(ret));
  }

  return ret;
}

int MediumInfoCommon::insert_medium_info(const int64_t trans_id, const compaction::ObMediumCompactionInfoKey &key)
{
  int ret = OB_SUCCESS;
  mds::MdsCtx ctx{mds::MdsWriter{transaction::ObTransID{trans_id}}};
  compaction::ObMediumCompactionInfo info;
  if (OB_FAIL(MediumInfoHelper::build_medium_compaction_info(allocator_, info, trans_id))) {
    LOG_WARN("fail to build medium info", K(ret), K(trans_id));
  } else if (OB_FAIL(mds_table_.set(key, info, ctx))) {
    LOG_WARN("fail to write data to mds table", K(ret), K(key), K(info));
  } else {
    const share::SCN &scn = mock_scn(trans_id);
    ctx.single_log_commit(scn, scn);
  }

  return ret;
}

int MediumInfoCommon::get_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_tablet_svr()->direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), KP(ls));
  }

  return ret;
}

int MediumInfoCommon::wait_for_mds_table_flush(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  int times = 0;
  share::SCN rec_scn = share::SCN::min_scn();
  do
  {
    ret = mds_table_.get_rec_scn(rec_scn);
    EXPECT_EQ(OB_SUCCESS, ret);

    // sleep
    ::ob_usleep(100_ms);
    ++times;
  } while (OB_SUCCESS == ret && !rec_scn.is_max() && times < 20);
  EXPECT_TRUE(rec_scn.is_max());

  // check mds sstable
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
  EXPECT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  EXPECT_NE(nullptr, tablet);

  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ret = tablet->fetch_table_store(table_store_wrapper);
  EXPECT_EQ(OB_SUCCESS, ret);
  const ObTabletTableStore *table_store = table_store_wrapper.get_member();
  EXPECT_EQ(1, table_store->mds_sstables_.count());

  if (::testing::Test::HasFailure()) {
    ret = OB_TIMEOUT;
  }

  return ret;
}

int MediumInfoCommon::wait_for_all_mds_nodes_released(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int times = 0;
  int64_t node_cnt = INT64_MAX;

  do {
    ret = mds_table_.get_node_cnt(node_cnt);
    EXPECT_EQ(OB_SUCCESS, ret);

    // sleep
    ::ob_usleep(100_ms);
    ++times;
  } while (OB_SUCCESS == ret && node_cnt != 0 && times < 60);
  EXPECT_EQ(0, node_cnt);

  if (::testing::Test::HasFailure()) {
    ret = OB_TIMEOUT;
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_UNITTEST_MEDIUM_INFO_COMMON
