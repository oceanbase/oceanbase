// owner: gaishun.gs
// owner group: storage

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

#define protected public
#define private public

#include "storage/ls/ob_ls.h"
#include "mittest/mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

class TestTabletAggregatedInfo : public TestIndexBlockDataPrepare
{
public:
  TestTabletAggregatedInfo();
  ~TestTabletAggregatedInfo();
};

TestTabletAggregatedInfo::TestTabletAggregatedInfo()
  : TestIndexBlockDataPrepare(
      "Test Tablet Aggregated Info",
      MINI_MERGE,
      OB_DEFAULT_MACRO_BLOCK_SIZE,
      10000,
      65536)
{
}

TestTabletAggregatedInfo::~TestTabletAggregatedInfo()
{
}

TEST_F(TestTabletAggregatedInfo, test_space_usage)
{
  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ObLSID ls_id(ls_id_);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ObTablet *tablet = tablet_handle.get_obj();

  // check tablet's space_usage with empty major sstable
  ObTabletHandle new_tablet_handle;
  const ObTabletPersisterParam param(ls_id, ls_handle.get_ls()->get_ls_epoch(), tablet_id, tablet->get_transfer_seq());
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_and_transform_tablet(param, *tablet, new_tablet_handle));
  ObTabletSpaceUsage space_usage = new_tablet_handle.get_obj()->tablet_meta_.space_usage_;
  ASSERT_EQ(0, space_usage.all_sstable_data_occupy_size_);
  ASSERT_EQ(0, space_usage.all_sstable_data_required_size_);
  ASSERT_EQ(0, space_usage.ss_public_sstable_occupy_size_);
  ASSERT_EQ(0, space_usage.all_sstable_meta_size_);
  ASSERT_EQ(0, space_usage.tablet_clustered_sstable_data_size_);
  ASSERT_NE(0, space_usage.tablet_clustered_meta_size_);

  // check tablet's space_usage without sstable
  tablet->table_store_addr_.ptr_->major_tables_.reset();
  ObTabletHandle new_tablet_handle2;
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_and_transform_tablet(param, *tablet, new_tablet_handle2));
  space_usage = new_tablet_handle2.get_obj()->tablet_meta_.space_usage_;
  ASSERT_EQ(0, space_usage.all_sstable_data_occupy_size_);
  ASSERT_EQ(0, space_usage.all_sstable_data_required_size_);
  ASSERT_EQ(0, space_usage.ss_public_sstable_occupy_size_);
  ASSERT_EQ(0, space_usage.all_sstable_meta_size_);
  ASSERT_EQ(0, space_usage.tablet_clustered_sstable_data_size_);
  ASSERT_NE(0, space_usage.tablet_clustered_meta_size_);
}

} // storage
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_aggregated_info.log*");
  OB_LOGGER.set_file_name("test_tablet_aggregated_info.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_2_0_0);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}