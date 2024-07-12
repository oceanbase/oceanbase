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

#include "mtlenv/storage/medium_info_common.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{
class TestMediumInfoReader : public MediumInfoCommon
{
public:
  TestMediumInfoReader() = default;
  virtual ~TestMediumInfoReader() = default;
};


TEST_F(TestMediumInfoReader, read_multi_medium_info_from_minor)
{
  int ret = OB_SUCCESS;

  // get ls
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();

  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 5);
    user_data.create_commit_scn_ = commit_scn;
    user_data.create_commit_version_ = 5;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(5)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(commit_scn, commit_scn);
  }

  {
    // insert data into mds table
    ret = insert_medium_info(1718647202742212010, 1718647202742212010);
    ASSERT_EQ(OB_SUCCESS, ret);

    // mds table flush
    share::SCN decided_scn = share::SCN::plus(share::SCN::min_scn(), 1718647202742212015);
    ret = tablet->mds_table_flush(decided_scn);
    ASSERT_EQ(OB_SUCCESS, ret);

    // wait for mds table flush
    tablet_handle.reset();
    ret = wait_for_mds_table_flush(tablet_id);
    ASSERT_EQ(OB_SUCCESS, ret);

    // wait all mds nodes to be released
    ret = wait_for_all_mds_nodes_released(tablet_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  {
    // insert data into mds table
    ret = insert_medium_info(1718647202742212020, 1718647202742212020);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = insert_medium_info(1718647202742212030, 1718647202742212030);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObTabletHandle tablet_handle;
    ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    tablet = tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);

    // mds table flush
    share::SCN decided_scn = share::SCN::plus(share::SCN::min_scn(), 1718647202742212035);
    ret = tablet->mds_table_flush(decided_scn);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // check mds minor merge has done
  {
    int times = 0;
    bool has_minor = false;
    do {
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
      ASSERT_EQ(OB_SUCCESS, ret);
      tablet = tablet_handle.get_obj();
      ASSERT_NE(nullptr, tablet);

      ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
      ret = tablet->fetch_table_store(table_store_wrapper);
      ASSERT_EQ(OB_SUCCESS, ret);
      const ObTabletTableStore *table_store = table_store_wrapper.get_member();

      if (table_store->mds_sstables_.count() != 1) {
        // sleep
        ::ob_usleep(100_ms);
        ++times;
        continue;
      } else {
        ObSSTable *sstable = table_store->mds_sstables_[0];
        has_minor = sstable->is_mds_minor_sstable();
        if (!has_minor) {
          // sleep
          ::ob_usleep(100_ms);
          ++times;
        }
      }
    } while (ret == OB_SUCCESS && !has_minor && times < 60);
  }

  // read medium info
  ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  common::ObSEArray<compaction::ObMediumCompactionInfo*, 1> medium_info_array;
  ret = tablet->read_medium_array(allocator_, medium_info_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, medium_info_array.count());
  ASSERT_EQ(1718647202742212010, medium_info_array[0]->medium_snapshot_);
  ASSERT_EQ(1718647202742212020, medium_info_array[1]->medium_snapshot_);
  ASSERT_EQ(1718647202742212030, medium_info_array[2]->medium_snapshot_);
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_medium_info_reader.log*");
  OB_LOGGER.set_file_name("test_medium_info_reader.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
