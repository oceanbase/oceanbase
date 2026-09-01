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
#include <gtest/gtest.h>
#define private public
#define protected public

#include "storage/backup/ob_backup_tablet_pairing_helper.h"
#include "storage/backup/ob_backup_data_store.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace backup
{

class TestBackupTabletPairingHelper : public ::testing::Test
{
public:
  void SetUp() override
  {
    helper_.init(OB_SERVER_TENANT_ID);
  }
  void TearDown() override
  {
    helper_.reset();
  }
protected:
  ObBackupTabletPairingHelper helper_;
};

// Test init and double init
TEST_F(TestBackupTabletPairingHelper, test_init)
{
  ObBackupTabletPairingHelper h;
  EXPECT_EQ(OB_SUCCESS, h.init(OB_SERVER_TENANT_ID));
  EXPECT_TRUE(h.is_inited_);
  EXPECT_TRUE(h.is_empty());
  EXPECT_EQ(0, h.get_pairing_count());
  // init twice should fail
  EXPECT_EQ(OB_INIT_TWICE, h.init(OB_SERVER_TENANT_ID));
}

// Test reset
TEST_F(TestBackupTabletPairingHelper, test_reset)
{
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(ObTabletID(100), ObTabletID(200)));
  EXPECT_FALSE(helper_.is_empty());
  helper_.reset();
  EXPECT_FALSE(helper_.is_inited_);
  // After reset, operations should fail with NOT_INIT
  ObTabletID paired;
  EXPECT_EQ(OB_NOT_INIT, helper_.get_paired_tablet_id(ObTabletID(100), paired));
}

// Test add_pairing basic
TEST_F(TestBackupTabletPairingHelper, test_add_pairing_basic)
{
  ObTabletID a(100);
  ObTabletID b(200);
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(a, b));
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(b, a));
  EXPECT_EQ(1, helper_.get_pairing_count());
  EXPECT_FALSE(helper_.is_empty());
}

// Test add_pairing with invalid arguments
TEST_F(TestBackupTabletPairingHelper, test_add_pairing_invalid_args)
{
  ObTabletID invalid;  // default constructed, invalid
  ObTabletID valid(100);
  EXPECT_EQ(OB_INVALID_ARGUMENT, helper_.add_pairing(invalid, valid));
  EXPECT_EQ(OB_INVALID_ARGUMENT, helper_.add_pairing(valid, invalid));
  EXPECT_EQ(OB_INVALID_ARGUMENT, helper_.add_pairing(invalid, invalid));
}

// Test add_pairing duplicate with same value (should succeed)
TEST_F(TestBackupTabletPairingHelper, test_add_pairing_duplicate_same)
{
  ObTabletID a(100);
  ObTabletID b(200);
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(a, b));
  // Adding the same pair again should succeed (idempotent)
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(a, b));
  EXPECT_FALSE(helper_.is_empty());
}

// Test add_pairing duplicate with conflicting value (should fail)
TEST_F(TestBackupTabletPairingHelper, test_add_pairing_conflict)
{
  ObTabletID a(100);
  ObTabletID b(200);
  ObTabletID c(300);
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(a, b));
  // Adding same key with different value should fail
  EXPECT_EQ(OB_ERR_UNEXPECTED, helper_.add_pairing(a, c));
}

// Test add_pairing not inited
TEST_F(TestBackupTabletPairingHelper, test_add_pairing_not_init)
{
  ObBackupTabletPairingHelper h;
  EXPECT_EQ(OB_NOT_INIT, h.add_pairing(ObTabletID(1), ObTabletID(2)));
}

// Test get_paired_tablet_id
TEST_F(TestBackupTabletPairingHelper, test_get_paired_tablet_id)
{
  ObTabletID a(100);
  ObTabletID b(200);
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(a, b));
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(b, a));

  ObTabletID paired;
  EXPECT_EQ(OB_SUCCESS, helper_.get_paired_tablet_id(a, paired));
  EXPECT_EQ(b, paired);

  EXPECT_EQ(OB_SUCCESS, helper_.get_paired_tablet_id(b, paired));
  EXPECT_EQ(a, paired);
}

// Test get_paired_tablet_id not found
TEST_F(TestBackupTabletPairingHelper, test_get_paired_not_found)
{
  ObTabletID paired;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, helper_.get_paired_tablet_id(ObTabletID(999), paired));
}

// Test get_paired_tablet_id invalid argument
TEST_F(TestBackupTabletPairingHelper, test_get_paired_invalid_arg)
{
  ObTabletID paired;
  ObTabletID invalid;
  EXPECT_EQ(OB_INVALID_ARGUMENT, helper_.get_paired_tablet_id(invalid, paired));
}

// Test get_paired_tablet_id not inited
TEST_F(TestBackupTabletPairingHelper, test_get_paired_not_init)
{
  ObBackupTabletPairingHelper h;
  ObTabletID paired;
  EXPECT_EQ(OB_NOT_INIT, h.get_paired_tablet_id(ObTabletID(1), paired));
}

// Test verify_descendent_count_match with equal counts
TEST_F(TestBackupTabletPairingHelper, test_verify_descendent_count_match_equal)
{
  ObArray<ObTabletID> desc1;
  ObArray<ObTabletID> desc2;
  desc1.push_back(ObTabletID(1));
  desc1.push_back(ObTabletID(2));
  desc2.push_back(ObTabletID(3));
  desc2.push_back(ObTabletID(4));
  EXPECT_EQ(OB_SUCCESS, ObBackupTabletPairingHelper::verify_descendent_count_match(desc1, desc2));
}

// Test verify_descendent_count_match with unequal counts
TEST_F(TestBackupTabletPairingHelper, test_verify_descendent_count_match_mismatch)
{
  ObArray<ObTabletID> desc1;
  ObArray<ObTabletID> desc2;
  desc1.push_back(ObTabletID(1));
  desc1.push_back(ObTabletID(2));
  desc2.push_back(ObTabletID(3));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObBackupTabletPairingHelper::verify_descendent_count_match(desc1, desc2));
}

// Test verify_descendent_count_match with empty arrays
TEST_F(TestBackupTabletPairingHelper, test_verify_descendent_count_match_empty)
{
  ObArray<ObTabletID> desc1;
  ObArray<ObTabletID> desc2;
  EXPECT_EQ(OB_SUCCESS, ObBackupTabletPairingHelper::verify_descendent_count_match(desc1, desc2));
}

// Test export_to_desc and load_from_desc roundtrip
TEST_F(TestBackupTabletPairingHelper, test_desc_roundtrip)
{
  // Add several bidirectional pairings
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(ObTabletID(100), ObTabletID(200)));
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(ObTabletID(200), ObTabletID(100)));
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(ObTabletID(300), ObTabletID(400)));
  EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(ObTabletID(400), ObTabletID(300)));
  EXPECT_EQ(2, helper_.get_pairing_count());

  // Export to desc — only canonical direction (smaller -> larger) is exported.
  ObBackupTabletPairingInfoDesc desc;
  EXPECT_EQ(OB_SUCCESS, helper_.export_to_desc(desc));
  EXPECT_EQ(2, desc.pairing_list_.count());

  // Load into a new helper; both directions should be rebuilt from the single-direction file.
  ObBackupTabletPairingHelper helper2;
  EXPECT_EQ(OB_SUCCESS, helper2.init(OB_SERVER_TENANT_ID));
  EXPECT_EQ(OB_SUCCESS, helper2.load_from_desc(desc));
  EXPECT_EQ(2, helper2.get_pairing_count());

  // Verify all pairings are preserved
  ObTabletID paired;
  EXPECT_EQ(OB_SUCCESS, helper2.get_paired_tablet_id(ObTabletID(100), paired));
  EXPECT_EQ(ObTabletID(200), paired);
  EXPECT_EQ(OB_SUCCESS, helper2.get_paired_tablet_id(ObTabletID(200), paired));
  EXPECT_EQ(ObTabletID(100), paired);
  EXPECT_EQ(OB_SUCCESS, helper2.get_paired_tablet_id(ObTabletID(300), paired));
  EXPECT_EQ(ObTabletID(400), paired);
  EXPECT_EQ(OB_SUCCESS, helper2.get_paired_tablet_id(ObTabletID(400), paired));
  EXPECT_EQ(ObTabletID(300), paired);
}

// Test export_to_desc not inited
TEST_F(TestBackupTabletPairingHelper, test_export_desc_not_init)
{
  ObBackupTabletPairingHelper h;
  ObBackupTabletPairingInfoDesc desc;
  EXPECT_EQ(OB_NOT_INIT, h.export_to_desc(desc));
}

// Test load_from_desc not inited
TEST_F(TestBackupTabletPairingHelper, test_load_desc_not_init)
{
  ObBackupTabletPairingHelper h;
  ObBackupTabletPairingInfoDesc desc;
  EXPECT_EQ(OB_NOT_INIT, h.load_from_desc(desc));
}

// Test load_from_desc with empty desc
TEST_F(TestBackupTabletPairingHelper, test_load_desc_empty)
{
  ObBackupTabletPairingInfoDesc desc;
  EXPECT_EQ(OB_SUCCESS, helper_.load_from_desc(desc));
  EXPECT_TRUE(helper_.is_empty());
}

// Test multiple pairings and bulk operations
TEST_F(TestBackupTabletPairingHelper, test_multiple_pairings)
{
  const int64_t PAIR_COUNT = 100;
  for (int64_t i = 1; i <= PAIR_COUNT; ++i) {
    ObTabletID a(i * 2);
    ObTabletID b(i * 2 + 1);
    EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(a, b));
    EXPECT_EQ(OB_SUCCESS, helper_.add_pairing(b, a));
  }
  EXPECT_EQ(PAIR_COUNT, helper_.get_pairing_count());

  // Export and reload — file contains one direction per pair, load rebuilds both.
  ObBackupTabletPairingInfoDesc desc;
  EXPECT_EQ(OB_SUCCESS, helper_.export_to_desc(desc));
  EXPECT_EQ(PAIR_COUNT, desc.pairing_list_.count());

  ObBackupTabletPairingHelper helper2;
  EXPECT_EQ(OB_SUCCESS, helper2.init(OB_SERVER_TENANT_ID));
  EXPECT_EQ(OB_SUCCESS, helper2.load_from_desc(desc));
  EXPECT_EQ(PAIR_COUNT, helper2.get_pairing_count());

  // Spot-check some pairings
  ObTabletID paired;
  EXPECT_EQ(OB_SUCCESS, helper2.get_paired_tablet_id(ObTabletID(50), paired));
  EXPECT_EQ(ObTabletID(51), paired);
  EXPECT_EQ(OB_SUCCESS, helper2.get_paired_tablet_id(ObTabletID(51), paired));
  EXPECT_EQ(ObTabletID(50), paired);
}

// Test ObBackupTabletPairingItem validity
TEST(TestBackupTabletPairingItem, test_validity)
{
  ObBackupTabletPairingItem item1;
  EXPECT_FALSE(item1.is_valid());

  ObBackupTabletPairingItem item2(ObTabletID(1), ObTabletID(2));
  EXPECT_TRUE(item2.is_valid());
  EXPECT_EQ(ObTabletID(1), item2.tablet_id_);
  EXPECT_EQ(ObTabletID(2), item2.paired_tablet_id_);
}

} // namespace backup
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_tablet_pairing_helper.log*");
  OB_LOGGER.set_file_name("test_backup_tablet_pairing_helper.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
