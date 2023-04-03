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

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#define private public
#include "rootserver/ob_locality_util.h"
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_cluster_version.h"
#undef private

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace rootserver
{
class TestLocalityDistribution : public ::testing::Test
{
public:
  virtual void SetUp() {
    common::ObClusterVersion::get_instance().init(cal_version(1, 4, 0, 60));
    ASSERT_EQ(OB_SUCCESS, locality_dist_.init());
    ASSERT_EQ(OB_SUCCESS, zone_list_A_.push_back(ObZone("zone1")));
    ASSERT_EQ(OB_SUCCESS, zone_list_A_.push_back(ObZone("zone2")));
    ASSERT_EQ(OB_SUCCESS, zone_list_A_.push_back(ObZone("zone3")));
    ASSERT_EQ(OB_SUCCESS, zone_list_A_.push_back(ObZone("zone4")));
    ASSERT_EQ(OB_SUCCESS, zone_list_A_.push_back(ObZone("zone5")));
    ASSERT_EQ(OB_SUCCESS, zone_list_B_.push_back(ObString("zone1")));
    ASSERT_EQ(OB_SUCCESS, zone_list_B_.push_back(ObString("zone2")));
    ASSERT_EQ(OB_SUCCESS, zone_list_B_.push_back(ObString("zone3")));
    ASSERT_EQ(OB_SUCCESS, zone_list_B_.push_back(ObString("zone4")));
    ASSERT_EQ(OB_SUCCESS, zone_list_B_.push_back(ObString("zone5")));
    ASSERT_EQ(OB_SUCCESS, zone_region_list_.push_back(ObZoneRegion("zone1", "HZ")));
    ASSERT_EQ(OB_SUCCESS, zone_region_list_.push_back(ObZoneRegion("zone2", "HZ")));
    ASSERT_EQ(OB_SUCCESS, zone_region_list_.push_back(ObZoneRegion("zone3", "SH")));
    ASSERT_EQ(OB_SUCCESS, zone_region_list_.push_back(ObZoneRegion("zone4", "SH")));
    ASSERT_EQ(OB_SUCCESS, zone_region_list_.push_back(ObZoneRegion("zone5", "SZ")));
  }
  virtual void TearDown() {
    zone_list_A_.reset();
    zone_list_B_.reset();
    zone_region_list_.reset();
  }
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
public:
  ObLocalityDistribution locality_dist_;
  common::ObSEArray<common::ObZone, 64> zone_list_A_;
  common::ObSEArray<common::ObString, 64> zone_list_B_;
  common::ObSEArray<share::schema::ObZoneRegion, 64> zone_region_list_;
};

TEST_F(TestLocalityDistribution, empty_locality_A)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1}@zone1, FULL{1}@zone2, FULL{1}@zone3, FULL{1}@zone4, FULL{1}@zone5";
  UNUSED(expected_locality);
  ObString empty_locality;
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(empty_locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, empty_locality_B)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1}@zone1, FULL{1}@zone2, FULL{1}@zone3, FULL{1}@zone4, FULL{1}@zone5";
  UNUSED(expected_locality);
  ObString empty_locality;
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(empty_locality, zone_list_B_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, random_blanks)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1}@zone1, FULL{1}@zone2, FULL{1}@zone3, FULL{1}@zone4, FULL{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("F {1 } @ zone1,F{      1}@zone3, F{   1} @zone4, F  {1}@zone2, F{1}@zone5        ");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, random_blanks2)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1}@zone1, FULL{1}@zone2, FULL{1}@zone3, FULL{1}@zone4, FULL{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("F{      1}@zone3, F{   1} @zone4, F  {1}@zone2, F{1}@zone5,F{1}@zone1        ");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, random_blanks_lower_case)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1}@zone1, FULL{1}@zone2, FULL{1}@zone3, FULL{1}@zone4, FULL{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("f {1 } @ zone1,F{      1}@zone3, f{   1} @zone4, f  {1}@zone2, F{1}@zone5        ");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, random_blanks_lower_case_and_full_name)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1}@zone1, FULL{1}@zone2, FULL{1}@zone3, FULL{1}@zone4, FULL{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("fULl {1 } @ zone1,FulL{      1}@zone3, f{   1} @zone4, fulL  {1}@zone2, F{1}@zone5        ");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, random_blanks_lower_case_and_full_name_plus_token)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1}@zone1, FULL{1}@zone2, FULL{1}@zone3, FULL{1}@zone4, FULL{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("fULl {+1 } @ zone1,FulL{      +1}@zone3, f{   1} @zone4, fulL  {1}@zone2, F{1}@zone5        ");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, random_blanks_lower_case_and_full_name_diff_replica_type)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "READONLY{1}@zone1, FULL{1}@zone2, FULL{1}@zone3, READONLY{1}@zone4, LOGONLY{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("readonly {1 } @ zone1,FulL{      1}@zone3, readonly{   1} @zone4, f  {1}@zone2, logonly{1}@zone5        ");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, all_server)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "READONLY{ALL_SERVER}@zone1, FULL{1}@zone2, FULL{1}@zone3, READONLY{ALL_SERVER}@zone4, LOGONLY{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("readonly {all_server } @ zone1,FulL{      1}@zone3, READonly{   All_server} @zone4, f  {1 }@zone2, logonly{1}@zone5");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, all_server_test)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "READONLY{ALL_SERVER}@zone1, FULL{1}@zone2, FULL{1}@zone3, READONLY{ALL_SERVER}@zone4, LOGONLY{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("readonly {all_server } @ zone1,FulL{      1}@zone3, READonly{   All_server} @zone4, f  {1 }@zone2, logonly{1}@zone5");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, all_server_with_multiple_replica_type_one_zone)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1},READONLY{ALL_SERVER}@zone1, FULL{1},READONLY{2}@zone2, "
                                  "READONLY{ALL_SERVER}@zone3, FULL{1}@zone4, LOGONLY{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("readonly {all_server } , F@ zone1,FulL{      1}, r{2}@zone2, READonly{   All_server} @zone3, f  {1 }@zone4, logonly{1}@zone5");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, every_zone)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1}@zone1, FULL{1}@zone2, FULL{1}@zone3, FULL{1}@zone4, FULL{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("f@everyzone");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, zone_split)
{
  const char *expected_locality = "FULL{1},READONLY{ALL_SERVER}@zone1, FULL{1},READONLY{2}@zone2, "
                                  "READONLY{ALL_SERVER}@zone3, FULL{1}@zone4, LOGONLY{1}@zone5";
  UNUSED(expected_locality);
  ObString locality("r{all_server}@zone1, f@zone1, F{1}, r{2}@zone2, R{All_server} @zone3,f{1}@zone4, logonly{1}@zone5");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, every_zone_and_all_server)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1},READONLY{ALL_SERVER}@zone1, FULL{1},READONLY{ALL_SERVER}@zone2, "
                                  "FULL{1},READONLY{ALL_SERVER}@zone3, FULL{1},READONLY{ALL_SERVER}@zone4, "
                                  "FULL{1},READONLY{ALL_SERVER}@zone5";
  UNUSED(expected_locality);
  ObString locality("f, r{ALL_SERVER}@everyzone");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, every_zone_and_all_server2)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1},READONLY{ALL_SERVER}@zone1, FULL{1},READONLY{ALL_SERVER}@zone2, "
                                  "FULL{1},READONLY{ALL_SERVER}@zone3, FULL{1},READONLY{ALL_SERVER}@zone4, "
                                  "FULL{1},READONLY{ALL_SERVER}@zone5";
  UNUSED(expected_locality);
  ObString locality("FULL{1},READONLY{ALL_SERVER}@zone1, FULL{1,MEMSTORE_PERCENT:100},READONLY{ALL_SERVER}@zone2, "
                    "FULL{1},READONLY{ALL_SERVER}@zone3, FULL{1,MEMSTORE_PERCENT:100},READONLY{ALL_SERVER}@zone4, "
                    "FULL{1},READONLY{ALL_SERVER}@zone5");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, hybrid)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1},LOGONLY{1}@[zone1,zone2]";
  UNUSED(expected_locality);
  ObString locality("F{1},l@[zone1, zone2]");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, hybrid2)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1}@zone1, LOGONLY{1}@zone2, FULL{1}@zone5, FULL{1},LOGONLY{1}@[zone3,zone4]";
  UNUSED(expected_locality);
  ObString locality("F@zone1, L@zone2,F{1},l@[zone4, zone3], F@[zone5]");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, memstore_percent_0)
{
  char output_locality[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  const char *expected_locality = "FULL{1}@zone1, LOGONLY{1}@zone2, FULL{1,MEMSTORE_PERCENT:0}@zone3, FULL{1,MEMSTORE_PERCENT:99}@zone4";
  UNUSED(expected_locality);
  ObString locality("F@zone1, L@zone2, F{1,memstore_percent:0}@[zone3], F{1,memstore_percent:99}@[zone4]");
  ASSERT_EQ(OB_SUCCESS, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
  ASSERT_EQ(OB_SUCCESS, locality_dist_.output_normalized_locality(output_locality, MAX_LOCALITY_LENGTH, pos));
  ASSERT_EQ(0, strncmp(output_locality, expected_locality, strlen(expected_locality)));
}

TEST_F(TestLocalityDistribution, logonly_with_memstore_error)
{
  ObString locality("l{1, memstore_percent:10}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, zone_name_intersect)
{
  ObString locality("f{1, memstore_percent:10},l@[zone1,zone2], f{1, memstore_percent:10},l@[zone1,zone2]");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, replica_num_zero_error)
{
  ObString locality("F{0, memstore_percent:10}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, memstore_percent_negative_error)
{
  ObString locality("F{1, memstore_percent:-1}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, memstore_percent_too_large_error)
{
  ObString locality("F{1, memstore_percent:101}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, attribute_multiple_error1)
{
  ObString locality("F{memstore_percent:1,memstore_percent:2, 1}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, attribute_multiple_error2)
{
  ObString locality("F{memstore_percent:1, 2, 1}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, replica_num_not_set_error2)
{
  ObString locality("F{memstore_percent:1}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, full_replica_greater_than_1_error)
{
  ObString locality("F{2}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, replica_in_region_greater_than_1_error)
{
  ObString locality("F{2},l@[z1, z2]");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, replica_in_region_greater_than_2_error)
{
  ObString locality("F{2},l@[zone1, zone2]");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, specific_zero_err)
{
  ObString locality("readonly{0}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, missing_left_brace_err)
{
  ObString locality("readonly 1}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, missing_right_brace_err)
{
  ObString locality("FulL{1@zone3");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, extrace_comma_err)
{
  ObString locality("readonly{1},@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, all_server_with_plus)
{
  ObString locality("readonly{+All_server}@zone4");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, extrace_special_token_err)
{
  ObString locality("readonly  {1..} @ zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, zone_not_exist_err)
{
  ObString locality("readonly @zone6");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

TEST_F(TestLocalityDistribution, empty_replica_error)
{
  ObString locality("   @zone3");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, empty_locality_error)
{
  ObString locality("   ,   ");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, empty_locality_error2)
{
  ObString locality("         ");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, invalid_replica_type_err)
{
  ObString locality("k@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, invalid_replica_type_err2)
{
  ObString locality("fulll@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, invalid_replica_type_err3)
{
  ObString locality("ful@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, overflow_err)
{
  ObString locality("r{1111111111111111111111111111111111111111111111111}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, negative_num_err)
{
  ObString locality("r{-111}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, num_begin_with_zero)
{
  ObString locality("r{011}@zone1");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, extra_comma)
{
  ObString locality("f{1}@zone1,");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_));
}

TEST_F(TestLocalityDistribution, l_not_allowed_for_set)
{
  ObString locality("f{1},l,r@[zone1,zone2]");
  ASSERT_EQ(OB_INVALID_ARGUMENT, locality_dist_.parse_locality(locality, zone_list_A_, &zone_region_list_));
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_locality_util.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_locality_util.log", true);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


