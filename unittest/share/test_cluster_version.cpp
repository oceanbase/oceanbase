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

#define USING_LOG_PREFIX SHARE
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_cluster_version.h"
#include <gtest/gtest.h>


namespace oceanbase
{
namespace share
{
using namespace common;
using namespace oceanbase::lib;
class TestClusterVersion: public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestClusterVersion, init)
{
  uint64_t version = 0;

  version = cal_version(1, 4, 0, 79);
  ObClusterVersion ver_1;
  ASSERT_EQ(ver_1.init(version), OB_SUCCESS);

  version = cal_version(1, 4, 79, 0);
  ObClusterVersion ver_2;
  ASSERT_EQ(ver_2.init(version), OB_INVALID_ARGUMENT);

  version = cal_version(3, 2, 0, 0);
  ObClusterVersion ver_3;
  ASSERT_EQ(ver_3.init(version), OB_SUCCESS);

  version = cal_version(3, 2, 0, 1);
  ObClusterVersion ver_4;
  ASSERT_EQ(ver_4.init(version), OB_SUCCESS);

  version = cal_version(3, 2, 0, 2);
  ObClusterVersion ver_5;
  ASSERT_EQ(ver_5.init(version), OB_SUCCESS);

  version = cal_version(3, 2, 1, 0);
  ObClusterVersion ver_6;
  ASSERT_EQ(ver_6.init(version), OB_INVALID_ARGUMENT);

  version = cal_version(3, 2, 1, 1);
  ObClusterVersion ver_18;
  ASSERT_EQ(ver_18.init(version), OB_INVALID_ARGUMENT);

  version = cal_version(3, 2, 2, 0);
  ObClusterVersion ver_7;
  ASSERT_EQ(ver_7.init(version), OB_INVALID_ARGUMENT);

  version = cal_version(3, 2, 2, 1);
  ObClusterVersion ver_19;
  ASSERT_EQ(ver_19.init(version), OB_INVALID_ARGUMENT);

  version = cal_version(3, 2, 0, 3);
  ObClusterVersion ver_8;
  ASSERT_EQ(ver_8.init(version), OB_INVALID_ARGUMENT);

  version = cal_version(3, 2, 0, 4);
  ObClusterVersion ver_17;
  ASSERT_EQ(ver_17.init(version), OB_INVALID_ARGUMENT);

  // major_patch_version(3rd) can be greator than 0 when:
  // - cluster_version >= "3.3"
  // - cluster_version >= "3.2.3.0"

  version = cal_version(3, 2, 3, 0);
  ObClusterVersion ver_9;
  ASSERT_EQ(ver_9.init(version), OB_SUCCESS);

  version = cal_version(3, 2, 3, 1);
  ObClusterVersion ver_10;
  ASSERT_EQ(ver_10.init(version), OB_SUCCESS);

  version = cal_version(3, 2, 4, 0);
  ObClusterVersion ver_11;
  ASSERT_EQ(ver_11.init(version), OB_SUCCESS);

  version = cal_version(3, 2, 4, 1);
  ObClusterVersion ver_12;
  ASSERT_EQ(ver_12.init(version), OB_SUCCESS);

  version = cal_version(3, 3, 0, 0);
  ObClusterVersion ver_13;
  ASSERT_EQ(ver_13.init(version), OB_SUCCESS);

  version = cal_version(3, 3, 1, 0);
  ObClusterVersion ver_14;
  ASSERT_EQ(ver_14.init(version), OB_SUCCESS);

  version = cal_version(3, 3, 0, 1);
  ObClusterVersion ver_15;
  ASSERT_EQ(ver_15.init(version), OB_SUCCESS);

  version = cal_version(3, 3, 1, 1);
  ObClusterVersion ver_16;
  ASSERT_EQ(ver_16.init(version), OB_SUCCESS);
}

TEST_F(TestClusterVersion, refresh_cluster_version)
{
  // 1.4.79
  ObClusterVersion ver_1;
  ASSERT_EQ(ver_1.refresh_cluster_version("1.4.0.79"), OB_INVALID_ARGUMENT);

  ObClusterVersion ver_2;
  ASSERT_EQ(ver_2.refresh_cluster_version("1.4.79.0"), OB_SUCCESS);

  ObClusterVersion ver_3;
  ASSERT_EQ(ver_3.refresh_cluster_version("1.4.79"), OB_SUCCESS);

  ASSERT_EQ(ver_2.get_cluster_version(), ver_3.get_cluster_version());

  // 3.2
  ObClusterVersion ver_4;
  ASSERT_EQ(ver_4.refresh_cluster_version("3.2.0"), OB_SUCCESS);

  ObClusterVersion ver_5;
  ASSERT_EQ(ver_5.refresh_cluster_version("3.2.0.0"), OB_SUCCESS);

  ASSERT_EQ(ver_4.get_cluster_version(), ver_5.get_cluster_version());

  // 3.2.1
  ObClusterVersion ver_6;
  ASSERT_EQ(ver_6.refresh_cluster_version("3.2.0.1"), OB_INVALID_ARGUMENT);

  ObClusterVersion ver_7;
  ASSERT_EQ(ver_7.refresh_cluster_version("3.2.1.0"), OB_SUCCESS);

  ObClusterVersion ver_8;
  ASSERT_EQ(ver_8.refresh_cluster_version("3.2.1"), OB_SUCCESS);

  ASSERT_EQ(ver_7.get_cluster_version(), ver_8.get_cluster_version());

  // 3.2.1.1
  ObClusterVersion ver_26;
  ASSERT_EQ(ver_26.refresh_cluster_version("3.2.1.1"), OB_INVALID_ARGUMENT);

  // 3.2.2
  ObClusterVersion ver_9;
  ASSERT_EQ(ver_9.refresh_cluster_version("3.2.0.2"), OB_INVALID_ARGUMENT);

  ObClusterVersion ver_10;
  ASSERT_EQ(ver_10.refresh_cluster_version("3.2.2.0"), OB_SUCCESS);

  ObClusterVersion ver_11;
  ASSERT_EQ(ver_11.refresh_cluster_version("3.2.2"), OB_SUCCESS);

  ASSERT_EQ(ver_10.get_cluster_version(), ver_11.get_cluster_version());

  // 3.2.2.1
  ObClusterVersion ver_25;
  ASSERT_EQ(ver_25.refresh_cluster_version("3.2.2.1"), OB_INVALID_ARGUMENT);

  // cluster version which is less than "3.2.3":
  // - should be format as "a.b.c" or "a.b.c.0"
  // - "a.b.0.c" is invalid

  // 3.2.3
  ObClusterVersion ver_12;
  ASSERT_EQ(ver_12.refresh_cluster_version("3.2.0.3"), OB_INVALID_ARGUMENT);

  ObClusterVersion ver_13;
  ASSERT_EQ(ver_13.refresh_cluster_version("3.2.3.0"), OB_SUCCESS);

  ObClusterVersion ver_14;
  ASSERT_EQ(ver_14.refresh_cluster_version("3.2.3"), OB_SUCCESS);

  ASSERT_EQ(ver_13.get_cluster_version(), ver_14.get_cluster_version());

  // 3.2.3.1
  ObClusterVersion ver_27;
  ASSERT_EQ(ver_27.refresh_cluster_version("3.2.3.1"), OB_SUCCESS);

  // 3.2.4
  ObClusterVersion ver_15;
  ASSERT_EQ(ver_15.refresh_cluster_version("3.2.0.4"), OB_INVALID_ARGUMENT);

  ObClusterVersion ver_16;
  ASSERT_EQ(ver_16.refresh_cluster_version("3.2.4.0"), OB_SUCCESS);

  ObClusterVersion ver_17;
  ASSERT_EQ(ver_17.refresh_cluster_version("3.2.4"), OB_SUCCESS);

  ASSERT_EQ(ver_16.get_cluster_version(), ver_17.get_cluster_version());

  // 3.2.4.1
  ObClusterVersion ver_18;
  ASSERT_EQ(ver_18.refresh_cluster_version("3.2.4.1"), OB_SUCCESS);

  // 3.3.x
  ObClusterVersion ver_19;
  ASSERT_EQ(ver_19.refresh_cluster_version("3.3.0"), OB_SUCCESS);

  ObClusterVersion ver_20;
  ASSERT_EQ(ver_20.refresh_cluster_version("3.3.0.0"), OB_SUCCESS);

  ASSERT_EQ(ver_19.get_cluster_version(), ver_20.get_cluster_version());

  ObClusterVersion ver_21;
  ASSERT_EQ(ver_21.refresh_cluster_version("3.3.0.1"), OB_SUCCESS);

  ObClusterVersion ver_22;
  ASSERT_EQ(ver_22.refresh_cluster_version("3.3.1.0"), OB_SUCCESS);

  ObClusterVersion ver_23;
  ASSERT_EQ(ver_23.refresh_cluster_version("3.3.1"), OB_SUCCESS);

  ASSERT_NE(ver_21.get_cluster_version(), ver_22.get_cluster_version());
  ASSERT_EQ(ver_22.get_cluster_version(), ver_23.get_cluster_version());

  ObClusterVersion ver_24;
  ASSERT_EQ(ver_24.refresh_cluster_version("3.3.1.1"), OB_SUCCESS);
}

TEST_F(TestClusterVersion, encode)
{
  uint64_t version = 0;

  // for cluster_version < 3.2.3, cluster version string which like "a.b.c" or "a.b.c.0" is encoded as "a|b|0|c".
  version = cal_version(1, 4, 0, 79);
  ObClusterVersion ver_1;
  ASSERT_EQ(ver_1.init(version), OB_SUCCESS);
  ObClusterVersion ver_2;
  ASSERT_EQ(ver_2.refresh_cluster_version("1.4.79.0"), OB_SUCCESS);
  ASSERT_EQ(ver_1.get_cluster_version(), ver_2.get_cluster_version());

  version = cal_version(3, 2, 0, 1);
  ObClusterVersion ver_3;
  ASSERT_EQ(ver_3.init(version), OB_SUCCESS);
  ObClusterVersion ver_4;
  ASSERT_EQ(ver_4.refresh_cluster_version("3.2.1.0"), OB_SUCCESS);
  ASSERT_EQ(ver_3.get_cluster_version(), ver_4.get_cluster_version());

  version = cal_version(3, 2, 0, 2);
  ObClusterVersion ver_5;
  ASSERT_EQ(ver_5.init(version), OB_SUCCESS);
  ObClusterVersion ver_6;
  ASSERT_EQ(ver_6.refresh_cluster_version("3.2.2.0"), OB_SUCCESS);
  ASSERT_EQ(ver_5.get_cluster_version(), ver_6.get_cluster_version());

  // for cluster_version >= 3.2.3, cluster_version string which like "a.b.c.d" is encoded as "a|b|c|d".
  version = cal_version(3, 2, 3, 0);
  ObClusterVersion ver_7;
  ASSERT_EQ(ver_7.init(version), OB_SUCCESS);
  ObClusterVersion ver_8;
  ASSERT_EQ(ver_8.refresh_cluster_version("3.2.3.0"), OB_SUCCESS);
  ASSERT_EQ(ver_7.get_cluster_version(), ver_8.get_cluster_version());

  version = cal_version(3, 2, 3, 1);
  ObClusterVersion ver_9;
  ASSERT_EQ(ver_9.init(version), OB_SUCCESS);
  ObClusterVersion ver_10;
  ASSERT_EQ(ver_10.refresh_cluster_version("3.2.3.1"), OB_SUCCESS);
  ASSERT_EQ(ver_9.get_cluster_version(), ver_10.get_cluster_version());

  version = cal_version(3, 2, 4, 0);
  ObClusterVersion ver_11;
  ASSERT_EQ(ver_11.init(version), OB_SUCCESS);
  ObClusterVersion ver_12;
  ASSERT_EQ(ver_12.refresh_cluster_version("3.2.4.0"), OB_SUCCESS);
  ASSERT_EQ(ver_11.get_cluster_version(), ver_12.get_cluster_version());

  version = cal_version(3, 2, 4, 1);
  ObClusterVersion ver_13;
  ASSERT_EQ(ver_13.init(version), OB_SUCCESS);
  ObClusterVersion ver_14;
  ASSERT_EQ(ver_14.refresh_cluster_version("3.2.4.1"), OB_SUCCESS);
  ASSERT_EQ(ver_13.get_cluster_version(), ver_14.get_cluster_version());

  version = cal_version(3, 3, 0, 1);
  ObClusterVersion ver_15;
  ASSERT_EQ(ver_15.init(version), OB_SUCCESS);
  version = cal_version(3, 3, 1, 0);
  ObClusterVersion ver_16;
  ASSERT_EQ(ver_16.init(version), OB_SUCCESS);
  ObClusterVersion ver_17;
  ASSERT_EQ(ver_17.refresh_cluster_version("3.3.0.1"), OB_SUCCESS);
  ObClusterVersion ver_18;
  ASSERT_EQ(ver_18.refresh_cluster_version("3.3.1.0"), OB_SUCCESS);

  ASSERT_EQ(ver_15.get_cluster_version(), ver_17.get_cluster_version());
  ASSERT_EQ(ver_16.get_cluster_version(), ver_18.get_cluster_version());
  ASSERT_NE(ver_15.get_cluster_version(), ver_16.get_cluster_version());
}

TEST_F(TestClusterVersion, is_valid)
{
  ASSERT_EQ(ObClusterVersion::is_valid("1.4.0.79"), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::is_valid("1.4.79.0"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("1.4.79"), OB_SUCCESS);

  ASSERT_EQ(ObClusterVersion::is_valid("3.2.0.0"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.0"), OB_SUCCESS);

  ASSERT_EQ(ObClusterVersion::is_valid("3.2.0.1"), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.1.0"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.1"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.1.1"), OB_INVALID_ARGUMENT);

  ASSERT_EQ(ObClusterVersion::is_valid("3.2.0.2"), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.2.0"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.2"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.2.1"), OB_INVALID_ARGUMENT);

  // for cluster_version < "3.2.3", 4th cluster version should be 0.
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.0.3"), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.3.0"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.3"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.3.1"), OB_SUCCESS);

  ASSERT_EQ(ObClusterVersion::is_valid("3.2.0.4"), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.4.0"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.4"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.2.4.1"), OB_SUCCESS);

  ASSERT_EQ(ObClusterVersion::is_valid("3.3.0.0"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.3.0.1"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.3.1.0"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.3.1"), OB_SUCCESS);
  ASSERT_EQ(ObClusterVersion::is_valid("3.3.1.1"), OB_SUCCESS);
}

TEST_F(TestClusterVersion, get_version)
{
  uint64_t version = 0;
  uint64_t version_1 = 0;
  uint64_t res_version = 0;
  // 1.4.79
  version = cal_version(1, 4, 0, 79);   // right
  version_1 = cal_version(1, 4, 79, 0); // wrong
  ASSERT_EQ(ObClusterVersion::get_version("1.4.0.79", res_version), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::get_version("1.4.79.0", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);
  ASSERT_EQ(ObClusterVersion::get_version("1.4.79", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);

  // 3.2
  version = cal_version(3, 2, 0, 0);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.0.0", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.0", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);

  // 3.2.1
  version = cal_version(3, 2, 0, 1);   // right
  version_1 = cal_version(3, 2, 1, 0); // wrong
  ASSERT_EQ(ObClusterVersion::get_version("3.2.1.1", res_version), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.0.1", res_version), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.1.0", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.1", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);

  // 3.2.2
  version = cal_version(3, 2, 0, 2);   // right
  version_1 = cal_version(3, 2, 2, 0); // wrong
  ASSERT_EQ(ObClusterVersion::get_version("3.2.2.1", res_version), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.0.2", res_version), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.2.0", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.2", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);

  // cluster version which is less than "3.2.3":
  // - should be format as "a.b.c" or "a.b.c.0"
  // - "a.b.0.c" is invalid

  // 3.2.3
  version = cal_version(3, 2, 3, 0);    // right
  version_1 = cal_version(3, 2, 0, 3);  // wrong
  ASSERT_EQ(ObClusterVersion::get_version("3.2.0.3", res_version), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.3.0", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.3", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);

  // 3.2.3.1
  version = cal_version(3, 2, 3, 1);    // right
  ASSERT_EQ(ObClusterVersion::get_version("3.2.3.1", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);

  // 3.2.4.0
  version = cal_version(3, 2, 4, 0);    // right
  version_1 = cal_version(3, 2, 0, 4);  // wrong
  ASSERT_EQ(ObClusterVersion::get_version("3.2.0.4", res_version), OB_INVALID_ARGUMENT);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.4.0", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.4", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);

  // 3.2.4.1
  version = cal_version(3, 2, 4, 1);
  ASSERT_EQ(ObClusterVersion::get_version("3.2.4.1", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);

  // 3.3
  version = cal_version(3, 3, 0, 0);
  ASSERT_EQ(ObClusterVersion::get_version("3.3.0.0", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_EQ(ObClusterVersion::get_version("3.3.0", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);

  // 3.3.0.1
  version = cal_version(3, 3, 0, 1);    // right
  version_1 = cal_version(3, 3, 1, 0);  // wrong
  ASSERT_EQ(ObClusterVersion::get_version("3.3.0.1", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);

  // 3.3.1
  version = cal_version(3, 3, 1, 0);    // right
  version_1 = cal_version(3, 3, 0, 1);  // wrong
  ASSERT_EQ(ObClusterVersion::get_version("3.3.1.0", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);
  ASSERT_EQ(ObClusterVersion::get_version("3.3.1", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
  ASSERT_NE(version_1, res_version);

  // 3.3.1.1
  version = cal_version(3, 3, 1, 1);
  ASSERT_EQ(ObClusterVersion::get_version("3.3.1.1", res_version), OB_SUCCESS);
  ASSERT_EQ(version, res_version);
}

TEST_F(TestClusterVersion, print_version_str)
{
  char version_str[OB_CLUSTER_VERSION_LENGTH] = {0};
  uint64_t version = 0;
  int64_t pos = 0;

  // 1.4.79
  version = cal_version(1, 4, 79, 0);
  ASSERT_EQ(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  version = cal_version(1, 4, 0, 79);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "1.4.79", OB_CLUSTER_VERSION_LENGTH));

  // 3.2
  version = cal_version(3, 2, 0, 0);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.2.0", OB_CLUSTER_VERSION_LENGTH));

  // 3.2.1
  version = cal_version(3, 2, 1, 0);
  ASSERT_EQ(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  version = cal_version(3, 2, 0, 1);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.2.1", OB_CLUSTER_VERSION_LENGTH));

  // 3.2.1.1
  version = cal_version(3, 2, 1, 1);
  ASSERT_EQ(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));

  // 3.2.2
  version = cal_version(3, 2, 2, 0);
  ASSERT_EQ(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  version = cal_version(3, 2, 0, 2);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.2.2", OB_CLUSTER_VERSION_LENGTH));

  // 3.2.2.1
  version = cal_version(3, 2, 2, 1);
  ASSERT_EQ(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));

  // 3.2.3
  version = cal_version(3, 2, 0, 3);
  ASSERT_EQ(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  version = cal_version(3, 2, 3, 0);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.2.3.0", OB_CLUSTER_VERSION_LENGTH));

  // 3.2.3.1
  version = cal_version(3, 2, 3, 1);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.2.3.1", OB_CLUSTER_VERSION_LENGTH));

  // 3.2.4
  version = cal_version(3, 2, 0, 4);
  ASSERT_EQ(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  version = cal_version(3, 2, 4, 0);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.2.4.0", OB_CLUSTER_VERSION_LENGTH));

  // 3.2.4.1
  version = cal_version(3, 2, 4, 1);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.2.4.1", OB_CLUSTER_VERSION_LENGTH));
  // 3.3
  version = cal_version(3, 3, 0, 0);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.3.0.0", OB_CLUSTER_VERSION_LENGTH));
  // 3.3.0.1
  version = cal_version(3, 3, 0, 1);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.3.0.1", OB_CLUSTER_VERSION_LENGTH));
  // 3.3.1
  version = cal_version(3, 3, 1, 0);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.3.1.0", OB_CLUSTER_VERSION_LENGTH));
  // 3.3.1.1
  version = cal_version(3, 3, 1, 1);
  ASSERT_NE(OB_INVALID_INDEX, ObClusterVersion::print_version_str(version_str, OB_CLUSTER_VERSION_LENGTH, version));
  ASSERT_EQ(0, STRNCMP(version_str, "3.3.1.1", OB_CLUSTER_VERSION_LENGTH));
}

} // end share
} // end oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
