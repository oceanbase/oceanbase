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
#include "lib/net/ob_addr.h"
#include "lib/hash_func/murmur_hash.h"

namespace oceanbase
{
using namespace common;
namespace unittest
{
int64_t cal_hash_v1(const uint64_t tenant_id, const common::ObAddr &addr, const int64_t cluster_id)
{
  // This calculation may cause hash conflicts
  int64_t ret_hash = murmurhash(&tenant_id, sizeof(tenant_id), addr.hash());
  ret_hash = (ret_hash | (cluster_id << 56));
  return ret_hash;
}

int64_t cal_hash_v2(const uint64_t tenant_id, const common::ObAddr &addr, const int64_t cluster_id)
{
  // ObAddr.hash() only generates a 32-bit value, so the algorithm of shifting cluster_id to the left by 32 bits is effective
  int64_t ret_hash = (addr.hash() | (cluster_id << 48));
  ret_hash = murmurhash(&tenant_id, sizeof(tenant_id), ret_hash);
  return ret_hash;
}

TEST(test_cluster_id_hash_conflict, test_cluster_id_hash)
{
  common::ObAddr addr;
  addr.parse_from_cstring("127.0.0.1:8080");

  int64_t tenant_id = OB_SERVER_TENANT_ID;

  // The binary of 112 and 120 are only one bit different
  int64_t cluster_id_11 = 112;
  int64_t cluster_id_12 = 120;

  int64_t cluster_id_21 = 111;
  int64_t cluster_id_22 = 112;


//  EXPECT_NE(cal_hash_v1(tenant_id, addr, cluster_id_11), cal_hash_v1(tenant_id, addr, cluster_id_12)); // hash conflict

  EXPECT_NE(cal_hash_v2(tenant_id, addr, cluster_id_11), cal_hash_v2(tenant_id, addr, cluster_id_12));
  EXPECT_NE(cal_hash_v2(tenant_id, addr, cluster_id_21), cal_hash_v2(tenant_id, addr, cluster_id_22));

  CLOG_LOG(INFO, "v1 ret_hash is ", "result", cal_hash_v1(tenant_id, addr, cluster_id_11));
  CLOG_LOG(INFO, "v1 ret_hash is ", "result", cal_hash_v1(tenant_id, addr, cluster_id_12));
  CLOG_LOG(INFO, "v1 ret_hash is ", "result", cal_hash_v1(tenant_id, addr, cluster_id_21));
  CLOG_LOG(INFO, "v1 ret_hash is ", "result", cal_hash_v1(tenant_id, addr, cluster_id_22));

  CLOG_LOG(INFO, "v2 ret_hash is ", "result", cal_hash_v2(tenant_id, addr, cluster_id_11));
  CLOG_LOG(INFO, "v2 ret_hash is ", "result", cal_hash_v2(tenant_id, addr, cluster_id_12));
  CLOG_LOG(INFO, "v2 ret_hash is ", "result", cal_hash_v2(tenant_id, addr, cluster_id_21));
  CLOG_LOG(INFO, "v2 ret_hash is ", "result", cal_hash_v2(tenant_id, addr, cluster_id_22));

}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_cluster_id_hash_conflict.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest:test_cluster_id_hash_conflict");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
