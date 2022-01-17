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

#ifndef OB_UNITTEST_TEST_ELECTION_CLUSTER_H_
#define OB_UNITTEST_TEST_ELECTION_CLUSTER_H_

#include <gtest/gtest.h>
#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"
#include "hf_mock_election_cluster.h"

namespace oceanbase {
using namespace common;

namespace unittest {

class TestElectionCluster : public ::testing::Test {
public:
  TestElectionCluster()
  {}
  virtual ~TestElectionCluster()
  {}
  virtual void SetUp()
  {}
  virtual void TearTown()
  {}
  static void SetUpTestCase();
  static void TearDownTestCase();
};

}  // namespace unittest
}  // namespace oceanbase

#endif
