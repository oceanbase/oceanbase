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

#define USING_LOG_PREFIX SHARE_PT

#include <gtest/gtest.h>
#include "share/partition_table/ob_replica_filter.h"
#include "fake_part_property_getter.h"
#include <sys/time.h>
#include <sys/resource.h>

namespace oceanbase {
namespace share {
using namespace common;
using namespace host;

TEST(TestReplicaFilter, filters)
{
  ObPartitionReplica r;
  r.table_id_ = combine_id(1, 1);
  r.partition_id_ = 0;
  r.partition_cnt_ = 1;
  r.server_ = B;
  r.zone_ = "xx";
  r.data_version_ = -1;

  ObIReplicaFilter* f = NULL;
  // valid version filter
  f = new ObValidVersionReplicaFilter();
  bool pass = false;
  ASSERT_EQ(OB_SUCCESS, f->check(r, pass));
  ASSERT_FALSE(pass);
  r.data_version_ = 1;
  ASSERT_EQ(OB_SUCCESS, f->check(r, pass));
  ASSERT_TRUE(pass);
  ASSERT_FALSE(f->skip_empty_partition());

  // version filter
  f = new ObVersionReplicaFilter(2);
  ASSERT_EQ(OB_SUCCESS, f->check(r, pass));
  ASSERT_FALSE(pass);
  r.data_version_ = 2;
  ASSERT_EQ(OB_SUCCESS, f->check(r, pass));
  ASSERT_TRUE(pass);
  ASSERT_FALSE(f->skip_empty_partition());

  // zone filter
  f = new ObZoneReplicaFilter("1");
  ASSERT_EQ(OB_SUCCESS, f->check(r, pass));
  ASSERT_FALSE(pass);
  r.zone_ = "1";
  ASSERT_EQ(OB_SUCCESS, f->check(r, pass));
  ASSERT_TRUE(pass);
  ASSERT_FALSE(f->skip_empty_partition());

  // server filter
  f = new ObServerReplicaFilter(A);
  ASSERT_EQ(OB_SUCCESS, f->check(r, pass));
  ASSERT_FALSE(pass);
  r.server_ = A;
  ASSERT_EQ(OB_SUCCESS, f->check(r, pass));
  ASSERT_TRUE(pass);
  ASSERT_TRUE(f->skip_empty_partition());
}

TEST(TestReplicaFilter, filter_holder)
{
  {
    ObReplicaFilterHolder filters;

    ObVersionReplicaFilter version_filter(1);
    ASSERT_EQ(OB_SUCCESS, filters.add(version_filter));

    ASSERT_NE(OB_SUCCESS, filters.add(version_filter));
    ObReplicaFilter* filter = NULL;
    ASSERT_NE(OB_SUCCESS, filters.add(*filter));
  }

  {
    ObReplicaFilterHolder filters;
    ASSERT_FALSE(filters.skip_empty_partition());
    ASSERT_EQ(OB_SUCCESS, filters.set_server(A));
    ASSERT_EQ(OB_SUCCESS, filters.set_zone("1"));
    ASSERT_EQ(OB_SUCCESS, filters.set_version(1));
    ASSERT_EQ(OB_SUCCESS, filters.set_valid_version());
    ASSERT_EQ(OB_SUCCESS, filters.set_server(A));
    ASSERT_EQ(OB_SUCCESS, filters.set_zone("1"));
    ASSERT_EQ(OB_SUCCESS, filters.set_version(1));
    ASSERT_EQ(OB_SUCCESS, filters.set_valid_version());
    ASSERT_TRUE(filters.skip_empty_partition());
    filters.reuse();
    ASSERT_FALSE(filters.skip_empty_partition());
    ASSERT_EQ(OB_SUCCESS, filters.set_server(A));
    ASSERT_EQ(OB_SUCCESS, filters.set_zone("1"));
    ASSERT_EQ(OB_SUCCESS, filters.set_version(1));
    ASSERT_EQ(OB_SUCCESS, filters.set_valid_version());
    ASSERT_EQ(OB_SUCCESS, filters.set_server(A));
    ASSERT_EQ(OB_SUCCESS, filters.set_zone("1"));
    ASSERT_EQ(OB_SUCCESS, filters.set_version(1));
    ASSERT_EQ(OB_SUCCESS, filters.set_valid_version());
    ASSERT_TRUE(filters.skip_empty_partition());
    ObPartitionReplica r;

    bool pass = false;
    ASSERT_EQ(OB_INVALID_ARGUMENT, filters.check(r, pass));

    r.table_id_ = combine_id(1, 1);
    r.partition_id_ = 0;
    r.partition_cnt_ = 1;
    r.server_ = A;
    r.zone_ = "xx";

    ASSERT_EQ(OB_SUCCESS, filters.check(r, pass));
    ASSERT_FALSE(pass);
    r.server_ = A;
    r.zone_ = "1";
    r.data_version_ = 1;
    ASSERT_EQ(OB_SUCCESS, filters.check(r, pass));
    ASSERT_TRUE(pass);

    {
      ObPartitionReplica r2;
      r2.assign(r);
      r2.server_ = B;
      ASSERT_EQ(OB_SUCCESS, filters.check(r2, pass));
      ASSERT_FALSE(pass);
    }

    {
      ObPartitionReplica r2;
      r2.assign(r);
      r2.zone_ = "2";
      ASSERT_EQ(OB_SUCCESS, filters.check(r2, pass));
      ASSERT_FALSE(pass);
    }

    {
      ObPartitionReplica r2;
      r2.assign(r);
      r2.data_version_ = 2;
      ASSERT_EQ(OB_SUCCESS, filters.check(r2, pass));
      ASSERT_FALSE(pass);
    }
  }
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
