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
#include "share/client_feedback/ob_client_feedback_manager.h"
#include "share/partition_table/ob_partition_location.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace obmysql;
using namespace std;

#define AS ASSERT_EQ(OB_SUCCESS, ret)
#define AF(x) ASSERT_EQ(x, ret)

TEST(ObPartitionInfo, common)
{
  ObFollowerFirstFeedback fff;
  fff.set_value(1000);

  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;

  INIT_SUCC(ret);
  pos = 0;
  ret = fff.serialize(buf, 2, pos);
  AF(OB_SIZE_OVERFLOW);
  ASSERT_EQ(0, pos);

  pos = 0;
  ret = fff.serialize(buf, LEN, pos);
  AS;
  ASSERT_TRUE(pos > 0);

  ObFollowerFirstFeedback fff2;
  pos = 0;
  ret = fff2.deserialize(buf, LEN, pos);
  AS;
  ASSERT_EQ(fff2.get_value(), fff.get_value());
  ASSERT_EQ(fff2.get_type(), fff.get_type());
}

TEST(ObFeedbackPartitionLocation, common)
{
  ObFeedbackPartitionLocation fpl;
  fpl.set_table_id(1);
  fpl.set_partition_id(1);
  fpl.set_schema_version(1);

  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;

  INIT_SUCC(ret);
  pos = 0;
  ret = fpl.serialize(buf, 2, pos);
  AF(OB_SIZE_OVERFLOW);
  ASSERT_EQ(0, pos);

  pos = 0;
  ret = fpl.serialize(buf, LEN, pos);
  AS;
  ASSERT_TRUE(pos > 0);

  ObFeedbackPartitionLocation fpl2;
  pos = 0;
  ret = fpl2.deserialize(buf, LEN, pos);
  AS;
  ASSERT_EQ(fpl2, fpl);


  fpl.reset();
  fpl.set_table_id(1);
  fpl.set_partition_id(1);
  fpl.set_schema_version(1);

  ObFeedbackReplicaLocation replica;
  replica.role_ = LEADER;
  replica.replica_type_ = REPLICA_TYPE_FULL;
  replica.server_.set_ip_addr("1.1.1.1", 111);

  ret = fpl.add_replica(replica);
  AS;

  replica.role_ = FOLLOWER;
  replica.replica_type_ = REPLICA_TYPE_BACKUP;
  replica.server_.set_ip_addr("2.2.2.2", 2222);
  ret = fpl.add_replica(replica);
  AS;
  pos = 0;
  ret = fpl.serialize(buf, LEN, pos);
  AS;
  ASSERT_TRUE(pos > 0);

  pos = 0;
  ret =  fpl2.deserialize(buf, LEN, pos);
  AS;

  ASSERT_EQ(fpl2, fpl);
}

TEST(ObFeedbackManager, common)
{
  INIT_SUCC(ret);
  ObFeedbackManager fbm;

  ObFeedbackPartitionLocation fpl;
  fpl.set_table_id(1);
  fpl.set_partition_id(1);
  fpl.set_schema_version(4);
  ObFeedbackReplicaLocation replica;
  replica.role_ = LEADER;
  replica.replica_type_ = REPLICA_TYPE_FULL;
  replica.server_.set_ip_addr("1.1.1.1", 111);
  ret = fpl.add_replica(replica);
  AS;
  replica.role_ = FOLLOWER;
  replica.replica_type_ = REPLICA_TYPE_BACKUP;
  replica.server_.set_ip_addr("2.2.2.2", 2222);
  ret = fpl.add_replica(replica);
  AS;

  ObPartitionLocation pl;
  pl.set_table_id(1);
  pl.set_partition_id(1);
  ObReplicaLocation replica2;
  replica2.role_ = LEADER;
  replica2.replica_type_ = REPLICA_TYPE_FULL;
  replica2.server_.set_ip_addr("1.1.1.1", 111);
  replica2.sql_port_ = 111;
  ret = pl.add(replica2);
  AS;
  replica2.role_ = FOLLOWER;
  replica2.replica_type_ = REPLICA_TYPE_BACKUP;
  replica2.server_.set_ip_addr("2.2.2.2", 2222);
  replica2.sql_port_ = 222;
  ret = pl.add(replica2);
  AS;

  ObFBPartitionParam param;
  param.schema_version_ = 1;
  param.original_partition_id_ = 111111;
  param.pl_.assign(pl);
  ret = fbm.add_partition_fb_info(param);
  AS;
  param.schema_version_ = 2;
  ret = fbm.add_partition_fb_info(param); // override
  AS;
  param.schema_version_ = 3;
  ret = fbm.add_partition_fb_info(param); // override
  AS;
  param.schema_version_ = 4;
  ret = fbm.add_partition_fb_info(param); // override
  AS;

  ret = fbm.add_follower_first_fb_info(FFF_HIT_LEADER);
  AS;
  ret = fbm.add_follower_first_fb_info(FFF_HIT_LEADER); // override
  AS;
  ret = fbm.add_follower_first_fb_info(FFF_HIT_LEADER); // override
  AS;
  ret = fbm.add_follower_first_fb_info(FFF_HIT_LEADER); // override
  AS;
  ret = fbm.add_follower_first_fb_info(FFF_HIT_LEADER); // override
  AS;

  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;

  ret = fbm.serialize(buf, 10, pos);
  AF(OB_SIZE_OVERFLOW);
  ASSERT_EQ(0, pos);

  pos = 0;
  ret = fbm.serialize(buf, LEN, pos);
  AS;
  ASSERT_TRUE(pos > 0);
  int64_t seri_len = pos;

  ObFeedbackManager fbm2;
  pos = 0;

  ret = fbm2.deserialize(buf, 10, pos);
  AF(OB_INVALID_DATA);
  ASSERT_EQ(0, pos);

  ret = fbm2.deserialize(buf, 9, pos);
  AF(OB_INVALID_DATA);
  ASSERT_EQ(0, pos);

  ret = fbm2.deserialize(buf, 8, pos);
  AF(OB_INVALID_DATA);
  ASSERT_EQ(0, pos);

  ret = fbm2.deserialize(buf, 7, pos);
  AF(OB_INVALID_DATA);
  ASSERT_EQ(0, pos);

  ret = fbm2.deserialize(buf, 6, pos);
  AF(OB_INVALID_DATA);
  ASSERT_EQ(0, pos);

  ret = fbm2.deserialize(buf, 5, pos);
  AF(OB_INVALID_DATA);
  ASSERT_EQ(0, pos);

  ret = fbm2.deserialize(buf, 4, pos);
  AF(OB_SIZE_OVERFLOW);
  ASSERT_EQ(0, pos);

  // just right deseri FollowerFirstFeedback
  ret = fbm2.deserialize(buf, 3, pos);
  AS;
  ASSERT_TRUE(pos > 0);
  pos = 0;

  ret = fbm2.deserialize(buf, 2, pos);
  AF(OB_INVALID_DATA);
  ASSERT_EQ(0, pos);

  ret = fbm2.deserialize(buf, 1, pos);
  AF(OB_SIZE_OVERFLOW);
  ASSERT_EQ(0, pos);

  ret = fbm2.deserialize(buf, 0, pos);
  AF(OB_INVALID_ARGUMENT);
  ASSERT_EQ(0, pos);

  ret = fbm2.deserialize(buf, seri_len, pos);
  AS;

  ASSERT_EQ(*(fbm2.get_follower_first_feedback()), *(fbm.get_follower_first_feedback()));
  ASSERT_EQ(*(fbm2.get_pl_feedback()), *(fbm.get_pl_feedback()));
}

class ObForwardCompatibilityFeedbackPartitionLocation : public ObAbstractFeedbackObject<ObForwardCompatibilityFeedbackPartitionLocation>
{
public:
  ObForwardCompatibilityFeedbackPartitionLocation() : ObAbstractFeedbackObject<ObForwardCompatibilityFeedbackPartitionLocation>(PARTITION_LOCATION_FB_ELE)
  {}
  virtual ~ObForwardCompatibilityFeedbackPartitionLocation() {}
  FB_OBJ_DEFINE_METHOD;

public:
  uint64_t table_id_;
  int64_t partition_id_;
};

int ObForwardCompatibilityFeedbackPartitionLocation::serialize_struct_content(char *buf, const int64_t len, int64_t &pos) const
{
  OB_FB_SER_START;
  OB_FB_ENCODE_INT(table_id_);
  OB_FB_ENCODE_INT(partition_id_);
  OB_FB_SER_END;
}

inline bool ObForwardCompatibilityFeedbackPartitionLocation::is_valid_obj() const
{
  return true;
}

int ObForwardCompatibilityFeedbackPartitionLocation::deserialize_struct_content(char *buf, const int64_t len, int64_t &pos)
{
  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);
  return OB_SUCCESS;
}

TEST(ObFeedbackPartitionLocation, forward_compatibility)
{
  ObForwardCompatibilityFeedbackPartitionLocation fcfpl;
  fcfpl.table_id_ = 2;
  fcfpl.partition_id_ = 3;

  INIT_SUCC(ret);
  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  ret = fcfpl.serialize(buf, LEN, pos);
  AS;

  ObFeedbackPartitionLocation fpl;
  int64_t pos2 = 0;
  ret = fpl.deserialize(buf, LEN, pos2);
  AS;

  ASSERT_EQ(pos, pos2);

  ASSERT_EQ(fcfpl.table_id_, fpl.get_table_id());
  ASSERT_EQ(fcfpl.partition_id_, fpl.get_partition_id());
  ASSERT_EQ(fpl.get_schema_version(), 0);
  ASSERT_EQ(fpl.get_replica_array()->count(), 0);
}

class ObBackwardCompatibilityFeedbackPartitionLocation : public ObAbstractFeedbackObject<ObBackwardCompatibilityFeedbackPartitionLocation>
{
public:
  ObBackwardCompatibilityFeedbackPartitionLocation() : ObAbstractFeedbackObject<ObBackwardCompatibilityFeedbackPartitionLocation>(PARTITION_LOCATION_FB_ELE)
  {}
  virtual ~ObBackwardCompatibilityFeedbackPartitionLocation() {}
  FB_OBJ_DEFINE_METHOD;

public:
  typedef common::ObSEArray<ObFeedbackReplicaLocation, 5> ObTestFeedbackReplicaLocationArray;
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t schema_version_;
  ObTestFeedbackReplicaLocationArray replicas_;
  int64_t test_1_;
  int64_t test_2_;
};

int ObBackwardCompatibilityFeedbackPartitionLocation::serialize_struct_content(char *buf, const int64_t len, int64_t &pos) const
{
  OB_FB_SER_START;
  OB_FB_ENCODE_INT(table_id_);
  OB_FB_ENCODE_INT(partition_id_);
  OB_FB_ENCODE_INT(schema_version_);
  OB_FB_ENCODE_STRUCT_ARRAY(replicas_, replicas_.count());
  OB_FB_ENCODE_INT(test_1_);
  OB_FB_ENCODE_INT(test_2_);
  OB_FB_SER_END;
}

inline bool ObBackwardCompatibilityFeedbackPartitionLocation::is_valid_obj() const
{
  return true;
}

int ObBackwardCompatibilityFeedbackPartitionLocation::deserialize_struct_content(char *buf, const int64_t len, int64_t &pos)
{
  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);
  return OB_SUCCESS;
}

TEST(ObFeedbackPartitionLocation, backward_compatibility)
{
  ObBackwardCompatibilityFeedbackPartitionLocation bcfpl;
  bcfpl.table_id_ = 22222222;
  bcfpl.partition_id_ = 33333333;
  bcfpl.schema_version_ = 55555555;
  bcfpl.test_1_ = 66666;
  bcfpl.test_2_ = 77777;

  ObFeedbackReplicaLocation replica;
  replica.role_ = LEADER;
  replica.replica_type_ = REPLICA_TYPE_FULL;
  replica.server_.set_ip_addr("1.1.1.1", 111);
  INIT_SUCC(ret);
  ret = bcfpl.replicas_.push_back(replica);
  AS;
  replica.role_ = FOLLOWER;
  replica.replica_type_ = REPLICA_TYPE_BACKUP;
  replica.server_.set_ip_addr("2.2.2.2", 2222);
  ret = bcfpl.replicas_.push_back(replica);
  AS;

  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  ret = bcfpl.serialize(buf, LEN, pos);
  AS;

  ObFeedbackPartitionLocation fpl;
  int64_t pos2 = 0;
  ret = fpl.deserialize(buf, LEN, pos2);
  AS;

  ASSERT_EQ(pos, pos2);

  ASSERT_EQ(bcfpl.table_id_, fpl.get_table_id());
  ASSERT_EQ(bcfpl.partition_id_, fpl.get_partition_id());
  ASSERT_EQ(bcfpl.schema_version_, fpl.get_schema_version());
  ASSERT_EQ(bcfpl.replicas_.count(), fpl.get_replica_array()->count());
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
