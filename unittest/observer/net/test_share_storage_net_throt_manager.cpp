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
#include <gmock/gmock.h>
#define private public
#include "observer/net/ob_shared_storage_net_throt_service.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;

namespace rootserver
{
class TestSSNTService : public testing::Test
{
public:
  TestSSNTService()
  {}
  virtual ~TestSSNTService()
  {}
  virtual void SetUp(){};
  virtual void TearDown(){};
  virtual void TestBody(){};
};

TEST_F(TestSSNTService, SSNT_service_test)
{
  int ret = OB_SUCCESS;
  ObSharedStorageNetThrotManager ssntm;
  ret = ssntm.init();
  EXPECT_EQ(ret, OB_SUCCESS);
  // register service
  int64_t time = ObTimeUtil::current_time();
  int64_t expire_time = time + 10 * 3L * 1000L * 1000L;

  ObAddr addr1(1, 1);
  common::ObSEArray<ObTrafficControl::ObStorageKey, 1> storage_ids_1;
  storage_ids_1.push_back(ObTrafficControl::ObStorageKey(1, 1, ObStorageInfoType::ALL_ZONE_STORAGE));
  storage_ids_1.push_back(ObTrafficControl::ObStorageKey(1, 2, ObStorageInfoType::ALL_ZONE_STORAGE));
  storage_ids_1.push_back(ObTrafficControl::ObStorageKey(2, 1, ObStorageInfoType::ALL_ZONE_STORAGE));
  storage_ids_1.push_back(ObTrafficControl::ObStorageKey(2, 2, ObStorageInfoType::ALL_ZONE_STORAGE));
  ObSSNTEndpointArg arg1(addr1, storage_ids_1, expire_time);
  ret = ssntm.register_endpoint(arg1);
  // register_endpoint second time
  ret = ssntm.register_endpoint(arg1);
  EXPECT_EQ(ret, OB_SUCCESS);
  // get endpoint value
  ObEndpointInfos infos;
  ret = ssntm.endpoint_infos_map_.get_refactored(addr1, infos);
  LOG_WARN("register endpoint1", K(addr1), K(infos));
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_EQ(infos.expire_time_, expire_time);
  ObSharedDeviceResourceArray predicted_resource;
  ObTrafficControl::ObStorageKey key[4];
  key[0] = ObTrafficControl::ObTrafficControl::ObStorageKey(1, 1, ObStorageInfoType::ALL_ZONE_STORAGE);
  key[1] = ObTrafficControl::ObTrafficControl::ObStorageKey(1, 2, ObStorageInfoType::ALL_ZONE_STORAGE);
  key[2] = ObTrafficControl::ObTrafficControl::ObStorageKey(2, 1, ObStorageInfoType::ALL_ZONE_STORAGE);
  key[3] = ObTrafficControl::ObTrafficControl::ObStorageKey(2, 2, ObStorageInfoType::ALL_ZONE_STORAGE);

  for (int i = 0; i < ResourceType::ResourceTypeCnt; ++i) {
    for (int j = 0; j < 4; ++j) {
      ObSharedDeviceResource rsc(key[j], static_cast<obrpc::ResourceType>(i), (i + 1) * 10 * (j + 1));
      predicted_resource.array_.push_back(rsc);
    }
  }

  ret = ssntm.register_or_update_predict_resource(addr1, &predicted_resource, expire_time);
  EXPECT_EQ(ret, OB_SUCCESS);
  ObQuotaPlanMap *quota_ptr1 = nullptr;
  ObQuotaPlanMap *quota_ptr2 = nullptr;
  ret = ssntm.bucket_throt_map_.get_refactored(
      ObTrafficControl::ObStorageKey(1, 1, ObStorageInfoType::ALL_ZONE_STORAGE), quota_ptr1);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.bucket_throt_map_.get_refactored(
      ObTrafficControl::ObStorageKey(1, 2, ObStorageInfoType::ALL_ZONE_STORAGE), quota_ptr2);
  EXPECT_EQ(ret, OB_SUCCESS);
  int64_t max_iops = 1000;
  int64_t max_bandwidth = 1000;
  ObSSNTKey ssntkey[6];
  ObSSNTValue *ssntvalue[6];
  ssntkey[0] = ObSSNTKey(addr1, key[0]);
  ssntkey[1] = ObSSNTKey(addr1, key[1]);
  ssntkey[2] = ObSSNTKey(addr1, key[2]);
  ssntkey[3] = ObSSNTKey(addr1, key[3]);

  ret = ssntm.cal_iops_quota(max_iops, quota_ptr1);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_iops_quota(max_iops, quota_ptr2);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_bw_quota(max_bandwidth, &ObSSNTResource::obw_, quota_ptr1);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_bw_quota(max_bandwidth, &ObSSNTResource::obw_, quota_ptr2);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_bw_quota(max_bandwidth, &ObSSNTResource::ibw_, quota_ptr1);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_bw_quota(max_bandwidth, &ObSSNTResource::ibw_, quota_ptr2);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_iops_quota(max_bandwidth, quota_ptr1);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_iops_quota(max_bandwidth, quota_ptr2);
  EXPECT_EQ(ret, OB_SUCCESS);

  quota_ptr1->get_refactored(ssntkey[0], ssntvalue[0]);
  quota_ptr2->get_refactored(ssntkey[1], ssntvalue[2]);

  EXPECT_EQ(ssntvalue[0]->assigned_resource_.ops_, 490);
  EXPECT_EQ(ssntvalue[0]->assigned_resource_.ips_, 510);
  EXPECT_EQ(ssntvalue[0]->assigned_resource_.iops_, 1000);
  EXPECT_EQ(ssntvalue[0]->assigned_resource_.obw_, 1000);
  EXPECT_EQ(ssntvalue[0]->assigned_resource_.ibw_, 1000);
  // for (int i = 0; i < 2; ++i) {
  //   LOG_WARN("key1 assigned_resource", K(ssntkey[i]), K(*(ssntvalue[i])));
  // }

  // for (auto it = ssntm.bucket_throt_map_.begin(); it != ssntm.bucket_throt_map_.end(); ++it) {
  //   for (auto it2 = it->second->begin(); it2 != it->second->end(); ++it2) {
  //     LOG_WARN("bucket_throt_map_ have", K(it->first), K(it2->first), K(*(it2->second)));
  //   }
  // }
  LOG_WARN("before clear", K(ssntm.endpoint_infos_map_.size()), K(ssntm.bucket_throt_map_.size()));

  ObAddr addr2(2, 2);
  common::ObSEArray<ObTrafficControl::ObStorageKey, 1> storage_ids_2;
  storage_ids_2.push_back(ObTrafficControl::ObStorageKey(1, 1, ObStorageInfoType::ALL_ZONE_STORAGE));
  storage_ids_2.push_back(ObTrafficControl::ObStorageKey(1, 2, ObStorageInfoType::ALL_ZONE_STORAGE));
  storage_ids_2.push_back(ObTrafficControl::ObStorageKey(2, 1, ObStorageInfoType::ALL_ZONE_STORAGE));
  storage_ids_2.push_back(ObTrafficControl::ObStorageKey(2, 2, ObStorageInfoType::ALL_ZONE_STORAGE));
  ObSSNTEndpointArg arg2(addr2, storage_ids_2, expire_time);
  ret = ssntm.register_endpoint(arg2);
  ObSharedDeviceResourceArray predicted_resource_2;
  ObTrafficControl::ObStorageKey key2 = ObTrafficControl::ObTrafficControl::ObStorageKey(1, 1, ObStorageInfoType::ALL_ZONE_STORAGE);
  ObTrafficControl::ObStorageKey key2_2 = ObTrafficControl::ObTrafficControl::ObStorageKey(1, 2, ObStorageInfoType::ALL_ZONE_STORAGE);
  for (int i = 0; i < ResourceType::ResourceTypeCnt; ++i) {
    ObSharedDeviceResource rsc;
    ObSharedDeviceResource rsc2;
    if (ResourceType::ResourceTypeCnt == 2) {
      rsc = ObSharedDeviceResource(key2, static_cast<obrpc::ResourceType>(i), 2000);
      rsc2 = ObSharedDeviceResource(key2_2, static_cast<obrpc::ResourceType>(i), 200);
    } else {
      rsc = ObSharedDeviceResource(key2, static_cast<obrpc::ResourceType>(i), 1000);
      rsc2 = ObSharedDeviceResource(key2_2, static_cast<obrpc::ResourceType>(i), 10);
    }
    predicted_resource_2.array_.push_back(rsc);
    predicted_resource_2.array_.push_back(rsc2);
  }
  ret = ssntm.register_or_update_predict_resource(addr2, &predicted_resource_2, expire_time);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.bucket_throt_map_.get_refactored(
      ObTrafficControl::ObStorageKey(1, 1, ObStorageInfoType::ALL_ZONE_STORAGE), quota_ptr1);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.bucket_throt_map_.get_refactored(
      ObTrafficControl::ObStorageKey(1, 2, ObStorageInfoType::ALL_ZONE_STORAGE), quota_ptr2);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_iops_quota(max_iops, quota_ptr1);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_iops_quota(max_iops, quota_ptr2);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_bw_quota(max_bandwidth, &ObSSNTResource::obw_, quota_ptr1);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_bw_quota(max_bandwidth, &ObSSNTResource::obw_, quota_ptr2);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_bw_quota(max_bandwidth, &ObSSNTResource::ibw_, quota_ptr1);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_bw_quota(max_bandwidth, &ObSSNTResource::ibw_, quota_ptr2);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_iops_quota(max_bandwidth, quota_ptr1);
  EXPECT_EQ(ret, OB_SUCCESS);
  ret = ssntm.cal_iops_quota(max_bandwidth, quota_ptr2);
  EXPECT_EQ(ret, OB_SUCCESS);

  ssntkey[4] = ObSSNTKey(addr2, key2);
  ssntkey[5] = ObSSNTKey(addr2, key2_2);

  // quota_ptr1 (1, 1), quota_ptr2 (1, 2)
  quota_ptr1->get_refactored(ssntkey[0], ssntvalue[0]);
  quota_ptr2->get_refactored(ssntkey[1], ssntvalue[1]);
  quota_ptr1->get_refactored(ssntkey[2], ssntvalue[2]);
  quota_ptr2->get_refactored(ssntkey[3], ssntvalue[3]);
  quota_ptr1->get_refactored(ssntkey[4], ssntvalue[4]);
  quota_ptr2->get_refactored(ssntkey[5], ssntvalue[5]);
  EXPECT_EQ(ssntvalue[0]->assigned_resource_.ops_, 250);
  EXPECT_EQ(ssntvalue[0]->assigned_resource_.ips_, 270);
  EXPECT_EQ(ssntvalue[0]->assigned_resource_.iops_, 520);
  EXPECT_EQ(ssntvalue[0]->assigned_resource_.obw_, 535);
  EXPECT_EQ(ssntvalue[0]->assigned_resource_.ibw_, 545);

  EXPECT_EQ(ssntvalue[1]->assigned_resource_.ops_, 250);
  EXPECT_EQ(ssntvalue[1]->assigned_resource_.ips_, 270);
  EXPECT_EQ(ssntvalue[1]->assigned_resource_.iops_, 520);
  EXPECT_EQ(ssntvalue[1]->assigned_resource_.obw_, 535);
  EXPECT_EQ(ssntvalue[1]->assigned_resource_.ibw_, 545);

  for (int i = 0; i < 2; ++i) {
    LOG_WARN("key2 assigned_resource", K(ssntkey[i]), K(*(ssntvalue[i])));
  }

  // test for clear expired endpoint
  LOG_WARN("before clear", K(ssntm.endpoint_infos_map_.size()), K(ssntm.bucket_throt_map_.size()));
  sleep(40);
  ssntm.clear_expired_infos();
  LOG_WARN("after clear", K(ssntm.endpoint_infos_map_.size()), K(ssntm.bucket_throt_map_.size()));
  ssntm.destroy();
}

}  // namespace rootserver
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#undef private