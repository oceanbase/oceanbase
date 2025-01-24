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
#include "share/io/ob_io_manager.h"
#include "common/storage/ob_fd_simulator.h"
#define private public
#include "share/ob_device_manager.h"
#undef private

using namespace oceanbase::common;

//test fd simulator
class TestFdSimulator: public ::testing::Test
{
public:
  TestFdSimulator() {}
  virtual ~TestFdSimulator(){}
  virtual void SetUp()
  {
  }
  virtual void TearDown()
  {
  }
  static void SetUpTestCase()
  {
  }
  static void TearDownTestCase()
  {
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestFdSimulator);
};

TEST_F(TestFdSimulator, test_fd)
{
  ObFdSimulator fd_sim;
  ASSERT_EQ(OB_SUCCESS, fd_sim.init());
  int device_type = 0;
  int device_flag = 0;
  oceanbase::common::ObArenaAllocator allocator;
  void* ctx = NULL;
  int test_total_num = 200;
  int test_1_num = 90; //<100
  int test_2_num = 30; //90-110
  int test_3_num = 80; //200
  int used_fd_cnt = 0;
  int free_fd_cnt = 0;
  int expect_used_fd_cnt = 0;
  int expect_free_fd_cnt = 0;

  int tmp_device_type = 0;
  int tmp_device_flag = 0;
  void* tmp_ctx = NULL;
  //init fds
  ObIOFd* fds = static_cast<ObIOFd*>(allocator.alloc(sizeof(ObIOFd)*test_total_num));

  for(int i =0; i<test_total_num;i++) {
    new(fds+i)ObIOFd();
  }
  //get fd from simulator
  ASSERT_TRUE(NULL != fds);
  ctx = fds;
  for (int i = 0; i < test_1_num + test_2_num; i++) {
    device_type = i % 5; 
    device_flag = i % 2;
    ASSERT_EQ(OB_SUCCESS, fd_sim.get_fd(ctx, device_type, device_flag, fds[i]));
    ObFdSimulator::get_fd_device_type(fds[i], tmp_device_type);
    ObFdSimulator::get_fd_flag(fds[i], tmp_device_flag);
    ASSERT_EQ(tmp_device_type, device_type);
    ASSERT_EQ(tmp_device_flag, device_flag);
    fd_sim.get_fd_stat(used_fd_cnt, free_fd_cnt);
    ASSERT_EQ(i + 1, used_fd_cnt);
    if (i < ObFdSimulator::DEFAULT_ARRAY_SIZE) {
      ASSERT_EQ(free_fd_cnt, ObFdSimulator::DEFAULT_ARRAY_SIZE - used_fd_cnt);
    } else {
      ASSERT_EQ(free_fd_cnt, 2*ObFdSimulator::DEFAULT_ARRAY_SIZE - used_fd_cnt);
    }
    fd_sim.fd_to_ctx(fds[i], tmp_ctx);
    ASSERT_EQ(tmp_ctx, ctx);
  } 
  //release some fd(relase the fd which fd_id %3 == 0)
  for (int i = 0; i < test_1_num + test_2_num; i++) {
    if (i % 3 == 0) {
      ASSERT_EQ(OB_SUCCESS, fd_sim.release_fd(fds[i]));
      ASSERT_EQ(OB_NOT_INIT, fd_sim.release_fd(fds[i]));
    }
  }
  fd_sim.get_fd_stat(used_fd_cnt, free_fd_cnt);
  expect_used_fd_cnt = (test_1_num + test_2_num)*2/3;
  ASSERT_EQ(expect_used_fd_cnt, used_fd_cnt);
  ASSERT_EQ(free_fd_cnt, 2*ObFdSimulator::DEFAULT_ARRAY_SIZE - expect_used_fd_cnt);

  //get fd, util fd num is 200, check the fd
  for (int i = 0; i < test_1_num + test_2_num; i++) {
    if (i % 3 == 0) {
      ASSERT_EQ(OB_SUCCESS, fd_sim.get_fd(ctx, device_type, device_flag, fds[i]));
    }
  }

  for (int i = test_1_num + test_2_num; i < test_1_num + test_2_num + test_3_num; i++) {
    ASSERT_EQ(OB_SUCCESS, fd_sim.get_fd(ctx, device_type, device_flag, fds[i]));
  }
  fd_sim.get_fd_stat(used_fd_cnt, free_fd_cnt);
  ASSERT_EQ(used_fd_cnt, test_1_num + test_2_num + test_3_num);
  ASSERT_EQ(free_fd_cnt, 0);

  //release all the fd
  for (int i = 0; i < test_1_num + test_2_num + test_3_num; i++) {
    ASSERT_EQ(OB_SUCCESS, fd_sim.release_fd(fds[i]));
    ASSERT_EQ(OB_NOT_INIT, fd_sim.release_fd(fds[i]));
  }
  fd_sim.get_fd_stat(used_fd_cnt, free_fd_cnt);
  ASSERT_EQ(free_fd_cnt, test_1_num + test_2_num + test_3_num);
  ASSERT_EQ(used_fd_cnt, 0);
}

//test fd simulator
class TestDeviceManager: public ::testing::Test
{
public:
  TestDeviceManager() {
  }
  virtual ~TestDeviceManager(){}
  virtual void SetUp()
  {
  }
  virtual void TearDown()
  {
  }
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
  }
  static void TearDownTestCase()
  {
  }
protected:
  // disallow copy
private:
  DISALLOW_COPY_AND_ASSIGN(TestDeviceManager);
};

TEST_F(TestDeviceManager, test_device_manager)
{
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
  ObDeviceManager &manager = ObDeviceManager::get_instance();
  int max_dev_num = ObDeviceManager::MAX_DEVICE_INSTANCE;
  ObIODevice* device_handle[2*max_dev_num];
  ObString storage_prefix_local(OB_LOCAL_PREFIX);
  ObString storage_prefix_oss(OB_OSS_PREFIX);
  manager.destroy();
  ASSERT_EQ(OB_SUCCESS, manager.init_devices_env());
  
  int32_t device_num = 0;
  int32_t device_map_cnt = 0;
  ObIODevice* tmp_dev_handle = NULL;
  MEMSET(device_handle, 0 , sizeof(ObIODevice*)*2*max_dev_num);
  const ObStorageIdMod storage_id_mode(0, ObStorageUsedMod::STORAGE_USED_DATA);

  //all the device is same
  for (int i = 0; i < max_dev_num; i++ ) {
    ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_local_device(storage_prefix_local, storage_id_mode, device_handle[i]));
    if (0 != i) {
      ASSERT_EQ(tmp_dev_handle, device_handle[i]);
    } else {
      tmp_dev_handle = device_handle[i];
      ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().add_device_channel(device_handle[i],
                                                                           16/*async_channel_count*/,
                                                                           2/*sync_channel_count*/,
                                                                           1024/*max_io_depth*/));
    }
  }
  device_num = manager.get_device_cnt();
  ASSERT_EQ(1, device_num);
  //release all the device
  for (int i = 0; i < max_dev_num; i++) {
    ASSERT_EQ(OB_SUCCESS, manager.release_device(device_handle[i]));
  }
  device_num = manager.get_device_cnt();
  ASSERT_EQ(1, device_num); //since we do not release automatic

 //MAX_DEVICE_INSTANCE different deivce
  for (int i = 0; i < max_dev_num; i++ ) {
    ObObjectStorageInfo tmp_storage_info;
    tmp_storage_info.device_type_ = ObStorageType::OB_STORAGE_OSS;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_storage_info.access_id_,
                                          sizeof(tmp_storage_info.access_id_),
                                          "%d", i));
    ObStorageIdMod tmp_storage_id_mod(i, ObStorageUsedMod::STORAGE_USED_DATA);
    ASSERT_EQ(OB_SUCCESS, manager.get_device(storage_prefix_oss, tmp_storage_info,
                                             tmp_storage_id_mod, device_handle[i]));
    //all the device is not same 
    if (NULL != tmp_dev_handle) {
      ASSERT_TRUE(device_handle[i] != tmp_dev_handle);
    }
    tmp_dev_handle = device_handle[i];
  }
  device_num = manager.get_device_cnt();
  ASSERT_EQ(max_dev_num, device_num);
   
  //exceed MAX_DEVICE_INSTANCE device, should fail
  ObObjectStorageInfo max_storage_info;
  max_storage_info.device_type_ = ObStorageType::OB_STORAGE_OSS;
  ASSERT_EQ(OB_SUCCESS, databuff_printf(max_storage_info.access_id_,
                                        sizeof(max_storage_info.access_id_),
                                        "%d", max_dev_num));
  ObStorageIdMod max_storage_id_mod(max_dev_num, ObStorageUsedMod::STORAGE_USED_DATA);
  ASSERT_EQ(OB_OUT_OF_ELEMENT, manager.get_device(storage_prefix_oss, max_storage_info,
                                                  max_storage_id_mod, tmp_dev_handle));
  //release some and get again, should suc(this device ref should be 0)
  ASSERT_EQ(OB_SUCCESS, manager.release_device(device_handle[0]));
  //get this device again
  ObObjectStorageInfo min_storage_info;
  min_storage_info.device_type_ = ObStorageType::OB_STORAGE_OSS;
  ASSERT_EQ(OB_SUCCESS, databuff_printf(max_storage_info.access_id_,
                                        sizeof(max_storage_info.access_id_),
                                        "%d", 0));
  ObStorageIdMod min_storage_id_mod(0, ObStorageUsedMod::STORAGE_USED_DATA);
  ASSERT_EQ(OB_SUCCESS, manager.get_device(storage_prefix_oss, min_storage_info,
                                           min_storage_id_mod, device_handle[0]));
  //copy device handle, test double release scenario
  tmp_dev_handle = device_handle[0];
  ASSERT_EQ(OB_SUCCESS, manager.release_device(device_handle[0]));
  //double release scenario, since the ref is 0, can not release again
  ASSERT_EQ(OB_INVALID_ARGUMENT, manager.release_device(tmp_dev_handle));
  //the device handle has been reset, so will be a null pointer error
  ASSERT_EQ(OB_INVALID_ARGUMENT, manager.release_device(device_handle[0]));               
  ASSERT_EQ(OB_SUCCESS, manager.get_device(storage_prefix_oss, max_storage_info,
                                           max_storage_id_mod, device_handle[0]));
  device_num = manager.get_device_cnt();
  ASSERT_EQ(max_dev_num, device_num);
  manager.destroy();
  device_num = manager.get_device_cnt();
  ASSERT_EQ(0, device_num);
  ASSERT_EQ(OB_SUCCESS, manager.init_devices_env());

  //get again
  for (int i = 0; i < max_dev_num; i++ ) {
    uint64_t storage_id = 0;
    if ( i >= max_dev_num/2) {
      storage_id = i;
    }
    
    ObObjectStorageInfo tmp_storage_info;
    tmp_storage_info.device_type_ = ObStorageType::OB_STORAGE_OSS;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_storage_info.access_id_,
                                          sizeof(tmp_storage_info.access_id_),
                                          "%lu", storage_id));
    ObStorageIdMod tmp_storage_id_mod(storage_id, ObStorageUsedMod::STORAGE_USED_DATA);
    ASSERT_EQ(OB_SUCCESS, manager.get_device(storage_prefix_oss, tmp_storage_info,
                                             tmp_storage_id_mod, device_handle[i]));
  }
  device_num = manager.get_device_cnt();
  ASSERT_EQ(max_dev_num/2 + 1, device_num);


  manager.destroy();
  ASSERT_EQ(0, manager.get_device_cnt());
  ASSERT_EQ(OB_SUCCESS, manager.init_devices_env());
  // same storage_id_mod, different storage_info, expect different device
  ObStorageIdMod default_storage_id_mod;
  for (int i = 0; i < max_dev_num / 2; i++) {
    ObObjectStorageInfo tmp_storage_info;
    tmp_storage_info.device_type_ = ObStorageType::OB_STORAGE_OSS;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_storage_info.access_id_,
                                          sizeof(tmp_storage_info.access_id_),
                                          "%d", i));
    ASSERT_EQ(OB_SUCCESS, manager.get_device(storage_prefix_oss, tmp_storage_info,
                                             default_storage_id_mod, device_handle[i]));
    if (0 != i) {
      ASSERT_NE(tmp_dev_handle, device_handle[i]);
    } else {
      tmp_dev_handle = device_handle[i];
    }
  }
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_device_manager.log");
  OB_LOGGER.set_file_name("test_storage_device_manager.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
