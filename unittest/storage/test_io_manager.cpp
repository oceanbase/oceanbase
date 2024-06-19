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

#define USING_LOG_PREFIX COMMON

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#include "share/io/ob_io_define.h"
#include "share/io/ob_io_manager.h"
#include "share/io/ob_io_calibration.h"
#include "share/io/io_schedule/ob_io_mclock.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#undef private
#include "share/ob_local_device.h"
#include "lib/thread/thread_pool.h"
#include "lib/file/file_directory_utils.h"
#include "common/ob_clock_generator.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/ob_tmp_file_cache.h"
#include "storage/blocksstable/ob_tmp_file_store.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"

#define ASSERT_SUCC(ret) ASSERT_EQ((ret), ::oceanbase::common::OB_SUCCESS)
#define ASSERT_FAIL(ret) ASSERT_NE((ret), ::oceanbase::common::OB_SUCCESS)

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

#define TEST_ROOT_DIR "./"
#define TEST_DATA_DIR TEST_ROOT_DIR "/data_dir"
#define TEST_SSTABLE_DIR TEST_DATA_DIR "/sstable"

static const int64_t IO_MEMORY_LIMIT = 10L * 1024L * 1024L * 1024L;
static const uint64_t TEST_TENANT_ID = 1;

int init_device(const int64_t media_id, ObLocalDevice &device)
{
  int ret = OB_SUCCESS;
  const int64_t IO_OPT_COUNT = 6;
  const int64_t block_size = 1024L * 1024L * 2L; // 2MB
  const int64_t data_disk_size = 1024L * 1024L * 1024L; // 1GB
  const int64_t data_disk_percentage = 50L;
  ObIODOpt io_opts[IO_OPT_COUNT];
  io_opts[0].key_ = "data_dir";                   io_opts[0].value_.value_str = oceanbase::MockTenantModuleEnv::get_instance().storage_env_.data_dir_;
  io_opts[1].key_ = "sstable_dir";                io_opts[1].value_.value_str = oceanbase::MockTenantModuleEnv::get_instance().storage_env_.sstable_dir_;
  io_opts[2].key_ = "block_size";                 io_opts[2].value_.value_int64 = block_size;
  io_opts[3].key_ = "datafile_disk_percentage";   io_opts[3].value_.value_int64 = data_disk_percentage;
  io_opts[4].key_ = "datafile_size";              io_opts[4].value_.value_int64 = data_disk_size;
  io_opts[5].key_ = "media_id";                   io_opts[5].value_.value_int64 = media_id;
  ObIODOpts init_opts;
  init_opts.opts_ = io_opts;
  init_opts.opt_cnt_ = IO_OPT_COUNT;
  bool need_format = false;
  if (OB_FAIL(device.init(init_opts))) {
    LOG_WARN("init device failed", K(ret));
  } else {
    int64_t reserved_size = 0;
    ObIODOpts opts_start;
    ObIODOpt opt_start;
    opts_start.opts_ = &(opt_start);
    opts_start.opt_cnt_ = 1;
    opt_start.set("reserved size", reserved_size);
    if (OB_FAIL(device.start(opts_start))) {
      LOG_WARN("start device failed", K(ret));
    }
  }
  return ret;
}

class TestIOStruct : public ::testing::Test
{
public:
static void SetUpTestCase()
{
  // prepare test directory and file
  system("mkdir -p " TEST_DATA_DIR);
  system("mkdir -p " TEST_SSTABLE_DIR);

  ASSERT_SUCC(oceanbase::MockTenantModuleEnv::get_instance().init());
  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1002);
  ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(OB_SERVER_TENANT_ID, 0)->set_limit(IO_MEMORY_LIMIT);
}

static void TearDownTestCase()
{
  ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  ObMallocAllocator::get_instance()->recycle_tenant_allocator(1002);
}

static ObIOInfo get_random_io_info()
{
  ObIOInfo io_info;
  const int64_t max_group_num = 0; //不包括OTHER GROUPS的其他group
  io_info.tenant_id_ = OB_SERVER_TENANT_ID;
  io_info.fd_.first_id_ = ObRandom::rand(0, 10000);
  io_info.fd_.second_id_ = ObRandom::rand(0, 10000);
  io_info.flag_.set_mode(static_cast<ObIOMode>(ObRandom::rand(0, (int)ObIOMode::MAX_MODE - 1)));
  io_info.flag_.set_resource_group_id(USER_RESOURCE_OTHER_GROUP_ID); // 0 means default
  io_info.flag_.set_wait_event(ObRandom::rand(1, 9999));
  io_info.timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  io_info.offset_ = ObRandom::rand(1, 1000L * 1000L * 1000L);
  io_info.size_ = ObRandom::rand(1, 1000L * 10L);
  io_info.flag_.set_read();
  io_info.user_data_buf_ = static_cast<char *>(ob_malloc(io_info.size_, ObNewModIds::TEST));
  return io_info;
}

static ObTenantIOConfig default_tenant_io_config()
{
  ObTenantIOConfig tenant_config;
  tenant_config.callback_thread_count_ = 2;
  tenant_config.memory_limit_ = 1024L * 1024L * 1024L;
  tenant_config.group_num_ = 0;
  tenant_config.unit_config_.min_iops_ = 1000;
  tenant_config.unit_config_.max_iops_ = 1000;
  tenant_config.unit_config_.weight_ = 1000;
  tenant_config.other_group_config_.min_percent_ = 100;
  tenant_config.other_group_config_.max_percent_ = 100;
  tenant_config.other_group_config_.weight_percent_ = 100;
  tenant_config.group_config_change_ = false;
  return tenant_config;
}
};

class TestIOCallback : public ObIOCallback
{
public:
  TestIOCallback()
    : number_(nullptr), allocator_(nullptr), help_buf_(nullptr)
  {}
  virtual ~TestIOCallback();
  virtual const char *get_data() override { return (char *)help_buf_; }
  virtual int64_t size() const override { return sizeof(TestIOCallback); }
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override;
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  virtual ObIAllocator *get_allocator() override { return allocator_; }
  TO_STRING_KV(KP(number_), KP(allocator_), KP(help_buf_));

public:
  int64_t *number_;
  ObIAllocator *allocator_;
  char *help_buf_;
};

TEST_F(TestIOStruct, IOFlag)
{
  // default invalid
  ObIOFlag flag;
  ObIOFlag flag2;
  ASSERT_FALSE(flag.is_valid());

  // normal usage
  flag.set_mode(ObIOMode::READ);
  flag.set_resource_group_id(USER_RESOURCE_OTHER_GROUP_ID);
  flag.set_wait_event(99);
  ASSERT_TRUE(flag.is_valid());

  // test io mode
  flag2 = flag;
  ASSERT_TRUE(flag2.is_valid());
  flag2.set_mode(ObIOMode::MAX_MODE);
  ASSERT_FALSE(flag2.is_valid());
  flag2.set_mode((ObIOMode)88);
  ASSERT_FALSE(flag2.is_valid());

  // test wait event number
  flag2 = flag;
  ASSERT_TRUE(flag2.is_valid());
  flag2.set_wait_event(0);
  ASSERT_FALSE(flag2.is_valid());
  flag2.set_wait_event(-100);
  ASSERT_FALSE(flag2.is_valid());

  // test reset
  flag2 = flag;
  ASSERT_TRUE(flag2.is_valid());
  flag2.reset();
  ASSERT_FALSE(flag2.is_valid());
}

TEST_F(TestIOStruct, IOInfo)
{
  // default invalid
  ObIOInfo info;
  ASSERT_FALSE(info.is_valid());

  // normal usage
  ObIOFd fd;
  fd.first_id_ = 0;
  fd.second_id_ = 0;
  info.tenant_id_ = OB_SERVER_TENANT_ID;
  info.fd_ = fd;
  info.flag_.set_mode(ObIOMode::READ);
  info.flag_.set_resource_group_id(USER_RESOURCE_OTHER_GROUP_ID);
  info.flag_.set_wait_event(1);
  info.timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  info.offset_ = 80;
  info.size_ = 1;
  char user_buf[1] = { 0 };
  info.user_data_buf_ = user_buf;
  ASSERT_TRUE(info.is_valid());

  // write io info require write buf
  info.flag_.set_mode(ObIOMode::WRITE);
  ASSERT_FALSE(info.is_valid());
  char tmp_buf[10] = { 0 };
  info.buf_ = tmp_buf;
  ASSERT_TRUE(info.is_valid());

  // test reset
  info.reset();
  ASSERT_FALSE(info.is_valid());
}

TEST_F(TestIOStruct, IOHandle)
{
  ObIOResult result;
  ObIORequest req;
  req.inc_ref(); // prevent from free
  result.inc_ref();
  req.set_result(result);
  ASSERT_EQ(1, req.ref_cnt_);
  ASSERT_EQ(2, result.result_ref_cnt_);
  ASSERT_EQ(0, result.out_ref_cnt_);
  // default invalid
  ObIOHandle handle;
  ASSERT_FALSE(handle.is_valid());
  ASSERT_TRUE(handle.is_empty());

  // normal usage
  handle.set_result(result);
  ASSERT_TRUE(handle.is_valid());
  ASSERT_FALSE(handle.is_empty());
  ASSERT_EQ(3, result.result_ref_cnt_);
  ASSERT_EQ(1, result.out_ref_cnt_);

  // copy assign
  ObIOHandle handle2 = handle;
  ASSERT_EQ(4, result.result_ref_cnt_);
  ASSERT_EQ(2, result.out_ref_cnt_);

  // reset
  handle2.reset();
  ASSERT_EQ(3, result.result_ref_cnt_);
  ASSERT_EQ(1, result.out_ref_cnt_);
  ASSERT_FALSE(handle2.is_valid());
  ASSERT_TRUE(handle2.is_empty());

  // test log
  LOG_INFO("test log io handle", K(handle), K(handle2));
}

TEST_F(TestIOStruct, IOAllocator)
{
  ObIOAllocator allocator;
  ASSERT_FALSE(allocator.is_inited_);

  // init
  const int64_t memory_limit = 1024L * 1024L * 1024L; // 1GB
  ASSERT_SUCC(allocator.init(TEST_TENANT_ID, memory_limit));
  ASSERT_TRUE(allocator.is_inited_);

  // alloc and free memory
  void *buf = allocator.alloc(1000);
  ASSERT_NE(buf, nullptr);
  allocator.free(buf);

  // alloc and free object
  ObIORequest *req = nullptr;
  allocator.alloc(req);
  ASSERT_NE(req, nullptr);
  allocator.free(req);

  // destroy
  allocator.destroy();
  ASSERT_FALSE(allocator.is_inited_);
}

TEST_F(TestIOStruct, IORequest)
{
  ObRefHolder<ObTenantIOManager> holder;
  OB_IO_MANAGER.get_tenant_io_manager(OB_SERVER_TENANT_ID, holder);
  ObTenantIOManager &tenant_io_mgr = *(holder.get_ptr());
  ObIOFd fd;
  fd.first_id_ = 0;
  fd.second_id_ = 1;

  // default invalid
  ObIOResult result;
  ASSERT_FALSE(result.is_inited_);
  ASSERT_FALSE(result.is_valid());
  ObIORequest req;
  ASSERT_FALSE(req.is_inited_);
  ASSERT_FALSE(req.is_valid());


  // prepare read request
  req.destroy();
  result.destroy();
  req.tenant_io_mgr_.hold(&tenant_io_mgr);
  result.tenant_io_mgr_.hold(&tenant_io_mgr);
  result.inc_ref();
  req.inc_ref();

  ObIOInfo read_info;
  read_info.tenant_id_ = OB_SERVER_TENANT_ID;
  read_info.fd_ = fd;
  read_info.flag_.set_mode(ObIOMode::READ);
  read_info.flag_.set_resource_group_id(USER_RESOURCE_OTHER_GROUP_ID);
  read_info.flag_.set_wait_event(1);
  read_info.timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  read_info.offset_ = 89;
  read_info.size_ = 1;
  char user_buf[1] = { 0 };
  read_info.user_data_buf_ = user_buf;

  ASSERT_TRUE(read_info.is_valid());
  ASSERT_SUCC(req.tenant_io_mgr_.get_ptr()->io_usage_.init(0));
  ASSERT_FAIL(result.init(read_info)); //not init cond yet
  ASSERT_FALSE(result.is_inited_);

  ASSERT_SUCC(result.basic_init());
  ASSERT_SUCC(result.init(read_info));
  ASSERT_SUCC(req.init(read_info ,&result));
  ASSERT_TRUE(req.is_inited_);
  ASSERT_TRUE(result.is_inited_);
  ASSERT_TRUE(req.is_valid());
  ASSERT_TRUE(result.is_valid());

  ASSERT_EQ(req.raw_buf_, nullptr); // read buf allocation is delayed
  ASSERT_SUCC(req.prepare());
  ASSERT_NE(req.raw_buf_, nullptr);
  ASSERT_EQ(2, result.result_ref_cnt_);

  // prepare write request
  ObIOInfo write_info = read_info;
  write_info.flag_.set_mode(ObIOMode::WRITE);
  req.destroy();
  ASSERT_EQ(1, result.result_ref_cnt_);
  result.reset();
  ASSERT_EQ(0, result.result_ref_cnt_);
  req.tenant_io_mgr_.hold(&tenant_io_mgr);
  result.tenant_io_mgr_.hold(&tenant_io_mgr);
  ASSERT_FAIL(result.init(write_info)); // not aligned
  ASSERT_TRUE(req.init(write_info ,&result));
  ASSERT_EQ(1, result.result_ref_cnt_); //inc ref even fail

  write_info.offset_ = DIO_READ_ALIGN_SIZE * 2;
  req.reset();
  result.reset();
  req.tenant_io_mgr_.hold(&tenant_io_mgr);
  result.tenant_io_mgr_.hold(&tenant_io_mgr);
  ASSERT_FAIL(result.init(write_info)); // only offset aligned, size not aligned
  ASSERT_TRUE(req.init(write_info ,&result));

  write_info.size_ = DIO_READ_ALIGN_SIZE * 4;
  req.reset();
  result.reset();
  req.tenant_io_mgr_.hold(&tenant_io_mgr);
  result.tenant_io_mgr_.hold(&tenant_io_mgr);
  ASSERT_FAIL(result.init(write_info)); // offset and size aligned, but write buf is null
  ASSERT_TRUE(req.init(write_info ,&result));

  write_info.buf_ = "test_write";
  req.reset();
  result.reset();
  req.tenant_io_mgr_.hold(&tenant_io_mgr);
  result.tenant_io_mgr_.hold(&tenant_io_mgr);
  ASSERT_SUCC(result.init(write_info));
  ASSERT_SUCC(req.init(write_info ,&result)); // normal usage
  ASSERT_TRUE(req.is_inited_);
  ASSERT_TRUE(result.is_inited_);
  ASSERT_NE(req.raw_buf_, nullptr); // write buf need copy immediately

  req.reset();
}

TEST_F(TestIOStruct, IOAbility)
{
  ObIOBenchResult item, item2;
  ASSERT_FALSE(item.is_valid());
  ASSERT_TRUE(item == item2);

  ObIOAbility io_ability;
  ASSERT_FALSE(io_ability.is_valid());
  ASSERT_FAIL(io_ability.add_measure_item(item)); // invalid measure item

  // normal usage
  item.mode_ = ObIOMode::READ;
  item.size_ = 10;
  item.rt_us_ = 10;
  item.iops_ = 100;
  ASSERT_TRUE(item.is_valid());
  ASSERT_SUCC(io_ability.add_measure_item(item));
  item.mode_ = ObIOMode::WRITE;
  ASSERT_SUCC(io_ability.add_measure_item(item));
  ASSERT_TRUE(io_ability.is_valid());

  // test equal
  ASSERT_FALSE(item == item2);
  item2 = item;
  ASSERT_TRUE(item == item2);
  item2.iops_ += 0.0001;
  ASSERT_FALSE(item == item2);

  // test sort
  item.size_ = 2;
  item.rt_us_ = 2;
  item.iops_ = 500;
  ASSERT_SUCC(io_ability.add_measure_item(item));
  item.size_ = 5;
  item.rt_us_ = 5;
  item.iops_ = 200;
  ASSERT_SUCC(io_ability.add_measure_item(item));
  ASSERT_EQ(2, io_ability.measure_items_[(int)ObIOMode::WRITE].at(0).size_);
  ASSERT_EQ(5, io_ability.measure_items_[(int)ObIOMode::WRITE].at(1).size_);
  ASSERT_EQ(10, io_ability.measure_items_[(int)ObIOMode::WRITE].at(2).size_);

  // test calculate smallest
  double iops = 0;
  double rt = 0;
  ASSERT_SUCC(io_ability.get_iops(ObIOMode::WRITE, 1, iops));
  ASSERT_SUCC(io_ability.get_rt(ObIOMode::WRITE, 1, rt));
  ASSERT_GE(iops, 500);
  ASSERT_LE(rt, 2);

  // test calculate middle
  ASSERT_SUCC(io_ability.get_iops(ObIOMode::WRITE, 8, iops));
  ASSERT_SUCC(io_ability.get_rt(ObIOMode::WRITE, 8, rt));
  ASSERT_GE(iops, 100); ASSERT_LE(iops, 200);
  ASSERT_GE(rt, 5);     ASSERT_LE(rt, 10);

  // test calculate biggest
  ASSERT_SUCC(io_ability.get_iops(ObIOMode::WRITE, 100, iops));
  ASSERT_SUCC(io_ability.get_rt(ObIOMode::WRITE, 100, rt));
  ASSERT_LE(iops, 100);
  ASSERT_GE(rt, 10);

  // test assign and equal
  ObIOAbility io_ability2;
  ASSERT_SUCC(io_ability2.assign(io_ability));
  ASSERT_TRUE(io_ability2.is_valid());
  ASSERT_TRUE(io_ability == io_ability2);

  // test destroy
  io_ability2.reset();
  ASSERT_FALSE(io_ability2.is_valid());
}


TEST_F(TestIOStruct, IOCalibration)
{
  ObIOCalibration bench;
  ASSERT_FALSE(bench.is_inited_);
  ASSERT_SUCC(bench.init());
  ASSERT_TRUE(bench.is_inited_);
  ObIOAbility io_ability;
  ASSERT_FALSE(io_ability.is_valid());
  ASSERT_FAIL(bench.update_io_ability(io_ability));
  ASSERT_SUCC(bench.reset_io_ability());
  ASSERT_SUCC(bench.get_io_ability(io_ability));
  ASSERT_FALSE(io_ability.is_valid());
  bench.destroy();
  ASSERT_FALSE(bench.is_inited_);

  ObIOBenchResult item;
  ASSERT_SUCC(ObIOCalibration::parse_calibration_string("READ:4096M:120:100000", item));
  ASSERT_TRUE(item.is_valid());
  ASSERT_SUCC(ObIOCalibration::parse_calibration_string("read:4K:10ms:1000", item));
  ASSERT_TRUE(item.is_valid());
  ASSERT_SUCC(ObIOCalibration::parse_calibration_string("r:2M:1s:100", item));
  ASSERT_TRUE(item.is_valid());
  ASSERT_SUCC(ObIOCalibration::parse_calibration_string("write:2M:1s:100", item));
  ASSERT_TRUE(item.is_valid());
  ASSERT_SUCC(ObIOCalibration::parse_calibration_string("W:12KB:20us:10000", item));
  ASSERT_TRUE(item.is_valid());

  ASSERT_FAIL(ObIOCalibration::parse_calibration_string(":12KB:20us:10000", item));
  ASSERT_FAIL(ObIOCalibration::parse_calibration_string("W::20us:10000", item));
  ASSERT_FAIL(ObIOCalibration::parse_calibration_string("W:12KB::10000", item));
  ASSERT_FAIL(ObIOCalibration::parse_calibration_string("W:12KB:20us:", item));
  ASSERT_FAIL(ObIOCalibration::parse_calibration_string("W:12KB:20us:X1000", item));
  ASSERT_FAIL(ObIOCalibration::parse_calibration_string("A:12KB:20us:1000", item));
  ASSERT_FAIL(ObIOCalibration::parse_calibration_string("W:12KB:20ks:1000", item));
  ASSERT_FAIL(ObIOCalibration::parse_calibration_string("W:12U:20us:1000", item));
}

TEST_F(TestIOStruct, IOStat)
{
  ObIOStat stat;
  stat.accumulate(100, 200, 300);
  ObIOStatDiff io_diff;
  ObCpuUsage cpu_diff;
  double avg_iops = 0;
  double avg_size = 0;
  double avg_rt = 0;
  double avg_cpu = 0;
  io_diff.diff(stat, avg_iops, avg_size, avg_rt);
  cpu_diff.get_cpu_usage(avg_cpu);
//  ASSERT_DOUBLE_EQ(avg_iops, 0);
  ASSERT_DOUBLE_EQ(avg_size, 2);
  ASSERT_DOUBLE_EQ(avg_rt, 3);
  ASSERT_DOUBLE_EQ(avg_cpu, 0);

  usleep(1000L * 1000L); // sleep 1s
  stat.accumulate(200, 600, 4000);
  io_diff.diff(stat, avg_iops, avg_size, avg_rt);
  ASSERT_NEAR(avg_iops, 200, 1);
  ASSERT_NEAR(avg_size, 3, 0.1);
  ASSERT_NEAR(avg_rt, 20, 0.1);
  ASSERT_GE(avg_cpu, 0);
}

TEST_F(TestIOStruct, IOScheduler)
{
  ObIOCalibration::get_instance().init();
  // test init
  ObIOConfig io_config = ObIOConfig::default_config();
  ObIOAllocator io_allocator;
  ASSERT_SUCC(io_allocator.init(TEST_TENANT_ID, IO_MEMORY_LIMIT));
  ASSERT_TRUE(io_config.is_valid());
  ObIOScheduler &scheduler = *(OB_IO_MANAGER.get_scheduler());

  // test schedule
  ObIOResult result;
  ObIORequest req;
  result.inc_ref();
  ObIOInfo io_info = get_random_io_info();
  ASSERT_TRUE(io_info.is_valid());
  ASSERT_SUCC(result.basic_init());
  ASSERT_SUCC(result.init(io_info));
  ASSERT_SUCC(req.init(io_info, &result));
  ObTenantIOConfig tenant_config = default_tenant_io_config();
  ObTenantIOClock io_clock;
  ObIOUsage io_usage;
  ASSERT_SUCC(io_clock.init(tenant_config, &io_usage));
  io_clock.destroy();
  ASSERT_SUCC(io_usage.init(0));
  ASSERT_SUCC(io_clock.init(tenant_config, &io_usage));
  //ASSERT_SUCC(scheduler.schedule_request(io_clock, req));

  // test destroy
  scheduler.destroy();
  ASSERT_FALSE(scheduler.is_inited_);

  ObIOCalibration::get_instance().destroy();
}

TEST_F(TestIOStruct, MClockQueue)
{
  ObIOAllocator io_allocator;
  ASSERT_SUCC(io_allocator.init(TEST_TENANT_ID, IO_MEMORY_LIMIT));
  ObTenantIOConfig io_config;
  io_config.callback_thread_count_ = 2;
  io_config.memory_limit_ = 1024L * 1024L * 1024L;
  io_config.group_num_ = 2;
  io_config.unit_config_.min_iops_ = 100;
  io_config.unit_config_.max_iops_ = 10000L;
  io_config.unit_config_.weight_ = 1000;

  ObTenantIOConfig::GroupConfig group_config_1;
  group_config_1.min_percent_ = 1;
  group_config_1.max_percent_ = 90;
  group_config_1.weight_percent_ = 60;
  ObTenantIOConfig::GroupConfig group_config_2;
  group_config_2.min_percent_ = 1;
  group_config_2.max_percent_ = 90;
  group_config_2.weight_percent_ = 30;
  io_config.group_configs_.push_back(group_config_1);
  io_config.group_configs_.push_back(group_config_2);
  io_config.group_ids_.push_back(1);
  io_config.group_ids_.push_back(2);
  io_config.other_group_config_.min_percent_ = 98;
  io_config.other_group_config_.max_percent_ = 100;
  io_config.other_group_config_.weight_percent_ = 10;
  ASSERT_TRUE(io_config.is_valid());
  ObTenantIOClock tenant_clock;
  ObIOUsage io_usage;
  ASSERT_SUCC(io_usage.init(2));
  ASSERT_SUCC(io_usage.refresh_group_num(2));
  ASSERT_SUCC(tenant_clock.init(io_config, &io_usage));
  ObMClockQueue mqueue1;
  ObMClockQueue mqueue2;
  const int64_t test_count = 6000L;
  ASSERT_SUCC(mqueue1.init());
  ASSERT_SUCC(mqueue2.init());
  ObArenaAllocator arena;
  for (int64_t i = 0; i < test_count; ++i) {
    char *buf = (char *)arena.alloc(sizeof(ObPhyQueue) * 2);
    ASSERT_TRUE(nullptr != buf);
    ObPhyQueue *phy_req = new (buf) ObPhyQueue;
    ObPhyQueue *phy_req2 = new (buf + sizeof(ObPhyQueue)) ObPhyQueue;
    ASSERT_SUCC(mqueue1.push_phyqueue(phy_req));
    ASSERT_SUCC(mqueue2.push_phyqueue(phy_req2));
  }
}

TEST_F(TestIOStruct, Test_Size)
{
  //callback
  int64_t size1 = sizeof(ObAsyncSingleMicroBlockIOCallback);
  int64_t size2 = sizeof(ObMultiDataBlockIOCallback);
  int64_t size3 = sizeof(ObSyncSingleMicroBLockIOCallback);
  int64_t size4 = sizeof(ObTmpPageCache::ObTmpPageIOCallback);
  int64_t size5 = sizeof(ObTmpPageCache::ObTmpMultiPageIOCallback);
  int64_t size6 = sizeof(oceanbase::ObStorageMetaCache::ObStorageMetaIOCallback);
  int64_t max_callback_size = std::max({size1, size2, size3, size4, size5, size6});
  int64_t size_request = sizeof(ObIORequest);
  int64_t size_result = sizeof(ObIOResult);
  int64_t size_info = sizeof(ObIOInfo);
  int64_t size_thread_cond = sizeof(ObThreadCond);
  int64_t size_flag = sizeof(ObIOFlag);
  ObRefHolder<ObTenantIOManager> tenant_io_mgr_;
  int64_t ref_size = sizeof(tenant_io_mgr_);
  int64_t time_size = sizeof(ObIOTimeLog);
  int64_t return_size = sizeof(ObIORetCode);
  int64_t trace_size = sizeof(ObCurTraceId::TraceId);
  int64_t fd_size = sizeof(ObIOFd);

  ASSERT_LT(max_callback_size, 512);
  LOG_INFO("qilu :check size", K(size1), K(size2), K(size3), K(size4), K(size5), K(size6), K(max_callback_size));
  LOG_INFO("qilu :check size", K(size_request), K(size_result), K(size_info), K(size_thread_cond), K(size_flag),
          K(ref_size), K(time_size), K(return_size), K(fd_size), K(trace_size));
  //mark: max_callback_size=208(ObMultiDataBlockIOCallback、ObStorageMetaIOCallback)
}

TEST_F(TestIOStruct, IOResult)
{
  ObRefHolder<ObTenantIOManager> holder;
  ASSERT_SUCC(OB_IO_MANAGER.get_tenant_io_manager(TEST_TENANT_ID, holder));
  ObIOFd fd;
  fd.first_id_ = 0;
  fd.second_id_ = 1;

  void *result_buf = holder.get_ptr()->io_allocator_.alloc(sizeof(ObIOResult));
  ObIOResult *result = new (result_buf) ObIOResult;
  void *req_buf = holder.get_ptr()->io_allocator_.alloc(sizeof(ObIORequest));
  ObIORequest *req = new (req_buf) ObIORequest;


  // default invalid
  ASSERT_FALSE(result->is_inited_);
  ASSERT_FALSE(result->is_valid());
  ASSERT_FALSE(req->is_inited_);
  ASSERT_FALSE(req->is_valid());

  // prepare test read request
  req->destroy();
  result->destroy();
  req->tenant_io_mgr_.hold(holder.get_ptr());
  result->tenant_io_mgr_.hold(holder.get_ptr());
  result->inc_ref();
  req->inc_ref();

  ObIOInfo read_info;
  read_info.tenant_id_ = OB_SERVER_TENANT_ID;
  read_info.fd_ = fd;
  read_info.flag_.set_mode(ObIOMode::READ);
  read_info.flag_.set_resource_group_id(10005);
  read_info.flag_.set_wait_event(1);
  read_info.timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  read_info.offset_ = 89;
  read_info.size_ = 1;
  char user_buf[1] = { 0 };
  read_info.user_data_buf_ = user_buf;

  ASSERT_TRUE(read_info.is_valid());
  ASSERT_SUCC(req->tenant_io_mgr_.get_ptr()->io_usage_.init(0));

  ASSERT_FAIL(result->init(read_info));
  ASSERT_FALSE(result->is_inited_);
  ASSERT_SUCC(result->basic_init());
  ASSERT_SUCC(result->init(read_info));
  ASSERT_TRUE(result->is_inited_);
  ASSERT_TRUE(result->is_valid());

  ASSERT_SUCC(req->init(read_info, result));
  ASSERT_TRUE(req->is_inited_);

  ASSERT_EQ(req->raw_buf_, nullptr); // read buf allocation is delayed
  ASSERT_SUCC(req->prepare());
  ASSERT_NE(req->raw_buf_, nullptr);
  ASSERT_EQ(result->get_resource_group_id(), 10005);
  ASSERT_EQ(req->get_resource_group_id(), 10005);

  // test finish
  result->finish_without_accumulate(OB_CANCELED);
  ASSERT_EQ(result->ret_code_.io_ret_, OB_CANCELED);
  result->finish_without_accumulate(OB_IO_ERROR);
  ASSERT_NE(result->ret_code_.io_ret_, OB_IO_ERROR); // finish only once

  //test free
  req->io_result_->dec_ref();
  ASSERT_EQ(1, req->io_result_->result_ref_cnt_);
  req->destroy();
  ASSERT_EQ(nullptr, req->io_result_);
}

TEST_F(TestIOStruct, IOCallbackManager)
{
  // test init
  ObIOCallbackManager callback_mgr;
  ASSERT_FALSE(callback_mgr.is_inited_);
  ASSERT_FAIL(callback_mgr.init(TEST_TENANT_ID, 0, 1000, nullptr));
  ObIOAllocator io_allocator;
  ASSERT_SUCC(io_allocator.init(TEST_TENANT_ID, IO_MEMORY_LIMIT));
  ASSERT_SUCC(callback_mgr.init(TEST_TENANT_ID, 2, 1000, &io_allocator));
  ASSERT_TRUE(callback_mgr.is_inited_);

  // test enqueue and dequeue
  ObIOResult result;
  ObIORequest req;
  req.inc_ref();
  result.inc_ref();
  ObIOInfo io_info = get_random_io_info();
  ASSERT_TRUE(io_info.is_valid());
  ASSERT_SUCC(result.basic_init());
  ASSERT_SUCC(result.init(io_info));
  ASSERT_SUCC(req.init(io_info, &result));
  ASSERT_FAIL(callback_mgr.enqueue_callback(req));
  char buf[32] = "test";
  req.raw_buf_ = buf;
  char callback_buf_[ObIOCallback::CALLBACK_BUF_SIZE] __attribute__ ((aligned (16)));
  TestIOCallback *test_callback = new (callback_buf_) TestIOCallback();
  result.io_callback_ = test_callback;
//  ObIOManager::get_instance().io_config_ = ObIOConfig::default_config();
  ASSERT_SUCC(callback_mgr.enqueue_callback(req));

  // test destroy
  callback_mgr.destroy();
  ASSERT_FALSE(callback_mgr.is_inited_);
}

TEST_F(TestIOStruct, IOFaultDetector)
{
  ObIOFaultDetector &detector = OB_IO_MANAGER.get_device_health_detector();
  ObIOConfig &io_config = (ObIOConfig &)detector.io_config_;

  // test get device health
  ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
  int64_t disk_abnormal_time = 0;
  ASSERT_SUCC(detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_NORMAL == dhs);
  ASSERT_TRUE(0 == disk_abnormal_time);

  // test read failure detection
  ObIOInfo io_info = get_random_io_info();
  ObIOResult result;
  ObIORequest req;
  req.inc_ref();
  result.inc_ref();
  ASSERT_SUCC(result.basic_init());
  ASSERT_SUCC(result.init(io_info));
  ASSERT_SUCC(req.init(io_info, &result));
  detector.reset_device_health();
  ASSERT_SUCC(detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_NORMAL == dhs);
  ASSERT_TRUE(0 == disk_abnormal_time);
  result.flag_.set_mode(ObIOMode::READ);
  io_config.data_storage_warning_tolerance_time_ = 1000L * 1000L;
  io_config.data_storage_error_tolerance_time_ = 3000L * 1000L;
  // io manager not init, skip this test
//  detector.record_io_err_failure(req);
//  usleep(2000L * 1000L);
//  ASSERT_SUCC(detector.get_device_health(is_device_warning, is_device_error));
//  ASSERT_TRUE(is_device_warning);
//  ASSERT_FALSE(is_device_error);
//  usleep(2000L * 1000L);
//  ASSERT_SUCC(detector.get_device_health(is_device_warning, is_device_error));
//  ASSERT_TRUE(is_device_warning);
//  ASSERT_TRUE(is_device_error);

  // test auto clean device warning, but not clean device error
  detector.reset_device_health();
  ASSERT_SUCC(detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_NORMAL == dhs);
  ASSERT_TRUE(0 == disk_abnormal_time);
  io_config.read_failure_black_list_interval_ = 1000L * 100L; // 100ms
  detector.set_device_warning();
  ASSERT_SUCC(detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_WARNING == dhs);
  ASSERT_TRUE(disk_abnormal_time > 0);
  usleep(io_config.read_failure_black_list_interval_ * 2);
  ASSERT_SUCC(detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_NORMAL == dhs);
  ASSERT_TRUE(0 == disk_abnormal_time);
  detector.set_device_error();
  usleep(io_config.read_failure_black_list_interval_ * 2);
  ASSERT_SUCC(detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_ERROR == dhs);
  ASSERT_TRUE(disk_abnormal_time > 0);
}

class TestIOManager : public TestIOStruct
{
  // basic use resource manager
public:
  static void SetUpTestCase()
  {
  }

  static void TearDownTestCase()
  {
    oceanbase::MockTenantModuleEnv::get_instance().destroy();
  }

  virtual void SetUp()
  {
    OB_IO_MANAGER.destroy();
    const int64_t memory_limit = 10L * 1024L * 1024L * 1024L; // 10GB
    ASSERT_SUCC(OB_IO_MANAGER.init(memory_limit));
    ASSERT_SUCC(OB_IO_MANAGER.start());

    // add io device
    ASSERT_SUCC(OB_IO_MANAGER.add_device_channel(THE_IO_DEVICE, 16, 2, 1024));

    // add tenant io manager
    const uint64_t tenant_id = OB_SERVER_TENANT_ID;
    ObTenantIOConfig io_config;
    io_config.memory_limit_ = memory_limit;
    io_config.callback_thread_count_ = 2;
    io_config.unit_config_.min_iops_ = 10000;
    io_config.unit_config_.max_iops_ = 100000;
    io_config.unit_config_.weight_ = 100;
    io_config.other_group_config_.min_percent_ = 100;
    io_config.other_group_config_.max_percent_ = 100;
    io_config.other_group_config_.weight_percent_ = 100;
  }
  virtual void TearDown()
  {
    OB_IO_MANAGER.stop();
    OB_IO_MANAGER.destroy();
  }
};

TEST_F(TestIOManager, memory_pool)
{
  ObRefHolder<ObTenantIOManager> tenant_holder;
  ASSERT_SUCC(OB_IO_MANAGER.get_tenant_io_manager(500, tenant_holder));
  ASSERT_NE(nullptr, tenant_holder.get_ptr());

  ObIORequest *io_request = nullptr;
  ASSERT_SUCC(tenant_holder.get_ptr()->io_request_pool_.alloc(io_request));
  ASSERT_NE(nullptr, io_request);
  io_request->tenant_io_mgr_.hold(tenant_holder.get_ptr());
  ASSERT_TRUE(tenant_holder.get_ptr()->io_request_pool_.contain(io_request));
  ASSERT_SUCC(tenant_holder.get_ptr()->io_request_pool_.recycle(io_request));
  io_request->tenant_io_mgr_.reset();

  ObIOResult *io_result = nullptr;
  ASSERT_SUCC(tenant_holder.get_ptr()->io_result_pool_.alloc(io_result));
  ASSERT_NE(nullptr, io_result);
  io_result->tenant_io_mgr_.hold(tenant_holder.get_ptr());
  ASSERT_TRUE(tenant_holder.get_ptr()->io_result_pool_.contain(io_result));
  ASSERT_SUCC(tenant_holder.get_ptr()->io_result_pool_.recycle(io_result));
  io_result->tenant_io_mgr_.reset();

  void *result_buf = tenant_holder.get_ptr()->io_allocator_.alloc(sizeof(ObIOResult));
  ObIOResult *result1 = new (result_buf) ObIOResult;
  result1->tenant_io_mgr_.hold(tenant_holder.get_ptr());
  ASSERT_FALSE(tenant_holder.get_ptr()->io_result_pool_.contain(result1));
  ASSERT_FAIL(tenant_holder.get_ptr()->io_result_pool_.recycle(result1));
  result1->~ObIOResult();
  tenant_holder.get_ptr()->io_allocator_.free(result1);

  void *req_buf = tenant_holder.get_ptr()->io_allocator_.alloc(sizeof(ObIORequest));
  ObIORequest *req1 = new (req_buf) ObIORequest;
  req1->tenant_io_mgr_.hold(tenant_holder.get_ptr());
  ASSERT_FALSE(tenant_holder.get_ptr()->io_request_pool_.contain(req1));
  ASSERT_FAIL(tenant_holder.get_ptr()->io_request_pool_.recycle(req1));
  req1->~ObIORequest();
  tenant_holder.get_ptr()->io_allocator_.free(req1);
}

TEST_F(TestIOManager, simple)
{
  ObIOFd fd;
  ASSERT_SUCC(THE_IO_DEVICE->open(TEST_ROOT_DIR "/test_io_file", O_CREAT | O_DIRECT | O_TRUNC | O_RDWR, 0644, fd));
  ASSERT_TRUE(fd.is_valid());
  ObIOManager &io_mgr = ObIOManager::get_instance();

  // fallocate
  const int64_t FILE_SIZE = 4 * 1024 * 1024;
  ASSERT_SUCC(THE_IO_DEVICE->fallocate(fd, 0, 0, FILE_SIZE)); // 4M
  {
    oceanbase::common::ObIODFileStat stat_buf;
    ASSERT_SUCC(THE_IO_DEVICE->stat(TEST_ROOT_DIR "/test_io_file", stat_buf));
    ASSERT_EQ(FILE_SIZE, stat_buf.size_);
  }

  // sync write
  const int64_t io_timeout_ms = 1000L * 5L;
  const int64_t write_io_size = DIO_READ_ALIGN_SIZE * 2;
  ObIOInfo io_info;
  io_info.tenant_id_ = OB_SERVER_TENANT_ID;
  io_info.fd_ = fd;
  io_info.flag_.set_write();
  io_info.flag_.set_resource_group_id(USER_RESOURCE_OTHER_GROUP_ID);
  io_info.flag_.set_wait_event(100);
  io_info.offset_ = 0;
  io_info.size_ = write_io_size;
  io_info.timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  char buf[write_io_size] = { 0 };
  const int64_t user_offset = write_io_size / 2;
  memset(buf, 'a', user_offset);
  memset(buf + user_offset, 'b', write_io_size - user_offset);
  io_info.buf_ = buf;
  io_info.user_data_buf_ = buf;
  ASSERT_SUCC(io_mgr.write(io_info));

  // check size
  {
    oceanbase::common::ObIODFileStat stat_buf;
    ASSERT_SUCC(THE_IO_DEVICE->stat(TEST_ROOT_DIR "/test_io_file", stat_buf));
    ASSERT_EQ(FILE_SIZE, stat_buf.size_);
  }

  // sync read and compare data buffer
  io_info.flag_.set_read();
  ObIOHandle io_handle;
  ASSERT_SUCC(io_mgr.read(io_info, io_handle));
  ASSERT_NE(nullptr, io_handle.get_buffer());
  ASSERT_EQ(io_info.size_, io_handle.get_data_size());
  ASSERT_EQ(0, memcmp(buf, io_handle.get_buffer(), write_io_size));

  // read end
  io_handle.reset();
  io_info.offset_ = FILE_SIZE;
  io_info.size_ = DIO_READ_ALIGN_SIZE;
  ASSERT_NE(OB_SUCCESS, io_mgr.read(io_info, io_handle));
  ASSERT_EQ(0, io_handle.get_data_size());

  // read tail
  io_handle.reset();
  io_info.offset_ = FILE_SIZE - DIO_READ_ALIGN_SIZE;
  io_info.size_ = DIO_READ_ALIGN_SIZE;
  ASSERT_SUCC(io_mgr.read(io_info, io_handle));
  ASSERT_EQ(DIO_READ_ALIGN_SIZE, io_handle.get_data_size());

  // read tail part
  io_handle.reset();
  io_info.offset_ = FILE_SIZE - DIO_READ_ALIGN_SIZE;
  io_info.size_ = DIO_READ_ALIGN_SIZE * 2;
  ASSERT_NE(OB_SUCCESS, io_mgr.read(io_info, io_handle));
  ASSERT_EQ(DIO_READ_ALIGN_SIZE, io_handle.get_data_size());

  // read with callback
  io_handle.reset();
  io_info.offset_ = user_offset;
  io_info.size_ = write_io_size - user_offset;
  ObArenaAllocator allocator;
  int64_t tmp_number = 0;
  TestIOCallback callback;
  callback.number_ = &tmp_number;
  callback.allocator_ = &allocator;
  io_info.callback_ = &callback;
  ASSERT_SUCC(io_mgr.read(io_info, io_handle));
  ASSERT_NE(nullptr, io_handle.get_buffer());
  ASSERT_EQ(io_info.size_, io_handle.get_data_size());
  ASSERT_EQ(0, memcmp(buf + user_offset, io_handle.get_buffer(), write_io_size - user_offset));
  ASSERT_EQ(100, tmp_number); // callback process called
  io_handle.reset();
  // ASSERT_EQ(10, tmp_number); // callback destructor called

  ASSERT_SUCC(THE_IO_DEVICE->close(fd));
}


struct IOPerfDevice
{
  IOPerfDevice() : device_id_(0), media_id_(0), async_channel_count_(0), sync_channel_count_(0), max_io_depth_(0),
                   file_size_(0), file_path_(), fd_(0), device_handle_(nullptr) {
    memset(file_path_, 0, sizeof(file_path_));
  }
  bool is_valid() const {
    return device_id_ > 0 && media_id_ >= 0 && async_channel_count_ > 0 && sync_channel_count_ > 0  && max_io_depth_ > 0
      && file_size_ > 0 && strlen(file_path_) > 0;
  }
  TO_STRING_KV(K(device_id_), K(media_id_), K(async_channel_count_), K(sync_channel_count_), K(max_io_depth_),
      K(file_size_), K(file_path_), K(fd_), KP(device_handle_));
  int32_t device_id_;
  int32_t media_id_;
  int32_t async_channel_count_;
  int32_t sync_channel_count_;
  int32_t max_io_depth_;
  int64_t file_size_;
  char file_path_[OB_MAX_FILE_NAME_LENGTH];
  int32_t fd_;
  ObLocalDevice *device_handle_;
};

struct IOPerfTenant
{
  IOPerfTenant() : tenant_id_(0), config_() {}
  bool is_valid() const { return tenant_id_ > 0 && config_.is_valid(); }
  TO_STRING_KV(K(tenant_id_), K(config_));
  int32_t tenant_id_;
  ObTenantIOConfig config_;
};

enum class IOPerfMode
{
  ROLLING,
  BATCH,
  ASYNC,
  UNKNOWN
};

struct IOPerfLoad
{
  IOPerfLoad()
    : tenant_id_(0), device_id_(0), mode_(ObIOMode::MAX_MODE), size_(0), depth_(0), iops_(0),
      thread_count_(0), is_sequence_(false), group_id_(0), start_delay_ts_(0), stop_delay_ts_(0),
      device_(nullptr), perf_mode_(IOPerfMode::UNKNOWN)
  {}
  TO_STRING_KV(K(tenant_id_), K(device_id_),
      "mode", ObIOMode::READ == mode_ ? "read" : ObIOMode::WRITE == mode_ ? "write" : "unknown",
      "group_id", group_id_, "io_size", size_, "io_depth", depth_, "target_iops", iops_,
      K(thread_count_), K(is_sequence_), K(start_delay_ts_), K(stop_delay_ts_), KP(device_), K(perf_mode_));
  bool is_valid() const {
    return tenant_id_ > 0 && group_id_ >= 0 && device_id_ > 0
      && mode_ < ObIOMode::MAX_MODE && size_ > 0 && depth_ > 0 && iops_ >= 0
      && thread_count_ > 0 && start_delay_ts_ >= 0 && stop_delay_ts_ > start_delay_ts_ && size_ > 0
      && (ObIOMode::WRITE == mode_ ? is_io_aligned(size_) : true)
      && perf_mode_ != IOPerfMode::UNKNOWN;
  }
  int32_t tenant_id_;
  int32_t device_id_;
  ObIOMode mode_;
  int32_t size_;
  int32_t depth_;
  int64_t iops_;
  int32_t thread_count_;
  bool is_sequence_;
  uint64_t group_id_;
  int64_t start_delay_ts_;
  int64_t stop_delay_ts_;
  IOPerfDevice *device_;
  IOPerfMode perf_mode_;
};

struct IOPerfScheduler
{
  IOPerfScheduler() : sender_count_(0), schedule_media_id_(0), io_greed_(0) {}
  int64_t sender_count_;
  int64_t schedule_media_id_;
  int64_t io_greed_;
  TO_STRING_KV(K(sender_count_), K(schedule_media_id_), K(io_greed_));
};

struct IOPerfResult
{
  IOPerfResult()
    : succ_count_(0), fail_count_(0), sum_rt_(0), start_delay_ts_(0), stop_delay_ts_(0), disk_rt_(0)
  {}
  TO_STRING_KV(K(succ_count_), K(fail_count_), K(sum_rt_), K(start_delay_ts_), K(stop_delay_ts_));
  int64_t succ_count_;
  int64_t fail_count_;
  int64_t sum_rt_;
  int64_t start_delay_ts_;
  int64_t stop_delay_ts_;
  int64_t disk_rt_;
};

class IOPerfRunner : public ThreadPool
{
public:
  IOPerfRunner()
    : abs_ts_(0), last_offset_(0), write_buf_(nullptr), user_buf_(nullptr), io_count_(0), report_count_(0), total_io_count_(0)
  {}
  int init(const int64_t absolute_ts, const IOPerfLoad &load);
  void destroy();
  virtual void run1() override;
  int do_batch_io();
  int do_perf_batch();
  int do_perf_rolling();
  int wait_and_count(ObIOHandle &io_handle);
  int wait_handles(ObFixedQueue<ObIOHandle> &handles);
  int print_result();
  TO_STRING_KV(K(load_), K(result_), K(last_offset_), KP(write_buf_), KP(user_buf_), K(fd_));
public:
  int64_t abs_ts_;
  IOPerfLoad load_;
  IOPerfResult result_;
  int64_t last_offset_;
  const char *write_buf_;
  char *user_buf_;
  ObIOFd fd_;
  ObConcurrentFIFOAllocator allocator_;
  ObFixedQueue<ObIOHandle> handle_queue_;
  int64_t io_count_;
  int64_t report_count_;
  int64_t total_io_count_;
};

class IOConfModify : public ThreadPool
{
public:
  IOConfModify()
    : modify_init_ts_(0)
  {}
  int init(int64_t modify_init_ts, int64_t modify_delay_ts, const IOPerfTenant &curr_tenant);
  void destroy();
  virtual void run1() override;
  int modify_tenant_io(const int64_t min_iops,
                       const int64_t max_iops,
                       const int64_t weight,
                       IOPerfTenant &curr_tenant);
  TO_STRING_KV(K(load_), K(modify_delay_ts_), K(fd_), K(curr_tenant_));
public:
  int64_t modify_init_ts_;
  int64_t modify_delay_ts_;
  IOPerfTenant curr_tenant_;
  ObConcurrentFIFOAllocator allocator_;
  IOPerfLoad load_;
  ObIOFd fd_;
};

class IOCallbackModifier : public ThreadPool
{
public:
  IOCallbackModifier()
    : modify_init_ts_(0)
  {}
  int init(int64_t modify_init_ts, int64_t modify_delay_ts, const IOPerfTenant &curr_tenant);
  void destroy();
  virtual void run1() override;
  int modify_callback_num(const int64_t thread_num,
                          IOPerfTenant &curr_tenant);
  TO_STRING_KV(K(load_), K(modify_delay_ts_), K(fd_), K(curr_tenant_));
public:
  int64_t modify_init_ts_;
  int64_t modify_delay_ts_;
  IOPerfTenant curr_tenant_;
  ObConcurrentFIFOAllocator allocator_;
  IOPerfLoad load_;
  ObIOFd fd_;
};

class IOGroupModify : public ThreadPool
{
public:
  IOGroupModify()
    : modify_init_ts_(0)
  {}
  int init(int64_t modify_init_ts, int64_t modify_delay_ts, const IOPerfTenant &curr_tenant);
  void destroy();
  virtual void run1() override;
  TO_STRING_KV(K(load_), K(modify_delay_ts_), K(fd_), K(curr_tenant_));
public:
  int64_t modify_init_ts_;
  int64_t modify_delay_ts_;
  IOPerfTenant curr_tenant_;
  ObConcurrentFIFOAllocator allocator_;
  IOPerfLoad load_;
  ObIOFd fd_;
};

class IOTracerSwitch : public ThreadPool
{
public:
  IOTracerSwitch()
  {}
  int init(int64_t switch_init_ts, int64_t switch_delay_ts, const IOPerfTenant &curr_tenant);
  void destroy();
  virtual void run1() override;
  int modify_tenant_io(IOPerfTenant &curr_tenant);
  TO_STRING_KV(K(load_), K(switch_init_ts_), K(switch_delay_ts_), K(curr_tenant_), K(load_));
public:
  int64_t switch_init_ts_;
  int64_t switch_delay_ts_;
  IOPerfTenant curr_tenant_;
  ObConcurrentFIFOAllocator allocator_;
  IOPerfLoad load_;
};

#define GROUP_PERF_CONFIG_FILE "io_perf.conf"

void write_group_perf_config();
int parse_group_perf_config(const char *config_file_path,
                      IOPerfScheduler &scheduler_config,
                      ObIArray<IOPerfDevice> &perf_devices,
                      ObIArray<IOPerfTenant> &perf_tenants,
                      ObIArray<IOPerfLoad> &perf_loads);


int prepare_file(const char *file_path, const int64_t file_size, int32_t &fd)
{
  int ret = OB_SUCCESS;
  fd = -1;
  bool need_create_perf_file = true;
  bool is_file_exist = false;
  int64_t exist_file_size = 0;
  if (OB_FAIL(FileDirectoryUtils::is_exists(file_path, is_file_exist))) {
    LOG_WARN("check file exist failed", K(ret));
  } else if (is_file_exist) {
    if (OB_FAIL(FileDirectoryUtils::get_file_size(file_path, exist_file_size))) {
      LOG_WARN("get file size failed", K(ret));
    } else if (exist_file_size == file_size) {
      if ((fd = ::open(file_path, O_DIRECT | O_RDWR)) < 0) {
        ret = OB_IO_ERROR;
        LOG_WARN("fail to open file", K(ret));
      } else {
        need_create_perf_file = false;
      }
    }
  }
  if (need_create_perf_file) {
    char cmd[1024] = { 0 };
    sprintf(cmd, "rm -rf %s", file_path);
    system(cmd);
    if ((fd = ::open(file_path, O_CREAT | O_TRUNC | O_DIRECT | O_RDWR, 0644)) < 0) {
      ret = OB_IO_ERROR;
      LOG_WARN("fail to create file", K(ret));
    } else {
      if (fallocate(fd, 0, 0, file_size) < 0) {
        ret = OB_IO_ERROR;
        LOG_WARN("fail to allocate file", K(ret), K(fd), K(file_size));
      } else {
        const int64_t buf_size = 1024L * 1024L; // 1MB
        const int64_t write_count = file_size / buf_size;
        sprintf(cmd, "dd if=/dev/zero of=%s bs=1M count=%ld", file_path, write_count);
        system(cmd);
        fsync(fd);
        LOG_INFO("prepare file finished", K(cmd), "file_size_mb", write_count);
      }
    }
  }
  return ret;
}

TEST_F(TestIOManager, tenant)
{
  ObTenantIOConfig default_config = ObTenantIOConfig::default_instance();
  default_config.unit_config_.max_iops_ = 20000L;
  int64_t current_ts = ObTimeUtility::fast_current_time();
  IOPerfLoad load;
  load.group_id_ = 0;
  load.depth_ = 1;
  IOPerfDevice device;
  device.device_id_ = 1;
  strcpy(device.file_path_, "./perf_test");
  device.file_size_ = 1024L * 1024L * 1024L;
  device.device_handle_ = static_cast<ObLocalDevice *>(THE_IO_DEVICE);
  prepare_file(device.file_path_, device.file_size_, device.fd_);
  load.device_ = &device;
  load.device_id_ = 1; // unused
  load.iops_ = 0;
  load.is_sequence_ = false;
  load.mode_ = ObIOMode::READ;
  load.perf_mode_ = IOPerfMode::ROLLING;
  load.size_ = 8192;
  load.start_delay_ts_ = 0;
  load.stop_delay_ts_ = 3L * 1000L * 1000L; // 3s
  load.tenant_id_ = 1001;
  load.thread_count_ = 16;
  IOPerfRunner runner;
  ASSERT_SUCC(runner.init(current_ts, load));
  usleep(2L * 1000L * 1000L); // 2s
  runner.wait();
  runner.destroy();
}

TEST_F(TestIOManager, perf)
{
  // use multi thread to do some io stress, maybe use test_io_performance
  bool is_perf_config_exist = false;
  ASSERT_SUCC(FileDirectoryUtils::is_exists(GROUP_PERF_CONFIG_FILE, is_perf_config_exist));
  if (!is_perf_config_exist) {
    write_group_perf_config();
  }
  // parse configs
  IOPerfScheduler scheduler_config;
  ObArray<IOPerfDevice> perf_devices;
  ObArray<IOPerfTenant> perf_tenants;
  ObArray<IOPerfLoad> perf_loads;
  ASSERT_SUCC(parse_group_perf_config(GROUP_PERF_CONFIG_FILE, scheduler_config, perf_devices, perf_tenants, perf_loads));
  ASSERT_TRUE(perf_devices.count() > 0);
  ASSERT_TRUE(perf_tenants.count() > 0);
  ASSERT_TRUE(perf_loads.count() > 0);

  ObIOManager::get_instance().destroy();
  const int64_t memory_limit = 30L * 1024L * 1024L * 1024L; // 30GB
  const int64_t queue_depth = 100L;
  ASSERT_SUCC(ObIOManager::get_instance().init(memory_limit, queue_depth, scheduler_config.sender_count_, scheduler_config.schedule_media_id_));
  ASSERT_SUCC(ObIOManager::get_instance().start());

  // prepare devices and files
  char *device_buf = (char *)malloc(sizeof(ObLocalDevice) * perf_devices.count());
  ASSERT_TRUE(nullptr != device_buf);
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    IOPerfDevice &curr_config = perf_devices.at(i);
    ASSERT_SUCC(prepare_file(curr_config.file_path_, curr_config.file_size_, curr_config.fd_));
    ObLocalDevice *device = new (device_buf + sizeof(ObLocalDevice) * i) ObLocalDevice;
    ASSERT_SUCC(init_device(curr_config.media_id_, *device));
    ASSERT_SUCC(OB_IO_MANAGER.add_device_channel(device, curr_config.async_channel_count_, curr_config.sync_channel_count_, curr_config.max_io_depth_));
    curr_config.device_handle_ = device;
  }
  // prepare tenant io manager
  for (int64_t i = 0; i < perf_tenants.count(); ++i) {
    IOPerfTenant &curr_config = perf_tenants.at(i);
    LOG_INFO("wenqu: tenant config", K(curr_config), K(i));
    ObRefHolder<ObTenantIOManager> tenant_holder;
    ASSERT_SUCC(OB_IO_MANAGER.get_tenant_io_manager(curr_config.tenant_id_, tenant_holder));
    ASSERT_SUCC(tenant_holder.get_ptr()->refresh_group_io_config());
  }
  // prepare perf runners
  char *runner_buf = (char *)malloc(perf_loads.count() * sizeof(IOPerfRunner));
  ObArray<IOPerfRunner *> runners;
  const int64_t start_ts = ObTimeUtility::current_time() + 10000L;
  for (int64_t i = 0; i < perf_loads.count(); ++i) {
    IOPerfRunner *runner = new (runner_buf + i * sizeof(IOPerfRunner)) IOPerfRunner();
    const IOPerfLoad &cur_load = perf_loads.at(i);
    ASSERT_SUCC(runner->init(start_ts, cur_load));
    ASSERT_SUCC(runners.push_back(runner));
  }
  // wait perf finished
  for (int64_t i = 0; i < runners.count(); ++i) {
    IOPerfRunner *runner = runners.at(i);
    runner->wait();
    ASSERT_SUCC(runner->print_result());
    runner->destroy();
  }
  free(runner_buf);

  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    ObLocalDevice *device_handle = perf_devices.at(i).device_handle_;
//    ASSERT_SUCC(OB_IO_MANAGER.remove_device_channel(device_handle));
    device_handle->destroy();
  }
  free(device_buf);
  LOG_INFO("wenqu: perf finished");
}

TEST_F(TestIOManager, alloc_memory)
{
  // use multi thread to do some io stress, maybe use test_io_performance
  bool is_perf_config_exist = false;
  ASSERT_SUCC(FileDirectoryUtils::is_exists(GROUP_PERF_CONFIG_FILE, is_perf_config_exist));
  if (!is_perf_config_exist) {
    write_group_perf_config();
  }
  // parse configs
  IOPerfScheduler scheduler_config;
  ObArray<IOPerfDevice> perf_devices;
  ObArray<IOPerfTenant> perf_tenants;
  ObArray<IOPerfLoad> perf_loads;
  ASSERT_SUCC(parse_group_perf_config(GROUP_PERF_CONFIG_FILE, scheduler_config, perf_devices, perf_tenants, perf_loads));
  ASSERT_TRUE(perf_devices.count() > 0);
  ASSERT_TRUE(perf_tenants.count() > 0);
  ASSERT_TRUE(perf_loads.count() > 0);

  ObIOManager::get_instance().destroy();
  const int64_t memory_limit = 30L * 1024L * 1024L * 1024L; // 30GB
  const int64_t queue_depth = 100L;
  ASSERT_SUCC(ObIOManager::get_instance().init(memory_limit, queue_depth, scheduler_config.sender_count_, scheduler_config.schedule_media_id_));
  ASSERT_SUCC(ObIOManager::get_instance().start());

  // prepare devices and files
  char *device_buf = (char *)malloc(sizeof(ObLocalDevice) * perf_devices.count());
  ASSERT_TRUE(nullptr != device_buf);
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    IOPerfDevice &curr_config = perf_devices.at(i);
    ASSERT_SUCC(prepare_file(curr_config.file_path_, curr_config.file_size_, curr_config.fd_));
    ObLocalDevice *device = new (device_buf + sizeof(ObLocalDevice) * i) ObLocalDevice;
    ASSERT_SUCC(init_device(curr_config.media_id_, *device));
    ASSERT_SUCC(OB_IO_MANAGER.add_device_channel(device, curr_config.async_channel_count_, curr_config.sync_channel_count_, curr_config.max_io_depth_));
    curr_config.device_handle_ = device;
  }
  // prepare tenant io manager
  for (int64_t i = 0; i < perf_tenants.count(); ++i) {
    IOPerfTenant &curr_config = perf_tenants.at(i);
    curr_config.config_.memory_limit_ = 16L* 1024L * 1024L; //16MB
    ObRefHolder<ObTenantIOManager> tenant_holder;
    ASSERT_SUCC(OB_IO_MANAGER.get_tenant_io_manager(curr_config.tenant_id_, tenant_holder));
    ASSERT_SUCC(tenant_holder.get_ptr()->refresh_group_io_config());
  }
  // prepare perf runners
  char *runner_buf = (char *)malloc(perf_loads.count() * sizeof(IOPerfRunner));
  ObArray<IOPerfRunner *> runners;
  const int64_t start_ts = ObTimeUtility::current_time() + 10000L;
  for (int64_t i = 0; i < perf_loads.count(); ++i) {
    IOPerfRunner *runner = new (runner_buf + i * sizeof(IOPerfRunner)) IOPerfRunner();
    const IOPerfLoad &cur_load = perf_loads.at(i);
    ASSERT_SUCC(runner->init(start_ts, cur_load));
    ASSERT_SUCC(runners.push_back(runner));
  }
  // wait perf finished
  for (int64_t i = 0; i < runners.count(); ++i) {
    IOPerfRunner *runner = runners.at(i);
    runner->wait();
    ASSERT_SUCC(runner->print_result());
    runner->destroy();
  }
  free(runner_buf);

  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    ObLocalDevice *device_handle = perf_devices.at(i).device_handle_;
    device_handle->destroy();
  }
  free(device_buf);}

TEST_F(TestIOManager, IOTracer)
{
  // use multi thread to do modify group_io_config
  bool is_perf_config_exist = false;
  ASSERT_SUCC(FileDirectoryUtils::is_exists(GROUP_PERF_CONFIG_FILE, is_perf_config_exist));
  if (!is_perf_config_exist) {
    write_group_perf_config();
  }
  // parse configs
  IOPerfScheduler scheduler_config;
  ObArray<IOPerfDevice> perf_devices;
  ObArray<IOPerfTenant> perf_tenants;
  ObArray<IOPerfLoad> perf_loads;
  ASSERT_SUCC(parse_group_perf_config(GROUP_PERF_CONFIG_FILE, scheduler_config, perf_devices, perf_tenants, perf_loads));
  ASSERT_TRUE(perf_devices.count() > 0);
  ASSERT_TRUE(perf_tenants.count() > 0);
  ASSERT_TRUE(perf_loads.count() > 0);

  ObIOManager::get_instance().destroy();
  const int64_t memory_limit = 30L * 1024L * 1024L * 1024L; // 30GB
  const int64_t queue_depth = 100L;
  ASSERT_SUCC(ObIOManager::get_instance().init(memory_limit, queue_depth, scheduler_config.sender_count_, scheduler_config.schedule_media_id_));
  ASSERT_SUCC(ObIOManager::get_instance().start());

  // prepare devices and files
  char *device_buf = (char *)malloc(sizeof(ObLocalDevice) * perf_devices.count());
  ASSERT_TRUE(nullptr != device_buf);
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    IOPerfDevice &curr_config = perf_devices.at(i);
    ASSERT_SUCC(prepare_file(curr_config.file_path_, curr_config.file_size_, curr_config.fd_));
    ObLocalDevice *device = new (device_buf + sizeof(ObLocalDevice) * i) ObLocalDevice;
    ASSERT_SUCC(init_device(curr_config.media_id_, *device));
    ASSERT_SUCC(OB_IO_MANAGER.add_device_channel(device, curr_config.async_channel_count_, curr_config.sync_channel_count_, curr_config.max_io_depth_));
    curr_config.device_handle_ = device;
  }
  // prepare tenant io manager
  for (int64_t i = 0; i < perf_tenants.count(); ++i) {
    IOPerfTenant &curr_config = perf_tenants.at(i);
    LOG_INFO("wenqu: tenant config", K(curr_config), K(i));
    ObRefHolder<ObTenantIOManager> tenant_holder;
    ASSERT_SUCC(OB_IO_MANAGER.get_tenant_io_manager(curr_config.tenant_id_, tenant_holder));
    ASSERT_SUCC(tenant_holder.get_ptr()->refresh_group_io_config());
  }
  // prepare perf runners
  char *runner_buf = (char *)malloc(perf_loads.count() * sizeof(IOPerfRunner));
  char *modifyer_buf = (char *)malloc(perf_loads.count() * sizeof(IOTracerSwitch));
  ObArray<IOPerfRunner *> runners;
  ObArray<IOTracerSwitch *> switches;
  const int64_t start_ts = ObTimeUtility::current_time() + 10000L;
  for (int64_t i = 0; i < perf_loads.count(); ++i) {
    IOPerfRunner *runner = new (runner_buf + i * sizeof(IOPerfRunner)) IOPerfRunner();
    const IOPerfLoad &cur_load = perf_loads.at(i);
    ASSERT_SUCC(runner->init(start_ts, cur_load));
    ASSERT_SUCC(runners.push_back(runner));
    LOG_INFO("runner start now");
  }
  //open tracer
  for (int64_t i = 0; i < perf_tenants.count(); ++i) {
    IOTracerSwitch *tracer_switch = new (modifyer_buf + i * sizeof(IOTracerSwitch)) IOTracerSwitch();
    IOPerfTenant &curr_tenant = perf_tenants.at(i);
    int64_t switch_init_ts = start_ts;
    int64_t switch_delay_ts = 1000000L; //1s后打开开关
    ASSERT_SUCC(tracer_switch->init(switch_init_ts, switch_delay_ts, curr_tenant));
    ASSERT_SUCC(switches.push_back(tracer_switch));
  }
  // wait perf finished
  for (int64_t i = 0; i < runners.count(); ++i) {
    IOPerfRunner *runner = runners.at(i);
    runner->wait();
    ASSERT_SUCC(runner->print_result());
    runner->destroy();
  }
  free(runner_buf);
  free(modifyer_buf);

  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    ObLocalDevice *device_handle = perf_devices.at(i).device_handle_;
//    ASSERT_SUCC(OB_IO_MANAGER.remove_device_channel(device_handle));
    device_handle->destroy();
  }
  free(device_buf);
  LOG_INFO("wenqu: modify finished");
}

TEST_F(TestIOManager, ModifyIOPS)
{
  // use multi thread to do modify group_io_config
  bool is_perf_config_exist = false;
  ASSERT_SUCC(FileDirectoryUtils::is_exists(GROUP_PERF_CONFIG_FILE, is_perf_config_exist));
  if (!is_perf_config_exist) {
    write_group_perf_config();
  }
  // parse configs
  IOPerfScheduler scheduler_config;
  ObArray<IOPerfDevice> perf_devices;
  ObArray<IOPerfTenant> perf_tenants;
  ObArray<IOPerfLoad> perf_loads;
  ASSERT_SUCC(parse_group_perf_config(GROUP_PERF_CONFIG_FILE, scheduler_config, perf_devices, perf_tenants, perf_loads));
  ASSERT_TRUE(perf_devices.count() > 0);
  ASSERT_TRUE(perf_tenants.count() > 0);
  ASSERT_TRUE(perf_loads.count() > 0);

  ObIOManager::get_instance().destroy();
  const int64_t memory_limit = 30L * 1024L * 1024L * 1024L; // 30GB
  const int64_t queue_depth = 100L;
  ASSERT_SUCC(ObIOManager::get_instance().init(memory_limit, queue_depth, scheduler_config.sender_count_, scheduler_config.schedule_media_id_));
  ASSERT_SUCC(ObIOManager::get_instance().start());

  // prepare devices and files
  char *device_buf = (char *)malloc(sizeof(ObLocalDevice) * perf_devices.count());
  ASSERT_TRUE(nullptr != device_buf);
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    IOPerfDevice &curr_config = perf_devices.at(i);
    ASSERT_SUCC(prepare_file(curr_config.file_path_, curr_config.file_size_, curr_config.fd_));
    ObLocalDevice *device = new (device_buf + sizeof(ObLocalDevice) * i) ObLocalDevice;
    ASSERT_SUCC(init_device(curr_config.media_id_, *device));
    ASSERT_SUCC(OB_IO_MANAGER.add_device_channel(device, curr_config.async_channel_count_, curr_config.sync_channel_count_, curr_config.max_io_depth_));
    curr_config.device_handle_ = device;
  }
  // prepare tenant io manager
  for (int64_t i = 0; i < perf_tenants.count(); ++i) {
    IOPerfTenant &curr_config = perf_tenants.at(i);
    LOG_INFO("wenqu: tenant config", K(curr_config), K(i));
    ObRefHolder<ObTenantIOManager> tenant_holder;
    ASSERT_SUCC(OB_IO_MANAGER.get_tenant_io_manager(curr_config.tenant_id_, tenant_holder));
    ASSERT_SUCC(tenant_holder.get_ptr()->refresh_group_io_config());
  }
  // prepare perf runners
  char *runner_buf = (char *)malloc(perf_loads.count() * sizeof(IOPerfRunner));
  char *modifyer_buf = (char *)malloc(perf_loads.count() * sizeof(IOConfModify));
  ObArray<IOPerfRunner *> runners;
  ObArray<IOConfModify *> modifyers;
  const int64_t start_ts = ObTimeUtility::current_time() + 10000L;
  for (int64_t i = 0; i < perf_loads.count(); ++i) {
    IOPerfRunner *runner = new (runner_buf + i * sizeof(IOPerfRunner)) IOPerfRunner();
    const IOPerfLoad &cur_load = perf_loads.at(i);
    ASSERT_SUCC(runner->init(start_ts, cur_load));
    ASSERT_SUCC(runners.push_back(runner));
    LOG_INFO("runner start now");
  }
  //prepare modifyer
  for (int64_t i = 0; i < perf_tenants.count(); ++i) {
    IOConfModify *modifyer=new (modifyer_buf + i * sizeof(IOConfModify)) IOConfModify();
    IOPerfTenant &curr_tenant = perf_tenants.at(i);
    int64_t modify_init_ts = start_ts;
    int64_t modify_delay_ts = 3000000L; //2s后开始修改
    ASSERT_SUCC(modifyer->init(modify_init_ts, modify_delay_ts, curr_tenant));
    ASSERT_SUCC(modifyers.push_back(modifyer));
  }
  // wait perf finished
  for (int64_t i = 0; i < runners.count(); ++i) {
    IOPerfRunner *runner = runners.at(i);
    runner->wait();
    ASSERT_SUCC(runner->print_result());
    runner->destroy();
  }
  free(runner_buf);
  free(modifyer_buf);

  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    ObLocalDevice *device_handle = perf_devices.at(i).device_handle_;
//    ASSERT_SUCC(OB_IO_MANAGER.remove_device_channel(device_handle));
    device_handle->destroy();
  }
  free(device_buf);
  LOG_INFO("wenqu: modify finished");
}

TEST_F(TestIOManager, ModifyCallbackThread)
{
  // use multi thread to do modify group_io_config
  bool is_perf_config_exist = false;
  ASSERT_SUCC(FileDirectoryUtils::is_exists(GROUP_PERF_CONFIG_FILE, is_perf_config_exist));
  if (!is_perf_config_exist) {
    write_group_perf_config();
  }
  // parse configs
  IOPerfScheduler scheduler_config;
  ObArray<IOPerfDevice> perf_devices;
  ObArray<IOPerfTenant> perf_tenants;
  ObArray<IOPerfLoad> perf_loads;
  ASSERT_SUCC(parse_group_perf_config(GROUP_PERF_CONFIG_FILE, scheduler_config, perf_devices, perf_tenants, perf_loads));
  ASSERT_TRUE(perf_devices.count() > 0);
  ASSERT_TRUE(perf_tenants.count() > 0);
  ASSERT_TRUE(perf_loads.count() > 0);

  ObIOManager::get_instance().destroy();
  const int64_t memory_limit = 30L * 1024L * 1024L * 1024L; // 30GB
  const int64_t queue_depth = 100L;
  ASSERT_SUCC(ObIOManager::get_instance().init(memory_limit, queue_depth, scheduler_config.sender_count_, scheduler_config.schedule_media_id_));
  ASSERT_SUCC(ObIOManager::get_instance().start());

  // prepare devices and files
  char *device_buf = (char *)malloc(sizeof(ObLocalDevice) * perf_devices.count());
  ASSERT_TRUE(nullptr != device_buf);
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    IOPerfDevice &curr_config = perf_devices.at(i);
    ASSERT_SUCC(prepare_file(curr_config.file_path_, curr_config.file_size_, curr_config.fd_));
    ObLocalDevice *device = new (device_buf + sizeof(ObLocalDevice) * i) ObLocalDevice;
    ASSERT_SUCC(init_device(curr_config.media_id_, *device));
    ASSERT_SUCC(OB_IO_MANAGER.add_device_channel(device, curr_config.async_channel_count_, curr_config.sync_channel_count_, curr_config.max_io_depth_));
    curr_config.device_handle_ = device;
  }
  // prepare tenant io manager
  for (int64_t i = 0; i < perf_tenants.count(); ++i) {
    IOPerfTenant &curr_config = perf_tenants.at(i);
    LOG_INFO("wenqu: tenant config", K(curr_config), K(i));
    ObRefHolder<ObTenantIOManager> tenant_holder;
    ASSERT_SUCC(OB_IO_MANAGER.get_tenant_io_manager(curr_config.tenant_id_, tenant_holder));
    ASSERT_SUCC(tenant_holder.get_ptr()->refresh_group_io_config());
  }
  // prepare perf runners
  char *runner_buf = (char *)malloc(perf_loads.count() * sizeof(IOPerfRunner));
  char *modifier_buf = (char *)malloc(perf_loads.count() * sizeof(IOConfModify));
  ObArray<IOPerfRunner *> runners;
  ObArray<IOCallbackModifier *> modifiers;
  const int64_t start_ts = ObTimeUtility::current_time() + 10000L;
  for (int64_t i = 0; i < perf_loads.count(); ++i) {
    IOPerfRunner *runner = new (runner_buf + i * sizeof(IOPerfRunner)) IOPerfRunner();
    const IOPerfLoad &cur_load = perf_loads.at(i);
    ASSERT_SUCC(runner->init(start_ts, cur_load));
    ASSERT_SUCC(runners.push_back(runner));
    LOG_INFO("runner start now");
  }
  //prepare modifier
  for (int64_t i = 0; i < perf_tenants.count(); ++i) {
    IOCallbackModifier *modifier=new (modifier_buf + i * sizeof(IOCallbackModifier)) IOCallbackModifier();
    IOPerfTenant &curr_tenant = perf_tenants.at(i);
    int64_t modify_init_ts = start_ts;
    int64_t modify_delay_ts = 2000000L; //2s后开始修改
    ASSERT_SUCC(modifier->init(modify_init_ts, modify_delay_ts, curr_tenant));
    ASSERT_SUCC(modifiers.push_back(modifier));
  }
  // wait perf finished
  for (int64_t i = 0; i < runners.count(); ++i) {
    IOPerfRunner *runner = runners.at(i);
    runner->wait();
    ASSERT_SUCC(runner->print_result());
    runner->destroy();
  }
  free(runner_buf);
  free(modifier_buf);

  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    ObLocalDevice *device_handle = perf_devices.at(i).device_handle_;
    device_handle->destroy();
  }
  free(device_buf);
  LOG_INFO("modify callback thread finished");
}

TEST_F(TestIOManager, ModifyGroupIO)
{
  // use multi thread to do modify group_io_config
  bool is_perf_config_exist = false;
  ASSERT_SUCC(FileDirectoryUtils::is_exists(GROUP_PERF_CONFIG_FILE, is_perf_config_exist));
  if (!is_perf_config_exist) {
    write_group_perf_config();
  }
  // parse configs
  IOPerfScheduler scheduler_config;
  ObArray<IOPerfDevice> perf_devices;
  ObArray<IOPerfTenant> perf_tenants;
  ObArray<IOPerfLoad> perf_loads;
  ASSERT_SUCC(parse_group_perf_config(GROUP_PERF_CONFIG_FILE, scheduler_config, perf_devices, perf_tenants, perf_loads));
  ASSERT_TRUE(perf_devices.count() > 0);
  ASSERT_TRUE(perf_tenants.count() > 0);
  ASSERT_TRUE(perf_loads.count() > 0);
  ObIOManager::get_instance().destroy();
  const int64_t memory_limit = 30L * 1024L * 1024L * 1024L; // 30GB
  const int64_t queue_depth = 100L;
  ASSERT_SUCC(ObIOManager::get_instance().init(memory_limit, queue_depth, scheduler_config.sender_count_, scheduler_config.schedule_media_id_));
  ASSERT_SUCC(ObIOManager::get_instance().start());
  // prepare devices and files
  char *device_buf = (char *)malloc(sizeof(ObLocalDevice) * perf_devices.count());
  ASSERT_TRUE(nullptr != device_buf);
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    IOPerfDevice &curr_config = perf_devices.at(i);
    ASSERT_SUCC(prepare_file(curr_config.file_path_, curr_config.file_size_, curr_config.fd_));
    ObLocalDevice *device = new (device_buf + sizeof(ObLocalDevice) * i) ObLocalDevice;
    ASSERT_SUCC(init_device(curr_config.media_id_, *device));
    ASSERT_SUCC(OB_IO_MANAGER.add_device_channel(device, curr_config.async_channel_count_, curr_config.sync_channel_count_, curr_config.max_io_depth_));
    curr_config.device_handle_ = device;
  }
  // prepare tenant io manager
  for (int64_t i = 0; i < perf_tenants.count(); ++i) {
    IOPerfTenant &curr_config = perf_tenants.at(i);
    if (curr_config.tenant_id_ == 1002) {
      LOG_INFO("qilu: tenant config", K(curr_config), K(i));
      ObRefHolder<ObTenantIOManager> tenant_holder;
      ASSERT_SUCC(OB_IO_MANAGER.get_tenant_io_manager(curr_config.tenant_id_, tenant_holder));
      ASSERT_SUCC(tenant_holder.get_ptr()->refresh_group_io_config());
    }
  }
  // prepare perf runners
  char *runner_buf = (char *)malloc(perf_loads.count() * sizeof(IOPerfRunner));
  char *modifyer_buf = (char *)malloc(perf_loads.count() * sizeof(IOGroupModify));
  ObArray<IOPerfRunner *> runners;
  ObArray<IOGroupModify *> modifyers;
  const int64_t start_ts = ObTimeUtility::current_time() + 10000L;
  for (int64_t i = 0; i < perf_loads.count(); ++i) {
    IOPerfRunner *runner = new (runner_buf + i * sizeof(IOPerfRunner)) IOPerfRunner();
    const IOPerfLoad &cur_load = perf_loads.at(i);
    if (cur_load.tenant_id_ == 1002) {
      ASSERT_SUCC(runner->init(start_ts, cur_load));
      ASSERT_SUCC(runners.push_back(runner));
      LOG_INFO("runner start now");
    }
  }
  //prepare modifyer
  for (int64_t i = 0; i < perf_tenants.count(); ++i) {
    IOPerfTenant &curr_tenant = perf_tenants.at(i);
    if (curr_tenant.tenant_id_ == 1002) {
      IOGroupModify *modifyer=new (modifyer_buf + i * sizeof(IOGroupModify)) IOGroupModify();
      int64_t modify_init_ts = start_ts;
      int64_t modify_delay_ts = 3000000L; //3s后开始修改
      ASSERT_SUCC(modifyer->init(modify_init_ts, modify_delay_ts, curr_tenant));
      ASSERT_SUCC(modifyers.push_back(modifyer));
    }
  }
  // wait perf finished
  for (int64_t i = 0; i < runners.count(); ++i) {
    IOPerfRunner *runner = runners.at(i);
    runner->wait();
    ASSERT_SUCC(runner->print_result());
    runner->destroy();
  }
  free(runner_buf);
  free(modifyer_buf);
  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
  for (int64_t i = 0; i < perf_devices.count(); ++i) {
    ObLocalDevice *device_handle = perf_devices.at(i).device_handle_;
//    ASSERT_SUCC(OB_IO_MANAGER.remove_device_channel(device_handle));
    device_handle->destroy();
  }
  free(device_buf);
  LOG_INFO("qilu: modify group finished");
}


TEST_F(TestIOManager, abnormal)
{
  // simulate submit failure
  // simulate get_event failure
  // simulate device hang
}

#define LOG_FILE_PATH "./test_io_manager.log"

int main(int argc, char **argv)
{
  set_memory_limit(20L * 1024L * 1024L * 1024L);
  system("rm -rf " LOG_FILE_PATH "*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_file_name(LOG_FILE_PATH, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/****************                 test io callback              ******************/
TestIOCallback::~TestIOCallback()
{
  if (nullptr != number_) {
    *number_ -= 90;
    number_ = nullptr;
  }
  if (nullptr != allocator_) {
    if (nullptr != help_buf_) {
      allocator_->free(help_buf_);
      help_buf_ = nullptr;
    }
    allocator_->free(this);
    LOG_INFO("success reset callback when out_rec_cnt = 0");
  }
}



int TestIOCallback::alloc_data_buf(const char *io_data_buffer, const int64_t data_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("Invalid data, the allocator is NULL, ", K(ret));
  } else if (OB_UNLIKELY(data_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid data buffer size", K(ret), K(data_size));
  } else if (OB_ISNULL(help_buf_ = static_cast<char *>(allocator_->alloc(data_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate help buf", K(ret), K(data_size), KP(help_buf_));
  } else {
    memset(help_buf_, 0, data_size);
    MEMCPY(help_buf_, io_data_buffer, data_size);
  }
  return OB_SUCCESS;
}

int TestIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    // for test, ignore
  } else if (OB_FAIL(alloc_data_buf(data_buffer, size))) {
    LOG_WARN("Fail to allocate memory, ", K(ret), K(size));
  }
  if (nullptr != number_) {
    *number_ += 100;
  }
  return OB_SUCCESS;
}

/****************                io perf              ******************/
void write_group_perf_config()
{
  int fd = -1;
  const char *file_name = GROUP_PERF_CONFIG_FILE;
  if (0 > (fd = ::open(file_name, O_RDWR | O_CREAT | O_TRUNC, 0644))) {
    LOG_WARN_RET(OB_ERR_SYS, "open perf config file failed", K(fd), K(file_name));
  } else {
    const char *file_buf =
      "sender_count      schedule_media_id     io_greed\n"
      "8                 0                     0\n"
      "\n"
      "device_id   media_id    async_channel   sync_channel    max_io_depth    file_size_gb    file_path\n"
      "1           0           8               1               64              1               ./perf_test\n"
      "\n"
      "tenant_id   min_iops    max_iops    weight          group\n"
      "1        5000        100000       700             10001: testgroup1: 80, 100, 60; 10002: testgroup2: 10, 60, 30; 0: OTHER_GROUPS: 10, 100, 10;\n"
      "500        1000        50000        1000            12345: testgroup1: 50, 50, 50; 0: OTHER_GROUPS: 50, 50, 50;\n"
      "\n"
      "tenant_id   device_id     group    io_mode     io_size_byte    io_depth    perf_mode     target_iops     thread_count    is_sequence     start_s    stop_s\n"
      "1        1             0        r           16384           10          rolling       0               16              0               0          8\n"
      "1        1             10001        r           16384           10          rolling       0               16              0               2          7\n"
      "1        1             10002        r           16384           10          rolling       0               16              0               0          6\n"
      "500        1             0        r           16384           100          rolling       0               16              0               0          10\n"
      "500        1             12345        r           16384           100          rolling       0               16              0               0          10\n"
      ;
    const int64_t file_len = strlen(file_buf);
    int write_ret = ::write(fd, file_buf, file_len);
    if (write_ret < file_len) {
      LOG_WARN_RET(OB_ERR_SYS, "write file content failed", K(write_ret), K(file_len));
    }
    close(fd);
  }
}

int parse_group_perf_config(const char *config_file_path,
                      IOPerfScheduler &scheduler_config,
                      ObIArray<IOPerfDevice> &perf_devices,
                      ObIArray<IOPerfTenant> &perf_tenants,
                      ObIArray<IOPerfLoad> &perf_loads)
{
  int ret = OB_SUCCESS;
  bool is_file_exist = false;
  FILE *file = nullptr;
  if (OB_FAIL(FileDirectoryUtils::is_exists(config_file_path, is_file_exist))) {
    LOG_WARN("failed to check file exists", K(ret));
  } else if (!is_file_exist) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("file not exist!");
  } else if (OB_UNLIKELY(nullptr == (file = ::fopen(config_file_path, "r")))) {
    ret = OB_IO_ERROR;
    LOG_WARN("failed to open iops data file, ", K(ret), K(errno), KERRMSG);
  } else {
    char curr_line[1024] = { 0 };
    const char *scheduler_header = "sender_count      schedule_media_id     io_greed";
    const char *device_header = "device_id   media_id    async_channel   sync_channel    max_io_depth    file_size_gb    file_path";
    const char *tenant_header = "tenant_id   min_iops    max_iops    weight          group";
    const char *load_header = "tenant_id   device_id     group    io_mode     io_size_byte    io_depth    perf_mode     target_iops     thread_count    is_sequence     start_s    stop_s";
    enum class PerfConfigType { SCHEDULER, DEVICE, TENANT, LOAD, MAX };
    PerfConfigType config_type = PerfConfigType::MAX;
    while (OB_SUCC(ret)) {
      if (OB_UNLIKELY(nullptr == fgets(curr_line, sizeof(curr_line), file))) {
        ret = OB_ITER_END;
      } else if (' ' == curr_line[0] || '\0' == curr_line[0] || '#' == curr_line[0]) {
        // ignore empty line
      } else if (0 == atoi(curr_line)) {
        if (0 == strncmp(curr_line, scheduler_header, strlen(scheduler_header))) {
          config_type = PerfConfigType::SCHEDULER;
        } else if (0 == strncmp(curr_line, device_header, strlen(device_header))) {
          config_type = PerfConfigType::DEVICE;
        } else if (0 == strncmp(curr_line, tenant_header, strlen(tenant_header))) {
          config_type = PerfConfigType::TENANT;
        } else if (0 == strncmp(curr_line, load_header, strlen(load_header))) {
          config_type = PerfConfigType::LOAD;
        }
      } else if (PerfConfigType::SCHEDULER == config_type) {
        int scan_ret = sscanf(curr_line, "%ld%ld%ld\n",
            &scheduler_config.sender_count_, &scheduler_config.schedule_media_id_, &scheduler_config.io_greed_);
        if (OB_UNLIKELY(3 != scan_ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("scan config file failed", K(ret), K(scan_ret));
        }
        LOG_INFO("qilu: parse scheduler config", K(ret), K(scheduler_config));
      } else if (PerfConfigType::DEVICE == config_type) {
        IOPerfDevice item;
        int scan_ret = sscanf(curr_line, "%d%d%d%d%d%ld%s\n",
            &item.device_id_, &item.media_id_, &item.async_channel_count_, &item.sync_channel_count_, &item.max_io_depth_, &item.file_size_, item.file_path_);
        if (OB_UNLIKELY(7 != scan_ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("scan config file failed", K(ret), K(scan_ret));
        } else {
          item.file_size_ *= 1024L * 1024L * 1024L;
          if (OB_UNLIKELY(!item.is_valid())) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid data", K(ret), K(item));
          } else if (OB_FAIL(perf_devices.push_back(item))) {
            LOG_WARN("add item failed", K(ret), K(item));
          }
          LOG_INFO("qilu: parse device", K(ret), K(item));
        }
      } else if (PerfConfigType::TENANT == config_type) {
        IOPerfTenant item;
        char group_config[1024] = { 0 };
        int scan_ret = sscanf(curr_line, "%d%ld%ld%ld%[^\n]\n",
            &item.tenant_id_, &item.config_.unit_config_.min_iops_, &item.config_.unit_config_.max_iops_,
            &item.config_.unit_config_.weight_, group_config);
        if (OB_UNLIKELY(5 != scan_ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("scan config file failed", K(ret), K(scan_ret));
        } else {
          item.config_.memory_limit_ = IO_MEMORY_LIMIT;
          item.config_.callback_thread_count_ = 0;
          // parse group config
          if (OB_FAIL(item.config_.parse_group_config(group_config))) {
            LOG_WARN("parse group config failed", K(ret), K(group_config));
          } else if (OB_UNLIKELY(!item.is_valid())) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid data", K(ret), K(item));
          } else if (OB_FAIL(perf_tenants.push_back(item))) {
            LOG_WARN("add item failed", K(ret), K(item));
          }
          LOG_INFO("qilu: parse tenant", K(ret), K(item), K(group_config), K(item.config_));
        }
      } else if (PerfConfigType::LOAD == config_type) {
        IOPerfLoad item;
        char io_mode[16] = { 0 };
        char perf_mode[16] = { 0 };
        int scan_ret = sscanf(curr_line, "%d%d%ld%s%d%d%s%ld%d%d%ld%ld\n",
            &item.tenant_id_, &item.device_id_, &item.group_id_, io_mode,
            &item.size_, &item.depth_, perf_mode, &item.iops_, &item.thread_count_,
            (int *)&item.is_sequence_, &item.start_delay_ts_, &item.stop_delay_ts_);
        if (OB_UNLIKELY(12 != scan_ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("scan config file failed", K(ret), K(scan_ret));
        } else {
          item.start_delay_ts_ *= 1000000L;
          item.stop_delay_ts_ *= 1000000L;
          switch (io_mode[0]) {
            case 'r' :
              item.mode_ = ObIOMode::READ;
              break;
            case 'w':
              item.mode_ = ObIOMode::WRITE;
              break;
            default:
              break;
          }
          const char *rolling_str = "rolling";
          const char *batch_str = "batch";
          const char *async_str = "async";
          if (0 == strncmp(perf_mode, rolling_str, strlen(rolling_str))) {
            item.perf_mode_ = IOPerfMode::ROLLING;
          } else if (0 == strncmp(perf_mode, batch_str, strlen(batch_str))) {
            item.perf_mode_ = IOPerfMode::BATCH;
          } else if (0 == strncmp(perf_mode, async_str, strlen(async_str))) {
            item.perf_mode_ = IOPerfMode::ASYNC;
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < perf_devices.count(); ++i) {
            if (item.device_id_ == perf_devices.at(i).device_id_) {
              item.device_ = &perf_devices.at(i);
              break;
            }
          }
          if (OB_UNLIKELY(!item.is_valid())) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid data", K(ret), K(item));
          } else if (OB_FAIL(perf_loads.push_back(item))) {
            LOG_WARN("add item failed", K(ret), K(item));
          }
          LOG_INFO("qilu: parse load", K(ret), K(item), K(io_mode), K(item.group_id_));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("config type not match", K(ret), K(config_type));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    ::fclose(file);
  }
  return ret;
}

int IOPerfRunner::init(const int64_t absolute_ts, const IOPerfLoad &load)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(load));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_BIG_BLOCK_SIZE, "perf runner", OB_SERVER_TENANT_ID, 1024L * 1024L * 1024L * 10L))) {
    LOG_WARN("init allocator failed", K(ret));
  } else if (OB_FAIL(handle_queue_.init(1000L * 10000L, &allocator_))) {
    LOG_WARN("init handle queue failed", K(ret));
  } else {
    abs_ts_ = absolute_ts;
    load_ = load;
    // prepare file
    fd_.first_id_ = ObIOFd::NORMAL_FILE_ID;
    fd_.second_id_ = load.device_->fd_;
    fd_.device_handle_ = load.device_->device_handle_;

    // prepare write buffer
    if (OB_SUCC(ret) && ObIOMode::WRITE == load_.mode_ && nullptr == write_buf_) {
      const int64_t buf_size = load_.size_ + DIO_READ_ALIGN_SIZE;
      void *tmp_buf = ob_malloc(buf_size, ObNewModIds::TEST);
      if (nullptr == tmp_buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(buf_size));
      } else {
        memset(tmp_buf, 'a', buf_size);
        write_buf_ = reinterpret_cast<const char *>(tmp_buf);
      }
    }

    // prepare read buffer
    if (OB_SUCC(ret) && ObIOMode::READ == load_.mode_ && nullptr == user_buf_) {
      const int64_t buf_size = load_.size_;
      void *tmp_buf = ob_malloc(buf_size, ObNewModIds::TEST);
      if (nullptr == tmp_buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(buf_size));
      } else {
        memset(tmp_buf, '0', buf_size);
        user_buf_ = reinterpret_cast<char *>(tmp_buf);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_thread_count(load_.thread_count_ + 2))) {
      LOG_WARN("set thread count failed", K(ret), K(load_));
    } else if (OB_FAIL(start())) {
      LOG_WARN("start thread pool failed", K(ret), K(load_));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void IOPerfRunner::destroy()
{
  stop();
  wait();
  load_ = IOPerfLoad();
  result_ = IOPerfResult();
  last_offset_ = 0;
  if (nullptr != write_buf_) {
    ob_free((void *)write_buf_);
    write_buf_ = nullptr;
  }
  if (nullptr != user_buf_) {
    ob_free((void *)user_buf_);
    user_buf_ = nullptr;
  }
  fd_.reset();
}

void IOPerfRunner::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_idx = get_thread_idx();
  if (thread_idx < load_.thread_count_) {
  const int64_t current_ts = ObTimeUtility::current_time();
  if (abs_ts_ + load_.start_delay_ts_ > current_ts) {
    usleep(abs_ts_ + load_.start_delay_ts_ - current_ts);
  }
  ATOMIC_CAS(&result_.start_delay_ts_, 0, ObTimeUtility::current_time());
    if (IOPerfMode::ROLLING == load_.perf_mode_)  {
      do_perf_rolling();
    } else {
      do_perf_batch();
    }
  } else if (thread_idx == load_.thread_count_) {
    const int64_t current_ts = ObTimeUtility::current_time();
    if (abs_ts_ + load_.stop_delay_ts_ > current_ts) {
      usleep(abs_ts_ + load_.stop_delay_ts_ - current_ts);
    }
    stop();
    usleep(100L * 1000L);
    wait_handles(handle_queue_);
    result_.stop_delay_ts_ = ObTimeUtility::current_time();
  } else {
    while (!has_set_stop()) {
      usleep(10L * 1000L);
      wait_handles(handle_queue_);
    }
  }
}

int IOPerfRunner::do_perf_batch()
{
  int ret = OB_SUCCESS;
  static const int64_t check_interval_ms = 50L; // 50ms
  int64_t io_count = 0;
  const int64_t check_count = load_.iops_ / load_.thread_count_ / (1000L / check_interval_ms);
  int64_t last_check_ts = ObTimeUtility::fast_current_time();
  const bool need_control_io_speed = 0 != load_.iops_;
  LOG_INFO("perf start", K(load_.tenant_id_), K(load_.group_id_));
  while (!has_set_stop()) {
    (void) do_batch_io();
    if (need_control_io_speed) {
      io_count += load_.depth_;
      if (io_count > check_count) {
        const int64_t sleep_us = check_interval_ms * 1000L - (ObTimeUtility::fast_current_time() - last_check_ts);
        if (sleep_us > 0) {
          usleep(sleep_us);
        }
        io_count -= check_count;
        last_check_ts = ObTimeUtility::fast_current_time();
      }
    }
  }
  return ret;
}

int IOPerfRunner::do_perf_rolling()
{
  int ret = OB_SUCCESS;
  ObIOInfo info;
  info.tenant_id_ = load_.tenant_id_;
  info.flag_.set_resource_group_id(load_.group_id_);
  info.flag_.set_mode(load_.mode_);
  info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  info.fd_ = fd_;
  info.size_ = load_.size_;
  info.timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  if (ObIOMode::WRITE == load_.mode_) {
    info.buf_ = reinterpret_cast<const char *>(upper_align(reinterpret_cast<int64_t>(write_buf_), DIO_READ_ALIGN_SIZE));
  } else if (ObIOMode::READ == load_.mode_) {
    info.user_data_buf_ = user_buf_;
  }
  ObArray<ObIOHandle *> handles;
  RLOCAL(int64_t, local_io_count);
  local_io_count = 0;
  const int64_t max_offset = load_.device_->file_size_ - load_.size_;
  for (int32_t i = 0; OB_SUCC(ret) && i < load_.depth_; ++i) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObIOHandle)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      ObIOHandle *cur_handle  = new (buf) ObIOHandle;
      if (load_.is_sequence_) {
        last_offset_ += load_.size_;
        info.offset_ = last_offset_ % (max_offset);
      } else {
        info.offset_ = lower_align(ObRandom::rand(0, max_offset), DIO_READ_ALIGN_SIZE);
      }
      if (info.flag_.is_read()) {
        if (OB_FAIL(ObIOManager::get_instance().aio_read(info, *cur_handle))) {
          LOG_WARN("fail to aio read", K(ret), K(i));
        }
      } else if (info.flag_.is_write()) {
        if (OB_FAIL(ObIOManager::get_instance().aio_write(info, *cur_handle))) {
          LOG_WARN("fail to aio write", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        ++local_io_count;
        if (OB_FAIL(handles.push_back(cur_handle))) {
          LOG_WARN("push back handle failed", K(ret), KPC(cur_handle));
        }
      }
    }
  }
  int64_t pos = 0;
  while (!has_set_stop() && OB_SUCC(ret)) {
    if (REACH_TIME_INTERVAL(1000L * 1000L)) {
      ATOMIC_FAA(&io_count_, local_io_count);
      ATOMIC_FAA(&total_io_count_, local_io_count);
      local_io_count = 0;
      ATOMIC_INC(&report_count_);
      if (load_.thread_count_ == ATOMIC_LOAD(&report_count_)) {
//        LOG_INFO("IO STATUS: perf io count", K(io_count_), K(load_.category_), K(total_io_count_));
        report_count_ = 0;
        io_count_ = 0;
      }
    }
    pos = pos % handles.count();
    ObIOHandle *cur_handle = handles[pos];
    wait_and_count(*cur_handle);
    if (load_.is_sequence_) {
      last_offset_ += load_.size_;
      info.offset_ = last_offset_ % (max_offset);
    } else {
      info.offset_ = lower_align(ObRandom::rand(0, max_offset), DIO_READ_ALIGN_SIZE);
    }
    if (info.flag_.is_read()) {
      if (OB_FAIL(ObIOManager::get_instance().aio_read(info, *cur_handle))) {
        LOG_WARN("fail to aio read", K(ret));
      }
    } else if (info.flag_.is_write()) {
      if (OB_FAIL(ObIOManager::get_instance().aio_write(info, *cur_handle))) {
        LOG_WARN("fail to aio write", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ++local_io_count;
    }
    ++pos;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < handles.count(); ++i) {
    int64_t tmp_pos = (pos + i) % handles.count();
    ObIOHandle *cur_handle = handles[tmp_pos];
    wait_and_count(*cur_handle);
    cur_handle->~ObIOHandle();
    allocator_.free(cur_handle);
  }
  return ret;
}

int IOPerfRunner::do_batch_io()
{
  int ret = OB_SUCCESS;
  ObIOInfo info;
  info.tenant_id_ = load_.tenant_id_;
  info.flag_.set_resource_group_id(load_.group_id_);
  info.flag_.set_mode(load_.mode_);
  info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  info.fd_ = fd_;
  info.size_ = load_.size_;
  info.timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  if (ObIOMode::WRITE == load_.mode_) {
    info.buf_ = reinterpret_cast<const char *>(upper_align(reinterpret_cast<int64_t>(write_buf_), DIO_READ_ALIGN_SIZE));
  } else if (ObIOMode::READ == load_.mode_) {
    info.user_data_buf_ = user_buf_;
  }
  ObFixedQueue<ObIOHandle> local_handles;
  if (OB_FAIL(local_handles.init(load_.depth_ * 2))) {
    LOG_WARN("init handles array failed", K(ret));
  }
  ObFixedQueue<ObIOHandle> &handles = IOPerfMode::ASYNC == load_.perf_mode_ ? handle_queue_ : local_handles;
  const int64_t max_offset = load_.device_->file_size_ - load_.size_;
  for (int32_t i = 0; OB_SUCC(ret) && i < load_.depth_; ++i) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObIOHandle)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      ObIOHandle *cur_handle  = new (buf) ObIOHandle;
      if (load_.is_sequence_) {
        last_offset_ += load_.size_;
        info.offset_ = last_offset_ % (max_offset);
      } else {
        info.offset_ = lower_align(ObRandom::rand(0, max_offset), DIO_READ_ALIGN_SIZE);
      }
      if (info.flag_.is_read()) {
        if (OB_FAIL(ObIOManager::get_instance().aio_read(info, *cur_handle))) {
          LOG_WARN("fail to aio read", K(ret), K(i));
        }
      } else if (info.flag_.is_write()) {
        if (OB_FAIL(ObIOManager::get_instance().aio_write(info, *cur_handle))) {
          LOG_WARN("fail to aio write", K(ret), K(i));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(handles.push(cur_handle))) {
        LOG_WARN("push back handle failed", K(ret), KPC(cur_handle));
      } else if (local_handles.get_total() > 0) {
        wait_handles(local_handles);
      }
    }
  }
  return ret;
}

int IOPerfRunner::wait_and_count(ObIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(io_handle.wait())) {
    LOG_WARN("fail to wait read io", K(ret));
    ATOMIC_INC(&result_.fail_count_);
  } else {
    ATOMIC_INC(&result_.succ_count_);
  }
  const int64_t delay = max(0, io_handle.get_rt());
  ATOMIC_FAA(&result_.sum_rt_, delay);
  return ret;
}

int IOPerfRunner::wait_handles(ObFixedQueue<ObIOHandle> &handles)
{
  int ret = OB_SUCCESS;
  ObIOHandle *cur_handle = nullptr;
  while (OB_SUCC(handles.pop(cur_handle))) {
    wait_and_count(*cur_handle);
    cur_handle->~ObIOHandle();
    allocator_.free(cur_handle);
  }
  return ret;
}

int IOPerfRunner::print_result()
{
  int ret = OB_SUCCESS;
  if (!(result_.start_delay_ts_ > 0 && result_.stop_delay_ts_ > 0 && result_.stop_delay_ts_ > result_.start_delay_ts_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io perf not finished", K(ret), K(result_));
  } else {
    LOG_INFO("io perf result",
        "perf_config", load_,
        "succ_cnt", result_.succ_count_,
        "fail_cnt", result_.fail_count_,
        "time_ms", (result_.stop_delay_ts_ - result_.start_delay_ts_) / 1000,
        "iops", result_.succ_count_ * 1000000L / max(1, result_.stop_delay_ts_ - result_.start_delay_ts_),
        "rt", result_.sum_rt_ / max(1, result_.succ_count_),
        "disk_rt", result_.disk_rt_ / max(1, result_.succ_count_));
  }
  return ret;
}

int IOConfModify::init(int64_t modify_init_ts, int64_t modify_delay_ts, const IOPerfTenant &curr_tenant)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!curr_tenant.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(curr_tenant));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_BIG_BLOCK_SIZE, "Modifier runner", OB_SERVER_TENANT_ID, 1024L * 1024L * 1024L * 10L))) {
    LOG_WARN("init allocator failed", K(ret));
  } else {
    curr_tenant_ = curr_tenant;
    modify_init_ts_ = modify_init_ts;
    modify_delay_ts_ = modify_delay_ts;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_thread_count(load_.thread_count_ + 1))) {
      LOG_WARN("set thread count failed", K(ret), K(modify_init_ts_), K(curr_tenant_));
    } else if (OB_FAIL(start())) {
      LOG_WARN("start thread failed", K(ret), K(modify_init_ts_), K(curr_tenant_));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void IOConfModify::destroy()
{
  stop();
  wait();
  curr_tenant_ = IOPerfTenant();
}

void IOConfModify::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_idx = get_thread_idx();
  LOG_INFO("modify thread start");
  const int64_t current_ts = ObTimeUtility::current_time();
  if (modify_init_ts_ + modify_delay_ts_ > current_ts) {
    usleep(modify_init_ts_ + modify_delay_ts_ - current_ts);
  }
  int64_t min_test = 50000;
  int64_t max_test = 1000000;
  int64_t weight_test = 1000;
  if (OB_FAIL(modify_tenant_io(min_test, max_test, weight_test, curr_tenant_))) {
    LOG_WARN("modify config failed", K(ret), K(curr_tenant_));
  }
}
int IOConfModify::modify_tenant_io( const int64_t min_iops,
                                    const int64_t max_iops,
                                    const int64_t weight,
                                    IOPerfTenant &curr_tenant)
{
  int ret = OB_SUCCESS;
  //ObTenantIOConfig io_config;
  curr_tenant.config_.unit_config_.min_iops_ = min_iops;
  curr_tenant.config_.unit_config_.max_iops_ = max_iops;
  curr_tenant.config_.unit_config_.weight_ = weight;

  if (OB_FAIL(OB_IO_MANAGER.refresh_tenant_io_config(curr_tenant.tenant_id_, curr_tenant.config_))) {
    LOG_WARN("refresh tenant io config failed", K(ret), K(curr_tenant.tenant_id_), K(curr_tenant.config_));
  }
  return ret;
}

int IOGroupModify::init(int64_t modify_init_ts, int64_t modify_delay_ts, const IOPerfTenant &curr_tenant)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!curr_tenant.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(curr_tenant));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_BIG_BLOCK_SIZE, "group modifier", OB_SERVER_TENANT_ID, 1024L * 1024L * 1024L * 10L))) {
    LOG_WARN("init allocator failed", K(ret));
  } else {
    curr_tenant_ = curr_tenant;
    modify_init_ts_ = modify_init_ts;
    modify_delay_ts_ = modify_delay_ts;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_thread_count(load_.thread_count_ + 1))) {
      LOG_WARN("set thread count failed", K(ret), K(modify_init_ts_), K(curr_tenant_));
    } else if (OB_FAIL(start())) {
      LOG_WARN("start thread failed", K(ret), K(modify_init_ts_), K(curr_tenant_));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void IOGroupModify::destroy()
{
  stop();
  wait();
  curr_tenant_ = IOPerfTenant();
}

void IOGroupModify::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_idx = get_thread_idx();
  LOG_INFO("modify thread start");

  //change 1
  int64_t current_ts = ObTimeUtility::current_time();
  if (modify_init_ts_ + modify_delay_ts_ > current_ts) {
    usleep(modify_init_ts_ + modify_delay_ts_ - current_ts);
  }
  if (OB_FAIL(OB_IO_MANAGER.modify_group_io_config(curr_tenant_.tenant_id_, INT64_MAX, 100, 100, 100))) {
    LOG_WARN("fail to modify group config", K(ret));
  } else if (OB_FAIL(OB_IO_MANAGER.modify_group_io_config(curr_tenant_.tenant_id_, 0, 0, 0, 0))) {
    LOG_WARN("fail to modify group config", K(ret));
  }

  //change 2
  usleep(3000L * 1000L); // sleep 3s
  if (OB_FAIL(OB_IO_MANAGER.modify_group_io_config(curr_tenant_.tenant_id_, INT64_MAX, 40, 40, 40))) {
    LOG_WARN("fail to modify group config", K(ret));
  } else if (OB_FAIL(OB_IO_MANAGER.modify_group_io_config(curr_tenant_.tenant_id_, 0, 60, 60, 60))) {
    LOG_WARN("fail to modify group config", K(ret));
  }
}

int IOTracerSwitch::init(int64_t switch_init_ts, int64_t switch_delay_ts, const IOPerfTenant &curr_tenant)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!curr_tenant.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(curr_tenant));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_BIG_BLOCK_SIZE, "Switch runner", OB_SERVER_TENANT_ID, 1024L * 1024L * 1024L * 10L))) {
    LOG_WARN("init allocator failed", K(ret));
  } else {
    curr_tenant_ = curr_tenant;
    switch_init_ts_ = switch_init_ts;
    switch_delay_ts_ = switch_delay_ts;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_thread_count(load_.thread_count_ + 1))) {
      LOG_WARN("set thread count failed", K(ret), K(load_));
    } else if (OB_FAIL(start())) {
      LOG_WARN("start thread failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void IOTracerSwitch::destroy()
{
  stop();
  wait();
  curr_tenant_ = IOPerfTenant();
}

void IOTracerSwitch::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_idx = get_thread_idx();
  LOG_INFO("modify thread start");
  const int64_t current_ts = ObTimeUtility::current_time();
  if (switch_init_ts_ + switch_delay_ts_ > current_ts) {
    usleep(switch_init_ts_ + switch_delay_ts_ - current_ts);
  }
  if (OB_FAIL(modify_tenant_io(curr_tenant_))) {
    LOG_WARN("modify config failed", K(ret), K(curr_tenant_));
  }
}

int IOTracerSwitch::modify_tenant_io(IOPerfTenant &curr_tenant)
{
  int ret = OB_SUCCESS;
  ATOMIC_SET(&curr_tenant.config_.enable_io_tracer_, true);
  if (OB_FAIL(OB_IO_MANAGER.refresh_tenant_io_config(curr_tenant.tenant_id_, curr_tenant.config_))) {
    LOG_WARN("refresh tenant io config failed", K(ret), K(curr_tenant.tenant_id_), K(curr_tenant.config_));
  }
  return ret;
}

int IOCallbackModifier::init(int64_t modify_init_ts, int64_t modify_delay_ts, const IOPerfTenant &curr_tenant)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!curr_tenant.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(curr_tenant));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_BIG_BLOCK_SIZE, "Modifier runner", OB_SERVER_TENANT_ID, 1024L * 1024L * 1024L * 10L))) {
    LOG_WARN("init allocator failed", K(ret));
  } else {
    curr_tenant_ = curr_tenant;
    modify_init_ts_ = modify_init_ts;
    modify_delay_ts_ = modify_delay_ts;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_thread_count(load_.thread_count_ + 1))) {
      LOG_WARN("set thread count failed", K(ret), K(modify_init_ts_), K(curr_tenant_));
    } else if (OB_FAIL(start())) {
      LOG_WARN("start thread failed", K(ret), K(modify_init_ts_), K(curr_tenant_));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void IOCallbackModifier::destroy()
{
  stop();
  wait();
  curr_tenant_ = IOPerfTenant();
}

void IOCallbackModifier::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_idx = get_thread_idx();
  LOG_INFO("modify thread start");
  const int64_t current_ts = ObTimeUtility::current_time();
  if (modify_init_ts_ + modify_delay_ts_ > current_ts) {
    usleep(modify_init_ts_ + modify_delay_ts_ - current_ts);
  }
  int64_t new_callback_num = 16;
  if (OB_FAIL(modify_callback_num(new_callback_num, curr_tenant_))) {
    LOG_WARN("modify config failed", K(ret), K(curr_tenant_));
  } else {
    LOG_INFO("modify callback thread num success", K(curr_tenant_));
  }
}
int IOCallbackModifier::modify_callback_num(const int64_t thread_num,
                                            IOPerfTenant &curr_tenant)
{
  int ret = OB_SUCCESS;
  curr_tenant.config_.callback_thread_count_ = thread_num;

  if (OB_FAIL(OB_IO_MANAGER.refresh_tenant_io_config(curr_tenant.tenant_id_, curr_tenant.config_))) {
    LOG_WARN("refresh tenant io config failed", K(ret), K(curr_tenant.tenant_id_), K(curr_tenant.config_));
  }
  return ret;
}
