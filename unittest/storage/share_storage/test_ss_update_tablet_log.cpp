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
#include "gtest/gtest.h"
#include <thread>
#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/incremental/share/ob_shared_tablet_extra_info.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

class TestSSUpdateTabletLog : public ::testing::Test
{
public:
  TestSSUpdateTabletLog();
  virtual ~TestSSUpdateTabletLog();
  virtual void SetUp();
  virtual void TearDown();

private:
  void init_update_log_();
  void init_ss_update_log_();

private:
  static const int32_t BLOCK_SIZE = 2 * 1024 * 1024;
  ObUpdateTabletLog update_log_;
  ObSSUpdateTabletLog ss_update_log_;
};

TestSSUpdateTabletLog::TestSSUpdateTabletLog()
{
}

TestSSUpdateTabletLog::~TestSSUpdateTabletLog()
{
}

void TestSSUpdateTabletLog::SetUp()
{
  init_update_log_();
  init_ss_update_log_();
}

void TestSSUpdateTabletLog::TearDown()
{
}

void TestSSUpdateTabletLog::init_update_log_()
{
  update_log_.ls_id_ = ObLSID(1001);
  update_log_.tablet_id_ = ObTabletID(200001);
  update_log_.ls_epoch_ = 0;
  update_log_.disk_addr_.set_sslog_tablet_meta_addr(update_log_.ls_id_,
                                                    update_log_.tablet_id_,
                                                    SCN::max_scn(),
                                                    SCN::max_scn(),
                                                    /*offset*/0,
                                                    /*size*/4096);
  //iter attr
  update_log_.tablet_attr_.iter_attr_.has_nested_table_ = false;
  update_log_.tablet_attr_.iter_attr_.has_transfer_table_ = false;
  update_log_.tablet_attr_.iter_attr_.is_empty_shell_ = false;
  update_log_.tablet_attr_.iter_attr_.valid_ = true;
  //calc space size
  update_log_.tablet_attr_.all_sstable_data_occupy_size_ = 100;
  update_log_.tablet_attr_.all_sstable_data_required_size_ = 100;
  update_log_.tablet_attr_.auto_part_size_ = 0;
  update_log_.tablet_attr_.backup_bytes_ = 0;
  update_log_.tablet_attr_.tablet_meta_size_ = 100;
  update_log_.tablet_attr_.ss_public_sstable_occupy_size_ = 100;

  update_log_.tablet_attr_.ha_status_ = 0;
  update_log_.tablet_attr_.ss_change_version_ = SCN::min_scn();
  update_log_.tablet_attr_.notify_ss_change_version_ = SCN::min_scn();
  update_log_.tablet_attr_.tablet_max_checkpoint_scn_ = SCN::min_scn();
  update_log_.tablet_attr_.last_match_tablet_meta_version_ = 0;

  update_log_.accelerate_info_.clog_checkpoint_scn_ = SCN::min_scn();
  update_log_.accelerate_info_.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  update_log_.accelerate_info_.ddl_checkpoint_scn_ = SCN::min_scn();
  update_log_.accelerate_info_.mds_checkpoint_scn_ = SCN::min_scn();
  update_log_.accelerate_info_.transfer_info_.reset();
  ASSERT_EQ(true, update_log_.is_valid());
}

void TestSSUpdateTabletLog::init_ss_update_log_()
{
  ss_update_log_.ls_id_ = ObLSID(1001);
  ss_update_log_.tablet_id_ = ObTabletID(200001);
  ss_update_log_.reorg_scn_ = SCN::min_scn();
  //iter attr
  ss_update_log_.ss_tablet_attr_.iter_attr_.has_nested_table_ = false;
  ss_update_log_.ss_tablet_attr_.iter_attr_.has_transfer_table_ = false;
  ss_update_log_.ss_tablet_attr_.iter_attr_.is_empty_shell_ = false;
  ss_update_log_.ss_tablet_attr_.iter_attr_.valid_ = true;
  //calc space size
  ss_update_log_.ss_tablet_attr_.all_sstable_data_occupy_size_ = 100;
  ss_update_log_.ss_tablet_attr_.all_sstable_data_required_size_ = 100;
  ss_update_log_.ss_tablet_attr_.backup_bytes_ = 0;
  ss_update_log_.ss_tablet_attr_.tablet_meta_size_ = 100;
  ss_update_log_.ss_tablet_attr_.ss_public_sstable_occupy_size_ = 100;

  ss_update_log_.ss_tablet_attr_.ha_status_ = 0;

  ss_update_log_.accelerate_info_.clog_checkpoint_scn_ = SCN::min_scn();
  update_log_.accelerate_info_.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  ss_update_log_.accelerate_info_.ddl_checkpoint_scn_ = SCN::min_scn();
  ss_update_log_.accelerate_info_.mds_checkpoint_scn_ = SCN::min_scn();
  ss_update_log_.accelerate_info_.transfer_info_.reset();
  ss_update_log_.must_cache_size_ = 1000;
  ss_update_log_.try_cache_size_ = 1000;
  ss_update_log_.tablet_serialize_size_ = 4096;
  ASSERT_EQ(true, ss_update_log_.is_valid());
}

TEST_F(TestSSUpdateTabletLog, update_tablet_log)
{
  int ret = OB_SUCCESS;
  ObUpdateTabletLog update_tablet_log;
  ObSSUpdateTabletLog ss_update_tablet_log;
  SCN row_scn(SCN::base_scn());
  ASSERT_EQ(OB_SUCCESS, ret = ss_update_log_.convert_to_update_log(row_scn, update_tablet_log));
  ASSERT_EQ(true, update_tablet_log.is_valid());
  SCN reorg_scn(SCN::min_scn());
  SCN deleted_scn;
  int64_t must_cache_size = 100000;
  int64_t try_cache_size = 1000;
  ASSERT_EQ(OB_SUCCESS, ret = update_log_.convert_to_ss_update_log(reorg_scn, deleted_scn, must_cache_size, try_cache_size, ss_update_tablet_log));
  ASSERT_EQ(true, ss_update_tablet_log.is_valid());
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_update_tablet_log.log*");
  OB_LOGGER.set_file_name("test_ss_update_tablet_log.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
