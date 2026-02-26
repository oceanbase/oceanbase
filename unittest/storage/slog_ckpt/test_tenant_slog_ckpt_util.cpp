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

#define private public
#define protected public
#include "storage/slog_ckpt/ob_tenant_slog_checkpoint_util.h"
#include "storage/slog_ckpt/ob_tenant_slog_checkpoint_workflow.h"
#undef private
#undef protected

#include <gtest/gtest.h>
#include <vector>
#include <map>


namespace oceanbase{
namespace storage{

using blocksstable::MacroBlockId;
using share::ObLSID;
using common::ObTabletID;
using TabletDfgtPicker = ObTenantSlogCkptUtil::TabletDefragmentPicker;

static const int64_t TEST_TENANT_ID = 1000;

static const char *TEST_LABEL = "testLabel";

static const int64_t TABLET_STORAGE_ALIGNED = DIO_READ_ALIGN_SIZE;

static const bool VERBOSE = true;

class TestSlogCkptUtil : public ::testing::Test {};

static MacroBlockId make_block(const int64_t block_idx) {
  MacroBlockId block_id;
  block_id.set_id_mode(0);
  block_id.set_version_v2();
  block_id.set_block_index(block_idx);
  OB_ASSERT(block_id.is_valid());
  return block_id;
}

static ObMetaDiskAddr make_disk_addr(const MacroBlockId &block_id, const int64_t &tablet_size) {
  ObMetaDiskAddr res;
  int ret = res.set_block_addr(block_id, 0, tablet_size, ObMetaDiskAddr::RAW_BLOCK);
  OB_ASSERT(ret == OB_SUCCESS);
  OB_ASSERT(res.is_valid());
  return res;
}

static ObMetaDiskAddr make_disk_addr(const int64_t &block_idx, const int64_t &tablet_size) {
  ObMetaDiskAddr res;
  int ret = res.set_block_addr(make_block(block_idx), 0, tablet_size, ObMetaDiskAddr::RAW_BLOCK);
  OB_ASSERT(ret == OB_SUCCESS);
  OB_ASSERT(res.is_valid());
  return res;
}

static ObTabletMapKey make_tablet_key(const int64_t &ls_id, const int64_t &tablet_id) {
   ObTabletMapKey res = ObTabletMapKey(ObLSID(ls_id), ObTabletID(tablet_id));
   OB_ASSERT(res.is_valid());
   return res;
}



struct ResultChecker {
  struct MapValue{
    std::vector<ObTabletStorageParam> write_params;
    int64_t total_size = 0;
  };

  ResultChecker() = default;

  bool add_tablet(const ObTabletStorageParam &param) {
    int ret = OB_SUCCESS;
    MacroBlockId block_id;
    int64_t offset = 0, size = 0;
    EXPECT_TRUE(OB_SUCC(param.original_addr_.get_block_addr(block_id, offset, size)));
    size = common::ob_aligned_to2(size, TABLET_STORAGE_ALIGNED);
    if (map[block_id].total_size + size > OB_DEFAULT_MACRO_BLOCK_SIZE) {
      // this block is full.
      return false;
    }
    map[block_id].write_params.push_back(param);
    map[block_id].total_size += size;
    return true;
  }

  bool check_size_amp_lt(const double &size_amp) const {
    for (auto iter = map.cbegin(); iter != map.cbegin(); ++iter) {
      const MapValue &val = iter->second;
      double this_size_amp = ObTenantSlogCkptUtil::cal_size_amplification(1, val.total_size);
      if (ObTenantSlogCkptUtil::precision_compare(this_size_amp, size_amp) >= 0) {
        if (VERBOSE) {
          printf("[ERROR]: block id:%s's size amp(%lf) >= %lf", ObCStringHelper().convert(iter->first), this_size_amp, size_amp);
        }
        return false;
      }
    }
    return true;
  }

  void erase(const ObArray<ObTabletStorageParam> &remove) {
    int ret = OB_SUCCESS;
    std::set<MacroBlockId> remove_set;
    for (int64_t i = 0; i < remove.count(); ++i) {
      const ObTabletStorageParam &at = remove.at(i);
      MacroBlockId block_id;
      int64_t _1 = 0, _2 = 0;
      EXPECT_TRUE(OB_SUCC(at.original_addr_.get_block_addr(block_id, _1, _2)));
      EXPECT_TRUE(block_id.is_valid());
      remove_set.insert(block_id);
    }
    for (const MacroBlockId &block_id : remove_set) {
      map.erase(block_id);
    }
  }

  bool result_must_contains(const ObArray<ObTabletStorageParam> &result,
    const std::set<MacroBlockId> &expected_block_set) const {
    int ret = OB_SUCCESS;
    std::set<MacroBlockId> block_set;
    for (int64_t i = 0; i < result.count(); ++i) {
      const ObTabletStorageParam &at = result.at(i);
      MacroBlockId block_id;
      int64_t _1 = 0, _2 = 0;
      EXPECT_TRUE(OB_SUCC(at.original_addr_.get_block_addr(block_id, _1, _2)));
      EXPECT_TRUE(block_id.is_valid());
      block_set.insert(block_id);
    }
    return expected_block_set == block_set;
  }

  bool check_picked_tablet_size(const ObArray<ObTabletStorageParam> &result,
    const int64_t &expected_size) const {
      int ret = OB_SUCCESS;
      int64_t result_size = 0;
      int64_t tmp_offset = 0, tmp_size = 0;
      MacroBlockId tmp_id;
      for (int64_t i = 0; i < result.size(); ++i) {
        if (OB_FAIL(result[i].original_addr_.get_block_addr(tmp_id, tmp_offset,
          tmp_size))) {
          return false;
        }
        result_size += common::ob_aligned_to2(tmp_size, TABLET_STORAGE_ALIGNED);
      }
      return result_size == expected_size;
  }

  bool check_size_aligned(const int64_t &size,
    const int64_t &aligned = TABLET_STORAGE_ALIGNED) const {
      return size % aligned == 0;
  }

  ObMacroBlockCommonHeader dummy_header;
  std::map<MacroBlockId, MapValue> map;
};


TEST_F(TestSlogCkptUtil, test_tsac_basic) {
  static const int64_t BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE - ObMacroBlockCommonHeader().get_serialize_size();

  int ret = OB_SUCCESS;
  int64_t expected_size = 0;
  ObMemAttr mem_attr(TEST_TENANT_ID, TEST_LABEL);

  TabletDfgtPicker map(mem_attr);
  ResultChecker result_checker;

  EXPECT_TRUE(OB_SUCC(map.create(109)));
  {
    // block 1: size amp ~= 1.2
    MacroBlockId block_id = make_block(1);
    ObMetaDiskAddr disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 3.);
    ObTabletMapKey tablet_key = make_tablet_key(0, 1);
    ObTabletStorageParam write_param;
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));

    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 3.);
    tablet_key = make_tablet_key(0, 2);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));

    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 6.);
    tablet_key = make_tablet_key(0, 3);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));

    // block 2: size amp ~= 2
    block_id = make_block(2);
    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 2.1);
    tablet_key = make_tablet_key(0, 4);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));
    expected_size += common::ob_aligned_to2(BLOCK_SIZE / 2.1, TABLET_STORAGE_ALIGNED);

    // block 3: size amp ~= 1.67
    block_id = make_block(3);
    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 5.0);
    tablet_key = make_tablet_key(0, 5);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));
    expected_size += common::ob_aligned_to2(BLOCK_SIZE / 5.0, TABLET_STORAGE_ALIGNED);

    block_id = make_block(3);
    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 5.0);
    tablet_key = make_tablet_key(0, 6);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));
    expected_size += common::ob_aligned_to2(BLOCK_SIZE / 5.0, TABLET_STORAGE_ALIGNED);

    block_id = make_block(3);
    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 5.0);
    tablet_key = make_tablet_key(0, 7);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));
    expected_size += common::ob_aligned_to2(BLOCK_SIZE / 5.0, TABLET_STORAGE_ALIGNED);
  }
  // tablet size must be aligned to 4K.
  EXPECT_TRUE(result_checker.check_size_aligned(map.total_tablet_size()));

  // pick result
  ObArray<ObTabletStorageParam> picked_tablets;
  double size_amp_threshold = 1.25;
  int64_t max_tablet_cnt = 100;
  int64_t picked_tablet_size = 0;
  EXPECT_TRUE(OB_SUCC(map.pick_tablets_for_defragment(picked_tablets, size_amp_threshold, max_tablet_cnt, -1, picked_tablet_size)));
  // expect picked tablets from block2, block3
  EXPECT_TRUE(result_checker.result_must_contains(picked_tablets, {make_block(2), make_block(3)}));
  result_checker.erase(picked_tablets);
  // size amp of remaining blocks must < size_amp_threshold
  EXPECT_TRUE(result_checker.check_size_amp_lt(size_amp_threshold));
  EXPECT_TRUE(result_checker.check_picked_tablet_size(picked_tablets, expected_size));
}

TEST_F(TestSlogCkptUtil, test_tsac_picked_threshold) {
  static const int64_t BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE - ObMacroBlockCommonHeader().get_serialize_size();

  int ret = OB_SUCCESS;
  int64_t expected_size = 0;
  ObMemAttr mem_attr(TEST_TENANT_ID, TEST_LABEL);

  TabletDfgtPicker map(mem_attr);
  ResultChecker result_checker;

  EXPECT_TRUE(OB_SUCC(map.create(109)));
  {
    // block 1: size amp ~= 1.2
    MacroBlockId block_id = make_block(1);
    ObMetaDiskAddr disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 3.);
    ObTabletMapKey tablet_key = make_tablet_key(0, 1);
    ObTabletStorageParam write_param;
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));

    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 3.);
    tablet_key = make_tablet_key(0, 2);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));

    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 6.);
    tablet_key = make_tablet_key(0, 3);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));

    // block 2: size amp ~= 2
    block_id = make_block(2);
    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 2.1);
    tablet_key = make_tablet_key(0, 4);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));
    expected_size += common::ob_aligned_to2(BLOCK_SIZE / 2.1, TABLET_STORAGE_ALIGNED);

    // block 3: size amp ~= 1.67
    block_id = make_block(3);
    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 5.0);
    tablet_key = make_tablet_key(0, 5);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));

    block_id = make_block(3);
    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 5.0);
    tablet_key = make_tablet_key(0, 6);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));

    block_id = make_block(3);
    disk_addr = make_disk_addr(block_id, BLOCK_SIZE / 5.0);
    tablet_key = make_tablet_key(0, 7);
    write_param.tablet_key_ = tablet_key;
    write_param.original_addr_ = disk_addr;
    EXPECT_TRUE(result_checker.add_tablet(write_param));
    EXPECT_TRUE(OB_SUCC(map.add_tablet(write_param)));
  }
  // tablet size must be aligned to 4K.
  EXPECT_TRUE(result_checker.check_size_aligned(map.total_tablet_size()));

  // pick result
  ObArray<ObTabletStorageParam> picked_tablets;
  double size_amp_threshold = 1.25;
  int64_t max_tablet_cnt = 100;
  int64_t max_picked_size = expected_size + BLOCK_SIZE / 10;
  int64_t picked_tablet_size = 0;
  EXPECT_TRUE(OB_SUCC(map.pick_tablets_for_defragment(picked_tablets, size_amp_threshold, max_tablet_cnt, max_picked_size, picked_tablet_size)));
  // expect picked tablets from block2
  EXPECT_TRUE(result_checker.result_must_contains(picked_tablets, {make_block(2)}));
  result_checker.erase(picked_tablets);
  // size amp of remaining blocks must < size_amp_threshold
  EXPECT_TRUE(result_checker.check_size_amp_lt(1.7));
  EXPECT_TRUE(result_checker.check_picked_tablet_size(picked_tablets, expected_size));
  EXPECT_LE(picked_tablet_size, max_picked_size);
}

TEST_F(TestSlogCkptUtil, test_log) {
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  const char *tag = "[tag]";
  STORAGE_LOG(WARN, "this is a log", KTIME(now), K(ret));

  int64_t total = 10024;
  ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::ProgressPrinter printer(total);
  for (int64_t i = 0; i < total; ++i) {
    printer.increase();
  }
}

TEST_F(TestSlogCkptUtil, test_optional_var) {
  int ret = OB_SUCCESS;
  ObCStringHelper helper;

  typedef ObTenantSlogCkptUtil::OptionalVariable<int64_t> optional_int_t;
  typedef ObTenantSlogCkptUtil::OptionalVariable<MacroBlockId> optional_block_id_t;


  optional_int_t int_val(-1);

  printf("%s\n", helper.convert(int_val));
  STORAGE_LOG(WARN, "log optional variable", K(int_val));

  int_val.set(123);
  EXPECT_TRUE(int_val.setted());
  printf("%s\n", helper.convert(int_val));
  STORAGE_LOG(WARN, "log optional variable", K(int_val));

  optional_block_id_t block_id_val;

  printf("%s\n", helper.convert(block_id_val));
  STORAGE_LOG(WARN, "log optional variable", K(block_id_val));

  block_id_val.set(make_block(0));
  EXPECT_TRUE(block_id_val.setted());
  printf("%s\n", helper.convert(block_id_val));
  STORAGE_LOG(WARN, "log optional variable", K(block_id_val));
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tenant_slog_ckpt_util.log*");
  OB_LOGGER.set_file_name("test_tenant_slog_ckpt_util.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}