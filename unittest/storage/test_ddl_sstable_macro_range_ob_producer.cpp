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
#define protected public
#define private public
#include "src/storage/high_availability/ob_storage_ha_macro_block_writer.h"
#include "storage/high_availability/ob_storage_ha_utils.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;

namespace unittest
{
class TestDDLCopySSTableMacroRangeObProducer : public ::testing::Test
{
public:
  TestDDLCopySSTableMacroRangeObProducer() = default;
  void SetUp() {}
  void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
};

TEST_F(TestDDLCopySSTableMacroRangeObProducer, shared_storage_mode)
{
  int ret = OB_SUCCESS;
  ObArray<MacroBlockId> macro_block_id_array;
  ObCopyMacroRangeInfo macro_range_info;
  const int64_t buf_size = ObDDLCopySSTableMacroRangeObProducer::MAX_BUF_SIZE;
  char buf[buf_size] = {0};

  for (int64_t i = 0; OB_SUCC(ret) && i < 128; ++i) {
    MacroBlockId m_local(i+1, (1L << 33), 0);
    m_local.id_mode_ = (uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE;
    ret = macro_block_id_array.push_back(m_local);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = ObStorageHAUtils::make_macro_id_to_datum(macro_block_id_array, buf, buf_size, macro_range_info.start_macro_block_end_key_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<MacroBlockId> tmp_macro_block_id_array;
  ret = ObStorageHAUtils::extract_macro_id_from_datum(macro_range_info.start_macro_block_end_key_, tmp_macro_block_id_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tmp_macro_block_id_array.count(), macro_block_id_array.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_id_array.count(); ++i) {
    const MacroBlockId &id1 = macro_block_id_array.at(i);
    const MacroBlockId &id2 = tmp_macro_block_id_array.at(i);
    ASSERT_EQ(id1, id2);
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_ddl_sstable_macro_range_ob_producer.log*");
  OB_LOGGER.set_file_name("test_ddl_sstable_macro_range_ob_producer.log", true, false);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_ddl_sstable_macro_range_ob_producer");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
