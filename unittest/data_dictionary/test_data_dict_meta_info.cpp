/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * This file defines test_ob_cdc_part_trans_resolver.cpp
 */

#include "gtest/gtest.h"
#include "lib/oblog/ob_log.h"
#include "logservice/palf/log_define.h"

#define private public
#include "logservice/data_dictionary/ob_data_dict_meta_info.h"
#undef private

namespace oceanbase
{
namespace datadict
{

TEST(ObDataDictMetaInfoItem, test_data_dict_meta_info_item)
{
  ObDataDictMetaInfoItem item;
  ObRandom random;
  const int64_t __MB__ = 1L << 20;
  const int64_t item_cnt = 100;
  const int64_t buf_size = 2 * __MB__;
  char *buf = static_cast<char*>(ob_malloc(buf_size, ObNewModIds::TEST));
  int64_t pos = 0;
  DataDictMetaInfoItemArr item_arr;
  for (int64_t i = 0; i < item_cnt; i++) {
    item.reset(random.get(), random.get(), random.get());
    EXPECT_EQ(OB_SUCCESS, item_arr.push_back(item));
    EXPECT_EQ(OB_SUCCESS, item.serialize(buf, buf_size, pos));
  }
  int64_t deserialize_pos = 0;
  for (int64_t i = 0; i < item_cnt; i++) {
    EXPECT_EQ(OB_SUCCESS, item.deserialize(buf, buf_size, deserialize_pos));
    EXPECT_EQ(item, item_arr.at(i));
  }
  ob_free(buf);
}

TEST(ObDataDictMetaInfoHeader, test_data_dict_meta_info_header)
{
  ObDataDictMetaInfoHeader header;
  ObRandom random;
  const int64_t __MB__ = 1L << 20;
  const int64_t buf_size = 2 * __MB__;
  char *buf = static_cast<char*>(ob_malloc(buf_size, ObNewModIds::TEST));
  header.magic_ = random.get_int32() && 0xFFFF;
  header.meta_version_ = 1;
  header.item_cnt_ = random.get_int32();
  header.min_snapshot_scn_ = random.get();
  header.max_snapshot_scn_ = random.get();
  header.data_size_ = random.get();
  header.checksum_ = random.get();
  int64_t serialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, header.serialize(buf, buf_size, serialize_pos));
  ObDataDictMetaInfoHeader header_deserialized;
  int64_t desirialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, header_deserialized.deserialize(buf, buf_size, desirialize_pos));
  EXPECT_EQ(header, header_deserialized);

  ob_free(buf);
}


TEST(ObDataDictMetaInfo, test_data_dict_meta_info)
{
  ObDataDictMetaInfo meta_info;
  DataDictMetaInfoItemArr item_arr;
  ObDataDictMetaInfoItem item;
  ObMySQLProxy dummy_proxy;
  const int64_t buffer_size = 1024*1024;
  const int64_t header_size = sizeof(ObDataDictMetaInfoHeader) * 2;
  ObRandom random;
  const int64_t max_scn_val = share::SCN::max_scn().get_val_for_logservice();
  MetaInfoQueryHelper helper(dummy_proxy, OB_SYS_TENANT_ID);
  int64_t item_count_expected = 0;
  int64_t total_item_data_size = 0;
  bool have_enough_buffer = true;
  while (have_enough_buffer ) {
    item.reset(random.get(0, max_scn_val),
        random.get(0, palf::LOG_MAX_LSN_VAL),
        random.get(0, palf::LOG_MAX_LSN_VAL));
    if (total_item_data_size + item.get_serialize_size() <= buffer_size - header_size) {
      item_count_expected++;
      total_item_data_size += item.get_serialize_size();
      EXPECT_EQ(OB_SUCCESS, item_arr.push_back(item));
    } else {
      have_enough_buffer = false;
    }
  }

  int64_t real_size = 0;
  share::SCN scn;

  char *buf = (char*)ob_malloc(buffer_size, "ddmh");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, helper.generate_data_(item_arr, buf, buffer_size, real_size, scn));
  EXPECT_EQ(OB_SUCCESS, meta_info.deserialize(buf, buffer_size, pos));
  ob_free(buf);
  EXPECT_TRUE(meta_info.check_integrity());
  EXPECT_EQ(item_count_expected, meta_info.item_arr_.count());
  const ObDataDictMetaInfoHeader &header = meta_info.get_header();
  EXPECT_EQ(pos - header.get_serialize_size(), header.get_data_size());
  EXPECT_EQ(item_count_expected, header.get_item_count());
  const DataDictMetaInfoItemArr &meta_info_item_arr = meta_info.get_item_arr();
  for (int64_t i = 0; i < item_count_expected; i++) {
    EXPECT_EQ(meta_info_item_arr.at(i), item_arr.at(i));
  }

  char *new_buf = static_cast<char*>(ob_malloc(buffer_size, ObNewModIds::TEST));
  int64_t new_ser_pos = 0;
  EXPECT_EQ(OB_SUCCESS, meta_info.serialize(new_buf, buffer_size, new_ser_pos));
  ObDataDictMetaInfo new_meta_info;
  int64_t new_deser_pos = 0;
  EXPECT_EQ(OB_SUCCESS, new_meta_info.deserialize(new_buf, buffer_size, new_deser_pos));
  EXPECT_TRUE(new_meta_info.check_integrity());
  EXPECT_EQ(meta_info.header_, new_meta_info.header_);
  for (int64_t i = 0; i < item_count_expected; i++) {
    EXPECT_EQ(meta_info.item_arr_.at(i), new_meta_info.item_arr_.at(i));
  }
  ob_free(new_buf);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_data_dict_meta_info.log");
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_data_dict_meta_info.log", true, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  logger.set_mod_log_levels("ALL.*:DEBUG, DATA_DICT.*:DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
