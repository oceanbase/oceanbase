/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/table/ob_table_config_util.h"
#include "lib/time/ob_time_utility.h"
#include "share/parameter/ob_parameter_macro.h"
#include <gtest/gtest.h>

namespace oceanbase
{
using namespace share;
namespace common
{
ObConfigContainer l_container;
static ObConfigContainer *local_container()
{
  return &l_container;
}
class TestObKvConfig : public ::testing::Test
{
public:
#undef OB_CLUSTER_PARAMETER
#define OB_CLUSTER_PARAMETER(args...) args
DEF_MODE_WITH_PARSER(_obkv_feature_mode, OB_CLUSTER_PARAMETER, "", common::ObKvFeatureModeParser,
  "_obkv_feature_mode is a option list to control specified OBKV features on/off.",
  ObParameterAttr(Section::OBSERVER, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#undef OB_CLUSTER_PARAMETER
};

TEST_F(TestObKvConfig, test_process_item)
{
  ObString key;
  ObString value;
  char str1[100] = {"ttl=off"};
  ASSERT_EQ(OB_SUCCESS, ObModeConfigParserUitl::parse_item_to_kv(str1, key, value));
  ASSERT_EQ(key, ObString("ttl"));
  ASSERT_EQ(value, ObString("off"));

  char str2[100] = {" ttl = off "};
  ASSERT_EQ(OB_SUCCESS, ObModeConfigParserUitl::parse_item_to_kv(str2, key, value));
  ASSERT_EQ(key, ObString("ttl"));
  ASSERT_EQ(value, ObString("off"));

  char str3[100] = {" rerouting =off "};
  ASSERT_EQ(OB_SUCCESS, ObModeConfigParserUitl::parse_item_to_kv(str3, key, value));
  ASSERT_EQ(key, ObString("rerouting"));
  ASSERT_EQ(value, ObString("off"));

  char str4[100] = {"ttl=on, rerouting=off"};
  common::ObSEArray<std::pair<ObString, ObString>, 8> kv_list;
  ASSERT_EQ(OB_SUCCESS, ObModeConfigParserUitl::get_kv_list(str4, kv_list));
  ASSERT_EQ(2, kv_list.count());
  ASSERT_EQ(kv_list.at(0).first, ObString("ttl"));
  ASSERT_EQ(kv_list.at(0).second, ObString("on"));
  ASSERT_EQ(kv_list.at(1).first, ObString("rerouting"));
  ASSERT_EQ(kv_list.at(1).second, ObString("off"));
}


TEST_F(TestObKvConfig, test_parse)
{
  ObKvFeatureModeParser parser;
  uint8_t arr[32];
  ASSERT_EQ(true, parser.parse("ttl=on, rerouting=off", arr, 32));
  ASSERT_EQ(arr[0], 0b00001001);

  MEMSET(arr, 0, 32);
  ASSERT_EQ(true, parser.parse("ttl=on, ttl=off, rerouting=on", arr, 32));
  ASSERT_EQ(arr[0], 0b0000110);

  MEMSET(arr, 0, 32);
  ASSERT_EQ(true, parser.parse("ttl=on, ttl=off", arr, 32));
  ASSERT_EQ(arr[0], 0b00000010);

  MEMSET(arr, 0, 32);
  ASSERT_EQ(true, parser.parse("ttl=on, hotkey=on, rerouting=on", arr, 32));
  ASSERT_EQ(arr[0], 0b00010101);

  MEMSET(arr, 0, 32);
  ASSERT_EQ(false, parser.parse("ttl=on, ttt=off", arr, 32));

  MEMSET(arr, 0, 32);
  ASSERT_EQ(false, parser.parse("ttl=on, rerouting=default", arr, 32));

  MEMSET(arr, 0, 32);
  ASSERT_EQ(true, parser.parse("", arr, 32));
  ASSERT_EQ(arr[0], 0);

  MEMSET(arr, 0, 32);
  ASSERT_EQ(false, parser.parse("ttl=on, ttt=off, ", arr, 32));

  MEMSET(arr, 0, 32);
  ASSERT_EQ(false, parser.parse("ttl=on, ttt=off,", arr, 32));
}

TEST_F(TestObKvConfig, test_obkv_feature_mode)
{
  const uint8_t *value = _obkv_feature_mode;
  ASSERT_NE((void*)NULL, (void*)value);
  ASSERT_EQ(value[0], 0);
  ASSERT_EQ(true, _obkv_feature_mode.set_value("ttl=off, rerouting=on, hotkey=on"));
  const uint8_t *value1 = _obkv_feature_mode.get();
  ASSERT_NE((void*)NULL, (void*)value1);
  ASSERT_EQ(value1[0], 0b00010110);
  // bad case
  ASSERT_EQ(false, _obkv_feature_mode.set_value("hotkey=off="));
  ASSERT_EQ(false, _obkv_feature_mode.set_value("=off"));
  ASSERT_EQ(false, _obkv_feature_mode.set_value(","));
  ASSERT_EQ(false, _obkv_feature_mode.set_value(" , "));
  ASSERT_EQ(false, _obkv_feature_mode.set_value(" ,hotkey=off"));
  ASSERT_EQ(false, _obkv_feature_mode.set_value(",hotkey=off"));
  ASSERT_EQ(false, _obkv_feature_mode.set_value("ttl=off,, hotkey=off"));
  ASSERT_EQ(false, _obkv_feature_mode.set_value("ttl=off, , hotkey=off"));
  ASSERT_EQ(false, _obkv_feature_mode.set_value("ttl=off, rerouting=on, "));
  ASSERT_EQ(false, _obkv_feature_mode.set_value("ttl=off, rerouting=on,"));
}

TEST_F(TestObKvConfig, testObKVFeatureMode)
{
  // rerouting=off, ttl=on
  uint8_t values[1] = {0b001001};
  ObKVFeatureMode kv_mode(values);
  ASSERT_EQ(true, kv_mode.is_ttl_enable());
  ASSERT_EQ(false, kv_mode.is_rerouting_enable());
  // default
  ObKVFeatureMode kv_mode_1;
  ASSERT_EQ(false, kv_mode_1.is_valid());
  kv_mode_1.set_ttl_mode(0b10);
  kv_mode_1.set_rerouting_mode(0b01);
  kv_mode_1.set_hotkey_mode(0b01);
  ASSERT_EQ(true, kv_mode_1.is_valid());
  ASSERT_EQ(false, kv_mode_1.is_ttl_enable());
  ASSERT_EQ(true, kv_mode_1.is_rerouting_enable());
  ASSERT_EQ(true, kv_mode_1.is_hotkey_enable());
  kv_mode_1.set_value(0b11101001);
  ASSERT_EQ(true, kv_mode_1.is_valid());
  ASSERT_EQ(true, kv_mode_1.is_ttl_enable());
  ASSERT_EQ(false, kv_mode_1.is_rerouting_enable());
  ASSERT_EQ(false, kv_mode_1.is_hotkey_enable());
  // bad case
  kv_mode_1.set_ttl_mode(0b11);
  ASSERT_EQ(false, kv_mode_1.is_valid());
  kv_mode_1.set_hotkey_mode(0b11);
  ASSERT_EQ(false, kv_mode_1.is_valid());
  kv_mode_1.set_rerouting_mode(0b11);
  ASSERT_EQ(false, kv_mode_1.is_valid());
  kv_mode_1.set_value(0b00111111);
  ASSERT_EQ(false, kv_mode_1.is_valid());
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}