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

#define USING_LOG_PREFIX SHARE

#include "plugin/sys/ob_plugin_mgr.h"
#include "plugin/sys/ob_plugin_handle.h"
#include "plugin/sys/ob_plugin_entry_handle.h"

using namespace oceanbase::common;
using namespace oceanbase::plugin;

TEST(TestObPluginMgr, test_find_plugin)
{
  ObPluginMgr plugin_mgr;
  ASSERT_EQ(OB_SUCCESS, plugin_mgr.init(ObString()));

  ObPluginEntryHandle *plugin_entry = nullptr;
  ASSERT_NE(OB_SUCCESS, plugin_mgr.find_plugin(OBP_PLUGIN_TYPE_FT_PARSER, ObString("fake"), plugin_entry));
  ASSERT_EQ(nullptr, plugin_entry);

  ASSERT_EQ(OB_SUCCESS, plugin_mgr.load_builtin_plugins());

  const char *not_exist_names[] = {
    "", "not_exist", "fake"
  };
  for (size_t i = 0; i < sizeof(not_exist_names)/sizeof(not_exist_names[0]); i++) {
    const ObString name(not_exist_names[i]);
    LOG_INFO("before find plugin(not exist)", K(name));
    ASSERT_NE(OB_SUCCESS, plugin_mgr.find_plugin(OBP_PLUGIN_TYPE_FT_PARSER, name, plugin_entry));
    ASSERT_EQ(nullptr, plugin_entry);
  }

  const char *plugin_names[] = {
    "space", "Space", "SPACE", "spaCE",
    "beng", "Beng", "BENG", "bENg",
    "ngram", "Ngram", "NGRAM", "NGram"
  };

  for (size_t i = 0; i < sizeof(plugin_names)/sizeof(plugin_names[0]); i++) {
    const ObString name(plugin_names[i]);
    LOG_INFO("before find plugin", K(name));
    ASSERT_EQ(OB_SUCCESS, plugin_mgr.find_plugin(OBP_PLUGIN_TYPE_FT_PARSER, name, plugin_entry));
    ASSERT_NE(nullptr, plugin_entry);
  }
}

class ObTestPluginAdaptor final : public ObIPluginDescriptor
{
public:
  ObTestPluginAdaptor() = default;
  virtual ~ObTestPluginAdaptor() = default;

  virtual int init(ObPluginParam *param) { return OB_SUCCESS; }
  virtual int deinit(ObPluginParam *param) { return OB_SUCCESS; }
};

TEST(TestObPluginMgr, test_find_plugin_with_version)
{
  ObMemAttr mem_attr(OB_SYS_TENANT_ID, "test");
  ObPluginMgr *plugin_mgr = OB_NEW(ObPluginMgr, mem_attr);
  ASSERT_EQ(OB_SUCCESS, plugin_mgr->init(ObString()));

  ObString test_plugin_name("test_ftparser");

  ObPlugin plugin_100 = {
    "oceanbase",
    OBP_MAKE_VERSION(1, 0, 0),
    "mulan",
    nullptr,
    nullptr
  };

  ObPlugin plugin_110 = {
    "oceanbase",
    OBP_MAKE_VERSION(1, 1, 0),
    "mulan",
    nullptr,
    nullptr
  };

  ObPlugin plugin_200 = {
    "oceanbase",
    OBP_MAKE_VERSION(2, 0, 0),
    "mulan",
    nullptr,
    nullptr
  };

  ObPluginHandle plugin_handle_100;
  ASSERT_EQ(OB_SUCCESS, plugin_handle_100.init(plugin_mgr,
                                               OBP_PLUGIN_API_VERSION_CURRENT,
                                               ObString("plugin_handle_100"),
                                               &plugin_100));

  ObIPluginDescriptor *test_ftparser_100 = new ObTestPluginAdaptor();

  ObPluginEntry plugin_entry_100;
  plugin_entry_100.interface_type = OBP_PLUGIN_TYPE_FT_PARSER;
  plugin_entry_100.name = test_plugin_name;
  plugin_entry_100.interface_version = OBP_MAKE_VERSION(1, 0, 0);
  plugin_entry_100.descriptor = test_ftparser_100;
  plugin_entry_100.description = ObString("this is a test");
  plugin_entry_100.plugin_handle = &plugin_handle_100;

  ObPluginHandle plugin_handle_110;
  ASSERT_EQ(OB_SUCCESS, plugin_handle_110.init(plugin_mgr,
                                               OBP_PLUGIN_API_VERSION_CURRENT,
                                               ObString("plugin_handle_110"),
                                               &plugin_110));

  ObIPluginDescriptor *test_ftparser_110 = new ObTestPluginAdaptor();

  ObPluginEntry plugin_entry_110;
  plugin_entry_110.interface_type = OBP_PLUGIN_TYPE_FT_PARSER;
  plugin_entry_110.name = test_plugin_name;
  plugin_entry_110.interface_version = OBP_MAKE_VERSION(1, 0, 0);
  plugin_entry_110.descriptor = test_ftparser_110;
  plugin_entry_110.description = ObString("this is a test");
  plugin_entry_110.plugin_handle = &plugin_handle_110;

  ObPluginHandle plugin_handle_200;
  ASSERT_EQ(OB_SUCCESS, plugin_handle_200.init(plugin_mgr,
                                               OBP_PLUGIN_API_VERSION_CURRENT,
                                               ObString("plugin_handle_200"),
                                               &plugin_200));
  ObPluginEntry plugin_entry_200;
  plugin_entry_200.interface_type = OBP_PLUGIN_TYPE_FT_PARSER;
  plugin_entry_200.name = test_plugin_name;
  plugin_entry_200.interface_version = OBP_MAKE_VERSION(1, 0, 0);
  plugin_entry_200.descriptor = new ObTestPluginAdaptor();
  plugin_entry_200.description = ObString("this is a test");
  plugin_entry_200.plugin_handle = &plugin_handle_200;

  ASSERT_EQ(OB_SUCCESS, plugin_mgr->register_plugin(plugin_entry_100));
  ASSERT_EQ(OB_SUCCESS, plugin_mgr->register_plugin(plugin_entry_110));
  ASSERT_NE(OB_SUCCESS, plugin_mgr->register_plugin(plugin_entry_100));
  ASSERT_NE(OB_SUCCESS, plugin_mgr->register_plugin(plugin_entry_110));

  ObPluginEntryHandle *entry_handle = nullptr;
  ASSERT_EQ(OB_SUCCESS, plugin_mgr->find_plugin(OBP_PLUGIN_TYPE_FT_PARSER, test_plugin_name, entry_handle));
  ASSERT_NE(nullptr, entry_handle);
  ASSERT_EQ(OBP_MAKE_VERSION(1, 1, 0), entry_handle->library_version());

  ASSERT_EQ(OB_SUCCESS, plugin_mgr->find_plugin(OBP_PLUGIN_TYPE_FT_PARSER, test_plugin_name,
                                                OBP_MAKE_VERSION(1, 1, 0), entry_handle));
  ASSERT_NE(nullptr, entry_handle);
  ASSERT_EQ(OBP_MAKE_VERSION(1, 1, 0), entry_handle->library_version());

  ASSERT_EQ(OB_SUCCESS, plugin_mgr->find_plugin(OBP_PLUGIN_TYPE_FT_PARSER, test_plugin_name,
                                                OBP_MAKE_VERSION(1, 0, 0), entry_handle));
  ASSERT_NE(nullptr, entry_handle);
  ASSERT_EQ(OBP_MAKE_VERSION(1, 0, 0), entry_handle->library_version());

  ASSERT_NE(OB_SUCCESS, plugin_mgr->find_plugin(OBP_PLUGIN_TYPE_FT_PARSER, test_plugin_name,
                                                OBP_MAKE_VERSION(2, 0, 0), entry_handle));
  ASSERT_EQ(nullptr, entry_handle);

  ASSERT_EQ(OB_SUCCESS, plugin_mgr->register_plugin(plugin_entry_200));

  ASSERT_EQ(OB_SUCCESS, plugin_mgr->find_plugin(OBP_PLUGIN_TYPE_FT_PARSER, test_plugin_name,
                                                OBP_MAKE_VERSION(2, 0, 0), entry_handle));
  ASSERT_NE(nullptr, entry_handle);
  ASSERT_EQ(OBP_MAKE_VERSION(2, 0, 0), entry_handle->library_version());

  ASSERT_EQ(OB_SUCCESS, plugin_mgr->find_plugin(OBP_PLUGIN_TYPE_FT_PARSER, test_plugin_name, entry_handle));
  ASSERT_NE(nullptr, entry_handle);
  ASSERT_EQ(OBP_MAKE_VERSION(2, 0, 0), entry_handle->library_version());
}

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
