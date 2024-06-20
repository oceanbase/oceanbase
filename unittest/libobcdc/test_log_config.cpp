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
 *
 * Test ObLogConfig
 */

#include <gtest/gtest.h>
#include "ob_log_config.h"

#define ADD_CONFIG_INT(var, value) \
    do { \
      std::string name_str = #var; \
      std::string value_str = #value; \
      var = value; \
      databuff_printf(config_buf_, sizeof(config_buf_), config_buf_pos_, "%s=%ld\n", #var, var); \
      config_map_.erase(name_str); \
      config_map_.insert(std::pair<std::string, std::string>(name_str, value_str)); \
    } while (0)

#define ADD_CONFIG_STR(var, value) \
    do { \
      std::string name_str = #var; \
      std::string value_str = value; \
      var = value; \
      databuff_printf(config_buf_, sizeof(config_buf_), config_buf_pos_, "%s=%s\n", #var, var); \
      config_map_.erase(name_str); \
      config_map_.insert(std::pair<std::string, std::string>(name_str, value_str)); \
    } while (0)

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{
class TestLogConfig : public ::testing::Test
{
  static const int64_t MAX_CONFIG_BUFFER_SIZE = 1 << 10;
public:
  TestLogConfig() {}
  ~TestLogConfig() {}

  virtual void SetUp();
  virtual void TearDown() {}

public:
  int64_t dml_parser_thread_num;
  int64_t sequencer_thread_num;
  int64_t formatter_thread_num;
  int64_t instance_num;
  int64_t instance_index;
  const char *log_level;
  const char *cluster_url;
  const char *cluster_user;
  const char *cluster_password;
  const char *config_fpath;
  const char *cluster_appname;
  const char *cluster_db_name;
  const char *timezone;
  const char *tb_white_list;
  const char *tb_black_list;
  int64_t sql_conn_timeout_us;
  int64_t sql_query_timeout_us;

  int64_t unknown_int_config;
  const char *unknown_str_config;

  char config_buf_[MAX_CONFIG_BUFFER_SIZE];
  int64_t config_buf_pos_;

  std::map<std::string, std::string> config_map_;
};

void TestLogConfig::SetUp()
{
  config_buf_pos_ = 0;

  ADD_CONFIG_INT(dml_parser_thread_num, 100);
  ADD_CONFIG_INT(sequencer_thread_num, 200);
  ADD_CONFIG_INT(formatter_thread_num, 300);
  ADD_CONFIG_INT(instance_num, 1);
  ADD_CONFIG_INT(instance_index, 0);
  ADD_CONFIG_INT(sql_conn_timeout_us, 13000000000);
  ADD_CONFIG_INT(sql_query_timeout_us, 12000000000);

  ADD_CONFIG_STR(log_level, "INFO");
  ADD_CONFIG_STR(cluster_url, "http:://www.test_url.com/abcdefg/");
  ADD_CONFIG_STR(cluster_user, "中华人民共和国");
  ADD_CONFIG_STR(cluster_password, "阿里巴巴");
  ADD_CONFIG_STR(config_fpath, "/home/abcdefg/hijklmn");
  ADD_CONFIG_STR(cluster_appname, "obtest");
  ADD_CONFIG_STR(cluster_db_name, "oceanbase");
  ADD_CONFIG_STR(timezone, "+8:00");
  ADD_CONFIG_STR(tb_white_list, "*.*.*");
  ADD_CONFIG_STR(tb_black_list, "|");

  // test unknown config
  ADD_CONFIG_INT(unknown_int_config, 1010);
  ADD_CONFIG_STR(unknown_str_config, "unknown");
}

TEST_F(TestLogConfig, init)
{
  ObLogConfig *config = new ObLogConfig();

  EXPECT_EQ(OB_SUCCESS, config->init());
  // After initialization, the configuration items are not detected by default
  // TODO
  // EXPECT_NE(OB_SUCCESS, config->check_all());
  config->print();
  if (NULL != config) {
    delete config;
    config = NULL;
  }
}

TEST_F(TestLogConfig, load_from_buffer)
{
  ObLogConfig *config = new ObLogConfig();
  EXPECT_EQ(OB_SUCCESS, config->init());

  EXPECT_EQ(OB_SUCCESS, config->load_from_buffer(config_buf_, strlen(config_buf_)));
  EXPECT_EQ(OB_SUCCESS, config->check_all());
  config->print();

  EXPECT_EQ(dml_parser_thread_num, config->dml_parser_thread_num);
  EXPECT_EQ(sequencer_thread_num, config->sequencer_thread_num);
  EXPECT_EQ(formatter_thread_num, config->formatter_thread_num);
  EXPECT_EQ(0, strcmp(cluster_url, config->cluster_url.str()));
  EXPECT_EQ(0, strcmp(log_level, config->log_level.str()));
  EXPECT_EQ(0, strcmp(cluster_user, config->cluster_user.str()));
  EXPECT_EQ(0, strcmp(cluster_password, config->cluster_password.str()));
  EXPECT_EQ(0, strcmp(config_fpath, config->config_fpath.str()));

  bool check_name = true;
  int64_t version = 0;
  EXPECT_NE(OB_SUCCESS,
      config->load_from_buffer(config_buf_, strlen(config_buf_), version, check_name));
  if (NULL != config) {
    delete config;
    config = NULL;
  }
}

TEST_F(TestLogConfig, load_from_map)
{
  ObLogConfig *config = new ObLogConfig();
  EXPECT_EQ(OB_SUCCESS, config->init());

  EXPECT_EQ(OB_SUCCESS, config->load_from_map(config_map_));
  EXPECT_EQ(OB_SUCCESS, config->check_all());
  config->print();

  EXPECT_EQ(dml_parser_thread_num, config->dml_parser_thread_num);
  EXPECT_EQ(sequencer_thread_num, config->sequencer_thread_num);
  EXPECT_EQ(formatter_thread_num, config->formatter_thread_num);
  EXPECT_EQ(0, strcmp(cluster_url, config->cluster_url.str()));
  EXPECT_EQ(0, strcmp(log_level, config->log_level.str()));
  EXPECT_EQ(0, strcmp(cluster_user, config->cluster_user.str()));
  EXPECT_EQ(0, strcmp(cluster_password, config->cluster_password.str()));
  EXPECT_EQ(0, strcmp(config_fpath, config->config_fpath.str()));

  bool check_name = true;
  int64_t version = 0;
  EXPECT_NE(OB_SUCCESS, config->load_from_map(config_map_, version, check_name));
  if (NULL != config) {
    delete config;
    config = NULL;
  }
}

TEST_F(TestLogConfig, load_big_conf_from_map)
{
  ObLogConfig *config = new ObLogConfig();
  EXPECT_EQ(OB_SUCCESS, config->init());

  // big conf
  std::string tb_white_list;
  std::string tb_black_list;
  tb_white_list.append("abcdefg.hijklmn.opqrsj");
  for (int i = 1; i < 5000; i++) {
    tb_white_list.append("|abcdefg.hijklmn.opqrsj");
  }
  tb_black_list.append("1234567.89101112.131415");
  for (int i = 0; i < 5000; i++) {
    tb_black_list.append("|1234567.89101112.131415");
  }
  config_map_.erase("tb_white_list");
  config_map_.erase("tb_black_list");
  config_map_.insert(std::pair<std::string, std::string>("tb_white_list", tb_white_list));
  config_map_.insert(std::pair<std::string, std::string>("tb_black_list", tb_black_list));

  EXPECT_EQ(OB_SUCCESS, config->load_from_map(config_map_));
  EXPECT_EQ(OB_SUCCESS, config->check_all());
  config->print();

  EXPECT_EQ(0, strcmp(tb_white_list.c_str(), config->get_tb_white_list_buf()));
  EXPECT_EQ(0, strcmp(tb_black_list.c_str(), config->get_tb_black_list_buf()));
}

TEST_F(TestLogConfig, load_from_file)
{
  // The ObLogConfig class is larger than the local variable stack and would overflow if located
  // Therefore, the dynamic construction method is used here
  ObLogConfig *config_from_buffer_ptr = new ObLogConfig();
  ObLogConfig *config_from_file_ptr = new ObLogConfig();

  EXPECT_EQ(OB_SUCCESS, config_from_buffer_ptr->init());
  EXPECT_EQ(OB_SUCCESS, config_from_file_ptr->init());
  ObLogConfig &config_from_buffer = *config_from_buffer_ptr;
  ObLogConfig &config_from_file = *config_from_file_ptr;
  const char *config_file = "libobcdc.conf";

  // Load configuration items from the Buffer and verify the accuracy of the configuration items
  EXPECT_EQ(OB_SUCCESS, config_from_buffer.load_from_buffer(config_buf_, strlen(config_buf_)));
  EXPECT_EQ(OB_SUCCESS, config_from_buffer.check_all());
  config_from_buffer.print();
  EXPECT_EQ(dml_parser_thread_num, config_from_buffer.dml_parser_thread_num);
  EXPECT_EQ(sequencer_thread_num, config_from_buffer.sequencer_thread_num);
  EXPECT_EQ(formatter_thread_num, config_from_buffer.formatter_thread_num);
  EXPECT_EQ(0, strcmp(cluster_url, config_from_buffer.cluster_url.str()));
  EXPECT_EQ(0, strcmp(log_level, config_from_buffer.log_level.str()));
  EXPECT_EQ(0, strcmp(cluster_user, config_from_buffer.cluster_user.str()));
  EXPECT_EQ(0, strcmp(cluster_password, config_from_buffer.cluster_password.str()));
  EXPECT_EQ(0, strcmp(config_fpath, config_from_buffer.config_fpath.str()));

  // Dump configuration items into a file
  EXPECT_EQ(OB_SUCCESS, config_from_buffer.dump2file(config_file));

  // Loading configuration items from a file
  EXPECT_EQ(OB_SUCCESS, config_from_file.load_from_file(config_file));

  // Verify the accuracy of configuration items
  config_from_file.print();
  EXPECT_EQ(dml_parser_thread_num, config_from_file.dml_parser_thread_num);
  EXPECT_EQ(sequencer_thread_num, config_from_file.sequencer_thread_num);
  EXPECT_EQ(formatter_thread_num, config_from_file.formatter_thread_num);
  EXPECT_EQ(0, strcmp(cluster_url, config_from_file.cluster_url.str()));
  EXPECT_EQ(0, strcmp(log_level, config_from_file.log_level.str()));
  EXPECT_EQ(0, strcmp(cluster_user, config_from_file.cluster_user.str()));
  EXPECT_EQ(0, strcmp(cluster_password, config_from_file.cluster_password.str()));
  EXPECT_EQ(0, strcmp(config_fpath, config_from_file.config_fpath.str()));

  if (NULL != config_from_buffer_ptr) {
    delete config_from_buffer_ptr;
    config_from_buffer_ptr = NULL;
  }

  if (NULL != config_from_file_ptr) {
    delete config_from_file_ptr;
    config_from_file_ptr = NULL;
  }
}

// Check that the ObLogConfig::check_all() function actually formats the cluster_url
// default check_all() removes the double quotes from cluster_url
TEST_F(TestLogConfig, format_cluster_url)
{
  ObLogConfig *config = new ObLogConfig();
  EXPECT_EQ(OB_SUCCESS, config->init());
  const char *URL = "http://abc.com/def/hijklmn";
  char cluster_url[1024];

  ASSERT_EQ(OB_SUCCESS, config->load_from_buffer(config_buf_, strlen(config_buf_)));
  ASSERT_EQ(OB_SUCCESS, config->check_all());

  snprintf(cluster_url, sizeof(cluster_url), "\"");
  ASSERT_TRUE(config->cluster_url.set_value(cluster_url));
  ASSERT_NE(OB_SUCCESS, config->format_cluster_url());

  snprintf(cluster_url, sizeof(cluster_url), "\"\"");
  ASSERT_TRUE(config->cluster_url.set_value(cluster_url));
  ASSERT_NE(OB_SUCCESS, config->format_cluster_url());

  snprintf(cluster_url, sizeof(cluster_url), "\"%s\"", URL);
  ASSERT_TRUE(config->cluster_url.set_value(cluster_url));
  ASSERT_EQ(OB_SUCCESS, config->format_cluster_url());
  EXPECT_EQ(0, strcmp(URL, config->cluster_url.str()));

  // No handling of single inverted commas
  snprintf(cluster_url, sizeof(cluster_url), "\'\'");
  ASSERT_TRUE(config->cluster_url.set_value(cluster_url));
  ASSERT_EQ(OB_SUCCESS, config->format_cluster_url());
  if (NULL != config) {
    delete config;
    config = NULL;
  }
}

} // namespace libobcdc
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
