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

#include <string>
#include <random>
#include <chrono>
#include <vector>
#include <unordered_map>
#include <type_traits>
#include <fstream>
#include "lib/ob_define.h"

// class ObOperator;

namespace oceanbase
{
namespace sql
{
class ObTestOpConfig
{
public:
  static ObTestOpConfig &get_instance()
  {
    static ObTestOpConfig test_op_config;
    return test_op_config;
  }

private:
  ObTestOpConfig()
  {}
  ~ObTestOpConfig()
  {}

  void init()
  {
    std::string nulls_probability;

    std::string test_config_file = test_filename_prefix_ + ".cfg";
    test_filename_origin_output_file_ = test_filename_prefix_ + "_origin_result.data";
    test_filename_vec_output_file_ = test_filename_prefix_ + "_vec_result.data";
    std::ifstream if_tests_config(test_config_file);
    OB_ASSERT(if_tests_config.is_open() == true);
    std::string line;

    while (std::getline(if_tests_config, line)) {
      if (line.size() <= 0) continue;
      if (line.at(0) == '#') continue;

      std::vector<std::string> out;
      const char delim = '=';
      tokenize(line, delim, out);
      OB_ASSERT(out.size() == 2);
      configs_map_[out[0]] = out[1];
    }

    set_configs();
    LOG_INFO("Testing config is: ", K(*this));
  }

  void tokenize(std::string const &str, const char delim, std::vector<std::string> &out)
  {
    size_t start;
    size_t end = 0;

    while ((start = str.find_first_not_of(delim, end)) != std::string::npos) {
      end = str.find(delim, start);
      out.push_back(str.substr(start, end - start));
    }
  }

  int get_config(std::string key, std::string &value)
  {
    int ret = OB_SUCCESS;
    if (configs_map_.count(key) == 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      value = configs_map_[key];
    }
    return ret;
  }

  void set_configs()
  {
    std::string digit_data_format;
    std::string string_data_format;
    std::string data_range_level;
    std::string nulls_probability;
    std::string skips_probability;
    std::string round;
    std::string batch_size;
    std::string output_result_to_file;
    get_config("digit_data_format", digit_data_format);
    get_config("string_data_format", string_data_format);
    get_config("data_range_level", data_range_level);
    get_config("nulls_probability", nulls_probability);
    get_config("skips_probability", skips_probability);
    get_config("round", round);
    get_config("batch_size", batch_size);
    get_config("output_result_to_file", output_result_to_file);

    if (!digit_data_format.empty()) { digit_data_format_ = static_cast<VectorFormat>(std::stoi(digit_data_format)); }
    if (!string_data_format.empty()) { string_data_format_ = static_cast<VectorFormat>(std::stoi(string_data_format)); }
    if (!data_range_level.empty()) { data_range_level_ = std::stoi(data_range_level); }
    if (!nulls_probability.empty()) { nulls_probability_ = std::stoi(nulls_probability); }
    if (!skips_probability.empty()) { skips_probability_ = std::stoi(skips_probability); }
    if (!round.empty()) { round_ = std::stoi(round); }
    if (!batch_size.empty()) { batch_size_ = std::stoi(batch_size); }
    if (!output_result_to_file.empty()) { output_result_to_file_ = std::stoi(output_result_to_file); }
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_KV(K(digit_data_format_), K(string_data_format_), K(data_range_level_), K(nulls_probability_),
         K(skips_probability_), K(round_), K(batch_size_), K(output_result_to_file_));
    return pos;
  }

private:
  std::unordered_map<std::string, std::string> configs_map_;

  bool run_in_background_{false};
  std::string test_filename_prefix_;
  std::string test_filename_origin_output_file_;
  std::string test_filename_vec_output_file_;

  bool output_result_to_file_{false};
  // data foramt
  VectorFormat digit_data_format_{VEC_UNIFORM};
  VectorFormat string_data_format_{VEC_UNIFORM};

  // distribution and data range
  int data_range_level_{0};

  // nulls and skips
  int nulls_probability_{0};
  int skips_probability_{0};

  // round and batch_size
  int round_{10};
  int batch_size_{256};
};

} // end namespace sql
} // end namespace oceanbase
