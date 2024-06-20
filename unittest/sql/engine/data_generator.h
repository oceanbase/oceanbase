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

#include <random>
#include <chrono>
#include <vector>
#include <unordered_map>
#include <type_traits>
#include "src/sql/engine/expr/ob_expr.h"
#include "src/sql/engine/ob_bit_vector.h"
#include "ob_test_config.h"
namespace oceanbase
{
namespace sql
{
class ObDataGenerator
{
public:
  static ObDataGenerator &get_instance()
  {
    static ObDataGenerator data_generator;
    return data_generator;
  }

  template <typename T>
  int generate_data(const int64_t op_id, const int expr_i, const int expr_count, const int round, const int batch_size,
                    const int len, bool &is_duplicate)
  {
    int ret = OB_SUCCESS;

    LOG_DEBUG("[DG]Start generate data for: ", K(op_id), K(round), K(expr_i), K(expr_count));

    // if it's a exist generated round data and corresponding expr_i have data
    if (op_2_round_2_temp_store_.count(op_id) != 0 && op_2_round_2_temp_store_[op_id].count(round) != 0
        && expr_i < op_2_round_2_temp_store_[op_id][round].size()) {
      return ret;
    }

    // generate new random data
    std::string generate_data;
    std::vector<TempDataStore> &temp_store = op_2_round_2_temp_store_[op_id][round];
    temp_store.push_back(TempDataStore());

    for (int i = 0; i < batch_size; i++) {
      if (std::is_same<T, int>::value) {
        int random_data;
        // if (expr_i == expr_count - 1) {
        //   random_data = 1;
        // } else {
        //   random_data = u_int32_dis_(e_);
        // }

        random_data = u_int32_dis_->operator()(e_);
        temp_store[expr_i].temp_int32_vector_.push_back(random_data);
        generate_data += std::to_string(random_data) + "  ";
      } else if (std::is_same<T, int64_t>::value) {
        int64_t random_data;
        random_data = u_int64_dis_->operator()(e_);
        temp_store[expr_i].temp_int64_vector_.push_back(random_data);
        generate_data += std::to_string(random_data) + "  ";
      } else if (std::is_same<T, double>::value) {
        double random_data;
        random_data = u_r_dis_->operator()(e_);
        temp_store[expr_i].temp_double_vector_.push_back(random_data);
        generate_data += std::to_string(random_data) + "  ";
      } else if (std::is_same<T, std::string>::value) {
        std::string random_data;
        random_data = str_rand(len);
        temp_store[expr_i].temp_string_vector_.push_back(random_data);
        generate_data += random_data + "  ";
      } else {
        LOG_INFO("Can not generate random value so far for: ");
        assert(false);
      }
    }

    LOG_DEBUG("Generate data: ", K(generate_data.data()));

    set_random_null(op_id, expr_i, expr_count, round, batch_size);

    return ret;
  }

  void set_random_null(const int64_t op_id, const int expr_i, const int expr_count, const int round,
                       const int batch_size)
  {
    std::string generate_data_nulls;
    bool is_null;
    for (int i = 0; i < batch_size; i++) {
      is_null = zero_one_rand_by_probability(ObTestOpConfig::get_instance().nulls_probability_);
      generate_data_nulls += std::to_string(!is_null) + "  ";
      op_2_round_2_temp_store_[op_id][round][expr_i].null_.push_back(is_null);
    }
    LOG_INFO("nulls : ", K(generate_data_nulls.data()));
  }

  void reset_temp_store(const uint64_t op_id, const int round)
  {
    if (op_2_round_2_temp_store_.count(op_id)) { op_2_round_2_temp_store_[op_id].erase(round); }
  }

  // Todo: replace this struct with ObTempColumnStore when br finish that
  struct TempDataStore
  {
    bool empty_{true};
    // temp store
    std::vector<int> temp_int32_vector_;
    std::vector<int64_t> temp_int64_vector_;
    std::vector<double> temp_double_vector_;
    std::vector<std::string> temp_string_vector_;

    std::vector<bool> null_;
  };

  using TempDataStores = std::vector<TempDataStore>;

  void register_op(const ObOperator *op)
  {
    LOG_INFO("id is ", K(op->get_spec().get_id()));
    intereseting_op_count_++;
  }

private:
  ObDataGenerator()
  {
    e_ = std::default_random_engine(std::chrono::system_clock::now().time_since_epoch().count());
    switch (ObTestOpConfig::get_instance().data_range_level_) {
    case 0:
      u_int32_dis_ = new std::uniform_int_distribution<int32_t>(-100, 100);
      u_int64_dis_ = new std::uniform_int_distribution<int64_t>(-100, 100);
      u_r_dis_ = new std::uniform_real_distribution<double>(-100, 100);
      break;
    case 1:
      u_int32_dis_ = new std::uniform_int_distribution<int32_t>(-10000, 10000);
      u_int64_dis_ = new std::uniform_int_distribution<int64_t>(-50000, 50000);
      u_r_dis_ = new std::uniform_real_distribution<double>(-10000, 10000);
      break;
    case 2:
      u_int32_dis_ = new std::uniform_int_distribution<int32_t>(INT32_MIN, INT32_MAX);
      u_int64_dis_ = new std::uniform_int_distribution<int64_t>(INT64_MIN, INT64_MAX);
      u_r_dis_ = new std::uniform_real_distribution<double>(-100000, 100000);
      break;
    default:
      u_int32_dis_ = new std::uniform_int_distribution<int32_t>(-100, 100);
      u_int64_dis_ = new std::uniform_int_distribution<int64_t>(-100, 100);
      u_r_dis_ = new std::uniform_real_distribution<double>(-100, 100);
      break;
    }
  }
  ~ObDataGenerator()
  {
    delete u_int32_dis_;
    delete u_int64_dis_;
    delete u_r_dis_;
  }

  std::string str_rand(int length)
  {
    std::uniform_int_distribution<int> u_l(0, length);
    std::uniform_int_distribution<int> u_data(0, 10000);

    int final_len = u_l(e_);
    char tmp;
    std::string buffer;
    for (int i = 0; i < final_len; i++) {
      tmp = u_data(e_) % 36;
      if (tmp < 10) {
        tmp += '0';
      } else {
        tmp -= 10;
        tmp += 'A';
      }
      buffer += tmp;
    }
    return buffer;
  }

  // probability means the probability of generating result of 1
  int zero_one_rand_by_probability(int probability)
  {
    OB_ASSERT(probability >= 0 && probability <= 100);
    std::uniform_int_distribution<int> u_l(0, 100);
    if (u_l(e_) < probability) { return 1; }
    return 0;
  }

private:
  std::uniform_int_distribution<int32_t> *u_int32_dis_{nullptr};
  std::uniform_int_distribution<int64_t> *u_int64_dis_{nullptr};
  std::uniform_real_distribution<double> *u_r_dis_{nullptr};
  std::default_random_engine e_;

  int last_round_{-1};
  int intereseting_op_count_{0};
  bool inited_{false};

  // temp store
  std::unordered_map<int, std::unordered_map<int, TempDataStores>> op_2_round_2_temp_store_;
  std::unordered_map<int, std::unordered_map<int, std::vector<bool>>> op_2_round_2_skips_;
};

} // end namespace sql
} // end namespace oceanbase
