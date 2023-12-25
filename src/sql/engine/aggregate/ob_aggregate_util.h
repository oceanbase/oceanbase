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

#ifndef OCEANBASE_SQL_ENGINE_AGGREGATE_UTIL_H
#define OCEANBASE_SQL_ENGINE_AGGREGATE_UTIL_H
#include <type_traits>

#include "common/data_buffer.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase
{
namespace sql
{

using namespace common::number;

template<typename src_type, class = void>
struct nmb_param_may_has_null: public std::true_type{};

template <typename src_type>
struct nmb_param_may_has_null<src_type, std::void_t<decltype(src_type::_param_maybe_null)>>
  : public std::conditional<src_type::_param_maybe_null, std::true_type, std::false_type>::type
{};

template<typename SRC_TYPE, typename SELECTOR_TYPE>
int number_accumulator(
    const SRC_TYPE &src,
    ObDataBuffer &allocator1,
    ObDataBuffer &allocator2,
    ObNumber &result,
    uint32_t *sum_digits,
    bool &all_skip,
    const SELECTOR_TYPE &selector)
{
  int ret = OB_SUCCESS;
  ObNumber res;
  ObNumber sum;
  uint32_t normal_sum_path_counter = 0;
  uint32_t fast_sum_path_counter = 0;
  int64_t sum_frag_val = 0;
  int64_t sum_int_val = 0;
  // TODO zuojiao.hzj: add new number accumulator to avoid memory allocate
  char buf_ori_result[ObNumber::MAX_CALC_BYTE_LEN];
  ObDataBuffer allocator_ori_result(buf_ori_result, ObNumber::MAX_CALC_BYTE_LEN);
  ObNumber ori_result;
  bool may_overflow = false;
  bool ori_result_copied = false;
  uint16_t i = 0; // row num in a batch
  for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
    i = selector.get_batch_index(it);
    if (nmb_param_may_has_null<SRC_TYPE>::value && src.at(i)->is_null()) {
      continue;
    }
    all_skip = false;
    ObNumber src_num(src.at(i)->get_number());
    if (OB_UNLIKELY(src_num.is_zero())) {
      // do nothing
    } else if (src_num.d_.is_2d_positive_decimal()) {
      sum_frag_val += src_num.get_digits()[1];
      sum_int_val += src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else if (src_num.d_.is_1d_positive_fragment()) {
      sum_frag_val += src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else if (src_num.d_.is_1d_positive_integer()) {
      sum_int_val += src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else if (src_num.d_.is_2d_negative_decimal()) {
      sum_frag_val -= src_num.get_digits()[1];
      sum_int_val -= src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else if (src_num.d_.is_1d_negative_fragment()) {
      sum_frag_val -= src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else if (src_num.d_.is_1d_negative_integer()) {
      sum_int_val -= src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else {
      if (OB_UNLIKELY(!ori_result_copied)) {
        // copy result to ori_result to fall back
        MEMSET(buf_ori_result, 0, sizeof(char) * ObNumber::MAX_CALC_BYTE_LEN);
        if (OB_FAIL(ori_result.deep_copy_v3(result, allocator_ori_result))) {
          LOG_WARN("deep copy number failed", K(ret));
        } else {
          ori_result_copied = true;
        }
      }
      if (OB_UNLIKELY(src_num.fast_sum_agg_may_overflow() && fast_sum_path_counter > 0)) {
        may_overflow = true;
        LOG_DEBUG("number accumulator may overflow, fall back to normal path",
                  K(src_num), K(sum_int_val), K(sum_frag_val));
        break;
      } else { // normal path
        ObDataBuffer &allocator = (normal_sum_path_counter % 2 == 0) ? allocator1 : allocator2;
        allocator.free();
        ret = result.add_v3(src_num, res, allocator, true, true);
        result = res;
        ++normal_sum_path_counter;
      }
    }
  }
  if (OB_UNLIKELY(may_overflow)) {
    fast_sum_path_counter = 0;
    normal_sum_path_counter = 0;
    result = ori_result;
    for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
      i = selector.get_batch_index(it);
      if (nmb_param_may_has_null<SRC_TYPE>::value && src.at(i)->is_null()) {
        continue;
      }
      ObNumber src_num(src.at(i)->get_number());
      if (OB_UNLIKELY(src_num.is_zero())) {
        // do nothing
      } else {
        ObDataBuffer &allocator = (normal_sum_path_counter % 2 == 0) ? allocator1 : allocator2;
        allocator.free();
        ret = result.add_v3(src_num, res, allocator, true, true);
        result = res;
        normal_sum_path_counter++;
      }
    }
    if (OB_SUCC(ret)) {
      result.assign(res.d_.desc_, res.get_digits());
    }
  } else if (OB_SUCC(ret) && !all_skip) {
    // construct sum result into number format
    const int64_t base = ObNumber::BASE;
    int64_t carry = 0;
    if (abs(sum_frag_val) >= base) {
      sum_int_val += sum_frag_val / base; // eg : (-21) / 10 = -2
      sum_frag_val = sum_frag_val % base; // eg : (-21) % 10 = -1
    }
    if (sum_int_val > 0 && sum_frag_val < 0) {
      sum_int_val -= 1;
      sum_frag_val += base;
    } else if (sum_int_val < 0 && sum_frag_val > 0) {
      sum_int_val += 1;
      sum_frag_val -= base;
    }
    if (abs(sum_int_val) >= base) {
      carry = sum_int_val / base; // eg : (-21) / 10 = -2
      sum_int_val = sum_int_val % base; // eg : (-21) % 10 = -1
    }
    if (0 == carry && 0 == sum_int_val && 0 == sum_frag_val) { // sum is zero
      sum.set_zero();
    } else if (carry >= 0 && sum_int_val >= 0 && sum_frag_val >= 0) { // sum is positive
      sum.d_.desc_ = NUM_DESC_2DIGITS_POSITIVE_DECIMAL;
      sum.d_.len_ -= (0 == sum_frag_val);
      if (carry > 0) {
        ++sum.d_.exp_;
        ++sum.d_.len_;
        sum.d_.len_ -= (sum_int_val == 0 && sum_frag_val == 0);
        // performance critical: set the tailing digits even they are 0, no overflow risk
        sum_digits[0] = static_cast<uint32_t>(carry);
        sum_digits[1] = static_cast<uint32_t>(sum_int_val);
        sum_digits[2] = static_cast<uint32_t>(sum_frag_val);
      } else { // 0 == carry
        if (0 == sum_int_val) {
          --sum.d_.exp_;
          --sum.d_.len_;
          sum_digits[0] = static_cast<uint32_t>(sum_frag_val);
        } else {
          sum_digits[0] = static_cast<uint32_t>(sum_int_val);
          sum_digits[1] = static_cast<uint32_t>(sum_frag_val);
        }
      }
    } else { // sum is negative
      sum.d_.desc_ = NUM_DESC_2DIGITS_NEGATIVE_DECIMAL;
      sum.d_.len_ -= (0 == sum_frag_val);
      // get abs of carry/sum_int_val/sum_frag_val
      carry = -carry;
      sum_int_val = -sum_int_val;
      sum_frag_val = -sum_frag_val;
      if (carry > 0) {
        --sum.d_.exp_; // notice here : different from postive
        ++sum.d_.len_;
        sum.d_.len_ -= (sum_int_val == 0 && sum_frag_val == 0);
        // performance critical: set the tailing digits even they are 0, no overflow risk
        sum_digits[0] = static_cast<uint32_t>(carry);
        sum_digits[1] = static_cast<uint32_t>(sum_int_val);
        sum_digits[2] = static_cast<uint32_t>(sum_frag_val);
      } else { // 0 == carry
        if (0 == sum_int_val) {
          ++sum.d_.exp_; // notice here : different from postive
          --sum.d_.len_;
          sum_digits[0] = static_cast<uint32_t>(sum_frag_val);
        } else {
          sum_digits[0] = static_cast<uint32_t>(sum_int_val);
          sum_digits[1] = static_cast<uint32_t>(sum_frag_val);
        }
      }
    }
    sum.assign(sum.d_.desc_, sum_digits);
    if (normal_sum_path_counter == 0 && result.is_zero()) {
      // all aggr result is filled in sum, just return sum
      if (sum.d_.len_ == 0) {
        result.set_zero();
      } else {
        result.assign(sum.d_.desc_, sum_digits);
      }
    } else if (sum.d_.len_ == 0) { // do nothing
    } else { // merge sum into aggr result
      ObDataBuffer &allocator = (normal_sum_path_counter % 2 == 0) ? allocator1 : allocator2;
      allocator.free();
      if (OB_FAIL(result.add_v3(sum, res, allocator, true, true))) {
        LOG_WARN("number_accumulator sum error", K(ret), K(sum), K(result));
      } else {
        result.assign(res.d_.desc_, res.get_digits());
      }
    }
  }

  LOG_DEBUG("number_accumulator done", K(ret), K(result),
            K(normal_sum_path_counter), K(fast_sum_path_counter));
  return ret;
}

}
}

#endif