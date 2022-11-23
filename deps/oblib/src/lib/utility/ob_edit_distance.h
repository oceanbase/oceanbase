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

#ifndef OCEANBASE_COMMON_OB_EDIT_DISTANCE_H_
#define OCEANBASE_COMMON_OB_EDIT_DISTANCE_H_

#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
/**
 * ObWords accepts a C language style string, 
 * and divides the string into word sequences according to spaces.
 */
class ObWords {
public:
  ObWords()
      : str_(NULL), start_(0), count_(0)
  {
  }

  ObWords(const char *str)
      : str_(str), start_(0), count_(0)
  {
  }

  inline int divide()
  {
    int ret = OB_SUCCESS;
    if (nullptr != str_) {
      const char *p = str_;
      int64_t word_length = 0;
      words_.reset();
      while(0 != *p) {
        if (' ' == *p) {
          if (0 != word_length && OB_FAIL(words_.push_back(ObString(word_length, p - word_length)))) {
            COMMON_LOG(WARN, "failed to push back word", K(ret), K(word_length));
          }
          word_length = 0;
        } else {
          ++word_length;
        }
        ++p;
      }
      if (0 != word_length && OB_FAIL(words_.push_back(ObString(word_length, p - word_length)))) {
        COMMON_LOG(WARN, "failed to push back word", K(ret), K(word_length));
      }
    }
    count_ = words_.size();
    return ret;
  }
  
  inline ObString &operator[](const int64_t idx) {return words_[idx];}

  inline int64_t get_start() { return start_; }

  inline int64_t get_count() { return count_; }
private:
  const char *str_;
  ObArray<ObString> words_;
  // start index in words_
  int64_t start_;
  // count of words from the start index in words_
  int64_t count_;
};

/**
 * ObEditDistance accepts a sequence type, which requires the following interface:
 * 1. operator[](), used to access the elements in the sequence.
 * 2. int get_start(), used to get the starting index of sequence to calculate edit distance.
 * 3. int get_count(), used to get count of elements in sequence that need to calculate edit distance.
 *
 * Note: The element type of the sequence type must have an interface:
 * 1. bool operator==(),which is used to compare whether two elements are equal.
 * 
 * for example, ObWords is a sequence type that ObEditDistance can process.
 */
template<typename T>
class ObEditDistance
{
public:
  typedef int64_t ob_ed_size_t;
public:
  static int cal_edit_distance(T a, T b, ob_ed_size_t &edit_dist) {
    const ob_ed_size_t a_count = a.get_count();
    const ob_ed_size_t b_count = b.get_count();
    if (0 == a_count * b_count) {
      edit_dist = a_count + b_count;
    } else {
      const ob_ed_size_t a_start = a.get_start();
      const ob_ed_size_t b_start = b.get_start();
      ob_ed_size_t dp[b_count + 1];
      ob_ed_size_t temp[b_count + 1];
      for (ob_ed_size_t i = 0; i <= b_count; ++i) {
        dp[i] = i;
      }

      for (ob_ed_size_t i = 1; i <= a_count; ++i) {
        for (ob_ed_size_t j = 0; j <= b_count; ++j) {
          temp[j] = dp[j];
        }
        dp[0] = i;
        for (ob_ed_size_t j = 1; j <= b_count; ++j) {
          if (a[a_start + i - 1] == b[b_start + j - 1]) {
            dp[j] = temp[j-1];
          } else {
            ob_ed_size_t temp_min = temp[j] < temp[j - 1] ? temp[j] : temp[j - 1];
            dp[j] = 1 + (temp_min < dp[j - 1] ? temp_min : dp[j - 1]);
          }
        }
      }
      edit_dist = dp[b_count];
    }
    return OB_SUCCESS;
  }
};
} // end namespace common
} // end namespace oceanbase
#endif //OCEANBASE_COMMON_OB_EDIT_DISTANCE_H_