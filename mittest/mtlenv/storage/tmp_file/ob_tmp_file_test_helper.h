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

#ifndef OB_TMP_FILE_TEST_HELPER_
#define OB_TMP_FILE_TEST_HELPER_
#include <vector>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <random>
#define USING_LOG_PREFIX STORAGE
#include "lib/lock/ob_spin_rwlock.h"

namespace oceanbase
{
using namespace common;
/* ------------------------------ Test Helper ------------------------------ */
void print_hex_data(const char *buffer, int64_t length)
{
  std::cout << std::hex << std::setfill('0');
  for (int64_t i = 0; i < length; ++i) {
      std::cout << std::setw(2) << static_cast<int>(static_cast<unsigned char>(buffer[i]));
  }
  std::cout << std::dec << std::endl;
}

void dump_hex_data(const char *buffer, int length, const std::string &filename)
{
  static SpinRWLock lock_;
  SpinWLockGuard guard(lock_);
  std::ifstream ifile(filename);
  if (ifile) {
  } else {
    std::ofstream file(filename, std::ios::out | std::ios::binary);
    if (file.is_open()) {
      for (int i = 0; i < length; ++i) {
        if (i % 8192 == 0) {
          if (i != 0) {
            file << std::endl;
          }
          file << "---page " << std::dec <<  i / 8192;
        }
        if (i % 16 == 0) {
          file << std::endl;
        } else if (i != 0 && i % 2 == 0) {
          file << " ";
        }
        file << std::hex << std::setw(2) << std::setfill('0')
             << (static_cast<int>(buffer[i]) & 0xFF);
      }
      file.close();
      std::cout << "Data has been written to " << filename << " in hex format." << std::endl;
    } else {
      std::cerr << "Error opening file " << filename << " for writing." << std::endl;
    }
  }
}

bool compare_and_print_hex_data(const char *lhs, const char *rhs,
                                int64_t buf_length, int64_t print_length,
                                std::string &filename)
{
  bool is_equal = false;
  static SpinRWLock lock_;
  SpinWLockGuard guard(lock_);
  static int64_t idx = 0;
  filename.clear();
  filename = std::to_string(ATOMIC_FAA(&idx, 1)) + "_cmp_and_dump_hex_data.txt";
  std::ofstream file(filename, std::ios::out | std::ios::binary);
  if (file.is_open()) {
    is_equal = true;
    for (int i = 0; i < buf_length; ++i) {
      if (lhs[i] != rhs[i]) {
        is_equal = false;
        int64_t print_begin = i - print_length / 2 >= 0 ? i - print_length / 2 : 0;
        int64_t print_end = print_begin + print_length < buf_length ? print_begin + print_length : buf_length;
        file << "First not equal happen at " << i
             << ", print length: " << print_end - print_begin
             << ", print begin: " << print_begin
             << ", print end: " << print_end << std::endl;
        file << std::endl << "lhs:" << std::endl;
        {
          const char *buffer = lhs + print_begin;
          int64_t length = print_end - print_begin;
          for (int64_t i = 0; i < length; ++i) {
            file << std::hex << std::setw(2) << std::setfill('0') << (static_cast<int>(buffer[i]) & 0xFF);
          }
        }
        file << std::endl << "rhs:" << std::endl;
        {
          const char *buffer = rhs + print_begin;
          int64_t length = print_end - print_begin;
          for (int64_t i = 0; i < length; ++i) {
            file << std::hex << std::setw(2) << std::setfill('0') << (static_cast<int>(buffer[i]) & 0xFF);
          }
        }
        std::cout << "not equal at " << i << std::endl;
        break;
      }
    }
    file.close();
    if (is_equal) {
      remove(filename.c_str());
    }
  } else {
    std::cerr << "Error opening file " << filename << " for writing." << std::endl;
  }
  return is_equal;
}

int64_t generate_random_int(const int64_t lower_bound, const int64_t upper_bound)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int64_t> dis(lower_bound, upper_bound);
  int64_t random_number = dis(gen);
  return random_number;
}

std::vector<int64_t> generate_random_sequence(const int64_t lower_bound,
                                              const int64_t upper_bound,
                                              const int64_t sequence_sum,
                                              unsigned seed = std::random_device{}())
{
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int64_t> dis(lower_bound, upper_bound);
  std::vector<int64_t> random_sequence;
  int64_t sum = 0;
  while (sum < sequence_sum) {
    int64_t rand_num = std::min(sequence_sum - sum, dis(gen));
    random_sequence.push_back(rand_num);
    sum += rand_num;
  }
  return random_sequence;
}

} // namespace oceanbase

#endif
