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

#include <unistd.h>
#include <stdio.h>
#include <cstdlib>
#include <string.h>
#include <sys/timeb.h>
#include <math.h>
#include <iostream>
#include <fstream>
#include <string>
#include "lib/utility/utility.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/number/ob_number_v2.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/utility.h"
#include "lib/ob_define.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "gtest/gtest.h"
#include "common/object/ob_object.h"
#include <set>

using namespace oceanbase;
using namespace common;
using namespace number;

// Test:
// Use common algorithm, partition for data, and rehash with a new seed,
// verify if data is evenly partitioned.
#define DEF_NUMBER_STDV_DIFF_SEED(ALGO, HASH_ALGO) \
TEST(ObHashFunc, number_stdv_diff_seed_##ALGO)  \
{                                               \
    OB_LOG(WARN, "\n\nTest number stdv different seed ", K(#ALGO) " hash");      \
    const uint64_t HASH_SEED = 16777213;                                  \
	const int64_t MAX_TEST_COUNT = 1000000000; /* 10^8 */                 \
    const int DIFF_BUCKET_NUM = 5;                                          \
    const int BUCKET_NUM[DIFF_BUCKET_NUM] = {7,8,31,32,256};                \
    const int MAX_BUCKET_NUM = 256;                                         \
    const int HASH_DIST_NUM = 256;                                          \
    double stdv[DIFF_BUCKET_NUM][MAX_BUCKET_NUM];                           \
    uint64_t  count[DIFF_BUCKET_NUM][MAX_BUCKET_NUM][HASH_DIST_NUM];        \
    uint64_t  count_upper[DIFF_BUCKET_NUM][MAX_BUCKET_NUM];                 \
    for (int i = 0; i < DIFF_BUCKET_NUM; i++) {                             \
        for (int j = 0; j < MAX_BUCKET_NUM; j++) {                      \
            count_upper[i][j] = 0;                                   \
            stdv[i][j] = 0;                                         \
            for (int k = 0; k < MAX_BUCKET_NUM; k++) {              \
                count[i][j][k] = 0;                                     \
            }                                                           \
        }                                                               \
    }                                                                       \
    const int64_t MAX_BUF_SIZE = 512;                                       \
    char buf_alloc[MAX_BUF_SIZE];                                           \
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);                        \
    number::ObNumber num;                                                   \
    ObObj obj;                                                              \
    uint64_t value;                                     \
    uint64_t value_dist;                                \
    uint64_t idx;                                       \
    uint64_t idx_dist;                                  \
    for (int64_t i = 0; i < MAX_TEST_COUNT; i++) {      \
        EXPECT_EQ(OB_SUCCESS, num.from(i, allocator));  \
        obj.set_number(num);                            \
        value = obj.HASH_ALGO();                      \
        value_dist = obj.HASH_ALGO(HASH_SEED);     \
        for (int64_t j = 0; j < DIFF_BUCKET_NUM; j++) { \
            idx = value % BUCKET_NUM[j];                \
            idx_dist = value_dist % HASH_DIST_NUM;      \
            count_upper[j][idx]++;                      \
            count[j][idx][idx_dist]++;                  \
        }                                               \
        allocator.free();                               \
    }                                                   \
    /* calculate standard deviation of every hash partition */                \
    for (int i = 0; i < DIFF_BUCKET_NUM; i++) {             \
        for (int j = 0; j < BUCKET_NUM[i]; j++) {           \
            double avg = count_upper[i][j] / HASH_DIST_NUM;     \
            for (int k = 0; k < HASH_DIST_NUM; k++) {           \
                stdv[i][j] += (count[i][j][k] - avg) * (count[i][j][k] - avg);  \
            }                                               \
            stdv[i][j] /= HASH_DIST_NUM;                    \
            stdv[i][j] = pow(stdv[i][j], 0.5);              \
            double std = stdv[i][j];                        \
        }                                               \
    }                                                   \
    double stdv_avg[DIFF_BUCKET_NUM];                       \
    for (int i = 0; i < DIFF_BUCKET_NUM; i++) {             \
        stdv_avg[i] = 0;                                    \
        for (int j = 0; j < BUCKET_NUM[i]; j++) {           \
            stdv_avg[i] += stdv[i][j];                      \
        }                                                   \
        stdv_avg[i] /= BUCKET_NUM[i];                       \
        double deviation = stdv_avg[i]	/ MAX_TEST_COUNT * BUCKET_NUM[i] * HASH_DIST_NUM;           \
        OB_LOG(WARN, "record average stdv of diff bucket num", K(BUCKET_NUM[i]), K(stdv_avg[i]), K(deviation));  \
    }                                                       \
}

// Test 10^9 times:
// change first uint32 in digits when size of digits is in [1, 8].
//HASH_ALGO is the name of hash method of ObObj.
#define DEF_NUMBER_STDV_FULL64(ALGO, HASH_ALGO) \
TEST(ObHashFunc, number_stdv_full64_##ALGO)  \
{                                            \
    OB_LOG(WARN, "\n\nTest number stdv full64 ", K(#ALGO) " hash");      \
    const int64_t MAX_TEST_COUNT = 1000000000; /* 10^9 */           \
    const int DIFF_BUCKET_NUM = 5;                              \
    const int BUCKET_NUM[DIFF_BUCKET_NUM] = {7,8,31,32, 256};        \
    const int MAX_BUCKET_NUM = 32;                              \
    const int MAX_DIGITS_LEGNTH = 8;                            \
    double variance[MAX_DIGITS_LEGNTH][DIFF_BUCKET_NUM]; \
    const int64_t MAX_BUF_SIZE = 512;                           \
    char buf_alloc[MAX_BUF_SIZE];                               \
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);            \
    number::ObNumber num;                                       \
    ObObj obj;                                                  \
    std::string str("");                                        \
    for (int length = 1; length <= MAX_DIGITS_LEGNTH; length++){ \
        uint64_t  count[DIFF_BUCKET_NUM][MAX_BUCKET_NUM];    \
        for (int j = 0; j < DIFF_BUCKET_NUM;j++) {                  \
            for (int k = 0; k < MAX_BUCKET_NUM;k++) {           \
                count[j][k] = 0;                         \
            }                                                   \
        }                                                       \
        /* construct ObNumber which length is i */                                    \
        str.append("123456789");                                    \
        EXPECT_EQ(OB_SUCCESS, num.from(str.c_str(), allocator));    \
        OB_LOG(WARN, "show numer", K(length), K(num));              \
        uint64_t value;                                  \
        for (int64_t i = 0; i < MAX_TEST_COUNT; i++) {             \
            num.get_digits()[0] = i;                               \
            obj.set_number(num);                                \
            value = obj.HASH_ALGO();                   \
            for (int64_t j = 0; j < DIFF_BUCKET_NUM; j++){      \
                count[j][value % BUCKET_NUM[j]]++;\
            }                                                   \
            allocator.free();                                   \
        }                                                       \
        /* calculate standard deviation of different count of buckets */  \
        for (int i = 0; i < DIFF_BUCKET_NUM; i++) {                 \
            variance[length - 1][i] = 0;                      \
            double average = (double)MAX_TEST_COUNT / (double)BUCKET_NUM[i];    \
            for (int j = 0; j < BUCKET_NUM[i]; j++){                            \
                variance[length - 1][i] += (count[i][j] - average) * (count[i][j] - average); \
            }                                                       \
            variance[length - 1][i] /= BUCKET_NUM[i];        \
        }                                                           \
    }                                                               \
    double stdv_averagelength[DIFF_BUCKET_NUM];              \
    for (int bucket_number = 0; bucket_number < DIFF_BUCKET_NUM; bucket_number++) {     \
        stdv_averagelength[bucket_number] = 0;               \
        for (int i = 0; i < MAX_DIGITS_LEGNTH; i++) {               \
            stdv_averagelength[bucket_number] += pow(variance[i][bucket_number], 0.5);  \
        }                                                           \
        stdv_averagelength[bucket_number] /= MAX_DIGITS_LEGNTH;      \
        double deviation =  stdv_averagelength[bucket_number] * BUCKET_NUM[bucket_number] / MAX_TEST_COUNT;    \
        OB_LOG(WARN, "record average deviation of diff length", K(BUCKET_NUM[bucket_number]), K(stdv_averagelength[bucket_number]), K(deviation));   \
    }   \
}

// Test for every byte in hash for 10^9 times:
// change first uint32 in digits when size of digits is in [1, 8],
// calculate standard deviation of different count of bytes as bucket count.
#define DEF_NUMBER_STDV_SINGLE_BYTE(ALGO, HASH_ALGO) \
TEST(ObHashFunc, number_stdv_single_byte_##ALGO)  \
{                                            \
    OB_LOG(WARN, "\n\nTest number stdv single byte ", K(#ALGO) " hash");    \
    const int64_t MAX_TEST_COUNT = 1000000000; /* 10^9 */       \
    const int64_t MAX_MOVE = 8;                                 \
    const int BUCKET_NUM = 256;                                 \
    const int MAX_DIGITS_LEGNTH = 8;                            \
    double variance[MAX_MOVE][MAX_DIGITS_LEGNTH];               \
    const int64_t MAX_BUF_SIZE = 512;                           \
    char buf_alloc[MAX_BUF_SIZE];                               \
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);            \
    number::ObNumber num;                                       \
    ObObj obj;                                                  \
    std::string str("");                                        \
    for (int length = 1; length <= MAX_DIGITS_LEGNTH; length++){    \
        uint64_t  count[MAX_MOVE][BUCKET_NUM];                  \
        for (int i = 0;i < MAX_MOVE; i++) {                     \
            for (int k = 0; k < BUCKET_NUM;k++) {               \
                count[i][k] = 0;                                \
            }                                                   \
        }                                                       \
        /* construct ObNumber which length is i */              \
        str.append("123456789");                                \
        EXPECT_EQ(OB_SUCCESS, num.from(str.c_str(), allocator));    \
        uint64_t value64;                                       \
        uint8_t *value;                                         \
        for (int64_t i = 0; i < MAX_TEST_COUNT; i++) {          \
            num.get_digits()[0] = i;                            \
            obj.set_number(num);                                \
            value64 = obj.HASH_ALGO();                          \
            value = (uint8_t *)&value64;                        \
            for (int m = 0; m < MAX_MOVE; m++) {                \
                count[m][value[m] % BUCKET_NUM]++;              \
            }                                                   \
            allocator.free();                                   \
        }                                                       \
        for (int m = 0; m < MAX_MOVE; m++) {                    \
            variance[m][length - 1] = 0;                        \
            double average = (double)MAX_TEST_COUNT / (double)BUCKET_NUM;   \
            for (int j = 0; j < BUCKET_NUM; j++){               \
                variance[m][length - 1] += (count[m][j] - average) * (count[m][j] - average);   \
            }                                                   \
            variance[m][length - 1] /= BUCKET_NUM;              \
        }                                                       \
    }                                                           \
    double stdv_averagelength[MAX_MOVE];                        \
    for (int move = 0; move < MAX_MOVE; move++) {               \
        stdv_averagelength[move] = 0;                           \
        for (int i = 0; i < MAX_DIGITS_LEGNTH; i++) {           \
            stdv_averagelength[move] += pow(variance[move][i], 0.5);    \
        }                                                               \
        stdv_averagelength[move] /= MAX_DIGITS_LEGNTH;          \
        double deviation = stdv_averagelength[move] * (double)BUCKET_NUM / (double)MAX_TEST_COUNT;  \
        OB_LOG(WARN, "record average stv of diff length", K(move), K(stdv_averagelength[move]), K(deviation));              \
    }                                                           \
}

// Test obnumber of 1-8 digits for 10^9 times
#define DEF_NUMBER_TIME(ALGO, HASH_ALGO)            \
TEST(ObHashFunc, number_time_##ALGO)                \
{                                                   \
    OB_LOG(WARN, "\n\nTest number time ", K(#ALGO) " hash");    \
    const int64_t MAX_TEST_COUNT = 1000000000; /* 10^9 */   \
    const int MAX_DIGITS_LEGNTH = 8;                        \
    const int64_t MAX_BUF_SIZE = 256;               \
    char buf_alloc[MAX_BUF_SIZE];                   \
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);    \
    number::ObNumber num;                           \
    ObObj obj;                                      \
    std::string str("");                            \
    int64_t sum_time_ms = 0;                        \
    for (int length = 1; length <= MAX_DIGITS_LEGNTH; length++){    \
        str.append("123456789");                                    \
        EXPECT_EQ(OB_SUCCESS, num.from(str.c_str(), allocator));    \
        obj.set_number(num);                                        \
        uint64_t value = 0;                                         \
        struct timeb tb1, tb2;                                      \
        ftime(&tb1);                                                \
        for (int i = 1; i <= MAX_TEST_COUNT; i++) {                 \
            value = obj.HASH_ALGO(value);                           \
        }                                                           \
        ftime(&tb2);                                                \
        int64_t time_ms = (tb2.time - tb1.time) * 1000 + (tb2.millitm - tb1.millitm);   \
        sum_time_ms += time_ms;                                                         \
        OB_LOG(WARN, "record time for each length", K(length), K(time_ms), K(value));   \
        allocator.free();                                                               \
    }                                                                                   \
    int64_t avg_time_ms = sum_time_ms / MAX_DIGITS_LEGNTH;                              \
    OB_LOG(WARN, "record avg time of all lengths", K(sum_time_ms), K(avg_time_ms));     \
}

int main(int argc, char **argv){
  OB_LOGGER.set_file_name("new_hash_algorithm_performance.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

const int CHARSET_NUM = 9;
ObCollationType collation_type[CHARSET_NUM] = {CS_TYPE_GBK_CHINESE_CI, CS_TYPE_UTF8MB4_GENERAL_CI,
    CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF16_GENERAL_CI, CS_TYPE_UTF16_BIN, CS_TYPE_BINARY,
    CS_TYPE_GBK_BIN, CS_TYPE_UTF16_UNICODE_CI, CS_TYPE_UTF8MB4_UNICODE_CI};
const int BIN_CHARSET_NUM = 4;
ObCollationType bin_collation_type[BIN_CHARSET_NUM] = {CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF16_BIN,
CS_TYPE_BINARY, CS_TYPE_GBK_BIN};
#define DEF_VARCHAR_STDV(ALGO, HASH_ALGO)            \
TEST(ObHashFunc, varchar_stdv_##ALGO)                \
{                                                    \
  OB_LOG(WARN, "\n\nTest varchar stdv ", K(#ALGO) " hash");      \
  const int64_t MAX_TEST_COUNT  = 100000000; /* 10^8 */ \
  const int DIFF_BUCKET_NUM = 4;                        \
  const int BUCKET_NUM[DIFF_BUCKET_NUM] = {7,8,31,32};  \
  const int MAX_BUCKET_NUM = 32;                        \
  uint64_t value;                                    \
  uint64_t count[DIFF_BUCKET_NUM][MAX_BUCKET_NUM];   \
  double variance[CHARSET_NUM][DIFF_BUCKET_NUM];     \
  for (int charset_index = 0; charset_index < CHARSET_NUM; charset_index++) {  \
    /* clear bucket counter */                        \
    for (int i = 0; i < DIFF_BUCKET_NUM; i++) {       \
      for (int j = 0; j < MAX_BUCKET_NUM; j++) {      \
          count[i][j] = 0;                            \
      }                                               \
    }                                                 \
    ObObj obj;                                        \
    ObString ob_str;                                  \
    std::string str;                                  \
    const char* ptr = NULL;                           \
    ObCollationType cs_type = collation_type[charset_index];    \
    /* calculate data count in every bucket */          \
    for (int i = 1; i <= MAX_TEST_COUNT; i++) {         \
        str = std::to_string(i);                        \
        ptr = str.c_str();                              \
        ob_str = ObString::make_string(ptr);            \
        obj.set_varchar(ob_str);                        \
        value = obj.HASH_ALGO(cs_type, 0);              \
        for (uint j = 0; j < DIFF_BUCKET_NUM; j++) {    \
            int bucket = BUCKET_NUM[j];                 \
            count[j][value % bucket]++;                 \
        }                                               \
    }                                                   \
    /* check if count of all buckets is equal to MAX_TEST_COUNT */ \
    for (int i = 0; i < DIFF_BUCKET_NUM; i++) {         \
        int sum = 0;                                    \
        for (int j = 0; j < BUCKET_NUM[i]; j++) {       \
            sum += count[i][j];                         \
        }                                               \
        if (sum != MAX_TEST_COUNT) {                    \
            OB_LOG(WARN, "ERROR:SUM != MAX_TEST_COUNT", K(sum), K(MAX_TEST_COUNT)); \
        }                                               \
    }                                                   \
    /* calculate standard deviation for every hash algorithm for different bucket counts */            \
    for (int i = 0; i < DIFF_BUCKET_NUM; i++) {         \
        double average_count = (double)MAX_TEST_COUNT / (double)BUCKET_NUM[i];  \
        double diff_sum = 0;                            \
        for (int j = 0; j < BUCKET_NUM[i]; j++) {       \
            diff_sum += ((double)count[i][j] - average_count) * ((double)count[i][j] - average_count);  \
        }                                               \
        diff_sum /= BUCKET_NUM[i];                      \
        variance[charset_index][i] = diff_sum;          \
    }                                                   \
  }                                                     \
  /* calculate standard deviation */                    \
  double stdv_fix_bucket[DIFF_BUCKET_NUM];              \
  /* calculate standard deviation for different collation */  \
  for (int j = 0; j < DIFF_BUCKET_NUM; j++) {           \
      stdv_fix_bucket[j] = 0;                           \
      for (int i = 0; i < CHARSET_NUM; i++) {           \
        stdv_fix_bucket[j] += pow(variance[i][j], 0.5); \
      }                                                 \
      stdv_fix_bucket[j] /= CHARSET_NUM;                \
      double deviation = stdv_fix_bucket[j] * (double)BUCKET_NUM[j] / (double)MAX_TEST_COUNT;   \
      OB_LOG(WARN, "fix bucketnum, average stdv:", K(BUCKET_NUM[j]), K(stdv_fix_bucket[j]), K(deviation));      \
  }                                                     \
}

#define DEF_VARCHAR_WORDS_STDV(ALGO, HASH_ALGO)            \
TEST(ObHashFunc, varchar_words_stdv_##ALGO)                \
{                                                           \
    OB_LOG(WARN, "\n\nTest varchar words stdv ", K(#ALGO) " hash");      \
    const int DIFF_BUCKET_NUM = 4;                          \
    const int BUCKET_NUM[DIFF_BUCKET_NUM] = {7,8,31,32};    \
    const int MAX_BUCKET_NUM = 32;                          \
    uint64_t value;                                         \
    uint64_t count[DIFF_BUCKET_NUM][MAX_BUCKET_NUM];        \
    double variance[CHARSET_NUM][DIFF_BUCKET_NUM];          \
    int64_t data_count = 0;                                 \
    for (int charset_index = 0; charset_index < CHARSET_NUM; charset_index++) { \
        /* clear bucket counter */                          \
        for (int i = 0; i < DIFF_BUCKET_NUM; i++) {         \
            for (int j = 0; j < MAX_BUCKET_NUM; j++) {      \
                count[i][j] = 0;                            \
            }                                               \
        }                                                   \
        std::ifstream words("/usr/share/dict/words");       \
        data_count = 0;                                     \
        ObObj obj;                                          \
        ObString ob_str;                                    \
        std::string str;                                    \
        const char* ptr = NULL;                             \
        ObCollationType cs_type = collation_type[charset_index];    \
        /* calculate count of data in every bucket */       \
        while(!words.eof()) {                               \
            data_count++;                                   \
            words >> str;                                   \
            ptr = str.c_str();                              \
            ob_str = ObString::make_string(ptr);            \
            obj.set_varchar(ob_str);                        \
            value = obj.HASH_ALGO(cs_type, 0);              \
            for (uint j = 0; j < DIFF_BUCKET_NUM; j++) {    \
                int bucket = BUCKET_NUM[j];                 \
                count[j][value % bucket]++;                 \
            }                                               \
        }                                                   \
        words.close();                                      \
        /* check if count of data in all buckets is equal to MAX_TEST_COUNT */ \
        for (int i = 0; i < DIFF_BUCKET_NUM; i++) {         \
            int sum = 0;                                    \
            for (int j = 0; j < BUCKET_NUM[i]; j++) {       \
                sum += count[i][j];                         \
            }                                               \
            if (sum != data_count) {                        \
                OB_LOG(WARN, "ERROR:SUM != data_count", K(sum), K(data_count)); \
            }                                               \
        }                                                   \
        /* check if count of all buckets is equal to MAX_TEST_COUNT */ \
        for (int i = 0; i < DIFF_BUCKET_NUM; i++) {         \
            double average_count = (double)data_count / (double)BUCKET_NUM[i];  \
            double diff_sum = 0;                                                \
            for (int j = 0; j < BUCKET_NUM[i]; j++) {                           \
                diff_sum += ((double)count[i][j] - average_count) *((double)count[i][j] - average_count);   \
            }                                               \
            diff_sum /= BUCKET_NUM[i];                      \
            variance[charset_index][i] = diff_sum;          \
        }                                                   \
    }                                                       \
    /* calculate standard deviation */                      \
    double stdv_fix_bucket[DIFF_BUCKET_NUM];                \
    /* calculate standard deviation for different collation */\
    for (int j = 0; j < DIFF_BUCKET_NUM; j++) {             \
        stdv_fix_bucket[j] = 0;                             \
        for (int i = 0; i < CHARSET_NUM; i++) {             \
            stdv_fix_bucket[j] += pow(variance[i][j], 0.5); \
        }                                                   \
        stdv_fix_bucket[j] /= CHARSET_NUM;                  \
        double deviation = stdv_fix_bucket[j] * (double)BUCKET_NUM[j] / data_count;         \
        OB_LOG(WARN, "fix bucketnum, average stdv:", K(j), K(stdv_fix_bucket[j]), K(data_count), K(deviation)); \
    }                                                       \
}

#define DEF_VARCHAR_TIME(ALGO, HASH_ALGO)            \
TEST(ObHashFunc, varchar_time_##ALGO)                \
{                                                    \
  OB_LOG(WARN, "\n\nTest varchar time ", K(#ALGO) " hash");  \
  const int64_t MAX_TEST_COUNT  = 100000000; /* 10^8 */ \
  const int64_t CALC_NUM = 8;    /* 11111111 */       \
  uint64_t value = 0;                                \
  int64_t time[CHARSET_NUM][CALC_NUM];               \
  for (int i = 0;i < CHARSET_NUM; i++) {             \
      for (int j = 0;j < CALC_NUM;j ++) {            \
          time[i][j] = 0;                            \
      }                                              \
  }                                                  \
  /* calculate hash for 1, 11, 111, 1111, ...,11111111 for every 10^8 times for every collation type */ \
  for (int charset_index = 0; charset_index < CHARSET_NUM; charset_index++) {  \
    ObCollationType cs_type = collation_type[charset_index];                    \
    ObObj obj;                                                                  \
    ObString ob_str;                                                            \
    std::string str;                                                            \
    const char* ptr = NULL;                                                     \
    struct timeb tb1, tb2;                              \
    int number = 1;                                     \
    for (int n = 0; n < CALC_NUM; n++) {                \
        str = std::to_string(number);                   \
        ptr = str.c_str();                              \
        ob_str = ObString::make_string(ptr);            \
        obj.set_varchar(ob_str);                        \
        ftime(&tb1);                                    \
        for (int i = 1; i <= MAX_TEST_COUNT; i++) {     \
            value = obj.HASH_ALGO(cs_type, value);      \
        }                                               \
        ftime(&tb2);                                    \
        int64_t time_ms = (tb2.time - tb1.time) * 1000 + (tb2.millitm - tb1.millitm);   \
        time[charset_index][n] = time_ms;               \
        number = number * 10 + 1;                       \
    }                                                   \
  }                                                     \
  /* calculate average calculation time for all collation for all hash algorithm */ \
  int64_t time_fixed_length[CALC_NUM];                  \
  for (int j = 0; j < CALC_NUM; j++) {                  \
      time_fixed_length[j] = 0;                         \
      for (int i = 0; i < CHARSET_NUM; i++) {           \
        time_fixed_length[j] += time[i][j];             \
      }                                                 \
			time_fixed_length[j] /= CHARSET_NUM;              \
      OB_LOG(WARN, "fix length, avg time:", K(j), K(time_fixed_length[j]));   \
  }                                                     \
}

#define DEF_VARCHAR_SAME_TAIL(ALGO, HASH_ALGO)            \
TEST(ObHashFunc, varchar_same_tail_##ALGO)                \
{                                                    \
  OB_LOG(WARN, "\n\nTest varchar same tail stdv ", K(#ALGO) " hash");  \
    const int64_t MAX_TEST_COUNT  = 100000000;  /* 10^8 */  \
    const int DIFF_BUCKET_NUM = 4;                          \
    const int ONE_TIME_LENGTH = 128;                        \
    const int BUCKET_NUM[DIFF_BUCKET_NUM] = {7,8,31,32};    \
    const int MAX_BUCKET_NUM = 32;                          \
    uint64_t value;                                         \
    uint64_t count[DIFF_BUCKET_NUM][MAX_BUCKET_NUM];        \
    double variance[CHARSET_NUM][DIFF_BUCKET_NUM];          \
    for (int charset_index = 0; charset_index < CHARSET_NUM; charset_index++) {     \
        /* clear bucket counter */                          \
        for (int i = 0; i < DIFF_BUCKET_NUM; i++) {         \
            for (int j = 0; j < MAX_BUCKET_NUM; j++) {      \
                count[i][j] = 0;                            \
            }                                               \
        }                                                   \
        ObObj obj;                                          \
        ObString ob_str;                                    \
        std::string str;                                    \
        char ptr[ONE_TIME_LENGTH + 9];                      \
        for (int i = 0; i < ONE_TIME_LENGTH + 9; i++) {     \
            if (i < ONE_TIME_LENGTH - 8 || i > ONE_TIME_LENGTH - 1) {   \
                ptr[i] = i % 10 + '0';                      \
            } else {                                        \
                ptr[i] = '0';                               \
            }                                               \
        }                                                   \
        ptr[ONE_TIME_LENGTH + 8] = '\0';                    \
        ObCollationType cs_type = collation_type[charset_index];    \
        /* calculate count of data in every bucket */       \
        for (int i = 0; i < MAX_TEST_COUNT; i++) {          \
            /* construct 136byte data, the first 120byte and the last 8byte are equal. */ \
            /* 121~128-th byte are from 00000000 to 99999999 */  \
            str = std::to_string(i);                        \
            memcpy(ptr + ONE_TIME_LENGTH - str.length(), str.c_str(), str.length());    \
            ob_str = ObString::make_string(ptr);            \
            obj.set_varchar(ob_str);                        \
            value = obj.HASH_ALGO(cs_type, 0);              \
            for (uint j = 0; j < DIFF_BUCKET_NUM; j++) {    \
                int bucket = BUCKET_NUM[j];                 \
                count[j][(value) % bucket]++;               \
            }                                               \
        }                                                   \
        /* check if count of data in all buckets is equal to MAX_TEST_COUNT */ \
        for (int i = 0; i < DIFF_BUCKET_NUM; i++) {         \
            int sum = 0;                                    \
            for (int j = 0; j < BUCKET_NUM[i]; j++) {       \
                sum += count[i][j];                         \
            }                                               \
            if (sum != MAX_TEST_COUNT) {                    \
                OB_LOG(WARN, "ERROR:SUM != MAX_TEST_COUNT", K(sum), K(MAX_TEST_COUNT)); \
            }                                               \
        }                                                   \
        /* calculate standard deviation for different count of buckets for every hash algorithm */ \
        for (int i = 0; i < DIFF_BUCKET_NUM; i++) {             \
            double average_count = (double)MAX_TEST_COUNT / (double)BUCKET_NUM[i];  \
            double diff_sum = 0;                            \
            for (int j = 0; j < BUCKET_NUM[i]; j++) {       \
                diff_sum += ((double)count[i][j] - average_count) * ((double)count[i][j] - average_count);  \
            }                                               \
            diff_sum /= BUCKET_NUM[i];                      \
            variance[charset_index][i] = diff_sum;          \
        }                                                   \
    }                                                       \
    /* calculate standard deviation */                      \
    double stdv_fix_bucket[DIFF_BUCKET_NUM];          \
    /* calculate standard deviation for different collation */\
    for (int j = 0; j < DIFF_BUCKET_NUM; j++) {             \
        stdv_fix_bucket[j] = 0;                             \
        for (int i = 0; i < CHARSET_NUM; i++) {             \
            stdv_fix_bucket[j] += pow(variance[i][j], 0.5); \
        }                                                   \
        stdv_fix_bucket[j] /= CHARSET_NUM;                  \
        double deviation = stdv_fix_bucket[j] * (double)BUCKET_NUM[j] / (double)MAX_TEST_COUNT; \
        OB_LOG(WARN, "fix bucketnum, average stdv:", K(BUCKET_NUM[j]), K(stdv_fix_bucket[j]), K(deviation));  \
    }                                                       \
}

#define DEF_VARCHAR_LONG_TEXT_STDV(ALGO, HASH_ALGO)            \
TEST(ObHashFunc, varchar_long_text_stdv_##ALGO)                \
{                                                            \
    OB_LOG(WARN, "\n\nTest varchar long text stdv ", K(#ALGO) " hash");  \
    const int64_t MAX_TEST_COUNT  = 10000000;   /* 10^7 */  \
    const int64_t TEXT_LENGTH = 2 * 1024;                   \
    const int DIFF_BUCKET_NUM = 4;                          \
    const int BUCKET_NUM[DIFF_BUCKET_NUM] = {7,8,31,32};    \
    const int MAX_BUCKET_NUM = 32;                          \
    uint64_t value;                                         \
    uint64_t count[DIFF_BUCKET_NUM][MAX_BUCKET_NUM];        \
    double variance[CHARSET_NUM][DIFF_BUCKET_NUM];          \
    /* construct 2K data */                                 \
    char data[TEXT_LENGTH + 1];                             \
    int length = 0;                                         \
    std::ifstream infile("/usr/share/dict/words");          \
    while (length < TEXT_LENGTH && !infile.eof()) {         \
        std::string str;                                    \
        infile >> str;                                      \
        int append_length = TEXT_LENGTH - length > str.length() ? str.length() : TEXT_LENGTH - length;  \
        memcpy(data + length, str.c_str(), append_length);  \
        length += append_length;                            \
    }                                                       \
    infile.close();                                         \
    for (int i = 0; i < 8; i++) {                           \
        data[i] = '0';                                      \
    }                                                       \
    data[TEXT_LENGTH] = '\0';                               \
    for (int charset_index = 0; charset_index < CHARSET_NUM; charset_index++) { \
        /* clear bucket counter */                          \
        for (int i = 0; i < DIFF_BUCKET_NUM; i++) {         \
            for (int j = 0; j < MAX_BUCKET_NUM; j++) {      \
                count[i][j] = 0;                            \
            }                                               \
        }                                                   \
        ObObj obj;                                          \
        ObString ob_str;                                    \
        std::string str;                                    \
        ObCollationType cs_type = collation_type[charset_index];    \
        /* calculate count of data in every bucket */       \
        for (int i = 0; i < MAX_TEST_COUNT; i++) {          \
            str = std::to_string(i);                        \
            memcpy(data + 8 - str.length(), str.c_str(), str.length()); \
            ob_str = ObString::make_string(data);           \
            obj.set_varchar(ob_str);                        \
            value = obj.HASH_ALGO(cs_type, 0);              \
            for (uint j = 0; j < DIFF_BUCKET_NUM; j++) {    \
                int bucket = BUCKET_NUM[j];                 \
                count[j][value % bucket]++;     \
            }                                               \
        }                                                   \
        /* check if count of all buckets is equal to MAX_TEST_COUNT */ \
        for (int i = 0; i < DIFF_BUCKET_NUM; i++) {         \
            int sum = 0;                                    \
            for (int j = 0; j < BUCKET_NUM[i]; j++) {       \
                sum += count[i][j];                         \
            }                                               \
            if (sum != MAX_TEST_COUNT) {                    \
                OB_LOG(WARN, "ERROR:SUM != MAX_TEST_COUNT", K(sum), K(MAX_TEST_COUNT)); \
            }                                               \
        }                                                   \
        /* calculate standard deviation for different count of buckets */                        \
        for (int i = 0; i < DIFF_BUCKET_NUM; i++) {         \
            double average_count = (double)MAX_TEST_COUNT / (double)BUCKET_NUM[i];  \
            double diff_sum = 0;                            \
            for (int j = 0; j < BUCKET_NUM[i]; j++) {       \
                diff_sum += ((double)count[i][j] - average_count) * ((double)count[i][j] - average_count);  \
            }                                               \
            diff_sum /= BUCKET_NUM[i];                      \
            variance[charset_index][i] = diff_sum;          \
        }                                                   \
    }                                                       \
    double stdv_fix_bucket[DIFF_BUCKET_NUM];                \
    /* calculate standard deviation for different collation */  \
    for (int j = 0; j < DIFF_BUCKET_NUM; j++) {             \
        stdv_fix_bucket[j] = 0;                             \
        for (int i = 0; i < CHARSET_NUM; i++) {             \
            stdv_fix_bucket[j] += pow(variance[i][j], 0.5); \
        }                                                   \
        stdv_fix_bucket[j] /= CHARSET_NUM;                  \
        double deviation = stdv_fix_bucket[j] * (double)BUCKET_NUM[j] / (double)MAX_TEST_COUNT; \
        OB_LOG(WARN, "fix bucketnum, average stdv:", K(j), K(stdv_fix_bucket[j]), K(deviation));  \
    }                                                       \
}   

#define DEF_VARCHAR_LONG_TEXT_TIME(ALGO, HASH_ALGO, LENGTH, TIMES)   \
TEST(ObHashFunc, varchar_long_text_time_##ALGO##_##LENGTH_##TIMES)                \
{                                                            \
    OB_LOG(WARN, "\n\nTest varchar long text time #ALGO hash, text length:", K(#LENGTH), K(#TIMES));  \
    const int64_t MAX_TEST_COUNT  = TIMES;                  \
    const int64_t TEXT_LENGTH = LENGTH;                     \
    uint64_t value = 0;                                     \
    char data[TEXT_LENGTH + 1];                             \
    int length = 0;                                         \
    std::ifstream infile("/usr/share/dict/words");          \
    while (length < TEXT_LENGTH && !infile.eof()) {         \
        std::string str;                                    \
        infile >> str;                                      \
        int append_length = TEXT_LENGTH - length > str.length() ? str.length() : TEXT_LENGTH - length;  \
        memcpy(data + length, str.c_str(), append_length);  \
        length += append_length;                            \
    }                                                       \
    infile.close();                                         \
    data[TEXT_LENGTH] = '\0';                               \
    ObObj obj;                                              \
    ObString ob_str;                                        \
    ob_str = ObString::make_string(data);                   \
    obj.set_varchar(ob_str);                                \
    int64_t average_time = 0;                               \
    for (int charset_index = 0; charset_index < CHARSET_NUM; charset_index++) { \
        value = 0;                                          \
        ObCollationType cs_type = collation_type[charset_index];    \
        struct timeb tb1, tb2;                              \
        ftime(&tb1);                                        \
        for (int i = 0; i < MAX_TEST_COUNT; i++) {          \
            value = obj.HASH_ALGO(cs_type, value);          \
        }                                                   \
        ftime(&tb2);                                        \
        int64_t time_ms = (tb2.time - tb1.time) * 1000 + (tb2.millitm - tb1.millitm);   \
        OB_LOG(WARN, "long text time", K(charset_index), K(time_ms), K(value));     \
        average_time += time_ms;                            \
    }                                                       \
    average_time /= CHARSET_NUM;                            \
    OB_LOG(WARN, "long text average time", K(average_time), K(obj.get_string_len()));   \
}

#define DEF_VARCHAR_STDV_SINGLE_BYTE(ALGO, HASH_ALGO)            \
TEST(ObHashFunc, varchar_stdv_single_byte_##ALGO)                \
{                                                            \
    OB_LOG(WARN, "\n\nTest varchar stdv single byte ", K(#ALGO) " hash");  \
    const int64_t MAX_TEST_COUNT  = 100000000;     /* 10^8 */     \
    const int BYTE_NUMBER = 8;                                \
    const int BUCKET_NUM = 256;                               \
    uint64_t value64;                                         \
    uint8_t *value;                                           \
    uint64_t count[BYTE_NUMBER][BUCKET_NUM];                  \
    double variance[BYTE_NUMBER][CHARSET_NUM];                \
    for (int charset_index = 0; charset_index < CHARSET_NUM; charset_index++) {   \
        /* clear bucket counter */                            \
        for (int move = 0; move < BYTE_NUMBER; move++){             \
            for (int j = 0; j < BUCKET_NUM; j++) {              \
                count[move][j] = 0;                       \
            }                                                   \
        }                                                       \
        ObObj obj;                                              \
        ObString ob_str;                                        \
        std::string str;                                        \
        const char* ptr = NULL;                                 \
        ObCollationType cs_type = collation_type[charset_index];    \
        /* calculate count of data in every bucket */           \
        for (int i = 1; i <= MAX_TEST_COUNT; i++) {              \
            str = std::to_string(i);                                \
            ptr = str.c_str();                                      \
            ob_str = ObString::make_string(ptr);                    \
            obj.set_varchar(ob_str);                                \
            value64 = obj.HASH_ALGO(cs_type, 0);                \
            value = (uint8_t *)&value64;                        \
            for (int move = 0; move < BYTE_NUMBER; move++) {    \
                count[move][value[move] % BUCKET_NUM]++;        \
            }                                                   \
        }                                                       \
        /* check if count of data in all buckets is equal to MAX_TEST_COUNT */ \
        for (int move = 0; move < BYTE_NUMBER; move++) {        \
            int sum = 0;                                        \
            for (int bucket_idx = 0; bucket_idx < BUCKET_NUM; bucket_idx++) {   \
                sum += count[move][bucket_idx];                 \
            }                                                   \
            if (sum != MAX_TEST_COUNT) {                        \
                OB_LOG(WARN, "ERROR:SUM != MAX_TEST_COUNT", K(move), K(sum), K(MAX_TEST_COUNT));    \
            }                                                   \
        }                                                       \
        for (int move = 0; move < BYTE_NUMBER; move++) {            \
            double average_count = (double)MAX_TEST_COUNT / (double)BUCKET_NUM; \
            double diff_sum = 0;                                \
            for (int j = 0; j < BUCKET_NUM; j++) {              \
                diff_sum += ((double)count[move][j] - average_count) * ((double)count[move][j] - average_count);    \
            }                                                   \
            diff_sum /= BUCKET_NUM;                             \
            variance[move][charset_index] = diff_sum;           \
        }                                                       \
    }                                                         \
    /* calculate standard deviation */                          \
    /* calculate standard deviation for different collation */  \
    double stdv_fix_move[BYTE_NUMBER];                        \
    for (int move = 0; move < BYTE_NUMBER; move++) {          \
        stdv_fix_move[move] = 0;                                \
        for (int i = 0; i < CHARSET_NUM; i++) {                 \
            stdv_fix_move[move] += pow(variance[move][i], 0.5); \
        }                                                       \
        stdv_fix_move[move] /= CHARSET_NUM;                  \
        OB_LOG(WARN, "fix right move, average stdv:", K(move), K(stdv_fix_move[move]));  \
    }                                                         \
    double stdv_avg = 0;                                        \
    for (int i = 0; i < BYTE_NUMBER; i++) {                     \
        stdv_avg += stdv_fix_move[i];                           \
    }                                                           \
    stdv_avg /= BYTE_NUMBER;                                    \
    OB_LOG(WARN, "average stdv:", K(stdv_avg));               \
}

#define DEF_NUMBER_CONFLICT(ALGO, HASH_ALGO)            \
TEST (ObHashFunc, hash_number_conflict_##ALGO)             \
{                                                           \
    const int64_t MAX_TEST_COUNT = 100000000; /* 10^8 */     \
    OB_LOG(WARN, "\n\nTest conflict ", #ALGO, " hash");          \
    std::set<int64_t> hash_set;                                  \
    const int64_t MAX_BUF_SIZE = 256;                                       \
    char buf_alloc[MAX_BUF_SIZE];                                           \
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);                        \
    number::ObNumber num;                                                   \
    ObObj obj;                                                              \
    uint64_t value = 0;                                 \
    for (int64_t i = 0; i < MAX_TEST_COUNT; i++) {      \
        EXPECT_EQ(OB_SUCCESS, num.from(i, allocator));  \
        obj.set_number(num);                            \
        value = obj.HASH_ALGO();                      \
        hash_set.insert(value);                         \
        allocator.free();                               \
    }                                                   \
    int64_t conflict_count = MAX_TEST_COUNT - hash_set.size();              \
    OB_LOG(WARN, "record conflict times:", K(MAX_TEST_COUNT), K(conflict_count));   \
}

#define DEF_VARCHAR_WORDS_CONFLICT(ALGO, HASH_ALGO)            \
TEST(ObHashFuncConflict, varchar_words_conflict_##ALGO)                \
{                                                           \
    OB_LOG(WARN, "\n\nTest varchar words conflict ", #ALGO, " hash");      \
    uint64_t value;                                         \
    for (int charset_index = 0; charset_index < BIN_CHARSET_NUM; charset_index++) { \
        std::ifstream words("/home/dachuan.sdc/words");       \
        int64_t data_count = 0;                             \
        ObObj obj;                                          \
        ObString ob_str;                                    \
        std::string str;                                    \
        const char* ptr = NULL;                             \
        ObCollationType cs_type = bin_collation_type[charset_index];    \
        std::set<int64_t> hash_set;                                  \
        /* calculate count of data in every bucket */       \
        while(!words.eof()) {                               \
            data_count++;                                   \
            words >> str;                                   \
            ptr = str.c_str();                              \
            ob_str = ObString::make_string(ptr);            \
            obj.set_varchar(ob_str);                        \
            value = obj.HASH_ALGO(cs_type, 0);              \
						if (hash_set.find(value) != hash_set.end()) {   \
						  OB_LOG(WARN, "conflict word", K(data_count), K(ob_str), K(value));	\
            }                                               \
						hash_set.insert(value);                         \
        }                                                   \
        int64_t conflict_count = data_count - hash_set.size();              \
        words.close();                                      \
        OB_LOG(WARN, "record conflict times:", K(charset_index), K(data_count), K(conflict_count));   \
    }                                                       \
}

#define DEF_NUMBER_TEST(ALGO, HASH_ALGO)            \
    DEF_NUMBER_STDV_FULL64(ALGO, HASH_ALGO)         \
    DEF_NUMBER_STDV_SINGLE_BYTE(ALGO, HASH_ALGO)    \
    DEF_NUMBER_TIME(ALGO, HASH_ALGO)                \
    DEF_NUMBER_STDV_DIFF_SEED(ALGO, HASH_ALGO)      \
    DEF_NUMBER_CONFLICT(ALGO, HASH_ALGO)

#define DEF_VARCHAR_TEST(ALGO, HASH_ALGO)                   \
    DEF_VARCHAR_STDV(ALGO, HASH_ALGO)                       \
    DEF_VARCHAR_WORDS_STDV(ALGO, HASH_ALGO)                 \
    DEF_VARCHAR_TIME(ALGO, HASH_ALGO)                       \
    DEF_VARCHAR_SAME_TAIL(ALGO, HASH_ALGO)                  \
    DEF_VARCHAR_LONG_TEXT_STDV(ALGO, HASH_ALGO)             \
    DEF_VARCHAR_LONG_TEXT_TIME(ALGO, HASH_ALGO, 128, 10000000)  \
    DEF_VARCHAR_LONG_TEXT_TIME(ALGO, HASH_ALGO, 2048, 100000)  \
    DEF_VARCHAR_STDV_SINGLE_BYTE(ALGO, HASH_ALGO)           \
    DEF_VARCHAR_WORDS_CONFLICT(ALGO, HASH_ALGO)

DEF_NUMBER_TEST(xx, hash_xx);
DEF_NUMBER_TEST(murmur, hash);
DEF_NUMBER_TEST(wy, hash_wy);

DEF_VARCHAR_TEST(xx, varchar_xx_hash);
DEF_VARCHAR_TEST(mysql, varchar_hash);
DEF_VARCHAR_TEST(murmur, varchar_murmur_hash);
DEF_VARCHAR_TEST(wy, varchar_wy_hash);
