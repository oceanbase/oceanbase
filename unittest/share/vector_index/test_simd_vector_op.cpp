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
#define private public
#define protected public
#include "share/vector_type/ob_vector_l2_distance.h"
#include "share/vector_type/ob_vector_add.h"
#include "share/vector_type/ob_vector_div.h"
#undef private
#undef protected

#include <iostream>
#include <chrono>

using namespace std;

namespace oceanbase
{
namespace common
{
using namespace std::chrono;
using namespace std::chrono::_V2;
class TestSimdVectorOp : public ::testing::Test
{
public:
  TestSimdVectorOp() {}
  ~TestSimdVectorOp() {}

private:
  ObArenaAllocator allocator_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSimdVectorOp);
};

TEST_F(TestSimdVectorOp, l2_distance)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  float vec1[3] = {0, 1, 2};
  float vec2[3] = {3, 4, 5};
  double square_normal = 0;
  double square_simd = 0;
  ASSERT_EQ(OB_SUCCESS, l2_square_normal(vec1, vec2, 3, square_normal));
  ASSERT_EQ(OB_SUCCESS, ObVectorL2Distance::l2_square_func(vec1, vec2, 3, square_simd));
  ASSERT_EQ(square_normal, square_simd);

  const int VEC_SIZE = 1000;
  const int LOOP_SIZE = 100000;
  float vec3[VEC_SIZE] = {0};
  float vec4[VEC_SIZE] = {0};
  std::srand(static_cast<unsigned int>(std::time(0)));
  long dur_normal = 0;
  long dur_simd = 0;
  for (int j = 0; j < LOOP_SIZE; ++j) {
    for (int i = 0; i < VEC_SIZE; ++i) {
      vec3[i] = static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
      vec4[i] = static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
    }

    system_clock::time_point start_normal = high_resolution_clock::now();
    ASSERT_EQ(OB_SUCCESS, l2_square_normal(vec3, vec4, VEC_SIZE, square_normal));
    dur_normal += duration_cast<nanoseconds>(high_resolution_clock::now() - start_normal).count();

    system_clock::time_point start_simd = high_resolution_clock::now();
    ASSERT_EQ(OB_SUCCESS, ObVectorL2Distance::l2_square_func(vec3, vec4, VEC_SIZE, square_simd));
    dur_simd += duration_cast<nanoseconds>(high_resolution_clock::now() - start_simd).count();
  }

  std::cout << "normal l2 distance: " << dur_normal/1000000.0 << "ms, simd l2 distance: " << dur_simd/1000000.0 << "ms" << std::endl;
  std::cout << "simd is " << double(dur_normal)/double(dur_simd) << "x faster than normal" << std::endl;
}

TEST_F(TestSimdVectorOp, add)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  float vec_normal[3] = {0, 1, 2};
  float vec_simd[3] = {0, 1, 2};
  float vec2[3] = {3, 4, 5};
  ASSERT_EQ(OB_SUCCESS, vector_add_normal(vec_normal, vec2, 3));
  ASSERT_EQ(OB_SUCCESS, ObVectorAdd::calc(vec_simd, vec2, 3));
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(vec_normal[i], vec_simd[i]);
  }

  const int VEC_SIZE = 1000;
  const int LOOP_SIZE = 100000;
  float vec3[VEC_SIZE] = {0};
  float vec_cp[VEC_SIZE] = {0};
  float vec4[VEC_SIZE] = {0};
  std::srand(static_cast<unsigned int>(std::time(0)));
  long dur_normal = 0;
  long dur_simd = 0;
  for (int j = 0; j < LOOP_SIZE; ++j) {
    for (int i = 0; i < VEC_SIZE; ++i) {
      vec3[i] = static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
      vec4[i] = static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
    }
    MEMCPY(vec_cp, vec3, sizeof(float) * VEC_SIZE);

    system_clock::time_point start_normal = high_resolution_clock::now();
    ASSERT_EQ(OB_SUCCESS, vector_add_normal(vec3, vec4, VEC_SIZE));
    dur_normal += duration_cast<nanoseconds>(high_resolution_clock::now() - start_normal).count();

    system_clock::time_point start_simd = high_resolution_clock::now();
    ASSERT_EQ(OB_SUCCESS, ObVectorAdd::calc(vec_cp, vec4, VEC_SIZE));
    dur_simd += duration_cast<nanoseconds>(high_resolution_clock::now() - start_simd).count();
  }

  std::cout << "normal add: " << dur_normal/1000000.0 << "ms, simd add: " << dur_simd/1000000.0 << "ms" << std::endl;
  std::cout << "simd is " << double(dur_normal)/double(dur_simd) << "x faster than normal" << std::endl;
}

TEST_F(TestSimdVectorOp, div)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  float vec_normal[3] = {0, 1, 2};
  float vec_simd[3] = {0, 1, 2};
  float divisor = 2.0;
  ASSERT_EQ(OB_SUCCESS, vector_div_normal(vec_normal, divisor, 3));
  ASSERT_EQ(OB_SUCCESS, ObVectorDiv::calc(vec_simd, divisor, 3));
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(vec_normal[i], vec_simd[i]);
  }

  const int VEC_SIZE = 1000;
  const int LOOP_SIZE = 100000;
  float vec3[VEC_SIZE] = {0};
  float vec_cp[VEC_SIZE] = {0};
  std::srand(static_cast<unsigned int>(std::time(0)));
  long dur_normal = 0;
  long dur_simd = 0;
  for (int j = 0; j < LOOP_SIZE; ++j) {
    for (int i = 0; i < VEC_SIZE; ++i) {
      vec3[i] = static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
    }
    MEMCPY(vec_cp, vec3, sizeof(float) * VEC_SIZE);

    system_clock::time_point start_normal = high_resolution_clock::now();
    ASSERT_EQ(OB_SUCCESS, vector_div_normal(vec3, divisor, VEC_SIZE));
    dur_normal += duration_cast<nanoseconds>(high_resolution_clock::now() - start_normal).count();

    system_clock::time_point start_simd = high_resolution_clock::now();
    ASSERT_EQ(OB_SUCCESS, vector_div_normal(vec_cp, divisor, VEC_SIZE));
    dur_simd += duration_cast<nanoseconds>(high_resolution_clock::now() - start_simd).count();
  }

  std::cout << "normal div: " << dur_normal/1000000.0 << "ms, simd div: " << dur_simd/1000000.0 << "ms" << std::endl;
  std::cout << "simd is " << double(dur_normal)/double(dur_simd) << "x faster than normal" << std::endl;
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  // system("rm -f test_array_meta.log");
  // OB_LOGGER.set_file_name("test_array_meta.log");
  // OB_LOGGER.set_log_level("DEBUG");
  return RUN_ALL_TESTS();
}