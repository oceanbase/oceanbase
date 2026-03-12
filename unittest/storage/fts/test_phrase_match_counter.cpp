/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "lib/allocator/page_arena.h"
#include "storage/retrieval/ob_phrase_match_counter.h"

#include <gtest/gtest.h>
#include <vector>

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::plugin;

namespace oceanbase
{
namespace storage
{

class PhraseMatchCounterTest : public ::testing::Test
{
protected:
  PhraseMatchCounterTest() {}
  virtual ~PhraseMatchCounterTest() {}

  static void SetUpTestCase()
  {
    LOG_INFO("SetUpTestCase");
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    LOG_INFO("TearDownTestCase");
    MockTenantModuleEnv::get_instance().destroy();
  }

  virtual void SetUp() {}
  virtual void TearDown() {}

  static constexpr double epsilon = 0.00001;

  static int single_test(
      const std::vector<int64_t> &id_vec,
      const std::vector<int64_t> &offset_vec,
      const std::vector<std::vector<int64_t>> &abs_pos_list_vec,
      const int64_t slop,
      const bool has_duplicate_tokens,
      double &total_match_count)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator(ObModIds::TEST);
    ObArray<int64_t> ids;
    ObArray<int64_t> offsets;
    ObArray<ObArray<int64_t>> abs_pos_lists;
    for (int64_t i = 0; OB_SUCC(ret) && i < id_vec.size(); ++i) {
      if (OB_FAIL(ids.push_back(id_vec[i]))) {
        LOG_WARN("failed to push back id", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < offset_vec.size(); ++i) {
      if (OB_FAIL(offsets.push_back(offset_vec[i]))) {
        LOG_WARN("failed to push back offset", K(ret));
      }
    }
    if (FAILEDx(abs_pos_lists.prepare_allocate(abs_pos_list_vec.size()))) {
      LOG_WARN("failed to prepare allocate abs pos lists", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < abs_pos_list_vec.size(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < abs_pos_list_vec[i].size(); ++j) {
        if (OB_FAIL(abs_pos_lists.at(i).push_back(abs_pos_list_vec[i][j]))) {
          LOG_WARN("failed to push back pos", K(ret));
        }
      }
    }
    ObPhraseMatchCounter *counter = nullptr;
    if (FAILEDx(ObPhraseMatchCounter::create(allocator, ids, offsets,
                                             slop, has_duplicate_tokens, counter))) {
      LOG_WARN("failed to create counter", K(ret));
    } else if (OB_FAIL(counter->count_matches(abs_pos_lists, total_match_count))) {
      LOG_WARN("failed to count matches", K(ret));
    } else {
      counter->reset();
    }
    return ret;
  }
};

TEST(PhraseMatchCounterTest, test_error_handling)
{
  int ret = OB_SUCCESS;
  {
    std::vector<int64_t> id_vec = {0};
    std::vector<int64_t> offset_vec = {0};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 6, 11, 13, 16, 19, 24, 26, 29, 31, 39, 41, 44, 46, 48, 53, 55, 57, 59, 61, 66, 67, 69, 72, 75, 76, 80, 82, 84, 89, 91},
      {3, 5, 9, 10, 12, 17, 21, 25, 30, 37, 42, 45, 47, 52, 54, 58, 62, 70, 77, 79, 83, 86, 90},
      {1, 4, 7, 8, 14, 15, 20, 22, 23, 27, 28, 32, 33, 35, 40, 49, 56, 63, 64, 71, 74, 78, 85, 87, 88},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 0, false, total_match_count);
    ASSERT_EQ(ret, OB_ERR_UNEXPECTED);
  }

  {
    std::vector<int64_t> id_vec = {0, 1, 0, 2};
    std::vector<int64_t> offset_vec = {0, 1, 2};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 6, 11, 13, 16, 19, 24, 26, 29, 31, 39, 41, 44, 46, 48, 53, 55, 57, 59, 61, 66, 67, 69, 72, 75, 76, 80, 82, 84, 89, 91},
      {3, 5, 9, 10, 12, 17, 21, 25, 30, 37, 42, 45, 47, 52, 54, 58, 62, 70, 77, 79, 83, 86, 90},
      {1, 4, 7, 8, 14, 15, 20, 22, 23, 27, 28, 32, 33, 35, 40, 49, 56, 63, 64, 71, 74, 78, 85, 87, 88},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 5, false, total_match_count);
    ASSERT_EQ(ret, OB_ERR_UNEXPECTED);
  }

  {
    std::vector<int64_t> id_vec = {0, 1, 0, 2};
    std::vector<int64_t> offset_vec = {0, 1, 2, 3};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 6, 11, 13, 16, 19, 24, 26, 29, 31, 39, 41, 44, 46, 48, 53, 55, 57, 59, 61, 66, 67, 69, 72, 75, 76, 80, 82, 84, 89, 91},
      {3, 5, 9, 10, 12, 17, 21, 25, 30, 37, 42, 45, 47, 52, 54, 58, 62, 70, 77, 79, 83, 86, 90},
      {1, 4, 7, 8, 14, 15, 20, 22, 23, 27, 28, 32, 33, 35, 40, 49, 56, 63, 64, 71, 74, 78, 85, 87, 88},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, -1, false, total_match_count);
    ASSERT_EQ(ret, OB_ERR_UNEXPECTED);
  }

  {
    std::vector<int64_t> id_vec = {0, 2};
    std::vector<int64_t> offset_vec = {0, 1};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 6, 11, 13, 16, 19, 24, 26, 29, 31, 39, 41, 44, 46, 48, 53, 55, 57, 59, 61, 66, 67, 69, 72, 75, 76, 80, 82, 84, 89, 91},
      {3, 5, 9, 10, 12, 17, 21, 25, 30, 37, 42, 45, 47, 52, 54, 58, 62, 70, 77, 79, 83, 86, 90},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 0, false, total_match_count);
    ASSERT_EQ(ret, OB_ERR_UNEXPECTED);
  }
}

TEST(PhraseMatchCounterTest, test_exact)
{
  int ret = OB_SUCCESS;
  {
    std::vector<int64_t> id_vec = {0, 1};
    std::vector<int64_t> offset_vec = {0, 1};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {0, 18, 34, 36, 38, 43, 50, 51, 60, 65, 68, 73, 81, 92},
      {},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 0, false, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, 0.0);
  }

  {
    std::vector<int64_t> id_vec = {0, 1, 0};
    std::vector<int64_t> offset_vec = {0, 1, 2};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {0, 18, 34, 36, 38, 43, 50, 51, 60, 65, 68, 73, 81, 92},
      {2, 6, 11, 13, 16, 19, 24, 26, 29, 31, 39, 41, 44, 46, 48, 53, 55, 57, 59, 61, 66, 67, 69, 72, 75, 76, 80, 82, 84, 89, 91},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 0, false, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, 0.0);
  }

  {
    std::vector<int64_t> id_vec = {0, 1, 2};
    std::vector<int64_t> offset_vec = {0, 1, 2};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 6, 11, 13, 16, 19, 24, 26, 29, 31, 39, 41, 44, 46, 48, 53, 55, 57, 59, 61, 66, 67, 69, 72, 75, 76, 80, 82, 84, 89, 91},
      {3, 5, 9, 10, 12, 17, 21, 25, 30, 37, 42, 45, 47, 52, 54, 58, 62, 70, 77, 79, 83, 86, 90},
      {1, 4, 7, 8, 14, 15, 20, 22, 23, 27, 28, 32, 33, 35, 40, 49, 56, 63, 64, 71, 74, 78, 85, 87, 88},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 0, false, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, 4.0);
  }

  {
    std::vector<int64_t> id_vec = {0, 1, 0, 2};
    std::vector<int64_t> offset_vec = {0, 1, 2, 3};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 6, 11, 13, 16, 19, 24, 26, 29, 31, 39, 41, 44, 46, 48, 53, 55, 57, 59, 61, 66, 67, 69, 72, 75, 76, 80, 82, 84, 89, 91},
      {3, 5, 9, 10, 12, 17, 21, 25, 30, 37, 42, 45, 47, 52, 54, 58, 62, 70, 77, 79, 83, 86, 90},
      {1, 4, 7, 8, 14, 15, 20, 22, 23, 27, 28, 32, 33, 35, 40, 49, 56, 63, 64, 71, 74, 78, 85, 87, 88},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 0, false, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, 6.0);
  }

  {
    std::vector<int64_t> id_vec = {0, 1, 2};
    std::vector<int64_t> offset_vec = {0, 1, 3};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 6, 11, 13, 16, 19, 24, 26, 29, 31, 39, 41, 44, 46, 48, 53, 55, 57, 59, 61, 66, 67, 69, 72, 75, 76, 80, 82, 84, 89, 91},
      {3, 5, 9, 10, 12, 17, 21, 25, 30, 37, 42, 45, 47, 52, 54, 58, 62, 70, 77, 79, 83, 86, 90},
      {1, 4, 7, 8, 14, 15, 20, 22, 23, 27, 28, 32, 33, 35, 40, 49, 56, 63, 64, 71, 74, 78, 85, 87, 88},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 0, false, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, 7.0);
  }

  {
    std::vector<int64_t> id_vec = {0, 0, 0};
    std::vector<int64_t> offset_vec = {0, 1, 3};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 6, 11, 13, 16, 19, 24, 26, 29, 31, 39, 41, 44, 46, 48, 53, 55, 57, 59, 61, 66, 67, 69, 72, 75, 76, 80, 82, 84, 89, 91},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 0, false, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, 1.0);
  }
}

TEST(PhraseMatchCounterTest, test_sloppy_with_unique_tokens)
{
  int ret = OB_SUCCESS;
  {
    // query: "a b c" ~ 2
    // doc: d c a d b c b a c c b b a b a c c a b d a c b c c a b a c c a c c d c d b d a c a b d a b a b a c d d b a b a c a b a d a b c c d a a d a b c a d c a a b c b a d a b a c b c c a b a d
    std::vector<int64_t> id_vec = {0, 1, 2};
    std::vector<int64_t> offset_vec = {0, 1, 2};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 7, 12, 14, 17, 20, 25, 27, 30, 38, 40, 43, 45, 47, 52, 54, 56, 58, 60, 65, 66, 68, 71, 74, 75, 79, 81, 83, 88, 90},
      {4, 6, 10, 11, 13, 18, 22, 26, 36, 41, 44, 46, 51, 53, 57, 61, 69, 76, 78, 82, 85, 89},
      {1, 5, 8, 9, 15, 16, 21, 23, 24, 28, 29, 31, 32, 34, 39, 48, 55, 62, 63, 70, 73, 77, 84, 86, 87},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 2, false, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_LT(total_match_count, 10.83333 + PhraseMatchCounterTest::epsilon);
    ASSERT_GT(total_match_count, 10.83333 - PhraseMatchCounterTest::epsilon);

    double total_match_count_2 = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 2, true, total_match_count_2);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, total_match_count_2);
  }

  {
    // query: "a b * c" ~ 2
    // doc: d c a d b c b a c c b b a b a c c a b d a c b c c a b a c c a c c d c d b d a c a b d a b a b a c d d b a b a c a b a d a b c c d a a d a b c a d c a a b c b a d a b a c b c c a b a d
    std::vector<int64_t> id_vec = {0, 1, 2};
    std::vector<int64_t> offset_vec = {0, 1, 3};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 7, 12, 14, 17, 20, 25, 27, 30, 38, 40, 43, 45, 47, 52, 54, 56, 58, 60, 65, 66, 68, 71, 74, 75, 79, 81, 83, 88, 90},
      {4, 6, 10, 11, 13, 18, 22, 26, 36, 41, 44, 46, 51, 53, 57, 61, 69, 76, 78, 82, 85, 89},
      {1, 5, 8, 9, 15, 16, 21, 23, 24, 28, 29, 31, 32, 34, 39, 48, 55, 62, 63, 70, 73, 77, 84, 86, 87},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 2, false, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_LT(total_match_count, 15.5 + PhraseMatchCounterTest::epsilon);
    ASSERT_GT(total_match_count, 15.5 - PhraseMatchCounterTest::epsilon);

    double total_match_count_2 = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 2, true, total_match_count_2);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, total_match_count_2);
  }

  {
    // query: "a b c d" ~ 5
    // doc: d c a d b c b a c c b b a b a c c a b d a c b c c a b a c c a c c d c d b d a c a b d a b a b a c d d b a b a c a b a d a b c c d a a d a b c a d c a a b c b a d a b a c b c c a b a d
    std::vector<int64_t> id_vec = {0, 1, 2, 3};
    std::vector<int64_t> offset_vec = {0, 1, 2, 3};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 7, 12, 14, 17, 20, 25, 27, 30, 38, 40, 43, 45, 47, 52, 54, 56, 58, 60, 65, 66, 68, 71, 74, 75, 79, 81, 83, 88, 90},
      {4, 6, 10, 11, 13, 18, 22, 26, 36, 41, 44, 46, 51, 53, 57, 61, 69, 76, 78, 82, 85, 89},
      {1, 5, 8, 9, 15, 16, 21, 23, 24, 28, 29, 31, 32, 34, 39, 48, 55, 62, 63, 70, 73, 77, 84, 86, 87},
      {0, 3, 19, 33, 35, 37, 42, 49, 50, 59, 64, 67, 72, 80, 91},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 5, false, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_LT(total_match_count, 10.4 + PhraseMatchCounterTest::epsilon);
    ASSERT_GT(total_match_count, 10.4 - PhraseMatchCounterTest::epsilon);

    double total_match_count_2 = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 5, true, total_match_count_2);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, total_match_count_2);
  }

  {
    // query: "b c d" ~ 8
    // doc: g d c g b e c d b g f c e a c f d g a a a e a c c b d b e c c f g d c d g g f c d d g f c b a b c g d g g f e e f a b b f d g a b f c d b f e c d a
    std::vector<int64_t> id_vec = {0, 1, 2};
    std::vector<int64_t> offset_vec = {0, 1, 2};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {4, 8, 25, 27, 45, 47, 58, 59, 64, 68},
      {2, 6, 11, 14, 23, 24, 29, 30, 34, 39, 44, 48, 66, 71},
      {1, 7, 16, 26, 33, 35, 40, 41, 50, 61, 67, 72},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 8, false, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_LT(total_match_count, 5.52262 + PhraseMatchCounterTest::epsilon);
    ASSERT_GT(total_match_count, 5.52262 - PhraseMatchCounterTest::epsilon);

    double total_match_count_2 = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 8, true, total_match_count_2);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, total_match_count_2);
  }
}

TEST(PhraseMatchCounterTest, test_sloppy_with_duplicate_tokens)
{
  int ret = OB_SUCCESS;
  {
    // query: "a b a" ~ 2
    // doc: b c b a b c b
    std::vector<int64_t> id_vec = {0, 1, 0};
    std::vector<int64_t> offset_vec = {0, 1, 2};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {3},
      {0, 2, 4, 6},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 2, true, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, 0.0);
  }

  {
    // query: "a b d a" ~ 2
    // doc: e d a d c a b g e g f d a d a g c f e a a e a g a g e b e d a d a g d f e d a d g c c f e g g a b f e g g f g c b d f a b d f d f a b a g c d c a c c d a c b c e b a e c d b g g c g c a c f b f e e e f d c d c g f e c g d d c e d c d e d b f f f b a d f g c b c c g b f e e g c a g e f e f b e d e d a f e e
    std::vector<int64_t> id_vec = {0, 1, 2, 0};
    std::vector<int64_t> offset_vec = {0, 1, 2, 3};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {2, 5, 12, 14, 19, 20, 22, 24, 30, 32, 38, 47, 59, 65, 67, 72, 76, 82, 92, 124, 139, 150},
      {6, 27, 48, 56, 60, 66, 78, 81, 86, 95, 119, 123, 129, 133, 145},
      {1, 3, 11, 13, 29, 31, 34, 37, 39, 57, 61, 63, 70, 75, 85, 101, 103, 110, 111, 114, 116, 118, 125, 147, 149},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 2, true, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(total_match_count, 0.0);
  }

  {
    // query: "g b b" ~ 4
    // doc: c a b g c a b b f e f g b g g c f a c c f g b e g a g b e a e a a a c c g g b b f e a g b d f b e b g c c a a a b g d a g e e c e a d d d b d b e b
    std::vector<int64_t> id_vec = {0, 1, 1};
    std::vector<int64_t> offset_vec = {0, 1, 2};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {3, 11, 13, 14, 21, 24, 26, 36, 37, 43, 50, 57, 60},
      {2, 6, 7, 12, 22, 27, 38, 39, 44, 47, 49, 56, 69, 71, 73},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 4, true, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_LT(total_match_count, 3.01667 + PhraseMatchCounterTest::epsilon);
    ASSERT_GT(total_match_count, 3.01667 - PhraseMatchCounterTest::epsilon);
  }

  {
    // query: "b e c e c f c" ~ 9
    // doc: f f g b c g e g f g a d e d a e f e f b a c b g f b c d e g d b b a f g f g c f d a a g a c a a b f f c c f d a c a c c b d b f e a a e b b c b a g g b a a f b e f c b d e b e c c b c f a b f f g d b g b d b g d a g g f e f f d a c f g d d d e b e a a e g f b a b b b e d
    std::vector<int64_t> id_vec = {0, 1, 2, 1, 2, 3, 2};
    std::vector<int64_t> offset_vec = {0, 1, 2, 3, 4, 5, 6};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {3, 19, 22, 25, 31, 32, 48, 60, 62, 68, 69, 71, 75, 79, 83, 86, 90, 94, 99, 101, 103, 122, 129, 131, 132, 133},
      {6, 12, 15, 17, 28, 64, 67, 80, 85, 87, 110, 121, 123, 126, 134},
      {4, 21, 26, 38, 45, 51, 52, 56, 58, 59, 70, 82, 88, 89, 91, 115},
      {0, 1, 8, 16, 18, 24, 34, 36, 39, 49, 50, 53, 63, 78, 81, 92, 95, 96, 109, 111, 112, 116, 128},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 9, true, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_LT(total_match_count, 1.10833 + PhraseMatchCounterTest::epsilon);
    ASSERT_GT(total_match_count, 1.10833 - PhraseMatchCounterTest::epsilon);
  }

  {
    // query: "f a e g c g" ~ 10
    // doc: d f g d e g d g a c e f c f a d c a f b e f a c a a c c e d e e g b d b b a e d d c b d f g f f f a f e a a a d f b e f a f f d b b c d f g d f e a a a d d f a a g f d c d a g f f g f f b c c e b g a a b e a c e c a a e a d e a a g d d g d f g g d b e c d d e d b f b g g c a e c a g d b g d e c b a f g b d e d a a e a g g b b c f c b b d e a
    std::vector<int64_t> id_vec = {0, 1, 2, 3, 4, 3};
    std::vector<int64_t> offset_vec = {0, 1, 2, 3, 4, 5};
    std::vector<std::vector<int64_t>> abs_pos_list_vec = {
      {1, 11, 13, 18, 21, 44, 46, 47, 48, 50, 56, 59, 61, 62, 68, 71, 78, 82, 88, 89, 91, 92, 120, 132, 150, 165},
      {8, 14, 17, 22, 24, 25, 37, 49, 52, 53, 54, 60, 73, 74, 75, 79, 80, 86, 99, 100, 103, 107, 108, 110, 113, 114, 137, 140, 149, 156, 157, 159, 171},
      {4, 10, 20, 28, 30, 31, 38, 51, 58, 72, 96, 102, 105, 109, 112, 125, 129, 138, 146, 154, 158, 170},
      {2, 5, 7, 32, 45, 69, 81, 87, 90, 98, 115, 118, 121, 122, 134, 135, 141, 144, 151, 160, 161},
      {9, 12, 16, 23, 26, 27, 41, 66, 84, 94, 95, 104, 106, 126, 136, 139, 147, 164, 166},
    };
    double total_match_count = 0;
    ret = PhraseMatchCounterTest::single_test(id_vec, offset_vec, abs_pos_list_vec, 10, true, total_match_count);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_LT(total_match_count, 1.95213 + PhraseMatchCounterTest::epsilon);
    ASSERT_GT(total_match_count, 1.95213 - PhraseMatchCounterTest::epsilon);
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  // system("rm -rf test_phrase_match_counter.log");
  // OB_LOGGER.set_file_name("test_phrase_match_counter.log", true);
  // OB_LOGGER.set_log_level("DEBUG");
  // oceanbase::storage::ObTestFTPluginHelper::file_name = argv[0];
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
