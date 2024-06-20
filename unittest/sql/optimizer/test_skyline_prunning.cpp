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
#include <assert.h>
#include "sql/optimizer/ob_skyline_prunning.h"
#include "lib/allocator/page_arena.h"


using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace test
{

class ObSkylinePrunningTest: public ::testing::Test
{
public:
  ObSkylinePrunningTest() {}
  virtual ~ObSkylinePrunningTest() {}
  virtual void SetUp();
  virtual void TearDown();
  static void to_array(const uint64_t *columns, const int64_t column_cnt, ObIArray<uint64_t> &column_ids);
  static void dummy_const_column_info(const int64_t column_cnt, ObIArray<bool> &column_const_info);
  template<typename T>
  static void check(const T *left,const T *right,
                    const ObSkylineDim::CompareStat status,
                    const ObSkylineDim::CompareStat reverse_status);

  void check_index_back_dim(const bool left_index_back,
                            const bool left_has_interesting_order,
                            const bool left_can_extract_range,
                            const int64_t left_index_column_cnt,
                            const uint64_t *left, const int64_t left_cnt,
                            const bool right_index_back,
                            const bool right_has_interesting_order,
                            const bool right_can_extract_range,
                            const int64_t right_index_column_cnt,
                            const uint64_t *right, const int64_t right_cnt,
                            const ObSkylineDim::CompareStat status,
                            const ObSkylineDim::CompareStat reverse_status);
  static void check_interest_dim(const uint64_t *left, const int64_t left_cnt,
                                 const uint64_t *right, const int64_t right_cnt,
                                 const ObSkylineDim::CompareStat status,
                                 const ObSkylineDim::CompareStat reverse_status);
  static void check_query_range_dim(const uint64_t *left, const int64_t left_cnt,
                                    const uint64_t *right, const int64_t right_cnt,
                                    const ObSkylineDim::CompareStat status,
                                    const ObSkylineDim::CompareStat reverse_status);
  ObIndexSkylineDim *create_skyline_index_dim(
      const bool index_back,
      const int64_t index_column_cnt,
      const uint64_t *filter_ids, const int64_t filter_cnt,
      const uint64_t *interest_ids, const int64_t interest_cnt,
      const uint64_t *range_ids, const int64_t range_cnt);
protected:
  ObArenaAllocator allocator_;
};

void ObSkylinePrunningTest::SetUp()
{
}

void ObSkylinePrunningTest::TearDown()
{
}

void ObSkylinePrunningTest::to_array(const uint64_t *columns,
                                     const int64_t column_cnt,
                                     ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < column_cnt; ++i) {
    ret = column_ids.push_back(columns[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }
}

void ObSkylinePrunningTest::dummy_const_column_info(const int64_t column_cnt,
                                                    ObIArray<bool> &column_const_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < column_cnt; i++) {
    ret = column_const_info.push_back(false);
    ASSERT_EQ(ret, OB_SUCCESS);
  }
}


ObIndexSkylineDim *ObSkylinePrunningTest::create_skyline_index_dim(
    const bool index_back,
    const int64_t index_column_cnt,
    const uint64_t *filter_ids, const int64_t filter_cnt,
    const uint64_t *interest_ids, const int64_t interest_cnt,
    const uint64_t *range_ids, const int64_t range_cnt)
{
  ObIndexSkylineDim *t = NULL;
  ObSkylineDimFactory::get_instance().create_skyline_dim<ObIndexSkylineDim>(allocator_, t);
  ObArray<uint64_t> filter_array;
  to_array(filter_ids, filter_cnt, filter_array);
  t->add_index_back_dim(index_back,
                        interest_cnt > 0,
                        range_cnt > 0,
                        index_column_cnt,
                        filter_array,
                        allocator_);
  ObArray<uint64_t> interest_array;
  ObArray<uint64_t> rowkey_array;
  ObArray<bool> const_column_info;
  to_array(interest_ids, interest_cnt, interest_array);
  to_array(range_ids, range_cnt, rowkey_array);
  dummy_const_column_info(interest_cnt, const_column_info);
  t->add_interesting_order_dim(index_back, range_cnt > 0, filter_array, interest_array, const_column_info, allocator_);
  t->add_query_range_dim(rowkey_array, allocator_, false);
  return t;
}

template<typename T>
void ObSkylinePrunningTest::check(const T *left,const T *right,
                                  const ObSkylineDim::CompareStat status,
                                  const ObSkylineDim::CompareStat reverse_status)
{
  ObSkylineDim::CompareStat tmp_status;
  int ret = left->compare(*right, tmp_status);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(status, tmp_status);
  SHARE_SCHEMA_LOG(WARN, "check failed", K(status), K(tmp_status), K(*left), K(*right));
  if (status != tmp_status) {
    SHARE_SCHEMA_LOG(WARN, "check failed", K(status), K(tmp_status), K(*left), K(*right));
  }
  ret = right->compare(*left, tmp_status);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(reverse_status, tmp_status);
  SHARE_SCHEMA_LOG(WARN, "check failed", K(reverse_status), K(tmp_status), K(*left), K(*right));
  if (reverse_status != tmp_status) {
    SHARE_SCHEMA_LOG(WARN, "check failed", K(reverse_status), K(tmp_status), K(*left), K(*right));
  }
}

void ObSkylinePrunningTest::check_index_back_dim(const bool left_index_back,
                                                 const bool left_has_interesting_order,
                                                 const bool left_can_extract_range,
                                                 const int64_t left_index_column_cnt,
                                                 const uint64_t *left, const int64_t left_cnt,
                                                 const bool right_index_back,
                                                 const bool right_has_interesting_order,
                                                 const bool right_can_extract_range,
                                                 const int64_t right_index_column_cnt,
                                                 const uint64_t *right, const int64_t right_cnt,
                                                 const ObSkylineDim::CompareStat status,
                                                 const ObSkylineDim::CompareStat reverse_status)
{
  ObIndexBackDim left_dim;
  left_dim.set_index_back(left_index_back);
  left_dim.set_interesting_order(left_has_interesting_order);
  left_dim.set_extract_range(left_can_extract_range);
  left_dim.set_index_column_cnt(left_index_column_cnt);
  if (left_cnt > 0) {
    ObArray<uint64_t> left_ids;
    to_array(left, left_cnt, left_ids);
    left_dim.add_filter_column_ids(left_ids);
  }

  ObIndexBackDim right_dim;
  right_dim.set_index_back(right_index_back);
  right_dim.set_interesting_order(right_has_interesting_order);
  right_dim.set_extract_range(right_can_extract_range);
  right_dim.set_index_column_cnt(right_index_column_cnt);
  if (right_cnt > 0) {
    ObArray<uint64_t> right_ids;
    to_array(right, right_cnt, right_ids);
    right_dim.add_filter_column_ids(right_ids);
  }
  check(&left_dim, &right_dim, status, reverse_status);
}

void ObSkylinePrunningTest::check_interest_dim(const uint64_t *left, const int64_t left_cnt,
                                               const uint64_t *right, const int64_t right_cnt,
                                               const ObSkylineDim::CompareStat status,
                                               const ObSkylineDim::CompareStat reverse_status)
{
  ObArray<uint64_t> left_ids;
  ObArray<uint64_t> right_ids;
  ObArray<bool> left_const_column_info;
  ObArray<bool> right_const_column_info;
  to_array(left, left_cnt, left_ids);
  to_array(right, right_cnt, right_ids);
  dummy_const_column_info(left_cnt, left_const_column_info);
  dummy_const_column_info(right_cnt, right_const_column_info);

  ObInterestOrderDim left_dim;
  if (left_cnt > 0) {
    left_dim.set_intereting_order(true);
    left_dim.add_interest_prefix_ids(left_ids);
    left_dim.add_const_column_info(left_const_column_info);
  } else {
    left_dim.set_intereting_order(false);
  }

  ObInterestOrderDim right_dim;
  if (right_cnt > 0) {
    right_dim.set_intereting_order(true);
    right_dim.add_interest_prefix_ids(right_ids);
    right_dim.add_const_column_info(right_const_column_info);
  } else {
    right_dim.set_intereting_order(false);
  }
  check(&left_dim, &right_dim, status, reverse_status);
}

void ObSkylinePrunningTest::check_query_range_dim(const uint64_t *left, const int64_t left_cnt,
                                                  const uint64_t *right, const int64_t right_cnt,
                                                  const ObSkylineDim::CompareStat status,
                                                  const ObSkylineDim::CompareStat reverse_status)
{
  ObArray<uint64_t> left_ids;
  ObArray<uint64_t> right_ids;
  to_array(left, left_cnt, left_ids);
  to_array(right, right_cnt, right_ids);

  ObQueryRangeDim left_dim;
  if (left_cnt > 0) {
    left_dim.add_rowkey_ids(left_ids);
  }

  ObQueryRangeDim right_dim;
  if (right_cnt > 0) {
    right_dim.add_rowkey_ids(right_ids);
  }
  check(&left_dim, &right_dim, status, reverse_status);
}

TEST_F(ObSkylinePrunningTest, basic)
{
  int ret = OB_SUCCESS;
  ObIndexBackDim *index_dim = NULL;
  ret = ObSkylineDimFactory::get_instance()
      .create_skyline_dim<ObIndexBackDim>(allocator_, index_dim);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObQueryRangeDim *range_dim = NULL;
  ret = ObSkylineDimFactory::get_instance()
      .create_skyline_dim<ObQueryRangeDim>(allocator_, range_dim);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObInterestOrderDim *interest_dim = NULL;
  ret = ObSkylineDimFactory::get_instance()
    .create_skyline_dim<ObInterestOrderDim>(allocator_, interest_dim);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObIndexSkylineDim *index_skyline_dim = NULL;
  ret = ObSkylineDimFactory::get_instance()
    .create_skyline_dim<ObIndexSkylineDim>(allocator_, index_skyline_dim);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObSkylinePrunningTest, index_dim)
{
  {
    ObIndexBackDim dim_true;
    dim_true.set_index_back(true);
    ObIndexBackDim dim_false;
    dim_false.set_index_back(false);
    ObIndexBackDim dim2_true;
    dim2_true.set_index_back(true);
    ObIndexBackDim dim2_false;
    dim2_false.set_index_back(false);

//    check(&dim_true, &dim_false, ObSkylineDim::RIGHT_DOMINATED, ObSkylineDim::LEFT_DOMINATED);
    check(&dim_true, &dim2_true, ObSkylineDim::EQUAL, ObSkylineDim::EQUAL);
    check(&dim_false, &dim2_false, ObSkylineDim::EQUAL, ObSkylineDim::EQUAL);
  }

  {
    //left (not index back), right (index back)
    //left_filter_columns = right_filter_columns
    //left_index_column_cnt = right_index_column_cnt
    //left dominate right
    int64_t left_index_column_cnt = 2;
    uint64_t left_filter_columns[] = { 17, 18};
    int64_t left_filter_column_cnt = sizeof (left_filter_columns) / sizeof(uint64_t);

    int64_t right_index_column_cnt = 2;
    uint64_t right_filter_columns[] = {17, 18};
    int64_t right_filter_column_cnt = sizeof (right_filter_columns) / sizeof(uint64_t);
    check_index_back_dim(false,     //index_back
                         false,     //interesting order
                         false,     //query range
                         left_index_column_cnt,
                         left_filter_columns,
                         left_filter_column_cnt,
                         true,      //index_back
                         false,     //interesting order
                         false,     //query range
                         right_index_column_cnt,
                         right_filter_columns,
                         right_filter_column_cnt,
                         ObSkylineDim::LEFT_DOMINATED,
                         ObSkylineDim::RIGHT_DOMINATED);
  }
  {
    //left (not index back), right (index back)
    //left_filter_columns = right_filter_columns
    //but left_index_column_cnt < right_index_column_cnt
    //left dominate_right
    int64_t left_index_column_cnt = 2;
    uint64_t left_filter_columns[] = { 17, 18};
    int64_t left_filter_column_cnt = sizeof (left_filter_columns) / sizeof(uint64_t);

    int64_t right_index_column_cnt = 5;
    uint64_t right_filter_columns[] = {17, 18};
    int64_t right_filter_column_cnt = sizeof (right_filter_columns) / sizeof(uint64_t);
    check_index_back_dim(false,     //index_back
                         false,     //interesting order
                         false,     //query range
                         left_index_column_cnt,
                         left_filter_columns,
                         left_filter_column_cnt,
                         true,      //index_back
                         false,     //interesting order
                         false,     //query range
                         right_index_column_cnt,
                         right_filter_columns,
                         right_filter_column_cnt,
                         ObSkylineDim::LEFT_DOMINATED,
                         ObSkylineDim::RIGHT_DOMINATED);
  }
  {
    //left (not index back), right (index back)
    //left_filter_columns = right_filter_columns
    //but left_index_column_cnt > right_index_column_cnt
    //left dominate_right
    int64_t left_index_column_cnt = 6;
    uint64_t left_filter_columns[] = { 17, 18};
    int64_t left_filter_column_cnt = sizeof (left_filter_columns) / sizeof(uint64_t);

    int64_t right_index_column_cnt = 4;
    uint64_t right_filter_columns[] = {17, 18};
    int64_t right_filter_column_cnt = sizeof (right_filter_columns) / sizeof(uint64_t);
    check_index_back_dim(false,     //index_back
                         false,     //interesting order
                         false,     //query range
                         left_index_column_cnt,
                         left_filter_columns,
                         left_filter_column_cnt,
                         true,      //index_back
                         false,     //interesting order
                         false,     //query range
                         right_index_column_cnt,
                         right_filter_columns,
                         right_filter_column_cnt,
                         ObSkylineDim::UNCOMPARABLE,
                         ObSkylineDim::UNCOMPARABLE);
  }

  {
    //left (not index back), right (index back)
    //left_filter_columns = right_filter_columns = {}
    //but left_index_column_cnt > right_index_column_cnt
    //left dominate_right
    //right need full index scan, not consider index_column_size
    int64_t left_index_column_cnt = 4;
    uint64_t left_filter_columns[] = {};
    int64_t left_filter_column_cnt = sizeof (left_filter_columns) / sizeof(uint64_t);

    int64_t right_index_column_cnt = 2;
    uint64_t right_filter_columns[] = {};
    int64_t right_filter_column_cnt = sizeof (right_filter_columns) / sizeof(uint64_t);
    check_index_back_dim(false,     //index_back
                         false,     //interesting order
                         false,     //query range
                         left_index_column_cnt,
                         left_filter_columns,
                         left_filter_column_cnt,
                         true,      //index_back
                         false,     //interesting order
                         false,     //query range
                         right_index_column_cnt,
                         right_filter_columns,
                         right_filter_column_cnt,
                         ObSkylineDim::LEFT_DOMINATED,
                         ObSkylineDim::RIGHT_DOMINATED);
  }
  {
    //left (not index back), right (index back)
    //left_filter_columns > right_filter_columns
    //but left_index_column_cnt = right_index_column_cnt
    //left dominate_right
    //right need full index scan, not consider index_column_size
    int64_t left_index_column_cnt = 2;
    uint64_t left_filter_columns[] = {17, 18};
    int64_t left_filter_column_cnt = sizeof (left_filter_columns) / sizeof(uint64_t);

    int64_t right_index_column_cnt = 2;
    uint64_t right_filter_columns[] = {17};
    int64_t right_filter_column_cnt = sizeof (right_filter_columns) / sizeof(uint64_t);
    check_index_back_dim(false,     //index_back
                         false,     //interesting order
                         false,     //query range
                         left_index_column_cnt,
                         left_filter_columns,
                         left_filter_column_cnt,
                         true,      //index_back
                         false,     //interesting order
                         false,     //query range
                         right_index_column_cnt,
                         right_filter_columns,
                         right_filter_column_cnt,
                         ObSkylineDim::LEFT_DOMINATED,
                         ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    //left (not index back), right (index back)
    //left_filter_columns > right_filter_columns
    //but left_index_column_cnt > right_index_column_cnt
    int64_t left_index_column_cnt = 4;
    uint64_t left_filter_columns[] = {17, 18};
    int64_t left_filter_column_cnt = sizeof (left_filter_columns) / sizeof(uint64_t);

    int64_t right_index_column_cnt = 2;
    uint64_t right_filter_columns[] = {17};
    int64_t right_filter_column_cnt = sizeof (right_filter_columns) / sizeof(uint64_t);
    check_index_back_dim(false,     //index_back
                         false,     //interesting order
                         false,     //query range
                         left_index_column_cnt,
                         left_filter_columns,
                         left_filter_column_cnt,
                         true,      //index_back
                         false,     //interesting order
                         false,     //query range
                         right_index_column_cnt,
                         right_filter_columns,
                         right_filter_column_cnt,
                         ObSkylineDim::UNCOMPARABLE,
                         ObSkylineDim::UNCOMPARABLE);
  }

  {
    //left (not index back), right (index back)
    //left_filter_columns > right_filter_columns
    //but left_index_column_cnt < right_index_column_cnt
    int64_t left_index_column_cnt = 2;
    uint64_t left_filter_columns[] = {17, 18};
    int64_t left_filter_column_cnt = sizeof (left_filter_columns) / sizeof(uint64_t);

    int64_t right_index_column_cnt = 5;
    uint64_t right_filter_columns[] = {17};
    int64_t right_filter_column_cnt = sizeof (right_filter_columns) / sizeof(uint64_t);
    check_index_back_dim(false,     //index_back
                         false,     //interesting order
                         false,     //query range
                         left_index_column_cnt,
                         left_filter_columns,
                         left_filter_column_cnt,
                         true,      //index_back
                         false,     //interesting order
                         false,     //query range
                         right_index_column_cnt,
                         right_filter_columns,
                         right_filter_column_cnt,
                         ObSkylineDim::LEFT_DOMINATED,
                         ObSkylineDim::RIGHT_DOMINATED);
  }


  {
    //left (not index back), right (index back)
    //left_filter_columns < right_filter_columns
    //not comparable
    int64_t left_index_column_cnt = 5;
    uint64_t left_filter_columns[] = {17, 18, 19};
    int64_t left_filter_column_cnt = sizeof (left_filter_columns) / sizeof(uint64_t);

    int64_t right_index_column_cnt = 6;
    uint64_t right_filter_columns[] = {17, 18};
    int64_t right_filter_column_cnt = sizeof (right_filter_columns) / sizeof(uint64_t);
    check_index_back_dim(true,     //index_back
                         false,     //interesting order
                         false,     //query range
                         left_index_column_cnt,
                         left_filter_columns,
                         left_filter_column_cnt,
                         true,      //index_back
                         false,     //interesting order
                         false,     //query range
                         right_index_column_cnt,
                         right_filter_columns,
                         right_filter_column_cnt,
                         ObSkylineDim::EQUAL,
                         ObSkylineDim::EQUAL);
  }

  {
    //left (not index back), right (index back)
    //left_filter_columns < right_filter_columns
    //not comparable
    int64_t left_index_column_cnt = 5;
    uint64_t left_filter_columns[] = {17, 18};
    int64_t left_filter_column_cnt = sizeof (left_filter_columns) / sizeof(uint64_t);

    int64_t right_index_column_cnt = 6;
    uint64_t right_filter_columns[] = {17, 18, 19};
    int64_t right_filter_column_cnt = sizeof (right_filter_columns) / sizeof(uint64_t);
    check_index_back_dim(true,     //index_back
                         false,     //interesting order
                         false,     //query range
                         left_index_column_cnt,
                         left_filter_columns,
                         left_filter_column_cnt,
                         true,      //index_back
                         false,     //interesting order
                         false,     //query range
                         right_index_column_cnt,
                         right_filter_columns,
                         right_filter_column_cnt,
                         ObSkylineDim::EQUAL,
                         ObSkylineDim::EQUAL);

  }

  {
    //left (not index back), right (index back)
    //left_filter_columns < right_filter_columns
    //not comparable
    int64_t left_index_column_cnt = 5;

    int64_t right_index_column_cnt = 6;
    check_index_back_dim(true,     //index_back
                         false,     //interesting order
                         false,     //query range
                         left_index_column_cnt,
                         NULL,
                         0,
                         true,      //index_back
                         false,     //interesting order
                         false,     //query range
                         right_index_column_cnt,
                         NULL,
                         0,
                         ObSkylineDim::EQUAL,
                         ObSkylineDim::EQUAL);
  }

}

TEST_F(ObSkylinePrunningTest, interest_dim)
{

  {
    uint64_t column_ids[] = {16,17,18};
    check_interest_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                       NULL, 0,
                       ObSkylineDim::LEFT_DOMINATED, ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    uint64_t column_ids[] = {16,17,18};
    uint64_t column_ids2[] = {17, 18, 19};
    check_interest_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                       column_ids2, sizeof(column_ids2)/sizeof(uint64_t),
                       ObSkylineDim::UNCOMPARABLE, ObSkylineDim::UNCOMPARABLE);
  }

  {
    uint64_t column_ids[] = {16, 20, 23, 55};
    uint64_t column_ids2[] = {16, 20, 23, 55};
    check_interest_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                       column_ids2, sizeof(column_ids2)/sizeof(uint64_t),
                       ObSkylineDim::EQUAL, ObSkylineDim::EQUAL);
  }

  {
    uint64_t column_ids[] = {16, 22, 25};
    uint64_t column_ids2[] = {25, 22, 16};
    check_interest_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                       column_ids2, sizeof(column_ids2)/sizeof(uint64_t),
                       ObSkylineDim::UNCOMPARABLE, ObSkylineDim::UNCOMPARABLE);
  }

  {
    uint64_t column_ids[] = {16, 22, 25};
    uint64_t column_ids2[] = {22, 25};
    check_interest_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                       column_ids2, sizeof(column_ids2)/sizeof(uint64_t),
                       ObSkylineDim::UNCOMPARABLE, ObSkylineDim::UNCOMPARABLE);
  }

  {
    uint64_t column_ids[] = {16, 22, 25};
    uint64_t column_ids2[] = {16};
    check_interest_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                       column_ids2, sizeof(column_ids2)/sizeof(uint64_t),
                       ObSkylineDim::LEFT_DOMINATED, ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    check_interest_dim(NULL, 0, NULL, 0,
                       ObSkylineDim::EQUAL, ObSkylineDim::EQUAL);
  }
}

TEST_F(ObSkylinePrunningTest, query_range_dim)
{
  {
    uint64_t column_ids[] = {16,17,18};
    check_query_range_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                          NULL, 0,
                          ObSkylineDim::LEFT_DOMINATED, ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    uint64_t column_ids[] = {16,17,18};
    uint64_t column_ids2[] = {17, 18, 19};
    check_query_range_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                          column_ids2, sizeof(column_ids2)/sizeof(uint64_t),
                          ObSkylineDim::UNCOMPARABLE, ObSkylineDim::UNCOMPARABLE);
  }

  {
    uint64_t column_ids[] = {16, 20, 23, 55};
    uint64_t column_ids2[] = {16, 20, 23, 55};
    check_query_range_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                       column_ids2, sizeof(column_ids2)/sizeof(uint64_t),
                       ObSkylineDim::EQUAL, ObSkylineDim::EQUAL);
  }

  {
    uint64_t column_ids[] = {16, 22, 25};
    uint64_t column_ids2[] = {25, 22, 16};
    check_query_range_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                       column_ids2, sizeof(column_ids2)/sizeof(uint64_t),
                       ObSkylineDim::EQUAL, ObSkylineDim::EQUAL);
  }

  {
    uint64_t column_ids[] = {16, 22, 25};
    uint64_t column_ids2[] = {22, 25};
    check_query_range_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                       column_ids2, sizeof(column_ids2)/sizeof(uint64_t),
                       ObSkylineDim::LEFT_DOMINATED, ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    uint64_t column_ids[] = {16, 22, 25};
    uint64_t column_ids2[] = {16};
    check_query_range_dim(column_ids, sizeof(column_ids)/sizeof(uint64_t),
                       column_ids2, sizeof(column_ids2)/sizeof(uint64_t),
                       ObSkylineDim::LEFT_DOMINATED, ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    check_query_range_dim(NULL, 0, NULL, 0,
                       ObSkylineDim::EQUAL, ObSkylineDim::EQUAL);
  }
}

TEST_F(ObSkylinePrunningTest, key_prefix_compare)
{
  {
    uint64_t left[] = {15, 16};
    bool left_column_const[] = {false, false};
    uint64_t right[] = {15, 16, 17, 18, 19};
    bool right_column_const[] = {false, false, false, false, false};
    KeyPrefixComp comp;
    int ret = comp(left, left_column_const, sizeof(left)/sizeof(uint64_t),
                   right, right_column_const, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    uint64_t left[] = {15, 17};
    bool left_column_const[] = {false, false};
    uint64_t right[] = {15, 16, 17, 18, 19};
    bool right_column_const[] = {false, false, false, false, false};
    KeyPrefixComp comp;
    int ret = comp(left, left_column_const, sizeof(left)/sizeof(uint64_t),
                   right, right_column_const, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::UNCOMPARABLE);
  }

  {
    uint64_t left[] = {17};
    bool left_column_const[] = {false};
    uint64_t right[] = {15, 16, 17};
    bool right_column_const[] = {false, false, false};
    KeyPrefixComp comp;
    int ret = comp(left, left_column_const, sizeof(left)/sizeof(uint64_t),
                   right, right_column_const, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::UNCOMPARABLE);
  }

  {
    uint64_t left[] = {15, 16, 17};
    bool left_column_const[] = {false, false, false};
    uint64_t right[] = {15, 16, 17};
    bool right_column_const[] = {false, false, false};
    KeyPrefixComp comp;
    int ret = comp(left, left_column_const, sizeof(left)/sizeof(uint64_t),
                   right, right_column_const, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::EQUAL);
  }

  {
    uint64_t left[] = {15, 17};
    bool left_column_const[] = {false, false};
    uint64_t right[] = {15, 16, 17};
    bool right_column_const[] = {false, true, false};
    KeyPrefixComp comp;
    int ret = comp(left, left_column_const, sizeof(left)/sizeof(uint64_t),
                   right, right_column_const, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    uint64_t left[] = {15};
    bool left_column_const[] = {false};
    uint64_t right[] = {15, 16, 17};
    bool right_column_const[] = {false, true, false};
    KeyPrefixComp comp;
    int ret = comp(left, left_column_const, sizeof(left)/sizeof(uint64_t),
                   right, right_column_const, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    uint64_t left[] = {15, 16};
    bool left_column_const[] = {false, false};
    uint64_t right[] = {15, 16, 17};
    bool right_column_const[] = {false, false, true};
    KeyPrefixComp comp;
    int ret = comp(left, left_column_const, sizeof(left)/sizeof(uint64_t),
                   right, right_column_const, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    uint64_t left[] = {15, 16};
    bool left_column_const[] = {false, false};
    uint64_t right[] = {17, 15, 16};
    bool right_column_const[] = {true, true, true};
    KeyPrefixComp comp;
    int ret = comp(left, left_column_const, sizeof(left)/sizeof(uint64_t),
                   right, right_column_const, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    uint64_t left[] = {15, 16};
    bool left_column_const[] = {false, false};
    uint64_t right[] = {15, 17, 18};
    bool right_column_const[] = {true, true, true};
    KeyPrefixComp comp;
    int ret = comp(left, left_column_const, sizeof(left)/sizeof(uint64_t),
                   right, right_column_const, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::UNCOMPARABLE);
  }

  {
    uint64_t left[] = {15, 16};
    bool left_column_const[] = {false};
    uint64_t right[] = {15, 16};
    bool right_column_const[] = {true, true, true};
    KeyPrefixComp comp;
    int ret = comp(left, left_column_const, sizeof(left)/sizeof(uint64_t),
                   right, right_column_const, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::EQUAL);
  }
}

TEST_F(ObSkylinePrunningTest, range_subset_compare)
{
  {
    uint64_t left[] = {16, 18};
    uint64_t right[] = {15, 16, 17, 18, 19};
    RangeSubsetComp comp;
    int ret = comp(left, sizeof(left)/sizeof(uint64_t),
                   right, sizeof(right)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp.get_result(), ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    uint64_t left2[] = {17, 18, 19};
    uint64_t right2[] = {17, 18, 19};
    RangeSubsetComp comp2;
    int ret = comp2(left2, sizeof(left2)/sizeof(uint64_t),
                    right2, sizeof(right2)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp2.get_result(), ObSkylineDim::EQUAL);
  }

  {
    uint64_t left2[] = {17, 18, 19};
    uint64_t right2[] = {17, 18, 20};
    RangeSubsetComp comp2;
    int ret = comp2(left2, sizeof(left2)/sizeof(uint64_t),
                    right2, sizeof(right2)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp2.get_result(), ObSkylineDim::UNCOMPARABLE);
  }

  {
    uint64_t left2[] = {20};
    uint64_t right2[] = {17, 18, 20};
    RangeSubsetComp comp2;
    int ret = comp2(left2, sizeof(left2)/sizeof(uint64_t),
                    right2, sizeof(right2)/sizeof(uint64_t));
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(comp2.get_result(), ObSkylineDim::RIGHT_DOMINATED);
  }
}

TEST_F(ObSkylinePrunningTest, skyline_index_compare)
{
  {
    uint64_t left_interest_ids[] = {16, 17, 18};
    uint64_t left_query_range_ids[] = {10, 20};

    uint64_t right_interest_ids[] = {16, 17, 18};
    uint64_t right_query_range_ids[] = {20, 10};
    ObIndexSkylineDim *dim1 = create_skyline_index_dim(
        false, 3 /*index_column_cnt*/, NULL, 0,
        left_interest_ids, sizeof(left_interest_ids)/sizeof(uint64_t),
        left_query_range_ids, sizeof(left_query_range_ids)/sizeof(uint64_t));
    ObIndexSkylineDim *dim2 = create_skyline_index_dim(
        false, 3 /*index_column_cnt*/, NULL, 0,
        right_interest_ids, sizeof(right_interest_ids)/sizeof(uint64_t),
        right_query_range_ids, sizeof(right_query_range_ids)/sizeof(uint64_t));
    check(dim1, dim2, ObSkylineDim::EQUAL, ObSkylineDim::EQUAL);
  }

  {
    uint64_t left_interest_ids[] = {16, 17, 18};
    uint64_t left_query_range_ids[] = {10};

    uint64_t right_interest_ids[] = {16, 17, 18};
    uint64_t right_query_range_ids[] = {20};
    ObIndexSkylineDim *dim1 = create_skyline_index_dim(
        false, 3 /*index_column_cnt*/, NULL, 0,
        left_interest_ids, sizeof(left_interest_ids)/sizeof(uint64_t),
        left_query_range_ids, sizeof(left_query_range_ids)/sizeof(uint64_t));
    ObIndexSkylineDim *dim2 = create_skyline_index_dim(
        false, 3 /*index_column_cnt*/, NULL, 0,
        right_interest_ids, sizeof(right_interest_ids)/sizeof(uint64_t),
        right_query_range_ids, sizeof(right_query_range_ids)/sizeof(uint64_t));
    check(dim1, dim2, ObSkylineDim::UNCOMPARABLE, ObSkylineDim::UNCOMPARABLE);

  }

  {
    uint64_t left_interest_ids[] = {16, 17, 20};
    uint64_t left_query_range_ids[] = {10, 20, 30};

    uint64_t right_interest_ids[] = {16, 17, 20};
    uint64_t right_query_range_ids[] = {30};
    ObIndexSkylineDim *dim1 = create_skyline_index_dim(
        false, 3/*index_column_cnt*/, NULL, 0,
        left_interest_ids, sizeof(left_interest_ids)/sizeof(uint64_t),
        left_query_range_ids, sizeof(left_query_range_ids)/sizeof(uint64_t));
    ObIndexSkylineDim *dim2 = create_skyline_index_dim(
        false, 3/*index_column_cnt*/, NULL, 0,
        right_interest_ids, sizeof(right_interest_ids)/sizeof(uint64_t),
        right_query_range_ids, sizeof(right_query_range_ids)/sizeof(uint64_t));
    check(dim1, dim2, ObSkylineDim::LEFT_DOMINATED, ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    ObIndexSkylineDim *dim1 = create_skyline_index_dim(
        true, 4/*index_column_cnt*/, NULL, 0,
        NULL, 0, NULL, 0);
    ObIndexSkylineDim *dim2 = create_skyline_index_dim(
        true, 4/*index_column_cnt*/, NULL, 0,
        NULL, 0, NULL, 0);
    check(dim1, dim2, ObSkylineDim::EQUAL, ObSkylineDim::EQUAL);
  }

  {
    uint64_t left_interest_ids[] = {16, 17, 20};
    ObIndexSkylineDim *dim1 = create_skyline_index_dim(
        false, 4/*index_column_cnt*/, NULL, 0,
        left_interest_ids, sizeof(left_interest_ids)/sizeof(uint64_t),
        NULL, 0);
    ObIndexSkylineDim *dim2 = create_skyline_index_dim(
        false, 4/*index_column_cnt*/, NULL, 0,
        NULL, 0, NULL, 0);
    check(dim1, dim2, ObSkylineDim::LEFT_DOMINATED, ObSkylineDim::RIGHT_DOMINATED);
  }

  {
    uint64_t left_query_range_ids[] = {20, 10, 11};
    ObIndexSkylineDim *dim1 = create_skyline_index_dim(
        false, 4/*index_column_cnt*/, NULL, 0,
        NULL, 0,
        left_query_range_ids, sizeof(left_query_range_ids)/sizeof(uint64_t));
    ObIndexSkylineDim *dim2 = create_skyline_index_dim(
        false, 4/*index_column_cnt*/, NULL, 0,
        NULL, 0, NULL, 0);
    check(dim1, dim2, ObSkylineDim::LEFT_DOMINATED, ObSkylineDim::RIGHT_DOMINATED);
  }
}

TEST_F(ObSkylinePrunningTest, has_dominate_index)
{
  ObSkylineDimRecorder recorder;
  uint64_t interest_ids[] = {16, 17};
  uint64_t range_ids[] = {10, 20};
  ObIndexSkylineDim *dim = create_skyline_index_dim(
      false, 4/*index_column_cnt*/, NULL, 0,
      interest_ids, sizeof(interest_ids)/sizeof(uint64_t),
      range_ids, sizeof(range_ids)/sizeof(uint64_t));

  uint64_t interest_ids_1[] = {16, 18};
  uint64_t range_ids_1[] = {20, 10};
  ObIndexSkylineDim *dim1 = create_skyline_index_dim(
      false, 2/*index_column_cnt*/, NULL, 0,
      interest_ids_1, sizeof(interest_ids_1)/sizeof(uint64_t),
      range_ids_1, sizeof(range_ids_1)/sizeof(uint64_t));

  uint64_t interest_ids_2[] = {16};
  uint64_t range_ids_2[] = {10};
  ObIndexSkylineDim *dim2 = create_skyline_index_dim(
      false, 1/*index_column_cnt*/, NULL, 0,
      interest_ids_2, sizeof(interest_ids_2)/sizeof(uint64_t),
      range_ids_2, sizeof(range_ids_2)/sizeof(uint64_t));

  bool has_add = false;
  ObArray<int64_t> remove_idxs;
  bool need_add = true;
  int ret = recorder.has_dominate_dim(*dim, remove_idxs, need_add);
  ASSERT_EQ(need_add ,true);
  ASSERT_EQ(remove_idxs.count(), 0);
  ret = recorder.add_index_dim(*dim, has_add);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(true, has_add);

  remove_idxs.reset();
  ret = recorder.has_dominate_dim(*dim1, remove_idxs, need_add);
  ASSERT_EQ(need_add ,true);
  ASSERT_EQ(remove_idxs.count(), 0);
  ret = recorder.add_index_dim(*dim1, has_add);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(true, has_add);
  ASSERT_EQ(recorder.get_dim_count(),  2);

  remove_idxs.reset();
  ret = recorder.has_dominate_dim(*dim2, remove_idxs, need_add);
  ASSERT_EQ(need_add ,false);
  ASSERT_EQ(remove_idxs.count(), 0);
  ret = recorder.add_index_dim(*dim2, has_add);
  ASSERT_EQ(false, has_add);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(recorder.get_dim_count(), 2);

  uint64_t interest_ids_3[] = {16, 17, 18};
  uint64_t range_ids_3[] = {10, 20, 30};
  ObIndexSkylineDim *dim3 = create_skyline_index_dim(
      false, 3 /*index_column_cnt*/, NULL, 0,
      interest_ids_3, sizeof(interest_ids_3)/sizeof(uint64_t),
      range_ids_3, sizeof(range_ids_3)/sizeof(uint64_t));

  ret = recorder.has_dominate_dim(*dim3, remove_idxs, need_add);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(need_add, true);
  ASSERT_EQ(remove_idxs.count(), 1);
  ASSERT_EQ(remove_idxs.at(0), 0);

  ret = recorder.add_index_dim(*dim3, has_add);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(true, has_add);
  ASSERT_EQ(recorder.get_dim_count(), 2);


  //unprunning index
  ObIndexSkylineDim *dim2_unprunning = create_skyline_index_dim(
      false, 1/*index_column_cnt*/, NULL, 0,
      interest_ids_2, sizeof(interest_ids_2)/sizeof(uint64_t),
      range_ids_2, sizeof(range_ids_2)/sizeof(uint64_t));
  dim2_unprunning->set_can_prunning(false); //can't prunning
  remove_idxs.reset();
  ret = recorder.has_dominate_dim(*dim2_unprunning, remove_idxs, need_add);
  ASSERT_EQ(need_add ,false);
  ASSERT_EQ(remove_idxs.count(), 0);
  ret = recorder.add_index_dim(*dim2_unprunning, has_add);
  ASSERT_EQ(true, has_add);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(recorder.get_dim_count(), 3);

  //check, can add again
  uint64_t interest_ids_4[] = {20, 21, 22};
  uint64_t range_ids_4[] = {19, 20};
  ObIndexSkylineDim *dim4 = create_skyline_index_dim(
      false, 4/*index_column_cnt*/, NULL, 0, //index back
      interest_ids_4, sizeof(interest_ids_4)/sizeof(uint64_t),
      range_ids_4, sizeof(range_ids_4)/sizeof(uint64_t));
  remove_idxs.reset();
  need_add = false;
  has_add = false;
  dim4->set_can_prunning(false);//can't prunning
  recorder.has_dominate_dim(*dim4, remove_idxs, need_add);
  ASSERT_EQ(need_add, true);
  ret = recorder.add_index_dim(*dim4, has_add);
  ASSERT_EQ(has_add, true);

  uint64_t interest_ids_5[] = {20, 21};
  uint64_t range_ids_5[] = {19};
  ObIndexSkylineDim *dim5 = create_skyline_index_dim(
      false, //index back
      2/*index_column_cnt*/, NULL, 0, //filter_ids,
      interest_ids_5, sizeof(interest_ids_5)/sizeof(uint64_t),
      range_ids_5, sizeof(range_ids_5)/sizeof(uint64_t));
  remove_idxs.reset();
  need_add = false;
  recorder.has_dominate_dim(*dim5, remove_idxs, need_add);
  ASSERT_EQ(need_add, true);

}

}

int main(int argc, char **argv)
{
  system("rm -rf test_skyline_prunning.log");
  if(argc >= 2)
  {
    if (strcmp("DEBUG", argv[1]) == 0
        || strcmp("WARN", argv[1]) == 0)
    OB_LOGGER.set_log_level(argv[1]);
  }
  OB_LOGGER.set_file_name("test_skyline_prunning.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


