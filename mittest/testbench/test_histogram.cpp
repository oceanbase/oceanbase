#include "gtest/gtest.h"
#include "share/stat/ob_opt_column_stat.h"
#include "lib/random/ob_random.h"

namespace oceanbase 
{
namespace unittest 
{
class TestDynamicHistogram : public ::testing::Test {
public:
  TestDynamicHistogram() : histogram(), allocator(), random() {
    bucket_width = 1;
  }
  ~TestDynamicHistogram() {}
  void SetUp();
  void Tear();

public:
  double_t bucket_width;
  ObHistogram histogram;
  ObArenaAllocator allocator;
  ObRandom random;
};

void TestDynamicHistogram::SetUp() {
  histogram.set_bucket_width(bucket_width);
  ASSERT_EQ(OB_SUCCESS, histogram.prepare_allocate_buckets(allocator, 100));
  EXPECT_EQ(100, histogram.get_bucket_size());
}

void TestDynamicHistogram::Tear() {
  histogram.reset();
}

TEST_F(TestDynamicHistogram, insert_buckets) {
  int64_t cumulative_count = 0;
  for (int i = 1; i <= 100; ++i) {
    int64_t count = random.rand(0, 100);
    cumulative_count += count;
    common::ObObj bucket_tail;
    bucket_tail.set_double(bucket_width * i);
    ObHistBucket bucket(bucket_tail, count, cumulative_count, count);
    ASSERT_EQ(OB_SUCCESS, histogram.add_bucket(bucket));
    EXPECT_EQ(i, histogram.get_bucket_cnt());
    common::ObObj endpoint_value;
    histogram.get_endpoint_value(endpoint_value);
    EXPECT_EQ(bucket_width * i, endpoint_value.get_double());
  }
  histogram.set_sample_size(cumulative_count);
  ASSERT_NE(OB_SUCCESS, histogram.add_bucket(ObHistBucket()));
  int64_t actual_cumulative_count = 0;
  for (int i = 0; i < 100; ++i) {
    actual_cumulative_count += histogram.get(i).endpoint_repeat_dynamic_count_;
    EXPECT_EQ(actual_cumulative_count, histogram.get(i).endpoint_num_);
  }
  EXPECT_EQ(cumulative_count, actual_cumulative_count);
  EXPECT_EQ(actual_cumulative_count, histogram.get_sample_size());
}

TEST_F(TestDynamicHistogram, inc_endpoint_count) {
  int64_t cumulative_count = 0;
  for (int i = 1; i <= 100; ++i) {
    int64_t count = random.rand(0, 100);
    cumulative_count += count;
    common::ObObj bucket_tail;
    bucket_tail.set_double(bucket_width * i);
    ObHistBucket bucket(bucket_tail, count, cumulative_count, count);
    ASSERT_EQ(OB_SUCCESS, histogram.add_bucket(bucket));
  }
  histogram.set_sample_size(cumulative_count);
  int64_t inc_count = 1000;
  for (int i = 0; i < inc_count; ++i) {
    int64_t endpoint = random.rand(0, 100);
    common::ObObj endpoint_value;
    endpoint_value.set_double(endpoint);
    ASSERT_EQ(OB_SUCCESS, histogram.inc_endpoint_repeat_count(endpoint_value));
  }
  int64_t actual_cumulative_count = 0;
  for (int i = 0; i < 100; ++i) {
    actual_cumulative_count += histogram.get(i).endpoint_repeat_dynamic_count_;
  }
  EXPECT_EQ(cumulative_count + inc_count, actual_cumulative_count);
  EXPECT_EQ(actual_cumulative_count, histogram.get_sample_size());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}