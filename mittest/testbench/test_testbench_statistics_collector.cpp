#include "gtest/gtest.h"
#include "testbench/ob_testbench_statistics_collector.h"
#include "lib/random/ob_random.h"
#include <thread>

namespace oceanbase 
{
namespace name
{
using namespace testbench;
class TestStatisticsCollector : public ::testing::Test {
public:
  TestStatisticsCollector() 
  : statistics_collector(bucket_capacity, bucket_min_ratio, bucket_max_ratio),
    random(),
    allocator("StatCollector")
  {}

  ~TestStatisticsCollector() {}

  void SetUp();
  void Tear();

public:
  int64_t bucket_capacity = 500;
  double_t bucket_min_ratio = 0.1;
  double_t bucket_max_ratio = 0.7;
  ObTestbenchStatisticsCollector statistics_collector;
  ObRandom random;
  ObArenaAllocator allocator;
};

void TestStatisticsCollector::SetUp() {
  ASSERT_EQ(OB_SUCCESS, statistics_collector.init());
  ASSERT_EQ(OB_SUCCESS, statistics_collector.start());
}

void TestStatisticsCollector::Tear() {
  statistics_collector.destroy();
}

TEST_F(TestStatisticsCollector, push_submit_task) {
  ObLatencyTaskType type = ObLatencyTaskType::DISTRIBUTED_TXN_LATENCY_TASK;
  std::vector<double_t> latencys;
  int latency_count = 1000;
  for (int i = 0; i < latency_count; ++i) {
    common::ObObj latency;
    double_t rnd_latency = random.rand(0, 100);
    latency.set_double(rnd_latency);
    latencys.push_back(rnd_latency);
    void *buf = allocator.alloc(sizeof(ObLatencyTask));
    ObLatencyTask *latency_task = new (buf)ObLatencyTask(type, latency);
    ASSERT_EQ(OB_SUCCESS, statistics_collector.push_latency_task(latency_task));
  }
  ASSERT_EQ(OB_SUCCESS, statistics_collector.sync_latency_task());
  ob_usleep(5000);
  while (!statistics_collector.is_snapshot_ready()) {}
  std::sort(latencys.begin(), latencys.end());
  const ObHistogram &histogram = statistics_collector.get_histogram(type);
  double_t bucket_width = histogram.get_bucket_width();
  ASSERT_GT(bucket_width, 0);
  for (int i = 0; i < 100; ++i) {
    double_t rnd_percentage = random.rand(1, 100) / static_cast<double_t>(100);
    double_t hist_latency = 0;
    EXPECT_EQ(OB_SUCCESS, statistics_collector.get_percentage_latency(type, rnd_percentage, hist_latency));
    double_t vec_latency = 0;
    vec_latency = latencys[rnd_percentage * latency_count] - bucket_width / 2;
    EXPECT_LT(std::abs(vec_latency - hist_latency), bucket_width);
  }
}

TEST_F(TestStatisticsCollector, concurrent_push) {
  ObLatencyTaskType type = ObLatencyTaskType::DISTRIBUTED_TXN_LATENCY_TASK;
  int64_t latency_count = 1000;
  auto process_task = [&]() {
    for (int64_t i = 0; i < latency_count; ++i) {
      common::ObObj latency;
      double_t rnd_latency = random.rand(0, 100);
      latency.set_double(rnd_latency);
      void *buf = allocator.alloc(sizeof(ObLatencyTask));
      ObLatencyTask *latency_task = new (buf)ObLatencyTask(type, latency);
      ASSERT_EQ(OB_SUCCESS, statistics_collector.push_latency_task(latency_task));
    }
  };
  std::thread thread1 = std::thread(process_task);
  std::thread thread2 = std::thread(process_task);
  thread1.join();
  thread2.join();
  const ObStatisticsQueueTask &queue = statistics_collector.get_queue_task(type);
  EXPECT_EQ(latency_count * 2, queue.get_snapshot_queue_cnt());
}
} // namespace name
} // namespace oceanbase

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}