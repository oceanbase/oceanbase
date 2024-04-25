#include "lib/hash_func/murmur_hash.h"
#include "lib/oblog/ob_log.h"

#include <gtest/gtest.h>
#include <random>

namespace oceanbase
{
namespace common
{
using namespace std;

void compare_array(uint64_t *h1, uint64_t *h2, size_t len)
{
  for (int i = 0; i < len; i++) {
    ASSERT_EQ(h1[i], h2[i]);
  }
}

template<typename T>
T rand_value(const T min_v, const T max_v)
{
  return min_v;
}

template<>
int64_t rand_value<int64_t>(const int64_t min_v, const int64_t max_v)
{
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> dist(min_v, max_v);
  return dist(rng);
}

template <size_t KEY_LEN, bool IS_BATCH_SEED>
void murmurhash64A_result(const void *keys, uint64_t *hashes, int32_t total_len, uint64_t *seeds) {
  #define SEEDS(i) (IS_BATCH_SEED ? seeds[i] : seeds[0])
  ASSERT_EQ(total_len % KEY_LEN, 0);
  ASSERT_EQ(KEY_LEN == 1 || KEY_LEN == 2 || KEY_LEN == 4 || KEY_LEN == 8 ||
            KEY_LEN == 16 || KEY_LEN == 32 || KEY_LEN == 64, true);

  for (int i = 0; i < total_len / KEY_LEN; i++) {
    hashes[i] = murmurhash64A((char *)keys + i * KEY_LEN, KEY_LEN, SEEDS(i));
  }
  #undef SEEDS
}

class TestMurmurHash
{
public:
  template <size_t KEY_LEN, size_t KEY_CNT>
  void check_hashes() {
    uint64_t h1[KEY_CNT];
    uint64_t h2[KEY_CNT];
    uint64_t seeds[KEY_CNT];
    for (int i = 0; i < KEY_CNT; i++) {
      seeds[i] = rand_value<int64_t>(INT64_MIN, INT64_MAX);
    }
    char keys[KEY_CNT * KEY_LEN];
    for (int i = 0; i < KEY_CNT * KEY_LEN; i++) {
      keys[i] = rand_value<int64_t>(INT8_MIN, INT8_MAX);
    }

    // is batch seed
    for (int i = 1; i <= KEY_CNT; i++) {
        murmurhash64A_result<KEY_LEN, true>(keys, h1, i * KEY_LEN, seeds);
        murmurhash64A<KEY_LEN, true>(keys, h2, i * KEY_LEN, seeds);
        compare_array(h1, h2, i);
    }

    // is not batch seed
    for (int i = 1; i <= KEY_CNT; i++) {
        murmurhash64A_result<KEY_LEN, false>(keys, h1, i * KEY_LEN, seeds);
        murmurhash64A<KEY_LEN, false>(keys, h2, i * KEY_LEN, seeds);
        compare_array(h1, h2, i);
    }
  }
};

TEST(TestMurmurHash, ALL) {
  TestMurmurHash test;
  test.check_hashes<1, 512>();
  test.check_hashes<2, 512>();
  test.check_hashes<4, 512>();
  test.check_hashes<8, 512>();
  test.check_hashes<16, 512>();
  test.check_hashes<32, 512>();
  test.check_hashes<64, 512>();
}
} // end namespace common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}