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
#include <stdio.h>
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/alloc/alloc_func.h"
#include "lib/ob_define.h"
#include "lib/compress/zlib/zlib.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/coro/testing.h"


using namespace oceanbase::obsys;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

namespace oceanbase
{
namespace unittest
{
class TestCompressorStress: public cotesting::DefaultRunnable
{
public:
  TestCompressorStress();
  virtual ~TestCompressorStress();
  int init(const int64_t dict_size, const int64_t sample_size, ObCompressor *compressor);
  void destroy();
  void run1() final;
private:
  ObArenaAllocator allocator_;
  ObArray<const char*> dict_;
  ObCompressor *compressor_;
  ObHashMap<int64_t, int64_t, hash::ReadWriteDefendMode> map_;
  int64_t sample_size_;
  bool is_inited_;
};

TestCompressorStress::TestCompressorStress()
  : allocator_(ObModIds::TEST),
    dict_(),
    compressor_(NULL),
    map_(),
    sample_size_(0),
    is_inited_(false)
{
}

TestCompressorStress::~TestCompressorStress()
{
}

int TestCompressorStress::init(
    const int64_t dict_size,
    const int64_t sample_size,
    ObCompressor *compressor)
{
  int ret = OB_SUCCESS;
  if (dict_size <=0 || sample_size <= 0 || NULL == compressor) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", KP(dict_size), K(sample_size), KP(compressor), K(ret));
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The TestCompressorStress has been inited, ", K(ret));
  } else if (OB_FAIL(map_.create(sample_size, ObModIds::TEST))) {
    COMMON_LOG(WARN, "Fail to create map, ", K(ret));
  } else {
    int64_t pos = 0;
    char word[64];
    char *buf = NULL;
    int64_t word_len = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < dict_size; ++i) {
      word_len = ObRandom::rand(5, 30);
      for (pos = 0; pos < word_len; ++pos) {
        word[pos] = static_cast<char> ('a' + ObRandom::rand(0, 25));
      }
      word[pos] = '\0';
      if (NULL == (buf = (char*) allocator_.alloc(pos))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        memcpy(buf, word, pos);
        if (OB_FAIL(dict_.push_back(buf))) {
          COMMON_LOG(WARN, "Fail to push buf to dict array, ", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      compressor_ = compressor;
      sample_size_ = sample_size;
      is_inited_ = true;
    }
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void TestCompressorStress::destroy()
{
  allocator_.reset();
  dict_.reset();
  compressor_ = NULL;
  map_.destroy();
  is_inited_ = false;
}

void TestCompressorStress::run1()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    int64_t data_buf_size = 64 * 1024;
    int64_t cmp_buf_size = 128 * 1024;
    int64_t max_overflow_size = 0;
    char *data_buf = (char*) malloc(data_buf_size);
    char *cmp_buf = (char*) malloc(cmp_buf_size);
    char *tmp_cmp_buf = (char*) malloc(cmp_buf_size);
    int64_t data_seed = 0;
    uint16_t seed[3] = {0, 0, 0};
    int64_t pos = 0;
    int64_t word_idx = 0;
    int64_t word_len = 0;
    int64_t expect_cmp_len = 0;
    int64_t actual_cmp_len = 0;
    int64_t tmp_cmp_len = 0;

    while(!has_set_stop()) {
      //generate data
      pos = 0;
      data_seed = ObRandom::rand(0, sample_size_ - 1);
      seed[0] = static_cast<uint16_t> (data_seed);
      seed[1] = static_cast<uint16_t> (data_seed >> 16);
      seed[2] = static_cast<uint16_t> (data_seed >> 32);
      seed48(seed);
      while (true) {
        word_idx = abs(static_cast<int> (jrand48(seed) % dict_.size()));
        word_len = strlen(dict_.at(word_idx));
        if (pos + word_len + 1 < data_buf_size) {
          memcpy(data_buf + pos, dict_.at(word_idx), word_len);
          pos += word_len;
          data_buf[pos] = ' ';
          pos += 1;
        } else {
          break;
        }
      }

      //decompress data
      if (tmp_cmp_len > 0) {
        ret = compressor_->decompress(tmp_cmp_buf, tmp_cmp_len, cmp_buf, cmp_buf_size, tmp_cmp_len);
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      //compress data
      ret = compressor_->get_max_overflow_size(pos, max_overflow_size);
      ASSERT_EQ(OB_SUCCESS, ret);
      cmp_buf_size = pos + max_overflow_size;
      ret = compressor_->compress(data_buf, pos, cmp_buf, cmp_buf_size, actual_cmp_len);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = map_.get_refactored(data_seed, expect_cmp_len);
      if (OB_SUCC(ret)) {
        ASSERT_EQ(expect_cmp_len, actual_cmp_len);
      } else {
        map_.set_refactored(data_seed, actual_cmp_len);
      }
      tmp_cmp_len = actual_cmp_len;
      memcpy(tmp_cmp_buf, cmp_buf, actual_cmp_len);
    }

    if (NULL != data_buf) {
      free(data_buf);
    }
    if (NULL != cmp_buf) {
      free(cmp_buf);
    }
    if (NULL != tmp_cmp_buf) {
      free(tmp_cmp_buf);
    }
  }
}



class ObCompressorTest : public testing::Test
{
public:
  static void SetUpTestCase()
  {
    memset(const_cast<char *>(compress_buffer), '\0', 100);
    memset(decompress_buffer, '\0', 100);
  }
  static void TearDownTestCase()
  {
  }
  static void test_invalid_argument(ObCompressor &compressor);
  static void test_overflow_size(ObCompressor &compressor);
  static void test_normal(ObCompressor &compressor);
  static const char *src_data;
  static char compress_buffer[1000];
  static char decompress_buffer[1000];
  static int64_t buffer_size;
  static int64_t dst_data_size;
  ObNoneCompressor none_compressor;
  ObLZ4Compressor lz4_compressor;
  ObSnappyCompressor snappy_compressor;
  ObZlibCompressor zlib_compressor;
  ObZstdCompressor zstd_compressor;
};

const char *ObCompressorTest::src_data =
    "OceanBase is the first without shared storage financial database in the world.";
int64_t ObCompressorTest::buffer_size = 1000;
int64_t ObCompressorTest::dst_data_size = 0;
char ObCompressorTest::compress_buffer[1000];
char ObCompressorTest::decompress_buffer[1000];

void ObCompressorTest::test_invalid_argument(ObCompressor &compressor)
{
  int ret = OB_SUCCESS;

  //test compress interface
  ret = compressor.compress(NULL,
                            static_cast<int64_t>(strlen(src_data)),
                            const_cast<char *>(compress_buffer),
                            buffer_size,
                            dst_data_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = compressor.compress(src_data,
                            0,
                            const_cast<char *>(compress_buffer),
                            buffer_size,
                            dst_data_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = compressor.compress(src_data,
                            static_cast<int64_t>(strlen(src_data)),
                            NULL,
                            buffer_size,
                            dst_data_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = compressor.compress(src_data,
                            static_cast<int64_t>(strlen(src_data)),
                            const_cast<char *>(compress_buffer),
                            0,
                            dst_data_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  //test decompress interface
  ret = compressor.decompress(NULL,
                              dst_data_size,
                              const_cast<char *>(decompress_buffer),
                              buffer_size,
                              dst_data_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = compressor.decompress(compress_buffer,
                              0,
                              const_cast<char *>(decompress_buffer),
                              buffer_size,
                              dst_data_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = compressor.decompress(compress_buffer,
                              dst_data_size,
                              NULL,
                              buffer_size,
                              dst_data_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = compressor.decompress(compress_buffer,
                              dst_data_size,
                              const_cast<char *>(decompress_buffer),
                              0,
                              dst_data_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

void ObCompressorTest::test_overflow_size(ObCompressor &compressor)
{
  int ret = OB_SUCCESS;

  ret = compressor.compress(src_data,
                            static_cast<int64_t>(strlen(src_data)),
                            const_cast<char *>(compress_buffer),
                            static_cast<int64_t>(strlen(src_data)),
                            dst_data_size);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
}

void ObCompressorTest::test_normal(ObCompressor &compressor)
{
  int ret = OB_SUCCESS;
  int compare_ret = 0;

  ret = compressor.compress(src_data,
                            static_cast<int64_t>(strlen(src_data)),
                            const_cast<char *>(compress_buffer),
                            buffer_size,
                            dst_data_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = compressor.decompress(compress_buffer,
                              dst_data_size,
                              const_cast<char *>(decompress_buffer),
                              static_cast<int64_t>(strlen(src_data)),
                              dst_data_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  compare_ret = strcmp(src_data, decompress_buffer);
  ASSERT_EQ(0, compare_ret);
}
TEST_F(ObCompressorTest, test_none)
{
  int ret = OB_SUCCESS;
  ret = none_compressor.compress(src_data,
                                 static_cast<int64_t>(strlen(src_data)),
                                 const_cast<char *>(compress_buffer),
                                 buffer_size,
                                 dst_data_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(static_cast<int64_t>(strlen(src_data)), dst_data_size);
  ret = none_compressor.decompress(compress_buffer,
                                   static_cast<int64_t>(strlen(compress_buffer)),
                                   const_cast<char *>(decompress_buffer),
                                   buffer_size,
                                   dst_data_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(static_cast<int64_t>(strlen(compress_buffer)), dst_data_size);
}

TEST_F(ObCompressorTest, test_zlib)
{
  //test invalid argument
  test_invalid_argument(zlib_compressor);

  //test overflow size
  test_overflow_size(zlib_compressor);

  //test normal
  test_normal(zlib_compressor);
}

TEST_F(ObCompressorTest, test_snappy)
{
  //test invalid argument
  test_invalid_argument(snappy_compressor);

  //test overflow size
  test_overflow_size(snappy_compressor);

  //test normal
  test_normal(snappy_compressor);
}

TEST_F(ObCompressorTest, test_lz4)
{
  test_normal(lz4_compressor);
}

TEST_F(ObCompressorTest, test_zstd)
{
  //test invalid argument
  test_invalid_argument(zstd_compressor);

  //test overflow size
  test_overflow_size(zstd_compressor);

  //test normal
  test_normal(zstd_compressor);
}

TEST(ObCompressorStress, compress_stable)
{
  int ret = OB_SUCCESS;
  const int64_t sleep_sec = 1;
  TestCompressorStress cmp_stress;
  ObZstdCompressor zstd_compressor;

  ret = cmp_stress.init(30000, 100000, &zstd_compressor);
  ASSERT_EQ(OB_SUCCESS, ret);
  cmp_stress.set_thread_count(20);
  cmp_stress.start();
  for (int64_t i = 0; i < sleep_sec; ++i) {
    common::ObLabelItem item;
    lib::get_tenant_label_memory(common::OB_SERVER_TENANT_ID, ObModIds::OB_COMPRESSOR, item);
    COMMON_LOG(INFO, "MEMORY USED: ",
        K(item.hold_),
        K(item.used_),
        K(item.alloc_count_),
        K(item.free_count_),
        K(item.count_));
    ASSERT_TRUE(item.alloc_count_ < 1000);
    sleep(1);
  }
  cmp_stress.stop();
  cmp_stress.wait();
  cmp_stress.destroy();
}


TEST_F(ObCompressorTest, test_zlib_stream)
{
  const char *data = "We often get questions about how the deflate() and inflate() functions should "
      "be used. Users wonder when they should provide more input, when they should use more output,"
      " what to do with a Z_BUF_ERROR, how to make sure the process terminates properly, and so on."
      " So for those who have read zlib.h (a few times), and would like further edification, "
      "below is an annotated example in C of simple routines to compress and decompress from "
      "an input file to an output file using deflate() and inflate() respectively. "
      "The annotations are interspersed between lines of the code. So please read between "
      "the lines. We hope this helps explain some of the intricacies of zlib.";

  const int64_t src_len = static_cast<int64_t>(strlen(data));
  const int64_t step = 1;
  int err;
  memset(compress_buffer, 0, sizeof(compress_buffer));
  memset(decompress_buffer, 0, sizeof(decompress_buffer));
  char *buf[100];

  //compress
  z_stream stream;
  stream.next_in = (Bytef *)data;
  stream.avail_in = (uInt)src_len;
  stream.zalloc = (alloc_func)0;
  stream.zfree = (free_func)0;
  stream.opaque = (voidpf)0;

  err = deflateInit(&stream, 6);
  ASSERT_TRUE(err == Z_OK);
  int flush = Z_NO_FLUSH;
  int64_t have = 0;
  int64_t j = 0;
  int64_t i = 0;
  do {
    stream.next_in = (Bytef *)(data + i);
    stream.avail_in = step;
    if (i + step >= src_len) {
      flush = Z_FINISH;
    }
    do {
      stream.avail_out = (uInt)step;
      stream.next_out = (Bytef *)buf;
      err = deflate(&stream, flush);
      have = step - stream.avail_out;
      if (have > 0) {
        memcpy(compress_buffer + j, buf, have);
        j += have;
        COMMON_LOG(INFO, "compress", K(i), K(j), K(have));
      }
    } while(stream.avail_out == 0);
    i += step;
  } while (flush != Z_FINISH);

  ASSERT_EQ(Z_STREAM_END, err);
  dst_data_size = j;
  err = deflateEnd(&stream);
  ASSERT_EQ(371, dst_data_size);


  //decompress
  z_stream stream2;
  stream2.next_in = (Bytef *)compress_buffer;
  stream2.avail_in = (uInt)dst_data_size;
  stream2.zalloc = (alloc_func)0;
  stream2.zfree = (free_func)0;

  err = inflateInit(&stream2);
  ASSERT_TRUE(err == Z_OK);
  i =0;
  j = 0;
  do {
    stream2.next_in = (Bytef *)(compress_buffer + i);
    stream2.avail_in = step;
    if (i >= src_len) {
      break;
    }
    do {
      stream2.avail_out = (uInt)step;
      stream2.next_out = (Bytef *)buf;
      err = inflate(&stream2, Z_NO_FLUSH);
      ASSERT_NE(Z_STREAM_ERROR, err);  /* state not clobbered */
      have = step - stream2.avail_out;
      if (have > 0) {
        memcpy(decompress_buffer + j, buf, have);
        j += have;
        COMMON_LOG(INFO, "decompress", K(i), K(j), K(have));
      }
    } while(stream2.avail_out == 0);
    i += step;
  } while (Z_STREAM_ERROR != err);

  ASSERT_EQ(src_len, j);
  err = inflateEnd(&stream2);
  ASSERT_EQ(0, strcmp(data, decompress_buffer));

  //decompress checksum error
  memset(decompress_buffer, 0, sizeof(decompress_buffer));
  z_stream stream3;
  stream3.next_in = (Bytef *)compress_buffer;
  stream3.avail_in = (uInt)dst_data_size;
  stream3.zalloc = (alloc_func)0;
  stream3.zfree = (free_func)0;
  compress_buffer[src_len/2] = (char)((int)compress_buffer[5] + 1);

  err = inflateInit(&stream3);
  ASSERT_TRUE(err == Z_OK);
  i =0;
  j = 0;
  do {
    stream3.next_in = (Bytef *)(compress_buffer + i);
    stream3.avail_in = step;
    if (i >= src_len) {
      break;
    }
    do {
      stream3.avail_out = (uInt)step;
      stream3.next_out = (Bytef *)buf;
      err = inflate(&stream3, Z_NO_FLUSH);
      ASSERT_NE(Z_STREAM_ERROR, err);  /* state not clobbered */
      have = step - stream3.avail_out;
      if (have > 0) {
        memcpy(decompress_buffer + j, buf, have);
        j += have;
        COMMON_LOG(INFO, "err decompress", K(i), K(j), K(have));
      }
    } while(stream3.avail_out == 0);
    i += step;
    if (Z_DATA_ERROR == err) {
      COMMON_LOG(INFO, "checksum error", K(i), K(j));
      ASSERT_EQ(Z_DATA_ERROR, err);
      ASSERT_EQ(dst_data_size, i);
      break;
    }
  } while (Z_STREAM_ERROR != err);

  ASSERT_NE(src_len, j);
  err = inflateEnd(&stream3);
  ASSERT_NE(0, strcmp(data, decompress_buffer));
}

TEST_F(ObCompressorTest, test_zlib_vs_ob)
{
  static const uint32_t MAX_DATA_SIZE = 1<<18;//256k
  const uint32_t compress_step = 10;

  unsigned char data[MAX_DATA_SIZE];
  uint64_t dst_data_size1 = 0;
  unsigned char compress_buffer1[static_cast<uint32_t>(MAX_DATA_SIZE * 1.1 + 14)];
  uint64_t dst_data_size2 = 0;
  unsigned char compress_buffer2[static_cast<uint32_t>(MAX_DATA_SIZE * 1.1 + 14)];
  uint64_t decompress_data_size = 0;
  unsigned char decompress_buffer[MAX_DATA_SIZE];

  timeval start, end;
  ObRandom test_random;
  int64_t num = 0;
  for (uint32_t i = 0; i < MAX_DATA_SIZE; i++) {
    num = test_random.get(0, 255);
    data[i] = (unsigned char)(num);
  }

  for (uint32_t test_len = 1; test_len < MAX_DATA_SIZE; test_len += compress_step) {
    gettimeofday(&start, NULL);
    for (int64_t i = 0; i < 2; i++) {
      dst_data_size1 = static_cast<uint32_t>(test_len * 1.1 + 14);
      compress2(compress_buffer1, &dst_data_size1, reinterpret_cast<const Bytef*>(data), test_len, 0);
    }
    gettimeofday(&end, NULL);
    COMMON_LOG(INFO, "zlib cost", "usec", (end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec, K(test_len));

    gettimeofday(&start, NULL);
    for (int64_t i = 0; i < 2; i++) {
      dst_data_size2 = static_cast<uint32_t>(test_len * 1.1 + 14);
      zlib_compressor.fast_level0_compress(compress_buffer2, &dst_data_size2, reinterpret_cast<const Bytef*>(data), test_len);
    }
    gettimeofday(&end, NULL);
    COMMON_LOG(INFO, "ob   cost", "usec",  (end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec);

    ASSERT_EQ(dst_data_size1, dst_data_size2);
    ASSERT_EQ(0, memcmp(compress_buffer1, compress_buffer2, dst_data_size2));

    memset(decompress_buffer, 0, sizeof(decompress_buffer));
    decompress_data_size = sizeof(decompress_buffer);
    int zlib_errno = uncompress(reinterpret_cast<Bytef*>(decompress_buffer),
                                reinterpret_cast<uLongf*>(&decompress_data_size),
                                reinterpret_cast<const Byte*>(compress_buffer1),
                                static_cast<uLong>(dst_data_size1));
    ASSERT_EQ(zlib_errno, Z_OK);
    ASSERT_EQ(test_len, decompress_data_size);
    ASSERT_EQ(0, memcmp(decompress_buffer, data, decompress_data_size));
  }
}
}
}

int main(int argc, char **argv)
{
  system("rm -f test_compress.log*");
  OB_LOGGER.set_file_name("test_compress.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
