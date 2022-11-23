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
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_memfrag_recycle_allocator.h"
#include "lib/random/ob_random.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{

class TestObMetaImage: public ObIRewritable, public lib::ThreadPool
{
public:
  TestObMetaImage();
  virtual ~TestObMetaImage();
  int init(const int64_t expire_duration);
  void destroy();
  virtual int rewrite_switch();
  void run1() final;
private:
  static const int64_t MAX_META_COUNT = 1024L * 1024L * 2L;
  struct TestObMeta
  {
    int64_t size_;
    char data_[0];
  };
  int do_work();
  inline void *alloc(const int64_t size)
  {
    return allocator_.alloc(size);
  }
  inline void free(void *ptr)
  {
    allocator_.free(ptr);
  }
  inline int need_rewrite(void *ptr, bool &is_need)
  {
    return allocator_.need_rewrite(ptr, is_need);
  }
  ObMemfragRecycleAllocator allocator_;
  TestObMeta **meta_array_;
};

TestObMetaImage::TestObMetaImage()
{
  meta_array_ = (TestObMeta**) ob_malloc(sizeof(TestObMeta*) * MAX_META_COUNT);
  memset(meta_array_, 0, sizeof(TestObMeta*) * MAX_META_COUNT);
}

TestObMetaImage::~TestObMetaImage()
{
  ob_free(meta_array_);
}

int TestObMetaImage::init(const int64_t expire_duration)
{
  return allocator_.init(ObModIds::OB_MACRO_BLOCK_META, this, expire_duration);
}

void TestObMetaImage::destroy()
{
  for (int64_t i = 0; i < MAX_META_COUNT; i++) {
    if (NULL != meta_array_[i]) {
      free(meta_array_[i]);
    }
  }
  allocator_.destroy();
}

void TestObMetaImage::run1()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = do_work())) {
    LIB_ALLOC_LOG(WARN, "do work failed, ", "ret", ret);
  }
}

int TestObMetaImage::do_work()
{
  int ret = OB_SUCCESS;
  int64_t loc = 0;
  int64_t size = 0;
  TestObMeta* old_meta = NULL;
  TestObMeta* new_meta = NULL;

  while (!has_set_stop()) {
    loc = ObRandom::rand(0, MAX_META_COUNT - 1);
    size = ObRandom::rand(128, 1000);
    new_meta = (TestObMeta*) alloc(size);
    if (NULL == new_meta) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      break;
    } else {
      new_meta->size_ = size;
    }

    old_meta =
        (TestObMeta*) ATOMIC_TAS(reinterpret_cast<volatile uint64_t*>(&meta_array_[loc]),
            (uint64_t) new_meta);
    if (NULL != old_meta) {
      free(old_meta);
    }
  }

  return ret;
}

int TestObMetaImage::rewrite_switch()
{
  int ret = OB_SUCCESS;
  TestObMeta *old_meta = NULL;
  TestObMeta *new_meta = NULL;
  TestObMeta *tmp_meta = NULL;
  bool is_need = false;

  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_META_COUNT; i++) {
    old_meta = meta_array_[i];
    if (OB_SUCCESS != (ret = need_rewrite(old_meta, is_need))) {
      LIB_ALLOC_LOG(WARN, "fail to decide if need rewrite, ", "ret", ret);
    } else if (is_need) {
      new_meta = (TestObMeta*) alloc(old_meta->size_);
      if (NULL == new_meta) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        break;
      } else {
        new_meta->size_ = old_meta->size_;

        tmp_meta =
            (TestObMeta*) ATOMIC_VCAS(reinterpret_cast<volatile uint64_t*>(&meta_array_[i]),
                                      (uint64_t) old_meta,
                                      (uint64_t) new_meta);
        if (old_meta == tmp_meta) {
          free(old_meta);
        } else {
          //retry
          free(new_meta);
          i--;
        }
      }
    }
  }

  return ret;
}

TEST(ObMRAllocatorRecycler, test_recycle)
{
  int ret = OB_SUCCESS;
  ObMemfragRecycleAllocator allocator[100];

// test null argument
  ret = ObMRAllocatorRecycler::get_instance().register_allocator(NULL);
  EXPECT_NE(OB_SUCCESS, ret);

// test normal register
  ret = ObMRAllocatorRecycler::get_instance().register_allocator(&allocator[0]);
  EXPECT_EQ(OB_SUCCESS, ret);

// test repeatly register
  ret = ObMRAllocatorRecycler::get_instance().register_allocator(&allocator[0]);
  EXPECT_NE(OB_SUCCESS, ret);

// test normal deregister
  ret = ObMRAllocatorRecycler::get_instance().deregister_allocator(&allocator[0]);
  EXPECT_EQ(OB_SUCCESS, ret);

// test repeatly deregister
  ret = ObMRAllocatorRecycler::get_instance().deregister_allocator(&allocator[0]);
  EXPECT_NE(OB_SUCCESS, ret);

// test invalid_deregister
  ret = ObMRAllocatorRecycler::get_instance().deregister_allocator(NULL);
  EXPECT_NE(OB_SUCCESS, ret);

// test invalid_deregister
  ret = ObMRAllocatorRecycler::get_instance().deregister_allocator(&allocator[1]);
  EXPECT_NE(OB_SUCCESS, ret);

// test register limit count
  ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < 100; i++) {
    ret = ObMRAllocatorRecycler::get_instance().register_allocator(&allocator[i]);
  }
  EXPECT_NE(OB_SUCCESS, ret);

  ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < 100; i++) {
    ret = ObMRAllocatorRecycler::get_instance().deregister_allocator(&allocator[i]);
  }

}

TEST(ObMemfragRecycleAllocator, test_allocator)
{
  int ret = OB_SUCCESS;
  ObMemfragRecycleAllocator allocator;
  TestObMetaImage image1;
  TestObMetaImage image2;
  void* data = NULL;

// test invalid argument
  ret = allocator.init(-1, NULL, -1);
  EXPECT_NE(OB_SUCCESS, ret);

// test invalid alloc, free ...
  data = allocator.alloc(100);
  EXPECT_TRUE(NULL == data);
  allocator.free(data);

  // test invalid switch
  ret = allocator.switch_state();
  EXPECT_NE(OB_SUCCESS, ret);

// test thread unsafe init
  ret = allocator.init(ObModIds::OB_MACRO_BLOCK_META, NULL, 0);
  EXPECT_EQ(OB_SUCCESS, ret);
// test invalid switch
  ret = allocator.switch_state();
  EXPECT_NE(OB_SUCCESS, ret);
// test destroy
  allocator.destroy();

// test normal init
  ret = allocator.init(ObModIds::OB_MACRO_BLOCK_META, &image1, 0);
  EXPECT_EQ(OB_SUCCESS, ret);

// test repeatly init
  ret = allocator.init(ObModIds::OB_MACRO_BLOCK_META, &image2, 0);
  EXPECT_NE(OB_SUCCESS, ret);

// test normal alloc
  data = allocator.alloc(100);
  EXPECT_TRUE(data != NULL);

// test normal free
  allocator.free(data);

// test invalid free
  allocator.free(&image1);

// test invalid need_rewrite
  bool is_need = false;
  ret = allocator.need_rewrite(NULL, is_need);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(!is_need);

// test normal need_rewrite
  data = allocator.alloc(100);
  ret = allocator.need_rewrite(data, is_need);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(!is_need);

  allocator.destroy();
}

TEST(ObMetaImage, test_image)
{
  const int64_t THREAD_COUNT = 9;
  int ret = OB_SUCCESS;
  TestObMetaImage image;

  ret = image.init(1000 * 1000);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObMRAllocatorRecycler::get_instance().set_schedule_interval_us(1000 * 1000);

  image.set_thread_count(THREAD_COUNT);
  image.start();
  sleep(60);
  image.stop();
  image.wait();

  image.destroy();

}

}
}

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
