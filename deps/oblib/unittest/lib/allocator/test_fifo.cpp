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

#include <stdlib.h>
#include <sys/time.h>
#include <gtest/gtest.h>
#include <map>
#include <queue>
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace std;

const int64_t page_size = OB_MALLOC_NORMAL_BLOCK_SIZE;

//ObFIFOAllocator::PageType page_size = ObFIFOAllocator::LARGE_PAGE;
//int64_t page_size = OB_MALLOC_BIG_BLOCK_SIZE;
//int64_t page_size = 128;
const int64_t idle_size = 2 * page_size;
const int64_t init_size = 0;

int64_t glibc_alloc_count = 0;
int64_t glibc_free_count = 0;
const ObMemAttr default_memattr(OB_SERVER_TENANT_ID, ObNewModIds::TEST);

#define MOCK_ALIGN 512
#define MOCK_ALLOC_ALIGN 1


class MockAllocator : public ObIAllocator
{
public:
  MockAllocator() : alloc_count_(0), free_count_(0), page_size_(page_size), last_alloc_addr_(NULL), status_(true)
  {
  }
  ~MockAllocator() {}

  void *alloc(const int64_t size)
  {
    void *p = NULL;
    if (status_) {
#if MOCK_ALLOC_ALIGN
    void *p_orig = ::malloc(size + MOCK_ALIGN - 1);
    p = reinterpret_cast<void *>((reinterpret_cast<int64_t>(p_orig) + MOCK_ALIGN - 1) & (~
                                                                                         (MOCK_ALIGN - 1)));
    addr_map_.insert(pair<void *, void *>(p, p_orig));
    LIB_ALLOC_LOG(DEBUG, "MOCK ALIGN");
#else
    p = ::malloc(size);
    LIB_ALLOC_LOG(DEBUG, "NOT MOCK ALIGN");
#endif
    LIB_ALLOC_LOG(DEBUG, "::malloc ", K(p));
    last_alloc_addr_ = p;
    ++alloc_count_;
    }  else {
        p = NULL;
    }
    return p;
  }

  void* alloc(const int64_t size, const ObMemAttr &attr)
  {
    void *p = NULL;
    UNUSED(attr);
    if (status_) {
#if MOCK_ALLOC_ALIGN
      void *p_orig = ::malloc(size + MOCK_ALIGN - 1);
      p = reinterpret_cast<void *>((reinterpret_cast<int64_t>(p_orig) + MOCK_ALIGN - 1) & (~
          (MOCK_ALIGN - 1)));
      addr_map_.insert(pair<void *, void *>(p, p_orig));
      LIB_ALLOC_LOG(DEBUG, "MOCK ALIGN");
#else
  p = ::malloc(size);
LIB_ALLOC_LOG(DEBUG, "NOT MOCK ALIGN");
#endif
LIB_ALLOC_LOG(DEBUG, "::malloc ", K(p));
last_alloc_addr_ = p;
++alloc_count_;
    }  else {
      p = NULL;
    }
    return p;
  }

  void *get_last_alloc()
  {
    return last_alloc_addr_;
  }

  void free(void *p)
  {
#if MOCK_ALLOC_ALIGN
    map<void *, void *>::iterator iter = addr_map_.find(p);
    if (iter == addr_map_.end()) {
      assert(false && "can not happen....");
    } else {
      p = iter->second;
    }
#endif
    ++free_count_;
    ::free(p);
  }

  bool is_leak()
  {
    return (alloc_count_ != free_count_);
  }

  void reset() {}
  void *mod_alloc(const int64_t size, const lib::ObLabel &label = nullptr) { UNUSED(size); UNUSED(label); return NULL; }
  void *mod_realloc(void *p, int64_t size, const lib::ObLabel &label = nullptr) { UNUSED(p); UNUSED(size); UNUSED(label); return NULL; }
  void mod_free(void *p, const lib::ObLabel &label = nullptr) { UNUSED(p); UNUSED(label); }
  void set_label(const lib::ObLabel &label) { UNUSED(label); };

  void set_status(bool status) { status_ = status; }

  int64_t alloc_count_;
  int64_t free_count_;
  const int64_t page_size_;
  void *last_alloc_addr_;
#ifdef MOCK_ALLOC_ALIGN
  map<void *, void *> addr_map_;
#endif
  bool status_;
};

class ObFIFOAllocatorTest : public ::testing::Test
{
public:
  ObFIFOAllocatorTest();
  virtual ~ObFIFOAllocatorTest();
  virtual void SetUp();
  virtual void TearDown();

  bool check_align(void *p, int64_t align);
};

struct AllocParam
{
  int64_t size_;
  int64_t align_;
};

bool test_align(void *p, int64_t align)
{
  return (reinterpret_cast<int64_t>(p) & (align - 1)) == 0;
}

#define ROUTINE_CHECK_PTR(fa) \
  EXPECT_TRUE((fa)->total() >= (fa)->used()); \
  EXPECT_TRUE((fa)->total() >= 0); \
  EXPECT_TRUE((fa)->used() >= 0);

#define ROUTINE_CHECK(fa) \
  EXPECT_TRUE((fa).total() >= (fa).used()); \
  EXPECT_TRUE((fa).total() >= 0); \
  EXPECT_TRUE((fa).used() >= 0);

// [1] just a simple test
TEST(ObFIFOAllocatorTest, simple_test)
{
  MockAllocator *mock_allocator = new MockAllocator();
  ObFIFOAllocator *fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  void *p = fa->alloc_align(101, 1024);
  ASSERT_TRUE(test_align(p, 1024));
  ROUTINE_CHECK_PTR(fa);
  fa->free(p);
  fa->reset();
  delete fa;
  delete mock_allocator;

  const int64_t large_page_size = OB_MALLOC_BIG_BLOCK_SIZE;
  mock_allocator = new MockAllocator();
  fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, large_page_size, default_memattr, init_size, idle_size));
  p = fa->alloc_align(101, 1024);
  ASSERT_TRUE(test_align(p, 1024));
  fa->free(p);
  fa->reset();
  delete fa;
  delete mock_allocator;
}

// [2] test invalid size. size <= 0.
TEST(FIFOAllocatorTest, invalid_size)
{
  MockAllocator *mock_allocator = new MockAllocator();
  ObFIFOAllocator *fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  EXPECT_TRUE(fa->alloc(-100) == NULL);
  EXPECT_TRUE(fa->alloc(1l << 44) == NULL);
  EXPECT_TRUE(fa->alloc(0) == NULL);
  delete fa;
  delete mock_allocator;
}

// [3] test invalid address to free
TEST(ObFIFOAllocator, invalid_free)
{
  MockAllocator *mock_allocator = new MockAllocator();
  ObFIFOAllocator *fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  void *p = NULL;
  void *ptr_to_free = NULL;
  p = fa->alloc(10);
  ptr_to_free = static_cast<void *>(static_cast<char *>(p) + 1);
  fa->free(ptr_to_free);
  fa->free(p);
  delete fa;
  delete mock_allocator;
}

// [4] test alignment
class ObFIFOAllocatorAlignParamTest : public ::testing::TestWithParam<AllocParam>
{
public:
  ObFIFOAllocatorAlignParamTest()
  {
    mock_allocator_ = new MockAllocator();
    fa_ = new ObFIFOAllocator();
    EXPECT_TRUE(OB_SUCCESS == fa_->init(mock_allocator_, page_size, default_memattr, init_size, idle_size));
  }
  virtual ~ObFIFOAllocatorAlignParamTest()
  {
    delete fa_;
    delete mock_allocator_;
  }
  bool check_align(void *p, int64_t align);

public:
  ObFIFOAllocator *fa_;
private:
  MockAllocator *mock_allocator_;
};

bool ObFIFOAllocatorAlignParamTest::check_align(void *p, int64_t align)
{
  int64_t pint = reinterpret_cast<int64_t>(p);
  pint = pint & (align - 1);
  bool is_align = (pint == 0);
  return is_align;
}

TEST_P(ObFIFOAllocatorAlignParamTest, testAlignBy)
{
  struct AllocParam align_param = GetParam();
  int64_t size = align_param.size_;
  int64_t align = align_param.align_;
  void *p = fa_->alloc_align(size, align);
  if ((align & (align - 1)) == 0) {
    EXPECT_TRUE(check_align(p, align));
    fa_->free(p);
  } else {
    EXPECT_TRUE(p ==  NULL);
  }
}


struct AllocParam ap1 = { 450, 64 };
struct AllocParam ap2 = { 234, 16};
struct AllocParam ap3 = { 1230, 16 };
struct AllocParam ap4 = { 4700, 32 };
struct AllocParam ap5 = { 56000, 4 };
struct AllocParam ap6 = { 10000, 1 };
struct AllocParam ap7 = { 7156, 1 };
struct AllocParam ap8 = { 8924, 1 };
struct AllocParam ap9 = { 4712, 32 };
struct AllocParam ap10 = { 56223, 1024 };
struct AllocParam ap11 = { 56, 4096 };

/*
struct AllocParam ap1 = { 45, 16 };
struct AllocParam ap2 = { 23, 16};
struct AllocParam ap3 = { 12, 16 };
struct AllocParam ap4 = { 47, 32 };
struct AllocParam ap5 = { 56, 4 };
struct AllocParam ap6 = { 10, 2 };
struct AllocParam ap7 = { 12, 1 };
struct AllocParam ap8 = { 8, 1 };
struct AllocParam ap9 = { 47, 32 };
struct AllocParam ap10 = { 6, 32 };
struct AllocParam ap11 = { 16, 16 };
*/
struct AllocParam alloc_param_normal[11] = { ap1, ap2, ap3, ap4, ap5, ap6, ap7, ap8, ap9, ap10, ap11 };

INSTANTIATE_TEST_CASE_P(ObFIFOAllocatorAlignParamTestInstance, ObFIFOAllocatorAlignParamTest,
                        testing::Values(ap1, ap2, ap3, ap4, ap5, ap6, ap7, ap8, ap9, ap10, ap11));

struct AllocParam ps1 = { page_size, 1 };
struct AllocParam ps2 = { page_size - 8 * 3, 1 };
struct AllocParam ps3 = { page_size - 8 * 3, 2 };
struct AllocParam ps4 = { page_size - 8 * 3, 4 };
struct AllocParam ps5 = { page_size + 1, 8 };
struct AllocParam ps6 = { page_size + 155, 8 };
struct AllocParam ps7 = { page_size * 2, 16 };
struct AllocParam ps8 = { page_size * 23, 16 };
struct AllocParam ps9 = { page_size * 99, 16 };
struct AllocParam ps10 = { page_size * 99, 256 };
struct AllocParam ps11 = { page_size, 1024 * 8 };
struct AllocParam alloc_param_special[11] = { ps1, ps2, ps3, ps4, ps5, ps6, ps7, ps8, ps9, ps10, ps11 };

class ObFIFOAllocatorSpecialPageListTest
{
public:
  ObFIFOAllocatorSpecialPageListTest()
  {
    mock_allocator_ = new MockAllocator();
    fa_ = new ObFIFOAllocator();
    EXPECT_TRUE(OB_SUCCESS == fa_->init(mock_allocator_ , page_size, default_memattr, init_size, idle_size));
  }
  virtual ~ObFIFOAllocatorSpecialPageListTest()
  {
    delete fa_;
    delete mock_allocator_;
  }

public:
  void check_special_list();
  ObFIFOAllocator *fa_;
private:
  MockAllocator *mock_allocator_;
};

void ObFIFOAllocatorSpecialPageListTest::check_special_list()
{
  LIB_ALLOC_LOG(DEBUG, "special page list is : START =================");
  int64_t index = 0;
  DLIST_FOREACH_NORET(iter, fa_->special_page_list_) {
    LIB_ALLOC_LOG(DEBUG, "Iterate Special List ", K(index), "special page ", iter, K(iter->get_next()));
    ++index;
  }
  LIB_ALLOC_LOG(DEBUG, "special page list is : END  =================");
}

// [5] After assigning the special page continuously, check the list of special_page
TEST(ObFIFOAllocatorSpecialPageListTest, special_list_test)
{
  ObFIFOAllocatorSpecialPageListTest special_list_test;
  queue<void *> ptr_queue;
  void *p = NULL;
  void *ptr_to_free = NULL;

  for (int64_t i = 0; i < 11; i++) {
    p = special_list_test.fa_->alloc_align(alloc_param_special[i].size_, alloc_param_special[i].align_);
    if (NULL != p) {
      ptr_queue.push(p);
    }
    special_list_test.check_special_list();
  }

  ROUTINE_CHECK_PTR(special_list_test.fa_);

  while (ptr_queue.size() > 0) {
    ptr_to_free = ptr_queue.front();
    ptr_queue.pop();
    assert(ptr_to_free != NULL);
    special_list_test.fa_->free(ptr_to_free);
    ROUTINE_CHECK_PTR(special_list_test.fa_);
    special_list_test.check_special_list();
  }
}

// [6] Test the special page list of non-fifo mode
TEST(ObFIFOAllocatorSpecialPageListTest, non_fifo_special_list_test)
{
  ObFIFOAllocatorSpecialPageListTest special_list_test;
  vector<void *> ptr_vector;
  void *p = NULL;
  void *ptr_to_free = NULL;
  int64_t pos = 0;

  for (int64_t i = 0; i < 11; i++) {
    p  = special_list_test.fa_->alloc_align(alloc_param_special[i].size_,
                                            alloc_param_special[i].align_);
    if (NULL != p) {
      ptr_vector.push_back(p);
    }
    ROUTINE_CHECK_PTR(special_list_test.fa_);
    special_list_test.check_special_list();
  }

  ROUTINE_CHECK_PTR(special_list_test.fa_);
  while (ptr_vector.size() > 0) {
    pos = (rand() + 1) % ptr_vector.size();
    ptr_to_free = ptr_vector[pos];
    ptr_vector.erase(ptr_vector.begin() + pos);
    special_list_test.fa_->free(ptr_to_free);
    ROUTINE_CHECK_PTR(special_list_test.fa_);
    special_list_test.check_special_list();
  }
}

class ObFIFOAllocatorNormalPageListTest
{
public:
  ObFIFOAllocatorNormalPageListTest()
  {
    mock_allocator_ = new MockAllocator();
    fa_ = new ObFIFOAllocator();
    EXPECT_TRUE(OB_SUCCESS == fa_->init(mock_allocator_, page_size, default_memattr, init_size, idle_size));
  }
  virtual ~ObFIFOAllocatorNormalPageListTest()
  {
    delete fa_;
    delete mock_allocator_;
  }

  bool check_align(void *p, int64_t align)
  {
    int64_t pint = reinterpret_cast<int64_t>(p);
    pint = pint & (align - 1);
    bool is_align = (pint == 0);
    return is_align;
  }

  int64_t get_offset(void *p)
  {
    char *p_addr = static_cast<char *>(p);
    char *current_page_addr = reinterpret_cast<char *>(fa_->current_using_);
    int64_t diff_offset = p_addr - current_page_addr;
    LIB_ALLOC_LOG(DEBUG, "get_offset ", K((void *)p_addr), K((void *)current_page_addr));
    return diff_offset;
  }

  MockAllocator *get_mock_allocator() { return mock_allocator_; }

  void print_normal_list()
  {
    LIB_ALLOC_LOG(DEBUG, "Iterate Page Using List Start");
    int64_t index = 0;
    DLIST_FOREACH_NORET(iter, fa_->using_page_list_) {
      LIB_ALLOC_LOG(DEBUG, "* * * * * *   Page Using ", K(index), K(iter));
      ++index;
    }
    LIB_ALLOC_LOG(DEBUG, "Iterate Page Using List END");
  }

  void print_free_list()
  {
    int64_t index = 0;
    LIB_ALLOC_LOG(DEBUG, "Iterate Page Free List Start");
    DLIST_FOREACH_NORET(iter, fa_->free_page_list_) {
      LIB_ALLOC_LOG(DEBUG, "* * * * * *  Page Free ", K(index), K(iter));
      ++index;
    }
    LIB_ALLOC_LOG(DEBUG, "Iterate Page Free List END");
  }

  int64_t generate_align()
  {
    return (1 << (rand() % 6));
  }

  int64_t generate_size(int64_t align)
  {
    int64_t start_offset = sizeof(ObFIFOAllocator::NormalPageHeader) + sizeof(
                               ObFIFOAllocator::ALLOC_HEADER) + sizeof(int64_t);
    int64_t after_align = (start_offset + align - 1);
    int64_t max_free_space = page_size - after_align;
    return rand() % max_free_space + 1;
  }

  int64_t generate_size()
  {
    const int64_t MAX_SIZE = page_size * 2 / 4; //TODO
    return rand() % MAX_SIZE + 1;
  }

public:
  ObFIFOAllocator *fa_;
private:
  MockAllocator *mock_allocator_;
};

// [7] Test the continuous allocation of ordinary pages, printing the using list and the free list.
TEST(ObFIFOAllocatorTest, normal_page_list)
{
  LIB_ALLOC_LOG(DEBUG, "===== Start Normal List Test ====");
  ObFIFOAllocatorNormalPageListTest normal_list_test;
  queue<void *> ptr_queue;
  void *p = NULL;
  void *ptr_to_free = NULL;

  for (int64_t i = 0; i < 11; i++) {
    p = normal_list_test.fa_->alloc_align(alloc_param_normal[i].size_, alloc_param_normal[i].align_);
    ROUTINE_CHECK_PTR(normal_list_test.fa_);
    LIB_ALLOC_LOG(DEBUG, "fa.used_ ", K(normal_list_test.fa_->used()));
    LIB_ALLOC_LOG(DEBUG, "used_  total_ ", K(normal_list_test.fa_->used()));
    LIB_ALLOC_LOG(DEBUG, "used_  total_ ", K(normal_list_test.fa_->total()));
    if (p != NULL) {
      ptr_queue.push(p);
    }
    normal_list_test.print_normal_list();
    normal_list_test.print_free_list();
  }

  int64_t free_count = 1;
  while (ptr_queue.size() > 0) {
    free_count++;
    ptr_to_free = ptr_queue.front();
    ptr_queue.pop();
    normal_list_test.fa_->free(ptr_to_free);
    ROUTINE_CHECK_PTR(normal_list_test.fa_);
    normal_list_test.print_normal_list();
    normal_list_test.print_free_list();
    LIB_ALLOC_LOG(DEBUG, "used_  total_ ", K(normal_list_test.fa_->used()));
    LIB_ALLOC_LOG(DEBUG, "used_  total_ ", K(normal_list_test.fa_->total()));
  }
}

// [8] test normal allocation. more test_count
TEST(ObFIFOAllocatorTest, normal_page_list_more)
{
  ObFIFOAllocatorNormalPageListTest normal_list_test;
  int64_t test_count = 10;
  int64_t size = 0;
  int64_t align = 0;
  void *p = NULL;
  queue<void *> ptr_queue;
  void *ptr_to_free = NULL;
  int64_t free_count = 0;

  for (int64_t i = 0; i < test_count; ++i) {
    LIB_ALLOC_LOG(DEBUG, "---- Test Index ----", K(i));
    align = normal_list_test.generate_align();
    size = normal_list_test.generate_size(align);
    fprintf(stdout, "[%ld] size = %ld, align = %ld\n", i, size, align);
    p = normal_list_test.fa_->alloc_align(size, align);
    ROUTINE_CHECK_PTR(normal_list_test.fa_);
    LIB_ALLOC_LOG(DEBUG, "used_  total_ ", K(normal_list_test.fa_->used()));
    LIB_ALLOC_LOG(DEBUG, "used_  total_ ", K(normal_list_test.fa_->total()));
    LIB_ALLOC_LOG(DEBUG, "FIFO alloc return ", K(p));
    EXPECT_TRUE(p != NULL);
    ptr_queue.push(p);
    if (rand() % 7 >= 2) {
      ptr_to_free = ptr_queue.front();
      LIB_ALLOC_LOG(DEBUG, "FIFO free ", K(ptr_to_free));
      ptr_queue.pop();
      normal_list_test.fa_->free(ptr_to_free);
      ++free_count;
      LIB_ALLOC_LOG(DEBUG, "used_  total_ ", K(normal_list_test.fa_->used()));
      LIB_ALLOC_LOG(DEBUG, "used_  total_ ", K(normal_list_test.fa_->total()));
    }
  }

  LIB_ALLOC_LOG(DEBUG, "batch free ", K(free_count));
  while (ptr_queue.size() > 0) {
    ptr_to_free = ptr_queue.front();
    ptr_queue.pop();
    normal_list_test.fa_->free(ptr_to_free);
    ROUTINE_CHECK_PTR(normal_list_test.fa_);
    LIB_ALLOC_LOG(DEBUG, "FIFO free ", K(ptr_to_free));
    ++free_count;
  }
}


// [9] Test the normal page list in non-fifo mode.
TEST(ObFIFOAllocatorNormalPageListTest , non_fifo_normal_list_test)
{
  ObFIFOAllocatorNormalPageListTest normal_list_test;
  vector<void *> ptr_vector;
  void *p = NULL;
  void *ptr_to_free = NULL;
  int64_t pos = 0;

  for (int64_t i = 0; i < 11; i++) {
    p = normal_list_test.fa_->alloc_align(alloc_param_normal[i].size_, alloc_param_normal[i].align_);
    if (p != NULL) {
      ptr_vector.push_back(p);
    }
    normal_list_test.print_normal_list();
    normal_list_test.print_free_list();
  }

  srand(1);
  while (ptr_vector.size() > 0) {
    pos = rand() % ptr_vector.size();
    ptr_to_free = ptr_vector[pos];
    ptr_vector.erase(ptr_vector.begin() + pos);
    normal_list_test.fa_->free(ptr_to_free);
    normal_list_test.print_normal_list();
    normal_list_test.print_free_list();
  }
}

// [10] same as [9] but more
TEST(ObFIFOAllocatorTest, non_fifo_normal_list_test_more)
{
  ObFIFOAllocatorNormalPageListTest normal_list_test;
  int64_t test_count = 100;
  int64_t size = 0;
  int64_t align = 0;
  void *p = NULL;
  vector<void *> ptr_vector;
  void *ptr_to_free = NULL;

  for (int64_t i = 0; i < test_count; ++i) {
    align = normal_list_test.generate_align();
    if (align > 128) {
      align = 64;
    }
    size = normal_list_test.generate_size(align);
    p = normal_list_test.fa_->alloc_align(size, align);
    ROUTINE_CHECK_PTR(normal_list_test.fa_);
    ptr_vector.push_back(p);
    LIB_ALLOC_LOG(INFO, "nijia", K(i), KP(p));
    EXPECT_TRUE(p != NULL);
  }

  for (int64_t i = 0; i < test_count; i++) {
    ptr_to_free = ptr_vector[(i + test_count / 2) % test_count];
    normal_list_test.fa_->free(ptr_to_free);
    ROUTINE_CHECK_PTR(normal_list_test.fa_);
  }
}

// [11] test reset function
TEST(ObFIFOAllocatorTest, reset)
{
  MockAllocator *mock_allocator = new MockAllocator();
  ObFIFOAllocator *fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  queue<void *> ptr_queue;
  int64_t alloc_count = 4;
  void *p = NULL;

  for (int64_t i = 0; i < alloc_count; i++) {
    p = fa->alloc(page_size / 2);
    EXPECT_FALSE(p == NULL);
    ptr_queue.push(p);
  }

  for (int i = 0; i < alloc_count; i++) {
    p = ptr_queue.front();
    ptr_queue.pop();
    fa->free(p);
  }

  fa->reset();

  delete fa;
  delete mock_allocator;
}

// When testing using ObConcurrentFIFOAllocator in a multithreaded environment. Expect: Print an ERROR log.
ObFIFOAllocator *global_fa;
pthread_cond_t can_alloc_cond;
pthread_cond_t can_free_cond;
volatile bool can_alloc_flag = false; // can alloc
volatile bool can_free_flag = false; // can free
pthread_mutex_t alloc_mutex;
pthread_mutex_t free_mutex;
void *allocated_addr = NULL;
MockAllocator *mock_allocator;

void *owner_routine(void  *data)
{
  //data = data;
  ObFIFOAllocator *fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  global_fa = fa;
  allocated_addr = fa->alloc(1);
  EXPECT_FALSE(allocated_addr == NULL);
  pthread_mutex_lock(&alloc_mutex);
  can_alloc_flag = true;
  LIB_ALLOC_LOG(DEBUG, "can_alloc_cond signal");
  pthread_cond_signal(&can_alloc_cond);
  pthread_mutex_unlock(&alloc_mutex);
  return NULL;
}

void *thief_routine(void *data)
{
  //data = data;
  pthread_mutex_lock(&alloc_mutex);
  while (!can_alloc_flag) {
    LIB_ALLOC_LOG(DEBUG, "wait can alloc");
    pthread_cond_wait(&can_alloc_cond, &alloc_mutex);
  }
  pthread_mutex_unlock(&alloc_mutex);
  LIB_ALLOC_LOG(DEBUG, "thief start to reuse ObConcurrentFIFOAllocator");
  ObFIFOAllocator *fa = global_fa;
  void *ptr = fa->alloc(1);
  EXPECT_TRUE(ptr == NULL);
  pthread_mutex_lock(&free_mutex);
  can_free_flag = true;
  pthread_cond_signal(&can_free_cond);
  pthread_mutex_unlock(&free_mutex);
  return NULL;
}

// this test case does not work for new policy of restricting using in multi-thread
/*
TEST(ObFIFOAllocatorTest, multithread_test)
{
  pthread_t owner;
  pthread_t thief;

  pthread_cond_init(&can_alloc_cond, NULL);
  pthread_cond_init(&can_free_cond, NULL);
  pthread_mutex_init(&alloc_mutex, NULL);
  pthread_mutex_init(&free_mutex, NULL);
  mock_allocator = new MockAllocator();
  pthread_create(&owner, NULL, owner_routine, NULL);
  pthread_create(&thief, NULL, thief_routine, NULL);
  pthread_mutex_lock(&free_mutex);
  while (!can_free_flag) {
    LIB_ALLOC_LOG(DEBUG, "wait can free");
    pthread_cond_wait(&can_free_cond, &free_mutex);
  }
  pthread_mutex_unlock(&free_mutex);
  global_fa->free(allocated_addr);
  delete global_fa;
  delete mock_allocator;
  pthread_join(owner, NULL);
  pthread_join(thief, NULL);
  pthread_cond_destroy(&can_alloc_cond);
  pthread_cond_destroy(&can_free_cond);
  pthread_mutex_destroy(&free_mutex);
  pthread_mutex_destroy(&alloc_mutex);
}
*/

// [12] Test double free
TEST(ObFIFOAllocator, double_free)
{
  MockAllocator *mock_allocator = new MockAllocator();
  ObFIFOAllocator *fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  void *p = NULL;
  p = fa->alloc(10);
  fa->free(p);
  fa->free(p);
  delete fa;
  delete mock_allocator;
}

// [13] Test the free list. Continuously allocate a number of page_size/2 memory blocks, release them continuously, and then re-apply for allocation.
TEST(ObFIFOAllocator, free_list_test)
{
  MockAllocator *mock_allocator = new MockAllocator();
  ObFIFOAllocator *fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  queue<void *> ptr_queue;
  void *p = NULL;
  int64_t test_count = 6;

  for (int64_t i = 0; i < test_count; i++) {
    p = fa->alloc(page_size / 2);
    ROUTINE_CHECK_PTR(fa);
    ptr_queue.push(p);
    LIB_ALLOC_LOG(DEBUG, "queue.push() ", K(p));
  }

  for (int64_t i = 0; i < test_count; i++) {
    p = ptr_queue.front();
    ptr_queue.pop();
    LIB_ALLOC_LOG(DEBUG, "queue.pop() ", K(p));
    fa->free(p);
    ROUTINE_CHECK_PTR(fa);
  }

  for (int64_t i = 0; i < test_count; i++) {
    p = fa->alloc(page_size / 2);
    ROUTINE_CHECK_PTR(fa);
    ptr_queue.push(p);
    LIB_ALLOC_LOG(DEBUG, "queue.push() ", K(p));
  }

  for (int64_t i = 0; i < test_count; i++) {
    p = ptr_queue.front();
    ptr_queue.pop();
    LIB_ALLOC_LOG(DEBUG, "queue.pop() ", K(p));
    fa->free(p);
    ROUTINE_CHECK_PTR(fa);
  }

  delete fa;
  delete mock_allocator;
}

// [14] When there are unreleased pages but the FIFOAllcoator is destroyed. Expect: Print Error log, dump page using.
// When executed under valgrind, there is a memory leak (predicted). memory leak
TEST(ObFIFOAllocator, dump_using_when_dctor)
{
  MockAllocator *mock_allocator = new MockAllocator();
  ObFIFOAllocator *fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  queue<void *> ptr_queue;
  int64_t test_count = 10;
  void *p = NULL;

  for (int64_t i = 0; i < test_count; i++) {
    p = fa->alloc_align(page_size / 2, 64);
    ROUTINE_CHECK_PTR(fa);
    ASSERT_TRUE(NULL != p);
    ptr_queue.push(p);
  }

  fa->alloc_align(page_size * 2, 64);
  fa->alloc_align(page_size * 3, 64);
  fa->alloc_align(page_size * 4, 64);

  EXPECT_TRUE(mock_allocator->is_leak());

  // "Won't Fix" for Coverity
  // Coverity treat these code as dead code because of ASSERT_TRUE(...) in line 932.
  // thus think there is a memory leak because we do not delete fa
  delete fa;
  delete mock_allocator;
}

// use gettimeofday() before, but the precise is not enough(there is some 0).
int64_t get_current_time()
{
  uint32_t low, high;
  __asm__ __volatile__("rdtsc":"=a"(low), "=d"(high));
  return ((uint64_t)high << 32) | low;
}

// [15] Comprehensive test. May allocate normal or special, and record the alloc/free time.
TEST(ObFIFOAllocatorTest, alloc_free_with_perftest)
{
  int64_t timestamp1 = 0;
  int64_t timestamp2 = 0;
  int64_t timestamp3 = 0;
  int64_t timestamp4 = 0;
  ObFIFOAllocatorNormalPageListTest normal_list_test;
  int64_t test_count = 10;
  int64_t size = 0;
  int64_t align = 0;
  void *p = NULL;
  queue<void *> ptr_queue;
  void *ptr_to_free;
  int64_t alloc_index = 0;
  int64_t free_index = 0;

  OB_LOGGER.set_log_level("INFO");
  for (int64_t i = 0; i < test_count; ++i) {
    LIB_ALLOC_LOG(DEBUG, "\n\n---- Test Index ----", K(i), K(alloc_index));
    ++alloc_index;
    align = normal_list_test.generate_align();
    size = normal_list_test.generate_size();
    timestamp1 = get_current_time();
    p = normal_list_test.fa_->alloc_align(size, align);
    ROUTINE_CHECK_PTR(normal_list_test.fa_);
    timestamp2 = get_current_time();
    LIB_ALLOC_LOG(INFO, "PERF_TAG alloc", K(timestamp2 - timestamp1));
    EXPECT_TRUE(p != NULL);
    ptr_queue.push(p);
    LIB_ALLOC_LOG(DEBUG, "TAG_QUEUE alloc return ----", K(p));
    LIB_ALLOC_LOG(DEBUG, "alloc_align ", K(size), K(align));
    if (rand() % 6 >= 2) {
      ptr_to_free = ptr_queue.front();
      ptr_queue.pop();
      LIB_ALLOC_LOG(DEBUG, "\n\nTAG_QUEUE free return ----", K(ptr_to_free), K(free_index));
      ++free_index;
      timestamp3 = get_current_time();
      normal_list_test.fa_->free(ptr_to_free);
      ROUTINE_CHECK_PTR(normal_list_test.fa_);
      timestamp4 = get_current_time();
      LIB_ALLOC_LOG(INFO, "PERF_TAG free ", K(timestamp4 - timestamp3));
    }
  }

  LIB_ALLOC_LOG(DEBUG, "--- Batch free");
  while (0 != ptr_queue.size()) {
    ptr_to_free = ptr_queue.front();
    ptr_queue.pop();
    ++free_index;
    timestamp3 = get_current_time();
    normal_list_test.fa_->free(ptr_to_free);
    ROUTINE_CHECK_PTR(normal_list_test.fa_);
    timestamp4 = get_current_time();
    LIB_ALLOC_LOG(INFO, "PERF_TAG free ", K(timestamp4 - timestamp3));
  }
  OB_LOGGER.set_log_level("DEBUG");
}

// [17] Performance comparison test vs thread-safe ObConcurrentFIFOAllocator
TEST(PerformanceTest, performance_compare_test)
{
  MockAllocator *mock_allocator = new MockAllocator();
  ObFIFOAllocator *new_fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == new_fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  ObConcurrentFIFOAllocator *old_fa = new ObConcurrentFIFOAllocator();
  old_fa->init(100 * idle_size, idle_size, page_size);
  int64_t timestamp1 = 0;
  int64_t timestamp2 = 0;
  int64_t timestamp3 = 0;
  //int64_t align;
  int64_t size;
  void *new_ptr = NULL;
  void *old_ptr = NULL;
  queue<void *> new_ptr_queue;
  queue<void *> old_ptr_queue;
  ObFIFOAllocatorNormalPageListTest normal_list_test;
  int64_t test_count = 100;

  OB_LOGGER.set_log_level("INFO");
  for (int64_t i = 0; i < test_count; i++) {
    //align = normal_list_test.generate_align();
    size = normal_list_test.generate_size();

    timestamp1 = get_current_time();
    new_ptr = new_fa->alloc(size);
    timestamp2 = get_current_time();
    old_ptr = old_fa->alloc(size);
    timestamp3 = get_current_time();
    LIB_ALLOC_LOG(INFO, "PERF_CMP_TAG performance_new-alloc is (t2-t1), old-alloc is (t3-t2)",
                      K(timestamp2 - timestamp1), K(timestamp3 - timestamp2));
    new_ptr_queue.push(new_ptr);
    old_ptr_queue.push(old_ptr);
  }

  while (new_ptr_queue.size() > 0) {
    new_ptr = new_ptr_queue.front();
    new_ptr_queue.pop();
    old_ptr = old_ptr_queue.front();
    old_ptr_queue.pop();

    timestamp1 = get_current_time();
    new_fa->free(new_ptr);
    timestamp2 = get_current_time();
    old_fa->free(old_ptr);
    timestamp3 = get_current_time();
    LIB_ALLOC_LOG(INFO, "PERF_CMP_TAG performance_new-free is (t2-t1), old-free is (t3-t2)",
                      K(timestamp2 - timestamp1), K(timestamp3 - timestamp2));
  }
  OB_LOGGER.set_log_level("DEBUG");
  delete new_fa;
  delete old_fa;
  delete mock_allocator;
}

TEST(MockAllocatorDead, mock_alloc_fail_tolerant)
{
  MockAllocator *mock_allocator = new MockAllocator();
  mock_allocator->set_status(false);
  ObFIFOAllocator *fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  mock_allocator->set_status(true);

  void* p = fa->alloc(page_size / 2);
  EXPECT_TRUE(p != NULL);
  fa->free(p);
  delete fa;
  delete mock_allocator;
}

TEST(TestModID, label)
{
  MockAllocator *mock_allocator = new MockAllocator();
  ObFIFOAllocator *fa = new ObFIFOAllocator();
  EXPECT_TRUE(OB_SUCCESS == fa->init(mock_allocator, page_size, default_memattr, init_size, idle_size));
  void* p = fa->alloc(page_size / 2);
  fa->set_label("FIFO");
  EXPECT_TRUE(p != NULL);
  fa->free(p);
  delete fa;
  delete mock_allocator;
}

TEST(TestFIFO, init_idle_max)
{
  MockAllocator mock_allocator;
  ObFIFOAllocator fa;
  // invalid arg
  ASSERT_NE(OB_SUCCESS, fa.init(&mock_allocator, 0, default_memattr));
  ASSERT_NE(OB_SUCCESS, fa.init(&mock_allocator, page_size, default_memattr, -1));
  ASSERT_NE(OB_SUCCESS, fa.init(&mock_allocator, page_size, default_memattr, 0, -1));
  ASSERT_NE(OB_SUCCESS, fa.init(&mock_allocator, page_size, default_memattr, 0, 0, -1));
  ASSERT_NE(OB_SUCCESS, fa.init(&mock_allocator, page_size, default_memattr, 1, 0, 0));
  ASSERT_NE(OB_SUCCESS, fa.init(&mock_allocator, page_size, default_memattr, 0, 1, 0));
  ASSERT_NE(OB_SUCCESS, fa.init(&mock_allocator, page_size, default_memattr, 0,  0, page_size - 1));
  ASSERT_NE(OB_SUCCESS, fa.init(&mock_allocator, page_size, default_memattr, 2,  1, page_size - 1));
  // succ
  ASSERT_EQ(OB_SUCCESS, fa.init(&mock_allocator, page_size, default_memattr, 0, page_size, page_size));
  ASSERT_EQ(OB_INIT_TWICE, fa.init(&mock_allocator, page_size, default_memattr, 0, page_size, page_size));

  // init && idle && max
  {
    ObFIFOAllocator fa;
    int64_t init_size = page_size * 10;
    ASSERT_EQ(OB_SUCCESS, fa.init(&mock_allocator, page_size, default_memattr, init_size));
    ASSERT_TRUE(fa.normal_total() >= init_size);
    int64_t total = fa.normal_total();
    ASSERT_EQ(OB_SUCCESS, fa.set_idle(init_size * 2, false));
    ASSERT_EQ(total, fa.normal_total());
    ASSERT_EQ(OB_SUCCESS, fa.set_idle(init_size * 2, true));
    ASSERT_NE(total, fa.normal_total());
    ASSERT_TRUE(fa.normal_total() >= init_size * 2);
    vector<void*> ptrs;
    while (ptrs.size() * 512 < init_size * 1.5) {
      void *ptr = fa.alloc(512);
      ASSERT_NE(nullptr, ptr);
      ptrs.push_back(ptr);
    }
    for (int64_t i = 0; i < ptrs.size(); i++) {
      fa.free(ptrs[i]);
    }
    ASSERT_EQ(0, fa.normal_used());
    ASSERT_EQ(OB_SUCCESS, fa.set_idle(init_size, true));
    ASSERT_TRUE(fa.normal_total() >= init_size);
    ASSERT_LT(fa.normal_total(), 2 * init_size);

    total = fa.total();
    ptrs.clear();
    fa.set_max(init_size, false);
    while (true) {
      void *ptr = fa.alloc(512);
      if (!ptr) break;
      ptrs.push_back(ptr);
    }
    ASSERT_EQ(OB_SUCCESS, fa.set_max(init_size / 2, true));
    ASSERT_EQ(total, fa.total());
    for (int64_t i = 0; i < ptrs.size(); i++) {
      fa.free(ptrs[i]);
    }
    // For simplicity of implementation, set_max shrink will not process current_using page
    ASSERT_TRUE(fa.normal_total() <= init_size / 2 + page_size);
  }
}

TEST(TestFIFO, max)
{
  MockAllocator mock_allocator;
  ObFIFOAllocator fa;
  int64_t pz = (64L << 10) + 128;
  int64_t init = pz * 16;
  int64_t idle = pz * 32;
  int64_t max = pz * 64;
  ASSERT_EQ(OB_SUCCESS, fa.init(&mock_allocator, pz, default_memattr, init, idle, max));
  int64_t normal_total = fa.normal_total();
  int64_t total = fa.total();
  ASSERT_GE(normal_total , init);
  ASSERT_LE(normal_total ,init);
  ASSERT_LE(normal_total , max);
  void *ptr = fa.alloc(64L << 10);
  ASSERT_NE(ptr, nullptr);
  ASSERT_NE(fa.used(), 0);
  ASSERT_EQ(fa.normal_total(), normal_total);
  ASSERT_EQ(fa.total(), total);
  fa.free(ptr);
  ptr = fa.alloc(pz);
  ASSERT_NE(ptr, nullptr);
  ASSERT_EQ(fa.normal_total(), normal_total);
  ASSERT_GT(fa.total(), total);
  fa.free(ptr);
  ASSERT_EQ(fa.used(), 0);

  vector<void*> ptrs;
  while (true) {
    void *ptr = fa.alloc(64L << 10);
    if (nullptr == ptr) break;
    ptrs.push_back(ptr);
  }
  ASSERT_LE(fa.total() , max);
  ASSERT_GE(fa.total() , max - pz);
  ASSERT_LE(fa.used() , max);
  ASSERT_GE(fa.used() , max - pz);
  for (int64_t i = 0; i < ptrs.size(); i++) {
    fa.free(ptrs[i]);
  }
  ASSERT_GE(fa.total() , idle);
  ASSERT_LE(fa.total() , idle + pz);
  ASSERT_EQ(fa.used(), 0);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_fifo_allocator.log", true, true);
  ::testing::InitGoogleTest(&argc, argv);
  int ret = 0;
  ret = RUN_ALL_TESTS();
  LIB_ALLOC_LOG(DEBUG, "glibc alloc/free count = ", K(glibc_alloc_count), K(glibc_free_count));
  return ret;
}
