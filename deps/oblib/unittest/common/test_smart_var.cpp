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
#include <pthread.h>
#include <string>
#include "lib/allocator/ob_malloc.h"
#define TEST_SMART_VAR
#include "common/ob_smart_var.h"
#undef TEST_SMART_VAR

const int64_t s_size = 2 << 20;
using namespace std;

#ifdef S
#undef S
namespace oceanbase
{
namespace common
{

bool has_malloc = false;
bool has_free = false;

bool make_malloc_fail = false;
void *smart_alloc(const int64_t nbyte, const char *label)
{
  has_malloc = true;
  return make_malloc_fail ? nullptr : ob_malloc(nbyte, label);
}

void smart_free(void *ptr)
{
  ob_free(ptr);
  has_free = true;
}

template<int64_t N>
struct Buffer { char buf[N];};

bool in_stack(void *ptr)
{
  void *stack_addr = nullptr;
  size_t stack_size = 0;
  abort_unless(0 == get_stackattr(stack_addr, stack_size));
  return (char*)ptr > (char*)stack_addr && (char*)ptr < (char*)stack_addr + stack_size;
}

void *test(void *)
{
  int ret = OB_SUCCESS;

  // basic
  {
   int v = 0;
   SMART_VAR(int, i, 100) {
     v = i;
   }
   EXPECT_EQ(v, 100);
  }

  // array
  {
    const int LEN = 100;
    char buf[LEN];
    char buf_cmp[LEN];
    memset(buf_cmp, 'A', LEN);
    SMART_VAR(char[LEN], b) {
      EXPECT_EQ(LEN, ARRAYSIZEOF(b));
      memset(b, 'A', LEN);
      memcpy(buf, b, LEN);
    }
    EXPECT_EQ(0, memcmp(buf, buf_cmp, LEN));
  }

  // nested
  {
    int v1, v2 = 0;
    SMART_VAR(int, i, 100) {
      v1 = i;
      SMART_VAR(int, i, 200) {
        v2 = i;
      }
    }
    EXPECT_EQ(v1, 100);
    EXPECT_EQ(v2, 200);
  }

  // construct && deconstruct
  {
    // scalar
    class S
    {
    public:
      S(int &k)
        : k_(k) { k_ += 1; }
      ~S() { k_ += 2; }
      int &k_;
    };
    int k = 0;
    {
      SMART_VAR(S, s, k) {
        UNUSEDx(s);
      }
    }
    EXPECT_EQ(k, 3);

    // array
    class V
    {
    public:
      V()
        : self_k_(2), k_(nullptr) {}
      ~V() { *k_ += self_k_; }
      int self_k_;
      int *k_;
    };
    const int N = 10;
    int ks[N] = {0};
    {
      SMART_VAR(V[N], v) {
        for (int i = 0; i < N; i++) {
          v[i].k_ = &ks[i];
        }
      }
    }
    for (int i = 0; i < N; i++) {
      EXPECT_EQ(ks[i], 2);
    }
  }

  // stack && heap
  {
    // stack
    bool from_stack = false;
    has_malloc = false;
    has_free = false;
    SMART_VAR(Buffer<s_size/2>, buf) {
      from_stack = in_stack(&buf);
    }
    EXPECT_TRUE(from_stack);
    EXPECT_FALSE(has_malloc);
    EXPECT_FALSE(has_free);

    // heap
    {
      bool from_heap = false;
      has_malloc = false;
      has_free = false;
      {
        SMART_VAR(Buffer<s_size/2>, buf) {
          int v;
          from_heap = !in_stack(&buf);
        }
      }
      EXPECT_TRUE(from_heap);
      EXPECT_TRUE(has_malloc);
      EXPECT_TRUE(has_free);
    }

    // make heap alloc fail
    {
      bool has_error = false;
      make_malloc_fail = true;
      {
        SMART_VAR(Buffer<s_size/2>, buf) {
          UNUSEDx(buf);
          EXPECT_TRUE(false);
        } else {
          has_error = true;
        }
      }
      EXPECT_TRUE(has_error);
      make_malloc_fail = false;
    }
  }

  // Overwrite error code
  {
    int err = -10000;
    ret = err;
    int path = 0;
    SMART_VAR(int, i) {
      UNUSEDx(i);
      path = 1;
    } else {
      path = 2;
    }
    EXPECT_EQ(ret, err);
    EXPECT_EQ(path, 2);
    ret = OB_SUCCESS;
  }

  // direct heap
  {
    has_malloc = false;
    int v = 0;
    HEAP_VAR(int, i, 10) {
      v = i;
    }
    EXPECT_TRUE(has_malloc);
    EXPECT_EQ(v, 10);
  }

  // check compile only
  {
    HEAP_VAR(char[100], c) {
    }
  }

  // SMART_VARS && HEAP_VARS
  {
    bool from_stack = false;
    int v = 0;
    {
      SMART_VARS_2((int, i, 10), (int, j, 10)) {
        v = i + j;
        from_stack = in_stack(&i);
        from_stack = from_stack && in_stack(&j);
      }
      EXPECT_TRUE(from_stack);
      EXPECT_EQ(20, v);
    }

    from_stack = false;
    v = 0;
    {
      HEAP_VARS_2((int, i), (int, j)) {
        i = j = 10;
        v = i + j;
        from_stack = in_stack(&i);
        from_stack = from_stack && in_stack(&j);
      }
      EXPECT_FALSE(from_stack);
      EXPECT_EQ(20, v);
    }

    from_stack = false;
    v = 0;
    {
      SMART_VARS_3((int, i), (int, j), (int, k)) {
        i = j = k = 10;
        v = i + j + k;
        from_stack = in_stack(&i);
        from_stack = from_stack && in_stack(&j);
        from_stack = from_stack && in_stack(&k);
      }
      EXPECT_TRUE(from_stack);
      EXPECT_EQ(30, v);
    }

    from_stack = false;
    v = 0;
    {
      HEAP_VARS_3((int, i, 10), (int, j, 10), (int, k, 10)) {
        v = i + j + k;
        from_stack = in_stack(&i);
        from_stack = from_stack && in_stack(&j);
        from_stack = from_stack && in_stack(&k);
      }
      EXPECT_FALSE(from_stack);
      EXPECT_EQ(30, v);
    }

    v = 0;
    {
      HEAP_VARS_2((int, i, (666 + 0)), (Buffer<100>, b)) {
        snprintf(b.buf, sizeof b.buf, "hello %d", i);
        sscanf(b.buf, "hello %d", &v);
      }
      EXPECT_EQ(666, v);
    }
  }

  // array initialization list
  {
    {
      SMART_VAR(char[2], buf, "") {
        EXPECT_EQ(strlen(buf), 0);
      }
    }
    {
      SMART_VAR(int[2], vals, 1, 2) {
        EXPECT_EQ(vals[0], 1);
        EXPECT_EQ(vals[1], 2);
      }
    }
    {
      using Pair = std::pair<int,int>;
      SMART_VAR(Pair[2], pairs, {1,2}, {2,1}) {
        EXPECT_EQ(pairs[0].first, 1);
        EXPECT_EQ(pairs[0].second, 2);
        EXPECT_EQ(pairs[1].first, 2);
        EXPECT_EQ(pairs[1].second, 1);
      }
    }
  }
  return nullptr;
}

template<int64_t N>
void do_alloc()
{
  int ret = OB_SUCCESS;
  char b[N];
  memset(b, reinterpret_cast<std::uintptr_t>(&b[0]) & 0xFF, N); // disable compiler optimize out
  has_malloc = false;
  SMART_VAR(char[(8L<<10)+1], buf) {
    UNUSEDx(buf);
  }
  EXPECT_TRUE(has_malloc);
}

void *test2(void *)
{
  cout << "alloc from heap when stack used large than SMART_VAR_MAX_STACK_USE_SIZE" << endl;
  int ret = OB_SUCCESS;
  bool is_overflow = false;
  int64_t used = 0;
  if (OB_FAIL(check_stack_overflow(is_overflow,
                                   get_reserved_stack_size(),
                                   &used))) {
  }
  ASSERT_EQ(OB_SUCCESS, ret), nullptr;

  constexpr static int64_t N = SMART_VAR_MAX_STACK_USE_SIZE;
  EXPECT_LT(used, N);
  do_alloc<N>();

  return nullptr;
}

TEST(utility, all)
{
  pthread_t th;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, s_size);
  pthread_create(&th, &attr, oceanbase::common::test, nullptr);
  pthread_join(th, nullptr);
  pthread_attr_destroy(&attr);
}

TEST(utility, used_size)
{
  pthread_t th;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, s_size);
  pthread_create(&th, &attr, oceanbase::common::test2, nullptr);
  pthread_join(th, nullptr);
  pthread_attr_destroy(&attr);
}

} // end namespace common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
#endif
