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

#include "lib/allocator/ob_malloc.h"
#include "lib/alloc/object_mgr.h"
#include "lib/utility/ob_test_util.h"
#include "lib/coro/testing.h"
#include <gtest/gtest.h>

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace std;

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class TestObjectMgr : public ::testing::Test {
public:
  void* Malloc(uint64_t size)
  {
    // void *p = NULL;
    // AObject *obj = os_.alloc_object(size);
    // if (obj != NULL) {
    //   p = obj->data_;
    // }
    // return p;
    return oceanbase::common::ob_malloc(size);
  }

  void Free(void* ptr)
  {
    // AObject *obj = reinterpret_cast<AObject*>(
    //     (char*)ptr - AOBJECT_HEADER_SIZE);
    // os_.free_object(obj);
    return oceanbase::common::ob_free(ptr);
  }

  // protected:
  //   ObjectMgr<1> om_;
};

TEST_F(TestObjectMgr, Basic2)
{
  cotesting::FlexPool(
      [] {
        void* p[128] = {};
        int64_t cnt = 1L << 18;
        uint64_t sz = 1L << 4;

        while (cnt--) {
          int i = 0;
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          p[i++] = ob_malloc(sz);
          while (i--) {
            ob_free(p[i]);
          }
          // sz = ((sz | reinterpret_cast<size_t>(p[0])) & ((1<<13) - 1));
        }

        cout << "done" << endl;
      },
      4)
      .start();
}

TEST_F(TestObjectMgr, TestName)
{
  void* p[128];

  Malloc(327800);
  Malloc(65536);
  Malloc(49408);
  Malloc(12376);
  Malloc(65344);

  p[1] = Malloc(2097152);
  Free(p[1]);

  Malloc(49248);
  Malloc(3424);
  Malloc(8);
  Malloc(3072);
  Malloc(176);
  p[11] = Malloc(96);
  p[10] = Malloc(65536);
  p[7] = Malloc(65568);
  p[6] = Malloc(96);
  p[5] = Malloc(65536);
  Malloc(2096160);
  Malloc(16);
  Malloc(65536);
  p[9] = Malloc(96);
  p[8] = Malloc(65536);
  p[4] = Malloc(65568);
  p[3] = Malloc(96);
  p[2] = Malloc(65536);
  Malloc(16);
  Free(p[2]);
  Free(p[3]);
  Free(p[4]);
  Free(p[5]);
  Free(p[6]);
  Free(p[7]);
  Free(p[8]);
  Free(p[9]);
  Free(p[10]);
  Free(p[11]);
  Malloc(12384);
  Malloc(12384);
  Malloc(12384);
  Malloc(96);
  Malloc(65536);
  p[13] = Malloc(96);
  p[12] = Malloc(65536);
  Free(p[12]);
  Free(p[13]);
  p[15] = Malloc(96);
  p[14] = Malloc(65536);
  Free(p[14]);
  Free(p[15]);
  p[17] = Malloc(96);
  p[16] = Malloc(65536);
  Malloc(65536);
  Free(p[16]);
  Free(p[17]);
  p[19] = Malloc(96);
  p[18] = Malloc(65536);
  Free(p[18]);
  Free(p[19]);
  Malloc(96);
}
