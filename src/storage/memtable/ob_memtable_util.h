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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_UTIL_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_UTIL_

#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/hash/ob_hashset.h"
#include "common/rowkey/ob_store_rowkey.h"

namespace oceanbase
{
using namespace lib;
using namespace common;
namespace memtable
{

typedef common::hash::ObHashSet<uint64_t> ObMemtableSet;

template <typename T>
const char *strarray(const common::ObIArray<T> &array)
{
  static const int64_t BUFFER_NUM = 4;
  static const int64_t BUFFER_SIZE = 4096;
  char *cl_buf = reinterpret_cast<char *>(GET_TSI(char[BUFFER_NUM*BUFFER_SIZE]));
  RLOCAL(uint64_t, i);
  char *buffer = NULL;
  if (OB_LIKELY(cl_buf != nullptr)) {
    char (&BUFFERS)[BUFFER_NUM][BUFFER_SIZE]
        = *reinterpret_cast<char (*)[BUFFER_NUM][BUFFER_SIZE]>(cl_buf);

    int ret = OB_SUCCESS;
    int64_t pos = 0;
    buffer = BUFFERS[i++ % BUFFER_NUM];
    // if (NULL == &array) {
    //   snprintf(buffer, BUFFER_SIZE, "NULL");
    // } else {
      if (OB_FAIL(common::databuff_printf(buffer, BUFFER_SIZE, pos, "["))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); i++) {
          if (OB_FAIL(common::databuff_print_obj(buffer, BUFFER_SIZE, pos, array.at(i)))) {
          } else if (OB_FAIL(common::databuff_printf(buffer, BUFFER_SIZE, pos, ","))) {
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(common::databuff_printf(buffer, BUFFER_SIZE, pos, "]"))) {
        }
      }
    //  }
    //no way to spit out err code, so no log in this func
    (void)ret;
  }
  return buffer;
}

class ObFakeStoreRowKey
{
public:
  ObFakeStoreRowKey(const char *str, const int64_t size)
  {
    for(int64_t i = 0; i < OBJ_CNT; i++) {
      obj_array_[i].set_char_value(str, (ObString::obstr_size_t)size);
    }
    rowkey_.assign(obj_array_, OBJ_CNT);
  }
  ~ObFakeStoreRowKey() {}
  const common::ObStoreRowkey &get_rowkey() const { return rowkey_; }
private:
  static const int64_t OBJ_CNT = 1;
private:
  common::ObStoreRowkey rowkey_;
  ObObj obj_array_[OBJ_CNT];
};

}
}

#endif //OCEANBASE_MEMTABLE_OB_MEMTABLE_UTIL_
