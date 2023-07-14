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

#ifndef OCEANBASE_SHARE_OB_DISPLAY_LIST
#define OCEANBASE_SHARE_OB_DISPLAY_LIST
#include <stdint.h>
#include "lib/container/ob_iarray.h"        // ObIArray
#include "lib/container/ob_se_array.h"      // ObSEArrayImpl
#include "lib/string/ob_string.h"           // ObString
#include "lib/ob_errno.h"                   // OB_xx
#include "lib/utility/ob_print_utils.h"     // COMMON_LOG
#include "lib/utility/ob_macro_utils.h"     // FOREACH_CNT_X
#include "lib/allocator/page_arena.h"       // ObArenaAllocator
#include "share/ob_errno.h"                 // KR, ob_error_name

namespace oceanbase
{
namespace share
{
/*
 * Parse List of objects to ObString using comma connected, which can be displayed in inner table or views
 *
 * Object type should inherit from ObDisplayType to implement all its interface functions.
 *
 * Pair function: display_str_to_list(...)
 *
 * @param [in] allocator:  allocator used to manage memory of ObString
 * @param [in] list:       List of objects
 * @param [out] str:       comma-connected string
 * @return OB_SUCCESS if success, otherwise failed
 */
template <typename Allocator, typename Type>
int list_to_display_str(Allocator &allocator, const common::ObIArray<Type> &list, common::ObString &str)
{
  int ret = OB_SUCCESS;
  str.reset();
  if (OB_UNLIKELY(list.empty())) {
    // do nothing
  } else {
    char *buf = NULL;
    int64_t pos = 0;
    const int64_t obj_max_str_len = list.at(0).max_display_str_len();
    const int64_t len = list.count() * obj_max_str_len;

    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", KR(ret), K(len), K(obj_max_str_len));
    } else {
      FOREACH_CNT_X(data, list, OB_SUCCESS == ret) {
        if (0 != pos) {
          if (pos + 1 < len) {
            buf[pos++] = ',';
          } else {
            ret = OB_BUF_NOT_ENOUGH;
            COMMON_LOG(WARN, "buffer not enough", KR(ret), K(pos), K(len));
          }
        }
        if (FAILEDx(data->to_display_str(buf, len, pos))) {
          COMMON_LOG(WARN, "to display str failed", KR(ret), KPC(data), K(len), K(pos));
        }
      }
    }

    if (OB_SUCC(ret)) {
      str.assign_ptr(buf, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  return ret;
}

/*
 * Parse comma-connected ObString into a list of objects
 *
 * Pair function with list_to_display_str(...)
 *
 * For more information, please refer to list_to_display_str(..)
 *
 * @param [in] str:   comma-connected string
 * @param [out] list: returned list
 * @return OB_SUCCESS if success, otherwise failed
 */
template <typename Type>
int display_str_to_list(const common::ObString &str, common::ObIArray<Type> &list)
{
  int ret = OB_SUCCESS;
  list.reset();
  common::ObArenaAllocator allocator;
  ObString tmp_str;
  if (OB_UNLIKELY(str.empty())) {
    // do nothing
  } else if (OB_FAIL(ob_write_string(allocator, str, tmp_str, true/*c_style*/))) {
    COMMON_LOG(WARN, "fail to write string", KR(ret), K(str));
  } else {
    char *data_str = NULL;
    char *save_ptr = NULL;
    while (OB_SUCC(ret)) {
      Type data;
      data_str = strtok_r((NULL == data_str ? tmp_str.ptr() : NULL), ",", &save_ptr);
      if (NULL != data_str) {
        if (OB_FAIL(data.parse_from_display_str(ObString::make_string(data_str)))) {
          COMMON_LOG(WARN, "fail to parse from display str", KR(ret), K(data_str), K(data));
        } else if (OB_FAIL(list.push_back(data))) {
          COMMON_LOG(WARN, "fail to push back into list", KR(ret), K(data), K(list));
        }
      } else {
        break;
      }
    } // while
  }
  if (OB_SUCC(ret) && !str.empty() && list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid str", KR(ret), K(str));
  }
  return ret;
}

// A type which can convert to/from display string
//
// If you want to use ObDisplayList, you can define a Type derived from ObDisplayType, then, define ObDisplayList<Type>
//
// If you does not want to inherit ObDisplayType, you must implement all interfaces just like ObDisplayType
class ObDisplayType
{
public:
  // max length of converted display string
  virtual int64_t max_display_str_len() const = 0;

  // to display string which can not include commas ','
  virtual int to_display_str(char *buf, const int64_t len, int64_t &pos) const = 0;

  // parse from display string
  virtual int parse_from_display_str(const common::ObString &str) = 0;
};


// ObDisplayList: a list which can convert to string or parse from string
template<typename T, int64_t LOCAL_ARRAY_SIZE = 1, typename BlockAllocatorT = ModulePageAllocator, bool auto_free = false>
class ObDisplayList : public common::ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>
{
public:
  // use ObSEArrayImpl constructors
  using common::ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::ObSEArrayImpl;

  ObDisplayList(int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
      const BlockAllocatorT &alloc = BlockAllocatorT("DisplayList")) :
      common::ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>(block_size, alloc)
  {}

  // specify label
  ObDisplayList(const lib::ObLabel &label) :
      common::ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>(label, OB_MALLOC_NORMAL_BLOCK_SIZE)
  {}

  // specify allocator and label
  ObDisplayList(ObIAllocator &alloc, const lib::ObLabel &label) :
      common::ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>
      (OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc, label))
  {}

  // to display string
  // See list_to_display_str(..) for more information
  int to_display_str(ObIAllocator &allocator, ObString &str) const
  {
    return list_to_display_str(allocator, *this, str);
  }

  // parse from display string
  // See display_str_to_list(...) for more information
  int parse_from_display_str(const common::ObString &str)
  {
    return display_str_to_list(str, *this);
  }
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_DISPLAY_LIST
