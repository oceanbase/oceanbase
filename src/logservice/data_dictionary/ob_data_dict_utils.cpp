/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
* Meta Dict Struct Define
* This file defines Struct of Meta Dict
*/

#include "ob_data_dict_utils.h"

#include "lib/allocator/ob_malloc.h"          // ob_malloc
#include "logservice/ob_log_handler.h"        // ObLogHandler

using namespace oceanbase::common;

namespace oceanbase
{
namespace datadict
{

void *ob_dict_malloc(const int64_t nbyte, const uint64_t tenant_id)
{
  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = tenant_id;
  mem_attr.label_ = "ObDataDict";
  return ob_malloc(nbyte, mem_attr);
}

void ob_dict_free(void *ptr)
{
  ob_free(ptr);
}

int deserialize_string_array(
    const char *buf,
    const int64_t data_len,
    int64_t &pos,
    ObIArray<ObString> &string_array,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  string_array.reset();

  if (OB_ISNULL(buf)
    || OB_UNLIKELY(data_len <= 0)
    || OB_UNLIKELY(pos < 0)
    || OB_UNLIKELY(data_len <= pos)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid arguments", KR(ret), K(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    DDLOG(WARN, "deserialize count failed", KR(ret));
  } else if (0 == count) {
    //do nothing
  } else {
    void *array_buf = NULL;
    const int64_t alloc_size = count * static_cast<int64_t>(sizeof(ObString));

    if (NULL == (array_buf = allocator.alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "alloc memory failed", KR(ret), K(alloc_size));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        ObString tmp_str;
        ObString str;
        if (OB_FAIL(tmp_str.deserialize(buf, data_len, pos))) {
          DDLOG(WARN, "string deserialize failed", KR(ret));
        } else if (OB_FAIL(deep_copy_str(tmp_str, str, allocator))) {
          DDLOG(WARN, "deep_copy_str failed", KR(ret));
        } else if (OB_FAIL(string_array.push_back(str))) {
          DDLOG(WARN, "push_back failed", KR(ret), K(str));
        }
      }
    }
  }

  return ret;
}

int deep_copy_str(const ObString &src,
    ObString &dest,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  if (src.length() > 0) {
    int64_t len = src.length() + 1;
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(ERROR, "allocate memory fail", KR(ret), K(len));
    } else {
      MEMCPY(buf, src.ptr(), len - 1);
      buf[len - 1] = '\0';
      dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len - 1));
    }
  } else {
    dest.reset();
  }

  return ret;
}

int deep_copy_str_array(
    const ObIArray<ObString> &src_arr,
    ObIArray<ObString> &dest_arr,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t src_arr_size = src_arr.count();
  dest_arr.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < src_arr_size; i++) {
    const ObString &src_str = src_arr.at(i);
    ObString dst_str;
    if (OB_FAIL(deep_copy_str(src_str, dst_str, allocator))) {
      DDLOG(WARN, "deep copy obstring failed", KR(ret), K(src_str), K(src_arr_size), K(i));
    } else if (OB_FAIL(dest_arr.push_back(dst_str))) {
      DDLOG(WARN, "dest str arr push back dst_str failded", KR(ret), K(src_str), K(dst_str), K(i));
    }
  }
  return ret;
}

int check_ls_leader(logservice::ObLogHandler *log_handler, bool &is_leader)
{
  int ret = OB_SUCCESS;
  common::ObRole role = common::ObRole::INVALID_ROLE;
  int64_t proposal_id = 0;

  if (OB_ISNULL(log_handler)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "log_handler_ is invalid", KR(ret));
  } else if (OB_FAIL(log_handler->get_role(role, proposal_id))) {
    DDLOG(WARN, "get ls role fail", K(ret), K(proposal_id));
  } else if (common::ObRole::LEADER == role) {
    is_leader = true;
  } else {
    is_leader = false;
  }

  return ret;
}

} // namespace datadict
} // namespace oceanbase
