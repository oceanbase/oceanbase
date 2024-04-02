/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_MINER_UTILS_H_
#define OCEANBASE_LOG_MINER_UTILS_H_

#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_se_array.h"          //ObSEArray
#include "lib/string/ob_fixed_length_string.h"
#include "lib/string/ob_string_buffer.h"
#include "libobcdc.h"
#ifndef OB_USE_DRCMSG
#include "ob_cdc_msg_convert.h"
#endif
#include <cstdint>
namespace oceanbase
{
namespace oblogminer
{
typedef common::ObFixedLengthString<common::OB_MAX_DATABASE_NAME_LENGTH> DbName;
typedef common::ObFixedLengthString<common::OB_MAX_TABLE_NAME_LENGTH> TableName;
typedef common::ObFixedLengthString<common::OB_MAX_COLUMN_NAME_LENGTH> ColumnName;
typedef common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH> TenantName;
typedef common::ObSEArray<ObString, 4> KeyArray;

int logminer_str2ll(const char *str, char* &endptr, int64_t &num);
int logminer_str2ll(const char *str, int64_t &num);

int write_keys(const KeyArray &key_arr, common::ObStringBuffer &buffer);
int write_signed_number(const int64_t num, common::ObStringBuffer &buffer);
int write_unsigned_number(const uint64_t num, common::ObStringBuffer &buffer);
int write_string_no_escape(const ObString &str, common::ObStringBuffer &buffer);

bool is_number(const char *str);

const char *empty_str_wrapper(const char *str);

const char *record_type_to_str(const RecordType type);
int64_t record_type_to_num(const RecordType type);
int parse_line(const char *key_str, const char *buf, const int64_t buf_len, int64_t &pos, int64_t &data);
int parse_int(const char *buf, const int64_t data_len, int64_t &pos, int64_t &data);
// allocate memory for value, which is a standard cstring
int parse_line(const char *key_str,
    const char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObIAllocator &alloc,
    char *&value);
// no memory alloc, direct assign contents in buffer to value
int parse_line(const char *key_str,
    const char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObString &value);
// int parse_string(const char *buf, const int64_t data_len, int64_t &pos, ObIAllocator &alloc, char *&value);
int parse_string(const char *buf, const int64_t data_len, int64_t &pos, ObString &value);
int expect_token(const char *buf, const int64_t data_len, int64_t &pos, const char *token);

int deep_copy_cstring(ObIAllocator &alloc, const char *src_str, char *&dst_str);

bool is_trans_end_record_type(const RecordType type);

int uint_to_bit(const uint64_t bit_val, ObStringBuffer &str_val);

int todigit(char c);

template<class Derived, class Base, class ...Args>
int init_component(Base *&ptr, Args&& ...arg)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "CompnentAlloc");
  Derived *tmp_var = OB_NEW(Derived, attr);
  const char *derived_name = typeid(Derived).name();
  const char *base_name = typeid(Base).name();
  if (OB_ISNULL(tmp_var)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOGMNR_LOG(ERROR, "allocate memory for component failed", K(tmp_var),
        K(derived_name), K(base_name));
  } else if (OB_FAIL(tmp_var->init(arg...))) {
    LOGMNR_LOG(ERROR, "init tmp_var failed", K(tmp_var),
      K(derived_name), K(base_name));
  } else {
    ptr = tmp_var;
    LOGMNR_LOG(INFO, "init component finished", K(derived_name), K(base_name));
  }
  return ret;
}

template<class Derived, class Base>
void destroy_component(Base *&ptr)
{
  if (OB_NOT_NULL(ptr)) {
    const char *derived_name = typeid(Derived).name();
    const char *base_name = typeid(Base).name();
    Derived *tmp_ptr = static_cast<Derived*>(ptr);
    tmp_ptr->destroy();
    OB_DELETE(Derived, "ComponentFree", tmp_ptr);
    ptr = nullptr;
    LOGMNR_LOG(INFO, "finished to destroy component", K(derived_name), K(base_name));
  }
}
}
}

#endif