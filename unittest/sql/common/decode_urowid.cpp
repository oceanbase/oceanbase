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

#define USING_LOG_PREFIX SQL
#include "lib/rowid/ob_urowid.h"
#include "lib/container/ob_array.h"
#include "lib/number/ob_number_v2.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/encode/ob_base64_encode.h"
#include "common/object/ob_object.h"
#include "share/ob_errno.h"

#include <gtest/gtest.h>
#include <cstdlib>
#include <iostream>
#include <string>
#include <cstring>

using std::cout;
using std::endl;

namespace oceanbase
{
namespace common
{
class TestAllocator: public ObIAllocator
{
public:
  virtual void *alloc(const int64_t size) override
  {
    return std::malloc(size);
  }
  virtual void free(void *ptr) override
  {
    return std::free(ptr);
  }
  virtual void *alloc(const int64_t size, const ObMemAttr &/* not used */)
  {
    return std::malloc(size);
  }
};

void decode_output(const char *rowid_str)
{
  TestAllocator alloc;
  ObURowIDData dec_data;
  ObArray<ObObj> pk_vals;
  ASSERT_EQ(OB_SUCCESS, ObURowIDData::decode2urowid(rowid_str, strlen(rowid_str), alloc, dec_data));
  ASSERT_EQ(OB_SUCCESS, dec_data.get_pk_vals(pk_vals));

  int pos = 0;
  char final_buf[16385] = { 0 }; // max is 16384
  
  pos += sprintf(final_buf + pos, "[");
  //pos += sprintf(final_buf + pos, "dba_len:%d, ", dec_data.get_guess_dba_len());
  //pos += sprintf(final_buf + pos, "version:%d, ", dec_data.get_version());
  //pos += sprintf(final_buf + pos, "pk_cnt:%ld, ", pk_vals.count()); 
  for (int i = 0; i < pk_vals.count(); ++i) {    
    char tmp_buf[16385] = { 0 };
    (void)pk_vals.at(i).to_string(tmp_buf, 1024);
    if (i != pk_vals.count() - 1) {
      pos += sprintf(final_buf + pos, "%s, ", tmp_buf);
    } else {
      pos += sprintf(final_buf + pos, "%s", tmp_buf);
    }
  }
  pos += sprintf(final_buf + pos, "]");
  printf("%s", final_buf);
}

} // end namesapce common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  if(argc >= 2)
  {
    for (int i = 1; i < argc; i++) {
      if (argv[i][0] == '*') {
        oceanbase::common::decode_output(argv[i]);
      }
    }
  }
  return 0;
}
