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

#ifndef _OCEANBASE_OB_LUA_API_
#define _OCEANBASE_OB_LUA_API_

#include "lib/utility/ob_macro_utils.h"
#include "lib/allocator/ob_allocator.h"

class lua_State;

namespace oceanbase
{
namespace diagnose
{
extern void *alloc(const int size);
extern void free(void *ptr);

class APIRegister
{
  static constexpr int PRINT_BUFFER_SIZE = 1 << 20; // 1M
public:
  APIRegister() : conn_fd_(-1), print_offset_(0), print_capacity_(0), print_buffer_(nullptr)
  { 
    set_print_buffer((char *)diagnose::alloc(PRINT_BUFFER_SIZE), PRINT_BUFFER_SIZE);
  }
  ~APIRegister()
  {
    diagnose::free(print_buffer_);
    set_print_buffer(nullptr, 0);
  }
  void register_api(lua_State* L);
  int get_fd() { return conn_fd_; }
  void set_fd(int fd) { conn_fd_ = fd; }
  int flush();
  int append(const char *buffer, const int len);
  int append(const char *buffer) { return append(buffer, strlen(buffer)); }
  void set_print_buffer(char *buffer, const int cap)
  {
    print_buffer_ = buffer;
    print_capacity_ = cap;
  }
  static APIRegister& get_instance()
  {
    static APIRegister instance;
    return instance;
  }
private:
  int conn_fd_;
  int print_offset_;
  int print_capacity_;
  char *print_buffer_;
  DISALLOW_COPY_AND_ASSIGN(APIRegister);
};

} // diagnose
} // oceanbase


#endif //_OCEANBASE_OB_LUA_API_