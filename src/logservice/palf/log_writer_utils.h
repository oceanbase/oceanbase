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

#ifndef OCEANBASE_LOGSERVICE_LOG_WRITER_
#define OCEANBASE_LOGSERVICE_LOG_WRITER_

#include <stdint.h>
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"                      // Serialization
#include "lib/utility/ob_print_utils.h"         // Print*
#include "lib/container/ob_se_array.h"          // ObSEArray

namespace oceanbase
{
namespace palf
{
struct LogWriteBuf {
public:
  LogWriteBuf();
  LogWriteBuf(const LogWriteBuf &buf);
  ~LogWriteBuf();
  void reset();
  bool is_valid() const;
  int merge(const LogWriteBuf &rhs, bool &has_merged);
  int push_back(const char *buf,
                const int64_t buf_len);
  // If can used lambad, the code is more beautiful
  int get_write_buf(const int64_t idx,
                    const char *&buf,
                    int64_t &buf_len) const;
  int64_t get_total_size() const;
  int64_t get_buf_count() const;
  bool check_memory_is_continous() const;
  // NB: check_memory_is_continous firstly, and ensure dest_buf can hold enough data.
  void memcpy_to_continous_memory(char *dest_buf) const;

  TO_STRING_KV("total_size", get_total_size(), "count", get_buf_count(), K_(write_buf));
  NEED_SERIALIZE_AND_DESERIALIZE;
  static constexpr int64_t MAX_COUNT = 2;

  struct InnerStruct {
    const char *buf_;
    int64_t buf_len_;
    TO_STRING_KV(KP(buf_), K_(buf_len));
  };
  ObSEArray<InnerStruct, MAX_COUNT> write_buf_;
};
} // end namespace palf
} // end namespace oceanbase

#endif

