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

#ifndef OCEANBASE_CLOG_OB_LOG_COMPRESS_H_
#define OCEANBASE_CLOG_OB_LOG_COMPRESS_H_

#include <stdint.h>

namespace oceanbase {
namespace clog {

class ObLogAllocator;
//@param [in] in_buf  Buf read from the clog file: contains compressed clog
//@param [in] in_size  Buf read from the clog file
//@param [out] out_buf The buf that stores the serialized content of the decompressed ObLogEntry
//@param [out] out_size The buf length of the serialized content of the decompressed ObLogEntry
//@param [out] uncompress_len The serialized content length of ObLogEntry after decompression
//@param [out] comsume_buf_len The size of in buf consumed should actually be equal to ObCompressedLogEntry
// The size in the corresponding ObLogCursor, which is the size occupied in the clog file
int uncompress(const char* in_buf, int64_t in_size, char*& out_buf, int64_t out_buf_size, int64_t& uncompress_len,
    int64_t& consume_buf_len);

inline bool is_compressed_clog(char high, char low)
{
  return ((0x43 == high) && (0x01 == low || 0x02 == low || 0x03 == low));
}
}  // end of namespace clog
}  // end of namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_COMPRESS_H_
