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

#ifndef OCEANBASE_COMMON_COMPRESS_ZIP_COMPRESSOR_H_
#define OCEANBASE_COMMON_COMPRESS_ZIP_COMPRESSOR_H_
#include "lib/compress/ob_compressor.h"

struct zip_t;

namespace oceanbase {
namespace common {
enum class ObZipCompressFlag {
  FAKE_FILE_HEADER,
  DATA,
  LAST_DATA,
  FILE_HEADER,
  TAIL
};

/**
 * ObZipCompressor has nothing to do with ObCompressor.
 * ObCompressor can compress data blocks, 
 * but because of the zip file special format (https://en.wikipedia.org/wiki/ZIP_(file_format)),
 * ObZipCompressor can't do that.
 * You can see, the parameters are inconsistent in compress() func.
 * 
 * This zip compressor now support compress only a file.
 */
class ObZipCompressor final {
public:
  ObZipCompressor();
  virtual ~ObZipCompressor();

  int compress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer, int64_t& dst_data_size,
      ObZipCompressFlag flag, const char* file_name);
  // decompress is not ot support now
private:
  struct zip_t *zip_;
};

}  // namespace common
}  // namespace oceanbase
#endif  // OCEANBASE_COMMON_COMPRESS_ZIP_COMPRESSOR_H_
