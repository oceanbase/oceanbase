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

#include "ob_log_compress.h"
#include "lib/compress/ob_compressor_pool.h"
#include "ob_log_entry.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace clog {
int uncompress(const char* in_buf, int64_t in_size, char*& out_buf, int64_t out_buf_size, int64_t& uncompress_len,
    int64_t& consume_buf_len)
{
  int ret = OB_SUCCESS;
  ObCompressedLogEntry comp_entry;
  const ObCompressedLogEntryHeader& header = comp_entry.get_header();
  common::ObCompressorType compress_type = INVALID_COMPRESSOR;
  common::ObCompressor* compressor = NULL;
  if (OB_FAIL(comp_entry.deserialize(in_buf, in_size, consume_buf_len))) {
    CLOG_LOG(WARN, "failed to deserialize ObCompressedLogEntry", K(ret));
  } else if (OB_FAIL(header.get_compressor_type(compress_type))) {
    CLOG_LOG(WARN, "failed to get compressor type", K(comp_entry), K(ret));
  } else if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compress_type, compressor))) {
    CLOG_LOG(WARN, "get_compressor failed", K(compress_type), K(ret));
  } else if (OB_ISNULL(compressor)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "got compressor is NULL", K(ret));
  } else if (OB_FAIL(compressor->decompress(
                 comp_entry.get_buf(), header.get_compressed_data_len(), out_buf, out_buf_size, uncompress_len))) {
  } else if (uncompress_len != header.get_orig_data_len()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "uncompress len is not expected", K(uncompress_len), K(header), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

}  // end of namespace clog
}  // end of namespace oceanbase
