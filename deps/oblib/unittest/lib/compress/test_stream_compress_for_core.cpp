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

#include <gtest/gtest.h>
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/alloc/alloc_func.h"
#include "lib/ob_define.h"
#include "lib/compress/zlib/zlib.h"
#include "lib/compress/zstd/ob_zstd_stream_compressor.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/coro/testing.h"

#define KB *(1U<<10)
#define MB *(1U<<20)
#define GB *(1U<<30)

using namespace oceanbase::obsys;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

int main(int argc, char **argv)
{
  system("rm -f yyy.log*");
  OB_LOGGER.set_file_name("yyy.log", true, true);
  OB_LOGGER.set_log_level("INFO");


  const int64_t RING_BUFFER_BYTES = 128 KB;
  const int64_t BLOCK_SIZE = 20 KB;
  int ret = OB_SUCCESS;
  ObZstdStreamCompressor zstd_compressor;
  ObLZ4StreamCompressor lz4_compressor;
  void *cctx = NULL;
  void *dctx = NULL;
  struct stat stat_buf;
  int fd = 0;
  char *file_buf = NULL;
  void *buf = NULL;
  int64_t buf_len = 0;
  const int64_t FILE_MAX_SIZE = 64 * 1024 * 1024;

  int64_t compress_buf_size = 0;
  int64_t total_data_size = 0;
  int64_t total_compressed_data_size = 0;
  int64_t total_uncomp_data_size = 0;
  if (argc < 2) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguemtn ", K(ret));
  } else {
    char path[256] = {0};
    char method[256] = {0};
    snprintf(method, 256, "%s", argv[1]);
    snprintf(path, 256, "%s", argv[2]);
    if (0 != strcmp("zstd", method) && 0 != strcmp("lz4", method)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(method), K(ret));
    } else {
      ObStreamCompressor &compressor = (0 == strcmp("zstd", method)) ? static_cast<ObStreamCompressor &>(zstd_compressor) : static_cast<ObStreamCompressor &>(lz4_compressor);
      COMMON_LOG(INFO, "start", "compressor_name", compressor.get_compressor_name());
      if (OB_FAIL(compressor.create_compress_ctx(cctx))) {
        COMMON_LOG(WARN, "failed to create compress ctx, ", K(ret));
      } else if (OB_FAIL(compressor.create_decompress_ctx(dctx))) {
        COMMON_LOG(WARN, "failed to create decompress ctx, ", K(ret));
      } else if (OB_FAIL(compressor.get_compress_bound_size(BLOCK_SIZE, compress_buf_size))) {
        COMMON_LOG(WARN, "failed to get compress bound size ctx, ", K(ret));
      } else if (-1 == (fd = open(path, O_RDONLY))) {
        ret = OB_IO_ERROR;
        CLOG_LOG(ERROR, "open file fail", K(path), KERRMSG, K(ret));
      } else if (-1 == fstat(fd, &stat_buf)) {
        ret = OB_IO_ERROR;
        CLOG_LOG(ERROR, "stat_buf error", K(path), KERRMSG, K(ret));
      } else if (stat_buf.st_size > FILE_MAX_SIZE) {
        ret = OB_INVALID_ARGUMENT;
        CLOG_LOG(ERROR, "invalid file size", K(path), K(stat_buf.st_size), K(ret));
      } else if (MAP_FAILED == (buf = mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED, fd, 0)) || NULL == buf) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "failed to mmap file", K(path), K(errno), KERRMSG, K(ret));
      } else {
        file_buf = static_cast<char *>(buf);
        buf_len = stat_buf.st_size;

        assert(cctx != NULL);
        assert(dctx != NULL);
        char in_ring_buf[RING_BUFFER_BYTES] = {0};
        char out_ring_buf[RING_BUFFER_BYTES] = {0};
        int64_t in_pos = 0;
        int64_t out_pos = 0;

        char* compress_buf = (char *)malloc(compress_buf_size);
        assert(compress_buf != NULL);

        int64_t file_buf_pos = 0;
        int64_t decompressed_size = 0;

        int64_t start_time= ObTimeUtility::current_time();
        while (OB_SUCC(ret)) {
          //int64_t in_size = rand() % BLOCK_SIZE + 1;
          int64_t in_size = 10 * 1024;
          if (total_data_size > 4294945792)
          {
            break;
            in_size = 256;
          }
          if (in_size + file_buf_pos > buf_len) {
            file_buf_pos = 0;
          }

          if (in_pos + in_size > RING_BUFFER_BYTES) {
            COMMON_LOG(INFO, "reset in ring buffer pos", K(in_pos), K(in_size), K(total_data_size),K(total_compressed_data_size),K(total_uncomp_data_size),K(ret));
            in_pos = 0;
          }

          MEMCPY(in_ring_buf + in_pos, file_buf + file_buf_pos, in_size);
          file_buf_pos += in_size;

          //compress
          int64_t compressed_size = 0;
          if (OB_FAIL(compressor.stream_compress(cctx, in_ring_buf + in_pos, in_size, compress_buf, compress_buf_size, compressed_size))) {
            COMMON_LOG(WARN, "failed to comrpess", K(in_pos), K(in_size), K(compress_buf_size), K(ret));
          } else {
            if (0 == compressed_size) {
              MEMCPY(compress_buf, in_ring_buf + in_pos, in_size);
              in_pos += in_size;
              COMMON_LOG(INFO, "succ to comrpess, not compressed", K(in_pos), K(in_size), K(compress_buf_size), K(compressed_size), K(ret));
            } else {
              in_pos += in_size;
              COMMON_LOG(INFO, "succ to comrpess", K(in_pos), K(in_size), K(compress_buf_size), K(compressed_size), K(ret));
              //COMMON_LOG(INFO, "succ to decomrpess", K(in_pos), K(in_size), K(compressed_size), K(ret));
            }
          }

          //decompress
          if (OB_SUCC(ret)) {
            if (out_pos + in_size > RING_BUFFER_BYTES) {
              //          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
              COMMON_LOG(INFO, "reset out ring buffer pos", K(out_pos), K(in_size), K(total_data_size),K(total_compressed_data_size),K(total_uncomp_data_size),K(ret));
              //         }
              out_pos = 0;
            }
            decompressed_size = 0;
            if (0 == compressed_size) {
              MEMCPY(out_ring_buf + out_pos, compress_buf, in_size);
              if (OB_FAIL(compressor.insert_uncompressed_block(dctx, out_ring_buf + out_pos, in_size))) {
                COMMON_LOG(WARN, "failed to insert block", K(out_pos), K(in_size), K(ret));
              } else {
                out_pos += in_size;
                total_uncomp_data_size += in_size;
                total_data_size += in_size;
                COMMON_LOG(INFO, "succ to decomrpess, not compressed", K(out_pos), K(in_pos), K(in_size), K(compressed_size), K(total_data_size),K(total_compressed_data_size),K(total_uncomp_data_size), K(ret));
              }
            } else if (OB_FAIL(compressor.stream_decompress(dctx, compress_buf, compressed_size, out_ring_buf + out_pos, in_size, decompressed_size))) {
              COMMON_LOG(WARN, "failed to decomrpess", K(out_pos), K(in_size), K(compressed_size), K(ret));
            } else if (in_size != decompressed_size) {
              ret = OB_ERR_UNEXPECTED;
              COMMON_LOG(WARN, "invalid size", K(out_pos), K(decompressed_size), K(in_size), K(ret));
            } else {
              total_compressed_data_size += in_size;
              total_data_size += in_size;
              out_pos += in_size;
              COMMON_LOG(INFO, "succ to decomrpess, compressed", K(out_pos), K(in_pos), K(in_size), K(decompressed_size), K(total_data_size),K(total_compressed_data_size),K(total_uncomp_data_size), K(compressed_size), K(ret));
            }
          }

          //compare
          if (OB_SUCC(ret)) {
            if (0 != MEMCMP(in_ring_buf, out_ring_buf, RING_BUFFER_BYTES)) {
              ret = OB_ERR_UNEXPECTED;
              if (0 != MEMCMP(in_ring_buf, out_ring_buf, in_pos)) {
              COMMON_LOG(WARN, "in_ring_buf before pos is not same as out_ring_buf", K(out_pos), K(in_pos), K(in_size), K(decompressed_size),
                         K(total_data_size),K(total_compressed_data_size),K(total_uncomp_data_size), K(compressed_size), K(ret));
              } else {
                COMMON_LOG(WARN, "in_ring_buf after pos is not same as out_ring_buf", K(out_pos), K(in_pos), K(in_size), K(decompressed_size),
                           K(total_data_size),K(total_compressed_data_size),K(total_uncomp_data_size), K(compressed_size), K(ret));
              }
            } else {
            }
          }
        }
        int64_t end_time= ObTimeUtility::current_time();
        int64_t use_time = end_time - start_time;
        COMMON_LOG(INFO, "YYY", K(use_time));
      }
    }
  }
  return ret;
}
