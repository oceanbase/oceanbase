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

#ifndef OCEANBASE_COMMON_COMPRESS_ZSTD_1_3_8_COMPRESSOR_
#define OCEANBASE_COMMON_COMPRESS_ZSTD_1_3_8_COMPRESSOR_
#include "lib/compress/ob_compressor.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace common
{

namespace zstd_1_3_8
{

class ObZstdCtxAllocator : public ObIAllocator
{
static constexpr int64_t ZSTD_ALLOCATOR_BLOCK_SIZE = (1LL << 20) - (17LL << 10);
public:
  ObZstdCtxAllocator(int64_t tenant_id);
  virtual ~ObZstdCtxAllocator();
  static ObZstdCtxAllocator &get_thread_local_instance()
  {
    thread_local ObZstdCtxAllocator allocator(ob_thread_tenant_id());
    return allocator;
  }
  void *alloc(const int64_t size) override;
  void *alloc(const int64_t size, const ObMemAttr &attr) override { return NULL; }
  void free(void *ptr) override;
  void reuse() override;
  void reset() override;
private:
  ObArenaAllocator allocator_;
};

class __attribute__((visibility ("default"))) ObZstdCompressor_1_3_8 : public ObCompressor
{
public:
  explicit ObZstdCompressor_1_3_8() {}
  virtual ~ObZstdCompressor_1_3_8() {}
  int compress(const char *src_buffer,
               const int64_t src_data_size,
               char *dst_buffer,
               const int64_t dst_buffer_size,
               int64_t &dst_data_size,
               ObIAllocator *allocator) override;
  int decompress(const char *src_buffer,
                 const int64_t src_data_size,
                 char *dst_buffer,
                 const int64_t dst_buffer_size,
                 int64_t &dst_data_size,
                 ObIAllocator *allocator) override;
  int compress(const char *src_buffer,
               const int64_t src_data_size,
               char *dst_buffer,
               const int64_t dst_buffer_size,
               int64_t &dst_data_size) override
  {
    return compress(src_buffer, src_data_size, dst_buffer,
                    dst_buffer_size, dst_data_size, NULL);
  }
  int decompress(const char *src_buffer,
                 const int64_t src_data_size,
                 char *dst_buffer,
                 const int64_t dst_buffer_size,
                 int64_t &dst_data_size) override
  {
    return decompress(src_buffer, src_data_size, dst_buffer,
                      dst_buffer_size, dst_data_size, NULL);
  }
  const char *get_compressor_name() const;
  ObCompressorType get_compressor_type() const;
  int get_max_overflow_size(const int64_t src_data_size,
                            int64_t &max_overflow_size) const;
  void reset_mem();

};
} // namespace zstd_1_3_8
} //namespace common
} //namespace oceanbase
#endif //OCEANBASE_COMMON_COMPRESS_ZSTD_1_3_8_COMPRESSOR_
