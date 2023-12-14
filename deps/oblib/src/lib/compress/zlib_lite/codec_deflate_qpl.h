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

#pragma once

#ifdef ENABLE_QPL_COMPRESSION

#include <array>
#include <atomic>
#include <map>
#include <random>
#include <memory>
#include <cstdio>
#include "qpl/qpl.h"

namespace oceanbase
{
namespace common
{
namespace ZLIB_LITE
{

class QplAllocator
{
public:
  void *(*allocate)(int64_t);
  void (*deallocate)(void *);
};

class CodecDeflateQpl final {
public:
  CodecDeflateQpl();
  ~CodecDeflateQpl();

  static CodecDeflateQpl & get_hardware_instance();
  static CodecDeflateQpl & get_software_instance();

  int  init(qpl_path_t path, QplAllocator &allocator, int32_t job_num);
  void deinit();

  const bool & is_job_pool_ready() const { return job_pool_ready_; }
  int64_t do_decompress_data(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size);
  int64_t do_compress_data(const char * source, uint32_t source_size, char * dest, uint32_t dest_size);

  const char *qpl_execute_path() const { return qpl_excute_path_; }

private:
  qpl_job * acquire_job(uint32_t & job_id);
  void release_job(uint32_t job_id, qpl_job *job_ptr);

private:
  bool try_lock_job(uint32_t index);
  void unlock_job(uint32_t index);

private:
  /// qpl excute path
  qpl_path_t   qpl_path_;
  const char  *qpl_excute_path_;

  QplAllocator allocator_;

  /// Maximum jobs running in parallel
  int32_t job_num_;
  /// Entire buffer for storing all job objects
  char * jobs_buffer_;
  /// Job pool for storing all job object pointers
  qpl_job ** job_ptr_pool_;
  /// Locks for accessing each job object pointers
  std::atomic_bool * job_ptr_locks_;
  bool job_pool_ready_;
  std::mt19937 *random_engine_;
  std::uniform_int_distribution<int> distribution_;
};

int  qpl_init(QplAllocator &allocator, int32_t job_num);
void qpl_deinit();

/// return negative value for failure and uint32_t for result
int64_t qpl_compress(const char* source, char* dest, int input_size, int max_output_size);
int64_t qpl_decompress(const char* source, char* dest, int input_size, int max_output_size);

bool qpl_hardware_enabled();

} //namespace ZLIB_LITE
} //namespace common
} //namespace oceanbase

#endif // ENABLE_QPL_COMPRESSION
