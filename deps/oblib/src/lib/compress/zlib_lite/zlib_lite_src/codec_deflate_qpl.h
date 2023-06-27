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

class CodecDeflateQpl final {
public:
  explicit CodecDeflateQpl(qpl_path_t path);
  ~CodecDeflateQpl();

  static CodecDeflateQpl & get_hardware_instance();
  static CodecDeflateQpl & get_software_instance();

  qpl_job * acquire_job(uint32_t & job_id);
  void release_job(uint32_t job_id);
  const bool & is_job_pool_ready() const { return job_pool_ready_; }
  int32_t do_decompress_data(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size);
  int32_t do_compress_data(const char * source, uint32_t source_size, char * dest, uint32_t dest_size);

private:
  bool try_lock_job(uint32_t index);
  void unlock_job(uint32_t index);

private:
  /// qpl excute path
  const char  *qpl_excute_path_;
  /// Maximum jobs running in parallel 
  static constexpr int MAX_JOB_NUMBER = 1024;
  /// Entire buffer for storing all job objects
  char * jobs_buffer_;
  /// Job pool for storing all job object pointers
  std::array<qpl_job *, MAX_JOB_NUMBER> job_ptr_pool_;
  /// Locks for accessing each job object pointers
  std::array<std::atomic_bool, MAX_JOB_NUMBER> job_ptr_locks_;
  bool job_pool_ready_;
  std::mt19937 random_engine_;
  std::uniform_int_distribution<int> distribution_;
};

int32_t qpl_compress(const char* source, char* dest, int input_size, int max_output_size);
int32_t qpl_decompress(const char* source, char* dest, int input_size, int max_output_size);

} //namespace ZLIB_LITE
} //namespace common
} //namespace oceanbase

#endif 
