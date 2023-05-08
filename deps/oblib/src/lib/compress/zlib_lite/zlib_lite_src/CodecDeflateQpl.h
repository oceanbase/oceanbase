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

typedef unsigned int UInt32;
typedef int Int32;

#define QPL_DEBUG(fmt,...)  \
     printf("[%s %s] %s (%s:%d)" fmt,__DATE__,__TIME__,__FUNCTION__,__FILE__,__LINE__,##__VA_ARGS__); \
     fflush(stdout);

class CodecDeflateQpl
{
public:
    explicit CodecDeflateQpl(qpl_path_t path);
    ~CodecDeflateQpl();

    static CodecDeflateQpl & getHardwareInstance();
    static CodecDeflateQpl & getSoftwareInstance();

    qpl_job * acquireJob(UInt32 & job_id);
    void releaseJob(UInt32 job_id);
    const bool & isJobPoolReady() { return job_pool_ready; }
    Int32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size);
    Int32 doCompressData(const char * source, UInt32 source_size, char * dest, UInt32 dest_size);

private:
    bool tryLockJob(UInt32 index);
    void unLockJob(UInt32 index);
    /// qpl excute path
    const char  *qpl_excute_path;
    /// Maximum jobs running in parallel 
    static constexpr auto MAX_JOB_NUMBER = 1024;
    /// Entire buffer for storing all job objects
    char * jobs_buffer;
    /// Job pool for storing all job object pointers
    std::array<qpl_job *, MAX_JOB_NUMBER> job_ptr_pool;
    /// Locks for accessing each job object pointers
    std::array<std::atomic_bool, MAX_JOB_NUMBER> job_ptr_locks;
    bool job_pool_ready;
    std::mt19937 random_engine;
    std::uniform_int_distribution<int> distribution;
};

Int32 qpl_compress(const char* source, char* dest, int inputSize, int maxOutputSize);
Int32 qpl_decompress(const char* source, char* dest, int inputSize, int maxOutputSize);

} //namespace ZLIB_LITE
} //namespace common
} //namespace oceanbase

#endif 
