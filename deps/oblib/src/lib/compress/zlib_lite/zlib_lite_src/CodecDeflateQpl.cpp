#ifdef ENABLE_QPL_COMPRESSION
#include <cassert>
#include <memory>
#include "CodecDeflateQpl.h"

namespace oceanbase
{
namespace common
{
namespace ZLIB_LITE
{
static constexpr Int32 RET_ERROR = -1;

CodecDeflateQpl::CodecDeflateQpl(qpl_path_t path)
    : random_engine(std::random_device()())
    , distribution(0, MAX_JOB_NUMBER - 1)
{
    UInt32 job_size = 0;
    const char * qpl_version = qpl_get_library_version();
    qpl_excute_path = (path == qpl_path_hardware) ? "Hardware" : "Software";
    /// Get size required for saving a single qpl job object
    qpl_get_job_size(path, &job_size);
    /// Allocate entire buffer for storing all job objects
    jobs_buffer = new char[job_size * MAX_JOB_NUMBER];
    /// Initialize pool for storing all job object pointers
    /// Reallocate buffer by shifting address offset for each job object.
    for (UInt32 index = 0; index < MAX_JOB_NUMBER; ++index)
    {
        qpl_job * qpl_job_ptr = (qpl_job *)(jobs_buffer + index * job_size);
        auto status = qpl_init_job(path, qpl_job_ptr); 
        if (status != QPL_STS_OK)
        {
            job_pool_ready = false;
            delete[] jobs_buffer;
            if (path == qpl_path_hardware)
            {
                QPL_DEBUG("Initialization of IAA hardware failed:%d, will attempt to use software DeflateQpl codec instead of hardware DeflateQpl codec. QPL Version: %s.\n",
                    status, qpl_version);
            }
            else
            {
                QPL_DEBUG("Initialization of software DeflateQpl codec failed:%d, QPL compression/decompression cannot be enabled.\n",status);
            }
    
            return;
        }
        job_ptr_pool[index] = qpl_job_ptr;
        unLockJob(index);
    }

    QPL_DEBUG("%s-assisted DeflateQpl codec is ready! QPL Version: %s\n", qpl_excute_path, qpl_version);
    
    job_pool_ready = true;
}

CodecDeflateQpl::~CodecDeflateQpl()
{
    if (isJobPoolReady())
    {
        for (UInt32 i = 0; i < MAX_JOB_NUMBER; ++i)
        {
            if (job_ptr_pool[i])
            {
                while (!tryLockJob(i));
                qpl_fini_job(job_ptr_pool[i]);
                unLockJob(i);
                job_ptr_pool[i] = nullptr;
            }
        }

        delete[] jobs_buffer;
        job_pool_ready = false;
    }
}

CodecDeflateQpl & CodecDeflateQpl::getHardwareInstance()
{
    static CodecDeflateQpl hw_codec(qpl_path_hardware);
    return hw_codec;
}

CodecDeflateQpl & CodecDeflateQpl::getSoftwareInstance()
{
    static CodecDeflateQpl sw_codec(qpl_path_software);
    return sw_codec;
}

void CodecDeflateQpl::releaseJob(UInt32 job_id)
{
    if (isJobPoolReady())
        unLockJob(MAX_JOB_NUMBER - job_id);
}

bool CodecDeflateQpl::tryLockJob(UInt32 index)
{
    bool expected = false;
    assert(index < MAX_JOB_NUMBER);
    return job_ptr_locks[index].compare_exchange_strong(expected, true);
}

void CodecDeflateQpl::unLockJob(UInt32 index)
{
    assert(index < MAX_JOB_NUMBER);
    job_ptr_locks[index].store(false);
}

qpl_job * CodecDeflateQpl::acquireJob(UInt32 & job_id)
{
    if (isJobPoolReady())
    {
        UInt32 retry = 0;
        auto index = distribution(random_engine);
        while (!tryLockJob(index))
        {
            index = distribution(random_engine);
            retry++;
            if (retry > MAX_JOB_NUMBER)
            {
                return nullptr;
            }
        }
        job_id = MAX_JOB_NUMBER - index;
        assert(index < MAX_JOB_NUMBER);
        return job_ptr_pool[index];
    }
    else
        return nullptr;
}

Int32 CodecDeflateQpl::doCompressData(const char * source, UInt32 source_size, char * dest, UInt32 dest_size) 
{
    UInt32 job_id = 0;
    qpl_job * job_ptr = nullptr;
    UInt32 compressed_size = 0;
    if (!(job_ptr = acquireJob(job_id)))
    {
        QPL_DEBUG("DeflateQpl %s codec failed, doCompressData->acquireJob fail.\n", qpl_excute_path);
        return RET_ERROR;
    }

    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->level = qpl_default_level;
    job_ptr->available_out = dest_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;

    qpl_status status = qpl_execute_job(job_ptr);

    if (status == QPL_STS_OK)
    {
        compressed_size = job_ptr->total_out;
        releaseJob(job_id);
        return compressed_size;
    }
    else
    {
        QPL_DEBUG("DeflateQpl %s codec failed, doCompressData->qpl_execute_job with error code: %d.\n", qpl_excute_path, status);
        releaseJob(job_id);
        return RET_ERROR;
    }
}

Int32 CodecDeflateQpl::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
{
    UInt32 job_id = 0;
    qpl_job * job_ptr = nullptr;
    UInt32 decompressed_size = 0;
    if (!(job_ptr = acquireJob(job_id)))
    {
        QPL_DEBUG("DeflateQpl %s codec failed, doDecompressData->acquireJob fail.\n", qpl_excute_path);
        return RET_ERROR;
    }

    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    qpl_status status = qpl_execute_job(job_ptr);

    if (status == QPL_STS_OK)
    {
        decompressed_size = job_ptr->total_out;
        releaseJob(job_id);
        return decompressed_size;
    }
    else
    {
        QPL_DEBUG("DeflateQpl %s codec failed, doDeCompressData->qpl_execute_job with error code: %d.\n", qpl_excute_path, status);
        releaseJob(job_id); 
        return RET_ERROR;
    }

    return decompressed_size;
}

Int32 qpl_compress(const char* source, char* dest, int inputSize, int maxOutputSize)
{
    Int32 res = RET_ERROR;
    if (CodecDeflateQpl::getHardwareInstance().isJobPoolReady())
        res = CodecDeflateQpl::getHardwareInstance().doCompressData(source, inputSize, dest, maxOutputSize);
    if (res == RET_ERROR)
        res = CodecDeflateQpl::getSoftwareInstance().doCompressData(source, inputSize, dest, maxOutputSize);
    return res;
}

Int32 qpl_decompress(const char* source, char* dest, int inputSize, int maxOutputSize)
{
    Int32 res = RET_ERROR;
    if (CodecDeflateQpl::getHardwareInstance().isJobPoolReady())
        res = CodecDeflateQpl::getHardwareInstance().doDecompressData(source, inputSize, dest, maxOutputSize);
    if (res == RET_ERROR)
        res = CodecDeflateQpl::getSoftwareInstance().doDecompressData(source, inputSize, dest, maxOutputSize);
    return res;
}


}//namespace ZLIB_LITE
}//namespace common
}//namespace oceanbase

#endif