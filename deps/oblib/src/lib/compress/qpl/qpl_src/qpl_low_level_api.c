#include <stdio.h>
#include <stdlib.h> 
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>

#include "qpl_low_level_api.h"
#include "qpl/qpl.h"

#define MAX_IO_THREAD 64
qpl_path_t execution_path = qpl_path_hardware;

qpl_job *g_qpl_job_pool[MAX_IO_THREAD];
atomic_bool g_job_lock[MAX_IO_THREAD];
static unsigned int g_qpl_job_size = 0;

int QPL_job_pool_init()
{
    qpl_status  status;
    status = qpl_get_job_size(execution_path, &g_qpl_job_size);
    if (status != QPL_STS_OK) {
        QPL_ERROR("QPL:qpl get job size faild %d.\n",status);
        return -1;
    }

    for (int i=0; i<MAX_IO_THREAD; i++) {
        g_job_lock[i] = false;
        g_qpl_job_pool[i] = (qpl_job *)calloc(1,g_qpl_job_size);
        if (!g_qpl_job_pool[i]) {
            QPL_ERROR("QPL:failed to request memory.\n");
            return -1;
        }
        status = qpl_init_job(execution_path, g_qpl_job_pool[i]);
        if (unlikely(status != QPL_STS_OK)) {
            memset(g_qpl_job_pool[i], 0, g_qpl_job_size);
            status = qpl_init_job(qpl_path_software, g_qpl_job_pool[i]);
            if (unlikely(status != QPL_STS_OK)) {
                QPL_ERROR("QPL:qpl init software job faild %d.\n",status);
                free(g_qpl_job_pool[i]);
                return -1;
            }
        }
    }
    return 0;
}

void QPL_job_pool_destroy()
{
    qpl_status  status;
    for(int i=0;i<MAX_IO_THREAD;i++) {
        if (g_qpl_job_pool[i]) {
            status = qpl_fini_job(g_qpl_job_pool[i]);
            if (unlikely(status != QPL_STS_OK)) {
                QPL_ERROR("QPL:qpl fini job faild %d.\n",status);
            }
            free(g_qpl_job_pool[i]);
        }
    }
    return;
}

static inline size_t QPL_get_index()
{
    size_t rdtsc = 0;
    unsigned lo, hi;
    __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi) : :);
    rdtsc = ((((uint64_t)hi) << 32) | (uint64_t)lo);
    return ((size_t)((rdtsc * 1000000001339ULL) >> 4)) % MAX_IO_THREAD;
}

static inline qpl_job *QPL_acquire_job(uint32_t * job_id)
{
    size_t index = QPL_get_index();
    bool expected  = false;
    while (atomic_compare_exchange_strong(&g_job_lock[index],&expected,true)==false) {
        expected  = false;
        index = QPL_get_index();
    }

    *job_id = index;
    return g_qpl_job_pool[index];
}

int QPL_compress_by_software(const char* source, char* dest, int inputSize, int maxOutputSize, uint32_t job_id)
{
    qpl_status  status;
    qpl_job *job = (qpl_job *)calloc(1,g_qpl_job_size);
    if (unlikely(!job)) {
        QPL_ERROR("QPL:qpl software compress alloc job faild.\n");
        return -1;
    }
    
    status = qpl_init_job(qpl_path_software, job);
    if (unlikely(status != QPL_STS_OK)) {
        QPL_ERROR("QPL:qpl software compress init job faild %d.\n",status);
        return -1;
    }
    int total_out = 0;
    job->op            = qpl_op_compress;
    job->level         = qpl_default_level;
    job->next_in_ptr   = (unsigned char*)source;
    job->next_out_ptr  = (unsigned char*)dest;
    job->available_in  = inputSize;
    job->available_out = maxOutputSize;
    job->flags         = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
    status = qpl_execute_job(job);
    total_out = job->total_out;
    if (unlikely(status != QPL_STS_OK)) {
        QPL_ERROR("QPL:qpl software compress execute job faild %d.\n",status);
        total_out = -1;
    }

    atomic_store(&g_job_lock[job_id],false);
    free(job);
    return total_out;
}

int QPL_compress(const char* source, char* dest, int inputSize, int maxOutputSize)
{
    qpl_status  status;
    uint32_t job_id = 0;
    qpl_job *job = QPL_acquire_job(&job_id);
    if (unlikely(!job)) {
        QPL_ERROR("QPL:qpl compress acquire job faild.\n");
        return -1;
    }

    job->op            = qpl_op_compress;
    job->level         = qpl_default_level;
    job->next_in_ptr   = (unsigned char*)source;
    job->next_out_ptr  = (unsigned char*)dest;
    job->available_in  = inputSize;
    job->available_out = maxOutputSize;
    job->flags         = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
    status = qpl_execute_job(job);
    if (unlikely(status != QPL_STS_OK)) {
        if (job->data_ptr.path == qpl_path_hardware) {
            QPL_ERROR("QPL:qpl hardware compress execute job faild, use software %d.\n",status);
            return QPL_compress_by_software(source, dest, inputSize, maxOutputSize, job_id);
        }
        QPL_ERROR("QPL:qpl software compress execute job faild %d.\n",status);
        atomic_store(&g_job_lock[job_id],false);
        return -1;
    }

    atomic_store(&g_job_lock[job_id],false);
    return job->total_out;
}

int QPL_decompress_by_software(const char* source, char* dest, int compressedSize, int maxDecompressedSize, uint32_t job_id)
{  
    qpl_status  status;
    qpl_job *job = (qpl_job *)calloc(1,g_qpl_job_size);
    if (unlikely(!job)) {
        QPL_ERROR("QPL:qpl software decompress alloc job faild.\n");
        return -1;
    }
    
    status = qpl_init_job(qpl_path_software, job);
    if (unlikely(status != QPL_STS_OK)) {
        QPL_ERROR("QPL:qpl software decompress init job faild %d.\n",status);
        return -1;
    }

    int total_out = 0;
    job->op            = qpl_op_decompress;
    job->next_in_ptr   = (unsigned char*)source;
    job->next_out_ptr  = (unsigned char*)dest;
    job->available_in  = compressedSize;
    job->available_out = maxDecompressedSize;
    job->flags         = QPL_FLAG_FIRST | QPL_FLAG_LAST;
    status = qpl_execute_job(job);
    total_out = job->total_out;
    if (unlikely(status != QPL_STS_OK)) {
        QPL_ERROR("QPL:qpl software decompress execute job faild %d.\n",status);
        total_out = -1;
    }

    atomic_store(&g_job_lock[job_id],false);
    free(job);
    return total_out;
}

int QPL_decompress(const char* source, char* dest, int compressedSize, int maxDecompressedSize)
{  
    qpl_status  status;
    uint32_t job_id = 0;
    qpl_job *job = QPL_acquire_job(&job_id);
    if (unlikely(!job)) {
        QPL_ERROR("QPL:qpl decompress acquire job faild.\n");
        return -1;
    }

    job->op            = qpl_op_decompress;
    job->next_in_ptr   = (unsigned char*)source;
    job->next_out_ptr  = (unsigned char*)dest;
    job->available_in  = compressedSize;
    job->available_out = maxDecompressedSize;
    job->flags         = QPL_FLAG_FIRST | QPL_FLAG_LAST;
    status = qpl_execute_job(job);
    if (unlikely(status != QPL_STS_OK)) {
        if (job->data_ptr.path == qpl_path_hardware) {
            QPL_ERROR("QPL:qpl hardware decompress execute job faild, use software %d.\n",status);
            return QPL_decompress_by_software(source, dest, compressedSize, maxDecompressedSize, job_id);
        }

        QPL_ERROR("QPL:qpl software decompress execute job faild %d.\n",status);
        atomic_store(&g_job_lock[job_id],false);
        return -1;
    }

    atomic_store(&g_job_lock[job_id],false);
    return job->total_out;
}


