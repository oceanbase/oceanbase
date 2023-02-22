#if defined(__cplusplus)
extern "C" {
#endif

#pragma once

#ifndef likely
#define likely(x) __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

static inline void QPL_ERROR(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    vfprintf(stdout, format, args);
    va_end(args);
}

int QPL_decompress(const char* source, char* dest, int compressedSize, int maxDecompressedSize);
int QPL_compress(const char* source, char* dest, int inputSize, int maxOutputSize);
void QPL_job_pool_destroy();
int QPL_job_pool_init();

#if defined(__cplusplus)
}
#endif
