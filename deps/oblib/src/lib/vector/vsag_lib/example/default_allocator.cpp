#include "default_allocator.h"

#include "vsag/options.h"
#include "../default_logger.h"
#include <stdio.h>

#include <stdlib.h>
#include <unistd.h>
void *_malloc(size_t size, const char *filename, int line){
    void *ptr = malloc(size);
    
    char buffer[128] = {0};
    sprintf(buffer, "./%p.memory", ptr);

    FILE *fp = fopen(buffer, "w");
    fprintf(fp, "[+]addr: %p, filename: %s, line: %d\n", ptr, filename, line);

    fflush(fp);
    fclose(fp);

    return ptr;
}

void _free(void *ptr, const char *filename, int line){
    char buffer[1280] = {0};
    sprintf(buffer, "./memory/%p.memory", ptr);

    if (unlink(buffer) < 0){
        printf("double free: %p\n", ptr);
        return;
    }

    return free(ptr);
}
//#define malloc(size)    _malloc(size, __FILE__, __LINE__)
//#define free(ptr)       _free(ptr, __FILE__, __LINE__)
void*
DefaultAllocator::Allocate(size_t size) {
    void* ptr = malloc(size);
    //vsag::logger::debug("allocate memoery,addr:{}, size:{}",ptr, size);
    return malloc(size);
}

void
DefaultAllocator::Deallocate(void* p) {
    //vsag::logger::debug("free memoery, alloctor:{}, free_addr:{}", (void*)this, p);
    free(p);
}

void*
DefaultAllocator::Reallocate(void* p, size_t size) {
    //vsag::logger::debug("re-allocate memoery,addr:{}, size:{}",p,size);
    return realloc(p, size);
}

std::string
DefaultAllocator::Name() {
    return "DefaultAllocator";
}

