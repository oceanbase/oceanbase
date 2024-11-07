#ifndef OB_VSAG_LIB_C_H
#define OB_VSAG_LIB_C_H
#include "ob_vsag_lib.h"
#include <dlfcn.h>
// Define a macro to load function symbols
#define LOAD_FUNCTION(handle, typeName, funcName)                             \
    do {                                                            \
        dlerror();  /* Clear existing errors */                     \
        funcName = (typeName)dlsym((handle), (#funcName));          \
        const char* dlsym_error = dlerror();                        \
        if (dlsym_error) {                                          \
            fprintf(stderr, "Cannot load symbol '" #funcName "': %s\n", dlsym_error); \
            dlclose(handle);                                        \
            return EXIT_FAILURE;                                    \
        }                                                           \
    } while (0)



namespace obvectorlib {
#ifdef __cplusplus
extern "C" {
#endif
typedef bool (*is_init_ptr)();
extern bool is_init_c();

typedef void (*set_log_level_ptr)(int64_t level_num);
extern void set_log_level_c(int64_t level_num);

typedef void (*set_logger_ptr)(void *logger_ptr);
extern void set_logger_c(void *logger_ptr);

typedef void (*set_block_size_limit_ptr)(uint64_t size);
extern void set_block_size_limit_c(uint64_t size);

typedef bool (*is_supported_index_ptr)(IndexType index_type);
extern bool is_supported_index_c(IndexType index_type);

typedef int (*create_index_ptr)(VectorIndexPtr& index_handler, IndexType index_type,
                        const char* dtype,
                        const char* metric,int dim,
                        int max_degree, int ef_construction, int ef_search, void* allocator);
extern int create_index_c(VectorIndexPtr& index_handler, IndexType index_type,
                        const char* dtype,
                        const char* metric,int dim,
                        int max_degree, int ef_construction, int ef_search, void* allocator = NULL);

typedef int (*build_index_ptr)(VectorIndexPtr& index_handler, float* vector_list, int64_t* ids, int dim, int size);              
extern int build_index_c(VectorIndexPtr& index_handler, float* vector_list, int64_t* ids, int dim, int size);

typedef int (*add_index_ptr)(VectorIndexPtr& index_handler, float* vector, int64_t* ids, int dim, int size);
extern int add_index_c(VectorIndexPtr& index_handler, float* vector, int64_t* ids, int dim, int size);

typedef int (*get_index_number_ptr)(VectorIndexPtr& index_handler, int64_t &size);
extern int get_index_number_c(VectorIndexPtr& index_handler, int64_t &size);

typedef int (*knn_search_ptr)(VectorIndexPtr& index_handler,float* query_vector, int dim, int64_t topk,
                      const float*& dist, const int64_t*& ids, int64_t &result_size, int ef_search,
                       void* invalid);
extern int knn_search_c(VectorIndexPtr& index_handler,float* query_vector, int dim, int64_t topk,
                      const float*& dist, const int64_t*& ids, int64_t &result_size, int ef_search,
                      void* invalid = NULL);

typedef int (*serialize_ptr)(VectorIndexPtr& index_handler, const std::string dir);          
extern int serialize_c(VectorIndexPtr& index_handler, const std::string dir);

typedef int (*deserialize_bin_ptr)(VectorIndexPtr& index_handler, const std::string dir);
extern int deserialize_bin_c(VectorIndexPtr& index_handler, const std::string dir);
 
typedef int (*delete_index_ptr)(VectorIndexPtr& index_handler);
extern int delete_index_c(VectorIndexPtr& index_handler);

typedef int (*fserialize_ptr)(VectorIndexPtr& index_handler, std::ostream& out_stream);
extern int fserialize_c(VectorIndexPtr& index_handler, std::ostream& out_stream);

typedef int (*fdeserialize_ptr)(VectorIndexPtr& index_handler, std::istream& in_stream);
extern int fdeserialize_c(VectorIndexPtr& index_handler, std::istream& in_stream);




#ifdef __cplusplus
}  // extern "C"
#endif
} // namesapce obvectorlib

#endif // OB_VSAG_LIB_H
