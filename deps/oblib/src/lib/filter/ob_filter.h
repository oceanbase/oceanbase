#ifndef OCEANBASE_COMMON__FILTER_H_
#define OCEANBASE_COMMON__FILTER_H_

#include "lib/container/ob_vector.h"
#include "lib/ob_define.h"
#include <sys/types.h>
#include <cstdint>
#include <chrono>

namespace oceanbase {
namespace common {

template <class T, class HashFunc>
class ObFilter {
public:
    virtual ~ObFilter(){};
    virtual int init(int64_t element_count) = 0;
    virtual int insert(const T& element) = 0;
    virtual int insert_hash(const uint32_t key_hash) = 0;
    virtual int insert_all(const T* keys, const int64_t size) = 0;
    virtual int may_contain(const T& element, bool& is_contain) const = 0;
    virtual bool could_merge(const ObFilter& other) = 0;
    virtual int merge(const ObFilter& other) = 0;
    virtual void destroy() = 0;
    virtual void clear() = 0;
    virtual int64_t get_deep_copy_size() const = 0;
    virtual int64_t get_serialize_size() const = 0;
    virtual int64_t get_nhash() const = 0;
    virtual int64_t get_nbit() const = 0;
    virtual int64_t get_nbytes() const = 0;
    virtual uint8_t* get_bits() = 0;
    virtual const uint8_t* get_bits() const = 0;
    virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const = 0;
    virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) = 0;
    virtual int deep_copy(const ObFilter<T, HashFunc>& other) = 0;
    virtual int deep_copy(const ObFilter<T, HashFunc>& other, char* buffer) = 0;
};


}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_COMMON_BLOOM_FILTER_H_