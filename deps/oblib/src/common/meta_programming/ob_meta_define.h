#ifndef SRC_COMMON_META_PROGRAMMING_OB_META_DEFINE_H
#define SRC_COMMON_META_PROGRAMMING_OB_META_DEFINE_H
#include "lib/allocator/ob_allocator.h"
namespace oceanbase
{
namespace common
{
namespace meta
{

struct DummyAllocator : public ObIAllocator
{
virtual void *alloc(const int64_t) override { return nullptr; }
virtual void* alloc(const int64_t, const ObMemAttr &) { return nullptr; }
virtual void free(void *ptr) override {}
static DummyAllocator &get_instance() { static DummyAllocator alloc; return alloc; }
};

}
}
}
#endif