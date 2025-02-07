#include <stdint.h>

#if defined(__clang__) || defined(__GNUC__)

#include <stdatomic.h>

typedef _Atomic(void*) atomicptr_t;
typedef _Atomic(int) atomic32_t;

static void*
atomic_load_ptr(atomicptr_t* src) {
  return atomic_load_explicit(src, memory_order_acquire);
}

static void
atomic_store_ptr(atomicptr_t* dst, void* val) {
  atomic_store_explicit(dst, val, memory_order_release);
}

static int32_t
atomic_load32(atomic32_t* src) {
  return atomic_load_explicit(src, memory_order_acquire);
}

static void
atomic_store32(atomic32_t* dst, int32_t val) {
  atomic_store_explicit(dst, val, memory_order_release);
}

static int32_t
atomic_incr32(atomic32_t* val) {
  return atomic_fetch_add_explicit(val, 1, memory_order_release) + 1;
}

static int32_t
atomic_decr32(atomic32_t* val) {
  return atomic_fetch_add_explicit(val, -1, memory_order_release) - 1;
}

static int32_t
atomic_add32(atomic32_t* val, int32_t add) {
  return atomic_fetch_add_explicit(val, add, memory_order_release) + add;
}

static int
atomic_cas_ptr(atomicptr_t* dst, void* val, void* ref) {
  return atomic_compare_exchange_weak_explicit(dst, &ref, val, memory_order_release, memory_order_acquire);
}

#elif defined(_MSC_VER)

#if defined( __x86_64__ ) || defined( _M_AMD64 ) || defined( _M_X64 ) || defined( _AMD64_ ) || defined( __arm64__ ) || defined( __aarch64__ )
#  define ARCH_64BIT 1
#else
#  define ARCH_64BIT 0
#endif

#ifdef _MSC_VER
#  define ALIGNED_STRUCT(name, alignment) __declspec(align(alignment)) struct name
#else
#  define ALIGNED_STRUCT(name, alignment) struct __attribute__((__aligned__(alignment))) name
#endif

ALIGNED_STRUCT(atomicptr_t, 8) {
  void* nonatomic;
};
typedef struct atomicptr_t atomicptr_t;

static void*
atomic_load_ptr(atomicptr_t* src) {
  return src->nonatomic;
}

static void
atomic_store_ptr(atomicptr_t* dst, void* val) {
  dst->nonatomic = val;
}

ALIGNED_STRUCT(atomic32_t, 4) {
  int32_t nonatomic;
};
typedef struct atomic32_t atomic32_t;

static int32_t
atomic_load32(atomic32_t* src) {
  return src->nonatomic;
}

static void
atomic_store32(atomic32_t* dst, int32_t val) {
  dst->nonatomic = val;
}

static int32_t
atomic_incr32(atomic32_t* val) {
  int32_t old = (int32_t)_InterlockedExchangeAdd((volatile long*)&val->nonatomic, 1);
  return (old + 1);
}

static int32_t
atomic_decr32(atomic32_t* val) {
  int32_t old = (int32_t)_InterlockedExchangeAdd((volatile long*)&val->nonatomic, -1);
  return (old - 1);
}

static int32_t
atomic_add32(atomic32_t* val, int32_t add) {
  int32_t old = (int32_t)_InterlockedExchangeAdd((volatile long*)&val->nonatomic, add);
  return (old + add);
}

static int
atomic_cas_ptr(atomicptr_t* dst, void* val, void* ref) {
#if ARCH_64BIT
  return (_InterlockedCompareExchange64((volatile long long*)&dst->nonatomic,
    (long long)val, (long long)ref) == (long long)ref) ? 1 : 0;
#else
  return (_InterlockedCompareExchange((volatile long*)&dst->nonatomic,
    (long)val, (long)ref) == (long)ref) ? 1 : 0;
#endif
}

#undef ARCH_64BIT
#undef ALIGNED_STRUCT

#else
#  error Compiler not supported yet
#endif
