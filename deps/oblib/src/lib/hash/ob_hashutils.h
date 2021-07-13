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

#ifndef OCEANBASE_LIB_HASH_OB_HASHUTILS_
#define OCEANBASE_LIB_HASH_OB_HASHUTILS_

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include <algorithm>
#include <typeinfo>
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_latch.h"
#include "lib/utility/ob_print_utils.h"

#define HASH_FATAL ERROR
#define HASH_WARNING WARN
#define HASH_NOTICE INFO
#define HASH_TRACE DEBUG
#define HASH_DEBUG DEBUG

#ifndef _PERF_TEST_
#define HASH_WRITE_LOG(_loglevel_, _fmt_, args...) \
  {                                                \
    _OB_LOG(_loglevel_, _fmt_, ##args);            \
  }
#else
#define HASH_WRITE_LOG(_loglevel_, _fmt_, args...) \
  {}
#endif

#ifdef __cplusplus
extern "C" {
#endif
struct _ParseNode;
typedef struct _ParseNode ParseNode;
extern uint64_t parsenode_hash(const ParseNode* node);
extern bool parsenode_equal(const ParseNode* node1, const ParseNode* node2);
#ifdef __cplusplus
}
#endif

namespace oceanbase {
namespace common {
namespace hash {
/**
 * Discard error code alias, please use the OB_HASH_XXX macros directly.
 *
enum
{
  HASH_EXIST = OB_HASH_EXIST,
  HASH_NOT_EXIST = OB_HASH_NOT_EXIST,
  HASH_OVERWRITE_SUCC = OB_HASH_OVERWRITE_SUCC,
  HASH_INSERT_SUCC = OB_HASH_INSERT_SUCC,
  HASH_GET_TIMEOUT = OB_HASH_GET_TIMEOUT,
  HASH_PLACEMENT_RETRY = OB_HASH_PLACEMENT_RETRY,
  HASH_FULL = OB_HASH_FULL,

  HASH_INVALID_ARGUMENT = OB_INVALID_ARGUMENT,
  HASH_NOT_INIT = OB_NOT_INIT,
  HASH_INIT_TWICE = OB_INIT_TWICE,
  HASH_CREATE_ERROR = OB_INIT_FAIL,
  HASH_ALLOC_MEM_FAIL = OB_ALLOCATE_MEMORY_FAILED,
  HASH_ERR = OB_ERR_UNEXPECTED, // general error
};
**/

class SpinLocker {
public:
  explicit SpinLocker(pthread_spinlock_t& spin) : succ_(false), spin_(NULL)
  {
    if (0 != pthread_spin_lock(&spin)) {
      HASH_WRITE_LOG(HASH_WARNING, "lock spin fail errno=%u", errno);
    } else {
      // HASH_WRITE_LOG(HASH_DEBUG, "lock spin succ spin=%p", &spin);
      spin_ = &spin;
      succ_ = true;
    }
  }
  ~SpinLocker()
  {
    if (NULL != spin_) {
      pthread_spin_unlock(spin_);
      // HASH_WRITE_LOG(HASH_DEBUG, "unlock spin succ spin=%p", spin_);
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  SpinLocker()
  {}

private:
  bool succ_;
  pthread_spinlock_t* spin_;
};

class MutexLocker {
public:
  explicit MutexLocker(pthread_mutex_t& mutex) : succ_(false), mutex_(NULL)
  {
    if (0 != pthread_mutex_lock(&mutex)) {
      HASH_WRITE_LOG(HASH_WARNING, "lock mutex fail errno=%u", errno);
    } else {
      // HASH_WRITE_LOG(HASH_DEBUG, "lock mutex succ mutex=%p", &mutex);
      mutex_ = &mutex;
      succ_ = true;
    }
  }
  ~MutexLocker()
  {
    if (NULL != mutex_) {
      (void)pthread_mutex_unlock(mutex_);
      // HASH_WRITE_LOG(HASH_DEBUG, "unlock mutex succ mutex=%p", mutex_);
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  MutexLocker()
  {}

private:
  bool succ_;
  pthread_mutex_t* mutex_;
};

class MutexWaiter {
public:
  MutexWaiter()
  {}
  ~MutexWaiter()
  {}
  int operator()(pthread_cond_t& cond, pthread_mutex_t& lock, struct timespec& ts)
  {
    return pthread_cond_timedwait(&cond, &lock, &ts);
  }
};

class MutexBroadCaster {
public:
  MutexBroadCaster()
  {}
  ~MutexBroadCaster()
  {}
  int operator()(pthread_cond_t& cond)
  {
    return pthread_cond_broadcast(&cond);
  }
};

class ReadLocker {
public:
  explicit ReadLocker(pthread_rwlock_t& rwlock) : succ_(false), rwlock_(NULL)
  {
    if (0 != pthread_rwlock_rdlock(&rwlock)) {
      HASH_WRITE_LOG(HASH_WARNING, "rdlock rwlock fail errno=%u", errno);
    } else {
      // HASH_WRITE_LOG(HASH_DEBUG, "rdlock rwlock succ rwlock=%p", &rwlock);
      rwlock_ = &rwlock;
      succ_ = true;
    }
  }
  ~ReadLocker()
  {
    if (NULL != rwlock_) {
      pthread_rwlock_unlock(rwlock_);
      // HASH_WRITE_LOG(HASH_DEBUG, "unlock rwlock succ rwlock=%p", rwlock_);
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  ReadLocker()
  {}

private:
  bool succ_;
  pthread_rwlock_t* rwlock_;
};

class WriteLocker {
public:
  explicit WriteLocker(pthread_rwlock_t& rwlock) : succ_(false), rwlock_(NULL)
  {
    if (0 != pthread_rwlock_wrlock(&rwlock)) {
      HASH_WRITE_LOG(HASH_WARNING, "wrlock wrlock fail errno=%u", errno);
    } else {
      // HASH_WRITE_LOG(HASH_DEBUG, "wrlock rwlock succ rwlock=%p", &rwlock);
      rwlock_ = &rwlock;
      succ_ = true;
    }
  }
  ~WriteLocker()
  {
    if (NULL != rwlock_) {
      pthread_rwlock_unlock(rwlock_);
      // HASH_WRITE_LOG(HASH_DEBUG, "unlock rwlock succ rwlock=%p", rwlock_);
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  WriteLocker()
  {}

private:
  bool succ_;
  pthread_rwlock_t* rwlock_;
};

class RWLockIniter {
public:
  explicit RWLockIniter(pthread_rwlock_t& rwlock) : succ_(false)
  {
    if (0 != pthread_rwlock_init(&rwlock, NULL)) {
      HASH_WRITE_LOG(HASH_WARNING, "init rwlock fail errno=%u rwlock=%p", errno, &rwlock);
    } else {
      succ_ = true;
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  RWLockIniter();

private:
  bool succ_;
};

class LatchReadLocker {
public:
  explicit LatchReadLocker(common::ObLatch& rwlock) : succ_(false), rwlock_(NULL)
  {
    int ret = common::OB_SUCCESS;
    if (common::OB_SUCCESS != (ret = rwlock.rdlock(ObLatchIds::HASH_MAP_LOCK))) {
      HASH_WRITE_LOG(HASH_WARNING, "rdlock rwlock fail errno=%u", ret);
    } else {
      rwlock_ = &rwlock;
      succ_ = true;
    }
  }
  ~LatchReadLocker()
  {
    if (NULL != rwlock_) {
      rwlock_->unlock();
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  LatchReadLocker()
  {}

private:
  bool succ_;
  common::ObLatch* rwlock_;
};

class LatchWriteLocker {
public:
  explicit LatchWriteLocker(common::ObLatch& rwlock) : succ_(false), rwlock_(NULL)
  {
    int ret = common::OB_SUCCESS;
    if (OB_SUCCESS != (ret = rwlock.wrlock(ObLatchIds::HASH_MAP_LOCK))) {
      HASH_WRITE_LOG(HASH_WARNING, "wrlock wrlock fail errno=%u", ret);
    } else {
      rwlock_ = &rwlock;
      succ_ = true;
    }
  }
  ~LatchWriteLocker()
  {
    if (NULL != rwlock_) {
      rwlock_->unlock();
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  LatchWriteLocker()
  {}

private:
  bool succ_;
  common::ObLatch* rwlock_;
};

///

class SpinReadLocker {
public:
  explicit SpinReadLocker(SpinRWLock& rwlock) : succ_(false), rwlock_(NULL)
  {
    if (0 != rwlock.rdlock()) {
      HASH_WRITE_LOG(HASH_WARNING, "rdlock rwlock fail errno=%u", errno);
    } else {
      rwlock_ = &rwlock;
      succ_ = true;
    }
  }
  ~SpinReadLocker()
  {
    if (NULL != rwlock_) {
      rwlock_->unlock();
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  SpinReadLocker()
  {}

private:
  bool succ_;
  SpinRWLock* rwlock_;
};

class SpinWriteLocker {
public:
  explicit SpinWriteLocker(SpinRWLock& rwlock) : succ_(false), rwlock_(NULL)
  {
    if (0 != rwlock.wrlock()) {
      HASH_WRITE_LOG(HASH_WARNING, "wrlock wrlock fail errno=%u", errno);
    } else {
      rwlock_ = &rwlock;
      succ_ = true;
    }
  }
  ~SpinWriteLocker()
  {
    if (NULL != rwlock_) {
      rwlock_->unlock();
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  SpinWriteLocker()
  {}

private:
  bool succ_;
  SpinRWLock* rwlock_;
};

class SpinIniter {
public:
  explicit SpinIniter(pthread_spinlock_t& spin) : succ_(false)
  {
    if (0 != pthread_spin_init(&spin, PTHREAD_PROCESS_PRIVATE)) {
      HASH_WRITE_LOG(HASH_WARNING, "init mutex fail errno=%u spin=%p", errno, &spin);
    } else {
      succ_ = true;
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  SpinIniter();

private:
  bool succ_;
};

class MutexIniter {
public:
  explicit MutexIniter(pthread_mutex_t& mutex) : succ_(false)
  {
    if (0 != pthread_mutex_init(&mutex, NULL)) {
      HASH_WRITE_LOG(HASH_WARNING, "init mutex fail errno=%u mutex=%p", errno, &mutex);
    } else {
      succ_ = true;
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }

private:
  MutexIniter();

private:
  bool succ_;
};

class NLock {
public:
  NLock()
  {}
  ~NLock()
  {}
};

class NCond {
public:
  NCond()
  {}
  ~NCond()
  {}
};

class NullIniter {
public:
  explicit NullIniter(NLock& nlock)
  {
    NLock* usr = NULL;
    usr = &nlock;
    UNUSED(usr);
  }

private:
  NullIniter();
};

class NullLocker {
public:
  explicit NullLocker(pthread_mutex_t& mutex)
  {
    pthread_mutex_t* usr = NULL;
    usr = &mutex;
    UNUSED(usr);
    // HASH_WRITE_LOG(HASH_DEBUG, "nulllocker lock succ mutex=%p", &mutex);
  }
  explicit NullLocker(NLock& nlock)
  {
    NLock* usr = NULL;
    usr = &nlock;
    UNUSED(usr);
    // HASH_WRITE_LOG(HASH_DEBUG, "nulllocker lock succ nlock=%p", &nlock);
  }
  ~NullLocker()
  {
    // HASH_WRITE_LOG(HASH_DEBUG, "nulllocker unlock succ");
  }
  bool lock_succ() const
  {
    return true;
  }

private:
  NullLocker()
  {}
};

template <class T>
class NWaiter {
public:
  NWaiter()
  {}
  ~NWaiter()
  {}
  int operator()(NCond& cond, T& lock, struct timespec& ts)
  {
    UNUSED(cond);
    UNUSED(lock);
    UNUSED(ts);
    return 0;
  }
};

class NBroadCaster {
public:
  NBroadCaster()
  {}
  ~NBroadCaster()
  {}
  int operator()(NCond& cond)
  {
    UNUSED(cond);
    return 0;
  }
};

struct LatchReadWriteDefendMode {
  typedef LatchReadLocker readlocker;
  typedef LatchWriteLocker writelocker;
  typedef common::ObLatch lock_type;
  typedef NCond cond_type;
  typedef NullIniter lock_initer;
  typedef NWaiter<lock_type> cond_waiter;
  typedef NBroadCaster cond_broadcaster;
};

struct ReadWriteDefendMode {
  typedef ReadLocker readlocker;
  typedef WriteLocker writelocker;
  typedef pthread_rwlock_t lock_type;
  typedef NCond cond_type;
  typedef RWLockIniter lock_initer;
  typedef NWaiter<lock_type> cond_waiter;
  typedef NBroadCaster cond_broadcaster;
};
struct SpinReadWriteDefendMode {
  typedef SpinReadLocker readlocker;
  typedef SpinWriteLocker writelocker;
  typedef SpinRWLock lock_type;
  typedef NCond cond_type;
  typedef NullIniter lock_initer;
  typedef NWaiter<lock_type> cond_waiter;
  typedef NBroadCaster cond_broadcaster;
};
struct SpinMutexDefendMode {
  typedef NullLocker readlocker;
  typedef SpinLocker writelocker;
  typedef pthread_spinlock_t lock_type;
  typedef NCond cond_type;
  typedef SpinIniter lock_initer;
  typedef NWaiter<lock_type> cond_waiter;
  typedef NBroadCaster cond_broadcaster;
};
struct MultiWriteDefendMode {
  typedef MutexLocker readlocker;
  typedef MutexLocker writelocker;
  typedef pthread_mutex_t lock_type;
  typedef pthread_cond_t cond_type;
  typedef MutexIniter lock_initer;
  typedef MutexWaiter cond_waiter;
  typedef MutexBroadCaster cond_broadcaster;
};
struct NoPthreadDefendMode {
  typedef NullLocker readlocker;
  typedef NullLocker writelocker;
  typedef NLock lock_type;
  typedef NCond cond_type;
  typedef NullIniter lock_initer;
  typedef NWaiter<lock_type> cond_waiter;
  typedef NBroadCaster cond_broadcaster;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

inline int64_t tv_to_microseconds(const timeval& tp)
{
  return (((int64_t)tp.tv_sec) * 1000000 + (int64_t)tp.tv_usec);
}

inline int64_t get_cur_microseconds_time()
{
  struct timeval tp;
  (void)gettimeofday(&tp, NULL);
  return tv_to_microseconds(tp);
}

inline timespec microseconds_to_ts(const int64_t microseconds)
{
  struct timespec ts;
  ts.tv_sec = microseconds / 1000000;
  ts.tv_nsec = (microseconds % 1000000) * 1000;
  return ts;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

enum { PRIME_NUM = 28 };
extern const int64_t PRIME_LIST[];

inline int64_t cal_next_prime(const int64_t n)
{
  int64_t prime = n;
  if (n > 0) {
    const int64_t* first = PRIME_LIST;
    const int64_t* last = PRIME_LIST + PRIME_NUM;
    const int64_t* pos = std::lower_bound(first, last, n);
    prime = ((pos == last) ? *(last - 1) : *pos);
  }
  return prime;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

template <class T>
struct pre_proc {
  T& operator()(T& t) const
  {
    return t;
  }
};

////////////////////////////////////////////////////////////////////////////////////////////////////

template <class _key>
struct equal_to {
  bool operator()(const _key& a, const _key& b) const
  {
    return (a == b);
  }
};

template <class _key>
struct equal_to<_key*> {
  bool operator()(_key* a, _key* b) const
  {
    return (*a == *b);
  }
};

template <class _key>
struct equal_to<const _key*> {
  bool operator()(const _key* a, const _key* b) const
  {
    return (*a == *b);
  }
};

template <>
struct equal_to<_ParseNode*> {
  bool operator()(_ParseNode* a, _ParseNode* b) const
  {
    return parsenode_equal(a, b);
  }
};

template <>
struct equal_to<const _ParseNode*> {
  bool operator()(const _ParseNode* a, const _ParseNode* b) const
  {
    return parsenode_equal(a, b);
  }
};
////////////////////////////////////////////////////////////////////////////////////////////////////

template <class _key>
struct hash_func {
  int64_t operator()(const _key& key) const
  {
    return key.hash();
  }
};
template <class _key>
struct hash_func<_key*> {
  int64_t operator()(_key* key) const
  {
    return key->hash();
  }
};
template <class _key>
struct hash_func<const _key*> {
  int64_t operator()(const _key* key) const
  {
    return key->hash();
  }
};
template <>
struct hash_func<ObString> {
  uint64_t operator()(const ObString& key) const
  {
    return murmurhash(key.ptr(), key.length(), 0);
  }
};
template <>
struct hash_func<const char*> {
  uint64_t operator()(const char* key) const
  {
    return murmurhash(key, static_cast<int32_t>(strlen(key)), 0);
  }
};
template <>
struct hash_func<char*> {
  uint64_t operator()(const char* key) const
  {
    return murmurhash(key, static_cast<int32_t>(strlen(key)), 0);
  }
};
template <>
struct hash_func<std::pair<int, uint32_t> > {
  int64_t operator()(std::pair<int, uint32_t> key) const
  {
    return key.first + key.second;
  }
};
template <>
struct hash_func<std::pair<uint64_t, uint64_t> > {
  int64_t operator()(std::pair<uint64_t, uint64_t> key) const
  {
    return (int64_t)(key.first + key.second);
  }
};

template <>
struct hash_func<_ParseNode*> {
  uint64_t operator()(const _ParseNode* key) const
  {
    return parsenode_hash(key);
  }
};
template <>
struct hash_func<const _ParseNode*> {
  uint64_t operator()(const _ParseNode* key) const
  {
    return parsenode_hash(key);
  }
};

#define _HASH_FUNC_SPEC(type)                 \
  template <>                                 \
  struct hash_func<type> {                    \
    int64_t operator()(const type& key) const \
    {                                         \
      return (int64_t)key;                    \
    }                                         \
  };
_HASH_FUNC_SPEC(int8_t);
_HASH_FUNC_SPEC(uint8_t);
_HASH_FUNC_SPEC(int16_t);
_HASH_FUNC_SPEC(uint16_t);
_HASH_FUNC_SPEC(int32_t);
_HASH_FUNC_SPEC(uint32_t);
_HASH_FUNC_SPEC(int64_t);
_HASH_FUNC_SPEC(uint64_t);

////////////////////////////////////////////////////////////////////////////////////////////////////

struct HashNullObj {};

template <class _key_type, class _value_type>
struct HashMapPair {
  typedef _key_type first_type;
  typedef _value_type second_type;
  first_type first;
  second_type second;
  HashMapPair() : first(), second()
  {}
  // HashMapPair(const first_type &a, const second_type &b) : first(a), second(b) {}

  int init(const first_type& a, const second_type& b)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(first, a)))) {
      _OB_LOG(ERROR, "copy first failed, ret=[%d]", ret);
    } else if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(second, b)))) {
      _OB_LOG(ERROR, "copy second failed, ret=[%d]", ret);
    }
    return ret;
  }

  int assign(const HashMapPair& other)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(first, other.first)))) {
      _OB_LOG(ERROR, "copy first failed, ret=[%d]", ret);
    } else if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(second, other.second)))) {
      _OB_LOG(ERROR, "copy second failed, ret=[%d]", ret);
    }
    return ret;
  }

  int overwrite(const HashMapPair& pair)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(second, pair.second)))) {
      _OB_LOG(ERROR, "copy failed, ret=[%d]", ret);
    }
    return ret;
  }
  TO_STRING_KV(K(first), K(second));

private:
  DISALLOW_COPY_AND_ASSIGN(HashMapPair);
};

struct NormalPairTag {};
struct HashMapPairTag {};

template <class T, class K = T>
struct PairTraits {
  typedef NormalPairTag PairTag;
};
template <class T, class K>
struct PairTraits<HashMapPair<T, K> > {
  typedef HashMapPairTag PairTag;
};

template <class Pair>
int copy(Pair& dest, const Pair& src, NormalPairTag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(dest, src)))) {
    _OB_LOG(ERROR, "copy failed, ret=[%d]", ret);
  }
  return ret;
}
template <class Pair>
int copy(Pair& dest, const Pair& src)
{
  return do_copy(dest, src, typename PairTraits<Pair>::PairTag());
}
template <class Pair>
int do_copy(Pair& dest, const Pair& src, NormalPairTag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(dest, src)))) {
    _OB_LOG(ERROR, "copy failed, ret=[%d]", ret);
  }
  return ret;
}
template <class Pair>
int do_copy(Pair& dest, const Pair& src, HashMapPairTag)
{
  return dest.overwrite(src);
}

template <class _pair_type>
struct pair_first {
  typedef typename _pair_type::first_type type;
  const type& operator()(const _pair_type& pair) const
  {
    return pair.first;
  }
};

template <class _pair_type>
struct pair_second {
  typedef typename _pair_type::second_type type;
  const type& operator()(const _pair_type& pair) const
  {
    return pair.second;
  }
};

////////////////////////////////////////////////////////////////////////////////////////////////////

struct BigArrayTag {};
struct NormalPointerTag {};

struct DefaultBigArrayAllocator {
  void* alloc(const int64_t sz)
  {
    void* p = NULL;
    if (sz > 0) {
      p = ob_malloc(sz, ObModIds::OB_HASH_BUCKET);
    }
    return p;
  }
  void free(void* p)
  {
    if (NULL != p) {
      ob_free(p);
      p = NULL;
    }
  }
};
template <class T, class Allocer = DefaultBigArrayAllocator>
class BigArrayTemp {
  class Block {
  public:
    Block() : array_(NULL), allocer_(NULL)
    {}
    ~Block()
    {
      destroy();
    }

  public:
    int create(const int64_t array_size, Allocer* allocer)
    {
      int ret = 0;
      if (NULL != array_) {
        ret = -1;
      } else if (0 == array_size || NULL == allocer) {
        ret = -1;
      } else if (NULL == (array_ = (T*)allocer->alloc(array_size * sizeof(T)))) {
        HASH_WRITE_LOG(HASH_WARNING, "alloc fail size=%ld array_size=%ld", array_size * sizeof(T), array_size);
        ret = -1;
      } else {
        memset(array_, 0, array_size * sizeof(T));
        allocer_ = allocer;
      }
      return ret;
    }
    void destroy()
    {
      if (NULL != allocer_ && NULL != array_) {
        allocer_->free(array_);
      }
      array_ = NULL;
      allocer_ = NULL;
    }

  public:
    T& operator[](const int64_t pos)
    {
      T& ret = array_[pos];
      return ret;
    }
    const T& operator[](const int64_t pos) const
    {
      T& ret = array_[pos];
      return ret;
    }

  private:
    T* array_;
    Allocer* allocer_;
  };

public:
  typedef BigArrayTag ArrayTag;
  typedef BigArrayTemp<T, Allocer> array_type;

public:
  BigArrayTemp() : blocks_(NULL), array_size_(0), blocks_num_(0)
  {}
  ~BigArrayTemp()
  {
    destroy();
  }

public:
  bool inited() const
  {
    return (NULL != blocks_);
  }
  int create(const int64_t total_size, const int64_t array_size = INT32_MAX)
  {
    int ret = 0;
    if (0 >= total_size || 0 >= array_size) {
      ret = -1;
    } else if (!is_2exp(array_size)) {
      HASH_WRITE_LOG(HASH_WARNING, "array_size=%ld invalid,  must be 2exp", array_size);
      ret = -1;
    } else {
      if (INT32_MAX == array_size) {
        blocks_num_ = 1;
        array_size_ = total_size;
      } else {
        blocks_num_ = upper_align(total_size, array_size) / array_size;
        array_size_ = array_size;
      }
      if (NULL == (blocks_ = (Block*)allocer_.alloc(blocks_num_ * sizeof(Block)))) {
        HASH_WRITE_LOG(HASH_WARNING, "alloc blocks fail blocks_num=%ld array_size=%ld", blocks_num_, array_size_);
        ret = -1;
      } else {
        memset(blocks_, 0, blocks_num_ * sizeof(Block));
        for (int64_t i = 0; i < blocks_num_; i++) {
          if (0 != blocks_[i].create(array_size_, &allocer_)) {
            ret = -1;
            break;
          }
        }
      }
    }
    if (0 != ret) {
      destroy();
    }
    return ret;
  }
  void destroy()
  {
    if (NULL != blocks_) {
      for (int64_t i = 0; i < blocks_num_; i++) {
        blocks_[i].destroy();
      }
      allocer_.free(blocks_);
    }
    blocks_ = NULL;
    array_size_ = 0;
    blocks_num_ = 0;
  }
  T& operator[](const int64_t pos)
  {
    int64_t block_pos = pos / array_size_;
    // int64_t array_pos = pos % array_size_;
    int64_t array_pos = mod_2exp(pos, array_size_);
    return blocks_[block_pos][array_pos];
  }
  const T& operator[](const int64_t pos) const
  {
    int64_t block_pos = pos / array_size_;
    // int64_t array_pos = pos % array_size_;
    int64_t array_pos = mod_2exp(pos, array_size_);
    return blocks_[block_pos][array_pos];
  }

public:
  static int64_t upper_align(const int64_t input, const int64_t align)
  {
    int64_t ret = input;
    ret = (input + align - 1) & ~(align - 1);
    return ret;
  }
  static bool is_2exp(const int64_t n)
  {
    return !(n & (n - 1));
  }
  static int64_t mod_2exp(const int64_t input, const int64_t align)
  {
    return input & (align - 1);
  }

private:
  Block* blocks_;
  Allocer allocer_;
  int64_t array_size_;
  int64_t blocks_num_;
};

template <class T>
class BigArray : public BigArrayTemp<T, DefaultBigArrayAllocator> {};

template <class T>
struct NormalPointer {
  typedef T* array_type;
};

template <class Array>
struct ArrayTraits {
  typedef typename Array::ArrayTag ArrayTag;
};
template <class Array>
struct ArrayTraits<Array*> {
  typedef NormalPointerTag ArrayTag;
};
template <class Array>
struct ArrayTraits<const Array*> {
  typedef NormalPointerTag ArrayTag;
};

template <class Array>
void construct(Array& array)
{
  do_construct(array, typename ArrayTraits<Array>::ArrayTag());
}

template <class Array>
void do_construct(Array& array, BigArrayTag)
{
  UNUSED(array);
}

template <class Array>
void do_construct(Array& array, NormalPointerTag)
{
  array = NULL;
}

template <class Array, class BucketAllocator>
int create(
    Array& array, const int64_t total_size, const int64_t array_size, const int64_t item_size, BucketAllocator& alloc)
{
  return do_create(array, total_size, array_size, item_size, typename ArrayTraits<Array>::ArrayTag(), alloc);
}

template <class Array, class BucketAllocator>
int do_create(Array& array, const int64_t total_size, const int64_t array_size, const int64_t item_size, BigArrayTag,
    BucketAllocator& alloc)
{
  UNUSED(item_size);
  UNUSED(alloc);
  return array.create(total_size, array_size);
}

template <class Array, class BucketAllocator>
int do_create(Array& array, const int64_t total_size, const int64_t array_size, const int64_t item_size,
    NormalPointerTag, BucketAllocator& alloc)
{
  UNUSED(array_size);
  int ret = 0;
  if (total_size <= 0 || item_size <= 0) {
    ret = -1;
  } else if (NULL == (array = (Array)alloc.alloc(total_size * item_size))) {
    _OB_LOG(WARN, "alloc memory failed,size:%ld", total_size * item_size);
    ret = -1;
  } else {
    // BACKTRACE(WARN, total_size * item_size > 65536, "hashutil create init size=%ld", total_size * item_size);
    memset(array, 0, total_size * item_size);
  }
  return ret;
}

template <class Array>
bool inited(const Array& array)
{
  return do_inited(array, typename ArrayTraits<Array>::ArrayTag());
}

template <class Array>
bool do_inited(const Array& array, BigArrayTag)
{
  return array.inited();
}

template <class Array>
bool do_inited(const Array& array, NormalPointerTag)
{
  return (NULL != array);
}

template <class Array, class BucketAllocator>
void destroy(Array& array, BucketAllocator& alloc)
{
  do_destroy(array, typename ArrayTraits<Array>::ArrayTag(), alloc);
}

template <class Array, class BucketAllocator>
void do_destroy(Array& array, BigArrayTag, BucketAllocator&)
{
  array.destroy();
}

template <class Array, class BucketAllocator>
void do_destroy(Array& array, NormalPointerTag, BucketAllocator& alloc)
{
  if (NULL != array) {
    alloc.free(array);
    array = NULL;
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

struct DefaultSimpleAllocerAllocator {
public:
  explicit DefaultSimpleAllocerAllocator(
      uint64_t tenant_id = OB_SERVER_TENANT_ID, const lib::ObLabel& label = ObModIds::OB_HASH_NODE)
  {
    attr_.tenant_id_ = tenant_id;
    attr_.label_ = label;
  }
  void* alloc(const int64_t sz)
  {
    void* p = NULL;
    if (sz > 0) {
      p = ob_malloc(sz, attr_);
    }
    return p;
  }
  void free(void* p)
  {
    if (NULL != p) {
      ob_free(p);
      p = NULL;
    }
  }
  void set_attr(const ObMemAttr& attr)
  {
    attr_ = attr;
  }
  void set_label(const lib::ObLabel& label)
  {
    attr_.label_ = label;
  }

private:
  ObMemAttr attr_;
};

template <class T, int64_t NODE_NUM>
struct SimpleAllocerBlock;

template <class T, int64_t NODE_NUM>
struct SimpleAllocerNode {
  typedef SimpleAllocerNode<T, NODE_NUM> Node;
  typedef SimpleAllocerBlock<T, NODE_NUM> Block;

  T data;
  uint32_t magic1;
  Node* next;
  Block* block;
  uint32_t magic2;
};

template <class T, int64_t NODE_NUM>
struct SimpleAllocerBlock {
  typedef SimpleAllocerNode<T, NODE_NUM> Node;
  typedef SimpleAllocerBlock<T, NODE_NUM> Block;

  int32_t ref_cnt;
  int32_t cur_pos;
  char nodes_buffer[NODE_NUM * sizeof(Node)];
  Node* nodes;
  Block* next;
};

template <bool B, class T>
struct NodeNumTraits {};

template <class T>
struct NodeNumTraits<false, T> {
  static const int32_t NODE_NUM =
      (common::OB_MALLOC_NORMAL_BLOCK_SIZE - 24 /*=sizeof(SimpleAllocerBlock 's members except nodes_buffer)*/ -
          128 /*for robust*/) /
      (32 /*sizeof(SimpleAllocerNode's members except data)*/ + sizeof(T));
};

template <class T>
struct NodeNumTraits<true, T> {
  static const int32_t NODE_NUM = 1;
};

#define IS_BIG_OBJ(T) ((common::OB_MALLOC_NORMAL_BLOCK_SIZE - 24 - 128) < (32 + sizeof(T)))

template <class T, int32_t NODE_NUM = NodeNumTraits<IS_BIG_OBJ(T), T>::NODE_NUM, class DefendMode = SpinMutexDefendMode,
    class Allocer = DefaultSimpleAllocerAllocator>
class SimpleAllocer {
  typedef SimpleAllocerNode<T, NODE_NUM> Node;
  typedef SimpleAllocerBlock<T, NODE_NUM> Block;
  const static uint32_t NODE_MAGIC1 = 0x89abcdef;
  const static uint32_t NODE_MAGIC2 = 0x12345678;
  typedef typename DefendMode::writelocker mutexlocker;
  typedef typename DefendMode::lock_type lock_type;
  typedef typename DefendMode::lock_initer lock_initer;

public:
  SimpleAllocer() : block_list_head_(NULL), free_list_head_(NULL)
  {
    lock_initer initer(lock_);
  }
  ~SimpleAllocer()
  {
    clear();
  }
  void set_attr(const ObMemAttr& attr)
  {
    allocer_.set_attr(attr);
  }
  void set_label(const lib::ObLabel& label)
  {
    allocer_.set_label(label);
  }
  void clear()
  {
    Block* iter = block_list_head_;
    while (NULL != iter) {
      Block* next = iter->next;
      if (0 != iter->ref_cnt) {
        HASH_WRITE_LOG(HASH_FATAL,
            "there is still node has not been free, ref_cnt=%d block=%p cur_pos=%d",
            iter->ref_cnt,
            iter,
            iter->cur_pos);
      } else {
        // delete iter;
        allocer_.free(iter);
      }
      iter = next;
    }
    block_list_head_ = NULL;
    free_list_head_ = NULL;
  }
  template <class... TYPES>
  T* alloc(TYPES&... args)
  {
    T* ret = NULL;
    mutexlocker locker(lock_);
    if (NULL != free_list_head_) {
      Node* node = free_list_head_;
      if (NODE_MAGIC1 != node->magic1 || NODE_MAGIC2 != node->magic2 || NULL == node->block) {
        HASH_WRITE_LOG(HASH_FATAL, "magic broken magic1=%x magic2=%x", node->magic1, node->magic2);
      } else {
        free_list_head_ = node->next;
        node->block->ref_cnt++;
        ret = &(node->data);
      }
    } else {
      Block* block = block_list_head_;
      if (NULL == block || block->cur_pos >= (int32_t)NODE_NUM) {
        // if (NULL == (block = new(std::nothrow) Block()))
        if (NULL == (block = (Block*)allocer_.alloc(sizeof(Block)))) {
          HASH_WRITE_LOG(HASH_WARNING, "new block fail");
        } else {
          // BACKTRACE(WARN, ((int64_t)sizeof(Block))>DEFAULT_BLOCK_SIZE,
          // "hashutil alloc block=%ld node=%ld T=%ld N=%d",
          //               sizeof(Block), sizeof(Node), sizeof(T), NODE_NUM);
          // memset(block, 0, sizeof(Block));
          block->ref_cnt = 0;
          block->cur_pos = 0;
          block->nodes = (Node*)(block->nodes_buffer);
          block->next = block_list_head_;
          block_list_head_ = block;
        }
      }
      if (NULL != block) {
        Node* node = block->nodes + block->cur_pos;
        block->cur_pos++;
        block->ref_cnt++;
        node->magic1 = NODE_MAGIC1;
        node->next = NULL;
        node->block = block;
        node->magic2 = NODE_MAGIC2;
        ret = &(node->data);
      }
    }
    if (NULL != ret) {
      new (ret) T(args...);
    }
    return ret;
  }
  void free(T* data)
  {
    mutexlocker locker(lock_);
    if (NULL == data) {
      HASH_WRITE_LOG(HASH_WARNING, "invalid param null pointer");
    } else {
      Node* node = (Node*)data;
      if (NODE_MAGIC1 != node->magic1 || NODE_MAGIC2 != node->magic2) {
        HASH_WRITE_LOG(HASH_FATAL, "magic broken magic1=%x magic2=%x", node->magic1, node->magic2);
      } else {
        data->~T();
        node->block->ref_cnt--;
        node->next = free_list_head_;
        free_list_head_ = node;
      }
    }
  }
  void inc_ref()
  {}
  void dec_ref()
  {}
  void gc()
  {
    mutexlocker locker(lock_);
    if (NULL != free_list_head_ && NULL != block_list_head_) {
      Block* block_iter = block_list_head_;
      Block* block_next = NULL;
      Block* block_prev = NULL;
      while (NULL != block_iter) {
        block_next = block_iter->next;
        if (0 == block_iter->ref_cnt && 0 != block_iter->cur_pos) {
          Node* node_iter = free_list_head_;
          Node* node_prev = free_list_head_;
          volatile int32_t counter = 0;
          while (NULL != node_iter && counter < NODE_NUM) {
            if (block_iter == node_iter->block) {
              if (free_list_head_ == node_iter) {
                free_list_head_ = node_iter->next;
              } else {
                node_prev->next = node_iter->next;
              }
              counter++;
            } else {
              node_prev = node_iter;
            }
            node_iter = node_iter->next;
          }

          if (block_list_head_ == block_iter) {
            block_list_head_ = block_iter->next;
          } else {
            block_prev->next = block_iter->next;
          }
          // delete block_iter;
          allocer_.free(block_iter);
          HASH_WRITE_LOG(HASH_DEBUG, "free succ block=%p", block_iter);
          block_iter = NULL;
        } else {
          block_prev = block_iter;
        }
        block_iter = block_next;
      }
    }
  }

private:
  Block* block_list_head_;
  Node* free_list_head_;
  lock_type lock_;
  Allocer allocer_;
};
}  // namespace hash
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_HASH_HASHUTILS_H_
