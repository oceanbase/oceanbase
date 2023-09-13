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

#ifndef  OCEANBASE_LIB_HASH_OB_HASHUTILS_
#define  OCEANBASE_LIB_HASH_OB_HASHUTILS_

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

#define HASH_FATAL    ERROR
#define HASH_WARNING  WARN
#define HASH_NOTICE   INFO
#define HASH_TRACE    DEBUG
#define HASH_DEBUG    DEBUG

#ifndef _PERF_TEST_
#define HASH_WRITE_LOG(_loglevel_, _fmt_, args...) { \
    _OB_LOG(_loglevel_, _fmt_, ##args); \
  }
#define HASH_WRITE_LOG_RET(_loglevel_, errcode, _fmt_, args...) { \
    _OB_LOG_RET(_loglevel_, errcode, _fmt_, ##args); \
  }
#else
#define HASH_WRITE_LOG(_loglevel_, _fmt_, args...) { \
#define HASH_WRITE_LOG_RET(_loglevel_, errcode, _fmt_, args...) { \
  }
#endif

#ifdef __cplusplus
extern "C" {
#endif
struct _ParseNode;
extern uint64_t parsenode_hash(const _ParseNode *node, int *ret);
extern bool parsenode_equal(const _ParseNode *node1, const _ParseNode *node2, int *ret);
#ifdef __cplusplus
}
#endif

namespace oceanbase
{
namespace common
{
namespace hash
{
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

class SpinLocker
{
public:
  explicit SpinLocker(pthread_spinlock_t &spin) : succ_(false), spin_(NULL)
  {
    if (0 != pthread_spin_lock(&spin)) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_ERR_SYS, "lock spin fail errno=%u", errno);
    } else {
      //HASH_WRITE_LOG(HASH_DEBUG, "lock spin succ spin=%p", &spin);
      spin_ = &spin;
      succ_ = true;
    }
  }
  ~SpinLocker()
  {
    if (NULL != spin_) {
      pthread_spin_unlock(spin_);
      //HASH_WRITE_LOG(HASH_DEBUG, "unlock spin succ spin=%p", spin_);
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }
private:
  SpinLocker() {}
private:
  bool succ_;
  pthread_spinlock_t *spin_;
};

class MutexLocker
{
public:
  explicit MutexLocker(pthread_mutex_t &mutex) : succ_(false), mutex_(NULL)
  {
    if (0 != pthread_mutex_lock(&mutex)) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_ERR_SYS, "lock mutex fail errno=%u", errno);
    } else {
      //HASH_WRITE_LOG(HASH_DEBUG, "lock mutex succ mutex=%p", &mutex);
      mutex_ = &mutex;
      succ_ = true;
    }
  }
  ~MutexLocker()
  {
    if (NULL != mutex_) {
      (void)pthread_mutex_unlock(mutex_);
      //HASH_WRITE_LOG(HASH_DEBUG, "unlock mutex succ mutex=%p", mutex_);
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }
private:
  MutexLocker() {}
private:
  bool succ_;
  pthread_mutex_t *mutex_;
};

class MutexWaiter
{
public:
  MutexWaiter() {}
  ~MutexWaiter() {}
  int operator()(pthread_cond_t &cond, pthread_mutex_t &lock, struct timespec &ts)
  {
    return ob_pthread_cond_timedwait(&cond, &lock, &ts);
  }
};

class MutexBroadCaster
{
public:
  MutexBroadCaster() {}
  ~MutexBroadCaster() {}
  int operator()(pthread_cond_t &cond)
  {
    return pthread_cond_broadcast(&cond);
  }
};

class ReadLocker
{
public:
  explicit ReadLocker(pthread_rwlock_t &rwlock) : succ_(false), rwlock_(NULL)
  {
    if (0 != pthread_rwlock_rdlock(&rwlock)) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_ERR_SYS, "rdlock rwlock fail errno=%u", errno);
    } else {
      //HASH_WRITE_LOG(HASH_DEBUG, "rdlock rwlock succ rwlock=%p", &rwlock);
      rwlock_ = &rwlock;
      succ_ = true;
    }
  }
  ~ReadLocker()
  {
    if (NULL != rwlock_) {
      pthread_rwlock_unlock(rwlock_);
      //HASH_WRITE_LOG(HASH_DEBUG, "unlock rwlock succ rwlock=%p", rwlock_);
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }
private:
  ReadLocker() {}
private:
  bool succ_;
  pthread_rwlock_t *rwlock_;
};

class WriteLocker
{
public:
  explicit WriteLocker(pthread_rwlock_t &rwlock) : succ_(false), rwlock_(NULL)
  {
    if (0 != pthread_rwlock_wrlock(&rwlock)) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_ERR_SYS, "wrlock wrlock fail errno=%u", errno);
    } else {
      //HASH_WRITE_LOG(HASH_DEBUG, "wrlock rwlock succ rwlock=%p", &rwlock);
      rwlock_ = &rwlock;
      succ_ = true;
    }
  }
  ~WriteLocker()
  {
    if (NULL != rwlock_) {
      pthread_rwlock_unlock(rwlock_);
      //HASH_WRITE_LOG(HASH_DEBUG, "unlock rwlock succ rwlock=%p", rwlock_);
    }
  }
  bool lock_succ() const
  {
    return succ_;
  }
private:
  WriteLocker() {}
private:
  bool succ_;
  pthread_rwlock_t *rwlock_;
};

class RWLockIniter
{
public:
  explicit RWLockIniter(pthread_rwlock_t &rwlock) : succ_(false)
  {
    if (0 != pthread_rwlock_init(&rwlock, NULL)) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_ERR_SYS, "init rwlock fail errno=%u rwlock=%p", errno, &rwlock);
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

class LatchReadLocker
{
public:
  explicit LatchReadLocker(common::ObLatch &rwlock) : succ_(false), rwlock_(NULL)
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
  LatchReadLocker() {}
private:
  bool succ_;
  common::ObLatch *rwlock_;
};

class LatchWriteLocker
{
public:
  explicit LatchWriteLocker(common::ObLatch &rwlock) : succ_(false), rwlock_(NULL)
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
  LatchWriteLocker() {}
private:
  bool succ_;
  common::ObLatch *rwlock_;
};


///


class SpinReadLocker
{
public:
  explicit SpinReadLocker(SpinRWLock &rwlock) : succ_(false), rwlock_(NULL)
  {
    if (0 != rwlock.rdlock()) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_ERR_SYS, "rdlock rwlock fail errno=%u", errno);
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
  SpinReadLocker() {}
private:
  bool succ_;
  SpinRWLock *rwlock_;
};

class SpinWriteLocker
{
public:
  explicit SpinWriteLocker(SpinRWLock &rwlock) : succ_(false), rwlock_(NULL)
  {
    if (0 != rwlock.wrlock()) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_ERR_SYS, "wrlock wrlock fail errno=%u", errno);
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
  SpinWriteLocker() {}
private:
  bool succ_;
  SpinRWLock *rwlock_;
};

class SpinIniter
{
public:
  explicit SpinIniter(pthread_spinlock_t &spin) : succ_(false)
  {
    if (0 != pthread_spin_init(&spin, PTHREAD_PROCESS_PRIVATE)) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_ERR_SYS, "init mutex fail errno=%u spin=%p", errno, &spin);
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

class MutexIniter
{
public:
  explicit MutexIniter(pthread_mutex_t &mutex) : succ_(false)
  {
    if (0 != pthread_mutex_init(&mutex, NULL)) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_ERR_SYS, "init mutex fail errno=%u mutex=%p", errno, &mutex);
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

class NLock
{
public:
  NLock() {}
  ~NLock() {}
};

class NCond
{
public:
  NCond() {}
  ~NCond() {}
};

class NullIniter
{
public:
  explicit NullIniter(NLock &nlock)
  {
    NLock *usr = NULL;
    usr = &nlock;
    UNUSED(usr);
  }
private:
  NullIniter();
};

class NullLocker
{
public:
  explicit NullLocker(pthread_mutex_t &mutex)
  {
    pthread_mutex_t *usr = NULL;
    usr = &mutex;
    UNUSED(usr);
    //HASH_WRITE_LOG(HASH_DEBUG, "nulllocker lock succ mutex=%p", &mutex);
  }
  explicit NullLocker(NLock &nlock)
  {
    NLock *usr = NULL;
    usr = &nlock;
    UNUSED(usr);
    //HASH_WRITE_LOG(HASH_DEBUG, "nulllocker lock succ nlock=%p", &nlock);
  }
  ~NullLocker()
  {
    //HASH_WRITE_LOG(HASH_DEBUG, "nulllocker unlock succ");
  }
  bool lock_succ() const
  {
    return true;
  }
private:
  NullLocker() {}
};

template <class T>
class NWaiter
{
public:
  NWaiter() {}
  ~NWaiter() {}
  int operator()(NCond &cond, T &lock, struct timespec &ts)
  {
    UNUSED(cond);
    UNUSED(lock);
    UNUSED(ts);
    return 0;
  }
};

class NBroadCaster
{
public:
  NBroadCaster() {}
  ~NBroadCaster() {}
  int operator()(NCond &cond)
  {
    UNUSED(cond);
    return 0;
  }
};

struct LatchReadWriteDefendMode
{
  typedef LatchReadLocker readlocker;
  typedef LatchWriteLocker writelocker;
  typedef common::ObLatch lock_type;
  typedef NCond cond_type;
  typedef NullIniter lock_initer;
  typedef NWaiter<lock_type> cond_waiter;
  typedef NBroadCaster cond_broadcaster;
};

struct ReadWriteDefendMode
{
  typedef ReadLocker readlocker;
  typedef WriteLocker writelocker;
  typedef pthread_rwlock_t lock_type;
  typedef NCond cond_type;
  typedef RWLockIniter lock_initer;
  typedef NWaiter<lock_type> cond_waiter;
  typedef NBroadCaster cond_broadcaster;
};
struct SpinReadWriteDefendMode
{
  typedef SpinReadLocker readlocker;
  typedef SpinWriteLocker writelocker;
  typedef SpinRWLock lock_type;
  typedef NCond cond_type;
  typedef NullIniter lock_initer;
  typedef NWaiter<lock_type> cond_waiter;
  typedef NBroadCaster cond_broadcaster;
};
struct SpinMutexDefendMode
{
  typedef NullLocker readlocker;
  typedef SpinLocker writelocker;
  typedef pthread_spinlock_t lock_type;
  typedef NCond cond_type;
  typedef SpinIniter lock_initer;
  typedef NWaiter<lock_type> cond_waiter;
  typedef NBroadCaster cond_broadcaster;
};
struct MultiWriteDefendMode
{
  typedef MutexLocker readlocker;
  typedef MutexLocker writelocker;
  typedef pthread_mutex_t lock_type;
  typedef pthread_cond_t cond_type;
  typedef MutexIniter lock_initer;
  typedef MutexWaiter cond_waiter;
  typedef MutexBroadCaster cond_broadcaster;
};
struct NoPthreadDefendMode
{
  typedef NullLocker readlocker;
  typedef NullLocker writelocker;
  typedef NLock lock_type;
  typedef NCond cond_type;
  typedef NullIniter lock_initer;
  typedef NWaiter<lock_type> cond_waiter;
  typedef NBroadCaster cond_broadcaster;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

inline int64_t tv_to_microseconds(const timeval &tp)
{
  return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
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
    const int64_t *first = PRIME_LIST;
    const int64_t *last = PRIME_LIST + PRIME_NUM;
    const int64_t *pos = std::lower_bound(first, last, n);
    prime = ((pos == last) ? * (last - 1) : *pos);
  }
  return prime;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

template <class T>
struct pre_proc
{
  T &operator()(T &t) const
  {
    return t;
  }
};

////////////////////////////////////////////////////////////////////////////////////////////////////

template <class _key>
struct equal_to
{
  bool operator()(const _key &a, const _key &b) const
  {
    return (a == b);
  }
};

template <class _key>
struct equal_to <_key *>
{
  bool operator()(_key *a, _key *b) const
  {
    return (*a == *b);
  }
};

template <class _key>
struct equal_to <const _key *>
{
  bool operator()(const _key *a, const _key *b) const
  {
    return (*a == *b);
  }
};

template <>
struct equal_to <_ParseNode *>
{
  bool operator()(_ParseNode *a, _ParseNode *b) const
  {
    return parsenode_equal(a, b, NULL);
  }
};

template <>
struct equal_to <const _ParseNode *>
{
  bool operator()(const _ParseNode *a, const _ParseNode *b) const
  {
    return parsenode_equal(a, b, NULL);
  }
};
////////////////////////////////////////////////////////////////////////////////////////////////////

template <class _key>
struct hash_func
{
  int operator()(const _key &key, uint64_t &res) const
  {
    return key.hash(res);
  }
};
template <class _key>
struct hash_func <_key *>
{
  int operator()(_key *key, uint64_t &res) const
  {
    return key->hash(res);
  }
};
template <class _key>
struct hash_func <const _key *>
{
  int operator()(const _key *key, uint64_t &res) const
  {
    return key->hash(res);
  }
};
template <>
struct hash_func <ObString>
{
  int operator()(const ObString &key, uint64_t &res) const
  {
    res = murmurhash(key.ptr(), key.length(), 0);
    return OB_SUCCESS;
  }
};
template <>
struct hash_func <const char *>
{
  int operator()(const char *key, uint64_t &res) const
  {
    res = murmurhash(key, static_cast<int32_t>(strlen(key)), 0);
    return OB_SUCCESS;
  }
};
template <>
struct hash_func <char *>
{
  int operator()(const char *key, uint64_t &res) const
  {
    res = murmurhash(key, static_cast<int32_t>(strlen(key)), 0);
    return OB_SUCCESS;
  }
};
template <>
struct hash_func <std::pair<int, uint32_t> >
{
  int operator()(std::pair<int, uint32_t> key, uint64_t &res) const
  {
    res = key.first + key.second;
    return OB_SUCCESS;
  }
};
template<>
struct hash_func <std::pair<uint64_t, uint64_t> >
{
  int operator()(std::pair<uint64_t, uint64_t> key, uint64_t &res) const
  {
    res = (uint64_t)(int64_t)(key.first + key.second);
    return OB_SUCCESS;
  }
};

template <>
struct hash_func <_ParseNode *>
{
  int operator() (const _ParseNode *key, uint64_t &res) const
  {
    res = parsenode_hash(key, NULL);
    return OB_SUCCESS;
  }
};
template <>
struct hash_func <const _ParseNode *>
{
  int operator() (const _ParseNode *key, uint64_t &res) const
  {
    res = parsenode_hash(key, NULL);
    return OB_SUCCESS;
  }
};

#define _HASH_FUNC_SPEC(type) \
  template <> \
  struct hash_func <type> \
  { \
    int operator() (const type &key, uint64_t &res) const \
    { \
      res = (uint64_t)(int64_t)key; \
      return OB_SUCCESS; \
    } \
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

struct HashNullObj
{
};

template <class _key_type, class _value_type>
struct HashMapPair
{
  typedef _key_type first_type;
  typedef _value_type second_type;
  first_type first;
  second_type second;
  HashMapPair() : first(), second() {}
  //HashMapPair(const first_type &a, const second_type &b) : first(a), second(b) {}

  int init(const first_type &a, const second_type &b)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(first, a)))) {
      _OB_LOG(ERROR, "copy first failed, ret=[%d]", ret);
    } else if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(second, b)))) {
      _OB_LOG(ERROR, "copy second failed, ret=[%d]", ret);
    }
    return ret;
  }

  int assign(const HashMapPair &other)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(first, other.first)))) {
      _OB_LOG(ERROR, "copy first failed, ret=[%d]", ret);
    } else if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(second, other.second)))) {
      _OB_LOG(ERROR, "copy second failed, ret=[%d]", ret);
    }
    return ret;
  }

  int overwrite(const HashMapPair &pair)
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

struct NormalPairTag
{
};
struct HashMapPairTag
{
};

template <class T, class K = T>
struct PairTraits
{
  typedef NormalPairTag PairTag;
};
template <class T, class K>
struct PairTraits <HashMapPair<T, K> >
{
  typedef HashMapPairTag PairTag;
};

template <class Pair>
int copy(Pair &dest, const Pair &src, NormalPairTag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(dest, src)))) {
    _OB_LOG(ERROR, "copy failed, ret=[%d]", ret);
  }
  return ret;
}
template <class Pair>
int copy(Pair &dest, const Pair &src)
{
  return do_copy(dest, src, typename PairTraits<Pair>::PairTag());
}
template <class Pair>
int do_copy(Pair &dest, const Pair &src, NormalPairTag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != (ret = copy_assign(dest, src)))) {
    _OB_LOG(ERROR, "copy failed, ret=[%d]", ret);
  }
  return ret;
}
template <class Pair>
int do_copy(Pair &dest, const Pair &src, HashMapPairTag)
{
  return dest.overwrite(src);
}

template <class _pair_type>
struct pair_first
{
  typedef typename _pair_type::first_type type;
  const type &operator()(const _pair_type &pair) const
  {
    return pair.first;
  }
};

template <class _pair_type>
struct pair_second
{
  typedef typename _pair_type::second_type type;
  const type &operator()(const _pair_type &pair) const
  {
    return pair.second;
  }
};

////////////////////////////////////////////////////////////////////////////////////////////////////

struct BigArrayTag
{
};
struct NormalPointerTag
{
};

struct DefaultBigArrayAllocator
{
  void *alloc(const int64_t sz)
  {
    void *p = NULL;
    if (sz > 0) {
      p = ob_malloc(sz, ObModIds::OB_HASH_BUCKET);
    }
    return p;
  }
  void free(void *p)
  {
    if (NULL != p) {
      ob_free(p);
      p = NULL;
    }
  }
};
template <class T, class Allocer = DefaultBigArrayAllocator>
class BigArrayTemp
{
  class Block
  {
  public:
    Block() : array_(NULL), allocer_(NULL) {}
    ~Block() { destroy(); }
  public:
    int create(const int64_t array_size, Allocer *allocer)
    {
      int ret = 0;
      if (NULL != array_) {
        ret = -1;
      } else if (0 == array_size || NULL == allocer) {
        ret = -1;
      } else if (NULL == (array_ = (T *)allocer->alloc(array_size * sizeof(T)))) {
        HASH_WRITE_LOG(HASH_WARNING, "alloc fail size=%ld array_size=%ld", array_size * sizeof(T),
                       array_size);
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
    T &operator[](const int64_t pos)
    {
      T &ret = array_[pos];
      return ret;
    }
    const T &operator[](const int64_t pos) const
    {
      T &ret = array_[pos];
      return ret;
    }
  private:
    T *array_;
    Allocer *allocer_;
  };
public:
  typedef BigArrayTag ArrayTag;
  typedef BigArrayTemp<T, Allocer> array_type;
public:
  BigArrayTemp() : blocks_(NULL), array_size_(0), blocks_num_(0) {}
  ~BigArrayTemp() { destroy(); }
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
      if (NULL == (blocks_ = (Block *)allocer_.alloc(blocks_num_ * sizeof(Block)))) {
        HASH_WRITE_LOG(HASH_WARNING, "alloc blocks fail blocks_num=%ld array_size=%ld", blocks_num_,
                       array_size_);
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
  T &operator[](const int64_t pos)
  {
    int64_t block_pos = pos / array_size_;
    //int64_t array_pos = pos % array_size_;
    int64_t array_pos = mod_2exp(pos, array_size_);
    return blocks_[block_pos][array_pos];
  }
  const T &operator[](const int64_t pos) const
  {
    int64_t block_pos = pos / array_size_;
    //int64_t array_pos = pos % array_size_;
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
  Block *blocks_;
  Allocer allocer_;
  int64_t array_size_;
  int64_t blocks_num_;
};

template <class T>
class BigArray : public BigArrayTemp<T, DefaultBigArrayAllocator>
{
};

template <class T>
struct NormalPointer
{
  typedef T *array_type;
};

template <class Array>
struct ArrayTraits
{
  typedef typename Array::ArrayTag ArrayTag;
};
template <class Array>
struct ArrayTraits<Array *>
{
  typedef NormalPointerTag ArrayTag;
};
template <class Array>
struct ArrayTraits<const Array *>
{
  typedef NormalPointerTag ArrayTag;
};

template <class Array>
void construct(Array &array)
{
  do_construct(array, typename ArrayTraits<Array>::ArrayTag());
}

template <class Array>
void do_construct(Array &array, BigArrayTag)
{
  UNUSED(array);
}

template <class Array>
void do_construct(Array &array, NormalPointerTag)
{
  array = NULL;
}

template <class Array, class BucketAllocator>
int create(Array &array, const int64_t total_size, const int64_t array_size, const int64_t item_size,
           BucketAllocator &alloc)
{
  return do_create(array, total_size, array_size, item_size, typename ArrayTraits<Array>::ArrayTag(),
                   alloc);
}

template <class Array, class BucketAllocator>
int do_create(Array &array, const int64_t total_size, const int64_t array_size, const int64_t item_size, BigArrayTag,
              BucketAllocator &alloc)
{
  UNUSED(item_size);
  UNUSED(alloc);
  return array.create(total_size, array_size);
}

template <class Array, class BucketAllocator>
int do_create(Array &array, const int64_t total_size, const int64_t array_size, const int64_t item_size,
              NormalPointerTag, BucketAllocator &alloc)
{
  UNUSED(array_size);
  int ret = 0;
  if (OB_UNLIKELY(total_size <= 0 || item_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (NULL == (array = (Array)alloc.alloc(total_size * item_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(WARN, "alloc memory failed,size:%ld", total_size * item_size);
  } else {
    //BACKTRACE(WARN, total_size * item_size > 65536, "hashutil create init size=%ld", total_size * item_size);
    memset(array, 0, total_size * item_size);
  }
  return ret;
}

template <class Array>
bool inited(const Array &array)
{
  return do_inited(array, typename ArrayTraits<Array>::ArrayTag());
}

template <class Array>
bool do_inited(const Array &array, BigArrayTag)
{
  return array.inited();
}

template <class Array>
bool do_inited(const Array &array, NormalPointerTag)
{
  return (NULL != array);
}

template <class Array, class BucketAllocator>
void destroy(Array &array, BucketAllocator &alloc)
{
  do_destroy(array, typename ArrayTraits<Array>::ArrayTag(), alloc);
}

template <class Array, class BucketAllocator>
void do_destroy(Array &array, BigArrayTag, BucketAllocator &)
{
  array.destroy();
}

template <class Array, class BucketAllocator>
void do_destroy(Array &array, NormalPointerTag, BucketAllocator &alloc)
{
  if (NULL != array) {
    alloc.free(array);
    array = NULL;
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

struct DefaultSimpleAllocerAllocator
{
public:
  explicit DefaultSimpleAllocerAllocator(uint64_t tenant_id = OB_SERVER_TENANT_ID,
                                         const lib::ObLabel &label = ObModIds::OB_HASH_NODE)
  {
    attr_.tenant_id_ = tenant_id;
    attr_.label_ = label;
  }
  void *alloc(const int64_t sz)
  {
    void *p = NULL;
    if (sz > 0) {
      p = ob_malloc(sz, attr_);
    }
    return p;
  }
  void free(void *p)
  {
    if (NULL != p) {
      ob_free(p);
      p = NULL;
    }
  }
  void set_attr(const ObMemAttr &attr) { attr_ = attr; }
  void set_label(const lib::ObLabel &label) { attr_.label_ = label; }
private:
  ObMemAttr attr_;
};

template <class T, int64_t NODE_NUM>
struct SimpleAllocerBlock;

template <class T, int64_t NODE_NUM>
struct SimpleAllocerNode
{
  typedef SimpleAllocerNode<T, NODE_NUM> Node;
  typedef SimpleAllocerBlock<T, NODE_NUM> Block;

  T data;
  uint32_t magic1;
  Node *next;
  Block *block;
  uint32_t magic2;
};

template <class T, int64_t NODE_NUM>
struct SimpleAllocerBlock
{
  typedef SimpleAllocerNode<T, NODE_NUM> Node;
  typedef SimpleAllocerBlock<T, NODE_NUM> Block;

  int32_t ref_cnt;
  int32_t cur_pos;
  Node nodes[NODE_NUM];
  Node *node_free_list;
  Block *prev;
  Block *next;
};

template <class T, const int NODE_PAGE_SIZE=common::OB_MALLOC_NORMAL_BLOCK_SIZE>
struct NodeNumTraits
{
  /*
     24 :  sizeof(SimpleAllocerBlock 's members except nodes)
     32 :  sizeof(SimpleAllocerNode's members except data)
     128:  for robust
  */
  static const int32_t NODE_NUM = ((NODE_PAGE_SIZE - 24 - 128) < (32 + sizeof(T))) ? 1 : ((NODE_PAGE_SIZE -
                                   24 - 128) /
                                  (32 + sizeof(T)));
};
/*
block_free_list_:
  Block C: node1->node2->node3...
    ^
    |
  Block B: node1->node2->node3...
    ^
    |
  Block A: node1->node2->node3...

alloc:
  1. fetch from block_free_list_
  2. fetch from block_remainder_
  3. alloc new block

free:
  check reference of block, zero-block will be destroy, block has 4 status:
  1. in the block_free_list_
  2. is the block_remainder_
  3. in the block_remainder_ && is the block_remainder_
  4. neither
*/
template <class T,
          int32_t NODE_NUM = NodeNumTraits<T>::NODE_NUM,
          class DefendMode = SpinMutexDefendMode,
          class Allocer = DefaultSimpleAllocerAllocator>
class SimpleAllocer
{
  typedef SimpleAllocerNode<T, NODE_NUM> Node;
  typedef SimpleAllocerBlock<T, NODE_NUM> Block;
  const static uint32_t NODE_MAGIC1 = 0x89abcdef;
  const static uint32_t NODE_MAGIC2 = 0x12345678;
  typedef typename DefendMode::writelocker mutexlocker;
  typedef typename DefendMode::lock_type lock_type;
  typedef typename DefendMode::lock_initer lock_initer;
public:
  SimpleAllocer() : leak_check_(true), block_remainder_(NULL), block_free_list_(NULL)
  {
    lock_initer initer(lock_);
  }
  ~SimpleAllocer()
  {
    if (leak_check_) {
      if (NULL != block_remainder_ ||
          NULL != block_free_list_) {
        HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "SimpleAllocer memory leak");
      }
    }
  }
  void set_leak_check(const bool check) { leak_check_ = check; }
  void set_attr(const ObMemAttr &attr) { allocer_.set_attr(attr); }
  void set_label(const lib::ObLabel &label) { allocer_.set_label(label); }
  template <class ... TYPES>
  T *alloc(TYPES&... args)
  {
    T *ret = NULL;
    mutexlocker locker(lock_);
    if (NULL != block_free_list_) {
      Block *block = block_free_list_;
      Node *node = block->node_free_list;
      if (NODE_MAGIC1 != node->magic1 || NODE_MAGIC2 != node->magic2 || NULL == node->block) {
        HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "magic broken magic1=%x magic2=%x", node->magic1, node->magic2);
      } else {
        block->node_free_list = node->next;
        if (block->node_free_list == NULL) {
          take_off_from_fl(block);
        }
        node->block->ref_cnt++;
        ret = &(node->data);
      }
    } else {
      Block *block = block_remainder_;
      if (NULL == block || block->cur_pos >= (int32_t)NODE_NUM) {
        if (NULL == (block = (Block *)allocer_.alloc(sizeof(Block)))) {
          HASH_WRITE_LOG_RET(HASH_WARNING, OB_ERR_UNEXPECTED, "new block fail");
        } else {
          block->ref_cnt = 0;
          block->cur_pos = 0;
          block->prev = block->next = NULL;
          block->node_free_list = NULL;
          block_remainder_ = block;
        }
      }
      if (NULL != block) {
        Node *node = &block->nodes[block->cur_pos];
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
      new(ret) T(args...);
    }
    return ret;
  }
  void free(T *data)
  {
    mutexlocker locker(lock_);
    if (NULL == data) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_INVALID_ARGUMENT, "invalid param null pointer");
    } else {
      Node *node = (Node *)data;
      if (NODE_MAGIC1 != node->magic1 || NODE_MAGIC2 != node->magic2) {
        HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "magic broken magic1=%x magic2=%x", node->magic1, node->magic2);
      } else {
        data->~T();
        Block *block = node->block;
        block->ref_cnt--;
        if (0 == block->ref_cnt) {
          if (block == block_remainder_) {
            block_remainder_ = NULL;
          }
          // non-NULL means this block is in the freelist
          if (block->next != NULL) {
            take_off_from_fl(block);
          }
          allocer_.free(block);
        } else {
          node->next = block->node_free_list;
          block->node_free_list = node;
          // NULL means this block isn't in the freelist
          if (block->next == NULL) {
            add_to_fl(block);
          }
        }
      }
    }
  }
  void add_to_fl(Block *block)
  {
    if (block_free_list_ == NULL) {
      block->prev = block->next = block;
      block_free_list_ = block;
    } else {
      block->prev = block_free_list_->prev;
      block->next = block_free_list_;
      block_free_list_->prev->next = block;
      block_free_list_->prev = block;
      block_free_list_ = block;
    }
  }
  void take_off_from_fl(Block *block)
  {
    if (block == block->next) {
      abort_unless(block == block_free_list_);
      block->prev = block->next = NULL;
      block_free_list_ = NULL;
    } else {
      block->prev->next = block->next;
      block->next->prev = block->prev;
      block_free_list_ = block->next;
      block->prev = block->next = NULL;
    }
  }
private:
  bool leak_check_;
  Block *block_remainder_;
  Block *block_free_list_;
  lock_type lock_;
  Allocer allocer_;
};
}
}
}

#endif //OCEANBASE_COMMON_HASH_HASHUTILS_H_
