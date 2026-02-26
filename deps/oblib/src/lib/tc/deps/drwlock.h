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
struct QRWLock
{
public:
  enum { MAX_UNSAFE_REF = MAX_N_CHAN };
  class RLockGuard
  {
    public:
    RLockGuard(QRWLock &rwlock): rwlock_(rwlock) { rwlock_.rdlock(); }
    ~RLockGuard() { rwlock_.rdunlock(); }
  private:
    QRWLock &rwlock_;
  };
  class RLockGuardUnsafe
  {
  public:
    RLockGuardUnsafe(QRWLock &rwlock, int idx): rwlock_(rwlock), idx_(idx) { rwlock_.rdlock_unsafe(idx); }
    ~RLockGuardUnsafe() { rwlock_.rdunlock_unsafe(idx_); }
  private:
    QRWLock &rwlock_;
    int idx_;
  };

  class WLockGuard
  {
  public:
    WLockGuard(QRWLock &rwlock): rwlock_(rwlock) { rwlock_.wrlock(); }
    ~WLockGuard() { rwlock_.wrunlock(); }
  private:
    QRWLock &rwlock_;
  };
private:
  struct ReadRef
  {
    ReadRef(): value_(0) {}
    ~ReadRef() {}
    int64_t value_;
  } CACHE_ALIGNED;
  uint64_t write_uid_ CACHE_ALIGNED;
  ReadRef read_ref_unsafe_[MAX_UNSAFE_REF];
  ReadRef read_ref_[MAX_CPU_NUM];
  ReadRef& get_read_ref() { return read_ref_[tc_itid() % arrlen(read_ref_)]; }
public:
  QRWLock(): write_uid_(0)
  {}
  ~QRWLock()
  {}
  bool try_rdlock_unsafe(int idx)
  {
    bool lock_succ = false;
    int64_t *ref = &read_ref_[idx].value_;
    if (0 == ATOMIC_LOAD(&write_uid_)) {
      ATOMIC_STORE(ref, 1);
      if (0 == ATOMIC_LOAD(&write_uid_)) {
        lock_succ = true;
      } else {
        ATOMIC_STORE(ref, 0);
      }
    }
    return lock_succ;
  }
  void rdlock_unsafe(int idx)
  {
    while (!try_rdlock_unsafe(idx)) {
      PAUSE();
    }
  }
  void rdunlock_unsafe(int idx)
  {
    int64_t *ref = &read_ref_[idx].value_;
    ATOMIC_STORE(ref, 0);
  }
  bool try_rdlock()
  {
    bool lock_succ = false;
    int64_t *ref = &get_read_ref().value_;
    if (0 == ATOMIC_LOAD(&write_uid_)) {
      ATOMIC_FAA(ref, 1);
      if (0 == ATOMIC_LOAD(&write_uid_)) {
        lock_succ = true;
      } else {
        ATOMIC_FAA(ref, -1);
      }
    }
    return lock_succ;
  }
  void rdlock()
  {
    while (!try_rdlock()) {
      PAUSE();
    }
  }
  void rdunlock()
  {
    int64_t *ref = &get_read_ref().value_;
    ATOMIC_FAA(ref, -1);
  }
  void wrlock()
  {
    while (!ATOMIC_BCAS(&write_uid_, 0, 1))
      ;
    for (int64_t i = 0; i < (int64_t)arrlen(read_ref_); i++) {
      while (ATOMIC_LOAD(&read_ref_[i].value_) > 0) {
        PAUSE();
      }
    }
    for (int64_t i = 0; i < (int64_t)arrlen(read_ref_unsafe_); i++) {
      while (ATOMIC_LOAD(&read_ref_unsafe_[i].value_) > 0) {
        PAUSE();
      }
    }
    ATOMIC_STORE(&write_uid_, 2);
  }
  void wrunlock()
  {
    ATOMIC_STORE(&write_uid_, 0);
  }
};
