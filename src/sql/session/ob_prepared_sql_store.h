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

#ifndef OCEANBASE_SQL_SESSION_OB_PREPARED_SQL_STORE_
#define OCEANBASE_SQL_SESSION_OB_PREPARED_SQL_STORE_

#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/allocator/ob_buddy_allocator.h"
#include "share/ob_define.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase {
namespace sql {
class ObPreparedSqlValue {
public:
  ObPreparedSqlValue() : ref_count_(0)
  {}

  int init()
  {
    int ret = common::OB_SUCCESS;
    if (0 != pthread_rwlock_init(&rwlock_, NULL)) {
      ret = common::OB_ERROR;
    }
    return ret;
  }

  int wlock()
  {
    return pthread_rwlock_wrlock(&rwlock_);
  }

  int try_rlock()
  {
    return pthread_rwlock_tryrdlock(&rwlock_);
  }

  int64_t ref_count_;
  pthread_rwlock_t rwlock_;
};

class ObPreparedSqlStoreAtomic {
public:
  ObPreparedSqlStoreAtomic() : rc_(common::OB_SUCCESS), value_(NULL)
  {}

  virtual ~ObPreparedSqlStoreAtomic()
  {}

  virtual void operator()(common::hash::HashMapPair<common::ObString, ObPreparedSqlValue*> entry)
  {
    UNUSED(entry);
  }

  int get_rc() const
  {
    return rc_;
  }

  common::ObString& get_sql()
  {
    return sql_;
  }

  ObPreparedSqlValue* get_prepared_sql_value()
  {
    return value_;
  }

protected:
  int rc_;
  ObPreparedSqlValue* value_;
  common::ObString sql_;
};

class ObPreparedSqlStoreAddRef : public ObPreparedSqlStoreAtomic {
public:
  ObPreparedSqlStoreAddRef()
  {}

  virtual ~ObPreparedSqlStoreAddRef()
  {}

  virtual void operator()(common::hash::HashMapPair<common::ObString, ObPreparedSqlValue*> entry)
  {
    rc_ = entry.second->try_rlock();
    if (0 == rc_) {
      entry.second->ref_count_++;
      sql_ = entry.first;
      value_ = entry.second;
      rc_ = common::OB_SUCCESS;
    } else if (EBUSY == rc_) {
      rc_ = common::OB_EAGAIN;
    } else {
      // LOG_ERROR("try rlock on ObPreparedSqlValue failed", K(rc_),
      //          K(entry.first));
      rc_ = common::OB_ERROR;
    }
  }
};

class ObPreparedSqlStoreDecRef : public ObPreparedSqlStoreAtomic {
public:
  ObPreparedSqlStoreDecRef()
  {}

  virtual ~ObPreparedSqlStoreDecRef()
  {}

  virtual void operator()(common::hash::HashMapPair<common::ObString, ObPreparedSqlValue*> entry)
  {
    entry.second->ref_count_--;
    if (0 == entry.second->ref_count_) {
      if (0 == entry.second->wlock()) {
        rc_ = common::OB_DEC_AND_LOCK;
        sql_ = entry.first;
      } else {
        rc_ = common::OB_ERROR;
      }
    } else {
      sql_ = entry.first;
      value_ = entry.second;
      rc_ = common::OB_SUCCESS;
    }
  }
};

class ObPreparedSqlStore {
public:
  ObPreparedSqlStore() : allocator_(common::get_global_tc_allocator())
  {}

  ~ObPreparedSqlStore()
  {}

  int init();

public:
  int store_sql(const common::ObString& sql, common::ObString& osql);
  int free_sql(const common::ObString& sql);
  // typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString,
  //                                                             ObPreparedSqlValue*>::AllocType,
  //                                  common::ObWrapperAllocator>
  // PreparedSqlMapAllocator;
  typedef common::hash::ObHashMap<common::ObString, ObPreparedSqlValue*> PreparedSqlMap;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPreparedSqlStore);

private:
  PreparedSqlMap psmap_;
  //  common::ObBuddyAllocator allocator_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_SESSION_OB_PREPARED_SQL_STORE_
