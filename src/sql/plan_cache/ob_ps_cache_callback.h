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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PS_CACHE_CALLBACK_H_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PS_CACHE_CALLBACK_H_

#include "lib/utility/ob_macro_utils.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"

namespace oceanbase
{

namespace sql
{
class ObPCVSet;

class ObGetClosedStmtIdOp
{
public:
  ObGetClosedStmtIdOp(common::ObIArray< std::pair<ObPsStmtId, int64_t> > *expired_ps,
                      common::ObIArray< std::pair<ObPsStmtId, int64_t> > *closed_ps)
    : closed_ps_(closed_ps),
      expired_ps_(expired_ps),
      used_size_(0),
      callback_ret_(OB_SUCCESS)
  {
  }

  // ref_count 为1表示所有session均不依赖当前cache中对象;
  int operator()(common::hash::HashMapPair<ObPsStmtId, ObPsStmtInfo*> &entry)
  {
    int &ret = callback_ret_;
    if (OB_ISNULL(closed_ps_) || OB_ISNULL(expired_ps_)) {
      callback_ret_ = common::OB_NOT_INIT;
      SQL_PC_LOG(WARN, "key_array not inited", K(callback_ret_));
    } else if (OB_ISNULL(entry.second)) {
      callback_ret_ = common::OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "ps session info is null", KP(entry.second), K_(callback_ret));
    } else if (1 == entry.second->get_ref_count()) {
      std::pair<ObPsStmtId, int64_t> id_time;
      id_time.first = entry.first;
      id_time.second = entry.second->get_last_closed_timestamp();
      if (entry.second->is_expired()) {
        // for expired ps info, only evicted once;
        // use cas, because auto cache evict and flush ps cache may concurrent processing
        if (ATOMIC_BCAS(entry.second->get_is_expired_evicted_ptr(), false, true)) {
          if (OB_SUCCESS != (callback_ret_ = expired_ps_->push_back(id_time))) {
            SQL_PC_LOG(WARN, "fail to push back key", K_(callback_ret));
          }
        }
      } else {
        if (OB_SUCCESS != (callback_ret_ = closed_ps_->push_back(id_time))) {
          SQL_PC_LOG(WARN, "fail to push back key", K_(callback_ret));
        } else {
          used_size_ += entry.second->get_item_and_info_size();
        }
      }
    }

    // The Story Behind Return Code:
    //   We change the interface for this because of supporting that iterations encounter an error
    //   to return immediately, yet for all the existing logics there, they don't care the return
    //   code and wants to continue iteration anyway. So to keep the old behavior and makes everyone
    //   else happy, we have to return OB_SUCCESS here. And we only make this return code thing
    //   affects the behavior in tenant meta manager washing tablet. If you want to change the
    //   behavior in such places, please consult the individual file owners to fully understand the
    //   needs there.
    return common::OB_SUCCESS;
  }

  int get_callback_ret() { return callback_ret_; }
  int64_t get_used_size() { return used_size_; }
private:
  common::ObIArray< std::pair<ObPsStmtId, int64_t> > *closed_ps_;
  common::ObIArray< std::pair<ObPsStmtId, int64_t> > *expired_ps_;
  int64_t used_size_;
  int callback_ret_;
  DISALLOW_COPY_AND_ASSIGN(ObGetClosedStmtIdOp);
};

class ObGetAllStmtIdOp
{
public:
  explicit ObGetAllStmtIdOp(common::ObIArray<ObPsStmtId> *key_array)
    : key_array_(key_array),
      callback_ret_(OB_SUCCESS)
  {
  }

  int operator()(common::hash::HashMapPair<ObPsStmtId, ObPsStmtInfo*> &entry)
  {
    int callback_ret_ = common::OB_SUCCESS;
    if (OB_ISNULL(key_array_)) {
      callback_ret_ = common::OB_NOT_INIT;
      SQL_PC_LOG_RET(WARN, callback_ret_, "key_array not inited", K(callback_ret_));
    } else if (OB_SUCCESS != (callback_ret_ = key_array_->push_back(entry.first))) {
      SQL_PC_LOG_RET(WARN, callback_ret_, "fail to push back key", K(callback_ret_));
    }

    // The Story Behind Return Code:
    //   We change the interface for this because of supporting that iterations encounter an error
    //   to return immediately, yet for all the existing logics there, they don't care the return
    //   code and wants to continue iteration anyway. So to keep the old behavior and makes everyone
    //   else happy, we have to return OB_SUCCESS here. And we only make this return code thing
    //   affects the behavior in tenant meta manager washing tablet. If you want to change the
    //   behavior in such places, please consult the individual file owners to fully understand the
    //   needs there.
    return common::OB_SUCCESS;
  }

  int get_callback_ret() { return callback_ret_; }
private:
  common::ObIArray<ObPsStmtId> *key_array_;
  int callback_ret_;
  DISALLOW_COPY_AND_ASSIGN(ObGetAllStmtIdOp);
};

class ObPsStmtItemRefAtomicOp
{
  typedef common::hash::HashMapPair<ObPsSqlKey, ObPsStmtItem *> PsStmtIdKV;
public:
  ObPsStmtItemRefAtomicOp(): stmt_item_(NULL), callback_ret_(common::OB_SUCCESS){}
  virtual ~ObPsStmtItemRefAtomicOp() {}

  virtual int get_value(ObPsStmtItem *&ps_item);
  void operator()(const PsStmtIdKV &entry);
  inline int get_callback_ret() const { return callback_ret_; }
private:
  ObPsStmtItem *stmt_item_;
  int callback_ret_;
  DISALLOW_COPY_AND_ASSIGN(ObPsStmtItemRefAtomicOp);
};

class ObPsStmtItemDerefAtomicOp
{
  typedef common::hash::HashMapPair<ObPsSqlKey, ObPsStmtItem *> PsStmtIdKV;
public:
  ObPsStmtItemDerefAtomicOp() : ret_(common::OB_SUCCESS), is_erase_(false) {}
  virtual ~ObPsStmtItemDerefAtomicOp() {}
  void operator()(const PsStmtIdKV &entry);
  int get_ret() const { return ret_; }
  bool is_erase() const { return is_erase_; }
private:
  int ret_;
  bool is_erase_;
  DISALLOW_COPY_AND_ASSIGN(ObPsStmtItemDerefAtomicOp);
};

class ObPsStmtItemEraseAtomicOp
{
  typedef common::hash::HashMapPair<ObPsSqlKey, ObPsStmtItem *> PsStmtIdKV;
public:
  ObPsStmtItemEraseAtomicOp(ObPsStmtId id) 
    : stmt_id_(id),
      ret_(common::OB_SUCCESS),
      need_erase_(false) {}
  virtual ~ObPsStmtItemEraseAtomicOp() {}
  void operator()(const PsStmtIdKV &entry);
  int get_ret() const { return ret_; }
  bool need_erase() const { return need_erase_; }
private:
  ObPsStmtId stmt_id_;
  int ret_;
  bool need_erase_;
  DISALLOW_COPY_AND_ASSIGN(ObPsStmtItemEraseAtomicOp);
};

class ObPsStmtInfoRefAtomicOp
{
  typedef common::hash::HashMapPair<ObPsStmtId, ObPsStmtInfo *> PsStmtInfoKV;
public:
  ObPsStmtInfoRefAtomicOp(): stmt_info_(NULL), callback_ret_(common::OB_SUCCESS) {}
  virtual ~ObPsStmtInfoRefAtomicOp() {}

  virtual int get_value(ObPsStmtInfo *&ps_info);
  inline int get_callback_ret() const { return callback_ret_; }
  void operator()(const PsStmtInfoKV &entry);
private:
  ObPsStmtInfo *stmt_info_;
  int callback_ret_;
  DISALLOW_COPY_AND_ASSIGN(ObPsStmtInfoRefAtomicOp);
};

class ObPsStmtInfoDerefAtomicOp
{
  typedef common::hash::HashMapPair<ObPsStmtId, ObPsStmtInfo *> PsStmtInfoKV;
public:
  ObPsStmtInfoDerefAtomicOp() : ret_(common::OB_SUCCESS) {}
  virtual ~ObPsStmtInfoDerefAtomicOp() {}
  void operator()(const PsStmtInfoKV &entry);
  int get_ret() const { return ret_; }
private:
  int ret_;
  DISALLOW_COPY_AND_ASSIGN(ObPsStmtInfoDerefAtomicOp);
};

class ObPsStmtInfoDestroyAtomicOp
{
  typedef common::hash::HashMapPair<ObPsStmtId, ObPsStmtInfo *> PsStmtInfoKV;
public:
  ObPsStmtInfoDestroyAtomicOp() : ret_(common::OB_SUCCESS), marked_erase_(false) {}
  virtual ~ObPsStmtInfoDestroyAtomicOp() {}
  void operator()(const PsStmtInfoKV &entry);
  int get_ret() const { return ret_; }
  bool marked_erase() const { return marked_erase_; }
private:
  int ret_;
  bool marked_erase_;
  DISALLOW_COPY_AND_ASSIGN(ObPsStmtInfoDestroyAtomicOp);
};


class ObPsPCVSetAtomicOp
{
protected:
  typedef common::hash::HashMapPair<ObPlanCacheKey, ObPCVSet *> PsPlanCacheKV;

public:
  ObPsPCVSetAtomicOp(const CacheRefHandleID ref_handle)
    : pcv_set_(NULL), ref_handle_(ref_handle) {}
  virtual ~ObPsPCVSetAtomicOp() {}
  // get pcv_set and lock
  virtual int get_value(ObPCVSet *&pcv_set);
  // get pcv_set and increase reference count
  void operator()(PsPlanCacheKV &entry);

protected:
  //when get value, need lock
  virtual int lock(ObPCVSet &pcv_set) = 0;
protected:
  // According to the interface of ObHashTable, all returned values will be passed
  // back to the caller via the callback functor.
  // pcv_set_ - the plan cache value that is referenced.
  ObPCVSet *pcv_set_;
  const CacheRefHandleID ref_handle_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPsPCVSetAtomicOp);
};

class ObPsPCVSetWlockAndRef : public ObPsPCVSetAtomicOp
{
public:
  ObPsPCVSetWlockAndRef(const CacheRefHandleID ref_handle)
    : ObPsPCVSetAtomicOp(ref_handle) {}
  virtual ~ObPsPCVSetWlockAndRef() {}
  int lock(ObPCVSet &pcv_set)
  {
    return pcv_set.lock(false/*wlock*/);
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObPsPCVSetWlockAndRef);
};

class ObPsPCVSetRlockAndRef : public ObPsPCVSetAtomicOp
{
public:
  ObPsPCVSetRlockAndRef(const CacheRefHandleID ref_handle)
    : ObPsPCVSetAtomicOp(ref_handle) {}
  virtual ~ObPsPCVSetRlockAndRef() {}
  int lock(ObPCVSet &pcvs)
  {
    return pcvs.lock(true/*rlock*/);
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObPsPCVSetRlockAndRef);
};

} //end of namespace sql
} //end of namespace oceanbase

#endif //OCEANBASE_SQL_PLAN_CACHE_OB_PS_CACHE_CALLBACK_H_
