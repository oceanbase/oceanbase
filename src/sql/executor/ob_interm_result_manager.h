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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_INTERM_RESULT_MANAGER_
#define OCEANBASE_SQL_EXECUTOR_OB_INTERM_RESULT_MANAGER_

#include "share/ob_scanner.h"
#include "sql/executor/ob_interm_result.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"

namespace oceanbase {
namespace sql {
class ObIntermResultPool;
class ObIntermResultManager;
class ObIntermResultRead {
public:
  ObIntermResultRead() : ret_(common::OB_SUCCESS), value_(NULL)
  {}

  virtual ~ObIntermResultRead()
  {}

  void operator()(common::hash::HashMapPair<ObIntermResultInfo, ObIntermResult*>& entry)
  {
    if (OB_ISNULL(entry.second)) {
      ret_ = common::OB_INVALID_ARGUMENT;
    } else {
      ret_ = entry.second->try_inc_cnt();
      value_ = entry.second;
    }
  }

  int get_ret() const
  {
    return ret_;
  }

  ObIntermResult* get_value()
  {
    return value_;
  }

private:
  int ret_;
  ObIntermResult* value_;
};

class ObIntermResultRecycle {
public:
  ObIntermResultRecycle() : ret_(common::OB_SUCCESS), value_(NULL)
  {}

  virtual ~ObIntermResultRecycle()
  {}

  void operator()(common::hash::HashMapPair<ObIntermResultInfo, ObIntermResult*>& entry)
  {
    if (OB_ISNULL(entry.second)) {
      ret_ = common::OB_INVALID_ARGUMENT;
    } else {
      ret_ = entry.second->try_begin_recycle();
      value_ = entry.second;
    }
  }

  int get_ret() const
  {
    return ret_;
  }

  ObIntermResult* get_value()
  {
    return value_;
  }

private:
  int ret_;
  ObIntermResult* value_;
};

class ObIntermResultGC : public common::ObTimerTask {
public:
  ObIntermResultGC();
  virtual ~ObIntermResultGC();

  void reset();
  void operator()(common::hash::HashMapPair<ObIntermResultInfo, ObIntermResult*>& entry);
  void runTimerTask();
  void set_ir_map(common::hash::ObHashMap<ObIntermResultInfo, ObIntermResult*>* ir_map);
  void set_ir_manager(ObIntermResultManager* ir_manager);

private:
  DISALLOW_COPY_AND_ASSIGN(ObIntermResultGC);

private:
  // ir map
  common::hash::ObHashMap<ObIntermResultInfo, ObIntermResult*>* ir_map_;
  // ir manager
  ObIntermResultManager* ir_manager_;
  // current time
  int64_t cur_time_;
  // list to store expire interm results;
  common::ObArenaAllocator allocator_;
  common::ObList<common::hash::HashMapPair<ObIntermResultInfo, ObIntermResult*>, common::ObArenaAllocator> expire_irs_;
  // how many interm results that have too long expire time
  int64_t invalid_time_ir_count_;
};

class ObIntermResultManager {
public:
  friend class ObIntermResultGC;

  static const int64_t DEFAULT_INTERM_RESULT_GC_DELAY_TIME = 1000000;
  static const int64_t INTERM_RMAP_BUCKET_SIZE = 1024;

  ObIntermResultManager();
  virtual ~ObIntermResultManager();

  static int build_instance();
  static ObIntermResultManager* get_instance();

  void reset();
  // return OB_ENTRY_NOT_EXIST for non-exist interm result.
  int update_expire_time(const ObIntermResultInfo& ir_info, const int64_t expire_time);
  int get_result(const ObIntermResultInfo& ir_info, ObIntermResultIterator& iter);
  int get_result_item(
      const ObIntermResultInfo& ir_info, const int64_t index, ObIntermResultItem& result_item, int64_t& total_cnt);

  int add_result(const ObIntermResultInfo& ir_info, ObIntermResult* interm_result, int64_t expire_time);
  int delete_result(const ObIntermResultInfo& ir_info);
  int delete_result(ObIntermResultIterator& iter);
  int alloc_result(ObIntermResult*& interm_result);
  int free_result(ObIntermResult* interm_result);
  const common::hash::ObHashMap<ObIntermResultInfo, ObIntermResult*>& get_ir_map() const;

private:
  int init();
  int free_result(const ObIntermResultInfo& ir_info);
  DISALLOW_COPY_AND_ASSIGN(ObIntermResultManager);

private:
  static ObIntermResultManager* instance_;
  bool inited_;
  common::hash::ObHashMap<ObIntermResultInfo, ObIntermResult*> ir_map_;
  // GC
  ObIntermResultGC ir_gc_;
  // gc time delay
  int64_t gc_delay_time_;
  // interm result pool
  ObIntermResultPool* ir_pool_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_INTERM_RESULT_MANAGER_ */
//// end of header file
