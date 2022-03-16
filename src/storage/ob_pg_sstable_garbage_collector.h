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

#ifndef OB_PG_SSTABLE_GARBAGE_COLLECTOR_H_
#define OB_PG_SSTABLE_GARBAGE_COLLECTOR_H_

#include "lib/task/ob_timer.h"
#include "lib/queue/ob_link_queue.h"
#include "ob_pg_mgr.h"
#include "ob_sstable.h"

namespace oceanbase {
namespace storage {

struct ObSSTableGCItem : common::ObLink
{
public:
  ObSSTableGCItem() : key_() {}
  virtual ~ObSSTableGCItem() {}

  bool is_valid()
  {
    return key_.is_valid();
  }

  TO_STRING_KV(K_(key));

public:
  ObITable::TableKey key_;
};

class ObPGSSTableGCTask : public common::ObTimerTask
{
public:
  ObPGSSTableGCTask();
  virtual ~ObPGSSTableGCTask();
  virtual void runTimerTask() override;
};

class ObPGSSTableGarbageCollector {
public:
  ObPGSSTableGarbageCollector();
  ~ObPGSSTableGarbageCollector();
  int init();
  static ObPGSSTableGarbageCollector& get_instance();
  void stop();
  void wait();
  void destroy();

  int gc_free_sstable();
  int push_sstable_into_gc_queue(ObITable::TableKey &key);

private:
  int schedule_gc_task();

  int gc_free_sstable_by_queue();
  int gc_free_sstable_by_pg_iter();

  int alloc_sstable_gc_item(ObSSTableGCItem *&item);
  int push_sstable_gc_item(const ObSSTableGCItem *item);
  void free_sstable_gc_item(const ObSSTableGCItem *item);

private:
  static const int64_t GC_INTERVAL_US = 20 * 1000L; // 20ms

  static const int64_t DO_ONE_ROUND_PG_ITER_RECYCLE_THRESHOLD = 50L;

  static const int64_t ONE_ROUND_RECYCLE_COUNT_THRESHOLD = 20000L;

  bool is_inited_;
  common::ObTimer timer_;
  ObPGSSTableGCTask gc_task_;

  common::ObLinkQueue free_sstables_queue_;
  int64_t do_gc_cnt_by_queue_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_SSTABLE_GARBAGE_COLLECTOR_H_
