/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HANDLER_H
#define SRC_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HANDLER_H
#include "lib/lock/ob_small_spin_lock.h"
#include "mds_table_handle.h"
#include "mds_table_mgr.h"

namespace oceanbase
{
namespace storage
{
class ObTabletPointer;
namespace mds
{

class ObMdsTableHandler
{
public:
  ObMdsTableHandler()
  : is_written_(false),
  lock_(common::ObLatchIds::MDS_TABLE_HANDLER_LOCK) {}
  ~ObMdsTableHandler();
  ObMdsTableHandler(const ObMdsTableHandler &) = delete;
  ObMdsTableHandler &operator=(const ObMdsTableHandler &);// value sematic for tablet ponter deep copy
  int get_mds_table_handle(mds::MdsTableHandle &handle,
                           const ObTabletID &tablet_id,
                           const share::ObLSID &ls_id,
                           const bool not_exist_create,
                           ObTabletPointer *pointer);
  int try_gc_mds_table();
  int try_release_nodes_below(const share::SCN &scn);
  void reset() { this->~ObMdsTableHandler(); }
  void set_tablet_status_written() { ATOMIC_CAS(&(is_written_), false, true); }
  void mark_removed_from_t3m(ObTabletPointer *pointer);
  bool is_tablet_status_written() const { return ATOMIC_LOAD(&(is_written_)); }
  TO_STRING_KV(K_(mds_table_handle));
private:
  MdsTableMgrHandle mds_table_mgr_handle_;// mgr handle destroy after table handle destroy
  MdsTableHandle mds_table_handle_;// primary handle, all other handles are copied from this one
  bool is_written_;
  mutable MdsLock lock_;
  // lock_ is defined as read prefered lock, this is necessary because if this lock is write prefered, can not avoid thread deadlock:
  // thread A is doing flush, access to MdsTableMgr's Bucket Lock first, then create DAG, the DAG create action will read tablet_status, and will lock mds_table_handler with RLockGuard.
  // thread B is doing mark_removed_from_t3m action, which will lock mds_table_handler with RLockGuard, then access to MdsTableMgr's Bucket to remove mds_table.
  // so far so good, cause both A and B lock mds_table_handler with RLockGuard, no wait relationship between A and B.
  // now, just consider if there is a another thread C, locking mds_table_handler with WLockGuard, and this lock operation is just happend after B before A, and lock_ is write prefered defined.
  // this is what may happened:
  // 1. thread A holding MdsTableMgr's Bucket Lock, trying to lock mds_table_handler with RLockGuard, hung there though, cause lock_ is write prefered, and thread C is trying to lock it with WLockGuard.
  // 2. thread B holding mds_table_handler's RLock, trying to lock MdsTableMgr's Bucket Lock, hung there obviously, cause thread A is holding it.
  // 3. thread C is trying lock mds_table_handler with WLockGuard, it will never got it, cause read count on lock_ will never decline to 0.

  // So, lock_ must be read predered, if you don't understand it, just keep it in your mind.
  // I know you just wonder why it's so complicated, the reason is complicated also, i can't explain it to you in a simple way.
  // Better not refact this in an "elegant way", i strongly suggest you just accept it.
};

}
}
}
#endif