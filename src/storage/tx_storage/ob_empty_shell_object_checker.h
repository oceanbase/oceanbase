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

#ifndef OCEABASE_STORAGE_OB_EMPTY_SHELL_OBJECT_CHECKER
#define OCEABASE_STORAGE_OB_EMPTY_SHELL_OBJECT_CHECKER

#include "lib/oblog/ob_log.h"
#include "lib/task/ob_timer.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace common
{
class ObTabletID;
}
namespace storage
{
class ObLS;
// check whether the ddl tablet can become empty shell.
class ObDDLEmptyShellChecker
{
public:
  ObDDLEmptyShellChecker() 
    : ls_(nullptr),
      last_check_normal_time_(),
      delayed_gc_tablet_infos_(),
      is_inited_(false)
    { }
  ~ObDDLEmptyShellChecker() { reset(); }
  void reset();
  int init(storage::ObLS *ls);
  int check_split_src_deleted_tablet(
      const ObTablet &tablet, 
      const ObTabletCreateDeleteMdsUserData &user_data, 
      bool &can_become_empty_shell, 
      bool &need_retry);
  int erase_tablet_record(
      const ObTabletID &tablet_id);
private:
  int check_disk_space_exceeds(
      const ObTabletID &tablet_id,
      bool &can_become_empty_shell);
  int check_tablets_cnt_exceeds(
      const ObTabletID &tablet_id,
      bool &can_become_empty_shell);
  int check_delay_deleted_time_exceeds(
      const ObTabletID &tablet_id,
      bool &can_become_empty_shell);
  int periodic_check_normal();
private:
  typedef typename common::hash::ObHashMap<common::ObTabletID,
                                          int64_t,
                                          common::hash::NoPthreadDefendMode,
                                          common::hash::hash_func<common::ObTabletID>,
                                          common::hash::equal_to<common::ObTabletID>,
                                          common::hash::SimpleAllocer<common::hash::HashMapTypes<common::ObTabletID, int64_t>::AllocType>,
                                          common::hash::NormalPointer,
                                          oceanbase::common::ObMalloc,
                                          2/*MAP_EXTEND_RATIO*/> DelayedGCTabletMap;
  typedef typename DelayedGCTabletMap::iterator DelayedGCTabletIterator;
  storage::ObLS *ls_;
  int64_t last_check_normal_time_; // to decide whether to check map leak.
  DelayedGCTabletMap delayed_gc_tablet_infos_;
  bool is_inited_;
};
} // storage
} // oceanbase

#endif
