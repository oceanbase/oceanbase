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


#ifndef OCEABASE_STORAGE_TABLET_COPY_DEPENDENCY_MGR
#define OCEABASE_STORAGE_TABLET_COPY_DEPENDENCY_MGR

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/queue/ob_link_queue.h"
#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_storage_ha_struct.h"

namespace oceanbase
{
namespace storage
{

/**
  * ObTabletCopyDependencyMgr is a manager that records the copy dependency of tablets when migration.
  * Copy dependency means that one tablet should be copied after another tablet has been copied.
  * Currently, split dest tablet should be copied after split src tablet has been copied, in order to perform macro block reuse.
  * Usage:
  * Init -> Add dependencies -> Refresh -> (Fetch ready tablets -> Copy -> Remove dependencies) * n times -> Done
  */
class ObTabletCopyDependencyMgr final
{
public:
  ObTabletCopyDependencyMgr();
  ~ObTabletCopyDependencyMgr();
  int init();
  void reset();
  void reuse();
  int destroy();
  // add a tablet that is not dependent on any other tablet
  int add_independent_tablet(
    const ObTabletID &tablet_id,
    const int64_t transfer_seq,
    const ObCopyTabletStatus::STATUS status,
    const int64_t data_size);
  // add tablets that is dependent on another tablet
  int add_dependent_tablet_pair(
    const ObTabletID &tablet_id,
    const ObTabletID &parent_id,
    const int64_t transfer_seq,
    const ObCopyTabletStatus::STATUS status,
    const int64_t data_size);
  // remove a tablet from mgr
  // only can remove a tablet that has no parent
  // will also remove all dependency for its children
  int remove_tablet_dependency(const ObTabletID &parent_id);
  // fetch a group of tablets that is ready to copy
  int fetch_ready_tablet_group(
    const int64_t tablet_count_threshold,
    const int64_t tablet_group_size_threshold,
    common::ObIArray<ObLogicTabletID> &tablet_group_ids);
  // refresh mgr, only call this when migration create all tablets finish
  int refresh_ready_tablets();
  int get_tablet_array(common::ObIArray<ObTabletID> &tablet_ids);
  int check_is_done(bool &is_done) const;
  int64_t get_tablet_count() const;
  void print_diagnosis_info() const;
  bool is_inited() const { return is_inited_; }
private:
  struct TabletDependencyInfo final
  {
  public:
    explicit TabletDependencyInfo(common::ObIAllocator &allocator);
    ~TabletDependencyInfo() {}
    void reset();
    bool is_valid() const;
    int assign(const TabletDependencyInfo &other);

    TO_STRING_KV(K_(is_fake), K_(logic_tablet_id), K_(status), K_(data_size), K_(in_degree), K_(child_tablet_infos));
  public:
    typedef common::ObList<TabletDependencyInfo *, common::ObIAllocator> ChildTabletInfos;
    bool is_fake_;
    ObLogicTabletID logic_tablet_id_;
    ObCopyTabletStatus::STATUS status_;
    int64_t data_size_;
    int64_t in_degree_;
    ChildTabletInfos child_tablet_infos_;
    DISALLOW_COPY_AND_ASSIGN(TabletDependencyInfo);
  };
  struct ReadyTabletInfo: public common::ObLink
  {
  public:
    ReadyTabletInfo();
    virtual ~ReadyTabletInfo() {}
    void reset();
    bool is_valid() const;

    TO_STRING_KV(K_(logic_tablet_id));
  public:
    ObLogicTabletID logic_tablet_id_;
    ObCopyTabletStatus::STATUS status_;
    int64_t data_size_;
  };
private:
  void inner_reset_();
  void reset_dependency_map_();
  void reset_ready_queue_();
  int get_dependency_info_(const ObTabletID &tablet_id, TabletDependencyInfo *&tablet_dep_info);
  int prepare_fake_dependency_info_(TabletDependencyInfo *&tablet_dep_info);
  int alloc_dependency_info_(TabletDependencyInfo *&tablet_dep_info);
  void free_dependency_info_(TabletDependencyInfo *&tablet_dep_info);
  int remove_parent_dependency_(const ObTabletID &tablet_id, const bool need_push_ready_queue);
  int add_dependency_(TabletDependencyInfo *parent_info, TabletDependencyInfo *child_info);
  int remove_dependency_(TabletDependencyInfo *child_info);
  int push_ready_tablet_(ReadyTabletInfo *ready_tablet);
  int pop_ready_tablet_(ReadyTabletInfo *&ready_tablet);
  int get_first_ready_tablet_(ReadyTabletInfo *&ready_tablet);
  int construct_ready_tablet_(
    const ObLogicTabletID &logic_tablet_id,
    const ObCopyTabletStatus::STATUS status,
    const int64_t data_size,
    ReadyTabletInfo *&ready_tablet);
  int alloc_ready_tablet_(ReadyTabletInfo *&ready_tablet);
  void free_ready_tablet_(ReadyTabletInfo *&ready_tablet);
private:
  typedef hash::ObHashMap<ObTabletID, TabletDependencyInfo *> TabletDependencyMap;
  bool is_inited_;
  common::SpinRWLock lock_;
  common::ObFIFOAllocator dependency_info_allocator_;
  common::ObSliceAlloc ready_info_allocator_;
  // true when first call refresh_ready_tablets
  bool is_ready_;
  // adjacency list of tablet dependency, record the child dag of the corresponding tablet and the indegree of itself
  TabletDependencyMap dependency_map_;
  // ready queue of tablet, record the tablet that can be copied
  common::ObSpLinkQueue ready_tablets_;
  int64_t ready_tablets_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletCopyDependencyMgr);
};

}
}
#endif