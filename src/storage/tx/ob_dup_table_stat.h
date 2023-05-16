//Copyright (c) 2023 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_TRANSACTION_DUP_TABLE_STAT_H
#define OCEANBASE_TRANSACTION_DUP_TABLE_STAT_H

#include "lib/hash/ob_hashmap.h"
#include "common/ob_simple_iterator.h"
#include "storage/tx/ob_trans_define.h"
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase
{

namespace transaction
{
struct DupTabletSetChangeStatus;
// base data structure for dup table
// addr_ hold server ip and port
class ObDupTableLSBaseStat
{
public:
  ObDupTableLSBaseStat() { reset(); }
  ~ObDupTableLSBaseStat() { destroy(); }
  void reset();
  void destroy() { reset(); }

  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  // void set_addr(const common::ObAddr &addr) { addr_ = addr; }
  void set_ls_id(share::ObLSID ls_id) { ls_id_ = ls_id; }

  uint64_t get_tenant_id() const { return tenant_id_; }
  // const common::ObAddr &get_addr() const { return addr_; }
  share::ObLSID get_ls_id() const { return ls_id_; }

  TO_STRING_KV(K_(tenant_id), K_(ls_id));

private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  // common::ObAddr addr_;
};

class ObDupTableLSLeaseMgrStat: public ObDupTableLSBaseStat
{
public:
  ObDupTableLSLeaseMgrStat() { reset(); }
  ~ObDupTableLSLeaseMgrStat() { destroy(); }

  void reset();
  void destroy() { reset(); }

  OB_INLINE void set_follower_addr(const common::ObAddr &follower_addr) { follower_addr_ = follower_addr; }
  OB_INLINE void set_grant_ts(const int64_t grant_ts) { grant_ts_ = grant_ts; }
  OB_INLINE void set_expired_ts(const int64_t expired_ts) { expired_ts_ = expired_ts; }
  OB_INLINE void set_cached_req_ts(const int64_t cached_req_ts) { cached_req_ts_ = cached_req_ts; }
  OB_INLINE void set_grant_req_ts(const int64_t grant_req_ts) { grant_req_ts_ = grant_req_ts; }
  OB_INLINE void set_remain_us (const int64_t remain_us) { remain_us_ = remain_us; }
  OB_INLINE void set_lease_interval(const int64_t lease_interval) { lease_interval_ = lease_interval; }
  OB_INLINE void set_max_replayed_scn(const share::SCN &max_replayed_scn) { max_replayed_scn_ = max_replayed_scn; }
  OB_INLINE void set_max_read_version(const int64_t max_read_version) { max_read_version_ = max_read_version; }
  OB_INLINE void set_max_commit_version(const int64_t max_commit_version) { max_commit_version_ = max_commit_version; }

  OB_INLINE const common::ObAddr &get_follower_addr() const { return follower_addr_; }
  OB_INLINE int64_t get_grant_ts() const { return grant_ts_; }
  OB_INLINE int64_t get_expired_ts() const { return expired_ts_; }
  OB_INLINE int64_t get_remain_us() const { return remain_us_; }
  OB_INLINE int64_t get_cached_req_ts() const { return cached_req_ts_; }
  OB_INLINE int64_t get_grant_req_ts() const { return grant_req_ts_; }
  OB_INLINE int64_t get_lease_interval() const { return lease_interval_; }
  OB_INLINE int64_t get_max_read_version() const { return max_read_version_; }
  OB_INLINE int64_t get_max_commit_version() const { return max_commit_version_; }
  OB_INLINE const share::SCN &get_max_replayed_scn() const { return max_replayed_scn_; }

  INHERIT_TO_STRING_KV("ObDupTableLSLeaseMgrStat", ObDupTableLSBaseStat, K_(follower_addr),
               K_(grant_ts), K_(expired_ts), K_(remain_us), K_(lease_interval), K_(grant_req_ts),
               K_(cached_req_ts), K_(max_replayed_scn), K_(max_read_version), K_(max_commit_version));

private:
  common::ObAddr follower_addr_;
  int64_t grant_ts_;
  int64_t expired_ts_;
  int64_t remain_us_;
  int64_t lease_interval_;
  int64_t grant_req_ts_;
  int64_t cached_req_ts_;
  int64_t max_read_version_;
  int64_t max_commit_version_;
  share::SCN max_replayed_scn_;
};

struct DupTableModID{
  static constexpr const char OB_VIRTUAL_DUP_LS_LEASE_MGR[] {"OB_VIRTUAL_DUP_LS_LEASE_MGR"};
  static constexpr const char OB_VIRTUAL_DUP_LS_TABLETS[] {"OB_VIRTUAL_DUP_LS_TABLETS"};
  static constexpr const char OB_VIRTUAL_DUP_LS_TABLET_SET[] {"OB_VIRTUAL_DUP_LS_TABLET_SET"};
};

typedef common::ObSimpleIterator<ObDupTableLSLeaseMgrStat,
        DupTableModID::OB_VIRTUAL_DUP_LS_LEASE_MGR, 16> ObDupLSLeaseMgrStatIterator;

typedef ObSEArray<ObDupTableLSLeaseMgrStat, 10> FollowerLeaseMgrStatArr;

enum class TabletSetAttr {
  INVALID = 0,
  DATA_SYNCING,
  READABLE,
  DELETING,
  MAX,
};

enum class TabletSetState {
  INVALID = 0,
  TMP,
  LOGGING,
  CONFIRMING,
  CONFIRMED,
  MAX,
};

const ObString &get_dup_ls_state_str(const bool is_master);
const ObString &get_dup_tablet_set_attr_str(const TabletSetAttr attr);
// for tablet set virtual table
const ObString &get_dup_tablet_set_state_str(const TabletSetState state);

class ObDupTableLSTabletsStat: public ObDupTableLSBaseStat
{
public:
  ObDupTableLSTabletsStat() { reset(); }
  ~ObDupTableLSTabletsStat() { destroy(); }

  void reset();
  void destroy() { reset(); }

  OB_INLINE void set_is_master(const bool is_master) { is_master_ = is_master; }
  OB_INLINE void set_unique_id(const uint64_t unique_id) { unique_id_ = unique_id; }
  OB_INLINE void set_tablet_id(const common::ObTabletID tablet_id) { tablet_id_ = tablet_id; }
  OB_INLINE void set_attr(const TabletSetAttr attr) { attr_ = attr; }
  OB_INLINE void set_refresh_schema_ts(const int64_t refresh_schema_ts) { refresh_schema_ts_ = refresh_schema_ts; }

  OB_INLINE int64_t get_unique_id() const { return unique_id_; }
  OB_INLINE common::ObTabletID get_tablet_id() const { return tablet_id_; }
  OB_INLINE const ObString &get_ls_state_str() { return get_dup_ls_state_str(is_master_); }
  OB_INLINE const ObString &get_tablet_set_attr_str() { return get_dup_tablet_set_attr_str(attr_); }
  OB_INLINE int64_t get_refresh_schema_ts() const { return refresh_schema_ts_; }

  INHERIT_TO_STRING_KV("ObDupTableLSTabletsStat", ObDupTableLSBaseStat, K_(is_master),
                       K_(unique_id), K_(tablet_id), K_(attr), K_(refresh_schema_ts));

private:
  bool is_master_;
  int64_t unique_id_;
  common::ObTabletID tablet_id_;
  TabletSetAttr attr_;
  int64_t refresh_schema_ts_;
  // bool need_gc_;
};

typedef common::ObSimpleIterator<ObDupTableLSTabletsStat,
        DupTableModID::OB_VIRTUAL_DUP_LS_TABLETS, 16> ObDupLSTabletsStatIterator;

class ObDupTableLSTabletSetStat: public ObDupTableLSBaseStat
{
public:
  ObDupTableLSTabletSetStat() { reset(); }
  ~ObDupTableLSTabletSetStat() { destroy(); }

  void reset();
  void destroy() { reset(); }
  OB_INLINE void set_is_master(const bool is_master) { is_master_ = is_master; }
  OB_INLINE void set_unique_id(const int64_t unique_id) { unique_id_ = unique_id; }
  OB_INLINE void set_count(const int64_t count) { count_ = count; }
  OB_INLINE void set_attr(const TabletSetAttr attr) { attr_ = attr; }
  OB_INLINE void set_readable_scn(const share::SCN &readable_scn) { readable_scn_ = readable_scn; }
  OB_INLINE void set_change_scn(const share::SCN &change_scn) { change_scn_ = change_scn; }
  OB_INLINE void set_need_confirm_scn(const share::SCN &need_confirm_scn) { need_confirm_scn_ = need_confirm_scn; }
  OB_INLINE void set_state(const TabletSetState state) { state_ = state; }
  OB_INLINE void set_trx_ref(const int64_t trx_ref) { trx_ref_ = trx_ref; }
  OB_INLINE void set_basic_info(const uint64_t tenant_id, const share::ObLSID ls_id,
                                const bool is_master)
  {
    set_tenant_id(tenant_id);
    set_ls_id(ls_id);
    set_is_master(is_master);
  }
  void set_from_change_status(struct DupTabletSetChangeStatus *tmp_status);
  // bool get_is_master() const { return is_master_; }
  OB_INLINE int64_t get_unique_id() const { return unique_id_; }
  OB_INLINE int64_t get_count() const { return count_; }
  // tablet_set_attr get_attr() const { return attr_; }
  OB_INLINE const share::SCN &get_readable_scn() const { return readable_scn_; }
  OB_INLINE const share::SCN &get_change_scn() const { return change_scn_; }
  OB_INLINE const share::SCN &get_need_confirm_scn() { return need_confirm_scn_; }
  // tablet_set_state get_state() { return state_; }
  OB_INLINE const ObString &get_ls_state_str() const { return get_dup_ls_state_str(is_master_); }
  OB_INLINE const ObString &get_tablet_set_attr_str() const { return get_dup_tablet_set_attr_str(attr_); }
  OB_INLINE const ObString &get_tablet_set_state_str() const { return get_dup_tablet_set_state_str(state_); }
  OB_INLINE int64_t get_trx_ref() const { return trx_ref_; }

  INHERIT_TO_STRING_KV("ObDupTableLSTabletSet", ObDupTableLSBaseStat, K_(is_master),
                       K_(unique_id), K_(count), K_(attr), K_(readable_scn),
                       K_(change_scn), K_(need_confirm_scn), K_(state), K_(trx_ref));

private:
  bool is_master_;
  int64_t unique_id_;
  int64_t count_;
  TabletSetAttr attr_;
  share::SCN readable_scn_;
  share::SCN change_scn_;
  share::SCN need_confirm_scn_;
  TabletSetState state_;
  int64_t trx_ref_;
};

typedef common::ObSimpleIterator<ObDupTableLSTabletSetStat,
        DupTableModID::OB_VIRTUAL_DUP_LS_TABLET_SET, 16> ObDupLSTabletSetStatIterator;



} // namespace transaction
} // namespace oceanbase
#endif
