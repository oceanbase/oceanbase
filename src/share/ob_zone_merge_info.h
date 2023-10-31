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

#ifndef OCEANBASE_SHARE_OB_ZONE_MERGE_INFO_
#define OCEANBASE_SHARE_OB_ZONE_MERGE_INFO_

#include "lib/list/ob_dlink_node.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/list/ob_dlist.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_region.h"
#include "common/ob_zone.h"
#include "common/ob_region.h"
#include "common/ob_zone_status.h"
#include "common/ob_zone_type.h"
#include "share/ob_replica_info.h"
#include "share/scn.h"
#include "common/ob_tablet_id.h"
#include "share/tablet/ob_tablet_info.h"

namespace oceanbase
{
namespace share
{
struct ObMergeInfoItem : public common::ObDLinkBase<ObMergeInfoItem>
{
public:
  typedef common::ObDList<ObMergeInfoItem> ItemList;
  ObMergeInfoItem(ItemList &list, const char *name, const SCN &scn, const bool need_update);
  ObMergeInfoItem(ItemList &list, const char *name, const int64_t value, const bool need_update);
  ObMergeInfoItem(const ObMergeInfoItem &item);

  ObMergeInfoItem &operator = (const ObMergeInfoItem &item);
  // Differ from operator=, won't assign <need_update_>
  void assign_value(const ObMergeInfoItem &item);
  bool is_valid() const;
  void set_val(const int64_t value, const bool need_update);
  void set_scn(const SCN &scn, const bool need_update);
  int set_scn(const uint64_t scn_val);
  int set_scn(const uint64_t scn_val, const bool need_update);
  const SCN &get_scn() const { return scn_; }
  //this interface is just used for operations of reading or writing inner tables
  uint64_t get_scn_val() const { return scn_.get_val_for_inner_table_field(); }
  int64_t get_value() const { return value_; }

  TO_STRING_KV(K_(name), K_(is_scn), K_(scn), K_(value), K_(need_update));
public:
  const char *name_;
  bool is_scn_;
  SCN scn_;
  int64_t value_;
  bool need_update_; // used to mark the table field need to be updated or not
};

struct ObZoneMergeInfo
{
public:
  enum MergeStatus
  {
    MERGE_STATUS_IDLE,
    MERGE_STATUS_MERGING,
    MERGE_STATUS_VERIFYING,
    MERGE_STATUS_MAX
  };
  enum ObMergeErrorType
  {
    NONE_ERROR,
    CHECKSUM_ERROR,
    ERROR_TYPE_MAX
  };
  ObZoneMergeInfo();
  ObZoneMergeInfo &operator =(const ObZoneMergeInfo &other) = delete;
  int assign(const ObZoneMergeInfo &other);
  // differ from assign, only exclude 'need_update_' copy
  int assign_value(const ObZoneMergeInfo &other);
  void reset();
  bool is_valid() const;
  const SCN &broadcast_scn() const { return broadcast_scn_.get_scn(); }
  const SCN &last_merged_scn() const { return last_merged_scn_.get_scn(); }
  const SCN &all_merged_scn() const { return all_merged_scn_.get_scn(); }
  const SCN &frozen_scn() const { return frozen_scn_.get_scn(); }

  TO_STRING_KV(K_(tenant_id), K_(zone), K_(is_merging), K_(broadcast_scn), K_(last_merged_scn),
    K_(last_merged_time), K_(all_merged_scn), K_(merge_start_time), K_(merge_status), K_(frozen_scn),
    K_(start_merge_fail_times));

public:
  uint64_t tenant_id_;
  common::ObZone zone_;
  ObMergeInfoItem::ItemList list_;

  ObMergeInfoItem is_merging_;
  ObMergeInfoItem broadcast_scn_;
  ObMergeInfoItem last_merged_scn_;
  ObMergeInfoItem last_merged_time_;
  ObMergeInfoItem all_merged_scn_;
  ObMergeInfoItem merge_start_time_;
  ObMergeInfoItem merge_status_;
  ObMergeInfoItem frozen_scn_;
  int64_t start_merge_fail_times_;
};

struct ObGlobalMergeInfo
{
public:
  ObGlobalMergeInfo();
  void reset();
  bool is_last_merge_complete() const;
  bool is_in_merge() const;
  bool is_valid() const;
  bool is_merge_error() const;
  bool is_in_verifying_status() const;
  ObGlobalMergeInfo &operator = (const ObGlobalMergeInfo &other) = delete;
  int assign(const ObGlobalMergeInfo &other);
  // differ from assign, only exclude 'need_update_' copy
  int assign_value(const ObGlobalMergeInfo &other);

  const SCN &frozen_scn() const { return frozen_scn_.get_scn(); }
  const SCN &global_broadcast_scn() const { return global_broadcast_scn_.get_scn(); }
  const SCN &last_merged_scn() const { return last_merged_scn_.get_scn(); }

  TO_STRING_KV(K_(tenant_id), K_(cluster), K_(frozen_scn),
    K_(global_broadcast_scn), K_(last_merged_scn), K_(is_merge_error),
    K_(merge_status), K_(error_type), K_(suspend_merging), K_(merge_start_time),
    K_(last_merged_time));

public:
  uint64_t tenant_id_;
  ObMergeInfoItem::ItemList list_;

  ObMergeInfoItem cluster_;
  ObMergeInfoItem frozen_scn_;
  ObMergeInfoItem global_broadcast_scn_;
  ObMergeInfoItem last_merged_scn_;
  ObMergeInfoItem is_merge_error_;
  ObMergeInfoItem merge_status_;
  ObMergeInfoItem error_type_;
  ObMergeInfoItem suspend_merging_;
  ObMergeInfoItem merge_start_time_;
  ObMergeInfoItem last_merged_time_;
};

typedef common::ObSEArray<common::ObZone, DEFAULT_ZONE_COUNT> ObZoneArray;
typedef common::ObSEArray<share::ObZoneMergeInfo, DEFAULT_ZONE_COUNT> ObZoneMergeInfoArray;

} // end namespace share
} // end namespace oceanbase

#endif  //OCEANBASE_SHARE_OB_ZONE_MERGE_INFO_
