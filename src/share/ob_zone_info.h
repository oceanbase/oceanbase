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

#ifndef OCEANBASE_SHARE_OB_ZONE_INFO_H_
#define OCEANBASE_SHARE_OB_ZONE_INFO_H_
#include "lib/list/ob_dlink_node.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/list/ob_dlist.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_fixed_length_string.h"
#include "common/ob_region.h"
#include "common/ob_zone.h"
#include "common/ob_region.h"
#include "common/ob_zone_status.h"
#include "common/ob_zone_type.h"
#include "share/ob_replica_info.h"

namespace oceanbase
{
namespace share
{

class ObZoneItemTransUpdater;

struct ObZoneInfoItem : public common::ObDLinkBase<ObZoneInfoItem>
{
public:
  typedef common::ObFixedLengthString<common::MAX_ZONE_INFO_LENGTH> Info;
  typedef common::ObDList<ObZoneInfoItem> ItemList;

  ObZoneInfoItem(ItemList &list, const char *name, int64_t value, const char *info);
  ObZoneInfoItem(const ObZoneInfoItem &item);
  ObZoneInfoItem &operator = (const ObZoneInfoItem &item);

  bool is_valid() const { return NULL != name_ && value_ >= 0; }
  TO_STRING_KV(K_(name), K_(value), K_(info));
  operator int64_t() const { return value_; }

  // update %value_ and %info_ after execute sql success
  int update(common::ObISQLClient &sql_client, const common::ObZone &zone,
             const int64_t value, const Info &info);
  // update %value_ and %info_. (set %info_ to empty)
  int update(common::ObISQLClient &sql_client, const common::ObZone &zone,
             const int64_t value);

  // update %value_ and %info_ will be rollback if transaction rollback or commit failed.
  int update(ObZoneItemTransUpdater &updater, const common::ObZone &zone,
             const int64_t value, const Info &info);
  // update %value_ and %info_. (set %info_ to empty)
  int update(ObZoneItemTransUpdater &updater, const common::ObZone &zone,
             const int64_t value);

public:
  const char *name_;
  int64_t value_;
  Info info_;
};

// Update item in transaction, if transaction rollback or commit failed the item value
// value will be rollback too.
class ObZoneItemTransUpdater
{
public:
  ObZoneItemTransUpdater();
  ~ObZoneItemTransUpdater();

  int start(common::ObMySQLProxy &sql_proxy);
  int update(const common::ObZone &zone, ObZoneInfoItem &item,
             const int64_t value, const ObZoneInfoItem::Info &info);
  int end(const bool commit);
  common::ObMySQLTransaction &get_trans() { return trans_; }
private:
  const static int64_t PTR_OFFSET = sizeof(void *);

  bool started_;
  bool success_;

  common::ObArenaAllocator alloc_;
  ObZoneInfoItem::ItemList list_;
  common::ObMySQLTransaction trans_;
};

struct ObGlobalInfo
{
public:
  ObGlobalInfo();
  ObGlobalInfo(const ObGlobalInfo &other);
  ObGlobalInfo &operator = (const ObGlobalInfo &other);
  void reset();
  bool is_valid() const;
  DECLARE_TO_STRING;
public:
  const common::ObZone zone_; // always be default value
  ObZoneInfoItem::ItemList list_;

  ObZoneInfoItem cluster_;
  ObZoneInfoItem privilege_version_;
  ObZoneInfoItem config_version_;
  ObZoneInfoItem lease_info_version_;
  ObZoneInfoItem time_zone_info_version_;
  ObZoneInfoItem storage_format_version_;
};

struct ObZoneInfo
{
public:
  enum RecoveryStatus
  {
    RECOVERY_STATUS_NORMAL = 0,
    RECOVERY_STATUS_SUSPEND,
    RECOVERY_STATUS_MAX,
  };
  enum StorageType
  {
    STORAGE_TYPE_LOCAL = 0,
    STORAGE_TYPE_MAX,
  };
  ObZoneInfo();
  ObZoneInfo(const ObZoneInfo &other);
  ObZoneInfo &operator =(const ObZoneInfo &other);
  void reset();
  bool is_valid() const;
  DECLARE_TO_STRING;

  int get_region(common::ObRegion &region) const;
  const char *get_status_str() const;

  inline int64_t get_item_count() const { return list_.get_size(); }
  bool can_switch_to_leader_while_daily_merge() const;
  // recovery status
  static const char *get_recovery_status_str(const RecoveryStatus status);
  static RecoveryStatus get_recovery_status(const char* status_str);
  // storage type
  static const char *get_storage_type_str(const StorageType storage_type);
  static StorageType get_storage_type(const char* storage_type_str);
  bool is_encryption() const {
    return zone_type_.value_ == common::ObZoneType::ZONE_TYPE_ENCRYPTION;
  }
  bool is_active() const {
    return ObZoneStatus::ACTIVE == status_;
  }
public:
  common::ObZone zone_;
  ObZoneInfoItem::ItemList list_;

  ObZoneInfoItem status_;
  ObZoneInfoItem region_;
  ObZoneInfoItem idc_;
  ObZoneInfoItem zone_type_;
  ObZoneInfoItem recovery_status_;
  ObZoneInfoItem storage_type_;
};


} // end namespace share
} // end namespace oceanbase

#endif  //OCEANBASE_SHARE_OB_CLUSTER_INFO_H_
