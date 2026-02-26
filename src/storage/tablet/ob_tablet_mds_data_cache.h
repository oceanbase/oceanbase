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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MDS_DATA_CACHE
#define OCEANBASE_STORAGE_OB_TABLET_MDS_DATA_CACHE

#include <stdint.h>
#include "storage/tablet/ob_tablet_status.h"

namespace oceanbase
{
namespace storage
{
class ObTabletCreateDeleteMdsUserData;
class ObTabletBindingMdsUserData;

// Only for cache the most frequent case, Nomal tablet status
class ObTabletStatusCache final
{
public:
  ObTabletStatusCache();
  ObTabletStatusCache(const ObTabletStatusCache&) = delete;
  ObTabletStatusCache &operator=(const ObTabletStatusCache&) = delete;
public:
  void set_value(
      const ObTabletStatus &tablet_status,
      const int64_t create_commit_version,
      const int64_t delete_commit_version);

  void set_value(const ObTabletCreateDeleteMdsUserData &user_data);

  ObTabletStatus get_tablet_status() const { return tablet_status_; }
  int64_t get_create_commit_version() const { return create_commit_version_; }
  int64_t get_delete_commit_version() const { return delete_commit_version_; }

  void reset();

  bool is_valid() const { return tablet_status_.is_valid(); }

public:
  TO_STRING_KV(K_(tablet_status),
               K_(create_commit_version),
               K_(delete_commit_version));
private:
  ObTabletStatus tablet_status_;
  int64_t create_commit_version_;
  int64_t delete_commit_version_;
};


class ObDDLInfoCache final
{
public:
  ObDDLInfoCache();
  ObDDLInfoCache(const ObDDLInfoCache&) = delete;
  ObDDLInfoCache &operator=(const ObDDLInfoCache&) = delete;
public:
  void set_value(
      const bool redefined,
      const int64_t schema_version,
      const int64_t snapshot_version);

  void set_value(const ObTabletBindingMdsUserData &user_data);

  bool is_redefined() const { return redefined_; }
  int64_t get_schema_version() const { return schema_version_; }
  int64_t get_snapshot_version() const { return snapshot_version_; }

  void reset();

  bool is_valid() const
  {
    return INT64_MAX != schema_version_
        && INT64_MAX != snapshot_version_;
  }

public:
  TO_STRING_KV(K_(redefined),
               K_(schema_version),
               K_(snapshot_version));
private:
  bool redefined_;
  int64_t schema_version_;
  int64_t snapshot_version_;
};

class ObTruncateInfoCache final
{
public:
  ObTruncateInfoCache()
    : newest_commit_version_(INT64_MAX),
      newest_schema_version_(INT64_MAX),
      cnt_(0),
      replay_seq_(0)
  {}
  ~ObTruncateInfoCache() { reset(); }
  bool is_valid() const
  {
    return INT64_MAX != newest_commit_version_ && INT64_MAX != newest_schema_version_ && cnt_ >= 0;
  }
  int64_t newest_commit_version() const { return newest_commit_version_; }
  int64_t newest_schema_version() const { return newest_schema_version_; }
  int64_t replay_seq() const { return replay_seq_; }
  int64_t count() const { return cnt_; }
  bool is_empty() const
  {
    return 0 == cnt_;
  }
  void replay_truncate_info()
  {
    newest_commit_version_ = INT64_MAX;
    newest_schema_version_ = INT64_MAX;
    cnt_ = 0;
    replay_seq_++;
  }
  void set_empty() { set_value(0, 0, 0); }
  void set_value(
    const int64_t newest_commit_version,
    const int64_t newest_schema_version,
    const uint32_t cnt)
  {
    newest_commit_version_ = newest_commit_version;
    newest_schema_version_ = newest_schema_version;
    cnt_ = cnt;
  }
  TO_STRING_KV(K_(newest_commit_version), K_(newest_schema_version), K_(cnt), K_(replay_seq));
private:
  void reset()
  {
    newest_commit_version_ = INT64_MAX;
    newest_schema_version_ = INT64_MAX;
    cnt_ = 0;
    replay_seq_ = 0;
  }
private:
  int64_t newest_commit_version_;
  int64_t newest_schema_version_;
  uint32_t cnt_;
  uint8_t replay_seq_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MDS_DATA_CACHE
