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

#ifndef OCEANBASE_OBSERVER_OB_TENANT_DATA_VERSION_H_
#define OCEANBASE_OBSERVER_OB_TENANT_DATA_VERSION_H_

#include "lib/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_version_def.h"

namespace oceanbase
{
namespace common
{
/**
 * ObTenantDataVersionMgr maintains the data_version of all tenants by a
 * hashmap<tenant_id, data_version>.
 *
 * The map<tenant_id, data_version> is persisted in a disk file, and whenever
 * the data_version of a tenant need to be updated, the disk file will be
 * modified before the map updated in memory, so we can ensure the data_version
 * will not go back in this machine.
 *
 * The path of the disk file is $observer_home/etc/observer.data_version.bin.
 * This file is composed of a header part and a data part.
 * | ---------------------HEADER (Not Readable)---------------------- |
 * | ObRecordHeader(magic_number, length, checksum...)                |
 * | ------------------------DATA (Readable)------------------------- |
 * | tenant_id: version_str version_val removed remove_timestamp\n    |
 * | 1001: 4.3.0.1 17180065793 0 0\n                                  |
 * | 1002: 4.3.0.1 17180065793 0 0\n                                  |
 * |                              ...                                 |
 * | ------------------------------------------------------------------

 * The insert/update/remove operations will set a mgr-level write lock first,
 * however the get operation can access the map directly without a lock,
 * therefore the overhead of the get operation is relatively small.
 *
 */
class ObTenantDataVersionMgr
{
public:
  ObTenantDataVersionMgr()
      : is_inited_(false), allocator_(lib::ObLabel("TenantDVMgr")),
        mock_data_version_(0), enable_compatible_monotonic_(false) {}
  ~ObTenantDataVersionMgr() {}
  static ObTenantDataVersionMgr& get_instance();
  int init(bool enable_compatible_monotonic);
  int get(const uint64_t tenant_id, uint64_t &data_version) const;
  /**
   * update the tenant's data_version, if the tenant is not in the mgr yet,
   * insert the entry instead.
   */
  int set(const uint64_t tenant_id, const uint64_t data_version);
  int remove(const uint64_t tenant_id);
  int load_from_file();
  bool is_enable_compatible_monotonic() {
    return ATOMIC_LOAD(&enable_compatible_monotonic_);
  }
  void set_enable_compatible_monotonic(bool enable) {
    ATOMIC_STORE(&enable_compatible_monotonic_, enable);
  }
  // for unittest
  void set_mock_data_version(const uint64_t data_version) {
    ATOMIC_SET(&mock_data_version_, data_version);
  }
private:
  struct ObTenantDataVersion
  {
    ObTenantDataVersion(uint64_t version)
        : removed_(false), remove_timestamp_(0), version_(version) {}
    ObTenantDataVersion(bool removed, uint64_t remove_timestamp_, uint64_t version)
        : removed_(removed), remove_timestamp_(remove_timestamp_), version_(version) {}
    ~ObTenantDataVersion() {}
    // tenant_id: version_str version_val removed remove_timestamp
    // for removed field, 1 stands for tenant is removed, 0 stands for tenant is active
    // e.g. 1001: 4.3.0.1 17180065793 0 0
    static constexpr const char *DUMP_BUF_FORMAT = "%lu: %s %lu %d %lu";
    // the max length of uint64 decimal format is 20, so:
    // tenant_id(20) + version_str(OB_SERVER_VERSION_LENGTH) + version_val(20) +
    // removed(1) + remove_timestamp(20) + spaces_and_others(10)
    static constexpr int64_t MAX_DUMP_BUF_SIZE = 20 + OB_SERVER_VERSION_LENGTH + 20 + 1 + 20 + 10;
    bool is_removed() const
    {
      return ATOMIC_LOAD(&removed_);
    }
    uint64_t get_remove_timestamp() const
    {
      return ATOMIC_LOAD(&remove_timestamp_);
    }
    void set_removed(bool removed, uint64_t remove_ts)
    {
      ATOMIC_STORE(&removed_, removed);
      ATOMIC_STORE(&remove_timestamp_, remove_ts);
    }
    uint64_t get_version() const
    {
      return ATOMIC_LOAD(&version_);
    }
    void set_version(uint64_t version)
    {
      ATOMIC_STORE(&version_, version);
    }
    TO_STRING_KV(K_(removed), K_(remove_timestamp), K_(version));
  private:
    bool removed_;
    uint64_t remove_timestamp_;
    uint64_t version_;
  };
  int set_(const uint64_t tenant_id, const uint64_t data_version);
  // for set: set data_version before dump
  int set_and_dump_to_file_(const uint64_t tenant_id, const uint64_t data_version, const bool need_to_insert);
  // for remove: remove `tenant_id`'s data_version before dump
  int remove_and_dump_to_file_(const uint64_t tenant_id, const int64_t remove_ts);
  int64_t get_max_dump_buf_size_() const;
  int dump_data_version_(char *buf, int64_t buf_length, int64_t &pos, const uint64_t tenant_id,
                         const bool removed, const int64_t remove_ts,
                         const uint64_t data_version);
  int load_data_version_(char *buf, int64_t &pos);
  int write_to_file_(char *buf, int64_t buf_length, int64_t data_length);
private:
  // we use NoPthreadDefendMode here, so the hashmap will not lock buckets before get/set.
  // to ensure thread safety, we have several promises:
  // 1. we only insert new entry into the hashmap, never delete or overwrite existing entry
  // 2. before insert new entry, we acquire a global lock in ObTenantDataVersionMgr
  typedef hash::ObHashMap<uint64_t, ObTenantDataVersion *, hash::NoPthreadDefendMode> ObTenantDataVersionMap;
  static constexpr const char *TENANT_DATA_VERSION_FILE_PATH = "etc/observer.data_version.bin";
  static constexpr int64_t TENANT_DATA_VERSION_FILE_MAX_SIZE = 1 << 26; // 64MB
  // The number of tenant won't be too large, so 1024 buckets should be enough
  static constexpr int64_t TENANT_DATA_VERSION_BUCKET_NUM = 1024;
  static constexpr int16_t OB_CONFIG_MAGIC = static_cast<int16_t>(0XBEDE);
  static const int16_t OB_CONFIG_VERSION = 1;
  bool is_inited_;
  ObTenantDataVersionMap map_;
  common::SpinRWLock lock_;
  common::ObArenaAllocator allocator_;
  // for unittest
  uint64_t mock_data_version_;
  bool enable_compatible_monotonic_;
};

class ObDataVersionPrinter
{
public:
  ObDataVersionPrinter(const uint64_t data_version);
  TO_STRING_KV("version_str", version_str_, "version_val", version_val_);
private:
  uint64_t version_val_;
  char version_str_[OB_SERVER_VERSION_LENGTH];
};

} // namespace common
} // namespace oceanbase
#define ODV_MGR (::oceanbase::common::ObTenantDataVersionMgr::get_instance())
#define DVP(data_version) (::oceanbase::common::ObDataVersionPrinter(data_version))
#define KDV(data_version) "data_version", DVP(data_version)
#endif // OCEANBASE_OBSERVER_OB_TENANT_DATA_VERSION_H_