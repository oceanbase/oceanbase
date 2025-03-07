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

#ifdef DEF_STORAGE_USE
DEF_STORAGE_USE(STORAGE_USED_DATA, ALL_ZONE_STORAGE)
DEF_STORAGE_USE(STORAGE_USED_CLOG, ALL_ZONE_STORAGE)
DEF_STORAGE_USE(STORAGE_USED_BACKUP, ALL_BACKUP_STORAGE_INFO)
DEF_STORAGE_USE(STORAGE_USED_ARCHIVE, ALL_BACKUP_STORAGE_INFO)
DEF_STORAGE_USE(STORAGE_USED_RESTORE, ALL_RESTORE_INFO)
DEF_STORAGE_USE(STORAGE_USED_DDL, ALL_DDL_STORAGE_INFO)
DEF_STORAGE_USE(STORAGE_USED_EXTERNAL, ALL_EXTERNAL_STORAGE_INFO)
DEF_STORAGE_USE(STORAGE_USED_EXPORT, ALL_EXPORT_STORAGE_INFO)
DEF_STORAGE_USE(STORAGE_USED_SQL_AUDIT, ALL_SQL_AUDIT_STORAGE_INFO)
DEF_STORAGE_USE(STORAGE_USED_OTHER, ALL_OTHER_STORAGE_INFO)
#endif // DEF_STORAGE_USE

#ifndef SRC_LIBRARY_SRC_COMMON_STORAGE_OB_DEVICE_COMMON_
#define SRC_LIBRARY_SRC_COMMON_STORAGE_OB_DEVICE_COMMON_

#include <dirent.h>
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{

const static int DEFAULT_OPT_ARG_NUM = 4;

class ObBaseDirEntryOperator
{
public:
  enum ObDirOpFlag {
    DOF_REG = 0,
    DOF_DIR = 1,
    // Set marker for object storage request when ObDirOpFlag is DOF_MARKER.
    // For example, list_files("oss://bucketname/uri"), if DOF_MARKER is specified with marker 'marker',
    // objects whose names are alphabetically greater than the 'marker' are returned
    DOF_REG_WITH_MARKER = 2,
    DOF_MAX_FLAG
  };
  ObBaseDirEntryOperator() : op_flag_(DOF_REG), size_(0), scan_count_(INT64_MAX), marker_(nullptr) {}
  virtual ~ObBaseDirEntryOperator() = default;
  virtual int func(const dirent *entry) = 0;
  virtual bool need_get_file_size() const { return false; }
  void set_dir_flag() {op_flag_ = DOF_DIR;}
  bool is_dir_scan() {return (op_flag_ == DOF_DIR) ? true : false;}
  void set_size(const int64_t size) { size_ = size; }
  int64_t get_size() const { return size_; }
  int set_marker_flag(const char *marker, const int64_t scan_count)
  {
    // List objects under the specified directory that are lexicographically greater than 'marker',
    // and the number of objects returned does not exceed op.get_scan_count()
    // If 'marker' is "", it means
    // the listing starts from the lexicographically smallest object in the specified directory
    // If op.get_scan_count() is <= 0,
    // it indicates there is no upper limit on the number of objects listed
    int ret = OB_SUCCESS;
    if (OB_ISNULL(marker)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "fail to set marker", K(ret), KP(marker));
    } else {
      marker_ = marker;
      op_flag_ = DOF_REG_WITH_MARKER;
      scan_count_ = scan_count;
    }
    return ret;
  }
  bool is_marker_scan() { return (op_flag_ == DOF_REG_WITH_MARKER) ? true : false; }
  int64_t get_scan_count() { return scan_count_; }
  const char *get_marker() const { return marker_; }
  TO_STRING_KV(K_(op_flag), K_(size), K_(scan_count));
private:
  int op_flag_;
  int64_t size_; // Always set 0 for directory.
  int64_t scan_count_; // Default value is INT64_MAX.
  const char *marker_;        // Default value is nullptr
};

/*ObStorageType and OB_STORAGE_TYPES_STR should be mapped one by one*/
enum ObStorageType : uint8_t
{
  OB_STORAGE_OSS = 0,
  OB_STORAGE_FILE = 1,
  OB_STORAGE_COS = 2,
  OB_STORAGE_LOCAL = 3,
  OB_STORAGE_S3 = 4,
  OB_STORAGE_LOCAL_CACHE = 5,
  OB_STORAGE_HDFS = 6,
  OB_STORAGE_MAX_TYPE
};

enum ObStorageAccessType
{
  OB_STORAGE_ACCESS_READER = 0,
  OB_STORAGE_ACCESS_NOHEAD_READER = 1,
  OB_STORAGE_ACCESS_ADAPTIVE_READER = 2,
  OB_STORAGE_ACCESS_OVERWRITER = 3,
  // OB_STORAGE_ACCESS_APPENDER correspond to ObStorageAppender.
  // In cases where the destination is S3, a special format is utilized to emulate the append interface.
  // Upon completion of data writing,
  // it is recommended to invoke the seal_file interface to write a seal meta file.
  // This step is designed to enhance subsequent access performance to the object.
  // Skipping this step does not compromise data integrity but may impact performance.
  OB_STORAGE_ACCESS_APPENDER = 4,
  // When utilizing the multipart writer interface,
  // it is crucial to invoke the complete interface upon successful data upload to ensure object visibility.
  // The object remains invisible until the complete interface responds successfully.
  // If an error occur before invoking complete or during its execution,
  // it is imperative to call the abort interface to delete the already uploaded data.
  OB_STORAGE_ACCESS_MULTIPART_WRITER = 5,
  OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER = 6,
  OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER = 7,
  OB_STORAGE_ACCESS_MAX_TYPE
};

enum ObStorageInfoType : uint8_t {
  ALL_ZONE_STORAGE,
  ALL_BACKUP_STORAGE_INFO,
  ALL_RESTORE_INFO,
  ALL_DDL_STORAGE_INFO,
  ALL_EXTERNAL_STORAGE_INFO,
  ALL_EXPORT_STORAGE_INFO,
  ALL_SQL_AUDIT_STORAGE_INFO,
  ALL_OTHER_STORAGE_INFO,
};

#define DEF_STORAGE_USE(name, table) name,
enum class ObStorageUsedMod : uint8_t {
#include "ob_device_common.h"
  STORAGE_USED_MAX
};
#undef DEF_STORAGE_USE

#define DEF_STORAGE_USE(name, table) table,
static const ObStorageInfoType __storage_table_mapper[] = {
#include "ob_device_common.h"
};
#undef DEF_STORAGE_USE

struct ObStorageIdMod
{
  ObStorageIdMod()
    : storage_id_(OB_INVALID_ID), storage_used_mod_(ObStorageUsedMod::STORAGE_USED_MAX)
  {}

  ObStorageIdMod(const uint64_t storage_id, const ObStorageUsedMod storage_used_mod)
    : storage_id_(storage_id), storage_used_mod_(storage_used_mod)
  {}

  virtual ~ObStorageIdMod() {}

  bool is_valid() const { return storage_used_mod_ != ObStorageUsedMod::STORAGE_USED_MAX
                                 && storage_id_ != OB_INVALID_ID; }

  void reset()
  {
    storage_id_ = OB_INVALID_ID;
    storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_MAX;
  }

  ObStorageInfoType get_category() const { return __storage_table_mapper[static_cast<uint8_t>(storage_used_mod_)]; }

  static const ObStorageIdMod get_default_id_mod()
  {
    static const ObStorageIdMod storage_id_mod(0, ObStorageUsedMod::STORAGE_USED_OTHER);
    return storage_id_mod;
  }

  static const ObStorageIdMod get_default_backup_id_mod()
  {
    static const ObStorageIdMod storage_id_mod(OB_INVALID_ID, ObStorageUsedMod::STORAGE_USED_BACKUP);
    return storage_id_mod;
  }

  static const ObStorageIdMod get_default_archive_id_mod()
  {
    static const ObStorageIdMod storage_id_mod(OB_INVALID_ID, ObStorageUsedMod::STORAGE_USED_ARCHIVE);
    return storage_id_mod;
  }

  static const ObStorageIdMod get_default_restore_id_mod()
  {
    static const ObStorageIdMod storage_id_mod(OB_INVALID_ID, ObStorageUsedMod::STORAGE_USED_RESTORE);
    return storage_id_mod;
  }

  TO_STRING_KV(K_(storage_id), K_(storage_used_mod));

  uint64_t storage_id_;
  ObStorageUsedMod storage_used_mod_;
};

}
}
#endif
