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

#ifndef OCEANBASE_SHARE_OBJECT_STORAGE_OB_OBJECT_STORAGE_STRUCT_H_
#define OCEANBASE_SHARE_OBJECT_STORAGE_OB_OBJECT_STORAGE_STRUCT_H_

#include "common/ob_region.h"
#include "common/ob_role.h"
#include "common/ob_timeout_ctx.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/container/ob_array.h"
#include "lib/restore/ob_storage.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/utility/utility.h"
#include "lib/worker.h"
#include "share/ob_define.h"
#include "share/ob_encryption_struct.h"
#include "share/ob_force_print_log.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{

namespace obrpc
{
class ObAdminStorageArg;
}

namespace share
{
const int64_t OB_MAX_STORAGE_STATE_INFO_LENGTH = 1024;
const int64_t OB_MAX_STORAGE_USED_FOR_LENGTH = 256;
const int64_t OB_MAX_STORAGE_STATE_LENGTH = 256;
const int64_t OB_MAX_STORAGE_ATTRIBUTE_LENGTH = 128;
const int64_t OB_MAX_STORAGE_SCOPE_LENGTH = 128;
const int64_t OB_STORAGE_DEFAULT_FIXED_STR_LEN = 2048; // default fixed string length
const int64_t OB_MAX_STORAGE_ENCRYPT_INFO_LENGTH = 256;
const int64_t OB_MAX_STORAGE_ENCRYPTKEY_LENGTH = OB_MAX_BACKUP_ACCESSKEY_LENGTH + 16;
const int64_t OB_MAX_STORAGE_SERIALIZEKEY_LENGTH = OB_MAX_STORAGE_ENCRYPTKEY_LENGTH * 2;
const int64_t OB_MAX_SHARED_STORAGE_LENGTH = OB_MAX_BACKUP_DEST_LENGTH + OB_MAX_STORAGE_ATTRIBUTE_LENGTH + OB_MAX_STORAGE_SCOPE_LENGTH;
const int64_t OB_INVALID_MAX_IOPS = -1;
const int64_t OB_INVALID_MAX_BANDWIDTH = -1;

const char *const STORAGE_ACCESS_ID = "access_id=";
const char *const OB_STR_WILDCARD = "%%";
const char *const STORAGE_ACCESS_KEY = "access_key=";
const char *const STORAGE_ACCESS_MODE = "access_mode=";
const char *const STORAGE_RAM_URL = "ram_url=";
const char *const STORAGE_ENCRYPT_KEY = "encrypt_key=";
const char *const STORAGE_ENCRYPT_INFO = "encrypt_info=";
const char *const STORAGE_MAX_IOPS = "max_iops=";
const char *const STORAGE_MAX_BANDWIDTH = "max_bandwidth=";
const char *const STORAGE_STS_TOKEN_KEY = "sts_token=";
const char *const STORAGE_IS_CONNECTIVE_KEY = "is_connective=";
const char *const STORAGE_USED_FOR_TYPE = "used_for_type=";
const char *const STORAGE_SCOPE_TYPE = "scope=";
const char *const OB_STR_STORAGE_ZONE = "zone";
const char *const OB_STR_STORAGE_DEST_PATH = "path";
const char *const OB_STR_STORAGE_DEST_ENDPOINT = "endpoint";
const char *const OB_STR_STORAGE_USEDFOR = "used_for";
const char *const OB_STR_STORAGE_STATE = "state";
const char *const OB_STR_STORAGE_EXTENSION = "extension";
const char *const OB_STR_STORAGE_MAX_IOPS = "max_iops";
const char *const OB_STR_STORAGE_MAX_BANDWIDTH = "max_bandwidth";
const char *const OB_STR_STORAGE_ID = "storage_id";
const char *const OB_STR_STORAGE_OP_ID = "op_id";
const char *const OB_STR_STORAGE_SUB_OP_ID = "sub_op_id";
const char *const OB_STR_STORAGE_OP_TYPE = "op_type";
const char *const OB_STR_STORAGE_OP_INFO = "op_info";
const char *const OB_STR_STORAGE_DEST_ENCRYPT_INFO = "encrypt_info";
const char *const OB_STR_STORAGE_DEST_AUTHORIZATION = "authorization";
const char *const OB_STR_STORAGE_DEST_EXTENSION = "extension";
const char *const OB_STR_STORAGE_SHARED_DATA = "Shared Data";
const char *const OB_STR_STORAGE_TABLE_DATA = "Table Data";
const char *const OB_STR_STORAGE_TMP_DATA = "Tmp Data";

typedef common::ObFixedLengthString<OB_MAX_BACKUP_DEST_LENGTH> ObStoragePathString;
typedef common::ObFixedLengthString<OB_MAX_BACKUP_AUTHORIZATION_LENGTH> ObStorageAccessinfoString;
typedef common::ObFixedLengthString<OB_MAX_STORAGE_ATTRIBUTE_LENGTH> ObStorageAttributeString;

typedef ObStoragePathString ObStorageSetPath;

struct ObStorageAccessMode final
{
  enum MODE : uint8_t
  {
    ACCESS_BY_ID = 0,
    ACCESS_BY_RAM_URL = 1,
    MAX
  };
  static const char *get_str(const MODE &mode);
  static MODE get_mode(const char *mode_str);
  static OB_INLINE bool is_valid(const MODE &mode) { return mode >= 0 && mode < MAX; }
};

struct ObZoneStorageState final
{
  enum STATE : uint8_t
  {
    ADDING = 0,
    ADDED = 1,
    DROPPING = 2,
    DROPPED = 3,
    CHANGING = 4,
    CHANGED = 5,
    MAX
  };
  static const char *get_str(const STATE &state);
  static STATE get_state(const char *state_str);
  static OB_INLINE bool is_valid(const STATE &state) { return state >= 0 && state < MAX; }
};

struct ObStorageUsedType final
{
  enum TYPE : uint8_t
  {
    USED_TYPE_DATA = 0,
    USED_TYPE_LOG,
    USED_TYPE_ALL,
    USED_TYPE_MAX
  };
  static const char *get_str(const TYPE &type);
  static TYPE get_type(const char *type_str);
  static OB_INLINE bool is_valid(const TYPE &type)
  {
    return type >= 0 && type < TYPE::USED_TYPE_MAX;
  }
};

struct ObScopeType final
{
  enum TYPE : uint8_t
  {
    ZONE = 0,
    REGION = 1,
    MAX
  };
  static const char *get_str(const TYPE &type);
  static TYPE get_type(const char *type_str);
  static OB_INLINE bool is_valid(const TYPE &type)
  {
    return type >= 0 && type < TYPE::MAX;
  }
};

class ObStorageDestCheck final
{
public:
  ObStorageDestCheck() = default;
  ~ObStorageDestCheck() = default;
  static int get_bucket_path(const ObString &uri, ObString &bucket_path, ObIAllocator &allocator);
  static int check_change_storage_accessinfo_exist(common::ObISQLClient &proxy,
                                                   const ObString &storage_path,
                                                   const ObString &access_info);
  static int check_change_storage_attribute_exist(common::ObISQLClient &proxy,
                                                   const ObString &storage_path,
                                                   const ObString &attribute);
#ifdef OB_BUILD_SHARED_STORAGE
  static int parse_shared_storage_info(const char *shared_storage_info,
                                       share::ObStorageUsedType::TYPE &used_for_type,
                                       int64_t &max_iops, int64_t &max_bandwidth,
                                       ObScopeType::TYPE &scope_type);
  static int parse_shared_storage_info(const ObString &shared_storage_info,
      obrpc::ObAdminStorageArg &result);
  static int parse_shared_storage_info(const ObString &shared_storage_info,
      obrpc::ObAdminStorageArg &result,
      ObBackupDest &storage_dest);
#endif
  static int set_storage_info_field(const char *info, char *field, const int64_t length);
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageDestCheck);
};

struct ObStorageDestAttr final
{
  OB_UNIS_VERSION(1);
public:
  ObStorageDestAttr() : path_(), endpoint_(), authorization_(), extension_()
  {
    reset();
  }
  ~ObStorageDestAttr() = default;
  void reset();
  ObStorageDestAttr &operator=(const ObStorageDestAttr &dest_attr);
  bool is_valid() const;
  int change_checksum_type(const ObStorageChecksumType &checksum_type);

  TO_STRING_KV(K_(path), K_(endpoint), K_(authorization), K_(extension));
  char path_[OB_MAX_BACKUP_PATH_LENGTH];
  char endpoint_[OB_MAX_BACKUP_ENDPOINT_LENGTH]; // host=xxx
  char authorization_[OB_MAX_BACKUP_AUTHORIZATION_LENGTH];
  char extension_[OB_MAX_BACKUP_EXTENSION_LENGTH]; // appid=xxx
};

struct ObZoneStorageTableInfo
{
  OB_UNIS_VERSION(1);
public:
  ObZoneStorageTableInfo() : dest_attr_(), state_(), used_for_(), storage_id_(),
                             op_id_(), sub_op_id_(), zone_(), max_iops_(), max_bandwidth_()
  {
    reset();
  }
  ~ObZoneStorageTableInfo() = default;
  void reset();
  ObZoneStorageTableInfo &operator=(const ObZoneStorageTableInfo &table_info);
  bool operator < (const ObZoneStorageTableInfo &other) const { return op_id_ != other.op_id_ ? op_id_ < other.op_id_ : sub_op_id_ < other.sub_op_id_; }
  bool is_valid() const;
  bool is_equal(const ObZoneStorageTableInfo &other) const { return op_id_ == other.op_id_; }

  TO_STRING_KV(K_(dest_attr), K_(state), K_(used_for), K_(storage_id), K_(op_id), K_(zone), K_(max_iops), K_(max_bandwidth));
  ObStorageDestAttr dest_attr_;
  ObZoneStorageState::STATE state_;
  ObStorageUsedType::TYPE used_for_;
  uint64_t storage_id_;
  uint64_t op_id_;
  uint64_t sub_op_id_;
  common::ObZone zone_;
  int64_t max_iops_;
  int64_t max_bandwidth_;
};
struct ObZoneStorageOperationTableInfo
{
public:
  ObZoneStorageOperationTableInfo();
  ~ObZoneStorageOperationTableInfo() = default;

  void reset()
  {
    op_id_ = UINT64_MAX;
    sub_op_id_ = UINT64_MAX;
    op_type_ = ObZoneStorageState::MAX;
  }

  TO_STRING_KV(K_(op_id), K_(sub_op_id), K_(op_type));

  uint64_t op_id_;
  uint64_t sub_op_id_;
  ObZoneStorageState::STATE op_type_;
};

struct ObDeviceConfig
{
public:
  bool is_valid() const;
  ObDeviceConfig() { reset(); }
  void reset()
  {
    memset(used_for_, 0, sizeof(used_for_));
    memset(path_, 0, sizeof(path_));
    memset(endpoint_, 0, sizeof(endpoint_));
    memset(access_info_, 0, sizeof(access_info_));
    memset(encrypt_info_, 0, sizeof(encrypt_info_));
    memset(extension_, 0, sizeof(extension_));
    memset(old_access_info_, 0, sizeof(access_info_));
    memset(old_encrypt_info_, 0, sizeof(encrypt_info_));
    memset(old_extension_, 0, sizeof(extension_));
    memset(state_, 0, sizeof(state_));
    memset(state_info_, 0, sizeof(state_info_));
    create_timestamp_ = 0;
    last_check_timestamp_ = 0;
    op_id_ = UINT64_MAX;
    sub_op_id_ = UINT64_MAX;
    storage_id_ = UINT64_MAX;
    max_iops_ = OB_INVALID_MAX_IOPS;
    max_bandwidth_ = OB_INVALID_MAX_BANDWIDTH;
  }

  TO_STRING_KV(KCSTRING_(used_for), KCSTRING_(path), KCSTRING_(endpoint),
               KCSTRING_(extension), KCSTRING_(old_extension), KCSTRING_(state),
               KCSTRING_(state_info), K_(create_timestamp), K_(last_check_timestamp), K_(op_id),
               K_(sub_op_id), K_(storage_id), K_(max_iops), K_(max_bandwidth));

  char used_for_[OB_MAX_STORAGE_USED_FOR_LENGTH];
  char path_[OB_MAX_BACKUP_DEST_LENGTH];
  char endpoint_[OB_MAX_BACKUP_ENDPOINT_LENGTH];
  char access_info_[OB_MAX_BACKUP_AUTHORIZATION_LENGTH];
  char encrypt_info_[OB_MAX_STORAGE_ENCRYPT_INFO_LENGTH];
  char extension_[OB_MAX_BACKUP_EXTENSION_LENGTH];
  char old_access_info_[OB_MAX_BACKUP_AUTHORIZATION_LENGTH];
  char old_encrypt_info_[OB_MAX_STORAGE_ENCRYPT_INFO_LENGTH];
  char old_extension_[OB_MAX_BACKUP_EXTENSION_LENGTH];
  char state_[OB_MAX_STORAGE_STATE_LENGTH];
  char state_info_[OB_MAX_STORAGE_STATE_INFO_LENGTH];
  int64_t create_timestamp_;
  int64_t last_check_timestamp_;
  uint64_t op_id_;
  uint64_t sub_op_id_;
  uint64_t storage_id_;
  int64_t max_iops_;
  int64_t max_bandwidth_;
};

} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_OBJECT_STORAGE_OB_OBJECT_STORAGE_STRUCT_H_ */
