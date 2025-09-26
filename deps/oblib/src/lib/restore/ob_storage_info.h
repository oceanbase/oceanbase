/*
 *  Copyright (c) 2022 OceanBase
 *  OceanBase is licensed under Mulan PubL v2.
 *  You can use this software according to the terms and conditions of the Mulan PubL v2.
 *  You may obtain a copy of Mulan PubL v2 at:
 *           http://license.coscl.org.cn/MulanPubL-2.0
 *  THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 *  EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 *  MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 *  See the Mulan PubL v2 for more details.
 *  Authors:
 *
 */

#ifndef OCEANBASE_LIB_RESTORE_OB_STORAGE_INFO_H_
#define OCEANBASE_LIB_RESTORE_OB_STORAGE_INFO_H_

#include "common/storage/ob_device_common.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include <curl/curl.h>
#include "lib/hash/ob_hashmap.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include <type_traits>

namespace oceanbase
{

namespace common
{

const int64_t OB_MAX_OBJECT_STORAGE_REGION_LENGTH = 128;
const int64_t OB_MAX_BACKUP_EXTENSION_LENGTH = 512;
const int64_t OB_MAX_BACKUP_ENDPOINT_LENGTH = 256;
const int64_t OB_MAX_BACKUP_ACCESSID_LENGTH = 256;
const int64_t OB_MAX_BACKUP_ACCESSKEY_LENGTH = 256;
const int64_t OB_MAX_BACKUP_STORAGE_INFO_LENGTH = 1600;
// OB_MAX_DEVICE_KEY_LENGTH = OB_MAX_BACKUP_STORAGE_INFO_LENGTH + strlen("&storage_type=x")
const int64_t OB_MAX_DEVICE_KEY_LENGTH = OB_MAX_BACKUP_STORAGE_INFO_LENGTH + 15;
const int64_t OB_MAX_BACKUP_ENCRYPTKEY_LENGTH = OB_MAX_BACKUP_ACCESSKEY_LENGTH + 32;
const int64_t OB_MAX_BACKUP_SERIALIZEKEY_LENGTH = OB_MAX_BACKUP_ENCRYPTKEY_LENGTH * 2;
// We have agreed with OCP that the maximum role_arn length shall not exceed 256
static constexpr int64_t OB_MAX_ROLE_ARN_LENGTH = 256;
// The limit on the maximum length of external_id in obs/oss/s3 is 128
static constexpr int64_t OB_MAX_EXTERNAL_ID_LENGTH = 128;
static constexpr int64_t OB_MAX_ASSUME_ROLE_JSON_DATA_LENGTH = 1024;
// STS_AK and STS_SK are used to connect to STS service of OCP.
// We have agreed with ocp that the maximum length of sts_sk/sts_ak is 32.
// And the maximum length of sts_url is 512.
static constexpr int64_t OB_MAX_STS_AK_LENGTH = 64;
static constexpr int64_t OB_MAX_STS_SK_LENGTH = 64;
static constexpr int64_t OB_MAX_STS_URL_LENGTH = 512;
static constexpr int64_t OB_PREDEFINED_STS_TOKEN_LENGTH = 1024;
static constexpr int64_t OB_MAX_STS_CREDENTIAL_LENGTH  = OB_MAX_STS_AK_LENGTH + OB_MAX_STS_SK_LENGTH + OB_MAX_STS_URL_LENGTH;

static constexpr int64_t OB_MAX_STS_SIGNATURE_LENGTH = 64;
static constexpr int64_t OB_MAX_STS_CONCAT_LENGTH = 512;
static constexpr int64_t OB_MAX_STS_SIGNATURE_NONCE_LENTH = 64;
static constexpr int64_t OB_MAX_STS_SIGNATURE_RAND_NUM = 10000;
static constexpr int64_t OB_MAX_STS_REQUEST_ID_LENTH = 64;
static constexpr int64_t OB_MAX_STS_CURL_CONNECTTIMEOUT_MS = 10000; // 10s
static constexpr int64_t OB_MAX_STS_CURL_TIMEOUT_SECONDS = 10; // 10s

// To ensure that the temporary credentials in the credential map are always valid,
// the credentials are refreshed every 20 minutes.
static constexpr int64_t CREDENTIAL_TASK_SCHEDULE_INTERVAL_US = 1200LL * 1000LL * 1000LL;  // 20min
const char *const ACCESS_ID = "access_id=";
const char *const ACCESS_KEY = "access_key=";
const char *const HOST = "host=";
const char *const APPID = "appid=";
const char *const DELETE_MODE = "delete_mode=";
const char *const REGION = "s3_region=";
const char* const SEPERATE_SYMBOL = "&";
const char *const MAX_IOPS = "max_iops=";
const char *const MAX_BANDWIDTH = "max_bandwidth=";
const char *const ENABLE_WORM = "enable_worm=";

const char *const ADDRESSING_MODEL = "addressing_model=";
const char *const ADDRESSING_MODEL_VIRTUAL_HOSTED_STYLE = "virtual_hosted_style";
const char *const ADDRESSING_MODEL_PATH_STYLE = "path_style";

const char *const CHECKSUM_TYPE = "checksum_type=";
const char *const CHECKSUM_TYPE_NO_CHECKSUM = "no_checksum";
const char *const CHECKSUM_TYPE_MD5 = "md5";
const char *const CHECKSUM_TYPE_CRC32 = "crc32";

const char *const ROLE_ARN = "role_arn=";
const char *const EXTERNAL_ID = "external_id=";
const char *const STS_AK = "sts_ak=";
const char *const STS_SK = "sts_sk=";
const char *const STS_URL = "sts_url=";
const char *const OSS_ROLE_ARN_PREFIX = "acs";
const char *const OBS_ROLE_ARN_PREFIX = "iam";
const char *const S3_ROLE_ARN_PREFIX = "arn";
const char *const COS_ROLE_ARN_PREFIX = "qcs";
const char *const STS_ACTION = "GetResourceSTSCredential";
const char *const STS_RESOURCE_SOURCE = "OBSERVER";

const char *const OB_DEVICE_CREDENTIAL_ALLOCATOR = "ObjDeviceCredentialAlloc";
static constexpr int64_t MAX_CREDENTIAL_IDLE_DURATION_US = 24 * 3600 * 1000 * 1000L;  // 24h

enum ObStorageAddressingModel
{
  OB_VIRTUAL_HOSTED_STYLE = 0,
  OB_PATH_STYLE = 1,
  OB_STORAGE_ADDRESSING_MAX_TYPE = 2
};
enum ObStorageChecksumType : uint8_t
{
  OB_NO_CHECKSUM_ALGO = 0,
  OB_MD5_ALGO = 1,
  OB_CRC32_ALGO = 2,
  OB_STORAGE_CHECKSUM_MAX_TYPE = 3
};

bool is_oss_supported_checksum(const ObStorageChecksumType checksum_type);
bool is_s3_supported_checksum(const ObStorageChecksumType checksum_type);
bool is_obdal_supported_checksum(const ObStorageType storage_type, const ObStorageChecksumType checksum_type);
const char *get_storage_checksum_type_str(const ObStorageChecksumType &type);
int get_storage_checksum_type(const char *checksum_type_str, ObStorageChecksumType &checksum_type);
bool is_use_obdal();
// [Extensions]
//   load_data_* : sql/engine/cmd/ob_load_data_storage_info.h

struct ObSTSToken
{
  ObSTSToken();
  virtual ~ObSTSToken();
  TO_STRING_KV(KP_(data), KP_(data_arr), K_(is_below_predefined_length));
  void reset();
  int set(const ObString &token);
  int assign(const ObSTSToken &token);
  const char *get_data() const;
  // "阿里云STS服务返回的安全令牌（STS Token）的长度不固定，强烈建议您不要假设安全令牌的最大长度。"
  // therefore, use allocator to alloc mem for sts_token dynamically
  int64_t length() const {return data_len_;};
  bool is_valid() const;
  bool is_below_predefined_length() const {return is_below_predefined_length_;};
  char *data_;
  ObArenaAllocator allocator_;
  // Since cloud vendors all claim that the length of sts_token is variable, we previously used
  // allocator for memory allocation. But later we found that allocating memory every time would
  // increase the cpu consumption, so we optimize it by using a fixed-length char array. When the
  // length of sts_token is less than 1024, allocation is no longer performed, but data_arr is used
  // directly for storage. Please refer to the documentation for details:
  //
  char data_arr_[OB_PREDEFINED_STS_TOKEN_LENGTH];
  int64_t data_len_;
  bool is_below_predefined_length_;
  bool is_inited_;
};

struct ObObjectStorageCredential
{
  ObObjectStorageCredential();
  virtual ~ObObjectStorageCredential()
  {
    reset();
  }
  TO_STRING_KV(K_(expiration_s), K_(access_time_us), K_(born_time_us),
      K_(access_id), KP_(access_key), K_(sts_token));
  void reset();
  int assign(const ObObjectStorageCredential &credential);

  // Temporary ak and sk to access bucket
  char access_id_[OB_MAX_BACKUP_ACCESSID_LENGTH];
  char access_key_[OB_MAX_BACKUP_ACCESSKEY_LENGTH];
  ObSTSToken sts_token_;
  // Expiration time of current ak/sk returned from STS Service
  int64_t expiration_s_;
  // Latest access time of the credential
  int64_t access_time_us_;
  int64_t born_time_us_;
};

class ObClusterStateBaseMgr
{
public:
  ObClusterStateBaseMgr() {}
  virtual ~ObClusterStateBaseMgr() {}
  virtual int is_supported_assume_version() const
  {
    return OB_SUCCESS;
  };
  virtual int is_supported_enable_worm_version() const
  {
    return OB_SUCCESS;
  };
  virtual int is_supported_azblob_version() const
  {
    return OB_SUCCESS;
  }
  virtual bool is_shared_storage_mode() const
  {
    return false;
  }
  virtual bool is_write_with_if_match() const { return false; }
  static ObClusterStateBaseMgr &get_instance()
  {
    static ObClusterStateBaseMgr mgr;
    return mgr;
  }
};

enum ObStorageDeleteMode: uint8_t
{
  NONE = 0,
  STORAGE_DELETE_MODE = 1,
  STORAGE_TAGGING_MODE = 2,
  MAX
};

class ObObjectStorageInfo;
class ObStorageAccount
{
public:
  ObStorageAccount();
  virtual ~ObStorageAccount() {};
  virtual void reset();
  virtual bool is_valid() const { return is_valid_; }
  virtual int assign(const ObObjectStorageInfo *storage_info) = 0;

  TO_STRING_KV(K(is_valid_), K(delete_mode_), K(endpoint_), K(access_id_),
      K(sts_token_), KP(access_key_));

public:
  bool is_valid_;
  char endpoint_[OB_MAX_BACKUP_ENDPOINT_LENGTH];
  char access_id_[OB_MAX_BACKUP_ACCESSID_LENGTH];
  char access_key_[OB_MAX_BACKUP_ACCESSKEY_LENGTH];
  ObStorageDeleteMode delete_mode_;
  ObSTSToken sts_token_;
};

// ObObjectStorageInfo 存储了需要访问对象存储的所有信息，包括 ak、sk、endpoint 等等
// ObObjectStorageInfo 可以由一个特定格式的字符串进行初始化，也可以由另外一个 ObObjectStorageInfo 对象赋值，除此之外不提供修改数据的途径
// 一些字段是可选的，例如 region, checksum_type, addressing_model，它们存在 extension 字段中，但为了方便提取，使用成员变量存放副本
class ObObjectStorageInfo
{
  OB_UNIS_VERSION(1);

public:
  ObObjectStorageInfo();
  virtual ~ObObjectStorageInfo();
  virtual void reset();
  virtual int set(const common::ObStorageType device_type, const char *storage_info);
  virtual int set(const char *uri, const char *storage_info);
  virtual int assign(const ObObjectStorageInfo &storage_info);
  virtual int clone(common::ObIAllocator &allocator, ObObjectStorageInfo *&storage_info) const;
  int reset_access_id_and_access_key(const char *access_id, const char *access_key);
  static int register_cluster_state_mgr(ObClusterStateBaseMgr *cluster_version_mgr);

public:
  int64_t hash() const;
  bool operator ==(const ObObjectStorageInfo &storage_info) const;
  bool operator !=(const ObObjectStorageInfo &storage_info) const;
  bool is_access_info_equal(const ObObjectStorageInfo &storage_info) const;

  ObStorageDeleteMode get_delete_mode() const { return delete_mode_; }
  ObStorageType get_type() const;
  const char *get_type_str() const;
  ObStorageChecksumType get_checksum_type() const;
  const char *get_checksum_type_str() const;
  const char *get_extension() const { return extension_; }

  bool is_hdfs_storage() const { return OB_STORAGE_HDFS == device_type_; }
  virtual bool is_backup_storage_info() const { return false; }
  bool is_enable_worm() const;
  bool is_assume_role_mode() const;
  virtual bool is_valid() const;
  virtual int validate_arguments() const;

  // This function allows the device_manager to determine the key values for different storage information.
  // Since delete_mode and addressing_mode are recorded in the extension field, they are not separately recorded.
  virtual int get_device_map_key_str(char *key_str, const int64_t len) const;
  virtual int64_t get_device_map_key_len() const;
  // This function retrieves authentication information.
  // When assume_role is not used, it functions the same as get_storage_info_str.
  virtual int get_authorization_str(char *authorization_str,
                                    const int64_t authorization_str_len,
                                    ObSTSToken &sts_token) const;

  virtual int get_storage_info_str(char *storage_info, const int64_t info_len) const;
  virtual int to_account(ObStorageAccount &account) const;
  bool is_write_with_if_match() const;

  TO_STRING_KV(K_(endpoint), K_(access_id), K_(extension), "type", get_type_str(),
      K_(checksum_type), K_(max_iops), K_(max_bandwidth), KP_(role_arn), KP_(external_id), K_(enable_worm));
protected:
  virtual int get_access_key_(char *buf, const int64_t buf_len) const;
  virtual int get_info_str_(char *storage_info, const int64_t info_len) const;
  virtual int append_extension_str_(char *storage_info, const int64_t info_len) const;
  virtual int parse_storage_info_(const char *storage_info, bool &has_appid);
  int set_storage_info_field_(const char *info, char *field, const int64_t length);
  int set_enable_worm_(const char *enable_worm);
  int set_delete_mode_(const char *delete_mode);
  int set_addressing_model_(const char *addressing_model);
  int set_checksum_type_(const char *checksum_type_str);

  template <typename T>
  static int clone_impl_(ObIAllocator &allocator, const T &src, T *&dst)
  {
    static_assert(
        std::is_base_of<ObObjectStorageInfo, T>::value,
        "T must be ObObjectStorageInfo or its derived class");
    int ret = OB_SUCCESS;
    dst = nullptr;
    if (OB_UNLIKELY(!src.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "src info is invalid", K(ret), K(src));
    } else if (OB_ISNULL(dst = OB_NEWx(T, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc dst", K(ret), K(src));
    } else if (OB_FAIL(dst->assign(src))) {
      OB_LOG(WARN, "fail to assign storage info", K(ret), K(src));
      OB_DELETEx(T, &allocator, dst);
      dst = nullptr;
    }
    return ret;
  }

public:
  // TODO: Rename device_type_ to storage_protocol_type_ for better clarity
  // Prefix in the storage_info string, such as 's3://', indicates the protocol used to access the
  // target. Currently, both OBS and GCS are accessed via the s3 protocol, hence s3_region is updated
  // to be an optional parameter
  common::ObStorageType device_type_;
  // Optional parameter. If not provided, the default value OB_MD5_ALGO will be used.
  // For OSS, OB_NO_CHECKSUM_ALGO indicates that no checksum algorithm will be used.
  // For Object Storage Services accessed via the S3 protocol,
  // OB_NO_CHECKSUM_ALGO is not supported.
  ObStorageChecksumType checksum_type_;                                 // Repeated in extension_
  ObStorageAddressingModel addressing_model_;                           // Repeated in extension_
  ObStorageDeleteMode delete_mode_;                                     // Repeated in extension_
  char region_[OB_MAX_OBJECT_STORAGE_REGION_LENGTH];                    // Repeated in extension_
  char endpoint_[OB_MAX_BACKUP_ENDPOINT_LENGTH];
  char access_id_[OB_MAX_BACKUP_ACCESSID_LENGTH];
  char access_key_[OB_MAX_BACKUP_ENCRYPTKEY_LENGTH];
  char extension_[OB_MAX_BACKUP_EXTENSION_LENGTH];
  int64_t max_iops_;
  int64_t max_bandwidth_;
  char role_arn_[OB_MAX_ROLE_ARN_LENGTH];                               // supported for assume role
  char external_id_[OB_MAX_EXTERNAL_ID_LENGTH];                         // supported for assume role
  bool is_assume_role_mode_;
  bool enable_worm_;
  static ObClusterStateBaseMgr *cluster_state_mgr_;
};

class ObTenantStsCredentialBaseMgr
{
public:
  ObTenantStsCredentialBaseMgr() {}
  ~ObTenantStsCredentialBaseMgr() {}
  virtual int get_sts_credential(char *sts_credential, const int64_t sts_credential_buf_len) = 0;
};

class ObStsCredential
{
public:
  ObStsCredential();
  virtual ~ObStsCredential();
  int init(const uint64_t tenant_id);
  void reset();
  int get_sts_credential();
  static int register_sts_credential_mgr(ObTenantStsCredentialBaseMgr *sts_credential_mgr);
  TO_STRING_KV(K_(tenant_id), K_(sts_ak), KP_(sts_sk), K_(sts_url),  KP_(sts_credential_mgr));
public:
  uint64_t tenant_id_;
  char sts_ak_[OB_MAX_STS_AK_LENGTH];
  char sts_sk_[OB_MAX_STS_SK_LENGTH];
  char sts_url_[OB_MAX_STS_URL_LENGTH];
  bool is_inited_;
private:
  static ObTenantStsCredentialBaseMgr *sts_credential_mgr_;
};

class ObDeviceCredentialKey
{
public:
  ObDeviceCredentialKey();
  virtual ~ObDeviceCredentialKey();
  int init(const ObObjectStorageInfo &storage_info);
  void reset();
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  int assign(const ObDeviceCredentialKey &other);
  bool operator==(const ObDeviceCredentialKey &other) const;
  bool operator!=(const ObDeviceCredentialKey &other) const;
  bool is_valid() const;

  int construct_signed_url(char *url_buf, const int64_t url_buf_len) const;
  TO_STRING_KV(K_(is_inited), KP_(role_arn), KP_(external_id), K_(tenant_id));

public:
  char role_arn_[OB_MAX_ROLE_ARN_LENGTH];
  char external_id_[OB_MAX_EXTERNAL_ID_LENGTH];
  // tenant_id is used to distinguish different tenants
  uint64_t tenant_id_;
  bool is_inited_;

private:
  int init_(const char *role_arn, const char *external_id);
};

int check_sts_credential_format(const char *sts_credential, ObStsCredential &credential_key);

class ObDeviceCredentialMgr
{
public:
  enum ResponseItem
  {
    AccessKeyId = 0,
    AccessKeySecret = 1,
    SecurityToken = 2,
    DurationSeconds = 3
  };
  // It is used to receive the response from STS
  class ResponseAndAllocator
  {
  public:
    ResponseAndAllocator(char *&response, common::ObArenaAllocator &allocator)
        : response_(response), allocator_(allocator)
    {}
    ~ResponseAndAllocator()
    {}
    char *&response_;
    ObArenaAllocator &allocator_;
  };

  static ObDeviceCredentialMgr &get_instance();
  virtual ~ObDeviceCredentialMgr();
  int init();
  // curl STS service to perform as assume role is used, and then update @credential_map_
  void destroy();
  int connect_to_sts(
      const ObDeviceCredentialKey &credential_key, ResponseAndAllocator &res_and_allocator);
  int curl_credential(
      const ObObjectStorageInfo &storage_info, const bool update_access_time = true);
  int curl_credential(
      const ObDeviceCredentialKey &credential_key, const bool update_access_time = true);
  int get_credential(
      const ObObjectStorageInfo &storage_info, ObObjectStorageCredential &device_credential);
  int get_credential(
      const ObDeviceCredentialKey &credential_key, ObObjectStorageCredential &device_credential);
  bool operator=(const ObDeviceCredentialMgr &) = delete;
  // refresh all managed credentials
  int refresh();
  void set_credential_duration_us(const int64_t duration_us)
  {
    credential_duration_us_ = duration_us;
  }

private:
  ObDeviceCredentialMgr();
  static int64_t on_write_data_(
      const void *ptr, const int64_t size, const int64_t nmemb, void *user_data);
  static int64_t debug_callback(
      CURL *handle, curl_infotype type, char *data, size_t size, void *userp);
  int get_credential_from_map_(
      const ObDeviceCredentialKey &credential_key, ObObjectStorageCredential &device_credential);
  int parse_device_credential_(const char *res_ptr, ObObjectStorageCredential &credential);

private:
  static const int64_t RESPONSE_ITEM_CNT = 4;
  static const char *response_items_[RESPONSE_ITEM_CNT];
  typedef hash::ObHashMap<ObDeviceCredentialKey, ObObjectStorageCredential> CredentialMap;
  bool is_inited_;
  CredentialMap credential_map_;
  common::SpinRWLock credential_lock_;
  // The time when the credential expires from the cache
  int64_t credential_duration_us_;
};

class CredentialAccessTimeCallBack
{
public:
  explicit CredentialAccessTimeCallBack(const bool update_access_time_us)
      : update_access_time_us_(update_access_time_us), original_access_time_us_(0)
  {}
  void operator()(hash::HashMapPair<ObDeviceCredentialKey, ObObjectStorageCredential> &v)
  {
    original_access_time_us_ = v.second.access_time_us_;
    if (update_access_time_us_) {
      v.second.access_time_us_ = ObTimeUtility::current_time();
    }
  };

  bool update_access_time_us_;
  int64_t original_access_time_us_;
};

// @brief ObClusterEnableObdalConfigBase is used to check whether enable obdal or not.
class ObClusterEnableObdalConfigBase
{
public:
  ObClusterEnableObdalConfigBase() {};
  virtual ~ObClusterEnableObdalConfigBase() {};
  virtual bool is_enable_obdal() const { return true; }
  static ObClusterEnableObdalConfigBase &get_instance() {
    static ObClusterEnableObdalConfigBase instance;
    return instance;
  }
};
extern ObClusterEnableObdalConfigBase *cluster_enable_obdal_config;

}
}

#endif
