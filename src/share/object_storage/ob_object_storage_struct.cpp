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

#define USING_LOG_PREFIX SHARE
#include "ob_object_storage_struct.h"
#include "rootserver/ob_root_service.h"
#include "share/object_storage/ob_zone_storage_table_operation.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_ss_format_util.h"
#endif

using namespace oceanbase;
using namespace lib;
using namespace common;
using namespace share;

static const char *access_mode_strs[] = {"access_mode=access_by_id",
                                         "access_mode=access_by_ram_url"};

const char *ObStorageAccessMode::get_str(const MODE &mode)
{
  const char *str = nullptr;
  if (mode < 0 || mode >= MAX) {
    str = "UNKNOWN";
  } else {
    str = access_mode_strs[mode];
  }
  return str;
}
ObStorageAccessMode::MODE ObStorageAccessMode::get_mode(const char *mode_str)
{
  ObStorageAccessMode::MODE mode = ObStorageAccessMode::MAX;
  const int64_t count = ARRAYSIZEOF(access_mode_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObStorageAccessMode::MAX) == count, "mode count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(mode_str, access_mode_strs[i])) {
      mode = static_cast<ObStorageAccessMode::MODE>(i);
      break;
    }
  }
  return mode;
}

static const char *state_strs[] = {"ADDING", "ADDED", "DROPPING", "DROPPED", "CHANGING", "CHANGED"};

const char *ObZoneStorageState::get_str(const STATE &state)
{
  const char *str = nullptr;
  if (state < 0 || state >= MAX) {
    str = "UNKNOWN";
  } else {
    str = state_strs[state];
  }
  return str;
}

ObZoneStorageState::STATE ObZoneStorageState::get_state(const char *state_str)
{
  ObZoneStorageState::STATE state = ObZoneStorageState::MAX;
  const int64_t count = ARRAYSIZEOF(state_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObZoneStorageState::MAX) == count, "state count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(state_str, state_strs[i])) {
      state = static_cast<ObZoneStorageState::STATE>(i);
      break;
    }
  }
  return state;
}

static const char *type_strs[] = {"DATA", "LOG", "ALL"};

const char *ObStorageUsedType::get_str(const TYPE &type)
{
  const char *str = nullptr;
  if (type < 0 || type >= TYPE::USED_TYPE_MAX) {
    str = "UNKNOWN";
  } else {
    str = type_strs[type];
  }
  return str;
}

ObStorageUsedType::TYPE ObStorageUsedType::get_type(const char *type_str)
{
  ObStorageUsedType::TYPE type = ObStorageUsedType::TYPE::USED_TYPE_MAX;

  const int64_t count = ARRAYSIZEOF(type_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObStorageUsedType::TYPE::USED_TYPE_MAX) == count,
                "type count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == STRCASECMP(type_str, type_strs[i])) {
      type = static_cast<ObStorageUsedType::TYPE>(i);
      break;
    }
  }
  return type;
}

static const char *scope_type_strs[] = {"ZONE", "REGION"};

const char *ObScopeType::get_str(const TYPE &type)
{
  const char *str = nullptr;
  if (type < 0 || type >= TYPE::MAX) {
    str = "UNKNOWN";
  } else {
    str = scope_type_strs[type];
  }
  return str;
}

ObScopeType::TYPE ObScopeType::get_type(const char *type_str)
{
  ObScopeType::TYPE type = ObScopeType::TYPE::MAX;

  const int64_t count = ARRAYSIZEOF(scope_type_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObScopeType::TYPE::MAX) == count,
                "type count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == STRCASECMP(type_str, scope_type_strs[i])) {
      type = static_cast<ObScopeType::TYPE>(i);
      break;
    }
  }
  return type;
}

//***********************ObStorageDestCheck***************************
// get bucket path from uri   oss://obbucket/user/test.txt -->  oss://obbucket
int ObStorageDestCheck::get_bucket_path(const ObString &uri, ObString &bucket_path,
                                        ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString tmp_bucket;
  ObString::obstr_size_t bucket_start = 0;
  ObString::obstr_size_t bucket_end = 0;
  char *buf = NULL;
  common::ObStorageType type;
  if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uri is empty", KR(ret), K(uri));
  } else if (OB_FAIL(get_storage_type_from_path(uri, type))) {
    LOG_WARN("failed to get storage type", KR(ret));
  } else {
    if (OB_STORAGE_OSS == type) {
      bucket_start = static_cast<ObString::obstr_size_t>(strlen(OB_OSS_PREFIX));
    } else if (OB_STORAGE_COS == type) {
      bucket_start = static_cast<ObString::obstr_size_t>(strlen(OB_COS_PREFIX));
    } else if (OB_STORAGE_S3 == type) {
      bucket_start = static_cast<ObString::obstr_size_t>(strlen(OB_S3_PREFIX));
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the storage type is not supported in shared storage mode", KR(ret), K(uri));
    }
    for (int64_t i = bucket_start; OB_SUCC(ret) && i < uri.length() - 2; i++) {
      if ('/' == *(uri.ptr() + i) && '/' == *(uri.ptr() + i + 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("uri has two // ", K(uri), KR(ret), K(i));
        break;
      }
    }
  }
  // get the bucket path
  if (OB_SUCC(ret)) {
    if (bucket_start >= uri.length()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("bucket name is NULL", K(uri), KR(ret));
    } else {
      for (bucket_end = bucket_start; OB_SUCC(ret) && bucket_end < uri.length(); ++bucket_end) {
        if ('/' == *(uri.ptr() + bucket_end)) {
          ObString::obstr_size_t bucket_length = bucket_end;
          if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(OB_MAX_URI_LENGTH)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory error", K(OB_MAX_URI_LENGTH), KR(ret));
          } else {
            // must end with '\0'
            int n = snprintf(buf, OB_MAX_URI_LENGTH, "%.*s", bucket_length, uri.ptr());
            if (n <= 0 || n >= OB_MAX_URI_LENGTH) {
              ret = OB_SIZE_OVERFLOW;
              LOG_WARN("fail to deep copy bucket", K(uri), K(bucket_start), K(bucket_length),
                     K(n), K(OB_MAX_URI_LENGTH), KR(ret));
            } else {
              bucket_path.assign_ptr(buf, n); // must include '\0'
            }
            break;
          }
        }
        if (OB_SUCC(ret) && bucket_end == uri.length()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("bucket name is invalid", K(uri), KR(ret));
        }
      }
      if (OB_SUCC(ret) && bucket_path.empty()) {
        bucket_path = uri;
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("get bucket object name ", K(uri), K(bucket_path));
  }
  return ret;
}

int ObStorageDestCheck::check_change_storage_accessinfo_exist(common::ObISQLClient &proxy,
                                                              const ObString &storage_path,
                                                              const ObString &access_info)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest dest;
  ObArray<share::ObStorageDestAttr> dest_attrs;
  char storage_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  if (OB_UNLIKELY(storage_path.empty() || access_info.empty())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("storage path or accessinfo should not be empty", KR(ret), K(storage_path));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "storage path or accessinfo is empty, it is");
  } else if (OB_FAIL(databuff_printf(storage_dest_str, OB_MAX_BACKUP_DEST_LENGTH, "%s&%s", storage_path.ptr(),
                                     access_info.ptr()))) {
    LOG_WARN("failed to set path", KR(ret), K(storage_path));
  } else if (OB_FAIL(dest.set(storage_dest_str))) {
    LOG_WARN("failed to set dest", KR(ret), K(dest));
  } else if (OB_FAIL(ObStorageInfoOperator::get_zone_storage_table_dest(proxy, dest_attrs))) {
    LOG_WARN("failed to get storage dest", KR(ret));
  } else {
    bool is_path_exist = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_attrs.count(); i++) {
      share::ObBackupDest table_dest;
      bool is_equal = false;
      if (OB_FAIL(table_dest.set(dest_attrs.at(i).path_, dest_attrs.at(i).endpoint_,
                                 dest_attrs.at(i).authorization_, dest_attrs.at(i).extension_))) {
        LOG_WARN("failed to set storage dest", KR(ret));
      } else if (OB_FAIL(dest.is_backup_path_equal(table_dest, is_equal))) {
        LOG_WARN("fail to compare backup path", KR(ret), K(table_dest), K(dest), K(is_equal));
      } else if (is_equal) {
        is_path_exist = true;
        if (dest.get_storage_info()->is_access_info_equal(*(table_dest.get_storage_info()))) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("the value of access id&access key or ram_url are the same as the table item", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "the value of access id and access key are the same as the table item, it is");
        }
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (false == is_path_exist) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("storage path is not exist in zone storage table", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "storage path is not exist in zone storage table, it is");
    }
  }
  return ret;
}

int ObStorageDestCheck::check_change_storage_attribute_exist(common::ObISQLClient &proxy,
                                                              const ObString &storage_path,
                                                              const ObString &attribute)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObStorageDestAttr> dest_attrs;
  char root_path[OB_MAX_BACKUP_PATH_LENGTH] = {0};
  char endpoint[OB_MAX_BACKUP_ENDPOINT_LENGTH] = {0};
  if (OB_UNLIKELY(storage_path.empty() || attribute.empty())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("storage path or attribute should not be empty", KR(ret), K(storage_path), K(attribute));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "storage path or attribute is empty, it is");
  } else if (OB_FAIL(ObStorageInfoOperator::parse_storage_path(
             storage_path.ptr(), root_path, sizeof(root_path), endpoint, sizeof(endpoint)))) {
    LOG_WARN("fail to parse storage path", KR(ret), K(storage_path));
  } else if (OB_FAIL(ObStorageInfoOperator::get_zone_storage_table_dest(proxy, dest_attrs))) {
    LOG_WARN("failed to get storage dest", KR(ret));
  } else {
    bool is_path_exist = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_attrs.count(); i++) {
      if (0 == STRCMP(dest_attrs.at(i).path_, root_path) &&
          0 == STRCMP(dest_attrs.at(i).endpoint_, endpoint)) {
        is_path_exist = true;
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (false == is_path_exist) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("storage path is not exist in zone storage table", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "storage path is not exist in zone storage table, it is");
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObStorageDestCheck::parse_shared_storage_info(const char *shared_storage_info,
                                                  share::ObStorageUsedType::TYPE &used_for_type,
                                                  int64_t &max_iops, int64_t &max_bandwidth,
                                                  ObScopeType::TYPE &scope_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(shared_storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(shared_storage_info));
  } else {
    char used_for_type_str[OB_MAX_STORAGE_USED_FOR_LENGTH] = {0};
    char scope_str[OB_MAX_STORAGE_SCOPE_LENGTH] = {0};
    char *saved_ptr = NULL;
    char tmp[OB_MAX_BACKUP_DEST_LENGTH] = {0};
    char *token = NULL;
    int64_t info_len = strlen(shared_storage_info);
    MEMCPY(tmp, shared_storage_info, info_len);
    tmp[info_len] = '\0';
    token = tmp;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "&", &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == STRNCASECMP(STORAGE_USED_FOR_TYPE, token, strlen(STORAGE_USED_FOR_TYPE))) {
        if (OB_FAIL(set_storage_info_field(token + strlen(STORAGE_USED_FOR_TYPE), used_for_type_str, sizeof(used_for_type_str)))) {
          LOG_WARN("failed to set used for type", KR(ret), K(token));
        } else {
          used_for_type = share::ObStorageUsedType::get_type(used_for_type_str);
        }
      } else if (0 == STRNCASECMP(STORAGE_MAX_IOPS, token, strlen(STORAGE_MAX_IOPS))) {
        const char *max_iops_start_str = token + strlen(STORAGE_MAX_IOPS);
        if (false == ObString(max_iops_start_str).is_numeric()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("max_iops is invalid argument", KR(ret), K(token));
        } else {
          char *end_str = nullptr;
          max_iops = strtoll(max_iops_start_str, &end_str, 10);
          if ('\0' != *end_str) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid int value", KR(ret), K(max_iops_start_str));
          }
        }
      } else if (0 == STRNCASECMP(STORAGE_MAX_BANDWIDTH, token, strlen(STORAGE_MAX_BANDWIDTH))) {
        bool is_valid = false;
        max_bandwidth = ObConfigCapacityParser::get(token + strlen(STORAGE_MAX_BANDWIDTH), is_valid, true /*check_unit*/);
        if (!is_valid) {
          ret = OB_CONVERT_ERROR;
          LOG_WARN("convert failed", KR(ret), K(token));
        }
      }
    }
  }
  return ret;
}

int ObStorageDestCheck::parse_shared_storage_info(
    const ObString &shared_storage_info,
    obrpc::ObAdminStorageArg &result)
{
  int ret = OB_SUCCESS;
  ObBackupDest dummy_storage_dest;
  return parse_shared_storage_info(shared_storage_info, result, dummy_storage_dest);
}

int ObStorageDestCheck::parse_shared_storage_info(
    const ObString &shared_storage_info,
    obrpc::ObAdminStorageArg &result,
    ObBackupDest &storage_dest)
{
  int ret = OB_SUCCESS;
  storage_dest.reset();
  char access_info[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = {0};
  char path[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  char attribute[OB_MAX_STORAGE_ATTRIBUTE_LENGTH] = {0};
  int64_t max_iops = 0;
  int64_t max_bandwidth = 0;
  share::ObScopeType::TYPE scope_type = share::ObScopeType::TYPE::MAX; // no use
  int64_t pos = 0;
  if (shared_storage_info.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("shared_storage_info is empty", KR(ret));
  } else if (OB_FAIL(storage_dest.set(shared_storage_info))) {
    LOG_WARN("fail to set storage dest", KR(ret), K(storage_dest));
  } else if (OB_UNLIKELY(!storage_dest.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage dest is valid", KR(ret), K(storage_dest));
  } else if (OB_FAIL(databuff_printf(access_info, sizeof(access_info), pos, "%s&%s",
             storage_dest.get_storage_info()->access_id_, storage_dest.get_storage_info()->access_key_))) {
    LOG_WARN("failed to set access info", KR(ret), K(storage_dest));
  // TODO: 等三月份方丹合入ram_url才能支持，先注释，现在只支持AK/SK的方式格式化启动 @xiaotao.ht
  // } else if (OB_FAIL(storage_dest.get_storage_info()->get_authorization_info(authorization, sizeof(authorization)))) {
  //   LOG_WARN("fail to get authorization info str", KR(ret), K(authorization));
  // } else if (ObStorageAccessMode::ACCESS_BY_RAM_URL == storage_dest.get_storage_info()->access_mode_ &&
  //            OB_FAIL(databuff_printf(access_info, sizeof(access_info), pos, "%s", authorization))) {
  //   LOG_WARN("failed to set access info", KR(ret), K(storage_dest));
  } else if (STRLEN(storage_dest.get_storage_info()->extension_) != 0 &&
             OB_FAIL(databuff_printf(access_info, sizeof(access_info), pos, "&%s", storage_dest.get_storage_info()->extension_))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(storage_dest));
  } else if (FALSE_IT(result.use_for_ = share::ObStorageUsedType::TYPE::USED_TYPE_ALL)) { // default type used for all (data and log)
  } else if (OB_FAIL(ObStorageDestCheck::parse_shared_storage_info(
                     shared_storage_info.ptr(), result.use_for_, max_iops, max_bandwidth, scope_type))) {
    LOG_WARN("fail to parse shared storage info", KR(ret));
  } else if (result.use_for_ != share::ObStorageUsedType::TYPE::USED_TYPE_ALL) { // used_for_type only support USED_TYPE_ALL
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("used for type is not supported", KR(ret), K(result.use_for_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use for type other than all is ");
  } else if (max_iops < 0 || max_bandwidth < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("max_iops or max_bandwidth is invalid", KR(ret), K(max_iops), K(max_bandwidth));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "max_iops or max_bandwidth");
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(databuff_printf(attribute, sizeof(attribute), "%s%ld&%s%ldB",
                     STORAGE_MAX_IOPS, max_iops, STORAGE_MAX_BANDWIDTH, max_bandwidth))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (OB_FAIL(storage_dest.get_backup_path_str(path, sizeof(path)))) {
    LOG_WARN("fail to set storage path", KR(ret), K(path), K(storage_dest));
  } else if (OB_FAIL(result.access_info_.assign(access_info))) {
    LOG_WARN("fail to assign access info", KR(ret));
  } else if (OB_FAIL(result.path_.assign(path))) {
    LOG_WARN("fail to assign path", KR(ret), K(path));
  } else if (OB_FAIL(result.attribute_.assign(attribute))) {
    LOG_WARN("fail to assign attribute", KR(ret), K(attribute));
  } else {
    result.op_ = obrpc::ObAdminStorageArg::AdminStorageOp::ADD;
    LOG_INFO("succeed to parse shared storage info", KR(ret), K(result));
  }
  return ret;
}
#endif

int ObStorageDestCheck::set_storage_info_field(const char *info, char *field, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (NULL == info || NULL == field) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(info), KP(field));
  } else {
    const int64_t info_len = strlen(info);
    int64_t pos = strlen(field);
    if (info_len >= length) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("info is too long ", KR(ret), K(info), K(length));
    } else if (OB_FAIL(databuff_printf(field, length, pos, "%s", info))) {
      LOG_WARN("failed to set storage info field", KR(ret), K(info), K(field));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObStorageDestAttr, path_, endpoint_, authorization_, extension_);
void ObStorageDestAttr::reset()
{
  path_[0] = '\0';
  endpoint_[0] = '\0';
  authorization_[0] = '\0';
  extension_[0] = '\0';
}

ObStorageDestAttr &ObStorageDestAttr::operator=(const ObStorageDestAttr &dest_attr)
{
  if (this != &dest_attr) {
    MEMCPY(path_, dest_attr.path_, sizeof(dest_attr.path_));
    MEMCPY(endpoint_, dest_attr.endpoint_, sizeof(dest_attr.endpoint_));
    MEMCPY(authorization_, dest_attr.authorization_, sizeof(dest_attr.authorization_));
    MEMCPY(extension_, dest_attr.extension_, sizeof(dest_attr.extension_));
  }
  return *this;
}

bool ObStorageDestAttr::is_valid() const
{
  return 0 != strlen(path_) &&
         ObString(path_).prefix_match(OB_FILE_PREFIX) ?
         (0 == strlen(endpoint_) && 0 == strlen(authorization_)) :
         (0 != strlen(endpoint_) && 0 != strlen(authorization_));
}

OB_SERIALIZE_MEMBER(ObZoneStorageTableInfo, dest_attr_, state_, used_for_, storage_id_, op_id_,
                    sub_op_id_, zone_, max_iops_, max_bandwidth_);
void ObZoneStorageTableInfo::reset()
{
  dest_attr_.reset();
  state_ = ObZoneStorageState::MAX;
  used_for_ = ObStorageUsedType::USED_TYPE_MAX;
  storage_id_ = OB_INVALID_ID;
  op_id_ = OB_INVALID_ID;
  sub_op_id_ = OB_INVALID_ID;
  zone_.reset();
  max_iops_ = OB_INVALID_MAX_IOPS;
  max_bandwidth_ = OB_INVALID_MAX_BANDWIDTH;
}

ObZoneStorageTableInfo &ObZoneStorageTableInfo::operator=(const ObZoneStorageTableInfo &table_info)
{
  if (this != &table_info) {
    dest_attr_ = table_info.dest_attr_;
    state_ = table_info.state_;
    used_for_ = table_info.used_for_;
    storage_id_ = table_info.storage_id_;
    op_id_ = table_info.op_id_;
    sub_op_id_ = table_info.sub_op_id_;
    zone_ = table_info.zone_;
    max_iops_ = table_info.max_iops_;
    max_bandwidth_ = table_info.max_bandwidth_;
  }
  return *this;
}

bool ObZoneStorageTableInfo::is_valid() const
{
  return dest_attr_.is_valid() && ObZoneStorageState::is_valid(state_) &&
         ObStorageUsedType::is_valid(used_for_) && !zone_.is_empty() &&
         storage_id_ != OB_INVALID_ID && op_id_ != OB_INVALID_ID && max_iops_ >= 0 && max_bandwidth_ >= 0;
}

ObZoneStorageOperationTableInfo::ObZoneStorageOperationTableInfo()
  : op_id_(UINT64_MAX), sub_op_id_(UINT64_MAX), op_type_(ObZoneStorageState::MAX)
{
}

bool ObDeviceConfig::is_valid() const
{
  bool bool_ret = true;
  if ((STRLEN(used_for_) <= 0) || (STRLEN(path_) <= 0)
      || (STRLEN(state_) <= 0) || (create_timestamp_ < 0)
      || (last_check_timestamp_ < 0)
      || (UINT64_MAX == op_id_)
      || (UINT64_MAX == sub_op_id_)
      || (UINT64_MAX == storage_id_)
      || (max_iops_ < 0)
      || (max_bandwidth_ < 0)) {
    bool_ret = false;
  } else {
    if (ObString(path_).prefix_match(OB_FILE_PREFIX)) {
      if ((STRLEN(endpoint_) > 0) || (STRLEN(access_info_) > 0)) {
        bool_ret = false;
      }
    } else {
      if ((STRLEN(endpoint_) <= 0) || (STRLEN(access_info_) <= 0)) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}
