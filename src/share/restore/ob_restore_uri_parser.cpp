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
#include <stdlib.h>
#include "ob_restore_uri_parser.h"
#include "common/ob_zone.h"
#include "lib/utility/utility.h"
#include "lib/restore/ob_storage.h"
#include "lib/restore/ob_storage_path.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;
namespace oceanbase {
namespace share {

ObAgentBackupInfo::ObAgentBackupInfo()
    : backup_type_(-1),
      curr_data_version_(-1),
      timestamp_(-1),
      backup_result_(-1),
      base_data_version_(-1),
      base_schema_version_(-1)
{}

ObRestoreURIParser::ExtraArgsCb::Action ObRestoreURIParser::ExtraArgsCb::actions_[] = {
    {"restore_user", ObRestoreURIParser::ExtraArgsCb::set_restore_user, true},
    {"restore_pass", ObRestoreURIParser::ExtraArgsCb::set_restore_pass, false},
    {"timestamp", ObRestoreURIParser::ExtraArgsCb::set_data_timestamp, false},
    {"locality", ObRestoreURIParser::ExtraArgsCb::set_locality, false},
    {"pool_list", ObRestoreURIParser::ExtraArgsCb::set_pool_list, false},
    {"primary_zone", ObRestoreURIParser::ExtraArgsCb::set_primary_zone, false},
    {"tcp_invited_nodes", ObRestoreURIParser::ExtraArgsCb::set_tcp_invited_nodes, false},
    {"base_data_version", ObRestoreURIParser::ExtraArgsCb::set_base_data_version, false},
    {"curr_data_version", ObRestoreURIParser::ExtraArgsCb::set_curr_data_version, false}};

ObRestoreURIParser::ExtraArgsCb::ExtraArgsCb(share::ObRestoreArgs& arg) : arg_(arg)
{
  MEMSET(is_set_, 0, sizeof(is_set_));
};

int ObRestoreURIParser::ExtraArgsCb::match(const char* key, const char* value)
{
  int ret = OB_SUCCESS;
  // defined by the extra params after question mark in oss uri
  bool found = false;
  for (int i = 0; i < ACTION_CNT && !found && OB_SUCC(ret); ++i) {
    if (0 == STRCASECMP(actions_[i].key, key)) {
      if (OB_FAIL(actions_[i].setter(arg_, value))) {
        LOG_WARN("fail set value", K(value), K(ret));
      } else {
        is_set_[i] = true;
        found = true;
      }
    }
  }
  if (!found) {
    LOG_TRACE("KV pair ignored by common restore uri parser.", K(key), K(value), K(ret));
  }
  return ret;
}

bool ObRestoreURIParser::ExtraArgsCb::check() const
{
  bool pass = true;
  for (int i = 0; i < ACTION_CNT && pass; ++i) {
    if (actions_[i].required && !is_set_[i]) {
      pass = false;
      LOG_USER_WARN(OB_MISS_ARGUMENT, actions_[i].key);
    }
  }
  return pass;
}

int ObRestoreURIParser::parse(const ObString& uri, ObRestoreArgs& arg)
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH];
  if (uri.length() >= OB_MAX_URI_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "the uri too long", "len", uri.length(), K(uri), K(ret));
  } else {
    MEMSET(uri_buf, 0, OB_MAX_URI_LENGTH);
    MEMCPY(uri_buf, uri.ptr(), uri.length());
    if (OB_FAIL(parse(uri_buf, arg))) {
      LOG_WARN("fail parse uri buf", K(uri_buf), K(ret));
    }
  }
  return ret;
}

int ObRestoreURIParser::parse(const char* uri, ObRestoreArgs& arg)
{
  int ret = OB_SUCCESS;
  char dir_path[OB_MAX_URI_LENGTH];
  char extra_args[OB_MAX_URI_LENGTH];
  ExtraArgsCb extra_arg_cb(arg);
  ObKVParser kv_parser('=', '&');
  DirParser dir_parser(arg);
  arg.reset();
  kv_parser.set_match_callback(extra_arg_cb);
  kv_parser.set_allow_space(false);
  int64_t uri_len = STRLEN(uri);
  if (OB_ISNULL(uri)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the oss argument is invalid", K(ret));
  } else if (uri_len >= OB_MAX_URI_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "the uri too long", K(uri_len), K(ret));
  } else if (OB_FAIL(split_uri(uri, arg, dir_path, extra_args))) {
    LOG_WARN("fail split uri", K(uri), K(ret));
  } else if (OB_FAIL(dir_parser.parse(dir_path))) {
    LOG_WARN("fail parse dir", K(dir_path), K(uri), K(ret));
  } else if (OB_FAIL(kv_parser.parse(extra_args))) {
    LOG_WARN("fail parse arg", K(extra_args), K(uri), K(ret));
  } else if (FALSE_IT(STRCPY(arg.storage_info_, extra_args))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("UNEXPECTED! Should not enter this branch", K(ret));
  } else if (!arg.is_parse_ok()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(uri), K(arg), K(ret));
  }
  return ret;
}

int ObRestoreURIParser::split_uri(const char* uri, ObRestoreArgs& arg, char* dir_path, char* extra_args)
{
  UNUSED(arg);
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* start = uri;
  while ('?' != start[pos] && '\0' != start[pos]) {
    pos++;
  }
  if ('?' != start[pos]) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extra arg seperator not found", K(ret));
  } else {
    STRNCPY(dir_path, start, pos);
    dir_path[pos] = '\0';
    pos++;
  }
  if (OB_SUCC(ret)) {
    if ('\0' == start[pos]) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty extra args", K(ret));
    } else {
      STRCPY(extra_args, start + pos);
    }
  }
  return ret;
}

int ObRestoreURIParserHelper::set_data_version(ObRestoreArgs& arg)
{
  int ret = OB_SUCCESS;
  char* backup_info_buf = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObStoragePath backup_info_path;
  ObStorageUtil util(true /*need retry*/);
  int64_t file_length = 0;

  if (0 == STRLEN(arg.storage_info_) || 0 == STRLEN(arg.uri_header_) || 0 == STRLEN(arg.cluster_name_) ||
      OB_INVALID_TENANT_ID == arg.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(arg));
  } else if (OB_FAIL(ObStoragePathUtil::generate_backup_info_file_path(ObString::make_string(arg.get_uri_header()),
                 ObString::make_string(arg.get_cluster_name()),
                 arg.tenant_id_,
                 backup_info_path))) {
    OB_LOG(WARN, "fail to generate backup_info_path", K(ret), K(arg));
  } else if (OB_FAIL(util.get_file_length(backup_info_path.get_obstring(), arg.get_storage_info(), file_length))) {
    OB_LOG(WARN, "fail to get file length", K(ret), K(backup_info_path), K(arg));
  } else if (file_length <= 0) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "backup info file is empty", K(ret), K(backup_info_path), K(arg));
  } else {
    const int64_t text_file_length = file_length + 1;
    if (OB_ISNULL(backup_info_buf = static_cast<char*>(allocator.alloc(text_file_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to allocate memory", K(ret), K(text_file_length));
    } else if (OB_FAIL(util.read_single_text_file(
                   backup_info_path.get_obstring(), arg.get_storage_info(), backup_info_buf, text_file_length))) {
      OB_LOG(WARN, "fail to read object", K(ret), K(backup_info_path), K(arg));
    }

    ObArray<ObAgentBackupInfo> backup_infos;
    ObAgentBackupInfo backup_info;
    if (OB_SUCC(ret)) {
      char* saveptr = NULL;
      char* line = strtok_r(backup_info_buf, "\n", &saveptr);
      ObString tmp_str;
      while (line != NULL && OB_SUCC(ret)) {
        int count = sscanf(line,
            "%ld_%ld_%ld_%ld_%ld_%ld_%lu",
            &backup_info.backup_type_,
            &backup_info.curr_data_version_,
            &backup_info.timestamp_,
            &backup_info.backup_result_,
            &backup_info.base_data_version_,
            &backup_info.base_schema_version_,
            &backup_info.cluster_version_);
        if (count == 5) {
          backup_info.base_schema_version_ = 0;
        }
        if (count < 5 || count > 7) {
          ret = OB_INVALID_DATA;
          OB_LOG(WARN, "fail to get backup info", K(line), K(ret));
        } else if (OB_FAIL(backup_infos.push_back(backup_info))) {
          OB_LOG(WARN, "fail to push backup info", K(ret));
        } else {
          line = strtok_r(NULL, "\n", &saveptr);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (backup_infos.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "backup info has no data", K(backup_infos.count()), K(ret));
      } else {
        bool only_get_cluster_version = false;
        if (OB_INVALID_VERSION != arg.curr_data_version_ || OB_INVALID_VERSION != arg.base_data_version_) {
          // When base_data_version and curr_data_version are set, the corresponding value is directly taken, and there
          // is no need to retrieve it from the file based on timestamp. For compatibility reasons, the original logic
          // is retained. But to circumvent the above problems, both agentrestore and observer are required to be
          // upgraded to the latest version.
          if (OB_INVALID_VERSION == arg.curr_data_version_ || OB_INVALID_VERSION == arg.base_data_version_) {
            ret = OB_INVALID_ARGUMENT;
            OB_LOG(WARN, "argument is invalid", K(ret), K(arg));
          } else {
            only_get_cluster_version = true;
          }
        }
        std::sort(&backup_infos.at(0), &backup_infos.at(0) + backup_infos.count(), ObAgentBackupInfo::info_compare);
        int64_t i = backup_infos.count() - 1;
        for (; i >= 0 && OB_SUCC(ret); --i) {
          if (only_get_cluster_version) {
            // uri contains base_data_versio and curr_data_version. Here only get cluster_version
            if (backup_infos.at(i).base_data_version_ == arg.base_data_version_ &&
                backup_infos.at(i).curr_data_version_ == arg.curr_data_version_) {
              arg.cluster_version_ = backup_infos.at(i).cluster_version_;
              break;
            }
          } else {
            if (backup_infos.at(i).timestamp_ <= arg.restore_timeu_ &&
                OB_SUCCESS == backup_infos.at(i).backup_result_) {
              arg.base_data_version_ = backup_infos.at(i).base_data_version_;
              arg.curr_data_version_ = backup_infos.at(i).curr_data_version_;
              arg.cluster_version_ = backup_infos.at(i).cluster_version_;
              break;
            }
          }
        }
        if (i < 0) {
          ret = OB_ENTRY_NOT_EXIST;
          OB_LOG(WARN, "fail to get backup info", K(ret), K(i), K(arg));
        } else if (OB_INVALID_VERSION == arg.base_data_version_ || OB_INVALID_VERSION == arg.curr_data_version_) {
          ret = OB_OSS_DATA_VERSION_NOT_MATCHED;
          OB_LOG(WARN, "fail to get data version from timestamp", K(arg), K(ret));
        }
      }
    }
  }

  OB_LOG(INFO, "get restore args", K(arg));
  return ret;
}

// parse: oss://071092/obfin001/1001 or file:///mnt/test_nfs_runiu/071092/obfin001/1001
int ObRestoreURIParser::DirParser::parse(const char* val)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. NULL ptr", K(ret));
  } else {
    int64_t val_len = STRLEN(val);
    if (val_len <= strlen(OB_OSS_PREFIX) || val_len <= strlen(OB_FILE_PREFIX) ||
        (0 != MEMCMP(val, OB_OSS_PREFIX, strlen(OB_OSS_PREFIX)) &&
            0 != MEMCMP(val, OB_FILE_PREFIX, strlen(OB_FILE_PREFIX)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("path has no oss common path", K(ret), K(val), K(OB_OSS_PREFIX), K(OB_FILE_PREFIX));
    } else {
      // get tenant_id
      int64_t tenant_pos = val_len - 1;
      while (tenant_pos > 0 && '/' != val[tenant_pos]) {
        tenant_pos--;
      }
      if ('/' == val[tenant_pos]) {
        if (tenant_pos <= 0 || tenant_pos >= val_len - 1) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid URI format", K(ret), K(val));
        } else if (OB_FAIL(ob_atoll(val + tenant_pos + 1, arg_.tenant_id_))) {
          LOG_WARN("fail parse tenant", K(val), K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_INVALID_TENANT_ID != arg_.tenant_id_) {  // get cluster_name
        int64_t cluster_pos = tenant_pos - 1;
        while (cluster_pos > 0 && '/' != val[cluster_pos]) {
          --cluster_pos;
        }
        if ('/' == val[cluster_pos]) {
          if (cluster_pos <= 0 || cluster_pos >= tenant_pos - 1) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Invalid URI format", K(ret), K(val));
          } else {
            const int64_t name_length = tenant_pos - cluster_pos - 1;
            if (name_length <= 0 || name_length >= sizeof(arg_.cluster_name_)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("Invalid URI format", K(ret), K(val), K(name_length));
            } else {
              MEMCPY(arg_.cluster_name_, val + cluster_pos + 1, name_length);
              arg_.cluster_name_[name_length] = '\0';
            }
          }
        }
        if (OB_SUCC(ret) && 0 != strlen(arg_.cluster_name_)) {  // get oss header
          if (cluster_pos <= 0 || cluster_pos >= sizeof(arg_.uri_header_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Invalid URI format", K(ret), K(val), K(cluster_pos));
          } else {
            STRNCPY(arg_.uri_header_, val, cluster_pos);
            arg_.uri_header_[cluster_pos] = '\0';
          }
        }
      }
      if (OB_FAIL(ret)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("dir parse fail", K(val), K(ret));
      }
    }
  }
  OB_LOG(INFO, "get uri header and cluster name", K(arg_));
  return ret;
}

int ObRestoreURIParser::ExtraArgsCb::set_data_timestamp(share::ObRestoreArgs& arg, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ob_atoll(val, arg.restore_timeu_))) {
    LOG_WARN("fail parse timestamp", K(val), K(ret));
  }
  return ret;
}

int ObRestoreURIParser::ExtraArgsCb::set_base_data_version(share::ObRestoreArgs& arg, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ob_atoll(val, arg.base_data_version_))) {
    LOG_WARN("fail parse base data version", K(val), K(ret));
  }
  return ret;
}

int ObRestoreURIParser::ExtraArgsCb::set_curr_data_version(share::ObRestoreArgs& arg, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ob_atoll(val, arg.curr_data_version_))) {
    LOG_WARN("fail parse curr data version", K(val), K(ret));
  }
  return ret;
}

int ObRestoreURIParser::ExtraArgsCb::set_locality(share::ObRestoreArgs& arg, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= MAX_LOCALITY_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    STRCPY(arg.locality_, val);
  }
  return ret;
}

int ObRestoreURIParser::ExtraArgsCb::set_primary_zone(share::ObRestoreArgs& arg, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= MAX_ZONE_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    STRCPY(arg.primary_zone_, val);
  }
  return ret;
}

int ObRestoreURIParser::ExtraArgsCb::set_tcp_invited_nodes(share::ObRestoreArgs& arg, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= share::ObRestoreArgs::OB_MAX_TCP_INVITED_NODES_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    STRCPY(arg.tcp_invited_nodes_, val);
  }
  return ret;
}

int ObRestoreURIParser::ExtraArgsCb::set_pool_list(share::ObRestoreArgs& arg, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= ObRestoreArgs::OB_MAX_POOL_LIST_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    STRCPY(arg.pool_list_, val);
  }
  return ret;
}

int ObRestoreURIParser::ExtraArgsCb::set_restore_user(share::ObRestoreArgs& arg, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= OB_MAX_USERNAME_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    STRCPY(arg.restore_user_, val);
  }
  return ret;
}

int ObRestoreURIParser::ExtraArgsCb::set_restore_pass(share::ObRestoreArgs& arg, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= OB_MAX_PASSWORD_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    STRCPY(arg.restore_pass_, val);
  }
  return ret;
}

int ObRestoreURIParser::ZoneMapCb::match(const char* key, const char* val)
{
  int ret = OB_SUCCESS;
  ObZone tmp;
  int hash_ret = zone_map_.get_refactored(ObZone(key), tmp);
  if (OB_SUCCESS == hash_ret) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key dumplicated in map", K(key), K(ret));
  } else if (OB_HASH_NOT_EXIST == hash_ret) {
    ret = zone_map_.set_refactored(ObZone(key), ObZone(val));
    LOG_INFO("parse zone map", "map_from", key, "map_to", val);
  }
  return ret;
}

ObPhysicalRestoreOptionParser::ExtraArgsCb::Action ObPhysicalRestoreOptionParser::ExtraArgsCb::actions_[] = {
    {"restore_job_id", ObPhysicalRestoreOptionParser::ExtraArgsCb::set_restore_job_id, false},
    {"backup_cluster_id", ObPhysicalRestoreOptionParser::ExtraArgsCb::set_backup_cluster_id, true},
    {"backup_cluster_name", ObPhysicalRestoreOptionParser::ExtraArgsCb::set_backup_cluster_name, true},
    {"pool_list", ObPhysicalRestoreOptionParser::ExtraArgsCb::set_pool_list, true},
    {"locality", ObPhysicalRestoreOptionParser::ExtraArgsCb::set_locality, false},
    {"primary_zone", ObPhysicalRestoreOptionParser::ExtraArgsCb::set_primary_zone, false},
};

ObPhysicalRestoreOptionParser::ExtraArgsCb::ExtraArgsCb(ObPhysicalRestoreJob& job) : job_(job)
{
  MEMSET(is_set_, 0, sizeof(is_set_));
};

int ObPhysicalRestoreOptionParser::ExtraArgsCb::match(const char* key, const char* value)
{
  int ret = OB_SUCCESS;
  // defined by the extra params after the question mark in oss uri
  bool found = false;
  for (int i = 0; i < ACTION_CNT && !found && OB_SUCC(ret); ++i) {
    if (0 == STRCASECMP(actions_[i].key, key)) {
      if (OB_FAIL(actions_[i].setter(job_, value))) {
        LOG_WARN("fail set value", K(value), K(ret));
      } else {
        is_set_[i] = true;
        found = true;
      }
    }
  }
  if (!found) {
    LOG_TRACE("KV pair ignored by common restore uri parser.", K(key), K(value), K(ret));
  }
  return ret;
}

bool ObPhysicalRestoreOptionParser::ExtraArgsCb::check() const
{
  bool pass = true;
  for (int i = 0; i < ACTION_CNT && pass; ++i) {
    if (actions_[i].required && !is_set_[i]) {
      pass = false;
      LOG_USER_WARN(OB_MISS_ARGUMENT, actions_[i].key);
    }
  }
  return pass;
}

int ObPhysicalRestoreOptionParser::parse(const ObString& uri, ObPhysicalRestoreJob& job)
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH];
  if (uri.length() >= OB_MAX_URI_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "the uri too long", "len", uri.length(), K(uri), K(ret));
  } else {
    MEMSET(uri_buf, 0, OB_MAX_URI_LENGTH);
    MEMCPY(uri_buf, uri.ptr(), uri.length());
    if (OB_FAIL(parse(uri_buf, job))) {
      LOG_WARN("fail parse uri buf", K(uri_buf), K(ret));
    }
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::parse(const char* uri, ObPhysicalRestoreJob& job)
{
  int ret = OB_SUCCESS;
  char extra_args[OB_MAX_URI_LENGTH];
  ExtraArgsCb extra_arg_cb(job);
  ObKVParser kv_parser('=', '&');
  kv_parser.set_match_callback(extra_arg_cb);
  kv_parser.set_allow_space(false);
  int64_t uri_len = STRLEN(uri);
  if (OB_ISNULL(uri)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the oss argument is invalid", K(ret));
  } else if (uri_len >= OB_MAX_URI_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "the uri too long", K(uri_len), K(ret));
  } else if (OB_FAIL(kv_parser.parse(uri))) {
    LOG_WARN("fail parse arg", K(extra_args), K(uri), K(ret));
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_restore_job_id(ObPhysicalRestoreJob& job, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ob_atoll(val, job.restore_job_id_))) {
    LOG_WARN("fail parse restore_job_id", K(val), K(ret));
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_backup_cluster_id(ObPhysicalRestoreJob& job, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ob_atoll(val, job.cluster_id_))) {
    LOG_WARN("fail parse cluster_id", K(val), K(ret));
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_backup_cluster_name(ObPhysicalRestoreJob& job, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= common::OB_MAX_CLUSTER_NAME_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    STRCPY(job.backup_cluster_name_, val);
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_pool_list(ObPhysicalRestoreJob& job, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    STRCPY(job.pool_list_, val);
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_locality(ObPhysicalRestoreJob& job, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= common::MAX_LOCALITY_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    STRCPY(job.locality_, val);
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_primary_zone(ObPhysicalRestoreJob& job, const char* val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= common::MAX_ZONE_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    STRCPY(job.primary_zone_, val);
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
