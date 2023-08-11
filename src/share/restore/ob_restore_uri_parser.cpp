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
#include "lib/restore/ob_storage_path.h"
#include "lib/restore/ob_storage.h"
#include "share/backup/ob_backup_io_adapter.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;
namespace oceanbase
{
namespace share
{

ObPhysicalRestoreOptionParser::ExtraArgsCb::Action ObPhysicalRestoreOptionParser::ExtraArgsCb::actions_[] = {
  {"pool_list",             ObPhysicalRestoreOptionParser::ExtraArgsCb::set_pool_list,            true},
  {"locality",              ObPhysicalRestoreOptionParser::ExtraArgsCb::set_locality,             false},
  {"primary_zone",          ObPhysicalRestoreOptionParser::ExtraArgsCb::set_primary_zone,         false},
  {"kms_encrypt",           ObPhysicalRestoreOptionParser::ExtraArgsCb::set_kms_encrypt,          false},
  {"concurrency",           ObPhysicalRestoreOptionParser::ExtraArgsCb::set_concurrency,          false},
};

ObPhysicalRestoreOptionParser::ExtraArgsCb::ExtraArgsCb(ObPhysicalRestoreJob &job)
  : job_(job)
{
  MEMSET(is_set_, 0, sizeof(is_set_));
};

int ObPhysicalRestoreOptionParser::ExtraArgsCb::match(const char *key, const char *value)
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

int ObPhysicalRestoreOptionParser::parse(
    const ObString &uri,
    ObPhysicalRestoreJob &job)
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

int ObPhysicalRestoreOptionParser::parse(
    const char *uri,
    ObPhysicalRestoreJob &job)
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


int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_pool_list(
    ObPhysicalRestoreJob &job,
    const char *val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(job.set_pool_list(ObString(val)))) {
    LOG_WARN("failed to set pool list", KR(ret), K(val));
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_locality(
    ObPhysicalRestoreJob &job,
    const char *val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= common::MAX_LOCALITY_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(job.set_locality(ObString(val)))) {
    LOG_WARN("failed to set locality", KR(ret), K(val));
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_primary_zone(
    ObPhysicalRestoreJob &job,
    const char *val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= common::MAX_ZONE_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(job.set_primary_zone(ObString(val)))) {
    LOG_WARN("failed to set primary zone", KR(ret), K(val));
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_kms_encrypt(
    ObPhysicalRestoreJob &job,
    const char *val)
{
  int ret = OB_SUCCESS;
  bool kms_encrypt = false;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STRLEN(val) >= common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH) {
    ret = OB_SIZE_OVERFLOW;
  } else if (0 != STRNCASECMP("true", val, STRLEN(val))) {
    kms_encrypt = false;
  } else {
    kms_encrypt = true;
  }
  if (OB_SUCC(ret)) {
    job.set_kms_encrypt(kms_encrypt);
  }
  return ret;
}

int ObPhysicalRestoreOptionParser::ExtraArgsCb::set_concurrency(
    ObPhysicalRestoreJob &job,
    const char *val)
{
  int ret = OB_SUCCESS;
  int64_t concurrency = 0;
  if (OB_ISNULL(val)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ob_atoll(val, concurrency))) {
    LOG_WARN("failed to atoll", K(ret), K(val));
  } else if (concurrency < 0 || concurrency > 100) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("concurrency is not valid", K(ret), K(concurrency));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "restore concurrency");
  } else {
    job.set_concurrency(concurrency);
  }
  return ret;
}

int ObPhysicalRestoreUriParser::parse(
    const common::ObString &multi_uri,
    common::ObArenaAllocator &allocator,
    common::ObIArray<common::ObString> &uri_list)
{
  int ret = OB_SUCCESS;
  uri_list.reset();
  char *buf = NULL, *tok = NULL, *ptr = NULL;
  
  if (multi_uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(multi_uri));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(multi_uri.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", KR(ret));
  } else {
    MEMCPY(buf, multi_uri.ptr(), multi_uri.length());
    buf[multi_uri.length()] = '\0';
    tok = buf;
    bool is_repeat = false;
    for (char *str = tok; OB_SUCC(ret); str = NULL) {
      tok = ::strtok_r(str, ",", &ptr);
      if (OB_ISNULL(tok)) {
        break;
      }

      ObString path(tok);
      const ObString actual_path = path.trim();
      if (OB_FAIL(validate_uri_type(actual_path))) {
        LOG_WARN("invalid uri", KR(ret), K(path), K(actual_path));
        LOG_USER_ERROR(OB_INVALID_BACKUP_DEST, tok);
      } else if (OB_FAIL(find_repeat_(uri_list, actual_path, is_repeat))) {
        LOG_WARN("failed to find repeat", KR(ret), K(actual_path), K(uri_list));
      } else if (is_repeat) {
        // skip repeat path
        LOG_INFO("skip repeat path", K(actual_path));
      } else if (OB_FAIL(uri_list.push_back(actual_path))) {
        LOG_WARN("failed to push back", KR(ret), K(actual_path));
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreUriParser::find_repeat_(const common::ObIArray<common::ObString> &uri_list, const common::ObString &uri, bool &is_repeat)
{
  int ret = OB_SUCCESS;
  is_repeat = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < uri_list.count(); i++) {
    const common::ObString &the_uri = uri_list.at(i);
    if (the_uri.compare(uri) == 0) {
      is_repeat = true;
      break;
    }
  }
  return ret;
}

}/* ns share*/
}/* ns oceanbase */


