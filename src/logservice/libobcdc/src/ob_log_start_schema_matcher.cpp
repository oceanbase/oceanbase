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
 *
 * Data Start Schema Matcher
 */

#include "ob_log_start_schema_matcher.h"
#include "ob_log_utils.h"                    // ob_cdc_malloc
#include "ob_log_instance.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace libobcdc
{
ObLogStartSchemaMatcher::ObLogStartSchemaMatcher() :
    buf_(NULL),
    buf_size_(0),
    tenant_schema_array_()
{ }

ObLogStartSchemaMatcher::~ObLogStartSchemaMatcher()
{
  (void)destroy();
}

int ObLogStartSchemaMatcher::init(const char *schema_version_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_pattern_(schema_version_str))) {
    OBLOG_LOG(ERROR, "init fail", KR(ret), K(schema_version_str));
  } else {
    // succ
  }
  return ret;
}

int ObLogStartSchemaMatcher::destroy()
{
  int ret = OB_SUCCESS;

  if (NULL != buf_) {
    ob_cdc_free(buf_);
    buf_ = NULL;
  }
  buf_size_ = 0;

  tenant_schema_array_.reset();
  return ret;
}

int ObLogStartSchemaMatcher::set_pattern_(const char *schema_version_str)
{
  int ret = OB_SUCCESS;
  char **buffer = &buf_;
  int64_t *buffer_size = &buf_size_;
  bool build_pattern = true;

  if (OB_ISNULL(schema_version_str) || 0 == strlen(schema_version_str)
      || 0 == strcmp(schema_version_str, DEFAULT_START_SCHEMA_VERSION_STR)) {
    ret = OB_SUCCESS;
    build_pattern = false;
    OBLOG_LOG(INFO, "schema_version_str is NULL or default", KR(ret), K(schema_version_str),
        K(DEFAULT_START_SCHEMA_VERSION_STR));
  } else {
    int tmp_ret = 0;
    *buffer_size = strlen(schema_version_str) + 1;
    // Alloc
    if (OB_ISNULL(*buffer = reinterpret_cast<char *>(ob_cdc_malloc(*buffer_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OBLOG_LOG(ERROR, "err alloc pattern string buffer", KR(ret), K(buffer_size));
    } else if (*buffer_size <= (tmp_ret = snprintf(*buffer, *buffer_size, "%s", schema_version_str))
             || (tmp_ret < 0)) {
      ret = OB_ERR_UNEXPECTED;
      OBLOG_LOG(ERROR, "err snprintf", KR(ret), K(tmp_ret), K(buffer_size), K(schema_version_str));
    } else {
      OBLOG_LOG(DEBUG, "pattern string", KR(ret), K(schema_version_str), K(buf_), K(buf_size_));
    }
  }

  // Split String
  if (OB_SUCC(ret) && build_pattern) {
    if (OB_FAIL(build_tenant_schema_version_())) {
      OBLOG_LOG(ERROR, "build tenant schema version failed", KR(ret), K(buf_), K(buf_size_));
    } else {
      //succ
      OBLOG_LOG(INFO, "data_start_schema_version set_pattern succ", KR(ret),
          K(tenant_schema_array_), K(schema_version_str));
    }
  }

  if (OB_UNLIKELY(OB_SUCCESS != ret) && NULL != *buffer) {
    ob_cdc_free(*buffer);
    *buffer = NULL;
    *buffer_size = 0;
  }

  return ret;
}

int ObLogStartSchemaMatcher::build_tenant_schema_version_()
{
  int ret = OB_SUCCESS;
  const char pattern_delimiter = '|';
  char **buffer = &buf_;
  bool done = false;

  TenantSchemaArray &tenant_schema_array = tenant_schema_array_;

  if (OB_ISNULL(buf_) || OB_UNLIKELY(buf_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    OBLOG_LOG(ERROR, "invalid buffer", KR(ret), K(buf_), K(buf_size_));
  } else {
    ObString remain(strlen(*buffer), *buffer);
    ObString cur_pattern;
    TenantSchema tenant_schema;

    while (OB_SUCCESS == ret && !done) {
      tenant_schema.reset();
      cur_pattern = remain.split_on(pattern_delimiter);
      if (cur_pattern.empty()) {
        cur_pattern = remain;
        done = true;
      }

      if (OB_SUCC(ret)) {
        ObString &str = cur_pattern;
        *(str.ptr() + str.length()) = '\0';
        str.set_length(1 + str.length());
      }

      uint64_t tenant_id = 0;
      int64_t schema_version = -1;
      if (OB_SUCC(ret)) {
        if (2 != sscanf(cur_pattern.ptr(), "%lu:%ld", &tenant_id, &schema_version)) {
          ret = OB_ERR_UNEXPECTED;
          OBLOG_LOG(ERROR, "sscanf failed, data_start_schema_version pattern is invalid",
              KR(ret), K(cur_pattern), K(tenant_id), K(schema_version));
        } else if (OB_UNLIKELY(0 == tenant_id) || OB_UNLIKELY(schema_version <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          OBLOG_LOG(ERROR, "data_start_schema_version input is invalid, set pattern fail",
              KR(ret), K(cur_pattern), K(tenant_id), K(schema_version));
        } else {
          tenant_schema.tenant_id_ = tenant_id;
          tenant_schema.schema_version_ = schema_version;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(tenant_schema_array.push_back(tenant_schema))) {
          OBLOG_LOG(ERROR, "tenant schema array push back failed", KR(ret), K(tenant_schema));
        }
      }
    } // while
  }

  return ret;
}

int ObLogStartSchemaMatcher::match_data_start_schema_version(const uint64_t tenant_id,
    bool &match,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  match = false;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    OBLOG_LOG(ERROR, "tenant_id is invalid", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && !match && i < tenant_schema_array_.count(); i++) {
      TenantSchema &tenant_schema = tenant_schema_array_.at(i);
      if (tenant_schema.tenant_id_ == tenant_id) {
        match = true;
        schema_version = tenant_schema.schema_version_;
      }
    }
  }

  if (OB_SUCC(ret) && match) {
    OBLOG_LOG(INFO, "[START_SCHEMA_MATCH] set_data_start_schema_version succ",
        K(tenant_id), KR(ret), K(match), K(schema_version));
  }
  return ret;
}

void ObLogStartSchemaMatcher::TenantSchema::reset()
{
  tenant_id_ = 0;
  schema_version_ = 0;
}
}
}
