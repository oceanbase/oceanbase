/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_load_data_storage_info.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

DEFINE_ENUM_FUNC(ObLoadDataFormat::Type, type, LOAD_DATA_FORMAT_DEF, ObLoadDataFormat::);

ObLoadDataStorageInfo::ObLoadDataStorageInfo() : load_data_format_(ObLoadDataFormat::INVALID_FORMAT)
{
}

ObLoadDataStorageInfo::~ObLoadDataStorageInfo() {}

int ObLoadDataStorageInfo::assign(const ObLoadDataStorageInfo &storage_info)
{
  int ret = OB_SUCCESS;
  if (this != &storage_info) {
    reset();
    if (OB_FAIL(ObObjectStorageInfo::assign(storage_info))) {
      LOG_WARN("fail to assign object storage info", KR(ret), K(storage_info));
    } else {
      load_data_format_ = storage_info.load_data_format_;
    }
  }
  return ret;
}

bool ObLoadDataStorageInfo::is_valid() const
{
  return ObObjectStorageInfo::is_valid() && ObLoadDataFormat::INVALID_FORMAT != load_data_format_;
}

void ObLoadDataStorageInfo::reset()
{
  load_data_format_ = ObLoadDataFormat::INVALID_FORMAT;
  ObObjectStorageInfo::reset();
}

int ObLoadDataStorageInfo::set(const ObStorageType device_type, const char *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObObjectStorageInfo::set(device_type, storage_info))) {
    LOG_WARN("fail to set object storage info", KR(ret), K(device_type), KP(storage_info));
  } else if (OB_FAIL(parse_load_data_params(storage_info))) {
    LOG_WARN("fail to parse load data params", KR(ret), KP(storage_info));
  } else if (OB_FAIL(set_load_data_param_defaults())) {
    LOG_WARN("fail to set load data param defaults", KR(ret));
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObLoadDataStorageInfo::parse_load_data_params(const char *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(storage_info));
  } else {
    const int64_t len = ::strlen(storage_info) + 1;
    char *str = nullptr;
    char *saved_ptr = nullptr;
    ObArenaAllocator allocator("LoadDataTmp");
    allocator.set_tenant_id(MTL_ID());
    if (OB_ISNULL(str = static_cast<char *>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", KR(ret), K(len));
    } else {
      MEMCPY(str, storage_info, len);
    }
    for (; OB_SUCC(ret); str = nullptr) {
      const char *token = ::strtok_r(str, "&", &saved_ptr);
      if (nullptr == token) {
        break;
      } else if (0 == ::strncmp(LOAD_DATA_FORMAT, token, ::strlen(LOAD_DATA_FORMAT))) {
        ObLoadDataFormat::Type load_data_format =
          ObLoadDataFormat::get_type_value(token + ::strlen(LOAD_DATA_FORMAT));
        if (OB_UNLIKELY(ObLoadDataFormat::INVALID_FORMAT == load_data_format)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid load data format value", KR(ret), K(token));
        } else {
          load_data_format_ = load_data_format;
        }
      } else {
      }
    }
  }
  return ret;
}

int ObLoadDataStorageInfo::set_load_data_param_defaults()
{
  int ret = OB_SUCCESS;
  if (ObLoadDataFormat::INVALID_FORMAT == load_data_format_) {
    load_data_format_ = ObLoadDataFormat::CSV;
  }
  return ret;
}

int ObLoadDataStorageInfo::get_storage_info_str(char *storage_info, const int64_t info_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObObjectStorageInfo::get_storage_info_str(storage_info, info_len))) {
    LOG_WARN("fail to get object storage info str", KR(ret), KPC(this), KP(storage_info),
             K(info_len));
  } else {
    int64_t pos = ::strlen(storage_info);
    if (OB_FAIL(databuff_printf(storage_info, info_len, pos, "&%s=%s", LOAD_DATA_FORMAT,
                                ObLoadDataFormat::get_type_string(load_data_format_)))) {
      LOG_WARN("fail to set load data format", K(ret), K(info_len));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
