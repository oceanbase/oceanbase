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
#pragma once

#include "lib/utility/ob_macro_utils.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace sql
{
const char *const LOAD_DATA_FORMAT = "load_data_format=";

struct ObLoadDataFormat
{
public:
  ObLoadDataFormat() {}
  ~ObLoadDataFormat() {}

#define LOAD_DATA_FORMAT_DEF(DEF) \
  DEF(INVALID_FORMAT, = 0)        \
  DEF(CSV, )                      \
  DEF(OB_BACKUP_1_4, )            \
  DEF(MAX_FORMAT, )

  DECLARE_ENUM(Type, type, LOAD_DATA_FORMAT_DEF, static);
};

class ObLoadDataStorageInfo : public share::ObBackupStorageInfo
{
public:
  using common::ObObjectStorageInfo::set;

public:
  ObLoadDataStorageInfo();
  virtual ~ObLoadDataStorageInfo();

  virtual int assign(const ObLoadDataStorageInfo &storage_info);
  bool is_valid() const override;
  void reset() override;

#define DEFINE_GETTER_AND_SETTER(type, name)            \
  OB_INLINE type get_##name() const { return name##_; } \
  OB_INLINE void set_##name(type name) { name##_ = name; }

  DEFINE_GETTER_AND_SETTER(ObLoadDataFormat::Type, load_data_format);

#undef DEFINE_GETTER_AND_SETTER

  int set(const common::ObStorageType device_type, const char *storage_info) override;
  int get_storage_info_str(char *storage_info, const int64_t info_len) const override;

  INHERIT_TO_STRING_KV("ObObjectStorageInfo", ObObjectStorageInfo, "load_data_format",
                       ObLoadDataFormat::get_type_string(load_data_format_));

private:
  int parse_load_data_params(const char *storage_info);
  int set_load_data_param_defaults();

private:
  ObLoadDataFormat::Type load_data_format_;
};

} // namespace sql
} // namespace oceanbase
