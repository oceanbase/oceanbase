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
 * obj2str helper
 */

#ifndef OCEANBASE_OBJ2STR_HELPER_H__
#define OCEANBASE_OBJ2STR_HELPER_H__

#include "common/object/ob_object.h"        // ObObj
#include "lib/allocator/ob_allocator.h"     // ObIAllocator
#include "lib/string/ob_string.h"           // ObString
#include "lib/worker.h"                     // Worker
#include "common/object/ob_obj_type.h"      // ObObjTypeClass
#include "share/object/ob_obj_cast.h"       // ObObjCastParams, ObObjCaster
#include "ob_log_hbase_mode.h"              // ObLogHbaseUtil
#include "ob_log_tenant_mgr.h"              // ObLogTenantMgr

namespace oceanbase
{
namespace common
{
class ObTimeZoneInfo;
}
namespace libobcdc
{

class IObCDCTimeZoneInfoGetter;
class ObObj2strHelper
{
public:
  ObObj2strHelper();
  virtual ~ObObj2strHelper();

public:
  // Converting objects to strings
  // NOTE:
  // 1. If the object is of string type ObStringTC (including: varchar, char, varbinary, binary)
  //  1) string_deep_copy == false
  //    the string points directly to the content of the original object
  //  2) string_deep_copy == true
  //    deep copy of the string
  // 2. otherwise use allocator to allocate memory and print the object into memory
   int obj2str(const uint64_t tenant_id,
       const uint64_t table_id,
       const uint64_t column_id,
       const common::ObObj &obj,
       common::ObString &str,
       common::ObIAllocator &allocator,
       const bool string_deep_copy,
       const common::ObIArray<common::ObString> &extended_type_info,
       const common::ObAccuracy &accuracy,
       const common::ObCollationType &collation_type,
       const ObTimeZoneInfoWrap *tz_info_wrap) const;

public:
  int init(IObCDCTimeZoneInfoGetter &timezone_info_getter,
      ObLogHbaseUtil &hbase_util,
      const bool enable_hbase_mode,
      const bool enable_convert_timestamp_to_unix_timestamp,
      const bool enable_backup_mode,
      IObLogTenantMgr &tenant_mgr);
  void destroy();

public:
  static const char *EMPTY_STRING;

private:
  // initialize ObCharsetUtils (refer to ob_sql_init.h #init_sql_expr_static_var())
  // fix
  // enum,set was developed at the stage when ob only supported utf8, and did not handle enum,set types when supporting other character sets,
  // resulting in incorrect charset when converting enum,set to string. This can lead to garbled data and problems such as compare hang. (Corresponding to server-side modifications.
  //
  int init_ob_charset_utils();

  int convert_timestamp_with_timezone_data_util_succ_(const common::ObObjType &target_type,
      common::ObObjCastParams &cast_param,
      const common::ObObj &in_obj,
      common::ObObj &str_obj,
      common::ObString &str,
      const uint64_t tenant_id) const;

  // max length of uint64_t
  static const int64_t MAX_BIT_DECIMAL_STR_LENGTH = 30;
  int convert_bit_obj_to_decimal_str_(const common::ObObj &obj,
      const common::ObObj &str_obj,
      common::ObString &str,
      common::ObIAllocator &allocator) const;
  int convert_hbase_bit_obj_to_positive_bit_str_(const common::ObObj &obj,
      const common::ObObj &str_obj,
      common::ObString &str,
      common::ObIAllocator &allocator) const;

  // max length of int64_t
  static const int64_t MAX_TIMESTAMP_UTC_LONG_STR_LENGTH = 30;
  int convert_mysql_timestamp_to_utc_(const common::ObObj &obj,
      common::ObString &str,
      common::ObIAllocator &allocator) const;

  // Oracle schema: char/nchar with automatic padding support
  // TODO MySQL schema: char/binary supports padding based on specific requirments
  bool need_padding_(const lib::Worker::CompatMode &compat_mode,
      const common::ObObj &obj) const;
  int convert_char_obj_to_padding_obj_(const lib::Worker::CompatMode &compat_mode,
      const common::ObObj &obj,
      const common::ObAccuracy &accuracy,
      const common::ObCollationType &collation_type,
      common::ObIAllocator &allocator,
      common::ObString &str) const;

  int convert_ob_geometry_to_ewkt_(const common::ObObj &obj,
      common::ObString &str,
      common::ObIAllocator &allocator) const;

  int convert_xmltype_to_text_(
      const common::ObObj &obj,
      common::ObString &str,
      common::ObIAllocator &allocator) const;

private:
  bool                          inited_;
  IObCDCTimeZoneInfoGetter      *timezone_info_getter_;
  ObLogHbaseUtil                *hbase_util_;
  bool                          enable_hbase_mode_;
  bool                          enable_convert_timestamp_to_unix_timestamp_;
  bool                          enable_backup_mode_;
  IObLogTenantMgr               *tenant_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObObj2strHelper);
};

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_OBJ2STR_HELPER_H__ */
