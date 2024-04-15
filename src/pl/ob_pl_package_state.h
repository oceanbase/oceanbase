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

#ifndef SRC_PL_OB_PL_PACKAGE_STATE_H_
#define SRC_PL_OB_PL_PACKAGE_STATE_H_

#include <cstdint>
#include "lib/container/ob_iarray.h"
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_bit_set.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "ob_pl_type.h"
#include "ob_pl_allocator.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace pl
{
struct ObPLExecCtx;
class ObPLResolveCtx;
class ObPLPackage;
class ObPLPkgAllocator;

enum PackageVarType
{
  INVALID = -1,
  CONST,
  VARIABLE,
  CURSOR
};

struct ObPackageStateVersion
{
  OB_UNIS_VERSION(1);

public:
  int64_t package_version_;
  int64_t package_body_version_;

  ObPackageStateVersion(int64_t package_version, int64_t package_body_version)
      : package_version_(package_version),
        package_body_version_(package_body_version) {}
  virtual ~ObPackageStateVersion() {
    package_version_ = common::OB_INVALID_VERSION;
    package_body_version_ = common::OB_INVALID_VERSION;
  }
  ObPackageStateVersion(const ObPackageStateVersion &other);
  bool is_valid() const { return package_version_ == common::OB_INVALID_VERSION?false:true; }
  ObPackageStateVersion &operator =(const ObPackageStateVersion &other);
  bool operator ==(const ObPackageStateVersion &other);
  int64_t get_package_version()
  {
    return is_valid() ?
              package_body_version_ != OB_INVALID_VERSION ?
                package_version_ > package_body_version_ ? package_version_ : package_body_version_
                : package_version_
              : OB_INVALID_ID;
  }
  TO_STRING_KV(K(package_version_), K(package_body_version_));
};

struct ObPackageVarSetName
{
  OB_UNIS_VERSION(1);

public:
  uint64_t package_id_;
  ObPackageStateVersion state_version_;
  PackageVarType var_type_;
  int64_t var_idx_;

  ObPackageVarSetName()
      : package_id_(common::OB_INVALID_ID),
        state_version_(common::OB_INVALID_VERSION, common::OB_INVALID_VERSION),
        var_type_(INVALID),
        var_idx_(common::OB_INVALID_INDEX) {}
  virtual ~ObPackageVarSetName() {}

  bool valid()
  {
    return package_id_ != OB_INVALID_ID
           && state_version_.is_valid()
           && var_type_ != INVALID
           && var_idx_ != OB_INVALID_INDEX;
  }

  int encode(common::ObIAllocator &alloc, common::ObString &var_name_str);
  int decode(common::ObIAllocator &alloc, const common::ObString &var_name_str);

  TO_STRING_KV(K(package_id_), K(state_version_), K(var_type_), K(var_idx_));
};

class ObPLPackageState
{
public:
  ObPLPackageState(uint64_t package_id,
                   const ObPackageStateVersion &state_version,
                   bool serially_reusable)
      : inner_allocator_(this),
        cursor_allocator_("PlPkgCursor", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        package_id_(package_id),
        state_version_(state_version),
        serially_reusable_(serially_reusable),
        changed_vars_(),
        types_(),
        vars_() {}
  virtual ~ObPLPackageState()
  {
    package_id_ = common::OB_INVALID_ID;
    changed_vars_.reset();
    types_.reset();
    vars_.reset();
    inner_allocator_.reset();
    cursor_allocator_.reset();
  }
  void reset(sql::ObSQLSessionInfo *session_info);
  common::ObIAllocator &get_pkg_allocator() { return inner_allocator_; }
  common::ObIAllocator &get_pkg_cursor_allocator() { return cursor_allocator_; }
  int add_package_var_val(const common::ObObj &value, ObPLType type);
  int set_package_var_val(int64_t var_idx, const common::ObObj &value, bool deep_copy_complex = true);
  int get_package_var_val(int64_t var_idx, common::ObObj &value);
  int update_changed_vars(int64_t var_idx);
  inline bool is_package_info_changed()
  {
    return (!changed_vars_.is_empty() && !serially_reusable_) ? true : false;
  }
  inline void reset_package_changed_info()
  {
    changed_vars_.reset();
  }
  inline bool check_version(const ObPackageStateVersion &state_version)
  {
    return state_version_ == state_version ? true : false;
  }
  int convert_changed_info_to_string_kvs(ObPLExecCtx &pl_ctx,
                                         common::ObIArray<common::ObString> &key,
                                         common::ObIArray<common::ObObj> &value);
  int make_pkg_var_kv_key(common::ObIAllocator &alloc,
                          int64_t var_idx,
                          PackageVarType var_type,
                          common::ObString &key);
  int make_pkg_var_kv_value(ObPLExecCtx &pl_ctx,
                          common::ObObj &var_val,
                          int64_t var_idx,
                          common::ObObj &value);
  int convert_info_to_string_kv(ObPLExecCtx &pl_ctx,
                                int64_t var_idx,
                                PackageVarType var_type,
                                common::ObString &key,
                                common::ObObj &value);
  inline bool get_serially_reusable() const { return serially_reusable_; }
  int remove_user_variables_for_package_state(sql::ObSQLSessionInfo &session);
  int check_package_state_valid(sql::ObExecContext &exec_ctx, bool &valid);
  uint64_t get_package_id() { return package_id_; }

  ObIArray<ObObj> &get_vars() { return vars_; }

  int shrink() { return inner_allocator_.shrink(); }

  TO_STRING_KV(K(package_id_), K(serially_reusable_), K(state_version_));

private:
  DISALLOW_COPY_AND_ASSIGN(ObPLPackageState);

  ObPLPkgAllocator inner_allocator_;
  ObArenaAllocator cursor_allocator_;
  uint64_t package_id_;
  ObPackageStateVersion state_version_;
  bool serially_reusable_;
  common::ObBitSet<> changed_vars_;
  common::ObSEArray<ObPLType, 64> types_;
  common::ObSEArray<ObObj, 64> vars_;
};
} //end namespace pl
} //end namespace oceanbase
#endif /* SRC_PL_OB_PL_PACKAGE_STATE_H_ */
