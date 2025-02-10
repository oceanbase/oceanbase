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
#include "ob_pl_package_encode_info.h"

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
class ObPLAllocator1;

enum PackageVarType
{
  INVALID = -1,
  CONST,
  VARIABLE,
  CURSOR
};

static const char *package_key_prefix_v1 = "pkg.";
static const char *package_key_prefix_v2 = "pkg.v2.";


struct ObPackageStateVersion
{
  OB_UNIS_VERSION(1);

public:
  int64_t package_version_;
  int64_t package_body_version_;
  int64_t header_merge_version_;
  int64_t body_merge_version_;
  int64_t header_public_syn_count_; // use to resolve object shadow same name public synonym issue
  int64_t body_public_syn_count_;

  ObPackageStateVersion(int64_t package_version, int64_t package_body_version)
      : package_version_(package_version),
        package_body_version_(package_body_version),
        header_merge_version_(common::OB_INVALID_VERSION),
        body_merge_version_(common::OB_INVALID_VERSION),
        header_public_syn_count_(0),
        body_public_syn_count_(0) {}
  virtual ~ObPackageStateVersion() {
    package_version_ = common::OB_INVALID_VERSION;
    package_body_version_ = common::OB_INVALID_VERSION;
    header_merge_version_ = common::OB_INVALID_VERSION;
    body_merge_version_ = common::OB_INVALID_VERSION;
    header_public_syn_count_ = 0;
    body_public_syn_count_ = 0;
  }
  void set_merge_version_and_public_syn_cnt(const ObPLPackage &head, const ObPLPackage *body);
  ObPackageStateVersion(const ObPackageStateVersion &other);
  bool is_valid() const { return package_version_ == common::OB_INVALID_VERSION?false:true; }
  ObPackageStateVersion &operator =(const ObPackageStateVersion &other);
  bool equal(const ObPackageStateVersion &other);
  bool operator ==(const ObPackageStateVersion &other) const;
  int64_t get_version_serialize_size();
  int encode(char *dst, const int64_t dst_len, int64_t &dst_pos);
  int decode(const char *src, const int64_t src_len, int64_t &src_pos);

  TO_STRING_KV(K(package_version_), K(package_body_version_),
               K(header_merge_version_), K(body_merge_version_),
               K(header_public_syn_count_), K(body_public_syn_count_));
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

  bool valid(bool new_encode_rule)
  {
    bool is_valid = package_id_ != OB_INVALID_ID
           && state_version_.is_valid();
    if (!new_encode_rule) {
      is_valid = is_valid && var_type_ != INVALID
                          && var_idx_ != OB_INVALID_INDEX;
    }
    return is_valid;
  }

  int encode(common::ObIAllocator &alloc, common::ObString &var_name_str);
  int decode(common::ObIAllocator &alloc, const common::ObString &var_name_str);

  int encode_key(common::ObIAllocator &alloc, common::ObString &key_str);
  int decode_key(common::ObIAllocator &alloc, const common::ObString &key_str);

  TO_STRING_KV(K(package_id_), K(state_version_), K(var_type_), K(var_idx_));
};

class ObPLPackageState
{
public:
  ObPLPackageState(uint64_t package_id,
                   const ObPackageStateVersion &state_version,
                   bool serially_reusable)
      : parent_alloc_(SET_IGNORE_MEM_VERSION(lib::ObMemAttr(MTL_ID(), "PLPkgSymbol")), OB_MALLOC_NORMAL_BLOCK_SIZE),
        inner_allocator_(PL_MOD_IDX::OB_PL_PACKAGE_SYMBOL, &parent_alloc_),
        cursor_allocator_(SET_IGNORE_MEM_VERSION(lib::ObMemAttr(MTL_ID(), "PLPkgCursor")), OB_MALLOC_NORMAL_BLOCK_SIZE),
        package_id_(package_id),
        state_version_(state_version),
        serially_reusable_(serially_reusable),
        changed_vars_(),
        types_(),
        vars_(),
        has_instantiated_(false) {
        types_.set_attr(SET_IGNORE_MEM_VERSION(lib::ObMemAttr(MTL_ID(), "PLPkgTypes")));
        vars_.set_attr(SET_IGNORE_MEM_VERSION(lib::ObMemAttr(MTL_ID(), "PLPkgVars")));
        }
  virtual ~ObPLPackageState()
  {
    package_id_ = common::OB_INVALID_ID;
    changed_vars_.reset();
    types_.reset();
    vars_.reset();
    inner_allocator_.reset();
    cursor_allocator_.reset();
    has_instantiated_ = false;
  }
  int init();
  void reset(sql::ObSQLSessionInfo *session_info);
  common::ObIAllocator &get_pkg_allocator() { return inner_allocator_; }
  common::ObIAllocator &get_pkg_cursor_allocator() { return cursor_allocator_; }
  int add_package_var_val(const common::ObObj &value, ObPLType type);
  int set_package_var_val(int64_t var_idx, const common::ObObj &value,
                          const ObPLResolveCtx &resolve_ctx, bool deep_copy_complex = true);
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
    return state_version_.equal(state_version);
  }

  static int check_version(const ObPackageStateVersion &state_version,
                            const ObPackageStateVersion &cur_state_version,
                            ObSchemaGetterGuard &schema_guard,
                            const ObPLPackage &spec,
                            const ObPLPackage *body,
                            bool &match);

  int convert_changed_info_to_string_kvs(ObPLExecCtx &pl_ctx,
                                         common::ObIArray<common::ObString> &key,
                                         common::ObIArray<common::ObObj> &value);
  int make_pkg_var_kv_key(common::ObIAllocator &alloc,
                          int64_t var_idx,
                          PackageVarType var_type,
                          common::ObString &key);
  int make_pkg_var_kv_value(ObPLExecCtx &pl_ctx,
                          ObPLResolveCtx &resolve_ctx,
                          common::ObObj &var_val,
                          int64_t var_idx,
                          common::ObObj &value);
  int encode_pkg_var_key(ObIAllocator &alloc, ObString &key);
  int encode_pkg_var_value(ObPLExecCtx &pl_ctx,
                           ObPLResolveCtx &resolve_ctx,
                           common::ObString &key,
                           common::ObObj &value,
                           ObIArray<ObString> &old_keys);
  int convert_info_to_string_kv(ObPLExecCtx &pl_ctx,
                                ObPLResolveCtx &resolve_ctx,
                                int64_t var_idx,
                                PackageVarType var_type,
                                common::ObString &key,
                                common::ObObj &value);
  int encode_info_to_string_kvs(ObPLExecCtx &pl_ctx,
                                ObPLResolveCtx &resolve_ctx,
                                common::ObString &key,
                                common::ObObj &value,
                                ObIArray<ObString> &old_keys);
  static int decode_pkg_var_value(const common::ObObj &serialize_value,
                                  ObPackageStateVersion &state_version,
                                  hash::ObHashMap<int64_t, ObPackageVarEncodeInfo> &value_map);
  inline bool get_serially_reusable() const { return serially_reusable_; }
  int remove_user_variables_for_package_state(sql::ObSQLSessionInfo &session);
  int check_package_state_valid(sql::ObExecContext &exec_ctx, ObPLResolveCtx &resolve_ctx, bool &valid);
  uint64_t get_package_id() { return package_id_; }

  ObIArray<ObObj> &get_vars() { return vars_; }

  ObPackageStateVersion& get_state_version() { return state_version_; }
  static int disable_expired_user_variables(sql::ObSQLSessionInfo &session, const ObString &key);
  static int need_use_new_sync_policy(int64_t tenant_id, bool &use_new);
  static int get_invalid_value(ObObj &val);
  static int is_invalid_value(const ObObj &val, bool &is_invalid);
  static int get_oversize_value(ObObj &val);
  static int is_oversize_value(const ObObj &val, bool &is_invalid);

  void set_has_instantiated(bool init) { has_instantiated_ = true; }
  bool has_instantiated() const { return has_instantiated_; }

  TO_STRING_KV(K(package_id_), K(serially_reusable_), K(state_version_));

private:
  DISALLOW_COPY_AND_ASSIGN(ObPLPackageState);
  ObArenaAllocator parent_alloc_;
  ObPLAllocator1 inner_allocator_;
  ObArenaAllocator cursor_allocator_;
  uint64_t package_id_;
  ObPackageStateVersion state_version_;
  bool serially_reusable_;
  common::ObBitSet<> changed_vars_;
  common::ObSEArray<ObPLType, 64> types_;
  common::ObSEArray<ObObj, 64> vars_;
  bool has_instantiated_;
};
} //end namespace pl
} //end namespace oceanbase
#endif /* SRC_PL_OB_PL_PACKAGE_STATE_H_ */
