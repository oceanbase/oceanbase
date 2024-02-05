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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_GRANTS_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_GRANTS_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/schema/ob_priv_type.h"
#include "share/schema/ob_schema_struct.h"
#include "common/ob_range.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObShowGrants : public common::ObVirtualTableScannerIterator
{
public:
  struct PrivKey {

    PrivKey() : obj_type_(share::schema::ObObjectType::INVALID) {}
    ObString db_name_;
    ObString table_name_;
    ObString column_name_;
    share::schema::ObObjectType obj_type_;
    uint64_t hash() const {
      uint64_t hash_val = 0;
      hash_val = db_name_.hash(hash_val);
      hash_val = table_name_.hash(hash_val);
      hash_val = column_name_.hash(hash_val);
      hash_val = murmurhash(&obj_type_, sizeof(obj_type_), hash_val);
      return hash_val;
    }
    static bool cmp(const std::pair<PrivKey, ObPrivSet> &l, const std::pair<PrivKey, ObPrivSet> &r) {
      bool ret = false;
      const PrivKey &left = l.first;
      const PrivKey &right = r.first;
      if (left.db_name_ == right.db_name_) {
        if (left.table_name_ == right.table_name_) {
          if (left.column_name_ == right.column_name_) {
            ret = left.obj_type_ < right.obj_type_;
          } else if (left.column_name_ < right.column_name_) {
            ret = true;
          } else {
            ret = false;
          }
        } else if (left.table_name_ < right.table_name_) {
          ret = true;
        } else {
          ret = false;
        }
      } else if (left.db_name_ < right.db_name_) {
        ret = true;
      } else {
        ret = false;
      }
      return ret;
    }
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    bool operator==(const PrivKey &other) const {
      return db_name_ == other.db_name_
          && table_name_ == other.table_name_
          && column_name_ == other.column_name_
          && obj_type_ == other.obj_type_;
    }

    TO_STRING_KV(K_(db_name), K_(table_name), K_(column_name));
  };
  typedef hash::ObHashMap<PrivKey, ObPrivSet, hash::NoPthreadDefendMode> PRIV_MAP;
public:
  ObShowGrants();
  virtual ~ObShowGrants();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_user_id(uint64_t user_id) { user_id_ = user_id; }
  inline void set_session_priv(share::schema::ObSessionPrivInfo session_priv)
  { session_priv_ = session_priv; }
  inline share::schema::ObSessionPrivInfo &get_session_priv()
  { return session_priv_; }
  inline const share::schema::ObSessionPrivInfo &get_session_priv() const
  { return session_priv_; }
  static int print_obj_privs_to_buff_ora(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    const share::ObPackedObjPriv obj_privs);

private:
  // @brief get string 'grant priv on priv_level to user'
  int get_grants_string(char *buf, const int64_t buf_len, int64_t &pos,
                        share::schema::ObNeedPriv &have_priv, common::ObString &user_name,
                        common::ObString &host_name,
                        ObIArray<std::pair<PrivKey, ObPrivSet>> *priv_key_array = NULL);

  int print_column_privs_to_buff(char *buf,
                                  const int64_t buf_len,
                                  int64_t &pos,
                                  ObIArray<std::pair<PrivKey, ObPrivSet>> &priv_key_array,
                                  const ObPrivType priv_type);
  // @brief append privileges info without considering grant_option
  int print_privs_to_buff(char *buf, const int64_t buf_len, int64_t &pos,
                          share::schema::ObPrivLevel priv_level, const ObPrivSet priv_set,
                          ObIArray<std::pair<PrivKey, ObPrivSet>> *priv_key_array = NULL);
  int priv_level_printf(char *buf, const int64_t buf_len, int64_t &pos,
                        share::schema::ObNeedPriv &have_priv);
  // @brief append grant option info
  int grant_priv_to_buff(char *buf, const int64_t buf_len, int64_t &pos, const ObPrivSet priv_set);

  int calc_show_user_id(uint64_t &show_user_id, common::ObIArray<uint64_t> &role_ids);
  int fill_row_cells(uint64_t show_user_id, const common::ObString &grants_str);

  int has_show_grants_priv(uint64_t show_user_id) const;
  int get_grants_string_ora(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      share::schema::ObOraNeedPriv &have_priv,
      ObString &db_name,
      ObString &obj_name,
      ObString &col_name,
      ObString &user_name,
      ObString &host_name);

  int priv_obj_info_ora(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObString &db_name,
      ObString &obj_name,
      ObString &col_name);

  int grant_role_to_buff(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      const ObUserInfo &user_info,
      bool with_admin_option);

  int add_priv_map_recursively(uint64_t user_id, PRIV_MAP &priv_map, bool expand_roles);
  int add_priv_map(PRIV_MAP &priv_map, PrivKey &priv_key, ObPrivSet added_priv);

private:
  uint64_t tenant_id_;
  uint64_t user_id_;
  share::schema::ObSessionPrivInfo session_priv_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObShowGrants);
};
}
}
#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_GRANTS_
