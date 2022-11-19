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
                        common::ObString &host_name);
  // @brief append privileges info without considering grant_option
  int print_privs_to_buff(char *buf, const int64_t buf_len, int64_t &pos,
                          share::schema::ObPrivLevel priv_level, const ObPrivSet priv_set);
  int priv_level_printf(char *buf, const int64_t buf_len, int64_t &pos,
                        share::schema::ObNeedPriv &have_priv);
  // @brief append grant option info
  int grant_priv_to_buff(char *buf, const int64_t buf_len, int64_t &pos, const ObPrivSet priv_set);

  int calc_show_user_id(uint64_t &show_user_id);
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
