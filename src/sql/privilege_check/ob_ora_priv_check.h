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

#ifndef OCEANBASE_SQL_PRIVILEGE_CHECK_OB_ORA_PRIV_CHECK_
#define OCEANBASE_SQL_PRIVILEGE_CHECK_OB_ORA_PRIV_CHECK_

#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_stmt_type.h"
namespace oceanbase {
namespace sql {

class ObOraSysChecker {

private: 
  static int map_obj_priv_array_to_sys_priv_array(
      const ObIArray<share::ObRawObjPriv> &raw_obj_priv_array,
      const uint64_t obj_id,
      const uint64_t obj_type,
      ObIArray<share::ObRawPriv> &sys_priv_array);

  static int map_obj_priv_to_sys_priv(
      const share::ObRawObjPriv raw_obj_priv,
      const uint64_t obj_id,
      const uint64_t obj_type,
      share::ObRawPriv& sys_priv);

  static int check_single_option_obj_priv_inner(
      share::ObPackedObjPriv &ur_privs,
      const uint64_t option,
      share::ObRawObjPriv raw_priv);

  static int check_single_option_obj_priv_inner(
      share::ObPackedObjPriv &ur_privs,
      share::ObRawObjPriv raw_priv);

  static int check_multi_option_obj_privs_inner(
      share::ObPackedObjPriv &ur_privs,
      const share::ObRawObjPrivArray &table_privs);

  static int get_roles_obj_privs(
      share::schema::ObSchemaGetterGuard &guard,
      const share::schema::ObObjPrivSortKey &obj_key, 
      share::ObPackedObjPriv &obj_privs,
      const bool fetch_public_role_flag,
      const ObIArray<uint64_t> &role_id_array);

  static int get_user_sys_priv_in_roles(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id, 
      share::ObPackedPrivArray &packed_array_out,
      const bool fetch_public_role_flag,
      const ObIArray<uint64_t> &role_id_array);

  static int check_p1_or_cond_p2_in_single(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t ur_id,
      const share::ObRawPriv p1,
      bool is_owner,
      const share::ObRawPriv p2);
      
  static int check_p1_or_cond_p2_in_roles(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t ur_id,
      const share::ObRawPriv p1,
      bool is_owner,
      const share::ObRawPriv p2,
      const ObIArray<uint64_t> &role_id_array);

  static int check_p1_or_owner_and_p2(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      const share::ObRawPriv p1,
      const share::ObRawPriv p2,
      const ObIArray<uint64_t> &role_id_array);

  static int check_owner_or_p1_or_objp2(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      const uint64_t obj_id,
      const uint64_t obj_type,
      const share::ObRawPriv p1,
      const share::ObRawObjPriv obj_p2,
      const ObIArray<uint64_t> &role_id_array);

  static int check_owner_or_p1(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      const share::ObRawPriv p1,
      const ObIArray<uint64_t> &role_id_array);

  static int check_single_sys_priv_inner(
      const share::ObPackedPrivArray& sys_packed_array,
      const share::ObRawPriv p1);

  static int check_single_sys_priv_inner(
      const share::schema::ObSysPriv *sys_priv,
      const share::ObRawPriv p1);

  static int check_p1_in_single(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t ur_id,
      const share::ObRawPriv p1);

  static int check_p1_in_roles(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t ur_id,
      const share::ObRawPriv p1,
      const ObIArray<uint64_t> &role_id_array);

  static int check_p1(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t ur_id,
      const share::ObRawPriv p1,
      const ObIArray<uint64_t> &role_id_array);

  static int check_obj_plist_or(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t obj_type,
      const uint64_t obj_id,
      const uint64_t col_id,
      const share::ObRawObjPrivArray &plist,
      const ObIArray<uint64_t> &role_id_array);

  static int check_obj_p1(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t obj_type,
      const uint64_t obj_id,
      const uint64_t col_id,
      const share::ObRawObjPriv p1,
      const uint64_t option,
      const ObIArray<uint64_t> &role_id_array);

  static int check_objplist_in_single(
      share::schema::ObSchemaGetterGuard &guard,
      share::schema::ObObjPrivSortKey &obj_key,
      const share::ObRawObjPrivArray &table_privs,
      const ObIArray<uint64_t> &ins_col_id,
      const ObIArray<uint64_t> &upd_col_id,
      const ObIArray<uint64_t> &ref_col_id);

  static int check_objplist_in_roles(
      share::schema::ObSchemaGetterGuard &guard,
      share::schema::ObObjPrivSortKey &obj_key,
      const share::ObRawObjPrivArray &table_privs,
      const ObIArray<uint64_t> &ins_col_id,
      const ObIArray<uint64_t> &upd_col_id,
      const ObIArray<uint64_t> &ref_col_id,
      const ObIArray<uint64_t> &role_id_array);

  static int check_objplist(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t grantor_id,
      const uint64_t obj_id,
      const uint64_t obj_type,
      const share::ObRawObjPrivArray &table_privs,
      const ObIArray<uint64_t> &ins_col_id,
      const ObIArray<uint64_t> &upd_col_id,
      const ObIArray<uint64_t> &ref_col_id,
      const ObIArray<uint64_t> &role_id_array);

  static int check_p1_or_plist_using_privs(
      const share::ObPackedPrivArray &priv_arary,
      const share::ObRawPriv p1,
      const uint64_t option,
      const share::ObRawPrivArray &plist);

  static int check_p1_or_plist_in_single(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPriv p1,
      const uint64_t option,
      const share::ObRawPrivArray &plist);
      
  static int check_p1_or_plist_in_roles(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPriv p1,
      const uint64_t option,
      const share::ObRawPrivArray &plist,
      const ObIArray<uint64_t> &role_id_array);
      
  static int check_owner_or_p1_or_access(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const ObString &database_name,
      const share::ObRawPriv p1,
      const uint64_t obj_type,
      const uint64_t obj_id,
      const ObIArray<uint64_t> &role_id_array);


  static int build_related_obj_priv_array(
      const share::ObRawPriv p1,
      const uint64_t obj_type,
      share::ObRawObjPrivArray &obj_priv_array);

  static int build_related_sys_priv_array(
      const share::ObRawPriv p1,
      share::ObRawPrivArray &sys_priv_array);

  static int check_p1_or_plist(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPriv p1,
      const uint64_t option,
      const share::ObRawPrivArray &plist,
      const ObIArray<uint64_t> &role_id_array);
  
  static int check_obj_p1_in_single(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t obj_type,
      const uint64_t obj_id,
      const uint64_t col_id,
      const share::ObRawObjPriv p1,
      const uint64_t option);

  static int check_obj_p1_in_roles(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t obj_type,
      const uint64_t obj_id,
      const uint64_t col_id,
      const share::ObRawObjPriv p1,
      const uint64_t option,
      const ObIArray<uint64_t> &role_id_array);

  static int check_plist_using_privs(
      const share::ObPackedPrivArray &priv_array,
      const uint64_t option,
      const share::ObRawPrivArray &plist);

  static int check_plist_and_in_roles(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPrivArray &plist,
      const ObIArray<uint64_t> &role_id_array,
      const uint64_t option = NO_OPTION);

  static int check_plist_or_in_roles(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPrivArray &plist,
      const ObIArray<uint64_t> &role_id_array,
      const uint64_t option = NO_OPTION);

  static int check_plist_or_in_single(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPrivArray &plist,
      const uint64_t option = NO_OPTION);

  static int check_plist_and_in_single(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPrivArray &plist,
      const uint64_t option = NO_OPTION);

  static int check_plist_and(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPrivArray &plist,
      const ObIArray<uint64_t> &role_id_array,
      const uint64_t option = NO_OPTION);

  static int check_plist_or(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPrivArray &plist,
      const ObIArray<uint64_t> &role_id_array,
      const uint64_t option = NO_OPTION);

  static int check_obj_plist_or_in_roles(
    share::schema::ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t user_id,
    const uint64_t obj_type,
    const uint64_t obj_id,
    const uint64_t col_id,
    const share::ObRawObjPrivArray &plist,
    const ObIArray<uint64_t> &role_id_array);

  static int check_obj_plist_or_in_single(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t obj_type,
      const uint64_t obj_id,
      const uint64_t col_id,
      const share::ObRawObjPrivArray &plist);

  static int check_ora_obj_priv_for_create_view(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      const uint64_t obj_id,
      const uint64_t col_id,
      const uint64_t obj_type,
      const uint64_t obj_owner_id);

  static int check_p1_with_plist_info(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPriv p1,
      const uint64_t option,
      share::ObRawPrivArray &plist,
      bool &has_other_priv,
      const ObIArray<uint64_t> &role_id_array);

  static int check_p1_with_plist_info_in_single(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPriv p1,
      const uint64_t option,
      share::ObRawPrivArray &plist,
      bool &has_other_priv);

  static int check_p1_with_plist_info_using_privs(
      const share::ObPackedPrivArray &priv_array,
      const share::ObRawPriv p1,
      const uint64_t option,
      share::ObRawPrivArray &plist,
      bool &has_other_priv);

  static int check_p1_with_plist_info_in_roles(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPriv p1,
      const uint64_t option,
      share::ObRawPrivArray &plist,
      bool &has_other_priv,
      const ObIArray<uint64_t> &role_id_array);

  static int build_related_plist(
      uint64_t obj_type,
      share::ObRawPriv& sys_priv,
      share::ObRawPrivArray& plist);
public:
  static bool is_super_user(
      const uint64_t user_id);

  static bool is_owner_user(
      const uint64_t user_id,
      const uint64_t owner_user_id);
      
  static int check_ora_grant_role_priv(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const ObSEArray<uint64_t, 4> &role_granted_id_array,
      const ObIArray<uint64_t> &role_id_array);

  static int check_ora_grant_sys_priv(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPrivArray &sys_priv_array,
      const ObIArray<uint64_t> &role_id_array);
      
  static int check_ora_grant_obj_priv(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      const uint64_t obj_id,
      const uint64_t obj_type,
      const share::ObRawObjPrivArray &table_priv_array,
      const ObIArray<uint64_t> &ins_col_ids,
      const ObIArray<uint64_t> &upd_col_ids,
      const ObIArray<uint64_t> &ref_col_ids,
      uint64_t &grantor_id_out,
      const ObIArray<uint64_t> &role_id_array);

  static int decide_grantor_id(
      share::schema::ObSchemaGetterGuard &guard,
      bool hash_sys_priv,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      uint64_t &grantor_id_out);

  static int check_ora_connect_priv(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const ObIArray<uint64_t> &role_id_array);

  static int check_ora_ddl_priv(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      const sql::stmt::StmtType stmt_type,
      const ObIArray<uint64_t> &role_id_array);

  static int check_ora_ddl_priv(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      const uint64_t obj_id,
      const uint64_t obj_type,
      stmt::StmtType stmt_type,
      const ObIArray<uint64_t> &role_id_array);

  static int check_ora_ddl_ref_priv(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      const uint64_t obj_id,
      const ObIArray<uint64_t> &col_ids,
      const uint64_t obj_type,
      stmt::StmtType stmt_type,
      const ObIArray<uint64_t> &role_id_array);

  static int check_ora_obj_privs_or(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_db_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      const uint64_t obj_id,
      const uint64_t col_id,
      const uint64_t obj_type,
      const share::ObRawObjPrivArray &raw_obj_priv_array,
      uint64_t check_flag,
      const uint64_t obj_owner_id,
      const ObIArray<uint64_t> &role_id_array);

  static int check_ora_obj_priv(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &database_name,
      const uint64_t obj_id,
      const uint64_t col_id,
      const uint64_t obj_type,
      const share::ObRawObjPriv raw_obj_priv,
      uint64_t check_flag,
      const uint64_t obj_owner_id,
      const ObIArray<uint64_t> &role_id_array);

  static int check_ora_show_process_priv(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const ObIArray<uint64_t> &role_id_array);

  static int check_access_to_obj(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const share::ObRawPriv p1,
      const uint64_t obj_type,
      const uint64_t obj_id,
      const common::ObString &database_name,
      const ObIArray<uint64_t> &role_id_array,
      bool &exists);

  static int check_access_to_mlog_base_table(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t obj_id,
      const common::ObString &database_name,
      const ObIArray<uint64_t> &role_id_array,
      bool &accessible);

  static int check_access_to_obj(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t obj_type,
      const uint64_t obj_id,
      const common::ObString &database_name,
      const ObIArray<uint64_t> &role_id_array,
      bool &accessible);

  static int build_related_sys_priv_array(
      const uint64_t obj_type,
      share::ObRawPrivArray &sys_priv_array);

  static int build_related_obj_priv_array(
      const uint64_t obj_type,
      share::ObRawObjPrivArray &obj_priv_array);

    static int check_obj_and_col_priv(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t obj_id,
      const uint64_t obj_type,
      const share::ObRawObjPriv p1,
      const ObIArray<uint64_t> &col_ids,
      const ObIArray<uint64_t> &role_id_array);

  static int check_ora_user_sys_priv(
    share::schema::ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t user_id,
    const ObString &database_name,
    const share::ObRawPriv& priv_id,
    const ObIArray<uint64_t> &role_id_array);
};  

}
}

#endif // OCEANBASE_SQL_PRIVILEGE_CHECK_OB_PRIVILEGE_CHECK
