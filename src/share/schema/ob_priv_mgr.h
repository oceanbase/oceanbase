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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_PRIV_MGR_H_
#define OCEANBASE_SHARE_SCHEMA_OB_PRIV_MGR_H_

#include <stdint.h>
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

template<class T, class V>
struct ObGetTablePrivKeyV3
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};
template<>
struct ObGetTablePrivKeyV3<ObTablePrivSortKey, ObTablePriv *>
{
  ObTablePrivSortKey operator()(const ObTablePriv *table_priv) const
  {
    ObTablePrivSortKey key;
    return NULL != table_priv ?
      table_priv->get_sort_key() :
      key;
  }
};

template<class T, class V>
struct ObGetRoutinePrivKeyV3
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};


template<>
struct ObGetRoutinePrivKeyV3<ObRoutinePrivSortKey, ObRoutinePriv *>
{
  ObRoutinePrivSortKey operator()(const ObRoutinePriv *routine_priv) const
  {
    ObRoutinePrivSortKey key;
    return NULL != routine_priv ?
      routine_priv->get_sort_key() :
      key;
  }
};

template<class T, class V>
struct ObGetColumnPrivKeyV3
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetColumnPrivKeyV3<ObColumnPrivSortKey, ObColumnPriv *>
{
  ObColumnPrivSortKey operator()(const ObColumnPriv *column_priv) const
  {
    ObColumnPrivSortKey key;
    return NULL != column_priv ?
      column_priv->get_sort_key() :
      key;
  }
};

template<class T, class V>
struct ObGetObjPrivKey
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};
template<>
struct ObGetObjPrivKey<ObObjPrivSortKey, ObObjPriv *>
{
  ObObjPrivSortKey operator()(const ObObjPriv *obj_priv) const
  {
    ObObjPrivSortKey key;
    return NULL != obj_priv ?
      obj_priv->get_sort_key() :
      key;
  }
};
class ObPrivMgr
{
  typedef common::ObSortedVector<ObDBPriv *> DBPrivInfos;
  typedef common::ObSortedVector<ObTablePriv *> TablePrivInfos;
  typedef common::ObSortedVector<ObRoutinePriv *> RoutinePrivInfos;
  typedef common::ObSortedVector<ObColumnPriv *> ColumnPrivInfos;
  typedef common::ObSortedVector<ObObjPriv *>ObjPrivInfos;
  typedef common::ObSortedVector<ObSysPriv *>SysPrivInfos;
  typedef common::hash::ObPointerHashMap<ObTablePrivSortKey, ObTablePriv *, ObGetTablePrivKeyV3, 128> TablePrivMap;
  typedef common::hash::ObPointerHashMap<ObRoutinePrivSortKey, ObRoutinePriv *, ObGetRoutinePrivKeyV3, 128> RoutinePrivMap;
  typedef common::hash::ObPointerHashMap<ObColumnPrivSortKey, ObColumnPriv *, ObGetColumnPrivKeyV3, 128> ColumnPrivMap;
  typedef common::hash::ObPointerHashMap<ObObjPrivSortKey, ObObjPriv *, ObGetObjPrivKey, 128> ObjPrivMap;
  typedef DBPrivInfos::iterator DBPrivIter;
  typedef DBPrivInfos::const_iterator ConstDBPrivIter;
  typedef TablePrivInfos::iterator TablePrivIter;
  typedef TablePrivInfos::const_iterator ConstTablePrivIter;
  typedef RoutinePrivInfos::iterator RoutinePrivIter;
  typedef RoutinePrivInfos::const_iterator ConstRoutinePrivIter;

  typedef ColumnPrivInfos::iterator ColumnPrivIter;
  typedef ColumnPrivInfos::const_iterator ConstColumnPrivIter;
  typedef SysPrivInfos::iterator SysPrivIter;
  typedef SysPrivInfos::const_iterator ConstSysPrivIter;
  typedef ObjPrivInfos::iterator ObjPrivIter;
  typedef ObjPrivInfos::const_iterator ConstObjPrivIter;
public:
  ObPrivMgr();
  explicit ObPrivMgr(common::ObIAllocator &allocator);
  virtual ~ObPrivMgr();
  int init();
  void reset();
  ObPrivMgr &operator =(const ObPrivMgr &other);
  int assign(const ObPrivMgr &other);
  int deep_copy(const ObPrivMgr &other);
  void dump() const;

  // db priv
  int add_db_privs(const common::ObIArray<ObDBPriv> &db_privs);
  int del_db_privs(const common::ObIArray<ObOriginalDBKey> &db_priv_keys);
  int add_db_priv(const ObDBPriv &db_priv);
  int del_db_priv(const ObOriginalDBKey &db_priv_key);
  int get_db_priv(const ObOriginalDBKey &db_priv_key,
                  const ObDBPriv *&db_priv,
                  bool db_is_pattern = false) const;
  int get_db_priv_set(const ObOriginalDBKey &db_priv_key,
                      ObPrivSet &priv_set,
                      bool is_pattern = false) const;
  // table priv
  int add_table_privs(const common::ObIArray<ObTablePriv> &table_privs);
  int del_table_privs(const common::ObIArray<ObTablePrivSortKey> &table_priv_keys);
  int add_table_priv(const ObTablePriv &table_priv);
  int del_table_priv(const ObTablePrivSortKey &table_priv_key);

  int add_column_privs(const common::ObIArray<ObColumnPriv> &column_privs);
  int add_column_priv(const ObColumnPriv &column_priv);
  int del_column_privs(const common::ObIArray<ObColumnPrivIdKey> &column_priv_keys);
  int del_column_priv(const ObColumnPrivIdKey &column_priv_key);

  int get_table_priv(const ObTablePrivSortKey &table_priv_key,
                     const ObTablePriv *&table_priv) const;
  int get_table_priv_set(const ObTablePrivSortKey &table_priv_key,
                         ObPrivSet &priv_set) const;

  int add_routine_privs(const common::ObIArray<ObRoutinePriv> &routine_privs);
  int del_routine_privs(const common::ObIArray<ObRoutinePrivSortKey> &routine_priv_keys);
  int add_routine_priv(const ObRoutinePriv &routine_priv);
  int del_routine_priv(const ObRoutinePrivSortKey &routine_priv_key);
  int get_routine_priv(const ObRoutinePrivSortKey &routine_priv_key,
                      const ObRoutinePriv *&routine_priv) const;

  int get_routine_priv_set(const ObRoutinePrivSortKey &routine_priv_key,
                          ObPrivSet &priv_set) const;
  int get_column_priv_in_table(const uint64_t tenant_id,
                                 const uint64_t user_id,
                                 const ObString &db,
                                 const ObString &table,
                                 ObIArray<const ObColumnPriv *> &column_privs) const;

  int get_column_priv_by_id(const uint64_t tenant_id,
                            const uint64_t priv_id,
                            const ObColumnPriv *&column_priv) const;
  int get_column_priv_id(const uint64_t tenant_id,
                        const uint64_t user_id,
                        const ObString &db,
                        const ObString &table,
                        const ObString &column,
                        uint64_t &column_priv_id) const;
  int get_column_priv_in_db(const uint64_t tenant_id,
                                 const uint64_t user_id,
                                 const ObString &db,
                                 ObIArray<const ObColumnPriv *> &column_privs) const;
  int get_column_priv(const ObColumnPrivSortKey &column_priv_key,
                      const ObColumnPriv *&column_priv) const;
  int get_column_priv_set(const ObColumnPrivSortKey &column_priv_key,
                         ObPrivSet &priv_set) const;
  int table_grant_in_db(const uint64_t tenant_id,
                        const uint64_t user_id,
                        const common::ObString &db,
                        bool &is_grant) const;
  int routine_grant_in_db(const uint64_t tenant_id,
                          const uint64_t user_id,
                          const ObString &db,
                          bool &is_grant) const;
  //obj priv
  int add_obj_privs(const common::ObIArray<ObObjPriv> &obj_privs);
  int del_obj_privs(const common::ObIArray<ObObjPrivSortKey> &obj_priv_keys);
  int add_obj_priv(const ObObjPriv &obj_priv);
  int del_obj_priv(const ObObjPrivSortKey &obj_priv);
  int get_obj_priv(const ObObjPrivSortKey &obj_priv_key,
                   const ObObjPriv *&obj_priv) const;
  int get_obj_privs_in_ur_and_obj(
      const uint64_t tenant_id,
      const ObObjPrivSortKey &obj_key,
      common::ObIArray<const ObObjPriv *> &obj_privs) const;
  int get_obj_privs_in_ur_and_obj(
      const uint64_t tenant_id,
      const ObObjPrivSortKey &obj_key,
      ObPackedObjPriv &obj_privs) const;
  int get_obj_privs_in_grantor_ur_obj_id(
      const uint64_t tenant_id,
      const ObObjPrivSortKey &obj_key,
      common::ObIArray<const ObObjPriv *> &obj_privs) const;
  int get_obj_privs_in_grantor_obj_id(
      const uint64_t tenant_id,
      const ObObjPrivSortKey &obj_key,
      common::ObIArray<const ObObjPriv *> &obj_privs) const;
  //sys priv
  int add_sys_privs(const common::ObIArray<ObSysPriv> &sys_privs);
  int del_sys_privs(const common::ObIArray<ObSysPrivKey> &sys_priv_keys);
  int add_sys_priv(const ObSysPriv &sys_priv);
  int del_sys_priv(const ObSysPrivKey &db_priv_key);
  int get_sys_priv(const ObSysPrivKey &sys_priv_key,
                   const ObSysPriv *&sys_priv) const;
  int get_sys_priv_array(const ObSysPrivKey &sys_priv_key,
                         const ObPackedPrivArray &packed_priv_array) const;                      
  // other
  int get_db_privs_in_tenant(const uint64_t tenant_id,
                             common::ObIArray<const ObDBPriv *> &db_privs) const;
  int get_db_privs_in_user(const uint64_t tenant_id,
                           const uint64_t user_id,
                           common::ObIArray<const ObDBPriv *> &db_privs) const;
  int get_table_privs_in_tenant(const uint64_t tenant_id,
                                common::ObIArray<const ObTablePriv *> &table_privs) const;
  int get_table_privs_in_user(const uint64_t tenant_id,
                              const uint64_t user_id,
                              common::ObIArray<const ObTablePriv *> &table_privs) const;
  int get_routine_privs_in_user(const uint64_t tenant_id,
                                const uint64_t user_id,
                                ObIArray<const ObRoutinePriv *> &routine_privs) const;



  int get_column_privs_in_user(const uint64_t tenant_id,
                                const uint64_t user_id,
                                ObIArray<const ObColumnPriv *> &column_privs) const;
  int get_obj_privs_in_tenant(const uint64_t tenant_id,
                              common::ObIArray<const ObObjPriv *> &obj_privs) const;
  int get_obj_privs_in_grantee(const uint64_t tenant_id,
                               const uint64_t grantee_id,
                               common::ObIArray<const ObObjPriv *> &obj_privs) const;
  int get_obj_privs_in_grantor(const uint64_t tenant_id,
                               const uint64_t grantor_id,
                               common::ObIArray<const ObObjPriv *> &obj_privs,
                               bool reset_flag) const;
  int get_obj_privs_in_obj(const uint64_t tenant_id,
                               const uint64_t obj_id,
                               const uint64_t obj_type,
                               common::ObIArray<const ObObjPriv *> &obj_privs,
                               bool reset_flag) const;
  int get_sys_privs_in_tenant(const uint64_t tenant_id,
                              common::ObIArray<const ObSysPriv *> &sys_privs) const;
  int get_sys_priv_in_grantee(const uint64_t tenant_id,
                              const uint64_t grantee_id,
                              ObSysPriv *& sys_priv) const;                                                        
  static const char *get_first_priv_name(ObPrivSet priv_set);
  static const char *get_priv_name(int64_t priv_shift);
  int get_priv_schema_count(int64_t &priv_scheam_count) const;
  int get_schema_statistics(const ObSchemaType schema_type, 
                            ObSchemaStatisticsInfo &schema_info) const;
private:
  int get_db_priv_iter(const ObOriginalDBKey &db_key,
                       DBPrivIter &target_db_priv_iter) const;
  int get_sys_priv_iter(const ObSysPrivKey &sys_key,
                        SysPrivIter &target_sys_priv_iter) const;
  int rebuild_routine_priv_hashmap();
  int rebuild_column_priv_hashmap_and_vec();
private:
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  DBPrivInfos db_privs_;
  TablePrivInfos table_privs_;
  TablePrivMap table_priv_map_;
  RoutinePrivInfos routine_privs_;
  RoutinePrivMap routine_priv_map_;

  ColumnPrivInfos column_privs_sort_by_name_;
  ColumnPrivInfos column_privs_sort_by_id_;
  ObjPrivInfos obj_privs_;
  ObjPrivMap obj_priv_map_;
  SysPrivInfos sys_privs_;
  static const char *priv_names_[];
};

} //end of schema
} //end of share
} //end of oceanbase
#endif //OB_OCEANBASE_SCHEMA_OB_PRIV_MGR_H_
