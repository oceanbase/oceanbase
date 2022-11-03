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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/ob_priv_common.h"
#include "ob_define.h"
#include "lib/charset/ob_charset.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
using namespace common;

namespace share
{

#define N_PIRVS_PER_GROUP 30

/* 300个权限*/
int group_id_arr[] = 
{
    0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 
    4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 
    5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
};

/* 300个权限 */
int th_in_group_arr[] = 
{
    0,
    0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26, 28,        /* 1- 15 */
    30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,        /* 16-30 */    
    0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26, 28,        /* 31-45 */
    30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,        /* 46-60 */    
    0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26, 28,        /* 61-75 */
    30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,        /* 76-90 */    
    0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26, 28,        /* 91-105 */
    30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,        /* 106-120 */    
    0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26, 28,        /* 121-135 */
    30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,        /* 136-150 */    
    0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26, 28,        /* 151-165 */
    30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,        /* 166-180 */    
    0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26, 28,        /* 181-195 */
    30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,        /* 196-210 */    
    0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26, 28,        /* 211-225 */
    30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,        /* 226-240 */    
    0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26, 28,        /* 241-255 */
    30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,        /* 256-270 */    
    0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22, 24, 26, 28,        /* 271-285 */
    30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,        /* 286-300 */    

};

#define NTHID_TO_PACKED(i) (1LL << i)

int ObPrivPacker::init_packed_array(
    ObPackedPrivArray &array)
{
  int ret = OB_SUCCESS;
  array.reset();
  OZ (array.push_back(0));
  OZ (array.push_back(0));
  OZ (array.push_back(0));
  return ret;
}

int ObPrivPacker::raw_priv_to_packed_info(
    const uint64_t option,
    const ObRawPriv priv, 
    int &group_id,
    ObPrivSet &packed_priv)
{  
  int ret = OB_SUCCESS;
  CK (priv > 0 && priv < PRIV_ID_MAX);
  group_id = group_id_arr[priv];
  packed_priv = NTHID_TO_PACKED(th_in_group_arr[priv]);
  CK (option == NO_OPTION || option == ADMIN_OPTION);
  if (option == ADMIN_OPTION) {
    packed_priv |= packed_priv << 1;
  }
  return ret;
}

int ObPrivPacker::raw_obj_priv_to_packed_info(
    const uint64_t option,
    const ObRawObjPriv priv,
    ObPackedObjPriv &packed_priv)
{  
  int ret = OB_SUCCESS;
  CK (priv >= 0 && priv <= OBJ_PRIV_ID_MAX);
  packed_priv = NTHID_TO_PACKED(th_in_group_arr[priv]);
  CK (option == NO_OPTION || option == GRANT_OPTION);
  if (option == GRANT_OPTION) {
    packed_priv |= packed_priv << 1;
  }
  return ret;
}

/* 判断一个相对的priv id是否在privset里面，同时输出option信息 */
int ObPrivPacker::has_raw_priv(
    const ObRawPriv raw_priv,
    const ObPrivSet priv_set,
    bool &exists,
    uint64_t &option)
{
  int ret = OB_SUCCESS;
  ObPrivSet packed_priv;
  exists = false;
  CK (raw_priv >= 0 && raw_priv <= PRIV_ID_MAX);
  if (OB_SUCC(ret)) {
    packed_priv = NTHID_TO_PACKED(th_in_group_arr[raw_priv]);
    if (packed_priv & priv_set) {
      exists = true;
      if ((packed_priv << 1) & priv_set) {
        option = 1;
      }
    }
  }
  return ret;
}

/* 将相对raw priv和group idx，还原到raw priv，根据option，push back到相应的链表 */
int ObPrivPacker::push_back_raw_priv_array(
    ObRawPriv raw_priv,
    bool exists,
    uint64_t option,
    ObRawPrivArray &raw_priv_array,
    ObRawPrivArray &raw_priv_array_with_option)
{
  int ret = OB_SUCCESS;
  if (exists) {
    if (option) {
      OZ (raw_priv_array_with_option.push_back(raw_priv));
    }
    else {
      OZ (raw_priv_array.push_back(raw_priv));
    }
  }
  return ret;
}

/* 解析packed_array到raw privs array，根据是否有option，输出两个链表 */
int ObPrivPacker::packed_array_to_raw_privs(
    const ObPackedPrivArray &packed_array,            /* in: packed info from sys priv schema */
    ObRawPrivArray &raw_priv_array,                   /* out: raw privs array */
    ObRawPrivArray &raw_priv_array_with_option)       /* out: raw privs with option array */
{
  int ret = OB_SUCCESS;
  ObPrivSet priv_set;
  ObRawPriv raw_priv;
  // unused
  // ObRawPriv related_raw_priv;
  bool stop = false;
  int i = 0;
  bool exists = false;
  uint64_t option;
  ARRAY_FOREACH(packed_array, group_idx) {
    priv_set = packed_array.at(group_idx);
    if (priv_set > 0) {
      raw_priv =  group_idx * N_PIRVS_PER_GROUP + 1;
      for (i = 0; OB_SUCC(ret) && !stop && i < N_PIRVS_PER_GROUP; i ++) {
        OZ (has_raw_priv(raw_priv, priv_set, exists, option));
        OZ (push_back_raw_priv_array(raw_priv, 
                                     exists,
                                     option, 
                                     raw_priv_array,
                                     raw_priv_array_with_option));
        OX (raw_priv ++);
        if (raw_priv >= PRIV_ID_MAX) {
          stop = true;
        }
      }
    }
  }
  return ret;
}

bool ObOraPrivCheck::user_is_owner(
   const ObString &user_name,
   const ObString &db_name)
{
  return ObCharset::case_sensitive_equal(user_name, db_name);
}

int ObPrivPacker::pack_raw_priv(
    const uint64_t option,
    const ObRawPriv priv,
    ObPackedPrivArray &packed_array)
{
  int group_id;
  ObPrivSet packed_priv;    
  int ret = OB_SUCCESS;
  
  OZ (raw_priv_to_packed_info(option, priv, group_id, packed_priv));

  if (OB_SUCC(ret)) {
    if (packed_array.count() > 0) {
      if (group_id >= packed_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group id error", K(ret));
      } else {
        packed_array[group_id] |= packed_priv;
      }
    } else {
      LOG_WARN("packed_array has no space reserved");
    }
  }
  return ret;
}

int ObPrivPacker::pack_raw_priv_list(
    const uint64_t option,
    const ObRawPrivArray &priv_list,
    ObPackedPrivArray &packed_array)
{
  int group_id;
  ObPrivSet packed_priv;   
  ObRawPriv priv; 
  int ret = OB_SUCCESS;
  
  OZ (ObPrivPacker::init_packed_array(packed_array));
  for (int i = 0; OB_SUCC(ret) && i < priv_list.count(); i++) {
    priv = priv_list.at(i);
    OZ (raw_priv_to_packed_info(option, priv, group_id, packed_priv));
    if (OB_SUCC(ret)) {
      if (packed_array.count() > 0) {
        if (group_id >= packed_array.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group id error", K(ret));
        } else {
          packed_array[group_id] |= packed_priv;
        }
      } else {
        LOG_WARN("packed_array has no space reserved");
      }
    }
  }
  return ret;
}

int ObPrivPacker::pack_raw_obj_priv(
    const uint64_t option,
    const ObRawPriv priv,
    ObPackedObjPriv &packed_obj_priv)
{ 
  int ret = OB_SUCCESS;  
  OZ (raw_obj_priv_to_packed_info(option, priv, packed_obj_priv),
      option, priv);
  return ret;
}

int ObPrivPacker::pack_raw_obj_priv_list(
    const uint64_t option,
    const ObRawObjPrivArray priv_list,
    ObPackedObjPriv &packed_obj_priv)
{ 
  int ret = OB_SUCCESS;
  packed_obj_priv = 0;
  for (int i = 0; OB_SUCC(ret) && i < priv_list.count(); i++) {
    const ObRawObjPriv priv = priv_list.at(i);
    OZ (append_raw_obj_priv(option, priv, packed_obj_priv),
        option, priv);
  }
  return ret;
}

int ObPrivPacker::append_raw_obj_priv(
    const uint64_t option,
    const ObRawObjPriv priv,
    ObPackedObjPriv &packed_obj_privs)
{
  int ret = OB_SUCCESS;
  ObPackedObjPriv tmp_packed_priv;
  OZ (raw_obj_priv_to_packed_info(option, priv, tmp_packed_priv));
  OX (packed_obj_privs |= tmp_packed_priv);
  return ret;
}

int ObPrivPacker::raw_obj_priv_from_pack(
    const ObPackedObjPriv &packed_obj_privs,
    ObRawObjPrivArray &raw_priv_array)
{
  int ret = OB_SUCCESS;
  ObRawObjPriv raw_priv;
  bool exists;
  raw_priv_array.reset();
  if (packed_obj_privs > 0) {
    CK (OBJ_PRIV_ID_MAX <= N_PIRVS_PER_GROUP);
    for (raw_priv = 1; OB_SUCC(ret) && raw_priv <= OBJ_PRIV_ID_MAX; raw_priv ++) {
      OZ (ObOraPrivCheck::raw_obj_priv_exists(raw_priv, packed_obj_privs, exists));
      if (OB_SUCC(ret) && exists) {
        OZ (raw_priv_array.push_back(raw_priv));
      }
    }
  }
  return ret;
}

int ObPrivPacker::raw_option_obj_priv_from_pack(
    const ObPackedObjPriv &packed_obj_privs,
    ObRawObjPrivArray &raw_priv_array)
{
  int ret = OB_SUCCESS;
  ObRawObjPriv raw_priv;
  bool exists;
  raw_priv_array.reset();
  if (packed_obj_privs > 0) {
    CK (OBJ_PRIV_ID_MAX <= N_PIRVS_PER_GROUP);
    for (raw_priv = 1; OB_SUCC(ret) && raw_priv <= OBJ_PRIV_ID_MAX; raw_priv ++) {
      OZ (ObOraPrivCheck::raw_obj_priv_exists(raw_priv, GRANT_OPTION, packed_obj_privs, exists));
      if (OB_SUCC(ret) && exists) {
        OZ (raw_priv_array.push_back(raw_priv));
      }
    }
  }
  return ret;
}

int ObPrivPacker::raw_no_option_obj_priv_from_pack(
    const ObPackedObjPriv &packed_obj_privs,
    ObRawObjPrivArray &raw_priv_array)
{
  int ret = OB_SUCCESS;
  ObRawObjPriv raw_priv;
  bool exists_grant;
  bool exists;
  raw_priv_array.reset();
  if (packed_obj_privs > 0) {
    CK (OBJ_PRIV_ID_MAX <= N_PIRVS_PER_GROUP);
    for (raw_priv = 1; OB_SUCC(ret) && raw_priv <= OBJ_PRIV_ID_MAX; raw_priv ++) {
      OZ (ObOraPrivCheck::raw_obj_priv_exists(raw_priv, packed_obj_privs, exists));
      OZ (ObOraPrivCheck::raw_obj_priv_exists(raw_priv, GRANT_OPTION, 
              packed_obj_privs, exists_grant));
      if (OB_SUCC(ret) && exists && !exists_grant) {
        OZ (raw_priv_array.push_back(raw_priv));
      }
    }
  }
  return ret;
}

int ObPrivPacker::merge_two_packed_array(
    ObPackedPrivArray &packed_array_1,
    const ObPackedPrivArray &packed_array_2)
{
  int ret = OB_SUCCESS;
  int i;
  if (packed_array_1.count() == 0) {
    packed_array_1 = packed_array_2;
  } else if (packed_array_2.count() > 0) {
    CK (packed_array_1.count() == packed_array_2.count());
    for (i = 0; i < packed_array_1.count() && OB_SUCC(ret); i++) {
      packed_array_1[i] |= packed_array_2[i];
    }
  }
  return ret;
}

int ObOraPrivCheck::raw_sys_priv_exists(
    const uint64_t option,
    const ObRawPriv priv,
    const ObPackedPrivArray &packed_array,
    bool &exists)
{
  int ret = OB_SUCCESS;
  int group_id;
  ObPrivSet packed_priv;    
  exists = false;
  if (packed_array.count() > 0) {
    if (priv == OBJ_PRIV_ID_NONE) {
      exists = FALSE;
    } else {
      OZ (ObPrivPacker::raw_priv_to_packed_info(option, priv, group_id, packed_priv));
      if (OB_SUCC(ret)) {
        if (group_id >= packed_array.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group id error", K(option), K(priv), K(packed_array), K(group_id), K(ret));
        } else if (OB_TEST_PRIVS(packed_array.at(group_id), packed_priv)) {
          exists = true;
        }
      }
    }
  }
  return ret;
}

int ObOraPrivCheck::raw_sys_priv_exists(
    const ObRawPriv priv,
    const ObPackedPrivArray &packed_array,
    bool &exists)
{
  int ret = OB_SUCCESS;
  int group_id;
  ObPrivSet packed_priv;    
  exists = false;
  if (priv > 0 && packed_array.count() > 0) {
    OZ (ObPrivPacker::raw_priv_to_packed_info(NO_OPTION, priv, group_id, packed_priv), priv);
    if (OB_SUCC(ret)) {
      if (group_id >= packed_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group id error", K(priv), K(packed_array), K(group_id), K(ret));
      } else if (OB_TEST_PRIVS(packed_array.at(group_id), packed_priv)) {
        exists = true;
      }
    }
  }
  return ret;
}

int ObOraPrivCheck::packed_sys_priv_list_or_exists(
    const ObPackedPrivArray &plist_to_be_checked,
    const ObPackedPrivArray &packed_array,
    bool &exists)
{
  int ret = OB_SUCCESS;
  int group_id;

  exists = false;
  for (group_id = 0; !exists && group_id < plist_to_be_checked.count(); group_id++) {
    if (plist_to_be_checked.at(group_id) != 0 
        && group_id < packed_array.count()
        && (OB_PRIV_HAS_ANY(plist_to_be_checked.at(group_id), packed_array.at(group_id))))
        exists = true;
  }
  return ret;
}

int ObOraPrivCheck::packed_sys_priv_list_and_exists(
    const ObPackedPrivArray &plist_to_be_checked,
    const ObPackedPrivArray &packed_array,
    bool &exists)
{
  int ret = OB_SUCCESS;
  int group_id;

  exists = true;
  for (group_id = 0; exists && group_id < plist_to_be_checked.count(); group_id++) {
    if (plist_to_be_checked.at(group_id) != 0) {
      if (group_id >= packed_array.count()) {
        exists = false;
      } else {
        if (OB_TEST_PRIVS(packed_array.at(group_id), plist_to_be_checked.at(group_id))) {
        } else {
          exists = false;
        }
      }
    }
  }
  return ret;
}

int ObOraPrivCheck::raw_obj_priv_exists(
    const ObRawObjPriv priv,
    const ObPackedObjPriv &obj_privs,
    bool &exists)
{
  int ret = OB_SUCCESS;
  exists = false;
  if (priv > 0) {
    ObPrivSet packed_priv;
    OZ (ObPrivPacker::raw_obj_priv_to_packed_info(NO_OPTION, priv, packed_priv));
    OX (exists = OB_TEST_PRIVS(obj_privs, packed_priv));
  }
  return ret;
}

int ObOraPrivCheck::raw_obj_priv_exists_with_info(
    const ObRawObjPriv priv,
    const ObPackedObjPriv &obj_privs,
    bool &exists,
    uint64_t &option)
{
  int ret = OB_SUCCESS;
  exists = false;
  option = NO_OPTION;
  if (priv > 0) {
    ObPrivSet packed_priv;
    OZ (ObPrivPacker::raw_obj_priv_to_packed_info(NO_OPTION, priv, packed_priv));
    OX (exists = OB_TEST_PRIVS(obj_privs, packed_priv));
    if (OB_SUCC(ret) && exists) {
      bool tmp_exist = false;
      OZ (ObPrivPacker::raw_obj_priv_to_packed_info(GRANT_OPTION, priv, packed_priv));
      OX (tmp_exist = OB_TEST_PRIVS(obj_privs, packed_priv));
      OX (option = tmp_exist ? GRANT_OPTION: NO_OPTION);
    }
  }
  return ret;
}

int ObOraPrivCheck::raw_obj_priv_exists(
    const ObRawObjPriv priv,
    const uint64_t option, 
    const ObPackedObjPriv &obj_privs,
    bool &exists)
{
  int ret = OB_SUCCESS;
  exists = false;
  if (priv > 0) {
    ObPrivSet packed_priv;
    OZ (ObPrivPacker::raw_obj_priv_to_packed_info(option, priv, packed_priv));
    OX (exists = OB_TEST_PRIVS(obj_privs, packed_priv));
  }
  return ret;
}

int ObOraPrivCheck::packed_obj_priv_list_or_exists(
    const ObPackedObjPriv &priv_list_to_be_checked,
    const ObPackedObjPriv &obj_privs,
    bool &exists)
{
  int ret = OB_SUCCESS;
  exists = OB_PRIV_HAS_ANY(obj_privs, priv_list_to_be_checked);
  return ret;
}

int ObOraPrivCheck::p1_or_cond_p2_exists(
    const ObRawPriv priv1,
    bool cond,
    const ObRawPriv priv2,
    const ObPackedPrivArray &packed_array,
    bool &exists)
{
  int ret = OB_SUCCESS;

  OZ (raw_sys_priv_exists(priv1, packed_array, exists));
  if (OB_SUCC(ret) && !exists && cond) {
    CK (priv1 != priv2);
    OZ (raw_sys_priv_exists(priv2, packed_array, exists));  
  }
  return ret;
}   

bool ObOraPrivCheck::raw_priv_can_be_granted_to_column(const ObRawObjPriv priv)
{
  return (OBJ_PRIV_ID_INSERT == priv
          || OBJ_PRIV_ID_UPDATE == priv
          || OBJ_PRIV_ID_REFERENCES == priv);
}

int ObPrivPacker::get_total_privs(
    const ObPackedPrivArray &packed_array,
    int &n_cnt)
{
  int ret = OB_SUCCESS;
  int i;
  int j;
  ObRawPriv raw_priv;
  n_cnt = 0;
  for (i = 0; i < packed_array.count() && OB_SUCC(ret); i++) {
    const ObPrivSet &item = packed_array[i];
    if (item > 0) {
      raw_priv  = N_PIRVS_PER_GROUP * i + 1;
      for (j = 0; j < N_PIRVS_PER_GROUP && OB_SUCC(ret); j++) {
        if ((item & NTHID_TO_PACKED(th_in_group_arr[raw_priv])) != 0)
          n_cnt ++;
        OX (raw_priv ++);
      }
    }
  }
  return ret;
}    

int ObPrivPacker::get_total_obj_privs(
    const ObPackedObjPriv &packed_obj_privs,
    int &n_cnt)
{
  int ret = OB_SUCCESS;
  ObRawObjPriv raw_priv;
  bool exists;
  n_cnt = 0;
  if (packed_obj_privs > 0) {
    CK (OBJ_PRIV_ID_MAX <= N_PIRVS_PER_GROUP);
    for (raw_priv = 1; OB_SUCC(ret) && raw_priv <= OBJ_PRIV_ID_MAX; raw_priv ++) {
      OZ (ObOraPrivCheck::raw_obj_priv_exists(raw_priv, packed_obj_privs, exists));
      if (OB_SUCC(ret) && exists) {
        n_cnt ++;
      }
    }
  }
  return ret;
}    

}//end namespace share
}//end namespace oceanbase