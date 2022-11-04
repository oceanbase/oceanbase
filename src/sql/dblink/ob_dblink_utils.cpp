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

#define USING_LOG_PREFIX SQL_OPT

#include "sql/dblink/ob_dblink_utils.h"

using namespace oceanbase::sql;

int GenUniqueAliasName::init()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    if (OB_FAIL(table_id_to_alias_name_.create(UNIQUE_NAME_BUCKETS, "DblinkAliasMap"))) {
      LOG_WARN("failed to init hashmap table_id_to_alias_name_", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void GenUniqueAliasName::reset()
{
  table_id_to_alias_name_.reuse();
  alias_name_suffix_ = 0;
}

int GenUniqueAliasName::get_unique_name(uint64_t table_id, ObString &alias_name)
{
  int ret = OB_SUCCESS;
  const ObString *value = table_id_to_alias_name_.get(table_id);
  if (NULL == value) {
    if (OB_FAIL(set_unique_name(table_id, alias_name))) {
      LOG_WARN("set_unique_name failed", K(ret), K(table_id), K(alias_name));
    }
  } else {
    alias_name = *(const_cast<ObString *>(value));
  }
  return ret;
}
int GenUniqueAliasName::set_unique_name(uint64_t &table_id, ObString &alias_name) {
  int ret = OB_SUCCESS;
  CheckUnique op(alias_name);
  if (alias_name.empty()) {
    //do nothing
  } else if (OB_FAIL(table_id_to_alias_name_.foreach_refactored(op))) {
    LOG_WARN("failed to foreach table_id_to_alias_name_", K(ret));
  } else {
    if (op.is_unique_) {
      if (OB_FAIL(table_id_to_alias_name_.set_refactored(table_id, alias_name))) {
        LOG_WARN("failed to set refactored", K(ret), K(table_id), K(alias_name));
      }
    } else {
      char *name_buf = NULL;
      // Reserve MAX_LENGTH_OF_SUFFIX bytes for the suffix of new_alias_name, 
      // and suffix is ​​a variable that increments from 0.
      // The length of MAX_LENGTH_OF_SUFFIX bytes is completely sufficient
      // for DBLINK to generate a unique alias_name.
      int64_t name_buf_len = alias_name.length() + MAX_LENGTH_OF_SUFFIX;
      if (OB_ISNULL(name_buf = static_cast<char *>(arena_allocator_.alloc(name_buf_len)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to alloc new name", K(name_buf), K(ret));
      } else {
        MEMCPY(name_buf, alias_name.ptr(), alias_name.length());
        // Every time a new alias_name is generated, 
        // the hash_map must be traversed to confirm whether the alias_name is unique.
        // The reason why the uniqueness of alias_name is not guaranteed by 
        // adding an additional hash_set is because each time you reverse sql,
        // the total number of alias_name is very small, 
        // and this is not necessary.
        do {
          int64_t name_len = alias_name.length();
          databuff_printf(name_buf, name_buf_len, name_len, "_%d", alias_name_suffix_++);
          name_len += strlen(name_buf + name_len);
          alias_name = ObString(name_len, name_buf);
          op.reset(alias_name);
          if (OB_FAIL(table_id_to_alias_name_.foreach_refactored(op))) {
            LOG_WARN("failed to foreach table_id_to_alias_name_", K(ret));
          }
        } while(OB_SUCC(ret) && !op.is_unique_);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(table_id_to_alias_name_.set_refactored(table_id, alias_name))) {
            LOG_WARN("failed to set refactored", K(ret), K(table_id), K(alias_name));
          }
        }
      }
    }
  }
  return ret;
}
