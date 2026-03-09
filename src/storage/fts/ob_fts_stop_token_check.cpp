/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "object/ob_object.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "share/rc/ob_tenant_base.h"
#include "storage/fts/ob_fts_stop_token_check.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/ob_storage_util.h"

namespace oceanbase
{
namespace storage
{

/**
* -----------------------------------ObStopTokenChecker-----------------------------------
*/

int ObStopTokenChecker::init(const ObCollationType coll, ObStopTokenTable *stop_token_hash_table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObStopTokenChecker has been initialized", K(ret));
  } else if (OB_UNLIKELY(coll == CS_TYPE_INVALID ||
                  coll >= CS_TYPE_PINYIN_BEGIN_MARK ||
                  nullptr == stop_token_hash_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the are invalid arguments", K(ret), K(coll), KP(stop_token_hash_table));
  } else {
    collation_type_ = coll;
    stop_token_hash_table_ = stop_token_hash_table;
    is_inited_ = true;
  }
  return ret;
}

int ObStopTokenChecker::check_is_stop_token(const ObFTToken &token, bool &is_stop_token) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObStopTokenChecker hasn't been initialized", K(ret));
  } else {
    ret = stop_token_hash_table_->exist_refactored(token);
    if (OB_HASH_NOT_EXIST == ret) {
      is_stop_token = false;
      ret = OB_SUCCESS;
    } else if (OB_HASH_EXIST == ret) {
      is_stop_token = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to exist refactored", K(ret), K(token));
    }
  }
  return ret;
}

/**
* -----------------------------------ObStopTokenCheckerGen-----------------------------------
*/

void ObStopTokenCheckerGen::reset()
{
  common::TCWLockGuard w_guard(lock_);
  is_inited_ = false;
  if (OB_LIKELY(stop_token_hash_tables_.created())) {
    StopTokenHashMap::iterator it;
    for (it = stop_token_hash_tables_.begin(); it != stop_token_hash_tables_.end(); ++it) {
      if (OB_LIKELY(nullptr != it->second)) {
        (void)it->second->destroy();
        OB_DELETE(ObStopTokenTable, &allocator_, it->second);
        it->second = nullptr;
      }
    }
    (void)stop_token_hash_tables_.destroy();
  }
  allocator_.reset();
}

int ObStopTokenCheckerGen::init()
{
  int ret = OB_SUCCESS;
  common::TCWLockGuard w_guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObStopTokenCheckerGen has been initialized", K(ret));
  } else {
    uint64_t tenant_id = OB_SERVER_TENANT_ID;
    allocator_.set_attr(ObMemAttr(tenant_id, "tokenCheckerGen"));
    ObCollationType general_coll_types[] = { CS_TYPE_UTF8MB4_GENERAL_CI,
                                             CS_TYPE_UTF8MB4_BIN };
    const int64_t general_coll_type_count = sizeof(general_coll_types) / sizeof(general_coll_types[0]);
    if (OB_LIKELY(!stop_token_hash_tables_.created()) &&
        OB_FAIL(stop_token_hash_tables_.create(ObCharset::VALID_COLLATION_TYPES,
                                       "st_hash_tables",
                                       "st_hash_tables",
                                       tenant_id))) {
        LOG_WARN("fail to create stop token hash tables", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < general_coll_type_count; ++i) {
      if (OB_FAIL(generate_stop_token_hash_table_by_coll(general_coll_types[i]))) {
        LOG_WARN("fail to initialize stop token table by collation", K(ret), K(general_coll_types[i]));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObStopTokenCheckerGen::convert_charset(
    const ObString &src_string,
    const ObCollationType from_collation,
    const ObCollationType to_collation,
    ObString &converted_string)
{
  int ret = OB_SUCCESS;
  converted_string.reset();
  if(CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(to_collation)) {
    converted_string = src_string;
  } else if (OB_FAIL(common::ObCharset::charset_convert(allocator_,
                                                        src_string,
                                                        from_collation,
                                                        to_collation,
                                                        converted_string))) {
    LOG_WARN("fail to convert charset",
        K(ret), K(src_string), K(from_collation), K(to_collation));
  }
  return ret;
}

int ObStopTokenCheckerGen::generate_stop_token_hash_table_by_coll(const ObCollationType coll)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_SERVER_TENANT_ID;
  ObStopTokenTable *stop_token_hash_table = OB_NEWx(ObStopTokenTable, &allocator_);
  if (OB_UNLIKELY(nullptr == stop_token_hash_table)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for stop token table", K(ret));
  } else if (OB_FAIL(stop_token_hash_table->create(DEFAULT_STOP_TOKEN_TABLE_CAPACITY,
                                                   "stop_token_tab", "stop_token_tab", tenant_id))) {
    LOG_WARN("fail to create stop token hash set", K(ret));
  } else {
    ObObjMeta meta;
    meta.set_varchar();
    meta.set_collation_type(coll);
    const int64_t stop_token_count = sizeof(ob_stop_token_table_utf8) / sizeof(ob_stop_token_table_utf8[0]);

    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(meta.get_type(), meta.get_collation_type());
    ObDatumCmpFuncType cmp_func = get_datum_cmp_func(meta, meta);

    if (OB_UNLIKELY(nullptr == basic_funcs || nullptr == cmp_func)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get basic funcs or cmp func",
          K(ret), K(meta), KP(basic_funcs), KP(cmp_func));
    } else if (OB_UNLIKELY(nullptr == basic_funcs->default_hash_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the default hash is null", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stop_token_count; ++i) {
      ObFTToken stop_token;
      uint64_t hash_val = 0;
      ObString converted_string;
      if (OB_FAIL(convert_charset(ob_stop_token_table_utf8[i],
                                  CS_TYPE_UTF8MB4_GENERAL_CI,
                                  coll,
                                  converted_string))) {
        LOG_WARN("fail to convert charset", K(ret), K(coll));
      } else if (OB_FAIL(stop_token.init(converted_string.ptr(),
                                         converted_string.length(),
                                         meta,
                                         basic_funcs->default_hash_,
                                         cmp_func))) {
        LOG_WARN("fail to initialize stop token",
            K(ret), K(converted_string), K(meta));
      } else if (OB_FAIL(stop_token.hash(hash_val))) {
        LOG_WARN("fail to calc stop token hash value", K(ret), K(stop_token));
      } else if (OB_FAIL(stop_token_hash_table->set_refactored(stop_token))) {
        LOG_WARN("fail to set stop token", K(ret), K(stop_token));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stop_token_hash_tables_.set_refactored(coll, stop_token_hash_table))) {
        LOG_WARN("fail to set stop token table", K(ret), K(coll));
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != stop_token_hash_table) {
      (void)stop_token_hash_table->destroy();
      OB_DELETE(ObStopTokenTable, &allocator_, stop_token_hash_table);
      stop_token_hash_table = nullptr;
    }
  }
  return ret;
}

int ObStopTokenCheckerGen::get_stop_token_checker_by_coll(
    const ObCollationType coll,
    ObStopTokenChecker &stop_token_checker)
{
  int ret = OB_SUCCESS;
  ObStopTokenTable *stop_token_hash_table = nullptr;
  stop_token_checker.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObStopTokenCheckerGen hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(CS_TYPE_INVALID == coll || coll >= CS_TYPE_PINYIN_BEGIN_MARK)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the are invalid argument", K(ret), K(coll));
  } else {
    {
      common::TCRLockGuard r_guard(lock_);
      if (OB_FAIL(stop_token_hash_tables_.get_refactored(coll, stop_token_hash_table))) {
        if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get stop token table", K(ret), K(coll));
        }
      } else if (OB_FAIL(stop_token_checker.init(coll, stop_token_hash_table))) {
        LOG_WARN("fail to initialize stop token checker", K(ret), K(coll));
      }
    }

    if (OB_SUCC(ret) && nullptr == stop_token_hash_table) {
      common::TCWLockGuard write_guard(lock_);
      if (OB_FAIL(stop_token_hash_tables_.get_refactored(coll, stop_token_hash_table))) {
        if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
          ret = OB_SUCCESS;
          if (OB_FAIL(generate_stop_token_hash_table_by_coll(coll))) {
            LOG_WARN("fail to generate stop token table by collation", K(ret), K(coll));
          } else if (OB_FAIL(stop_token_hash_tables_.get_refactored(coll, stop_token_hash_table))) {
            LOG_WARN("fail to get stop token table after creation", K(ret), K(coll));
          }
        } else {
          LOG_WARN("fail to get stop token table", K(ret), K(coll));
        }
      }
      if (FAILEDx(stop_token_checker.init(coll, stop_token_hash_table))) {
        LOG_WARN("fail to initialize stop token checker", K(ret), K(coll));
      }
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
