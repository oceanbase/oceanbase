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

#define USING_LOG_PREFIX SQL

#include "lib/rc/ob_rc.h"
#include "src/sql/ob_sql_ccl_rule_manager.h"
#include "src/share/schema/ob_multi_version_schema_service.h"
#include "src/share/schema/ob_schema_getter_guard.h"
#include "src/share/ob_server_struct.h"
#include "src/share/schema/ob_ccl_rule_mgr.h"
#include "src/sql/engine/expr/ob_expr_like.h"
#include "src/sql/ob_sql_utils.h"
#include "src/sql/ob_sql_context.h"
#include "observer/ob_server_utils.h"
#include <type_traits>

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase {
namespace sql {

ObSQLCCLRuleLevelConcurrencyMapWrapper::~ObSQLCCLRuleLevelConcurrencyMapWrapper() {
  alloc_.reset();
  concurrency_map_.clear();
}

int ObSQLCCLRuleLevelConcurrencyMapWrapper::init(const ObMemAttr &bucket_attr) {
  int ret = OB_SUCCESS;
  bucket_attr_ = bucket_attr;
  if (OB_FAIL(alloc_.init(lib::ObMallocAllocator::get_instance(),
                              OB_MALLOC_NORMAL_BLOCK_SIZE, bucket_attr_))) {
    LOG_WARN("failed to init fifo allocator", K(ret));
  } else if (OB_FAIL(concurrency_map_.create(hash::cal_next_prime(OB_PLAN_CACHE_BUCKET_NUMBER), bucket_attr_))) {
    LOG_WARN("failed to init concurrency_map_", K(ret));
  }

  if (OB_FAIL(ret)) {
    alloc_.reset();
    concurrency_map_.clear();
  }

  return ret;
}
ObSQLCCLRuleManager::ObSQLCCLRuleManager():
    inited_(false)
{}

int ObSQLCCLRuleManager::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    if (OB_FAIL(rule_level_concurrency_map_wrapper_.init(ObMemAttr(tenant_id, "ObSqlCclRuleMgr", ObCtxIds::DEFAULT_CTX_ID)))) {
      LOG_WARN("fail to create hash map", KR(ret));
    } else if (OB_FAIL(format_sqlid_level_concurrency_map_wrapper_.init(ObMemAttr(tenant_id, "ObSqlCclRuleMgr", ObCtxIds::DEFAULT_CTX_ID)))) {
      LOG_WARN("fail to create hash map", KR(ret));
    } else if (OB_FAIL(init_whitelist())) {
      LOG_WARN("failed to init whitelist", K(ret));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

int ObSQLCCLRuleManager::mtl_new(ObSQLCCLRuleManager* &sql_ccl_rule_mgr)
{
  int ret = OB_SUCCESS;
  sql_ccl_rule_mgr = OB_NEW(ObSQLCCLRuleManager, ObMemAttr(MTL_ID(), "ObSqlCclRuleMgr"));
  if (nullptr == sql_ccl_rule_mgr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObSQLCCLRuleManager", K(ret));
  }
  return ret;
}

int ObSQLCCLRuleManager::mtl_init(ObSQLCCLRuleManager* &sql_ccl_rule_mgr)
{
  int ret = OB_SUCCESS;
  if (nullptr == sql_ccl_rule_mgr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLCCLRuleManager not alloc yet", K(ret));
  } else {
    uint64_t tenant_id = lib::current_resource_owner_id();
    if (OB_FAIL(sql_ccl_rule_mgr->init(tenant_id))) {
      LOG_WARN("failed to init request manager", K(ret));
    } else {
      // do nothing
    }
    LOG_INFO("mtl init finish", K(tenant_id), K(ret));
  }
  if (OB_FAIL(ret) && sql_ccl_rule_mgr != nullptr) {
    // cleanup
    common::ob_delete(sql_ccl_rule_mgr);
    sql_ccl_rule_mgr = nullptr;
  }
  return ret;
}

void ObSQLCCLRuleManager::mtl_destroy(ObSQLCCLRuleManager* &sql_ccl_rule_mgr)
{
  common::ob_delete(sql_ccl_rule_mgr);
  sql_ccl_rule_mgr = nullptr;
}

void ObCCLRuleIncRefAtomicOp::operator()(common::hash::HashMapPair<ObFormatSQLIDCCLRuleKey, ObCCLRuleConcurrencyValueWrapper*> &entry)
{
  if (try_inc_ref_count(entry.second)) {
    value_ = entry.second;
  } else {
    value_ = nullptr;
  }
}

bool ObCCLRuleIncRefAtomicOp::try_inc_ref_count(ObCCLRuleConcurrencyValueWrapper *&value)
{
  int64_t ref_cnt = ATOMIC_LOAD(&(value->cur_concurrency_));
  while (ref_cnt > 0 && !ATOMIC_BCAS(&(value->cur_concurrency_), ref_cnt, ref_cnt + 1)) {
    ref_cnt = ATOMIC_LOAD(&(value->cur_concurrency_));
  }
  return ref_cnt > 0;
}

bool ObCCLRuleDelAtomicOp::operator()(common::hash::HashMapPair<ObFormatSQLIDCCLRuleKey, ObCCLRuleConcurrencyValueWrapper*> &entry)
{
  return ATOMIC_LOAD(&(entry.second->cur_concurrency_)) == 0;
}


int ObSQLCCLRuleLevelConcurrencyMapWrapper::insert(const ObFormatSQLIDCCLRuleKey &key,
                                                   int max_concurrency,
                                                   ObCCLRuleConcurrencyValueWrapper *&p_concurrency) {
  int ret = OB_SUCCESS;
  if (NULL == (p_concurrency = static_cast<ObCCLRuleConcurrencyValueWrapper*>(alloc_.alloc(sizeof(ObCCLRuleConcurrencyValueWrapper))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate ObCCLRuleConcurrencyValueWrapper failed", KR(ret));
  } else {
    // we must deep copy fromat_sqlid string into concurrency_map_
    // because the life of the passed in variable *key*'s format_sqlid_
    // is associated with the life of coresponding sql
    ObFormatSQLIDCCLRuleKey real_key;
    real_key.ccl_rule_id_ = key.ccl_rule_id_;
    if (OB_FAIL(ob_write_string(alloc_, key.format_sqlid_,
                                real_key.format_sqlid_))) {
      LOG_WARN("fail to deep copy format_sqlid into real_key", K(ret));
    } else {
      p_concurrency->ccl_rule_id_ = real_key.ccl_rule_id_;
      p_concurrency->format_sqlid_ = real_key.format_sqlid_;
      p_concurrency->cur_concurrency_ = 1;
      p_concurrency->max_concurrency_ = max_concurrency;
      if (OB_FAIL(concurrency_map_.set_refactored(real_key, p_concurrency))) {
        if (ret == OB_HASH_EXIST) {
          //other sql may insert the same key, ignore
          //throw this error code
        } else {
          LOG_WARN("fail to insert and get value from concurrency_map_",
            K(ret));
        }
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(p_concurrency)) {
        if (OB_NOT_NULL(p_concurrency->format_sqlid_.ptr())) {
          alloc_.free(p_concurrency->format_sqlid_.ptr());
        }
        alloc_.free(p_concurrency);
      }
    }
  }

  return ret;
}

int ObSQLCCLRuleManager::init_whitelist()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && CCL_WHITELIST_RULES[i].words[0] != nullptr; ++i) {
    ObSEArray<ObString, 2> keywords;
    for (int j = 0; OB_SUCC(ret) && CCL_WHITELIST_RULES[i].words[j] != nullptr; ++j) {
      if (OB_FAIL(keywords.push_back(ObString(CCL_WHITELIST_RULES[i].words[j])))) {
        LOG_WARN("fail to push_back value into keywords", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(whitelist_keywords_array_.push_back(keywords))) {
      LOG_WARN("fail to push_back value into whitelist_keywords_array_", K(ret));
    }
  }
  return ret;
}

int ObSQLCCLRuleLevelConcurrencyMapWrapper::inc_concurrency(const ObFormatSQLIDCCLRuleKey & key,
                                                            int max_concurrency,
                                                            ObIArray<ObCCLRuleConcurrencyValueWrapper*>& matched_ccl_value_wrappers)
{
  int ret = OB_SUCCESS;
  ObCCLRuleIncRefAtomicOp ccl_rule_op;

  do {
    //Read lock
    if (OB_FAIL(concurrency_map_.read_atomic(key, ccl_rule_op))) {
      if (ret == OB_HASH_NOT_EXIST) {
        //Write lock
        if (OB_FAIL(insert(key, max_concurrency, ccl_rule_op.get_value_for_update()))) {
          //There maybe other thread insert the same key, ret == OB_HASH_EXIST
          if (OB_HASH_EXIST != ret) {
            LOG_WARN("fail to insert key", K(ret), K(key));
          }
        }
      } else {
        LOG_WARN("fail to get value by key", K(ret), K(key));
      }
    } else if (NULL == ccl_rule_op.get_value()) {
      //Key exist, but concurrency == 0, which means it has been decrease to 0 but not deleted yet
      if (OB_FAIL(insert(key, max_concurrency, ccl_rule_op.get_value_for_update()))) {
        if (OB_HASH_EXIST != ret) {
          LOG_WARN("fail to insert key", K(ret), K(key));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // do something
      if (ccl_rule_op.get_value()->max_concurrency_ == 0 ||
          ccl_rule_op.get_value()->cur_concurrency_ >= ccl_rule_op.get_value()->max_concurrency_ + 1) {
        ret = OB_REACH_MAX_CCL_CONCURRENT_NUM;
      }
    }

    if (OB_HASH_EXIST == ret) {
        ob_usleep(10);
    }
  } while (OB_HASH_EXIST == ret);

  //Here we don't need to check ret == OB_SUCCESS
  //Because ret maybe OB_REACH_MAX_CCL_CONCURRENT_NUM
  //Which we still need add this to matched_ccl_value_wrappers for future concurrency decrement
  //Once logic reach here, concurrenct guarantee to >= 1
  int tmp_ret = ret;
  if (OB_FAIL(matched_ccl_value_wrappers.push_back(ccl_rule_op.get_value()))) {
    LOG_WARN("fail to push_back value into matched_ccl_value_wrappers", K(ret));
  } else {
    ret = tmp_ret;
  }

  return ret;
}

void ObSQLCCLRuleManager::dec_rule_level_concurrency(ObCCLRuleConcurrencyValueWrapper *p_value_wrapper)
{
  rule_level_concurrency_map_wrapper_.dec_concurrency(p_value_wrapper);
}
void ObSQLCCLRuleManager::dec_format_sqlid_level_concurrency(ObCCLRuleConcurrencyValueWrapper *p_value_wrapper)
{
  format_sqlid_level_concurrency_map_wrapper_.dec_concurrency(p_value_wrapper);
}

int ObSQLCCLRuleLevelConcurrencyMapWrapper::dec_concurrency(ObCCLRuleConcurrencyValueWrapper *p_value_wrapper)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(p_value_wrapper)) {
    ATOMIC_FAA(&(p_value_wrapper->cur_concurrency_), -1);
    ObFormatSQLIDCCLRuleKey real_key(p_value_wrapper->ccl_rule_id_, p_value_wrapper->format_sqlid_);
    ObCCLRuleDelAtomicOp del_op;
    bool is_erased = false;
    if (ATOMIC_LOAD(&(p_value_wrapper->cur_concurrency_)) == 0) {
      if (OB_FAIL(concurrency_map_.erase_if(real_key, del_op, is_erased))) {
        if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to erase key", K(ret), K(real_key));
        }
      } else if (is_erased) {
        if (OB_NOT_NULL(p_value_wrapper->format_sqlid_.ptr())) {
          alloc_.free(p_value_wrapper->format_sqlid_.ptr());
        }
        alloc_.free(p_value_wrapper);
      }
    }
  }
  return ret;
}

int ObSQLCCLRuleManager::match_keywords_in_sql(const ObString &sql,
                                              const ObIArray<ObString> &ccl_keywords_array,
                                              bool &match, bool case_sensitive) const
{
  int ret = OB_SUCCESS;
  if (!ccl_keywords_array.empty()) {
    const char *text_ptr = sql.ptr();
    uint32_t text_len = sql.length();
    for (size_t i = 0; match && i < ccl_keywords_array.count(); i++) {
      char *new_text = static_cast<char*>(MEMMEM(text_ptr, text_len,
                                               ccl_keywords_array.at(i).ptr(),
                                               ccl_keywords_array.at(i).length()));
      text_len -= new_text != NULL ? new_text - text_ptr + ccl_keywords_array.at(i).length() : 0;
      if (OB_UNLIKELY(text_len < 0)) {
        match = false;
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected result of memmem", K(sql),
                    K(ccl_keywords_array.at(i)));
      } else {
        match = new_text != NULL;
        text_ptr = new_text + ccl_keywords_array.at(i).length();
      }
    }
  }
  return ret;
}

int ObSQLCCLRuleManager::is_whitelist_sql(const ObString &sql, bool &match) const {
  int ret = OB_SUCCESS;
  for (size_t i = 0; i < whitelist_keywords_array_.count(); i++) {
    if (OB_FAIL(match_keywords_in_sql(sql, whitelist_keywords_array_.at(i), match))) {
      LOG_WARN("fail to match keywords in sql", K(ret));
    } else if (match) {
      break;
    }
  }
  return ret;
}

int ObSQLCCLRuleManager::match_ccl_rule(
  ObIAllocator &alloc, const ObString &username, const ObString &sql,
  ObCCLAffectDMLType sql_dml_type, const common::hash::ObHashSet<ObCCLDatabaseTableHashWrapper> &sql_relate_databases,
  const common::hash::ObHashSet<ObCCLDatabaseTableHashWrapper> &sql_relate_tables, ObCCLRuleSchema &ccl_rule,
  bool &match) const
{
  int ret = OB_SUCCESS;
  LOG_TRACE("match ccl rule schema:", K(ccl_rule));
  // 0. username and dml
  match = ccl_rule.get_affect_user_name() == "%" || ccl_rule.get_affect_user_name() == username;
  if (match) {
    ObString ipstr;
    if (ccl_rule.get_affect_host() == "%") {
      //do nothing, ccl affect for all ip
    } else if (OB_FAIL(observer::ObServerUtils::get_server_ip(&alloc, ipstr))) {
      SERVER_LOG(ERROR, "get server ip failed", K(ret));
    } else {
      match = ccl_rule.get_affect_host() == ipstr;
    }
  }

  // 1. dml
  if (match) {
    match = ccl_rule.get_affect_dml() == ObCCLAffectDMLType::ALL
            || ccl_rule.get_affect_dml() == sql_dml_type;
  }

  // 2. match database
  if (match) {
    if (ccl_rule.affect_for_all_databases()) {
      // rule applay to all database
    } else if (OB_FAIL(sql_relate_databases.exist_refactored(
                   ObCCLDatabaseTableHashWrapper(
                       ccl_rule.get_tenant_id(), ccl_rule.get_name_case_mode(),
                       ccl_rule.get_affect_database())))) {
      if (ret == OB_HASH_EXIST) {
        ret = OB_SUCCESS;
        match = true;
      } else if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        match = false;
      } else {
        LOG_WARN("hash set exist refactored failed", K(ret));
      }
    }
  }
  // 3. match table
  if (match) {
    if (ccl_rule.affect_for_all_tables()) {
      // rule applay to all table, do nothing
    } else if (OB_FAIL(sql_relate_tables.exist_refactored(
                   ObCCLDatabaseTableHashWrapper(
                       ccl_rule.get_tenant_id(), ccl_rule.get_name_case_mode(),
                       ccl_rule.get_affect_table())))) {
      if (ret == OB_HASH_EXIST) {
        ret = OB_SUCCESS;
        match = true;
      } else if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        match = false;
      } else {
        LOG_WARN("hash set exist refactored failed", K(ret));
      }
    }
  }
  // 4. match keywords and check whether match whitelist keywords
  bool match_whitelist_keywords = true;
  ObString sql_upper_case;
  if (match && OB_FAIL(match_keywords_in_sql(sql, ccl_rule.get_ccl_keywords_array(), match))) {
    LOG_WARN("fail to match keywords in sql", K(ret));
  } else if (match && OB_FAIL(ob_simple_low_to_up(alloc, sql, sql_upper_case))) {
    LOG_WARN("fail to convert sql to upper case", K(ret));
  } else if (match && OB_FAIL(is_whitelist_sql(sql_upper_case, match_whitelist_keywords))) {
    LOG_WARN("fail to match keywords in sql", K(ret));
  } else {
    match = match && !match_whitelist_keywords;
  }
  return ret;
}

int ObSQLCCLRuleManager::match_ccl_rule_with_sql(ObIAllocator &alloc,
                                                 const ObString& user_name,
                                                 ObSqlCtx &sql_ctx,
                                                 const ObString &sql,
                                                 bool is_ps_mode,
                                                 const ObIArray<const common::ObObjParam *> &param_store,
                                                 const ObString &format_sqlid,
                                                 CclRuleContainsInfo contians_info,
                                                 ObCCLAffectDMLType sql_dml_type,
                                                 const common::hash::ObHashSet<ObCCLDatabaseTableHashWrapper> &sql_relate_databases,
                                                 const common::hash::ObHashSet<ObCCLDatabaseTableHashWrapper> &sql_relate_tables,
                                                 uint64_t &limited_by_ccl_rule_id,
                                                 ObSqlString &reconstruct_sql)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("current ccl instrest : ", K(user_name), K(sql), K(is_ps_mode), K(format_sqlid), K(contians_info), K(sql_dml_type));
  LOG_TRACE("current sql relate database ids: ", K(sql_relate_databases));
  LOG_TRACE("current sql relate table ids: ", K(sql_relate_tables));

  ObCCLRuleMgr::CCLRuleInfos *candidate_ccl_rules = NULL;
  if (OB_ISNULL(sql_ctx.schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_ctx.schema_guard_ is null ", K(ret));
  } else if (!sql_ctx.schema_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_ctx.schema_guard_ is not init ", K(ret));
  } else if (OB_FAIL(sql_ctx.schema_guard_->get_ccl_rule_infos(MTL_ID(), contians_info, candidate_ccl_rules))) {
    LOG_WARN("fail to get ccl_rule_infos from sql_ctx.schema_guard_", K(ret));
  } else {
    if (is_ps_mode) {
      LOG_TRACE("orignal ps sql:", K(sql));
      LOG_TRACE("after ccl reconstruct: ", K(reconstruct_sql));
    }

    bool match = false;
    FOREACH_X(ccl_rule_iter, *candidate_ccl_rules, OB_SUCC(ret)) {
      const ObCCLRuleSchema* ccl_rule_schema = nullptr;
      if (OB_FAIL(sql_ctx.schema_guard_->get_ccl_rule_with_ccl_rule_id(MTL_ID(), (*ccl_rule_iter)->get_ccl_rule_id(), ccl_rule_schema))) {
        LOG_WARN("fail to get ccl rule schema", K(ret));
      } else if (OB_FAIL(match_ccl_rule(alloc, user_name, is_ps_mode ? reconstruct_sql.string() : sql, sql_dml_type,
                                 sql_relate_databases, sql_relate_tables, *(const_cast<ObCCLRuleSchema*>(ccl_rule_schema)),
                                 match))) {
        LOG_WARN("fail to match ccl rule", K(sql), K(reconstruct_sql), K(sql_dml_type), K(ccl_rule_schema));
      } else if (match) {
        uint64_t matched_ccl_rule_id = ccl_rule_schema->get_ccl_rule_id();
        if (ccl_rule_schema->get_affect_scope() == ObCCLAffectScope::RULE_LEVEL) {
          ObFormatSQLIDCCLRuleKey ccl_rule_key(matched_ccl_rule_id, "");
          if (OB_FAIL(inc_rule_level_concurrency(ccl_rule_key, ccl_rule_schema->get_max_concurrency(),
                                                 sql_ctx.matched_ccl_rule_level_values_))) {
            if (OB_REACH_MAX_CCL_CONCURRENT_NUM == ret) {
              limited_by_ccl_rule_id = ccl_rule_schema->get_ccl_rule_id();
              LOG_USER_ERROR(OB_REACH_MAX_CCL_CONCURRENT_NUM,
                             ccl_rule_schema->get_ccl_rule_name().ptr(),
                             ccl_rule_schema->get_max_concurrency());
            } else {
              LOG_WARN("fail to inc rule level concurrency", K(ret));
            }
          }
        } else {
          ObFormatSQLIDCCLRuleKey format_sqlid_ccl_rule_key(matched_ccl_rule_id, format_sqlid);
          if (OB_FAIL(inc_format_sqlid_level_concurrency(
                  format_sqlid_ccl_rule_key,
                  ccl_rule_schema->get_max_concurrency(),
                  sql_ctx.matched_ccl_format_sqlid_level_values_))) {
            if (OB_REACH_MAX_CCL_CONCURRENT_NUM == ret) {
              limited_by_ccl_rule_id = ccl_rule_schema->get_ccl_rule_id();
              LOG_USER_ERROR(OB_REACH_MAX_CCL_CONCURRENT_NUM,
                             ccl_rule_schema->get_ccl_rule_name().ptr(),
                             ccl_rule_schema->get_max_concurrency());
            } else {
              LOG_WARN("fail to inc format sqlid level concurrency", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}
}
}