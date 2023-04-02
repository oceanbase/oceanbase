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

#ifndef OCEANBASE_SHARE_PARAMETER_OB_PARAMETER_ATTR_H_
#define OCEANBASE_SHARE_PARAMETER_OB_PARAMETER_ATTR_H_
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase {
namespace common {

#define DECL_EVAL_MACRO(macro, args...) macro(args)
#define DECL_ATTR_LIST(M)                                                              \
  DECL_EVAL_MACRO(M, Section, ROOT_SERVICE, LOAD_BALANCE, DAILY_MERGE, LOCATION_CACHE, \
                  SSTABLE, LOGSERVICE, CACHE, TRANS, TENANT, RPC, OBPROXY, OBSERVER, RESOURCE_LIMIT); \
  DECL_EVAL_MACRO(M, Scope, CLUSTER, TENANT);                                          \
  DECL_EVAL_MACRO(M, Source, DEFAULT, FILE, OBADMIN, CMDLINE, CLUSTER, TENANT);        \
  DECL_EVAL_MACRO(M, Session, NO, YES);                                                \
  DECL_EVAL_MACRO(M, VisibleLevel, SYS, COMMON, INVISIBLE);                            \
  DECL_EVAL_MACRO(M, EditLevel, READONLY, STATIC_EFFECTIVE, DYNAMIC_EFFECTIVE);        \
  DECL_EVAL_MACRO(M, CompatMode, MYSQL, ORACLE, COMMON);

#define _ENUM_EXP(arg) arg

#define DECL_ATTR(ATTR_CLS, args...)                                           \
typedef struct ATTR_CLS {                                                      \
  enum ATTR_CLS ## Info {                                                      \
    LST_DO(_ENUM_EXP, (,), args)                                               \
  };                                                                           \
  static const char *VALUES[];                                                 \
} ATTR_CLS;

DECL_ATTR_LIST(DECL_ATTR);

// TODO: whether we need this
struct InfluencePlan {};
struct NeedSerialize {};

class ObParameterAttr
{
public:
  ObParameterAttr() : section_(Section::OBSERVER),
                      scope_(Scope::CLUSTER),
                      source_(Source::DEFAULT),
                      session_(Session::NO),
                      visible_level_(VisibleLevel::COMMON),
                      edit_level_(EditLevel::DYNAMIC_EFFECTIVE),
                      compat_mode_(CompatMode::COMMON) {}

  // constructor without scope, session, visible_level and compat_mode
  ObParameterAttr(Section::SectionInfo section_info,
                  Source::SourceInfo source_info,
                  EditLevel::EditLevelInfo edit_level_info)
                : section_(section_info), scope_(Scope::CLUSTER),
                  source_(source_info), session_(Session::NO),
                  visible_level_(VisibleLevel::COMMON),
                  edit_level_(edit_level_info),
                  compat_mode_(CompatMode::COMMON) {}

  void set_scope(Scope::ScopeInfo scope_info) { scope_ = scope_info; }

  const char *get_section() const { return Section::VALUES[section_]; }
  const char *get_scope() const { return Scope::VALUES[scope_]; }
  const char *get_source() const { return Source::VALUES[source_]; }
  const char *get_session() const { return Session::VALUES[session_]; }
  const char *get_visible_level() const { return VisibleLevel::VALUES[visible_level_]; }
  const char *get_edit_level() const { return EditLevel::VALUES[edit_level_]; }
  const char *get_compat_mode() const { return CompatMode::VALUES[compat_mode_]; }
  bool is_static() const;
  bool is_readonly() const;
  bool is_invisible() const;

private:
  Section::SectionInfo section_;
  Scope::ScopeInfo scope_;
  Source::SourceInfo source_;
  Session::SessionInfo session_;
  VisibleLevel::VisibleLevelInfo visible_level_;
  EditLevel::EditLevelInfo edit_level_;
  CompatMode::CompatModeInfo compat_mode_;
};

} // common
} // oceanbase

#endif
