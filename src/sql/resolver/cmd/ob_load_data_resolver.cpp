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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_load_data_resolver.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/dml/ob_default_value_utils.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/resolver/ob_resolver.h"
#include "lib/json/ob_json.h"
#include "lib/json/ob_json_print_utils.h"

namespace oceanbase {
using namespace oceanbase::common;
using namespace share::schema;

namespace sql {
/*

LOAD DATA [LOW_PRIORITY | CONCURRENT] [LOCAL] INFILE 'file_name'
    [REPLACE | IGNORE]
    INTO TABLE tbl_name
    [PARTITION (partition_name [, partition_name] ...)]
    [CHARACTER SET charset_name]
    [{FIELDS | COLUMNS}
        [TERMINATED BY 'string']
        [[OPTIONALLY] ENCLOSED BY 'char']
        [ESCAPED BY 'char']
    ]
    [LINES
        [STARTING BY 'string']
        [TERMINATED BY 'string']
    ]
    [IGNORE number {LINES | ROWS}]
    [(col_name_or_user_var
        [, col_name_or_user_var] ...)]
    [SET col_name={expr | DEFAULT},
        [, col_name={expr | DEFAULT}] ...]
*/

int ObLoadDataResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode* node = const_cast<ParseNode*>(&parse_tree);
  ObLoadDataStmt* load_stmt = NULL;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;

  if (OB_ISNULL(node) || OB_UNLIKELY(T_LOAD_DATA != node->type_) || OB_UNLIKELY(ENUM_TOTAL_COUNT != node->num_child_) ||
      OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP_(session_info), K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", K(ret));
  } else if (OB_ISNULL(load_stmt = create_stmt<ObLoadDataStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create select stmt");
  } else {
    stmt_ = load_stmt;
  }

  if (OB_SUCC(ret)) {
    /* 0. opt_load_local */
    ObLoadArgument& load_args = load_stmt->get_load_arguments();
    ParseNode* opt_load_local = node->children_[ENUM_OPT_LOCAL];
    if (OB_NOT_NULL(opt_load_local)) {
      switch (opt_load_local->type_) {
        case T_REMOTE_OSS:
          load_args.load_file_storage_ = ObLoadFileLocation::OSS;
          break;
        case T_LOCAL:
          // load_args.load_file_storage_ = ObLoadFileLocation::CLIENT_DISK;
          // break;
          // not support local
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "load data local");
      }
    } else {
      load_args.load_file_storage_ = ObLoadFileLocation::SERVER_DISK;
    }
  }

  if (OB_SUCC(ret)) {
    /* 1. file name */
    ObLoadArgument& load_args = load_stmt->get_load_arguments();
    ParseNode* file_name_node = node->children_[ENUM_FILE_NAME];
    if (OB_ISNULL(file_name_node) || OB_UNLIKELY(T_VARCHAR != file_name_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node", "child", file_name_node);
    } else {
      ObString file_name(file_name_node->str_len_, file_name_node->str_value_);
      if (ObLoadFileLocation::OSS != load_args.load_file_storage_) {
        load_args.file_name_ = file_name;
        char* full_path_buf = nullptr;
        char* actual_path = nullptr;
        if (OB_ISNULL(full_path_buf = static_cast<char*>(allocator_->alloc(DEFAULT_BUF_LENGTH)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_ISNULL(actual_path = realpath(file_name_node->str_value_, full_path_buf))) {
          ret = OB_FILE_NOT_EXIST;
          LOG_WARN("file not exist", K(ret), K(file_name));
        } else {
          load_args.full_file_path_ = actual_path;
        }
        // security check for mysql mode
        if (OB_SUCC(ret) && share::is_mysql_mode()) {
          ObString secure_file_priv;
          ObString full_file_path = load_args.full_file_path_;
          char buf[DEFAULT_BUF_LENGTH];
          if (OB_FAIL(session_info_->get_secure_file_priv(secure_file_priv))) {
            LOG_WARN("fail to get secure file priv", K(ret));
          } else if (0 == secure_file_priv.case_compare(N_NULL)) {
            ret = OB_ERR_NO_PRIVILEGE;
            LOG_WARN("no priv", K(ret), K(secure_file_priv), K(full_file_path));
          } else if (secure_file_priv.empty()) {
            // pass security check
          } else {
             /* here is to check when "set global secure_file_priv=real_secure_file_path"
            *  1. real_secure_file_path should be a legal dir
            *  2. real_secure_file_path shoule own the same 'priv' with full_file_path
            */
            struct stat path_stat;
            char real_secure_file_path[DEFAULT_BUF_LENGTH];
            MEMSET(real_secure_file_path, 0, sizeof(real_secure_file_path));
            char* buf = NULL;
            if (NULL == (buf = realpath(to_cstring(secure_file_priv), real_secure_file_path))) {
                // pass
            } else if (0 != stat(real_secure_file_path, &path_stat)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("stat error", K(ret), K(secure_file_priv));
            } else if (!S_ISDIR(path_stat.st_mode)) {
              ret = OB_ERR_NO_PRIVILEGE;
              LOG_WARN("no priv", K(ret), K(secure_file_priv), K(full_file_path));
            } else {
            //check  exist the same 'prev'
              int64_t data_len = strlen(real_secure_file_path);
              // case like "set global secure_file_priv= '/tmp/' " should be valid
              if (data_len < DEFAULT_BUF_LENGTH && real_secure_file_path[data_len - 1] != '/') {
                real_secure_file_path[data_len++] = '/';
              } 
              if(full_file_path.length() < data_len || 0 != MEMCMP(real_secure_file_path, full_file_path.ptr(), data_len)){
                  ret = OB_ERR_NO_PRIVILEGE;
                  LOG_WARN("no priv", K(ret), K(secure_file_priv), K(full_file_path));
              }
            }
          }
        }
      } else {
        load_args.file_name_ = file_name.split_on('?');
        load_args.access_info_ = file_name;
        if (load_args.file_name_.length() <= 0 || load_args.access_info_ <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file name or access key");
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    /* 2. opt_duplicate */
    ObLoadArgument& load_args = load_stmt->get_load_arguments();
    ObLoadDupActionType dupl_action = ObLoadDupActionType::LOAD_STOP_ON_DUP;
    if (NULL == node->children_[ENUM_DUPLICATE_ACTION]) {
      if (ObLoadFileLocation::CLIENT_DISK == load_args.load_file_storage_) {
        dupl_action = ObLoadDupActionType::LOAD_IGNORE;
      }
    } else if (T_IGNORE == node->children_[ENUM_DUPLICATE_ACTION]->type_) {
      dupl_action = ObLoadDupActionType::LOAD_IGNORE;
    } else if (T_REPLACE == node->children_[ENUM_DUPLICATE_ACTION]->type_) {
      dupl_action = ObLoadDupActionType::LOAD_REPLACE;
    } else {
      dupl_action = ObLoadDupActionType::LOAD_INVALID_MODE;
      ret = OB_ERR_UNEXPECTED;
      // should not be here, parser will put error before this
      LOG_WARN("unknown dumplicate settings", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (share::is_oracle_mode() &&
          (ObLoadDupActionType::LOAD_IGNORE == dupl_action || ObLoadDupActionType::LOAD_REPLACE == dupl_action)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "load data with ignore or replace option in oracle mode");
      }
      load_args.dupl_action_ = dupl_action;
    }
  }

  if (OB_SUCC(ret)) {
    /* 3. table name */
    ObLoadArgument& load_args = load_stmt->get_load_arguments();
    ObString& database_name = load_args.database_name_;
    ObString& table_name = load_args.table_name_;
    uint64_t& table_id = load_args.table_id_;
    uint64_t database_id = session_info_->get_database_id();
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    bool cte_table_fisrt = false;
    if (OB_ISNULL(node->children_[ENUM_TABLE_NAME])) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else if (OB_FAIL(resolve_table_relation_node(
                   parse_tree.children_[ENUM_TABLE_NAME], table_name, load_args.database_name_))) {
      SQL_RESV_LOG(WARN, "failed to resolve table name", K(table_name), K(database_name), K(ret));
    } else if (OB_FAIL(check_if_table_exists(tenant_id, database_name, table_name, cte_table_fisrt, table_id))) {
      SQL_RESV_LOG(WARN, "table not exist", K(ret));
    } else {
      load_args.database_id_ = database_id;
      load_args.tenant_id_ = tenant_id;
      int32_t size = table_name.length() + database_name.length() + 6;  //  eg: `test`.`t1`
      char* buf = NULL;
      int64_t pos = 0;
      if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(size * sizeof(char))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for database and table name failed", K(ret));
      } else if (OB_FAIL(databuff_printf(buf,
                     size,
                     pos,
                     share::is_oracle_mode() ? "\"%.*s\".\"%.*s\"" : "`%.*s`.`%.*s`",
                     database_name.length(),
                     database_name.ptr(),
                     table_name.length(),
                     table_name.ptr()))) {
        LOG_WARN("fail to print combined name", K(ret), K(size), K(pos));
      } else {
        load_args.combined_name_.assign_ptr(buf, pos);
      }
      LOG_DEBUG("resovle table info result", K(tenant_id), K(database_name), K(table_name));
    }
  }

  if (OB_SUCC(ret)) {
    /* 4. opt_charset */
    ObLoadArgument& load_args = load_stmt->get_load_arguments();
    const ParseNode* child_node = node->children_[ENUM_OPT_CHARSET];
    if (NULL != child_node) {
      if (OB_UNLIKELY(1 != child_node->num_child_) || OB_ISNULL(child_node->children_) ||
          OB_ISNULL(child_node->children_[0]) || T_SET_CHARSET == child_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child node", K(child_node));
      } else if (T_DEFAULT == child_node->children_[0]->type_) {
        load_args.is_default_charset_ = true;
      } else {
        load_args.charset_.assign_ptr(
            child_node->children_[0]->str_value_, static_cast<int32_t>(child_node->children_[0]->str_len_));
        load_args.is_default_charset_ = false;
      }
    } else {
      load_args.is_default_charset_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    /* 5. opt_field */
    ObDataInFileStruct& data_struct_in_file = load_stmt->get_data_struct_in_file();
    bool no_default_escape = false;
    IS_NO_BACKSLASH_ESCAPES(session_info_->get_sql_mode(), no_default_escape);
    if (no_default_escape) {
      data_struct_in_file.field_escaped_char_ = INT64_MAX;
      data_struct_in_file.field_escaped_str_ = "";
    }
    const ParseNode *child_node = node->children_[ENUM_OPT_FIELD];
    if (NULL != child_node) {
      if (T_INTO_FIELD_LIST != child_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to resolve field_list_node", K(ret), KP(child_node));
      } else if (OB_FAIL(resolve_field_list_node(*child_node, data_struct_in_file))) {
        LOG_WARN("failed to resolve field_list_node", K(ret), KP(child_node));
      }
    }
  }

  if (OB_SUCC(ret)) {
    /* 6. opt_line */
    ObDataInFileStruct& data_struct_in_file = load_stmt->get_data_struct_in_file();
    const ParseNode* child_node = node->children_[ENUM_OPT_LINE];
    if (NULL != child_node) {
      if (T_INTO_LINE_LIST != child_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to resolve line_list_node", K(ret), KP(child_node));
      } else if (OB_FAIL(resolve_line_list_node(*child_node, data_struct_in_file))) {
        LOG_WARN("failed to resolve line_list_node", K(ret), KP(child_node));
      }
    }
  }

  if (OB_SUCC(ret)) {
    /* 7. ignore rows */
    ObLoadArgument& load_args = load_stmt->get_load_arguments();
    const ParseNode* child_node = node->children_[ENUM_OPT_IGNORE_ROWS];
    if (NULL != child_node) {
      if (T_IGNORE_ROWS != child_node->type_ && T_GEN_ROWS != child_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to resolve ignore rows", K(ret), K(child_node));
      } else if (OB_UNLIKELY(1 != child_node->num_child_) || OB_ISNULL(child_node->children_) ||
                 OB_ISNULL(child_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child node", K(child_node));
      } else {
        int64_t ignore_row_num = child_node->children_[0]->value_;
        load_args.ignore_rows_ = ignore_row_num > 0 ? ignore_row_num : 0;
      }
    }
  }

  if (OB_SUCC(ret)) {
    /* 8. opt_field_or_var_spec */
    const ParseNode* child_node = node->children_[ENUM_OPT_FIELD_OR_VAR];
    if (NULL == child_node) {  // default insert into all columns
      if (OB_FAIL(resolve_empty_field_or_var_list_node(*load_stmt))) {
        LOG_WARN("resolve empty field var list failed", K(ret));
      } else {
        load_stmt->set_default_table_columns();
      }
    } else {
      if (OB_FAIL(resolve_field_or_var_list_node(*child_node, case_mode, *load_stmt))) {
        LOG_WARN("failed to resolve field or var list_node", K(ret), K(child_node));
      }
    }
  }

  if (OB_SUCC(ret)) {
    /* 9. opt_load_set_spec */
    const ParseNode* child_node = node->children_[ENUM_OPT_SET_FIELD];
    if (NULL != child_node) {
      /*
      if (T_VALUE_LIST != child_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to resolve set clause", K(ret), K(child_node));
      } else if (OB_FAIL(resolve_set_clause(*child_node, case_mode, *load_stmt))) {
        LOG_WARN("failed to resolve set_var list_node", K(ret));
      }
      */
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "load data set");
    }
  }

  if (OB_SUCC(ret)) {
    /* 10. opt_hint */
    const ParseNode* child_node = node->children_[ENUM_OPT_HINT];
    if (OB_NOT_NULL(child_node)) {
      if (OB_FAIL(resolve_hints(*child_node))) {
        LOG_WARN("fail to resolve hints", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(validate_stmt(load_stmt))) {
      LOG_WARN("failed to validate stmt");
    }
  }

  LOG_DEBUG("finish resolve load_data", KPC(load_stmt), K(ret));
  return ret;
}

bool is_ascii_str(ObString& my_str)
{
  bool ret_bool = true;
  if (!my_str.empty()) {
    for (int64_t i = 0; ret_bool && i < my_str.length(); ++i) {
      if (!ob_isascii(my_str[i])) {
        ret_bool = false;
      }
    }
  }
  return ret_bool;
}

int ObLoadDataResolver::resolve_hints(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  ObLoadDataStmt* stmt = NULL;

  if (OB_ISNULL(stmt = static_cast<ObLoadDataStmt*>(get_basic_stmt()))) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt not created", K(ret));
  } else if (node.type_ != T_HINT_OPTION_LIST) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid node type", K(node.type_), K(ret));
  } else {
    ObLoadDataHint& stmt_hints = stmt->get_hints();

    for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; i++) {
      ParseNode* hint_node = node.children_[i];
      if (!hint_node) {
        continue;
      }
      LOG_DEBUG("LOAD DATA resolve hint node", "type", hint_node->type_);

      switch (hint_node->type_) {

        case T_QUERY_TIMEOUT: {
          int64_t timeout_value = hint_node->children_[0]->value_;
          if (timeout_value > OB_MAX_USER_SPECIFIED_TIMEOUT) {
            timeout_value = OB_MAX_USER_SPECIFIED_TIMEOUT;
            LOG_USER_WARN(OB_ERR_TIMEOUT_TRUNCATED);
          }
          if (OB_FAIL(stmt_hints.set_value(ObLoadDataHint::QUERY_TIMEOUT, timeout_value))) {
            LOG_WARN("fail to set timeout", K(ret), K(timeout_value));
          }
          break;
        }
        case T_LOG_LEVEL: {
          const char* str = hint_node->children_[0]->str_value_;
          int32_t length = static_cast<int32_t>(hint_node->children_[0]->str_len_);
          if (NULL != str) {
            int tmp_ret = OB_SUCCESS;
            ObString log_level(length, str);
            if (0 == log_level.case_compare("disabled")) {
              // allowed for variables
            } else if (OB_UNLIKELY(tmp_ret = OB_LOGGER.parse_check(str, length))) {
              LOG_WARN("Log level parse check error", K(tmp_ret));
            } else if (OB_FAIL(stmt_hints.set_value(ObLoadDataHint::LOG_LEVEL, ObString(length, str)))) {
              LOG_WARN("fail to set log level", K(ret), K(length));
            }
          }
          break;
        }
        case T_LOAD_BATCH_SIZE: {
          if (1 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("max concurrent node should have 1 child", K(ret));
          } else if (OB_ISNULL(hint_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child of max concurrent node should not be NULL", K(ret));
          } else if (OB_FAIL(stmt_hints.set_value(ObLoadDataHint::BATCH_SIZE, hint_node->children_[0]->value_))) {
            LOG_WARN("fail to set concurrent value", K(ret));
          }
          break;
        }
        case T_PARALLEL: {
          if (1 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("stmt parallel degree node should have 1 child", K(ret), K(hint_node->num_child_));
          } else if (OB_ISNULL(hint_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child of stmt parallel degree node should not be NULL", K(ret));
          } else if (OB_FAIL(stmt_hints.set_value(ObLoadDataHint::PARALLEL_THREADS, hint_node->children_[0]->value_))) {
            LOG_WARN("fail to set concurrent value", K(ret));
          } else {
            LOG_DEBUG("LOAD DATA resolve parallel", "value", hint_node->children_[0]->value_);
          }
          break;
        }
        default:
          ret = OB_ERR_HINT_UNKNOWN;
          LOG_WARN("Unknown hint", "hint_name", get_type_name(hint_node->type_));
          break;
      }
    }
  }

  return ret;
}

// validation for loaddata statement obeys the following rules:
// 0. in loaddata Ver1, only ascii charset are supported.
// 1. according to the defined charset, escaped and enclosed valid char length should <= 1.
// 2. field/line separators are recognized in the same way as mysql does
// 3. escaped and enclosed str length > 1 or the string not start with an ascii char,
//   push a warning, indicating it is not a ascii char.
int ObLoadDataResolver::validate_stmt(ObLoadDataStmt* stmt)
{
  int ret = OB_SUCCESS;
  int64_t field_sep_char;
  int64_t escape_char;

  // int64_t line_sep_char;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else {
    ObDataInFileStruct& data_struct_in_file = stmt->get_data_struct_in_file();

    if (share::is_oracle_mode()) {
      ObResolverUtils::escape_char_for_oracle_mode(data_struct_in_file.field_enclosed_str_);
      ObResolverUtils::escape_char_for_oracle_mode(data_struct_in_file.field_escaped_str_);
      ObResolverUtils::escape_char_for_oracle_mode(data_struct_in_file.field_term_str_);
      ObResolverUtils::escape_char_for_oracle_mode(data_struct_in_file.line_start_str_);
      ObResolverUtils::escape_char_for_oracle_mode(data_struct_in_file.line_term_str_);
      LOG_DEBUG("LOAD DATA : do mannual escape in oracle mode", K(data_struct_in_file));
    }

    if (OB_UNLIKELY(data_struct_in_file.field_enclosed_str_.length() > 1) ||
        OB_UNLIKELY(data_struct_in_file.field_escaped_str_.length() > 1)) {
      ret = OB_WRONG_FIELD_TERMINATORS;

    } else {
      if ((!data_struct_in_file.field_enclosed_str_.empty() &&
              !ob_isascii(data_struct_in_file.field_enclosed_str_[0])) ||
          (!data_struct_in_file.field_escaped_str_.empty() && !ob_isascii(data_struct_in_file.field_escaped_str_[0])) ||
          !is_ascii_str(data_struct_in_file.field_term_str_) || !is_ascii_str(data_struct_in_file.line_start_str_) ||
          !is_ascii_str(data_struct_in_file.line_term_str_)) {
        /*
          MySQL: //from sql_class.cc select_export::prepare
          Current LOAD DATA INFILE recognizes field/line separators "as is" without
          converting from client charset to data file charset. So, it is supposed,
          that input file of LOAD DATA INFILE consists of data in one charset and
          separators in other charset. For the compatibility with that [buggy]
          behaviour SELECT INTO OUTFILE implementation has been saved "as is" too,
          but the new warning message has been added:

            Non-ASCII separator arguments are not supported
         */
        /*
         * LOAD DATA INFILE for OB is not support non-ascii separators now!
         */
        ret = OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED;
        LOG_USER_ERROR(OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED);
      }
      // field enclose char
      field_sep_char = (data_struct_in_file.field_enclosed_str_.empty()
                            ? INT64_MAX
                            : static_cast<int64_t>(data_struct_in_file.field_enclosed_str_[0]));
      // field escape char
      escape_char = (data_struct_in_file.field_escaped_str_.empty()
                         ? INT64_MAX
                         : static_cast<int64_t>(data_struct_in_file.field_escaped_str_[0]));
      /*
      if (OB_SUCC(ret)) {
        if (escape_char != ObDataInFileStruct::DEFAULT_FIELD_ESCAPED_CHAR) {
          ret = OB_WRONG_FIELD_TERMINATORS;
          LOG_USER_ERROR(OB_WRONG_FIELD_TERMINATORS);
        }
      }
      */
      if (OB_SUCC(ret)) {
        const char* is_ambiguous_field_sep = strchr("ntrb0ZN", static_cast<int>(field_sep_char));
        const char* is_unsafe_field_sep = strchr(".0123456789e+-", static_cast<int>(field_sep_char));
        if (OB_NOT_NULL(is_ambiguous_field_sep) || OB_NOT_NULL(is_unsafe_field_sep)) {
          ret = OB_WARN_AMBIGUOUS_FIELD_TERM;
        }
      }
      data_struct_in_file.field_enclosed_char_ = field_sep_char;
      data_struct_in_file.field_escaped_char_ = escape_char;
    }
    if (OB_SUCC(ret)) {
      stmt->get_load_arguments().is_csv_format_ =
          (1 == data_struct_in_file.line_term_str_.length() && 1 == data_struct_in_file.field_term_str_.length() &&
              0 == data_struct_in_file.line_start_str_.length() && stmt->get_table_assignment().count() == 0);
    }
    LOG_DEBUG("LOAD DATA : data_struct_in_file validation done", K(data_struct_in_file));
  }
  return ret;
}

int ObLoadDataResolver::resolve_field_node(
    const ParseNode& node, const ObNameCaseMode case_mode, ObLoadDataStmt& load_stmt)
{
  int ret = OB_SUCCESS;
  ObQualifiedName q_name;
  const ObColumnSchemaV2* col_schema = NULL;
  ObString& database_name = load_stmt.get_load_arguments().database_name_;
  uint64_t table_id = load_stmt.get_load_arguments().table_id_;
  if (OB_UNLIKELY(T_COLUMN_REF != node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not T_COLUMN_LIST", K(ret), K(node.type_));
  } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(&node, case_mode, q_name))) {
    LOG_WARN("failed to resolve column def", K(ret));
  } else if ((q_name.database_name_.length() > 0 && q_name.database_name_.case_compare(database_name) != 0) ||
             (q_name.tbl_name_.length() > 0 && q_name.tbl_name_.case_compare(database_name) != 0)) {
    // TODO: check error return code
    ret = OB_ERR_BAD_FIELD_ERROR;
    ObString column_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
    LOG_USER_ERROR(
        OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(), (int)strlen("field list"), "field list");
    LOG_WARN("unknown column in field list", K(column_name));
  } else if (OB_FAIL(get_column_schema(table_id, q_name.col_name_, col_schema, false))) {
    LOG_WARN("get column schema failed", K(ret), K(q_name.tbl_name_), K(q_name.col_name_));
  } else if (OB_ISNULL(col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null");
  } else {
    ObIArray<ObLoadDataStmt::FieldOrVarStruct>& field_or_var_list = load_stmt.get_field_or_var_list();
    ObLoadDataStmt::FieldOrVarStruct tmp_struct;
    tmp_struct.is_table_column_ = true;
    tmp_struct.field_or_var_name_ = q_name.col_name_;
    tmp_struct.column_id_ = col_schema->get_column_id();
    tmp_struct.column_type_ = col_schema->get_data_type();
    if (OB_FAIL(field_or_var_list.push_back(tmp_struct))) {
      LOG_WARN("failed to push back item", K(ret));
    }
  }
  return ret;
}

int ObLoadDataResolver::resolve_user_vars_node(const ParseNode& node, ObLoadDataStmt& load_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_USER_VARIABLE_IDENTIFIER != node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not T_USER_VARIABLE_IDENTIFIER", K(ret), K(node.type_));
  } else if (OB_UNLIKELY(1 != node.num_child_) || OB_ISNULL(node.children_) || OB_ISNULL(node.children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid node", K(node.num_child_), K(node.children_), K(ret));
  } else {
    ObIArray<ObLoadDataStmt::FieldOrVarStruct>& field_or_var_list = load_stmt.get_field_or_var_list();
    ObString user_var;
    user_var.assign_ptr(
        const_cast<char*>(node.children_[0]->str_value_), static_cast<int32_t>(node.children_[0]->str_len_));
    ObLoadDataStmt::FieldOrVarStruct tmp_struct;
    tmp_struct.is_table_column_ = false;
    tmp_struct.field_or_var_name_.assign_ptr(
        const_cast<char*>(node.children_[0]->str_value_), static_cast<int32_t>(node.children_[0]->str_len_));
    tmp_struct.column_type_ = ColumnType::ObMaxType;  // unknown type
    tmp_struct.column_id_ = OB_INVALID_ID;
    if (OB_FAIL(field_or_var_list.push_back(tmp_struct))) {
      LOG_WARN("failed to push back item", K(ret));
    }
  }
  return ret;
}

int ObLoadDataResolver::resolve_empty_field_or_var_list_node(ObLoadDataStmt& load_stmt)
{
  int ret = OB_SUCCESS;
  ObIArray<ObLoadDataStmt::FieldOrVarStruct>& field_or_var_list = load_stmt.get_field_or_var_list();
  uint64_t table_id = load_stmt.get_load_arguments().table_id_;
  const ObTableSchema* table_schema = NULL;
  if (OB_FAIL(schema_checker_->get_table_schema(table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret));
  } else {
    int64_t column_count = table_schema->get_column_count();
    const ObColumnSchemaV2* column_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      if (OB_ISNULL(column_schema = table_schema->get_column_schema_by_idx(i))) {
        LOG_WARN("get column schema failed", K(ret));
      } else if (!column_schema->is_hidden()) {
        ObLoadDataStmt::FieldOrVarStruct tmp_struct;
        tmp_struct.is_table_column_ = true;
        tmp_struct.field_or_var_name_ = column_schema->get_column_name_str();
        tmp_struct.column_id_ = column_schema->get_column_id();
        tmp_struct.column_type_ = column_schema->get_data_type();
        if (OB_FAIL(field_or_var_list.push_back(tmp_struct))) {
          LOG_WARN("failed to push back item", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLoadDataResolver::resolve_field_or_var_list_node(
    const ParseNode& node, const ObNameCaseMode case_mode, ObLoadDataStmt& load_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_COLUMN_LIST != node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not T_COLUMN_LIST", K(ret), K(node.type_));
  } else if (OB_UNLIKELY(node.num_child_ <= 0) || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child node", K(node.num_child_), K(ret));
  } else {
    const ParseNode* child_node = NULL;
    for (int32_t i = 0; i < node.num_child_ && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(child_node = node.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid grand child node", K(child_node), K(i), K(ret));
      } else if (T_COLUMN_REF == child_node->type_) {
        if (OB_FAIL(resolve_field_node(*child_node, case_mode, load_stmt))) {
          LOG_WARN("failed to resolve field node", K(ret));
        }
      } else if (T_USER_VARIABLE_IDENTIFIER == child_node->type_) {
        if (OB_FAIL(resolve_user_vars_node(*child_node, load_stmt))) {
          LOG_WARN("failed to resolve user vars node", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("resolve field var list failed", K(ret), K(child_node));
      }
    }  // end of for
  }
  return ret;
}

int ObLoadDataResolver::resolve_set_clause(
    const ParseNode& node, const ObNameCaseMode case_mode, ObLoadDataStmt& load_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_VALUE_LIST != node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not T_VALUE_LIST", K(ret), K(node.type_));
  } else if (OB_UNLIKELY(node.num_child_ <= 0) || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child node", K(node.num_child_), K(ret));
  } else {
    const ParseNode* child_node = NULL;
    for (int32_t i = 0; i < node.num_child_ && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(child_node = node.children_[i]) || T_ASSIGN_ITEM != child_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid grand child node", K(child_node), K(i), K(ret));
      } else if (OB_FAIL(resolve_each_set_node(*child_node, case_mode, load_stmt))) {
        LOG_WARN("failed to resolve field node", K(ret), K(child_node));
      }
    }  // end of for
  }
  return ret;
}

int ObLoadDataResolver::build_column_ref_expr(ObQualifiedName& q_name, ObRawExpr*& column_expr)
{
  int ret = OB_SUCCESS;
  ObLoadDataStmt* load_stmt = static_cast<ObLoadDataStmt*>(stmt_);
  uint64_t table_id = load_stmt->get_load_arguments().table_id_;
  const ObString& db_name = load_stmt->get_load_arguments().table_name_;
  const ObString& tb_name = load_stmt->get_load_arguments().table_name_;
  const ObColumnSchemaV2* col_schema = NULL;
  ObColumnRefRawExpr* col_expr = NULL;
  // check DB name and TABLE name
  // for load data, any column should belong to the loaded table
  if ((q_name.database_name_.length() > 0 && q_name.database_name_.compare(db_name) != 0) ||
      (q_name.tbl_name_.length() > 0 && q_name.tbl_name_.compare(tb_name) != 0)) {
    // TODO: check error return code
    // ERROR 1054 (42S22): Unknown column 'xxx' in 'field list'
    ret = OB_ERR_BAD_FIELD_ERROR;
    ObString column_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
    LOG_USER_ERROR(
        OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(), (int)strlen("field list"), "field list");
    LOG_WARN("unknown column in field list",
        K(q_name.database_name_),
        K(q_name.tbl_name_),
        K(q_name.col_name_),
        K(db_name),
        K(tb_name));
  } else if (OB_FAIL(get_column_schema(table_id, q_name.col_name_, col_schema, false))) {
    LOG_WARN("get column schema failed", K(ret), K(q_name.tbl_name_), K(q_name.col_name_));
  } else if (OB_ISNULL(col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null");
  } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*params_.expr_factory_, *col_schema, col_expr))) {
    LOG_WARN("build column expr failed", K(ret));
  } else {
    col_expr->set_column_attr(tb_name, q_name.col_name_);
    col_expr->set_database_name(db_name);
    col_expr->set_ref_id(table_id, col_schema->get_column_id());
    col_expr->set_data_type(col_schema->get_data_type());
    column_expr = col_expr;

    ColumnItem column_item;
    column_item.set_default_value(col_schema->get_cur_default_value());
    column_item.expr_ = col_expr;
    column_item.table_id_ = col_expr->get_table_id();
    column_item.column_id_ = col_expr->get_column_id();
    column_item.column_name_ = col_expr->get_column_name();
    if (OB_FAIL(load_stmt->add_column_item(column_item))) {
      LOG_WARN("add column item failed", K(ret));
    }
  }
  return ret;
}

int ObLoadDataResolver::resolve_column_ref_expr(ObIArray<ObQualifiedName>& columns, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* real_ref_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ObQualifiedName& q_name = columns.at(i);
    if (OB_FAIL(build_column_ref_expr(q_name, real_ref_expr))) {
      LOG_WARN("build column expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr))) {
      LOG_WARN("replace column ref expr failed", K(ret));
    }
  }
  return ret;
}

/*
 * This function examines subqueries in set clause recursively,
 * and ensures that no subquery read data from the loaded table.
 *
 * Call deepth has already examined in subquery resolving phase,
 * don't need to do it again
 */
int recursively_check_subquery_tables(ObSelectStmt* subquery_stmt, uint64_t loaded_table_id)
{
  int ret = OB_SUCCESS;
  TableItem* item = NULL;
  ObIArray<TableItem*>& table_items = subquery_stmt->get_table_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    if (NULL == (item = table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_items is null", K(table_items), K(i));
    } else
      switch (item->type_) {
        case TableItem::BASE_TABLE:
        case TableItem::ALIAS_TABLE:
          if (item->ref_id_ == loaded_table_id) {
            ret = OB_ERR_UPDATE_TABLE_USED;
            LOG_USER_ERROR(OB_ERR_UPDATE_TABLE_USED, item->table_name_.ptr());
          }
          break;
        case TableItem::JOINED_TABLE:  // TODO:need validation
        case TableItem::CTE_TABLE:     // TODO:need validation
        case TableItem::GENERATED_TABLE:
          if (OB_FAIL(recursively_check_subquery_tables(item->ref_query_, loaded_table_id))) {
            LOG_WARN("check joined table failed.", KPC(item));
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected error.", KPC(item));
      }
  }
  return ret;
}

int ObLoadDataResolver::resolve_sys_vars(ObIArray<ObVarInfo>& sys_vars)
{
  int ret = OB_SUCCESS;
  ObQueryCtx* query_ctx = NULL;
  if (OB_ISNULL(stmt_) || OB_ISNULL(query_ctx = stmt_->get_query_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt_ or query_ctx is null", K_(stmt), K(query_ctx));
  } else if (OB_FAIL(ObRawExprUtils::merge_variables(sys_vars, query_ctx->variables_))) {
    LOG_WARN("failed to record variables", K(ret));
  }
  return ret;
}

int ObLoadDataResolver::resolve_subquery_info(ObIArray<ObSubQueryInfo>& subquery_info)
{
  int ret = OB_SUCCESS;
  UNUSED(subquery_info);
  // in the future work
  /*
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_info.count(); ++i) {
      const ObSubQueryInfo &info = subquery_info.at(i);
      ObSelectResolver child_resolver(params_);
      child_resolver.set_current_level(0);
      child_resolver.set_parent_namespace_resolver(NULL);
      ObSelectStmt *sub_stmt = NULL;
      ObLoadDataStmt *load_data_stmt = NULL;
      ObRawExpr *target_expr = NULL;
      if (OB_ISNULL(info.sub_query_) || OB_ISNULL(info.ref_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subquery info is invalid", K_(info.sub_query), K_(info.ref_expr));
      } else if (OB_UNLIKELY(T_SELECT != info.sub_query_->type_)) {
        ret = OB_ERR_ILLEGAL_TYPE;
        LOG_WARN("Unknown statement type in subquery", "stmt_type", info.sub_query_->type_);
      } else if (OB_FAIL(child_resolver.resolve_child_stmt(*(info.sub_query_)))) {
        LOG_WARN("resolve select subquery failed", K(ret));
      } else {
        //get the one subquery stmt
        if (OB_ISNULL(sub_stmt = child_resolver.get_child_stmt())){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subquery stmt is NULL");
        } else if (OB_ISNULL(load_data_stmt = static_cast<ObLoadDataStmt*>(stmt_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("load data stmt is null");
        } else {
          //checking
          //1. the subquery should return 1 column, e.g. 'set col = (subquery)'
          //2. the subquery is not allowed to read any value from the loaded table
          if (sub_stmt->get_select_item_size() != 1) {
            ret = OB_ERR_INVALID_COLUMN_NUM;
            LOG_USER_ERROR(ret, sub_stmt->get_select_item_size());
          } else if (OB_FAIL(recursively_check_subquery_tables(sub_stmt, load_data_stmt->get_table_id()))) {
            LOG_WARN("recursively check subquery tables failed", K(ret));
          } else {
            //set useful things to subquery info
            if (OB_ISNULL(target_expr = sub_stmt->get_select_item(0).expr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("target expr is null");
            } else if (OB_FAIL(info.ref_expr_->add_column_type(target_expr->get_result_type()))) {
              LOG_WARN("add column type to subquery ref expr failed", K(ret));
            } else {
              info.ref_expr_->set_output_column(1);
              info.ref_expr_->set_ref_stmt(sub_stmt);
              sub_stmt->set_subquery_flag(true);
            }
          }
        }
      }
    }
  */
  return ret;
}

int ObLoadDataResolver::resolve_default_func(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObLoadDataResolver::resolve_default_expr(ObRawExpr*& expr, ObLoadDataStmt& load_stmt, uint64_t column_id)
{
  int ret = OB_SUCCESS;
  ColumnItem* col_item = NULL;
  ObConstRawExpr* const_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(col_item = load_stmt.get_column_item_by_idx(column_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get column item failed", K(ret));
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(
                 static_cast<ObItemType>(col_item->default_value_.get_type()), const_expr))) {
    LOG_WARN("create const expr failed", K(ret));
  } else {
    const_expr->set_value(col_item->default_value_);
    expr = const_expr;
  }
  return ret;
}

int ObLoadDataResolver::resolve_each_set_node(
    const ParseNode& node, const ObNameCaseMode case_mode, ObLoadDataStmt& load_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ASSIGN_ITEM != node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not T_COLUMN_LIST", K(ret), K(node.type_));
  } else if (OB_UNLIKELY(2 != node.num_child_) || OB_ISNULL(node.children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid node", K(node.num_child_), K(node.children_), K(ret));
  } else {
    ParseNode* column_node = node.children_[0];
    ParseNode* expr_node = node.children_[1];
    // target
    ObAssignment assignment;
    // 1. resolve column ref expr
    ObColumnRefRawExpr* ref_expr = NULL;
    ObRawExpr* raw_expr = NULL;
    ObQualifiedName q_name;
    if (OB_FAIL(ObResolverUtils::resolve_column_ref(column_node, case_mode, q_name))) {
      LOG_WARN("fail to resolve column name", K(ret));
    } else if (OB_FAIL(build_column_ref_expr(q_name, raw_expr))) {
      LOG_WARN("fail to build column ref expr", K(ret));
    } else if (OB_ISNULL(ref_expr = static_cast<ObColumnRefRawExpr*>(raw_expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null");
    } else if (ref_expr->is_generated_column()) {
      ret = OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
      const ObString& column_name = ref_expr->get_column_name();
      const ObString& table_name = load_stmt.get_load_arguments().table_name_;
      LOG_USER_ERROR(OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
          column_name.length(),
          column_name.ptr(),
          table_name.length(),
          table_name.ptr());
    } else {
      assignment.column_expr_ = ref_expr;
    }
    // 2. resolve value expr
    ObRawExpr* expr = NULL;
    if (OB_SUCC(ret)) {
      ObSEArray<ObQualifiedName, 16> columns;
      ObSEArray<ObSubQueryInfo, 1> sub_query_info;
      ObSEArray<ObVarInfo, 1> sys_vars;
      ObSEArray<ObAggFunRawExpr*, 1> aggr_exprs;
      ObSEArray<ObWinFunRawExpr*, 1> win_exprs;
      ObSEArray<ObOpRawExpr*, 1> op_exprs;
      ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
      ObCollationType collation_connection = CS_TYPE_INVALID;
      ObCharsetType character_set_connection = CHARSET_INVALID;
      if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(stmt_) || OB_ISNULL(session_info_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("resolve status is invalid", K_(params_.expr_factory), K_(stmt), K_(session_info));
      } else if (OB_FAIL(params_.session_info_->get_collation_connection(collation_connection))) {
        LOG_WARN("fail to get collation_connection", K(ret));
      } else if (OB_FAIL(params_.session_info_->get_character_set_connection(character_set_connection))) {
        LOG_WARN("fail to get character_set_connection", K(ret));
      } else {
        ObExprResolveContext ctx(*params_.expr_factory_, session_info_->get_timezone_info(), OB_NAME_CASE_INVALID);
        ctx.dest_collation_ = collation_connection;
        ctx.connection_charset_ = character_set_connection;
        ctx.param_list_ = params_.param_list_;
        ctx.is_extract_param_type_ = !params_.is_prepare_protocol_;  // when prepare do not extract
        ctx.external_param_info_ = &params_.external_param_info_;
        ctx.current_scope_ = current_scope_;  // working or not?
        ctx.stmt_ = static_cast<ObStmt*>(&load_stmt);
        ctx.query_ctx_ = params_.query_ctx_;
        ObRawExprResolverImpl expr_resolver(ctx);
        if (OB_FAIL(session_info_->get_name_case_mode(ctx.case_mode_))) {
          LOG_WARN("fail to get name case mode", K(ret));
        } else if (OB_FAIL(expr_resolver.resolve(expr_node,
                       expr,
                       columns,
                       sys_vars,
                       sub_query_info,
                       aggr_exprs,
                       win_exprs,
                       op_exprs,
                       user_var_exprs))) {
          LOG_WARN("resolve expr failed", K(ret));
        } else if (T_DEFAULT == expr->get_expr_type()) {
          if (OB_FAIL(resolve_default_expr(expr, load_stmt, ref_expr->get_column_id()))) {
            LOG_WARN("failed to resolve default_expr", K(ret));
          }
        } else if (OB_FAIL(resolve_subquery_info(sub_query_info))) {
          LOG_WARN("resolve sub query info failed", K(ret));
        } else if (OB_FAIL(resolve_column_ref_expr(columns, expr))) {
          LOG_WARN("resolve columns failed", K(ret));
        } else if (sys_vars.count() > 0 /*&& OB_FAIL(resolve_sys_vars(sys_vars))*/) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "system variables in load data");
          LOG_WARN("system variables in set expr is not supported", K(ret));
        } else if (OB_FAIL(resolve_default_func(expr))) {
          LOG_WARN("resolve default function failed", K(ret));
        } else if (aggr_exprs.count() > 0) {
          // not support group function
          ret = OB_ERR_INVALID_GROUP_FUNC_USE;
          LOG_WARN("invalid scope for agg function", K(ret), K(current_scope_));
        } else if (win_exprs.count() > 0) {
          // not support window function
          ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
          LOG_WARN("invalid scope for window function", K(ret), K(current_scope_));
        } else if (expr->has_flag(CNT_OUTER_JOIN_SYMBOL)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("(+) is not suppose to be here");
        }
        assignment.expr_ = expr;
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(load_stmt.add_assignment(assignment))) {
      LOG_WARN("fail to add assignment");
    }
  }

  return ret;
}

int ObLoadDataResolver::check_if_table_exists(
    uint64_t tenant_id, const ObString& db_name, const ObString& table_name, bool cte_table_fisrt, uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  uint64_t database_id = OB_INVALID_ID;
  bool is_table_exist = false;
  bool is_index_table = false;
  const ObTableSchema* tschema = NULL;

  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolver isn't init", K(stmt_), K_(schema_checker), K_(session_info), K_(allocator));
  } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, database_id))) {
    LOG_WARN("get database id failed", K(ret));
  } else if (OB_FAIL(schema_checker_->check_table_exists(
                 tenant_id, database_id, table_name, is_index_table, is_table_exist))) {
    LOG_WARN("fail to check table or index exist", K(tenant_id), K(database_id), K(table_name), K(ret));
  } else if (!is_table_exist) {
    // a alias table is impossiable
    // TODO: support synonym tables, return not exist for now
    // see ObDMLResolver::resolve_table_relation_recursively
    ret = OB_TABLE_NOT_EXIST;
    LOG_INFO("table not exist", K(tenant_id), K(database_id), K(table_name), K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(
                 tenant_id, database_id, table_name, false /*data table first*/, cte_table_fisrt, tschema))) {
    // it's possiable to get "table not exist" ret here
    LOG_WARN("get table schema failed", K(ret));
  } else {
    table_id = tschema->get_table_id();
  }
  return ret;
}

int ObLoadDataResolver::resolve_string_node(const ParseNode& node, ObString& target_str)
{
  int ret = OB_SUCCESS;
  switch (node.type_) {
    case T_VARCHAR:
    case T_QUESTIONMARK:
    case T_CLOSED_STR:
    case T_ESCAPED_STR:
      target_str.assign_ptr(node.str_value_, static_cast<int32_t>(node.str_len_));
      break;
    case T_OPTIONALLY_CLOSED_STR:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support optionally enclosed string", KR(ret));
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node type must be varchar or ?", K(ret), K(node.type_));
  }
  return ret;
}

int ObLoadDataResolver::resolve_field_list_node(const ParseNode& node, ObDataInFileStruct& data_struct_in_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_INTO_FIELD_LIST != node.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node type is not T_INTO_FIELD_LIST", K(ret), K(node.type_));
  } else if (OB_UNLIKELY(node.num_child_ <= 0) || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child node", K(node.num_child_), K(ret));
  } else {
    const ParseNode* child_node = NULL;
    for (int32_t i = 0; i < node.num_child_ && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(child_node = node.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid grand child node", K(child_node), K(i), K(ret));
      } else {
        switch (child_node->type_) {
          case T_FIELD_TERMINATED_STR: {
            if (OB_UNLIKELY(child_node->num_child_ != 1) || OB_ISNULL(child_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid grand child node", K(ret), K(i), K(child_node->num_child_));
            } else if (OB_FAIL(resolve_string_node(*child_node->children_[0], data_struct_in_file.field_term_str_))) {
              LOG_WARN("failed to resolve string node", K(ret));
            }
            break;
          }
          case T_OPTIONALLY_CLOSED_STR:
            data_struct_in_file.is_opt_field_enclosed_ = true;
            break;
          case T_CLOSED_STR: {
            if (OB_UNLIKELY(child_node->num_child_ != 1) || OB_ISNULL(child_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid grand child node", K(child_node->num_child_), K(i), K(ret));
            } else if (OB_FAIL(
                           resolve_string_node(*child_node->children_[0], data_struct_in_file.field_enclosed_str_))) {
              LOG_WARN("failed to resolve char node", K(ret));
            }
            break;
          }
          case T_ESCAPED_STR: {
            if (OB_UNLIKELY(child_node->num_child_ != 1) || OB_ISNULL(child_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid grand child node", K(child_node->num_child_), K(i), K(ret));
            } else if (OB_FAIL(
                           resolve_string_node(*child_node->children_[0], data_struct_in_file.field_escaped_str_))) {
              LOG_WARN("failed to resolve char node", K(ret));
            }
            break;
          }
          default: {
            break;
          }
        }  // end of switch
      }    // end of else
    }      // end of for
  }
  return ret;
}

int ObLoadDataResolver::resolve_line_list_node(const ParseNode& node, ObDataInFileStruct& data_struct_in_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_INTO_LINE_LIST != node.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node type is not T_INTO_LINE_LIST", K(ret), K(node.type_));
  } else if (OB_UNLIKELY(node.num_child_ <= 0) || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child node", K(node.num_child_), K(ret));
  } else {
    const ParseNode* child_node = NULL;
    for (int32_t i = 0; i < node.num_child_ && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(child_node = node.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid grand child node", K(child_node), K(i), K(ret));
      } else if (T_LINE_TERMINATED_STR == child_node->type_) {
        if (OB_ISNULL(child_node->children_) || OB_ISNULL(child_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid grand child node", K(child_node), K(i), K(ret));
        } else if (OB_FAIL(resolve_string_node(*child_node->children_[0], data_struct_in_file.line_term_str_))) {
          LOG_WARN("failed to resolve string node", K(ret));
        }
      } else if (T_LINE_START_STR == child_node->type_) {
        if (OB_ISNULL(child_node->children_) || OB_ISNULL(child_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid grand child node", K(child_node), K(i), K(ret));
        } else if (OB_FAIL(resolve_string_node(*child_node->children_[0], data_struct_in_file.line_start_str_))) {
          LOG_WARN("failed to resolve string node", K(ret));
        }
      } else {
        // do nothing
      }
    }  // end of for
  }
  return ret;
}

int ObLoadDataResolver::resolve_char_node(const ParseNode& node, int32_t& single_char)
{
  int ret = OB_SUCCESS;
  if (0 == node.str_len_) {
    single_char = INT_MAX;
  } else if (1 == node.str_len_ && !OB_ISNULL(node.str_value_)) {
    if (!ob_isascii(node.str_value_[0])) {
      ret = OB_ERR_INVALID_SEPARATOR;
      LOG_WARN(
          "Non-ASCII separator arguments are not fully supported", K(ret), K(static_cast<int32_t>(node.str_value_[0])));
    } else {
      single_char = node.str_value_[0];
    }
  } else {
    ret = OB_WRONG_FIELD_TERMINATORS;
    LOG_WARN("closed str should be a character", K(ret), K(static_cast<int32_t>(node.str_value_[0])));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
