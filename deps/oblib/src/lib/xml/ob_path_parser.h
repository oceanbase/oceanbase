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
 * This file contains interface support for the Xml Path abstraction.
 */

#ifndef OCEANBASE_SQL_OB_PATH_PARSER
#define OCEANBASE_SQL_OB_PATH_PARSER

#include "ob_xpath.h"
#include "ob_tree_base.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "src/share/datum/ob_datum.h"
#include "lib/json_type/ob_json_path.h"
#include "lib/xml/ob_mul_mode_reader.h"
#include "lib/xml/ob_multi_mode_interface.h"
namespace oceanbase {
namespace common {

class ObPathItem
{
public:
  static constexpr char DOLLAR = '$';
  static constexpr char SLASH = '/';
  static constexpr char BEGIN_ARRAY = '[';
  static constexpr char END_ARRAY = ']';
  static constexpr char DOUBLE_QUOTE = '"';
  static constexpr char SINGLE_QUOTE = '\'';
  static constexpr char WILDCARD = '*';
  static constexpr char AT = '@';
  static constexpr char BRACE_START = '(';
  static constexpr char BRACE_END = ')';
  static constexpr char COLON = ':';
  static constexpr char DOT = '.';
  static constexpr char UNDERLINE = '_';
  static constexpr char UNION = '|';
  static constexpr char MINUS = '-';
  static constexpr char COM_LT = '<';
  static constexpr char COM_GT = '>';
  static constexpr char COM_EQ = '=';
  static constexpr char CAL_ADD = '+';
  static constexpr char CAL_SUB = '-';
  static constexpr char CAL_MUL = '*';
  static constexpr char* COM_LE = const_cast<char*>("<=");
  static constexpr char* COM_GE = const_cast<char*>(">=");
  static constexpr char* COM_NE = const_cast<char*>("!=");
  static constexpr char* OR = const_cast<char*>("or");
  static constexpr char* AND = const_cast<char*>("and");
  static constexpr char* DIV = const_cast<char*>("div");
  static constexpr char* MOD = const_cast<char*>("mod");
  static constexpr char* DOUBLE_SLASH = const_cast<char*>("//");
  static constexpr char* DOUBLE_COLON = const_cast<char*>("::");
  static constexpr char* DOUBLE_DOT = const_cast<char*>("..");
  static constexpr char* ANCESTOR = const_cast<char*>("ancestor::");
  static constexpr char* ANCESTOR_OR_SELF = const_cast<char*>("ancestor-or-self::");
  static constexpr char* ATTRIBUTE = const_cast<char*>("attribute::");
  static constexpr char* CHILD = const_cast<char*>("child::");
  static constexpr char* DESCENDANT = const_cast<char*>("descendant::");
  static constexpr char* DESCENDANT_OR_SELF = const_cast<char*>("descendant-or-self::");
  static constexpr char* FOLLOWING = const_cast<char*>("following::");
  static constexpr char* FOLLOWING_SIBLING = const_cast<char*>("following-sibling::");
  static constexpr char* NAMESPACE = const_cast<char*>("namespace::");
  static constexpr char* PARENT = const_cast<char*>("parent::");
  static constexpr char* PRECEDING = const_cast<char*>("preceding::");
  static constexpr char* PRECEDING_SIBLING = const_cast<char*>("preceding-sibling::");
  static constexpr char* SELF = const_cast<char*>("self::");
  static constexpr char* NODE = const_cast<char*>("node(");
  static constexpr char* TEXT = const_cast<char*>("text(");
  static constexpr char* COMMENT = const_cast<char*>("comment(");
  static constexpr char* PROCESSING_INSTRUCTION = const_cast<char*>("processing-instruction(");
};

// todo: path cache
class ObPathParser {
public:
  ObPathParser(ObMulModeMemCtx* ctx, const ObParserType& parser_type, const ObString& path,
              ObString& default_ns, ObPathVarObject* pass_var) :
    allocator_(ctx->allocator_), parser_type_(parser_type), expression_(path), default_ns_(default_ns),
    pass_var_(pass_var), bad_index_(-1), index_(0), len_(path.length()),
    is_first_node_(true), ctx_(ctx) {}
  explicit ObPathParser(const ObString& path, common::ObIAllocator *allocator);
  virtual ~ObPathParser() {}
  int to_string(ObStringBuffer& str);  // transfer all pathnodes to string
  int parse_path(ObPathArgType patharg_type = NOT_SUBPATH);
  int parse_location_path(ObPathArgType patharg_type = NOT_SUBPATH);
  ObPathNode* get_root() {return root_node_;}
private:
  int parse_xpath_node(ObPathArgType patharg_type);
  int parse_location_node(bool is_absolute);
  int parse_primary_expr_node(ObPathArgType patharg_type);
  int parse_non_abbrevited_location_node(bool is_absolute);
  int parse_double_slash_node();
  int parse_double_dot_node(bool is_absolute);
  int parse_single_dot_node(bool is_absolute);
  int parse_axis_info(ObPathLocationNode*& location);
  int parse_nodetest_info(ObPathLocationNode*& location);
  int parse_namespace_info(ObPathLocationNode*& location, ObString& ns_str);
  int parse_union_node(ObPathArgType patharg_type);
  int get_filter_char_type(ObXpathFilterChar& filter_char);
  int parse_arg(ObPathNode*& arg, ObPathArgType patharg_type, bool is_filter, bool negtive = false);
  int parse_filter_node(ObPathNode*& filter, ObPathArgType patharg_type);
  int parse_func_type(ObFuncType& func_type);
  int parse_func_arg(ObPathFuncNode*& func_node, ObPathArgType patharg_type = NOT_SUBPATH);
  int parse_func_node(ObPathArgType patharg_type = NOT_SUBPATH);
  int trans_to_filter_op(ObPathRootNode*& origin_root, int filter_num, bool is_first, ObPathNode*& op_root);
  int push_filter_char_in(const ObXpathFilterChar& in, ObPathVectorPointers& node_stack,
                          ObFilterCharPointers& char_stack, ObPathArgType patharg_type);
  int get_xpath_ident(char*& str, uint64_t& length, bool& is_func);
  int get_xpath_literal(char*& str, uint64_t& length);
  int get_xpath_number(double& num);
  int get_subpath_str(bool is_filter, ObString& subpath);
  int parse_subpath(ObString& subpath,ObPathNode*& node, bool is_filter, ObPathArgType patharg_type);
  int get_xpath_subpath(ObPathNode*& node, bool is_filter, ObPathArgType patharg_type);
  int check_nodetest(const ObString& str, ObSeekType& seek_type, char*& arg, uint64_t& arg_len);
  int alloc_path_node(ObPathNode*& node);
  int alloc_root_node(ObPathRootNode*& node);
  int alloc_filter_op_node(ObPathFilterOpNode*& node);
  int alloc_location_node(ObPathLocationNode*& node);
  int alloc_filter_node(ObPathFilterNode*& node);
  int alloc_func_node(ObPathFuncNode*& node);
  int alloc_arg_node(ObPathArgNode*& node);
  int jump_over_filter();
  int jump_over_quote();
  int jump_over_brace(bool is_brace);
  int trans_to_pure_index_filter(ObPathNode*& node);
  int check_cmp(bool& is_cmp);
  int check_is_legal_xpath(const ObPathArgType& patharg_type);
  bool is_prefix_match_letter_operator();
  bool is_prefix_match_function();
  bool path_prefix_match(const char *str);
  bool is_path_end_with_brace();
  bool is_function_path();
  bool is_number_begin();
  bool is_literal_begin();
  bool is_last_letter_location(int last_idx);
  bool is_last_letter_operator(const int& last_idx);
  bool is_negtive();
  common::ObIAllocator *allocator_;
  ObParserType parser_type_;
  ObString expression_;
  ObString default_ns_;
  ObPathVarObject* pass_var_;
  ObPathNode* root_node_;
  uint64_t bad_index_;
  uint64_t index_;
  uint64_t len_;
  bool is_first_node_;
  ObMulModeMemCtx* ctx_;
};

class ObPathParserUtil
{
public:
  static bool is_xml_name_start_char(const char ch);
  static bool is_xml_name_char(const char ch);
  static bool is_end_of_xpathkeyword(const char ch);
  static bool is_xpath_ident_terminator(const char ch);
  static bool check_is_legal_tagname(const char* name, int length);
  static bool is_xpath_transform_terminator(const char ch);
  static bool is_left_brace(const char ch);
  static bool is_operator(const char ch);
  static bool is_nodetest_start_char(const char ch);
  static bool is_function_start_char(const char ch);
  static bool is_func_must_in_pred(const ObFuncType& func_type);
  static bool is_illegal_comp_for_filter(const ObFilterType& type, ObPathNode* left, ObPathNode* right);
  static bool is_boolean_ans(ObFilterType type);
  static bool is_boolean_subpath_arg(ObPathNode* node);
  static bool is_position(ObPathNode* node);
};

}
}
#endif  // OCEANBASE_SQL_OB_XPATH_PARSE