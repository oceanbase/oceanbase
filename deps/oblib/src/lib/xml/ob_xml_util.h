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
 * This file contains interface define for the xml util abstraction.
 */

#ifndef OCEANBASE_SQL_OB_XML_UTIL
#define OCEANBASE_SQL_OB_XML_UTIL

#include "lib/xml/ob_multi_mode_interface.h"
#include "ob_tree_base.h"
#include "lib/xml/ob_xml_tree.h"
#include "ob_xpath.h"

namespace oceanbase {
namespace common {

enum ObXpathArgType {
  XC_TYPE_BOOLEAN = 0,
  XC_TYPE_NUMBER,
  XC_TYPE_STRING,
  XC_TYPE_NODE,
  XC_TYPE_MAX       // invalid type, or count of ObXpathArgType
};

enum ObXpathCompareType {
  XC_EQ = 0,
  XC_NE,
  XC_LT,
  XC_LE,
  XC_GT,
  XC_GE,
  XC_MAX          // invalid type, or count of ObXpathCompareType
};

static constexpr int CMP_ARG_TYPE_NUM = static_cast<int>(ObXpathArgType::XC_TYPE_MAX);
static constexpr int XC_TYPE_NUM = static_cast<int>(ObXpathCompareType::XC_MAX);


enum ObXmlBinaryType {
  DocumentType = 0,
  ContentType,
  UnparsedType,
  MaxType
};

struct ObIMulModeBaseCmp {
  bool operator()(const ObIMulModeBase* a, const ObIMulModeBase* b) {
    bool is_smaller = false;
    if (OB_ISNULL(a) && OB_ISNULL(b)) {
      is_smaller = true;
    } else if (OB_NOT_NULL(a) && OB_NOT_NULL(b)) { // is tree node
      ObNodeMemType a_type = a->get_internal_type();
      ObNodeMemType b_type = b->get_internal_type();
      if (a_type == ObNodeMemType::TREE_TYPE && b_type == ObNodeMemType::TREE_TYPE) {
        is_smaller = (a < b);
      } else {
        ObIMulModeBase* ref_a = const_cast<ObIMulModeBase*>(a);
        ObIMulModeBase* ref_b = const_cast<ObIMulModeBase*>(b);
        is_smaller = (ref_a->is_node_before(ref_b));
      }
    } else {
      is_smaller =  false;
    }
    return is_smaller;
  }
};

struct ObIMulModeBaseUnique {
  bool operator()(const ObIMulModeBase* a, const ObIMulModeBase* b) {
    bool is_eq = false;
    if (OB_ISNULL(a) && OB_ISNULL(b)) {
      is_eq = true;
    } else if (OB_NOT_NULL(a) && OB_NOT_NULL(b)) { // is tree node
      ObNodeMemType a_type = a->get_internal_type();
      ObNodeMemType b_type = b->get_internal_type();
      if (a_type == ObNodeMemType::TREE_TYPE && b_type == ObNodeMemType::TREE_TYPE) {
        is_eq = (a == b);
      } else {
        if (a->type() == b->type()) {
          ObIMulModeBase* ref_a = const_cast<ObIMulModeBase*>(a);
          ObIMulModeBase* ref_b = const_cast<ObIMulModeBase*>(b);
          is_eq = (ref_a->is_equal_node(ref_b));
        } else {
          is_eq = false;
        }
      }
    } else {
      is_eq =  false;
    }
    return is_eq;
  }
};

struct ObXmlKeyCompare {
  int operator()(const ObString& left_key, const ObString& right_key) {
    return left_key.compare(right_key);
  }
};

class ObNsPair final
{
public:
  ObNsPair() : key_(nullptr), value_(nullptr) {}
  explicit ObNsPair(const ObString &key, ObString value)
      : key_(key),
        value_(value)
  {
  }
  ObNsPair(const ObString& key) : key_(key), value_(nullptr) {}
  explicit ObNsPair(const ObString &key, ObString& value)
      : key_(key),
        value_(value)
  {}
  ~ObNsPair() {}
  OB_INLINE common::ObString get_key() const { return key_; }
  OB_INLINE void set_xml_key(const common::ObString &new_key)
  {
    key_.assign_ptr(new_key.ptr(), new_key.length());
  }
  OB_INLINE common::ObString get_value() const { return value_; }
  OB_INLINE void set_value(const common::ObString &new_value)
  {
    value_.assign_ptr(new_value.ptr(), new_value.length());
  }
  bool operator<(const ObNsPair& right) const
  {
    return key_ < right.key_;
  }
  bool operator==(const ObNsPair& right) const
  {
    return key_ == right.key_;
  }
  bool operator==(long null) const
  {
    // ptr must be null
    return key_.ptr() == nullptr && value_.ptr() == nullptr;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "key = %s", key_.ptr());
    return pos;
  }
  common::ObString key_;
  common::ObString value_;
};
struct ObNsPairCmp {
  int operator()(const ObNsPair* left, const ObNsPair* right) {
    INIT_SUCC(ret);
    if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = (left < right);
    } else {
      if (left->key_.length() != right->key_.length()) {
        ret = (left->key_.length() < right->key_.length());
      } else { // do Lexicographic order when length equals
        ret = (left->key_.compare(right->key_) < 0);
      }
    }
    return ret;
  }
};
struct ObNsPairUnique {
  // for ns, if key is equal, then definition is duplicate
  int operator()(const ObNsPair* left, const ObNsPair* right) {
    bool ret_bool = false;
    if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret_bool = (left == right);
    } else {
      ret_bool = (left->key_ == right->key_);
    }
    return ret_bool;
  }
};

class ObXmlUtil {
public:
  static const char* get_charset_name(ObCollationType collation_type);
  static const char* get_charset_name(ObCharsetType charset_type);
  static ObCharsetType check_support_charset(const ObString& cs_name);

  static bool is_container_tc(ObMulModeNodeType type);
  static bool is_node(ObMulModeNodeType type);
  // test type
  static bool is_text(ObMulModeNodeType type);
  // comment type
  static bool is_comment(ObMulModeNodeType type);
  // element type
  static bool is_element(ObMulModeNodeType type);
  // pi type
  static bool is_pi(ObMulModeNodeType type);
  static bool use_text_serializer(ObMulModeNodeType type);
  static bool use_attribute_serializer(ObMulModeNodeType type);
  static bool use_element_serializer(ObMulModeNodeType type);

  static int append_newline_and_indent(ObStringBuffer &j_buf, uint64_t level, uint64_t size);

  static int append_qname(ObStringBuffer &j_buf, const ObString& prefix, const ObString& localname);
  static int add_ns_def_if_necessary(uint32_t format_flag, ObStringBuffer &buf, const ObString& origin_prefix,
                                    ObNsSortedVector* element_ns_vec, ObVector<ObNsPair*>& delete_ns_vec);
  static int add_attr_ns_def(ObIMulModeBase *cur, uint32_t format_flag, ObStringBuffer &buf,
                             ObNsSortedVector* element_ns_vec, ObVector<ObNsPair*>& delete_ns_vec);
  static int restore_ns_vec(ObNsSortedVector* element_ns_vec, ObVector<ObNsPair*>& delete_ns_vec);
  static int init_extend_ns_vec(ObIAllocator *allocator,
                                ObIMulModeBase *src,
                                ObNsSortedVector& ns_vec);
  static int delete_dup_ns_definition(ObIMulModeBase *src,
                                      ObNsSortedVector& origin_vec,
                                      ObVector<ObNsPair*>& delete_vec);
  static int check_ns_conflict(ObIMulModeBase* cur_parent,
                              ObIMulModeBase* &last_parent,
                              ObXmlBin *cur,
                              common::hash::ObHashMap<ObString, ObString>& ns_map,
                              bool& conflict);
  static int ns_to_extend(ObMulModeMemCtx* mem_ctx,
                          common::hash::ObHashMap<ObString, ObString>& ns_map,
                          ObStringBuffer *buffer);
  static int create_mulmode_tree_context(ObIAllocator *allocator, ObMulModeMemCtx*& ctx);
  static int xml_bin_type(const ObString& data, ObMulModeNodeType& type);
  static int xml_bin_header_info(const ObString& data, ObMulModeNodeType& type, int64_t& size);
  static int cast_to_string(const ObString &val, ObIAllocator &allocator, ObStringBuffer& result, ObCollationType cs_type);

  // safe cast
  // if cast type not match, return null;
  // should be carefull when use these functions
  template<typename XmlNodeClass>
  static XmlNodeClass* xml_node_cast(ObXmlNode* src, ObMulModeNodeType xml_type) {
    XmlNodeClass* res = nullptr;
    if (OB_NOT_NULL(src) && src->type() == xml_type) {
      res = static_cast<XmlNodeClass*>(src);
    }
    return res;
  }
	// cast to string
	static int to_string(ObIAllocator &allocator, double &in, char *&out);
	static int to_string(ObIAllocator &allocator, bool &in, char *&out);
	static int to_string(ObIAllocator &allocator, ObNodeTypeAndContent *in, char *&out);

	// cast to boolean
	static int to_boolean(double &in, bool &out);
	static int to_boolean(bool &in, bool &out) { out = in; return 0; };
	static int to_boolean(char *in, bool &out);
	static int to_boolean(ObNodeTypeAndContent *in, bool &out);
	static int to_boolean(ObPathStr *in, bool &out);

	static int check_bool_rule(bool &in, bool &out) { out = in; return 0; };
	static int check_bool_rule(double &in, bool &out);
	static int check_bool_rule(char *in, bool &out);
	static int check_bool_rule(ObPathStr *in, bool &out);
	static int check_bool_rule(ObNodeTypeAndContent *in, bool &out);

	// cast to number
	static int to_number(const char *in, const uint64_t length, double &out);
	static int to_number(bool &in, double &out);
	static int to_number(double &in, double &out) { out = in; return 0; };
	static int to_number(ObSeekResult *in, double &out);
	static int to_number(ObPathStr *in, double &out);
	static int to_number(ObNodeTypeAndContent *in, double &out);
	static int dfs_xml_text_node(ObMulModeMemCtx *ctx, ObIMulModeBase *xml_doc, ObString &res);
	static int get_array_from_mode_base(ObIMulModeBase *left, ObIArray<ObIMulModeBase*> &res);
	static int alloc_arg_node(ObIAllocator *allocator, ObPathArgNode*& node);
	static int alloc_filter_node(ObIAllocator *allocator, ObXmlPathFilter*& node);
	static int compare(double left, double right, ObFilterType op, bool &res);
	static int compare(ObString left, ObString right, ObFilterType op, bool &res);
	static int compare(bool left, bool right, ObFilterType op, bool &res);
	static int init_print_ns(ObIAllocator *allocator, ObIMulModeBase *src, ObNsSortedVector& ns_vec, ObNsSortedVector*& vec_point);

	// 调用的时候特殊处理OB_OP_NOT_ALLOW
	//	calculate: + - * div %
	static int calculate(double left, double right, ObFilterType op, double &res);

	// logic compare: and/or
	static int logic_compare(bool left, bool right, ObFilterType op, bool &res);

	// union: |
	template<class LeftType, class RightType>
	static int inner_union(LeftType left, RightType right, bool &res)
	{
		INIT_SUCC(ret);
		UNUSED(right);
		if (OB_FAIL(ObXmlUtil::check_bool_rule(left, res))) {
		}
		return ret;
	}

	static bool check_xpath_arg_type(ObArgType type)
	{
		if (type != ObArgType::PN_BOOLEAN ||
				type != ObArgType::PN_DOUBLE ||
				type != ObArgType::PN_STRING ||
				type != ObArgType::PN_SUBPATH) {
			return false;
		}
		return true;
	}

	static ObXpathArgType arg_type_correspondence(ObArgType arg_type)
	{
		switch (arg_type) {
		case ObArgType::PN_BOOLEAN:
			return ObXpathArgType::XC_TYPE_BOOLEAN;

		case ObArgType::PN_DOUBLE:
			return ObXpathArgType::XC_TYPE_NUMBER;

		case ObArgType::PN_STRING:
			return ObXpathArgType::XC_TYPE_STRING;

		case ObArgType::PN_SUBPATH:
			return ObXpathArgType::XC_TYPE_NODE;

		default:
			return ObXpathArgType::XC_TYPE_BOOLEAN;

		}
	}

	static ObXpathCompareType filter_type_correspondence(ObFilterType filter_type) {
		switch (filter_type) {
			case ObFilterType::PN_CMP_EQUAL:
				return ObXpathCompareType::XC_EQ;

			case ObFilterType::PN_CMP_UNEQUAL:
				return ObXpathCompareType::XC_NE;

			case ObFilterType::PN_CMP_GT:
				return ObXpathCompareType::XC_GT;

			case ObFilterType::PN_CMP_GE:
				return ObXpathCompareType::XC_GE;

			case ObFilterType::PN_CMP_LE:
				return ObXpathCompareType::XC_LE;

			case ObFilterType::PN_CMP_LT:
				return ObXpathCompareType::XC_LT;

			default:
				return ObXpathCompareType::XC_EQ;
		}
	}

	/*
	compare[all 6]: =, !=, >, >=, <, <=
	calculate[all 5]: +, -, *, div, %
	union[all 1]: |
	logic operation[all 2]: and, or
	*/

	// 0-7: = != < <= > >=
	// 0-2: bool number string node-set
	static constexpr ObXpathArgType compare_cast[CMP_ARG_TYPE_NUM][CMP_ARG_TYPE_NUM][XC_TYPE_NUM] = {
		// left bool
		{
			/*right bool*/
			{
        XC_TYPE_BOOLEAN,  // =
        XC_TYPE_BOOLEAN,  // !=
        XC_TYPE_NUMBER,  // >
        XC_TYPE_NUMBER,  // >=
        XC_TYPE_NUMBER,  // <
        XC_TYPE_NUMBER   // <=
      },

			/*right number*/
			{
        XC_TYPE_BOOLEAN,  // =
        XC_TYPE_BOOLEAN,  // !=
        XC_TYPE_NUMBER,   // >
        XC_TYPE_NUMBER,   // >=
        XC_TYPE_NUMBER,   // <
        XC_TYPE_NUMBER    // <=
      },

			/*right string*/
			{
        XC_TYPE_BOOLEAN,  // =
        XC_TYPE_BOOLEAN,  // !=
        XC_TYPE_NUMBER,   // >
        XC_TYPE_NUMBER,   // >=
        XC_TYPE_NUMBER,   // <
        XC_TYPE_NUMBER    // <=
      },

			/*right node-set*/
			{
        XC_TYPE_BOOLEAN,  // =
        XC_TYPE_BOOLEAN,  // !=
        XC_TYPE_BOOLEAN,   // >
        XC_TYPE_BOOLEAN,   // >=
        XC_TYPE_BOOLEAN,   // <
        XC_TYPE_BOOLEAN    // <=
      },
		},
		// left number
		{
			/*right bool*/
			{
        XC_TYPE_BOOLEAN,  // =
        XC_TYPE_BOOLEAN,  // !=
        XC_TYPE_NUMBER,   // >
        XC_TYPE_NUMBER,   // >=
        XC_TYPE_NUMBER,   // <
        XC_TYPE_NUMBER    // <=
      },

			/*right number*/
			{
        XC_TYPE_NUMBER,   // =
        XC_TYPE_NUMBER,   // !=
        XC_TYPE_NUMBER,   // >
        XC_TYPE_NUMBER,   // >=
        XC_TYPE_NUMBER,   // <
        XC_TYPE_NUMBER    // <=
      },

			/*right string*/
			{
        XC_TYPE_NUMBER,   // =
        XC_TYPE_NUMBER,   // !=
        XC_TYPE_NUMBER,   // >
        XC_TYPE_NUMBER,   // >=
        XC_TYPE_NUMBER,   // <
        XC_TYPE_NUMBER    // <=
      },

			/*right node-set*/
			{
        XC_TYPE_NUMBER,  // =
        XC_TYPE_NUMBER,  // !=
        XC_TYPE_NUMBER,   // >
        XC_TYPE_NUMBER,   // >=
        XC_TYPE_NUMBER,   // <
        XC_TYPE_NUMBER    // <=
      },
		},
		// left string
		{
			/*right bool*/
			{
        XC_TYPE_BOOLEAN,  // =
        XC_TYPE_BOOLEAN,  // !=
        XC_TYPE_NUMBER,   // >
        XC_TYPE_NUMBER,   // >=
        XC_TYPE_NUMBER,   // <
        XC_TYPE_NUMBER    // <=
      },

			/*right number*/
			{
        XC_TYPE_STRING,   // =
        XC_TYPE_STRING,   // !=
        XC_TYPE_NUMBER,   // >
        XC_TYPE_NUMBER,   // >=
        XC_TYPE_NUMBER,   // <
        XC_TYPE_NUMBER    // <=
      },

			/*right string*/
			{
        XC_TYPE_STRING,   // =
        XC_TYPE_STRING,   // !=
        XC_TYPE_STRING,   // >
        XC_TYPE_STRING,   // >=
        XC_TYPE_STRING,   // <
        XC_TYPE_STRING    // <=
      },

			/*right node-set*/
			{
        XC_TYPE_STRING,   // =
        XC_TYPE_STRING,   // !=
        XC_TYPE_STRING,   // >
        XC_TYPE_STRING,   // >=
        XC_TYPE_STRING,   // <
        XC_TYPE_STRING    // <=
      }
		},
		// left node-set
		{
			/*right bool*/
			{
        XC_TYPE_BOOLEAN,  // =
        XC_TYPE_BOOLEAN,  // !=
        XC_TYPE_BOOLEAN,   // >
        XC_TYPE_BOOLEAN,   // >=
        XC_TYPE_BOOLEAN,   // <
        XC_TYPE_BOOLEAN    // <=
      },

			/*right number*/
			{
        XC_TYPE_NUMBER,   // =
        XC_TYPE_NUMBER,   // !=
        XC_TYPE_NUMBER,   // >
        XC_TYPE_NUMBER,   // >=
        XC_TYPE_NUMBER,   // <
        XC_TYPE_NUMBER    // <=
      },

			/*right string*/
			{
        XC_TYPE_STRING,   // =
        XC_TYPE_STRING,   // !=
        XC_TYPE_STRING,   // >
        XC_TYPE_STRING,   // >=
        XC_TYPE_STRING,   // <
        XC_TYPE_STRING    // <=
      },

			/*right node-set*/
			{
        XC_TYPE_STRING,   // =
        XC_TYPE_STRING,   // !=
        XC_TYPE_STRING,   // >
        XC_TYPE_STRING,   // >=
        XC_TYPE_STRING,   // <
        XC_TYPE_STRING    // <=
      }
		}
	};

  // don't use, this is just for obcdc
  static int xml_bin_to_text(
      ObIAllocator &allocator,
      const ObString &bin,
      ObString &text);

  static bool is_xml_doc_over_depth(uint64_t depth);
  static int revert_escape_character(ObIAllocator &allocator, ObString &input_str, ObString &output_str);
};

class ObMulModeFactory
{
public:
  ObMulModeFactory() {}
  ~ObMulModeFactory() {}

  static int get_xml_base(ObMulModeMemCtx* ctx,
                          const ObString &buf,
                          ObNodeMemType in_type,
                          ObNodeMemType expect_type,
                          ObIMulModeBase *&out,
                          ObMulModeNodeType parse_type = ObMulModeNodeType::M_DOCUMENT,
                          bool is_for_text = false,
                          bool should_check = false);

  static int get_xml_tree(ObMulModeMemCtx* ctx,
                          const ObString &str,
                          ObNodeMemType in_type,
                          ObXmlNode *&out,
                          ObMulModeNodeType parse_type = ObMulModeNodeType::M_DOCUMENT);

  static int get_xml_base(ObMulModeMemCtx* ctx,
                          const char *ptr,
                          uint64_t length,
                          ObNodeMemType in_type,
                          ObNodeMemType expect_type,
                          ObIMulModeBase *&out,
                          ObMulModeNodeType parse_type = ObMulModeNodeType::M_DOCUMENT,
                          bool is_for_text = false,
                          bool should_check = false);

  static int transform(ObMulModeMemCtx* ctx, ObIMulModeBase *src,
                       ObNodeMemType expect_type, ObIMulModeBase *&out);
  static int add_unparsed_text_into_doc(ObMulModeMemCtx* ctx,
                                        ObString text,
                                        ObXmlDocument *&doc);
};

} // namespace common
} // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_XML_UTIL
