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
 * This file contains interface implement for the xml util abstraction.
 */
#define USING_LOG_PREFIX SQL_ENG

#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_multi_mode_interface.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xml_bin.h"
#include "lib/alloc/malloc_hook.h"

namespace oceanbase {
namespace common {

const char *ObXmlUtil::get_charset_name(ObCollationType collation_type)
{
  return get_charset_name(ObCharset::charset_type_by_coll(collation_type));
}

const char *ObXmlUtil::get_charset_name(ObCharsetType charset_type)
{
  const char *ret_name = "invalid_type";
  switch(charset_type) {
    case CHARSET_BINARY: {
      ret_name = "BINARY";
      break;
    }
    case CHARSET_UTF8MB4: {
      ret_name = "UTF-8";
      break;
    }
    case CHARSET_GBK: {
      ret_name = "GBK";
      break;
    }
    case CHARSET_UTF16: {
      ret_name = "UTF-16";
      break;
    }
    case CHARSET_GB18030: {
      ret_name = "GB18030";
      break;
    }
    case CHARSET_LATIN1: {
      ret_name = "LATIN1";
      break;
    }
    case CHARSET_GB18030_2022: {
      ret_name = "GB18030_2022";
      break;
    }
    default: {
      break;
    }
  }
  return ret_name;
}

ObCharsetType ObXmlUtil::check_support_charset(const ObString& cs_name)
{
  ObCharsetType charset_type = CHARSET_INVALID;
  if (cs_name.case_compare("utf-8") == 0) {
    charset_type = CHARSET_UTF8MB4;
  } else if (cs_name.case_compare("utf-16") == 0) {
    charset_type = CHARSET_UTF16;
  } else if (cs_name.case_compare("gbk") == 0) {
    charset_type = CHARSET_GBK;
  } else if (cs_name.case_compare("gb18030") == 0) {
    charset_type = CHARSET_GB18030;
  } else if (cs_name.case_compare("latin1") == 0) {
    charset_type = CHARSET_LATIN1;
  } else if (cs_name.case_compare("gb18030_2022") == 0) {
    charset_type = CHARSET_GB18030_2022;
  }
  return charset_type;
}

bool ObXmlUtil::is_container_tc(ObMulModeNodeType type)
{
  return (type == ObMulModeNodeType::M_DOCUMENT ||
          type == ObMulModeNodeType::M_CONTENT ||
          type == ObMulModeNodeType::M_UNPARSED ||
          type == ObMulModeNodeType::M_UNPARESED_DOC ||
          type == ObMulModeNodeType::M_ELEMENT);
}

bool ObXmlUtil::is_node(ObMulModeNodeType type)
{
  return type == ObMulModeNodeType::M_INSTRUCT
          || type == ObMulModeNodeType::M_ELEMENT
          || type == ObMulModeNodeType::M_TEXT
          || type == ObMulModeNodeType::M_CDATA
          || type == ObMulModeNodeType::M_COMMENT;
}

bool ObXmlUtil::is_text(ObMulModeNodeType type)
{
  return type == ObMulModeNodeType::M_TEXT
        || type == ObMulModeNodeType::M_CDATA;
}

bool ObXmlUtil::is_element(ObMulModeNodeType type)
{
  return type == ObMulModeNodeType::M_ELEMENT;
}

bool ObXmlUtil::is_pi(ObMulModeNodeType type)
{
  return type == ObMulModeNodeType::M_INSTRUCT;
}

bool ObXmlUtil::use_element_serializer(ObMulModeNodeType type)
{
  return is_container_tc(type);
}

bool ObXmlUtil::use_attribute_serializer(ObMulModeNodeType type)
{
  return (type == ObMulModeNodeType::M_ATTRIBUTE
      ||  type == ObMulModeNodeType::M_NAMESPACE
      ||  type == ObMulModeNodeType::M_INSTRUCT);
}

bool ObXmlUtil::use_text_serializer(ObMulModeNodeType type)
{
  return (type == ObMulModeNodeType::M_TEXT
      ||  type == ObMulModeNodeType::M_COMMENT
      ||  type == ObMulModeNodeType::M_CDATA);
}

bool ObXmlUtil::is_comment(ObMulModeNodeType type)
{
  return type == ObMulModeNodeType::M_COMMENT;
}

int ObXmlUtil::append_newline_and_indent(ObStringBuffer &j_buf, uint64_t level, uint64_t size)
{
  // Append newline and two spaces per indentation level.
  INIT_SUCC(ret);

  if (level > OB_XML_PARSER_MAX_DEPTH_) {
    ret = OB_ERR_JSON_OUT_OF_DEPTH;  // error code need change
    LOG_WARN("is_pretty level is too deep", K(ret), K(level));
  } else if (OB_FAIL(j_buf.append("\n"))) {
    LOG_WARN("fail to append newline to buffer", K(ret), K(level), K(size));
  } else if (OB_FAIL(j_buf.reserve(level * size))) {
    LOG_WARN("fail to reserve memory for buffer", K(ret), K(level), K(size));
  } else {
    char str[level * size];
    MEMSET(str, ' ', level * size);
    if (OB_FAIL(j_buf.append(str, level * size))) {
      LOG_WARN("fail to append space to buffer", K(ret), K(level), K(size));
    }
  }

  return ret;
}

int ObXmlUtil::append_qname(ObStringBuffer &j_buf, const ObString& prefix, const ObString& localname) {
  INIT_SUCC(ret);
  if (!prefix.empty()) {
    if (OB_FAIL(j_buf.append(prefix))) {
      LOG_WARN("fail to print prefix in attr", K(ret), K(prefix));
    } else if (OB_FAIL(j_buf.append(":"))) {
      LOG_WARN("fail to print : in attr", K(ret));
    }
  }
  if (OB_SUCC(ret) && !localname.empty() && OB_FAIL(j_buf.append(localname))) {
    LOG_WARN("fail to print value in attr", K(ret), K(localname));
  }
  return ret;
}

int ObXmlUtil::create_mulmode_tree_context(ObIAllocator *allocator, ObMulModeMemCtx*& ctx)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to allocate mem ctx, allocator is null", K(ret));
  } else {
    ObMulModeMemCtx* mem_ctx = static_cast<ObMulModeMemCtx*>(allocator->alloc(sizeof(ObMulModeMemCtx)));
    if (OB_ISNULL(mem_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate mem ctx, allocator is null", K(ret));
    } else {
      mem_ctx->allocator_ = allocator;
      new (&mem_ctx->page_allocator_) ModulePageAllocator(*allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR);
      new (&mem_ctx->mode_arena_) LibTreeModuleArena(DEFAULT_PAGE_SIZE, mem_ctx->page_allocator_);
      ctx = mem_ctx;
    }
  }
  return ret;
}

int ObXmlUtil::xml_bin_type(const ObString& data, ObMulModeNodeType& type)
{
  INIT_SUCC(ret);
  ObMulBinHeaderSerializer desserializer(data.ptr(), data.length());
  if (OB_FAIL(desserializer.deserialize())) {
    LOG_WARN("deserialize failed", K(ret), K(data));
  } else {
    type = desserializer.type();
  }
  return ret;
}

int ObXmlUtil::xml_bin_header_info(const ObString& data, ObMulModeNodeType& type, int64_t& size)
{
  INIT_SUCC(ret);
  ObMulBinHeaderSerializer desserializer(data.ptr(), data.length());
  if (OB_FAIL(desserializer.deserialize())) {
    LOG_WARN("deserialize failed", K(ret), K(data));
  } else {
    type = desserializer.type();
    size = desserializer.total_;
  }
  return ret;
}

int ObMulModeFactory::get_xml_base(ObMulModeMemCtx* ctx, const ObString &buf,
                  ObNodeMemType in_type, ObNodeMemType expect_type,
                  ObIMulModeBase *&out, ObMulModeNodeType node_type,
                  bool is_for_text, bool should_check)
{
  return get_xml_base(ctx, buf.ptr(), buf.length(), in_type, expect_type, out, node_type, is_for_text, should_check);
}

int ObMulModeFactory::get_xml_tree(ObMulModeMemCtx* ctx, const ObString &str,
                  ObNodeMemType in_type, ObXmlNode *&out, ObMulModeNodeType parse_type)
{
  INIT_SUCC(ret);
  const char *ptr = str.ptr();
  uint64_t length = str.length();
  void *buf = NULL;

  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(ctx), KP(ctx->allocator_));
  } else if (OB_ISNULL(ptr) || length == 0) {
    ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
    LOG_WARN("param is NULL", K(ret), KP(ptr), K(length));
  } else if (in_type != ObNodeMemType::TREE_TYPE && in_type != ObNodeMemType::BINARY_TYPE) { // check in_type
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param in_type is invalid", K(ret), K(in_type));
  } else if (in_type == ObNodeMemType::TREE_TYPE) {
    ObXmlDocument *xnode = NULL;
    if (OB_FAIL(ObXmlParserUtils::parse_document_text(ctx, str, xnode))) {
      LOG_WARN("fail to get xml tree", K(ret), K(length), K(in_type));
    } else {
      out = xnode;
    }
  } else if (in_type == ObNodeMemType::BINARY_TYPE) {
    ObXmlBin bin(str, ctx);
    ObIMulModeBase *xnode = NULL;
    if (OB_FAIL(bin.to_tree(xnode))) {
      LOG_WARN("fail to change bin to tree", K(ret), K(in_type), K(bin));
    } else {
      out = static_cast<ObXmlNode*>(xnode);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect  type",K(in_type));
  }

  return ret;
}

int ObMulModeFactory::add_unparsed_text_into_doc(ObMulModeMemCtx* ctx, ObString text, ObXmlDocument *&doc) // TODO ObXmlDocument -> ObXmlNode
{
  INIT_SUCC(ret);
  if (OB_ISNULL(doc = OB_NEWx(ObXmlDocument, ctx->allocator_, ObMulModeNodeType::M_UNPARSED, ctx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create document", K(ret));
  } else if(OB_FAIL(doc->append_unparse_text(text))) {
    LOG_WARN("fail to add unparse text to doc", K(ret));
  }
  return ret;
}

/*
  * get_xml_base special for text
  * in_type=binary_type, expect_type=binary_type
*/
int ObMulModeFactory::get_xml_base(ObMulModeMemCtx* ctx, const char *ptr, uint64_t length,
                  ObNodeMemType in_type, ObNodeMemType expect_type,
                  ObIMulModeBase *&out, ObMulModeNodeType parse_type,
                  bool is_for_text, bool should_check)
{
  INIT_SUCC(ret);
  void *buf = NULL;
  const ObString str(length, ptr);

  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(ctx), KP(ptr));
  } else if (OB_ISNULL(ptr) || length == 0) {
    ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
    LOG_WARN("param is NULL", K(ret), KP(ptr), K(length));
  } else if (in_type != ObNodeMemType::TREE_TYPE && in_type != ObNodeMemType::BINARY_TYPE) { // check in_type
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param in_type is invalid", K(ret), K(in_type));
  } else if (expect_type != ObNodeMemType::TREE_TYPE && expect_type != ObNodeMemType::BINARY_TYPE) { // check expect_type
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expect_type is invalid", K(ret), K(expect_type));
  } else if (in_type == ObNodeMemType::TREE_TYPE) {
    ObXmlDocument *xnode = NULL;
    if (parse_type == ObMulModeNodeType::M_UNPARSED && OB_FAIL(add_unparsed_text_into_doc(ctx, str, xnode))) {
      LOG_WARN("failed to get unparse tree", K(ret), K(length), K(in_type), K(expect_type));
    } else if (parse_type == ObMulModeNodeType::M_CONTENT && OB_FAIL(ObXmlParserUtils::parse_content_text(ctx, str, xnode))) {
      LOG_WARN("fail to get xml content tree", K(ret), K(length), K(in_type), K(expect_type));
    } else if (parse_type == ObMulModeNodeType::M_DOCUMENT && OB_FAIL(ObXmlParserUtils::parse_document_text(ctx, str, xnode))) {
      LOG_WARN("fail to get xml tree", K(ret), K(length), K(in_type), K(expect_type));
    } else if (expect_type == ObNodeMemType::TREE_TYPE) {
      out = xnode;
    } else { // expect bin
      ObXmlBin *bin = nullptr;
      if (OB_ISNULL(bin = OB_NEWx(ObXmlBin, ctx->allocator_, (ctx)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(in_type), K(expect_type), K(sizeof(ObXmlBin)));
      } else if (OB_FAIL(bin->parse_tree(xnode))) {
        LOG_WARN("fail to parse tree", K(ret), K(in_type), K(expect_type));
      } else {
        out = bin;
      }
    }
  } else if (in_type == ObNodeMemType::BINARY_TYPE) {
    ObXmlBin bin(ctx);
    ObXmlBin *bin_new = nullptr;
    if (OB_FAIL(bin.parse(ptr, length))) {
      LOG_WARN("fail to reset iter", K(ret), K(in_type), K(expect_type));
    } else if (bin.type() == M_UNPARESED_DOC) {
      ObStringBuffer* buffer = nullptr;
      ObXmlDocument *x_doc = nullptr;
      ObString unparsed_text;
      if (OB_ISNULL(buffer = OB_NEWx(ObStringBuffer, ctx->allocator_, (ctx->allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate buffer", K(ret), K(in_type), K(expect_type));
      } else if (OB_FAIL(bin.print(*buffer, ObXmlFormatType::NO_FORMAT, 0, 0))) {
        LOG_WARN("fail to print xml", K(ret), K(in_type), K(expect_type));
      } else if (OB_FALSE_IT(unparsed_text.assign_ptr(buffer->ptr(), buffer->length()))) {
      } else if (is_for_text && bin.type() == M_UNPARESED_DOC) {
        // special for text
        if (OB_FAIL(bin.construct(bin_new, ctx->allocator_))) {
          LOG_WARN("fail to dup res", K(ret), K(in_type), K(expect_type));
        } else {
          out = bin_new;
        }
      } else if (OB_FAIL(ObXmlParserUtils::parse_document_text(ctx, unparsed_text, x_doc))) {
        if (ret == OB_ERR_PARSER_SYNTAX) {
          ret = OB_ERR_XML_PARSE;
          LOG_WARN("parse xml plain text as document failed.", K(ret), K(unparsed_text));
        } else {
          LOG_WARN("document unparsed unexpected err", K(ret), K(unparsed_text));
        }
      } else if (expect_type == ObNodeMemType::BINARY_TYPE) {
        ObIMulModeBase* tree = x_doc;
        if (OB_ISNULL(bin_new = OB_NEWx(ObXmlBin, ctx->allocator_, (ctx)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), K(in_type), K(expect_type), K(sizeof(ObXmlBin)));
        } else if (OB_FAIL(bin_new->parse_tree(tree))) {
          LOG_WARN("fail to reset iter", K(ret), K(in_type), K(expect_type));
        } else {
          out = bin_new;
        }
      } else {
        out = x_doc;
      }
    } else if (bin.type() == M_UNPARSED) {
      ObStringBuffer* buffer = nullptr;
      ObXmlDocument *x_doc = nullptr;
      ObString unparsed_text;
      if (OB_ISNULL(buffer = OB_NEWx(ObStringBuffer, ctx->allocator_, (ctx->allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate buffer", K(ret), K(in_type), K(expect_type));
      } else if (OB_FAIL(bin.print(*buffer, ObXmlFormatType::NO_FORMAT, 0, 0))) {
        LOG_WARN("fail to print xml", K(ret), K(in_type), K(expect_type));
      } else if (OB_FALSE_IT(unparsed_text.assign_ptr(buffer->ptr(), buffer->length()))) {
      } else if (OB_FAIL(ObXmlParserUtils::parse_content_text(ctx, unparsed_text, x_doc))) {
        LOG_DEBUG("fail to parse unparse", K(ret), K(in_type), K(expect_type));
        if (should_check && ret == OB_ERR_PARSER_SYNTAX) {
          ret = OB_ERR_XML_PARSE;
          LOG_WARN("unparsed xml parse content failed.", K(ret), K(unparsed_text));
        } else {
          ret = OB_SUCCESS;
          if (expect_type == BINARY_TYPE) {
            if (OB_FAIL(bin.construct(bin_new, ctx->allocator_))) {
              LOG_WARN("fail to dup res", K(ret), K(in_type), K(expect_type));
            } else {
              out = bin_new;
            }
          } else if (OB_FAIL(bin.to_tree(out))) {
            LOG_WARN("fail to tree", K(ret), K(in_type), K(expect_type));
          }
        }
      } else if (expect_type == BINARY_TYPE) {
        if (OB_FAIL(bin.construct(bin_new, ctx->allocator_))) {
          LOG_WARN("fail to dup res", K(ret), K(in_type), K(expect_type));
        } else {
          out = bin_new;
        }
      } else {
        out = x_doc;
      }
    } else {
      if (expect_type == TREE_TYPE) {
        if (OB_FAIL(bin.to_tree(out))) {
          LOG_WARN("fail to tree", K(ret), K(in_type), K(expect_type));
        }
      } else {
        if (OB_FAIL(bin.construct(bin_new, ctx->allocator_))) {
          LOG_WARN("fail to dup res", K(ret), K(in_type), K(expect_type));
        } else {
          out = bin_new;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect xml type",K(in_type), K(expect_type));
  }

  if (OB_SUCC(ret) && OB_ISNULL(out->get_allocator())) {
    out->set_allocator(ctx->allocator_);
  }
  return ret;
}


int ObMulModeFactory::transform(ObMulModeMemCtx* ctx, ObIMulModeBase *src,
                         ObNodeMemType expect_type, ObIMulModeBase *&out)
{
  INIT_SUCC(ret);
  void *buf = NULL;
  ObNodeMemType src_type = src->get_internal_type();


  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_) ||OB_ISNULL(src)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(ctx), KP(src));
  } else if (src_type != ObNodeMemType::TREE_TYPE && src_type != ObNodeMemType::BINARY_TYPE) { // check in_type
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("param src_type is invalid", K(ret), K(src_type));
  } else if (expect_type != ObNodeMemType::TREE_TYPE && expect_type != ObNodeMemType::BINARY_TYPE) { // check expect_type
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("param expect_type is invali", K(ret), K(expect_type));
  } else if (src_type == ObNodeMemType::TREE_TYPE) { // input:tree
    if (expect_type == ObNodeMemType::BINARY_TYPE) { // to bin
      if (OB_ISNULL(buf = ctx->allocator_->alloc(sizeof(ObXmlBin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(src_type), K(expect_type), K(sizeof(ObXmlBin)));
      } else {
        ObXmlBin *bin = new (buf) ObXmlBin(ctx);
        if (OB_FAIL(bin->parse_tree(src, false))) {
          LOG_WARN("fail to parse tree", K(ret), K(src_type), K(expect_type));
        } else {
          out = bin;
        }
      }
    } else { // to tree, itself
      out = src;
    }
  } else if (src_type == ObNodeMemType::BINARY_TYPE) { // input:bin
    if (expect_type == ObNodeMemType::TREE_TYPE) { // to tree
      ObXmlBin *bin = static_cast<ObXmlBin*>(src);
      if (OB_FAIL(bin->to_tree(out))) {
        LOG_WARN("fail to change bin to tree", K(ret), K(src_type), K(expect_type));
      }
    } else { // to bin, itself
      out = src;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect xml type",K(src_type), K(expect_type));
  }

  return ret;
}

// don use just for cdc
int ObXmlUtil::xml_bin_to_text(
    ObIAllocator &allocator,
    const ObString &bin,
    ObString &text) {
  INIT_SUCC(ret);
  // oblib can not dep src/share/rc/ob_tenant_base.h,
  // so can not use MTL_ID(), so there just use defualt tenant.
  // and this function is used for obcdc, not observer, is fine.
  ObArenaAllocator tmp_alloc(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObStringBuffer *buffer = nullptr;
  ObMulModeMemCtx *xml_mem_ctx = nullptr;
  ObIMulModeBase *base = nullptr;
  ObXmlDocument *doc = nullptr;

  if (bin.empty()) {
  } else if (OB_ISNULL(buffer = OB_NEWx(ObStringBuffer, &tmp_alloc, (&tmp_alloc)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to string buffer", K(ret));
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&tmp_alloc, xml_mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_FAIL(ObMulModeFactory::get_xml_base(xml_mem_ctx,
                                                    bin,
                                                    ObNodeMemType::BINARY_TYPE,
                                                    ObNodeMemType::BINARY_TYPE,
                                                    base))) {
    LOG_WARN("fail to get xml base", K(ret), K(bin));
  } else if (OB_FAIL(base->print(*buffer, 0, 0, 0, CS_TYPE_UTF8MB4_GENERAL_CI))) {
    LOG_WARN("print_document failed", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, buffer->string(), text))) {
    LOG_WARN("ob_write_string failed", K(ret), K(*buffer));
  }
  return ret;
}

int ObXmlUtil::to_string(ObIAllocator &allocator, double &in, char *&out)
{
	INIT_SUCC(ret);
	ObStringBuffer res_buf(&allocator);
	if (std::isnan(in)) {
		res_buf.append("NaN");
	} else if (std::isinf(in) && in > 0) {
		res_buf.append("Infinity");
	} else if (std::isinf(in) && in < 0) {
		res_buf.append("-Infinity");
	} else {
		// TODO 科学计数法是否需要考虑 待定
		uint64_t out_len;
		const int64_t number_str_size = 256;
		double abs_value = fabs(in);
		char number_str[number_str_size] = {0};
		// bool force_sci = (abs_value < NOSCI_MIN_DOUBLE) || (abs_value > NOSCI_MAX_DOUBLE);
		out_len = ob_gcvt_strict(in, ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE, number_str_size,
					number_str, NULL, FALSE, TRUE, FALSE);
		res_buf.append(number_str, out_len);
	}
	out = res_buf.ptr();
	return ret;
}

int ObXmlUtil::to_string(ObIAllocator &allocator, bool &in, char *&out)
{
	INIT_SUCC(ret);
	ObStringBuffer res_buf(&allocator);
	if (in && OB_FAIL(res_buf.append("true"))) {
		LOG_WARN("append true failed", K(ret));
	} else if (!in && OB_FAIL(res_buf.append("false"))) {
		LOG_WARN("append false failed", K(ret));
	} else {
		out = res_buf.ptr();
	}
	return ret;
}

int ObXmlUtil::to_string(ObIAllocator &allocator, ObNodeTypeAndContent *in, char *&out)
{
	INIT_SUCC(ret);
	if (OB_ISNULL(in) || OB_ISNULL(in->content_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), K(in));
  } else {
    switch(in->type_) {
	  case ObArgType::PN_BOOLEAN: {
		return to_string(allocator, in->content_->boolean_, out);
    }

	  case ObArgType::PN_DOUBLE: {
		return to_string(allocator, in->content_->double_, out);
    }

	  case ObArgType::PN_STRING: {
      if (in->content_->str_.len_ == 0) {
        out = nullptr;
      } else if (OB_ISNULL(out = static_cast<char*> (allocator.alloc(sizeof(char) * in->content_->str_.len_ + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("ArgNodeContent cast to string invalid value", K(ret), K(in->content_->str_.len_));
      } else {
        MEMCPY(out, in->content_->str_.name_, in->content_->str_.len_);
        out[in->content_->str_.len_] = 0;
      }

		break;
    }

	  default:
		ret = OB_OP_NOT_ALLOW;
		LOG_WARN("ArgNodeContent cast to boolean invalid value", K(ret), K(in));
	  }
  }
	return ret;
}

int ObXmlUtil::to_boolean(double &in, bool &out)
{
	INIT_SUCC(ret);
  if (in == 0) out = false;
  else if (in == 1) out = true;
  else {
    ret = OB_OP_NOT_ALLOW;
		LOG_WARN("int cast to boolean invalid value", K(ret), K(in));
  }
	return ret;
}

int ObXmlUtil::to_boolean(char *in, bool &out)
{
	INIT_SUCC(ret);
	if (strcmp(in, "true") == 0) {
		out = true;
	} else if (strcmp(in, "false") == 0) {
		out = false;
	} else {
		ret = OB_OP_NOT_ALLOW;
		LOG_WARN("char* cast to boolean invalid value", K(ret), K(in));
	}
	return ret;
}

int ObXmlUtil::to_boolean(ObNodeTypeAndContent *in, bool &out)
{
	INIT_SUCC(ret);
  if (OB_ISNULL(in)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("unexpected null param", K(ret));
	} else if (OB_ISNULL(in->content_)) {
		ret = OB_BAD_NULL_ERROR;
		LOG_WARN("in content_ null", K(ret));
  } else {
	  switch(in->type_) {
	  case ObArgType::PN_BOOLEAN:
		return to_boolean(in->content_->boolean_, out);

	  case ObArgType::PN_DOUBLE:
		return to_boolean(in->content_->double_, out);

	  case ObArgType::PN_STRING:
		return to_boolean(&(in->content_->str_), out);

	  default:
		ret = OB_OP_NOT_ALLOW;
		LOG_WARN("ArgNodeContent cast to boolean invalid value", K(ret), K(in));
	  }
  }
  return ret;
}

int ObXmlUtil::to_boolean(ObPathStr *in, bool &out)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(in)) {
    out = false;
  } else if (in->len_ == 0) {
    out = false;
  } else {
    out = true;
  }
  return ret;
}

int ObXmlUtil::check_bool_rule(double &in, bool &out)
{
	out = in == 0 || std::isnan(in) ? false : true;
	return OB_SUCCESS;
}

int ObXmlUtil::check_bool_rule(char *in, bool &out)
{
	out = strlen(in) > 0 ? true : false;
	return OB_SUCCESS;
}

int ObXmlUtil::check_bool_rule(ObPathStr *in, bool &out)
{
	out = in->len_ > 0 ? true : false;
	return OB_SUCCESS;
}


int ObXmlUtil::check_bool_rule(ObNodeTypeAndContent *in, bool &out)
{
	INIT_SUCC(ret);
	if (OB_ISNULL(in)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("unexpected null param", K(ret));
	} else if (OB_ISNULL(in->content_)) {
		ret = OB_BAD_NULL_ERROR;
		LOG_WARN("in content_ null", K(ret));
  } else {
		switch(in->type_) {
	  case ObArgType::PN_BOOLEAN:
		return check_bool_rule(in->content_->boolean_, out);

	  case ObArgType::PN_DOUBLE:
		return check_bool_rule(in->content_->double_, out);

	  case ObArgType::PN_STRING:
		return check_bool_rule(&(in->content_->str_), out);

	  default:
		ret = OB_OP_NOT_ALLOW;
		LOG_WARN("ArgNodeContent cast to boolean invalid value", K(ret), K(in));
	  }
	}
	return ret;
}

int ObXmlUtil::to_number(const char *in, const uint64_t length, double &out)
{
	INIT_SUCC(ret);
	double ret_val = 0.0;
  if (OB_ISNULL(in)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("in is null", K(ret));
  } else {
    char *endptr = NULL;
    int err = 0;
    ret_val = ObCharset::strntodv2(in, length, &endptr, &err);
    if (EOVERFLOW == err && (-DBL_MAX == ret_val || DBL_MAX == ret_val)) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("faild to cast string to double, cause in is out of range", K(ret), K(length),
                                                                             KP(in), K(ret_val));
    } else {
      ObString tmp_str(length, in);
      ObString trimed_str = tmp_str.trim();
      // 1. only one of data and endptr is null, it is invalid input.
      if ((OB_ISNULL(in) || OB_ISNULL(endptr)) && in != endptr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null pointer(s)", K(ret), KP(in), KP(endptr));
      } else if (OB_UNLIKELY(in == endptr) || OB_UNLIKELY(EDOM == err)) { // 2. data == endptr include NULL == NULL.
        ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; //1366
        LOG_WARN("wrong value", K(ret), K(length), K(ret_val));
      } else { // 3. so here we are sure that both data and endptr are not NULL.
        endptr += ObCharset::scan_str(endptr, in + length, OB_SEQ_SPACES);
        if (endptr < in + length) {
          ret = OB_ERR_DATA_TRUNCATED; //1265
          LOG_DEBUG("check_convert_str_err", K(length), K(in - endptr));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid double value", KP(in), K(length), K(ret));
  } else {
    out = ret_val;
  }
  return ret;
}

int ObXmlUtil::to_number(ObPathStr *in, double &out)
{
	INIT_SUCC(ret);
  if (OB_ISNULL(in)) {
    ret = OB_OP_NOT_ALLOW;
	  LOG_WARN("ArgNodeContent check bool rule invalid value", K(ret), K(in));
  } else if (OB_FAIL(to_number(in->name_, in->len_, out))) {
		LOG_WARN("to number failed", K(ret));
	}
	return ret;
}

int ObXmlUtil::to_number(ObNodeTypeAndContent *in, double &out)
{
	INIT_SUCC(ret);
  if (OB_ISNULL(in)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("unexpected null param", K(ret));
	} else if (OB_ISNULL(in->content_)) {
		ret = OB_BAD_NULL_ERROR;
		LOG_WARN("in content_ null", K(ret));
  } else {
	  switch(in->type_) {
	  case ObArgType::PN_BOOLEAN:
		return to_number(in->content_->boolean_, out);

	  case ObArgType::PN_DOUBLE:
		return to_number(in->content_->double_, out);

	  case ObArgType::PN_STRING:
		return to_number(&(in->content_->str_), out);

	  default:
		ret = OB_OP_NOT_ALLOW;
		LOG_WARN("ArgNodeContent check bool rule invalid value", K(ret), K(in));
	  }
  }

	return ret;
}

// special treatment err=OB_OP_NOT_ALLOW,
// TODO errcode rename
int ObXmlUtil::to_number(ObSeekResult *in, double &out)
{
	INIT_SUCC(ret);
	ObArray<ObIMulModeBase*> node_array;
	if (OB_ISNULL(in)) {
		ret = OB_BAD_NULL_ERROR;
		LOG_WARN("to number get in null", K(ret));
	} else if (in->is_scalar_) {
		ret = OB_ERR_UNEXPECTED;
		LOG_WARN("compare get scalar unexpected", K(ret));
	} else if (OB_FAIL(get_array_from_mode_base(in->result_.base_, node_array))) { // get children
		LOG_WARN("get child array failed", K(ret));
	} else if (node_array.size() < 1) {
		ret = OB_OP_NOT_ALLOW;
		LOG_WARN("node size 0", K(ret));
	} else {
		ObString text;
		double number = 0;
		bool tmp_res = false;
		ObIMulModeBase *tmp_node = node_array.at(0);
		if (OB_ISNULL(tmp_node)) {
			ret = OB_BAD_NULL_ERROR;
			LOG_WARN("to number get in null", K(ret));
		} else if (ObMulModeNodeType::M_TEXT != tmp_node->type()) {
			ret = OB_OP_NOT_ALLOW;
			LOG_WARN("first type not text", K(ret));
		} else if (OB_FAIL(tmp_node->get_value(text))) {
			LOG_WARN("tmp xml node get value failed", K(ret));
		} else if (OB_FAIL(to_number(text.ptr(), text.length(), out))) {
			ret = ret = OB_OP_NOT_ALLOW; // rename error code
			LOG_WARN("to number failed", K(ret), K(text));
		}
	}
	return ret;
}

int ObXmlUtil::to_number(bool &in, double &out)
{
	out = in ? 1 : 0;
	return OB_SUCCESS;
}


/*
	compare number and number
	support all
*/
int ObXmlUtil::compare(double left, double right, ObFilterType op, bool &res)
{
	INIT_SUCC(ret);
	switch(op) {
	case ObFilterType::PN_CMP_EQUAL:  // =
		res = left == right ? true : false;
		break;

	case ObFilterType::PN_CMP_UNEQUAL:  // !=
		res = left != right ? true : false;
		break;

	case ObFilterType::PN_CMP_GT:  // >
		res = left > right ? true : false;
		break;

	case ObFilterType::PN_CMP_GE:  // >=
		res = left >= right ? true : false;
		break;

	case ObFilterType::PN_CMP_LT:  // <
		res = left < right ? true : false;
		break;

	case ObFilterType::PN_CMP_LE:  // <=
		res = left <= right ? true : false;
		break;

	default:
		ret = OB_INVALID_ARGUMENT;
		LOG_WARN("compare invalid argument", K(ret), K(left), K(right), K(op));
		break;
	}
	return ret;
}

/*
	compare: string and string
	support: all
*/
int ObXmlUtil::compare(ObString left, ObString right, ObFilterType op, bool &res)
{
	INIT_SUCC(ret);
	switch(op) {
	case ObFilterType::PN_CMP_EQUAL:
		res = left == right ? true : false;
		break;

	case ObFilterType::PN_CMP_UNEQUAL:
		res = left != right ? true : false;
		break;

	case ObFilterType::PN_CMP_GT:  // >
		res = left > right ? true : false;
		break;

	case ObFilterType::PN_CMP_GE:  // >=
		res = left >= right ? true : false;
		break;

	case ObFilterType::PN_CMP_LT:  // <
		res = left < right ? true : false;
		break;

	case ObFilterType::PN_CMP_LE:  // <=
		res = left <= right ? true : false;
		break;

	default:
		ret = OB_INVALID_ARGUMENT;
		LOG_WARN("compare invalid argument", K(ret), K(op));
		break;
	}

	return ret;
}

/*
	compare: boolean and boolean
	support: all
*/
int ObXmlUtil::compare(bool left, bool right, ObFilterType op, bool &res)
{
	INIT_SUCC(ret);
	uint32_t tmp_left = left ? 1 : 0;
	uint32_t tmp_right = right ? 1 : 0;
	switch(op) {
	case ObFilterType::PN_CMP_EQUAL:
		res = tmp_left == tmp_right ? true : false;
		break;

	case ObFilterType::PN_CMP_UNEQUAL:
		res = tmp_left != tmp_right ? true : false;
		break;

	case ObFilterType::PN_CMP_GT:
		res = tmp_left > tmp_right ? true : false;
		break;

	case ObFilterType::PN_CMP_GE:
		res = tmp_left >= tmp_right ? true : false;
		break;

	case ObFilterType::PN_CMP_LT:
		res = tmp_left < tmp_right ? true : false;
		break;

	case ObFilterType::PN_CMP_LE:
		res = tmp_left <= tmp_right ? true : false;
		break;

	default:
		ret = OB_INVALID_ARGUMENT;
		LOG_WARN("compare invalid argument", K(ret), K(op));
		break;
	}
	return ret;
}

int ObXmlUtil::init_print_ns(ObIAllocator *allocator, ObIMulModeBase *src, ObNsSortedVector& ns_vec, ObNsSortedVector*& ns_vec_point)
{
  INIT_SUCC(ret);
  if (OB_NOT_NULL(src) && src->check_extend()) {
    if (OB_FAIL(ObXmlUtil::init_extend_ns_vec(allocator, src, ns_vec))) {
      LOG_WARN("fail to init ns vector by extend area", K(ret));
    } else {
      ns_vec_point = &ns_vec;
    }
  } else {
    ns_vec_point = nullptr;
  }
  return ret;
}

int ObXmlUtil::calculate(double left, double right, ObFilterType op, double &res)
{
	INIT_SUCC(ret);
	switch (op) {
	case ObFilterType::PN_CMP_ADD:
		res = left + right;
		break;

	case ObFilterType::PN_CMP_SUB:
		res = left - right;
		break;

	case ObFilterType::PN_CMP_MUL:
		res = left * right;
		break;

	case ObFilterType::PN_CMP_DIV:
		res = left / right;
		break;

	case ObFilterType::PN_CMP_MOD:
		res = fmod(left, right);
		break;

	default:
		ret = OB_INVALID_ARGUMENT;
		LOG_WARN("calculate invalid argument", K(ret), K(op));
		break;
	}
	return ret;
}

int ObXmlUtil::logic_compare(bool left, bool right, ObFilterType op, bool &res)
{
	INIT_SUCC(ret);
	switch (op) {
	case PN_AND_COND:
		res = left && right ? true : false;
		break;

	case PN_OR_COND:
		res = left || right ? true : false;
		break;

	default:
		ret = OB_INVALID_ARGUMENT;
		LOG_WARN("logic compare invalid argument", K(ret), K(op));
		break;
	}
	return ret;
}

int ObXmlUtil::dfs_xml_text_node(ObMulModeMemCtx *ctx, ObIMulModeBase *xml_doc, ObString &res)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buff(ctx->allocator_);
  ObPathExprIter xpath_iter(ctx->allocator_);
  ObString xpath_str = ObString::make_string("//text()");
  ObString default_ns; // unused
  ObIMulModeBase *result_node = NULL;
  if (OB_FAIL(xpath_iter.init(ctx, xpath_str, default_ns, xml_doc, NULL))) {
    LOG_WARN("fail to init xpath iterator", K(xpath_str), K(default_ns), K(ret));
  } else if (OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
  }

  while (OB_SUCC(ret)) {
    ObString content;
    if (OB_FAIL(xpath_iter.get_next_node(result_node))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next xml node", K(ret));
      }
    } else if (OB_ISNULL(result_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xpath result node is null", K(ret));
    } else if (result_node->type() != M_TEXT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid xml node type", K(ret), K(result_node->type()));
    } else if (OB_FAIL(result_node->get_value(content))) {
      LOG_WARN("fail to get text node content", K(ret));
    } else if (OB_FAIL(buff.append(content))) {
      LOG_WARN("fail to append text node content", K(ret), K(content));
    }
  }

  if (ret == OB_ITER_END) {
    res.assign_ptr(buff.ptr(), buff.length());
    ret = OB_SUCCESS;
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = xpath_iter.close())) {
    LOG_WARN("fail to close xpath iter", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

int ObXmlUtil::get_array_from_mode_base(ObIMulModeBase *node, ObIArray<ObIMulModeBase*> &res)
{
	INIT_SUCC(ret);
	if (OB_XML_TYPE != node->data_type()) { // filter xml type
		ret = OB_ERR_UNEXPECTED;
		LOG_WARN("comprare ObIMulModeBase operator not xml type", K(ret), K(node->data_type()));
	} else if (OB_ISNULL(node)) {
			ret = OB_BAD_NULL_ERROR;
			LOG_WARN("xml node null", K(ret));
  } else if (!is_container_tc(node->type())) {
    if (OB_FAIL(res.push_back(node))) {
      LOG_WARN("get child failed", K(ret), K(node->type()));
    }
  } else if (OB_FAIL(node->get_children(res))) { // get children
    LOG_WARN("get child failed", K(ret), K(node));
  }
	return ret;
}

int ObXmlUtil::alloc_arg_node(ObIAllocator *allocator, ObPathArgNode*& node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathArgNode* arg_node =
    static_cast<ObPathArgNode*> (allocator->alloc(sizeof(ObPathArgNode)));
    if (OB_ISNULL(arg_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at location_node", K(ret));
    } else {
      node = arg_node;
    }
  }
  return ret;
}

int ObXmlUtil::alloc_filter_node(ObIAllocator *allocator, ObXmlPathFilter*& node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObXmlPathFilter* filter_node =
    static_cast<ObXmlPathFilter*> (allocator->alloc(sizeof(ObXmlPathFilter)));
    if (OB_ISNULL(filter_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at location_node", K(ret));
    } else {
      node = filter_node;
    }
  }
  return ret;
}

int ObXmlUtil::cast_to_string(const ObString &val, ObIAllocator &allocator, ObStringBuffer& result, ObCollationType cs_type)
{
  INIT_SUCC(ret);
  ObMulModeNodeType node_type = M_MAX_TYPE;
  ParamPrint param_list;
  ObIMulModeBase *node = NULL;
  ObMulModeMemCtx* mem_ctx = NULL;
  ObNsSortedVector* ns_vec_point = nullptr;
  ObNsSortedVector ns_vec;

  if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_FAIL(ObXmlUtil::xml_bin_type(val, node_type))) {
    LOG_WARN("xml bin type failed", K(val));
  } else if (OB_FAIL(ObMulModeFactory::get_xml_base(mem_ctx, val,
                                                    ObNodeMemType::BINARY_TYPE,
                                                    ObNodeMemType::BINARY_TYPE,
                                                    node, M_DOCUMENT, true))) {
    LOG_WARN("fail to get xml base", K(ret), K(val));
  } else if (OB_FAIL(ObXmlUtil::init_print_ns(&allocator, node, ns_vec, ns_vec_point))) {
    LOG_WARN("fail to init ns vector by extend area", K(ret));
  // default size value of print_document is 2
  } else if (OB_FAIL(node->print_document(result, cs_type, node_type == M_UNPARESED_DOC ? ObXmlFormatType::NO_FORMAT : ObXmlFormatType::WITH_FORMAT, 2, ns_vec_point))) {
    LOG_WARN("print document failed", K(ret));
  }

  return ret;
}

bool ObXmlUtil::is_xml_doc_over_depth(uint64_t depth)
{
  return depth > OB_XML_PARSER_MAX_DEPTH_;
}

int ObXmlUtil::revert_escape_character(ObIAllocator &allocator, ObString &input_str, ObString &output_str)
{
  int ret = OB_SUCCESS;
  const char *ptr = input_str.ptr();
  ObStringBuffer buff(&allocator);
  int64_t idx = 0;
  while(idx < input_str.length() && OB_SUCC(ret)) {
    ObString ref;
    int64_t ref_len = 0;
    if (*(ptr+idx) == '&' && ObXmlParserUtils::is_entity_ref(input_str, idx, ref, ref_len)) {
      // append entity ref and increment idx
      if (OB_FAIL(buff.append(ref))) {
        LOG_WARN("fail to append ref char", K(ret));
      } else {
        idx += ref_len;
      }
    } else if (OB_FAIL(buff.append(ptr+idx, 1))) {
      LOG_WARN("fail to append char", K(ret));
    } else {
      idx++;
    }
  }

  if (OB_SUCC(ret)) {
    ObString res(buff.length(), buff.ptr());
    if (OB_FAIL(ob_write_string(allocator, res, output_str))) {
      LOG_WARN("fail to write string", K(ret), K(res));
    }
  }

  return ret;
}

int ObXmlUtil::init_extend_ns_vec(ObIAllocator *allocator, ObIMulModeBase *src, ObNsSortedVector& ns_vec)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(src) || OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObXmlBin* bin = static_cast<ObXmlBin*>(src);
    ObXmlBin extend_bin;
    ns_vec.reset();
    if (OB_FAIL(bin->get_extend(extend_bin))) {
      LOG_WARN("fail to get extend bin", K(ret));
    } else {
      ObNsPairCmp cmp;
      ObNsPairUnique unique;
      ObXmlBin bin_buffer;
      ObNsSortedVector::iterator pos = ns_vec.end();
      ObIMulModeBase* cur = nullptr;
      int64_t num_children = extend_bin.attribute_size();

      for (int64_t i = 0; OB_SUCC(ret) && i < num_children; i ++) {
        cur = extend_bin.attribute_at(i, &bin_buffer);
        if (OB_ISNULL(cur)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get child from extend", K(ret), K(i));
        } else if (cur->type() != M_NAMESPACE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should be all ns", K(ret), K(i));
        } else {
          ObNsPair* tmp_ns = static_cast<ObNsPair*> (allocator->alloc(sizeof(ObNsPair)));
          if (OB_ISNULL(tmp_ns)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate row buffer failed at seek result", K(ret));
          } else {
            tmp_ns = new (tmp_ns) ObNsPair();
            ObString tmp_key;
            if (OB_FAIL(cur->get_key(tmp_key))) {
              LOG_WARN("failed to get ns", K(ret), K(i));
            } else {
              tmp_ns->set_xml_key(tmp_key);
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(cur->get_value(tmp_ns->value_))) {
              LOG_WARN("failed to get ns", K(ret), K(i));
            } else if (OB_FAIL(ns_vec.insert_unique(tmp_ns, pos, cmp, unique))) {
              LOG_WARN("should notduplicated nodes", K(ret), K(i));
            }
          }
        }
      } // end for
    }
  }
  return ret;
}
int ObXmlUtil::delete_dup_ns_definition(ObIMulModeBase *data, ObNsSortedVector& origin_vec, ObVector<ObNsPair*>& delete_vec)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(data)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (data->attribute_size() > 0) {
    ObNsPairCmp cmp;
    ObNsPairUnique unique;
    ObNsPair tmp_pair;
    ObXmlBin bin_buffer;
    ObIMulModeBase* cur = nullptr;
    int64_t num_children = data->attribute_size();

    for (int64_t i = 0; OB_SUCC(ret) && i < num_children; i++) {
      ObNsSortedVector::iterator pos = origin_vec.end();
      cur = data->attribute_at(i, &bin_buffer);
      if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get child from element", K(ret), K(i));
      } else if (cur->type() != ObMulModeNodeType::M_NAMESPACE) {
      } else if (OB_FAIL(cur->get_key(tmp_pair.key_)) || OB_FAIL(cur->get_value(tmp_pair.value_))) {
        LOG_WARN("failed to get ns", K(ret), K(i));
      } else if (OB_FAIL(origin_vec.find(&tmp_pair, pos, cmp, unique)) || pos == origin_vec.end()) {
        if (ret == OB_ENTRY_NOT_EXIST) { // didn't find, not duplicate ns, it's normal
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(delete_vec.push_back(*pos))) { // record ns that will be delete
        LOG_WARN("failed to record", K(ret), K(i));
      } else if (OB_FAIL(origin_vec.remove(pos))) { // remove duplicate ns
        LOG_WARN("failed to remove duplicate", K(ret), K(i));
      }
    }
  }
  return ret;
}
int ObXmlUtil::check_ns_conflict(ObIMulModeBase* cur_parent,
                                ObIMulModeBase* &last_parent,
                                ObXmlBin *cur,
                                common::hash::ObHashMap<ObString, ObString>& ns_map,
                                bool& conflict)
{
  INIT_SUCC(ret);
  conflict = false;
  if (OB_ISNULL(cur)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_ISNULL(last_parent) && ns_map.size() == 0) {
    // add directly
    int ns_num = cur->attribute_size();
    bool end_check = false;
    for (int pos = 0; OB_SUCC(ret) && !end_check &&pos < ns_num; ++pos) {
      ObXmlBin buff(*cur);
      ObXmlBin* tmp = &buff;
      if (OB_FAIL(cur->construct(tmp, nullptr))) {
        LOG_WARN("failed to dup bin.", K(ret));
      } else if (OB_FAIL(tmp->set_at(pos))) {
        LOG_WARN("failed to set at child.", K(ret));
      } else if (tmp->type() == M_NAMESPACE) {
        ObString key;
        ObString value;
        // init ns node
        if (OB_FAIL(tmp->get_key(key))) {
          LOG_WARN("failed to eval key.", K(ret));
        } else if (OB_FAIL(tmp->get_value(value))) {
          LOG_WARN("failed to eval value.", K(ret));
        } else if (OB_FAIL(ns_map.set_refactored(key, value))) {
          LOG_WARN("fail to add ns from map", K(ret), K(key));
        }
      } else if (tmp->type() == M_ATTRIBUTE) {
      } else {
        end_check = true;  // neither ns nor attribute, stop searching
      }
    }
  } else if (last_parent == cur_parent) {
    // do nothing, same parent means same ancestor, don't need to check/add ns
  } else {
    // check conflicts, record new ns definition idx
    int ns_num = cur->attribute_size();
    ObArray<int> new_ns_idx;
    bool end_check = false;
    for (int pos = 0; OB_SUCC(ret) && !end_check && pos < ns_num && !conflict; ++pos) {
      ObXmlBin buff(*cur);
      ObXmlBin* tmp = &buff;
      if (OB_FAIL(cur->construct(tmp, nullptr))) {
        LOG_WARN("failed to dup bin.", K(ret));
      } else if (OB_FAIL(tmp->set_at(pos))) {
        LOG_WARN("failed to set at child.", K(ret));
      } else if (tmp->type() == M_NAMESPACE) {
        ObString key;
        ObString value;
        ObString* find_val;
        // init ns node
        if (OB_FAIL(tmp->get_key(key))) {
          LOG_WARN("failed to eval key.", K(ret));
        } else if (OB_FAIL(tmp->get_value(value))) {
          LOG_WARN("failed to eval value.", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_NOT_NULL(find_val = ns_map.get(key))) {
          if (find_val->compare(value) != 0) {
            conflict = true;
          }
        } else if (OB_FAIL(new_ns_idx.push_back(pos))){
          LOG_WARN("failed to record idx.", K(ret));
        }
      } else if (tmp->type() == M_ATTRIBUTE) {
      } else {
        end_check = true;  // neither ns nor attribute, stop searching
      }
    }
    for (int pos = 0; OB_SUCC(ret) && pos < new_ns_idx.size() && !conflict; ++pos) {
      ObXmlBin buff(*cur);
      ObXmlBin* tmp = &buff;
      if (OB_FAIL(cur->construct(tmp, nullptr))) {
        LOG_WARN("failed to dup bin.", K(ret));
      } else if (OB_FAIL(tmp->set_at(new_ns_idx[pos]))) {
        LOG_WARN("failed to set at child.", K(ret));
      } else if (tmp->type() == M_NAMESPACE) {
        ObString key;
        ObString value;
        // init ns node
        if (OB_FAIL(tmp->get_key(key))) {
          LOG_WARN("failed to eval key.", K(ret));
        } else if (OB_FAIL(tmp->get_value(value))) {
          LOG_WARN("failed to eval value.", K(ret));
        } else if (OB_FAIL(ns_map.set_refactored(key, value))) {
          LOG_WARN("fail to add ns from map", K(ret), K(key));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("must be ns.", K(ret));
      }
    }
  }
  // update parent anyways
  last_parent = cur_parent;
  return ret;
}
int ObXmlUtil::ns_to_extend(ObMulModeMemCtx* mem_ctx,
                            common::hash::ObHashMap<ObString, ObString>& ns_map,
                            ObStringBuffer *buffer)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(buffer)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObXmlElement element_ns(ObMulModeNodeType::M_ELEMENT, mem_ctx);
    ret = element_ns.init();
    ObArray<ObXmlAttribute> ns_vec;
    common::hash::ObHashMap<ObString, ObString>::iterator ns_map_iter;
    for (ns_map_iter = ns_map.begin(); OB_SUCC(ret) && ns_map_iter != ns_map.end(); ns_map_iter++) {
      ObXmlAttribute ns_node(ObMulModeNodeType::M_NAMESPACE, mem_ctx);
      ObString key;
      ObString value;
      // init ns node
      if (OB_SUCC(ns_vec.push_back(ns_node))) {
        ns_vec[ns_vec.size() - 1].set_xml_key(ns_map_iter->first);
        ns_vec[ns_vec.size() - 1].set_value(ns_map_iter->second);
        if (OB_FAIL(element_ns.add_attribute(&ns_vec[ns_vec.size() - 1]))) {
          LOG_WARN("fail to add ns", K(ret));
        }
      }
    }
    // serialize element node as extend area
    if (OB_SUCC(ret)) {
      ObXmlElementSerializer serializer_element(&element_ns, buffer);
      if (OB_FAIL(serializer_element.serialize(0))) {
        LOG_WARN("failed to serialize.", K(ret));
      }
    }
  }
  return ret;
}
int ObXmlUtil::add_ns_def_if_necessary(uint32_t format_flag, ObStringBuffer &x_buf, const ObString& origin_prefix,
                                       ObNsSortedVector* element_ns_vec, ObVector<ObNsPair*>& delete_ns_vec)
{
  INIT_SUCC(ret);
  ObString prefix;
  if (origin_prefix.empty()) {
    prefix = "xmlns";
  } else {
    prefix = origin_prefix;
  }
  if (OB_ISNULL(element_ns_vec)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (element_ns_vec->size() > 0) {
    ObNsPair tmp_pair(prefix);
    ObNsPairCmp cmp;
    ObNsPairUnique unique;
    ObNsSortedVector::iterator pos = element_ns_vec->end();
    if (OB_FAIL(element_ns_vec->find(&tmp_pair, pos, cmp, unique)) || pos == element_ns_vec->end()) {
      if (ret == OB_ENTRY_NOT_EXIST) { // didn't find, not duplicate ns, it's normal
        ret = OB_SUCCESS;
      }
    } else if (OB_NOT_NULL(pos)) {
      if (OB_FAIL(x_buf.append(" "))) {
        LOG_WARN("fail to print space in ns", K(ret));
      }
      // append default ns or prefix ns
      if (OB_FAIL(ret)) {
      } else if ((*pos)->key_.ptr() == nullptr
                || (*pos)->key_.length() == 0
                || (*pos)->key_.case_compare("xmlns") == 0) {
        if (OB_FAIL(x_buf.append("xmlns"))) {
          LOG_WARN("fail to append default ns", K(ret));
        }
      } else if (OB_FAIL(ObXmlUtil::append_qname(x_buf, "xmlns", (*pos)->key_))) {
        LOG_WARN("fail to print prefix in ns", K(ret));
      }
      // append ns value
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(x_buf.append("=\""))) {
        LOG_WARN("fail to print =\" in ns", K(ret));
      } else if (!(format_flag & NO_ENTITY_ESCAPE)) {
        if (OB_FAIL(ObXmlParserUtils::escape_xml_text((*pos)->value_, x_buf))) {
          LOG_WARN("fail to print text with escape char", K(ret));
        }
      } else if (OB_FAIL(x_buf.append((*pos)->value_))) {
        LOG_WARN("fail to print value in ns", K(ret), K((*pos)->value_));
      }
      // delete ns definition that already printed
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(x_buf.append("\""))) {
        LOG_WARN("fail to print \" in ns", K(ret));
      } else if (OB_FAIL(delete_ns_vec.push_back(*pos))) { // record duplicate ns
        LOG_WARN("failed to record duplicate", K(ret));
      } else if (OB_FAIL(element_ns_vec->remove(pos))) { // remove duplicate ns
        LOG_WARN("failed to remove duplicate", K(ret));
      }
    }
  }
  return ret;
}

int ObXmlUtil::add_attr_ns_def(ObIMulModeBase *cur, uint32_t format_flag, ObStringBuffer &buf,
                              ObNsSortedVector* element_ns_vec, ObVector<ObNsPair*>& delete_ns_vec)
{
  INIT_SUCC(ret);
  ObXmlBin* bin = nullptr;
  if (OB_ISNULL(element_ns_vec) || OB_ISNULL(cur) || OB_ISNULL(bin = static_cast<ObXmlBin*>(cur))) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    int attr_num = bin->attribute_size();
    bool end_check = false;
    for (int pos = 0; OB_SUCC(ret) && !end_check && pos < attr_num; ++pos) {
      ObXmlBin buff(*bin);
      ObXmlBin* tmp = &buff;
      if (OB_FAIL(bin->construct(tmp, nullptr))) {
        LOG_WARN("failed to dup bin.", K(ret));
      } else if (OB_FAIL(tmp->set_at(pos))) {
        LOG_WARN("failed to set at child.", K(ret));
      } else if (tmp->type() == M_NAMESPACE) {
      } else if (tmp->type() == M_ATTRIBUTE) {
        ObString prefix = tmp->get_prefix();
        if (prefix.empty()) {
        } else if (OB_FAIL(ObXmlUtil::add_ns_def_if_necessary(format_flag, buf, prefix, element_ns_vec, delete_ns_vec))) {
          LOG_WARN("failed to add attribute ns.", K(ret));
        }
      } else {
        end_check = true;  // neither ns nor attribute, stop searching
      }
    }
  }
  return ret;
}
int ObXmlUtil::restore_ns_vec(ObNsSortedVector* element_ns_vec, ObVector<ObNsPair*>& delete_ns_vec)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(element_ns_vec)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObNsPairCmp cmp;
    ObNsPairUnique unique;
    for (int i = 0; OB_SUCC(ret) && i < delete_ns_vec.size(); ++i) {
      ObNsSortedVector::iterator pos = element_ns_vec->end();
      if (OB_FAIL(element_ns_vec->insert_unique(delete_ns_vec[i], pos, cmp, unique))) {
        LOG_WARN("failed to restore ns", K(ret), K(i));
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
