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
#define USING_LOG_PREFIX SHARE
#ifndef OCEANBASE_UNIT_TEST_XML_UTILS_H_
#define OCEANBASE_UNIT_TEST_XML_UTILS_H_

#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/oblog/ob_log.h"
namespace oceanbase {
namespace common {

namespace ObXmlTestUtils {

class ObXmlNodeVisitor {
public:
  virtual int visit(ObXmlNode* node);
  virtual int visit_xml_document(ObXmlDocument* node) = 0;
  virtual int visit_xml_content(ObXmlDocument* node) = 0;
  virtual int visit_xml_element(ObXmlElement* node) = 0;
  virtual int visit_xml_attribute(ObXmlAttribute* node) = 0;
  virtual int visit_xml_text(ObXmlText* node) = 0;
  virtual int visit_xml_comment(ObXmlText* node) = 0;
  virtual int visit_xml_cdata(ObXmlText* node) = 0;
  virtual int visit_xml_processing_instruction(ObXmlAttribute* node) = 0;
};

int ObXmlNodeVisitor::visit(ObXmlNode* node) {
  INIT_SUCC(ret);
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else {
    switch (node->type())
    {
    case ObMulModeNodeType::M_DOCUMENT:
      if (OB_FAIL(visit_xml_document(ObXmlUtil::xml_node_cast<ObXmlDocument>(node, node->type())))) {
        LOG_WARN("visit document failed", K(ret), K(node->type()));
      }
      break;
    case ObMulModeNodeType::M_CONTENT:
      if (OB_FAIL(visit_xml_content(ObXmlUtil::xml_node_cast<ObXmlDocument>(node, node->type())))) {
        LOG_WARN("visit document failed", K(ret), K(node->type()));
      }
      break;
    case ObMulModeNodeType::M_ELEMENT:
      if (OB_FAIL(visit_xml_element(ObXmlUtil::xml_node_cast<ObXmlElement>(node, node->type())))) {
        LOG_WARN("visit element failed", K(ret), K(node->type()));
      }
      break;
    case ObMulModeNodeType::M_ATTRIBUTE:
    case ObMulModeNodeType::M_NAMESPACE:
      if (OB_FAIL(visit_xml_attribute(ObXmlUtil::xml_node_cast<ObXmlAttribute>(node, node->type())))) {
        LOG_WARN("visit attribute failed", K(ret), K(node->type()));
      }
      break;
    case ObMulModeNodeType::M_TEXT:
      if (OB_FAIL(visit_xml_text(ObXmlUtil::xml_node_cast<ObXmlText>(node, node->type())))) {
        LOG_WARN("visit text failed", K(ret), K(node->type()));
      }
      break;
    case ObMulModeNodeType::M_CDATA:
      if (OB_FAIL(visit_xml_cdata(ObXmlUtil::xml_node_cast<ObXmlText>(node, node->type())))) {
        LOG_WARN("visit cdata failed", K(ret), K(node->type()));
      }
      break;
    case ObMulModeNodeType::M_COMMENT:
      if (OB_FAIL(visit_xml_comment(ObXmlUtil::xml_node_cast<ObXmlText>(node, node->type())))) {
        LOG_WARN("visit comment failed", K(ret), K(node->type()));
      }
      break;
    case ObMulModeNodeType::M_INSTRUCT:
      if (OB_FAIL(visit_xml_processing_instruction(ObXmlUtil::xml_node_cast<ObXmlAttribute>(node, node->type())))) {
        LOG_WARN("visit processing instruction failed", K(ret), K(node->type()));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node type incorrect", K(ret), K(node->type()));
      break;
    }
  }
  return ret;
}

class ObXmlTreeTextWriter : public ObXmlNodeVisitor{
public:
  ObXmlTreeTextWriter(ObIAllocator* allocator) : buffer_(allocator) {}
  virtual ~ObXmlTreeTextWriter() {}

  virtual int visit_xml_document(ObXmlDocument* node);
  virtual int visit_xml_content(ObXmlDocument* node);
  virtual int visit_xml_element(ObXmlElement* node);
  virtual int visit_xml_attribute(ObXmlAttribute* node);
  virtual int visit_xml_text(ObXmlText* node);
  virtual int visit_xml_comment(ObXmlText* node);
  virtual int visit_xml_cdata(ObXmlText* node);
  virtual int visit_xml_processing_instruction(ObXmlAttribute* node);

  int append_qname(const ObString& prefix, const ObString& localname) {
    INIT_SUCC(ret);
    if (!prefix.empty()) {
      buffer_.append(prefix);
      buffer_.append(":");
    }
    if (!localname.empty()) {
      buffer_.append(localname);
    }
    return ret;
  }

  void reuse() {
    buffer_.reuse();
  }

  ObString get_xml_text() {
    return ObString(buffer_.length(), buffer_.ptr());
  }

private:
  ObStringBuffer buffer_;
};

int ObXmlTreeTextWriter::visit_xml_document(ObXmlDocument* node) {
  INIT_SUCC(ret);
  if(OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null node", K(ret));
  } else {
    if (node->has_xml_decl()) {
      buffer_.append("<?xml");
      if (!node->get_version().empty()) {
        buffer_.append(" version=\"");
        buffer_.append(node->get_version());
        buffer_.append("\"");
      }
      if (! node->get_encoding().empty() || node->get_encoding_flag()) {
        buffer_.append(" encoding=\"");
        buffer_.append(node->get_encoding());
        buffer_.append("\"");
      }

      switch(node->get_standalone()) {
      case OB_XML_STANDALONE_NO:
        buffer_.append(" standalone=\"no\"");
        break;
      case OB_XML_STANDALONE_YES:
        buffer_.append(" standalone=\"yes\"");
        break;
      }
      buffer_.append("?>\n");
    }

    for (int i = 0; OB_SUCC(ret) && i < node->size(); ++i) {
      ObXmlNode* child = node->at(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get child failed", K(ret), K(i));
      } else if (OB_FAIL(visit(child))) {
        LOG_WARN("visit child failed", K(ret), K(i));
      } else {
        buffer_.append("\n");
      }
    }
  }
  return ret;
}

int ObXmlTreeTextWriter::visit_xml_content(ObXmlDocument* node) {
  INIT_SUCC(ret);
  if(OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null node", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node->size(); ++i) {
      ObXmlNode* child = node->at(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get child failed", K(ret), K(i));
      } else if (OB_FAIL(visit(child))) {
        LOG_WARN("visit child failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObXmlTreeTextWriter::visit_xml_element(ObXmlElement* node) {
  INIT_SUCC(ret);
  if(OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null node", K(ret));
  } else {
    buffer_.append("<");
    append_qname(node->get_prefix(), node->get_key());

    for (int i = 0; OB_SUCC(ret) && i < node->attribute_size(); ++i) {
      ObXmlAttribute* attr = NULL;
      if (OB_FAIL(node->get_attribute(attr, i))) {
        LOG_WARN("get attr failed", K(ret), K(i));
      } else if (OB_ISNULL(attr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get attr", K(ret));
      } else {
        buffer_.append(" ");
        if (OB_FAIL(visit(attr))) {
          LOG_WARN("visit attr failed", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (node->is_empty()) {
        buffer_.append("/>");
      } else {
        buffer_.append(">");
        for (int i = 0; OB_SUCC(ret) && i < node->size(); ++i) {
          ObXmlNode* child = node->at(i);
          if (OB_ISNULL(child)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get child failed", K(ret), K(i));
          } else if (OB_FAIL(visit(child))) {
            LOG_WARN("visit child failed", K(ret), K(i));
          }
        }
      }
    }
    if (OB_SUCC(ret) && !node->is_empty()) {
      buffer_.append("</");
      append_qname(node->get_prefix(), node->get_key());
      buffer_.append(">");
    }
  }
  return ret;
}

int ObXmlTreeTextWriter::visit_xml_attribute(ObXmlAttribute* node) {
  INIT_SUCC(ret);
  if(OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null node", K(ret));
  } else {
    ObString attr_value;
    append_qname(node->get_prefix(), node->get_key());
    buffer_.append("=\"");
    node->get_value(attr_value);
    buffer_.append(attr_value);
    buffer_.append("\"");
  }
  return ret;
}

int ObXmlTreeTextWriter::visit_xml_text(ObXmlText* node) {
  INIT_SUCC(ret);
  if(OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null node", K(ret));
  } else {
    ObString value;
    node->get_value(value);
    buffer_.append(value);
  }
  return ret;
}

int ObXmlTreeTextWriter::visit_xml_cdata(ObXmlText* node) {
  INIT_SUCC(ret);
  if(OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null node", K(ret));
  } else {
    ObString value;
    buffer_.append("<![CDATA[");
    node->get_value(value);
    buffer_.append(value);
    buffer_.append("]]>");
  }
  return ret;
}

int ObXmlTreeTextWriter::visit_xml_comment(ObXmlText* node) {
  INIT_SUCC(ret);
  if(OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null node", K(ret));
  } else {
    ObString value;
    buffer_.append("<!--");
    node->get_value(value);
    buffer_.append(value);
    buffer_.append("-->");
  }
  return ret;
}

int ObXmlTreeTextWriter::visit_xml_processing_instruction(ObXmlAttribute* node) {
  INIT_SUCC(ret);
  if(OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null node", K(ret));
  } else {
    ObString value;
    buffer_.append("<?");
    buffer_.append(node->get_key());
    node->get_value(value);
    if (!value.empty()) {
      buffer_.append(" ");
      buffer_.append(value);
    }
    buffer_.append("?>");
  }
  return ret;
}


class ObXmlWriterUtils {
public:
  static ObString to_xml_text(ObIAllocator* allocator, ObXmlNode& node) {
     ObXmlTreeTextWriter writer(allocator);
     writer.visit(&node);
     return writer.get_xml_text();
  }
};

};


}//end namespace share
}//end namespace oceanbase

#endif // OCEANBASE_UNIT_TEST_XML_UTILS_H_
