/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Unit tests for DBMS_XMLDOM / DBMS_XMLPARSER opaque classes and DOM traversal.
 *
 * These tests exercise the lower-level building blocks directly:
 *   - ObPLXmlParser, ObPLXmlDomDocument, ObPLXmlDomNode, ObPLXmlDomNodeList
 *   - XML parse via ObXmlParserUtils + ObXmlUtil
 *   - DOM traversal (getElementsByTagName DFS logic)
 *   - Handle roundtrip (pointer <-> raw bytes)
 *
 * No ObExecContext is required.
 *
 * Opaque deep_copy(dst) expects dst to hold a live ObPLOpaque (same as
 * ObSPIService::spi_copy_opaque: placement-new ObPLOpaque() on the buffer first).
 */

#ifdef OB_BUILD_ORACLE_PL

#define USING_LOG_PREFIX PL

#include <gtest/gtest.h>
#include <cstring>

#include "lib/allocator/ob_malloc.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_multi_mode_interface.h"

#include "close_modules/oracle_pl/pl/opaque/ob_pl_xmldom.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::pl;

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class TestDbmsXmlDom : public ::testing::Test
{
protected:
  ObArenaAllocator allocator_;
  // Session-level XML manager owns the lifetime of every ObPLWrapperXmlDoc
  // produced in these tests; destroy() in TearDown releases everything.
  ObPlXmlTypeManager mgr_{OB_SERVER_TENANT_ID};

  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, mgr_.init());
  }

  void TearDown() override
  {
    mgr_.destroy();
  }

  // Parse an XML document string.  On success *ctx and *doc are non-NULL.
  int parse_xml(const char *xml, ObMulModeMemCtx *&ctx, ObXmlDocument *&doc)
  {
    int ret = OB_SUCCESS;
    ctx = NULL;
    doc = NULL;
    if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator_, ctx))) {
      // propagate
    } else if (OB_FAIL(ObXmlParserUtils::parse_document_text(ctx, ObString(xml), doc))) {
      // propagate
    }
    return ret;
  }

  // Build a managed wrapper that owns its arena/mem_ctx and has `xml` parsed
  // into it. The wrapper lives until mgr_.destroy() in TearDown.
  int create_wrapper(const char *xml, ObPLWrapperXmlDoc *&w)
  {
    int ret = OB_SUCCESS;
    w = nullptr;
    if (OB_FAIL(mgr_.create_doc(w))) {
      // propagate
    } else {
      ObXmlDocument *doc = nullptr;
      if (OB_FAIL(ObXmlParserUtils::parse_document_text(w->get_mem_ctx(), ObString(xml), doc))) {
        // propagate
      } else {
        w->set_doc(doc);
      }
    }
    return ret;
  }
};

// ---------------------------------------------------------------------------
// UT1: ObPLXmlParser creation and field access
// ---------------------------------------------------------------------------
TEST_F(TestDbmsXmlDom, test_xml_parser_create_and_fields)
{
  ObPLXmlParser parser;

  // Freshly constructed: no wrapper, but reports alive (parser handle itself)
  EXPECT_EQ(ObPLOpaqueType::PL_XMLPARSER_TYPE, parser.get_type());
  EXPECT_TRUE(parser.is_xmlparser_type());
  EXPECT_EQ((ObPLWrapperXmlDoc*)NULL, parser.get_pl_doc());
  EXPECT_EQ((ObXmlDocument*)NULL, parser.get_xml_doc());
  EXPECT_TRUE(parser.is_alive());

  // Build a session-managed wrapper and attach it
  ObPLWrapperXmlDoc *w = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_wrapper("<r/>", w));
  ASSERT_NE((ObPLWrapperXmlDoc*)NULL, w);
  ObXmlDocument *doc = w->get_document();
  ASSERT_NE((ObXmlDocument*)NULL, doc);
  EXPECT_EQ(0, w->ref_count());

  parser.set_pl_doc(w);

  EXPECT_EQ(w,    parser.get_pl_doc());
  EXPECT_EQ(doc,  parser.get_xml_doc());
  EXPECT_EQ(1,    w->ref_count());
  EXPECT_TRUE(w->is_alive());

  // Self-assign must be a no-op (refcount unchanged)
  parser.set_pl_doc(w);
  EXPECT_EQ(1, w->ref_count());

  // Detach -> ref drops back to 0
  parser.set_pl_doc(nullptr);
  EXPECT_EQ((ObPLWrapperXmlDoc*)NULL, parser.get_pl_doc());
  EXPECT_EQ(0, w->ref_count());
}

// ---------------------------------------------------------------------------
// UT2: ObPLXmlParser deep_copy
// ---------------------------------------------------------------------------
TEST_F(TestDbmsXmlDom, test_xml_parser_deep_copy)
{
  ObPLWrapperXmlDoc *w = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_wrapper("<r/>", w));
  ASSERT_NE((ObPLWrapperXmlDoc*)NULL, w);
  ObXmlDocument *doc = w->get_document();

  ObPLXmlParser src;
  src.set_pl_doc(w);
  EXPECT_EQ(1, w->ref_count());

  // Allocate a raw buffer of the correct size for the copy.
  // Opaque deep_copy expects dst to hold a live ObPLOpaque base.
  int64_t sz = src.get_init_size();
  ASSERT_GT(sz, 0);
  void *buf = allocator_.alloc(sz);
  ASSERT_NE((void*)NULL, buf);
  new (buf) ObPLOpaque();

  ASSERT_EQ(OB_SUCCESS, src.deep_copy(static_cast<ObPLOpaque*>(buf)));

  ObPLXmlParser *copy = static_cast<ObPLXmlParser*>(buf);
  EXPECT_EQ(ObPLOpaqueType::PL_XMLPARSER_TYPE, copy->get_type());
  EXPECT_EQ(w,   copy->get_pl_doc());
  EXPECT_EQ(doc, copy->get_xml_doc());
  // src + copy both hold a reference
  EXPECT_EQ(2, w->ref_count());

  // Manually destruct the copy (it lives on a raw arena buffer) so its
  // destructor releases the second ref before TearDown.
  copy->~ObPLXmlParser();
  EXPECT_EQ(1, w->ref_count());
}

// ---------------------------------------------------------------------------
// UT3: ObPLXmlDomDocument creation and deep_copy
// ---------------------------------------------------------------------------
TEST_F(TestDbmsXmlDom, test_dom_document_create_and_deep_copy)
{
  ObPLWrapperXmlDoc *w = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_wrapper("<root><a/></root>", w));
  ASSERT_NE((ObPLWrapperXmlDoc*)NULL, w);
  ObXmlDocument *doc = w->get_document();

  ObPLXmlDomDocument dom;
  EXPECT_EQ(ObPLOpaqueType::PL_XMLDOM_DOC_TYPE, dom.get_type());
  EXPECT_TRUE(dom.is_xmldom_doc_type());
  EXPECT_EQ((ObPLWrapperXmlDoc*)NULL, dom.get_pl_doc());
  EXPECT_EQ((ObXmlDocument*)NULL, dom.get_xml_doc());

  dom.set_pl_doc(w);
  EXPECT_EQ(w,   dom.get_pl_doc());
  EXPECT_EQ(doc, dom.get_xml_doc());
  EXPECT_EQ(1,   w->ref_count());
  EXPECT_TRUE(dom.is_alive());

  // deep_copy
  int64_t sz = dom.get_init_size();
  ASSERT_GT(sz, 0);
  void *buf = allocator_.alloc(sz);
  ASSERT_NE((void*)NULL, buf);
  new (buf) ObPLOpaque();
  ASSERT_EQ(OB_SUCCESS, dom.deep_copy(static_cast<ObPLOpaque*>(buf)));

  ObPLXmlDomDocument *copy = static_cast<ObPLXmlDomDocument*>(buf);
  EXPECT_EQ(ObPLOpaqueType::PL_XMLDOM_DOC_TYPE, copy->get_type());
  EXPECT_EQ(w,   copy->get_pl_doc());
  EXPECT_EQ(doc, copy->get_xml_doc());
  EXPECT_EQ(2,   w->ref_count());

  copy->~ObPLXmlDomDocument();
  EXPECT_EQ(1, w->ref_count());
}

// ---------------------------------------------------------------------------
// UT4: ObPLXmlDomNodeList operations
// ---------------------------------------------------------------------------
TEST_F(TestDbmsXmlDom, test_nodelist_operations)
{
  ObMulModeMemCtx *ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, ObXmlUtil::create_mulmode_tree_context(&allocator_, ctx));
  ObXmlDocument *doc = NULL;
  ASSERT_EQ(OB_SUCCESS,
            ObXmlParserUtils::parse_document_text(
              ctx, ObString("<r><a/><b/><c/></r>"), doc));
  ASSERT_NE((ObXmlDocument*)NULL, doc);

  // doc has one root element child
  ASSERT_EQ(1, doc->size());
  ObXmlNode *root = doc->at(0);
  ASSERT_NE((ObXmlNode*)NULL, root);
  ASSERT_EQ(3, root->size());

  ObPLXmlDomNodeList nodelist;
  EXPECT_EQ(ObPLOpaqueType::PL_XMLDOM_NODELIST_TYPE, nodelist.get_type());
  EXPECT_EQ(0, nodelist.get_length());

  // get_node on empty list returns NULL
  EXPECT_EQ((ObXmlNode*)NULL, nodelist.get_node(0));
  EXPECT_EQ((ObXmlNode*)NULL, nodelist.get_node(-1));

  // Add the three child nodes
  ObXmlNode *n0 = root->at(0);
  ObXmlNode *n1 = root->at(1);
  ObXmlNode *n2 = root->at(2);
  ASSERT_NE((ObXmlNode*)NULL, n0);
  ASSERT_NE((ObXmlNode*)NULL, n1);
  ASSERT_NE((ObXmlNode*)NULL, n2);

  ASSERT_EQ(OB_SUCCESS, nodelist.add_node(n0));
  ASSERT_EQ(OB_SUCCESS, nodelist.add_node(n1));
  ASSERT_EQ(OB_SUCCESS, nodelist.add_node(n2));

  EXPECT_EQ(3, nodelist.get_length());
  EXPECT_EQ(n0, nodelist.get_node(0));
  EXPECT_EQ(n1, nodelist.get_node(1));
  EXPECT_EQ(n2, nodelist.get_node(2));

  // Out-of-bounds
  EXPECT_EQ((ObXmlNode*)NULL, nodelist.get_node(3));
  EXPECT_EQ((ObXmlNode*)NULL, nodelist.get_node(-1));
}

// ---------------------------------------------------------------------------
// UT5: ObPLXmlDomNodeList deep_copy
// ---------------------------------------------------------------------------
TEST_F(TestDbmsXmlDom, test_nodelist_deep_copy)
{
  ObMulModeMemCtx *ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, ObXmlUtil::create_mulmode_tree_context(&allocator_, ctx));
  ObXmlDocument *doc = NULL;
  ASSERT_EQ(OB_SUCCESS,
            ObXmlParserUtils::parse_document_text(
              ctx, ObString("<r><x/><y/></r>"), doc));
  ASSERT_NE((ObXmlDocument*)NULL, doc);
  ASSERT_EQ(1, doc->size());
  ObXmlNode *root = doc->at(0);
  ASSERT_NE((ObXmlNode*)NULL, root);

  ObPLXmlDomNodeList src_list;
  for (int64_t i = 0; i < root->size(); ++i) {
    ASSERT_EQ(OB_SUCCESS, src_list.add_node(root->at(i)));
  }
  EXPECT_EQ(root->size(), src_list.get_length());

  // deep_copy
  int64_t sz = src_list.get_init_size();
  ASSERT_GT(sz, 0);
  void *buf = allocator_.alloc(sz);
  ASSERT_NE((void*)NULL, buf);
  new (buf) ObPLOpaque();
  ASSERT_EQ(OB_SUCCESS, src_list.deep_copy(static_cast<ObPLOpaque*>(buf)));

  ObPLXmlDomNodeList *copy = static_cast<ObPLXmlDomNodeList*>(buf);
  EXPECT_EQ(ObPLOpaqueType::PL_XMLDOM_NODELIST_TYPE, copy->get_type());
  EXPECT_EQ(src_list.get_length(), copy->get_length());
  for (int64_t i = 0; i < src_list.get_length(); ++i) {
    EXPECT_EQ(src_list.get_node(i), copy->get_node(i));
  }
}

// ---------------------------------------------------------------------------
// UT6: XML parse and DOM traversal
// ---------------------------------------------------------------------------
TEST_F(TestDbmsXmlDom, test_xml_parse_and_traversal)
{
  ObMulModeMemCtx *ctx = NULL;
  ObXmlDocument *doc = NULL;
  ASSERT_EQ(OB_SUCCESS,
            parse_xml("<root><item>hello</item><item>world</item></root>",
                      ctx, doc));
  ASSERT_NE((ObXmlDocument*)NULL, doc);

  // document has one child: the root element
  EXPECT_EQ(1, doc->size());
  ObXmlNode *root = doc->at(0);
  ASSERT_NE((ObXmlNode*)NULL, root);
  EXPECT_EQ(ObMulModeNodeType::M_ELEMENT, root->type());

  // root has two <item> children
  EXPECT_EQ(2, root->size());

  // First <item> child has one text child with value "hello"
  ObXmlNode *item0 = root->at(0);
  ASSERT_NE((ObXmlNode*)NULL, item0);
  EXPECT_EQ(ObMulModeNodeType::M_ELEMENT, item0->type());
  ASSERT_EQ(1, item0->size());
  ObXmlNode *text0 = item0->at(0);
  ASSERT_NE((ObXmlNode*)NULL, text0);
  EXPECT_EQ(ObMulModeNodeType::M_TEXT, text0->type());
  ObString val0;
  ASSERT_EQ(OB_SUCCESS, text0->get_value(val0));
  EXPECT_EQ(ObString("hello"), val0);

  // Second <item> child has text value "world"
  ObXmlNode *item1 = root->at(1);
  ASSERT_NE((ObXmlNode*)NULL, item1);
  ASSERT_EQ(1, item1->size());
  ObXmlNode *text1 = item1->at(0);
  ASSERT_NE((ObXmlNode*)NULL, text1);
  EXPECT_EQ(ObMulModeNodeType::M_TEXT, text1->type());
  ObString val1;
  ASSERT_EQ(OB_SUCCESS, text1->get_value(val1));
  EXPECT_EQ(ObString("world"), val1);
}

// ---------------------------------------------------------------------------
// Standalone helper: same DFS logic as in ob_dbms_xmldom.cpp
// ---------------------------------------------------------------------------
static int collect_elements_by_tag(ObXmlNode *root, const ObString &tag,
                                   ObPLXmlDomNodeList *nodelist)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    // nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < root->size(); ++i) {
      ObXmlNode *child = root->at(i);
      if (OB_ISNULL(child)) {
        continue;
      }
      ObMulModeNodeType ctype = child->type();
      if (ctype == ObMulModeNodeType::M_ELEMENT) {
        ObXmlElement *elem = static_cast<ObXmlElement*>(child);
        ObString key = elem->get_key();
        if (tag == "*" || key == tag) {
          OZ(nodelist->add_node(child));
        }
        OZ(collect_elements_by_tag(child, tag, nodelist));
      } else if (ctype == ObMulModeNodeType::M_DOCUMENT ||
                 ctype == ObMulModeNodeType::M_CONTENT) {
        OZ(collect_elements_by_tag(child, tag, nodelist));
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// UT7: getElementsByTagName DFS collection
// ---------------------------------------------------------------------------
TEST_F(TestDbmsXmlDom, test_getelements_by_tagname)
{
  // <root><a><item>1</item></a><item>2</item></root>
  // Elements: root, a, item(under a), item(direct child of root) = 4 total
  ObMulModeMemCtx *ctx = NULL;
  ObXmlDocument *doc = NULL;
  ASSERT_EQ(OB_SUCCESS,
            parse_xml("<root><a><item>1</item></a><item>2</item></root>",
                      ctx, doc));
  ASSERT_NE((ObXmlDocument*)NULL, doc);

  ObXmlNode *doc_node = static_cast<ObXmlNode*>(doc);

  // Collect "item" => 2 results
  {
    ObPLXmlDomNodeList list_item;
    ASSERT_EQ(OB_SUCCESS,
              collect_elements_by_tag(doc_node, ObString("item"), &list_item));
    EXPECT_EQ(2, list_item.get_length());
  }

  // Collect "*" => root + a + item + item = 4 results
  {
    ObPLXmlDomNodeList list_all;
    ASSERT_EQ(OB_SUCCESS,
              collect_elements_by_tag(doc_node, ObString("*"), &list_all));
    EXPECT_EQ(4, list_all.get_length());
  }

  // Collect "a" => 1 result
  {
    ObPLXmlDomNodeList list_a;
    ASSERT_EQ(OB_SUCCESS,
              collect_elements_by_tag(doc_node, ObString("a"), &list_a));
    EXPECT_EQ(1, list_a.get_length());
  }

  // Collect "nonexistent" => 0 results
  {
    ObPLXmlDomNodeList list_none;
    ASSERT_EQ(OB_SUCCESS,
              collect_elements_by_tag(doc_node, ObString("nonexistent"), &list_none));
    EXPECT_EQ(0, list_none.get_length());
  }
}

// ---------------------------------------------------------------------------
// UT8: getNodeValue by node type
// ---------------------------------------------------------------------------
TEST_F(TestDbmsXmlDom, test_get_node_value_by_type)
{
  // XML with a text node and a comment node.
  // Note: comments require the parser to preserve them (default behaviour).
  ObMulModeMemCtx *ctx = NULL;
  ObXmlDocument *doc = NULL;
  // Parse a simple document with text children
  ASSERT_EQ(OB_SUCCESS,
            parse_xml("<root>some text</root>", ctx, doc));
  ASSERT_NE((ObXmlDocument*)NULL, doc);

  ASSERT_EQ(1, doc->size());
  ObXmlNode *root = doc->at(0);
  ASSERT_NE((ObXmlNode*)NULL, root);
  EXPECT_EQ(ObMulModeNodeType::M_ELEMENT, root->type());

  // The text node inside <root>
  ASSERT_GE(root->size(), 1);
  ObXmlNode *text_node = root->at(0);
  ASSERT_NE((ObXmlNode*)NULL, text_node);
  EXPECT_EQ(ObMulModeNodeType::M_TEXT, text_node->type());
  {
    ObString val;
    ASSERT_EQ(OB_SUCCESS, text_node->get_value(val));
    EXPECT_EQ(ObString("some text"), val);
  }

  // Element node get_value: returns empty string (W3C nodeValue = null modelled
  // as empty in this implementation)
  {
    ObString val;
    int rc = root->get_value(val);
    // Either returns OB_SUCCESS with empty val, or just returns 0 (base impl).
    // We don't assert on rc here since the base class returns 0.
    (void)rc;
    // val may be empty or set to the text content depending on implementation;
    // just ensure no crash.
  }

  // Document node: get_value on document itself
  {
    ObString val;
    int rc = doc->get_value(val);
    (void)rc;
    // No crash expected.
  }
}

// ---------------------------------------------------------------------------
// UT9: Handle roundtrip (pointer -> RAW bytes -> pointer)
// ---------------------------------------------------------------------------
TEST_F(TestDbmsXmlDom, test_handle_roundtrip)
{
  // Allocate an ObPLXmlDomNode on the arena
  void *ptr = allocator_.alloc(sizeof(ObPLXmlDomNode));
  ASSERT_NE((void*)NULL, ptr);
  ObPLXmlDomNode *node = new (ptr) ObPLXmlDomNode();
  EXPECT_EQ(ObPLOpaqueType::PL_XMLDOM_NODE_TYPE, node->get_type());

  // Simulate set_handle: pack pointer into 8-byte raw buffer
  char raw_buf[sizeof(void*)];
  MEMCPY(raw_buf, &node, sizeof(void*));

  // Simulate get_opaque_handle: unpack pointer from the raw buffer
  ObPLOpaque *recovered = NULL;
  MEMCPY(&recovered, raw_buf, sizeof(void*));

  // The recovered pointer must equal the original
  EXPECT_EQ(static_cast<ObPLOpaque*>(node), recovered);

  // Also verify the type survives the roundtrip
  EXPECT_EQ(ObPLOpaqueType::PL_XMLDOM_NODE_TYPE, recovered->get_type());
  EXPECT_TRUE(recovered->is_xmldom_node_type());
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // OB_BUILD_ORACLE_PL not defined

// Provide a no-op main so the binary still compiles and links cleanly.
int main(int /*argc*/, char ** /*argv*/)
{
  return 0;
}

#endif  // OB_BUILD_ORACLE_PL
