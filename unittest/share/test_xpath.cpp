#include <gtest/gtest.h>
#include "lib/string/ob_sql_string.h"
#define private public
#include "test_xml_utils.h"
#include "lib/xml/ob_xpath.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/xml/ob_xml_parser.h"
#include "libxml2/libxml/xmlmemory.h"
#include "lib/xml/ob_xml_util.h"
#undef private

using namespace oceanbase::common;

using namespace ObXmlTestUtils;
class TestXPath : public ::testing::Test {
public:
  TestXPath()
  {}
  ~TestXPath()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestXPath);
};

ObString axis_path_map[30][2] = {
  {"/self::abc","/self::abc"},
  {"/parent::a","/parent::a"},
  {"/ancestor::a1","/ancestor::a1"},
  {"/ancestor-or-self::a1","/ancestor-or-self::a1"},
  {"/child::a1","/a1"},
  {"/ancestor::_a1","/ancestor::_a1"},
  {"/descendant::a_2","/descendant::a_2"},
  {"/descendant-or-self::a1","/descendant-or-self::a1"},
  {"/following-sibling::a1","/following-sibling::a1"},
  {"/following::a1","/following::a1"}, //10
  {"/preceding-sibling::a1","/preceding-sibling::a1"},
  {"/preceding::k","/preceding::k"},
  {"/attribute::k","/@k"},
  {"/@k","/@k"},
  {"/namespace::k","/namespace::k"}, // 15
  {"/self","/self"},
  {"/parent","/parent"},
  {"/ancestor","/ancestor"},
  {"/ancestor-or-self","/ancestor-or-self"},
  {"/child","/child"},
  {"/child::ancestor","/ancestor"},
  {"/descendant","/descendant"},
  {"/descendant-or-self","/descendant-or-self"},
  {"/following-sibling","/following-sibling"},
  {"/following","/following"}, //25
  {"/preceding-sibling","/preceding-sibling"},
  {"/preceding","/preceding"},
  {"/attribute","/attribute"},
  {"/a/preceding-sibling","/a/preceding-sibling"},
  {"/namespace","/namespace"}, //30
};

TEST_F(TestXPath, test_axis)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  std::cout<<"------begin axis test------"<<std::endl;
  ObString default_ns;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  for (int i = 0; i < 30; i++) {
    std::cout<<i<<" input: "<<axis_path_map[i][0]<<std::endl;
    ObPathParser test_path(ctx, ObParserType::PARSER_XML_PATH, axis_path_map[i][0], default_ns, nullptr);
    ret = test_path.parse_path();
    ASSERT_EQ(OB_SUCCESS, ret);
    ObJsonBuffer buf(&allocator);
    ret = test_path.to_string(buf);
    std::cout<<buf.ptr()<<std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString str2(buf.ptr());
    ASSERT_EQ(axis_path_map[i][1], str2);
  }
}

ObString abbreviation_path_map[30][2] = {
  {"/","/"},
  {"abc","abc"},
  {".","self::node()"},
  {"..","parent::node()"},
  {"//a","//a"},
  {"/.","/self::node()"},
  {"/..","/parent::node()"},
  {"/*","/*"},
  {"/@*","/@*"},
  {"/attribute::*","/@*"}, //10
  {"/a/./..","/a/self::node()/parent::node()"},
  {"/.tag","/self::tag"},
  {"/.text()","/self::node()"},
  {"//*","//*"},
  {"@test","@test"}, // 15
  {"/*/*/*","/*/*/*"},
  {"//@ba","/descendant-or-self::node()/@ba"},
  {"/./a","/self::node()/a"},
  {"../a","parent::node()/a"},
  {"/a/child::*","/a/*"}, // 20
  {"/a/b/c/ancestor-or-self::*","/a/b/c/ancestor-or-self::*"},
  {"/.//@x","/self::node()/descendant-or-self::node()/@x"},
};

TEST_F(TestXPath, test_abbreviation)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  std::cout<<"------begin abbreviation test------"<<std::endl;
  ObJsonBuffer buf(&allocator);
  ObString default_ns;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  for (int i = 0; i < 22; i++) {
    buf.reset();
    std::cout<<i<<" input: "<<abbreviation_path_map[i][0]<<std::endl;
    ObPathParser test_path(ctx, ObParserType::PARSER_XML_PATH, abbreviation_path_map[i][0], default_ns, nullptr);
    ret = test_path.parse_path();
    ASSERT_EQ(OB_SUCCESS, ret);
    std::cout<<"parse successed."<<std::endl;
    ret = test_path.to_string(buf);
    std::cout<<buf.ptr()<<std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString str2(buf.ptr());
    ASSERT_EQ(abbreviation_path_map[i][1], str2);
  }
}

ObString nodetest_path_map[30][2] = {
  {"node()","node()"},
  {"text()","text()"},
  {"comment()","comment()"},
  {"processing-instruction()","processing-instruction()"},
  {"processing-instruction(\"abc\")","processing-instruction(\"abc\")"},
  {"processing-instruction('abc')","processing-instruction(\"abc\")"},
  {"/a/parent::b/text()","/a/parent::b/text()"},
  {"a/following::node()","a/following::node()"},
  {"node","node"},
  {"/text","/text"}, //10
  {"comment","comment"},
  {"processing-instruction","processing-instruction"},
  {"/.text()","/self::node()"},
  {"/node()/text()","/node()/text()"},
  {"element/comment","element/comment"}, // 15
  {"text   ()","text()"},
  {"text   (    )","text()"},
  {"text   (    )    ","text()"},
};

TEST_F(TestXPath, test_nodetest)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString default_ns;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  std::cout<<"------begin nodetest test------"<<std::endl;
  for (int i = 0; i < 18; i++) {
    std::cout<<i<<" input: "<<nodetest_path_map[i][0]<<std::endl;
    ObPathParser test_path(ctx, ObParserType::PARSER_XML_PATH, nodetest_path_map[i][0], default_ns, nullptr);
    ret = test_path.parse_path();
    ASSERT_EQ(OB_SUCCESS, ret);
    std::cout<<"parse successed."<<std::endl;
    ObJsonBuffer buf(&allocator);
    ret = test_path.to_string(buf);
    std::cout<<buf.ptr()<<std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString str2(buf.ptr());
    ASSERT_EQ(nodetest_path_map[i][1], str2);
  }
}

ObString ns_path_map[16][2] = {
  {"/f:a","/ns_str:a"},
  {"/@f:b","/@ns_str:b"},
  {"/self:a","/ns_str:a"},
  {"/processing-instruction:abc","/ns_str:abc"},
  {"/parent:a","/ns_str:a"},
  {"/ancestor:a","/ns_str:a"},
  {"/ancestor-or-self:a","/ns_str:a"},
  {"/descendant-or-self:a","/ns_str:a"},
  {"/following-sibling:a","/ns_str:a"},
  {"/preceding-sibling:a","/ns_str:a"}, //10
  {"preceding:a","ns_str:a"},
  {"attribute:a","ns_str:a"},
  {"namespace:a","ns_str:a"},
  {"/text:a","/ns_str:a"},
  {"/node:a","/ns_str:a"},
  {"/comment:a","/ns_str:a"},// 16
};
ObString ns_str[15] = {
  "f",
  "self",
  "processing-instruction",
  "parent",
  "ancestor",
  "ancestor-or-self",
  "descendant-or-self",
  "following-sibling",
  "preceding-sibling",
  "preceding",
  "attribute",
  "namespace",
  "text",
  "node",
  "comment"
};
TEST_F(TestXPath, test_ns)
{
  int ret = OB_SUCCESS;
  ObString default_ns;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObPathVarObject ans;
  ObString data_value("ns_str");
  ObDatum* data =
    static_cast<ObDatum*> (allocator.alloc(sizeof(ObDatum)));
  if (OB_ISNULL(data)) {
    ret = OB_BAD_NULL_ERROR;
  } else {
    data = new (data) ObDatum();
    data->set_string(data_value);
    for (int i = 0; i < 15 && OB_SUCC(ret); ++i) {
      ret = ans.add(ns_str[i], data);
    }
    std::cout<<"------begin ns test------"<<std::endl;
    for (int i = 0; i < 16 && OB_SUCC(ret); i++) {
      std::cout<<i<<" input: "<<ns_path_map[i][0]<<std::endl;
      ObPathParser test_path(ctx, ObParserType::PARSER_XML_PATH, ns_path_map[i][0], default_ns, &ans);
      ret = test_path.parse_path();
      ASSERT_EQ(OB_SUCCESS, ret);
      std::cout<<"parse successed."<<std::endl;
      ObJsonBuffer buf(&allocator);
      ret = test_path.to_string(buf);
      std::cout<<buf.ptr()<<std::endl;
      ASSERT_EQ(OB_SUCCESS, ret);
      ObString str2(buf.ptr());
      ASSERT_EQ(ns_path_map[i][1], str2);
    }
  }
}

ObString path_map[35][2] = {
  {"/abc","/abc"},
  {"/_abc","/_abc"},
  {"/atest123","/atest123"},
  {"/ab-cd","/ab-cd"},
  {"/ab_cd","/ab_cd"},
  {"/a..a","/a..a"},
  {"Setp/@LValue", "Setp/@LValue"}, // following: user path
  {"/Rule/Format/Setp", "/Rule/Format/Setp"},
  {"/Rule/ExpressionText", "/Rule/ExpressionText"},
  {"/Rule/Format", "/Rule/Format"}, // 10
  {"/Grade/Format/list_Step", "/Grade/Format/list_Step"},
  {"@G_SORT", "@G_SORT"},
  {"@IS_ADM", "@IS_ADM"},
  {"@Min", "@Min"},
  {"@Max", "@Max"},
  {"@Amount", "@Amount"},
  {"@Desc", "@Desc"},
  {"@PD", "@PD"},
  {"//text()", "//text()"},
  {"Setp/@LValue", "Setp/@LValue"}, // 20
  {"Setp/@LCode","Setp/@LCode"},
  {"Element/@ElementCode","Element/@ElementCode"},
  {"//Element","//Element"},
  {"/12","/12"}, // 24 bad path
  {"/$a","/$a"},
  {"$b","$b"},
  {"abc(","abc("},
  {"test()","test()"},
  {"/.*",""},
  {"/..abc",""},
  {"/@",""},
  {"/child::",""},
};
TEST_F(TestXPath, test_tag_and_badpath)
{
  int ret = OB_SUCCESS;
  ObString default_ns;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  std::cout<<"------begin tag test------"<<std::endl;
  for (int i = 0; i < 32; i++) {
    std::cout<<i<<" input: "<<path_map[i][0].ptr()<<std::endl;
    ObPathParser test_path(ctx, ObParserType::PARSER_XML_PATH, path_map[i][0], default_ns, nullptr);
    ret = test_path.parse_path();
    if (i < 23) {
      ASSERT_EQ(OB_SUCCESS, ret);
      std::cout<<"parse successed."<<std::endl;
      ObJsonBuffer buf(&allocator);
      ret = test_path.to_string(buf);
      std::cout<<buf.ptr()<<std::endl;
      ASSERT_EQ(OB_SUCCESS, ret);
      ObString str2(buf.ptr());
      ASSERT_EQ(path_map[i][1], str2);
    } else {
      ASSERT_EQ(true, OB_FAIL(ret));
      std::cout<<"bad index:" <<test_path.bad_index_<<std::endl;
      if (test_path.bad_index_ < test_path.len_) {
        std::cout<<"str:" <<test_path.expression_<<", fail_char"<<test_path.expression_[test_path.bad_index_]<<std::endl;
      }
    }
  }
}

ObString func_map[12][2] = {
  {"count(/abc)","count(/abc)"},
  {"count(123)","count(123)"},
  {"count(1+1)", "count(1 + 1)"},
  {"count(\"abc\")","count(\"abc\")"},
  {"/a[1 + 1]","/a[1 + 1]"},
  {"/a[position() = 1]","/a[1]"},
  {"a[last() > 3]","a[last() > 3]"},
  {"/a[true()]", "/a[true()]"},
  {"a[false()]", "a[false()]"},
  {"/a[boolean(/a)]", "/a[boolean(/a)]"}, // 10
  {"a[not(a)]", "a[not(a)]"}
};

TEST_F(TestXPath, test_parse_good_func)
{
  int ret = OB_SUCCESS;
  ObString default_ns;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  std::cout<<"------begin tag test------"<<std::endl;
  for (int i = 0; i < 11; i++) {
    std::cout<<i<<" input: "<<func_map[i][0].ptr()<<std::endl;
    ObPathParser test_path(ctx, ObParserType::PARSER_XML_PATH, func_map[i][0], default_ns, nullptr);
    ret = test_path.parse_path();
      ASSERT_EQ(OB_SUCCESS, ret);
      std::cout<<"parse successed."<<std::endl;
      ObJsonBuffer buf(&allocator);
      ret = test_path.to_string(buf);
      std::cout<<buf.ptr()<<std::endl;
      ASSERT_EQ(OB_SUCCESS, ret);
      ObString str2(buf.ptr());
      ASSERT_EQ(func_map[i][1], str2);
  }
}

static ObString bad_func_map[10] = {
  "count()",
  "position()",
  "count(/abc, /e)",
  "last()",
  "/abc/count(/abc)",
  "count(/abc[1])",
  "count(position())",
};
static int func_errcode[10] = {
  OB_ERR_PARSER_SYNTAX,
  OB_OP_NOT_ALLOW,
  OB_ERR_PARSER_SYNTAX,
  OB_OP_NOT_ALLOW,
  OB_INVALID_ARGUMENT,
  OB_OP_NOT_ALLOW,
  OB_ERR_PARSER_SYNTAX,
};
TEST_F(TestXPath, test_parse_bad_func)
{
  int ret = OB_SUCCESS;
  ObString default_ns;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  std::cout<<"------begin tag test------"<<std::endl;
  for (int i = 0; i < 7; i++) {
    std::cout<<i<<" input: "<<bad_func_map[i].ptr()<<std::endl;
    ObPathParser test_path(ctx, ObParserType::PARSER_XML_PATH, bad_func_map[i], default_ns, nullptr);
    ret = test_path.parse_path();
    ASSERT_EQ(ret, func_errcode[i]);
    std::cout<<"ret = "<< ret << ", bad index:" <<test_path.bad_index_<<std::endl;
    if (test_path.bad_index_ < test_path.len_) {
      std::cout<<"str:" <<test_path.expression_<<", fail_char"<<test_path.expression_[test_path.bad_index_]<<std::endl;
    }
  }
}

ObString good_filter_map[37][2] = {
  {"1 + 1","1 + 1"},
  {"2.5 - 0.5","2.5 - 0.5"},
  {"1 * 5","1 * 5"},
  {"5 div 1","5 div 1"},
  {"1 < 0","1 < 0"},
  {"1 <= 0","1 <= 0"},
  {"12 > 20","12 > 20"},
  {"12 >= 20","12 >= 20"},
  {"1 or 0","1 or 0"},
  {"1 and 0","1 and 0"}, //10
  {".5 + .6","0.5 + 0.6"},
  {"-1 + -1.5","-1 + -1.5"},
  {"-2.5 --0.5", "-2.5 - -0.5"},
  {"-2.5 ---0.5", "-2.5 - 0.5"},
  {"-2.5 + 0.5 * 1", "-2.5 + 0.5 * 1"},
  {"-2.5 + 0.5 div 1", "-2.5 + 0.5 div 1"},
  {"-2.5 - ---0.5 div 1 and 1 + 1", "-2.5 - -0.5 div 1 and 1 + 1"},
  {"/abc[position() >= 1][last() <= 10] ", "/abc[position() >= 1][last() <= 10]"},
  {"/abc[position() >= 1][last() <= 10][1 and 1<2] ", "/abc[position() >= 1][last() <= 10][1 and 1 < 2]"},
  {"/abc[1+1 | 1-1]","/abc[1 + 1 | 1 - 1]" }, // 20
  {"/abc | /efg", "/abc | /efg"},
  {"/abc | /de |/fg", "/abc | /de | /fg"},
  {"/Grade/Format/list_Step", "/Grade/Format/list_Step"}, // user path
  {"@G_SORT", "@G_SORT"},
  {"/Grade/Format/list_Step[text()=\"' || B.GRADE || '\"]/@PD", "/Grade/Format/list_Step[text() = \"' || B.GRADE || '\"]/@PD"},
  {"abc + abc", "abc + abc"},
  {"\"abc\" > \"abc\"", "\"abc\" > \"abc\""},
  {"count(/abc) + count(/abc)", "count(/abc) + count(/abc)"},
  {"count(/abc) > count(/abc)", "count(/abc) > count(/abc)"},
  {"1 or 1 < 2", "1 or 1 < 2"}, // 30
  {"/abc[abc > abc]", "/abc[abc > abc]"},
  {"/abc[1]", "/abc[1]"},
  {"/Grade/Format/list_Step[text()=\"' || A.GRADE || '\"]/@PD", "/Grade/Format/list_Step[text() = \"' || A.GRADE || '\"]/@PD"},
  {"/Page/DataSources/*[ID=\"' || datasource_id_ || '\"]", "/Page/DataSources/*[ID = \"' || datasource_id_ || '\"]"},
  {"/Page/*/*/Group/Elements/Element[@id=\"' || s_element_id_ || '\"]", "/Page/*/*/Group/Elements/Element[@id = \"' || s_element_id_ || '\"]"},
  {"/Page/DataSources/DataSource[@id=\"' || s_data_source_id_ || '\"]","/Page/DataSources/DataSource[@id = \"' || s_data_source_id_ || '\"]"} //36
};

TEST_F(TestXPath, test_parse_good_filter)
{
  int ret = OB_SUCCESS;
  ObString default_ns;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  std::cout<<"------begin tag test------"<<std::endl;
  for (int i = 0; i < 36; i++) {
    std::cout<<i<<" input: "<<good_filter_map[i][0].ptr()<<std::endl;
    ObPathParser test_path(ctx, ObParserType::PARSER_XML_PATH, good_filter_map[i][0], default_ns, nullptr);
    ret = test_path.parse_path();
      ASSERT_EQ(OB_SUCCESS, ret);
      std::cout<<"parse successed."<<std::endl;
      ObJsonBuffer buf(&allocator);
      ret = test_path.to_string(buf);
      ASSERT_EQ(OB_SUCCESS, ret);
      ObString str2(buf.ptr());
      std::cout<<good_filter_map[i][1].ptr()<<std::endl;
      std::cout<<str2.ptr()<<std::endl;
      ASSERT_EQ(good_filter_map[i][1], str2);
  }
}

static ObString bad_filter_map[10] = {
  "(1 < 2) = 1",
  "\"abc\" + \"abc\"",
  "1 + (1 or 0)",
  "1<2<3",
  "(1<2)<3",
  "\"abc\"+ abc",
  "1 <= (1 and 1)",
};
static int filter_errcode[10] = {
  OB_OP_NOT_ALLOW,
  OB_OP_NOT_ALLOW,
  OB_OP_NOT_ALLOW,
  OB_OP_NOT_ALLOW,
  OB_OP_NOT_ALLOW,
  OB_OP_NOT_ALLOW,
  OB_OP_NOT_ALLOW,
};
TEST_F(TestXPath, test_parse_bad_filter)
{
  int ret = OB_SUCCESS;
  ObString default_ns;
  ObArenaAllocator allocator(ObModIds::TEST);
  std::cout<<"------begin tag test------"<<std::endl;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  for (int i = 0; i < 7; i++) {
    std::cout<<i<<" input: "<<bad_filter_map[i].ptr()<<std::endl;
    ObPathParser test_path(ctx, ObParserType::PARSER_XML_PATH, bad_filter_map[i], default_ns, nullptr);
    ret = test_path.parse_path();
    ASSERT_EQ(ret, filter_errcode[i]);
    std::cout<<"ret = "<< ret << ", bad index:" <<test_path.bad_index_<<std::endl;
    if (test_path.bad_index_ < test_path.len_) {
      std::cout<<"str:" <<test_path.expression_<<", fail_char"<<test_path.expression_[test_path.bad_index_]<<std::endl;
    }
  }
}

TEST_F(TestXPath, test_good_path)
{
  int ret = OB_SUCCESS;
  ObString default_ns;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  // 用于解析
  ObString str0 = "/self::a/b/@c";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "/self::a/b/@c";
  ObPathParser test_path(ctx, ObParserType::PARSER_XML_PATH, str0, default_ns, nullptr);
  // 解析
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = test_path.to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"end test"<<std::endl;
}

ObString eval_count[1] = {"2"};
TEST_F(TestXPath, test_eval_count)
{
  // 用于解析
  ObString str0 = "count(bookstore/book/title)";
  ObString xml_text("<bookstore><book><title>Learning XML-first</title><title>Learning XML-last</title></book></bookstore>");
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "count(bookstore/book/title)";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);



  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 1) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      std::cout<<i<<": "<<s.ptr()<<std::endl;
      ASSERT_EQ(s, eval_count[i]);
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString seek_element[2] = {"<title>Learning XML-first</title>", "<title>Learning XML-last</title>"};
TEST_F(TestXPath, test_seek_element_by_tag)
{
  // 用于解析
  ObString str0 = "bookstore/book/title";
  ObString xml_text("<bookstore><book><title>Learning XML-first</title><title>Learning XML-last</title></book></bookstore>");
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/book/title";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 2) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_element[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString seek_all_element[2] = {"<title>Learning XML-first</title>", "<title>Learning XML-last</title>"};
TEST_F(TestXPath, test_seek_all_element)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><book><title>Learning XML-first</title><title>Learning XML-last</title></book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "bookstore/book/*";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/book/*";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 2) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_all_element[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString seek_ns_element[1] = {"<b:book xmlns:a=\"aaa\" xmlns:b=\"bbb\">test2</b:book>"};
TEST_F(TestXPath, test_seek_ns_element)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><book>test1</book><b:book xmlns:a=\"aaa\" xmlns:b=\"bbb\">test2</b:book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "bookstore/a:book";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/bbb:book";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 1) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_ns_element[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}


ObString seek_root[1] = {"<bookstore><book>test1</book></bookstore>"};
TEST_F(TestXPath, test_seek_root)
{
  int ret = OB_SUCCESS;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><book>test1</book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "/";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "/";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 1) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObStringBuffer buf_str(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      xnode->print_document(buf_str, type, false);
      ObString s(buf_str.ptr());
      ASSERT_EQ(s, seek_root[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString seek_all_attribute[3] = {"a:lang=\" ns_a \"", "lang=\" no_ns \"", "b:lang=\" ns_b \""};
TEST_F(TestXPath, test_seek_all_attribute)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  // 用于解析
  ObString str0 = "bookstore/book/@*";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/book/@*";
  ObString xml_text("<bookstore><book xmlns:a=\"aaa\" xmlns:b=\"bbb\" a:lang=\" ns_a \" lang=\" no_ns \" b:lang=\" ns_b \"></book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 3) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_all_attribute[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }

}

ObString seek_certain_attribute[1] = {"lang=\" no_ns \""};
TEST_F(TestXPath, test_seek_certain_attribute)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  // 用于解析
  ObString str0 = "bookstore/book/@lang";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/book/@lang";
  ObString xml_text("<bookstore><book xmlns:a=\"aaa\" xmlns:b=\"bbb\" a:lang=\" ns_a \" lang=\" no_ns \" b:lang=\" ns_b \"></book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 1) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_certain_attribute[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString seek_ns_attribute[1] = {"b:lang=\" ns_b \""};
TEST_F(TestXPath, test_seek_ns_attribute)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><book xmlns:a=\"aaa\" xmlns:b=\"bbb\" a:lang=\" ns_a \" lang=\" no_ns \" b:lang=\" ns_b \"></book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "bookstore/book/@a:lang";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/book/@bbb:lang";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 1) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_ns_attribute[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }

}

ObString seek_node[5] = {"abc", "<book>book1</book>", "<!-- test comment -->", "<?price type=\"text/xsl\" href=\"show_book.xsl\"?>", "<book>book2</book>"};
TEST_F(TestXPath, test_seek_node)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore>abc<book>book1</book><!-- test comment --><?price type=\"text/xsl\" href=\"show_book.xsl\"?><book>book2</book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "bookstore/node()";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/node()";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 5) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_node[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }

}

ObString seek_text[2] = {"book1", "book2"};
TEST_F(TestXPath, test_seek_text)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><book>book1</book><book xmlns:a=\"aaa\" xmlns:b=\"bbb\" a:lang=\" ns_a \" lang=\" no_ns \" b:lang=\" ns_b \">book2</book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "bookstore/book/text()";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/book/text()";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 2) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_text[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString seek_comment[2] = {"<!-- test comment1 -->", "<!-- test comment2 -->"};
TEST_F(TestXPath, test_seek_comment)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><!-- test comment1 --><book>book1</book><book>book2</book><!-- test comment2 --></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "bookstore/comment()";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/comment()";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 2) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_comment[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString seek_pi_wildcard[2] = {"<?price1 type=\"text/xsl\" href=\"show_book.xsl\"?>", "<?price2 type=\"text/xsl\" href=\"show_book.xsl\"?>"};
TEST_F(TestXPath, test_seek_pi_wildcard)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><?price1 type=\"text/xsl\" href=\"show_book.xsl\"?><!-- test comment --><book>book1</book><book>book2</book><?price2 type=\"text/xsl\" href=\"show_book.xsl\"?></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "bookstore/processing-instruction()";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/processing-instruction()";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 2) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_pi_wildcard[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString seek_certain_pi[1] = {"<?price2 type=\"text/xsl\" href=\"show_book.xsl\"?>"};
TEST_F(TestXPath, test_seek_certain_pi)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><?price1 type=\"text/xsl\" href=\"show_book.xsl\"?><!-- test comment --><book>book1</book><book>book2</book><?price2 type=\"text/xsl\" href=\"show_book.xsl\"?></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "bookstore/processing-instruction(\"price2\")";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "bookstore/processing-instruction(\"price2\")";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("bbb");
  ret = pass.add("a", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 1) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_certain_pi[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString seek_default_ns_attribute[1] = {"b1=\"b1\""};
TEST_F(TestXPath, test_seek_default_ns_attribute)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<a xmlns=\"ns1\" xmlns:f=\"ns2\"><f:b b1=\"b1\" b2=\"b2\">bbb1</f:b><b b1=\"b1\" b2=\"b2\">bbb2</b></a>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "/a/h:b/@b1";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "/ns1:a/ns2:b/@b1";
  ObString default_ns("ns1");
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 1) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_default_ns_attribute[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }

}

ObString seek_self[3] = {"<b1>b1<b11>b11</b11><b12>b12</b12></b1>", "<b2>b2<b21>b21</b21><b22>b22</b22></b2>", "<b3>b3<b31>b31</b31><b32>b32</b32></b3>"};
TEST_F(TestXPath, test_seek_self)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<root>root<test1>test1</test1><test>"
                    "<a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a>"
                    "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1>"
                    "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"
                    "<b3>b3<b31>b31</b31><b32>b32</b32></b3></b>"
                    "<c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c>"
                    "</test>test<test2>test2</test2></root>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "/root/*/b/*/self::*";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "/root/*/b/*/self::*";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 3) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_self[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString seek_descendant[6] = {"b11", "b12", "b21", "b22", "b31", "b32"};
TEST_F(TestXPath, test_seek_descendant)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<root>root<test1>test1</test1><test>"
                    "<a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a>"
                    "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1>"
                    "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"
                    "<b3>b3<b31>b31</b31><b32>b32</b32></b3></b>"
                    "<c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c>"
                    "</test>test<test2>test2</test2></root>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  // '/descendant::node()/*'
  ObString str0 = "root/*/b/descendant::*/node()/text()";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "root/*/b/descendant::*/node()/text()";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 6) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, seek_descendant[i]);
      std::cout<<i<<": "<<s.ptr()<<std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString descendant_or_self[9] = {"b1", "b2", "b3", "b11", "b12", "b21", "b22", "b31", "b32"};
TEST_F(TestXPath, test_seek_descendant_or_self)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<root>root<test1>test1</test1><test>"
                    "<a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a>"
                    "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1>"
                    "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"
                    "<b3>b3<b31>b31</b31><b32>b32</b32></b3></b>"
                    "<c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c>"
                    "</test>test<test2>test2</test2></root>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "root/*/b/descendant-or-self::*/node()/text()";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "root/*/b/descendant-or-self::*/node()/text()";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 9) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      std::cout<<i<<": "<<s.ptr()<<std::endl;
      ASSERT_EQ(s, descendant_or_self[i]);
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString descentent_text[23] = {"root","test_end","test1","test_start",
"a","a1","a2","a3","b","b1","b11","b12","b2","b21","b22","b3",
"b31","b32","c","c1","c2","c3","test2"};
TEST_F(TestXPath, test_seek_descentent_self_text)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<root>root<test1>test1</test1><test>test_start"
                    "<a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a>"
                    "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1>"
                    "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"
                    "<b3>b3<b31>b31</b31><b32>b32</b32></b3></b>"
                    "<c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c>"
                    "</test>test_end<test2>test2</test2></root>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "/descendant-or-self::node()/text()";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "/descendant-or-self::node()/text()";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 23) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      std::cout<<i<<": "<<s.ptr()<<std::endl;
      ASSERT_EQ(s, descentent_text[i]);
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString user_extract = {"<Format>"
                        "<Step Min=\"74\" Max=\"100\" Amount=\"\" Desc=\"\" PD=\"0.00035\" G_SORT=\"19\" IsDef=\"0\" IS_ADM=\"1\">AAA</Step>"
                      "</Format>"};
TEST_F(TestXPath, test_seek_user_extract)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<Rule>"
                      "<Format>"
                        "<Step Min=\"74\" Max=\"100\" Amount=\"\" Desc=\"\" PD=\"0.00035\" G_SORT=\"19\" IsDef=\"0\" IS_ADM=\"1\">AAA</Step>"
                      "</Format>"
                    "</Rule>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "/Rule/Format";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "/Rule/Format";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 1) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      std::cout<<i<<": "<<s.ptr()<<std::endl;
      ASSERT_EQ(s, user_extract);
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString parent_str = {"<b2>b2<b21>b21</b21><b22>b22</b22></b2>"};
TEST_F(TestXPath, test_seek_parent)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<root>root<test1>test1</test1><test>"
                    "<a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a>"
                    "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1>"
                    "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"
                    "<b3>b3<b31>b31</b31><b32>b32</b32></b3></b>"
                    "<c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c>"
                    "</test>test<test2>test2</test2></root>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "root/*/b/b2/b22/parent::*";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "root/*/b/b2/b22/parent::*";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 1) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      std::cout<<i<<": "<<s.ptr()<<std::endl;
      ASSERT_EQ(s, parent_str);
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString ancestor_str[5] = {"<root>root<test1>test1</test1><test><a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a><b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1><b2>b2<b21>b21</b21><b22>b22</b22></b2><b3>b3<b31>b31</b31><b32>b32</b32></b3></b><c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c></test>test<test2>test2</test2></root>",
                            "<test><a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a><b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1><b2>b2<b21>b21</b21><b22>b22</b22></b2><b3>b3<b31>b31</b31><b32>b32</b32></b3></b><c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c></test>",
                            "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1><b2>b2<b21>b21</b21><b22>b22</b22></b2><b3>b3<b31>b31</b31><b32>b32</b32></b3></b>",
                            "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"};
TEST_F(TestXPath, test_seek_ancestor)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<root>root<test1>test1</test1><test>"
                    "<a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a>"
                    "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1>"
                    "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"
                    "<b3>b3<b31>b31</b31><b32>b32</b32></b3></b>"
                    "<c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c>"
                    "</test>test<test2>test2</test2></root>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "root/*/b/b2/b22/ancestor::*";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "root/*/b/b2/b22/ancestor::*";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 4) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      std::cout<<i<<": "<<s.ptr()<<std::endl;
      ASSERT_EQ(s, ancestor_str[i]);
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

ObString ancestor_str_or_self[5] = {"<root>root<test1>test1</test1><test><a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a><b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1><b2>b2<b21>b21</b21><b22>b22</b22></b2><b3>b3<b31>b31</b31><b32>b32</b32></b3></b><c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c></test>test<test2>test2</test2></root>",
                            "<test><a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a><b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1><b2>b2<b21>b21</b21><b22>b22</b22></b2><b3>b3<b31>b31</b31><b32>b32</b32></b3></b><c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c></test>",
                            "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1><b2>b2<b21>b21</b21><b22>b22</b22></b2><b3>b3<b31>b31</b31><b32>b32</b32></b3></b>",
                            "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"};
TEST_F(TestXPath, test_seek_ancestor_or_self)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<root>root<test1>test1</test1><test>"
                    "<a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a>"
                    "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1>"
                    "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"
                    "<b3>b3<b31>b31</b31><b32>b32</b32></b3></b>"
                    "<c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c>"
                    "</test>test<test2>test2</test2></root>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "root/*/b/b2/b22/ancestor::*";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "root/*/b/b2/b22/ancestor::*";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 4) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      std::cout<<i<<": "<<s.ptr()<<std::endl;
      ASSERT_EQ(s, ancestor_str[i]);
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

TEST_F(TestXPath, test_seek_root_ancestor)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<root>root<test1>test1</test1><test>"
                    "<a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a>"
                    "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1>"
                    "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"
                    "<b3>b3<b31>b31</b31><b32>b32</b32></b3></b>"
                    "<c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c>"
                    "</test>test<test2>test2</test2></root>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "root/ancestor::*";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "root/ancestor::*";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  ObIMulModeBase* res;
  ret = pathiter.get_next_node(res);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestXPath, test_seek_root_parent)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<root>root<test1>test1</test1><test>"
                    "<a>a<a1>a1</a1><a2>a2</a2><a3>a3</a3></a>"
                    "<b>b<b1>b1<b11>b11</b11><b12>b12</b12></b1>"
                    "<b2>b2<b21>b21</b21><b22>b22</b22></b2>"
                    "<b3>b3<b31>b31</b31><b32>b32</b32></b3></b>"
                    "<c>c<c1>c1</c1><c2>c2</c2><c3>c3</c3></c>"
                    "</test>test<test2>test2</test2></root>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 用于解析
  ObString str0 = "root/parent::*";
  std::cout<<str0.ptr()<<std::endl;
  ObString str1 = "root/parent::*";
  ObString default_ns;
  ObPathVarObject pass;
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObPathExprIter pathiter;
  pathiter.init(ctx,str0, default_ns, doc, &pass);
  // 解析
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout<<buf.ptr()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证是否相等 （ObSqlString直接相比会报错，因为没有重载==
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  std::cout<<"parse node successed, test eval node_begin:"<<std::endl;

  ASSERT_EQ(OB_SUCCESS, ret);
  ObIMulModeBase* res;
  ret = pathiter.get_next_node(res);
  ASSERT_EQ(OB_ITER_END, ret);
}

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  /*
  system("rm -f test_xpath.log");
  OB_LOGGER.set_file_name("test_xpath.log");
  OB_LOGGER.set_log_level("INFO");
  */
  return RUN_ALL_TESTS();
}