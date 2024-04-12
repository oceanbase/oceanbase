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
#include <gtest/gtest.h>
#define private public
#include "lib/lob/ob_lob_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_diff.h"
#include "lib/timezone/ob_timezone_info.h"
#undef private

#include <sys/time.h>    
 
namespace oceanbase {
namespace common {

class TestJsonBin : public ::testing::Test {
public:
  TestJsonBin()
  {}
  ~TestJsonBin()
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
  DISALLOW_COPY_AND_ASSIGN(TestJsonBin);

};

static std::string buf_to_hex(const char* ptr, int len)
{
  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < len; ++i) ss << static_cast<int>(ptr[i]) << " ";
  return ss.str();
}

static int init_update_ctx(ObIAllocator &allocator, ObJsonBin *bin)
{
  INIT_SUCC(ret);
  ObJsonBinUpdateCtx *update_ctx = nullptr;
  ObLobInRowUpdateCursor *cursor = nullptr;
  if (OB_ISNULL(update_ctx = OB_NEWx(ObJsonBinUpdateCtx, &allocator, &allocator))) {
    LOG_WARN("alloc update_ctx fail", K(ret));
  } else if (OB_ISNULL(cursor = OB_NEWx(ObLobInRowUpdateCursor, &allocator, &allocator))) {
    LOG_WARN("alloc cursor fail", K(ret));
  } else if (OB_FAIL(cursor->init(bin->get_cursor()))) {
    LOG_WARN("init cursor fail", K(ret));
  } else {
    update_ctx->set_lob_cursor(cursor);
    bin->get_cursor()->reset();
    bin->set_cursor(cursor);
    bin->get_ctx()->update_ctx_ = update_ctx;
    bin->get_ctx()->is_update_ctx_alloc_ = true;
  }
  return ret;
}

static void check_diff_valid(ObIAllocator &allocator, const ObString& old_str, ObJsonBinUpdateCtx &update_ctx)
{
  ObJsonBuffer old_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, old_buf.append(old_str));
  ASSERT_EQ(old_buf.length(), old_str.length());
  ObString new_buf;
  update_ctx.current_data(new_buf);
  for (int i = 0; i < update_ctx.binary_diffs_.count(); ++i) {
    ObJsonBinaryDiff &diff = update_ctx.binary_diffs_[i];
    if (diff.dst_offset_ >= old_buf.length()) {
      ASSERT_EQ(OB_SUCCESS, old_buf.append(new_buf.ptr() + diff.dst_offset_, diff.dst_len_));
    } else {
      MEMCPY(old_buf.ptr() + diff.dst_offset_, new_buf.ptr() + diff.dst_offset_, diff.dst_len_);
    }
  }
  ASSERT_EQ(old_buf.length(), new_buf.length());
  for (int i = 0; i < old_buf.length(); ++i) {
    uint8_t old_c = old_buf.ptr()[i];
    uint8_t new_c = new_buf.ptr()[i];
    ASSERT_EQ(new_c, old_c) << " pos " << i << " old buf " << buf_to_hex(old_buf.ptr(), old_buf.length()) << " new buf "<< buf_to_hex(new_buf.ptr(), new_buf.length());
  }
  ASSERT_EQ(0, MEMCMP(old_buf.ptr(), new_buf.ptr(), new_buf.length()));
}

static void check_json_diff_valid(ObIAllocator &allocator, const ObString& j_text, ObJsonBinUpdateCtx &target_update_ctx, int json_diff_count)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *j_base = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));

  ObJsonDiffArray &json_diffs = target_update_ctx.json_diffs_;
  ObString update_buffer;
  target_update_ctx.current_data(update_buffer);

  ASSERT_EQ(json_diff_count, json_diffs.count());

  ObJsonPathCache path_cache(&allocator);
  for (int i = 0; i < json_diffs.count(); ++i) {
    ObJsonPath* json_path = nullptr;
    ObString path_str = json_diffs[i].path_;
    int path_idx = path_cache.size();
    ASSERT_EQ(OB_SUCCESS, path_cache.find_and_add_cache(json_path, path_str, path_idx));
    ASSERT_EQ(path_cache.path_stat_at(path_idx), ObPathParseStat::OK_NOT_NULL);
    ObJsonSeekResult hit;
    if (json_diffs[i].op_ == ObJsonDiffOp::REPLACE
        || json_diffs[i].op_ == ObJsonDiffOp::REMOVE) {
      ASSERT_EQ(OB_SUCCESS, j_base->seek(*json_path, json_path->path_node_cnt(), true, false, hit));
      ASSERT_EQ(1, hit.size());
      ObIJsonBase *parent = nullptr;
      ObIJsonBase *json_old = hit[0];
      ObJsonBin j_new_bin(&allocator);
      j_new_bin.set_seek_flag(false);
      if (json_diffs[i].op_ == ObJsonDiffOp::REPLACE) {
        ASSERT_EQ(OB_SUCCESS, j_new_bin.reset(
            json_diffs[i].value_type_,
            json_diffs[i].value_,
            0,
            json_diffs[i].entry_var_type_,
            nullptr));
        ASSERT_EQ(OB_SUCCESS, json_old->get_parent(parent));
        ObJsonNode *json_new = nullptr;
        ASSERT_EQ(OB_SUCCESS, j_new_bin.to_tree(json_new));
        if(OB_NOT_NULL(parent)) {
          ASSERT_EQ(OB_SUCCESS, parent->replace(json_old, json_new));
        } else {
          ASSERT_EQ(OB_SUCCESS, j_base->replace(json_old, json_new));
        }
      } else {
        if(OB_NOT_NULL(parent)) {
          parent = j_base;
        }
        // if (j_base->json_type() == ObJsonNodeType::J_OBJECT) {
        //   ASSERT_EQ(OB_SUCCESS, parent->object_remove())
        // } else {
        //   ASSERT_EQ(OB_SUCCESS, parent->array_remove())
        // }
      }
    } else if (json_diffs[i].op_ == ObJsonDiffOp::INSERT) {
      ASSERT_EQ(OB_SUCCESS, j_base->seek(*json_path, json_path->path_node_cnt(), true, false, hit));
      ASSERT_EQ(0, hit.size());
    } else {
      ASSERT_FALSE(true) << "invalid op" << (int)json_diffs[i].op_;
    }
  }
  ObJsonBuffer j_src_buffer(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_src_buffer, false));
  ObJsonBin j_target_bin;
  j_target_bin.set_seek_flag(false);
  ObJsonBinCtx j_target_bin_ctx;
  ASSERT_EQ(OB_SUCCESS, j_target_bin.reset(update_buffer, 0, &j_target_bin_ctx));
  ObJsonBuffer j_target_buffer(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_target_bin.print(j_target_buffer, false));
  ASSERT_EQ(std::string(j_src_buffer.ptr(), j_src_buffer.length()), std::string(j_target_buffer.ptr(), j_target_buffer.length()));
  ASSERT_NE(std::string(j_src_buffer.ptr(), j_src_buffer.length()), std::string(j_text.ptr(), j_text.length()));
  ASSERT_NE(std::string(j_target_buffer.ptr(), j_target_buffer.length()), std::string(j_text.ptr(), j_text.length()));
  std::cout << "-------- origin" << std::endl;
  std::cout << std::string(j_text.ptr(), j_text.length()) << std::endl;
  std::cout << "-------- src" << std::endl;
  std::cout << std::string(j_src_buffer.ptr(), j_src_buffer.length()) << std::endl;
  std::cout << "-------- target" << std::endl;
  std::cout << std::string(j_target_buffer.ptr(), j_target_buffer.length()) << std::endl;
}

// rapidjson 解析仅包含字符串的json text测试
// 输入: json text
// 预期: 解析完整json tree
TEST_F(TestJsonBin, test_tree_to_bin)
{
  common::ObString j_text("{ \"greeting\" : 1, \"farewell\" : 2, \"json_text\" : 3 }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  common::ObString result;

  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // check result
  uint64_t doc_header_len = sizeof(ObJsonBinDocHeader);
  char *ptr = result.ptr() + doc_header_len;
  int64_t offset = 0;
  ObJsonBinHeader *header = reinterpret_cast<ObJsonBinHeader *>(ptr);
  ASSERT_EQ(header->is_continuous_, 1);
  ASSERT_EQ(header->type_, ObJBVerType::J_OBJECT_V0);

  offset += OB_JSON_BIN_HEADER_LEN;
  int64_t count;
  ObJsonVar::read_var(ptr + offset, header->count_size_, &count);
  ASSERT_EQ(count, 3);

  offset += ObJsonVar::get_var_size(header->count_size_);
  uint64_t obj_size;
  ObJsonVar::read_var(ptr + offset, header->obj_size_size_, &obj_size);

  ASSERT_EQ(doc_header_len + obj_size, result.length());
  fprintf(stdout, "[test] used_size is %lu\n", obj_size);

  offset += ObJsonVar::get_var_size(header->obj_size_size_);
  char *key_entry = ptr + offset;

  uint64_t key_entry_size = ObJsonVar::get_var_size(header->entry_size_);
  common::ObString expext_key_array[] = { "farewell","greeting", "json_text" };
  for (int64_t i = 0; i < count; i++) {
    uint64_t key_offset, key_len;
    ObJsonVar::read_var(key_entry, header->entry_size_, &key_offset);
    ObJsonVar::read_var(key_entry + key_entry_size, header->entry_size_, &key_len);
    key_entry += key_entry_size * 2;
    char *key_ptr = reinterpret_cast<char*>(ptr) + key_offset;
    ASSERT_EQ(expext_key_array[i].length(), key_len);
    ASSERT_TRUE(STRNCMP(expext_key_array[i].ptr(), key_ptr, key_len) == 0);
  }

  char *value_entry = ptr + offset + key_entry_size * 2 * count;
  int64_t expect_value_array[] = { 2, 1, 3 };
  for (int64_t i = 0; i < count; i++) {
    int64_t value_offset;
    ObJsonVar::read_var(value_entry, header->entry_size_, &value_offset);
    uint64_t value_entry_size = ObJsonVar::get_var_size(header->entry_size_);
    uint8_t value_type = *reinterpret_cast<uint8_t*>(value_entry + value_entry_size);
    value_entry += sizeof(uint8_t) + value_entry_size;
    int64_t val = value_offset;
    // int64_t pos = value_offset;
    // ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(reinterpret_cast<char*>(root), result.length(), pos, &val));
    ASSERT_EQ((expect_value_array[i]), val);
    uint8_t expect_type = static_cast<uint8_t>(ObJsonNodeType::J_INT);
    ASSERT_EQ((value_type & 0x7F), expect_type);
    ASSERT_EQ(0x80, (value_type & 0x80));
  }
}

TEST_F(TestJsonBin, t2b_array)
{
  common::ObString j_text("[\"greeting\", 1.1, 2, -10, true, null]");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_TRUE(j_bin->is_bin());

  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // check result
  uint64_t doc_header_len = sizeof(ObJsonBinDocHeader);
  char *ptr = result.ptr() + doc_header_len;
  uint64_t offset = 0;
  ObJsonBinHeader *header = reinterpret_cast<ObJsonBinHeader *>(ptr);
  offset += OB_JSON_BIN_HEADER_LEN;

  ASSERT_EQ(header->is_continuous_, 1);
  ASSERT_EQ(header->type_, ObJBVerType::J_ARRAY_V0);

  uint64_t count, array_size;
  ObJsonVar::read_var(ptr + offset, header->count_size_, &count);
  offset += ObJsonVar::get_var_size(header->count_size_);
  ObJsonVar::read_var(ptr + offset, header->obj_size_size_, &array_size);
  offset += ObJsonVar::get_var_size(header->obj_size_size_);
  ASSERT_EQ(count, 6);
  ASSERT_EQ(doc_header_len + array_size, result.length());
  fprintf(stdout, "[test] array_size is %lu\n", array_size);

  uint64_t value_entry_size = ObJsonVar::get_var_size(header->entry_size_);
  // "greeting"
  {
    common::ObString expect_str("greeting");
    uint64_t value_offset;
    ObJsonVar::read_var(ptr + offset, header->entry_size_, &value_offset);
    offset += value_entry_size;
    uint8_t value_type = *reinterpret_cast<uint8_t*>(ptr + offset);
    offset += sizeof(uint8_t);

    uint8_t expect_type = static_cast<uint8_t>(ObJBVerType::J_STRING_V0);
    ASSERT_EQ(value_type, expect_type);

    int64_t num = 0;
    int64_t pos = value_offset;
    uint8_t redant_type = *reinterpret_cast<uint8_t*>(ptr + pos);
    pos += sizeof(uint8_t);

    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(ptr, result.length(), pos, &num));
    uint64_t length = static_cast<uint64_t>(num);
    ASSERT_EQ(length, expect_str.length());

    char* val = ptr + pos;
    ASSERT_TRUE(STRNCMP(expect_str.ptr(), val, length) == 0);
  }
  // 1.1
  {
      uint64_t value_offset;
      ObJsonVar::read_var(ptr + offset, header->entry_size_, &value_offset);
      offset += value_entry_size;
      uint8_t value_type = *reinterpret_cast<uint8_t*>(ptr + offset);
      offset += sizeof(uint8_t);

      uint8_t expect_type = static_cast<uint8_t>(ObJsonNodeType::J_DOUBLE);
      ASSERT_EQ(value_type, expect_type);

      double val = *reinterpret_cast<double*>(ptr + value_offset);
      ASSERT_FLOAT_EQ(val, 1.1);
  }
  // 2 inlined
  {
    uint64_t value_offset;
    ObJsonVar::read_var(ptr + offset, header->entry_size_, &value_offset);
    offset += value_entry_size;
    uint8_t value_type = *reinterpret_cast<uint8_t*>(ptr + offset);
    offset += sizeof(uint8_t);

    uint8_t expect_type = static_cast<uint8_t>(ObJBVerType::J_INT_V0);
    ASSERT_EQ((value_type & 0x7F), expect_type);
    ASSERT_EQ((value_type & 0x80), 0x80);

    // int64_t val = 0;
    // int64_t pos = common_header_len + value_offset;
    // ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(ptr, result.length(), pos, &val));
    uint64_t tval = static_cast<uint64_t>(value_offset);
    ASSERT_EQ(tval, 2);
  }
  // -10
  {
    uint64_t value_offset;
    ObJsonVar::read_var(ptr + offset, header->entry_size_, &value_offset);
    offset += value_entry_size;
    uint8_t value_type = *reinterpret_cast<uint8_t*>(ptr + offset);
    offset += sizeof(uint8_t);

    uint8_t expect_type = static_cast<uint8_t>(ObJBVerType::J_INT_V0);
    ASSERT_EQ((value_type & 0x7F), expect_type);
    ASSERT_EQ((value_type & 0x80), 0x80);

    // int64_t val = 0;
    // int64_t pos = common_header_len + value_offset;
    // ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(ptr, result.length(), pos, &val));
    int64_t val = static_cast<int64_t>(static_cast<int8_t>(static_cast<uint8_t>(value_offset)));
    ASSERT_EQ(val, -10);
  }
  // true
  {
    uint64_t value_offset;
    ObJsonVar::read_var(ptr + offset, header->entry_size_, &value_offset);
    offset += value_entry_size;
    uint8_t value_type = *reinterpret_cast<uint8_t*>(ptr + offset);
    offset += sizeof(uint8_t);

    uint8_t expect_type = static_cast<uint8_t>(ObJBVerType::J_BOOLEAN_V0);
    ASSERT_EQ((value_type & 0x7F), expect_type);
    ASSERT_EQ((value_type & 0x80), 0x80);

    char val = static_cast<char>(value_offset);
    ASSERT_EQ(val, 1);
  }
  // null
  {
    uint64_t value_offset;
    ObJsonVar::read_var(ptr + offset, header->entry_size_, &value_offset);
    offset += value_entry_size;
    uint8_t value_type = *reinterpret_cast<uint8_t*>(ptr + offset);
    offset += sizeof(uint8_t);

    uint8_t expect_type = static_cast<uint8_t>(ObJsonNodeType::J_NULL);
    ASSERT_EQ((value_type & 0x7F), expect_type);
    ASSERT_EQ((value_type & 0x80), 0x80);

    char val = static_cast<char>(value_offset);
    ASSERT_EQ(val, '\0');
  }
}

TEST_F(TestJsonBin, deserialize_bin_to_tree)
{
  common::ObString j_text("[\"greeting\", 1.1, 2, -10, true, null]");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));

  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  ObJsonNode *new_json_tree = nullptr;
  ASSERT_EQ(OB_SUCCESS, bin->to_tree(new_json_tree));

  ObJsonArray *json_array = static_cast<ObJsonArray *>(new_json_tree);
  // check string "greeting"
  ObJsonString *json_string = static_cast<ObJsonString *>((*json_array)[0]);
  ASSERT_TRUE(json_string->json_type() == ObJsonNodeType::J_STRING);
  ASSERT_TRUE(json_string->value().case_compare("greeting") == 0);
  // check double 1.1
  ObJsonDouble *json_double = static_cast<ObJsonDouble *>((*json_array)[1]);
  ASSERT_TRUE(json_double->json_type() == ObJsonNodeType::J_DOUBLE);
  ASSERT_TRUE(json_double->value() == 1.1);
  // check uint 2
  ObJsonUint *json_uint = static_cast<ObJsonUint *>((*json_array)[2]);
  ASSERT_TRUE(json_uint->json_type() == ObJsonNodeType::J_INT);
  ASSERT_TRUE(json_uint->value() == 2);
  // check int -10
  ObJsonInt *json_int = static_cast<ObJsonInt *>((*json_array)[3]);
  ASSERT_TRUE(json_int->json_type() == ObJsonNodeType::J_INT);
  ASSERT_TRUE(json_int->value() == -10);
  // check boolean true
  ObJsonBoolean *json_boolean = static_cast<ObJsonBoolean *>((*json_array)[4]);
  ASSERT_TRUE(json_boolean->json_type() == ObJsonNodeType::J_BOOLEAN);
  ASSERT_TRUE(json_boolean->value() == true);
  // check null NULL
  ObJsonNull *json_null = static_cast<ObJsonNull *>((*json_array)[5]);
  ASSERT_TRUE(json_null->json_type() == ObJsonNodeType::J_NULL);
}

TEST_F(TestJsonBin, test_bin_lookup)
{
  common::ObString j_text("{ \"greeting\" : [\"test\", 1.1, 2, -10, true, null], \"farewell\" : 2, \"json_text\" : 3 }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  uint64_t root_member_count = bin->element_count();
  ASSERT_EQ(3, root_member_count);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());

  ObString lkey("greeting");
  ObIJsonBase* j_bin1 = NULL;
  ASSERT_EQ(OB_SUCCESS, bin->get_object_value(lkey, j_bin1));
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin1->json_type());
  uint64_t sub_member_count = j_bin1->element_count();
  ASSERT_EQ(6, sub_member_count);
  // "test"
  {
    ObIJsonBase* j_bin_sub = NULL;
    ASSERT_EQ(OB_SUCCESS, j_bin1->get_array_element(0, j_bin_sub));
    ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_sub->json_type());
    ObString ele0("test");
    uint64_t data_len = j_bin_sub->get_data_length();
    const char *data = j_bin_sub->get_data();
    ObString actual(data_len, data);
    ASSERT_EQ(0, ele0.compare(actual));
  }
  // 1.1
  {
    ObIJsonBase* j_bin_sub = NULL;
    ASSERT_EQ(OB_SUCCESS, j_bin1->get_array_element(1, j_bin_sub));
    ASSERT_EQ(ObJsonNodeType::J_DOUBLE, j_bin_sub->json_type());
    ASSERT_FLOAT_EQ(1.1, j_bin_sub->get_double());
  }
  // 2
  {
    ObIJsonBase* j_bin_sub = NULL;
    ASSERT_EQ(OB_SUCCESS, j_bin1->get_array_element(2, j_bin_sub));
    ASSERT_EQ(ObJsonNodeType::J_INT, j_bin_sub->json_type());
    ASSERT_EQ(2, j_bin_sub->get_uint());
  }
  // -10
  {
    ObIJsonBase* j_bin_sub = NULL;
    ASSERT_EQ(OB_SUCCESS, j_bin1->get_array_element(3, j_bin_sub));
    ASSERT_EQ(ObJsonNodeType::J_INT, j_bin_sub->json_type());
    ASSERT_EQ(-10, j_bin_sub->get_int());
  }
  // true
  {
    ObIJsonBase* j_bin_sub = NULL;
    ASSERT_EQ(OB_SUCCESS, j_bin1->get_array_element(4, j_bin_sub));
    ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, j_bin_sub->json_type());
    ASSERT_EQ(true, j_bin_sub->get_boolean());
  }
  // null
  {
    ObIJsonBase* j_bin_sub = NULL;
    ASSERT_EQ(OB_SUCCESS, j_bin1->get_array_element(5, j_bin_sub));
    ASSERT_EQ(ObJsonNodeType::J_NULL, j_bin_sub->json_type());
  }
}

TEST_F(TestJsonBin, test_wrapper_to_string_object)
{
  set_compat_mode(lib::Worker::CompatMode::MYSQL);
  // json text 转 json tree
  common::ObString json_text("{ \"greeting\" : \"Hello!\", \"farewell\" : \"bye-bye!\", \"json_text\" : \"test!\" }");
  common::ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, json_text.ptr(), json_text.length(), syntaxerr, NULL, json_tree));

  // wrapper to string
  ObJsonBin bin(&allocator);
  bin.parse_tree(json_tree);

  ObIJsonBase *j_base = &bin;
  ObJsonBuffer buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_base->print(buf, true));
  std::cout << buf.ptr() << std::endl;
  // 由于构建树的过程，进行了排序，顺序已经打乱，所以输出的顺序和用户的输入顺序不一样
  EXPECT_STREQ(buf.ptr(), "{\"farewell\": \"bye-bye!\", \"greeting\": \"Hello!\", \"json_text\": \"test!\"}");
}

TEST_F(TestJsonBin, test_json_bin_load_without_common_header)
{
  common::ObString j_text("[\"greeting\", 1.1, 2, -10, true, null]");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));

  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // check result
  uint64_t doc_header_len = sizeof(ObJsonBinDocHeader);
  char *ptr = result.ptr() +  doc_header_len;
  uint64_t offset = 0;
  ObJsonBinHeader *header = reinterpret_cast<ObJsonBinHeader *>(ptr);
  ASSERT_EQ(header->type_, ObJBVerType::J_ARRAY_V0);

  offset += OB_JSON_BIN_HEADER_LEN;
  uint64_t obj_size;
  ObJsonVar::read_var(ptr + offset + ObJsonVar::get_var_size(header->count_size_), header->obj_size_size_, &obj_size);
  ASSERT_EQ(doc_header_len + obj_size, result.length());
  fprintf(stdout, "[test] obj_size is %lu\n", obj_size);

  ObJsonBin test_bin(result.ptr(), result.length(), &allocator);
  test_bin.set_seek_flag(false);
  test_bin.reset_iter();
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, test_bin.json_type());
  uint64_t sub_member_count = test_bin.element_count();
  ASSERT_EQ(6, sub_member_count);
  // "greeting"
  {
    test_bin.element(0);
    uint8_t actual_type = (static_cast<uint8>(test_bin.json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_STRING), actual_type);
    ObString ele0("greeting");
    ObString data(test_bin.get_data_length(), test_bin.get_data());
    ASSERT_EQ(0, ele0.compare(data));
  }
  // 1.1
  {
    ASSERT_EQ(OB_SUCCESS, test_bin.move_parent_iter());
    test_bin.element(1);
    uint8_t actual_type = (static_cast<uint8>(test_bin.json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_DOUBLE), actual_type);
    ASSERT_FLOAT_EQ(1.1, test_bin.get_double());
  }
  // 2
  {
    ASSERT_EQ(OB_SUCCESS, test_bin.move_parent_iter());
    test_bin.element(2);
    uint8_t actual_type = (static_cast<uint8>(test_bin.json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_INT), actual_type);
    ASSERT_EQ(2, test_bin.get_uint());
  }
  // -10
  {
    ASSERT_EQ(OB_SUCCESS, test_bin.move_parent_iter());
    test_bin.element(3);
    uint8_t actual_type = (static_cast<uint8>(test_bin.json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_INT), actual_type);
    ASSERT_EQ(-10, test_bin.get_int());
  }
  // true
  {
    ASSERT_EQ(OB_SUCCESS, test_bin.move_parent_iter());
    test_bin.element(4);
    uint8_t actual_type = (static_cast<uint8>(test_bin.json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_BOOLEAN), actual_type);
    ASSERT_EQ(true, test_bin.get_boolean());
  }
  // null
  {
    test_bin.reset_iter(); // test reset iter
    test_bin.element(5);
    uint8_t actual_type = (static_cast<uint8>(test_bin.json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_NULL), actual_type);
  }
}

TEST_F(TestJsonBin, test_bin_update)
{
  common::ObString j_text("{ \"greeting\" : [\"test\", 1.1, 2, -10, true, null], \"farewell\" : 2, \"json_text\" : 3 }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  uint64_t root_member_count = bin->element_count();
  ASSERT_EQ(3, root_member_count);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  ObString lkey("greeting");
  bin->lookup(lkey);
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
  uint64_t sub_member_count = bin->element_count();
  ASSERT_EQ(6, sub_member_count);
  // "test" --> "hahahahahah"
  {
    bin->element(0);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_STRING), actual_type);
    ObString ele0("test");
    ObString data(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele0.compare(data));

    ObJsonString new_val("hahahahahah", strlen("hahahahahah"));
    ObIJsonBase *j_base_new = &new_val;
    ObIJsonBase *j_bin_val = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base_new,
        ObJsonInType::JSON_BIN, j_bin_val));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->update(0, static_cast<ObJsonBin *>(j_bin_val)));
    ASSERT_EQ(2, update_ctx.binary_diffs_.count());
    ASSERT_EQ(update_ctx.is_rebuild_all_, false);
    check_diff_valid(allocator, result, update_ctx);
    check_json_diff_valid(allocator, j_text, update_ctx, 1);
    bin->element(0);
    actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_STRING), actual_type);
    ObString new_ele("hahahahahah");
    ObString data1(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, new_ele.compare(data1));
  }
  // 1.1 --> 3.1415
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    bin->element(1);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_DOUBLE), actual_type);
    ASSERT_FLOAT_EQ(1.1, bin->get_double());

    ObJsonDouble new_val(3.1415);
    ObIJsonBase *j_base_new = &new_val;
    ObIJsonBase *j_bin_val = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base_new,
        ObJsonInType::JSON_BIN, j_bin_val));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->update(1, static_cast<ObJsonBin *>(j_bin_val)));
    ASSERT_EQ(3, update_ctx.binary_diffs_.count());
    ASSERT_EQ(update_ctx.is_rebuild_all_, false);
    check_diff_valid(allocator, result, update_ctx);
    check_json_diff_valid(allocator, j_text, update_ctx, 2);
    bin->element(1);
    actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_DOUBLE), actual_type);
    ASSERT_FLOAT_EQ(3.1415, bin->get_double());
  }
  // 2 ---> -655666
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    bin->element(2);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_INT), actual_type);
    ASSERT_EQ(2, bin->get_uint());

    ObJsonInt new_val(-655666);
    ObIJsonBase *j_base_new = &new_val;
    ObIJsonBase *j_bin_val = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base_new,
        ObJsonInType::JSON_BIN, j_bin_val));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->update(2, static_cast<ObJsonBin *>(j_bin_val)));
    ASSERT_EQ(5, update_ctx.binary_diffs_.count());
    ASSERT_EQ(update_ctx.is_rebuild_all_, false);
    check_diff_valid(allocator, result, update_ctx);
    check_json_diff_valid(allocator, j_text, update_ctx, 3);
    bin->element(2);
    actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_INT), actual_type);
    ASSERT_EQ(-655666, bin->get_int());
  }
  // -10 ---> 655666
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    bin->element(3);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_INT), actual_type);
    ASSERT_EQ(-10, bin->get_int());

    ObJsonUint new_val(655666);
    ObIJsonBase *j_base_new = &new_val;
    ObIJsonBase *j_bin_val = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base_new,
        ObJsonInType::JSON_BIN, j_bin_val));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->update(3, static_cast<ObJsonBin *>(j_bin_val)));
    ASSERT_EQ(7, update_ctx.binary_diffs_.count());
    ASSERT_EQ(update_ctx.is_rebuild_all_, false);
    check_diff_valid(allocator, result, update_ctx);
    check_json_diff_valid(allocator, j_text, update_ctx, 4);
    bin->element(3);
    actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_UINT), actual_type);
    ASSERT_EQ(655666, bin->get_uint());
  }
  // true
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    bin->element(4);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_BOOLEAN), actual_type);
    ASSERT_EQ(true, bin->get_boolean());
  }
  // null
  {
    bin->reset_iter(); // test reset iter
    bin->lookup(lkey);
    bin->element(5);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_NULL), actual_type);
  }
}

TEST_F(TestJsonBin, test_bin_remove)
{
  common::ObString j_text("{ \"greeting\" : [\"test\", 1.1, 2, -10, true, null], \"farewell\" : 2, \"json_text\" : 3 }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  uint64_t root_member_count = bin->element_count();
  ASSERT_EQ(3, root_member_count);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());

  ObString lkey("greeting");
  bin->lookup(lkey);
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
  uint64_t sub_member_count = bin->element_count();
  ASSERT_EQ(6, sub_member_count);
  // "test"
  {
    bin->element(0);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_STRING), actual_type);
    ObString ele0("test");
    ObString data(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele0.compare(data));
    
  }
  // 1.1
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->remove(0));
    ASSERT_EQ(1, update_ctx.binary_diffs_.count());
    ASSERT_EQ(update_ctx.is_rebuild_all_, false);
    check_diff_valid(allocator, result, update_ctx);
    check_json_diff_valid(allocator, j_text, update_ctx, 1);
    bin->element(0);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_DOUBLE), actual_type);
    ASSERT_FLOAT_EQ(1.1, bin->get_double());
  }
  // 2
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->remove(0));
    ASSERT_EQ(2, update_ctx.binary_diffs_.count());
    ASSERT_EQ(update_ctx.is_rebuild_all_, false);
    check_diff_valid(allocator, result, update_ctx);
    check_json_diff_valid(allocator, j_text, update_ctx, 2);
    bin->element(0);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_INT), actual_type);
    ASSERT_EQ(2, bin->get_uint());
  }
  // -10
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->remove(0));
    ASSERT_EQ(3, update_ctx.binary_diffs_.count());
    ASSERT_EQ(update_ctx.is_rebuild_all_, false);
    check_diff_valid(allocator, result, update_ctx);
    check_json_diff_valid(allocator, j_text, update_ctx, 3);
    bin->element(0);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_INT), actual_type);
    ASSERT_EQ(-10, bin->get_int());
  }
  // true
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->remove(0));
    ASSERT_EQ(4, update_ctx.binary_diffs_.count());
    ASSERT_EQ(update_ctx.is_rebuild_all_, false);
    check_diff_valid(allocator, result, update_ctx);
    check_json_diff_valid(allocator, j_text, update_ctx, 4);
    bin->element(0);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_BOOLEAN), actual_type);
    ASSERT_EQ(true, bin->get_boolean());
  }
  // null
  {
    bin->reset_iter(); // test reset iter
    bin->lookup(lkey);
    ASSERT_EQ(OB_SUCCESS, bin->remove(0));
    ASSERT_EQ(5, update_ctx.binary_diffs_.count());
    ASSERT_EQ(update_ctx.is_rebuild_all_, false);
    check_diff_valid(allocator, result, update_ctx);
    check_json_diff_valid(allocator, j_text, update_ctx, 5);
    bin->element(0);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_NULL), actual_type);
  }
  ASSERT_EQ(OB_SUCCESS, bin->reset_iter());
  size_t free_space;
  ASSERT_EQ(OB_SUCCESS, bin->get_free_space(free_space));
  ASSERT_GT(free_space, 0);
  fprintf(stdout, "[test] after remove free_space is %zu\n", free_space);
  ASSERT_EQ(OB_SUCCESS, bin->rebuild());
  ASSERT_EQ(OB_SUCCESS, bin->get_free_space(free_space));
  fprintf(stdout, "[test] after rebuild free_space is %zu\n", free_space);
  ASSERT_EQ(0, free_space);
}

TEST_F(TestJsonBin, test_bin_update_with_obj)
{
  common::ObString j_text("{ \"a\" : [1,2.2,-3,null,\"hahah\"], \"b\" : { \"b1\" : 321, \"b2\" : { \"b21\" : \"testing\" } }, \"c\" : \"ccccc\" }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin1 = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin1));

  common::ObString j_text2("{ \"a\" : [1,2.2,-3,null,\"hahah\"], \"b\" : { \"b1\" : 321, \"b2\" : { \"b21\" : \"killing!!!\", \"b22\" : \"newwuwuwu\" } }, \"c\" : \"ccccc\" }");
  ObIJsonBase *j_bin2 = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text2,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin2));

  common::ObString result2;
  ASSERT_EQ(OB_SUCCESS, j_bin2->get_raw_binary(result2, &allocator));
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin2);
  bin->set_seek_flag(false);
  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  // 1. update j_bin2.b.b1 from 321 ---> "updated"
  {
    common::ObJsonBuffer buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, j_bin1->print(buf, true));
    std::cout << buf.ptr() << std::endl;
    ObJsonBin *bin2 = static_cast<ObJsonBin *>(j_bin2);
    bin2->set_seek_flag(false);
    ASSERT_EQ(OB_SUCCESS, bin2->lookup("b"));
    size_t idx;
    ASSERT_EQ(OB_SUCCESS, bin2->lookup_index("b1", &idx));
    ObJsonString j_str("updated", strlen("updated"));
    ObIJsonBase *j_tree_str = &j_str;
    ObIJsonBase *j_bin_str = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
        ObJsonInType::JSON_BIN, j_bin_str));
    ASSERT_EQ(OB_SUCCESS, bin2->update(idx, static_cast<ObJsonBin *>(j_bin_str)));
    ASSERT_EQ(2, update_ctx.binary_diffs_.count());
    ASSERT_EQ(update_ctx.is_rebuild_all_, false);
    check_diff_valid(allocator, result2, update_ctx);
    check_json_diff_valid(allocator, j_text2, update_ctx, 1);
    ASSERT_EQ(OB_SUCCESS, bin2->element(idx));
    uint8_t actual_type = (static_cast<uint8_t>(bin2->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8_t>(ObJsonNodeType::J_STRING), actual_type);
    ObString new_ele("updated");
    ObString data(bin2->get_data_length(), bin2->get_data());
    ASSERT_EQ(0, new_ele.compare(data));
    ASSERT_EQ(OB_SUCCESS, bin2->reset_iter());
    size_t free_space;
    ASSERT_EQ(OB_SUCCESS, bin2->get_free_space(free_space));
    ASSERT_EQ(free_space, 2); // from 321 in storage not inlined, use 2 bytes
    fprintf(stdout, "[test] after update bin2 free_space is %zu\n", free_space);
    ASSERT_EQ(OB_SUCCESS, bin2->reset_iter());
  }
  // 2. update j_bin1.b with j_bin2.b
  common::ObString result1;
  ASSERT_EQ(OB_SUCCESS, j_bin1->get_raw_binary(result1, &allocator));
  ObJsonBin *bin1 = static_cast<ObJsonBin *>(j_bin1);
  bin1->set_seek_flag(false);

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin1));
  ObJsonBinUpdateCtx &update_ctx1 = *bin1->get_update_ctx();

  {
    size_t free_space;
    ObJsonBin *bin2 = static_cast<ObJsonBin *>(j_bin2);
    bin2->set_seek_flag(false);
    ASSERT_EQ(OB_SUCCESS, bin2->get_free_space(free_space));
    ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin1);
    bin->set_seek_flag(false);
    ASSERT_EQ(OB_SUCCESS, bin->get_free_space(free_space));
    size_t idx;
    ASSERT_EQ(OB_SUCCESS, bin->lookup_index("b", &idx));
    ASSERT_EQ(OB_SUCCESS, bin2->lookup("b"));

    ObJsonBuffer rbuf(&allocator);
    ASSERT_EQ(OB_SUCCESS, bin2->rebuild_at_iter(rbuf));
  
    // ASSERT_EQ(OB_SUCCESS, bin2->get_free_space(free_space));
    ObJsonBin testbin(rbuf.ptr(), rbuf.length(), &allocator);
    testbin.set_seek_flag(false);
    ASSERT_EQ(OB_SUCCESS, testbin.reset_iter());
    ASSERT_EQ(OB_SUCCESS, testbin.get_free_space(free_space));
    ObIJsonBase *j_bin_test = &testbin;
    common::ObJsonBuffer buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, j_bin_test->print(buf, true));
    std::cout << buf.ptr() << std::endl;

    ASSERT_EQ(OB_SUCCESS, bin->update(idx, bin2));
    ASSERT_EQ(2, update_ctx1.binary_diffs_.count());
    ASSERT_EQ(update_ctx1.is_rebuild_all_, false);
    check_diff_valid(allocator, result1, update_ctx1);
    check_json_diff_valid(allocator, j_text, update_ctx1, 1);
    // print free space
    ASSERT_EQ(OB_SUCCESS, testbin.reset_iter());
    ASSERT_EQ(OB_SUCCESS, bin->get_free_space(free_space));
    ASSERT_GT(free_space, 0);
    fprintf(stdout, "[test] after update bin free_space is %zu\n", free_space);
    
    buf.reuse();
    ASSERT_EQ(OB_SUCCESS, bin->print(buf, true));
    std::cout << buf.ptr() << std::endl;
    
    ASSERT_EQ(OB_SUCCESS, bin->rebuild());
    ASSERT_EQ(OB_SUCCESS, bin->get_free_space(free_space));
    ASSERT_EQ(free_space, 0);
    fprintf(stdout, "[test] after rebuild bin free_space is %zu\n", free_space);
    buf.reuse();
    ASSERT_EQ(OB_SUCCESS, bin->print(buf, true));
    std::cout << buf.ptr() << std::endl;
    // check value
    EXPECT_STREQ(buf.ptr(), "{\"a\": [1, 2.2, -3, null, \"hahah\"], \"b\": {\"b1\": \"updated\", \"b2\": {\"b21\": \"killing!!!\", \"b22\": \"newwuwuwu\"}}, \"c\": \"ccccc\"}");
    buf.reuse();
    ASSERT_EQ(OB_SUCCESS, bin->print(buf, true));
    std::cout << buf.ptr() << std::endl;
    // check value
    EXPECT_STREQ(buf.ptr(), "{\"a\": [1, 2.2, -3, null, \"hahah\"], \"b\": {\"b1\": \"updated\", \"b2\": {\"b21\": \"killing!!!\", \"b22\": \"newwuwuwu\"}}, \"c\": \"ccccc\"}");
  }
}

TEST_F(TestJsonBin, issue_37549565)
{
  int ret = OB_SUCCESS;
  common::ObString path_str("$**.Width");
  std::cout<<"path_expression:"<<path_str.ptr()<<std::endl;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path(path_str, &allocator);
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);

  //common::ObString json_str("{\"name\": \"Safari\", \"os\": \"Mac\"}");
  //common::ObString json_str("{\"x\": \"Safari\", \"os\": \"Mac\", \"resolution\": {\"x\": \"1920\", \"y\": \"1080\"}}");
  common::ObString j_text("[\"sss\",{\"a\":\"100\",\"b\":100},[\"1\",2,\"a\",\"%%100%%\"]]");
  std::cout<<"json_data:"<<j_text.ptr()<<std::endl;
  
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonSeekResult hit;
  int cnt = test_path.path_node_cnt();
  ret = j_bin->seek(test_path, cnt, false, false, hit);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestJsonBin, test_bin_to_tree_after_seek)
{
  int ret = OB_SUCCESS;
  common::ObString path_str("$.x");
  std::cout<<"path_expression:"<<path_str.ptr()<<std::endl;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path(path_str, &allocator);
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);

  //common::ObString json_str("{\"name\": \"Safari\", \"os\": \"Mac\"}");
  //common::ObString json_str("{\"x\": \"Safari\", \"os\": \"Mac\", \"resolution\": {\"x\": \"1920\", \"y\": \"1080\"}}");
  common::ObString j_text("{\"x\": [1, 2, 3], \"os\": [4, 5,6] }");
  std::cout<<"json_data:"<<j_text.ptr()<<std::endl;
  
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonSeekResult hit;
  int cnt = test_path.path_node_cnt();
  ret = j_bin->seek(test_path, cnt, false, false, hit);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(hit.size(), 1);
  ObJsonBuffer rbuf(&allocator);
  ASSERT_EQ(OB_SUCCESS, hit[0]->print(rbuf, true));
  std::cout << rbuf.ptr() << std::endl;
  // check value
  EXPECT_STREQ(rbuf.ptr(), "[1, 2, 3]");
  rbuf.reuse();
  ASSERT_EQ(OB_SUCCESS, hit[0]->print(rbuf, true));
  std::cout << rbuf.ptr() << std::endl;
  // check value
  EXPECT_STREQ(rbuf.ptr(), "[1, 2, 3]");
  std::cout<<"end test"<<std::endl;
}

TEST_F(TestJsonBin, datetime)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  int64_t datetime = 0;
  int32_t date = 0;

  // CAST_FUNC_NAME(datetime, json)
  ObTime ob_time1(DT_TYPE_DATETIME);
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(946684800000000, NULL, ob_time1));
  ObJsonDatetime j_datetime(ObJsonNodeType::J_DATETIME, ob_time1);
  ObIJsonBase *j_tree_datetime = &j_datetime;
  ObIJsonBase *j_bin_datetime = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_datetime,
        ObJsonInType::JSON_BIN, j_bin_datetime));
  ObString res_str;
  ASSERT_EQ(OB_SUCCESS, j_bin_datetime->get_raw_binary(res_str, &allocator));

  // json_cell_str
  ObIJsonBase *j_bin2 = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, res_str,
      ObJsonInType::JSON_BIN, ObJsonInType::JSON_BIN, j_bin2));

  common::ObJsonBuffer buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_bin2->print(buf, false));
  std::cout << buf.ptr() << std::endl;
  EXPECT_STREQ(buf.ptr(), "2000-01-01 00:00:00.000000");
}

void createTime(ObTime &ob_time)
{
  ob_time.mode_ = DT_TYPE_TIME;
  common::ObString str("19:18:17.654321");
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_ob_time_without_date(str, ob_time));
}

//  void createDate(ObTime &ob_time)
//  {
//    ob_time.mode_ = DT_TYPE_DATE;
//    common::ObString str("20210906");
//    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_ob_time_with_date(str, ob_time));
//  }
//
//  void createDateTime(ObTime &ob_time)
//  {
//    ob_time.mode_ = DT_TYPE_DATETIME;
//    common::ObString str("2021-09-06 11:12:13.456789");
//    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_ob_time_with_date(str, ob_time));
//    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::validate_datetime(ob_time));
//  }
//
//  TEST_F(TestJsonBin, dateAndTimeTest)
//  {
//    common::ObArenaAllocator allocator(ObModIds::TEST);
//    ObJsonArray array(&allocator);
//    ObTime obtime, obdate, obdatetime;
//    createTime(obtime);
//    createDate(obdate);
//    createDateTime(obdatetime);
//    ObJsonDatetime jt(ObJsonNodeType::J_TIME, obtime);
//    ObJsonDatetime jd(ObJsonNodeType::J_DATE, obdate);
//    ObJsonDatetime jtd(ObJsonNodeType::J_DATETIME, obdatetime);
//    // add to array
//    ASSERT_EQ(OB_SUCCESS, array.append(&jt));
//    ASSERT_EQ(OB_SUCCESS, array.append(&jd));
//    ASSERT_EQ(OB_SUCCESS, array.append(&jtd));
//
//    ObIJsonBase *j_tree_array = &array;
//    ObIJsonBase *j_bin_array = NULL;
//    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_array,
//          ObJsonInType::JSON_BIN, j_bin_array));
//    ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin_array->json_type());
//    ASSERT_EQ(3, j_bin_array->element_count());
//
//    // check time 19:18:17.654321
//    ObIJsonBase *j_bin1 = NULL;
//    ASSERT_EQ(OB_SUCCESS, j_bin_array->get_array_element(0, j_bin1));
//    ASSERT_EQ(ObJsonNodeType::J_TIME, j_bin1->json_type());
//    ObTime jt1;
//    j_bin1->get_obtime(jt1);
//    ASSERT_EQ(19, jt1.parts_[DT_HOUR]);
//    ASSERT_EQ(18, jt1.parts_[DT_MIN]);
//    ASSERT_EQ(17, jt1.parts_[DT_SEC]);
//    ASSERT_EQ(654321, jt1.parts_[DT_USEC]);
//
//    // check date 20210906
//    j_bin1 = NULL;
//    ASSERT_EQ(OB_SUCCESS, j_bin_array->get_array_element(1, j_bin1));
//    ASSERT_EQ(ObJsonNodeType::J_DATE, j_bin1->json_type());
//    j_bin1->get_obtime(jt1);
//    ASSERT_EQ(2021, jt1.parts_[DT_YEAR]);
//    ASSERT_EQ(9, jt1.parts_[DT_MON]);
//    ASSERT_EQ(6, jt1.parts_[DT_MDAY]);
//
//    // check datetime 2021-09-06 11:12:13.456789
//    j_bin1 = NULL;
//    ASSERT_EQ(OB_SUCCESS, j_bin_array->get_array_element(2, j_bin1));
//    ASSERT_EQ(ObJsonNodeType::J_DATETIME, j_bin1->json_type());
//    j_bin1->get_obtime(jt1);
//    ASSERT_EQ(11, jt1.parts_[DT_HOUR]);
//    ASSERT_EQ(12, jt1.parts_[DT_MIN]);
//    ASSERT_EQ(13, jt1.parts_[DT_SEC]);
//    ASSERT_EQ(456789, jt1.parts_[DT_USEC]);
//    ASSERT_EQ(2021, jt1.parts_[DT_YEAR]);
//    ASSERT_EQ(9, jt1.parts_[DT_MON]);
//    ASSERT_EQ(6, jt1.parts_[DT_MDAY]);
//  }

long getCurrentTime()
{
   struct timeval tv;
   gettimeofday(&tv,NULL);
   return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

TEST_F(TestJsonBin, test_seek_member) {
  int ret = OB_SUCCESS;
  common::ObString path_str("$.\"resolution\".x");
  std::cout<<"path_expression:"<<path_str.ptr()<<std::endl;
  long t_path1 = getCurrentTime();
  ObArenaAllocator allocator(ObModIds::TEST); 
  ObJsonPath test_path(path_str, &allocator);
  ret = test_path.parse_path();
  std::cout<<"time of parse_path:"<<getCurrentTime()-t_path1<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  //common::ObString json_str("{\"name\": \"Safari\", \"os\": \"Mac\"}");
  common::ObString j_text("{\"x\": \"Safari\", \"os\": \"Mac\", \"resolution\": {\"x\": \"1920\", \"y\": \"1080\"}}");
  std::cout<<"json_data:"<<j_text.ptr()<<std::endl;
 
  long t_json1 =getCurrentTime();
  std::cout<<"time of parse_json:"<<getCurrentTime()-t_json1<<std::endl;
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonSeekResult hit;
  int cnt = test_path.path_node_cnt();
  long t_seek1 =getCurrentTime();
  ret = j_bin->seek(test_path, cnt, false, false, hit);
  std::cout<<"time of seek:"<<getCurrentTime()-t_seek1<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(false, hit[0]->is_tree());
  uint64_t data_len = hit[0]->get_data_length();
  const char *data = hit[0]->get_data();
  ObString actual(data_len, data);
  std::cout.write(actual.ptr(), actual.length());
  std::cout<<std::endl;
  ASSERT_TRUE(actual.case_compare("1920") == 0);
  std::cout<<"end test"<<std::endl;
}

TEST_F(TestJsonBin, test_seek_member_wildcard) {
  common::ObString path_str("$.*");
  std::cout<<"path_expression:"<<path_str.ptr()<<std::endl;
  long t_path1 = getCurrentTime();
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path(path_str, &allocator);
  std::cout<<"time of parse_path:"<<getCurrentTime()-t_path1<<std::endl;
  ASSERT_EQ(OB_SUCCESS, test_path.parse_path());

  //common::ObString json_str("{\"name\": \"Safari\", \"os\": \"Mac\"}");
  common::ObString j_text("{\"name\": \"Safari\", \"os\": \"Mac\", \"resolution\": {\"x\": \"1920\", \"y\": \"1080\"}}");
  std::cout<<"json_data:"<<j_text.ptr()<<std::endl;
  
  long t_json1 =getCurrentTime();
  std::cout<<"time of parse_json:"<<getCurrentTime()-t_json1<<std::endl;
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonSeekResult hit;
  int cnt = test_path.path_node_cnt();
  long t_seek1 =getCurrentTime();
  std::cout<<"time of seek:"<<getCurrentTime()-t_seek1<<std::endl;
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(test_path, cnt, false, false, hit));

  ASSERT_EQ(hit.size(), 3);
  common::ObString expect_val[] = { "Mac", "Safari", "{\"x\": \"1920\", \"y\": \"1080\"}" };
  for (int i=0; i<hit.size(); ++i) {
    ObJsonBuffer jbuf(&allocator);
    ASSERT_EQ(false, hit[i]->is_tree());
    hit[i]->print(jbuf, false);
    std::cout<<i<<": "<<jbuf.ptr()<<std::endl;
    ASSERT_EQ(0, expect_val[i].compare(jbuf.string()));
  }
  std::cout<<"end test"<<std::endl;
}

TEST_F(TestJsonBin, test_seek_array_cell) {
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path("$[0][0 to last]", &allocator);
  ASSERT_EQ(OB_SUCCESS, test_path.parse_path());
  common::ObString j_text("[1, 2, 3, 4, 5]");
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonSeekResult hit;
  int cnt = test_path.path_node_cnt();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(test_path, cnt, true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(false, hit[0]->is_tree());
  ASSERT_EQ(1, hit[0]->get_uint());
}

TEST_F(TestJsonBin, test_seek_array_range) {
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path("$[last-4 to last -3]", &allocator);
  ASSERT_EQ(OB_SUCCESS, test_path.parse_path());
  common::ObString j_text("[1, 2, 3, 4, 5]");
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonSeekResult hit;
  int cnt = test_path.path_node_cnt();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(test_path, cnt, false, false, hit));
  ASSERT_EQ(hit.size(), 2);
  for(int i=0; i<hit.size(); ++i){
    ASSERT_EQ(false, hit[i]->is_tree());
    ASSERT_EQ(i+1, hit[i]->get_uint());
  }
}

TEST_F(TestJsonBin, test_seek_ellipsis) {
  common::ObString path_str("$**[0]");
  std::cout<<"path_expression:"<<path_str.ptr()<<std::endl;
  long t_path1 = getCurrentTime();
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path(path_str, &allocator);
  std::cout<<"time of parse_path:"<<getCurrentTime()-t_path1<<std::endl;
  ASSERT_EQ(OB_SUCCESS, test_path.parse_path());
  //common::ObString json_str("{\"name\": \"Safari\", \"os\": \"Mac\"}");
  //common::ObString json_str("{\"x\": \"Safari\", \"os\": \"Mac\", \"resolution\": {\"x\": \"1920\", \"y\": \"1080\"}}");
  common::ObString j_text("{\"x\": [1, 2, 3], \"os\": [4, 5,6] }");
  std::cout<<"json_data:"<<j_text.ptr()<<std::endl;
  
  long t_json1 =getCurrentTime();
  std::cout<<"time of parse_json:"<<getCurrentTime()-t_json1<<std::endl;
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonSeekResult hit;
  int cnt = test_path.path_node_cnt();
  long t_seek1 =getCurrentTime();
  std::cout<<"time of seek:"<<getCurrentTime()-t_seek1<<std::endl;
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(test_path, cnt, false, false, hit));

  ASSERT_EQ(hit.size(), 2);
  uint64_t expect_value[] = { 1, 4 };
  for (int i=0; i<hit.size(); ++i) {
    ObJsonBuffer jbuf(&allocator);
    ASSERT_EQ(false, hit[i]->is_tree());
    hit[i]->print(jbuf, false);
    std::cout<<i<<": "<<jbuf.ptr()<<std::endl;
    ASSERT_EQ(expect_value[i], hit[i]->get_uint());
  }
  std::cout<<"end test"<<std::endl;
}

TEST_F(TestJsonBin, test_bin_object_add)
{
  common::ObString j_text("{ \"greeting\" : [\"test\", 1.1, 2, -10, true, null], \"farewell\" : 2, \"json_text\" : 3 }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  ObIJsonBase *j_bin1 = nullptr;
  ObString lkey("greeting");
  ASSERT_EQ(OB_SUCCESS, j_bin->get_object_value(lkey, j_bin1));

  ObJsonString j_str("hahahahahah", strlen("hahahahahah"));
  ObIJsonBase *j_tree_str = &j_str;
  ObIJsonBase *j_bin_str = NULL;
  // TREE -> BIN
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
      ObJsonInType::JSON_BIN, j_bin_str));
  ASSERT_EQ(OB_SUCCESS, j_bin->object_add("greeting", j_bin_str));

  ASSERT_FALSE(update_ctx.is_rebuild_all_);
  ASSERT_EQ(2, update_ctx.binary_diffs_.count());
  check_diff_valid(allocator, result, update_ctx);
  check_json_diff_valid(allocator, j_text, update_ctx, 1);

  // after j_bin has been update, j_bin1 is unavailable
  ASSERT_EQ(false, j_bin->is_tree());
  size_t free_space;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_free_space(free_space));
  ASSERT_GT(free_space, 0);
  fprintf(stdout, "[test] after update bin free_space is %zu\n", free_space);

  // append new key, will make j_bin to tree
  ASSERT_EQ(OB_SUCCESS, j_bin->object_add("test_new_key", j_bin_str));
  ASSERT_EQ(false, j_bin->is_tree());
  ASSERT_TRUE(update_ctx.is_rebuild_all_);

  ObJsonBuffer buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_bin->print(buf, true));
  std::cout << buf.ptr() << std::endl;
  EXPECT_STREQ(buf.ptr(), "{\"farewell\": 2, \"greeting\": \"hahahahahah\", \"json_text\": 3, \"test_new_key\": \"hahahahahah\"}");
}

TEST_F(TestJsonBin, test_bin_object_add2)
{
  common::ObString j_text("{ \"greeting\" : [\"test\", 1.1, 2, -10, true, null], \"farewell\" : 2, \"json_text\" : 3 }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  ObIJsonBase *j_bin1 = nullptr;
  ObString lkey("greeting");
  ASSERT_EQ(OB_SUCCESS, j_bin->get_object_value(lkey, j_bin1));

  ObJsonString j_str("hahahahahah", strlen("hahahahahah"));
  ObIJsonBase *j_tree_str = &j_str;
  ObIJsonBase *j_bin_str = NULL;
  // TREE -> BIN
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
      ObJsonInType::JSON_BIN, j_bin_str));
  ASSERT_EQ(OB_SUCCESS, j_bin->object_add("greeting", j_bin_str));
  // after j_bin has been update, j_bin1 is unavailable
  ASSERT_EQ(false, j_bin->is_tree());

  ASSERT_FALSE(update_ctx.is_rebuild_all_);
  ASSERT_EQ(2, update_ctx.binary_diffs_.count());
  check_diff_valid(allocator, result, update_ctx);
  check_json_diff_valid(allocator, j_text, update_ctx, 1);

  size_t free_space;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_free_space(free_space));
  //ASSERT_GT(free_space, 0);
  fprintf(stdout, "[test] after update bin free_space is %zu\n", free_space);

  // append new key, will make j_bin to tree
  ASSERT_EQ(OB_SUCCESS, j_bin->object_add("json_texa", j_bin_str));
  ASSERT_EQ(false, j_bin->is_tree());
  ASSERT_TRUE(update_ctx.is_rebuild_all_);

  ObJsonBuffer buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_bin->print(buf, true));
  std::cout << buf.ptr() << std::endl;
  EXPECT_STREQ(buf.ptr(), "{\"farewell\": 2, \"greeting\": \"hahahahahah\", \"json_texa\": \"hahahahahah\", \"json_text\": 3}");
}

TEST_F(TestJsonBin, test_bin_append)
{
  common::ObString j_text("{}");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  common::ObString key_str1("greeting");
  common::ObString j_text1("[\"greeting\", 1.1, 2, -10, true, null]");
  ObIJsonBase *j_bin1 = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text1,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin1));
  ObJsonBin *bin1 = static_cast<ObJsonBin *>(j_bin1);
  bin1->set_seek_flag(false);

  ASSERT_EQ(OB_SUCCESS, bin->add(key_str1, bin1));
  ASSERT_TRUE(update_ctx.is_rebuild_all_);
  ASSERT_EQ(OB_SUCCESS, bin->lookup(key_str1));

  common::ObString str2("json_string-json_string-json_string-json_string-json_string-json_string-json_string-"
                        "json_string-json_string-json_string-json_string-json_string-json_string-json_string-"
                        "json_string-json_string-json_string-json_string-json_string-json_string-json_string");
  ObJsonString j_str(str2.ptr(), str2.length());
  ObIJsonBase *j_tree_str = &j_str;
  ObIJsonBase *j_bin_str = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
      ObJsonInType::JSON_BIN, j_bin_str));
  ObJsonBin *bin2 = static_cast<ObJsonBin *>(j_bin_str);
  bin2->set_seek_flag(false);
  ASSERT_EQ(OB_SUCCESS, bin->append(bin2));
  ASSERT_TRUE(update_ctx.is_rebuild_all_);
  ObJsonBuffer buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_bin->print(buf, true));
  std::cout << buf.ptr() << std::endl;
}

TEST_F(TestJsonBin, test_bin_array_insert)
{
  common::ObString j_text("[\"greeting\", 1.1, 2, -10, true, null]");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  common::ObString str1("json_string-json_string-json_string-json_string-json_string-json_string-json_string-"
                        "json_string-json_string-json_string-json_string-json_string-json_string-json_string-"
                        "json_string-json_string-json_string-json_string-json_string-json_string-json_string");
  ObJsonString json_str1(str1.ptr(), str1.length());
  ObIJsonBase *j_base_str = &json_str1;
  ObIJsonBase *j_bin_str = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base_str,
        ObJsonInType::JSON_BIN, j_bin_str));
  ObJsonBin *bin1 = static_cast<ObJsonBin *>(j_bin_str);

  ASSERT_EQ(OB_SUCCESS, bin->insert(bin1, 10));
  ASSERT_EQ(7, bin->element_count());
  ASSERT_TRUE(update_ctx.is_rebuild_all_);
  ObJsonBuffer buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_bin_str->print(buf, true));
  std::cout << buf.ptr() << std::endl;
}

void validate_large_array(const ObIJsonBase *j_base, uint64_t size)
{
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_base->json_type());
  ASSERT_EQ(size, j_base->element_count());
  for (int i = 0; i < size/4; i++) {
    // check boolean true
    ObIJsonBase *j_bin1 = NULL;
    bool value;
    ASSERT_EQ(OB_SUCCESS, j_base->get_array_element(i * 4, j_bin1));
    ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, j_bin1->json_type());
    ASSERT_EQ(true, j_bin1->get_boolean());
    // check boolean true
    j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, j_base->get_array_element(i * 4 + 1, j_bin1));
    ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, j_bin1->json_type());
    ASSERT_EQ(false, j_bin1->get_boolean());
    // check null
    j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, j_base->get_array_element(i * 4 + 2, j_bin1));
    ASSERT_EQ(ObJsonNodeType::J_NULL, j_bin1->json_type());
    // check string
    j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, j_base->get_array_element(i * 4 + 3, j_bin1));
    ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin1->json_type());
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, j_bin1->print(buf, false));
    // std::cout << buf.ptr() << std::endl;
    EXPECT_STREQ(buf.ptr(), "a");
  }
}

void large_test_make_array(ObIAllocator &allocator, ObJsonArray &array)
{
  for (int i = 0; i < 20000; i++) {
    // add boolean true
    void *buf = allocator.alloc(sizeof(ObJsonBoolean));
    ObJsonBoolean *item1 = new(buf)ObJsonBoolean(true);
    ASSERT_EQ(OB_SUCCESS, array.append(item1));
    // add boolean false
    buf = allocator.alloc(sizeof(ObJsonBoolean));
    ObJsonBoolean *item2 = new(buf)ObJsonBoolean(false);
    ASSERT_EQ(OB_SUCCESS, array.append(item2));
    // add json null
    buf = allocator.alloc(sizeof(ObJsonNull));
    ObJsonNull *item3 = new(buf)ObJsonNull();
    ASSERT_EQ(OB_SUCCESS, array.append(item3));
    // add json string
    buf = allocator.alloc(sizeof(ObJsonString));
    ObJsonString *item4 = new(buf)ObJsonString("a", 1);
    ASSERT_EQ(OB_SUCCESS, array.append(item4));
  }
}

TEST_F(TestJsonBin, large_array)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonArray array(&allocator);
  large_test_make_array(allocator, array);
  ObIJsonBase *j_base_array = &array;
  ObIJsonBase *j_bin_array = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base_array,
        ObJsonInType::JSON_BIN, j_bin_array));
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin_array->json_type());

  // check val
  validate_large_array(j_bin_array, 80000);
  // create new json bin load and validate
  {
    ObString raw_bin;
    j_bin_array->get_raw_binary(raw_bin, &allocator);
    ObJsonBin new_json_bin(raw_bin.ptr(), raw_bin.length(), &allocator);
    ASSERT_EQ(OB_SUCCESS, new_json_bin.reset_iter());
    ObIJsonBase *j_base_new = &new_json_bin;
    validate_large_array(j_base_new, 80000);
  }
  //allocator.reset();
}

TEST_F(TestJsonBin, large_two_depth_array)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonArray array(&allocator);
  large_test_make_array(allocator, array);
  ObIJsonBase *wr = &array;
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, wr,
        ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin->json_type());

  // check val
  validate_large_array(j_bin, 80000);
  // make array2
  {
    ObJsonArray array2(&allocator);
    large_test_make_array(allocator, array2);
    ObJsonArray array3(&allocator);
    ASSERT_EQ(OB_SUCCESS, array3.append(&array));
    ASSERT_EQ(OB_SUCCESS, array3.append(&array2));
    ObIJsonBase *j_base_new = &array3;
    ObIJsonBase *j_bin_val = NULL;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base_new,
        ObJsonInType::JSON_BIN, j_bin_val));
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin_val->json_type());
    ObIJsonBase *j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, j_bin_val->get_array_element(0, j_bin1));
    validate_large_array(j_bin1, 80000);
    j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, j_bin_val->get_array_element(1, j_bin1));
    validate_large_array(j_bin1, 80000);
  }
  //allocator.reset();
}

TEST_F(TestJsonBin, large_object_array)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonArray array(&allocator);
  large_test_make_array(allocator, array);
  ObIJsonBase *wr = &array;
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, wr,
        ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin->json_type());

  // check val
  validate_large_array(j_bin, 80000);
  // make obj { "a" : array, "b" : "c" }
  {
    ObJsonObject obj(&allocator);
    ASSERT_EQ(OB_SUCCESS, obj.add("a", &array));
    ObJsonString temp("c", 1);
    ASSERT_EQ(OB_SUCCESS, obj.add("b", &temp));
    ObIJsonBase *j_base_new = &obj;
    ObIJsonBase *j_bin_val = NULL;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base_new,
          ObJsonInType::JSON_BIN, j_bin_val));
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_bin_val->json_type());
    ObIJsonBase *j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, j_bin_val->get_object_value("a", j_bin1));
    validate_large_array(j_bin1, 80000);
    j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, j_bin_val->get_object_value("b", j_bin1));
    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, j_bin1->print(buf, false));
    EXPECT_STREQ(buf.ptr(), "c");
    // large obj to raw binary and read raw binary again
    ObString raw_bin;
    j_bin_val->get_raw_binary(raw_bin, &allocator);
    ObJsonBin new_json_bin(raw_bin.ptr(), raw_bin.length(), &allocator);
    ASSERT_EQ(OB_SUCCESS, new_json_bin.reset_iter());
    ObIJsonBase *nnwr = &new_json_bin;
    j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, nnwr->get_object_value("a", j_bin1));
    validate_large_array(j_bin1, 80000);
    j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, nnwr->get_object_value("b", j_bin1));
    buf.reuse();
    ASSERT_EQ(OB_SUCCESS, j_bin1->print(buf, false));
    EXPECT_STREQ(buf.ptr(), "c");
  }
  //allocator.reset();
}

TEST_F(TestJsonBin, large_50_depth_array)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonArray array(&allocator);
  large_test_make_array(allocator, array);

  // make 50 depth array
  constexpr size_t depth = 50;
  ObJsonArray deeply_nested_array(&allocator);
  ObJsonArray *curr = &deeply_nested_array;
  for (int i = 1; i < depth; i++) {
    void *buf = allocator.alloc(sizeof(ObJsonArray));
    ObJsonArray *arr = new(buf)ObJsonArray(&allocator);
    ASSERT_EQ(OB_SUCCESS, curr->append(arr));
    curr = arr;
  }
  ASSERT_EQ(OB_SUCCESS, curr->append(&array));
  ObIJsonBase *wr = &deeply_nested_array;
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, wr,
        ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin->json_type());
  const ObIJsonBase *curr_wraper = j_bin;
  for (int i = 0; i < depth; i++) {
    ASSERT_EQ(1, curr_wraper->element_count());
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, curr_wraper->json_type());
    ObIJsonBase *j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, curr_wraper->get_array_element(0, j_bin1));
    curr_wraper = j_bin1;
  }
  validate_large_array(curr_wraper, 80000);
  //allocator.reset();
}

TEST_F(TestJsonBin, large_50_depth_object)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonArray array(&allocator);
  large_test_make_array(allocator, array);

  // make 50 depth array
  constexpr size_t depth = 50;
  ObJsonObject deeply_nested_obj(&allocator);
  ObJsonObject *curr = &deeply_nested_obj;
  for (int i = 1; i < depth; i++) {
    void *buf = allocator.alloc(sizeof(ObJsonObject));
    ObJsonObject *obj = new(buf)ObJsonObject(&allocator);
    ASSERT_EQ(OB_SUCCESS, curr->add("ob", obj));
    curr = obj;
  }
  ASSERT_EQ(OB_SUCCESS, curr->add("ob", &array));
  ObIJsonBase *wr = &deeply_nested_obj;
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, wr,
        ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_bin->json_type());
  const ObIJsonBase *curr_wraper = j_bin;
  for (int i = 0; i < depth; i++) {
    ASSERT_EQ(1, curr_wraper->element_count());
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, curr_wraper->json_type());
    ObIJsonBase *j_bin1 = NULL;
    ASSERT_EQ(OB_SUCCESS, curr_wraper->get_object_value("ob", j_bin1));
    curr_wraper = j_bin1;
  }
  validate_large_array(curr_wraper, 80000);
  //allocator.reset();
}

TEST_F(TestJsonBin, large_array_update)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonArray j_arr(&allocator);
  large_test_make_array(allocator, j_arr);
  ObIJsonBase *j_tree_arr = &j_arr;
  ObIJsonBase *j_bin_arr = NULL;
  // TREE -> BIN
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_arr,
      ObJsonInType::JSON_BIN, j_bin_arr));

  common::ObString j_text("{ \"greeting\" : [\"test\", 1.1, 2, -10, true, null], \"farewell\" : 2, \"json_text\" : 3 }");
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  ObString lkey("greeting");
  bin->lookup(lkey);

  // use large array update
  {
    bin->element(0);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_STRING), actual_type);
    ObString ele0("test");
    ObString data(bin->get_data_length(), bin->get_data());
    std::cout << "data:" << data << std::endl;
    ASSERT_EQ(0, ele0.compare(data));

    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->update(0, static_cast<ObJsonBin *>(j_bin_arr)));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(2, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);

    ASSERT_EQ(OB_SUCCESS, bin->element(0));
    ASSERT_EQ(80000, bin->element_count());
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->update(1, static_cast<ObJsonBin *>(j_bin_arr)));
    ASSERT_TRUE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(OB_SUCCESS, bin->element(1));
    ASSERT_EQ(80000, bin->element_count());
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_ARRAY), actual_type);
    bin->element(0);
    ASSERT_EQ(bin->element_count(), 80000);

    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    bin->element(1);
    ASSERT_EQ(bin->element_count(), 80000);
  }
  //allocator.reset();
}

TEST_F(TestJsonBin, large_array_insert)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonArray j_arr(&allocator);
  large_test_make_array(allocator, j_arr);
  ObIJsonBase *j_tree_arr = &j_arr;
  ObIJsonBase *j_bin_arr = NULL;
  // TREE -> BIN
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_arr,
      ObJsonInType::JSON_BIN, j_bin_arr));

  common::ObString j_text("{ \"greeting\" : [\"test\", 1.1, 2, -10, true, null], \"farewell\" : 2, \"json_text\" : 3 }");
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  ObString lkey("greeting");
  bin->lookup(lkey);

  // use large array update
  {
    bin->element(0);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_STRING), actual_type);
    ObString ele0("test");
    ObString data(bin->get_data_length(), bin->get_data());
    std::cout << "data:" << data << std::endl;
    ASSERT_EQ(0, ele0.compare(data));

    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->array_insert(0, static_cast<ObJsonBin *>(j_bin_arr)));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(2, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
    ASSERT_EQ(OB_SUCCESS, bin->element(0));
    ASSERT_EQ(80000, bin->element_count());
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->array_insert(1, static_cast<ObJsonBin *>(j_bin_arr)));
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
    ASSERT_TRUE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(OB_SUCCESS, bin->element(1));
    ASSERT_EQ(80000, bin->element_count());
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
    bin->element(0);
    ASSERT_EQ(bin->element_count(), 80000);

    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    bin->element(1);
    ASSERT_EQ(bin->element_count(), 80000);
  }
  //allocator.reset();
}

void parse_string_with_length(ObIAllocator *allocator, uint64_t size)
{
  char *str = reinterpret_cast<char*>(allocator->alloc(size + 1));
  MEMSET(str, 'o', size);
  str[size] = '\0';
  ObJsonString jstr(str, size);

  ObIJsonBase *j_base = &jstr;
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(allocator, j_base,
        ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin->json_type());
  uint64_t data_len = j_bin->get_data_length();
  const char *data = j_bin->get_data();
  ObString actual(data_len, data);
  ASSERT_EQ(size, data_len);
  ASSERT_EQ(0, MEMCMP(str, data, size));
  allocator->free(str);
}

TEST_F(TestJsonBin, test_string_len)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  parse_string_with_length(&allocator, 0);
  parse_string_with_length(&allocator, serialization::OB_MAX_V1B);
  parse_string_with_length(&allocator, serialization::OB_MAX_V2B);
  parse_string_with_length(&allocator, serialization::OB_MAX_V3B);
  parse_string_with_length(&allocator, serialization::OB_MAX_V4B);
  parse_string_with_length(&allocator, serialization::OB_MAX_V4B + 1);
  // ob_malloc only support 4G here
  // parse_string_with_length(&allocator, serialization::OB_MAX_V5B);
  // parse_string_with_length(&allocator, serialization::OB_MAX_V6B);
  // parse_string_with_length(&allocator, serialization::OB_MAX_V7B);
  // parse_string_with_length(&allocator, serialization::OB_MAX_V8B);
  // parse_string_with_length(&allocator, serialization::OB_MAX_V9B);
  // parse_string_with_length(&allocator, serialization::OB_MAX_V9B + 1);
}

TEST_F(TestJsonBin, test_binary_replace)
{
  common::ObString j_text("{ \"greeting\" : [\"test\", 1.1, 2, -10, true, null], \"farewell\" : 2, \"json_text\" : 3 }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  uint64_t root_member_count = bin->element_count();
  ASSERT_EQ(3, root_member_count);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());

  ObString lkey("greeting");
  bin->lookup(lkey);
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
  uint64_t sub_member_count = bin->element_count();
  ASSERT_EQ(6, sub_member_count);

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();
  // "test" --> "hahahahahah"
  {
    ASSERT_EQ(OB_SUCCESS, bin->element(0));
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_STRING), actual_type);
    ObString ele0("test");
    ObString data(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele0.compare(data));

    ObJsonString j_str("hahahahahah", strlen("hahahahahah"));
    ObIJsonBase *j_tree_str = &j_str;
    ObIJsonBase *j_bin_str = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
        ObJsonInType::JSON_BIN, j_bin_str));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

    ObString objstr;
    ASSERT_EQ(OB_SUCCESS, bin->raw_binary_at_iter(objstr));
    ObJsonBin tmp_bin(objstr.ptr(), objstr.length(), &allocator);
    ASSERT_EQ(OB_SUCCESS, tmp_bin.reset_iter());
    ObIJsonBase *j_tmp_bin = &tmp_bin;
    ObIJsonBase *j_bin_replace = NULL;
    ASSERT_EQ(OB_SUCCESS, bin->get_array_element(0, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_str));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(2, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    check_json_diff_valid(allocator, j_text, update_ctx, 1);
    bin->element(0);
    actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_STRING), actual_type);
    ObString new_ele("hahahahahah");
    ObString data1(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, new_ele.compare(data1));
  }

  // "test" --> "hahahahahah"
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->element(0));
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_STRING), actual_type);
    ObString ele0("hahahahahah");
    ObString data(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele0.compare(data));

    ObJsonString j_str("hahahahahahaaaa", strlen("hahahahahahaaaa"));
    ObIJsonBase *j_tree_str = &j_str;
    ObIJsonBase *j_bin_str = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
        ObJsonInType::JSON_BIN, j_bin_str));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

    ObString objstr;
    ASSERT_EQ(OB_SUCCESS, bin->raw_binary_at_iter(objstr));
    ObJsonBin tmp_bin(objstr.ptr(), objstr.length(), &allocator);
    ASSERT_EQ(OB_SUCCESS, tmp_bin.reset_iter());
    ObIJsonBase *j_tmp_bin = &tmp_bin;
    ObIJsonBase *j_bin_replace = NULL;
    ASSERT_EQ(OB_SUCCESS, bin->get_array_element(0, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_str));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(4, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    bin->element(0);
    actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_STRING), actual_type);
    ObString new_ele("hahahahahahaaaa");
    ObString data1(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, new_ele.compare(data1));
  }
  // 1.1 --> 3.1415
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    bin->element(1);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_DOUBLE), actual_type);
    ASSERT_FLOAT_EQ(1.1, bin->get_double());

    ObJsonDouble j_double(3.1415);
    ObIJsonBase *j_tree_double = &j_double;
    ObIJsonBase *j_bin_double = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_double,
        ObJsonInType::JSON_BIN, j_bin_double));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

    ObString objstr;
    ASSERT_EQ(0, bin->raw_binary_at_iter(objstr));
    ObJsonBin tmp_bin(objstr.ptr(), objstr.length(), &allocator);
    ASSERT_EQ(OB_SUCCESS, tmp_bin.reset_iter());
    ObIJsonBase *j_tmp_bin = &tmp_bin;
    ObIJsonBase *j_bin_replace = NULL;
    ASSERT_EQ(OB_SUCCESS, bin->get_array_element(1, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_double));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(5, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    bin->element(1);
    actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_DOUBLE), actual_type);
    ASSERT_FLOAT_EQ(3.1415, bin->get_double());
  }
  // 2 ---> -655666
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    bin->element(2);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_INT), actual_type);
    ASSERT_EQ(2, bin->get_uint());

    ObJsonInt j_int(-655666);
    ObIJsonBase *j_tree_int = &j_int;
    ObIJsonBase *j_bin_int = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_int,
        ObJsonInType::JSON_BIN, j_bin_int));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

    ObString objstr;
    ASSERT_EQ(0, bin->raw_binary_at_iter(objstr));
    ObJsonBin tmp_bin(objstr.ptr(), objstr.length(), &allocator);
    ASSERT_EQ(OB_SUCCESS, tmp_bin.reset_iter());
    ObIJsonBase *j_tmp_bin = &tmp_bin;
    ObIJsonBase *j_bin_replace = NULL;
    ASSERT_EQ(OB_SUCCESS, bin->get_array_element(2, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_int));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(7, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    bin->element(2);
    actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_INT), actual_type);
    ASSERT_EQ(-655666, bin->get_int());
  }
  // -10 ---> 655666
  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    bin->element(3);
    uint8_t actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_INT), actual_type);
    ASSERT_EQ(-10, bin->get_int());

    ObJsonUint j_uint(655666);
    ObIJsonBase *j_tree_uint = &j_uint;
    ObIJsonBase *j_bin_uint = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_uint,
        ObJsonInType::JSON_BIN, j_bin_uint));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

    ObString objstr;
    ASSERT_EQ(0, bin->raw_binary_at_iter(objstr));
    ObJsonBin tmp_bin(objstr.ptr(), objstr.length(), &allocator);
    ASSERT_EQ(OB_SUCCESS, tmp_bin.reset_iter());
    ObIJsonBase *j_tmp_bin = &tmp_bin;
    ObIJsonBase *j_bin_replace = NULL;
    ASSERT_EQ(OB_SUCCESS, bin->get_array_element(3, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_uint));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(9, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    bin->element(3);
    actual_type = (static_cast<uint8>(bin->json_type()) & 0x7F);
    ASSERT_EQ(static_cast<uint8>(ObJsonNodeType::J_UINT), actual_type);
    ASSERT_EQ(655666, bin->get_uint());
  }
}

TEST_F(TestJsonBin, test_double_nan)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  double value = sqrt(-100);
  std::cout << "origin value: "<< value << std::endl;
  ObJsonDouble left_val(value);
  ObIJsonBase *left_base = &left_val;
  ObIJsonBase *new_bin = NULL;
  ASSERT_EQ(OB_INVALID_NUMERIC, ObJsonBaseFactory::transform(&allocator, left_base,
      ObJsonInType::JSON_BIN, new_bin));
}

TEST_F(TestJsonBin, test_bin_construct_from_tree)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  {
    ObString new_value_str("abcd");
    ObJsonString new_value_node(new_value_str.ptr(), new_value_str.length());
    ObIJsonBase *j_bin_str = nullptr;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(
        &allocator,
        &new_value_node,
        ObJsonInType::JSON_BIN,
        j_bin_str));
    ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_str->json_type());
    ASSERT_TRUE(j_bin_str->is_bin());
  }

  {
    ObJsonNull j_null(true);
    ObIJsonBase *j_tree_null = &j_null;
    ObIJsonBase *j_bin_null = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_null,
        ObJsonInType::JSON_BIN, j_bin_null));
    ASSERT_EQ(ObJsonNodeType::J_NULL, j_bin_null->json_type());
    ASSERT_TRUE(j_bin_null->is_bin());
    //ASSERT_EQ(1, j_bin_null);
  }

  {
    ObJsonInt j_int(-655666);
    ObIJsonBase *j_tree_int = &j_int;
    ObIJsonBase *j_bin_int = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_int,
        ObJsonInType::JSON_BIN, j_bin_int));
    ASSERT_EQ(ObJsonNodeType::J_INT, j_bin_int->json_type());
    ASSERT_TRUE(j_bin_int->is_bin());
  }

  {
    ObString data("my opaque");
    ObJsonOpaque j_opaque(data, ObObjType::ObVarcharType);
    ObIJsonBase *j_tree_opaque = &j_opaque;
    ObIJsonBase *j_bin_opaque = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_opaque,
        ObJsonInType::JSON_BIN, j_bin_opaque));
    ASSERT_EQ(ObJsonNodeType::J_OPAQUE, j_bin_opaque->json_type());
    ASSERT_TRUE(j_bin_opaque->is_bin());
    ASSERT_EQ(ObObjType::ObVarcharType, j_bin_opaque->field_type());
    ASSERT_EQ("my opaque", std::string(j_bin_opaque->get_data(), j_bin_opaque->get_data_length()));

    ObIJsonBase *j_tree_opaque_2 = NULL;
    // TREE -> BIN
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_bin_opaque,
        ObJsonInType::JSON_TREE, j_tree_opaque_2));
    ASSERT_EQ(ObJsonNodeType::J_OPAQUE, j_tree_opaque_2->json_type());
    ASSERT_TRUE(j_tree_opaque_2->is_tree());
    ASSERT_EQ("my opaque", std::string(j_tree_opaque_2->get_data(), j_tree_opaque_2->get_data_length()));
  }
}


TEST_F(TestJsonBin, test_bin_construct_from_bin)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  {
    common::ObString j_text("{ \"my_str\" : \"hello my_str\", \"my_inline_number\" : 2, \"my_null\" : null, \"my_number\" : 655450}");
    ObArenaAllocator allocator(ObModIds::TEST);
    ObIJsonBase *j_bin = NULL;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    common::ObString result;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_bin->json_type());
    ASSERT_TRUE(j_bin->is_bin());

    ObIJsonBase *child_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_object_value(ObString("my_inline_number"), child_bin));
    ASSERT_EQ(2, child_bin->get_int());
    ObString raw_bin;
    ASSERT_EQ(OB_SUCCESS, child_bin->get_raw_binary(raw_bin, &allocator));
  }

  {
    common::ObString j_text("[\"hello my_str\", 2, null, 655450]");
    ObArenaAllocator allocator(ObModIds::TEST);
    ObIJsonBase *j_bin = NULL;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    common::ObString result;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin->json_type());
    ASSERT_TRUE(j_bin->is_bin());
  }
}

TEST_F(TestJsonBin, test_bin_construct_from_nested_bin)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  {
    common::ObString j_text("{\"my_obj\" : { \"my_str\" : \"hello my_str\", \"my_inline_number\" : 2, \"my_null\" : null, \"my_number\" : 655450}}");
    ObArenaAllocator allocator(ObModIds::TEST);
    ObIJsonBase *j_bin = NULL;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    common::ObString result;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_bin->json_type());
    ASSERT_TRUE(j_bin->is_bin());
  }

  {
    common::ObString j_text("{\"my_array\" : [\"hello my_str\", 2, null, 655450]}");
    ObArenaAllocator allocator(ObModIds::TEST);
    ObIJsonBase *j_bin = NULL;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    common::ObString result;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_bin->json_type());
    ASSERT_TRUE(j_bin->is_bin());
  }
}

TEST_F(TestJsonBin, test_empty_object_seek)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  {
    ObString path_str("$.\"data\".\"a\"");
    ObJsonPath test_path(path_str, &allocator);
    ASSERT_EQ(OB_SUCCESS, test_path.parse_path());

    common::ObString j_text("{\"data\" : {}}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        ObJsonBaseFactory::get_json_base(&allocator, j_text,
            ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    common::ObString result;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_bin->json_type());
    ASSERT_TRUE(j_bin->is_bin());

    ObJsonSeekResult hit;
    int cnt = test_path.path_node_cnt();
    ASSERT_EQ(OB_SUCCESS, j_bin->seek(test_path, cnt, false, false, hit));
    ASSERT_EQ(0, hit.size());
  }
}

TEST_F(TestJsonBin, test_empty_array_seek)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  {
    ObString path_str("$.\"data[0]\"");
    ObJsonPath test_path(path_str, &allocator);
    ASSERT_EQ(OB_SUCCESS, test_path.parse_path());

    common::ObString j_text("{\"data\" : []}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        ObJsonBaseFactory::get_json_base(&allocator, j_text,
            ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    common::ObString result;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_bin->json_type());
    ASSERT_TRUE(j_bin->is_bin());

    ObJsonSeekResult hit;
    int cnt = test_path.path_node_cnt();
    ASSERT_EQ(OB_SUCCESS, j_bin->seek(test_path, cnt, false, false, hit));
    ASSERT_EQ(0, hit.size());
  }
}


TEST_F(TestJsonBin, test_array_array)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  {
    const char *str = "[[\"sfsdfasdfasdfasdfasdfasdfasdfasdf\", \"sdfsdfsdfsdfsdfsdsdfsfsdfsdfsdfsd\"], [\"sfsdfasdfasdfasdfasdfasdfasdfasdf\", \"sdfsdfsdfsdfsdfsdsdfsfsdfsdfsdfsd\"], 217]";
    common::ObString j_text(str);
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        ObJsonBaseFactory::get_json_base(&allocator, j_text,
            ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    common::ObString result;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin->json_type());
    ASSERT_TRUE(j_bin->is_bin());

    ObJsonBuffer print_buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, j_bin->print(print_buf, false));
    ASSERT_EQ(std::string(j_text.ptr(), j_text.length()), std::string(print_buf.string().ptr(), print_buf.string().length()));

    ObString raw_bin;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_bin, &allocator));

    ObJsonBin new_bin(raw_bin.ptr(), raw_bin.length(), &allocator);
    ASSERT_EQ(OB_SUCCESS, new_bin.reset_iter());
    print_buf.reset();
    ASSERT_EQ(OB_SUCCESS, new_bin.print(print_buf, false));
    ASSERT_EQ(std::string(j_text.ptr(), j_text.length()), std::string(print_buf.string().ptr(), print_buf.string().length()));

  }
}


TEST_F(TestJsonBin, test_bin_insert)
{
  common::ObString j_text("{ \"greeting\" : [\"test\", 1.1, 2, -10, true, null], \"farewell\" : 2, \"json_text\" : 3 }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin& bin = *static_cast<ObJsonBin*>(j_bin); //(result.ptr(), result.length(), &allocator);
  bin.set_seek_flag(false);
  ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
  ASSERT_EQ(3, bin.element_count());
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin.json_type());

  ObString lkey("greeting");
  bin.lookup(lkey);
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin.json_type());
  ASSERT_EQ(6, bin.element_count());

  ObString new_value_str("abcd");
  ObJsonString new_value_node(new_value_str.ptr(), new_value_str.length());
  ObIJsonBase *j_bin_str = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(
      &allocator,
      &new_value_node,
      ObJsonInType::JSON_BIN,
      j_bin_str));
  ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_str->json_type());
  ASSERT_TRUE(j_bin_str->is_bin());

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, &bin));
  ObJsonBinUpdateCtx &update_ctx = *bin.get_update_ctx();

  ASSERT_EQ(OB_SUCCESS, bin.array_insert(3, j_bin_str));
  ASSERT_FALSE(update_ctx.is_rebuild_all_);
  ASSERT_EQ(2, update_ctx.binary_diffs_.count());
  check_diff_valid(allocator, result, update_ctx);
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin.json_type());
  ASSERT_EQ(7, bin.element_count());
  ASSERT_EQ(OB_SUCCESS, bin.element(3));
  ASSERT_EQ(ObJsonNodeType::J_STRING, bin.json_type());
  ASSERT_EQ(std::string(new_value_str.ptr(), new_value_str.length()), std::string(bin.get_data(), bin.get_data_length()));
}

TEST_F(TestJsonBin, test_inline_replace)
{
  common::ObString j_text("{ \"name\" : \"Mike\", \"age\" : 30, \"ids\" : [1, 200, 3 , 400, true, 100.1, null, 20000, false] }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  uint64_t root_member_count = bin->element_count();
  ASSERT_EQ(3, root_member_count);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  {
    ObString name_key("name");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(name_key));
    ASSERT_EQ(ObJsonNodeType::J_STRING, bin->json_type());
    ObString ele0("Mike");
    ObString data(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele0.compare(data));

    ObJsonNull j_null(true);
    ObIJsonBase *j_tree_null = &j_null;
    ObIJsonBase *j_bin_null = nullptr;

    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_null,
        ObJsonInType::JSON_BIN, j_bin_null));
    ASSERT_EQ(ObJsonNodeType::J_NULL, j_bin_null->json_type());
    ASSERT_TRUE(j_bin_null->is_bin());

    ObIJsonBase *j_bin_replace = nullptr;
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->get_object_value(name_key, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_null));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(1, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    ASSERT_EQ(OB_SUCCESS, bin->lookup(name_key));
    ASSERT_EQ(ObJsonNodeType::J_NULL, bin->json_type());
  }

  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ObString age_key("age");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(age_key));
    ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
    ASSERT_EQ(30, bin->get_int());

    ObJsonInt j_int(35);
    ObIJsonBase *j_tree_int = &j_int;
    ObIJsonBase *j_bin_int = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_int,
        ObJsonInType::JSON_BIN, j_bin_int));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

    ObIJsonBase *j_bin_replace = nullptr;
    ASSERT_EQ(OB_SUCCESS, bin->get_object_value(age_key, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_int));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(2, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    ASSERT_EQ(OB_SUCCESS, bin->lookup(age_key));
    ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
    ASSERT_EQ(j_int.get_int(), bin->get_int());
  }

  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ObString ids_key("ids");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(ids_key));
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
    ASSERT_EQ(9, bin->element_count());

    {
      ASSERT_EQ(OB_SUCCESS, bin->element(0));
      ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
      ASSERT_EQ(1, bin->get_int());

      ObJsonInt j_int(15);
      ObIJsonBase *j_tree_int = &j_int;
      ObIJsonBase *j_bin_int = NULL;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_int,
          ObJsonInType::JSON_BIN, j_bin_int));
      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

      ObIJsonBase *j_bin_replace = nullptr;
      ASSERT_EQ(OB_SUCCESS, bin->get_array_element(0, j_bin_replace));
      ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_int));
      ASSERT_FALSE(update_ctx.is_rebuild_all_);
      ASSERT_EQ(3, update_ctx.binary_diffs_.count());
      check_diff_valid(allocator, result, update_ctx);
      ASSERT_EQ(OB_SUCCESS, bin->element(0));
      ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
      ASSERT_EQ(j_int.get_int(), bin->get_int());
    }
  }
}

TEST_F(TestJsonBin, test_inplace_replace)
{
  common::ObString j_text("{ \"name\" : \"Mike\", \"money\" : 3000.5, \"ids\" : [1, 200, 3 , 400, true, 100.1, null, 20000, false] }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  uint64_t root_member_count = bin->element_count();
  ASSERT_EQ(3, root_member_count);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  {
    ObString name_key("name");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(name_key));
    ASSERT_EQ(ObJsonNodeType::J_STRING, bin->json_type());
    ObString ele0("Mike");
    ObString data(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele0.compare(data));

    ObJsonString j_str("John", strlen("John"));
    ObIJsonBase *j_tree_str = &j_str;
    ObIJsonBase *j_bin_str = nullptr;

    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
        ObJsonInType::JSON_BIN, j_bin_str));
    ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_str->json_type());
    ASSERT_TRUE(j_bin_str->is_bin());

    ObIJsonBase *j_bin_replace = nullptr;
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->get_object_value(name_key, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_str));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(1, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    ASSERT_EQ(OB_SUCCESS, bin->lookup(name_key));
    ASSERT_EQ(ObJsonNodeType::J_STRING, bin->json_type());
    ObString ele1("John");
    ObString data1(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele1.compare(data1));
  }

  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ObString money_key("money");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(money_key));
    ASSERT_EQ(ObJsonNodeType::J_DOUBLE, bin->json_type());
    ASSERT_FLOAT_EQ(3000.5, bin->get_double());

    ObJsonDouble j_double(41234.5);
    ObIJsonBase *j_tree_double = &j_double;
    ObIJsonBase *j_bin_double = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_double,
        ObJsonInType::JSON_BIN, j_bin_double));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

    ObIJsonBase *j_bin_replace = nullptr;
    ASSERT_EQ(OB_SUCCESS, bin->get_object_value(money_key, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_double));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(2, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    ASSERT_EQ(OB_SUCCESS, bin->lookup(money_key));
    ASSERT_EQ(ObJsonNodeType::J_DOUBLE, bin->json_type());
    ASSERT_FLOAT_EQ(j_double.get_double(), bin->get_double());
  }

  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ObString ids_key("ids");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(ids_key));
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
    ASSERT_EQ(9, bin->element_count());

    {
      ASSERT_EQ(OB_SUCCESS, bin->element(7));
      ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
      ASSERT_EQ(20000, bin->get_int());

      ObJsonInt j_int(300);
      ObIJsonBase *j_tree_int = &j_int;
      ObIJsonBase *j_bin_int = NULL;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_int,
          ObJsonInType::JSON_BIN, j_bin_int));
      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

      ObIJsonBase *j_bin_replace = nullptr;
      ASSERT_EQ(OB_SUCCESS, bin->get_array_element(7, j_bin_replace));
      ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_int));
      ASSERT_FALSE(update_ctx.is_rebuild_all_);
      ASSERT_EQ(3, update_ctx.binary_diffs_.count());
      check_diff_valid(allocator, result, update_ctx);
      ASSERT_EQ(OB_SUCCESS, bin->element(7));
      ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
      ASSERT_EQ(j_int.get_int(), bin->get_int());
    }
  }
}

TEST_F(TestJsonBin, test_append_replace)
{
  common::ObString j_text("{ \"name\" : \"Mike\", \"age\" : 30, \"ids\" : [1, 200, 3 , 400, true, 100.1, null, 20000, false] }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  uint64_t root_member_count = bin->element_count();
  ASSERT_EQ(3, root_member_count);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  {
    ObString name_key("name");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(name_key));
    ASSERT_EQ(ObJsonNodeType::J_STRING, bin->json_type());
    ObString ele0("Mike");
    ObString data(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele0.compare(data));

    ObJsonString j_str("JohnSnow", strlen("JohnSnow"));
    ObIJsonBase *j_tree_str = &j_str;
    ObIJsonBase *j_bin_str = nullptr;

    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
        ObJsonInType::JSON_BIN, j_bin_str));
    ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_str->json_type());
    ASSERT_TRUE(j_bin_str->is_bin());

    ObIJsonBase *j_bin_replace = nullptr;
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->get_object_value(name_key, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_str));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(2, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    ASSERT_EQ(OB_SUCCESS, bin->lookup(name_key));
    ASSERT_EQ(ObJsonNodeType::J_STRING, bin->json_type());
    ObString ele1("JohnSnow");
    ObString data1(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele1.compare(data1));
  }

  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ObString age_key("age");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(age_key));
    ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
    ASSERT_FLOAT_EQ(30, bin->get_int());

    ObJsonInt j_int(3000);
    ObIJsonBase *j_tree_int = &j_int;
    ObIJsonBase *j_bin_int = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_int,
        ObJsonInType::JSON_BIN, j_bin_int));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

    ObIJsonBase *j_bin_replace = nullptr;
    ASSERT_EQ(OB_SUCCESS, bin->get_object_value(age_key, j_bin_replace));
    ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_int));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(4, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    ASSERT_EQ(OB_SUCCESS, bin->lookup(age_key));
    ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
    ASSERT_FLOAT_EQ(j_int.get_int(), bin->get_int());
  }

  {
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ObString ids_key("ids");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(ids_key));
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
    ASSERT_EQ(9, bin->element_count());

    {
      ASSERT_EQ(OB_SUCCESS, bin->element(4));
      ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, bin->json_type());
      ASSERT_TRUE(bin->get_boolean());

      ObJsonInt j_int(300);
      ObIJsonBase *j_tree_int = &j_int;
      ObIJsonBase *j_bin_int = NULL;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_int,
          ObJsonInType::JSON_BIN, j_bin_int));
      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());

      ObIJsonBase *j_bin_replace = nullptr;
      ASSERT_EQ(OB_SUCCESS, bin->get_array_element(4, j_bin_replace));
      ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_int));
      ASSERT_FALSE(update_ctx.is_rebuild_all_);
      ASSERT_EQ(6, update_ctx.binary_diffs_.count());
      check_diff_valid(allocator, result, update_ctx);
      ASSERT_EQ(OB_SUCCESS, bin->element(4));
      ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
      ASSERT_EQ(j_int.get_int(), bin->get_int());
    }
  }
}


TEST_F(TestJsonBin, test_remove_diff)
{
  common::ObString j_text("{ \"name\" : \"Mike\", \"age\" : 30, \"ids\" : [1, 200, 3 , 400, true, 100.1, null, 20000, false] }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  uint64_t root_member_count = bin->element_count();
  ASSERT_EQ(3, root_member_count);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  {
    ObString name_key("name");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(name_key));
    ASSERT_EQ(ObJsonNodeType::J_STRING, bin->json_type());
    ObString ele0("Mike");
    ObString data(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele0.compare(data));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->remove(name_key));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(1, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, bin->lookup(name_key));
  }

  {
    ObString age_key("age");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(age_key));
    ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
    ASSERT_FLOAT_EQ(30, bin->get_int());
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    ASSERT_EQ(OB_SUCCESS, bin->remove(age_key));
    ASSERT_FALSE(update_ctx.is_rebuild_all_);
    ASSERT_EQ(2, update_ctx.binary_diffs_.count());
    check_diff_valid(allocator, result, update_ctx);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, bin->lookup(age_key));
  }

  {
    ObString ids_key("ids");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(ids_key));
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
    ASSERT_EQ(9, bin->element_count());

    {
      ASSERT_EQ(OB_SUCCESS, bin->element(4));
      ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, bin->json_type());
      ASSERT_TRUE(bin->get_boolean());
      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
      ASSERT_EQ(OB_SUCCESS, bin->remove(4));
      ASSERT_FALSE(update_ctx.is_rebuild_all_);
      ASSERT_EQ(3, update_ctx.binary_diffs_.count());
      check_diff_valid(allocator, result, update_ctx);
      ASSERT_EQ(OB_SUCCESS, bin->element(4));
      ASSERT_EQ(ObJsonNodeType::J_DOUBLE, bin->json_type());
      ASSERT_EQ(100.1, bin->get_double());
    }

    {
      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
      ASSERT_EQ(OB_SUCCESS, bin->element(4));
      ASSERT_EQ(ObJsonNodeType::J_DOUBLE, bin->json_type());
      ASSERT_EQ(100.1, bin->get_double());
      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
      ASSERT_EQ(OB_SUCCESS, bin->remove(4));
      ASSERT_FALSE(update_ctx.is_rebuild_all_);
      ASSERT_EQ(4, update_ctx.binary_diffs_.count());
      check_diff_valid(allocator, result, update_ctx);
      ASSERT_EQ(OB_SUCCESS, bin->element(4));
      ASSERT_EQ(ObJsonNodeType::J_NULL, bin->json_type());
    }

  }
}

TEST_F(TestJsonBin, test_insert_diff)
{
  common::ObString j_text("{ \"name\" : \"Mike\", \"age\" : 30, \"ids\" : [1, 200, 3 , 400, true, 100.1, null, 20000, false] }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  uint64_t root_member_count = bin->element_count();
  ASSERT_EQ(3, root_member_count);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  {
    ObString sex_key("sex");
    ObJsonString j_str("male", strlen("male"));
    ObIJsonBase *j_tree_str = &j_str;
    ObIJsonBase *j_bin_str = nullptr;

    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
        ObJsonInType::JSON_BIN, j_bin_str));
    ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_str->json_type());
    ASSERT_TRUE(j_bin_str->is_bin());

    ASSERT_EQ(OB_SUCCESS, bin->object_add(sex_key, j_bin_str));
    ASSERT_EQ(OB_SUCCESS, bin->lookup(sex_key));
    ASSERT_EQ(ObJsonNodeType::J_STRING, bin->json_type());
    ObString ele0("male");
    ObString data(bin->get_data_length(), bin->get_data());
    ASSERT_EQ(0, ele0.compare(data));
    ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    //LOG_INFO("diff", K(update_ctx.cur_node_diff_));
    ASSERT_TRUE(update_ctx.is_rebuild_all_);
    update_ctx.is_rebuild_all_ = false;
    ObString curr_data;
    ASSERT_EQ(OB_SUCCESS, update_ctx.current_data(curr_data));
    ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator, curr_data, result));
  }

  {
    ObString ids_key("ids");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(ids_key));
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());
    ASSERT_EQ(9, bin->element_count());

    {
      ASSERT_EQ(OB_SUCCESS, bin->element(0));
      ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
      ASSERT_EQ(1, bin->get_int());

      ObJsonInt j_int(300);
      ObIJsonBase *j_tree_int = &j_int;
      ObIJsonBase *j_bin_int = NULL;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_int,
          ObJsonInType::JSON_BIN, j_bin_int));

      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
      ASSERT_EQ(OB_SUCCESS, bin->array_insert(0, j_bin_int));

      ASSERT_EQ(OB_SUCCESS, bin->element(0));
      ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
      ASSERT_EQ(j_int.get_int(), bin->get_int());
      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
      //LOG_INFO("diff", K(update_ctx.cur_node_diff_));
      ASSERT_EQ(2, update_ctx.binary_diffs_.count());
      ASSERT_FALSE(update_ctx.is_rebuild_all_);
      check_diff_valid(allocator, result, update_ctx);
    }
    ASSERT_EQ(10, bin->element_count());
    {
      ASSERT_EQ(OB_SUCCESS, bin->element(3));
      ASSERT_EQ(ObJsonNodeType::J_INT, bin->json_type());
      ASSERT_EQ(3, bin->get_int());

      ObJsonString j_str("weight", strlen("weight"));
      ObIJsonBase *j_tree_str = &j_str;
      ObIJsonBase *j_bin_str = nullptr;

      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
          ObJsonInType::JSON_BIN, j_bin_str));
      ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_str->json_type());
      ASSERT_TRUE(j_bin_str->is_bin());

      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
      ASSERT_EQ(OB_SUCCESS, bin->array_insert(3, j_bin_str));

      ASSERT_EQ(OB_SUCCESS, bin->element(3));
      ASSERT_EQ(ObJsonNodeType::J_STRING, bin->json_type());
      ObString ele0("weight");
      ObString data(bin->get_data_length(), bin->get_data());
      ASSERT_EQ(0, ele0.compare(data));
      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
      //LOG_INFO("diff", K(update_ctx.cur_node_diff_));
      ASSERT_EQ(4, update_ctx.binary_diffs_.count());
      ASSERT_FALSE(update_ctx.is_rebuild_all_);
      check_diff_valid(allocator, result, update_ctx);
    }
    ASSERT_EQ(11, bin->element_count());
    {
      ASSERT_EQ(OB_SUCCESS, bin->element(10));
      ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, bin->json_type());
      ASSERT_FALSE(bin->get_boolean());

      ObJsonString j_str("weight2", strlen("weight2"));
      ObIJsonBase *j_tree_str = &j_str;
      ObIJsonBase *j_bin_str = nullptr;

      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
          ObJsonInType::JSON_BIN, j_bin_str));
      ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_str->json_type());
      ASSERT_TRUE(j_bin_str->is_bin());

      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
      ASSERT_EQ(OB_SUCCESS, bin->array_insert(10, j_bin_str));

      ASSERT_EQ(OB_SUCCESS, bin->element(10));
      ASSERT_EQ(ObJsonNodeType::J_STRING, bin->json_type());
      ObString ele0("weight2");
      ObString data(bin->get_data_length(), bin->get_data());
      ASSERT_EQ(0, ele0.compare(data));
      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
      //LOG_INFO("diff", K(update_ctx.cur_node_diff_));
      ASSERT_EQ(6, update_ctx.binary_diffs_.count());
      ASSERT_FALSE(update_ctx.is_rebuild_all_);
      check_diff_valid(allocator, result, update_ctx);
    }
    ASSERT_EQ(12, bin->element_count());
    {
      ASSERT_EQ(OB_SUCCESS, bin->element(11));
      ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, bin->json_type());
      ASSERT_FALSE(bin->get_boolean());
    }
  }
}

TEST_F(TestJsonBin, test_replace_parent_and_child)
{
  common::ObString j_text("{ \"name\" : \"Mike\", \"age\" : 30, \"order\" : {\"stcok\" : 1000, \"name\": \"myxxxx\"} }");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_bin = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  // root
  ObJsonBin *bin = static_cast<ObJsonBin *>(j_bin);
  bin->set_seek_flag(false);
  uint64_t root_member_count = bin->element_count();
  ASSERT_EQ(3, root_member_count);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());

  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  {
    ObString order_key("order");
    ASSERT_EQ(OB_SUCCESS, bin->lookup(order_key));
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, bin->json_type());
    ASSERT_EQ(2, bin->element_count());

    {
      ObJsonString j_str("weightxxxxx", strlen("weightxxxxx"));
      ObIJsonBase *j_tree_str = &j_str;
      ObIJsonBase *j_bin_str = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
          ObJsonInType::JSON_BIN, j_bin_str));
      ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_str->json_type());
      ASSERT_TRUE(j_bin_str->is_bin());
      ObIJsonBase *j_bin_replace = nullptr;
      ASSERT_EQ(OB_SUCCESS, bin->get_object_value(ObString("name"), j_bin_replace));
      ASSERT_EQ(OB_SUCCESS, bin->replace(j_bin_replace, j_bin_str));
      ASSERT_FALSE(update_ctx.is_rebuild_all_);
      ASSERT_EQ(2, update_ctx.binary_diffs_.count());
      check_diff_valid(allocator, result, update_ctx);
      ASSERT_EQ(OB_SUCCESS, bin->lookup(ObString("name")));
      ASSERT_EQ(ObJsonNodeType::J_STRING, bin->json_type());
      ObString ele0("weightxxxxx");
      ObString data(bin->get_data_length(), bin->get_data());
      ASSERT_EQ(0, ele0.compare(data));
      ASSERT_EQ(OB_SUCCESS, bin->move_parent_iter());
    }
  }
}

TEST_F(TestJsonBin, test_decimal)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  number::ObNumber res_nmb;
  ObString nmb_str("3.14");
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  ASSERT_EQ(OB_SUCCESS, res_nmb.from_sci_opt(nmb_str.ptr(), nmb_str.length(), allocator, &res_precision, &res_scale));

  ObJsonDecimal j_dec(res_nmb, res_precision, res_scale);
  ObIJsonBase *j_tree_dec = &j_dec;
  ObIJsonBase *j_bin_dec = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_dec,
      ObJsonInType::JSON_BIN, j_bin_dec));

  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin_dec->get_raw_binary(result, &allocator));

  ObJsonBin bin(result.ptr(), result.length(), &allocator);
  ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
}

TEST_F(TestJsonBin, test_double)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonDouble j_double(3.1415);
  ObIJsonBase *j_tree_double = &j_double;
  ObIJsonBase *j_bin_double = NULL;
  // TREE -> BIN
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_double,
      ObJsonInType::JSON_BIN, j_bin_double));
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin_double->get_raw_binary(result, &allocator));

  ObJsonBin bin(result.ptr(), result.length(), &allocator);
  ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
}

TEST_F(TestJsonBin, test_get_parent)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  {
    common::ObString j_text("{ \"name\" : \"Mike\", \"age\" : 30, \"order\" : {\"stcok\" : 100, \"name\": \"myxxxx\"} }");
    ObArenaAllocator allocator(ObModIds::TEST);
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    common::ObString result;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
    ObJsonBin *bin = static_cast<ObJsonBin*>(j_bin);
    bin->set_seek_flag(false);
    ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
    ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

    common::ObString path_str("$.order.stcok");
    ObJsonPath test_path(path_str, &allocator);
    ASSERT_EQ(OB_SUCCESS, test_path.parse_path());

    ObJsonSeekResult hit;
    int cnt = test_path.path_node_cnt();
    ASSERT_EQ(OB_SUCCESS, j_bin->seek(test_path, cnt, false, false, hit));
    ASSERT_EQ(1, hit.size());
    ObIJsonBase *j_child_bin = hit[0];
    ObIJsonBase *j_parent_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, j_child_bin->get_parent(j_parent_bin));
    ASSERT_NE(nullptr, j_parent_bin);
    ASSERT_TRUE(j_parent_bin->is_bin());

    ObJsonString j_str("weightxxxxx", strlen("weightxxxxx"));
    ObIJsonBase *j_tree_str = &j_str;
    ObIJsonBase *j_bin_str = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_str,
        ObJsonInType::JSON_BIN, j_bin_str));
    ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_str->json_type());
    ASSERT_TRUE(j_bin_str->is_bin());
    ASSERT_EQ(OB_SUCCESS, j_parent_bin->replace(j_child_bin, j_bin_str));
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
  }

  {
    common::ObString j_text("{ \"name\" : \"Mike\", \"age\" : 30, \"order\" : {\"stcok\" : 100, \"name\": \"myxxxx\"} }");
    ObArenaAllocator allocator(ObModIds::TEST);
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    common::ObString result;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
    ObJsonBin *bin = static_cast<ObJsonBin*>(j_bin);
    bin->set_seek_flag(false);
    ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
    ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

    common::ObString path_str("$.age");
    ObJsonPath test_path(path_str, &allocator);
    ASSERT_EQ(OB_SUCCESS, test_path.parse_path());

    ObJsonSeekResult hit;
    int cnt = test_path.path_node_cnt();
    ASSERT_EQ(OB_SUCCESS, j_bin->seek(test_path, cnt, false, false, hit));
    ASSERT_EQ(1, hit.size());
    ObIJsonBase *j_child_bin = hit[0];
    ObIJsonBase *j_parent_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, j_child_bin->get_parent(j_parent_bin));
    ASSERT_NE(nullptr, j_parent_bin);
    ASSERT_TRUE(j_parent_bin->is_bin());

    ObJsonInt j_int(200);
    ObIJsonBase *j_tree_int = &j_int;
    ObIJsonBase *j_bin_int = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree_int,
        ObJsonInType::JSON_BIN, j_bin_int));
    ASSERT_TRUE(j_bin_int->is_bin());
    ASSERT_EQ(OB_SUCCESS, j_parent_bin->replace(j_child_bin, j_bin_int));
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
  }
}

TEST_F(TestJsonBin, test_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonObject tree(&allocator);
  ObJsonBin bin(&allocator);
  // ObJsonNull j_null_true(true);
  ObJsonNull j_null_false(false);
  // ASSERT_EQ(OB_SUCCESS, tree.add("j_null_true", &j_null_true));
  ASSERT_EQ(OB_SUCCESS, tree.add("j_null_false", &j_null_false));

  ObJsonBoolean j_bool_false(false);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_bool_false", &j_bool_false));
  ObJsonBoolean j_bool_true(false);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_bool_true", &j_bool_true));

  ObJsonInt j_int_0(0);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_0", &j_int_0));
  ObJsonInt j_int_1(1);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_1", &j_int_1));
  ObJsonInt j_int_2(3);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_2", &j_int_2));
  ObJsonInt j_int_3(100);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_3", &j_int_3));
  ObJsonInt j_int_4(256);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_4", &j_int_4));
  ObJsonInt j_int_5(6000);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_5", &j_int_5));
  ObJsonInt j_int_6(4234234);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_6", &j_int_6));
  ObJsonInt j_int_7(4234234123214321421L);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_7", &j_int_7));
  ObJsonInt j_int_8(-1);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_8", &j_int_8));
  ObJsonInt j_int_9(-3);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_9", &j_int_9));
  ObJsonInt j_int_10(-100);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_10", &j_int_10));
  ObJsonInt j_int_11(-256);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_11", &j_int_11));
  ObJsonInt j_int_12(-6000);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_12", &j_int_12));
  ObJsonInt j_int_13(-4234234);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_13", &j_int_13));
  ObJsonInt j_int_14(-4234234123214321421L);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_14", &j_int_14));
  ObJsonOInt j_int_15(INT_MIN);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_15", &j_int_15));
  ObJsonOInt j_int_16(INT_MAX);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_int_16", &j_int_16));

  ObJsonOInt j_oint_0(0);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_0", &j_oint_0));
  ObJsonOInt j_oint_1(1);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_1", &j_oint_1));
  ObJsonOInt j_oint_2(3);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_2", &j_oint_2));
  ObJsonOInt j_oint_3(100);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_3", &j_oint_3));
  ObJsonOInt j_oint_4(256);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_4", &j_oint_4));
  ObJsonOInt j_oint_5(6000);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_6", &j_oint_5));
  ObJsonOInt j_oint_6(4234234);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_6", &j_oint_6));
  ObJsonOInt j_oint_7(4234234123214321421L);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_7", &j_oint_7));
  ObJsonOInt j_oint_8(-1);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_8", &j_oint_8));
  ObJsonOInt j_oint_9(-3);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_9", &j_oint_9));
  ObJsonOInt j_oint_10(-100);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_10", &j_oint_10));
  ObJsonOInt j_oint_11(-256);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_11", &j_oint_11));
  ObJsonOInt j_oint_12(-6000);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_12", &j_oint_12));
  ObJsonOInt j_oint_13(-4234234);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_13", &j_oint_13));
  ObJsonOInt j_oint_14(-4234234123214321421L);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_14", &j_oint_14));
  ObJsonOInt j_oint_15(INT_MIN);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_15", &j_oint_15));
  ObJsonOInt j_oint_16(INT_MAX);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_oint_16", &j_oint_16));

  ObJsonUint j_uint_0(0);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_uint_0", &j_uint_0));
  ObJsonUint j_uint_1(1);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_uint_1", &j_uint_1));
  ObJsonUint j_uint_2(3);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_uint_2", &j_uint_2));
  ObJsonUint j_uint_3(100);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_uint_3", &j_uint_3));
  ObJsonUint j_uint_4(256);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_uint_4", &j_uint_4));
  ObJsonUint j_uint_5(6000);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_uint_5", &j_uint_5));
  ObJsonUint j_uint_6(4234234);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_uint_6", &j_uint_6));
  ObJsonUint j_uint_7(4234234123214321421L);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_uint_7", &j_uint_7));
  ObJsonUint j_uint_8(UINT_MAX);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_uint_8", &j_uint_8));
  ObJsonUint j_uint_9(ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_uint_9", &j_uint_9));

  ObJsonOLong j_olong_0(0);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_olong_0", &j_olong_0));
  ObJsonOLong j_olong_1(1);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_olong_1", &j_olong_1));
  ObJsonOLong j_olong_2(3);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_olong_2", &j_olong_2));
  ObJsonOLong j_olong_3(100);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_olong_3", &j_olong_3));
  ObJsonOLong j_olong_4(256);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_olong_4", &j_olong_4));
  ObJsonOLong j_olong_5(6000);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_olong_5", &j_olong_5));
  ObJsonOLong j_olong_6(4234234);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_olong_6", &j_olong_6));
  ObJsonOLong j_olong_7(4234234123214321421L);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_olong_7", &j_olong_7));
  ObJsonOLong j_olong_8(UINT_MAX);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_olong_8", &j_olong_8));
  ObJsonOLong j_olong_9(ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_olong_9", &j_olong_9));

  number::ObNumber res_nmb;
  ObString nmb_str("3.14");
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  ASSERT_EQ(OB_SUCCESS, res_nmb.from_sci_opt(nmb_str.ptr(), nmb_str.length(), allocator, &res_precision, &res_scale));

  ObJsonDecimal j_dec_1(res_nmb, res_precision, res_scale);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_dec_1", &j_dec_1));

  nmb_str.assign_ptr("-1.1234e9", STRLEN("-1.1234e9"));
  res_precision = 10;
  res_scale = 4;
  ASSERT_EQ(OB_SUCCESS, res_nmb.from_sci_opt(nmb_str.ptr(), nmb_str.length(), allocator, &res_precision, &res_scale));
  ObJsonDecimal j_dec_2(res_nmb, res_precision, res_scale);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_dec_2", &j_dec_2));

  ObJsonDouble j_double_0(0);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_double_0", &j_double_0));
  ObJsonDouble j_double_1(1.12222222222222);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_double_1", &j_double_1));
  ObJsonDouble j_double_2(112132132131223.32132132132131232122);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_double_2", &j_double_2));
  ObJsonDouble j_double_3(1.121321321321321e9);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_double_3", &j_double_3));
  ObJsonDouble j_double_4(0.1212e-9);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_double_4", &j_double_4));
  ObJsonDouble j_double_5(-0.12213222e-9);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_double_5", &j_double_5));
  ObJsonDouble j_double_6(-1.21321321321321);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_double_6", &j_double_6));
  ObJsonDouble j_double_7(-21321321321.32132132132131232122);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_double_7", &j_double_7));
  ObJsonDouble j_double_8(2.12213212222);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_double_8", &j_double_8));

  ObJsonOFloat j_ofloat_0(0);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_ofloat_0", &j_ofloat_0));
  ObJsonOFloat j_ofloat_1(1.12222222222222);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_ofloat_1", &j_ofloat_1));
  ObJsonOFloat j_ofloat_2(112132132131223.32132132132131232122);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_ofloat_2", &j_ofloat_2));
  ObJsonOFloat j_ofloat_3(1.121321321321321e9);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_ofloat_3", &j_ofloat_3));
  ObJsonOFloat j_ofloat_4(0.1212e-9);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_ofloat_4", &j_ofloat_4));
  ObJsonOFloat j_ofloat_5(-0.12213222e-9);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_ofloat_5", &j_ofloat_5));
  ObJsonOFloat j_ofloat_6(-1.21321321321321);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_ofloat_6", &j_ofloat_6));
  ObJsonOFloat j_ofloat_7(-21321321321.32132132132131232122);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_ofloat_7", &j_ofloat_7));
  ObJsonOFloat j_ofloat_8(2.12213212222);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_ofloat_8", &j_ofloat_8));

  ObJsonOpaque j_opaque_0(ObString("my opaque"), ObObjType::ObVarcharType);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_opaque_0", &j_opaque_0));

  ObTime ob_time_0(DT_TYPE_DATETIME);
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(946684800000000, NULL, ob_time_0));
  ObJsonDatetime j_datetime_0(ObJsonNodeType::J_DATETIME, ob_time_0);
  ASSERT_EQ(OB_SUCCESS, tree.add("j_datetime_0", &j_datetime_0));

  ObIJsonBase *j_tree = &tree;
  ObIJsonBase *j_bin = &bin;
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(&tree));
  ObJsonBuffer j_tree_buffer(&allocator);
  ObJsonBuffer j_bin_buffer(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_tree_buffer, false));
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_bin_buffer, false));
  ObString j_tree_str = j_tree_buffer.string();
  ObString j_bin_str = j_bin_buffer.string();
  ASSERT_EQ(std::string(j_tree_str.ptr(), j_tree_str.length()), std::string(j_bin_str.ptr(), j_bin_str.length()));
  ObJsonBuffer j_tree_buffer_2(&allocator);
  ObJsonNode *tree_2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, bin.to_tree(tree_2));
  ASSERT_EQ(OB_SUCCESS, tree_2->print(j_tree_buffer_2, false));
  ObString j_tree_str_2 = j_tree_buffer_2.string();
  ASSERT_EQ(std::string(j_tree_str.ptr(), j_tree_str.length()), std::string(j_tree_str_2.ptr(), j_tree_str_2.length()));

  ObJsonBuffer j_bin_buffer_2(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.rebuild(j_bin_buffer_2));
  ObJsonBin bin2;
  ObJsonBinCtx bin_ctx_2;
  ASSERT_EQ(OB_SUCCESS, bin2.reset(j_bin_buffer_2.string(), 0, &bin_ctx_2));
  ObJsonBuffer j_bin_buffer_2_1(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin2.print(j_bin_buffer_2_1, false));
  ObString j_bin_str_2 = j_bin_buffer_2_1.string();
  ASSERT_EQ(std::string(j_tree_str.ptr(), j_tree_str.length()), std::string(j_bin_str_2.ptr(), j_bin_str_2.length()));

}

static void json_set(ObIAllocator& allocator, ObString& j_text, std::vector<std::pair<ObString, ObString>> &updates)
{
  ObIJsonBase *j_bin = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
    ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonBin *bin = static_cast<ObJsonBin*>(j_bin);
  bin->set_seek_flag(false);
  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();
  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  ObJsonPathCache path_cache(&allocator);
  for (int i = 0 ; i < updates.size(); ++i) {
    ObString& path = updates[i].first;
    ObString& value = updates[i].second;
    ObJsonPath* json_path = nullptr;
    ObIJsonBase *j_new_node = nullptr;
    int path_idx = path_cache.size();
    ASSERT_EQ(OB_SUCCESS, path_cache.find_and_add_cache(json_path, path, path_idx));
    ASSERT_EQ(path_cache.path_stat_at(path_idx), ObPathParseStat::OK_NOT_NULL);
    ObJsonSeekResult hit;
    ASSERT_EQ(OB_SUCCESS, j_bin->seek(*json_path, json_path->path_node_cnt(), true, false, hit));
    ASSERT_EQ(1, hit.size());
    ObIJsonBase *j_parent = nullptr;
    ObIJsonBase *j_old_node = hit[0];
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, value,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_new_node));
    ASSERT_EQ(OB_SUCCESS, j_old_node->get_parent(j_parent));
    if(OB_NOT_NULL(j_parent)) {
      ASSERT_EQ(OB_SUCCESS, j_parent->replace(j_old_node, j_new_node));
    } else {
      ASSERT_EQ(OB_SUCCESS, j_bin->replace(j_old_node, j_new_node));
    }
  }
  check_diff_valid(allocator, result, update_ctx);
  check_json_diff_valid(allocator, j_text, update_ctx, 2);
}

TEST_F(TestJsonBin, test_nested_append_update)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  common::ObString j_text("{ \"name\" : \"Mike\", \"age\" : 30, \"like\" : [] , \"name1\" : \"Mike1\", \"name2\" : \"Mike2\",\"name3\" : \"Mike3\",\"name4\" : \"Mike4\",\"name5\" : \"Mike5\"}");
  std::vector<std::pair<ObString, ObString>> updates;
  updates.push_back({ObString("$.like"), ObString("[1, 2, 3]")});
  updates.push_back({ObString("$.like[1]"), ObString("{\"K\":\"V\"}")});
  json_set(allocator, j_text, updates);
}


TEST_F(TestJsonBin, test_nested_append_update_2)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  common::ObString j_text("{ \"name\" : \"Mike\", \"age\" : 30, \"like\" : [] , \"name1\" : \"Mike1\", \"name2\" : \"Mike2\",\"name3\" : \"Mike3\",\"name4\" : \"Mike4\",\"name5\" : \"Mike5\"}");
  std::vector<std::pair<ObString, ObString>> updates;
  updates.push_back({ObString("$.like"), ObString("[\"x\", [\"a\", \"b\", \"c\"], \"z\"]")});
  updates.push_back({ObString("$.like[1][1]"), ObString("{\"K\":\"V\"}")});
  json_set(allocator, j_text, updates);
}


TEST_F(TestJsonBin, test_array_remove)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  common::ObString j_text("{ \"name\" : \"Mike\", \"age\" : 30, \"like\" : [1, 2, 3] }");
  ObIJsonBase *j_bin = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
    ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonBin *bin = static_cast<ObJsonBin*>(j_bin);
  bin->set_seek_flag(false);
  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));

  common::ObString path_str("$.like[1]");
  ObJsonPath test_path(path_str, &allocator);
  ASSERT_EQ(OB_SUCCESS, test_path.parse_path());

  ObJsonSeekResult hit;
  int cnt = test_path.path_node_cnt();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(test_path, cnt, false, false, hit));
  ASSERT_EQ(1, hit.size());
  ObIJsonBase *j_child_bin = hit[0];
  ObIJsonBase *j_parent_bin = nullptr;
  ASSERT_EQ(OB_SUCCESS, j_child_bin->get_parent(j_parent_bin));
  ASSERT_NE(nullptr, j_parent_bin);
  ASSERT_EQ(OB_SUCCESS, j_parent_bin->array_remove(1));

  ASSERT_FALSE(update_ctx.is_rebuild_all_);
  ASSERT_EQ(1, update_ctx.binary_diffs_.count());
  check_diff_valid(allocator, result, update_ctx);
  ASSERT_EQ(OB_SUCCESS, bin->lookup(ObString("like")));
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, bin->json_type());

  ObJsonBuffer j_bin_buffer(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin->print(j_bin_buffer, false));
  ASSERT_EQ("[1, 3]", std::string(j_bin_buffer.ptr(), j_bin_buffer.length()));
}

TEST_F(TestJsonBin, test_array_remove_2)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  common::ObString j_text("[ \"name\" , \"Mike\", \"age\" , 30, \"like\", [1, 2, 3] ]");
  ObIJsonBase *j_bin = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
    ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonBin *bin = static_cast<ObJsonBin*>(j_bin);
  bin->set_seek_flag(false);
  ASSERT_EQ(OB_SUCCESS, init_update_ctx(allocator, bin));
  ObJsonBinUpdateCtx &update_ctx = *bin->get_update_ctx();

  common::ObString result;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(result, &allocator));
  ASSERT_EQ(OB_SUCCESS, j_bin->array_remove(1));

  ASSERT_TRUE(update_ctx.is_rebuild_all_);
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin->json_type());

  ObJsonBuffer j_bin_buffer(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin->print(j_bin_buffer, false));
  ASSERT_EQ("[\"name\", \"age\", 30, \"like\", [1, 2, 3]]", std::string(j_bin_buffer.ptr(), j_bin_buffer.length()));
}


} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  /*
  system("rm -f test_json_bin.log");
  OB_LOGGER.set_file_name("test_json_bin.log");
  OB_LOGGER.set_log_level("INFO");
  */
  return RUN_ALL_TESTS();
}