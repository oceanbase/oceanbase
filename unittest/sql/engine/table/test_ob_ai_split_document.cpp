/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/ai_split_document/ob_ai_split_document.h"
#include "lib/ai_split_document/ob_ai_split_document_util.h"
#include "lib/string/ob_string_buffer.h"
#include <iostream>
#include <chrono>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace std;

class ObAiSplitDocumentTest : public ::testing::Test
{
public:
  ObAiSplitDocumentTest() : allocator_("AiSplitTest") {}
  virtual ~ObAiSplitDocumentTest() {}

  virtual void SetUp() override {
    allocator_.reuse();
  }

  virtual void TearDown() override {
    allocator_.reuse();
  }

protected:
  ObArenaAllocator allocator_;
};

// ========== 测试 ObAiSplitDocParams ==========

// TEST_F(ObAiSplitDocumentTest, test_split_content_to_lines)
// {
//   int ret = OB_SUCCESS;
//   ObMarkdownSplitIterator iterator;
//   ObString content("First sentence. Second sentence. Third sentence. Fourth sentence. Fifth sentence.");
//   ObAiSplitDocParams params;
//   params.type_ = ObAiSplitContentType::MARKDOWN;
//   params.by_ = ObAiSplitByUnit::SENTENCE;
//   params.max_ = 2;
//   params.overlap_ = 1;

//   cout << "========== Starting test ==========" << endl;
//   cout << "Content: " << content.ptr() << endl;
//   cout << "Content length: " << content.length() << endl;

//   ret = iterator.open(content, allocator_, params);
//   cout << "Iterator open ret: " << ret << endl;

//   ObAiSplitDocChunk chunk;
//   int chunk_count = 0;

//   while (OB_SUCC(ret)) {
//     if (OB_FAIL(iterator.get_next_row(chunk))) {
//       if (ret == OB_ITER_END) {
//         cout << "Iterator ended, total chunks: " << chunk_count << endl;
//       } else {
//         cout << "Failed to get next section, ret: " << ret << endl;
//       }
//     } else {
//       chunk_count++;
//       cout << "========== Chunk " << chunk_count << " ==========" << endl;
//       cout << "Chunk ID: " << chunk.chunk_id_ << endl;
//       cout << "Chunk offset: " << chunk.chunk_offset_ << endl;
//       cout << "Chunk length: " << chunk.chunk_length_ << endl;
//       cout << "Chunk text: [" << string(chunk.chunk_text_.ptr(), chunk.chunk_text_.length()) << "]" << endl;
//       chunk.reset();
//     }
//   }

//   cout << "========== Test finished ==========" << endl;
// }

// TEST_F(ObAiSplitDocumentTest, test_chunk_size)
// {
//   UErrorCode status = U_ZERO_ERROR;
//   ObString content("First sentence. Second sentence. Third sentence. Fourth sentence. Fifth sentence.");
//   icu::BreakIterator *bi = icu::BreakIterator::createSentenceInstance(icu::Locale::getDefault(), status);
//   UText *utext = utext_openUTF8(NULL, content.ptr(), content.length(), &status);
//   bi->setText(utext, status);
//   int boundary = bi->next();
//   cout << "boundary start: " << boundary << endl;
//   while (boundary != icu::BreakIterator::DONE) {
//     cout << "boundary: " << boundary << endl;
//     boundary = bi->next();
//   }
//   delete bi;
//   utext_close(utext);
//   // cout << "chunk size: " << sizeof(chunk) << endl;
// }


TEST_F(ObAiSplitDocumentTest, test_markdown_split_iterator)
{
  int ret = OB_SUCCESS;
  ObString content("# 一级标题 1：文本与排版\n\n"
"## 段落与换行\n"
"这是第一段文本，用于展示普通段落。\n\n");

  ObTextSplitIterator iterator;
  ObAiSplitDocParams params;
  params.type_ = ObAiSplitContentType::TEXT;
  params.by_ = ObAiSplitByUnit::SENTENCE;
  params.max_ = 1;
  params.overlap_ = 0;

  cout << "========== Starting markdown test ==========" << endl;
  cout << "Content length: " << content.length() << endl;

  ret = iterator.open(content, allocator_, params);
  cout << "Iterator open ret: " << ret << endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  ObAiSplitDocChunk chunk;
  int chunk_count = 0;
  ObString section_title;
  ObString section_content;
  int index = 0;
  // while (OB_SUCC(ret)) {
  //   if (OB_FAIL(iterator.get_next_section(section_title, section_content))) {
  //     break;
  //   }
  //   index++;
  //   cout << "Section " << index << " title: [" << string(section_title.ptr(), section_title.length()) << "]" << endl;
  //   cout << "Section " << index << " content: [" << string(section_content.ptr(), section_content.length()) << "]"<< endl;
  //   cout << "Section " << index << " title length: " << section_title.length() << endl;
  //   cout << "Section " << index << " content length: " << section_content.length() << endl;
  //   cout << "--------------------------------" << endl;

  // }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(iterator.get_next_row(chunk))) {
      if (ret == OB_ITER_END) {
        cout << "Iterator ended, total chunks: " << chunk_count << endl;
      } else {
        cout << "Failed to get next section, ret: " << ret << endl;
      }
    } else {
      chunk_count++;
      cout << "========== Chunk " << chunk_count << " ==========" << endl;
      cout << "Chunk ID: " << chunk.chunk_id_ << endl;
      cout << "Chunk offset: " << chunk.chunk_offset_ << endl;
      cout << "Chunk length: " << chunk.chunk_length_ << endl;
      cout << "Chunk text: [" << string(chunk.chunk_text_.ptr(), chunk.chunk_text_.length()) << "]" << endl;
      cout << "--------------------------------" << endl;
    }
  }

  cout << "========== Test finished ==========" << endl;
}

// 性能对比测试：snprintf vs ObStringBuffer::append
TEST_F(ObAiSplitDocumentTest, test_string_concat_performance)
{
  const int ITERATIONS = 100000;
  ObString section_title("## Section Title: This is a test title");
  ObString content("This is the content of the section. It contains multiple sentences. "
                   "We want to test the performance of string concatenation.");

  cout << "========== String Concatenation Performance Test ==========" << endl;
  cout << "Iterations: " << ITERATIONS << endl;
  cout << "Title length: " << section_title.length() << endl;
  cout << "Content length: " << content.length() << endl;
  cout << "Total length: " << (section_title.length() + content.length()) << endl;
  cout << endl;

  // ========== 方法1: snprintf ==========
  {
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < ITERATIONS; i++) {
      allocator_.reuse();

      int total_len = section_title.length() + content.length();
      char* buf = static_cast<char*>(allocator_.alloc(total_len + 1));
      if (buf != nullptr) {
        int written = snprintf(buf, total_len + 1, "%.*s%.*s",
                              section_title.length(), section_title.ptr(),
                              content.length(), content.ptr());
        ASSERT_EQ(written, total_len);
        ObString result(total_len, buf);
      }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    cout << "Method 1: snprintf" << endl;
    cout << "  Total time: " << duration.count() << " us" << endl;
    cout << "  Average time per operation: " << (duration.count() / (double)ITERATIONS) << " us" << endl;
    cout << "  Operations per second: " << (ITERATIONS * 1000000.0 / duration.count()) << endl;
    cout << endl;
  }

  // ========== 方法2: ObStringBuffer::append 两次 ==========
  {
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < ITERATIONS; i++) {
      allocator_.reuse();

      ObStringBuffer buf(&allocator_);
      int ret = buf.reserve(section_title.length() + content.length());
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = buf.append(section_title);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = buf.append(content);
      ASSERT_EQ(OB_SUCCESS, ret);

      ObString result;
      ret = buf.get_result_string(result);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(result.length(), section_title.length() + content.length());
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    cout << "Method 2: ObStringBuffer::append (2 times)" << endl;
    cout << "  Total time: " << duration.count() << " us" << endl;
    cout << "  Average time per operation: " << (duration.count() / (double)ITERATIONS) << " us" << endl;
    cout << "  Operations per second: " << (ITERATIONS * 1000000.0 / duration.count()) << endl;
    cout << endl;
  }

  // ========== 方法3: memcpy 两次（基准测试）==========
  {
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < ITERATIONS; i++) {
      allocator_.reuse();

      int total_len = section_title.length() + content.length();
      char* buf = static_cast<char*>(allocator_.alloc(total_len));
      if (buf != nullptr) {
        memcpy(buf, section_title.ptr(), section_title.length());
        memcpy(buf + section_title.length(), content.ptr(), content.length());
        ObString result(total_len, buf);
      }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    cout << "Method 3: memcpy (2 times) - Baseline" << endl;
    cout << "  Total time: " << duration.count() << " us" << endl;
    cout << "  Average time per operation: " << (duration.count() / (double)ITERATIONS) << " us" << endl;
    cout << "  Operations per second: " << (ITERATIONS * 1000000.0 / duration.count()) << endl;
    cout << endl;
  }

  // ========== 正确性验证 ==========
  cout << "========== Correctness Verification ==========" << endl;

  // 方法1: snprintf
  allocator_.reuse();
  int total_len = section_title.length() + content.length();
  char* buf1 = static_cast<char*>(allocator_.alloc(total_len + 1));
  snprintf(buf1, total_len + 1, "%.*s%.*s",
           section_title.length(), section_title.ptr(),
           content.length(), content.ptr());
  ObString result1(total_len, buf1);

  // 方法2: ObStringBuffer
  allocator_.reuse();
  ObStringBuffer buf2(&allocator_);
  buf2.reserve(section_title.length() + content.length());
  buf2.append(section_title);
  buf2.append(content);
  ObString result2;
  buf2.get_result_string(result2);

  // 方法3: memcpy
  allocator_.reuse();
  char* buf3 = static_cast<char*>(allocator_.alloc(total_len));
  memcpy(buf3, section_title.ptr(), section_title.length());
  memcpy(buf3 + section_title.length(), content.ptr(), content.length());
  ObString result3(total_len, buf3);

  cout << "Result 1 (snprintf): [" << string(result1.ptr(), result1.length()) << "]" << endl;
  cout << "Result 2 (ObStringBuffer): [" << string(result2.ptr(), result2.length()) << "]" << endl;
  cout << "Result 3 (memcpy): [" << string(result3.ptr(), result3.length()) << "]" << endl;
  cout << endl;

  ASSERT_EQ(result1.length(), result2.length());
  ASSERT_EQ(result1.length(), result3.length());
  ASSERT_EQ(0, memcmp(result1.ptr(), result2.ptr(), result1.length()));
  ASSERT_EQ(0, memcmp(result1.ptr(), result3.ptr(), result1.length()));

  cout << "All methods produce identical results!" << endl;
  cout << "========== Performance Test Finished ==========" << endl;
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
