/**
 * Copyright (c) 2023 OceanBase
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
#define protected public
#define private public

#include "share/datum/ob_datum_funcs.h"
#include "share/rc/ob_tenant_base.h"
#include "share/text_analysis/ob_text_analyzer.h"


namespace oceanbase
{
namespace share
{

class TestTextAnalyzer : public ::testing::Test
{
public:
  TestTextAnalyzer() : allocator_(), analysis_ctx_(), token_cmp_func_() {}
  virtual ~TestTextAnalyzer() {}
  virtual void SetUp();
  virtual void TearDowm() {}
private:
  void analyze_test(
      ObITextAnalyzer &analyzer,
      const char *raw_doc,
      const int64_t raw_doc_len,
      const char **target_tokens,
      const int64_t *target_token_len,
      const int64_t *target_token_freq,
      const int64_t target_token_cnt);
  void find_token_in_target_array(
      const ObDatum &query_token,
      const char **target_tokens,
      const int64_t *target_token_len,
      const int64_t target_token_cnt,
      int64_t &idx);
private:
  ObArenaAllocator allocator_;
  ObTextAnalysisCtx analysis_ctx_;
  common::ObDatumCmpFuncType token_cmp_func_;
};

void TestTextAnalyzer::SetUp()
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  analysis_ctx_.cs_ = ObCharset::get_charset(CS_TYPE_UTF8MB4_GENERAL_CI);
  sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI);
  token_cmp_func_ = basic_funcs->null_first_cmp_;
}

void TestTextAnalyzer::analyze_test(
    ObITextAnalyzer &analyzer,
    const char *raw_doc,
    const int64_t raw_doc_len,
    const char **target_tokens,
    const int64_t *target_token_len,
    const int64_t *target_token_freq,
    const int64_t target_token_cnt)
{
  ObDatum doc_datum;
  doc_datum.set_string(raw_doc, raw_doc_len);
  LOG_DEBUG("start test one tokenization", K(analyzer), K(doc_datum), K(doc_datum.get_string()));

  ObITokenStream *token_stream;
  ASSERT_EQ(OB_SUCCESS, analyzer.analyze(doc_datum, token_stream));
  ASSERT_NE(nullptr, token_stream);

  int ret = OB_SUCCESS;
  int64_t token_cnt = 0;
  while (OB_SUCC(ret)) {
    ObDatum token;
    int64_t token_freq = 0;
    if (OB_FAIL(token_stream->get_next(token, token_freq))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Failed to get next token from token stream", KPC(token_stream));
      }
    } else {
      ASSERT_TRUE(token_cnt < target_token_cnt);
      LOG_INFO("print token", K(token), K(token.get_string()), K(token_freq));
      int64_t idx = -1;
      find_token_in_target_array(token, target_tokens, target_token_len, target_token_cnt, idx);
      ASSERT_TRUE(idx >= 0 && idx < target_token_cnt) << "idx:" << idx;
      ASSERT_EQ(token_freq, target_token_freq[idx]) << "token_freq:" << token_freq << "target_token_freq" << target_token_freq[idx];
      ++token_cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(token_cnt, target_token_cnt);
}

void TestTextAnalyzer::find_token_in_target_array(
    const ObDatum &query_token,
    const char **target_tokens,
    const int64_t *target_token_len,
    const int64_t target_token_cnt,
    int64_t &idx)
{
  idx = -1;
  for (int64_t i = 0; i < target_token_cnt; ++i) {
    ObDatum target_token_datum;
    target_token_datum.set_string(target_tokens[i], target_token_len[i]);
    int cmp_ret = 0;
    ASSERT_EQ(OB_SUCCESS, token_cmp_func_(target_token_datum, query_token, cmp_ret));
    if (0 == cmp_ret) {
      idx = i;
      break;
    }
  }
  if (idx == -1) {
    LOG_INFO("query token not found", K(query_token), K(query_token.get_string()));
  }
}

TEST_F(TestTextAnalyzer, test_basic_english_analyzer)
{
  ObEnglishTextAnalyzer analyzer;
  analysis_ctx_.need_grouping_ = false;

  ASSERT_EQ(OB_SUCCESS, analyzer.init(analysis_ctx_, allocator_));

  const int64_t doc_len_1 = 64;
  const char doc_1[doc_len_1] = {"Try to tokenize basic english doc."};
  const int64_t token_cnt_1 = 6;
  const char *tokens1[token_cnt_1] = {"try", "to", "tokenize", "basic", "english", "doc"};
  const int64_t tokens_len_1[token_cnt_1] = {3, 2, 8, 5, 7, 3};
  const int64_t tokens_freq_1[token_cnt_1] = {1, 1, 1, 1, 1, 1};
  analyze_test(analyzer, doc_1, doc_len_1, tokens1, tokens_len_1, tokens_freq_1, token_cnt_1);

  // not deduplicated
  const int64_t doc_len_2 = 64;
  const char doc_2[doc_len_2] = {"oceanbase@oceanbase.com, \t https://www.oceanbase.com/"};
  const int64_t token_cnt_2 = 7;
  const char *tokens_2[token_cnt_2] = {"oceanbase", "oceanbase", "com", "https", "www", "oceanbase", "com"};
  const int64_t tokens_2_len[token_cnt_2] = {9, 9, 3, 5, 3, 9, 3};
  const int64_t tokens_freq_2[token_cnt_2] = {1, 1, 1, 1, 1, 1, 1};
  analyze_test(analyzer, doc_2, doc_len_2, tokens_2, tokens_2_len, tokens_freq_2, token_cnt_2);

  // won't trim extremely short phrase for now
  const int64_t doc_len_3 = 64;
  const char doc_3[doc_len_3] = {"if (a==b and c > !d)      then x=1;"};
  const int64_t token_cnt_3 = 9;
  const char *tokens_3[token_cnt_3] = {"if", "a", "b", "and", "c", "d", "then", "x", "1"};
  const int64_t tokens_len_3[token_cnt_3] = {2, 1, 1, 3, 1, 1, 4, 1, 1};
  const int64_t tokens_freq_3[token_cnt_3] = {1, 1, 1, 1, 1, 1, 1, 1, 1};
  analyze_test(analyzer, doc_3, doc_len_3, tokens_3, tokens_len_3, tokens_freq_3, token_cnt_3);

  // test paragraphs
  const int64_t doc_len_4 = 128;
  const char doc_4[doc_len_4] = {"PARAGRAPH1\nPARAGRAPH2\nPARAGRAPH3"};
  const int64_t token_cnt_4 = 3;
  const char *tokens_4[token_cnt_4] = {"paragraph1","paragraph2","paragraph3"};
  const int64_t tokens_len_4[token_cnt_4] = {10,10,10};
  const int64_t tokens_freq_4[token_cnt_4] = {1, 1, 1};
  analyze_test(analyzer, doc_4, doc_len_4, tokens_4, tokens_len_4, tokens_freq_4, token_cnt_4);

  // test non-english text
  const int64_t doc_len_5 = 128;
  const char doc_5[doc_len_5] = {"乘骐骥以驰骋兮，来吾道夫先路"};
  const int64_t token_cnt_5 = 1;
  const char *tokens_5[token_cnt_5] = {"乘骐骥以驰骋兮，来吾道夫先路"};
  const int64_t tokens_len_5[token_cnt_5] = {42};
  const int64_t tokens_freq_5[token_cnt_5] = {1};
  analyze_test(analyzer, doc_5, doc_len_5, tokens_5, tokens_len_5, tokens_freq_5, token_cnt_5);

  analyzer.reset();

  // grouping test
  analysis_ctx_.need_grouping_ = true;
  ASSERT_EQ(OB_SUCCESS, analyzer.init(analysis_ctx_, allocator_));
  analyze_test(analyzer, doc_1, doc_len_1, tokens1, tokens_len_1, tokens_freq_1, token_cnt_1);
  analyze_test(analyzer, doc_3, doc_len_3, tokens_3, tokens_len_3, tokens_freq_3, token_cnt_3);
  analyze_test(analyzer, doc_4, doc_len_4, tokens_4, tokens_len_4, tokens_freq_4, token_cnt_4);

  const int64_t doc_len_6 = 64;
  const char doc_6[doc_len_6] = {"oceanbase@oceanbase.com, \t https://www.oceanbase.com/"};
  const int64_t token_cnt_6 = 4;
  const char *tokens_6[token_cnt_6] = {"oceanbase", "com", "https", "www"};
  const int64_t tokens_len_6[token_cnt_6] = {9, 3, 5, 3};
  const int64_t tokens_freq_6[token_cnt_6] = {3, 2, 1, 1};
  analyze_test(analyzer, doc_6, doc_len_6, tokens_6, tokens_len_6, tokens_freq_6, token_cnt_6);

  // test invalid character
  const int64_t doc_len_7 = 128;
  const char doc_7[doc_len_7] = {(char)0xFF};
  const int64_t token_cnt_7 = 0;
  const char *tokens_7[token_cnt_7] = {};
  const int64_t tokens_len_7[token_cnt_7] = {};
  const int64_t tokens_freq_7[token_cnt_7] = {};
  analyze_test(analyzer, doc_7, doc_len_7, tokens_7, tokens_len_7, tokens_freq_7, token_cnt_7);

  // test invalid character in string
  const int64_t doc_len_8 = 128;
  const char doc_8[doc_len_8] = {"test invalid character here"};
  ((char *)doc_8)[16] = char(0xFF);
  const int64_t token_cnt_8 = 3;
  const char *tokens_8[token_cnt_8] = {"test", "invalid", "cha"};
  const int64_t tokens_len_8[token_cnt_8] = {4, 7, 3};
  const int64_t tokens_freq_8[token_cnt_8] = {1, 1, 1};
  analyze_test(analyzer, doc_8, doc_len_8, tokens_8, tokens_len_8, tokens_freq_8, token_cnt_8);
}

}; // namespace share
}; // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_text_analyzer.log*");
  OB_LOGGER.set_file_name("test_text_analyzer.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  // oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
