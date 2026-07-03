/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "storage/fts/analyzer/filter/ob_icu_normalizer2_filter.h"
#include "storage/fts/analyzer/tokenizer/ob_keyword_tokenizer.h"
#include "unittest/storage/fts/test_analyzer_helper.h"

#include <unicode/putil.h>

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{

// static constexpr char ICU_FOLDING_DATA_DIR[] = OB_ICU_TEST_DATA_DIR;

class FTICUNormalizer2FilterTest : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    LOG_INFO("SetUpTestCase");
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    // u_setDataDirectory(ICU_FOLDING_DATA_DIR);
  }

  static void TearDownTestCase()
  {
    LOG_INFO("TearDownTestCase");
    MockTenantModuleEnv::get_instance().destroy();
  }
};

TEST_F(FTICUNormalizer2FilterTest, nfc_compose)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObICUNormalizer2FilterSpec spec(ObICUNormalizer2FilterSpec::Name::NFC,
                                  UNormalization2Mode::UNORM2_COMPOSE);

  ObICUNormalizer2Filter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#1 Combining sequence",
      filter,
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"},
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#2 Ordering of combining marks",
      filter,
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"},
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#3 Hangul & conjoining jamo",
      filter,
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"},
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#4 Singleton equivalence",
      filter,
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"},
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#5 Ligatures and multigraphs",
      filter,
      {u8"ﬁle", u8"oﬃce", u8"ﬂower", u8"baﬄe", u8"ǅungla", u8"ǆak", u8"ﬃ", u8"straße", u8"ﬁancée"},
      {u8"ﬁle", u8"oﬃce", u8"ﬂower", u8"baﬄe", u8"ǅungla", u8"ǆak", u8"ﬃ", u8"straße", u8"ﬁancée"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#6 Font variants",
      filter,
      {u8"ℌello", u8"ℍotel", u8"𝔸𝕓𝕔", u8"𝖧𝖾𝗅𝗅𝗈", u8"𝛂θήνα", u8"𝚺igma", u8"𝐀𝐁𝐂", u8"𝘈𝘣𝘤"},
      {u8"ℌello", u8"ℍotel", u8"𝔸𝕓𝕔", u8"𝖧𝖾𝗅𝗅𝗈", u8"𝛂θήνα", u8"𝚺igma", u8"𝐀𝐁𝐂", u8"𝘈𝘣𝘤"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#7 Linebreaking differences",
      filter,
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"},
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#8 Positional variant forms",
      filter,
      {u8"ﻙ", u8"ﻚ", u8"ﻛ", u8"ﻜ", u8"ﺳ", u8"ﺴ"},
      {u8"ﻙ", u8"ﻚ", u8"ﻛ", u8"ﻜ", u8"ﺳ", u8"ﺴ"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#9 Circled variants",
      filter,
      {u8"①②③", u8"ⒶⒷⒸ", u8"ⓐⓑⓒ", u8"㉑㊷", u8"㊙㊣", u8"㋐㋑㋒"},
      {u8"①②③", u8"ⒶⒷⒸ", u8"ⓐⓑⓒ", u8"㉑㊷", u8"㊙㊣", u8"㋐㋑㋒"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#10 Width variants",
      filter,
      {u8"ＡＢＣ１２３", u8"ｔｅｓｔ　１２３", u8"ｶﾀｶﾅ", u8"ﾊﾟﾋﾟﾌﾟﾍﾟﾎﾟ", u8"ｶﾞｯﾂﾎﾟｰｽﾞ", u8"Ｘ線"},
      {u8"ＡＢＣ１２３", u8"ｔｅｓｔ　１２３", u8"ｶﾀｶﾅ", u8"ﾊﾟﾋﾟﾌﾟﾍﾟﾎﾟ", u8"ｶﾞｯﾂﾎﾟｰｽﾞ", u8"Ｘ線"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#11 Rotated variants",
      filter,
      {u8"︷abc︸", u8"︷数据︸", u8"︷Ω︸", u8"︷ガ︸"},
      {u8"︷abc︸", u8"︷数据︸", u8"︷Ω︸", u8"︷ガ︸"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#12 Superscripts/subscripts",
      filter,
      {u8"i⁹", u8"i₉", u8"H₂O", u8"CO₂", u8"x⁵+y⁷", u8"Na⁺"},
      {u8"i⁹", u8"i₉", u8"H₂O", u8"CO₂", u8"x⁵+y⁷", u8"Na⁺"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#13 Squared characters",
      filter,
      {u8"㎏", u8"㍑", u8"㌀", u8"㏄", u8"㌖", u8"㍻"},
      {u8"㎏", u8"㍑", u8"㌀", u8"㏄", u8"㌖", u8"㍻"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#14 Fractions",
      filter,
      {u8"½", u8"⅓", u8"⅝", u8"3½", u8"⅔杯"},
      {u8"½", u8"⅓", u8"⅝", u8"3½", u8"⅔杯"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#15 CJK compatibility ideographs",
      filter,
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"},
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"});
}

TEST_F(FTICUNormalizer2FilterTest, nfc_decompose)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObICUNormalizer2FilterSpec spec(ObICUNormalizer2FilterSpec::Name::NFC,
                                  UNormalization2Mode::UNORM2_DECOMPOSE);

  ObICUNormalizer2Filter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#1 Combining sequence",
      filter,
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"},
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#2 Ordering of combining marks",
      filter,
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"},
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#3 Hangul & conjoining jamo",
      filter,
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"},
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#4 Singleton equivalence",
      filter,
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"},
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#5 Ligatures and multigraphs",
      filter,
      {u8"ﬁle", u8"oﬃce", u8"ﬂower", u8"baﬄe", u8"ǅungla", u8"ǆak", u8"ﬃ", u8"straße", u8"ﬁancée"},
      {u8"ﬁle", u8"oﬃce", u8"ﬂower", u8"baﬄe", u8"ǅungla", u8"ǆak", u8"ﬃ", u8"straße", u8"ﬁancée"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#6 Font variants",
      filter,
      {u8"ℌello", u8"ℍotel", u8"𝔸𝕓𝕔", u8"𝖧𝖾𝗅𝗅𝗈", u8"𝛂θήνα", u8"𝚺igma", u8"𝐀𝐁𝐂", u8"𝘈𝘣𝘤"},
      {u8"ℌello", u8"ℍotel", u8"𝔸𝕓𝕔", u8"𝖧𝖾𝗅𝗅𝗈", u8"𝛂θήνα", u8"𝚺igma", u8"𝐀𝐁𝐂", u8"𝘈𝘣𝘤"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#7 Linebreaking differences",
      filter,
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"},
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#8 Positional variant forms",
      filter,
      {u8"ﻙ", u8"ﻚ", u8"ﻛ", u8"ﻜ", u8"ﺳ", u8"ﺴ"},
      {u8"ﻙ", u8"ﻚ", u8"ﻛ", u8"ﻜ", u8"ﺳ", u8"ﺴ"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#9 Circled variants",
      filter,
      {u8"①②③", u8"ⒶⒷⒸ", u8"ⓐⓑⓒ", u8"㉑㊷", u8"㊙㊣", u8"㋐㋑㋒"},
      {u8"①②③", u8"ⒶⒷⒸ", u8"ⓐⓑⓒ", u8"㉑㊷", u8"㊙㊣", u8"㋐㋑㋒"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#10 Width variants",
      filter,
      {u8"ＡＢＣ１２３", u8"ｔｅｓｔ　１２３", u8"ｶﾀｶﾅ", u8"ﾊﾟﾋﾟﾌﾟﾍﾟﾎﾟ", u8"ｶﾞｯﾂﾎﾟｰｽﾞ", u8"Ｘ線"},
      {u8"ＡＢＣ１２３", u8"ｔｅｓｔ　１２３", u8"ｶﾀｶﾅ", u8"ﾊﾟﾋﾟﾌﾟﾍﾟﾎﾟ", u8"ｶﾞｯﾂﾎﾟｰｽﾞ", u8"Ｘ線"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#11 Rotated variants",
      filter,
      {u8"︷abc︸", u8"︷数据︸", u8"︷Ω︸", u8"︷ガ︸"},
      {u8"︷abc︸", u8"︷数据︸", u8"︷Ω︸", u8"︷ガ︸"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#12 Superscripts/subscripts",
      filter,
      {u8"i⁹", u8"i₉", u8"H₂O", u8"CO₂", u8"x⁵+y⁷", u8"Na⁺"},
      {u8"i⁹", u8"i₉", u8"H₂O", u8"CO₂", u8"x⁵+y⁷", u8"Na⁺"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#13 Squared characters",
      filter,
      {u8"㎏", u8"㍑", u8"㌀", u8"㏄", u8"㌖", u8"㍻"},
      {u8"㎏", u8"㍑", u8"㌀", u8"㏄", u8"㌖", u8"㍻"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#14 Fractions",
      filter,
      {u8"½", u8"⅓", u8"⅝", u8"3½", u8"⅔杯"},
      {u8"½", u8"⅓", u8"⅝", u8"3½", u8"⅔杯"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#15 CJK compatibility ideographs",
      filter,
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"},
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"});
}

TEST_F(FTICUNormalizer2FilterTest, nfkc_compose)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObICUNormalizer2FilterSpec spec(ObICUNormalizer2FilterSpec::Name::NFKC,
                                  UNormalization2Mode::UNORM2_COMPOSE);

  ObICUNormalizer2Filter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#1 Combining sequence",
      filter,
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"},
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#2 Ordering of combining marks",
      filter,
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"},
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#3 Hangul & conjoining jamo",
      filter,
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"},
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#4 Singleton equivalence",
      filter,
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"},
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#5 Ligatures and multigraphs",
      filter,
      {u8"ﬁle", u8"oﬃce", u8"ﬂower", u8"baﬄe", u8"ǅungla", u8"ǆak", u8"ﬃ", u8"straße", u8"ﬁancée"},
      {u8"file", u8"office", u8"flower", u8"baffle", u8"Džungla", u8"džak", u8"ffi", u8"straße", u8"fiancée"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#6 Font variants",
      filter,
      {u8"ℌello", u8"ℍotel", u8"𝔸𝕓𝕔", u8"𝖧𝖾𝗅𝗅𝗈", u8"𝛂θήνα", u8"𝚺igma", u8"𝐀𝐁𝐂", u8"𝘈𝘣𝘤"},
      {u8"Hello", u8"Hotel", u8"Abc", u8"Hello", u8"αθήνα", u8"Σigma", u8"ABC", u8"Abc"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#7 Linebreaking differences",
      filter,
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"},
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#8 Positional variant forms",
      filter,
      {u8"ﻙ", u8"ﻚ", u8"ﻛ", u8"ﻜ", u8"ﺳ", u8"ﺴ"},
      {u8"ك", u8"ك", u8"ك", u8"ك", u8"س", u8"س"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#9 Circled variants",
      filter,
      {u8"①②③", u8"ⒶⒷⒸ", u8"ⓐⓑⓒ", u8"㉑㊷", u8"㊙㊣", u8"㋐㋑㋒"},
      {u8"123", u8"ABC", u8"abc", u8"2142", u8"秘正", u8"アイウ"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#10 Width variants",
      filter,
      {u8"ＡＢＣ１２３", u8"ｔｅｓｔ　１２３", u8"ｶﾀｶﾅ", u8"ﾊﾟﾋﾟﾌﾟﾍﾟﾎﾟ", u8"ｶﾞｯﾂﾎﾟｰｽﾞ", u8"Ｘ線"},
      {u8"ABC123", u8"test 123", u8"カタカナ", u8"パピプペポ", u8"ガッツポーズ", u8"X線"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#11 Rotated variants",
      filter,
      {u8"︷abc︸", u8"︷数据︸", u8"︷Ω︸", u8"︷ガ︸"},
      {u8"{abc}", u8"{数据}", u8"{Ω}", u8"{ガ}"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#12 Superscripts/subscripts",
      filter,
      {u8"i⁹", u8"i₉", u8"H₂O", u8"CO₂", u8"x⁵+y⁷", u8"Na⁺"},
      {u8"i9", u8"i9", u8"H2O", u8"CO2", u8"x5+y7", u8"Na+"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#13 Squared characters",
      filter,
      {u8"㎏", u8"㍑", u8"㌀", u8"㏄", u8"㌖", u8"㍻"},
      {u8"kg", u8"リットル", u8"アパート", u8"cc", u8"キロメートル", u8"平成"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#14 Fractions",
      filter,
      {u8"½", u8"⅓", u8"⅝", u8"3½", u8"⅔杯"},
      {u8"1⁄2", u8"1⁄3", u8"5⁄8", u8"31⁄2", u8"2⁄3杯"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#15 CJK compatibility ideographs",
      filter,
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"},
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"});
}

TEST_F(FTICUNormalizer2FilterTest, nfkc_decompose)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObICUNormalizer2FilterSpec spec(ObICUNormalizer2FilterSpec::Name::NFKC,
                                  UNormalization2Mode::UNORM2_DECOMPOSE);

  ObICUNormalizer2Filter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#1 Combining sequence",
      filter,
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"},
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#2 Ordering of combining marks",
      filter,
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"},
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#3 Hangul & conjoining jamo",
      filter,
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"},
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#4 Singleton equivalence",
      filter,
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"},
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#5 Ligatures and multigraphs",
      filter,
      {u8"ﬁle", u8"oﬃce", u8"ﬂower", u8"baﬄe", u8"ǅungla", u8"ǆak", u8"ﬃ", u8"straße", u8"ﬁancée"},
      {u8"file", u8"office", u8"flower", u8"baffle", u8"Džungla", u8"džak", u8"ffi", u8"straße", u8"fiancée"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#6 Font variants",
      filter,
      {u8"ℌello", u8"ℍotel", u8"𝔸𝕓𝕔", u8"𝖧𝖾𝗅𝗅𝗈", u8"𝛂θήνα", u8"𝚺igma", u8"𝐀𝐁𝐂", u8"𝘈𝘣𝘤"},
      {u8"Hello", u8"Hotel", u8"Abc", u8"Hello", u8"αθήνα", u8"Σigma", u8"ABC", u8"Abc"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#7 Linebreaking differences",
      filter,
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"},
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#8 Positional variant forms",
      filter,
      {u8"ﻙ", u8"ﻚ", u8"ﻛ", u8"ﻜ", u8"ﺳ", u8"ﺴ"},
      {u8"ك", u8"ك", u8"ك", u8"ك", u8"س", u8"س"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#9 Circled variants",
      filter,
      {u8"①②③", u8"ⒶⒷⒸ", u8"ⓐⓑⓒ", u8"㉑㊷", u8"㊙㊣", u8"㋐㋑㋒"},
      {u8"123", u8"ABC", u8"abc", u8"2142", u8"秘正", u8"アイウ"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#10 Width variants",
      filter,
      {u8"ＡＢＣ１２３", u8"ｔｅｓｔ　１２３", u8"ｶﾀｶﾅ", u8"ﾊﾟﾋﾟﾌﾟﾍﾟﾎﾟ", u8"ｶﾞｯﾂﾎﾟｰｽﾞ", u8"Ｘ線"},
      {u8"ABC123", u8"test 123", u8"カタカナ", u8"パピプペポ", u8"ガッツポーズ", u8"X線"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#11 Rotated variants",
      filter,
      {u8"︷abc︸", u8"︷数据︸", u8"︷Ω︸", u8"︷ガ︸"},
      {u8"{abc}", u8"{数据}", u8"{Ω}", u8"{ガ}"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#12 Superscripts/subscripts",
      filter,
      {u8"i⁹", u8"i₉", u8"H₂O", u8"CO₂", u8"x⁵+y⁷", u8"Na⁺"},
      {u8"i9", u8"i9", u8"H2O", u8"CO2", u8"x5+y7", u8"Na+"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#13 Squared characters",
      filter,
      {u8"㎏", u8"㍑", u8"㌀", u8"㏄", u8"㌖", u8"㍻"},
      {u8"kg", u8"リットル", u8"アパート", u8"cc", u8"キロメートル", u8"平成"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#14 Fractions",
      filter,
      {u8"½", u8"⅓", u8"⅝", u8"3½", u8"⅔杯"},
      {u8"1⁄2", u8"1⁄3", u8"5⁄8", u8"31⁄2", u8"2⁄3杯"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#15 CJK compatibility ideographs",
      filter,
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"},
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"});
}

TEST_F(FTICUNormalizer2FilterTest, nfkc_cf_compose)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObICUNormalizer2FilterSpec spec(ObICUNormalizer2FilterSpec::Name::NFKC_CF,
                                  UNormalization2Mode::UNORM2_COMPOSE);

  ObICUNormalizer2Filter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#1 Combining sequence",
      filter,
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"},
      {u8"café", u8"café", u8"ångström", u8"ångström", u8"йогурт", u8"йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#2 Ordering of combining marks",
      filter,
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"},
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#3 Hangul & conjoining jamo",
      filter,
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"},
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#4 Singleton equivalence",
      filter,
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"},
      {u8"ω", u8"ω", u8"å", u8"å", u8"ωmega", u8"ωmega", u8"ångström", u8"ångström"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#5 Ligatures and multigraphs",
      filter,
      {u8"ﬁle", u8"oﬃce", u8"ﬂower", u8"baﬄe", u8"ǅungla", u8"ǆak", u8"ﬃ", u8"straße", u8"ﬁancée"},
      {u8"file", u8"office", u8"flower", u8"baffle", u8"džungla", u8"džak", u8"ffi", u8"strasse", u8"fiancée"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#6 Font variants",
      filter,
      {u8"ℌello", u8"ℍotel", u8"𝔸𝕓𝕔", u8"𝖧𝖾𝗅𝗅𝗈", u8"𝛂θήνα", u8"𝚺igma", u8"𝐀𝐁𝐂", u8"𝘈𝘣𝘤"},
      {u8"hello", u8"hotel", u8"abc", u8"hello", u8"αθήνα", u8"σigma", u8"abc", u8"abc"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#7 Linebreaking differences",
      filter,
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"},
      {u8"foo bar", u8"ооо ромашка", u8"北京 大学", u8"12 34", u8"a b", u8"株式 会社"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#8 Positional variant forms",
      filter,
      {u8"ﻙ", u8"ﻚ", u8"ﻛ", u8"ﻜ", u8"ﺳ", u8"ﺴ"},
      {u8"ك", u8"ك", u8"ك", u8"ك", u8"س", u8"س"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#9 Circled variants",
      filter,
      {u8"①②③", u8"ⒶⒷⒸ", u8"ⓐⓑⓒ", u8"㉑㊷", u8"㊙㊣", u8"㋐㋑㋒"},
      {u8"123", u8"abc", u8"abc", u8"2142", u8"秘正", u8"アイウ"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#10 Width variants",
      filter,
      {u8"ＡＢＣ１２３", u8"ｔｅｓｔ　１２３", u8"ｶﾀｶﾅ", u8"ﾊﾟﾋﾟﾌﾟﾍﾟﾎﾟ", u8"ｶﾞｯﾂﾎﾟｰｽﾞ", u8"Ｘ線"},
      {u8"abc123", u8"test 123", u8"カタカナ", u8"パピプペポ", u8"ガッツポーズ", u8"x線"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#11 Rotated variants",
      filter,
      {u8"︷abc︸", u8"︷数据︸", u8"︷Ω︸", u8"︷ガ︸"},
      {u8"{abc}", u8"{数据}", u8"{ω}", u8"{ガ}"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#12 Superscripts/subscripts",
      filter,
      {u8"i⁹", u8"i₉", u8"H₂O", u8"CO₂", u8"x⁵+y⁷", u8"Na⁺"},
      {u8"i9", u8"i9", u8"h2o", u8"co2", u8"x5+y7", u8"na+"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#13 Squared characters",
      filter,
      {u8"㎏", u8"㍑", u8"㌀", u8"㏄", u8"㌖", u8"㍻"},
      {u8"kg", u8"リットル", u8"アパート", u8"cc", u8"キロメートル", u8"平成"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#14 Fractions",
      filter,
      {u8"½", u8"⅓", u8"⅝", u8"3½", u8"⅔杯"},
      {u8"1⁄2", u8"1⁄3", u8"5⁄8", u8"31⁄2", u8"2⁄3杯"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#15 CJK compatibility ideographs",
      filter,
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"},
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"});
}

TEST_F(FTICUNormalizer2FilterTest, nfkc_cf_decompose)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObICUNormalizer2FilterSpec spec(ObICUNormalizer2FilterSpec::Name::NFKC_CF,
                                  UNormalization2Mode::UNORM2_DECOMPOSE);

  ObICUNormalizer2Filter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#1 Combining sequence",
      filter,
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"},
      {u8"café", u8"café", u8"ångström", u8"ångström", u8"йогурт", u8"йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#2 Ordering of combining marks",
      filter,
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"},
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#3 Hangul & conjoining jamo",
      filter,
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"},
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#4 Singleton equivalence",
      filter,
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"},
      {u8"ω", u8"ω", u8"å", u8"å", u8"ωmega", u8"ωmega", u8"ångström", u8"ångström"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#5 Ligatures and multigraphs",
      filter,
      {u8"ﬁle", u8"oﬃce", u8"ﬂower", u8"baﬄe", u8"ǅungla", u8"ǆak", u8"ﬃ", u8"straße", u8"ﬁancée"},
      {u8"file", u8"office", u8"flower", u8"baffle", u8"džungla", u8"džak", u8"ffi", u8"strasse", u8"fiancée"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#6 Font variants",
      filter,
      {u8"ℌello", u8"ℍotel", u8"𝔸𝕓𝕔", u8"𝖧𝖾𝗅𝗅𝗈", u8"𝛂θήνα", u8"𝚺igma", u8"𝐀𝐁𝐂", u8"𝘈𝘣𝘤"},
      {u8"hello", u8"hotel", u8"abc", u8"hello", u8"αθήνα", u8"σigma", u8"abc", u8"abc"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#7 Linebreaking differences",
      filter,
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"},
      {u8"foo bar", u8"ооо ромашка", u8"北京 大学", u8"12 34", u8"a b", u8"株式 会社"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#8 Positional variant forms",
      filter,
      {u8"ﻙ", u8"ﻚ", u8"ﻛ", u8"ﻜ", u8"ﺳ", u8"ﺴ"},
      {u8"ك", u8"ك", u8"ك", u8"ك", u8"س", u8"س"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#9 Circled variants",
      filter,
      {u8"①②③", u8"ⒶⒷⒸ", u8"ⓐⓑⓒ", u8"㉑㊷", u8"㊙㊣", u8"㋐㋑㋒"},
      {u8"123", u8"abc", u8"abc", u8"2142", u8"秘正", u8"アイウ"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#10 Width variants",
      filter,
      {u8"ＡＢＣ１２３", u8"ｔｅｓｔ　１２３", u8"ｶﾀｶﾅ", u8"ﾊﾟﾋﾟﾌﾟﾍﾟﾎﾟ", u8"ｶﾞｯﾂﾎﾟｰｽﾞ", u8"Ｘ線"},
      {u8"abc123", u8"test 123", u8"カタカナ", u8"パピプペポ", u8"ガッツポーズ", u8"x線"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#11 Rotated variants",
      filter,
      {u8"︷abc︸", u8"︷数据︸", u8"︷Ω︸", u8"︷ガ︸"},
      {u8"{abc}", u8"{数据}", u8"{ω}", u8"{ガ}"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#12 Superscripts/subscripts",
      filter,
      {u8"i⁹", u8"i₉", u8"H₂O", u8"CO₂", u8"x⁵+y⁷", u8"Na⁺"},
      {u8"i9", u8"i9", u8"h2o", u8"co2", u8"x5+y7", u8"na+"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#13 Squared characters",
      filter,
      {u8"㎏", u8"㍑", u8"㌀", u8"㏄", u8"㌖", u8"㍻"},
      {u8"kg", u8"リットル", u8"アパート", u8"cc", u8"キロメートル", u8"平成"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#14 Fractions",
      filter,
      {u8"½", u8"⅓", u8"⅝", u8"3½", u8"⅔杯"},
      {u8"1⁄2", u8"1⁄3", u8"5⁄8", u8"31⁄2", u8"2⁄3杯"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#15 CJK compatibility ideographs",
      filter,
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"},
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"});
}

/*
TEST_F(FTICUNormalizer2FilterTest, folding)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObTokenAttr token;

  ObICUNormalizer2FilterSpec spec(ObTokenFilterType::TOKEN_FILTER_TYPE_ICU_FOLDING);
  ObICUNormalizer2Filter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#1 Combining sequence",
      filter,
      {u8"café", u8"café", u8"Ångström", u8"Ångström", u8"Йогурт", u8"Йогурт", u8"άλφα", u8"άλφα", u8"ガラス", u8"ガラス"},
      {u8"cafe", u8"cafe", u8"angstrom", u8"angstrom", u8"иогурт", u8"иогурт", u8"αλφα", u8"αλφα", u8"カラス", u8"カラス"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#2 Ordering of combining marks",
      filter,
      {u8"q̣̇", u8"q̣̇", u8"ạ̄", u8"ạ̄", u8"ṳ̂", u8"ṳ̂", u8"й̣", u8"й̣"},
      {u8"q", u8"q", u8"a", u8"a", u8"u", u8"u", u8"и", u8"и"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#3 Hangul & conjoining jamo",
      filter,
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"},
      {u8"가", u8"가", u8"한", u8"한", u8"한글", u8"한글", u8"값", u8"값"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#4 Singleton equivalence",
      filter,
      {u8"Ω", u8"Ω", u8"Å", u8"Å", u8"Ωmega", u8"Ωmega", u8"Ångström", u8"Ångström"},
      {u8"ω", u8"ω", u8"a", u8"a", u8"ωmega", u8"ωmega", u8"angstrom", u8"angstrom"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#5 Ligatures and multigraphs",
      filter,
      {u8"ﬁle", u8"oﬃce", u8"ﬂower", u8"baﬄe", u8"ǅungla", u8"ǆak", u8"ﬃ", u8"straße", u8"ﬁancée"},
      {u8"file", u8"office", u8"flower", u8"baffle", u8"dzungla", u8"dzak", u8"ffi", u8"strasse", u8"fiancee"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#6 Font variants",
      filter,
      {u8"ℌello", u8"ℍotel", u8"𝔸𝕓𝕔", u8"𝖧𝖾𝗅𝗅𝗈", u8"𝛂θήνα", u8"𝚺igma", u8"𝐀𝐁𝐂", u8"𝘈𝘣𝘤"},
      {u8"hello", u8"hotel", u8"abc", u8"hello", u8"αθηνα", u8"σigma", u8"abc", u8"abc"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#7 Linebreaking differences",
      filter,
      {u8"foo bar", u8"ООО Ромашка", u8"北京 大学", u8"12 34", u8"A B", u8"株式 会社"},
      {u8"foo bar", u8"ооо ромашка", u8"北京 大学", u8"12 34", u8"a b", u8"株式 会社"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#8 Positional variant forms",
      filter,
      {u8"ﻙ", u8"ﻚ", u8"ﻛ", u8"ﻜ", u8"ﺳ", u8"ﺴ"},
      {u8"ك", u8"ك", u8"ك", u8"ك", u8"س", u8"س"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#9 Circled variants",
      filter,
      {u8"①②③", u8"ⒶⒷⒸ", u8"ⓐⓑⓒ", u8"㉑㊷", u8"㊙㊣", u8"㋐㋑㋒"},
      {u8"123", u8"abc", u8"abc", u8"2142", u8"秘正", u8"アイウ"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#10 Width variants",
      filter,
      {u8"ＡＢＣ１２３", u8"ｔｅｓｔ　１２３", u8"ｶﾀｶﾅ", u8"ﾊﾟﾋﾟﾌﾟﾍﾟﾎﾟ", u8"ｶﾞｯﾂﾎﾟｰｽﾞ", u8"Ｘ線"},
      {u8"abc123", u8"test 123", u8"カタカナ", u8"ハヒフヘホ", u8"カッツホス", u8"x線"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#11 Rotated variants",
      filter,
      {u8"︷abc︸", u8"︷数据︸", u8"︷Ω︸", u8"︷ガ︸"},
      {u8"{abc}", u8"{数据}", u8"{ω}", u8"{カ}"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#12 Superscripts/subscripts",
      filter,
      {u8"i⁹", u8"i₉", u8"H₂O", u8"CO₂", u8"x⁵+y⁷", u8"Na⁺"},
      {u8"i9", u8"i9", u8"h2o", u8"co2", u8"x5+y7", u8"na+"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#13 Squared characters",
      filter,
      {u8"㎏", u8"㍑", u8"㌀", u8"㏄", u8"㌖", u8"㍻"},
      {u8"kg", u8"リットル", u8"アハト", u8"cc", u8"キロメトル", u8"平成"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#14 Fractions",
      filter,
      {u8"½", u8"⅓", u8"⅝", u8"3½", u8"⅔杯"},
      {u8"1/2", u8"1/3", u8"5/8", u8"31/2", u8"2/3杯"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#15 CJK compatibility ideographs",
      filter,
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"},
      {u8"神", u8"神", u8"豈", u8"豈", u8"車", u8"車"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#21 Dashes folding",
      filter,
      {u8"normal‐hyphen", u8"non‑breaking", u8"horizontal―bar", u8"minus−sign"},
      {u8"normal-hyphen", u8"non-breaking", u8"horizontal-bar", u8"minus-sign"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#22 Underscore folding",
      filter,
      {u8"double‗lowline", u8"centreline﹎lowline", u8"wavy﹏lowline"},
      {u8"double lowline", u8"centreline_lowline", u8"wavy_lowline"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#23 Punctuation folding",
      filter,
      {u8"reversed‵prime", u8"⁅squarebackets⁆", u8"fraction⁄dash", u8"under‸caret", u8"‸"},
      {u8"reversed'prime", u8"[squarebackets]", u8"fraction/dash", u8"undercaret"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#24 Dingbats folding",
      filter,
      {u8"⓵", u8"➀", u8"➊", u8"⓫", u8"❛heavycomma❜"},
      {u8"1", u8"1", u8"1", u8"11", u8"'heavycomma'"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#25 CJK radicals",
      filter,
      {u8"⺁", u8"⺂", u8"⻍", u8"⻗", u8"⻳"},
      {u8"厂", u8"乛", u8"辶", u8"雨", u8"龟"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "#26 Native digits",
      filter,
      {u8"١", u8"۲", u8"߃", u8"४", u8"৫", u8"੬"},
      {u8"1", u8"2", u8"3", u8"4", u8"5", u8"6"});

  {
    // empty output and pos inc
    MockTokenStream input({u8"café", u8"\u0301", u8"^", u8"가"});
    filter.set_input(&input);

    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string(u8"cafe"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(1, token.pos_inc_);

    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string(u8"가"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(3, token.pos_inc_);

    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));
    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));
  }
}
*/

TEST_F(FTICUNormalizer2FilterTest, error_cases)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObTokenAttr token;

  {
    // invalid spec
    ObICUNormalizer2FilterSpec invalid_spec(ObICUNormalizer2FilterSpec::Name::INVALID,
                                            UNormalization2Mode::UNORM2_COMPOSE);
    ObICUNormalizer2Filter filter;
    EXPECT_EQ(OB_INVALID_ARGUMENT, filter.init(invalid_spec, allocator));
  }

  {
    // init twice
    ObICUNormalizer2FilterSpec spec;
    ObICUNormalizer2Filter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    EXPECT_EQ(OB_INIT_TWICE, filter.init(spec, allocator));
  }

  {
    // use before init
    MockTokenStream input({u8"café"});
    ObICUNormalizer2Filter filter;
    EXPECT_EQ(OB_NOT_INIT, filter.get_next_token(token));
    filter.set_input(&input);
    EXPECT_EQ(OB_NOT_INIT, filter.get_next_token(token));
  }

  {
    // use after reset
    ObICUNormalizer2FilterSpec spec;
    ObICUNormalizer2Filter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    MockTokenStream input({u8"café", u8"𝐀𝐁𝐂"});
    filter.set_input(&input);
    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string(u8"café"), std::string(token.token_ptr_, token.token_len_));

    filter.reset();
    EXPECT_EQ(OB_NOT_INIT, filter.get_next_token(token));
  }

  {
    // get before set input
    ObICUNormalizer2FilterSpec spec;
    ObICUNormalizer2Filter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    EXPECT_EQ(OB_ERR_UNEXPECTED, filter.get_next_token(token));
  }
}

TEST_F(FTICUNormalizer2FilterTest, edge_cases_should_succeed)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObICUNormalizer2FilterSpec spec(ObICUNormalizer2FilterSpec::Name::NFC,
                                  UNormalization2Mode::UNORM2_COMPOSE);
  ObTokenAttr token;

  {
    // empty input stream
    ObICUNormalizer2Filter filter;
    MockTokenStream input({});
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    filter.set_input(&input);
    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));
  }

  {
    // exhaust stream
    ObICUNormalizer2Filter filter;
    MockTokenStream input({u8"café", u8"가"});
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    filter.set_input(&input);

    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string(u8"café"), std::string(token.token_ptr_, token.token_len_));

    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string(u8"가"), std::string(token.token_ptr_, token.token_len_));

    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));
    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));
  }

  {
    // reentrant reset
    ObICUNormalizer2Filter filter;
    filter.reset();
    filter.reset();

    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    MockTokenStream input({u8"Ångström"});
    filter.set_input(&input);

    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string(u8"Ångström"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));

    filter.reset();
    filter.reset();
  }

  {
    // two filters with different configs
    ObICUNormalizer2FilterSpec compose_spec(ObICUNormalizer2FilterSpec::Name::NFC,
                                            UNormalization2Mode::UNORM2_COMPOSE);
    ObICUNormalizer2Filter compose_filter;
    MockTokenStream compose_input({u8"café"});

    ObICUNormalizer2FilterSpec decompose_spec(ObICUNormalizer2FilterSpec::Name::NFC,
                                              UNormalization2Mode::UNORM2_DECOMPOSE);
    ObICUNormalizer2Filter decompose_filter;
    MockTokenStream decompose_input({u8"Å"});

    ASSERT_EQ(OB_SUCCESS, compose_filter.init(compose_spec, allocator));
    ASSERT_EQ(OB_SUCCESS, decompose_filter.init(decompose_spec, allocator));

    compose_filter.set_input(&compose_input);
    decompose_filter.set_input(&decompose_input);

    ASSERT_EQ(OB_SUCCESS, compose_filter.get_next_token(token));
    EXPECT_EQ(std::string(u8"café"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, compose_filter.get_next_token(token));

    ASSERT_EQ(OB_SUCCESS, decompose_filter.get_next_token(token));
    EXPECT_EQ(std::string(u8"Å"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, decompose_filter.get_next_token(token));
  }

  {
    // two filters with same config
    ObICUNormalizer2Filter filter1;
    ObICUNormalizer2Filter filter2;
    MockTokenStream input1({u8"ガラス"});
    MockTokenStream input2({u8"Ångström"});

    ASSERT_EQ(OB_SUCCESS, filter1.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, filter2.init(spec, allocator));

    filter1.set_input(&input1);
    filter2.set_input(&input2);

    ASSERT_EQ(OB_SUCCESS, filter1.get_next_token(token));
    EXPECT_EQ(std::string(u8"ガラス"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, filter1.get_next_token(token));

    ASSERT_EQ(OB_SUCCESS, filter2.get_next_token(token));
    EXPECT_EQ(std::string(u8"Ångström"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, filter2.get_next_token(token));
  }
}

TEST_F(FTICUNormalizer2FilterTest, memory_management)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObICUNormalizer2FilterSpec spec;

  std::string token1(10, 'a');
  std::string token2(120, 'b');
  std::string token3(300, 'c');
  std::string token4(700, 'd');
  std::string token5(1300, 'e');
  std::string token6(600, 'f');
  std::string token7(200, 'g');

  MockTokenStream input({
      token1.c_str(),
      token2.c_str(),
      token3.c_str(),
      token4.c_str(),
      token5.c_str(),
      token6.c_str(),
      token7.c_str(),
  });
  ObICUNormalizer2Filter filter;
  ObTokenAttr token;

  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
  filter.set_input(&input);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
  EXPECT_EQ(token1, std::string(token.token_ptr_, token.token_len_));

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
  EXPECT_EQ(token2, std::string(token.token_ptr_, token.token_len_));

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
  EXPECT_EQ(token3, std::string(token.token_ptr_, token.token_len_));

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
  EXPECT_EQ(token4, std::string(token.token_ptr_, token.token_len_));

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
  EXPECT_EQ(token5, std::string(token.token_ptr_, token.token_len_));

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
  EXPECT_EQ(token6, std::string(token.token_ptr_, token.token_len_));

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
  EXPECT_EQ(token7, std::string(token.token_ptr_, token.token_len_));

  EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));

  filter.reset();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
