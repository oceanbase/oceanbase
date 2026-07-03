/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "storage/fts/analyzer/tokenizer/ob_standard_tokenizer.h"
#include "unittest/storage/fts/test_analyzer_helper.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{

class FTStandardTokenizerTest : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    LOG_INFO("SetUpTestCase");
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    LOG_INFO("TearDownTestCase");
    MockTenantModuleEnv::get_instance().destroy();
  }
};

TEST_F(FTStandardTokenizerTest, basic_cases)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObStandardTokenizerSpec spec;

  ObStandardTokenizer tokenizer;
  ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "english",
      tokenizer,
      u8"In 2026, OceanBase released a practical tokenizer test suite for search workloads, covering email-style tokens, version numbers like v2.0, decimals such as 3.14159, and names like O'Reilly across several production scenarios.",
      {u8"In", u8"2026", u8"OceanBase", u8"released", u8"a", u8"practical", u8"tokenizer", u8"test",
       u8"suite", u8"for", u8"search", u8"workloads", u8"covering", u8"email", u8"style", u8"tokens",
       u8"version", u8"numbers", u8"like", u8"v2.0", u8"decimals", u8"such", u8"as", u8"3.14159",
       u8"and", u8"names", u8"like", u8"O'Reilly", u8"across", u8"several", u8"production", u8"scenarios"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "english 2",
      tokenizer,
      u8"The search team reviewed logs from New York, compared token counts between offline and online jobs, and verified that punctuation-heavy phrases did not unexpectedly merge during indexing.",
      {u8"The", u8"search", u8"team", u8"reviewed", u8"logs", u8"from", u8"New", u8"York",
       u8"compared", u8"token", u8"counts", u8"between", u8"offline", u8"and", u8"online", u8"jobs",
       u8"and", u8"verified", u8"that", u8"punctuation", u8"heavy", u8"phrases", u8"did", u8"not",
       u8"unexpectedly", u8"merge", u8"during", u8"indexing"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "french",
      tokenizer,
      u8"Le café du quartier reste célèbre pour son élève modèle, sa façade rénovée, et son rôle dans une étude sur le traitement linguistique des données textuelles.",
      {u8"Le", u8"café", u8"du", u8"quartier", u8"reste", u8"célèbre", u8"pour", u8"son", u8"élève",
       u8"modèle", u8"sa", u8"façade", u8"rénovée", u8"et", u8"son", u8"rôle", u8"dans", u8"une",
       u8"étude", u8"sur", u8"le", u8"traitement", u8"linguistique", u8"des", u8"données",
       u8"textuelles"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "german",
      tokenizer,
      u8"Die Straße vor dem großen Gebäude blieb trotz starker Änderungen im Verkehrsplan ein zentrales Beispiel für die Analyse deutscher Textdaten im Suchsystem.",
      {u8"Die", u8"Straße", u8"vor", u8"dem", u8"großen", u8"Gebäude", u8"blieb", u8"trotz",
       u8"starker", u8"Änderungen", u8"im", u8"Verkehrsplan", u8"ein", u8"zentrales", u8"Beispiel",
       u8"für", u8"die", u8"Analyse", u8"deutscher", u8"Textdaten", u8"im", u8"Suchsystem"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "spanish",
      tokenizer,
      u8"¿Cómo está el niño? El artículo describe el corazón de la ciudad, la música española, y la relación entre lenguaje, búsqueda y procesamiento de texto.",
      {u8"Cómo", u8"está", u8"el", u8"niño", u8"El", u8"artículo", u8"describe", u8"el", u8"corazón",
       u8"de", u8"la", u8"ciudad", u8"la", u8"música", u8"española", u8"y", u8"la", u8"relación",
       u8"entre", u8"lenguaje", u8"búsqueda", u8"y", u8"procesamiento", u8"de", u8"texto"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "vietnamese",
      tokenizer,
      u8"Tiếng Việt có nhiều dấu, ví dụ như hòa bình, Nguyễn, trường học và trách nhiệm, nên cần kiểm tra cả nội dung từ và số lượng mã điểm trong các câu dài gần với dữ liệu thật.",
      {u8"Tiếng", u8"Việt", u8"có", u8"nhiều", u8"dấu", u8"ví", u8"dụ", u8"như", u8"hòa",
       u8"bình", u8"Nguyễn", u8"trường", u8"học", u8"và", u8"trách", u8"nhiệm", u8"nên",
       u8"cần", u8"kiểm", u8"tra", u8"cả", u8"nội", u8"dung", u8"từ", u8"và", u8"số",
       u8"lượng", u8"mã", u8"điểm", u8"trong", u8"các", u8"câu", u8"dài", u8"gần", u8"với",
       u8"dữ", u8"liệu", u8"thật"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "indonesian",
      tokenizer,
      u8"Bahasa Indonesia digunakan dalam banyak dokumen teknis, sehingga pengujian perlu mencakup kata serapan, bentuk berimbuhan, dan istilah pencarian yang sering muncul di sistem nyata.",
      {u8"Bahasa", u8"Indonesia", u8"digunakan", u8"dalam", u8"banyak", u8"dokumen", u8"teknis",
       u8"sehingga", u8"pengujian", u8"perlu", u8"mencakup", u8"kata", u8"serapan", u8"bentuk",
       u8"berimbuhan", u8"dan", u8"istilah", u8"pencarian", u8"yang", u8"sering", u8"muncul",
       u8"di", u8"sistem", u8"nyata"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "malay",
      tokenizer,
      u8"Bahasa Melayu dalam bahan rujukan moden sering mengandungi e-mel, kerjasama, dan istilah pinjaman, jadi pemisahan token perlu diperiksa pada ayat yang lebih panjang.",
      {u8"Bahasa", u8"Melayu", u8"dalam", u8"bahan", u8"rujukan", u8"moden", u8"sering",
       u8"mengandungi", u8"e", u8"mel", u8"kerjasama", u8"dan", u8"istilah", u8"pinjaman",
       u8"jadi", u8"pemisahan", u8"token", u8"perlu", u8"diperiksa", u8"pada", u8"ayat", u8"yang",
       u8"lebih", u8"panjang"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "chinese",
      tokenizer,
      u8"OceanBase原生分布式数据库支持事务处理、分析处理与全文检索能力，并可在复杂业务场景中同时处理结构化与非结构化数据。",
      {u8"OceanBase", u8"原生", u8"分布", u8"式", u8"数据", u8"库", u8"支持", u8"事务", u8"处理",
       u8"分析", u8"处理", u8"与", u8"全文", u8"检索", u8"能力", u8"并", u8"可在", u8"复杂",
       u8"业务", u8"场景", u8"中", u8"同时", u8"处理", u8"结构", u8"化", u8"与", u8"非",
       u8"结构", u8"化", u8"数据"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "chinese mixed",
      tokenizer,
      u8"第1代数据库支持HTAP与AI搜索，并在2026年发布升级版本。",
      {u8"第", u8"1", u8"代", u8"数据", u8"库", u8"支持", u8"HTAP", u8"与", u8"AI", u8"搜索",
       u8"并", u8"在", u8"2026", u8"年", u8"发布", u8"升级", u8"版本"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "japanese",
      tokenizer,
      u8"私は検索システムの評価のために、全文検索、形態素解析、データベース性能、そして実運用に近い長めの文章をまとめて確認します。",
      {u8"私", u8"は", u8"検索", u8"システム", u8"の", u8"評価", u8"の", u8"ため", u8"に",
       u8"全文", u8"検索", u8"形態素", u8"解析", u8"データベース", u8"性能", u8"そして", u8"実",
       u8"運用", u8"に", u8"近い", u8"長め", u8"の", u8"文章", u8"を", u8"まとめ", u8"て",
       u8"確認", u8"し", u8"ます"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "japanese mixed",
      tokenizer,
      u8"日本語テキストとEnglishTokenが混在するケースを検証する",
      {u8"日本語", u8"テキスト", u8"と", u8"EnglishToken", u8"が", u8"混在", u8"する",
       u8"ケース", u8"を", u8"検証", u8"する"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "korean",
      tokenizer,
      u8"저는 실제 검색 서비스를 가정하고 데이터베이스, 전체 텍스트 검색, 성능 테스트, 그리고 운영 환경에 가까운 긴 문장을 함께 점검합니다.",
      {u8"저는", u8"실제", u8"검색", u8"서비스를", u8"가정하고", u8"데이터베이스", u8"전체",
       u8"텍스트", u8"검색", u8"성능", u8"테스트", u8"그리고", u8"운영", u8"환경에", u8"가까운",
       u8"긴", u8"문장을", u8"함께", u8"점검합니다"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "korean mixed",
      tokenizer,
      u8"검색엔진과 OceanBase 시스템을 함께 테스트한다",
      {u8"검색엔진과", u8"OceanBase", u8"시스템을", u8"함께", u8"테스트한다"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "thai",
      tokenizer,
      u8"ฉันกำลังทดสอบระบบค้นหาข้อความแบบเต็มในฐานข้อมูลจริงเพื่อดูว่าการตัดคำในประโยคยาวและคำที่ไม่มีเว้นวรรคจะสอดคล้องกับผลลัพธ์มาตรฐานหรือไม่",
      {u8"ฉัน", u8"กำลัง", u8"ทดสอบ", u8"ระบบ", u8"ค้นหา", u8"ข้อความ", u8"แบบ", u8"เต็ม",
       u8"ใน", u8"ฐาน", u8"ข้อมูล", u8"จริง", u8"เพื่อ", u8"ดู", u8"ว่าการ", u8"ตัด", u8"คำ",
       u8"ใน", u8"ประโยค", u8"ยาว", u8"และ", u8"คำ", u8"ที่", u8"ไม่มี", u8"เว้น", u8"วรรค",
       u8"จะ", u8"สอดคล้อง", u8"กับ", u8"ผลลัพธ์", u8"มาตรฐาน", u8"หรือ", u8"ไม่"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "cjk mixed",
      tokenizer,
      u8"中文English日本語한국어 mixed text with 123 numbers, OceanBase, UTF-8, and search tokens together.",
      {u8"中文", u8"English", u8"日本語", u8"한국어", u8"mixed", u8"text", u8"with", u8"123",
       u8"numbers", u8"OceanBase", u8"UTF", u8"8", u8"and", u8"search", u8"tokens", u8"together"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "multilingual",
      tokenizer,
      u8"OceanBase supports 中文检索, 日本語検索, 한국어 검색, Thai text segmentation, and multilingual ranking in one distributed search service.",
      {u8"OceanBase", u8"supports", u8"中文", u8"检索", u8"日本語", u8"検索", u8"한국어", u8"검색",
       u8"Thai", u8"text", u8"segmentation", u8"and", u8"multilingual", u8"ranking", u8"in",
       u8"one", u8"distributed", u8"search", u8"service"});
}

TEST_F(FTStandardTokenizerTest, emojis_and_punctuations)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObStandardTokenizerSpec spec;

  ObStandardTokenizer tokenizer;
  ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "single emoji",
      tokenizer,
      u8"😊",
      {u8"😊"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "multiple emojis",
      tokenizer,
      u8"😊😄😂🤣🥳",
      {u8"😊", u8"😄", u8"😂", u8"🤣", u8"🥳"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "flags",
      tokenizer,
      u8"🇨🇳🇺🇸🇯🇵🏴󠁧󠁢󠁥󠁮󠁧󠁿",
      {u8"🇨🇳", u8"🇺🇸", u8"🇯🇵", u8"🏴󠁧󠁢󠁥󠁮󠁧󠁿"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "ZWJ sequences",
      tokenizer,
      u8"👨‍👩‍👧‍👦👩🏾‍❤️‍💋‍👨🏼",
      {u8"👨‍👩‍👧‍👦", u8"👩🏾‍❤️‍💋‍👨🏼"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "keycap sequences 1",
      tokenizer,
      u8"1️⃣#️⃣*️⃣",
      {u8"1️⃣", u8"#️⃣", u8"*️⃣"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "keycap sequences 2",
      tokenizer,
      u8"1️⃣2️⃣3️⃣😄1️⃣#️⃣*️⃣",
      {u8"1️⃣2️⃣3️⃣", u8"😄", u8"1️⃣", u8"#️⃣", u8"*️⃣"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "emoji mixed",
      tokenizer,
      u8"Happy🇨🇳😊! Family emoji 👨‍👩‍👧‍👦 appears beside plain words and database terms.",
      {u8"Happy", u8"🇨🇳", u8"😊", u8"Family", u8"emoji",
       u8"👨‍👩‍👧‍👦", u8"appears", u8"beside", u8"plain", u8"words",
       u8"and", u8"database", u8"terms"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "emoji in chinese",
      tokenizer,
      u8"数据库😊搜索🚀能力🙂很好",
      {u8"数据", u8"库", u8"😊", u8"搜索", u8"🚀", u8"能力", u8"🙂", u8"很好"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "emoji in japanese",
      tokenizer,
      u8"検索😊性能🚀テストを行う",
      {u8"検索", u8"😊", u8"性能", u8"🚀", u8"テスト", u8"を", u8"行う"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "emoji and precomposed characters",
      tokenizer,
      u8"café 😊 résumé 👨‍👩‍👧‍👦",
      {u8"café", u8"😊", u8"résumé", u8"👨‍👩‍👧‍👦"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "english punctuations",
      tokenizer,
      u8"O'Reilly wrote e.g. notes about co-operate, re-enter, e-mail, Tom's book, Tom’s article, and 90's music.",
      {u8"O'Reilly", u8"wrote", u8"e.g", u8"notes", u8"about", u8"co",
       u8"operate", u8"re", u8"enter", u8"e", u8"mail", u8"Tom's",
       u8"book", u8"Tom’s", u8"article", u8"and", u8"90", u8"s",
       u8"music"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "number boundary",
      tokenizer,
      u8"v2.0 3.14159 1,234.56 X-86 1990s 90's 12:30",
      {u8"v2.0", u8"3.14159", u8"1,234.56", u8"X", u8"86", u8"1990s",
       u8"90", u8"s", u8"12", u8"30"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "chinese and english punctuations",
      tokenizer,
      u8"数据库，搜索；分词：tokenizer！English, Chinese，mixed punctuation.",
      {u8"数据", u8"库", u8"搜索", u8"分词", u8"tokenizer", u8"English",
       u8"Chinese", u8"mixed", u8"punctuation"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "whitespaces",
      tokenizer,
      u8"word1\tword2\nword3  word4",
      {u8"word1", u8"word2", u8"word3", u8"word4"});
}

TEST_F(FTStandardTokenizerTest, composite_and_compatibility_characters)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObStandardTokenizerSpec spec;

  ObStandardTokenizer tokenizer;
  ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "full-width and half-width",
      tokenizer,
      u8"ＡＢＣ １２３ ｔｅｓｔ test 中文１２３",
      {u8"ＡＢＣ", u8"１２３", u8"ｔｅｓｔ", u8"test", u8"中文", u8"１２３"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "precomposed characters",
      tokenizer,
      u8"café naïve Ångström élève über fiancée Málaga résumé",
      {u8"café", u8"naïve", u8"Ångström", u8"élève", u8"über", u8"fiancée",
       u8"Málaga", u8"résumé"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "decomposed_characters",
      tokenizer,
      u8"café naïve Ångström élève über fiancée Málaga résumé",
      {u8"café", u8"naïve", u8"Ångström", u8"élève", u8"über",
       u8"fiancée", u8"Málaga", u8"résumé"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "precomposed and decomposed letters",
      tokenizer,
      u8"é é Å Å ü ü ñ ñ ó ó",
      {u8"é", u8"é", u8"Å", u8"Å", u8"ü", u8"ü", u8"ñ",
       u8"ñ", u8"ó", u8"ó"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "precomposed and decomposed words",
      tokenizer,
      u8"résumé résumé café café Málaga Málaga fiancée fiancée",
      {u8"résumé", u8"résumé", u8"café", u8"café", u8"Málaga",
       u8"Málaga", u8"fiancée", u8"fiancée"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "ligatures",
      tokenizer,
      u8"ﬁle ﬁnish office file finish offline efficient traffic ﬂower ﬂight flow floor offline",
      {u8"ﬁle", u8"ﬁnish", u8"office", u8"file", u8"finish", u8"offline",
       u8"efficient", u8"traffic", u8"ﬂower", u8"ﬂight", u8"flow",
       u8"floor", u8"offline"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "composite and compatibility characters mixed",
      tokenizer,
      u8"ﬁancée café 😊 ﬁle résumé 👨‍👩‍👧‍👦",
      {u8"ﬁancée", u8"café", u8"😊", u8"ﬁle", u8"résumé", u8"👨‍👩‍👧‍👦"});
}

TEST_F(FTStandardTokenizerTest, max_token_length_cases)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);

  {
    ObStandardTokenizerSpec spec(8);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "ASCII characters",
        tokenizer,
        u8"internationalization localization characterization",
        {u8"internat", u8"ionaliza", u8"tion", u8"localiza",
        u8"tion", u8"characte", u8"rization"});
  }

  {
    ObStandardTokenizerSpec spec(10);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "english",
        tokenizer,
        u8"OceanBaseTokenizerVerificationNeedsConsistentBehaviorAcrossDifferentLanguageInputs",
        {u8"OceanBaseT", u8"okenizerVe", u8"rification", u8"NeedsConsi",
        u8"stentBehav", u8"iorAcrossD", u8"ifferentLa", u8"nguageInpu",
        u8"ts"});
  }

  {
    ObStandardTokenizerSpec spec(6);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "precomposed characters",
        tokenizer,
        u8"coöperation Ångströmstraße élèveüber fiancéerésumé",
        {u8"coöper", u8"ation", u8"Ångstr", u8"ömstra", u8"ße",
        u8"élèveü", u8"ber", u8"fiancé", u8"erésum", u8"é"});
  }

  {
    ObStandardTokenizerSpec spec(5);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "decomposed characters",
        tokenizer,
        u8"cafécafécafé naïvenaïve résumérésumé",
        {u8"café", u8"café", u8"café", u8"naïv", u8"enaï",
        u8"ve", u8"résu", u8"mére", u8"́sume", u8"́"});
  }

  {
    ObStandardTokenizerSpec spec(2);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "chinese",
        tokenizer,
        u8"中华人民共和国数据库系统全文搜索能力",
        {u8"中华", u8"人民", u8"共和", u8"国", u8"数据", u8"库",
        u8"系统", u8"全文", u8"搜索", u8"能力"});
  }

  {
    ObStandardTokenizerSpec spec(2);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "chinese 2",
        tokenizer,
        u8"分布式数据库全文检索事务处理混合负载能力验证",
        {u8"分布", u8"式", u8"数据", u8"库", u8"全文", u8"检索",
        u8"事务", u8"处理", u8"混合", u8"负载", u8"能力", u8"验证"});
  }

  {
    ObStandardTokenizerSpec spec(3);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "japanese",
        tokenizer,
        u8"データベースシステムトークナイザー検証",
        {u8"データ", u8"ベース", u8"システ", u8"ム", u8"トーク",
        u8"ナイ", u8"ザー", u8"検証"});
  }

  {
    ObStandardTokenizerSpec spec(3);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "japanese 2",
        tokenizer,
        u8"全文検索システムのトークナイザー動作確認",
        {u8"全文", u8"検索", u8"システ", u8"ム", u8"の", u8"トーク",
        u8"ナイ", u8"ザー", u8"動作", u8"確認"});
  }

  {
    ObStandardTokenizerSpec spec(5);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "korean",
        tokenizer,
        u8"데이터베이스시스템토크나이저검증",
        {u8"데이터베이", u8"스시스템토", u8"크나이저검", u8"증"});
  }

  {
    ObStandardTokenizerSpec spec(4);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "korean 2",
        tokenizer,
        u8"전체텍스트검색시스템토크나이저동작검증",
        {u8"전체텍스", u8"트검색시", u8"스템토크", u8"나이저동", u8"작검증"});
  }

  {
    ObStandardTokenizerSpec spec(5);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "thai",
        tokenizer,
        u8"การทดสอบระบบค้นหาข้อความแบบเต็มในฐานข้อมูล",
        {u8"การ", u8"ทดสอบ", u8"ระบบ", u8"ค้นหา", u8"ข้อคว",
        u8"าม", u8"แบบ", u8"เต็ม", u8"ใน", u8"ฐาน", u8"ข้อมู", u8"ล"});
  }

  {
    ObStandardTokenizerSpec spec(1);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "emojis",
        tokenizer,
        u8"😊😄😂🤣🥳",
        {u8"😊", u8"😄", u8"😂", u8"🤣", u8"🥳"});
  }

  {
    ObStandardTokenizerSpec spec(2);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "flags and zwj sequences",
        tokenizer,
        u8"🇨🇳🇺🇸👨‍👩‍👧‍👦",
        {u8"🇨🇳", u8"🇺🇸", u8"👨‍", u8"👩‍", u8"👧‍", u8"👦"});
  }

  {
    ObStandardTokenizerSpec spec(2);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "keycap sequences",
        tokenizer,
        u8"1️⃣2️⃣3️⃣",
        {u8"1️", u8"⃣2", u8"️⃣", u8"3️", u8"⃣"});
  }

  {
    ObStandardTokenizerSpec spec(4);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "mixed",
        tokenizer,
        u8"OceanBase分布式数据库😊😄search-tokenizer",
        {u8"Ocea", u8"nBas", u8"e", u8"分布", u8"式", u8"数据",
        u8"库", u8"😊", u8"😄", u8"sear", u8"ch", u8"toke",
        u8"nize", u8"r"});
  }

  {
    ObStandardTokenizerSpec spec(3);
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    FTAnalyzerTestHelper::assert_tokenizer_output(
        "multilingual",
        tokenizer,
        u8"中文English日本語한국어😊tokenizer",
        {u8"中文", u8"Eng", u8"lis", u8"h", u8"日本語", u8"한국어",
        u8"😊", u8"tok", u8"eni", u8"zer"});
  }
}

TEST_F(FTStandardTokenizerTest, error_cases)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObTokenAttr token;

  {
    // invalid spec
    ObStandardTokenizerSpec invalid_spec(-1);
    ObStandardTokenizer tokenizer;
    EXPECT_EQ(OB_INVALID_ARGUMENT, tokenizer.init(invalid_spec, allocator));
  }

  {
    // init twice
    ObStandardTokenizerSpec spec;
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    EXPECT_EQ(OB_INIT_TWICE, tokenizer.init(spec, allocator));
  }

  {
    // use before init
    ObStandardTokenizer tokenizer;
    EXPECT_EQ(OB_NOT_INIT, tokenizer.set_input(u8"hello", 5, CS_TYPE_UTF8MB4_BIN));
    EXPECT_EQ(OB_NOT_INIT, tokenizer.get_next_token(token));
  }

  {
    // invalid input arguments
    ObStandardTokenizerSpec spec;
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));

    EXPECT_EQ(OB_INVALID_ARGUMENT, tokenizer.set_input(nullptr, 0, CS_TYPE_UTF8MB4_BIN));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tokenizer.set_input(u8"hello", -1, CS_TYPE_UTF8MB4_BIN));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tokenizer.set_input(u8"hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI));
  }

  {
    // invalid utf8
    const char invalid_utf8[] = {
        'a',
        static_cast<char>(0xF0),
        static_cast<char>(0x28),
        static_cast<char>(0x8C),
        static_cast<char>(0x28),
        'b'};
    ObStandardTokenizerSpec spec;
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    EXPECT_NE(OB_SUCCESS,
              tokenizer.set_input(invalid_utf8, sizeof(invalid_utf8), CS_TYPE_UTF8MB4_BIN));
  }

  {
    // use after reset
    ObStandardTokenizerSpec spec;
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"hello", 5, CS_TYPE_UTF8MB4_BIN));
    ASSERT_EQ(OB_SUCCESS, tokenizer.get_next_token(token));

    tokenizer.reset();
    EXPECT_EQ(OB_NOT_INIT, tokenizer.get_next_token(token));
  }
}

TEST_F(FTStandardTokenizerTest, edge_cases_should_succeed)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObStandardTokenizerSpec spec;
  ObTokenAttr token;

  {
    // empty input
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"", 0, CS_TYPE_UTF8MB4_BIN));
    EXPECT_EQ(OB_ITER_END, tokenizer.get_next_token(token));
    EXPECT_EQ(OB_ITER_END, tokenizer.get_next_token(token));
  }

  {
    // exhaust stream
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"two tokens", 12, CS_TYPE_UTF8MB4_BIN));

    ASSERT_EQ(OB_SUCCESS, tokenizer.get_next_token(token));
    EXPECT_EQ(std::string(u8"two"), std::string(token.token_ptr_, token.token_len_));

    ASSERT_EQ(OB_SUCCESS, tokenizer.get_next_token(token));
    EXPECT_EQ(std::string(u8"tokens"), std::string(token.token_ptr_, token.token_len_));

    EXPECT_EQ(OB_ITER_END, tokenizer.get_next_token(token));
    EXPECT_EQ(OB_ITER_END, tokenizer.get_next_token(token));
  }

  {
    // overwrite input before emission
    ObStandardTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"first text", 5, CS_TYPE_UTF8MB4_BIN));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"second text", 6, CS_TYPE_UTF8MB4_BIN));

    ASSERT_EQ(OB_SUCCESS, tokenizer.get_next_token(token));
    EXPECT_EQ(std::string(u8"second"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, tokenizer.get_next_token(token));
  }

  {
    // reentrant reset
    ObStandardTokenizer tokenizer;
    tokenizer.reset();
    tokenizer.reset();

    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"reset ok", 8, CS_TYPE_UTF8MB4_BIN));
    ASSERT_EQ(OB_SUCCESS, tokenizer.get_next_token(token));
    EXPECT_EQ(std::string(u8"reset"), std::string(token.token_ptr_, token.token_len_));

    tokenizer.reset();
    tokenizer.reset();
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
