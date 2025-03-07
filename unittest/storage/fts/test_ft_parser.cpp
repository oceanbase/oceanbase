/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "share/ob_tenant_mem_limit_getter.h"
#include "storage/fts/dict/ob_ft_cache.h"
#include "storage/fts/dict/ob_ft_cache_dict.h"
#include "storage/fts/dict/ob_ft_dat_dict.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_hub.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
#include "storage/fts/dict/ob_ft_trie.h"
#include "storage/fts/dict/ob_ik_dic.h"
#include "storage/fts/ik/ob_ik_char_util.h"
#include "storage/fts/ik/ob_ik_token.h"
#include "storage/fts/ob_ik_ft_parser.h"

#include <alloca.h>
#include <chrono>
#include <gtest/gtest.h>
#include <iostream>
#include <ostream>
#include <string>
#include <unistd.h>
#include <vector>

#define USING_LOG_PREFIX STORAGE_FTS

using namespace oceanbase::plugin;

namespace oceanbase
{
namespace storage
{
// mock range dict
int ObFTRangeDict::build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  ObIKDictLoader::RawDict dict_text;
  switch (desc.type_) {
  case ObFTDictType::DICT_IK_MAIN: {
    dict_text = ObIKDictLoader::dict_text();
  } break;
  case ObFTDictType::DICT_IK_QUAN: {
    dict_text = ObIKDictLoader::dict_quen_text();
  } break;
  case ObFTDictType::DICT_IK_STOP: {
    dict_text = ObIKDictLoader::dict_stop();
  } break;
  default:
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Not supported dict type.", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObIKDictIterator iter(dict_text);
    if (OB_FAIL(iter.init())) {
      LOG_WARN("Failed to init iterator.", K(ret));
    } else if (OB_FAIL(ObFTRangeDict::build_ranges(desc, iter, range_container))) {
      LOG_WARN("Failed to build ranges.", K(ret));
    }
  }

  return ret;
}

class FTParserTest : public ::testing::Test
{
protected:
  FTParserTest() {}
  virtual ~FTParserTest() {}

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
  virtual void SetUp()
  {
    const int64_t KV_CACHE_WASH_TIMER_INTERVAL_US = 60 * 1000L * 1000L;
    const int64_t DEFAULT_BUCKET_NUM = 10000000L;
    const int64_t DEFAULT_MAX_CACHE_SIZE = 1024L * 1024L * 1024L * 1024L;
    int ret = OB_SUCCESS;
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
    if (OB_FAIL(ObKVGlobalCache::get_instance().init(&(ObTenantMemLimitGetter::get_instance()),
                                                     DEFAULT_BUCKET_NUM,
                                                     DEFAULT_MAX_CACHE_SIZE,
                                                     lib::ACHUNK_SIZE,
                                                     KV_CACHE_WASH_TIMER_INTERVAL_US))) {
      if (OB_INIT_TWICE == ret) {
        ret = OB_SUCCESS;
      }
    }
    ObDictCache::get_instance().init("dict cache");
  }
  virtual void TearDown()
  {
    ObDictCache::get_instance().destroy();
    ObKVGlobalCache::get_instance().destroy();
    ObClockGenerator::destroy();
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }
};

TEST_F(FTParserTest, test_cache)
{
  int ret = OB_SUCCESS;
  ObDictCache &cache = ObDictCache::get_instance();
  ObFTDAT dat;
  dat.mem_block_size_ = sizeof(ObFTDAT);
  ObFTDAT *ptr = &dat;

  ObDictCacheKey key(1, 1, ObFTDictType::DICT_IK_MAIN, 0);
  ObDictCacheValue value(ptr);
  ret = cache.put(key, value);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObKVCacheHandle handle;
  const ObDictCacheValue *new_value = nullptr;
  ret = cache.get(key, new_value, handle);

  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(new_value->dat_block_->mem_block_size_, dat.mem_block_size_);
}

TEST_F(FTParserTest, test_trie)
{
  std::vector<std::string> dict = {
      u8"啊",         u8"你好",     u8"你好啊",       u8"世界",
      "齐发",         "齐发式",     "齐名",           "齐名并价",
      "齐唱",         "齐国",       "齐国人",         "齐堂主",
      "齐声",         "齐声叫好",   "齐声欢唱",       "齐声高唱",
      "齐备",         "齐大非偶",   "齐大非耦",       "齐天",
      "齐天大",       "齐天大圣",   "齐天洪福",       "齐头",
      "齐头并进",     "齐头式",     "齐奏",           "齐宣王",
      "齐家",         "齐家治国",   "齐家治国平天下", "齐射",
      "齐山区",       "齐岳山",     "齐崭崭",         "齐心",
      "齐心一力",     "齐心併力",   "齐心协力",       "齐心合力",
      "齐心同力",     "齐心宝树王", "齐心并力",       "齐心戮力",
      "齐心涤虑",     "齐截",       "齐戳戳",         "齐手",
      "齐抓共管",     "齐揍",       "齐放",           "齐整",
      "齐整如一",     "齐文宣帝",   "齐明",           "齐明点",
      "齐格勒",       "齐桓公",     "齐楚",           "齐次图",
      "齐次坐标环",   "齐次多项式", "齐次积分方程",   "齐步",
      "齐步走",       "齐河",       "齐河县",         "齐洛诺夫",
      "齐烟九点",     "齐物",       "齐特拉琴",       "齐王",
      "齐王舍牛",     "齐用",       "齐白石",         "齐眉",
      "齐眉举案",     "齐眉棍",     "齐眉穗儿",       "齐秦",
      "齐纨鲁缟",     "齐纳二极管", "齐耳",           "齐聚",
      "齐聚一堂",     "齐肩",       "齐胸",           "齐腰",
      "齐自勉",       "齐藤",       "齐行",           "齐衰",
      "齐豫",         "齐趋并驾",   "齐足并驰",       "齐足并驱",
      "齐轨连辔",     "齐辉",       "齐达内",         "齐进",
      "齐量等观",     "齐金蝉",     "齐镳并驱",       "齐集",
      "齐首",         "齐驱并进",   "齐驱并驾",       "齐驱并骤",
      "齐鲁",         "齐鲁大地",   "齐鸣",           "齐齐哈尔",
      "齐齐哈尔市",   "齐齐整整",   "齑粉",           "齑身粉骨",
      "齧合",         "齧断",       "齧齿类",         "齰舌缄唇",
      "齿亡舌存",     "齿位",       "齿冠",           "齿冷",
      "齿剑如归",     "齿印",       "齿危发秀",       "齿唇音",
      "齿垢",         "齿如含贝",   "齿如编贝",       "齿如齐贝",
      "齿孔酸",       "齿寒",       "齿少心锐",       "齿少气锐",
      "齿弊舌存",     "齿形",       "齿接手",         "齿条",
      "齿根",         "齿槽",       "齿牙为猾",       "齿牙为祸",
      "齿牙之猾",     "齿牙余论",   "齿牙春色",       "齿牙馀惠",
      "齿牙馀慧",     "齿牙馀论",   "齿状",           "齿状回",
      "齿状物",       "齿甘乘肥",   "齿白",           "齿白唇红",
      "齿科",         "齿缝",       "齿耙",           "齿腔",
      "齿舌",         "齿舞",       "齿若编贝",       "齿落舌钝",
      "齿豁头童",     "齿质",       "齿轮",           "齿轮传动钻机",
      "齿轮厂",       "齿轮油",     "齿轮泵",         "齿轮箱",
      "齿轮试验机",   "齿轮轴",     "齿轴",           "齿过肩随",
      "齿间",         "齿音",       "齿颊挂人",       "齿颊生香",
      "齿髓",         "齿鲸",       "齿鸟类",         "齿龈",
      "齿龈炎",       "龃齿",       "龃龉",           "龃龉不合",
      "龄期",         "龄级",       "龅牙",           "龆年",
      "龇牙",         "龇牙咧嘴",   "龇牙裂嘴",       "龈上牙石",
      "龈下刮治器",   "龈下刮治术", "龈下牙石",       "龈乳头",
      "龈乳头炎",     "龈交",       "龈切除术",       "龈变性",
      "龈口炎",       "龈成形术",   "龈沟",           "龈沟液取样纸条",
      "龈沟液测量仪", "龈流血指数", "龈炎",           "龈炎指数",
      "龈牙纤维",     "龈牙结合部", "龈瓣",           "龈纤维瘤病多毛综合征",
      "龈袋",         "龈谷",       "龈齿弹舌",       "龈龈计较",
      "龋失补",       "龋失补牙",   "龋失补牙面",     "龋病学",
      "龋齿",         "龌龊",       "龌龊事",         "龙三",
      "龙与地下城",
  };

  // test simple match
  {
    int ret = 0;
    ObArenaAllocator allocator_(ObModIds::TEST);
    ObFTTrie<void> trie(allocator_, CS_TYPE_UTF8MB4_GENERAL_CI);

    ret = trie.insert(ObString("你好"), {});
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = trie.insert(ObString("你好吗"), {});
    ASSERT_EQ(OB_SUCCESS, ret);

    // all memory
    ObFTDATBuilder<void> builder(allocator_);
    ret = builder.init(trie);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = builder.build_from_trie(trie);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObFTDAT *mem = nullptr;
    size_t mem_len = 0;
    ret = builder.get_mem_block(mem, mem_len);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObFTDATReader<void> reader(mem);
    ObDATrieHit hit(nullptr, 0); // not used in reader
    reader.match_with_hit("你", hit, hit);
    ASSERT_TRUE(hit.is_prefix());
    reader.match_with_hit("好", hit, hit);
    ASSERT_TRUE(hit.is_match());
    ASSERT_TRUE(hit.is_prefix());
    reader.match_with_hit("吗", hit, hit);
    ASSERT_TRUE(hit.is_match());
    ASSERT_TRUE(hit.is_prefix());
    reader.match_with_hit("你", hit, hit);
    ASSERT_TRUE(hit.is_unmatch());

    ObFTCacheDict dict(CS_TYPE_UTF8MB4_GENERAL_CI, mem);
    ret = dict.init();
    ASSERT_EQ(OB_SUCCESS, ret);
    bool is_match = false;
    ret = dict.match("你好吗", is_match);
    ASSERT_EQ(OB_SUCCESS, ret);

    is_match = false;
    ret = dict.match("你好", is_match);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  {
    int ret = OB_SUCCESS;
    std::vector<std::string> words;
    for (int i = 0; i < 100; i++) {
      words.push_back(std::to_string(i));
    }

    ObArenaAllocator allocator(ObModIds::TEST);
    size_t size = ObArrayHashMap::estimate_size(words.size());
    ObArrayHashMap *map = static_cast<ObArrayHashMap *>(allocator.alloc(size));
    map->init(words.size());
    for (int i = 0; i < words.size(); i++) {
      ObString str(words[i].size(), words[i].data());
      ObFTWordCode code = i;
      ret = map->insert(str, code);
    }

    for (int i = 0; i < words.size(); i++) {
      ObString str(words[i].size(), words[i].data());
      ObFTWordCode code;
      ObFTSingleWord word;
      word.set_word(str.ptr(), str.length());
      ret = map->find(word, code);
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_EQ(code, i);
    }
  }

  // test given dict
  {
    int ret = 0;
    ObArenaAllocator allocator(ObModIds::TEST);
    ObFTTrie<void> trie(allocator, CS_TYPE_UTF8MB4_GENERAL_CI);

    for (std::string &words : dict) {
      ret = trie.insert(ObString(words.length(), words.c_str()), {});
      ASSERT_EQ(ret, OB_SUCCESS);
    }

    ObFTDATBuilder<void> builder(allocator);
    ret = builder.init(trie);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = builder.build_from_trie(trie);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObFTDAT *mem = nullptr;
    size_t mem_len = 0;
    ret = builder.get_mem_block(mem, mem_len);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObFTCacheDict cache_dict(CS_TYPE_UTF8MB4_GENERAL_CI, mem);

    for (std::string &words : dict) {
      bool is_match = false;
      ret = cache_dict.match(ObString(words.length(), words.c_str()), is_match);
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_TRUE(is_match);
    }
  }
}

TEST_F(FTParserTest, IK_LLT)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc(ObModIds::TEST);
  ObFTDictHub hub;
  hub.init();

  auto case_insensitive_equal = [](const std::string &a, const std::string &b) {
    bool ret
        = a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](char a, char b) {
            return std::tolower(a) == std::tolower(b);
          });
    if (!ret) {
      std::cout << "compare [" << a << "] and [" << b << "]";
    }
    return ret;
  };

  auto ik_test = [&](char *word, int32_t len, bool smart, std::vector<std::string> expect) {
    ObArenaAllocator allocator(ObModIds::TEST);

    ObCharsetInfo cs;
    cs.name = OB_UTF8MB4_BIN;
    ObFTParserParam param;
    param.cs_ = &cs;
    ObITokenIterator *iter = nullptr;
    param.allocator_ = &allocator;
    // param.cs_ = CS_TYPE_UTF8MB4_BIN;
    param.fulltext_ = word;
    param.ft_length_ = len;
    param.parser_version_ = 1;
    param.ik_param_.mode_ = smart ? ObFTIKParam::Mode::SMART : ObFTIKParam::Mode::MAX_WORD;

    ObIKFTParser parser(allocator, &hub);
    ret = parser.init(param);
    ASSERT_EQ(OB_SUCCESS, ret);

    const char *output_word;
    int64_t word_len;
    int64_t offset;
    int64_t char_cnt;
    int64_t word_freq;

    int i = 0;

    while (OB_SUCC(ret)) {
      if (OB_FAIL(parser.get_next_token(output_word, word_len, char_cnt, word_freq))) {
        if (OB_ITER_END != ret) {
          std::cout << "Failed to get next token" << static_cast<int>(ret) << "\n";
        }
        break;
      } else {
      }

      std::string actual(output_word, word_len);
      ASSERT_TRUE(case_insensitive_equal(actual, expect[i]));
      i++;
    }
    ASSERT_EQ(i, expect.size());
  };

  {
    char test_str[] = u8"hello world";
    std::vector<std::string> expect = {u8"hello", u8"world"};
    ik_test(test_str, sizeof(test_str), true, expect);
  }

  {
    char test_str[] = u8"hello world";
    std::vector<std::string> expect = {u8"hello", u8"world"};
    ik_test(test_str, sizeof(test_str), false, expect);
  }

  {
    char test_str[] = u8"20多人 20几万";
    std::vector<std::string> expect = {u8"20", u8"多人", u8"20", u8"几万"};
    ik_test(test_str, sizeof(test_str), true, expect);
  }

  {
    char test_str[] = u8"20多人 20几万";
    std::vector<std::string> expect = {u8"20", u8"多人", u8"20", u8"几万", u8"万"};
    ik_test(test_str, sizeof(test_str), false, expect);
  }

  {
    char test_str[] = u8"一九九五年12月31日";
    std::vector<std::string> expect = {u8"一九九五", u8"年", u8"12月", u8"31日"};
    ik_test(test_str, sizeof(test_str), true, expect);
  }

  {
    char test_str[] = u8"-2e-12 xxxx1E++300/++";
    std::vector<std::string> expect = {u8"2e-12", u8"xxxx1e++300"};
    ik_test(test_str, sizeof(test_str), true, expect);
  }

  {
    char test_str[] = u8"中华人民共和国人民大会堂";
    std::vector<std::string> expect = {u8"中华人民共和国", u8"人民大会堂"};
    ik_test(test_str, sizeof(test_str), true, expect);
  }

  {
    char test_str[] = u8"古田县城关六一四路四百零五号";
    std::vector<std::string> expect = {u8"古田县", u8"城关", u8"六一四", u8"路", u8"四百零五号"};
    ik_test(test_str, sizeof(test_str), true, expect);
  }

  {
    char test_str[] = u8"作者博客：www.baidu.com  电子邮件地址：squarious@gmail.com";
    std::vector<std::string> expect = {
        u8"作者",
        u8"博客",
        u8"www.baidu.com",
        u8"电子",
        u8"邮件地址",
        u8"squarious@gmail.com",
    };
    ik_test(test_str, sizeof(test_str), true, expect);
  }

  {
    char test_str[] = u8"作者博客：www.baidu.com  电子邮件地址：squarious@gmail.com";
    std::vector<std::string> expect = {
        u8"作者",
        u8"博客",
        u8"www.baidu.com",
        u8"www",
        u8"baidu",
        u8"com",
        u8"电子邮件",
        u8"电子",
        u8"邮件地址",
        u8"邮件",
        u8"地址",
        u8"squarious@gmail.com",
        u8"squarious",
        u8"gmail",
        u8"com",
    };
    ik_test(test_str, sizeof(test_str), false, expect);
  }

  {
    char test_str[]
        = u8"神话电视连续剧  20092008년을 마무리 할까 합니다  右のテキストエリアに訳文が にちほん ";
    std::vector<std::string> expect
        = {u8"神话", u8"电视连续剧", u8"20092008", u8"년", u8"을", u8"마", u8"무", u8"리",
           u8"할",   u8"까",         u8"합",       u8"니", u8"다", u8"右", u8"の", u8"テ",
           u8"キ",   u8"ス",         u8"ト",       u8"エ", u8"リ", u8"ア", u8"に", u8"訳",
           u8"文",   u8"が",         u8"に",       u8"ち", u8"ほ", u8"ん"};
    ik_test(test_str, sizeof(test_str), true, expect);
  }

  {
    char test_str[] = u8"据路透社报道，印度尼西亚社会事务部一官员星期二(29日)"
                      u8"表示，日惹市附近当地时间27日晨5时53分发生的里氏6."
                      u8"2级地震已经造成至少5427人死亡，20000余人受伤，近20万人无家可归。";

    std::vector<std::string> expect
        = {u8"据",     u8"路透社",   u8"报道",  u8"印度尼西亚", u8"社会", u8"事务部", u8"一",
           u8"官员",   u8"星期二",   u8"29日",  u8"表示",       u8"日",   u8"惹",     u8"市",
           u8"附近",   u8"当地时间", u8"27日",  u8"晨",         u8"5时",  u8"53分",   u8"发生",
           u8"的",     u8"里氏",     u8"6.2级", u8"地震",       u8"已经", u8"造成",   u8"至少",
           u8"5427人", u8"死亡",     u8"20000", u8"余人",       u8"受伤", u8"近",     u8"20",
           u8"万人",   u8"无家可归"};
    ik_test(test_str, sizeof(test_str), true, expect);
  }

  {
    char test_str[]
        = u8"中华人民共和国人民大会堂有16人在唱《hello world》，十里相送 一百二十个人 1.2立方米";
    std::vector<std::string> expect = {u8"中华人民共和国",
                                       u8"人民大会堂",
                                       u8"有",
                                       u8"16人",
                                       u8"在唱",
                                       u8"hello",
                                       u8"world",
                                       u8"十里",
                                       u8"相送",
                                       u8"一百二十",
                                       u8"个人",
                                       u8"1.2立方米"};
    ik_test(test_str, sizeof(test_str), true, expect);
  }
}

TEST_F(FTParserTest, test_char_util)
{
  const char *eng = "ab";
  const char *cjk = u8"你好啊，世界！";

  ObFTCharUtil::CharType type;
  ObFTCharUtil::classify_first_char(common::CS_TYPE_UTF8MB4_BIN, cjk, 3, type);
  ASSERT_EQ(type, ObFTCharUtil::CharType::CHINESE);
  ObFTCharUtil::classify_first_char(common::CS_TYPE_UTF8MB4_BIN, eng, 1, type);
}

TEST_F(FTParserTest, test_lex_container)
{
  char buf[] = "abcdefghijklmnopqrstuvwxyz";
  int ret;

  auto gen_lexeme = [&](int from, int to) {
    ObIKToken lex;
    lex.length_ = to - from;
    lex.offset_ = from;
    lex.ptr_ = buf;
    return lex;
  };

  {
    // add repeated lexeme
    ObArenaAllocator alloc;
    ObFTSortList list(alloc);
    ret = list.add_token(gen_lexeme(1, 4));
    ASSERT_EQ(ret, OB_SUCCESS);

    ret = list.add_token(gen_lexeme(1, 4));
    ASSERT_EQ(ret, OB_SUCCESS);

    ASSERT_EQ(list.tokens().size(), 1);
  }
  {
    // making sure the lexeme is correctly inserted.
    // 1-3 6-9 11-15 10-16
    ObArenaAllocator alloc;
    ObFTSortList list(alloc);

    ret = list.add_token(gen_lexeme(1, 3));
    ASSERT_EQ(ret, OB_SUCCESS);

    ret = list.add_token(gen_lexeme(6, 9));
    ASSERT_EQ(ret, OB_SUCCESS);

    ret = list.add_token(gen_lexeme(11, 15));
    ASSERT_EQ(ret, OB_SUCCESS);

    ret = list.add_token(gen_lexeme(10, 16));
    ASSERT_EQ(ret, OB_SUCCESS);

    ASSERT_EQ(list.tokens().size(), 4);

    ObFTSortList::CellIter iter = list.tokens().begin();
    ASSERT_EQ(iter->offset_, 1);
    ASSERT_EQ(iter->length_, 2);

    iter++;
    ASSERT_EQ(iter->offset_, 6);
    ASSERT_EQ(iter->length_, 3);

    iter++;
    ASSERT_EQ(iter->offset_, 10);
    ASSERT_EQ(iter->length_, 6);

    iter++;
    ASSERT_EQ(iter->offset_, 11);
    ASSERT_EQ(iter->length_, 4);
  }

  {
    ObArenaAllocator alloc;
    ObIKTokenChain path(alloc);

    bool is_add = false;
    ret = path.add_token_if_conflict(gen_lexeme(1, 4), is_add);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_TRUE(is_add);

    ret = path.add_token_if_conflict(gen_lexeme(2, 5), is_add);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_TRUE(is_add);

    // should not add
    ret = path.add_token_if_conflict(gen_lexeme(6, 7), is_add);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_FALSE(is_add);
  }
  {
    bool is_add = false;
    ObArenaAllocator alloc;
    ObIKTokenChain path(alloc);

    ret = path.add_token_if_no_conflict(gen_lexeme(1, 4), is_add);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_TRUE(is_add);

    ret = path.add_token_if_no_conflict(gen_lexeme(5, 8), is_add);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_TRUE(is_add);
    ASSERT_EQ(path.payload(), 6);
  }
  {
    // [1,4) [4,5) is not cross
    ObArenaAllocator alloc;
    ObIKTokenChain path(alloc);
    bool is_add = false;

    ret = path.add_token_if_no_conflict(gen_lexeme(1, 4), is_add);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_TRUE(is_add);

    ret = path.add_token_if_no_conflict(gen_lexeme(4, 5), is_add);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_TRUE(is_add);
  }
}
TEST_F(FTParserTest, test_ik) { int ret = OB_SUCCESS; }

// smart mode
// Time cost pertime: 6ms.
// Run times: 5000.
// Words(by char):12707.

// max mode
// Time cost pertime: 6ms.
// Run times: 5000.
// Words(by char):12707.
TEST_F(FTParserTest, DISABLED_benchmark)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator alloc(ObModIds::TEST);
  ObFTDictHub hub;
  hub.init();

  auto case_insensitive_compare = [](const std::string &a, const std::string &b) {
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](char a, char b) {
             return std::tolower(a) == std::tolower(b);
           });
  };

  auto ik_tokenize = [&](char *word, int32_t len, bool smart) {
    ObArenaAllocator allocator(ObModIds::TEST);

    ObCharsetInfo cs;
    cs.name = OB_UTF8MB4_BIN;
    ObFTParserParam param;
    param.cs_ = &cs;
    ObITokenIterator *iter = nullptr;
    param.allocator_ = &allocator;
    // param.cs_ = CS_TYPE_UTF8MB4_BIN;
    param.fulltext_ = word;
    param.ft_length_ = len;
    param.parser_version_ = 1;
    param.ik_param_.mode_ = smart ? ObFTIKParam::Mode::SMART : ObFTIKParam::Mode::MAX_WORD;

    ObIKFTParser parser(allocator, &hub);
    ret = parser.init(param);
    ASSERT_EQ(OB_SUCCESS, ret);

    const char *output_word;
    int64_t word_len;
    int64_t offset;
    int64_t char_cnt;
    int64_t word_freq;

    int i = 0;

    while (OB_SUCC(ret)) {
      if (OB_FAIL(parser.get_next_token(output_word, word_len, char_cnt, word_freq))) {
        if (OB_ITER_END != ret) {
          std::cout << "Failed to get next token" << static_cast<int>(ret) << "\n";
        }
        break;
      }
    }
    allocator.reset();
  };

  char test_str[]
      = u8"18岁那年，有个自称算命先生看了我的手相后说，此生你将注定与男人纠缠不清。我说怎么可能，"
        u8"我不漂亮，也无贪欲。我不想要太多，一生只想爱一次，只要一个爱我的丈夫，然后我是他的好妻"
        u8"子。为他做饭、洗衣带孩子。我要和他相伴到老。算命先生还说我曾有快乐的童年，但这说明不了"
        u8"什么，未来每一天都在变，没有长久的苦难，当然也没有长久的幸福。听到这话的时候，我感觉自"
        u8"己从里到外开始发冷。那么多年，我一直企图摆脱这个咒语一样的预言，却总是徒劳无功。我碰到"
        u8"过很多男人，不是他爱我我不爱他就是我爱他而他不爱我，还有就是我们彼此相爱却因为有缘无份"
        u8"而不得不分开。他们都说我是好姑娘，结果是我至今仍孑然一身。23岁的时候，我经历了第一场爱"
        u8"情的失败。那是我的初恋，他叫钟建。我们分手的时候也是这样的春天。我还清晰地记得，我们坐"
        u8"在江边的茶园里。我们都不敢看对方的眼睛。我的眼睛四处逡巡。我看见柳树发芽了，鹅黄的叶子"
        u8"在阳光下快乐地疯长。河里有很多垃圾漂过，河堤上有情侣在接吻。茶叶一根根笔直地站立在水中"
        u8"，这是上好的绿茶。我想起刚认识钟建的那一年那个算命先生的话，我想这是不是就是一切纠缠和"
        u8"苦难的开始呢，我很害怕。和钟建分手一年后，我来到了现在的这座城市。我曾在这个城市读了四"
        u8"年大学，我像熟悉我的家乡一样熟悉这座城市。最重要的是我最好的朋友邓澜在这座城市。我做了"
        u8"电台DJ，一档深夜音乐节目。从此，我用声音和文字与世界交流。我居无定所，像无根的池萍一样"
        u8"在这个城市的四处飘荡。一年内我搬了五次家。从这个城市的南边搬到北边，再从北边搬到西边然"
        u8"后东边。我恨极了这种漂泊流浪没有尽头没有希望的日子。我每天晚上十点半出门上班，十二点下"
        u8"班。我像幽灵一样穿越这个城市的夜晚。我化很精致的妆，穿很漂亮的衣服。像人们清晨出门那样"
        u8"。有时候我会去酒吧坐一坐，更多的时候我下班就回家上网。我买了一台二手联想电脑，很破却已"
        u8"经足够我夜深人静的时候上线游荡。我每天准时凌晨一点上网，然后在各个BBS之间游荡，写写看"
        u8"看，停停走走。我很少仔细看贴子，走马观花逛完一圈的时候天就亮。我在节目里侃侃而谈，实际"
        u8"上我勤于思考却拙于表达，我总是不知道该用怎样的词语才能恰当地表达出自己的起初想法。我在"
        u8"网上认识人不多，其中有一个叫野鬼。他说他是孤魂野鬼，只在夜晚出没。碰到他的时候我叫幽冥"
        u8"。他说女孩子不应该取我这们的名字，我笑就，因为我也只在夜晚活动。我们从不问对方是干什么"
        u8"的，我只是滔滔不绝地对他诉说，说我的生活，说我喜欢自己自己的声音在这个城市的夜里四处散"
        u8"布，说我陷在绝望的爱情里找不到出路，还说我希望有一座房子，面朝大海，春暖花开。他总是安"
        u8"静地听我说话，无论我怎么思维混乱、言辞颠倒他也从不怀疑我是否在撒谎。我喜欢相互信任，即"
        u8"使是在网上。我叫他野鬼，可他从不叫我幽冥，他叫我丫头，我们的称呼常常会打动我的心，但那"
        u8"只限于夜晚。白天我是一个坚硬冷漠的人，我甚至从不在白天上网。那个叫野鬼的人从不问我为什"
        u8"么，只是对我说，我不该是一个缺少阳光的女子，我令他心疼。无论我对野鬼说过 "
        u8"只作为一个符号存储在我的电脑里。关掉电脑以后，他就像空气一样立刻从我的视野和脑海里消失"
        u8"，甚至于一场小小的病毒也会让他从此不再出现。放映《花样年华》后，我曾经对他说他是我的“"
        u8"树洞”，他沉默数秒后表示反对。他说“树洞”是没有感情和生命的，而他有。上大学的时候老师告"
        u8"诉我们所谓悲剧就是人类自己将美好的东西撕碎给人类看。我一直记得这句话，我想努力制造一个"
        u8"喜剧，却不小心把这个喜剧撕碎了，成了悲剧。邓澜说我把自己的生活搞得乱七八糟，该有个人来"
        u8"照顾我，还说如果我再不嫁就没人要了，那口气和我爸妈一样。我说好啊，那你给我介绍个好人吧"
        u8"，好要有足够的胸怀愿意收留我。其实，那时候我也就26岁，比起30岁的现在来说还算得上是花样"
        u8"年华。第一次见到江凯文是在培根路的1812酒吧，我和邓澜一进门就看见坐在吧台的江凯文，他一"
        u8"个人在喝闷酒。邓澜为我们作了介绍，虽是第一次见面，其实我们都早已在对方心里盘踞。有很多"
        u8"次，邓澜对我欲言又止。邓澜欲言又止的是江凯文是个离异的男人，有一个小孩，和她妈妈在一起"
        u8"。邓澜觉得这对我来说有些不公。我不在乎这些。我们相爱了。第一次感到和一个人心有灵犀的默"
        u8"契是这样幸福。我想，我会一辈子爱他，我不要再让忧郁溢满他的眼神。我也相信他爱我一如我爱"
        u8"他。然后，我开始小心翼翼地经营这份爱情。他不喜欢我做电台的工作，我听他的话换了一份朝九"
        u8"晚五的工作。每天做好晚饭等他回家。我戒了网，彻底忘记那个叫野鬼的人。我希望我的爱可以抚"
        u8"平他心里的伤口。一切原本都是好好的。如果我不说结婚的话，可能一切都不会发生。和江凯文相"
        u8"恋一年的时候，我想结婚了。我并不是想要一个所谓老婆的名分，我从来都觉得婚姻束缚不了两个"
        u8"不再相爱了的人，没了爱，婚姻又要来干什么。我只是想为他生个孩子，给他一个完整的家。我说"
        u8"出结婚的想法以后，江凯文就消失了。我去他上班的地方找他，他避而不见。我给他打电话，我说"
        u8"我错了，我不要你娶我，我只要我们在一起。他在电话里沉默不语。我想不明白凯文为什么这样惧"
        u8"怕婚姻。我每天神思恍惚，晚上回家总是胡思乱想，我迅速地憔悴下去。我到1812去喝酒，这是我"
        u8"和凯文第一次见面的地方。誓言还在耳边，一切却已经改变。我喝了很多酒。我看见有一个头盖骨"
        u8"在酒柜里，好像曾经被摔碎过，胶布像绷带一样缠满了整个头颅。我叫来服务生，我想知道为什么"
        u8"他们为什么会放一个头骨在这里，我还想知道这个头骨生前是男是女，他（她）是否也曾有过悲伤"
        u8"的爱情。服务生说这是老板从华西医院拿来的头骨，生前是一个非常漂亮的女孩，只在这个世上呆"
        u8"了20年。万圣节的时候，客人玩得太疯，摔到地上碎了，所以用胶布粘起来。我说她一定很疼了，"
        u8"你们把她放在这里，她会嫌吵的。服务生说，也许她就喜欢这种生活呢，夜夜笙歌多好啊。我说你"
        u8"们怎么可以这样啊，我拉住服务生一定要他说这个薄的女孩生前有没有过刻骨铭心的爱，我一杯接"
        u8"一杯地喝酒。这时，邓澜和凯文一起出现在我的面前。凯文冲过来抱住我，不停地说对不起对不起"
        u8"，他温热的泪滴在我的脸上和我的泪混在一起，我又感到了他的温度，这让我温暖。我哭着说我不"
        u8"能没有他。我可以不要婚姻，不要名分，什么都不要，只要他别离开我。他终于说出他的苦衷。他"
        u8"以前在部队的时候曾摔断过颈椎，虽然现在好了，但是随时有可能得发导致瘫痪，他不要我为了他"
        u8"受苦。我说生生死死我都要和他在一起，无论贫穷，疾病。除非他不再爱我了。他说傻丫头，我怎"
        u8"么可能不爱你呢，没有你我活着还有什么意思。失而复得的爱情让我觉得多年的坎坷其实算不了什"
        u8"么，幸福一定会属于我，只要我不放弃。凯文对我很好，他似乎也对这份有过波折的爱倍加珍惜。"
        u8"我以为日子可以就这样一直过下去，结婚对我来说已经不重要，重要的是我和凯文在一起。而且，"
        u8"我们彼此相爱。不知道是不是老天故意和我开玩笑。一个月后，凯文又消失了。我满世界找他，疯"
        u8"了一样。我相信他一定有什么苦衷，不然他决不会突然失踪。我求邓澜帮我找到他。我蜷缩在家里"
        u8"，不吃不喝等他的电话。很久以后，在我快要死去的时候，凯文从西藏打回电话说他不能给我一个"
        u8"家，他很穷，没有自己的房子他不会娶我。我立刻倾尽所有买了一套房子。我说我们有自己的房子"
        u8"了，你快回来啊。他回来了，却不愿意再回我身边。他瘦了很多，我相信这些日子里他也常常思念"
        u8"我，不然不会这么消瘦。我忽然发现我根本不懂凯文，在一起一年，一直以为我们了解对方就像了"
        u8"解自己一样。一直相信有了爱其他一切都不重要了。而此刻，我才发现我完全不知道凯文在想什么"
        u8"，他要的又是什么。我感到悲哀。我每天幽灵一样游荡在这个城市的大街小巷，我想我该忘记这个"
        u8"男人，这个多变的男人。我走在路上的时候就像梦游一样，好几次差点被车撞到。有好几次，我恍"
        u8"惚看见凯文在跟踪我，我想一定是我看花眼了。同事结婚，赶去祝福。坐在角落里翻看他们的婚纱"
        u8"照，心里悲凉无比。有个客人走过来对我说，“咦，怎么凯文不上来啊？我看他在楼下已经站了半"
        u8"个小时了，很焦躁的样子。我还以为他在等你呢。”我个人并不知道我和凯文之间后来发生的那些"
        u8"事。我站起来就往楼下冲，只看到楼下一地的烟蒂。我因此确信凯文依然是爱我的，只要他爱我无"
        u8"论发生过什么我都会原谅他。关于这段故事，我已经不想再叙述。凯文最终没有成为我相位终身的"
        u8"爱人。他曾经是爱我的，这毋庸置疑。他第二次离开我的原因是他遇到了他的初恋情人。她一直是"
        u8"他心里的痛，暗恋多年却不敢表达。后来女孩出国，他也结婚生子。现在女孩回来了，三十多岁的"
        u8"女人风采依然，而且一直单身。凯文自从在街上看到她的第一眼就认定她才是他生命里的天使，而"
        u8"此刻的他又是自由身，他相信这是上天安排的缘分，于是，他离开我，开始疯狂地追求她。这是他"
        u8"在一次醉酒后告诉邓澜的。他还说我是一个好女孩，他不能欺骗我。看见我一天天憔悴下去，他也"
        u8"很心疼，他怕我想不开所以才跟踪我。听到这番话的时候，我觉得自己真是滑稽。居然为了这么一"
        u8"个男人如此消瘦。他以为没了他，我会自杀，我不会这么蠢，为了一个不爱自己的男人而折磨自己"
        u8"。早知道他离开我的原因，我甚至不会有一丝一毫的难过，如果伤心，也是为自己的有眼无珠而伤"
        u8"心。结局是，我又回到电台。而凯文依然没有圆他那个青春年少的梦，他曾回来求我说他发现他爱"
        u8"的还是我，没有我他将无法生活。我哑然失笑。我说这也是我曾经对你说过的话，不过，这是我说"
        u8"过的最愚蠢的话。我早已不爱你了，而我也做不了你的天使。这世界谁没谁，生活都会照样继续。"
        u8"没什么大不了的。后来，邓澜对我讲述了凯文的第一次婚姻。这些往事，凯文从没对我说过，我也"
        u8"从不。凯文以前在部队，出身贫寒，最大的梦想就是出人头地。而无论他怎么努力钻营，机会也一"
        u8"直没有垂青于他。为了改变命运，他娶了并不喜欢的团长的女儿。这次婚姻并没有带给他转机，骄"
        u8"横的爱人终于激怒了他。于是，他离婚了。他似乎不再追逐名利，只想找一个相爱的人好好生活。"
        u8"如果不钻营投机，他会是一个相当优秀的人，聪明而体贴。邓澜以为他已经醒悟了，所以才力撮我"
        u8"们。她说她对不起我，不该让我们相识。我笑笑，说一切都过去了。何况，这怨不得谁，是老天早"
        u8"就注定的。18岁的时候就注定了的。选择爱就是选择劫难。我换了城市。继续做DJ的工作。有听众"
        u8"曾说，我的声音透着绝望，那透明清凉的绝望。朋友说我越来越不正常。我说是，一个迷恋夜晚的"
        u8"女子怎么可能正常。有人在背后开始对我指指点点，说一个30岁的女人还不结婚一定是有问题。我"
        u8"不为所动。 但是我知道，总有一天，我会老去，且没有人会再听我说话。";
  {
    // load dict
    ik_tokenize(test_str, sizeof(test_str), true);
    std::chrono::system_clock::time_point now = std::chrono::high_resolution_clock::now();
    long start
        = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    int times = 1000;
    for (int i = 0; i < times; i++) {
      ik_tokenize(test_str, sizeof(test_str), true);
    }
    now = std::chrono::high_resolution_clock::now();
    long end = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    long time_cost_in_ns = end - start;
    std::cout << "Time cost pertime: " << time_cost_in_ns / 1000000 / times << ".\n";
    std::cout << "Run times: " << times << ".\n";
    std::cout << "Words(by char):" << sizeof(test_str) << ".\n";
  }

  {
    std::chrono::system_clock::time_point now = std::chrono::high_resolution_clock::now();
    long start
        = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    int times = 1000;
    for (int i = 0; i < times; i++) {
      ik_tokenize(test_str, sizeof(test_str), false);
    }
    now = std::chrono::high_resolution_clock::now();
    long end = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    long time_cost_in_ns = end - start;
    std::cout << "Time cost pertime: " << time_cost_in_ns / 1000000 / times << ".\n";
    std::cout << "Run times: " << times << ".\n";
    std::cout << "Words(by char):" << sizeof(test_str) << ".\n";
  }
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_ft_parser.log");
  OB_LOGGER.set_file_name("test_ft_parser.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  // oceanbase::storage::ObTestFTPluginHelper::file_name = argv[0];
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
