/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/fts/analyzer/filter/ob_stop_word_filter.h"

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "storage/fts/ob_fts_stop_token_check.h"
#include "storage/ob_storage_util.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{

class ObStopWordFilter::ObBuiltinStopWordChecker final
{
public:
  ObBuiltinStopWordChecker()
      : lang_(ObStopWordLanguageKind::LANGUAGE_INVALID),
        hash_func_(nullptr),
        cmp_func_(nullptr),
        stop_token_table_(nullptr),
        stop_table_alloc_(nullptr)
  {}
  ~ObBuiltinStopWordChecker() { reset(); }

  int init(const ObStopWordLanguageKind lang, common::ObIAllocator &alloc)
  {
    int ret = OB_SUCCESS;
    reset();
    lang_ = lang;
    if (is_noop_builtin_language(lang_)) {
      // no-op checker
    } else if (OB_FAIL(init_token_meta(CS_TYPE_UTF8MB4_GENERAL_CI))) {
      LOG_WARN("failed to init stopword token meta", K(ret), K(static_cast<int>(lang_)));
    } else if (ObStopWordLanguageKind::LANGUAGE_ENGLISH == lang_) {
      if (OB_FAIL(build_english_stop_token_checker(alloc))) {
        LOG_WARN("failed to build english stop token checker", K(ret));
      }
    } else if (ObStopWordLanguageKind::LANGUAGE_THAI == lang_) {
      if (OB_FAIL(build_thai_stop_token_checker(alloc))) {
        LOG_WARN("failed to build thai stop token checker", K(ret));
      }
    } else if (ObStopWordLanguageKind::LANGUAGE_INDONESIAN == lang_) {
      if (OB_FAIL(build_indonesian_stop_token_checker(alloc))) {
        LOG_WARN("failed to build indonesian stop token checker", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected builtin stopword language", K(ret), K(static_cast<int>(lang_)));
    }
    if (OB_FAIL(ret)) {
      reset();
    }
    return ret;
  }

  int check_is_stop_word(const ObTokenAttr &token, bool &is_stop_word)
  {
    int ret = OB_SUCCESS;
    ObFTToken ft_token;
    is_stop_word = false;
    if (!token.is_valid() || is_noop_builtin_language(lang_)) {
      // leave false
    } else if (OB_FAIL(build_ft_token_from_buf(token.token_ptr_, token.token_len_, ft_token))) {
      LOG_WARN("failed to build ft token for stopword checker",
          K(ret), K(token.token_len_), K(static_cast<int>(lang_)));
    } else if (OB_FAIL(stop_token_checker_.check_is_stop_token(ft_token, is_stop_word))) {
      LOG_WARN("failed to check stop token", K(ret), K(ft_token), K(static_cast<int>(lang_)));
    }
    return ret;
  }

  void reset()
  {
    lang_ = ObStopWordLanguageKind::LANGUAGE_INVALID;
    token_meta_.reset();
    hash_func_ = nullptr;
    cmp_func_ = nullptr;
    stop_token_checker_.reset();
    destroy_owned_stop_table();
  }

private:
  static bool is_noop_builtin_language(const ObStopWordLanguageKind lang)
  {
    return ObStopWordLanguageKind::LANGUAGE_NONE == lang
        || ObStopWordLanguageKind::LANGUAGE_VIETNAMESE == lang
        || ObStopWordLanguageKind::LANGUAGE_MALAY == lang;
  }

  int build_english_stop_token_checker(common::ObIAllocator &alloc)
  {
    int ret = OB_SUCCESS;
    static const char *ENGLISH_STOP_WORDS[] = {
        "a", "an", "and", "are", "as", "at", "be", "but",
        "by", "for", "if", "in", "into", "is", "it", "no",
        "not", "of", "on", "or", "such", "that", "the", "their",
        "then", "there", "these", "they", "this", "to", "was", "will",
        "with",
    };
    if (OB_FAIL(build_stop_token_checker(ENGLISH_STOP_WORDS, ARRAYSIZEOF(ENGLISH_STOP_WORDS), alloc))) {
      LOG_WARN("failed to build english stop token checker", K(ret));
    }
    return ret;
  }

  int init_token_meta(const ObCollationType coll_type)
  {
    int ret = OB_SUCCESS;
    sql::ObExprBasicFuncs *basic_funcs = nullptr;
    token_meta_.reset();
    token_meta_.set_varchar();
    token_meta_.set_collation_type(coll_type);
    basic_funcs = ObDatumFuncs::get_basic_func(token_meta_.get_type(), token_meta_.get_collation_type());
    cmp_func_ = get_datum_cmp_func(token_meta_, token_meta_);
    if (OB_UNLIKELY(nullptr == basic_funcs || nullptr == basic_funcs->default_hash_ || nullptr == cmp_func_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get basic funcs or cmp func", K(ret), K(token_meta_), KP(basic_funcs), KP(cmp_func_));
    } else {
      hash_func_ = basic_funcs->default_hash_;
    }
    return ret;
  }

  int build_thai_stop_token_checker(common::ObIAllocator &alloc)
  {
    int ret = OB_SUCCESS;
    static const char *THAI_STOP_WORDS[] = {
        "ไว้", "ไม่", "ไป", "ได้", "ให้", "ใน", "โดย", "แห่ง",
        "แล้ว", "และ", "แรก", "แบบ", "แต่", "เอง", "เห็น", "เลย",
        "เริ่ม", "เรา", "เมื่อ", "เพื่อ", "เพราะ", "เป็นการ", "เป็น", "เปิดเผย",
        "เปิด", "เนื่องจาก", "เดียวกัน", "เดียว", "เช่น", "เฉพาะ", "เคย", "เข้า",
        "เขา", "อีก", "อาจ", "อะไร", "ออก", "อย่าง", "อยู่", "อยาก",
        "หาก", "หลาย", "หลังจาก", "หลัง", "หรือ", "หนึ่ง", "ส่วน", "ส่ง",
        "สุด", "สําหรับ", "ว่า", "วัน", "ลง", "ร่วม", "ราย", "รับ",
        "ระหว่าง", "รวม", "ยัง", "มี", "มาก", "มา", "พร้อม", "พบ",
        "ผ่าน", "ผล", "บาง", "น่า", "นี้", "นํา", "นั้น", "นัก",
        "นอกจาก", "ทุก", "ที่สุด", "ที่", "ทําให้", "ทํา", "ทาง", "ทั้งนี้",
        "ทั้ง", "ถ้า", "ถูก", "ถึง", "ต้อง", "ต่างๆ", "ต่าง", "ต่อ",
        "ตาม", "ตั้งแต่", "ตั้ง", "ด้าน", "ด้วย", "ดัง", "ซึ่ง", "ช่วง",
        "จึง", "จาก", "จัด", "จะ", "คือ", "ความ", "ครั้ง", "คง",
        "ขึ้น", "ของ", "ขอ", "ขณะ", "ก่อน", "ก็", "การ", "กับ",
        "กัน", "กว่า", "กล่าว",
    };
    if (OB_FAIL(build_stop_token_checker(THAI_STOP_WORDS, ARRAYSIZEOF(THAI_STOP_WORDS), alloc))) {
      LOG_WARN("failed to build thai stop token checker", K(ret));
    }
    return ret;
  }

  int build_indonesian_stop_token_checker(common::ObIAllocator &alloc)
  {
    int ret = OB_SUCCESS;
    static const char *INDONESIAN_STOP_WORDS[] = {
        "ada", "adanya", "adalah", "adapun", "agak", "agaknya", "agar", "akan",
        "akankah", "akhirnya", "aku", "akulah", "amat", "amatlah", "anda", "andalah",
        "antar", "diantaranya", "antara", "antaranya", "diantara", "apa", "apaan", "mengapa",
        "apabila", "apakah", "apalagi", "apatah", "atau", "ataukah", "ataupun", "bagai",
        "bagaikan", "sebagai", "sebagainya", "bagaimana", "bagaimanapun", "sebagaimana", "bagaimanakah", "bagi",
        "bahkan", "bahwa", "bahwasanya", "sebaliknya", "banyak", "sebanyak", "beberapa", "seberapa",
        "begini", "beginian", "beginikah", "beginilah", "sebegini", "begitu", "begitukah", "begitulah",
        "begitupun", "sebegitu", "belum", "belumlah", "sebelum", "sebelumnya", "sebenarnya", "berapa",
        "berapakah", "berapalah", "berapapun", "betulkah", "sebetulnya", "biasa", "biasanya", "bila",
        "bilakah", "bisa", "bisakah", "sebisanya", "boleh", "bolehkah", "bolehlah", "buat",
        "bukan", "bukankah", "bukanlah", "bukannya", "cuma", "percuma", "dahulu", "dalam",
        "dan", "dapat", "dari", "daripada", "dekat", "demi", "demikian", "demikianlah",
        "sedemikian", "dengan", "depan", "di", "dia", "dialah", "dini", "diri",
        "dirinya", "terdiri", "dong", "dulu", "enggak", "enggaknya", "entah", "entahlah",
        "terhadap", "terhadapnya", "hal", "hampir", "hanya", "hanyalah", "harus", "haruslah",
        "harusnya", "seharusnya", "hendak", "hendaklah", "hendaknya", "hingga", "sehingga", "ia",
        "ialah", "ibarat", "ingin", "inginkah", "inginkan", "ini", "inikah", "inilah",
        "itu", "itukah", "itulah", "jangan", "jangankan", "janganlah", "jika", "jikalau",
        "juga", "justru", "kala", "kalau", "kalaulah", "kalaupun", "kalian", "kami",
        "kamilah", "kamu", "kamulah", "kan", "kapan", "kapankah", "kapanpun", "dikarenakan",
        "karena", "karenanya", "ke", "kecil", "kemudian", "kenapa", "kepada", "kepadanya",
        "ketika", "seketika", "khususnya", "kini", "kinilah", "kiranya", "sekiranya", "kita",
        "kitalah", "kok", "lagi", "lagian", "selagi", "lah", "lain", "lainnya",
        "melainkan", "selaku", "lalu", "melalui", "terlalu", "lama", "lamanya", "selama",
        "selamanya", "lebih", "terlebih", "bermacam", "macam", "semacam", "maka", "makanya",
        "makin", "malah", "malahan", "mampu", "mampukah", "mana", "manakala", "manalagi",
        "masih", "masihkah", "semasih", "masing", "mau", "maupun", "semaunya", "memang",
        "mereka", "merekalah", "meski", "meskipun", "semula", "mungkin", "mungkinkah", "nah",
        "namun", "nanti", "nantinya", "nyaris", "oleh", "olehnya", "seorang", "seseorang",
        "pada", "padanya", "padahal", "paling", "sepanjang", "pantas", "sepantasnya", "sepantasnyalah",
        "para", "pasti", "pastilah", "per", "pernah", "pula", "pun", "merupakan",
        "rupanya", "serupa", "saat", "saatnya", "sesaat", "saja", "sajalah", "saling",
        "bersama", "sama", "sesama", "sambil", "sampai", "sana", "sangat", "sangatlah",
        "saya", "sayalah", "se", "sebab", "sebabnya", "sebuah", "tersebut", "tersebutlah",
        "sedang", "sedangkan", "sedikit", "sedikitnya", "segala", "segalanya", "segera", "sesegera",
        "sejak", "sejenak", "sekali", "sekalian", "sekalipun", "sesekali", "sekaligus", "sekarang",
        "sekitar", "sekitarnya", "sela", "selain", "selalu", "seluruh", "seluruhnya", "semakin",
        "sementara", "sempat", "semua", "semuanya", "sendiri", "sendirinya", "seolah", "seperti",
        "sepertinya", "sering", "seringnya", "serta", "siapa", "siapakah", "siapapun", "disini",
        "disinilah", "sini", "sinilah", "sesuatu", "sesuatunya", "suatu", "sesudah", "sesudahnya",
        "sudah", "sudahkah", "sudahlah", "supaya", "tadi", "tadinya", "tak", "tanpa",
        "setelah", "telah", "tentang", "tentu", "tentulah", "tentunya", "tertentu", "seterusnya",
        "tapi", "tetapi", "setiap", "tiap", "setidaknya", "tidak", "tidakkah", "tidaklah",
        "toh", "waduh", "wah", "wahai", "sewaktu", "walau", "walaupun", "wong",
        "yaitu", "yakni", "yang",
    };
    if (OB_FAIL(build_stop_token_checker(INDONESIAN_STOP_WORDS, ARRAYSIZEOF(INDONESIAN_STOP_WORDS), alloc))) {
      LOG_WARN("failed to build indonesian stop token checker", K(ret));
    }
    return ret;
  }

  int build_stop_token_checker(const char **words, const int64_t word_count, common::ObIAllocator &alloc)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(build_stop_token_table(words, word_count, alloc))) {
      LOG_WARN("failed to build stop token table", K(ret), KP(words), K(word_count));
    } else if (OB_FAIL(stop_token_checker_.init(token_meta_.get_collation_type(), stop_token_table_))) {
      LOG_WARN("failed to init stop token checker", K(ret), K(token_meta_), K(word_count));
    }
    return ret;
  }

  int build_stop_token_table(const char **words, const int64_t word_count, common::ObIAllocator &alloc)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == words || 0 >= word_count)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid stopword table", K(ret), KP(words), K(word_count));
    } else {
      void *buf = alloc.alloc(sizeof(ObStopTokenTable));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc stop token table", K(ret));
      } else {
        stop_token_table_ = new (buf) ObStopTokenTable();
        stop_table_alloc_ = &alloc;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stop_token_table_->create(
              word_count, "FTSwTokTab", "FTSwTokTab", common::OB_SERVER_TENANT_ID))) {
        LOG_WARN("failed to create stop token hash set", K(ret), K(word_count));
      }
    }
    ObFTToken stop_token;
    for (int64_t i = 0; OB_SUCC(ret) && i < word_count; ++i) {
      const char *word = words[i];
      const int32_t word_len = static_cast<int32_t>(STRLEN(word));
      if (OB_FAIL(stop_token.init(word, word_len, token_meta_, hash_func_, cmp_func_))) {
        LOG_WARN("failed to init stopword ft token", K(ret), K(i));
      } else if (OB_FAIL(stop_token_table_->set_refactored(stop_token))) {
        LOG_WARN("failed to insert stopword into hash set", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
      destroy_owned_stop_table();
    }
    return ret;
  }

  void destroy_owned_stop_table()
  {
    if (nullptr != stop_token_table_ && nullptr != stop_table_alloc_) {
      (void)stop_token_table_->destroy();
      stop_token_table_->~ObStopTokenTable();
      stop_table_alloc_->free(stop_token_table_);
      stop_token_table_ = nullptr;
      stop_table_alloc_ = nullptr;
    }
  }

  int build_ft_token_from_buf(const char *ptr, const int32_t len, ObFTToken &ft_token) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == ptr || 0 >= len || nullptr == hash_func_ || nullptr == cmp_func_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument for building ft token from buffer", K(ret), KP(ptr), K(len));
    } else if (OB_FAIL(ft_token.init(ptr, len, token_meta_, hash_func_, cmp_func_))) {
      LOG_WARN("failed to init ft token", K(ret), KP(ptr), K(len), K(token_meta_));
    }
    return ret;
  }

private:
  ObStopWordLanguageKind lang_;
  ObObjMeta token_meta_;
  sql::ObExprHashFuncType hash_func_;
  ObDatumCmpFuncType cmp_func_;
  ObStopTokenChecker stop_token_checker_;
  ObStopTokenTable *stop_token_table_;
  common::ObIAllocator *stop_table_alloc_;

  DISALLOW_COPY_AND_ASSIGN(ObBuiltinStopWordChecker);
};

ObStopWordFilter::ObStopWordFilter()
    : builtin_checker_(nullptr),
      stopword_table_name_(),
      checker_owner_alloc_(nullptr),
      source_type_(StopwordSourceType::SOURCE_INVALID),
      language_kind_(ObStopWordLanguageKind::LANGUAGE_INVALID),
      is_inited_(false)
{}

ObStopWordFilter::~ObStopWordFilter()
{
  reset();
}

int ObStopWordFilter::init_custom_stop_word_checker(const common::ObString &table_name,
                                                    common::ObIAllocator &alloc)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(table_name);
  UNUSED(alloc);
  LOG_WARN("custom stopword table is not supported currently", K(ret), K(table_name));
  return ret;
}

int ObStopWordFilter::check_is_stop_word(const ObTokenAttr &token, bool &is_stop_word) const
{
  int ret = OB_SUCCESS;
  is_stop_word = false;
  if (!token.is_valid()) {
    // keep default false
  } else if (OB_NOT_NULL(builtin_checker_)) {
    if (OB_FAIL(builtin_checker_->check_is_stop_word(token, is_stop_word))) {
      LOG_WARN("failed to check builtin stopword", K(ret), K(token));
    }
  } else if (StopwordSourceType::SOURCE_CUSTOM_TABLE == source_type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("custom stopword table is not implemented", K(ret), K(stopword_table_name_));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("builtin stopword checker is null", K(ret), K(source_type_));
  }
  return ret;
}

void ObStopWordFilter::destroy_builtin_checker()
{
  if (OB_NOT_NULL(builtin_checker_) && OB_NOT_NULL(checker_owner_alloc_)) {
    builtin_checker_->~ObBuiltinStopWordChecker();
    checker_owner_alloc_->free(builtin_checker_);
    builtin_checker_ = nullptr;
  }
}

void ObStopWordFilter::destroy_stopword_table_name()
{
  if (OB_NOT_NULL(stopword_table_name_.ptr()) && OB_NOT_NULL(checker_owner_alloc_)) {
    checker_owner_alloc_->free(stopword_table_name_.ptr());
  }
  stopword_table_name_.reset();
}

int ObStopWordFilter::init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  const ObStopWordFilterSpec *stop_spec = nullptr;
  ObStopWordLanguageKind lang_kind = ObStopWordLanguageKind::LANGUAGE_INVALID;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("stop word filter init twice", K(ret));
  } else if (OB_UNLIKELY(ObTokenFilterType::TOKEN_FILTER_TYPE_STOP != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid token filter spec type for stop word filter", K(ret), K(spec.type_));
  } else {
    stop_spec = static_cast<const ObStopWordFilterSpec *>(&spec);
    if (ObStopWordLanguageKind::LANGUAGE_INVALID != stop_spec->language_) {
      lang_kind = stop_spec->language_;
    } else {
      lang_kind = ObStopWordLanguageKind::LANGUAGE_ENGLISH;
    }
    if (!stop_spec->stopword_table_.empty()) {
      checker_owner_alloc_ = &alloc;
      if (OB_FAIL(ob_write_string(alloc, stop_spec->stopword_table_, stopword_table_name_))) {
        LOG_WARN("failed to copy stopword table name", K(ret), K(stop_spec->stopword_table_));
      } else if (OB_FAIL(init_custom_stop_word_checker(stopword_table_name_, alloc))) {
        LOG_WARN("failed to init custom stopword checker", K(ret), K(stopword_table_name_));
      } else {
        source_type_ = StopwordSourceType::SOURCE_CUSTOM_TABLE;
        language_kind_ = lang_kind;
        is_inited_ = true;
      }
    } else {
      checker_owner_alloc_ = &alloc;
      void *buf = alloc.alloc(sizeof(ObBuiltinStopWordChecker));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc builtin stopword checker", K(ret), K(static_cast<int>(lang_kind)));
      } else if (FALSE_IT(builtin_checker_ = new (buf) ObBuiltinStopWordChecker())) {
      } else if (OB_FAIL(builtin_checker_->init(lang_kind, alloc))) {
        LOG_WARN("failed to init builtin stopword checker", K(ret), K(static_cast<int>(lang_kind)));
        destroy_builtin_checker();
      } else {
        source_type_ = StopwordSourceType::SOURCE_BUILTIN;
        language_kind_ = lang_kind;
        is_inited_ = true;
      }
    }
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    source_type_ = StopwordSourceType::SOURCE_INVALID;
    language_kind_ = ObStopWordLanguageKind::LANGUAGE_INVALID;
    destroy_stopword_table_name();
    destroy_builtin_checker();
    checker_owner_alloc_ = nullptr;
  }
  return ret;
}

int ObStopWordFilter::get_next_token(ObTokenAttr &token)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(input_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stop word filter not initialized", K(ret), KP(input_), K(language_kind_));
  } else {
    bool found_token = false;
    bool is_stop_word = false;
    int32_t pending_pos_inc = 0;
    while (OB_SUCC(ret) && !found_token) {
      if (OB_FAIL(input_->get_next_token(token))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("upstream token stream failed", K(ret));
        }
      } else if (!token.is_valid()) {
        // invalid upstream token; pull again
      } else if (OB_FAIL(check_is_stop_word(token, is_stop_word))) {
        LOG_WARN("failed to check whether token is stopword", K(ret), K(token));
      } else if (is_stop_word) {
        pending_pos_inc += token.pos_inc_;
      } else {
        token.pos_inc_ += pending_pos_inc;
        found_token = true;
      }
    }
  }
  return ret;
}

void ObStopWordFilter::reset()
{
  if (OB_NOT_NULL(input_) || IS_INIT || OB_NOT_NULL(builtin_checker_)) {
    input_ = nullptr;
    source_type_ = StopwordSourceType::SOURCE_INVALID;
    language_kind_ = ObStopWordLanguageKind::LANGUAGE_INVALID;
    destroy_stopword_table_name();
    destroy_builtin_checker();
    checker_owner_alloc_ = nullptr;
    is_inited_ = false;
  }
}

} // namespace storage
} // namespace oceanbase
