/**
 * Copyright (code) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <algorithm>
#include <bitset>
#include <iterator>
#include <map>
#include <utility>
#include "lib/charset/ob_ctype.h"
#include "lib/charset/mb_wc.h"
#include "lib/charset/str_uca_type.h"
#include "lib/charset/uca900_data.h"
#include "lib/charset/uca900_zh_data.h"
#include "lib/charset/uca900_ja_data.h"
#include "lib/charset/ob_template_helper.h"
#include "lib/charset/ob_ctype_uca_tab.h"
#include "lib/charset/ob_byteorder.h"
#define OB_UCA_NPAGES 256
#define OB_UCA_NCHARS 256
#define OB_UCA_CMASK  255
#define OB_UCA_PSHIFT 8
#define OB_UCA_MAX_EXPANSION  6
#define OB_UCA_CNT_FLAG_SIZE 4096
#define OB_UCA_CNT_FLAG_MASK 4095
#define OB_UCA_CNT_HEAD  1
#define OB_UCA_CNT_TAIL  2
#define OB_UCA_CNT_MID1  4
#define OB_UCA_CNT_MID2  8
#define OB_UCA_CNT_MID3  16
#define OB_UCA_CNT_MID4  32
#define OB_UCA_PREVIOUS_CONTEXT_HEAD 64
#define OB_UCA_PREVIOUS_CONTEXT_TAIL 128

static inline uint16_t *ob_char_weight_addr(ObUCAInfo *uca, ob_wc_t wc) {
  unsigned int page, ofst;
  return wc > uca->maxchar ? nullptr
                           : (uca->weights[page = (wc >> 8)]
                                  ? uca->weights[page] + (ofst = (wc & 0xFF)) *
                                                             uca->lengths[page]
                                  : nullptr);
}
static inline uint16_t *ob_char_weight_addr_900(ObUCAInfo *uca, ob_wc_t wc) {
  if (wc > uca->maxchar) return nullptr;
  unsigned int page = wc >> 8;
  unsigned int ofst = wc & 0xFF;
  uint16_t *weights = uca->weights[page];
  if (weights) {
    return UCA900_WEIGHT_ADDR(weights, 0, ofst);
  } else {
    return nullptr;
  }
}
static inline void ob_uca_add_contraction_flag(char *flags,
                                               ob_wc_t wc,
                                               int flag) {
  flags[wc & OB_UCA_CNT_FLAG_MASK] |= flag;
}
static inline bool ob_uca_have_contractions(const ObUCAInfo *uca) {
  return uca->have_contractions;
}
struct trie_node_cmp {
  bool operator()(const ObContraction &a, const ob_wc_t b) { return a.ch < b; }
  bool operator()(const ObContraction &a, const ObContraction &b) {
    return a.ch < b.ch;
  }
};
static std::vector<ObContraction>::const_iterator
find_contraction_part_in_trie(const std::vector<ObContraction> &cont_nodes,
                              ob_wc_t ch) {
  if (cont_nodes.empty()) {
    return cont_nodes.end();
  } else {
    return std::lower_bound(cont_nodes.begin(), cont_nodes.end(), ch,
                          trie_node_cmp());
  }
}
static std::vector<ObContraction>::iterator find_contraction_part_in_trie(
    std::vector<ObContraction> &cont_nodes, ob_wc_t ch) {
  if (cont_nodes.empty()) {
    return cont_nodes.end();
  } else {
    return std::lower_bound(cont_nodes.begin(), cont_nodes.end(), ch,
                          trie_node_cmp());
  }
}
const uint16_t *ob_uca_contraction2_weight(
    const std::vector<ObContraction> *cont_nodes, ob_wc_t wc1, ob_wc_t wc2) {
  if (!cont_nodes) return nullptr;
  if (!cont_nodes->empty()) {
    std::vector<ObContraction>::const_iterator node_it1 =
        find_contraction_part_in_trie(*cont_nodes, wc1);
    if (node_it1 == cont_nodes->end() || node_it1->ch != wc1) {
      return nullptr;
    } else {
      std::vector<ObContraction>::const_iterator node_it2 =
        find_contraction_part_in_trie(node_it1->child_nodes, wc2);
      if (node_it2 != node_it1->child_nodes.end() && node_it2->ch == wc2 &&
        node_it2->is_contraction_tail) {
        return node_it2->weight;
      }
    }
  }
  return nullptr;
}
static inline bool ob_uca_can_be_previous_context_head(const char *flags,
                                                       ob_wc_t wc) {
  return flags[wc & OB_UCA_CNT_FLAG_MASK] & OB_UCA_PREVIOUS_CONTEXT_HEAD;
}
static inline bool ob_uca_can_be_previous_context_tail(const char *flags,
                                                       ob_wc_t wc) {
  return flags[wc & OB_UCA_CNT_FLAG_MASK] & OB_UCA_PREVIOUS_CONTEXT_TAIL;
}
static inline const uint16_t *ob_uca_contraction_weight(
    const std::vector<ObContraction> *cont_nodes, const ob_wc_t *wc,
    size_t len) {
  if (!cont_nodes) return nullptr;
  std::vector<ObContraction>::const_iterator node_it;
  for (size_t ch_ind = 0; ch_ind < len; ++ch_ind) {
    node_it = find_contraction_part_in_trie(*cont_nodes, wc[ch_ind]);
    if (node_it == cont_nodes->end() || node_it->ch != wc[ch_ind]) {
      return nullptr;
    } else {
      cont_nodes = &node_it->child_nodes;
    }
  }
  if (node_it->is_contraction_tail) {
    return node_it->weight;
  } else {
    return nullptr;
  }
}
class ob_uca_scanner {
 protected:
  ob_uca_scanner(const ObCharsetInfo *cs_arg, const unsigned char *str, size_t length)
      : wbeg(nochar),
        sbeg(str),
        send(str + length),
        uca(cs_arg->uca),
        cs(cs_arg),
        sbeg_dup(str) {}
 public:
  unsigned int get_weight_level() const { return weight_lv; }
 protected:
  unsigned int weight_lv{0};
  const uint16_t *wbeg;
  unsigned int wbeg_stride{0};
  const unsigned char *sbeg;
  const unsigned char *send;
  const ObUCAInfo *uca;
  uint16_t implicit[10];
  ob_wc_t prev_char{0};  // Previous code point we scanned, if any.
  const ObCharsetInfo *cs;
  unsigned int num_of_ce_left{0};
  const unsigned char *sbeg_dup;
 protected:
  const uint16_t *contraction_find(ob_wc_t wc0, size_t *chars_skipped);
  inline const uint16_t *previous_context_find(ob_wc_t wc0, ob_wc_t wc1);
};
template <class Mb_wc>
struct uca_scanner_any : public ob_uca_scanner {
  uca_scanner_any(const Mb_wc mb_wc, const ObCharsetInfo *cs_arg,
                  const unsigned char *str, size_t length)
      : ob_uca_scanner(cs_arg, str, length), mb_wc(mb_wc) {
    // UCA 9.0.0 uses a different table format from what this scanner expects.
    ob_charset_assert(cs_arg->uca == nullptr || cs_arg->uca->version != UCA_V900);
  }
  unsigned int get_char_index() const { return char_index; }
  inline int next();
 private:
  unsigned int char_index{0};
  const Mb_wc mb_wc;
  inline int next_implicit(ob_wc_t ch);
};
template <class Mb_wc, int LEVELS_FOR_COMPARE>
class uca_scanner_900 : public ob_uca_scanner {
 public:
  uca_scanner_900(const Mb_wc mb_wc, const ObCharsetInfo *cs_arg,
                  const unsigned char *str, size_t length)
      : ob_uca_scanner(cs_arg, str, length), mb_wc(mb_wc) {}
  inline int next();
  template <class T, class U>
  inline void for_each_weight(T func, U preaccept_data);
 private:
  const Mb_wc mb_wc;
  inline int next_raw();
  inline int more_weight();
  uint16_t apply_case_first(uint16_t weight);
  uint16_t apply_reorder_param(uint16_t weight);
  inline int next_implicit(ob_wc_t ch);
  void ob_put_jamo_weights(ob_wc_t *hangul_jamo, int jamo_cnt);
  bool return_origin_weight{true};
  bool has_quaternary_weight{false};
  int handle_ja_contraction_quat_wt();
  int handle_ja_common_quat_wt(ob_wc_t wc);
};
const uint16_t *ob_uca_scanner::contraction_find(ob_wc_t wc0,
                                                 size_t *chars_skipped) {
  const unsigned char *beg = nullptr;
  auto mb_wc = cs->cset->mb_wc;
  const unsigned char *s = sbeg;
  const std::vector<ObContraction> *cont_nodes = uca->contraction_nodes;
  const ObContraction *longest_contraction = nullptr;
  std::vector<ObContraction>::const_iterator node_it;
  for (;;) {
    node_it = find_contraction_part_in_trie(*cont_nodes, wc0);
    if (node_it == cont_nodes->end() || node_it->ch != wc0) break;
    if (node_it->is_contraction_tail) {
      longest_contraction = &(*node_it);
      beg = s;
      *chars_skipped = node_it->contraction_len - 1;
    }
    int mblen;
    if ((mblen = mb_wc(cs, &wc0, s, send)) <= 0) break;
    s += mblen;
    cont_nodes = &node_it->child_nodes;
  }
  if (longest_contraction != nullptr) {
    const uint16_t *cweight = longest_contraction->weight;
    if (uca->version == UCA_V900) {
      cweight += weight_lv;
      wbeg = cweight + OB_UCA_900_CE_SIZE;
      wbeg_stride = OB_UCA_900_CE_SIZE;
      num_of_ce_left = 7;
    } else {
      wbeg = cweight + 1;
      wbeg_stride = OB_UCA_900_CE_SIZE;
    }
    sbeg = beg;
    return cweight;
  } else {
    return nullptr;
  }
}
ALWAYS_INLINE
const uint16_t *ob_uca_scanner::previous_context_find(ob_wc_t wc0, ob_wc_t wc1) {
  std::vector<ObContraction>::const_iterator node_it1 =
      find_contraction_part_in_trie(*uca->contraction_nodes, wc1);
  if (node_it1 == uca->contraction_nodes->end() || node_it1->ch != wc1) {
    return nullptr;
  }
  std::vector<ObContraction>::const_iterator node_it2 =
      find_contraction_part_in_trie(node_it1->child_nodes_context, wc0);
  if (node_it2 != node_it1->child_nodes_context.end() && node_it2->ch == wc0) {
    if (uca->version == UCA_V900) {
      wbeg = node_it2->weight + OB_UCA_900_CE_SIZE + weight_lv;
      wbeg_stride = OB_UCA_900_CE_SIZE;
      num_of_ce_left = 7;
    } else {
      wbeg = node_it2->weight + 1;
      wbeg_stride = OB_UCA_900_CE_SIZE;
    }
    return node_it2->weight + weight_lv;
  }
  return nullptr;
}
#define HANGUL_JAMO_MAX_LENGTH 3
static int ob_decompose_hangul_syllable(ob_wc_t syllable, ob_wc_t *jamo) {
  if (syllable < 0xAC00 || syllable > 0xD7AF) return 0;
  constexpr unsigned int syllable_base = 0xAC00;
  constexpr unsigned int leadingjamo_base = 0x1100;
  constexpr unsigned int voweljamo_base = 0x1161;
  constexpr unsigned int trailingjamo_base = 0x11A7;
  constexpr unsigned int voweljamo_cnt = 21;
  constexpr unsigned int trailingjamo_cnt = 28;
  const unsigned int syllable_index = syllable - syllable_base;
  const unsigned int v_t_combination = voweljamo_cnt * trailingjamo_cnt;
  const unsigned int leadingjamo_index = syllable_index / v_t_combination;
  const unsigned int voweljamo_index =
      (syllable_index % v_t_combination) / trailingjamo_cnt;
  const unsigned int trailingjamo_index = syllable_index % trailingjamo_cnt;
  jamo[0] = leadingjamo_base + leadingjamo_index;
  jamo[1] = voweljamo_base + voweljamo_index;
  jamo[2] = trailingjamo_index ? (trailingjamo_base + trailingjamo_index) : 0;
  return trailingjamo_index ? 3 : 2;
}
template <class Mb_wc, int LEVELS_FOR_COMPARE>
void uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE>::ob_put_jamo_weights(
    ob_wc_t *hangul_jamo, int jamo_cnt) {
  for (int jamoind = 0; jamoind < jamo_cnt; jamoind++) {
    uint16_t *implicit_weight = implicit + jamoind * OB_UCA_900_CE_SIZE;
    unsigned int page = hangul_jamo[jamoind] >> 8;
    unsigned int code = hangul_jamo[jamoind] & 0xFF;
    const uint16_t *jamo_weight_page = uca->weights[page];
    implicit_weight[0] = UCA900_WEIGHT(jamo_weight_page, 0, code);
    implicit_weight[1] = UCA900_WEIGHT(jamo_weight_page, 1, code);
    implicit_weight[2] = UCA900_WEIGHT(jamo_weight_page, 2, code);
  }
  implicit[9] = jamo_cnt;
}
static uint16_t change_zh_implicit(uint16_t weight) {
  ob_charset_assert(weight >= 0xFB00);
  switch (weight) {
    case 0xFB00:
      return 0xF621;
    case 0xFB40:
      return 0xBDBF;
    case 0xFB41:
      return 0xBDC0;
    case 0xFB80:
      return 0xBDC1;
    case 0xFB84:
      return 0xBDC2;
    case 0xFB85:
      return 0xBDC3;
    default:
      return weight + 0xF622 - 0xFBC0;
  }
}
static uint16_t change_zh2_implicit(uint16_t weight) {
  ob_charset_assert(weight >= 0xFB00);
  switch (weight) {
    case 0xFB40:
      return 0x5C47;
    case 0xFB41:
      return 0x5C48;
    case 0xFB80:
      return 0x5C49;
    case 0xFB84:
      return 0x5C50;
    case 0xFB85:
      return 0x5C51;
    default:
      return weight;
  }
}
static uint16_t change_zh3_implicit(uint16_t weight) {
  switch (weight) {
    case 0xFB40:
      return 0x1CAB;
    case 0xFB41:
      return 0x1CAC;
    case 0xFB80:
      return 0x1CAD;
    case 0xFB84:
      return 0x1CAE;
    case 0xFB85:
      return 0x1CAF;
    default:
      return weight;
  }
}
template <class Mb_wc, int LEVELS_FOR_COMPARE>
ALWAYS_INLINE int uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE>::next_implicit(
    ob_wc_t ch) {
  ob_wc_t hangul_jamo[HANGUL_JAMO_MAX_LENGTH];
  int jamo_cnt;
  if ((jamo_cnt = ob_decompose_hangul_syllable(ch, hangul_jamo))) {
    ob_put_jamo_weights(hangul_jamo, jamo_cnt);
    num_of_ce_left = jamo_cnt - 1;
    wbeg = implicit + OB_UCA_900_CE_SIZE + weight_lv;
    wbeg_stride = OB_UCA_900_CE_SIZE;
    return *(implicit + weight_lv);
  }
  unsigned int page;
  if (ch >= 0x17000 && ch <= 0x18AFF)  // Tangut character
  {
    page = 0xFB00;
    implicit[3] = (ch - 0x17000) | 0x8000;
  } else {
    page = ch >> 15;
    implicit[3] = (ch & 0x7FFF) | 0x8000;
    if ((ch >= 0x3400 && ch <= 0x4DB5) || (ch >= 0x20000 && ch <= 0x2A6D6) ||
        (ch >= 0x2A700 && ch <= 0x2B734) || (ch >= 0x2B740 && ch <= 0x2B81D) ||
        (ch >= 0x2B820 && ch <= 0x2CEA1)) {
      page += 0xFB80;
    } else if ((ch >= 0x4E00 && ch <= 0x9FD5) || (ch >= 0xFA0E && ch <= 0xFA29)) {
      page += 0xFB40;
    } else {
      page += 0xFBC0;
    }
  }
  if (cs->coll_param == &zh_coll_param) {
    page = change_zh_implicit(page);
  }
  if (cs->coll_param == &zh2_coll_param) {
    page = change_zh2_implicit(page);
  }
  if (cs->coll_param == &zh3_coll_param) {
    page = change_zh3_implicit(page);
  }
  implicit[0] = page;
  implicit[1] = 0x0020;
  implicit[2] = 0x0002;
  // implicit[3] is set above.
  implicit[4] = 0;
  implicit[5] = 0;
  num_of_ce_left = 1;
  wbeg = implicit + OB_UCA_900_CE_SIZE + weight_lv;
  wbeg_stride = OB_UCA_900_CE_SIZE;
  return *(implicit + weight_lv);
}
template <class Mb_wc>
ALWAYS_INLINE int uca_scanner_any<Mb_wc>::next_implicit(ob_wc_t ch) {
  implicit[0] = (ch & 0x7FFF) | 0x8000;
  implicit[1] = 0;
  wbeg = implicit;
  wbeg_stride = OB_UCA_900_CE_SIZE;
  unsigned int page = ch >> 15;
  if (ch >= 0x3400 && ch <= 0x4DB5) {
    page += 0xFB80;
  } else if (ch >= 0x4E00 && ch <= 0x9FA5) {
    page += 0xFB40;
  } else {
    page += 0xFBC0;
  }
  return page;
}
template <class Mb_wc>
ALWAYS_INLINE int uca_scanner_any<Mb_wc>::next() {

  if (wbeg[0]) {
    return *wbeg++;
  }
  do {
    ob_wc_t wc = 0;
    int mblen = mb_wc(&wc, sbeg, send);
    if (mblen <= 0) {
      ++weight_lv;
      return -1;
    }
    sbeg += mblen;
    char_index++;
    if (wc > uca->maxchar) {

      wbeg = nochar;
      wbeg_stride = 0;
      return 0xFFFD;
    }
    if (ob_uca_have_contractions(uca)) {
      const uint16_t *cweight;
      if (ob_uca_can_be_previous_context_tail(uca->contraction_flags, wc) &&
          wbeg != nochar &&
          ob_uca_can_be_previous_context_head(uca->contraction_flags,
                                              prev_char) &&
          (cweight = previous_context_find(prev_char, wc))) {
        prev_char = 0;
        return *cweight;
      } else if (ob_uca_can_be_contraction_head(uca->contraction_flags, wc)) {
        size_t chars_skipped;
        if ((cweight = contraction_find(wc, &chars_skipped))) {
          char_index += chars_skipped;
          return *cweight;
        }
      }
      prev_char = wc;
    }
    unsigned int page = wc >> 8;
    unsigned int code = wc & 0xFF;
    const uint16_t *wpage = uca->weights[page];
    if (!wpage) {
      return next_implicit(wc);
    }
    wbeg = wpage + code * uca->lengths[page];
    wbeg_stride = UCA900_DISTANCE_BETWEEN_WEIGHTS;
  } while (!wbeg[0]);
  return *wbeg++;
}
template <class Mb_wc, int LEVELS_FOR_COMPARE>
inline int uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE>::more_weight() {
  while (num_of_ce_left != 0 && *wbeg == 0) {
    wbeg += wbeg_stride;
    --num_of_ce_left;
  }
  if (num_of_ce_left != 0) {
    uint16_t rtn = *wbeg;
    wbeg += wbeg_stride;
    --num_of_ce_left;
    return rtn;
  }
  return -1;
}
static inline bool is_hiragana_char(ob_wc_t wc) {
  return wc >= 0x3041 && wc <= 0x3096;
}
static inline bool is_katakana_char(ob_wc_t wc) {
  return (wc >= 0x30A1 && wc <= 0x30FA) ||
         (wc >= 0xFF66 && wc <= 0xFF9D);
}
static inline bool is_katakana_iteration(ob_wc_t wc) {
  return wc == 0x30FD || wc == 0x30FE;
}
static inline bool is_hiragana_iteration(ob_wc_t wc) {
  return wc == 0x309D || wc == 0x309E;
}
static inline bool is_ja_length_mark(ob_wc_t wc) { return wc == 0x30FC; }
template <class Mb_wc, int LEVELS_FOR_COMPARE>
ALWAYS_INLINE int
uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE>::handle_ja_contraction_quat_wt() {
  if (weight_lv == 3) {
    wbeg = nochar;
    num_of_ce_left = 0;
    if (is_katakana_char(prev_char)) {
      return JA_KATA_QUAT_WEIGHT;
    } else if (is_hiragana_char(prev_char)) {
      return JA_HIRA_QUAT_WEIGHT;
    }
  }
  return 0;
}
template <class Mb_wc, int LEVELS_FOR_COMPARE>
ALWAYS_INLINE int
uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE>::handle_ja_common_quat_wt(
    ob_wc_t wc) {
  if (weight_lv == 0 && !has_quaternary_weight) {
    if (is_katakana_char(wc) || is_katakana_iteration(wc) ||
        is_hiragana_char(wc) || is_hiragana_iteration(wc) ||
        is_ja_length_mark(wc))
      has_quaternary_weight = true;
  } else if (weight_lv == 3) {
    wbeg = nochar;
    num_of_ce_left = 0;
    if (is_katakana_char(wc) || is_katakana_iteration(wc) ||
        is_ja_length_mark(wc)) {
      return JA_KATA_QUAT_WEIGHT;
    } else if (is_hiragana_char(wc) || is_hiragana_iteration(wc)) {
      return JA_HIRA_QUAT_WEIGHT;
    }
    return -1;
  }
  return 0;
}

template <class Mb_wc, int LEVELS_FOR_COMPARE>
ALWAYS_INLINE int uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE>::next_raw() {
  int remain_weight = more_weight();
  if (remain_weight >= 0) {
    return remain_weight;
  }
  do {
    ob_wc_t wc = 0;
    int mblen = mb_wc(&wc, sbeg, send);
    if (mblen <= 0) {
      if (LEVELS_FOR_COMPARE == 1) {
        ++weight_lv;
        return -1;
      }
      if (++weight_lv < LEVELS_FOR_COMPARE) {
        if (LEVELS_FOR_COMPARE == 4 && cs->coll_param == &ja_coll_param) {
          if (weight_lv == 3 && !has_quaternary_weight) return -1;
        }
        sbeg = sbeg_dup;
        return 0;
      }
      return -1;
    }
    sbeg += mblen;
    ob_charset_assert(wc <= uca->maxchar);
    if (ob_uca_have_contractions(uca)) {
      const uint16_t *cweight;
            if (ob_uca_can_be_previous_context_tail(uca->contraction_flags, wc) &&
          ob_uca_can_be_previous_context_head(uca->contraction_flags,
                                              prev_char) &&
          (cweight = previous_context_find(prev_char, wc))) {
        // For Japanese kana-sensitive collation.
        if (LEVELS_FOR_COMPARE == 4 && cs->coll_param == &ja_coll_param) {
          int quat_wt = handle_ja_contraction_quat_wt();
          prev_char = 0;
          if (quat_wt > 0) return quat_wt;
        }
        prev_char = 0;
        return *cweight;
      } else if (ob_uca_can_be_contraction_head(uca->contraction_flags, wc)) {

        size_t chars_skipped;
        if ((cweight = contraction_find(wc, &chars_skipped))) return *cweight;
      }
      prev_char = wc;
    }
    if (LEVELS_FOR_COMPARE == 4 && cs->coll_param == &ja_coll_param) {
      int quat_wt = handle_ja_common_quat_wt(wc);
      if (quat_wt == -1)
        continue;
      else if (quat_wt)
        return quat_wt;
    }
    unsigned int page = wc >> 8;
    unsigned int code = wc & 0xFF;
    const uint16_t *wpage = uca->weights[page];
    if (!wpage) return next_implicit(wc);
    wbeg = UCA900_WEIGHT_ADDR(wpage, weight_lv, code);
    wbeg_stride = UCA900_DISTANCE_BETWEEN_WEIGHTS;
    num_of_ce_left = UCA900_NUM_OF_CE(wpage, code);
  } while (!wbeg[0]);
  uint16_t rtn = *wbeg;
  wbeg += wbeg_stride;
  --num_of_ce_left;
  return rtn;
}
template <class Mb_wc, int LEVELS_FOR_COMPARE>
template <class T, class U>
ALWAYS_INLINE void uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE>::for_each_weight(
    T func, U preaccept_data) {
  if (cs->tailoring || cs->mbminlen != 1 || cs->coll_param) {
    // Slower, generic path.
    int s_res;
    while ((s_res = next()) >= 0) {
      if (!func(s_res, s_res == 0)) return;
    }
    return;
  }
    const uint16_t *ascii_wpage =
      UCA900_WEIGHT_ADDR(uca->weights[0], weight_lv, 0);
    const unsigned char *send_local = (send - sbeg > 3) ? (send - 3) : sbeg;
  for (;;) {
        int s_res;
    while ((s_res = more_weight()) >= 0) {
      if (!func(s_res, s_res == 0)) return;
    }
        const unsigned char *sbeg_local = sbeg;
    while (sbeg_local < send_local && preaccept_data(sizeof(uint32))) {
            uint32 four_bytes;
      memcpy(&four_bytes, sbeg_local, sizeof(four_bytes));
      if (((four_bytes + 0x01010101u) & 0x80808080) ||
          ((four_bytes - 0x20202020u) & 0x80808080))
        break;
      const int s_res0 = ascii_wpage[sbeg_local[0]];
      const int s_res1 = ascii_wpage[sbeg_local[1]];
      const int s_res2 = ascii_wpage[sbeg_local[2]];
      const int s_res3 = ascii_wpage[sbeg_local[3]];
      ob_charset_assert(s_res0 != 0);
      ob_charset_assert(s_res1 != 0);
      ob_charset_assert(s_res2 != 0);
      ob_charset_assert(s_res3 != 0);
      func(s_res0, false);
      func(s_res1, false);
      func(s_res2, false);
      func(s_res3, false);
      sbeg_local += sizeof(uint32);
    }
    sbeg = sbeg_local;
    // Do a single code point in the generic path.
    s_res = next_raw();
    if (s_res == 0) {
      // Level separator, so we have to update our page pointer.
      ascii_wpage += UCA900_DISTANCE_BETWEEN_LEVELS;
    }
    if (s_res < 0 || !func(s_res, s_res == 0)) return;
  }
}
template <class Mb_wc, int LEVELS_FOR_COMPARE>
uint16_t uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE>::apply_reorder_param(
    uint16_t weight) {
  if (cs->coll_param == &zh_coll_param
      || cs->coll_param == &zh2_coll_param
      || cs->coll_param == &zh3_coll_param) return weight;
  const Reorder_param *param = cs->coll_param->reorder_param;
  if (weight >= START_WEIGHT_TO_REORDER && weight <= param->max_weight) {
    for (int rec_ind = 0; rec_ind < param->wt_rec_num; ++rec_ind) {
      const Reorder_wt_rec *wt_rec = param->wt_rec + rec_ind;
      if (weight >= wt_rec->old_wt_bdy.begin &&
          weight <= wt_rec->old_wt_bdy.end) {
        if (param == &ja_reorder_param && wt_rec->new_wt_bdy.begin == 0) {
          return_origin_weight = !return_origin_weight;
          if (return_origin_weight) break;
          wbeg -= wbeg_stride;
          ++num_of_ce_left;
          return 0xFB86;
        }
        // Regular (non-Japanese-specific) reordering.
        return weight - wt_rec->old_wt_bdy.begin + wt_rec->new_wt_bdy.begin;
      }
    }
  }
  return weight;
}
// See Unicode TR35 section 3.14.1.
static bool is_tertiary_weight_upper_case(uint16_t weight) {
  if ((weight >= 0x08 && weight <= 0x0C) || weight == 0x0E || weight == 0x11 ||
      weight == 0x12 || weight == 0x1D) {
    return true;
  } else {
    return false;
  }
}
template <class Mb_wc, int LEVELS_FOR_COMPARE>
uint16_t uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE>::apply_case_first(
    uint16_t weight) {
  if (cs->coll_param->case_first == CASE_FIRST_UPPER && weight_lv == 2 &&
      weight < 0x20) {
    if (is_tertiary_weight_upper_case(weight)) {
      weight |= CASE_FIRST_UPPER_MASK;
    } else {
      weight |= CASE_FIRST_LOWER_MASK;
    }
  }
  return weight;
}
template <class Mb_wc, int LEVELS_FOR_COMPARE>
ALWAYS_INLINE int uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE>::next() {
  int res = next_raw();
  Coll_param *param = cs->coll_param;
  if (res > 0 && param) {
    if (param->reorder_param && weight_lv == 0) {
      res = apply_reorder_param(res);
    }
    if (param->case_first != CASE_FIRST_OFF) {
      res = apply_case_first(res);
    }
  }
  return res;
}
template <class Scanner, int LEVELS_FOR_COMPARE, class Mb_wc>
static int ob_strnncoll_uca(const ObCharsetInfo *cs, const Mb_wc mb_wc,
                            const unsigned char *s, size_t slen, const unsigned char *t,
                            size_t tlen, bool t_is_prefix) {
  Scanner sscanner(mb_wc, cs, s, slen);
  Scanner tscanner(mb_wc, cs, t, tlen);
  int s_res = 0;
  int t_res = 0;
  for (unsigned int current_lv = 0; current_lv < LEVELS_FOR_COMPARE; ++current_lv) {
    do {
      s_res = sscanner.next();
      t_res = tscanner.next();
    } while (s_res == t_res && s_res >= 0 &&
             sscanner.get_weight_level() == current_lv &&
             tscanner.get_weight_level() == current_lv);
        if (sscanner.get_weight_level() == tscanner.get_weight_level()) {
      if (s_res == t_res && s_res >= 0) continue;
      break;  // Error or inequality found, end.
    }
    if (tscanner.get_weight_level() > current_lv) {
      // t ran out of weights on this level, and s didn't.
      if (t_is_prefix) {
        // Consume the rest of the weights from s.
        do {
          s_res = sscanner.next();
        } while (s_res >= 0 && sscanner.get_weight_level() == current_lv);
        if (s_res < 0) break;  // Error found, end.
        // s is now also on the next level. Continue comparison.
        continue;
      } else {
        // s is longer than t (and t_prefix isn't set).
        return 1;
      }
    }
    if (sscanner.get_weight_level() > current_lv) {
      // s ran out of weights on this level, and t didn't.
      return -1;
    }
    break;
  }
  return (s_res - t_res);
}
static inline int ob_space_weight(const ObCharsetInfo *cs)
{
  if (cs->uca && cs->uca->version == UCA_V900)
    return UCA900_WEIGHT(cs->uca->weights[0], 0, 0x20);
  else
    return cs->uca->weights[0][0x20 * cs->uca->lengths[0]];
}
template <class Mb_wc>
static int ob_strnncollsp_uca(const ObCharsetInfo *cs, Mb_wc mb_wc,
                              const unsigned char *s, size_t slen, const unsigned char *t,
                              size_t tlen) {
  int s_res, t_res;
  uca_scanner_any<Mb_wc> sscanner(mb_wc, cs, s, slen);
  uca_scanner_any<Mb_wc> tscanner(mb_wc, cs, t, tlen);
  do {
    s_res = sscanner.next();
    t_res = tscanner.next();
  } while (s_res == t_res && s_res > 0);
  if (s_res > 0 && t_res < 0) {
    t_res = ob_space_weight(cs);
    do {
      if (s_res != t_res) return (s_res - t_res);
      s_res = sscanner.next();
    } while (s_res > 0);
    return 0;
  }
  if (s_res < 0 && t_res > 0) {
    s_res = ob_space_weight(cs);
    do {
      if (s_res != t_res) return (s_res - t_res);
      t_res = tscanner.next();
    } while (t_res > 0);
    return 0;
  }
  return (s_res - t_res);
}
template <class Mb_wc>
static void ob_hash_sort_uca(const ObCharsetInfo *cs, Mb_wc mb_wc,
                             const unsigned char *s, size_t slen, ulong *n1,
                             ulong *n2, const bool calc_end_space __attribute__((unused)),
                             hash_algo hash_algo) {
  int s_res;
  ulong tmp1;
  ulong tmp2;
  int space_weight = ob_space_weight(cs);
  slen = cs->cset->lengthsp(cs, pointer_cast<const char *>(s), slen);
  uca_scanner_any<Mb_wc> scanner(mb_wc, cs, s, slen);
  if (NULL == hash_algo) {
    tmp1 = *n1;
    tmp2 = *n2;
    while ((s_res = scanner.next()) > 0) {
      tmp1 ^= (((tmp1 & 63) + tmp2) * (s_res >> 8)) + (tmp1 << 8);
      tmp2 += 3;
      tmp1 ^= (((tmp1 & 63) + tmp2) * (s_res & 0xFF)) + (tmp1 << 8);
      tmp2 += 3;
      if (s_res != space_weight) {
        *n1 = tmp1;
        *n2 = tmp2;
      }
    }
  } else {
    unsigned char data[HASH_BUFFER_LENGTH];
    unsigned int length = 0;
    tmp1 = *n1;
    unsigned int last_non_space_len = 0;
    while ((s_res = scanner.next()) > 0) {
      if (length > HASH_BUFFER_LENGTH - 4) {
        tmp1 = hash_algo((void*) &data, length, tmp1);
        length = 0;
        if (last_non_space_len > 0) {
          *n1 = hash_algo((void*) &data, last_non_space_len, tmp1);
          last_non_space_len = 0;
        }
      }
      memcpy(data + length, &s_res, 4);
      length += 4;
      if (s_res != space_weight) {
        last_non_space_len = length;
      }
    }
    if (last_non_space_len > 0) {
      n1[0] = hash_algo((void*) &data, last_non_space_len, tmp1);
    }
  }
}
template <class Mb_wc>
static size_t ob_strnxfrm_uca(const ObCharsetInfo *cs, Mb_wc mb_wc, unsigned char *dst,
                              size_t dstlen, unsigned int num_codepoints,
                              const unsigned char *src, size_t srclen, unsigned int flags) {
  unsigned char *d0 = dst;
  unsigned char *de = dst + dstlen;
  int s_res;
  uca_scanner_any<Mb_wc> scanner(mb_wc, cs, src, srclen);
  while (dst < de && (s_res = scanner.next()) > 0) {
    *dst++ = s_res >> 8;
    if (dst < de) *dst++ = s_res & 0xFF;
  }
  if (dst < de) {
    ob_charset_assert(num_codepoints >= scanner.get_char_index());
    num_codepoints -= scanner.get_char_index();
    if (num_codepoints) {
      unsigned int space_count = std::min<unsigned int>((de - dst) / 2, num_codepoints);
      s_res = ob_space_weight(cs);
      for (; space_count; space_count--) {
        dst = store16be(dst, s_res);
      }
    }
  }
  if ((flags & OB_STRXFRM_PAD_TO_MAXLEN) && dst < de) {
    s_res = ob_space_weight(cs);
    for (; dst < de;) {
      *dst++ = s_res >> 8;
      if (dst < de) *dst++ = s_res & 0xFF;
    }
  }
  return dst - d0;
}
static int ob_uca_charcmp_900(const ObCharsetInfo *cs, ob_wc_t wc1,
                              ob_wc_t wc2) {
  uint16_t *weight1_ptr = ob_char_weight_addr_900(cs->uca, wc1);
  uint16_t *weight2_ptr = ob_char_weight_addr_900(cs->uca, wc2);
  if (!weight1_ptr || !weight2_ptr) return wc1 != wc2;
  if (weight1_ptr[0] && weight2_ptr[0] && weight1_ptr[0] != weight2_ptr[0])
    return 1;
  size_t length1 = weight1_ptr[-UCA900_DISTANCE_BETWEEN_LEVELS];
  size_t length2 = weight2_ptr[-UCA900_DISTANCE_BETWEEN_LEVELS];
  for (int level = 0; level < cs->levels_for_compare; ++level) {
    size_t wt_ind1 = 0;
    size_t wt_ind2 = 0;
    uint16_t *weight1 = weight1_ptr + level * UCA900_DISTANCE_BETWEEN_LEVELS;
    uint16_t *weight2 = weight2_ptr + level * UCA900_DISTANCE_BETWEEN_LEVELS;
    while (wt_ind1 < length1 && wt_ind2 < length2) {
      // Zero weight is ignorable.
      for (; wt_ind1 < length1 && !*weight1; wt_ind1++)
        weight1 += UCA900_DISTANCE_BETWEEN_WEIGHTS;
      if (wt_ind1 == length1) break;
      for (; wt_ind2 < length2 && !*weight2; wt_ind2++)
        weight2 += UCA900_DISTANCE_BETWEEN_WEIGHTS;
      if (wt_ind2 == length2) break;
      // Check if these two non-ignorable weights are equal.
      if (*weight1 != *weight2) return 1;
      wt_ind1++;
      wt_ind2++;
      weight1 += UCA900_DISTANCE_BETWEEN_WEIGHTS;
      weight2 += UCA900_DISTANCE_BETWEEN_WEIGHTS;
    }
    for (; wt_ind1 < length1; wt_ind1++) {
      if (*weight1) return 1;
      weight1 += UCA900_DISTANCE_BETWEEN_WEIGHTS;
    }
    for (; wt_ind2 < length2; wt_ind2++) {
      if (*weight2) return 1;
      weight2 += UCA900_DISTANCE_BETWEEN_WEIGHTS;
    }
  }
  return 0;
}
static int ob_uca_charcmp(const ObCharsetInfo *cs, ob_wc_t wc1, ob_wc_t wc2) {
  if (wc1 == wc2) return 0;
  if (cs->uca != nullptr && cs->uca->version == UCA_V900)
    return ob_uca_charcmp_900(cs, wc1, wc2);
  size_t length1, length2;
  uint16_t *weight1 = ob_char_weight_addr(cs->uca, wc1);
  uint16_t *weight2 = ob_char_weight_addr(cs->uca, wc2);
  if (!weight1 || !weight2) {
    return wc1 != wc2;
  } else if (weight1[0] != weight2[0]) {
    return 1;
  }
  length1 = cs->uca->lengths[wc1 >> OB_UCA_PSHIFT];
  length2 = cs->uca->lengths[wc2 >> OB_UCA_PSHIFT];
  if (length1 > length2) {
    return memcmp((const void *)weight1, (const void *)weight2, length2 * 2)
               ? 1
               : weight1[length2];
  } else if (length1 < length2) {
    return memcmp((const void *)weight1, (const void *)weight2, length1 * 2)
               ? 1
               : weight2[length1];
  }
  return memcmp((const void *)weight1, (const void *)weight2, length1 * 2);
}
static int ob_wildcmp_uca_impl(const ObCharsetInfo *cs, const char *str,
                               const char *str_end, const char *wildstr,
                               const char *wildend, int escape, int w_one,
                               int w_many, int recurse_level) {
  while (wildstr != wildend) {
    int result = -1;
    auto mb_wc = cs->cset->mb_wc;
        ob_wc_t w_wc;
    while (true) {
      int mb_len;
      if ((mb_len = mb_wc(cs, &w_wc, (const unsigned char *)wildstr,
                          (const unsigned char *)wildend)) <= 0) {
        return 1;
      }
      wildstr += mb_len;
      // If we found '%' (w_many), break out this loop.
      if (w_wc == (ob_wc_t)w_many) {
        result = 1;
        break;
      }
      bool escaped = false;
      if (w_wc == (ob_wc_t)escape && wildstr < wildend) {
        if ((mb_len = mb_wc(cs, &w_wc, (const unsigned char *)wildstr,
                            (const unsigned char *)wildend)) <= 0)
          return 1;
        wildstr += mb_len;
        escaped = true;
      }
      ob_wc_t s_wc;
      if ((mb_len = mb_wc(cs, &s_wc, (const unsigned char *)str,
                          (const unsigned char *)str_end)) <= 0) {
        return 1;
      }
      str += mb_len;
      // If we found '_' (w_one), skip one character in expression string.
      if (!escaped && w_wc == (ob_wc_t)w_one) {
        result = 1;
      } else {
        if (ob_uca_charcmp(cs, s_wc, w_wc)) return 1;
      }
      if (wildstr == wildend) {
        return (str != str_end);
      }
    }
    if (w_wc == (ob_wc_t)w_many) {
      // Remove any '%' and '_' following w_many in the pattern string.
      for (;;) {
        if (wildstr == wildend) {
                    return 0;
        }
        int mb_len_wild =
            mb_wc(cs, &w_wc, (const unsigned char *)wildstr, (const unsigned char *)wildend);
        if (mb_len_wild <= 0) return 1;
        wildstr += mb_len_wild;
        if (w_wc == (ob_wc_t)w_many) continue;
        if (w_wc == (ob_wc_t)w_one) {
                    ob_wc_t s_wc;
          int mb_len =
              mb_wc(cs, &s_wc, (const unsigned char *)str, (const unsigned char *)str_end);
          if (mb_len <= 0) return 1;
          str += mb_len;
          continue;
        }
        break;
      }
      // No character in the expression string to match w_wc.
      if (str == str_end) return -1;
      // Skip the escape character ('\') in the pattern if needed.
      if (w_wc == (ob_wc_t)escape && wildstr < wildend) {
        int mb_len =
            mb_wc(cs, &w_wc, (const unsigned char *)wildstr, (const unsigned char *)wildend);
        if (mb_len <= 0) return 1;
        wildstr += mb_len;
      }
      while (true) {
                int mb_len = 0;
        while (str != str_end) {
          ob_wc_t s_wc;
          if ((mb_len = mb_wc(cs, &s_wc, (const unsigned char *)str,
                              (const unsigned char *)str_end)) <= 0)
            return 1;
          if (!ob_uca_charcmp(cs, s_wc, w_wc)) break;
          str += mb_len;
        }
        // No character in the expression string is equal to w_wc.
        if (str == str_end) return -1;
        str += mb_len;
                result = ob_wildcmp_uca_impl(cs, str, str_end, wildstr, wildend, escape,
                                     w_one, w_many, recurse_level + 1);
        if (result <= 0) return result;
      }
    }
  }
  return (str != str_end ? 1 : 0);
}
static int ob_strcasecmp_uca(const ObCharsetInfo *cs, const char *s,
                             const char *t) {
  const ObUnicaseInfo *uni_plane = cs->caseinfo;
  const ObUnicaseInfoChar *page;
  while (s[0] && t[0]) {
    ob_wc_t s_wc, t_wc;
    if (static_cast<unsigned char>(s[0]) < 128) {
      s_wc = uni_plane->page[0][static_cast<unsigned char>(s[0])].tolower;
      s++;
    } else {
      int res;
      res = cs->cset->mb_wc(cs, &s_wc, pointer_cast<const unsigned char *>(s),
                            pointer_cast<const unsigned char *>(s + 4));
      if (res <= 0) return strcmp(s, t);
      s += res;
      if (s_wc <= uni_plane->maxchar && (page = uni_plane->page[s_wc >> 8]))
        s_wc = page[s_wc & 0xFF].tolower;
    }
    if (static_cast<unsigned char>(t[0]) < 128) {
      t_wc = uni_plane->page[0][static_cast<unsigned char>(t[0])].tolower;
      t++;
    } else {
      int res = cs->cset->mb_wc(cs, &t_wc, pointer_cast<const unsigned char *>(t),
                                pointer_cast<const unsigned char *>(t + 4));
      if (res <= 0) return strcmp(s, t);
      t += res;
      if (t_wc <= uni_plane->maxchar && (page = uni_plane->page[t_wc >> 8]))
        t_wc = page[t_wc & 0xFF].tolower;
    }
    if (s_wc != t_wc) return static_cast<int>(s_wc) - static_cast<int>(t_wc);
  }
  return static_cast<int>(static_cast<unsigned char>(s[0])) -
         static_cast<int>(static_cast<unsigned char>(t[0]));
}
extern "C" {
static int ob_wildcmp_uca(const ObCharsetInfo *cs, const char *str,
                          const char *str_end, const char *wildstr,
                          const char *wildend, int escape, int w_one,
                          int w_many) {
  return ob_wildcmp_uca_impl(cs, str, str_end, wildstr, wildend, escape, w_one,
                             w_many, 1);
}
}  // extern "C"
extern "C" {
static int ob_strnncoll_any_uca(const ObCharsetInfo *cs, const unsigned char *s,
                                size_t slen, const unsigned char *t, size_t tlen,
                                bool t_is_prefix) {
  if (cs->cset->mb_wc == ob_mb_wc_utf8mb4_thunk) {
    return ob_strnncoll_uca<uca_scanner_any<Mb_wc_utf8mb4>, 1>(
        cs, Mb_wc_utf8mb4(), s, slen, t, tlen, t_is_prefix);
  }
  Mb_wc_through_function_pointer mb_wc(cs);
  return ob_strnncoll_uca<uca_scanner_any<decltype(mb_wc)>, 1>(
      cs, mb_wc, s, slen, t, tlen, t_is_prefix);
}
static int ob_strnncollsp_any_uca(const ObCharsetInfo *cs, const unsigned char *s,
                                  size_t slen, const unsigned char *t, size_t tlen,
                                  bool diff_if_only_endspace_difference __attribute__((unused))) {
  if (cs->cset->mb_wc == ob_mb_wc_utf8mb4_thunk) {
    return ob_strnncollsp_uca(cs, Mb_wc_utf8mb4(), s, slen, t, tlen);
  }
  Mb_wc_through_function_pointer mb_wc(cs);
  return ob_strnncollsp_uca(cs, mb_wc, s, slen, t, tlen);
}
static void ob_hash_sort_any_uca(const ObCharsetInfo *cs, const unsigned char *s,
                                 size_t slen, ulong *n1, ulong *n2,
                                 const bool calc_end_space,
                                 hash_algo hash_algo) {
  if (cs->cset->mb_wc == ob_mb_wc_utf8mb4_thunk) {
    ob_hash_sort_uca(cs, Mb_wc_utf8mb4(), s, slen, n1, n2, calc_end_space, hash_algo);
  } else {
    Mb_wc_through_function_pointer mb_wc(cs);
    ob_hash_sort_uca(cs, mb_wc, s, slen, n1, n2, calc_end_space, hash_algo);
  }
}
static size_t ob_strnxfrm_any_uca(const ObCharsetInfo *cs, unsigned char *dst,
                                  size_t dstlen, unsigned int num_codepoints,
                                  const unsigned char *src, size_t srclen, unsigned int flags,
                                  bool *is_valid_unicode) {
  *is_valid_unicode = true;
  if (cs->cset->mb_wc == ob_mb_wc_utf8mb4_thunk) {
    return ob_strnxfrm_uca(cs, Mb_wc_utf8mb4(), dst, dstlen, num_codepoints,
                           src, srclen, flags);
  }
  Mb_wc_through_function_pointer mb_wc(cs);
  return ob_strnxfrm_uca(cs, mb_wc, dst, dstlen, num_codepoints, src, srclen,
                         flags);
}
static int ob_strnncoll_uca_900(const ObCharsetInfo *cs, const unsigned char *s,
                                size_t slen, const unsigned char *t, size_t tlen,
                                bool t_is_prefix) {
  if (cs->cset->mb_wc == ob_mb_wc_utf8mb4_thunk) {
    switch (cs->levels_for_compare) {
      case 1:
        return ob_strnncoll_uca<uca_scanner_900<Mb_wc_utf8mb4, 1>, 1>(
            cs, Mb_wc_utf8mb4(), s, slen, t, tlen, t_is_prefix);
      case 2:
        return ob_strnncoll_uca<uca_scanner_900<Mb_wc_utf8mb4, 2>, 2>(
            cs, Mb_wc_utf8mb4(), s, slen, t, tlen, t_is_prefix);
      default:
        ob_charset_assert(false);
      case 3:
        return ob_strnncoll_uca<uca_scanner_900<Mb_wc_utf8mb4, 3>, 3>(
            cs, Mb_wc_utf8mb4(), s, slen, t, tlen, t_is_prefix);
      case 4:
        return ob_strnncoll_uca<uca_scanner_900<Mb_wc_utf8mb4, 4>, 4>(
            cs, Mb_wc_utf8mb4(), s, slen, t, tlen, t_is_prefix);
    }
  }
  Mb_wc_through_function_pointer mb_wc(cs);
  switch (cs->levels_for_compare) {
    case 1:
      return ob_strnncoll_uca<uca_scanner_900<decltype(mb_wc), 1>, 1>(
          cs, mb_wc, s, slen, t, tlen, t_is_prefix);
    case 2:
      return ob_strnncoll_uca<uca_scanner_900<decltype(mb_wc), 2>, 2>(
          cs, mb_wc, s, slen, t, tlen, t_is_prefix);
    default:
      ob_charset_assert(false);
    case 3:
      return ob_strnncoll_uca<uca_scanner_900<decltype(mb_wc), 3>, 3>(
          cs, mb_wc, s, slen, t, tlen, t_is_prefix);
    case 4:
      return ob_strnncoll_uca<uca_scanner_900<decltype(mb_wc), 4>, 4>(
          cs, mb_wc, s, slen, t, tlen, t_is_prefix);
  }
  return 0;
}
static int ob_strnncollsp_uca_900(const ObCharsetInfo *cs, const unsigned char *s,
                                  size_t slen, const unsigned char *t, size_t tlen,
                                  bool diff_if_only_endspace_difference  __attribute__((unused))) {
  // We are a NO PAD collation, so this is identical to strnncoll.
  return ob_strnncoll_uca_900(cs, s, slen, t, tlen, false);
}
}  // extern "C"
template <class Mb_wc, int LEVELS_FOR_COMPARE>
static void ob_hash_sort_uca_900_tmpl(const ObCharsetInfo *cs, const Mb_wc mb_wc,
                                      const unsigned char *s, size_t slen, ulong *n1) {
  uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE> scanner(mb_wc, cs, s, slen);

  uint64 h = *n1;
  h ^= 14695981039346656037ULL;
  scanner.for_each_weight(
      [&](int s_res, bool) -> bool {
        h ^= s_res;
        h *= 1099511628211ULL;
        return true;
      },
      [](int) { return true; });
  *n1 = h;
}
extern "C" {
static void ob_hash_sort_uca_900(const ObCharsetInfo *cs, const unsigned char *s,
                                 size_t slen, ulong *n1, ulong *, const bool, hash_algo) {
  if (cs->cset->mb_wc == ob_mb_wc_utf8mb4_thunk) {
    switch (cs->levels_for_compare) {
      case 1:
        return ob_hash_sort_uca_900_tmpl<Mb_wc_utf8mb4, 1>(cs, Mb_wc_utf8mb4(),
                                                           s, slen, n1);
      case 2:
        return ob_hash_sort_uca_900_tmpl<Mb_wc_utf8mb4, 2>(cs, Mb_wc_utf8mb4(),
                                                           s, slen, n1);
      default:
        ob_charset_assert(false);
      case 3:
        return ob_hash_sort_uca_900_tmpl<Mb_wc_utf8mb4, 3>(cs, Mb_wc_utf8mb4(),
                                                           s, slen, n1);
      case 4:
        return ob_hash_sort_uca_900_tmpl<Mb_wc_utf8mb4, 4>(cs, Mb_wc_utf8mb4(),
                                                           s, slen, n1);
    }
  }
  Mb_wc_through_function_pointer mb_wc(cs);
  switch (cs->levels_for_compare) {
    case 1:
      return ob_hash_sort_uca_900_tmpl<decltype(mb_wc), 1>(cs, mb_wc, s, slen,
                                                           n1);
    case 2:
      return ob_hash_sort_uca_900_tmpl<decltype(mb_wc), 2>(cs, mb_wc, s, slen,
                                                           n1);
    default:
      ob_charset_assert(false);
    case 3:
      return ob_hash_sort_uca_900_tmpl<decltype(mb_wc), 3>(cs, mb_wc, s, slen,
                                                           n1);
    case 4:
      return ob_hash_sort_uca_900_tmpl<decltype(mb_wc), 4>(cs, mb_wc, s, slen,
                                                           n1);
  }
}
}

#define STRING_WITH_LEN(X) (X), ((sizeof(X) - 1))
enum ob_charset_err {
  OB_ERR_COLLATION_PARSER_ERROR = 85,
  OB_ERR_FAILED_TO_RESET_BEFORE_PRIMARY_IGNORABLE_CHAR = 86,
  OB_ERR_FAILED_TO_RESET_BEFORE_TERTIARY_IGNORABLE_CHAR = 87,
  OB_ERR_SHIFT_CHAR_OUT_OF_RANGE = 88,
  OB_ERR_RESET_CHAR_OUT_OF_RANGE = 89,
  OB_ERR_UNKNOWN_LDML_TAG = 90,
  OB_ERR_FAILED_TO_RESET_BEFORE_SECONDARY_IGNORABLE_CHAR = 91,
  OB_ERR_ERROR_LAST = 91,
};
typedef enum ob_coll_lexem_num_en {
  OB_COLL_LEXEM_EOF = 0,
  OB_COLL_LEXEM_SHIFT = 1,
  OB_COLL_LEXEM_RESET = 4,
  OB_COLL_LEXEM_CHAR = 5,
  OB_COLL_LEXEM_ERROR = 6,
  OB_COLL_LEXEM_OPTION = 7,
  OB_COLL_LEXEM_EXTEND = 8,
  OB_COLL_LEXEM_CONTEXT = 9
} ob_coll_lexem_num;
static const char *ob_coll_lexem_num_to_str(ob_coll_lexem_num term) {
  switch (term) {
    case OB_COLL_LEXEM_EOF:
      return "EOF";
    case OB_COLL_LEXEM_SHIFT:
      return "Shift";
    case OB_COLL_LEXEM_RESET:
      return "&";
    case OB_COLL_LEXEM_CHAR:
      return "Character";
    case OB_COLL_LEXEM_OPTION:
      return "Bracket option";
    case OB_COLL_LEXEM_EXTEND:
      return "/";
    case OB_COLL_LEXEM_CONTEXT:
      return "|";
    case OB_COLL_LEXEM_ERROR:
      return "ERROR";
  }
  return nullptr;
}
struct ObCollLexem {
  ob_coll_lexem_num term;
  const char *beg;
  const char *end;
  const char *prev;
  int diff;
  int code;
};
struct ObCollRule {
  ob_wc_t base[OB_UCA_MAX_EXPANSION];
  ob_wc_t curr[OB_UCA_MAX_CONTRACTION];
  int diff[4];
  size_t before_level;
  bool with_context;
};
typedef enum {
  ob_shift_method_simple = 0,
  ob_shift_method_expand
} ObCollShiftMethod;
struct ObCollRules {
  ObUCAInfo *uca;
  size_t nrules;
  size_t mrules;
  ObCollRule *rule;
  ObCharsetLoader *loader;
  ObCollShiftMethod shift_after_method;
};
struct ObCollRuleParser {
  ObCollLexem tok[2];
  ObCollRule rule;
  ObCollRules *rules;
  char errstr[128];
};
static size_t ob_wstrnlen(ob_wc_t *s, size_t maxlen) {
  for (size_t i = 0; i < maxlen; i++) {
    if (s[i] == 0) {
      return i;
    }
  }
  return maxlen;
}
static void ob_coll_lexem_init(ObCollLexem *lexem, const char *str,
                               const char *str_end) {
  lexem->beg = str;
  lexem->prev = str;
  lexem->end = str_end;
  lexem->diff = 0;
  lexem->code = 0;
}
static int lex_cmp(ObCollLexem *lexem, const char *pattern,
                   size_t patternlen) {
  size_t lexemlen = lexem->beg - lexem->prev;
  if (lexemlen < patternlen) {
    return 1;
  } else {
  return strncasecmp(lexem->prev, pattern, patternlen);
  }
}
static void ob_coll_lexem_print_error(ObCollLexem *lexem, char *errstr,
                                      size_t errsize, const char *txt,
                                      const char *col_name) {
  char tail[30];
  size_t len = lexem->end - lexem->prev;
  strmake(tail, lexem->prev, std::min(len, sizeof(tail) - 1));
  errstr[errsize - 1] = '\0';
  snprintf(errstr, errsize - 1, "%s at '%s' for COLLATION : %s",
           txt[0] ? txt : "Syntax error", tail, col_name);
}
static int ch2x(int ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  } else if (ch >= 'a' && ch <= 'f') {
    return 10 + ch - 'a';
  } else if (ch >= 'A' && ch <= 'F') {
    return 10 + ch - 'A';
  }
  return -1;
}
static ob_coll_lexem_num ob_coll_lexem_next(ObCollLexem *lexem) {
  const char *beg;
  ob_coll_lexem_num rc;
  for (beg = lexem->beg; beg < lexem->end; beg++) {
    switch (*beg) {
      case ' ':
      case '\t':
      case '\r':
      case '\n':
        continue;
      case '[':
      {
        size_t nbrackets;
        for (beg++, nbrackets = 1; beg < lexem->end; beg++) {
          if (*beg == '[') {
            nbrackets++;
          } else if (*beg == ']') {
            if (--nbrackets == 0) {
              rc = OB_COLL_LEXEM_OPTION;
              beg++;
              goto ex;
            }
          }
        }
        rc = OB_COLL_LEXEM_ERROR;
        goto ex;
      }
      case '&':
        beg++;
        rc = OB_COLL_LEXEM_RESET;
        goto ex;
      case '=':
        beg++;
        lexem->diff = 0;
        rc = OB_COLL_LEXEM_SHIFT;
        goto ex;
      case '/':
        beg++;
        rc = OB_COLL_LEXEM_EXTEND;
        goto ex;
      case '|':
        beg++;
        rc = OB_COLL_LEXEM_CONTEXT;
        goto ex;
      case '<':
      {
        for (beg++, lexem->diff = 1;
             (beg < lexem->end) && (*beg == '<') && (lexem->diff <= 3);
             beg++, lexem->diff++)
          ;
        rc = OB_COLL_LEXEM_SHIFT;
        goto ex;
      }
      default:
        break;
    }
    if ((*beg == '\\') && (beg + 2 < lexem->end) && (beg[1] == 'u') &&
        ob_isxdigit(&ob_charset_utf8mb4_general_ci, beg[2])) {
      int ch;
      beg += 2;
      lexem->code = 0;
      while ((beg < lexem->end) && ((ch = ch2x(beg[0])) >= 0)) {
        lexem->code = (lexem->code << 4) + ch;
        beg++;
      }
      rc = OB_COLL_LEXEM_CHAR;
      goto ex;
    }
    if (*beg >= 0x21 && *beg <= 0x7E) {
      lexem->code = *beg++;
      rc = OB_COLL_LEXEM_CHAR;
      goto ex;
    }
    if (((unsigned char)*beg) > 0x7F)
    {
      ObCharsetInfo *cs = &ob_charset_utf8mb4_general_ci;
      ob_wc_t wc;
      int nbytes = cs->cset->mb_wc(cs, &wc, pointer_cast<const unsigned char *>(beg),
                                   pointer_cast<const unsigned char *>(lexem->end));
      if (nbytes > 0) {
        rc = OB_COLL_LEXEM_CHAR;
        beg += nbytes;
        lexem->code = (int)wc;
        goto ex;
      }
    }
    rc = OB_COLL_LEXEM_ERROR;
    goto ex;
  }
  rc = OB_COLL_LEXEM_EOF;
ex:
  lexem->prev = lexem->beg;
  lexem->beg = beg;
  lexem->term = rc;
  return rc;
}
static inline size_t ob_coll_rule_reset_length(ObCollRule *r) {
  return ob_wstrnlen(r->base, OB_UCA_MAX_EXPANSION);
}
static inline size_t ob_coll_rule_shift_length(ObCollRule *r) {
  return ob_wstrnlen(r->curr, OB_UCA_MAX_CONTRACTION);
}
static int ob_coll_rule_expand(ob_wc_t *wc, size_t limit, ob_wc_t code) {
  size_t i;
  for (i = 0; i < limit; i++) {
    if (wc[i] == 0) {
      wc[i] = code;
      return 1;
    }
  }
  return 0;
}
static void ob_coll_rule_reset(ObCollRule *r) { memset(r, 0, sizeof(*r)); }
static int ob_coll_rules_realloc(ObCollRules *rules, size_t n) {
  if (rules->nrules < rules->mrules ||
      (rules->rule = static_cast<ObCollRule *>(rules->loader->mem_realloc(
           rules->rule, sizeof(ObCollRule) * (rules->mrules = n + 128)))))
    return 0;
  return -1;
}
static int ob_coll_rules_add(ObCollRules *rules, ObCollRule *rule) {
  if (ob_coll_rules_realloc(rules, rules->nrules + 1)) return -1;
  rules->rule[rules->nrules++] = rule[0];
  return 0;
}
static void ob_coll_rule_shift_at_level(ObCollRule *r, int level) {
  switch (level) {
    case 4:
      r->diff[3]++;
      break;
    case 3:
      r->diff[2]++;
      r->diff[3] = 0;
      break;
    case 2:
      r->diff[1]++;
      r->diff[2] = r->diff[3] = 0;
      break;
    case 1:
      r->diff[0]++;
      r->diff[1] = r->diff[2] = r->diff[3] = 0;
      break;
    case 0:

      break;
    default:
      ob_charset_assert(0);
  }
}
static ObCollLexem *ob_coll_parser_curr(ObCollRuleParser *p) {
  return &p->tok[0];
}
static ObCollLexem *ob_coll_parser_next(ObCollRuleParser *p) {
  return &p->tok[1];
}
static int ob_coll_parser_scan(ObCollRuleParser *p) {
  ob_coll_parser_curr(p)[0] = ob_coll_parser_next(p)[0];
  ob_coll_lexem_next(ob_coll_parser_next(p));
  return 1;
}
static void ob_coll_parser_init(ObCollRuleParser *p, ObCollRules *rules,
                                const char *str, const char *str_end) {
    memset(p, 0, sizeof(*p));
  p->rules = rules;
  p->errstr[0] = '\0';
  ob_coll_lexem_init(ob_coll_parser_curr(p), str, str_end);
  ob_coll_lexem_next(ob_coll_parser_curr(p));
  ob_coll_parser_next(p)[0] = ob_coll_parser_curr(p)[0];
  ob_coll_lexem_next(ob_coll_parser_next(p));
}
static int ob_coll_parser_expected_error(ObCollRuleParser *p,
                                         ob_coll_lexem_num term) {
  snprintf(p->errstr, sizeof(p->errstr), "%s expected",
           ob_coll_lexem_num_to_str(term));
  return 0;
}
static int ob_coll_parser_too_long_error(ObCollRuleParser *p,
                                         const char *name) {
  snprintf(p->errstr, sizeof(p->errstr), "%s is too long", name);
  return 0;
}
static int ob_coll_parser_scan_term(ObCollRuleParser *p,
                                    ob_coll_lexem_num term) {
  if (ob_coll_parser_curr(p)->term != term) {
    return ob_coll_parser_expected_error(p, term);
  }
  return ob_coll_parser_scan(p);
}
static int ob_coll_parser_scan_setting(ObCollRuleParser *p) {
  ObCollRules *rules = p->rules;
  ObCollLexem *lexem = ob_coll_parser_curr(p);
  if (!lex_cmp(lexem, STRING_WITH_LEN("[version 4.0.0]"))) {
    rules->uca = &ob_uca_v400;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[version 5.2.0]"))) {
    rules->uca = &ob_uca_v520;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[shift-after-method expand]"))) {
    rules->shift_after_method = ob_shift_method_expand;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[shift-after-method simple]"))) {
    rules->shift_after_method = ob_shift_method_simple;
  } else {
    return 0;
  }
  return ob_coll_parser_scan(p);
}
static int ob_coll_parser_scan_settings(ObCollRuleParser *p) {

  while (ob_coll_parser_curr(p)->term == OB_COLL_LEXEM_OPTION) {
    if (!ob_coll_parser_scan_setting(p)) {
      return 0;
    }
  }
  return 1;
}
static int ob_coll_parser_scan_reset_before(ObCollRuleParser *p) {
  ObCollLexem *lexem = ob_coll_parser_curr(p);
  if (!lex_cmp(lexem, STRING_WITH_LEN("[before primary]")) ||
      !lex_cmp(lexem, STRING_WITH_LEN("[before 1]"))) {
    p->rule.before_level = 1;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[before secondary]")) ||
             !lex_cmp(lexem, STRING_WITH_LEN("[before 2]"))) {
    p->rule.before_level = 2;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[before tertiary]")) ||
             !lex_cmp(lexem, STRING_WITH_LEN("[before 3]"))) {
    p->rule.before_level = 3;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[before quaternary]")) ||
             !lex_cmp(lexem, STRING_WITH_LEN("[before 4]"))) {
    p->rule.before_level = 4;
  } else {
    p->rule.before_level = 0;
    return 0;
  }
  return ob_coll_parser_scan(p);
}
static int ob_coll_parser_scan_logical_position(ObCollRuleParser *p,
                                                ob_wc_t *pwc, size_t limit) {
  ObCollRules *rules = p->rules;
  ObCollLexem *lexem = ob_coll_parser_curr(p);
  if (!lex_cmp(lexem, STRING_WITH_LEN("[first non-ignorable]"))) {
    lexem->code = rules->uca->first_non_ignorable;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[last non-ignorable]"))) {
    lexem->code = rules->uca->last_non_ignorable;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[first primary ignorable]"))) {
    lexem->code = rules->uca->first_primary_ignorable;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[last primary ignorable]"))) {
    lexem->code = rules->uca->last_primary_ignorable;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[first secondary ignorable]"))) {
    lexem->code = rules->uca->first_secondary_ignorable;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[last secondary ignorable]"))) {
    lexem->code = rules->uca->last_secondary_ignorable;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[first tertiary ignorable]"))) {
    lexem->code = rules->uca->first_tertiary_ignorable;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[last tertiary ignorable]"))) {
    lexem->code = rules->uca->last_tertiary_ignorable;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[first trailing]"))) {
    lexem->code = rules->uca->first_trailing;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[last trailing]"))) {
    lexem->code = rules->uca->last_trailing;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[first variable]"))) {
    lexem->code = rules->uca->first_variable;
  } else if (!lex_cmp(lexem, STRING_WITH_LEN("[last variable]"))) {
    lexem->code = rules->uca->last_variable;
  } else {
    return 0;
  }
  if (!ob_coll_rule_expand(pwc, limit, lexem->code)) {
        ob_charset_assert(0);
    return ob_coll_parser_too_long_error(p, "Logical position");
  }
  return ob_coll_parser_scan(p);
}
static int ob_coll_parser_scan_character_list(ObCollRuleParser *p,
                                              ob_wc_t *pwc, size_t limit,
                                              const char *name) {
  if (ob_coll_parser_curr(p)->term != OB_COLL_LEXEM_CHAR) {
    return ob_coll_parser_expected_error(p, OB_COLL_LEXEM_CHAR);
  }
  if (!ob_coll_rule_expand(pwc, limit, ob_coll_parser_curr(p)->code)) {
    return ob_coll_parser_too_long_error(p, name);
  }
  if (!ob_coll_parser_scan_term(p, OB_COLL_LEXEM_CHAR)) {
    return 0;
  }
  while (ob_coll_parser_curr(p)->term == OB_COLL_LEXEM_CHAR) {
    if (!ob_coll_rule_expand(pwc, limit, ob_coll_parser_curr(p)->code)) {
      return ob_coll_parser_too_long_error(p, name);
    }
    ob_coll_parser_scan(p);
  }
  return 1;
}
static int ob_coll_parser_scan_reset_sequence(ObCollRuleParser *p) {
  ob_coll_rule_reset(&p->rule);
  if (ob_coll_parser_curr(p)->term == OB_COLL_LEXEM_OPTION) {
    ob_coll_parser_scan_reset_before(p);
  }
  if (ob_coll_parser_curr(p)->term == OB_COLL_LEXEM_OPTION) {
    if (!ob_coll_parser_scan_logical_position(p, p->rule.base, 1)) return 0;
  } else {
    if (!ob_coll_parser_scan_character_list(p, p->rule.base,
                                            OB_UCA_MAX_EXPANSION, "Expansion"))
      return 0;
  }
  if ((p->rules->shift_after_method == ob_shift_method_expand ||
       p->rule.before_level == 1) &&
      p->rules->uca->version < UCA_V900)
  {
    if (!ob_coll_rule_expand(p->rule.base, OB_UCA_MAX_EXPANSION,
                             p->rules->uca->last_non_ignorable)) {
      return ob_coll_parser_too_long_error(p, "Expansion");
    }
  }
  return 1;
}
static int ob_coll_parser_scan_shift_sequence(ObCollRuleParser *p) {
  ObCollRule before_extend;
  memset(&p->rule.curr, 0, sizeof(p->rule.curr));
  if (!ob_coll_parser_scan_character_list(
          p, p->rule.curr, OB_UCA_MAX_CONTRACTION, "Contraction"))
    return 0;
  before_extend = p->rule;

  if (ob_coll_parser_curr(p)->term == OB_COLL_LEXEM_EXTEND) {
    ob_coll_parser_scan(p);
    if (!ob_coll_parser_scan_character_list(p, p->rule.base,
                                            OB_UCA_MAX_EXPANSION, "Expansion"))
      return 0;
  } else if (ob_coll_parser_curr(p)->term == OB_COLL_LEXEM_CONTEXT) {
        ob_coll_parser_scan(p);
    p->rule.with_context = true;
    if (!ob_coll_parser_scan_character_list(
            p, p->rule.curr + 1, OB_UCA_MAX_EXPANSION - 1, "context"))
      return 0;
        if (ob_coll_parser_curr(p)->term == OB_COLL_LEXEM_EXTEND) {
      ob_coll_parser_scan(p);
      size_t len = ob_wstrnlen(p->rule.base, OB_UCA_MAX_EXPANSION);
      if (!ob_coll_parser_scan_character_list(
              p, p->rule.base + len, OB_UCA_MAX_EXPANSION - len, "Expansion"))
        return 0;
    }
  }
  if (ob_coll_rules_add(p->rules, &p->rule)) {
    return 0;
  }
  p->rule = before_extend;
  return 1;
}
static int ob_coll_parser_scan_shift(ObCollRuleParser *p) {
  if (ob_coll_parser_curr(p)->term == OB_COLL_LEXEM_SHIFT) {
    ob_coll_rule_shift_at_level(&p->rule, ob_coll_parser_curr(p)->diff);
    return ob_coll_parser_scan(p);
  }
  return 0;
}
static int ob_coll_parser_scan_rule(ObCollRuleParser *p) {
  if (!ob_coll_parser_scan_term(p, OB_COLL_LEXEM_RESET) ||
      !ob_coll_parser_scan_reset_sequence(p)) {
    return 0;
  }
  if (!ob_coll_parser_scan_shift(p)) {
    return ob_coll_parser_expected_error(p, OB_COLL_LEXEM_SHIFT);
  }
  if (!ob_coll_parser_scan_shift_sequence(p)) {
    return 0;
  }
  while (ob_coll_parser_scan_shift(p)) {
    if (!ob_coll_parser_scan_shift_sequence(p)) {
      return 0;
    }
  }
  return 1;
}
static int ob_coll_parser_exec(ObCollRuleParser *p) {
  if (!ob_coll_parser_scan_settings(p)) {
    return 0;
  }
  while (ob_coll_parser_curr(p)->term == OB_COLL_LEXEM_RESET) {
    if (!ob_coll_parser_scan_rule(p)) return 0;
  }
  return ob_coll_parser_scan_term(p, OB_COLL_LEXEM_EOF);
}
static int ob_coll_rule_parse(ObCollRules *rules, const char *str,
                              const char *str_end, const char *col_name) {
  ObCollRuleParser p;
  ob_coll_parser_init(&p, rules, str, str_end);
  if (!ob_coll_parser_exec(&p)) {
    rules->loader->errcode = OB_ERR_COLLATION_PARSER_ERROR;
    ob_coll_lexem_print_error(ob_coll_parser_curr(&p), rules->loader->errarg,
                              sizeof(rules->loader->errarg) - 1, p.errstr,
                              col_name);
    return 1;
  }
  return 0;
}
static void spread_case_mask(uint16_t *to, size_t to_stride,
                             size_t tailored_ce_cnt, uint16_t case_mask) {
  for (size_t i = 0; i < tailored_ce_cnt; ++i) {
    uint16_t *case_weight = &to[(i * OB_UCA_900_CE_SIZE + 2) * to_stride];
    if (*case_weight > CASE_FIRST_UPPER_MASK) {
      case_mask = *case_weight & 0xFF00;
    } else if (*case_weight) {
      *case_weight |= case_mask;
    }
  }
}
static void change_weight_if_case_first(ObCharsetInfo *cs,
                                        const ObUCAInfo *dst, ObCollRule *r,
                                        uint16_t *to, size_t to_stride,
                                        size_t curr_len,
                                        size_t tailored_ce_cnt) {

  if (!(cs->coll_param && cs->coll_param->case_first == CASE_FIRST_UPPER &&
        cs->levels_for_compare == 3))
    return;
  ob_charset_assert(cs->uca->version == UCA_V900);
  // How many CEs this character has with non-ignorable primary weight.
  int tailored_pri_cnt = 0;
  int origin_pri_cnt = 0;
  for (size_t i = 0; i < tailored_ce_cnt; ++i) {
        if (to[(i * OB_UCA_900_CE_SIZE + 2) * to_stride] > CASE_FIRST_UPPER_MASK) {
      spread_case_mask(to, to_stride, tailored_ce_cnt, 0);
      return;
    }
    if (to[i * OB_UCA_900_CE_SIZE * to_stride]) {
      tailored_pri_cnt++;
    }
  }
  if (r->before_level == 1 || r->diff[0]) tailored_pri_cnt--;
  // Use the DUCET weight to detect the character's case.
  ObUCAInfo *src = &ob_uca_v900;
  int changed_ce = 0;
  ob_wc_t *curr = r->curr;
  for (size_t i = 0; i < curr_len; ++i) {
    const uint16_t *from = ob_char_weight_addr_900(src, *curr);
    unsigned int page = *curr >> 8;
    unsigned int code = *curr & 0xFF;
    curr++;
    int ce_cnt =
        src->weights[page] ? UCA900_NUM_OF_CE(src->weights[page], code) : 0;
    for (int i_ce = 0; i_ce < ce_cnt; ++i_ce) {
      if (from[i_ce * UCA900_DISTANCE_BETWEEN_WEIGHTS]) origin_pri_cnt++;
    }
  }
  int case_to_copy = 0;
  if (origin_pri_cnt <= tailored_pri_cnt)
    case_to_copy = origin_pri_cnt;
  else
    case_to_copy = tailored_pri_cnt - 1;
  int upper_cnt = 0;
  int lower_cnt = 0;
  curr = r->curr;
  uint16_t case_mask = 0;
  for (size_t curr_ind = 0; curr_ind < curr_len; ++curr_ind) {
    const uint16_t *from = ob_char_weight_addr_900(src, *curr);
    unsigned int page = *curr >> 8;
    unsigned int code = *curr & 0xFF;
    curr++;
    int ce_cnt =
        src->weights[page] ? UCA900_NUM_OF_CE(src->weights[page], code) : 0;
    changed_ce = 0;
    for (int i_ce = 0; i_ce < ce_cnt; ++i_ce) {
      uint16_t primary_weight = from[i_ce * UCA900_DISTANCE_BETWEEN_WEIGHTS];
      if (primary_weight) {
        uint16_t case_weight = from[i_ce * UCA900_DISTANCE_BETWEEN_WEIGHTS +
                                  2 * UCA900_DISTANCE_BETWEEN_LEVELS];
        uint16_t *ce_to = nullptr;
        if (is_tertiary_weight_upper_case(case_weight)) {
          if (!case_to_copy)
            upper_cnt++;
          else
            case_mask = CASE_FIRST_UPPER_MASK;
        } else {
          if (!case_to_copy)
            lower_cnt++;
          else
            case_mask = CASE_FIRST_LOWER_MASK;
        }
        if (case_to_copy) {
          do {
            ce_to = to + changed_ce * OB_UCA_900_CE_SIZE * to_stride;
            changed_ce++;
          } while (*ce_to == 0);
          ce_to[2 * to_stride] |= case_mask;
          case_to_copy--;
        }
      }
    }
  }
  if (origin_pri_cnt <= tailored_pri_cnt) {
    for (int i = origin_pri_cnt; i < tailored_pri_cnt; ++i) {
      const int offset = changed_ce * OB_UCA_900_CE_SIZE * to_stride;
      if (to[offset] && to[offset] < dst->extra_ce_pri_base)
        to[offset + 2 * to_stride] = 0;
    }
  } else {
    if (upper_cnt && lower_cnt)
      case_mask = CASE_FIRST_MIXED_MASK;
    else if (upper_cnt && !lower_cnt)
      case_mask = CASE_FIRST_UPPER_MASK;
    else
      case_mask = CASE_FIRST_LOWER_MASK;
    bool skipped_extra_ce = false;
    for (int i = tailored_ce_cnt - 1; i >= 0; --i) {
      int offset = i * OB_UCA_900_CE_SIZE * to_stride;
      if (to[offset] && to[offset] < dst->extra_ce_pri_base) {
        if ((r->before_level == 1 || r->diff[0]) && !skipped_extra_ce) {
          skipped_extra_ce = true;
          continue;
        }
        to[(i * OB_UCA_900_CE_SIZE + 2) * to_stride] |= case_mask;
        break;
      }
    }
  }
  spread_case_mask(to, to_stride, tailored_ce_cnt, case_mask);
}
static size_t ob_char_weight_put_900(ObUCAInfo *dst, uint16_t *to,
                                     size_t to_stride, size_t to_length,
                                     uint16_t *to_num_ce,
                                     const ObCollRule *rule,
                                     size_t base_len) {
  size_t count;
  int total_ce_cnt = 0;
  const ob_wc_t *base = rule->base;
  for (count = 0; base_len;) {
    const uint16_t *from = nullptr;
    size_t from_stride = 0;
    int ce_cnt = 0;
    for (size_t chlen = base_len; chlen > 1; chlen--) {
      if ((from = ob_uca_contraction_weight(dst->contraction_nodes, base,
                                            chlen))) {
        from_stride = 1;
        base += chlen;
        base_len -= chlen;
        ce_cnt = *(from + OB_UCA_MAX_WEIGHT_SIZE - 1);
        break;
      }
    }
    if (!from) {
      unsigned int page = *base >> 8;
      unsigned int code = *base & 0xFF;
      base++;
      base_len--;
      if (dst->weights[page]) {
        from = UCA900_WEIGHT_ADDR(dst->weights[page], 0, code);
        from_stride = UCA900_DISTANCE_BETWEEN_LEVELS;
        ce_cnt = UCA900_NUM_OF_CE(dst->weights[page], code);
      }
    }
    for (int weight_ind = 0;
         weight_ind < ce_cnt * OB_UCA_900_CE_SIZE && count < to_length;
         weight_ind++) {
      *to = *from;
      to += to_stride;
      from += from_stride;
      count++;
    }
    total_ce_cnt += ce_cnt;
  }
    if ((rule->diff[0] || rule->diff[1] || rule->diff[2]) && count < to_length) {
    *to = rule->diff[0] ? dst->extra_ce_pri_base : 0;
    to += to_stride;
    *to = rule->diff[1] ? dst->extra_ce_sec_base : 0;
    to += to_stride;
    *to = rule->diff[2] ? dst->extra_ce_ter_base : 0;
    to += to_stride;
    total_ce_cnt++;
    count += 3;
  }
  total_ce_cnt =
      std::min(total_ce_cnt, (OB_UCA_MAX_WEIGHT_SIZE - 1) / OB_UCA_900_CE_SIZE);
  *to_num_ce = total_ce_cnt;
  return total_ce_cnt;
}
static size_t ob_char_weight_put(ObUCAInfo *dst, uint16_t *to, size_t to_stride,
                                 size_t to_length, uint16_t *to_num_ce,
                                 const ObCollRule *rule, size_t base_len,
                                 enum_uca_ver uca_ver) {
  if (uca_ver == UCA_V900)
    return ob_char_weight_put_900(dst, to, to_stride, to_length, to_num_ce,
                                  rule, base_len);
  const ob_wc_t *base = rule->base;
  size_t count = 0;
  while (base_len != 0) {
    const uint16_t *from = nullptr;
    for (size_t chlen = base_len; chlen > 1; chlen--) {
      if ((from = ob_uca_contraction_weight(dst->contraction_nodes, base,
                                            chlen))) {
        base += chlen;
        base_len -= chlen;
        break;
      }
    }
    if (!from) {
      from = ob_char_weight_addr(dst, *base);
      base++;
      base_len--;
    }
    for (; from && *from && count < to_length;) {
      *to = *from++;
      to += to_stride;
      count++;
    }
  }
  *to = 0;
  return count;
}
static bool ob_uca_copy_page(ObCharsetInfo *cs, ObCharsetLoader *loader,
                             const ObUCAInfo *src, ObUCAInfo *dst,
                             size_t page) {
  const unsigned int dst_size = 256 * dst->lengths[page] * sizeof(uint16_t);
  if (!(dst->weights[page] = (uint16_t *)(loader->once_alloc)(dst_size)))
    return true;
  ob_charset_assert(src->lengths[page] <= dst->lengths[page]);
  memset(dst->weights[page], 0, dst_size);
  if (cs->uca && cs->uca->version == UCA_V900) {
    const unsigned int src_size = 256 * src->lengths[page] * sizeof(uint16_t);
    memcpy(dst->weights[page], src->weights[page], src_size);
  } else if (src->lengths[page] > 0) {
    for (unsigned int chc = 0; chc < 256; chc++) {
      memcpy(dst->weights[page] + chc * dst->lengths[page],
             src->weights[page] + chc * src->lengths[page],
             src->lengths[page] * sizeof(uint16_t));
    }
  }
  return false;
}
static bool apply_primary_shift_900(ObCharsetLoader *loader,
                                    ObCollRules *rules, ObCollRule *r,
                                    uint16_t *to, size_t to_stride,
                                    size_t nweights,
                                    uint16_t *const last_weight_ptr) {
    int last_sec_pri = 0;
  for (last_sec_pri = nweights - 2; last_sec_pri >= 0; --last_sec_pri) {
    if (to[last_sec_pri * to_stride * OB_UCA_900_CE_SIZE]) break;
  }
  if (last_sec_pri >= 0) {
    to[last_sec_pri * to_stride * OB_UCA_900_CE_SIZE]--;
    if (rules->shift_after_method == ob_shift_method_expand) {

      last_weight_ptr[0] += 0x1000;
    }
  } else {
    loader->errcode = OB_ERR_FAILED_TO_RESET_BEFORE_PRIMARY_IGNORABLE_CHAR;
    snprintf(loader->errarg, sizeof(loader->errarg), "U+%04lX", r->base[0]);
    return true;
  }
  return false;
}
static bool apply_secondary_shift_900(ObCharsetLoader *loader,
                                      ObCollRules *rules, ObCollRule *r,
                                      uint16_t *to, size_t to_stride,
                                      size_t nweights,
                                      uint16_t *const last_weight_ptr) {
  int last_sec_sec;
  for (last_sec_sec = nweights - 2; last_sec_sec >= 0; --last_sec_sec) {
    if (to[last_sec_sec * OB_UCA_900_CE_SIZE * to_stride + to_stride]) break;
  }
  if (last_sec_sec >= 0) {
    // Reset before.
    to[last_sec_sec * OB_UCA_900_CE_SIZE * to_stride + to_stride]--;
    if (rules->shift_after_method == ob_shift_method_expand) {
            last_weight_ptr[to_stride] += 0x100;
    }
  } else {
    loader->errcode = OB_ERR_FAILED_TO_RESET_BEFORE_SECONDARY_IGNORABLE_CHAR;
    snprintf(loader->errarg, sizeof(loader->errarg), "U+%04lX", r->base[0]);
    return true;
  }
  return false;
}
static bool apply_tertiary_shift_900(ObCharsetLoader *loader,
                                     ObCollRules *rules, ObCollRule *r,
                                     uint16_t *to, size_t to_stride,
                                     size_t nweights,
                                     uint16_t *const last_weight_ptr) {
  int last_sec_ter;
  for (last_sec_ter = nweights - 2; last_sec_ter >= 0; --last_sec_ter) {
    if (to[last_sec_ter * OB_UCA_900_CE_SIZE * to_stride + 2 * to_stride])
      break;
  }
  if (last_sec_ter >= 0) {
    // Reset before.
    to[last_sec_ter * OB_UCA_900_CE_SIZE * to_stride + 2 * to_stride]--;
    if (rules->shift_after_method == ob_shift_method_expand) {
            last_weight_ptr[to_stride * 2] += 0x10;
    }
  } else {
    loader->errcode = OB_ERR_FAILED_TO_RESET_BEFORE_TERTIARY_IGNORABLE_CHAR;
    snprintf(loader->errarg, sizeof(loader->errarg), "U+%04lX", r->base[0]);
    return true;
  }
  return false;
}
static bool apply_shift_900(ObCharsetLoader *loader, ObCollRules *rules,
                            ObCollRule *r, uint16_t *to, size_t to_stride,
                            size_t nweights) {
  // nweights should not less than 1 because of the extra CE.
  ob_charset_assert(nweights);
  // Apply level difference.
  uint16_t *const last_weight_ptr =
      to + (nweights - 1) * to_stride * OB_UCA_900_CE_SIZE;
  last_weight_ptr[0] += r->diff[0];
  last_weight_ptr[to_stride] += r->diff[1];
  last_weight_ptr[to_stride * 2] += r->diff[2];
  if (r->before_level == 1)  // Apply "&[before primary]".
    return apply_primary_shift_900(loader, rules, r, to, to_stride, nweights,
                                   last_weight_ptr);
  else if (r->before_level == 2)  // Apply "[before 2]".
    return apply_secondary_shift_900(loader, rules, r, to, to_stride, nweights,
                                     last_weight_ptr);
  else if (r->before_level == 3)  // Apply "[before 3]".
    return apply_tertiary_shift_900(loader, rules, r, to, to_stride, nweights,
                                    last_weight_ptr);
  return false;
}
static bool apply_shift(ObCharsetLoader *loader, ObCollRules *rules,
                        ObCollRule *r, int level, uint16_t *to,
                        size_t to_stride, size_t nweights) {
  if (rules->uca->version == UCA_V900)
    return apply_shift_900(loader, rules, r, to, to_stride, nweights);
  ob_charset_assert(to_stride == 1);
  if (nweights) {
    to[nweights - 1] += r->diff[0];
    if (r->before_level == 1)
    {
      if (nweights >= 2) {
        to[nweights - 2]--;
        if (rules->shift_after_method == ob_shift_method_expand) {

          to[nweights - 1] += 0x1000;
        }
      } else {
        loader->errcode = OB_ERR_FAILED_TO_RESET_BEFORE_PRIMARY_IGNORABLE_CHAR;
        snprintf(loader->errarg, sizeof(loader->errarg), "U+%04lX", r->base[0]);
        return true;
      }
    }
  } else {
    ob_charset_assert(to[0] == 0);
    to[0] = r->diff[level];
  }
  return false;
}
static ObContraction *add_contraction_to_trie(
  std::vector<ObContraction> *cont_nodes, ObCollRule *r) {
  ObContraction new_node{0, {}, {}, {}, false, 0};
  if (r->with_context)  // previous-context contraction
  {
    ob_charset_assert(ob_wstrnlen(r->curr, OB_UCA_MAX_CONTRACTION) == 2);
    std::vector<ObContraction>::iterator node_it =
        find_contraction_part_in_trie(*cont_nodes, r->curr[1]);
    if (node_it == cont_nodes->end() || node_it->ch != r->curr[1]) {
      new_node.ch = r->curr[1];
      node_it = cont_nodes->insert(node_it, new_node);
    }
    cont_nodes = &node_it->child_nodes_context;
    node_it = find_contraction_part_in_trie(*cont_nodes, r->curr[0]);
    if (node_it == cont_nodes->end() || node_it->ch != r->curr[0]) {
      new_node.ch = r->curr[0];
      node_it = cont_nodes->insert(node_it, new_node);
    }
    node_it->is_contraction_tail = true;
    node_it->contraction_len = 2;
    return &(*node_it);
  } else {
    size_t contraction_len = ob_wstrnlen(r->curr, OB_UCA_MAX_CONTRACTION);
    std::vector<ObContraction>::iterator node_it;
    for (size_t ch_ind = 0; ch_ind < contraction_len; ++ch_ind) {
      node_it = find_contraction_part_in_trie(*cont_nodes, r->curr[ch_ind]);
      if (node_it == cont_nodes->end() || node_it->ch != r->curr[ch_ind]) {
        new_node.ch = r->curr[ch_ind];
        node_it = cont_nodes->insert(node_it, new_node);
      }
      cont_nodes = &node_it->child_nodes;
    }
    node_it->is_contraction_tail = true;
    node_it->contraction_len = contraction_len;
    return &(*node_it);
  }
}
static bool apply_one_rule(ObCharsetInfo *cs, ObCharsetLoader *loader,
                           ObCollRules *rules, ObCollRule *r, int level,
                           ObUCAInfo *dst) {
  size_t nweights;
  size_t nreset = ob_coll_rule_reset_length(r);
  size_t nshift = ob_coll_rule_shift_length(r);
  uint16_t *to, *to_num_ce;
  size_t to_stride;
  if (nshift >= 2)
  {
    size_t i;
    int flag;
    ob_uca_add_contraction_flag(
        dst->contraction_flags, r->curr[0],
        r->with_context ? OB_UCA_PREVIOUS_CONTEXT_HEAD : OB_UCA_CNT_HEAD);
    for (i = 1, flag = OB_UCA_CNT_MID1; i < nshift - 1; i++, flag <<= 1)
      ob_uca_add_contraction_flag(dst->contraction_flags, r->curr[i], flag);
    ob_uca_add_contraction_flag(
        dst->contraction_flags, r->curr[i],
        r->with_context ? OB_UCA_PREVIOUS_CONTEXT_TAIL : OB_UCA_CNT_TAIL);
    ObContraction *trie_node =
        add_contraction_to_trie(dst->contraction_nodes, r);
    to = trie_node->weight;
    to_stride = 1;
    to_num_ce = &to[OB_UCA_MAX_WEIGHT_SIZE - 1];
    nweights =
        ob_char_weight_put(dst, to, to_stride, OB_UCA_MAX_WEIGHT_SIZE - 1,
                           to_num_ce, r, nreset, rules->uca->version);
  } else {
    ob_wc_t pagec = (r->curr[0] >> 8);
    ob_charset_assert(dst->weights[pagec]);
    if (cs->uca && cs->uca->version == UCA_V900) {
      to = ob_char_weight_addr_900(dst, r->curr[0]);
      to_stride = UCA900_DISTANCE_BETWEEN_LEVELS;
      to_num_ce = to - UCA900_DISTANCE_BETWEEN_LEVELS;
    } else {
      to = ob_char_weight_addr(dst, r->curr[0]);
      to_stride = 1;
      to_num_ce = to + (dst->lengths[pagec] - 1);
    }
    if (dst->lengths[pagec] == 0) {
      nweights = 0;
    } else {
      nweights = ob_char_weight_put(dst, to, to_stride, dst->lengths[pagec] - 1,
                                    to_num_ce, r, nreset, rules->uca->version);
    }
  }
  change_weight_if_case_first(cs, dst, r, to, to_stride, nshift, nweights);
  return apply_shift(loader, rules, r, level, to, to_stride, nweights);
}
static int check_rules(ObCharsetLoader *loader, const ObCollRules *rules,
                       const ObUCAInfo *dst, const ObUCAInfo *src) {
  const ObCollRule *r, *rlast;
  for (r = rules->rule, rlast = rules->rule + rules->nrules; r < rlast; r++) {
    if (r->curr[0] > dst->maxchar) {
      loader->errcode = OB_ERR_SHIFT_CHAR_OUT_OF_RANGE;
      snprintf(loader->errarg, sizeof(loader->errarg), "u%04X",
               (unsigned int)r->curr[0]);
      return true;
    } else if (r->base[0] > src->maxchar) {
      loader->errcode = OB_ERR_RESET_CHAR_OUT_OF_RANGE;
      snprintf(loader->errarg, sizeof(loader->errarg), "u%04X",
               (unsigned int)r->base[0]);
      return true;
    }
  }
  return false;
}
static void synthesize_lengths_900(unsigned char *lengths, const uint16_t *const *weights,
                                   unsigned int npages) {
  for (unsigned int page = 0; page < npages; ++page) {
    int max_len = 0;
    if (weights[page]) {
      for (unsigned int code = 0; code < 256; ++code) {
        max_len = std::max<int>(max_len, weights[page][code]);
      }
    }
    if (max_len == 0) {
      lengths[page] = 0;
    } else {
      lengths[page] = max_len * OB_UCA_900_CE_SIZE + 1;
    }
  }
}
static void copy_ja_han_pages(const ObCharsetInfo *cs, ObUCAInfo *dst) {
  if (!cs->uca || cs->uca->version != UCA_V900 ||
      cs->coll_param != &ja_coll_param) {
    return;
  }
  for (int page = MIN_JA_HAN_PAGE; page <= MAX_JA_HAN_PAGE; page++) {
    // In DUCET, weight is not assigned to code points in [U+4E00, U+9FFF].
    ob_charset_assert(dst->weights[page] == nullptr);
    dst->weights[page] = ja_han_pages[page - MIN_JA_HAN_PAGE];
  }
}
static void copy_zh_han_pages(ObUCAInfo *dst) {
  for (int page = MIN_ZH_HAN_PAGE; page <= MAX_ZH_HAN_PAGE; page++) {
    if (zh_han_pages[page - MIN_ZH_HAN_PAGE]) {
      dst->weights[page] = zh_han_pages[page - MIN_ZH_HAN_PAGE];
    }
  }
}
static void copy_zh2_han_pages(ObUCAInfo *dst) {
  for (int page = MIN_ZH2_HAN_PAGE; page <= MAX_ZH2_HAN_PAGE; page++) {
    if (zh2_han_pages[page - MIN_ZH2_HAN_PAGE]) {
      dst->weights[page] = zh2_han_pages[page - MIN_ZH2_HAN_PAGE];
    }
  }
}
static void copy_zh3_han_pages(ObUCAInfo *dst) {
  for (int page = MIN_ZH3_HAN_PAGE; page <= MAX_ZH3_HAN_PAGE; page++) {
    if (zh3_han_pages[page - MIN_ZH3_HAN_PAGE]) {
      dst->weights[page] = zh3_han_pages[page - MIN_ZH3_HAN_PAGE];
    }
  }
}
static inline ob_wc_t convert_implicit_to_ch(uint16_t first, uint16_t second) {
  if (first < 0xFB80) {
    return (((first - 0xFB40) << 15) | (second & 0x7FFF));
  } else if (first < 0xFBC0) {
    return (((first - 0xFB80) << 15) | (second & 0x7FFF));
  } else {
    return (((first - 0xFBC0) << 15) | (second & 0x7FFF));
  }
}
static void modify_all_zh_pages(Reorder_param *reorder_param, ObUCAInfo *dst,
                                int npages) {
  std::map<int, int> zh_han_to_single_weight_map;
  for (int i = 0; i < ZH_HAN_WEIGHT_PAIRS; i++) {
    zh_han_to_single_weight_map[zh_han_to_single_weight[i * 2]] =
        zh_han_to_single_weight[i * 2 + 1];
  }
  for (int page = 0; page < npages; page++) {
    if (!dst->weights[page] ||
        (page >= MIN_ZH_HAN_PAGE && page <= MAX_ZH_HAN_PAGE &&
         zh_han_pages[page - MIN_ZH_HAN_PAGE]))
      continue;
    for (int off = 0; off < 256; off++) {
      uint16_t *wbeg = UCA900_WEIGHT_ADDR(dst->weights[page], 0, off);
      int num_of_ce = UCA900_NUM_OF_CE(dst->weights[page], off);
      for (int ce = 0; ce < num_of_ce; ce++) {
        ob_charset_assert(reorder_param->wt_rec_num == 1);
        if (*wbeg >= reorder_param->wt_rec[0].old_wt_bdy.begin &&
            *wbeg <= reorder_param->wt_rec[0].old_wt_bdy.end) {
          *wbeg = *wbeg + reorder_param->wt_rec[0].new_wt_bdy.begin -
                  reorder_param->wt_rec[0].old_wt_bdy.begin;
        } else if (*wbeg >= 0xFB00) {
          uint16_t next_wt = *(wbeg + UCA900_DISTANCE_BETWEEN_WEIGHTS);
          if (*wbeg >= 0xFB40 && *wbeg <= 0xFBC1) {  // Han's implicit weight
                        ob_wc_t ch = convert_implicit_to_ch(*wbeg, next_wt);
            if (zh_han_to_single_weight_map.find(ch) !=
                zh_han_to_single_weight_map.end()) {
              *wbeg = zh_han_to_single_weight_map[ch];
              *(wbeg + UCA900_DISTANCE_BETWEEN_WEIGHTS) = 0;
              wbeg += UCA900_DISTANCE_BETWEEN_WEIGHTS;
              ce++;
              continue;
            }
          }
          *wbeg = change_zh_implicit(*wbeg);
          wbeg += UCA900_DISTANCE_BETWEEN_WEIGHTS;
          ce++;
        }
        wbeg += UCA900_DISTANCE_BETWEEN_WEIGHTS;
      }
    }
  }
}
static void modify_all_zh2_pages(Reorder_param *reorder_param, ObUCAInfo *dst,
                                 int npages) {
  std::map<int, int> zh2_han_to_single_weight_map;
  for (int i = 0; i < ZH2_HAN_WEIGHT_PAIRS; i++) {
    zh2_han_to_single_weight_map[zh2_han_to_single_weight[i * 2]] =
        zh2_han_to_single_weight[i * 2 + 1];
  }
  for (int page = 0; page < npages; page++) {
        if (!dst->weights[page] ||
        (page >= MIN_ZH2_HAN_PAGE && page <= MAX_ZH2_HAN_PAGE &&
         zh2_han_pages[page - MIN_ZH2_HAN_PAGE]))
      continue;
    for (int off = 0; off < 256; off++) {
      uint16_t *wbeg = UCA900_WEIGHT_ADDR(dst->weights[page], 0, off);
      int num_of_ce = UCA900_NUM_OF_CE(dst->weights[page], off);
      for (int ce = 0; ce < num_of_ce; ce++) {
        ob_charset_assert(reorder_param->wt_rec_num == 1);
        if (*wbeg >= reorder_param->wt_rec[0].old_wt_bdy.begin &&
            *wbeg <= reorder_param->wt_rec[0].old_wt_bdy.end) {
          *wbeg = *wbeg + reorder_param->wt_rec[0].new_wt_bdy.begin -
                  reorder_param->wt_rec[0].old_wt_bdy.begin;
        } else if (*wbeg >= 0xFB00) {
          uint16_t next_wt = *(wbeg + UCA900_DISTANCE_BETWEEN_WEIGHTS);
          if (*wbeg >= 0xFB40 && *wbeg <= 0xFBC1) {  // Han's implicit weight
                        ob_wc_t ch = convert_implicit_to_ch(*wbeg, next_wt);
            if (zh2_han_to_single_weight_map.find(ch) !=
                zh2_han_to_single_weight_map.end()) {
              *wbeg = zh2_han_to_single_weight_map[ch] >> 16;
              *(wbeg + UCA900_DISTANCE_BETWEEN_WEIGHTS) = zh2_han_to_single_weight_map[ch] & 0xFFFF;
              wbeg += UCA900_DISTANCE_BETWEEN_WEIGHTS;
              ce++;
              continue;
            }
          }
          *wbeg = change_zh2_implicit(*wbeg);
          wbeg += UCA900_DISTANCE_BETWEEN_WEIGHTS;
          ce++;
        }
        wbeg += UCA900_DISTANCE_BETWEEN_WEIGHTS;
      }
    }
  }
}
static void modify_all_zh3_pages(Reorder_param *reorder_param, ObUCAInfo *dst,
                                 int npages) {
  std::map<int, int> zh3_han_to_single_weight_map;
  for (int i = 0; i < ZH3_HAN_WEIGHT_PAIRS; i++) {
    zh3_han_to_single_weight_map[zh3_han_to_single_weight[i * 2]] =
        zh3_han_to_single_weight[i * 2 + 1];
  }
  for (int page = 0; page < npages; page++) {
        if (!dst->weights[page] ||
        (page >= MIN_ZH3_HAN_PAGE && page <= MAX_ZH3_HAN_PAGE &&
         zh3_han_pages[page - MIN_ZH3_HAN_PAGE]))
      continue;
    for (int off = 0; off < 256; off++) {
      uint16_t *wbeg = UCA900_WEIGHT_ADDR(dst->weights[page], 0, off);
      int num_of_ce = UCA900_NUM_OF_CE(dst->weights[page], off);
      for (int ce = 0; ce < num_of_ce; ce++) {
        ob_charset_assert(reorder_param->wt_rec_num == 1);
        if (*wbeg >= reorder_param->wt_rec[0].old_wt_bdy.begin &&
            *wbeg <= reorder_param->wt_rec[0].old_wt_bdy.end) {
          *wbeg = *wbeg + reorder_param->wt_rec[0].new_wt_bdy.begin -
                  reorder_param->wt_rec[0].old_wt_bdy.begin;
        } else if (*wbeg >= 0xFB00) {
          uint16_t next_wt = *(wbeg + UCA900_DISTANCE_BETWEEN_WEIGHTS);
          if (*wbeg >= 0xFB40 && *wbeg <= 0xFBC1) {  // Han's implicit weight
                        ob_wc_t ch = convert_implicit_to_ch(*wbeg, next_wt);
            if (zh3_han_to_single_weight_map.find(ch) !=
                zh3_han_to_single_weight_map.end()) {
              *wbeg = zh3_han_to_single_weight_map[ch] >> 16;
              *(wbeg + UCA900_DISTANCE_BETWEEN_WEIGHTS) = zh3_han_to_single_weight_map[ch] & 0xFFFF;
              wbeg += UCA900_DISTANCE_BETWEEN_WEIGHTS;
              ce++;
              continue;
            }
          }
          *wbeg = change_zh3_implicit(*wbeg);
          wbeg += UCA900_DISTANCE_BETWEEN_WEIGHTS;
          ce++;
        }
        wbeg += UCA900_DISTANCE_BETWEEN_WEIGHTS;
      }
    }
  }
}
static bool init_weight_level(ObCharsetInfo *cs, ObCharsetLoader *loader,
                              ObCollRules *rules, int level, ObUCAInfo *dst,
                              const ObUCAInfo *src,
                              bool lengths_are_temporary) {
  ObCollRule *r, *rlast;
  size_t i, npages = (src->maxchar + 1) / 256;
  bool has_contractions = false;
  dst->maxchar = src->maxchar;
  if (check_rules(loader, rules, dst, src)) {
    return true;
  }
  if (lengths_are_temporary) {
    if (!(dst->lengths = (unsigned char *)(loader->mem_malloc)(npages))) return true;
    if (!(dst->weights =
              (uint16_t **)(loader->once_alloc)(npages * sizeof(uint16_t *)))) {
      (loader->mem_free)(dst->lengths);
      return true;
    }
  } else {
    if (!(dst->lengths = (unsigned char *)(loader->once_alloc)(npages)) ||
        !(dst->weights =
              (uint16_t **)(loader->once_alloc)(npages * sizeof(uint16_t *))))
      return true;
  }
  memcpy(dst->lengths, src->lengths, npages);
  memcpy(dst->weights, src->weights, npages * sizeof(uint16_t *));
    for (r = rules->rule, rlast = rules->rule + rules->nrules; r < rlast; r++) {
    if (!r->curr[1])
    {
      unsigned int pagec = (r->curr[0] >> 8);
      if (r->base[1])
      {
        dst->lengths[pagec] = OB_UCA_MAX_WEIGHT_SIZE;
      } else {
        unsigned int pageb = (r->base[0] >> 8);
        if ((r->diff[0] || r->diff[1] || r->diff[2]) &&
            dst->lengths[pagec] < (src->lengths[pageb] + 3)) {
          if ((src->lengths[pageb] + 3) > OB_UCA_MAX_WEIGHT_SIZE)
            dst->lengths[pagec] = OB_UCA_MAX_WEIGHT_SIZE;
          else
            dst->lengths[pagec] = src->lengths[pageb] + 3;
        } else if (dst->lengths[pagec] < src->lengths[pageb])
          dst->lengths[pagec] = src->lengths[pageb];
      }
      dst->weights[pagec] = nullptr;
    } else
      has_contractions = true;
  }
  if (has_contractions) {
    dst->have_contractions = true;
    dst->contraction_nodes = new std::vector<ObContraction>(0);
    if (!(dst->contraction_flags =
              (char *)(loader->once_alloc)(OB_UCA_CNT_FLAG_SIZE)))
      return true;
    memset(dst->contraction_flags, 0, OB_UCA_CNT_FLAG_SIZE);
  }
  if (cs->coll_param == &zh_coll_param
      || cs->coll_param == &zh2_coll_param
      || cs->coll_param == &zh3_coll_param) {
        bool rc;
    for (i = 0; i < npages; i++) {
      if (dst->lengths[i] && (rc = ob_uca_copy_page(cs, loader, src, dst, i)))
        return rc;
    }
    if (cs->coll_param == &zh_coll_param) {
      modify_all_zh_pages(cs->coll_param->reorder_param, dst, npages);
      copy_zh_han_pages(dst);
    } else if (cs->coll_param == &zh2_coll_param) {
      modify_all_zh2_pages(cs->coll_param->reorder_param, dst, npages);
      copy_zh2_han_pages(dst);
    } else {
      modify_all_zh3_pages(cs->coll_param->reorder_param, dst, npages);
      copy_zh3_han_pages(dst);
    }
  } else {
    for (i = 0; i < npages; i++) {
      bool rc;
            if (!dst->weights[i] && dst->lengths[i] &&
          (rc = ob_uca_copy_page(cs, loader, src, dst, i)))
        return rc;
    }
    copy_ja_han_pages(cs, dst);
  }
    for (r = rules->rule; r < rlast; r++) {
    if (apply_one_rule(cs, loader, rules, r, level, dst)) return true;
  }
  return false;
}
static bool ob_comp_in_rulelist(const ObCollRules *rules, ob_wc_t wc) {
  ObCollRule *r, *rlast;
  for (r = rules->rule, rlast = rules->rule + rules->nrules; r < rlast; r++) {
    if (r->curr[0] == wc && r->curr[1] == 0) return true;
  }
  return false;
}
static inline bool ob_compchar_is_normal_char(unsigned int dec_ind) {
  return uni_dec[dec_ind].decomp_tag == DECOMP_TAG_NONE;
}
static inline bool ob_compchar_is_normal_char(const Unidata_decomp *decomp) {
  return ob_compchar_is_normal_char(decomp - std::begin(uni_dec));
}
static Unidata_decomp *get_decomposition(ob_wc_t ch) {
  auto comp_func = [](Unidata_decomp x, Unidata_decomp y) {
    return x.charcode < y.charcode;
  };
  Unidata_decomp to_find = {ch, CHAR_CATEGORY_LU, DECOMP_TAG_NONE, {0}};
  Unidata_decomp *decomp = std::lower_bound(
      std::begin(uni_dec), std::end(uni_dec), to_find, comp_func);
  if (decomp == std::end(uni_dec) || decomp->charcode != ch) return nullptr;
  return decomp;
}
static Combining_mark *ob_find_combining_mark(ob_wc_t code) {
  auto comp_func = [](Combining_mark x, Combining_mark y) {
    return x.charcode < y.charcode;
  };
  Combining_mark to_find = {code, 0};
  return std::lower_bound(std::begin(combining_marks),
                          std::end(combining_marks), to_find, comp_func);
}
static bool ob_is_inheritance_of_origin(const ob_wc_t *origin_dec,
                                        const ob_wc_t *dec_codes,
                                        ob_wc_t *dec_diff) {
  int ind0, ind1, ind2;
  if (origin_dec[0] != dec_codes[0]) return false;
  for (ind0 = ind1 = ind2 = 1; ind0 < OB_UCA_MAX_CONTRACTION &&
                               ind1 < OB_UCA_MAX_CONTRACTION &&
                               origin_dec[ind0] && dec_codes[ind1];) {
    if (origin_dec[ind0] == dec_codes[ind1]) {
      ind0++;
      ind1++;
    } else {
      Combining_mark *mark0 = ob_find_combining_mark(origin_dec[ind0]);
      Combining_mark *mark1 = ob_find_combining_mark(dec_codes[ind1]);
      if (mark0->ccc == mark1->ccc) return false;
      dec_diff[ind2++] = dec_codes[ind1++];
    }
  }
  if (ind0 >= OB_UCA_MAX_CONTRACTION || !origin_dec[ind0]) {
    while (ind1 < OB_UCA_MAX_CONTRACTION) {
      dec_diff[ind2++] = dec_codes[ind1++];
    }
    return true;
  }
  return false;
}
static int ob_coll_add_inherit_rules(
    ObCollRules *rules, ObCollRule *r, const Unidata_decomp *decomp_rec,
    std::bitset<array_elements(uni_dec)> *comp_added) {
  for (unsigned int dec_ind = 0; dec_ind < array_elements(uni_dec); dec_ind++) {
        if (!ob_compchar_is_normal_char(dec_ind) || comp_added->test(dec_ind) ||
        (decomp_rec != nullptr &&
         uni_dec[dec_ind].decomp_tag != decomp_rec->decomp_tag))
      continue;
        ob_wc_t dec_diff[OB_UCA_MAX_CONTRACTION]{r->curr[0], 0};
    ob_wc_t orig_dec[OB_UCA_MAX_CONTRACTION]{0};
    if (decomp_rec == nullptr) {
            orig_dec[0] = r->curr[0];
    } else {
      memcpy(orig_dec, decomp_rec->dec_codes, sizeof(orig_dec));
    }
    if (ob_is_inheritance_of_origin(orig_dec, uni_dec[dec_ind].dec_codes,
                                    dec_diff) &&
        !ob_comp_in_rulelist(rules, uni_dec[dec_ind].charcode)) {
      ObCollRule newrule{{0}, {uni_dec[dec_ind].charcode, 0}, {0}, 0, false};
      memcpy(newrule.base, dec_diff, sizeof(newrule.base));
      if (ob_coll_rules_add(rules, &newrule)) return 1;
      comp_added->set(dec_ind);
    }
  }
  return 0;
}
static bool combining_mark_in_rulelist(const ob_wc_t *dec_codes,
                                       const ObCollRule *r_start,
                                       const ObCollRule *r_end) {
  for (int i = 1; i < OB_UCA_MAX_CONTRACTION; ++i) {
    if (!*(dec_codes + i)) return false;
    for (const ObCollRule *r = r_start; r < r_end; ++r) {
      if (r->curr[0] == *(dec_codes + i)) {
        return true;
      }
    }
  }
  return false;
}
static int add_normalization_rules(const ObCharsetInfo *cs,
                                   ObCollRules *rules) {
  if (!cs->coll_param || !cs->coll_param->norm_enabled) return 0;
  const int orig_rule_num = rules->nrules;
  for (Unidata_decomp *decomp = std::begin(uni_dec); decomp < std::end(uni_dec);
       ++decomp) {
    if (!ob_compchar_is_normal_char(decomp) ||
        ob_comp_in_rulelist(rules, decomp->charcode) ||
        !combining_mark_in_rulelist(decomp->dec_codes, rules->rule,
                                    rules->rule + orig_rule_num))
      continue;
    ObCollRule newrule{{0}, {decomp->charcode, 0}, {0}, 0, false};
    memcpy(newrule.base, decomp->dec_codes, sizeof(newrule.base));
    if (ob_coll_rules_add(rules, &newrule)) return 1;
  }
  return 0;
}
static int ob_coll_check_rule_and_inherit(const ObCharsetInfo *cs,
                                          ObCollRules *rules) {
  if (rules->uca->version != UCA_V900) return 0;
    std::bitset<array_elements(uni_dec)> comp_added;
  int orig_rule_num = rules->nrules;
  for (int i = 0; i < orig_rule_num; ++i) {
    ObCollRule r = *(rules->rule + i);
        if ((cs->coll_param != &zh_coll_param
         || cs->coll_param != &zh2_coll_param
         || cs->coll_param != &zh3_coll_param) && r.curr[1]) continue;
    Unidata_decomp *decomp_rec = get_decomposition(r.curr[0]);
    if (ob_coll_add_inherit_rules(rules, &r, decomp_rec, &comp_added)) return 1;
  }
  return 0;
}
static inline void ob_set_weight_rec(
    Reorder_wt_rec (&wt_rec)[2 * UCA_MAX_CHAR_GRP], int rec_ind,
    uint16_t old_begin, uint16_t old_end, uint16_t new_begin, uint16_t new_end) {
  wt_rec[rec_ind] = {{old_begin, old_end}, {new_begin, new_end}};
}
static void ob_calc_char_grp_param(const ObCharsetInfo *cs, int &rec_ind) {
  int weight_start = START_WEIGHT_TO_REORDER;
  int grp_ind = 0;
  Reorder_param *param = cs->coll_param->reorder_param;
  for (; grp_ind < UCA_MAX_CHAR_GRP; ++grp_ind) {
    if (param->reorder_grp[grp_ind] == CHARGRP_NONE) break;
    for (Char_grp_info *info = std::begin(char_grp_infos);
         info < std::end(char_grp_infos); ++info) {
      if (param->reorder_grp[grp_ind] != info->group) continue;
      ob_set_weight_rec(
          param->wt_rec, grp_ind, info->grp_wt_bdy.begin, info->grp_wt_bdy.end,
          weight_start,
          weight_start + info->grp_wt_bdy.end - info->grp_wt_bdy.begin);
      weight_start = param->wt_rec[grp_ind].new_wt_bdy.end + 1;
      break;
    }
  }
  rec_ind = grp_ind;
}
static void ob_calc_char_grp_gap_param(ObCharsetInfo *cs, int &rec_ind) {
  Reorder_param *param = cs->coll_param->reorder_param;
  uint16_t weight_start = param->wt_rec[rec_ind - 1].new_wt_bdy.end + 1;
  Char_grp_info *last_grp = nullptr;
  for (Char_grp_info *info = std::begin(char_grp_infos);
       info < std::end(char_grp_infos); ++info) {
    for (int ind = 0; ind < UCA_MAX_CHAR_GRP; ++ind) {
      if (param->reorder_grp[ind] == CHARGRP_NONE) break;
      if (param->reorder_grp[ind] != info->group) continue;
      if (param->max_weight < info->grp_wt_bdy.end)
        param->max_weight = info->grp_wt_bdy.end;
            if (!last_grp && info->grp_wt_bdy.begin > START_WEIGHT_TO_REORDER) {
        ob_set_weight_rec(param->wt_rec, rec_ind, START_WEIGHT_TO_REORDER,
                          info->grp_wt_bdy.begin - 1, weight_start,
                          weight_start + (info->grp_wt_bdy.begin - 1) -
                              START_WEIGHT_TO_REORDER);
        weight_start = param->wt_rec[rec_ind].new_wt_bdy.end + 1;
        rec_ind++;
      }

      if (last_grp && last_grp->grp_wt_bdy.end < (info->grp_wt_bdy.begin - 1)) {
        ob_set_weight_rec(param->wt_rec, rec_ind, last_grp->grp_wt_bdy.end + 1,
                          info->grp_wt_bdy.begin - 1, weight_start,
                          weight_start + (info->grp_wt_bdy.begin - 1) -
                              (last_grp->grp_wt_bdy.end + 1));
        weight_start = param->wt_rec[rec_ind].new_wt_bdy.end + 1;
        rec_ind++;
      }
      last_grp = info;
      break;
    }
  }
  param->wt_rec_num = rec_ind;
}
static int ob_prepare_reorder(ObCharsetInfo *cs) {
    if (!cs->coll_param->reorder_param
      || cs->coll_param == &zh_coll_param
      || cs->coll_param == &zh2_coll_param
      || cs->coll_param == &zh3_coll_param)
    return 0;
    int rec_ind = 0;
  ob_calc_char_grp_param(cs, rec_ind);
  ob_calc_char_grp_gap_param(cs, rec_ind);
  return rec_ind;
}
static void adjust_japanese_weight(ObCharsetInfo *cs, int rec_ind) {
  Reorder_param *param = cs->coll_param->reorder_param;
  param->wt_rec[rec_ind - 1].new_wt_bdy.begin = 0;
  param->wt_rec[rec_ind - 1].new_wt_bdy.end = 0;
  param->wt_rec[rec_ind].old_wt_bdy.begin = param->wt_rec[1].old_wt_bdy.end + 1;
  param->wt_rec[rec_ind].old_wt_bdy.end = 0x54A3;
  param->wt_rec[rec_ind].new_wt_bdy.begin = 0;
  param->wt_rec[rec_ind].new_wt_bdy.end = 0;
  param->wt_rec_num++;
  param->max_weight = 0x54A3;
}
static bool ob_prepare_coll_param(ObCharsetInfo *cs, ObCollRules *rules) {
  if (rules->uca->version != UCA_V900 || !cs->coll_param) return false;
  int rec_ind = ob_prepare_reorder(cs);
  if (add_normalization_rules(cs, rules)) return true;
  if (cs->coll_param == &ja_coll_param) adjust_japanese_weight(cs, rec_ind);

  return false;
}
static bool create_tailoring(ObCharsetInfo *cs, ObCharsetLoader *loader) {
  if (!cs->tailoring)
    return false;
  ObCollRules rules;
  ObUCAInfo new_uca, *src_uca = nullptr;
  int rc = 0;
  ObUCAInfo *src, *dst;
  size_t npages;
  bool lengths_are_temporary;
  loader->errcode = 0;
  *loader->errarg = '\0';
  memset(&rules, 0, sizeof(rules));
  rules.loader = loader;
  rules.uca = cs->uca ? cs->uca : &ob_uca_v400;
  memset(&new_uca, 0, sizeof(new_uca));
  if ((rc = ob_coll_rule_parse(&rules, cs->tailoring,
                              cs->tailoring + strlen(cs->tailoring), cs->name)))
    goto ex;
  if ((rc = ob_coll_check_rule_and_inherit(cs, &rules))) goto ex;
  if ((rc = ob_prepare_coll_param(cs, &rules))) goto ex;
  if (rules.uca->version == UCA_V520) {
    src_uca = &ob_uca_v520;
    cs->caseinfo = &ob_unicase_unicode520;
  } else if (rules.uca->version == UCA_V400) {
    src_uca = &ob_uca_v400;
    if (!cs->caseinfo) cs->caseinfo = &ob_unicase_default;
  } else {
    src_uca = cs->uca ? cs->uca : &ob_uca_v400;
    if (!cs->caseinfo) cs->caseinfo = &ob_unicase_default;
  }
  src = src_uca;
  dst = &new_uca;
  dst->extra_ce_pri_base = cs->uca->extra_ce_pri_base;
  dst->extra_ce_sec_base = cs->uca->extra_ce_sec_base;
  dst->extra_ce_ter_base = cs->uca->extra_ce_ter_base;
  if (cs->coll_param && cs->coll_param == &zh_coll_param) {
    dst->extra_ce_pri_base = ZH_EXTRA_CE_PRI;
  }
  if (cs->coll_param && cs->coll_param == &zh2_coll_param) {
    dst->extra_ce_pri_base = ZH2_EXTRA_CE_PRI;
  }
  if (cs->coll_param && cs->coll_param == &zh3_coll_param) {
    dst->extra_ce_pri_base = ZH3_EXTRA_CE_PRI;
  }
  npages = (src->maxchar + 1) / 256;
  if (rules.uca->version == UCA_V900) {
    if (!(src->lengths = (unsigned char *)(loader->mem_malloc)(npages))) goto ex;
    synthesize_lengths_900(src->lengths, src->weights, npages);
  }
  lengths_are_temporary = (rules.uca->version == UCA_V900);
  if ((rc = init_weight_level(cs, loader, &rules, 0, dst, src,
                              lengths_are_temporary)))
    goto ex;
  if (lengths_are_temporary) {
    (loader->mem_free)(src->lengths);
    (loader->mem_free)(dst->lengths);
    src->lengths = nullptr;
    dst->lengths = nullptr;
  }
  new_uca.version = src_uca->version;
  if (!(cs->uca = (ObUCAInfo *)(loader->once_alloc)(sizeof(ObUCAInfo)))) {
    rc = 1;
    goto ex;
  }
  memset(cs->uca, 0, sizeof(ObUCAInfo));
  cs->uca[0] = new_uca;
ex:
  (loader->mem_free)(rules.rule);
  if (rc != 0 && loader->errcode) {
    if (new_uca.contraction_nodes) delete (new_uca.contraction_nodes);
    loader->reporter(ERROR_LEVEL, loader->errcode, loader->errarg);
  }
  return rc;
}
extern "C" {
static bool ob_coll_init_uca(ObCharsetInfo *cs, ObCharsetLoader *loader) {
  cs->pad_char = ' ';
  cs->ctype = ob_charset_utf8mb4_unicode_ci.ctype;
  if (!cs->caseinfo) cs->caseinfo = &ob_unicase_default;
  if (!cs->uca) cs->uca = &ob_uca_v400;
  return create_tailoring(cs, loader);
}
static void ob_coll_uninit_uca(ObCharsetInfo *cs) {
  if (cs->uca && cs->uca->contraction_nodes) {
    delete (cs->uca->contraction_nodes);
    cs->uca->contraction_nodes = nullptr;
    cs->state &= ~OB_CS_READY;
  }
}
}  // extern "C"
bool ob_propagate_uca_900(const ObCharsetInfo *cs,
                          const unsigned char *str __attribute__((unused)),
                          size_t length __attribute__((unused))) {
  return !ob_uca_have_contractions(cs->uca);
}
template <class Mb_wc, int LEVELS_FOR_COMPARE>
static size_t ob_strnxfrm_uca_900_tmpl(const ObCharsetInfo *cs,
                                       const Mb_wc mb_wc, unsigned char *dst,
                                       size_t dstlen, const unsigned char *src,
                                       size_t srclen, unsigned int flags) {
  unsigned char *d0 = dst;
  unsigned char *dst_end = dst + dstlen;
  uca_scanner_900<Mb_wc, LEVELS_FOR_COMPARE> scanner(mb_wc, cs, src, srclen);
  ob_charset_assert((dstlen % 2) == 0);
  if ((dstlen % 2) == 1) {
    // Emergency workaround for optimized mode.
    --dst_end;
  }
  if (dst != dst_end) {
    scanner.for_each_weight(
        [&dst, dst_end](
            int s_res, bool is_level_separator __attribute__((unused))) -> bool {
          ob_charset_assert(is_level_separator == (s_res == 0));
          if (LEVELS_FOR_COMPARE == 1) ob_charset_assert(!is_level_separator);
          dst = store16be(dst, s_res);
          return (dst < dst_end);
        },
        [&dst, dst_end](int num_weights) {
          return (dst < dst_end - num_weights * 2);
        });
  }
  if (flags & OB_STRXFRM_PAD_TO_MAXLEN) {
    memset(dst, 0, dst_end - dst);
    dst = dst_end;
  }
  return dst - d0;
}
extern "C" {
static size_t ob_strnxfrm_uca_900(const ObCharsetInfo *cs, unsigned char *dst,
                                  size_t dstlen,
                                  unsigned int num_codepoints __attribute__((unused)),
                                  const unsigned char *src, size_t srclen, unsigned int flags,
                                  bool *is_valid_unicode) {
  *is_valid_unicode = true;
  if (cs->cset->mb_wc == ob_mb_wc_utf8mb4_thunk) {
    switch (cs->levels_for_compare) {
      case 1:
        return ob_strnxfrm_uca_900_tmpl<Mb_wc_utf8mb4, 1>(
            cs, Mb_wc_utf8mb4(), dst, dstlen, src, srclen, flags);
      case 2:
        return ob_strnxfrm_uca_900_tmpl<Mb_wc_utf8mb4, 2>(
            cs, Mb_wc_utf8mb4(), dst, dstlen, src, srclen, flags);
      default:
        ob_charset_assert(false);
      case 3:
        return ob_strnxfrm_uca_900_tmpl<Mb_wc_utf8mb4, 3>(
            cs, Mb_wc_utf8mb4(), dst, dstlen, src, srclen, flags);
      case 4:
        return ob_strnxfrm_uca_900_tmpl<Mb_wc_utf8mb4, 4>(
            cs, Mb_wc_utf8mb4(), dst, dstlen, src, srclen, flags);
    }
  } else {
    Mb_wc_through_function_pointer mb_wc(cs);
    switch (cs->levels_for_compare) {
      case 1:
        return ob_strnxfrm_uca_900_tmpl<decltype(mb_wc), 1>(
            cs, mb_wc, dst, dstlen, src, srclen, flags);
      case 2:
        return ob_strnxfrm_uca_900_tmpl<decltype(mb_wc), 2>(
            cs, mb_wc, dst, dstlen, src, srclen, flags);
      default:
        ob_charset_assert(false);
      case 3:
        return ob_strnxfrm_uca_900_tmpl<decltype(mb_wc), 3>(
            cs, mb_wc, dst, dstlen, src, srclen, flags);
      case 4:
        return ob_strnxfrm_uca_900_tmpl<decltype(mb_wc), 4>(
            cs, mb_wc, dst, dstlen, src, srclen, flags);
    }
  }
}
static size_t ob_strnxfrmlen_uca_900(const ObCharsetInfo *cs, size_t len) {
    // We really ought to have len % 4 == 0, but not all calling code conforms.
  const size_t num_codepoints = (len + 3) / 4;
  const size_t max_num_weights_per_level = num_codepoints * 8;
  size_t max_num_weights = max_num_weights_per_level * cs->levels_for_compare;
  if (cs->coll_param && cs->coll_param->reorder_param) {
    max_num_weights += max_num_weights_per_level;
  }
  return (max_num_weights + (cs->levels_for_compare - 1)) * sizeof(uint16_t);
}
}  // extern "C"
ObCollationHandler ob_collation_any_uca_handler = {
    ob_coll_init_uca,
    ob_coll_uninit_uca,   ob_strnncoll_any_uca,  ob_strnncollsp_any_uca,
    ob_strnxfrm_any_uca,  ob_strnxfrmlen_simple, NULL, ob_like_range_mb,
    ob_wildcmp_uca,       ob_strcasecmp_uca,     ob_instr_mb,
    ob_hash_sort_any_uca, ob_propagate_complex};
ObCollationHandler ob_collation_uca_900_handler = {
    ob_coll_init_uca,
    ob_coll_uninit_uca,   ob_strnncoll_uca_900,   ob_strnncollsp_uca_900,
    ob_strnxfrm_uca_900,  ob_strnxfrmlen_uca_900, NULL, ob_like_range_mb,
    ob_wildcmp_uca,       ob_strcasecmp_uca,      ob_instr_mb,
    ob_hash_sort_uca_900, ob_propagate_uca_900};
static ObCollationHandler ob_collation_utf16_uca_handler =
{
  // ob_coll_init_uca,
  NULL,
  NULL,
  ob_strnncoll_any_uca,
  ob_strnncollsp_any_uca,
  ob_strnxfrm_any_uca,
  ob_strnxfrmlen_simple,
  NULL,
  ob_like_range_generic,
  ob_wildcmp_uca,
  NULL,
  ob_instr_mb,
  ob_hash_sort_any_uca,
  ob_propagate_complex
};
#define OB_CS_UTF8MB4_UCA_FLAGS (OB_CS_COMPILED|OB_CS_STRNXFRM|OB_CS_UNICODE|OB_CS_UNICODE_SUPPLEMENT)
ObCharsetInfo ob_charset_utf8mb4_unicode_ci=
{
  224,0,0,
  OB_CS_UTF8MB4_UCA_FLAGS | OB_CS_CI,
  OB_UTF8MB4,
  OB_UTF8MB4_UNICODE_CI,
  "",
  "",
  NULL,
  ctype_utf8,
  NULL,
  NULL,
  NULL,
  &ob_uca_v400,
  //NULL,
  //NULL,
  &ob_unicase_default,
  NULL,
  NULL,
  8,
  1,
  1,
  1,
  4,
  1,
  9,
  0xFFFF,
  ' ',
  0,
  1,
  1,
  &ob_charset_utf8mb4_handler,
  &ob_collation_any_uca_handler,
  PAD_SPACE};
#define OB_CS_UTF16_UCA_FLAGS (OB_CS_COMPILED|OB_CS_STRNXFRM|OB_CS_UNICODE|OB_CS_NONASCII)
ObCharsetInfo ob_charset_utf16_unicode_ci=
{
    101,0,0,
    OB_CS_UTF16_UCA_FLAGS | OB_CS_CI,
    OB_UTF16,
    OB_UTF16_UNICODE_CI,
    "",
    "",
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    &ob_uca_v400,
    // NULL,
    // NULL,
    &ob_unicase_default,
    NULL,
    NULL,
    8,
    1,
    1,
    2,
    4,
    1,
    9,
    0xFFFF,
    ' ',
    0,
    1,
    1,
    &ob_charset_utf16_handler,
    &ob_collation_utf16_uca_handler,
    PAD_SPACE};
ObCharsetInfo ob_charset_utf8mb4_0900_ai_ci = {
    255, 0, 0,
    OB_CS_UTF8MB4_UCA_FLAGS | OB_CS_PRIMARY,
    OB_UTF8MB4,
    OB_UTF8MB4 "_0900_ai_ci",
    "UTF-8 Unicode",
    nullptr,
    nullptr,
    ctype_utf8,
    nullptr,
    nullptr,
    nullptr,
    &ob_uca_v900,
    &ob_unicase_unicode900,
    nullptr,
    nullptr,
    0,
    1,
    1,
    1,
    4,
    1,
    9,
    0x10FFFF,
    ' ',
    false,
    1,
    1,
    &ob_charset_utf8mb4_handler,
    &ob_collation_uca_900_handler,
    NO_PAD};
ObCharsetInfo ob_charset_utf8mb4_zh_0900_as_cs = {
    185,
    0,
    0,
    OB_CS_UTF8MB4_UCA_FLAGS | OB_CS_CSSORT,
    OB_UTF8MB4,
    OB_UTF8MB4 "_zh_0900_as_cs",
    "",
    zh_cldr_30,
    &zh_coll_param,
    ctype_utf8,
    nullptr,
    nullptr,
    nullptr,
    &ob_uca_v900,
    &ob_unicase_unicode900,
    nullptr,
    nullptr,
    0,
    1,
    1,
    1,
    4,
    1,
    32,
    0x10FFFF,
    ' ',
    false,
    3,
    1,
    &ob_charset_utf8mb4_handler,
    &ob_collation_uca_900_handler,
    NO_PAD};
ObCharsetInfo ob_charset_utf8mb4_zh2_0900_as_cs = {
    185,
    0,
    0,
    OB_CS_UTF8MB4_UCA_FLAGS | OB_CS_CSSORT,
    OB_UTF8MB4,
    OB_UTF8MB4 "_zh2_0900_as_cs",
    "",
    zh2_cldr_30,
    &zh2_coll_param,
    ctype_utf8,
    nullptr,
    nullptr,
    nullptr,
    &ob_uca_v900,
    &ob_unicase_unicode900,
    nullptr,
    nullptr,
    0,
    1,
    1,
    1,
    4,
    1,
    32,
    0x10FFFF,
    ' ',
    false,
    3,
    1,
    &ob_charset_utf8mb4_handler,
    &ob_collation_uca_900_handler,
    NO_PAD};
ObCharsetInfo ob_charset_utf8mb4_zh3_0900_as_cs = {
    185,
    0,
    0,
    OB_CS_UTF8MB4_UCA_FLAGS | OB_CS_CSSORT,
    OB_UTF8MB4,
    OB_UTF8MB4 "_zh3_0900_as_cs",
    "",
    zh2_cldr_30,
    &zh3_coll_param,
    ctype_utf8,
    nullptr,
    nullptr,
    nullptr,
    &ob_uca_v900,
    &ob_unicase_unicode900,
    nullptr,
    nullptr,
    0,
    1,
    1,
    1,
    4,
    1,
    32,
    0x10FFFF,
    ' ',
    false,
    3,
    1,
    &ob_charset_utf8mb4_handler,
    &ob_collation_uca_900_handler,
    NO_PAD};
static size_t ob_strnxfrm_utf8mb4_0900_bin(
    const ObCharsetInfo *cs __attribute__((unused)), unsigned char *dst, size_t dstlen,
    unsigned int nweights __attribute__((unused)), const unsigned char *src, size_t srclen,
    unsigned int flags, bool *is_valid_unicode) {
  *is_valid_unicode = true;
  size_t weight_len = std::min<size_t>(srclen, dstlen);
  memcpy(dst, src, weight_len);
  if (flags & OB_STRXFRM_PAD_TO_MAXLEN) {
    memset(dst + weight_len, 0, dstlen - weight_len);
    return dstlen;
  } else {
    return weight_len;
  }
}
static int ob_strnncollsp_utf8mb4_0900_bin(const ObCharsetInfo *cs,
                                           const unsigned char *s, size_t slen,
                                           const unsigned char *t, size_t tlen,
                                           bool diff_if_only_endspace_difference __attribute__((unused))) {
  return ob_strnncoll_mb_bin(cs, s, slen, t, tlen, false);
}
static ObCollationHandler ob_collation_utf8mb4_0900_bin_handler = {
    nullptr,
    nullptr,
    ob_strnncoll_mb_bin,
    ob_strnncollsp_utf8mb4_0900_bin,
    ob_strnxfrm_utf8mb4_0900_bin,
    ob_strnxfrmlen_simple,
    NULL,
    ob_like_range_mb,
    ob_wildcmp_mb_bin,
    NULL,//ob_strcasecmp_mb_bin,
    ob_instr_mb,
    ob_hash_sort_mb_bin,
    ob_propagate_simple};
ObCharsetInfo ob_charset_utf8mb4_0900_bin = {
  309,
  0,
  0,                                        // number
  OB_CS_UTF8MB4_UCA_FLAGS | OB_CS_BINSORT,  // state
  OB_UTF8MB4,                               // cs name
  OB_UTF8MB4 "_0900_bin",                   // name
  "",                                       // comment
  nullptr,                                  // tailoring
  nullptr,                                  // coll_param
  ctype_utf8,                               // ctype
  nullptr,                                  // to_lower
  nullptr,                                  // to_upper
  nullptr,                                  // sort_order
  nullptr,                                  // uca
  &ob_unicase_unicode900,                   // caseinfo
  nullptr,                                  // state_map
  nullptr,                                  // ident_map
  1,                                        // strxfrm_multiply
  1,                                        // caseup_multiply
  1,                                        // casedn_multiply
  1,                                        // mbminlen
  4,                                        // mbmaxlen
  1,                                        // mbmaxlenlen
  0,                                        // min_sort_char
  0x10FFFF,                                 // max_sort_char
  ' ',                                      // pad char
  false,  // escape_with_backslash_is_dangerous
  1,      // levels_for_compare
  1,      // levels_for_order
  &ob_charset_utf8mb4_handler,
  &ob_collation_utf8mb4_0900_bin_handler,
  NO_PAD};
