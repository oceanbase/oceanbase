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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_NUMBER_FORMAT_MODELS_H_
#define OCEANBASE_SQL_ENGINE_EXPR_NUMBER_FORMAT_MODELS_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/container/ob_iarray.h"

//see oracle doc Number Format Models: https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements004.htm

namespace oceanbase
{
namespace sql
{
class ObNFMKeyWord;

struct ObNFMElem
{
  enum ElementType : int64_t
  {
    INVALID_TYPE = -1,
    NFM_COMMA,
    NFM_PERIOD,
    NFM_DOLLAR,
    NFM_ZERO,
    NFM_NINE,
    NFM_B,
    NFM_C,
    NFM_D,
    NFM_EEEE,
    NFM_G,
    NFM_L,
    NFM_MI,
    NFM_PR,
    NFM_RN,
    NFM_S,
    NFM_TME,
    NFM_TM9,
    NFM_TM,
    NFM_U,
    NFM_V,
    NFM_X,
    NFM_FM,
    MAX_TYPE_NUMBER
  };

  enum ElementGroup : int64_t
  {
    INVALID_GROUP = -1,
    NUMBER_GROUP,           // include 0, 9
    GROUPING_GROUP,         // include ',', '.'
    ISO_GROUPING_GROUP,     // include D, G
    DOLLAR_GROUP,           // include $
    CURRENCY_GROUP,         // include C, L, U
    EEEE_GROUP,             // include EEEE
    ROMAN_GROUP,            // include RN
    MULTI_GROUP,            // include V
    HEX_GROUP,              // include X
    SIGN_GROUP,             // include MI, PR, S
    BLANK_GROUP,            // include B
    FILLMODE_GROUP,         // include FM
    TM_GROUP,               // include TME, TM9, TM
    MAX_GROUP_NUMBER
  };

  #define NFM_COMMA_FLAG     (1 << 1)
  #define NFM_PERIOD_FLAG    (1 << 2)
  #define NFM_DOLLAR_FLAG    (1 << 3)
  #define NFM_ZERO_FLAG      (1 << 4)
  #define NFM_NINE_FLAG      (1 << 5)
  #define NFM_BLANK_FLAG     (1 << 6)
  #define NFM_C_FLAG         (1 << 7)
  #define NFM_D_FLAG         (1 << 8)
  #define NFM_EEEE_FLAG      (1 << 9)
  #define NFM_G_FLAG         (1 << 10)
  #define NFM_L_FLAG         (1 << 11)
  #define NFM_MI_FLAG        (1 << 12)
  #define NFM_PR_FLAG        (1 << 13)
  #define NFM_RN_FLAG        (1 << 14)
  #define NFM_S_FLAG         (1 << 15)
  #define NFM_TME_FLAG       (1 << 16)
  #define NFM_TM_FLAG        (1 << 17)
  #define NFM_U_FLAG         (1 << 18)
  #define NFM_MULTI_FLAG     (1 << 19)
  #define NFM_HEX_FLAG       (1 << 20)
  #define NFM_FILLMODE_FLAG  (1 << 21)

  static bool has_type(const int32_t elem_type, const int32_t flag)
  {
    return flag & elem_type;
  }
  static bool has_number_group(const int32_t flag)
  {
    return (flag & NFM_ZERO_FLAG) || (flag & NFM_NINE_FLAG);
  }
  static bool has_grouping_group(const int32_t flag)
  {
    return (flag & NFM_COMMA_FLAG) || (flag & NFM_PERIOD_FLAG);
  }
  static bool has_iso_grouping_group(const int32_t flag)
  {
    return (flag & NFM_D_FLAG) || (flag & NFM_G_FLAG);
  }
  static bool has_currency_group(const int32_t flag)
  {
    return (flag & NFM_C_FLAG) || (flag & NFM_L_FLAG) || (flag & NFM_U_FLAG);
  }
  static bool has_sign_group(const int32_t flag)
  {
    return (flag & NFM_MI_FLAG) || (flag & NFM_PR_FLAG) || (flag & NFM_S_FLAG);
  }
  static bool has_tm_group(const int32_t flag)
  {
    return (flag & NFM_TME_FLAG) || (flag & NFM_TM_FLAG);
  }

  static const ObNFMKeyWord NFM_KEYWORDS[MAX_TYPE_NUMBER];
  static inline bool is_valid_type(const ElementType elem_type)
  {
    return elem_type > INVALID_TYPE && elem_type < MAX_TYPE_NUMBER;
  }
  static inline bool is_valid_group_type(const ElementGroup elem_group)
  {
    return elem_group > INVALID_GROUP && elem_group < MAX_GROUP_NUMBER;
  }

  enum ElemCaseMode
  {
    IGNORE_CASE,
    UPPER_CASE,
    LOWER_CASE
  };

  ObNFMElem() : offset_(-1), is_last_elem_(false), keyword_(nullptr),
    prefix_type_(INVALID_TYPE), suffix_type_(INVALID_TYPE), case_mode_(IGNORE_CASE) {}
  common::ObString get_elem_type_name() const;
  TO_STRING_KV("elem_type", get_elem_type_name(), K_(keyword), K_(offset), K_(case_mode));

  int32_t offset_;
  bool is_last_elem_;
  const ObNFMKeyWord *keyword_;
  ElementType prefix_type_; // type of prefix node
  ElementType suffix_type_; // type of suffix node
  ElemCaseMode case_mode_;
};

struct ObNFMKeyWord {
  ObNFMKeyWord(const char *str, const ObNFMElem::ElementType type,
              const ObNFMElem::ElementGroup group, const int32_t output_len)
              : ptr_(str), len_(static_cast<int32_t>(strlen(str))),
              output_len_(output_len), elem_type_(type), elem_group_(group) {}
  ObNFMKeyWord(const char *str, int32_t len, const ObNFMElem::ElementType type,
              const ObNFMElem::ElementGroup group, const int32_t output_len)
              : ptr_(str), len_(len), output_len_(output_len),
              elem_type_(type), elem_group_(group) {}
  inline common::ObString to_obstring() const
  {
    return common::ObString(len_, ptr_);
  }
  const char *ptr_;
  int32_t len_;
  int32_t output_len_;
  ObNFMElem::ElementType elem_type_;
  ObNFMElem::ElementGroup elem_group_;
  TO_STRING_KV("value", common::ObString(len_, ptr_), K_(len),
              K_(elem_type), K_(elem_group), K_(output_len));
};

struct OBNFMDesc
{
  enum ElemPos
  {
    INVALID_POS,
    FIRST_POS,
    MIDDLE_POS,
    LAST_POS
  };

  OBNFMDesc() : digital_start_(-1), digital_end_(-1),
                decimal_pos_(INT32_MAX), pre_num_count_(0),
                post_num_count_(0), multi_(0),
                output_len_(0), zero_start_(-1),
                zero_end_(-1), last_separator_(-1), elem_flag_(0), upper_case_flag_(0),
                sign_appear_pos_(INVALID_POS), currency_appear_pos_(INVALID_POS),
                elem_x_count_(0), nls_currency_(), iso_grouping_() {}

  int32_t digital_start_;                   // offset of the first digit
  int32_t digital_end_;                     // offset of the last digit
  int32_t decimal_pos_;                     // offset of the decimal point
  int32_t pre_num_count_;                   // the number of digits before decimal point
  int32_t post_num_count_;                  // the number of digits after decimal point
  int32_t multi_;                           // the number of digits after 'V'
  int32_t output_len_;                      // output result length
  int32_t zero_start_;                      // offset of the first zero
  int32_t zero_end_;                        // offset of the last zero
  int32_t last_separator_;                  // offset of the last thousands separator
  int32_t elem_flag_;                       // bitmap marker element appears
  int32_t upper_case_flag_;                 // bitmap marker element case mode
  ElemPos sign_appear_pos_;                 // position the sign class appears
  ElemPos currency_appear_pos_;             // position the currency class appears
  int32_t elem_x_count_;                    // number of 'X'
  common::ObString nls_currency_;
  common::ObString iso_grouping_;
};

class ObNFMDescPrepare
{
public:
  static int fmt_desc_prepare(const common::ObSEArray<ObNFMElem*, 64> &fmt_elem_list,
                              OBNFMDesc &fmt_desc, bool need_check = true);

private:
  static int check_conflict_group(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_comma_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_period_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_dollar_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_zero_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_nine_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_b_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_c_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_d_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_eeee_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_g_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_l_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_mi_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_pr_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_rn_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_s_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_tm_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_u_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_v_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_x_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
  static int check_elem_fm_is_valid(
             const ObNFMElem *elem_item,
             const OBNFMDesc &fmt_desc);
};

struct ObNFMObj
{
  ObNFMObj() : obj_type_(common::ObNullType), num_(), float_(0), double_(0) {}
  ObNFMObj(const common::ObObjType &obj_type)
    : obj_type_(obj_type), num_(), float_(0), double_(0) {}

  const common::number::ObNumber& get_number() const {return num_;}
  float get_float() const {return float_;}
  double get_double() const {return double_;}
  void set_number(const common::number::ObNumber &num) {num_ = num;}
  void set_float(const float f) {float_ = f;}
  void set_double(const double d) {double_ = d;}
  common::ObObjType get_obj_type() const {return obj_type_;}
  void set_obj_type(const common::ObObjType obj_type) {obj_type_ = obj_type;}

  common::ObObjType obj_type_;
  common::number::ObNumber num_;
  float float_;
  double double_;
};

class ObNFMBase
{
public:
  typedef common::ObSEArray<uint32_t, 64> BigNumber;
  typedef common::ObSEArray<uint64_t, 32> HexNumber;

  ObNFMBase() : fmt_str_(), fmt_desc_(), allocator_() {}
  ~ObNFMBase() { allocator_.reset(); }
  const common::ObSEArray<ObNFMElem*, 64> &get_elem_list() const
  { return fmt_elem_list_; }
  const common::ObString &get_fmt_str() const { return fmt_str_; }
  int search_keyword(const char *cur_ch, const int32_t remain_len,
                     const ObNFMKeyWord *&match_key, ObNFMElem::ElemCaseMode &case_mode) const;
  int parse_fmt(const char* fmt_str, const int32_t fmt_len, bool need_check = true);
  int fill_str(char *str, const int32_t str_len, const int32_t offset,
               const char fill_ch, const int32_t fill_len) const;
  int int_to_roman_str(const int64_t val, char *buf, const int64_t buf_len, int64_t &pos) const;
  int append_decimal_digit(BigNumber &num, const uint32_t decimal_digit) const;
  int decimal_to_hex(const common::ObString &num_str, char *buf,
                     const int64_t buf_len, int64_t &pos) const;
  int build_hex_number(const char *str, const int32_t str_len,
                       int32_t &offset, HexNumber &hex_num) const;
  int check_hex_str_valid(const char *str, const int32_t str_len, const int32_t offset) const;
  int num_str_to_sci(const common::ObString &num_str, const int32_t scale,
                     char *buf, const int64_t len, int64_t &pos, bool is_tm) const;
  int get_integer_part_len(const common::ObString &num_str, int32_t &integer_part_len) const;
  int get_decimal_part_len(const common::ObString &num_str, int32_t &decimal_part_len) const;
  int hex_to_num(const char c, int32_t &val) const;
  int get_nls_currency(const ObSQLSessionInfo &session);
  int get_iso_grouping(const ObSQLSessionInfo &session);
  int remove_leading_zero(char *buf, int64_t &offset);
  int process_fillmode(char *buf, const int64_t buf_len, int64_t &pos) const;
  int conv_num_to_nfm_obj(const common::ObObj &obj, common::ObExprCtx &expr_ctx, ObNFMObj &nfm_obj);
  int conv_num_to_nfm_obj(const common::ObObjMeta &obj_meta, const ObDatum &obj, ObNFMObj &nfm_obj,
                          common::ObIAllocator &alloc);
  // the conversion range of roman numerals is 1~3999, so if it is not a valid int64, negative
  // number returns INT64_MIN, positive number returns INT64_MAX, will not affect the final result.
  int cast_obj_to_int(const ObNFMObj &nfm_obj, int64_t &res_val);
  int cast_obj_to_num_str(const ObNFMObj &nfm_obj, const int64_t scale, common::ObString &num_str);
  bool is_digit(const char c) const;
  bool is_zero(const common::ObString &num_str) const;

  static const char *const LOWER_RM1[9];
  static const char *const LOWER_RM10[9];
  static const char *const LOWER_RM100[9];
  static const char *const UPPER_RM1[9];
  static const char *const UPPER_RM10[9];
  static const char *const UPPER_RM100[9];

protected:
  common::ObString fmt_str_;
  OBNFMDesc fmt_desc_;
  common::ObArenaAllocator allocator_;
  common::ObSEArray<ObNFMElem*, 64> fmt_elem_list_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObNFMBase);
};

class ObNFMToChar : public ObNFMBase
{
public:
  ObNFMToChar() {}
  int convert_num_to_fmt_str(const common::ObObj &obj,
                             const char *fmt_str, const int32_t fmt_len,
                             common::ObExprCtx &expr_ctx,
                             common::ObString &res_str);
  int convert_num_to_fmt_str(const common::ObObjMeta &obj_meta,
                             const common::ObDatum &obj,
                             common::ObIAllocator &alloc,
                             const char *fmt_str, const int32_t fmt_len,
                             ObEvalCtx &ctx,
                             common::ObString &res_str);
  int calc_result_length(const common::ObObj &obj, int32_t &length);
private:
  int process_fmt_conv(const ObSQLSessionInfo &session,
                       const char *fmt_str, const int32_t fmt_len,
                       const ObNFMObj &nfm_obj, char *buf,
                       const int64_t buf_len, int64_t &pos);
  int process_roman_format(const ObNFMObj &nfm_obj, char *buf,
                           const int64_t buf_len, int64_t &pos);
  int process_hex_format(const ObNFMObj &nfm_obj, char *buf,
                         const int64_t buf_len, int64_t &pos);
  int process_tm_format(const ObNFMObj &nfm_obj, char *buf,
                        const int64_t buf_len, int64_t &pos);
  int process_tme_format(const ObNFMObj &nfm_obj, char *buf,
                         const int64_t buf_len, int64_t &pos);
  int process_mul_format(const ObNFMObj &nfm_obj, common::ObString &num_str);
  int process_sci_format(const common::ObString &origin_str, const int32_t scale,
                         common::ObString &num_str);
  int process_output_fmt(const common::ObString &num_str, const int32_t integer_part_len,
                        char *buf, const int64_t buf_len, int64_t &pos);
private:
  DISALLOW_COPY_AND_ASSIGN(ObNFMToChar);
};

class ObNFMToNumber : public ObNFMBase
{
public:
  ObNFMToNumber() {}
  int convert_char_to_num(const common::ObString &in_str,
                          const common::ObString &in_fmt_str,
                          common::ObExprCtx &expr_ctx,
                          common::number::ObNumber &res_num);
  int convert_char_to_num(const common::ObString &in_str,
                          const common::ObString &in_fmt_str,
                          common::ObIAllocator &alloc,
                          ObEvalCtx &ctx,
                          common::number::ObNumber &res_num);

private:
  int process_fmt_conv(const ObSQLSessionInfo &session,
                       const common::ObString &in_str,
                       const common::ObString &in_fmt_str,
                       common::ObIAllocator &alloc,
                       common::number::ObNumber &res_num);
  int process_hex_format(const common::ObString &in_str,
                         common::number::ObNumber &res_num,
                         common::ObIAllocator &allocator);
  int process_output_fmt(const common::ObString &in_str,
                         const int32_t integer_part_len,
                         common::ObString &num_str);
private:
  DISALLOW_COPY_AND_ASSIGN(ObNFMToNumber);
};
}
}

#endif // OCEANBASE_SQL_ENGINE_EXPR_NUMBER_FORMAT_MODELS_H_
