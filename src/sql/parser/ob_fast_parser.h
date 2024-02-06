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

#ifndef OCEANBASE_SQL_PARSER_FAST_PARSER_
#define OCEANBASE_SQL_PARSER_FAST_PARSER_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/charset/ob_charset.h"
#include "sql/parser/ob_parser_utils.h"
#include "sql/parser/ob_char_type.h"
#include "sql/parser/parse_malloc.h"
#include "sql/udr/ob_udr_struct.h"
#include "sql/plan_cache/ob_plan_cache_struct.h"

namespace oceanbase
{
namespace sql
{
struct FPContext
{
public:
	bool enable_batched_multi_stmt_;
	bool is_udr_mode_;
	ObCharsets4Parser charsets4parser_;
	ObSQLMode sql_mode_;
	QuestionMarkDefNameCtx *def_name_ctx_;

	FPContext()
		: enable_batched_multi_stmt_(false),
			is_udr_mode_(false),
			sql_mode_(0),
			def_name_ctx_(nullptr)
	{}
	FPContext(ObCharsets4Parser charsets4parser)
		: enable_batched_multi_stmt_(false),
			is_udr_mode_(false),
			charsets4parser_(charsets4parser),
			sql_mode_(0),
			def_name_ctx_(nullptr)
	{}
};

struct ObFastParser final
{
public:
	static int parse(const common::ObString &stmt,
									 const FPContext &fp_ctx,
									 common::ObIAllocator &allocator,
									 char *&no_param_sql,
									 int64_t &no_param_sql_len,
									 ParamList *&param_list,
									 int64_t &param_num);
	static int parse(const common::ObString &stmt,
									 const FPContext &fp_ctx,
									 common::ObIAllocator &allocator,
			 						 char *&no_param_sql,
			 						 int64_t &no_param_sql_len,
			 						 ParamList *&param_list,
			 						 int64_t &param_num,
									 ObFastParserResult &fp_result,
									 int64_t &values_token_pos);
};

static const char INVALID_CHAR = -1;
struct ObRawSql {
	explicit ObRawSql() :
		raw_sql_(nullptr), raw_sql_len_(0),
		cur_pos_(0), search_end_(false) {}

	inline void init(const char *raw_sql, const int64_t len)
	{
		cur_pos_ = 0;
		raw_sql_ = raw_sql;
		raw_sql_len_ = len;
	}
	inline bool is_search_end()
	{
		return search_end_ || cur_pos_ > raw_sql_len_ - 1;
	}
	inline char peek()
	{
		if (cur_pos_ >= raw_sql_len_ - 1) {
			return INVALID_CHAR;
		}
		return raw_sql_[cur_pos_ + 1];
	}
	inline char scan(const int64_t offset)
	{
		if (cur_pos_ + offset >= raw_sql_len_) {
			search_end_ = true;
			cur_pos_ = raw_sql_len_;
			return INVALID_CHAR;
		}
		cur_pos_ += offset;
		return raw_sql_[cur_pos_];
	}
	inline char scan() { return scan(1); }
	inline char reverse_scan()
	{
		if (cur_pos_ <= 0 || cur_pos_ >= raw_sql_len_ + 1) {
			search_end_ = true;
			return INVALID_CHAR;
		}
		return raw_sql_[--cur_pos_];
	}
	inline char char_at(int64_t idx)
	{
		if (idx < 0 || idx >= raw_sql_len_) {
			return INVALID_CHAR;
		}
		return raw_sql_[idx];
	}
	inline const char *ptr(const int64_t pos)
	{
		if (pos < 0 || pos >= raw_sql_len_) {
			return nullptr;
		}
		return &(raw_sql_[pos]);
	}
	inline int64_t strncasecmp(int64_t pos, const char *str, const int64_t size)
	{
		// It is not necessary to check if str is nullptr
		char ch = char_at(pos);
		for (int64_t i = 0; i < size; i++) {
			if (ch >= 'A' && ch <= 'Z') {
				ch += 32;
			}
			if (ch != str[i]) {
				return -1;
			}
			ch = char_at(++pos);
		}
		return 0;
	}
	inline int64_t strncasecmp(const char *str, const int64_t size)
	{
		return strncasecmp(cur_pos_, str, size);
	}
	// Access a character (no bounds check)
	// char operator[] (const int64_t idx) const { return raw_sql_[idx]; }
	// For debug
	common::ObString to_string() const
	{
		if (OB_UNLIKELY(nullptr == raw_sql_ || 0 == raw_sql_len_)) {
			return common::ObString(0, nullptr);
		}
		return common::ObString(raw_sql_len_, raw_sql_);
	}

	const char *raw_sql_;
	int64_t raw_sql_len_;
	int64_t cur_pos_;
	bool search_end_;
};

class ObFastParserBase
{
public:
	// For performance reasons, virtual functions are not used
	// So the callback function below must be implemented in a derived class
	typedef int (ObFastParserBase::*ParseNextTokenFunc) ();
	typedef int (ObFastParserBase::*ProcessIdfFunc) (bool is_number_begin);

	explicit ObFastParserBase(common::ObIAllocator &allocator,
														const FPContext fp_ctx);
	~ObFastParserBase() {}
	int parse(const common::ObString &stmt,
						char *&no_param_sql,
						int64_t &no_param_sql_len,
						ParamList *&param_list,
						int64_t &param_num,
						int64_t &values_token_pos);

	int do_trim_for_insert(char *new_sql_buf,
                         int64_t buff_len,
                         const ObString &no_trim_sql,
                         ObString &after_trim_sql,
                         bool &trimed_succ);


  int parser_insert_str(common::ObIAllocator &allocator,
                        int64_t values_token_pos,
                        const ObString &old_no_param_sql,
                        ObString &new_truncated_sql,
                        bool &can_batch_opt,
                        int64_t &params_count,
                        int64_t &upd_params_count,
                        int64_t &lenth_delta,
                        int64_t &row_count);

	const ObQuestionMarkCtx &get_question_mark_ctx() const { return question_mark_ctx_; }

protected:
	enum TokenType
	{
		INVALID_TOKEN,
		NORMAL_TOKEN, // token that needs to be kept as it is
		PARAM_TOKEN,  // token that needs to be parameterized
		IGNORE_TOKEN  // token that need to be ignored such as comments
	};

  enum ROW_STATE
  {
    START_STATE = 0,
    LEFT_PAR_STATE,
    PARS_MATCH,
    ON_DUPLICATE_KEY,
    UNEXPECTED_STATE
  };

  enum TRIM_STATE
  {
    START_TRIM_STATE = 0,
    WITH_SPLII_CH,
    WITH_OTHER_CH,
    WITH_SPACE_CH,
    TRIM_FAILED
  };

	// In the process of judging the identifer, we need to continuously scan next char
	// in order to prevent the problem of memory access out of bounds, so we need to write
	// a lot of judgment logic like the following
	// if (cur_pos_ < raw_sql_len_ - 1) {
	//  scan(); // do something
	// }
	// There are too many such branches, making our code look very ugly. therefore, we use a special
	// value of -1. when the allowed length is exceeded, peek, scan, reverse_scan, char_at return -1
	// this will not affect any correctness issues, and will make the code look better
	static const int64_t PARSER_NODE_SIZE = sizeof(ParseNode);
	static const int64_t FIEXED_PARAM_NODE_SIZE = PARSER_NODE_SIZE + sizeof(ParamList);

protected:
	/**
	 * Check whether it is a specify character and whether it is a multi byte specify
	 * character corresponding to the character set
	 * @param [in] : str the string being retrieved
	 * @param [in] : len the length of the string being retrieved
	 * @param [in] : pos the location to start the retrieved
	 * @param [out] : byte_len byte size of a specify character
	 */
	#define DEF_MULTI_BYTE_CHARACTER_CHECK_FUNCS(CHARACTER_NAME) 										 \
	inline bool is_multi_byte_##CHARACTER_NAME(const char *str, const int64_t len,	 \
																						 const int64_t pos, int64_t &byte_len) \
	{																																								 \
		bool bool_ret = false;																												 \
		if (pos >= len) {																															 \
			return bool_ret;																														 \
		}																																							 \
		if (is_##CHARACTER_NAME(str[pos])) {																					 \
			bool_ret = true;																														 \
			byte_len = 1;																																 \
		} else if (is_oracle_mode_																										 \
			&& (CHARSET_UTF8MB4 == charset_type_ || CHARSET_UTF16 == charset_type_)) {	 \
			if (pos + 3 < len && -1 != is_utf8_multi_byte_##CHARACTER_NAME(str, pos)) {	 \
				bool_ret = true;																													 \
				byte_len = 3;																															 \
			}																																						 \
		} else if (is_oracle_mode_																										 \
			&& (ObCharset::is_gb_charset(charset_type_))) {		 													 \
			if (pos + 2 < len && -1 != is_gbk_multi_byte_##CHARACTER_NAME(str, pos)) {	 \
				bool_ret = true;																													 \
				byte_len = 2;																															 \
			}																																						 \
		}																																							 \
		return bool_ret;																															 \
	}       
	#define DEF_CHARACTER_CHECK_FUNCS(CHARACTER_NAME, SPECIFY_CHARACTER)			       \
	inline bool is_##CHARACTER_NAME(char ch)																	   		 \
	{																																						     \
		return SPECIFY_CHARACTER == ch;																								 \
	}
	#define DEF_RAW_SQL_MULTI_BYTE_CHARACTER_CHECK(CHARACTER_NAME, pos, byte_len) \
	is_multi_byte_##CHARACTER_NAME(raw_sql_.raw_sql_, raw_sql_.raw_sql_len_, pos, byte_len)
	#define IS_MULTI_SPACE(pos, byte_len) \
	DEF_RAW_SQL_MULTI_BYTE_CHARACTER_CHECK(space, pos, byte_len)
	#define IS_MULTI_COMMA(pos, byte_len) \
	DEF_RAW_SQL_MULTI_BYTE_CHARACTER_CHECK(comma, pos, byte_len)
	#define IS_MULTI_LEFT_PARENTHESIS(pos, byte_len) \
	DEF_RAW_SQL_MULTI_BYTE_CHARACTER_CHECK(left_parenthesis, pos, byte_len)
	#define IS_MULTI_RIGHT_PARENTHESIS(pos, byte_len) \
	DEF_RAW_SQL_MULTI_BYTE_CHARACTER_CHECK(right_parenthesis, pos, byte_len)
	#define CHECK_EQ_STRNCASECMP(str, size) (0 == raw_sql_.strncasecmp(str, size))

  #define DEF_RAW_SQL_MULTI_BYTE_CHARACTER_CHECK_V2(CHARACTER_NAME, raw_sql, pos, byte_len) \
  is_multi_byte_##CHARACTER_NAME(raw_sql.raw_sql_, raw_sql.raw_sql_len_, pos, byte_len)
  #define IS_MULTI_SPACE_V2(raw_sql, pos, byte_len) \
  DEF_RAW_SQL_MULTI_BYTE_CHARACTER_CHECK_V2(space, raw_sql, pos, byte_len)
  #define IS_MULTI_COMMA_V2(raw_sql, pos, byte_len) \
  DEF_RAW_SQL_MULTI_BYTE_CHARACTER_CHECK_V2(space, raw_sql, pos, byte_len)

	DEF_CHARACTER_CHECK_FUNCS(comma, ',');
	DEF_CHARACTER_CHECK_FUNCS(left_parenthesis, '(');
	DEF_CHARACTER_CHECK_FUNCS(right_parenthesis, ')');
	DEF_MULTI_BYTE_CHARACTER_CHECK_FUNCS(space);
	DEF_MULTI_BYTE_CHARACTER_CHECK_FUNCS(comma);
	DEF_MULTI_BYTE_CHARACTER_CHECK_FUNCS(left_parenthesis);
	DEF_MULTI_BYTE_CHARACTER_CHECK_FUNCS(right_parenthesis);
	void process_leading_space();
	void remove_multi_stmt_end_space();
	inline void set_callback_func(ParseNextTokenFunc func1, ProcessIdfFunc func2)
	{
		parse_next_token_func_ = func1;
		process_idf_func_ = func2;
	}
	inline bool is_valid_token()
	{
		return cur_token_type_ != INVALID_TOKEN;
	}
	inline bool is_valid_char(char ch)
	{
		return ch != INVALID_CHAR;
	}
	// [ \t\n\r\f\v]
	inline bool is_space(char ch)
	{
		return is_valid_char(ch) && SPACE_FLAGS[static_cast<uint8_t>(ch)];
	}
	char *parse_strdup_with_replace_multi_byte_char(
	const char *str, const size_t dup_len, char *out_str, int64_t &out_len);
	inline bool is_digit(char ch)
	{
		return is_valid_char(ch) && DIGIT_FLAGS[static_cast<uint8_t>(ch)];
	}
	// [^\n\r]
	inline bool is_non_newline(char ch)
	{
		return is_valid_char(ch) && '\n' != ch && '\r' != ch;
	}
	// [0-9a-fA-F]
	inline bool is_hex(char ch)
	{
		return is_valid_char(ch) && HEX_FLAGS[static_cast<uint8_t>(ch)];
	}
	// [0-1]
	inline bool is_binary(char ch)
	{
		return '0' == ch || '1' == ch;
	}
	// [A-Za-z0-9$-]
	inline bool is_identifier_char(char ch)
	{
		uint8_t ind = static_cast<uint8_t>(ch);
		return is_valid_char(ch) &&
		(is_oracle_mode_ ? ORACLE_IDENTIFIER_FALGS[ind] : MYSQL_IDENTIFIER_FALGS[ind]);
	}
	// [A-Za-z_]
	inline bool is_sys_var_first_char(char ch)
	{
		return is_valid_char(ch) && SYS_VAR_FIRST_CHAR[static_cast<uint8_t>(ch)];
	}
	// [A-Za-z0-9_]
	inline bool is_sys_var_char(char ch)
	{
		return is_valid_char(ch) && SYS_VAR_CHAR[static_cast<uint8_t>(ch)];
	}
	// [`'\"A-Za-z0-9_\.$/%]
	inline bool is_user_var_char(char ch)
	{
		bool is_oracle_user_var =
		USER_VAR_CHAR[static_cast<uint8_t>(ch)] || '/' == ch || '%' == ch || '\"' == ch || '\'' == ch;
		return is_valid_char(ch) &&
		(is_oracle_mode_ ? is_oracle_user_var : (is_oracle_user_var || '`' == ch));
	}
	// [A-Za-z0-9_\.$]
	inline bool is_user_var_char_without_quota(char ch)
	{
		return is_valid_char(ch) && USER_VAR_CHAR[static_cast<uint8_t>(ch)];
	}
	void reset_parser_node(ParseNode *node);
	//{U}
	int64_t is_latin1_char(const int64_t pos);
	// ({U_2}{U}|{U_3}{U}{U}|{U_4}{U}{U}{U}
	int64_t is_utf8_char(const int64_t pos);
	// NOTES: No boundary check, the caller guarantees safety!!!
	// ([\\\xe3\][\\\x80\][\\\x80])
	int64_t is_utf8_multi_byte_space(const char *str, const int64_t start_pos);
	// ([\\\xef\][\\\xbc\][\\\x8c])
	int64_t is_utf8_multi_byte_comma(const char *str, const int64_t start_pos);
	// ([\\\xef\][\\\xbc\][\\\x88])
	int64_t is_utf8_multi_byte_left_parenthesis(const char *str, const int64_t start_pos);
	// ([\\\xef\][\\\xbc\][\\\x89])
	int64_t is_utf8_multi_byte_right_parenthesis(const char *str, const int64_t start_pos);
	// {GB_1}{GB_2}
	int64_t is_gbk_char(const int64_t pos);
	// ([\\\xa1][\\\xa1])
	int64_t is_gbk_multi_byte_space(const char *str, const int64_t start_pos);
	// ([\\\xa3][\\\xac])
	int64_t is_gbk_multi_byte_comma(const char *str, const int64_t start_pos);
	// ([\\\xa3][\\\xa8])
	int64_t is_gbk_multi_byte_left_parenthesis(const char *str, const int64_t start_pos);
	// ([\\\xa3][\\\xa9])
	int64_t is_gbk_multi_byte_right_parenthesis(const char *str, const int64_t start_pos);
	// [\x80-\xbf]
	inline bool is_u(char ch)
	{
		return is_valid_char(ch) &&
		static_cast<uint8_t>(ch) >= 0x80 && static_cast<uint8_t>(ch) <= 0xbf;
	}
	// [\xc2-\xdf]
	inline bool is_u2(char ch)
	{
		return is_valid_char(ch) &&
		static_cast<uint8_t>(ch) >= 0xc2 && static_cast<uint8_t>(ch) <= 0xdf;
	}
	// [\xe0-\xef]
	inline bool is_u3(char ch)
	{
		return is_valid_char(ch) &&
		static_cast<uint8_t>(ch) >= 0xe0 && static_cast<uint8_t>(ch) <= 0xef;
	}
	// [\xf0-\xf4]
	inline bool is_u4(char ch)
	{
		return is_valid_char(ch) &&
		static_cast<uint8_t>(ch) >= 0xf0 && static_cast<uint8_t>(ch) <= 0xf4;
	}
	// [\x81-\xfe]
	inline bool is_gb1(char ch)
	{
		return is_valid_char(ch) &&
		static_cast<uint8_t>(ch) >= 0x81 && static_cast<uint8_t>(ch) <= 0xfe;
	}
	// [\x40-\xfe]
	inline bool is_gb2(char ch)
	{
		return is_valid_char(ch) &&
		static_cast<uint8_t>(ch) >= 0x40 && static_cast<uint8_t>(ch) <= 0xfe;
	}
    inline bool is_latin1(char ch)
	{
		return is_valid_char(ch) &&
		static_cast<uint8_t>(ch) >= 0x80 && static_cast<uint8_t>(ch) <= 0xFF;
	}
	// [0-9]{n}
	inline bool is_n_continuous_digits(const char *str,
									   const int64_t pos,
									   const int64_t len,
									   const int64_t n);

	inline bool is_normal_char(char ch)
	{
		return is_valid_char(ch) && (is_oracle_mode_ ?
					 ORACLE_NORMAL_CHAR_FLAGS[static_cast<uint8_t>(ch)] :
					 MYSQL_NORMAL_CHAR_FLAGS[static_cast<uint8_t>(ch)]);
	}
	// [A-Za-z]
	inline bool is_first_identifier_char(char ch)
	{
		return is_valid_char(ch) && ORACLE_FIRST_IDENTIFIER_FLAGS[static_cast<uint8_t>(ch)];
	}
	/**
	 * Used to parse [A-Za-z] or {UTF8_GB_CHAR}
	 * @param [in] : pos the position of the first character
	 * Return the next position of the position that meets the condition
	 * and return -1 if it is not satisfied
	 */
	int64_t is_first_identifier_flags(const int64_t pos);
	// Add parameterized nodes
	int add_bool_type_node(bool is_true);
	int add_null_type_node();
	int add_nowait_type_node();
	void lex_store_param(ParseNode *node, char *buf);
	void append_no_param_sql();
	void process_escape_string(char *str_buf, int64_t &str_buf_len);
	ParseNode *new_node(char *&buf, ObItemType type);
	char* parse_strndup(const char *str, size_t nbyte, char *buf);
	int64_t get_question_mark(ObQuestionMarkCtx *ctx,
														void *malloc_pool,
														const char *name,
														const int64_t name_len,
														char *buf);
	int64_t get_question_mark_by_defined_name(QuestionMarkDefNameCtx *ctx,
																						const char *name,
																						const int64_t name_len);
	/**
	 * The hexadecimal number in mysql mode has the following two representations:
	 * x'([0-9A-F])*' or 0x([0-9A-F])+
	 * @param [in] : when is_quote is true, it means the first one. when "\`" does not appear
	 * as a pair, only an 'x' is reserved
	 */
	int process_hex_number(bool is_quote);
	/**
	 * The binary in mysql mode has the following two representations:
	 * b'([01])*' or 0b([01])+
	 * @param [in] : when is_quote is true, it means the first one. when "\`" does not appear
	 * as a pair, only an 'b' is reserved
	 */
	int process_binary(bool is_quote);
	int process_hint();
	int process_question_mark();
	int process_ps_statement();
	int process_number(bool has_minus);
	int process_negative();
	int process_identifier_begin_with_l(bool &need_process_ws);
	int process_identifier_begin_with_t(bool &need_process_ws);
	int process_date_related_type(const char quote, ObItemType item_type);
	int process_time_relate_type(bool &need_process_ws, ObItemType type = T_INVALID);
	void process_token();
	void process_system_variable(bool is_contain_quote);
	void parse_integer(ParseNode *node);
	void process_user_variable(bool is_contain_quote);
	// Used to process '`' and keep all characters before the next '`'
	int process_backtick();
	// Used to process '\"' and keep all characters before the next '\"'
	int process_double_quote();
	// Until "*/" appears, all characters before it should be ignored
	int process_comment_content(bool is_mysql_comment = false);
	/**
	 * Used to check the escape character encountered in the string
	 * Character sets marked with escape_with_backslash_is_dangerous, such as
	 * big5, cp932, gbk, sjis. the escape character (0x5C) may be part of a multi-byte
	 * character and requires special judgment
	 */
	void check_real_escape(bool &is_real_escape);
	/**
	 * Used to parse whitespace
	 * @param [in] : pos the position of the first character
	 * Return the next position of the position that meets the condition
	 * and return -1 if it is not satisfied
	 */
	int64_t is_whitespace(int64_t pos);
	/**
	 * Used to parse ({space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
	 * @param [in] : pos the position of the first character
	 * Return the next position of the position that meets the condition
	 * and return -1 if it is not satisfied
	 */
	int64_t is_hint_begin(int64_t pos);
	/**
	 * Used to parse [A-Za-z0-9$_#] or {UTF8_GB_CHAR}
	 * @param [in] : pos the position of the first character
	 * Return the next position of the position that meets the condition
	 * and return -1 if it is not satisfied
	 */
	int64_t is_identifier_flags(const int64_t pos);
	// \({space}*{int_num}{space}*,{space}*{int_num}{space}*\)
	int64_t is_2num_second(int64_t pos);
	// to{space}+(day|hour|minute|second{interval_pricision}?)
	int64_t is_interval_ds(int64_t pos);
	// to{space}+(year|month)
	int64_t is_interval_ym(int64_t pos);

	/**
	 * Used to parse ({interval_pricision}{space}*|{space}+)to{space}+
	 * @param [in] : pos the position of the first character
	 * Return the next position of the position that meets the condition
	 * and return -1 if it is not satisfied
	 */
	int64_t is_interval_pricision_with_space(int64_t pos);
	/**
	 * Used to parse {space}*\({space}*{int_num}{space}*\)
	 * @param [in] : pos the position of the first character
	 * Return the next position of the position that meets the condition
	 * and return -1 if it is not satisfied
	 */
	int64_t is_interval_pricision(int64_t pos);
	/**
	 * Used to parse {space}*{int_num}{space}*
	 * @param [in] : pos the position of the first character
	 * Return the next position of the position that meets the condition
	 * and return -1 if it is not satisfied
	 */
	int64_t is_digit_with_space(int64_t pos);
	/**
	 * Used to parse the following interval-related tokens compatible with oracle
	 * Interval{whitespace}?'[^']*'{space}*(year|month){interval_pricision}?
	 * Interval{whitespace}?'[^']*'{space}*(year|month)({interval_pricision}{space}*|
	 * {space}+)to{space}+(year|month)
	 * Interval{whitespace}?'[^']*'{space}*second{space}*\({space}*{int_num}{space}*,
	 * {space}*{int_num}{space}*\)
	 * Interval{whitespace}?'[^']*'{space}*(day|hour|minute|second){interval_pricision}?
	 * Interval{whitespace}?'[^']*'{space}*(day|hour|minute|second)({interval_pricision}{space}*|
	 * {space}+)to{space}+(day|hour|minute|second{interval_pricision}?)
	 */
	int process_interval();

	int process_insert_or_replace(const char *str, const int64_t size);
	virtual int process_values(const char *str) = 0;

	int get_one_insert_row_str(ObRawSql &raw_sql,
                             ObString &str,
                             bool &is_valid,
                             int64_t &ins_params_count,
                             bool &is_insert_up,
                             int64_t &on_duplicate_params,
                             int64_t &end_pos);

	int copy_trimed_data_buff(char *new_sql_buf,
                            int64_t buf_len,
                            int64_t &pos,
                            const int64_t start_pos,
                            const int64_t end_pos,
                            ObRawSql &src);

	bool is_split_character(ObRawSql &raw_sql);

	int check_is_on_duplicate_key(ObRawSql &raw_sql, bool &is_on_duplicate_key);
	bool skip_space(ObRawSql &raw_sql);

protected:
	ObRawSql raw_sql_;
	char *no_param_sql_;
	int64_t no_param_sql_len_;
	int param_num_;
	bool is_oracle_mode_;
	bool is_batched_multi_stmt_split_on_;
	bool is_udr_mode_;
	QuestionMarkDefNameCtx *def_name_ctx_;
	int64_t cur_token_begin_pos_;
	int64_t copy_begin_pos_;
	int64_t copy_end_pos_;
	char *tmp_buf_;
	int64_t tmp_buf_len_;
	int64_t last_escape_check_pos_;
	ParamList *param_node_list_;
	ParamList *tail_param_node_;
	TokenType cur_token_type_;
	ObQuestionMarkCtx question_mark_ctx_;
	common::ObIAllocator &allocator_;
	common::ObCharsetType charset_type_;
	const ObCharsetInfo *charset_info_;
	bool get_insert_;
	int64_t values_token_pos_;
	ParseNextTokenFunc parse_next_token_func_;
	ProcessIdfFunc process_idf_func_;

private:
	DISALLOW_COPY_AND_ASSIGN(ObFastParserBase);
};

class ObFastParserMysql final : public ObFastParserBase
{
public:
	explicit ObFastParserMysql(
		common::ObIAllocator &allocator,
		const FPContext fp_ctx)
		: ObFastParserBase(allocator, fp_ctx),
			sql_mode_(fp_ctx.sql_mode_)
	{
		is_oracle_mode_ = false;
		set_callback_func(
		static_cast<ParseNextTokenFunc>(&ObFastParserMysql::parse_next_token),
		static_cast<ProcessIdfFunc>(&ObFastParserMysql::process_identifier));
	}
	~ObFastParserMysql() {}
	virtual int process_values(const char *str) override;
	ObIArray<ObValuesTokenPos> &get_values_tokens() { return values_tokens_; }
private:
	ObSQLMode sql_mode_;
	int parse_next_token();
	int process_identifier(bool is_number_begin);
	/** 
	 * In case of two adjacent string literal, such as " 'a' 'b' ", the two string will be
	 * concatenate into 'ab'. However, the string 'a' will used as the column name if it appears
	 * in the select list, which means we must save it rather than just skipping the 'sqnewline'.
	 * so, we remember the first string as a child of the 'T_VARCHAR' node which represents
	 * " 'a' 'b' ", whose str_value_ is 'ab'. This will save us from modifying our grammar and a
	 * a lot of troubles.
	 */
	int process_string(const char quote);
	int process_zero_identifier();
	int process_identifier_begin_with_n();
private:
	ObSEArray<ObValuesTokenPos, 4> values_tokens_;
	DISALLOW_COPY_AND_ASSIGN(ObFastParserMysql);
};

class ObFastParserOracle final : public ObFastParserBase
{
public:
	explicit ObFastParserOracle(
		common::ObIAllocator &allocator,
		const FPContext fp_ctx)
		: ObFastParserBase(allocator, fp_ctx)
	{
		is_oracle_mode_ = true;
		set_callback_func(
		static_cast<ParseNextTokenFunc>(&ObFastParserOracle::parse_next_token),
		static_cast<ProcessIdfFunc>(&ObFastParserOracle::process_identifier));
	}
	~ObFastParserOracle() {}
	virtual int process_values(const char *str) override;
private:
	int parse_next_token();
	int process_identifier(bool is_number_begin);
	/**
	 * @param [in] : if in_q_quote is true, means that the current token
	 * starts with ("N"|"n")?("Q"|"q"){sqbegin}
	 * else, means that the current token starts with ("N"|"n")?{sqbegin }
	 */
	int process_string(const bool in_q_quote);
	int process_identifier_begin_with_n();
	char *parse_strndup_with_trim_space_for_new_line(const char *str,
                                                     size_t nbyte,
													 char *buf,
													 int *connection_collation,
												     int64_t *new_len);

private:
	DISALLOW_COPY_AND_ASSIGN(ObFastParserOracle);
};
} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_PARSER_FAST_PARSER_ */
