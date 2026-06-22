/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_PARSER_OB_PARSER_FULLWIDTH_CONVERTER_H_
#define OCEANBASE_SQL_PARSER_OB_PARSER_FULLWIDTH_CONVERTER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "sql/parser/parse_node.h"
#ifndef SQL_PARSER_COMPILATION
#include "share/config/ob_server_config.h"
#endif

namespace oceanbase
{
namespace sql
{
/**
 * @brief Position mapping for fullwidth conversion
 *
 * Maintains a list of position mappings to translate error positions
 * from converted SQL back to original SQL.
 */

class FullWidthSymbolPosMapping {
public:
  FullWidthSymbolPosMapping() {};

  OB_INLINE int64_t count() const { return entries_.count(); }

  int add_entry(int32_t orig_pos, int32_t conv_pos, int32_t orig_len, int32_t conv_len);

  // Adjust parse result locations from converted SQL positions back to original SQL positions
  // should be called after parsing is done and before any error positions are reported
  void adjust_parse_result(ParseResult &parse_result, int32_t stmt_len);
  // convert a position in the converted SQL back to the original SQL position
  int32_t conv_to_orig_pos(int32_t conv_pos, bool is_end_pos) const;

private:
  struct FullWidthSymbolPosMapEntry {
    int32_t orig_pos_;            // Position in original SQL
    int32_t conv_pos_;            // Position in converted SQL
    int32_t orig_len_;            // Length in original SQL (full width bytes)
    int32_t conv_len_;            // Length in converted SQL (ASCII: 1 byte)
    int32_t accumulated_offset_;  // accumulated offset of all conversions before the target position

    TO_STRING_KV(K(orig_pos_), K(conv_pos_), K(orig_len_), K(conv_len_), K(accumulated_offset_));
  };

  // Recursively adjust ParseNode tree locations
  void adjust_stmt_loc(ParseNode *node, int32_t stmt_len);

  common::ObSEArray<FullWidthSymbolPosMapEntry, 16> entries_;
};

/**
 * @brief fullwidth converter for SQL preprocessing
 *
 * Converts full-width characters to ASCII equivalents while respecting
 * SQL syntax (preserving characters inside string literals and comments).
 * Also maintains position mapping to translate error positions from
 * converted SQL back to original SQL.
 */
class ObFullWidthSymbolConverter {
public:
  ObFullWidthSymbolConverter(common::ObIAllocator *allocator, common::ObCharsetType charset_type)
      : allocator_(allocator), charset_type_(charset_type)
  {}

  /**
   * @brief Convert fullwidth symbols in SQL to ASCII equivalents
   *
   * @param input Original SQL string
   * @param output Converted SQL string (allocated from allocator)
   * @return OB_SUCCESS on success, error code otherwise
   *
   * This function:
   * 1. Scans the input SQL for fullwidth symbols
   * 2. Skips conversion inside string literals ('...', "...", q'...')
   * 3. Skips conversion inside comments (-- ..., / * ... * /)
   * 4. Converts supported fullwidth symbols to ASCII
   * 5. Builds internal position mapping for error reporting
   */
  int convert(const common::ObString &input, common::ObString &output);

  // Adjust parse result locations from converted SQL positions back to original SQL positions
  void adjust_parse_result(ParseResult &parse_result, int32_t stmt_len);

  /**
   * @brief Check if conversion is needed for the given charset and SQL input
   *
   * This function combines two checks:
   * 1. Charset capability check: whether the charset supports fullwidth conversion
   * 2. Content pre-scan: whether the input SQL contains potential fullwidth symbols that need to be converted
   *
   * @param charset_type The connection charset type
   * @param input The SQL string to check
   * @return true if conversion is both supported and needed, false otherwise
   */
  OB_INLINE static bool need_convert(common::ObCharsetType charset_type,
                                     const common::ObString &input)
  {
    bool need_convert = false;
#ifndef SQL_PARSER_COMPILATION
    if (!GCONF._enable_sql_parse_fullwidth_symbols) {
      // Disabled by cluster config for upgrade compatibility
    } else
#endif
    if (!input.empty()) {
      const unsigned char *bytes = reinterpret_cast<const unsigned char *>(input.ptr());
      const int64_t len = input.length();
      if (charset_type == CHARSET_UTF8MB4) {
        // UTF-8: check for 0xe3 (U+3000 space) or 0xef (U+FF01-FF5D operators) prefix
        for (int64_t i = 0; i < len && !need_convert; ++i) {
          if (bytes[i] == 0xe3 || bytes[i] == 0xef) {
            need_convert = true;
          }
        }
      } else if (ObCharset::is_gb_charset(charset_type)) {
        // GBK/GB18030: check for 0xa1 or 0xa3 prefix
        for (int64_t i = 0; i < len && !need_convert; ++i) {
          if (bytes[i] == 0xa1 || bytes[i] == 0xa3) {
            need_convert = true;
          }
        }
      } else if (charset_type == CHARSET_HKSCS || charset_type == CHARSET_HKSCS31) {
        // HKSCS: check for 0xa1, 0xa2 or 0xc6 prefix
        for (int64_t i = 0; i < len && !need_convert; ++i) {
          if (bytes[i] == 0xa1 || bytes[i] == 0xa2 || bytes[i] == 0xc6) {
            need_convert = true;
          }
        }
      } else {
        // no need to convert for other charsets
      }
    }
    return need_convert;
  }

  // Whether any fullwidth conversions were performed
  OB_INLINE bool has_conversions() const { return pos_mapping_.count() > 0; }

  /**
   * @brief Map an fullwidth sequence to its ASCII equivalent
   *
   * @param p Pointer to the first byte of the candidate sequence
   * @param remaining Remaining bytes starting from p
   * @return ASCII equivalent character, or '\0' if not a supported fullwidth
   */
  OB_INLINE static char map_fullwidth_symbol_to_ascii(const char *p,
                                                      int64_t remaining,
                                                      common::ObCharsetType charset_type,
                                                      int32_t &char_len)
  {
    char result = '\0';
    char_len = 0;
    if (charset_type == CHARSET_UTF8MB4) {
      result = map_utf8_fullwidth_symbol_to_ascii(p, remaining, char_len);
    } else if (ObCharset::is_gb_charset(charset_type)) {
      result = map_gbk_fullwidth_symbol_to_ascii(p, remaining, char_len);
    } else if (charset_type == CHARSET_HKSCS || charset_type == CHARSET_HKSCS31) {
      result = map_hkscs_fullwidth_symbol_to_ascii(p, remaining, char_len);
    }
    return result;
  }

private:
  OB_INLINE static char map_utf8_fullwidth_symbol_to_ascii(const char *p,
                                                           int64_t remaining,
                                                           int32_t &char_len)
  {
    char result = '\0';
    char_len = 0;
    if (NULL != p && remaining >= 3) {
      unsigned char c0 = static_cast<unsigned char>(p[0]);
      unsigned char c1 = static_cast<unsigned char>(p[1]);
      unsigned char c2 = static_cast<unsigned char>(p[2]);
      if (c0 == 0xe3 && c1 == 0x80 && c2 == 0x80) {
        result = ' ';  // E38080 -> U+3000 full-width space
      } else if (c0 == 0xef && c1 == 0xbc) {
        switch (c2) {
          case 0x81: result = '!'; break;  // EFBC81 -> U+FF01 ！
          case 0x83: result = '#'; break;  // EFBC83 -> U+FF03 ＃
          case 0x85: result = '%'; break;  // EFBC85 -> U+FF05 ％
          case 0x86: result = '&'; break;  // EFBC86 -> U+FF06 ＆
          case 0x88: result = '('; break;  // EFBC88 -> U+FF08 （
          case 0x89: result = ')'; break;  // EFBC89 -> U+FF09 ）
          case 0x8a: result = '*'; break;  // EFBC8A -> U+FF0A ＊
          case 0x8b: result = '+'; break;  // EFBC8B -> U+FF0B ＋
          case 0x8c: result = ','; break;  // EFBC8C -> U+FF0C ，
          case 0x8d: result = '-'; break;  // EFBC8D -> U+FF0D －
          case 0x8e: result = '.'; break;  // EFBC8E -> U+FF0E ．
          case 0x8f: result = '/'; break;  // EFBC8F -> U+FF0F ／
          case 0x9a: result = ':'; break;  // EFBC9A -> U+FF1A ：
          case 0x9c: result = '<'; break;  // EFBC9C -> U+FF1C ＜
          case 0x9d: result = '='; break;  // EFBC9D -> U+FF1D ＝
          case 0x9e: result = '>'; break;  // EFBC9E -> U+FF1E ＞
          case 0x9f: result = '?'; break;  // EFBC9F -> U+FF1F ？
          case 0xa0: result = '@'; break;  // EFBCA0 -> U+FF20 ＠
          case 0xbb: result = '['; break;  // EFBCBB -> U+FF3B ［
          case 0xbd: result = ']'; break;  // EFBCBD -> U+FF3D ］
          case 0xbe: result = '^'; break;  // EFBCBE -> U+FF3E ＾
          default:
            break;
        }
      } else if (c0 == 0xef && c1 == 0xbd) {
        switch (c2) {
          case 0x9b: result = '{'; break;  // EFBD9B -> U+FF5B ｛
          case 0x9c: result = '|'; break;  // EFBD9C -> U+FF5C ｜
          case 0x9d: result = '}'; break;  // EFBD9D -> U+FF5D ｝
          default: break;
        }
      }
    }
    char_len = (result != '\0') ? 3 : 0;
    return result;
  }

  OB_INLINE static char map_gbk_fullwidth_symbol_to_ascii(const char *p,
                                                          int64_t remaining,
                                                          int32_t &char_len)
  {
    char result = '\0';
    char_len = 0;
    if (NULL != p && remaining >= 2) {
      unsigned char c0 = static_cast<unsigned char>(p[0]);
      unsigned char c1 = static_cast<unsigned char>(p[1]);
      if (c0 == 0xa1 && c1 == 0xa1) {
        result = ' '; // A1A1 -> U+3000 full-width space
      } else if (c0 == 0xa3) {
        switch (c1) {
          case 0xa1: result = '!'; break;  // A3A1 -> U+FF01 ！
          case 0xa3: result = '#'; break;  // A3A3 -> U+FF03 ＃
          case 0xa5: result = '%'; break;  // A3A5 -> U+FF05 ％
          case 0xa6: result = '&'; break;  // A3A6 -> U+FF06 ＆
          case 0xa8: result = '('; break;  // A3A8 -> U+FF08 （
          case 0xa9: result = ')'; break;  // A3A9 -> U+FF09 ）
          case 0xaa: result = '*'; break;  // A3AA -> U+FF0A ＊
          case 0xab: result = '+'; break;  // A3AB -> U+FF0B ＋
          case 0xac: result = ','; break;  // A3AC -> U+FF0C ，
          case 0xad: result = '-'; break;  // A3AD -> U+FF0D －
          case 0xae: result = '.'; break;  // A3AE -> U+FF0E ．
          case 0xaf: result = '/'; break;  // A3AF -> U+FF0F ／
          case 0xba: result = ':'; break;  // A3BA -> U+FF1A ：
          case 0xbc: result = '<'; break;  // A3BC -> U+FF1C ＜
          case 0xbd: result = '='; break;  // A3BD -> U+FF1D ＝
          case 0xbe: result = '>'; break;  // A3BE -> U+FF1E ＞
          case 0xbf: result = '?'; break;  // A3BF -> U+FF1F ？
          case 0xc0: result = '@'; break;  // A3C0 -> U+FF20 ＠
          case 0xdb: result = '['; break;  // A3DB -> U+FF3B ［
          case 0xdd: result = ']'; break;  // A3DD -> U+FF3D ］
          case 0xde: result = '^'; break;  // A3DE -> U+FF3E ＾
          case 0xfb: result = '{'; break;  // A3FB -> U+FF5B ｛
          case 0xfc: result = '|'; break;  // A3FC -> U+FF5C ｜
          case 0xfd: result = '}'; break;  // A3FD -> U+FF5D ｝
          default:
            break;
        }
      }
      char_len = (result != '\0') ? 2 : 0;
    }
    return result;
  }

  OB_INLINE static char map_hkscs_fullwidth_symbol_to_ascii(const char *p,
                                                     int64_t remaining,
                                                     int32_t &char_len)
  {
    char result = '\0';
    char_len = 0;
    if (NULL != p && remaining >= 2) {
      unsigned char c0 = static_cast<unsigned char>(p[0]);
      unsigned char c1 = static_cast<unsigned char>(p[1]);
      // U+FF3E ^ has no valid encoding in HKSCS:
      if (c0 == 0xa1) {
        switch (c1) {
          case 0x40: result = ' '; break;  // A140 -> U+3000 full-width space
          case 0x41: result = ','; break;  // A141 -> U+FF0C ，
          case 0x44: result = '.'; break;  // A144 -> U+FF0E ．
          case 0x47: result = ':'; break;  // A147 -> U+FF1A ：
          case 0x48: result = '?'; break;  // A148 -> U+FF1F ？
          case 0x49: result = '!'; break;  // A149 -> U+FF01 ！
          case 0x55: result = '|'; break;  // A155 -> U+FF5C ｜
          case 0x5d: result = '('; break;  // A15D -> U+FF08 （
          case 0x5e: result = ')'; break;  // A15E -> U+FF09 ）
          case 0x61: result = '{'; break;  // A161 -> U+FF5B ｛
          case 0x62: result = '}'; break;  // A162 -> U+FF5D ｝
          case 0xad: result = '#'; break;  // A1AD -> U+FF03 ＃
          case 0xae: result = '&'; break;  // A1AE -> U+FF06 ＆
          case 0xaf: result = '*'; break;  // A1AF -> U+FF0A ＊
          case 0xcf: result = '+'; break;  // A1CF -> U+FF0B ＋
          case 0xd0: result = '-'; break;  // A1D0 -> U+FF0D －
          case 0xd5: result = '<'; break;  // A1D5 -> U+FF1C ＜
          case 0xd6: result = '>'; break;  // A1D6 -> U+FF1E ＞
          case 0xd7: result = '='; break;  // A1D7 -> U+FF1D ＝
          case 0xfe: result = '/'; break;  // A1FE -> U+FF0F ／
          default:
            break;
        }
      } else if (c0 == 0xa2) {
        switch (c1) {
          case 0x48: result = '%'; break;  // A248 -> U+FF05 ％
          case 0x49: result = '@'; break;  // A249 -> U+FF20 ＠
          default:
            break;
        }
      } else if (c0 == 0xc6) {
        switch (c1) {
          case 0xe4: result = '['; break;  // C6E4 -> U+FF3B ［
          case 0xe5: result = ']'; break;  // C6E5 -> U+FF3D ］
          default:
            break;
        }
      }
    }
    char_len = (result != '\0') ? 2 : 0;
    return result;
  }

private:
  int64_t find_next_convertible_char_pos(const char *input,
                                         int64_t len,
                                         int64_t pos,
                                         char &ascii,
                                         int32_t &char_len) const;
  bool is_comment_start(const char *input, int64_t pos, int64_t len) const;
  int64_t skip_comment(const char *input, int64_t pos, int64_t len) const;
  bool is_string_start(const char *input, int64_t pos, int64_t len) const;
  int64_t skip_string_literal(const char *input, int64_t pos, int64_t len) const;

private:
  common::ObIAllocator *allocator_;
  common::ObCharsetType charset_type_;
  FullWidthSymbolPosMapping pos_mapping_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_PARSER_OB_PARSER_FULLWIDTH_CONVERTER_H_ */
