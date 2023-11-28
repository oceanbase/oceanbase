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
 *
 * Table Matcher
 */

#ifndef OCEANBASE_LIBOBCDC_TABLE_MATCHER_H__
#define OCEANBASE_LIBOBCDC_TABLE_MATCHER_H__

#include <fnmatch.h>        // fnmatch, FNM_CASEFOLD
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace libobcdc
{
class IObLogTableMatcher
{
public:
  virtual ~IObLogTableMatcher() {}

public:
  /// Matching functions
  /// Because the oracle schema introduces a case-sensitive mechanism, database/table needs to be case-sensitive
  //// The fnmatch() function can no longer use FNM_NOESCAPE
  ///
  /// @param [in] tenant_name     Tenant name
  /// @param [in] db_name         database name
  /// @param [in] tb_name         table name
  /// @param [out] matched        return value, indicating whether or not it matches
  /// @param [in] fnmatch_flags   fnmatch flags
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  virtual int table_match(const char *tenant_name,
      const char *db_name,
      const char *tb_name,
      bool &matched,
      const int fntatch_flags) = 0;

  /// check database match
  ///
  /// @param [in]  tenant_name   tenant name
  /// @param [in]  db_name       database name
  /// @param [out] matched       is matched or not
  /// @param [in]  fnmatch_flags fnmatch flags
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  virtual int database_match(const char *tenant_name,
      const char *db_name,
      bool &matched,
      const int fnmatch_flags) = 0;

  /// check tenant match
  ///
  /// @param [in] tenant_name   tenant name
  /// @param [out] matched      is matched or not
  /// @param [in] fnmatch_flags fnmatch flags
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  virtual int tenant_match(const char *tenant_name,
      bool &matched,
      const int fnmatch_flags = FNM_CASEFOLD) = 0;

  /// PG match function: matches based on tenatn_name and tablegroup_name
  ///
  ///
  /// @param [in] tenant_name         tenant name
  /// @param [in] tablegroup_name     tablegroup name
  /// @param [out] matched            is match or not
  /// @param [in] fnmatch_flags       fnmatch flags
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  virtual int tablegroup_match(const char *tenant_name,
      const char *tablegroup_name,
      bool &matched,
      const int fntatch_flags) = 0;

  /// is serving the cluster
  ///
  /// @param [out] matched            is matched or not
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  virtual int cluster_match(bool &matched) = 0;
};

/*
 * Impl.
 *
 */
class ObLogTableMatcher : public IObLogTableMatcher
{
public:
  ObLogTableMatcher();
  virtual ~ObLogTableMatcher();

public:
  int table_match(const char *tenant_name,
      const char *db_name,
      const char *tb_name,
      bool &matched,
      const int fnmatch_flags);

  int database_match(const char *tenant_name,
      const char *db_name,
      bool &matched,
      const int fnmatch_flags);

  int tenant_match(const char *tenant_name,
      bool &matched,
      const int fnmatch_flags = FNM_CASEFOLD);

  int tablegroup_match(const char *tenant_name,
      const char *tablegroup_name,
      bool &matched,
      const int fntatch_flags);

  int cluster_match(bool &matched);

  static int match(const char *pattern1,
                   const common::ObIArray<common::ObString> &pattern2,
                   bool &matched,
                   const int fnmatch_flags = FNM_CASEFOLD);
  /*
   * Init table matcher.
   */
  int init(const char *tb_white_list,
      const char *tb_black_list,
      const char *tg_white_list,
      const char *tg_black_list);

  /*
   * Destroy.
   */
  int destroy();

private:
  int table_match_pattern_(const bool is_black,
      const char* tenant_name,
      const char* db_name,
      const char* tb_name,
      bool& matched,
      const int fnmatch_flags);

  int database_match_pattern_(const bool is_black,
      const char *tenant_name,
      const char *db_name,
      bool &matched,
      const int fnmatch_flags);

  int tenant_match_pattern_(const bool is_black,
      const char* tenant_name,
      bool& matched,
      const int fnmatch_flags);

  int tablegroup_match_pattern_(const bool is_black,
      const char* tenant_name,
      const char* tablegroup_name,
      bool& matched,
      const int fnmatch_flags);

  int set_pattern_internal_(const char* pattern_str,
      const bool is_pg,
      const bool is_black);

  // Set table whitelist
  int set_pattern_(const char *pattern_str);
  // Set table blacklist
  int set_black_pattern_(const char *black_pattern_str);
  // Build table pattern array.
  int build_patterns_(const bool is_black);

  // Set tablegroup whitelist
  int set_pg_pattern_(const char *pattern_str);
  // Set tablegroup blacklist
  int set_black_pg_pattern_(const char *black_pattern_str);
  // Build pg pattern array.
  int build_pg_patterns_(const bool is_black);

private:
  struct Pattern
  {
    // Tenant.
    common::ObString tenant_pattern_;
    // Database.
    common::ObString database_pattern_;
    // Table.
    common::ObString table_pattern_;

    /*
     * Reset. Set patterns to empty string.
     */
    void reset();
    TO_STRING_KV(K_(tenant_pattern), K_(database_pattern), K_(table_pattern));
  };
  typedef common::ObArray<Pattern> PatternArray;

  struct PgPattern
  {
    // Tenant.
    common::ObString tenant_pattern_;
    // Tablegroup.
    common::ObString tablegroup_pattern_;

    /*
     * Reset. Set patterns to empty string.
     */
    void reset();
    TO_STRING_KV(K_(tenant_pattern), K_(tablegroup_pattern));
  };
  typedef common::ObArray<PgPattern> PgPatternArray;
private:
  // Pattern Array.
  PatternArray patterns_;
  // Buffer.
  char *buf_;
  int64_t buf_size_;

  // balcklist array
  PatternArray black_patterns_;
  // Buffer.
  char *black_buf_;
  int64_t black_buf_size_;

  // PG Whitelist
  // PgPattern Array.
  PgPatternArray pg_patterns_;
  char *pg_buf_;
  int64_t pg_buf_size_;

  // PG Blacklist
  // PgPattern Array.
  PgPatternArray black_pg_patterns_;
  char *black_pg_buf_;
  int64_t black_pg_buf_size_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTableMatcher);
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_TABLE_MATCHER_H__ */
