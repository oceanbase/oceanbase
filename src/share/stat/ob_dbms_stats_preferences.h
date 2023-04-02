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

#ifndef OB_DBMS_STATS_PREFERENCES_H
#define OB_DBMS_STATS_PREFERENCES_H

#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase {
using namespace sql;
namespace common {

class ObStatPrefs
{
  public:
    ObStatPrefs() : allocator_(NULL), session_info_(NULL), pvalue_(), is_decoded_(false),
                    is_global_prefs_(false) {}
    ObStatPrefs(ObIAllocator *alloc,
                ObSQLSessionInfo *session_info,
                const ObString &pvalue) :
      allocator_(alloc), session_info_(session_info), pvalue_(pvalue), is_decoded_(false),
      is_global_prefs_(false) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) {
      UNUSED(param);
      return OB_NOT_IMPLEMENT;
    }
    virtual const char* get_stat_pref_name() const { return nullptr; }
    virtual const char* get_stat_pref_default_value() const { return nullptr; }
    int decode_pref_result(ObIAllocator *allocator,
                           const ObObj &name_obj,
                           const ObObj &val_obj,
                           ObTableStatParam &param);
    int dump_pref_name_and_value(ObString &pref_name, ObString &pvalue);
    bool is_decoded() const { return is_decoded_; }
    void set_is_global_prefs(const bool is_global) { is_global_prefs_ = is_global; }
    bool is_global_prefs() const { return is_global_prefs_; }
    TO_STRING_KV(K(pvalue_),
                 K(is_decoded_),
                 K(is_global_prefs_));
  protected:
    ObIAllocator *allocator_;
    ObSQLSessionInfo *session_info_;
    ObString pvalue_;
    bool is_decoded_;
    bool is_global_prefs_;
};

class ObCascadePrefs : public ObStatPrefs
{
  public:
    ObCascadePrefs() : ObStatPrefs() {}
    ObCascadePrefs(ObIAllocator *alloc,
                   ObSQLSessionInfo *session_info,
                   const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "CASCADE"; }
    virtual const char* get_stat_pref_default_value() const { return "DBMS_STATS.AUTO_CASCADE"; }
};

class ObDegreePrefs : public ObStatPrefs
{
  public:
    ObDegreePrefs() : ObStatPrefs() {}
    ObDegreePrefs(ObIAllocator *alloc,
                  ObSQLSessionInfo *session_info,
                  const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "DEGREE"; }
    virtual const char* get_stat_pref_default_value() const { return NULL; }
};

class ObEstimatePercentPrefs : public ObStatPrefs
{
  public:
    ObEstimatePercentPrefs() : ObStatPrefs() {}
    ObEstimatePercentPrefs(ObIAllocator *alloc,
                           ObSQLSessionInfo *session_info,
                           const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "ESTIMATE_PERCENT"; }
    virtual const char* get_stat_pref_default_value() const { return "DBMS_STATS.AUTO_SAMPLE_SIZE";}
};

class ObGranularityPrefs : public ObStatPrefs
{
  public:
    ObGranularityPrefs() : ObStatPrefs() {}
    ObGranularityPrefs(ObIAllocator *alloc,
                       ObSQLSessionInfo *session_info,
                       const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "GRANULARITY"; }
    virtual const char* get_stat_pref_default_value() const { return "AUTO"; }
};

class ObIncrementalPrefs : public ObStatPrefs
{
  public:
    ObIncrementalPrefs() : ObStatPrefs() {}
    ObIncrementalPrefs(ObIAllocator *alloc,
                       ObSQLSessionInfo *session_info,
                       const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "INCREMENTAL"; }
    virtual const char* get_stat_pref_default_value() const { return "FALSE"; }
};

class ObIncrementalLevelPrefs : public ObStatPrefs
{
  public:
    ObIncrementalLevelPrefs() : ObStatPrefs() {}
    ObIncrementalLevelPrefs(ObIAllocator *alloc,
                            ObSQLSessionInfo *session_info,
                            const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "INCREMENTAL_LEVEL"; }
    virtual const char* get_stat_pref_default_value() const { return "PARTITION"; }
};

class ObMethodOptPrefs : public ObStatPrefs
{
  public:
    ObMethodOptPrefs() : ObStatPrefs() {}
    ObMethodOptPrefs(ObIAllocator *alloc,
                     ObSQLSessionInfo *session_info,
                     const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "METHOD_OPT"; }
    virtual const char* get_stat_pref_default_value() const { return "FOR ALL COLUMNS SIZE AUTO"; }
  private:
    int check_global_method_opt_prefs_value_validity(ObString &method_opt_val);
};

class ObNoInvalidatePrefs : public ObStatPrefs
{
  public:
    ObNoInvalidatePrefs() : ObStatPrefs() {}
    ObNoInvalidatePrefs(ObIAllocator *alloc,
                        ObSQLSessionInfo *session_info,
                        const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "NO_INVALIDATE"; }
    virtual const char* get_stat_pref_default_value() const { return "DBMS_STATS.AUTO_INVALIDATE"; }
};

class ObOptionsPrefs : public ObStatPrefs
{
  public:
    ObOptionsPrefs() : ObStatPrefs() {}
    ObOptionsPrefs(ObIAllocator *alloc,
                   ObSQLSessionInfo *session_info,
                   const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "OPTIONS"; }
    virtual const char* get_stat_pref_default_value() const { return "GATHER"; }
};

class ObStalePercentPrefs : public ObStatPrefs
{
  public:
    ObStalePercentPrefs() : ObStatPrefs() {}
    ObStalePercentPrefs(ObIAllocator *alloc,
                        ObSQLSessionInfo *session_info,
                        const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "STALE_PERCENT"; }
    virtual const char* get_stat_pref_default_value() const { return "10"; }
};

class ObApproximateNdvPrefs : public ObStatPrefs
{
  public:
    ObApproximateNdvPrefs() : ObStatPrefs() {}
    ObApproximateNdvPrefs(ObIAllocator *alloc,
                          ObSQLSessionInfo *session_info,
                          const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "APPROXIMATE_NDV"; }
    virtual const char* get_stat_pref_default_value() const { return "TRUE"; }
};

class ObEstimateBlockPrefs : public ObStatPrefs
{
  public:
    ObEstimateBlockPrefs() : ObStatPrefs() {}
    ObEstimateBlockPrefs(ObIAllocator *alloc,
                          ObSQLSessionInfo *session_info,
                          const ObString &pvalue) :
      ObStatPrefs(alloc, session_info, pvalue) {}
    virtual int check_pref_value_validity(ObTableStatParam *param = NULL) override;
    virtual const char* get_stat_pref_name() const { return "ESTIMATE_BLOCK"; }
    virtual const char* get_stat_pref_default_value() const { return "TRUE"; }
};

template <class T>
static int new_stat_prefs(ObIAllocator &allocator, ObSQLSessionInfo *session_info,
                          const ObString &opt_value, T *&src)
{
  int ret = OB_SUCCESS;
  src = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator.alloc(sizeof(T)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "failed to allocate memory", K(ptr), K(sizeof(T)));
  } else {
    src = new (ptr) T(&allocator, session_info, opt_value);
  }
  return ret;
};

class ObDbmsStatsPreferences
{
public:

  static int reset_global_pref_defaults(ObExecContext &ctx);

  static int get_prefs(ObExecContext &ctx,
                       const ObTableStatParam &param,
                       const ObString &opt_name,
                       ObObj &result);

  static int set_prefs(ObExecContext &ctx,
                       const ObIArray<uint64_t> &table_ids,
                       const ObString &opt_name,
                       const ObString &opt_value);

  static int delete_user_prefs(ObExecContext &ctx,
                               const ObIArray<uint64_t> &table_ids,
                               const ObString &opt_name);

  static int get_sys_default_stat_options(ObExecContext &ctx,
                                          ObIArray<ObStatPrefs*> &stat_prefs,
                                          ObTableStatParam &param);

  static int gen_init_global_prefs_sql(ObSqlString &init_sql,
                                       bool is_reset_prefs = false,
                                       int64_t *expect_affected_rows = NULL);

private:
  static int do_get_prefs(ObExecContext &ctx,
                          ObIAllocator *allocator,
                          const ObSqlString &raw_sql,
                          bool &get_result,
                          ObObj &result);

  static int get_user_prefs_sql(const uint64_t tenant_id,
                                const uint64_t table_id,
                                const ObString &opt_name,
                                const ObString &opt_value,
                                const int64_t current_time,
                                ObSqlString &sql_string);

  static int gen_sname_list_str(ObIArray<ObStatPrefs*> &stat_prefs,
                                ObSqlString &sname_list);

  static int do_get_sys_perfs(ObExecContext &ctx,
                              const ObSqlString &raw_sql,
                              ObIArray<ObStatPrefs*> &need_acquired_prefs,
                              ObTableStatParam &param);

  static int decode_perfs_result(ObIAllocator *allocator,
                                 sqlclient::ObMySQLResult &client_result,
                                 ObIArray<ObStatPrefs*> &need_acquired_prefs,
                                 ObTableStatParam &param);

  static int get_no_acquired_prefs(ObIArray<ObStatPrefs*> &stat_prefs,
                                   ObIArray<ObStatPrefs*> &no_acquired_prefs);
};


} // end of sql
} // end of namespace

#endif //OB_DBMS_STATS_PREFERENCES_H