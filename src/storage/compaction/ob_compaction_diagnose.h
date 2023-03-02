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

#ifndef SRC_STORAGE_COMPACTION_OB_COMPACTION_DIAGNOSE_H_
#define SRC_STORAGE_COMPACTION_OB_COMPACTION_DIAGNOSE_H_

#include "storage/ob_i_store.h"
#include "ob_tablet_merge_task.h"

namespace oceanbase
{
namespace storage
{
class ObPGPartition;
}
namespace rootserver
{
  class ObMajorFreezeService;
}
using namespace storage;
using namespace share;
namespace compaction
{
struct ObDiagnoseTabletCompProgress;

struct ObScheduleSuspectInfo : public common::ObDLinkBase<ObScheduleSuspectInfo>, public ObMergeDagHash
{
  ObScheduleSuspectInfo()
   : ObMergeDagHash(),
     tenant_id_(OB_INVALID_ID),
     add_time_(0),
     suspect_info_("\0")
  {}
  int64_t hash() const;
  bool is_valid() const;
  ObScheduleSuspectInfo & operator = (const ObScheduleSuspectInfo &other);

  static int64_t gen_hash(int64_t tenant_id, int64_t dag_hash);
  TO_STRING_KV(K_(tenant_id), K_(merge_type), K_(ls_id), K_(tablet_id), K_(add_time), K_(suspect_info));
  int64_t tenant_id_;
  int64_t add_time_;
  char suspect_info_[common::OB_DIAGNOSE_INFO_LENGTH];
};

class ObScheduleSuspectInfoMgr {
public:
  ObScheduleSuspectInfoMgr();
  ~ObScheduleSuspectInfoMgr() { destroy(); }

  static ObScheduleSuspectInfoMgr &get_instance() {
    static ObScheduleSuspectInfoMgr instance_;
    return instance_;
  }

  int init();
  void destroy();

  int add_suspect_info(const int64_t key_value, ObScheduleSuspectInfo &info);
  int get_suspect_info(const int64_t key_value, ObScheduleSuspectInfo &info);
  int del_suspect_info(const int64_t key_value);
  int gc_info();

private:
  static const int64_t SUSPECT_INFO_BUCKET_NUM = 1000;
  static const int64_t SUSPECT_INFO_LIMIT = 10000;
  static const int64_t GC_INFO_TIME_LIMIT = 1L * 60 * 60 * 1000 * 1000L; // 1hr
  typedef common::hash::ObHashMap<int64_t, ObScheduleSuspectInfo *> InfoMap;

public:
  static const int64_t EXTRA_INFO_LEN = 900;

private:
  bool is_inited_;
  common::DefaultPageAllocator allocator_;
  common::SpinRWLock lock_;
  InfoMap info_map_;
};

struct ObCompactionDiagnoseInfo
{
  enum ObDiagnoseStatus
  {
    DIA_STATUS_NOT_SCHEDULE = 0,
    DIA_STATUS_RUNNING = 1,
    DIA_STATUS_FAILED = 2,
    DIA_STATUS_UNCOMPACTED = 3,
    DIA_STATUS_MAX
  };
  const static char *ObDiagnoseStatusStr[DIA_STATUS_MAX];
  static const char * get_diagnose_status_str(ObDiagnoseStatus status);
  TO_STRING_KV(K_(merge_type), K_(tenant_id), K_(ls_id), K_(tablet_id), K_(status), K_(timestamp),
      K_(diagnose_info));

  storage::ObMergeType merge_type_;
  int64_t tenant_id_;
  int64_t ls_id_;
  int64_t tablet_id_;
  int64_t timestamp_;
  ObDiagnoseStatus status_;
  char diagnose_info_[common::OB_DIAGNOSE_INFO_LENGTH];
};

class ObCompactionDiagnoseMgr
{
public:
  ObCompactionDiagnoseMgr();
  ~ObCompactionDiagnoseMgr() { reset(); }
  void reset();
  int init(ObCompactionDiagnoseInfo *info_array, const int64_t max_cnt);
  int diagnose_all_tablets(const int64_t tenant_id);
  int diagnose_tenant_tablet();
  int diagnose_tenant_major_merge();
  int64_t get_cnt() { return idx_; }
  static int diagnose_dag(
      const storage::ObMergeType merge_type,
      const ObLSID ls_id,
      const ObTabletID tablet_id,
      const int64_t merge_version,
      ObTabletMergeDag &dag,
      ObDiagnoseTabletCompProgress &input_progress);
  static int check_system_compaction_config(char *tmp_str, const int64_t buf_len);
private:
  int diagnose_tablet_mini_merge(const ObLSID &ls_id, ObTablet &tablet);
  int diagnose_tablet_minor_merge(const ObLSID &ls_id, ObTablet &tablet);
  int diagnose_tablet_medium_merge(
      const ObLSID &ls_id,
      ObTablet &tablet);
  int diagnose_tablet_major_merge(
      const int64_t compaction_scn,
      const ObLSID &ls_id,
      ObTablet &tablet);
  int diagnose_tablet_merge(
      ObTabletMergeDag &dag,
      const ObMergeType type,
      const ObLSID ls_id,
      const ObTabletID tablet_id,
      int64_t merge_version = ObVersionRange::MIN_VERSION);
  int diagnose_no_dag(
      ObTabletMergeDag &dag,
      const ObMergeType merge_type,
      const ObLSID ls_id,
      const ObTabletID tablet_id,
      const int64_t compaction_scn);
  int get_suspect_and_warning_info(
      ObTabletMergeDag &dag,
      const ObMergeType merge_type,
      const ObLSID ls_id,
      const ObTabletID tablet_id,
      ObScheduleSuspectInfo &info);
  int diagnose_medium_scn_table(const int64_t compaction_scn);
  OB_INLINE bool can_add_diagnose_info() { return idx_ < max_cnt_; }
  int get_suspect_info(
      const ObMergeType merge_type,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObScheduleSuspectInfo &ret_info);
  int check_if_need_diagnose(rootserver::ObMajorFreezeService *&major_freeze_service,
                             bool &need_diagnose) const;
  int do_tenant_major_merge_diagnose(rootserver::ObMajorFreezeService *major_freeze_service);

private:
  static const int64_t WAIT_MEDIUM_SCHEDULE_INTERVAL = 1000L * 1000L * 120L; // 120 seconds
  static const int64_t SUSPECT_INFO_WARNING_THRESHOLD = 1000L * 1000L * 60L * 5; // 5 mins
  bool is_inited_;
  ObCompactionDiagnoseInfo *info_array_;
  int64_t max_cnt_;
  int64_t idx_;
};

class ObCompactionDiagnoseIterator
{
public:
  ObCompactionDiagnoseIterator()
   : allocator_("CompDiagnose"),
     info_array_(nullptr),
     cnt_(0),
     cur_idx_(0),
     is_opened_(false)
  {
  }
  virtual ~ObCompactionDiagnoseIterator() { reset(); }
  int open(const int64_t tenant_id);
  int get_next_info(ObCompactionDiagnoseInfo &info);
  void reset();

private:
  int get_diagnose_info(const int64_t tenant_id);
private:
  const int64_t MAX_DIAGNOSE_INFO_CNT = 1000;
  ObArenaAllocator allocator_;
  ObCompactionDiagnoseInfo *info_array_;
  int64_t cnt_;
  int64_t cur_idx_;
  bool is_opened_;
};

#define DEL_SUSPECT_INFO(type, ls_id, tablet_id) \
{ \
      compaction::ObMergeDagHash dag_hash;                                                      \
      dag_hash.merge_type_ = type;                                                                     \
      dag_hash.ls_id_ = ls_id;                                                                           \
      dag_hash.tablet_id_ = tablet_id;                                                                   \
      int64_t tenant_id = MTL_ID();                                                                     \
      int64_t hash_value = ObScheduleSuspectInfo::gen_hash(tenant_id, dag_hash.inner_hash());          \
      if (OB_FAIL(ObScheduleSuspectInfoMgr::get_instance().del_suspect_info(hash_value))) { \
        if (OB_HASH_NOT_EXIST != ret) {                                                                \
          STORAGE_LOG(WARN, "failed to add suspect info", K(ret), K(dag_hash), K(tenant_id));         \
        } else {                                                                                      \
          ret = OB_SUCCESS;                                                                           \
        }                                                                                            \
      } else {                                                                                      \
        STORAGE_LOG(DEBUG, "success to add suspect info", K(ret), K(dag_hash), K(tenant_id));       \
      }                                                                                       \
}

#define DEFINE_SUSPECT_PRINT_KV(n)                                                               \
  template <LOG_TYPENAME_TN##n>                                                                  \
  int ADD_SUSPECT_INFO(storage::ObMergeType type, const ObLSID ls_id,   \
                const ObTabletID tablet_id, \
                const char *info_string, LOG_PARAMETER_KV##n)                                    \
  {                                                                                              \
    int64_t __pos = 0;                                                                           \
    int ret = OB_SUCCESS;                                                                        \
    compaction::ObScheduleSuspectInfo info;                                                      \
    info.tenant_id_ = MTL_ID();                                                                  \
    info.merge_type_ = type;                                                                     \
    info.ls_id_ = ls_id;                                                                           \
    info.tablet_id_ = tablet_id;                                                                   \
    info.add_time_ = ObTimeUtility::fast_current_time();                                          \
    char *buf = info.suspect_info_;                                                              \
    const int64_t buf_size = ::oceanbase::common::OB_DIAGNOSE_INFO_LENGTH;                       \
    if (strlen(info_string) < buf_size) {                                                         \
      strncpy(buf, info_string, strlen(info_string));                                             \
    }                                                                                             \
    __pos += strlen(info_string);                                                                 \
    if (__pos > 0 && __pos < buf_size) {                                                           \
      buf[__pos++] = '.';                                                                          \
    }                                                                                          \
    SIMPLE_TO_STRING_##n                                                                      \
    if (OB_FAIL(ObScheduleSuspectInfoMgr::get_instance().add_suspect_info(info.hash(), info))) { \
      STORAGE_LOG(WARN, "failed to add suspect info", K(ret), K(info));                          \
    } else {                                                                                      \
      STORAGE_LOG(DEBUG, "success to add suspect info", K(ret), K(info));                          \
    }                                                                                              \
    return ret;                                                                                \
  }

#define DEFINE_DIAGNOSE_PRINT_KV(n)                                                               \
  template <LOG_TYPENAME_TN##n>                                                                  \
  int SET_DIAGNOSE_INFO(ObCompactionDiagnoseInfo &diagnose_info, storage::ObMergeType type,     \
                const int64_t tenant_id, const ObLSID ls_id, const ObTabletID tablet_id,          \
                ObCompactionDiagnoseInfo::ObDiagnoseStatus status,                               \
                const int64_t timestamp,                                                         \
                LOG_PARAMETER_KV##n)                                                             \
  {                                                                                              \
    int64_t __pos = 0;                                                                           \
    int ret = OB_SUCCESS;                                                                        \
    diagnose_info.merge_type_ = type;                                                            \
    diagnose_info.ls_id_ = ls_id.id();                                                     \
    diagnose_info.tenant_id_ = tenant_id;                                                    \
    diagnose_info.tablet_id_ = tablet_id.id();                                                \
    diagnose_info.status_ = status;                                                          \
    diagnose_info.timestamp_ = timestamp;                                                          \
    char *buf = diagnose_info.diagnose_info_;                                                    \
    const int64_t buf_size = ::oceanbase::common::OB_DIAGNOSE_INFO_LENGTH;                       \
    SIMPLE_TO_STRING_##n                                                                      \
    buf[__pos] = '\0';                                                                             \
    return ret;                                                                              \
  }

#define DEFINE_COMPACITON_INFO_ADD_KV(n)                                                               \
  template <LOG_TYPENAME_TN##n>                                                                  \
  void ADD_COMPACTION_INFO_PARAM(char *buf, const int64_t buf_size, LOG_PARAMETER_KV##n)                  \
  {                                                                                              \
    int64_t __pos = strlen(buf);                                                                  \
    int ret = OB_SUCCESS;                                                                        \
    SIMPLE_TO_STRING_##n                                                                        \
    if (__pos > 0) {                                                                            \
      buf[__pos - 1] = ';';                                                                      \
    }                                                                                             \
    buf[__pos] = '\0';                                                                             \
  }

#define SIMPLE_TO_STRING_1                                                                    \
    if (OB_FAIL(ret)) {                                                                          \
    } else if (OB_FAIL(::oceanbase::common::logdata_print_key_obj(buf, buf_size - 1, __pos, key1, false, obj1))) { \
    } else if (__pos + 1 >= buf_size) {                                                          \
    } else {                                                                                     \
      buf[__pos++] = ',';                                                                       \
    }

#define SIMPLE_TO_STRING_2                                                                    \
    SIMPLE_TO_STRING_1                                                                        \
    if (OB_FAIL(ret)) {                                                                          \
    } else if (OB_FAIL(::oceanbase::common::logdata_print_key_obj(buf, buf_size - 1, __pos, key2, false, obj2))) { \
    } else if (__pos + 1 >= buf_size) {                                                          \
    } else {                                                                                     \
      buf[__pos++] = ',';                                                                       \
    }

#define SIMPLE_TO_STRING_3                                                                    \
    SIMPLE_TO_STRING_2                                                                        \
    if (OB_FAIL(ret)) {                                                                          \
    } else if (OB_FAIL(::oceanbase::common::logdata_print_key_obj(buf, buf_size - 1, __pos, key3, false, obj3))) { \
    } else if (__pos + 1 >= buf_size) {                                                          \
    } else {                                                                                     \
      buf[__pos++] = ',';                                                                       \
    }
#define SIMPLE_TO_STRING_4                                                                    \
    SIMPLE_TO_STRING_3                                                                        \
    if (OB_FAIL(ret)) {                                                                          \
    } else if (OB_FAIL(::oceanbase::common::logdata_print_key_obj(buf, buf_size - 1, __pos, key4, false, obj4))) { \
    } else if (__pos + 1 >= buf_size) {                                                          \
    } else {                                                                                     \
      buf[__pos++] = ',';                                                                       \
    }

#define SIMPLE_TO_STRING_5                                                                    \
    SIMPLE_TO_STRING_4                                                                        \
    if (OB_FAIL(ret)) {                                                                          \
    } else if (OB_FAIL(::oceanbase::common::logdata_print_key_obj(buf, buf_size - 1, __pos, key5, false, obj5))) { \
    } else if (__pos + 1 >= buf_size) {                                                          \
    } else {                                                                                     \
      buf[__pos++] = ',';                                                                       \
    }

DEFINE_SUSPECT_PRINT_KV(1)
DEFINE_SUSPECT_PRINT_KV(2)
DEFINE_SUSPECT_PRINT_KV(3)
DEFINE_SUSPECT_PRINT_KV(4)
DEFINE_SUSPECT_PRINT_KV(5)

DEFINE_DIAGNOSE_PRINT_KV(1)
DEFINE_DIAGNOSE_PRINT_KV(2)
DEFINE_DIAGNOSE_PRINT_KV(3)
DEFINE_DIAGNOSE_PRINT_KV(4)
DEFINE_DIAGNOSE_PRINT_KV(5)

DEFINE_COMPACITON_INFO_ADD_KV(1)
DEFINE_COMPACITON_INFO_ADD_KV(2)
DEFINE_COMPACITON_INFO_ADD_KV(3)
DEFINE_COMPACITON_INFO_ADD_KV(4)
DEFINE_COMPACITON_INFO_ADD_KV(5)

}//compaction
}//oceanbase

#endif
