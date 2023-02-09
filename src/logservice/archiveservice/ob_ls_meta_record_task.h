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

#ifndef OCEANBASE_ARCHIVE_OB_LS_META_RECORD_TASK_H_
#define OCEANBASE_ARCHIVE_OB_LS_META_RECORD_TASK_H_

#include <cstdint>
#include "share/backup/ob_archive_struct.h"
#include "share/ob_ls_id.h"                    // ObLSID

namespace oceanbase
{
namespace archive
{
// basic ls meta record class, single file no more than 2MB
class LSRecordTaskBase
{
public:
  explicit LSRecordTaskBase(const share::ObArchiveLSMetaType &type) : type_(type) {}
  virtual ~LSRecordTaskBase() {}
  // task record interval
  // NB: use ns as record interval
  virtual int64_t get_record_interval() = 0;

  // ls list to record
  virtual int get_ls_array(common::ObIArray<share::ObLSID> &array) = 0;

  bool is_valid() const { return type_.is_valid(); }

  const share::ObArchiveLSMetaType &get_type() const { return type_; }

  // NB: we have the limit of no more than 2MB in per write
  // @param[in] id, the ls id
  // @param[in] base_scn, the start_scn of archive
  // @char[in] data, the buffer to save data to persist
  // @param[in] data_size, the max size of data
  // @param[out] real_size, the real_size of data to persist
  // @param[out] scn, the scn corresponding to the data, for example the read version to read data
  virtual int get_data(const share::ObLSID &id,
      const share::SCN &base_scn,
      char *data,
      const int64_t data_size,
      int64_t &real_size,
      share::SCN &scn) = 0;

private:
  share::ObArchiveLSMetaType type_;

private:
  DISALLOW_COPY_AND_ASSIGN(LSRecordTaskBase);
};

// define RecordTask, class name ObArchiveLS##CLASS
#define DEFINE_LS_RECORD_TASK(CLASS) \
  class ObArchiveLS##CLASS : public oceanbase::archive::LSRecordTaskBase   \
  {    \
  public:   \
    explicit ObArchiveLS##CLASS(const share::ObArchiveLSMetaType &type) : LSRecordTaskBase(type) {}   \
    virtual int64_t get_record_interval(); \
    virtual int get_ls_array(common::ObIArray<share::ObLSID> &array); \
    virtual int get_data(const share::ObLSID &id, const share::SCN &base_scn, char *data, const int64_t data_size, int64_t &real_size, share::SCN &scn);  \
  private:    \
    DISALLOW_COPY_AND_ASSIGN(ObArchiveLS ## CLASS);    \
  };

// ========================= define ls record task scope begin ==================== //

DEFINE_LS_RECORD_TASK(SchemaMeta);

// ========================= define ls record task scope end ==================== //
} // namespace archive
} // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_LS_META_RECORD_TASK_H_ */
