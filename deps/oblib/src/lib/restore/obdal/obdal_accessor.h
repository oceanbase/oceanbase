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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OBDAL_OBDAL_ACCESSOR_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OBDAL_OBDAL_ACCESSOR_H_
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_tracepoint.h"
#include <opendal.h>

namespace oceanbase
{
namespace common
{

class ObDalAccessor
{
public:
  static int init_env(void *malloc,
                      void *free,
                      void *log_handler,
                      const int32_t log_level,
                      const int64_t thread_cnt,
                      const int64_t pool_max_idle_per_host,
                      const int64_t pool_max_idle_time_s,
                      const int64_t connect_timeout_s);
  static void fin_env();

public:
  static int64_t obdal_get_tenant_id();
  // operator
  static int obdal_operator_options_new(opendal_operator_options *&options);
  static int obdal_operator_options_set(opendal_operator_options *options, const char *key, const char *value);
  static int obdal_operator_options_free(opendal_operator_options *&options);

  static int obdal_operator_new(const char *scheme, const opendal_operator_options *options, opendal_operator *&op);
  static int obdal_operator_free(opendal_operator *&op);
  static int obdal_operator_write(const opendal_operator *op, const char *path, const char *buf, const int64_t buf_size);
  static int obdal_operator_write_with_if_not_exists(const opendal_operator *op, const char *path, const char *buf, const int64_t buf_size);
  static int obdal_operator_reader(const opendal_operator *op, const char *path, opendal_reader *&reader);
  static int obdal_operator_writer(const opendal_operator *op, const char *path, opendal_writer *&writer);
  static int obdal_operator_append_writer(const opendal_operator *op, const char *path, opendal_writer *&writer);
  static int obdal_operator_multipart_writer(const opendal_operator *op, const char *path, opendal_multipart_writer *&writer);
  static int obdal_operator_delete(const opendal_operator *op, const char *path);
  static int obdal_operator_stat(const opendal_operator *op, const char *path, opendal_metadata *&meta);
  static int obdal_operator_list(const opendal_operator *op, const char *path, const int64_t limit, const bool recursive, const char *start_after, opendal_lister *&lister);
  static int obdal_operator_deleter(const opendal_operator *op, opendal_deleter *&deleter);

  static int obdal_operator_put_object_tagging(const opendal_operator *op, const char *path, const opendal_object_tagging *tagging);
  static int obdal_operator_get_object_tagging(const opendal_operator *op, const char *path, opendal_object_tagging *&tagging);

  // tagging
  static int obdal_object_tagging_new(opendal_object_tagging *&tagging);
  static int obdal_object_tagging_set(opendal_object_tagging *tagging, const char *key, const char *value);
  static int obdal_object_tagging_get(const opendal_object_tagging *tagging, const char *key, opendal_bytes &value);
  static int obdal_object_tagging_free(opendal_object_tagging *&tagging);

  // list
  static int obdal_lister_next(opendal_lister *lister, struct opendal_entry *&entry);
  static int obdal_lister_free(opendal_lister *&lister);

  // entry
  static int obdal_entry_path(const opendal_entry *entry, char *&path);
  static int obdal_entry_name(const opendal_entry *entry, char *&name);
  static int obdal_entry_metadata(const opendal_entry *entry, struct opendal_metadata *&meta);
  static int obdal_entry_free(opendal_entry *&entry);

  // metadata
  static int obdal_metadata_content_length(const opendal_metadata *metadata, int64_t &content_length);
  static int obdal_metadata_last_modified(const opendal_metadata *metadata, int64_t &last_modified_time_s);
  static int obdal_metadata_free(opendal_metadata *&metadata);

  // reader
  static int obdal_reader_read(opendal_reader *reader, char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size);
  static int obdal_reader_free(opendal_reader *&reader);

  // writer
  static int obdal_writer_write(opendal_writer *writer, const char *buf, const int64_t buf_size);
  static int obdal_writer_write_with_offset(opendal_writer *writer, const int64_t offset, const char *buf, const int64_t buf_size);
  static int obdal_writer_close(opendal_writer *writer);
  static int obdal_writer_abort(opendal_writer *writer);
  static int obdal_writer_free(opendal_writer *&writer);

  // multipart_writer
  static int obdal_multipart_writer_initiate(opendal_multipart_writer *writer);
  static int obdal_multipart_writer_write( opendal_multipart_writer *writer, const char *buf, const int64_t buf_size, const int64_t part_id);
  static int obdal_multipart_writer_close(opendal_multipart_writer *writer);
  static int obdal_multipart_writer_abort(opendal_multipart_writer *writer);
  static int obdal_multipart_writer_free(opendal_multipart_writer *&writer);

  // deleter
  static int obdal_deleter_delete(opendal_deleter *deleter, const char *path);
  static int obdal_deleter_deleted(opendal_deleter *deleter, const char *path, bool &deleted);
  static int obdal_deleter_flush(opendal_deleter *deleter, int64_t &deleted);
  static int obdal_deleter_free(opendal_deleter *&deleter);
  static int obdal_bytes_free(opendal_bytes *bytes);
  static int obdal_c_char_free(char *&c_char);
};


} // namespace common
} // namespace oceanbase

#endif
