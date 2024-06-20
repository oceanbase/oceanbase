/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
*/

#include "ob_data_dict_storager.h"

#include "logservice/ob_log_handler.h"        // ObLogHandler
#include "ob_data_dict_iterator.h"            // ObDataDictIterator

using namespace oceanbase::share;
using namespace oceanbase::logservice;

#define SERIALIZE_SCHEMA_TO_BUF \
    if (OB_ISNULL(schema)) { \
      ret = OB_ERR_UNEXPECTED; \
      DDLOG(WARN, "expect valid schema", KR(ret), KP(schema), K(idx), K(count)); \
    } else if (OB_FAIL(meta.init(*schema))) { \
      DDLOG(WARN, "init meta with schema failed", KR(ret), K(schema), K(meta)); \
    } else { \
      const int64_t meta_serialize_size = meta.get_serialize_size(); \
      header.set_dict_serialize_length(meta_serialize_size); \
      header.set_storage_type(ObDictMetaStorageType::FULL); \
      const int64_t total_serialize_size = header.get_serialize_size() + meta_serialize_size; \
      const int64_t expect_buf_len = pos + total_serialize_size; \
      if (OB_UNLIKELY(expect_buf_len > buf_len)) { \
        const int64_t block_cnt = (expect_buf_len / block_size) + 1; \
        const int64_t alloc_size = block_cnt * block_size; \
        buf = static_cast<char*>(allocator.realloc(buf, buf_len, alloc_size)); \
        buf_len = alloc_size; \
      } \
      if (OB_FAIL(ret)) { \
      } else if (OB_ISNULL(buf)) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        DDLOG(WARN, "expect valid buf", KR(ret), KP(buf), K(buf_len), K(expect_buf_len), K(pos)); \
      } else if (OB_FAIL(header.serialize(buf, buf_len, pos))) { \
        DDLOG(WARN, "serialize meta header failed", KR(ret), K(header), KP(buf), K(buf_len), K(pos)); \
      } else if (OB_FAIL(meta.serialize(buf, buf_len, pos))) { \
        DDLOG(WARN, "serialize meta failed", KR(ret), K(header), K(meta), KP(buf), K(buf_len), K(pos)); \
      } else { \
      } \
    }


namespace oceanbase
{
namespace datadict
{

const int64_t ObDataDictStorage::DEFAULT_PALF_BUF_SIZE = 2 * _M_;
const int64_t ObDataDictStorage::DEFAULT_DICT_BUF_SIZE = 4 * _M_;
const char *ObDataDictStorage::DEFAULT_DDL_MDS_MSG = "ddl_trans commit";
const int64_t ObDataDictStorage::DEFAULT_DDL_MDS_MSG_LEN = strlen(DEFAULT_DDL_MDS_MSG);

ObDataDictStorage::ObDataDictStorage(ObIAllocator &allocator)
  : tenant_id_(OB_INVALID_TENANT_ID),
    allocator_(allocator),
    snapshot_scn_(),
    start_lsn_(),
    end_lsn_(),
    log_handler_(NULL),
    log_base_header_(ObLogBaseType::DATA_DICT_LOG_BASE_TYPE, ObReplayBarrierType::NO_NEED_BARRIER),
    cb_queue_(),
    palf_buf_(NULL),
    dict_buf_(NULL),
    palf_buf_len_(DEFAULT_PALF_BUF_SIZE),
    dict_buf_len_(DEFAULT_DICT_BUF_SIZE),
    palf_pos_(0),
    dict_pos_(0),
    total_log_cnt_(0),
    total_dict_size_(0)
{
}

void ObDataDictStorage::reset()
{
  reuse();
  reset_buf_();
  tenant_id_ = OB_INVALID_TENANT_ID;
}

void ObDataDictStorage::reuse()
{
  snapshot_scn_.reset();
  start_lsn_.reset();
  end_lsn_.reset();
  log_handler_ = NULL;
  reset_cb_queue_();
  palf_pos_ = 0;
  dict_pos_ = 0;
  total_log_cnt_ = 0;
  total_dict_size_ = 0;
}

int ObDataDictStorage::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_NOT_NULL(palf_buf_) || OB_NOT_NULL(dict_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect palf_buf and dict_buf NULL", KR(ret), KP_(palf_buf), KP_(dict_buf));
  } else {
    tenant_id_ = tenant_id;
    DDLOG(INFO, "data_dict_storager init success", K_(tenant_id));
  }

  return ret;
}

int ObDataDictStorage::prepare(const share::SCN &snapshot_scn, ObLogHandler *log_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! snapshot_scn.is_valid())
      || OB_ISNULL(log_handler)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid log_handler", KR(ret), K_(tenant_id), K(snapshot_scn_));
  } else if (OB_FAIL(prepare_buf_())) {
    DDLOG(WARN, "prepare_buf_ failed", KR(ret));
  } else {
    reuse();
    snapshot_scn_ = snapshot_scn;
    log_handler_ = log_handler;
    DDLOG(INFO, "data_dict_storager prepare success", K_(tenant_id), K(snapshot_scn));
  }

  return ret;
}

template<class DATA_DICT_META>
int ObDataDictStorage::handle_dict_meta(
    const DATA_DICT_META &data_dict_meta,
    ObDictMetaHeader &header)
{
  int ret = OB_SUCCESS;
  const int64_t dict_serialize_size = data_dict_meta.get_serialize_size();
  // serialize_header
  header.set_snapshot_scn(snapshot_scn_);
  header.set_dict_serialize_length(dict_serialize_size);
  header.set_storage_type(ObDictMetaStorageType::FULL);
  const int64_t header_serialize_size = header.get_serialize_size();
  const int64_t total_serialize_size = dict_serialize_size
      + header_serialize_size
      + log_base_header_.get_serialize_size();

  if (! need_new_palf_buf_(total_serialize_size)) {
    if (OB_FAIL(serialize_to_palf_buf_(header, data_dict_meta))) {
      DDLOG(WARN, "serialize header_and_dict to palf_buf_ failed", KR(ret), K(header), K(data_dict_meta));
    }
  } else if (OB_FAIL(submit_to_palf_())) {
    DDLOG(WARN, "submit_data_dict_to_palf_ failed", KR(ret), K_(palf_buf_len), K_(palf_pos));
  } else if (! need_new_palf_buf_(total_serialize_size)) {
    // check if palf_buf_len is enough for header + data_dict.
    if (OB_FAIL(serialize_to_palf_buf_(header, data_dict_meta))) {
      DDLOG(WARN, "serialize header_and_dict to palf_buf_ failed", KR(ret), K(header), K(data_dict_meta));
    }
  } else if (OB_FAIL(prepare_dict_buf_(dict_serialize_size))) {
    DDLOG(WARN, "prepare_dict_buf_ failed", KR(ret), K(dict_serialize_size), K_(dict_buf_len), K_(dict_pos));
  } else if (OB_FAIL(data_dict_meta.serialize(dict_buf_, dict_buf_len_, dict_pos_))) {
    DDLOG(WARN, "serialize data_dict_meta to dict_buf failed", KR(ret),
        K(dict_serialize_size), K_(dict_buf_len), K_(dict_pos));
  } else if (OB_FAIL(segment_dict_buf_to_palf_(header))) {
    DDLOG(WARN, "segment_dict_buf_to_palf_ failed", KR(ret), K(header), K_(dict_buf_len), K_(dict_pos), K_(palf_pos));
  }

  if (OB_SUCC(ret)) {
    DDLOG(TRACE, "handle data_dict success", K(header), K(data_dict_meta));
  }

  return ret;
}

template int ObDataDictStorage::handle_dict_meta(const ObDictTenantMeta &data_dict_meta, ObDictMetaHeader &header);
template int ObDataDictStorage::handle_dict_meta(const ObDictDatabaseMeta &data_dict_meta, ObDictMetaHeader &header);
template int ObDataDictStorage::handle_dict_meta(const ObDictTableMeta &data_dict_meta, ObDictMetaHeader &header);

int ObDataDictStorage::finish(
    palf::LSN &start_lsn,
    palf::LSN &end_lsn,
    bool is_dump_success,
    bool &is_any_log_callback_fail,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const static int64_t WAIT_TIMEOUT_MS = 10;

  // try submit remian palf_buf if exist data not submit to palf.
  if (OB_FAIL(submit_to_palf_())) {
    DDLOG(WARN, "try submit remain palf_buf to palf failed", KR(ret));
  }

  if (OB_FAIL(wait_palf_callback_(is_any_log_callback_fail, stop_flag))) {
    DDLOG(WARN, "wait palf_callback failed", KR(ret), K(is_dump_success), K(is_any_log_callback_fail), K(stop_flag));
  } else if (is_dump_success && ! is_any_log_callback_fail) {
    if (OB_UNLIKELY(! start_lsn_.is_valid())
        || OB_UNLIKELY(! end_lsn_.is_valid())) {
      ret = OB_STATE_NOT_MATCH;
      DDLOG(WARN, "invalid start_lsn or end_lsn for data_dict_service", KR(ret),
          K_(tenant_id), K_(snapshot_scn), K(start_lsn), K(end_lsn));
    } else {
      start_lsn = start_lsn_;
      end_lsn = end_lsn_;
      DDLOG(INFO, "finish persist data_dict", K_(total_log_cnt), K_(total_dict_size));
    }
  }

  reset_buf_(); // reset palf_buf and dict_buf anyway.

  return ret;
}

int ObDataDictStorage::gen_and_serialize_dict_metas(
    ObIAllocator &allocator,
    const ObIArray<const ObTenantSchema*> &tenant_schemas,
    const ObIArray<const ObDatabaseSchema*> &database_schemas,
    const ObIArray<const ObTableSchema*> &table_schemas,
    char *&buf,
    int64_t &buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(buf)
      || OB_UNLIKELY(buf_len > 0)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "expect empty input buf", KR(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY((0 >= tenant_schemas.count())
      && (0 >= database_schemas.count())
      && (0 >= table_schemas.count()))) {
    DDLOG(INFO, "all schema_array is empty, use default msg", KCSTRING(DEFAULT_DDL_MDS_MSG));
    buf = static_cast<char*>(allocator.alloc(DEFAULT_DDL_MDS_MSG_LEN + 1)); // with '\0'

    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "expect valid buf", KR(ret), KP(buf), K(DEFAULT_DDL_MDS_MSG_LEN));
    } else {
      buf_len = DEFAULT_DDL_MDS_MSG_LEN + 1;
      pos = DEFAULT_DDL_MDS_MSG_LEN + 1;
      MEMCPY(buf, DEFAULT_DDL_MDS_MSG, DEFAULT_DDL_MDS_MSG_LEN);
      buf[DEFAULT_DDL_MDS_MSG_LEN] = '\0';
    }
  } else {
    const static int64_t block_size = 2 * _M_;
    buf_len = block_size; // init to 2M
    buf = static_cast<char*>(allocator.alloc(buf_len));

    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "expect valid buf", KR(ret), KP(buf), K(buf_len));
    } else {
      ARRAY_FOREACH_N(tenant_schemas, idx, count) {
        const ObTenantSchema *schema = tenant_schemas.at(idx);
        ObDictTenantMeta meta(&allocator);
        ObDictMetaHeader header(ObDictMetaType::TENANT_META);
        SERIALIZE_SCHEMA_TO_BUF;
      }

      ARRAY_FOREACH_N(database_schemas, idx, count) {
        const ObDatabaseSchema *schema = database_schemas.at(idx);
        ObDictDatabaseMeta meta(&allocator);
        ObDictMetaHeader header(ObDictMetaType::DATABASE_META);
        SERIALIZE_SCHEMA_TO_BUF;
      }

      ARRAY_FOREACH_N(table_schemas, idx, count) {
        const ObTableSchema *schema = table_schemas.at(idx);
        ObDictTableMeta meta(&allocator);
        ObDictMetaHeader header(ObDictMetaType::TABLE_META);
        SERIALIZE_SCHEMA_TO_BUF;
      }
    }
  }

  return ret;
}

int ObDataDictStorage::parse_dict_metas(
    ObIAllocator &allocator,
    const char* buf,
    const int64_t buf_len,
    const int64_t pos,
    ObIArray<const ObDictTenantMeta*> &tenant_metas,
    ObIArray<const ObDictDatabaseMeta*> &database_metas,
    ObIArray<const ObDictTableMeta*> &table_metas)
{
  int ret = OB_SUCCESS;
  ObDataDictIterator iterator;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len < pos)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid args", KR(ret), KP(buf), K(pos), K(buf_len));
  } else if (OB_FAIL(iterator.init(OB_SERVER_TENANT_ID))) {
    DDLOG(WARN, "iterator init failed", KR(ret), KP(buf), K(pos), K(buf_len));
  } else if (0 == strncmp(buf, DEFAULT_DDL_MDS_MSG, DEFAULT_DDL_MDS_MSG_LEN)) {
    // found DEFAULT_DDL_MDS_MSG, means ddl has no schema change.
    // buf == DEFAULT_DDL_MDS_MSG if in OB4.0.0.0 or DDL doesn't change schema.
    DDLOG(INFO, "detect default_ddl_msg", KR(ret), K(buf_len), KCSTRING(buf));
  } else if (OB_FAIL(iterator.append_log_buf(buf, buf_len, pos))) {
    DDLOG(WARN, "append_log_buf failed", KR(ret), KP(buf), K(pos), K(buf_len));
  } else {
    while (OB_SUCC(ret)) {
      ObDictMetaHeader header;
      if (OB_FAIL(iterator.next_dict_header(header))) {
        if (OB_ITER_END != ret) {
          DDLOG(WARN, "next_dict_header failed", KR(ret), KP(buf), K(buf_len), K(pos));
        }
      } else {
        if (header.get_dict_meta_type() == ObDictMetaType::TENANT_META) {
          ObDictTenantMeta *meta = static_cast<ObDictTenantMeta*>(allocator.alloc(sizeof(ObDictTenantMeta)));
          if (OB_ISNULL(meta)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            DDLOG(WARN, "alloc memroy for ObDictTenantMeta failed", KR(ret), KP(meta));
          } else {
            new (meta) ObDictTenantMeta(&allocator);

            if (OB_FAIL(iterator.next_dict_entry(header, *meta))) {
              DDLOG(WARN, "next_dict_entry for tenant_meta failed", KR(ret), K(header));
            } else if (OB_FAIL(tenant_metas.push_back(meta))) {
              DDLOG(WARN, "push_back tenant_meta failed", KR(ret), K(header), KPC(meta));
            }
          }
        } else if (header.get_dict_meta_type() == ObDictMetaType::DATABASE_META) {
          ObDictDatabaseMeta *meta = static_cast<ObDictDatabaseMeta*>(allocator.alloc(sizeof(ObDictDatabaseMeta)));
          if (OB_ISNULL(meta)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            DDLOG(WARN, "alloc memroy for ObDictDatabaseMeta failed", KR(ret), KP(meta));
          } else {
            new (meta) ObDictDatabaseMeta(&allocator);

            if (OB_FAIL(iterator.next_dict_entry(header, *meta))) {
              DDLOG(WARN, "next_dict_entry for database_meta failed", KR(ret), K(header));
            } else if (OB_FAIL(database_metas.push_back(meta))) {
              DDLOG(WARN, "push_back database_meta failed", KR(ret), K(header), KPC(meta));
            }
          }
        } else if (header.get_dict_meta_type() == ObDictMetaType::TABLE_META) {
          ObDictTableMeta *meta = static_cast<ObDictTableMeta*>(allocator.alloc(sizeof(ObDictTableMeta)));
          if (OB_ISNULL(meta)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            DDLOG(WARN, "alloc memroy for ObDictTableMeta failed", KR(ret), KP(meta));
          } else {
            new (meta) ObDictTableMeta(&allocator);

            if (OB_FAIL(iterator.next_dict_entry(header, *meta))) {
              DDLOG(WARN, "next_dict_entry for table_meta failed", KR(ret), K(header));
            } else if (OB_FAIL(table_metas.push_back(meta))) {
              DDLOG(WARN, "push_back table_meta failed", KR(ret), K(header), KPC(meta));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          DDLOG(WARN, "unknown data_dict_meta_type", KR(ret), K(header));
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObDataDictStorage::prepare_buf_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_ || ! is_user_tenant(tenant_id_))) {
    ret = OB_STATE_NOT_MATCH;
    DDLOG(WARN, "data_dict_service only work for user_tenant", KR(ret), K_(tenant_id));
  } else if (OB_NOT_NULL(palf_buf_) || OB_NOT_NULL(dict_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect invalid palf_buf and dict_buf before prepare dump data_dict", KR(ret),
        KP_(palf_buf), KP_(dict_buf));
  } else {
    palf_buf_len_ = DEFAULT_PALF_BUF_SIZE;
    dict_buf_len_ = DEFAULT_DICT_BUF_SIZE;
    palf_buf_ = static_cast<char*>(ob_dict_malloc(palf_buf_len_, tenant_id_));
    dict_buf_ = static_cast<char*>(ob_dict_malloc(dict_buf_len_ , tenant_id_));
  }

  return ret;
}

void ObDataDictStorage::reset_buf_()
{
  if (OB_NOT_NULL(palf_buf_)) {
    ob_dict_free(palf_buf_);
    palf_buf_len_ = 0;
    palf_buf_ = NULL;
  }
  if (OB_NOT_NULL(dict_buf_)) {
    ob_dict_free(dict_buf_);
    dict_buf_len_ = 0;
    dict_buf_ = NULL;
  }
}

int ObDataDictStorage::serialize_log_base_header_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(palf_buf_)
      || OB_UNLIKELY(palf_pos_ != 0)) {
    ret = OB_STATE_NOT_MATCH;
    DDLOG(WARN, "expect valid palf_buf and palf_pos", KR(ret), K_(palf_pos));
  } else if (OB_FAIL(log_base_header_.serialize(palf_buf_, palf_buf_len_, palf_pos_))) {
    DDLOG(WARN, "serialize log_base_header failed", KR(ret), K_(palf_pos), K_(log_base_header));
  }

  return ret;
}

int ObDataDictStorage::prepare_dict_buf_(const int64_t required_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(required_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid args", KR(ret), K(required_size));
  } else if (required_size <= dict_buf_len_) {
    // current dict_buf is enough.
    dict_pos_ = 0;
  } else {
    const static int64_t block_size = 2 * _M_;
    const int64_t block_cnt = (required_size / block_size) + 1;
    const int64_t alloc_size = block_size * block_cnt;

    ob_dict_free(dict_buf_);
    dict_buf_ = NULL;

    if (OB_ISNULL(dict_buf_ = static_cast<char*>(ob_dict_malloc(alloc_size, tenant_id_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "malloc data_dict_buf failed", KR(ret),
          K(alloc_size), K(required_size), K(block_cnt), K(block_size));
    } else {
      dict_buf_len_ = alloc_size;
      dict_pos_ = 0;
    }
  }

  return ret;
}

template<class DATA_DICT_META>
int ObDataDictStorage::serialize_to_palf_buf_(
    const ObDictMetaHeader &header,
    const DATA_DICT_META &data_dict)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(palf_buf_)) {
    ret = OB_STATE_NOT_MATCH;
    DDLOG(WARN, "palf_buf shoule be valid", KR(ret));
  } else if (palf_pos_ == 0) {
    if (OB_FAIL(serialize_log_base_header_())) {
      DDLOG(WARN, "serialize_log_base_header_ failed", KR(ret), K_(palf_pos));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(header.serialize(palf_buf_, palf_buf_len_, palf_pos_))) {
    DDLOG(WARN, "serialize header to palf_buf failed", KR(ret), K(header),
        K_(palf_buf_len), K_(palf_pos), "header_serialize_size", header.get_serialize_size());
  } else if (OB_FAIL(data_dict.serialize(palf_buf_, palf_buf_len_, palf_pos_))) {
    DDLOG(WARN, "serialize data_dict to palf_buf failed", KR(ret), K(header), K(data_dict),
        K_(palf_buf_len), K_(palf_pos), "dict_serialize_size", data_dict.get_serialize_size());
  } else {
    DDLOG(DEBUG, "serialize data_dict to palf_buf success", KR(ret), K(header), K(data_dict),
        K_(palf_buf_len), K_(palf_pos),
        "header_size", header.get_serialize_size(),
        "data_dict_size", data_dict.get_serialize_size());
  }

  return ret;
}

template int ObDataDictStorage::serialize_to_palf_buf_(const ObDictMetaHeader &header, const ObDictTenantMeta &data_dict);
template int ObDataDictStorage::serialize_to_palf_buf_(const ObDictMetaHeader &header, const ObDictDatabaseMeta &data_dict);
template int ObDataDictStorage::serialize_to_palf_buf_(const ObDictMetaHeader &header, const ObDictTableMeta &data_dict);

int ObDataDictStorage::segment_dict_buf_to_palf_(ObDictMetaHeader &header)
{
  int ret = OB_SUCCESS;
  const int64_t header_serialize_size = header.get_serialize_size();
  int64_t segment_pos = 0;

  while (OB_SUCC(ret) && segment_pos < dict_pos_) {
    if (OB_UNLIKELY(palf_pos_ > 0)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "palf_buf expect empty", KR(ret), K_(palf_buf_len), K_(palf_pos));
    } else if (OB_FAIL(serialize_log_base_header_())) {
      DDLOG(WARN, "serialize_log_base_header_ failed", KR(ret));
    } else {
      const int64_t palf_remain_size = palf_buf_len_ - palf_pos_ - header_serialize_size;
      const int64_t dict_remain_size = dict_pos_ - segment_pos; // dict not persist buf len
      const int64_t copy_size = std::min(palf_remain_size, dict_remain_size);

      if (0 == segment_pos) {
        header.set_storage_type(ObDictMetaStorageType::FIRST);
      } else if (dict_pos_ == (segment_pos + copy_size)) {
        header.set_storage_type(ObDictMetaStorageType::LAST);
      } else {
        header.set_storage_type(ObDictMetaStorageType::MIDDLE);
      }

      if (OB_UNLIKELY(palf_remain_size <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "expect valid palf_remain_size after serialize headers", KR(ret),
            K_(palf_pos), K(header_serialize_size), K_(palf_buf_len));
      } else if (OB_FAIL(header.serialize(palf_buf_, palf_buf_len_, palf_pos_))) {
        DDLOG(WARN, "serialize header to palf_buf failed", KR(ret), K(header), K_(palf_pos));
      } else {
        MEMCPY(
            palf_buf_ + palf_pos_,
            dict_buf_ + segment_pos,
            copy_size);
        palf_pos_ += copy_size;
        segment_pos += copy_size;

        // submit palf_buf to palf
        // expect all palf_buf except LAST should submit to palf.
        if ((ObDictMetaStorageType::LAST != header.get_storage_type())
            && OB_FAIL(submit_to_palf_())) {
          DDLOG(WARN, "submit_to_palf_ failed", KR(ret),
              K(header), K_(palf_pos), K_(dict_pos), K(copy_size), K(segment_pos));
        }
      }
    }
  }

  return ret;
}

int ObDataDictStorage::submit_to_palf_()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  ObDataDictPersistCallback *callback = NULL;
  const SCN &ref_scn = snapshot_scn_; // ns
  const bool need_nonblock = false; // TODO 是否需要non-block?
  const bool allow_compression = true;
  palf::LSN lsn;
  SCN submit_scn;

  if (OB_ISNULL(palf_buf_)
      || OB_ISNULL(log_handler_)
      || OB_UNLIKELY(palf_pos_ < 0)
      || OB_UNLIKELY(palf_buf_len_ < palf_pos_)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid args", KR(ret), K_(palf_buf_len), K_(palf_pos));
  } else if (OB_UNLIKELY(! log_handler_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "log_handler_ is not valid", KR(ret));
  } else if (OB_UNLIKELY(palf_pos_ == 0)) {
    DDLOG(INFO, "empty palf_buf, do nothing", K_(palf_buf_len), K_(palf_pos));
  } else if (OB_FAIL(check_ls_leader(log_handler_, is_leader))) {
    DDLOG(WARN, "check_ls_leader failed", KR(ret), K(is_leader));
  } else if (OB_UNLIKELY(! is_leader)) {
    ret = OB_STATE_NOT_MATCH;
    DDLOG(INFO, "do-nothing on non-leader logstream.", KR(ret), K(is_leader));
  } else if (OB_FAIL(alloc_palf_cb_(callback))) {
    DDLOG(WARN, "alloc_palf_cb_ failed", KR(ret));
  } else if (OB_FAIL(log_handler_->append(
      palf_buf_,
      palf_pos_,
      ref_scn,
      need_nonblock,
      allow_compression,
      callback,
      lsn,
      submit_scn
      ))){
    DDLOG(WARN, "append log to palf failed", KR(ret),
        K_(palf_buf), K_(palf_pos), K(ref_scn), K(need_nonblock), K(lsn), K(submit_scn));
  } else if (OB_FAIL(update_palf_lsn_(lsn))) {
    DDLOG(WARN, "update_palf_lsn_ failed", KR(ret), K(lsn), K_(start_lsn), K_(end_lsn));
  } else {
    cb_queue_.push(callback);
    // submit to palf success
    DDLOG(DEBUG, "submit palf_buf to palf succ", K(lsn), K(submit_scn), K_(palf_pos));
    total_log_cnt_++;
    total_dict_size_ += palf_pos_;
    palf_pos_ = 0;
  }

  return ret;
}

int ObDataDictStorage::alloc_palf_cb_(ObDataDictPersistCallback *&callback)
{
  int ret = OB_SUCCESS;
  const int64_t callback_size = sizeof(ObDataDictPersistCallback);
  void *buf = NULL;

  if (OB_ISNULL(buf = allocator_.alloc(callback_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    DDLOG(WARN, "alloc memory for data_dict_palf_callback failed", KR(ret), K(callback_size));
  } else {
    callback = new(buf) ObDataDictPersistCallback();
  }

  return ret;
}

int ObDataDictStorage::update_palf_lsn_(const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid LSN", KR(ret), K(lsn));
  } else if (! start_lsn_.is_valid()) {
    start_lsn_ = lsn;
    end_lsn_ = lsn;
  } else {
    end_lsn_ = lsn;
  }

  return ret;
}

// invoke after all data_dict dumped into palf.
int ObDataDictStorage::wait_palf_callback_(bool &is_any_cb_fail, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  static const int64_t CHECK_CB_INTERVAL = 10 * _MSEC_;
  static const int64_t PRINT_CB_STATUS_INTERVAL = 5 * _SEC_; // print callback status interval when callback not all invoked
  is_any_cb_fail = false;
  bool is_all_cb_invoked = true;
  bool print_cb_status = false;

  // exit loop if any of below cases occur: (1) check_callback_list_ failed; (2) all log_callback invoked.
  do {
    if (OB_FAIL(check_callback_list_(is_all_cb_invoked, is_any_cb_fail, print_cb_status, stop_flag))) {
      DDLOG(WARN, "check_callback_list_ failed", KR(ret));
    } else if (! is_all_cb_invoked) {
      if (REACH_TIME_INTERVAL_THREAD_LOCAL(PRINT_CB_STATUS_INTERVAL)) {
        print_cb_status = true;
      }
      usleep(CHECK_CB_INTERVAL);
    }
  } while (OB_SUCC(ret) && ! is_all_cb_invoked);

  return ret;
}

// invoke after all data_dict dumped into palf.
int ObDataDictStorage::check_callback_list_(
    bool &is_all_invoked,
    bool &has_cb_on_fail,
    bool &need_print_cb_status,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  is_all_invoked = true;
  has_cb_on_fail = false;
  QLink *item = cb_queue_.top();
  // for stat
  int64_t total_cb_count = 0;
  int64_t not_invoked_cb_count = 0;
  int64_t failed_cb_count = 0;

  while (OB_SUCC(ret) && OB_NOT_NULL(item)) {
    total_cb_count++;
    QLink *next = item->next_;
    ObDataDictPersistCallback *cb = static_cast<ObDataDictPersistCallback*>(item);

    if (OB_ISNULL(cb)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "convert ObLink to ObDataDictPersistCallback failed", KR(ret), K(item));
    } else {
      if (! cb->is_invoked()) {
        not_invoked_cb_count++;
      } else if (! cb->is_success()) {
        failed_cb_count++;
      }
    }

    item = next;
  }

  if (not_invoked_cb_count > 0) {
    is_all_invoked = false;
  }

  if (failed_cb_count > 0) {
    has_cb_on_fail = true;
  }

  if (is_all_invoked || need_print_cb_status) {
    // log callback status, NOTICE: stop_flag may set if ls role change or tenant stop.
    DDLOG(INFO, "[STAT] callbacks_status", KR(ret), K(total_cb_count), K(not_invoked_cb_count), K(failed_cb_count),
        K(is_all_invoked), K(need_print_cb_status), K(stop_flag));
  }

  if (need_print_cb_status) {
    need_print_cb_status = false;
  }

  return ret;
}

void ObDataDictStorage::reset_cb_queue_()
{
  int ret = OB_SUCCESS;
  while (! cb_queue_.empty()) {
    QLink *item = NULL;
    if (OB_ISNULL(item = cb_queue_.pop())) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "pop item from data_dict_meta persist_callback_queue failed", KR(ret));
    } else {
      allocator_.free(item);
      item = NULL;
    }
  }
}

} // namespace datadict
} // namespace oceanbase
