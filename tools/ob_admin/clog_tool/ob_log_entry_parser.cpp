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

#include <string.h>
#include "ob_log_entry_parser.h"
#include "clog/ob_info_block_handler.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/ob_pg_log.h"
#include "share/ob_rpc_struct.h"
#include "clog/ob_log_compress.h"
#include "share/ob_encrypt_kms.h"
#include "share/ob_encryption_util.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace memtable;
using namespace storage;
namespace clog {

int ObLogStat::init()
{
  int ret = OB_SUCCESS;
  const int64_t HASH_BUCKET_NUM = 241;
  if (OB_FAIL(compressed_tenant_ids_.create(HASH_BUCKET_NUM))) {
    CLOG_LOG(ERROR, "failed to init");
  }
  return ret;
}

bool ObInfoEntryDumpFunctor::operator()(const ObPartitionKey& partition_key, const uint64_t min_log_id)
{
  int bool_ret = true;
  int ret = OB_SUCCESS;
  if ((!partition_key.is_valid()) || OB_INVALID_FILE_ID == file_id_) {
    ret = OB_INVALID_ARGUMENT;
    bool_ret = false;
    CLOG_LOG(ERROR, "ObInfoEntryDumpFunctor get invalid argument", K(ret), K(partition_key), K(file_id_));
  } else {
    fprintf(stdout,
        "||IndexInfoBlock|file_id:%lu|partition_key:%s|min_log_id:%ld|\n",
        file_id_,
        to_cstring(partition_key),
        min_log_id);
  }
  return bool_ret;
}

int ObLogEntryParser::init(uint64_t file_id, char* buf, int64_t buf_len, const ObLogEntryFilter& filter,
    const common::ObString& host, const int32_t port, const char* config_file, const bool is_ofs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(OB_INVALID_FILE_ID == file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid buf or buf_len", KP(buf), K(buf_len), K(file_id), K(ret));
  } else if (OB_ISNULL(compress_rbuf_.buf_ = static_cast<char*>(
                           allocator_.alloc_aligned(OB_MALLOC_BIG_BLOCK_SIZE, CLOG_DIO_ALIGN_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to allocate memory", K(ret));
  } else if (OB_FAIL(log_stat_.init())) {
    CLOG_LOG(WARN, "failed to init log_stat", K(file_id));
  } else if (OB_FAIL(ObLogEntryParserImpl::init(file_id, filter, host, port, config_file))) {
    CLOG_LOG(WARN, "failed to init ObLogEntryParserImpl");
  } else {
    is_inited_ = true;
    log_file_type_ = OB_UNKNOWN_FILE_TYPE;
    buf_cur_ = buf;
    buf_end_ = buf + buf_len;
    is_ofs_ = is_ofs;
    compress_rbuf_.buf_len_ = OB_MALLOC_BIG_BLOCK_SIZE;
  }
  return ret;
}

void ObLogEntryParser::advance_(const int64_t step)
{
  cur_offset_ += static_cast<offset_t>(step);
  buf_cur_ += step;
  CLOG_LOG(DEBUG, "advance", K(step));
}

int ObLogEntryParser::advance_to_next_align_()
{
  int ret = OB_SUCCESS;
  bool break_on_fail = true;
  CLOG_LOG(INFO, "DEBUG for break", K(cur_offset_));
  const char* env_val = getenv("CLOG_TOOL_BREAK_ON_FAIL");
  if (NULL != env_val) {
    break_on_fail = strcasecmp(env_val, "false") ? true : false;
  }
  if (break_on_fail) {
    ret = OB_ITER_END;
  } else {
    ObLogBlockMetaV2 block_meta;
    const int64_t step = SKIP_STEP - cur_offset_ % SKIP_STEP;
    if ((buf_end_ - buf_cur_) < step + block_meta.get_serialize_size()) {
      ret = OB_ITER_END;
    } else {
      advance_(step);
    }
  }
  return ret;
}

int ObLogEntryParser::dump_block_(const ObLogBlockMetaV2& meta)
{

  int ret = OB_SUCCESS;
  if (!meta.check_meta_checksum()) {
    // fatal error, data corrupt, but go on
    CLOG_LOG(ERROR, "check block meta checksum error", K(ret), K(meta));
    advance_(meta.get_serialize_size());
  } else {
    if (!filter_.is_valid()) {
      fprintf(stdout,
          "$$$file_id:%lu offset:%ld len:%ld  BlockMeta: %s|\n",
          file_id_,
          cur_offset_,
          meta.get_serialize_size(),
          to_cstring(meta));
    }
    ObBlockType type = meta.get_block_type();
    switch (type) {
      case OB_DATA_BLOCK:
        advance_(meta.get_serialize_size());
        break;
      case OB_HEADER_BLOCK:  // not used any more
        // file header is CLOG_DIO_ALIGN_SIZE bytes,
        // skip it
        advance_(meta.get_total_len());
        break;
      case OB_INFO_BLOCK: {
        if (OB_UNKNOWN_FILE_TYPE == log_file_type_) {
          log_file_type_ = OB_CLOG_FILE_TYPE;
        }
        if (0 == cur_offset_) {
          is_ofs_ = true;
        }
        advance_(meta.get_serialize_size());
        int64_t pos = 0;
        if (OB_CLOG_FILE_TYPE == log_file_type_) {
          ObCommitInfoBlockHandler commit_info_block_handler;
          if (OB_FAIL(commit_info_block_handler.init())) {
            CLOG_LOG(WARN, "failed to init clog handler", K(ret));
          } else if (OB_FAIL(commit_info_block_handler.resolve_info_block(buf_cur_, buf_end_ - buf_cur_, pos))) {
            CLOG_LOG(WARN, "failed to resolve_info_block in clog file", K(ret));
          } else {
            CLOG_LOG(INFO, "dump info block success", K(meta), K(is_ofs_));
            advance_(meta.get_data_len());
          }
        } else if (OB_ILOG_FILE_TYPE == log_file_type_) {
          ObIndexInfoBlockHandler index_info_block_handler;
          if (OB_FAIL(index_info_block_handler.init())) {
            CLOG_LOG(WARN, "failed to init clog handler", K(ret));
          } else if (OB_FAIL(index_info_block_handler.resolve_info_block(buf_cur_, buf_end_ - buf_cur_, pos))) {
            CLOG_LOG(WARN, "failed to resolve_info_block in ilog file", K(ret));
          } else {
            ObInfoEntryDumpFunctor fn(file_id_);
            ObIndexInfoBlockHandler::MinLogIdInfo min_log_id_info;
            if (OB_FAIL(index_info_block_handler.get_all_min_log_id_info(min_log_id_info))) {
              CLOG_LOG(WARN, "get_all_min_log_id_info failed", K(ret));
            } else if (OB_FAIL(min_log_id_info.for_each(fn))) {
              CLOG_LOG(ERROR, "hash_map parse fail: for_each", K(ret));
            } else {
              CLOG_LOG(INFO, "index log info block print complete", K(meta));
            }
          }
        } else {
          CLOG_LOG(ERROR, "invalid clog type while dump info block", K(log_file_type_));
        }
      }
        if (!is_ofs_) {
          ret = OB_ITER_END;  // info block is the end for local file
        }
        break;
      case OB_TRAILER_BLOCK:
        ret = OB_ITER_END;
        // iter will never encounter a trailer for InfoBlock lies ahead
        break;
      default:
        ret = OB_INVALID_DATA;
        CLOG_LOG(ERROR, "unknown block meta type", K(ret), K(meta), K(file_id_), K(cur_offset_));
    }
  }
  return ret;
}

// if return ILOG_ENTRY, that means ilog_entry prepared well
int ObLogEntryParser::get_type_(ObCLogItemType& type)
{
  int ret = OB_SUCCESS;
  type = UNKNOWN_TYPE;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log entry parser is not inited", K(ret));
  } else if (OB_ISNULL(buf_cur_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "buf cur is null", KP(buf_cur_), K(ret));
  } else if (0 == buf_end_ - buf_cur_) {
    CLOG_LOG(INFO, "reach end", K(cur_offset_));
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY((buf_end_ - buf_cur_) < 2)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "buf not enough", K_(cur_offset), K(ret));
  } else if (OB_FAIL(clog::parse_log_item_type(buf_cur_, buf_end_ - buf_cur_, type))) {
    CLOG_LOG(ERROR, "parse_log_item_type fail", K(ret), KP_(buf_cur), K_(buf_end), K(type));
  }

  return ret;
}

int ObLogEntryParser::dump_all_entry(bool is_hex)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "log entry parser is not inited", K(is_inited_), K(ret));
  } else {
    dump_hex_ = is_hex;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(parse_next_entry())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(WARN, "failed to parse_next_entry", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogEntryParser::format_dump_entry()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "log entry parser is not inited", K(is_inited_), K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(format_dump_next_entry())) {
        if (OB_ITER_END != ret)
          CLOG_LOG(WARN, "failed to format_dump_entry", K(ret));
      }
    }
  }
  return ret;
}

int ObLogEntryParser::stat_log()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "log entry parser is not inited", K(is_inited_), K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(stat_next_entry())) {
        if (OB_ITER_END != ret)
          CLOG_LOG(WARN, "failed to stat_log", K(ret));
      }
    }
  }
  return ret;
}

// return OB_EAGAIN: to prepare buffer and do get_next_entry_ again
int ObLogEntryParser::parse_next_entry()
{
  int ret = OB_SUCCESS;
  ObCLogItemType item_type = UNKNOWN_TYPE;
  if (OB_FAIL(get_type_(item_type)) && OB_ITER_END != ret) {
    CLOG_LOG(ERROR, "failed to get item type", K(ret));
  } else if (OB_ITER_END == ret) {
    // reach end
  } else if (ILOG_ENTRY == item_type) {
    if (OB_ILOG_FILE_TYPE != log_file_type_) {
      log_file_type_ = OB_ILOG_FILE_TYPE;
    }
    CLOG_LOG(DEBUG, "ilog entry magic", K(cur_offset_), K(item_type));
    int64_t pos = 0;
    ObIndexEntry entry;
    if (OB_FAIL(entry.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
      CLOG_LOG(ERROR,
          "ilog entry deserialize error, maybe corrupted",
          K(ret),
          K(item_type),
          KP(buf_cur_),
          KP(buf_end_),
          K(pos),
          K(cur_offset_),
          K(file_id_));
      // just go to next item
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else {
      advance_(pos);
      if (OB_FAIL(dump_ilog_entry(entry))) {
        CLOG_LOG(WARN, "failed to dump_ilog_entry", K(ret), K(cur_offset_), KP(buf_cur_), KP(buf_end_), K(entry));
        ret = OB_SUCCESS;  // skip this entry, go on to handle next entry
      }
    }
  } else if (CLOG_ENTRY == item_type) {
    if (OB_CLOG_FILE_TYPE != log_file_type_) {
      log_file_type_ = OB_CLOG_FILE_TYPE;
    }
    CLOG_LOG(DEBUG, "clog entry magic", K(cur_offset_), K(item_type));
    int64_t pos = 0;
    ObLogEntry entry;
    if (OB_FAIL(entry.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
      CLOG_LOG(ERROR,
          "clog entry deserialize error, maybe corrupted",
          K(ret),
          K(item_type),
          KP(buf_cur_),
          KP(buf_end_),
          K(pos),
          K(cur_offset_),
          K(file_id_));
      // just go to next item
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else {
      if (OB_FAIL(dump_clog_entry(entry, pos))) {
        // fatal error, data corrupt
        CLOG_LOG(WARN, "failed to dump_clog_entry", K(ret), K(cur_offset_), KP(buf_cur_), KP(buf_end_), K(entry));
        ret = OB_SUCCESS;  // skip this entry, go on to handle next entry
      }
      advance_(pos);
    }
  } else if (EOF_BUF == item_type) {
    CLOG_LOG(DEBUG, "eof magic", K(cur_offset_));
    // pointing to start of EOF
    ret = OB_ITER_END;
    CLOG_LOG(INFO, "reach eof", K(file_id_), K(cur_offset_));
  } else if (BLOCK_META == item_type) {
    CLOG_LOG(DEBUG, "block magic", K(cur_offset_));
    ObLogBlockMetaV2 meta;
    int64_t pos = 0;
    if (OB_FAIL(meta.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
      CLOG_LOG(ERROR,
          "log block meta deserialize error, maybe corrupted",
          K(ret),
          K(item_type),
          KP(buf_cur_),
          KP(buf_end_),
          K(pos),
          K(cur_offset_),
          K(file_id_));
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else if (OB_FAIL(dump_block_(meta))) {
      if (OB_ITER_END != ret) {
        CLOG_LOG(WARN, "dump block error", K(ret), K(meta), K(file_id_), K(cur_offset_), KP(buf_cur_), KP(buf_end_));
      }
    }
  } else if (PADDING_ENTRY == item_type) {
    CLOG_LOG(DEBUG, "padding entry magic", K(cur_offset_));
    // data written by new writer
    ObPaddingEntry pe;
    int64_t pos = 0;
    if (OB_FAIL(pe.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
      CLOG_LOG(ERROR, "padding entry deserialize error");
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else {
      if (!filter_.is_valid()) {
        fprintf(stdout, "Padding: %s|\n", to_cstring(pe));
      }
      // skip padding entry
      advance_(pe.get_entry_size());
    }
  } else if (CLOG_ENTRY_COMPRESSED_ZSTD == item_type || CLOG_ENTRY_COMPRESSED_LZ4 == item_type ||
             CLOG_ENTRY_COMPRESSED_ZSTD_138 == item_type) {
    int64_t local_pos = 0;
    int64_t uncompress_len = 0;
    int64_t pos = 0;
    ObLogEntry entry;
    if (OB_FAIL(clog::uncompress(
            buf_cur_, buf_end_ - buf_cur_, compress_rbuf_.buf_, compress_rbuf_.buf_len_, uncompress_len, pos))) {
      CLOG_LOG(ERROR, "failed to uncompress clog entry", K(ret), K(item_type));
    } else if (OB_FAIL(entry.deserialize(compress_rbuf_.buf_, uncompress_len, local_pos))) {
      CLOG_LOG(ERROR, "failed to deserialize clog entry", K(ret));
    }
    if (OB_FAIL(ret)) {
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else {
      if (OB_FAIL(dump_clog_entry(entry, pos))) {
        CLOG_LOG(WARN, "failed to dump_clog_entry", K(ret), K(entry));
        ret = OB_SUCCESS;
      }
      advance_(pos);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "error unexpected", K(ret), K(item_type));
  }
  return ret;
}

int ObLogEntryParserImpl::init(const int64_t file_id, const ObLogEntryFilter& filter, const common::ObString& host,
    const int32_t port, const char* config_file)
{
  int ret = OB_SUCCESS;
  UNUSED(config_file);
  if (OB_UNLIKELY(file_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(NULL == (print_buf_ = static_cast<char*>(allocator_.alloc(PRINT_BUF_SIZE))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    file_id_ = file_id;
    cur_offset_ = 0;
    filter_ = filter;
    is_inited_ = true;
  }
  if (!host.empty() && port > 0) {
    if (OB_SUCCESS != client_.init()) {
      CLOG_LOG(WARN, "failed to init net client", K(ret));
    } else if (OB_SUCCESS != client_.get_proxy(rpc_proxy_)) {
      CLOG_LOG(WARN, "failed to get_proxy", K(ret));
    } else {
      host_addr_.set_ip_addr(host, port);
      rpc_proxy_.set_server(host_addr_);
    }
  }
  return ret;
}

int ObLogEntryParser::format_dump_next_entry()
{
  int ret = OB_SUCCESS;
  ObCLogItemType item_type = UNKNOWN_TYPE;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "log entry parser is not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(get_type_(item_type)) && OB_ITER_END != ret) {
    CLOG_LOG(ERROR, "failed to get item type", K(ret));
  } else if (OB_ITER_END == ret) {
    // reach end
  } else if (ILOG_ENTRY == item_type) {
    ret = OB_ITER_END;
    CLOG_LOG(ERROR, "should be clog file rather than ilog file", K(ret));
  } else if (CLOG_ENTRY == item_type) {
    if (OB_CLOG_FILE_TYPE != log_file_type_) {
      log_file_type_ = OB_CLOG_FILE_TYPE;
    }
    CLOG_LOG(DEBUG, "clog entry magic", K(cur_offset_), K(item_type));
    int64_t pos = 0;
    ObLogEntry entry;
    if (OB_FAIL(entry.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
      CLOG_LOG(ERROR,
          "entry deserialize error, maybe corrupted",
          K(ret),
          K(item_type),
          KP(buf_cur_),
          KP(buf_end_),
          K(pos),
          K(cur_offset_),
          K(file_id_));
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else {
      if (OB_FAIL(format_dump_clog_entry(entry))) {
        // fatal error, data corrupt
        CLOG_LOG(ERROR, "failed to dump_clog_entry", K(ret), K(cur_offset_), KP(buf_cur_), KP(buf_end_), K(entry));
        ret = OB_SUCCESS;  // skip this entry, go on to handle next entry
      } else {             /*do nothing*/
      }
      advance_(pos);
    }
  } else if (EOF_BUF == item_type) {
    CLOG_LOG(DEBUG, "eof magic", K(cur_offset_));
    // pointing to start of EOF
    ret = OB_ITER_END;
    CLOG_LOG(INFO, "reach eof", K(file_id_), K(cur_offset_));  // FIXME
  } else if (BLOCK_META == item_type) {
    CLOG_LOG(DEBUG, "block magic", K(cur_offset_));
    ObLogBlockMetaV2 meta;
    int64_t pos = 0;
    if (OB_FAIL(meta.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
      CLOG_LOG(ERROR,
          "log block meta deserialize error, maybe corrupted",
          K(ret),
          K(item_type),
          KP(buf_cur_),
          KP(buf_end_),
          K(pos),
          K(cur_offset_),
          K(file_id_));
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else if (OB_FAIL(skip_block_(meta))) {
      if (OB_ITER_END != ret) {
        CLOG_LOG(WARN, "dump block error", K(ret), K(meta), K(file_id_), K(cur_offset_), KP(buf_cur_), KP(buf_end_));
      }
    }
  } else if (PADDING_ENTRY == item_type) {
    CLOG_LOG(DEBUG, "padding entry magic", K(cur_offset_));
    // data written by new writer
    ObPaddingEntry pe;
    int64_t pos = 0;
    if (OB_FAIL(pe.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
      CLOG_LOG(ERROR, "padding entry deserialize error");
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else {
      // skip padding entry
      advance_(pe.get_entry_size());
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "error unexpected", K(ret), K(item_type));
  }
  return ret;
}

int ObLogEntryParser::stat_next_entry()
{
  int ret = OB_SUCCESS;
  ObCLogItemType item_type = UNKNOWN_TYPE;
  if (OB_FAIL(get_type_(item_type)) && OB_ITER_END != ret) {
    CLOG_LOG(ERROR, "failed to get item type", K(ret));
  } else if (OB_ITER_END == ret) {
    // reach end
  } else if (ILOG_ENTRY == item_type) {
    ret = OB_ITER_END;
    // fprintf(stdout, "invalid argument is ilog file, stat only support clog file");
  } else if (CLOG_ENTRY == item_type) {
    if (OB_CLOG_FILE_TYPE != log_file_type_) {
      log_file_type_ = OB_CLOG_FILE_TYPE;
    }
    CLOG_LOG(DEBUG, "clog entry magic", K(cur_offset_), K(item_type));
    int64_t pos = 0;
    ObLogEntry entry;
    if (OB_FAIL(entry.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
      CLOG_LOG(ERROR,
          "entry deserialize error, maybe corrupted",
          K(ret),
          K(item_type),
          KP(buf_cur_),
          KP(buf_end_),
          K(pos),
          K(cur_offset_),
          K(file_id_));
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else {
      if (OB_FAIL(stat_clog_entry(entry, pos, false /*is_compressed=false*/))) {
        CLOG_LOG(ERROR, "fail to stat_clog_entry", K(ret));
      }
      advance_(pos);
    }
  } else if (EOF_BUF == item_type) {
    CLOG_LOG(DEBUG, "eof magic", K(cur_offset_));
    // pointing to start of EOF
    ret = OB_ITER_END;
    CLOG_LOG(INFO, "reach eof", K(file_id_), K(cur_offset_), K(ret));
  } else if (BLOCK_META == item_type) {
    CLOG_LOG(DEBUG, "block magic", K(cur_offset_));
    ObLogBlockMetaV2 meta;
    int64_t pos = 0;
    if (OB_FAIL(meta.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
      CLOG_LOG(ERROR,
          "log block meta deserialize error, maybe corrupted",
          K(ret),
          K(item_type),
          KP(buf_cur_),
          KP(buf_end_),
          K(pos),
          K(cur_offset_),
          K(file_id_));
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else if (OB_FAIL(stat_block_(meta))) {
      if (OB_ITER_END != ret) {
        CLOG_LOG(WARN, "dump block error", K(ret), K(meta), K(file_id_), K(cur_offset_), KP(buf_cur_), KP(buf_end_));
      }
    }
  } else if (PADDING_ENTRY == item_type) {
    CLOG_LOG(DEBUG, "padding entry magic", K(cur_offset_));
    // data written by new writer
    ObPaddingEntry pe;
    int64_t pos = 0;
    if (OB_FAIL(pe.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
      CLOG_LOG(ERROR, "padding entry deserialize error");
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else {
      // skip padding entry
      advance_(pe.get_entry_size());
      log_stat_.padding_size_ += pe.get_entry_size();
    }
  } else if (CLOG_ENTRY_COMPRESSED_ZSTD == item_type || CLOG_ENTRY_COMPRESSED_LZ4 == item_type ||
             CLOG_ENTRY_COMPRESSED_ZSTD_138 == item_type) {
    int64_t local_pos = 0;
    int64_t uncompress_len = 0;
    int64_t pos = 0;
    ObLogEntry entry;
    if (OB_FAIL(clog::uncompress(
            buf_cur_, buf_end_ - buf_cur_, compress_rbuf_.buf_, compress_rbuf_.buf_len_, uncompress_len, pos))) {
      CLOG_LOG(ERROR, "failed to uncompress clog entry", K(ret), K(item_type));
    } else if (OB_FAIL(entry.deserialize(compress_rbuf_.buf_, uncompress_len, local_pos))) {
      CLOG_LOG(ERROR, "failed to deserialize clog entry", K(ret));
    }
    if (OB_FAIL(ret)) {
      if (OB_FAIL(advance_to_next_align_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(ERROR, "unexpected error", K(ret));
        }
      }
    } else {
      if (OB_FAIL(stat_clog_entry(entry, pos, true /*is_compressed=true*/))) {
        CLOG_LOG(WARN, "failed to stat_clog_entry", K(ret), K(entry));
        ret = OB_SUCCESS;
      }
      advance_(pos);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "error unexpected", K(ret), K(item_type));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_memtable_mutator(const char* buf, int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMemtableMutatorIterator mmi;
  ObMemtableMutatorMeta meta;
  int64_t meta_pos = 0;
  if (OB_FAIL(meta.deserialize(buf, len, meta_pos))) {
    CLOG_LOG(ERROR, "meta.deserialize fail", K(ret));
  } else if (ObTransRowFlag::is_normal_row(meta.get_flags())) {
    if (OB_FAIL(mmi.deserialize(buf, len, pos))) {
      CLOG_LOG(ERROR, "mmi.deserialize fail", K(ret));
    } else {
      fprintf(stdout, " %s", to_cstring(mmi.get_meta()));
    }

    while (OB_SUCCESS == ret) {
      ObMemtableMutatorRow row;
      if (OB_FAIL(mmi.get_next_row(row)) && OB_ITER_END != ret) {
        CLOG_LOG(ERROR, "mmi.get_next_row fail", K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        if (dump_hex_) {
          int64_t pos = row.rowkey_.to_smart_string(print_buf_, PRINT_BUF_SIZE - 1);
          print_buf_[pos] = '\0';
          fprintf(stdout, "  MutatorRow={%s} HexedRowkey={%s} | OLD_ROW={", to_cstring(row), print_buf_);
        } else {
          fprintf(stdout, "  MutatorRow={%s} | OLD_ROW={", to_cstring(row));
        }
        ObCellReader new_cci;
        ObCellReader old_cci;
        if (NULL != row.old_row_.data_ && row.old_row_.size_ > 0) {
          if (OB_FAIL(old_cci.init(row.old_row_.data_, row.old_row_.size_, SPARSE))) {
            CLOG_LOG(ERROR, "old cci.init fail", K(ret));
          }
          while (OB_SUCC(ret) && OB_SUCC(old_cci.next_cell())) {
            uint64_t column_id = OB_INVALID_ID;
            const ObObj* value = NULL;
            if (OB_FAIL(old_cci.get_cell(column_id, value))) {
              CLOG_LOG(ERROR, "failed to get cell", K(ret));
            } else if (NULL == value) {
              ret = OB_ERR_UNEXPECTED;
              CLOG_LOG(ERROR, "got value is NULL", K(ret));
            } else if (ObExtendType == value->get_type() && ObActionFlag::OP_END_FLAG == value->get_ext()) {
              ret = OB_ITER_END;
            } else if (OB_FAIL(dump_obj(*value, column_id))) {
              CLOG_LOG(ERROR, "failed to dump obj", K(*value), K(ret));
            } else { /*do nothing*/
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        } else if (NULL == row.old_row_.data_ && 0 == row.old_row_.size_) {
          // no data of old row
        } else {
          CLOG_LOG(ERROR, "old cci.init fail", K(row), K(ret));
        }

        fprintf(stdout, "} | NEW_ROW={");
        if (OB_SUCC(ret) && OB_FAIL(new_cci.init(row.new_row_.data_, row.new_row_.size_, SPARSE))) {
          CLOG_LOG(ERROR, "new cci.init fail", K(ret));
        } else {
          while (OB_SUCC(ret) && OB_SUCC(new_cci.next_cell())) {
            uint64_t column_id = OB_INVALID_ID;
            const ObObj* value = NULL;
            if (OB_FAIL(new_cci.get_cell(column_id, value))) {
            } else if (NULL == value) {
              ret = OB_ERR_UNEXPECTED;
              CLOG_LOG(ERROR, "value is NULL", K(ret));
            } else if (ObExtendType == value->get_type() && ObActionFlag::OP_END_FLAG == value->get_ext()) {
              ret = OB_ITER_END;
            } else if (OB_FAIL(dump_obj(*value, column_id))) {
              CLOG_LOG(ERROR, "failed to dump obj", K(*value), K(ret));
            } else { /*do nothing*/
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
        fprintf(stdout, "}");
      }
    }
  } else if (ObTransRowFlag::is_big_row_start(meta.get_flags())) {
    fprintf(stdout, "BIG_ROW_START: %s", to_cstring(meta));
  } else if (ObTransRowFlag::is_big_row_mid(meta.get_flags())) {
    fprintf(stdout, "BIG_ROW_MID: %s", to_cstring(meta));
  } else if (ObTransRowFlag::is_big_row_end(meta.get_flags())) {
    fprintf(stdout, "BIG_ROW_END: %s", to_cstring(meta));
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObLogEntryParserImpl::dump_sp_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObSpTransRedoLogHelper helper;
  ObSpTransRedoLog log(helper);
  int64_t pos = 0;
  if (OB_FAIL(log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(log.decrypt_table_key())) {
    fprintf(stdout, " %s|||", to_cstring(log));
    dump_memtable_mutator(log.get_mutator().get_data(), log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransRedoLogHelper helper;
  ObTransRedoLog log(helper);
  int64_t pos = 0;
  if (OB_FAIL(log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(log.decrypt_table_key())) {
    fprintf(stdout, " %s|||", to_cstring(log));
    dump_memtable_mutator(log.get_mutator().get_data(), log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_prepare_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObTransPrepareLogHelper helper;
  ObTransPrepareLog log(helper);
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "prepare log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_sp_trans_commit_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObSpTransCommitLogHelper helper;
  ObSpTransCommitLog log(helper);
  int64_t pos = 0;
  // an sp commit log may contain redo data
  if (OB_FAIL(log.init_for_deserialize())) {
    CLOG_LOG(WARN, "sp redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(log.decrypt_table_key())) {
    fprintf(stdout, " %s|||", to_cstring(log));
    dump_memtable_mutator(log.get_mutator().get_data(), log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "commit log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_commit_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  PartitionLogInfoArray partition_log_info_array;
  ObTransCommitLog log(partition_log_info_array);
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "commit log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_sp_trans_abort_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObSpTransAbortLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "sp abort log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_abort_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObTransAbortLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "commit log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_clear_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObTransClearLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "clear log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_prepare_commit_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  ObTransPrepareLogHelper helper;
  ObTransPrepareLog p_log(helper);
  PartitionLogInfoArray partition_log_info_array;
  ObTransCommitLog c_log(partition_log_info_array);
  if (OB_FAIL(p_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "prepare log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else if (OB_FAIL(c_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "commit log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(p_log));
    fprintf(stdout, " %s|||", to_cstring(c_log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_redo_prepare_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransRedoLogHelper helper;
  ObTransRedoLog r_log(helper);
  int64_t pos = 0;
  if (OB_FAIL(r_log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(r_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(r_log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(r_log.decrypt_table_key()) || ret == OB_EAGAIN) {
    if (OB_SUCC(ret)) {
      fprintf(stdout, " %s|||", to_cstring(r_log));
      dump_memtable_mutator(r_log.get_mutator().get_data(), r_log.get_mutator().get_position());
    } else {
      // impossible branch
    }
    ObTransPrepareLogHelper helper;
    ObTransPrepareLog p_log(helper);
    if (OB_FAIL(p_log.deserialize(data, len, pos))) {
      CLOG_LOG(WARN, "prepare log deserialize error", K(ret), KP(data), K(len), K(pos));
    } else {
      fprintf(stdout, " %s|||", to_cstring(p_log));
    }
  } else {
    CLOG_LOG(WARN, "redo log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_redo_prepare_commit_log(
    const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTransRedoLogHelper helper;
  ObTransRedoLog r_log(helper);
  if (OB_FAIL(r_log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(r_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserializde log", K(ret));
  } else if (OB_FAIL(r_log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(r_log.decrypt_table_key()) || ret == OB_EAGAIN) {
    if (OB_SUCC(ret)) {
      fprintf(stdout, " %s|||", to_cstring(r_log));
      dump_memtable_mutator(r_log.get_mutator().get_data(), r_log.get_mutator().get_position());
    } else {
      // impossible branch
    }
    ObTransPrepareLogHelper helper;
    ObTransPrepareLog p_log(helper);
    PartitionLogInfoArray partition_log_info_array;
    ObTransCommitLog c_log(partition_log_info_array);
    if (OB_FAIL(p_log.deserialize(data, len, pos))) {
      CLOG_LOG(WARN, "prepare log deserialize error", K(ret), KP(data), K(len), K(pos));
    } else if (OB_FAIL(c_log.deserialize(data, len, pos))) {
      CLOG_LOG(WARN, "commit log deserialize error", K(ret), KP(data), K(len), K(pos));
    } else {
      fprintf(stdout, " %s|||", to_cstring(p_log));
      fprintf(stdout, " %s|||", to_cstring(c_log));
    }
  } else {
    CLOG_LOG(WARN, "redo log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_prepare_commit_clear_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTransPrepareLogHelper helper;
  ObTransPrepareLog p_log(helper);
  PartitionLogInfoArray partition_log_info_array;
  ObTransCommitLog c_log(partition_log_info_array);
  ObTransClearLog cl_log;
  if (OB_FAIL(p_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "prepare log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else if (OB_FAIL(c_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "commit log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else if (OB_FAIL(cl_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "clear log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(p_log));
    fprintf(stdout, " %s|||", to_cstring(c_log));
    fprintf(stdout, " %s|||", to_cstring(cl_log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_redo_prepare_commit_clear_log(
    const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTransRedoLogHelper helper;
  ObTransRedoLog r_log(helper);
  if (OB_FAIL(r_log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(r_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(r_log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(r_log.decrypt_table_key()) || ret == OB_EAGAIN) {
    if (OB_SUCC(ret)) {
      fprintf(stdout, " %s|||", to_cstring(r_log));
      dump_memtable_mutator(r_log.get_mutator().get_data(), r_log.get_mutator().get_position());
    } else {
      // impossible branch
    }
    ObTransPrepareLogHelper helper;
    ObTransPrepareLog p_log(helper);
    PartitionLogInfoArray partition_log_info_array;
    ObTransCommitLog c_log(partition_log_info_array);
    ObTransClearLog cl_log;
    if (OB_FAIL(p_log.deserialize(data, len, pos))) {
      CLOG_LOG(WARN, "prepare log deserialize error", K(ret), KP(data), K(len), K(pos));
    } else if (OB_FAIL(c_log.deserialize(data, len, pos))) {
      CLOG_LOG(WARN, "commit log deserialize error", K(ret), KP(data), K(len), K(pos));
    } else if (OB_FAIL(cl_log.deserialize(data, len, pos))) {
      CLOG_LOG(WARN, "clear log deserialize error", K(ret), KP(data), K(len), K(pos));
    } else {
      fprintf(stdout, " %s|||", to_cstring(p_log));
      fprintf(stdout, " %s|||", to_cstring(c_log));
      fprintf(stdout, " %s|||", to_cstring(cl_log));
    }
  } else {
    CLOG_LOG(WARN, "redo log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_mutator_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransMutatorLogHelper helper;
  ObTransMutatorLog log(helper);
  int64_t pos = 0;
  if (OB_FAIL(log.init_for_deserialize())) {
    CLOG_LOG(WARN, "trans mutator log init for deserialize fail", K(ret));
  } else if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(log.decrypt_table_key())) {
    fprintf(stdout, " %s|||", to_cstring(log));
    dump_memtable_mutator(log.get_mutator().get_data(), log.get_mutator().get_position());
  } else {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_mutator_abort_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObTransMutatorAbortLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_state_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObTransStateLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_mutator_state_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransMutatorLogHelper helper;
  ObTransMutatorLog m_log(helper);
  int64_t pos = 0;
  if (OB_FAIL(m_log.init_for_deserialize())) {
    CLOG_LOG(WARN, "trans mutator log init for deserialize fail", K(ret));
  } else if (OB_FAIL(m_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(m_log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(m_log.decrypt_table_key()) || ret == OB_EAGAIN) {
    if (OB_SUCC(ret)) {
      fprintf(stdout, " %s|||", to_cstring(m_log));
      dump_memtable_mutator(m_log.get_mutator().get_data(), m_log.get_mutator().get_position());
    } else {
      // impossible branch
    }
    ObTransStateLog s_log;
    if (OB_FAIL(s_log.deserialize(data, len, pos))) {
      CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
    } else {
      fprintf(stdout, " %s|||", to_cstring(s_log));
    }
  } else {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_part_split_src_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObPartitionSplitSourceLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_part_split_dest_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObPartitionSplitDestLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_checkpoint_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObCheckpointLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_partition_schema_version_change_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObPGSchemaChangeLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "ObPGSchemaChangeLog deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " OB_LOG_PARTITION_SCHEMA_VERSION_CHANGE %s|||", to_cstring(log));
  }
  return ret;
}
int ObLogEntryParserImpl::dump_new_offline_partition_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObOfflinePartitionLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "ObOfflinePartitionLog log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " OB_LOG_OFFLINE_PARTITION_LOG_V2 %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_add_partition_to_pg_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObAddPartitionToPGLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "ObAddPartitionToPGLog log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " OB_LOG_ADD_PARTITION_TO_PG %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_remove_partition_from_pg_log(const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  ObRemovePartitionFromPGLog log;
  int64_t pos = 0;
  if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "ObRemovePartitionFroPGLog log deserialize error", K(ret), KP(data), K(len), K(pos));
  } else {
    fprintf(stdout, " OB_LOG_REMOVE_PARTITION_FROM_PG %s|||", to_cstring(log));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_clog_entry(ObLogEntry& entry, int64_t pos)
{
  int ret = OB_SUCCESS;
  bool need_print = false;
  ObLogEntry* entry_ptr = &entry;
  ObLogEntry decrypted_entry;
  if (!entry.check_integrity(true /*ignore_batch_commited*/)) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(ERROR, "entry.check_integrity() fail", K(entry), K(ret));
  } else if (OB_FAIL(check_filter(entry, need_print))) {
    CLOG_LOG(ERROR, "check filter failed", K(ret), K(entry));
  } else if (!need_print) {
    CLOG_LOG(DEBUG, "ignore log", K(entry));
  } else {
    if (OB_SUCC(ret)) {
      ret = dump_clog_entry_(*entry_ptr, pos);
    }
  }
  return ret;
}

int ObLogEntryParserImpl::dump_clog_entry_(ObLogEntry& entry, int64_t pos)
{
  int ret = OB_SUCCESS;
  clog::ObLogType log_type = entry.get_header().get_log_type();
  fprintf(stdout,
      "$$$file_id:%lu offset:%ld len:%ld  %s|log_id:%lu|%s |||",
      file_id_,
      cur_offset_,
      pos,
      get_log_type(log_type),
      entry.get_header().get_log_id(),
      to_cstring(entry));
  if (OB_LOG_MEMBERSHIP == log_type) {
    int64_t pos = 0;
    ObMembershipLog member_log;
    if (OB_FAIL(member_log.deserialize(entry.get_buf(), entry.get_header().get_data_len(), pos))) {
      CLOG_LOG(ERROR, "failed to deserialize member_log", K(entry), K(ret));
    } else {
      fprintf(stdout, "||ObMembershipLog:%s", to_cstring(member_log));
    }
  } else if (OB_LOG_RENEW_MEMBERSHIP == log_type) {
    int64_t pos = 0;
    ObRenewMembershipLog renew_ms_log;
    if (OB_FAIL(renew_ms_log.deserialize(entry.get_buf(), entry.get_header().get_data_len(), pos))) {
      CLOG_LOG(ERROR, "failed to deserialize ObRenewMembershipLog", K(entry), K(ret));
    } else {
      fprintf(stdout, "||ObRenewMembershipLog:%s", to_cstring(renew_ms_log));
    }
  } else if (OB_LOG_SUBMIT == log_type) {
    int64_t trans_inc = -1;
    int64_t submit_log_type_tmp = -1;
    const char* buf = entry.get_buf();
    int64_t buf_len = entry.get_header().get_data_len();
    int64_t pos = 0;
    if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &submit_log_type_tmp))) {
      CLOG_LOG(ERROR, "failed to decode submit_log_type", K(entry), K(ret));
    } else {
      ObStorageLogType submit_log_type = static_cast<ObStorageLogType>(submit_log_type_tmp);
      const uint64_t real_tenant_id = entry.get_header().get_partition_key().get_tenant_id();
      if (ObStorageLogTypeChecker::is_trans_log(submit_log_type)) {
        if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &trans_inc))) {
          CLOG_LOG(ERROR, "failed to decode trans_inc", K(ret), KP(buf), K(pos), K(buf_len));
        } else if (OB_FAIL(dump_trans_log(submit_log_type, trans_inc, real_tenant_id, buf, buf_len, pos))) {
          CLOG_LOG(ERROR, "failed to dump_trans_log", K(ret), KP(buf), K(pos), K(buf_len));
        }
      } else if (submit_log_type == OB_LOG_MAJOR_FREEZE) {
        fprintf(stdout, "\t|||freeze log type:%s ", get_submit_log_type(submit_log_type));
        ObStorageLogType log_type = storage::OB_LOG_UNKNOWN;
        ObFreezeType freeze_type = INVALID_FREEZE;
        ObPartitionKey partition_key;
        ObSavedStorageInfo info;
        int64_t frozen_version = -1;
        if (OB_FAIL(dump_freeze_log(buf, buf_len, log_type, freeze_type, partition_key, frozen_version, info))) {
          CLOG_LOG(ERROR, "failed to dump freeze log", K(ret));
        } else {
          fprintf(stdout,
              "|FreezeType:%s|frozen_version:%ld|frozen_timestamp:%s|schema_version:%ld|saved_storage_info:%s |||",
              get_freeze_type(freeze_type),
              frozen_version,
              time2str(info.get_frozen_timestamp()),
              info.get_schema_version(),
              to_cstring(info));
        }
      } else if (OB_LOG_OFFLINE_PARTITION == submit_log_type) {
        fprintf(stdout, "|| OB_LOG_OFFLINE_PARTITION");
      } else if (OB_LOG_OFFLINE_PARTITION_V2 == submit_log_type) {
        if (OB_FAIL(dump_new_offline_partition_log(buf + pos, buf_len - pos))) {
          CLOG_LOG(WARN, "dump_new_offline_partition_log fail", K(ret), K(buf), K(pos), K(buf_len));
        }
      } else if (OB_PARTITION_SCHEMA_VERSION_CHANGE_LOG == submit_log_type) {
        if (OB_FAIL(dump_partition_schema_version_change_log(buf + pos, buf_len - pos))) {
          CLOG_LOG(WARN, "dump partition schema change log fail", K(ret), K(buf), K(pos), K(buf_len));
        }
      } else if (OB_LOG_ADD_PARTITION_TO_PG == submit_log_type) {
        if (OB_FAIL(dump_add_partition_to_pg_log(buf + pos, buf_len - pos))) {
          CLOG_LOG(WARN, "dump_add_partition_to_pg_log fail", K(ret), K(buf), K(pos), K(buf_len));
        }
      } else if (OB_LOG_REMOVE_PARTITION_FROM_PG == submit_log_type) {
        if (OB_FAIL(dump_remove_partition_from_pg_log(buf + pos, buf_len - pos))) {
          CLOG_LOG(WARN, "dump_remove_partition_from_pg_log fail", K(ret), K(buf), K(pos), K(buf_len));
        }
      } else if (ObStorageLogTypeChecker::is_split_log(submit_log_type)) {
        switch (submit_log_type) {
          case OB_LOG_SPLIT_SOURCE_PARTITION: {
            if (OB_FAIL(dump_part_split_src_log(buf + pos, buf_len - pos))) {
              CLOG_LOG(ERROR, "dump_part_split_src_log fail", K(ret), K(buf), K(pos), K(buf_len));
            }
            break;
          }
          case OB_LOG_SPLIT_DEST_PARTITION: {
            if (OB_FAIL(dump_part_split_dest_log(buf + pos, buf_len - pos))) {
              CLOG_LOG(ERROR, "dump_part_split_dest_log fail", K(ret), K(buf), K(pos), K(buf_len));
            }
            break;
          }
          default: {
            CLOG_LOG(ERROR, "unknown log type", K(submit_log_type));
            break;
          }
        }
      } else if (submit_log_type == OB_LOG_TRANS_CHECKPOINT) {
        if (OB_FAIL(dump_trans_checkpoint_log(buf + pos, buf_len - pos))) {
          CLOG_LOG(ERROR, "dump_trans_checkpoint_log fail", K(ret), K(buf), K(pos), K(buf_len));
        }
      } else { /*todo may be other logs need be printed?*/
      }
    }
  } else if (OB_LOG_AGGRE == log_type) {
    const char* log_buf = entry.get_buf();
    int64_t buf_len = entry.get_header().get_data_len();
    int32_t next_log_offset = 0;
    const uint64_t real_tenant_id = entry.get_header().get_partition_key().get_tenant_id();
    while (OB_SUCC(ret) && next_log_offset < buf_len) {
      int64_t saved_pos = next_log_offset;
      int64_t pos = next_log_offset;
      int64_t log_type_in_buf = storage::OB_LOG_UNKNOWN;
      ObStorageLogType log_type = storage::OB_LOG_UNKNOWN;
      int64_t trans_inc = 0;
      int64_t submit_timestamp = 0;
      if (OB_FAIL(serialization::decode_i32(log_buf, buf_len, pos, &next_log_offset))) {
        REPLAY_LOG(ERROR, "serialization decode_i32 failed", K(ret));
      } else if (OB_FAIL(serialization::decode_i64(log_buf, buf_len, pos, &submit_timestamp))) {
        REPLAY_LOG(ERROR, "serialization::decode_i64 failed", K(ret));
      } else if (OB_FAIL(serialization::decode_i64(log_buf, buf_len, pos, &log_type_in_buf))) {
        REPLAY_LOG(ERROR, "serialization::decode_i64 failed", K(ret));
      } else if (OB_FAIL(serialization::decode_i64(log_buf, buf_len, pos, &trans_inc))) {
        REPLAY_LOG(ERROR, "serialization::decode_i64 failed", K(ret));
      } else {
        const int64_t buf_len = next_log_offset - saved_pos - AGGRE_LOG_RESERVED_SIZE;
        log_type = static_cast<ObStorageLogType>(log_type_in_buf);
        const char* buf = log_buf + saved_pos + AGGRE_LOG_RESERVED_SIZE;
        int64_t pos = 16;
        if (ObStorageLogTypeChecker::is_trans_log(log_type)) {
          if (OB_FAIL(dump_trans_log(log_type, trans_inc, real_tenant_id, buf, buf_len, pos))) {
            CLOG_LOG(ERROR, "failed to dump_trans_log", K(entry), K(ret), KP(buf), K(pos), K(buf_len));
          }
        }
      }
    }
  } else if (OB_LOG_ARCHIVE_CHECKPOINT == log_type || OB_LOG_ARCHIVE_KICKOFF == log_type) {
    int64_t pos = 0;
    ObLogArchiveInnerLog log;
    if (OB_FAIL(log.deserialize(entry.get_buf(), entry.get_header().get_data_len(), pos))) {
      CLOG_LOG(ERROR, "failed to deserialize archive_inner_log", K(entry), K(ret));
    } else {
      fprintf(stdout, "|| ObArchiveInnerLog:%s", to_cstring(log));
    }
  } else { /*todo may be other logs need be printed?*/
  }
  fprintf(stdout, "\n");
  return ret;
}

int ObLogEntryParser::dump_ilog_entry(ObIndexEntry& entry)
{
  int ret = OB_SUCCESS;
  if (!entry.check_integrity()) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(ERROR, "entry.check_integrity() fail", K(entry), K(ret));
  } else {
    fprintf(stdout, "INDEX_LOG: %s||\n", to_cstring(entry));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_freeze_log(const char* buf, const int64_t buf_len, ObStorageLogType& log_type,
    ObFreezeType& freeze_type, ObPartitionKey& pkey, int64_t& frozen_version, ObSavedStorageInfo& info)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  log_type = storage::OB_LOG_UNKNOWN;
  freeze_type = INVALID_FREEZE;
  int64_t local_log_type = storage::OB_LOG_UNKNOWN;
  int64_t local_freeze_type = INVALID_FREEZE;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &local_log_type))) {
    CLOG_LOG(ERROR, "deserialize log_type error", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &local_freeze_type))) {
    CLOG_LOG(ERROR, "deserialize freeze_type error", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(pkey.deserialize(buf, buf_len, pos))) {
    CLOG_LOG(ERROR, "deserialize partition key error", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &frozen_version))) {
    CLOG_LOG(ERROR, "deserialize frozen version error", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(info.deserialize(buf, buf_len, pos))) {
    CLOG_LOG(ERROR, "deserialize saved storage info error", K(buf_len), K(pos), K(ret));
  } else {
    log_type = static_cast<ObStorageLogType>(local_log_type);
    freeze_type = static_cast<ObFreezeType>(local_freeze_type);
  }
  return ret;
}

int ObLogEntryParserImpl::dump_obj(const common::ObObj& obj, uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (dump_hex_) {
    int64_t buf_len = PRINT_BUF_SIZE - 1;  // reserver one character for '\0'
    int64_t pos = 0;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      CLOG_LOG(ERROR, "log entry parser is not inited", K(is_inited_), K(ret));
    } else if (OB_FAIL(obj.print_smart(print_buf_, buf_len, pos))) {
      CLOG_LOG(ERROR, "failed to print smart obj", K(column_id), K(obj), K(ret));
    } else if (pos >= PRINT_BUF_SIZE - 1) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "UNEXPECTED pos", K(pos), K(buf_len), K(ret));
    } else {
      print_buf_[pos] = '\0';
      fprintf(stdout, " %ld:%s", column_id, print_buf_);
    }
  } else {
    fprintf(stdout, " %ld:%s", column_id, to_cstring(obj));
  }
  return ret;
}

int ObLogEntryParser::skip_block_(const ObLogBlockMetaV2& meta)
{

  int ret = OB_SUCCESS;
  if (!meta.check_meta_checksum()) {
    // fatal error, data corrupt
    ret = OB_INVALID_DATA;
    CLOG_LOG(ERROR, "check block meta checksum error", K(ret), K(meta));
  } else {
    ObBlockType type = meta.get_block_type();
    switch (type) {
      case OB_DATA_BLOCK:
        advance_(meta.get_serialize_size());
        break;
      case OB_HEADER_BLOCK:  // not used any more
        // file header is CLOG_DIO_ALIGN_SIZE bytes,
        // skip it
        advance_(meta.get_total_len());
        break;
      case OB_INFO_BLOCK:
        if (is_ofs_) {
          advance_(meta.get_serialize_size() + meta.get_data_len());
        } else {
          ret = OB_ITER_END;  // info block is the end for local file
        }
        break;
      case OB_TRAILER_BLOCK:
        ret = OB_ITER_END;  // trailer block is the end
        break;
      default:
        ret = OB_INVALID_DATA;
        CLOG_LOG(ERROR, "unknown block meta type", K(ret), K(meta), K(file_id_), K(cur_offset_));
    }
  }
  return ret;
}

int ObLogEntryParserImpl::format_dump_clog_entry(ObLogEntry& entry)
{
  int ret = OB_SUCCESS;
  if (!entry.check_integrity()) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(ERROR, "entry.check_integrity() fail", K(entry), K(ret));
  } else {
    // only handle redo log
    if (OB_LOG_SUBMIT == entry.get_header().get_log_type()) {
      int64_t trans_inc = -1;
      int64_t submit_log_type_tmp = -1;
      const char* buf = entry.get_buf();
      int64_t buf_len = entry.get_header().get_data_len();
      int64_t pos = 0;
      if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &submit_log_type_tmp))) {
        CLOG_LOG(ERROR, "failed to decode submit_log_type", K(entry), K(ret));
      } else {
        int submit_log_type = static_cast<int>(submit_log_type_tmp);
        const uint64_t real_tenant_id = entry.get_header().get_partition_key().get_tenant_id();
        if (((OB_LOG_SP_TRANS_REDO & submit_log_type) || (OB_LOG_TRANS_REDO & submit_log_type) ||
                OB_LOG_SP_ELR_TRANS_COMMIT == submit_log_type || OB_LOG_SP_TRANS_COMMIT == submit_log_type) &&
            submit_log_type < OB_LOG_TRANS_MAX) {
          if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &trans_inc))) {
            CLOG_LOG(ERROR, "failed to decode trans_inc", K(ret), KP(buf), K(pos), K(buf_len));
          } else {
            switch (submit_log_type) {
              case OB_LOG_SP_TRANS_REDO: {
                if (OB_FAIL(format_dump_sp_trans_redo_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(ERROR, "format_dump_sp_trans_redo_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              case OB_LOG_SP_TRANS_COMMIT:
              case OB_LOG_SP_ELR_TRANS_COMMIT: {
                if (OB_FAIL(format_dump_sp_trans_commit_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(ERROR, "format_dump_sp_trans_redo_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              case OB_LOG_TRANS_REDO: {
                if (OB_FAIL(format_dump_trans_redo_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(ERROR, "format_dump_trans_redo_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              case OB_LOG_TRANS_REDO_WITH_PREPARE: {
                if (OB_FAIL(format_dump_trans_redo_prepare_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(ERROR, "format_dump_trans_redo_prepare_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT: {
                if (OB_FAIL(format_dump_trans_redo_prepare_commit_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(WARN, "format_dump_trans_redo_prepare_commit_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR: {
                if (OB_FAIL(
                        format_dump_trans_redo_prepare_commit_clear_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(
                      WARN, "format_dump_trans_redo_prepare_commit_clear_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              default: {
                CLOG_LOG(ERROR, "unknown log type", K(submit_log_type));
                break;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogEntryParserImpl::format_dump_sp_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObSpTransRedoLogHelper helper;
  ObSpTransRedoLog log(helper);
  int64_t pos = 0;
  if (OB_FAIL(log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(log.decrypt_table_key())) {
    cur_trans_id_ = log.get_trans_id();
    format_dump_memtable_mutator(log.get_mutator().get_data(), log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::format_dump_sp_trans_commit_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObSpTransCommitLogHelper helper;
  ObSpTransCommitLog log(helper);
  int64_t pos = 0;
  if (OB_FAIL(log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(log.decrypt_table_key())) {
    cur_trans_id_ = log.get_trans_id();
    format_dump_memtable_mutator(log.get_mutator().get_data(), log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::format_dump_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransRedoLogHelper helper;
  ObTransRedoLog log(helper);
  int64_t pos = 0;
  if (OB_FAIL(log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(log.decrypt_table_key())) {
    cur_trans_id_ = log.get_trans_id();
    format_dump_memtable_mutator(log.get_mutator().get_data(), log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::format_dump_trans_redo_prepare_log(
    const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransRedoLogHelper helper;
  ObTransRedoLog r_log(helper);
  int64_t pos = 0;
  if (OB_FAIL(r_log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(r_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(r_log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(r_log.decrypt_table_key())) {
    cur_trans_id_ = r_log.get_trans_id();
    format_dump_memtable_mutator(r_log.get_mutator().get_data(), r_log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "redo log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::format_dump_trans_redo_prepare_commit_log(
    const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTransRedoLogHelper helper;
  ObTransRedoLog r_log(helper);
  if (OB_FAIL(r_log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(r_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(r_log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(r_log.decrypt_table_key())) {
    cur_trans_id_ = r_log.get_trans_id();
    format_dump_memtable_mutator(r_log.get_mutator().get_data(), r_log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "redo log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::format_dump_trans_redo_prepare_commit_clear_log(
    const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTransRedoLogHelper helper;
  ObTransRedoLog r_log(helper);
  if (OB_FAIL(r_log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(r_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(r_log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(r_log.decrypt_table_key())) {
    cur_trans_id_ = r_log.get_trans_id();
    format_dump_memtable_mutator(r_log.get_mutator().get_data(), r_log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "redo log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParserImpl::format_dump_memtable_mutator(const char* buf, int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMemtableMutatorIterator mmi;
  ObMemtableMutatorMeta meta;
  int64_t meta_pos = 0;
  if (OB_FAIL(meta.deserialize(buf, len, meta_pos))) {
    CLOG_LOG(ERROR, "meta.deserialize fail", K(ret));
  } else if (ObTransRowFlag::is_normal_row(meta.get_flags())) {
    if (OB_FAIL(mmi.deserialize(buf, len, pos))) {
      CLOG_LOG(ERROR, "mmi.deserialize fail", K(ret));
    }
    while (OB_SUCCESS == ret) {
      ObMemtableMutatorRow row;
      if (OB_FAIL(mmi.get_next_row(row)) && OB_ITER_END != ret) {
        CLOG_LOG(ERROR, "mmi.get_next_row fail", K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        if (T_DML_LOCK == row.dml_type_) {
          // do nothing
        } else {
          fprintf(stdout, "%s\t", to_cstring(cur_trans_id_));
          fprintf(stdout, "%s\t", get_row_dml_type_str(row.dml_type_));
          fprintf(stdout, "%lu\t", row.table_id_);
          int64_t pos = row.rowkey_.to_format_string(print_buf_, PRINT_BUF_SIZE - 1);
          print_buf_[pos] = '\0';
          fprintf(stdout, "%s\t", print_buf_);

          ObCellReader old_cci;
          if (NULL != row.old_row_.data_ && row.old_row_.size_ > 0) {
            if (OB_FAIL(old_cci.init(row.old_row_.data_, row.old_row_.size_, SPARSE))) {
              CLOG_LOG(ERROR, "old cci.init fail", K(ret));
            }
            int64_t index = 0;
            while (OB_SUCC(ret) && OB_SUCC(old_cci.next_cell())) {
              uint64_t column_id = OB_INVALID_ID;
              const ObObj* value = NULL;
              if (OB_FAIL(old_cci.get_cell(column_id, value))) {
                CLOG_LOG(ERROR, "failed to get cell", K(ret));
              } else if (NULL == value) {
                ret = OB_ERR_UNEXPECTED;
                CLOG_LOG(ERROR, "got value is NULL", K(ret));
              } else if (ObExtendType == value->get_type() && ObActionFlag::OP_END_FLAG == value->get_ext()) {
                ret = OB_ITER_END;
              } else {
                if (index > 0) {
                  fprintf(stdout, " ");  // separate columns
                }
                if (OB_SUCC(ret) && OB_FAIL(format_dump_obj(*value, column_id))) {
                  CLOG_LOG(ERROR, "failed to dump obj", K(*value), K(ret));
                } else {
                  ++index;
                }
              }
            }
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            }
          } else if (NULL == row.old_row_.data_ && 0 == row.old_row_.size_) {
            // no data of old row
          } else {
            CLOG_LOG(ERROR, "old cci.init fail", K(row), K(ret));
          }

          if (OB_SUCC(ret) && T_DML_DELETE != row.dml_type_) {
            ObCellReader new_cci;
            if (OB_FAIL(new_cci.init(row.new_row_.data_, row.new_row_.size_, SPARSE))) {
              CLOG_LOG(ERROR, "new cci.init fail", K(ret));
            } else {
              int64_t index = 0;
              while (OB_SUCC(ret) && OB_SUCC(new_cci.next_cell())) {
                uint64_t column_id = OB_INVALID_ID;
                const ObObj* value = NULL;
                if (OB_FAIL(new_cci.get_cell(column_id, value))) {
                } else if (NULL == value) {
                  ret = OB_ERR_UNEXPECTED;
                } else if (ObExtendType == value->get_type() && ObActionFlag::OP_END_FLAG == value->get_ext()) {
                  ret = OB_ITER_END;
                } else {
                  if (OB_SUCC(ret) && OB_FAIL(format_dump_obj(*value, column_id))) {
                    CLOG_LOG(ERROR, "failed to dump obj", K(*value), K(ret));
                  } else {
                    ++index;
                  }
                }
              }
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
              }
            }
          }
          fprintf(stdout, "\n");
        }
      }
    }
  } else {
    fprintf(stdout, "BIG_ROW: %s\n", to_cstring(mmi.get_meta()));
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObLogEntryParserImpl::format_dump_obj(const common::ObObj& obj, uint64_t column_id)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = PRINT_BUF_SIZE - 1;  // reserver one character for '\0'
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "log entry parser is not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(obj.print_format(print_buf_, buf_len, pos))) {
    CLOG_LOG(ERROR, "failed to print smart obj", K(column_id), K(obj), K(ret));
  } else if (pos >= PRINT_BUF_SIZE - 1) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "UNEXPECTED pos", K(pos), K(buf_len), K(ret));
  } else {
    print_buf_[pos] = '\0';
    fprintf(stdout, "%lu:%s", column_id, print_buf_);
  }
  return ret;
}

int ObLogEntryParser::stat_block_(const ObLogBlockMetaV2& meta)
{

  int ret = OB_SUCCESS;
  if (!meta.check_meta_checksum()) {
    // fatal error, data corrupt
    ret = OB_INVALID_DATA;
    CLOG_LOG(ERROR, "check block meta checksum error", K(ret), K(meta));
  } else {
    ObBlockType type = meta.get_block_type();
    switch (type) {
      case OB_DATA_BLOCK: {
        int64_t size = meta.get_serialize_size();
        advance_(size);
        log_stat_.data_block_header_size_ += size;
        break;
      }
      case OB_HEADER_BLOCK:
        // not used any more
        // file header is CLOG_DIO_ALIGN_SIZE bytes,
        // skip it
        advance_(meta.get_total_len());
        break;
      case OB_INFO_BLOCK: {
        ret = OB_ITER_END;  // info block is the end
        break;
      }
      case OB_TRAILER_BLOCK:
        ret = OB_ITER_END;
        break;
        // iter will never encounter a trailer for InfoBlock lies ahead
      default:
        ret = OB_INVALID_DATA;
        CLOG_LOG(ERROR, "unknown block meta type", K(ret), K(meta), K(file_id_), K(cur_offset_));
    }
  }
  return ret;
}

int ObLogEntryParser::stat_clog_entry(const ObLogEntry& entry, const int64_t pos, const bool is_compressed)
{
  int ret = OB_SUCCESS;
  if (!entry.check_integrity()) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(ERROR, "entry.check_integrity() fail", K(entry), K(ret));
  } else if (is_compressed && OB_FAIL(log_stat_.compressed_tenant_ids_.set_refactored(
                                  entry.get_header().get_partition_key().get_tenant_id(), 1 /* overwrite*/))) {
    CLOG_LOG(ERROR, "failed to set_refactored", K(entry), K(ret));
  } else {
    log_stat_.log_header_size_ += entry.get_header().get_serialize_size();
    log_stat_.log_size_ += entry.get_header().get_data_len();
    log_stat_.primary_table_id_ = entry.get_header().get_partition_key().get_table_id();
    if (OB_LOG_SUBMIT == entry.get_header().get_log_type()) {
      if (is_compressed) {
        log_stat_.compressed_log_cnt_++;
        log_stat_.compressed_log_size_ += pos;
        log_stat_.original_log_size_ += entry.get_total_len();
      } else {
        log_stat_.non_compressed_log_cnt_++;
      }

      ++log_stat_.total_log_count_;
      int64_t trans_inc = -1;
      int64_t submit_log_type_tmp = -1;
      const char* buf = entry.get_buf();
      int64_t buf_len = entry.get_header().get_data_len();
      int64_t pos = 0;
      if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &submit_log_type_tmp))) {
        CLOG_LOG(ERROR, "failed to decode submit_log_type", K(entry), K(ret));
      } else {
        int submit_log_type = static_cast<int>(submit_log_type_tmp);
        const uint64_t real_tenant_id = entry.get_header().get_partition_key().get_tenant_id();
        if ((OB_LOG_SP_TRANS_COMMIT & submit_log_type) || (OB_LOG_SP_ELR_TRANS_COMMIT & submit_log_type)) {
          ++log_stat_.sp_trans_count_;
        } else if (OB_LOG_TRANS_PREPARE & submit_log_type) {
          ++log_stat_.dist_trans_count_;
        }

        if (((OB_LOG_SP_TRANS_REDO & submit_log_type) || (OB_LOG_TRANS_REDO & submit_log_type) ||
                OB_LOG_SP_ELR_TRANS_COMMIT == submit_log_type || OB_LOG_SP_TRANS_COMMIT == submit_log_type) &&
            submit_log_type < OB_LOG_TRANS_MAX) {
          if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &trans_inc))) {
            CLOG_LOG(ERROR, "failed to decode trans_inc", K(ret), KP(buf), K(pos), K(buf_len));
          } else {
            log_stat_.trans_log_size_ += buf_len - pos;
            switch (submit_log_type) {
              case OB_LOG_TRANS_REDO: {
                if (OB_FAIL(stat_trans_redo_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(ERROR, "dump_trans_redo_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              case OB_LOG_SP_TRANS_REDO: {
                if (OB_FAIL(stat_sp_trans_redo_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(ERROR, "dump_sp_trans_redo_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              case OB_LOG_TRANS_REDO_WITH_PREPARE: {
                if (OB_FAIL(stat_trans_redo_prepare_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(ERROR, "dump_trans_redo_prepare_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              case OB_LOG_SP_TRANS_COMMIT:
              case OB_LOG_SP_ELR_TRANS_COMMIT: {
                if (OB_FAIL(stat_sp_trans_commit_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(WARN, "dump_sp_trans_commit_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT: {
                if (OB_FAIL(stat_trans_redo_prepare_commit_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(WARN, "dump_trans_redo_prepare_commit_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR: {
                if (OB_FAIL(stat_trans_redo_prepare_commit_clear_log(buf + pos, buf_len - pos, real_tenant_id))) {
                  CLOG_LOG(WARN, "dump_trans_redo_prepare_commit_clear_log fail", K(ret), K(buf), K(pos), K(buf_len));
                }
                break;
              }
              default: {
                CLOG_LOG(ERROR, "unknown log type", K(submit_log_type));
                break;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogEntryParser::stat_sp_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObSpTransRedoLogHelper helper;
  ObSpTransRedoLog log(helper);
  int64_t pos = 0;
  if (OB_FAIL(log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(log.decrypt_table_key())) {
    log_stat_.mutator_size_ += log.get_mutator().get_position();
    stat_memtable_mutator(log.get_mutator().get_data(), log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParser::stat_sp_trans_commit_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObSpTransCommitLogHelper helper;
  ObSpTransCommitLog log(helper);
  int64_t pos = 0;
  if (OB_FAIL(log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(log.decrypt_table_key())) {
    log_stat_.mutator_size_ += log.get_mutator().get_position();
    stat_memtable_mutator(log.get_mutator().get_data(), log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParser::stat_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransRedoLogHelper helper;
  ObTransRedoLog log(helper);
  int64_t pos = 0;
  if (OB_FAIL(log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(log.decrypt_table_key())) {
    log_stat_.mutator_size_ += log.get_mutator().get_position();
    stat_memtable_mutator(log.get_mutator().get_data(), log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParser::stat_trans_redo_prepare_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTransRedoLogHelper helper;
  ObTransRedoLog r_log(helper);
  int64_t pos = 0;
  if (OB_FAIL(r_log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(r_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialzie log", K(ret));
  } else if (OB_FAIL(r_log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(r_log.decrypt_table_key())) {
    log_stat_.mutator_size_ += r_log.get_mutator().get_position();
    stat_memtable_mutator(r_log.get_mutator().get_data(), r_log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "redo log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParser::stat_trans_redo_prepare_commit_log(const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTransRedoLogHelper helper;
  ObTransRedoLog r_log(helper);
  if (OB_FAIL(r_log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(r_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(r_log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(r_log.decrypt_table_key())) {
    log_stat_.mutator_size_ += r_log.get_mutator().get_position();
    stat_memtable_mutator(r_log.get_mutator().get_data(), r_log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "redo log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParser::stat_trans_redo_prepare_commit_clear_log(
    const char* data, int64_t len, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTransRedoLogHelper helper;
  ObTransRedoLog r_log(helper);
  if (OB_FAIL(r_log.init_for_deserialize())) {
    CLOG_LOG(WARN, "redo log init for deserialize fail", K(ret));
  } else if (OB_FAIL(r_log.deserialize(data, len, pos))) {
    CLOG_LOG(WARN, "failed to deserialize log", K(ret));
  } else if (OB_FAIL(r_log.replace_encrypt_info_tenant_id(real_tenant_id))) {
    CLOG_LOG(WARN, "failed to replace encrypt info tenant id", K(ret));
  } else if (OB_SUCC(r_log.decrypt_table_key())) {
    log_stat_.mutator_size_ += r_log.get_mutator().get_position();
    stat_memtable_mutator(r_log.get_mutator().get_data(), r_log.get_mutator().get_position());
  } else if (ret == OB_EAGAIN) {
    // impossible branch
  } else {
    CLOG_LOG(WARN, "redo log deserialize error", K(ret), KP(data), K(len), K(pos));
  }
  return ret;
}

int ObLogEntryParser::stat_memtable_mutator(const char* buf, int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMemtableMutatorIterator mmi;
  ObMemtableMutatorMeta meta;
  int64_t meta_pos = 0;
  if (OB_FAIL(meta.deserialize(buf, len, meta_pos))) {
    CLOG_LOG(ERROR, "meta.deserialize fail", K(ret));
  } else if (ObTransRowFlag::is_normal_row(meta.get_flags())) {
    if (OB_FAIL(mmi.deserialize(buf, len, pos))) {
      CLOG_LOG(ERROR, "mmi.deserialize fail", K(ret));
    }

    while (OB_SUCC(ret)) {
      ObMemtableMutatorRow row;
      if (OB_FAIL(mmi.get_next_row(row)) && OB_ITER_END != ret) {
        CLOG_LOG(ERROR, "mmi.get_next_row fail", K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        ++log_stat_.total_row_count_;
        if (row.table_id_ == log_stat_.primary_table_id_) {
          ++log_stat_.primary_row_count_;
          log_stat_.new_primary_row_size_ += row.new_row_.size_;
        }
        log_stat_.old_row_size_ += row.old_row_.size_;
        log_stat_.new_row_size_ += row.new_row_.size_;
      }
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogEntryParserImpl::check_filter(const ObLogEntry& entry, bool& need_print)
{
  int ret = OB_SUCCESS;
  need_print = true;
  const ObLogEntryHeader& header = entry.get_header();
  const ObPartitionKey& pkey = header.get_partition_key();
  const uint64_t log_id = header.get_log_id();
  if ((filter_.is_table_id_valid() && filter_.get_table_id() != pkey.get_table_id()) ||
      (filter_.is_partition_id_valid() && filter_.get_partition_id() != pkey.get_partition_id()) ||
      (filter_.is_log_id_valid() && filter_.get_log_id() != log_id)) {
    need_print = false;
  }
  if (need_print) {
    CLOG_LOG(INFO, "find target log entry", K(header));
  }
  return ret;
}

int ObLogEntryParserImpl::dump_trans_log(const ObStorageLogType log_type, const int64_t trans_inc,
    const uint64_t real_tenant_id, const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  fprintf(stdout, "\t|||Trans: log_type:%s, trans_inc:%ld", get_submit_log_type(log_type), trans_inc);
  switch (log_type) {
    case OB_LOG_TRANS_REDO: {
      if (OB_FAIL(dump_trans_redo_log(buf + pos, buf_len - pos, real_tenant_id))) {
        CLOG_LOG(ERROR, "dump_trans_redo_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_SP_TRANS_REDO: {
      if (OB_FAIL(dump_sp_trans_redo_log(buf + pos, buf_len - pos, real_tenant_id))) {
        CLOG_LOG(ERROR, "dump_sp_trans_redo_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_TRANS_PREPARE: {
      if (OB_FAIL(dump_trans_prepare_log(buf + pos, buf_len - pos))) {
        CLOG_LOG(ERROR, "dump_trans_prepare_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_TRANS_REDO_WITH_PREPARE: {
      if (OB_FAIL(dump_trans_redo_prepare_log(buf + pos, buf_len - pos, real_tenant_id))) {
        CLOG_LOG(ERROR, "dump_trans_redo_prepare_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_TRANS_COMMIT: {
      if (OB_FAIL(dump_trans_commit_log(buf + pos, buf_len - pos))) {
        CLOG_LOG(WARN, "dump_trans_commit_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_SP_TRANS_COMMIT:
    case OB_LOG_SP_ELR_TRANS_COMMIT: {
      if (OB_FAIL(dump_sp_trans_commit_log(buf + pos, buf_len - pos, real_tenant_id))) {
        CLOG_LOG(WARN, "dump_sp_trans_commit_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_TRANS_CLEAR: {
      if (OB_FAIL(dump_trans_clear_log(buf + pos, buf_len - pos))) {
        CLOG_LOG(WARN, "dump_trans_clear_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_TRANS_ABORT: {
      if (OB_FAIL(dump_trans_abort_log(buf + pos, buf_len - pos))) {
        CLOG_LOG(WARN, "dump_sp_trans_abort_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_SP_TRANS_ABORT: {
      if (OB_FAIL(dump_sp_trans_abort_log(buf + pos, buf_len - pos))) {
        CLOG_LOG(WARN, "dump_sp_trans_abort_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_TRANS_PREPARE_WITH_COMMIT: {
      if (OB_FAIL(dump_trans_prepare_commit_log(buf + pos, buf_len - pos))) {
        CLOG_LOG(WARN, "dump_trans_prepare_commit_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT: {
      if (OB_FAIL(dump_trans_redo_prepare_commit_log(buf + pos, buf_len - pos, real_tenant_id))) {
        CLOG_LOG(WARN, "dump_trans_redo_prepare_commit_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_TRANS_PREPARE_WITH_COMMIT_WITH_CLEAR: {
      if (OB_FAIL(dump_trans_prepare_commit_clear_log(buf + pos, buf_len - pos))) {
        CLOG_LOG(WARN, "dump_trans_prepare_commit_clear_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR: {
      if (OB_FAIL(dump_trans_redo_prepare_commit_clear_log(buf + pos, buf_len - pos, real_tenant_id))) {
        CLOG_LOG(WARN, "dump_trans_redo_prepare_commit_clear_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_MUTATOR: {
      if (OB_FAIL(dump_trans_mutator_log(buf + pos, buf_len - pos, real_tenant_id))) {
        CLOG_LOG(WARN, "dump_trans_mutator_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_TRANS_STATE: {
      if (OB_FAIL(dump_trans_state_log(buf + pos, buf_len - pos))) {
        CLOG_LOG(WARN, "dump_trans_state_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_MUTATOR_WITH_STATE: {
      if (OB_FAIL(dump_trans_mutator_state_log(buf + pos, buf_len - pos, real_tenant_id))) {
        CLOG_LOG(WARN, "dump_trans_mutator_state_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    case OB_LOG_MUTATOR_ABORT: {
      if (OB_FAIL(dump_trans_mutator_abort_log(buf + pos, buf_len - pos))) {
        CLOG_LOG(WARN, "dump_trans_mutator_abort_log fail", K(ret), K(buf), K(pos), K(buf_len));
      }
      break;
    }
    default: {
      CLOG_LOG(ERROR, "unknown log type", K(log_type));
      break;
    }
  }
  return ret;
}

}  // end of namespace clog
}  // end of namespace oceanbase
