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

#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "lib/compress/ob_compressor_pool.h"
#include "share/ob_encryption_util.h"
#include "share/rc/ob_tenant_base.h"
#include "ob_macro_block.h"
#include "ob_macro_block_bare_iterator.h"
#include "ob_macro_block_reader.h"
#include "ob_micro_block_reader.h"
#include "ob_sstable_printer.h"
#include "ob_micro_block_header.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace compaction;
namespace blocksstable
{

ObMacroBlockReader::ObMacroBlockReader()
    :compressor_(NULL),
     uncomp_buf_(NULL),
     uncomp_buf_size_(0),
     decrypt_buf_(NULL),
     decrypt_buf_size_(0),
     allocator_(ObModIds::OB_CS_SSTABLE_READER),
     encryption_(nullptr)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObMacroBlockReader::~ObMacroBlockReader()
{
  if (nullptr != encryption_) {
    encryption_->~ObMicroBlockEncryption();
    ob_free(encryption_);
    encryption_ = nullptr;
  }
  if (nullptr != compressor_) {
    compressor_->reset_mem();
  }
}

#ifdef OB_BUILD_TDE_SECURITY
int ObMacroBlockReader::init_encrypter_if_needed()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObMemAttr attr(MTL_ID(), ObModIds::OB_CS_SSTABLE_READER);

  if (nullptr != encryption_) {
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObMicroBlockEncryption), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for encrypter", K(ret));
  } else {
    encryption_ = new (buf) ObMicroBlockEncryption();
  }

  return ret;
}
#endif


int ObMacroBlockReader::decompress_data(
    const common::ObCompressorType compressor_type,
    const char *buf,
    const int64_t size,
    const char *&uncomp_buf,
    int64_t &uncomp_size,
    bool &is_compressed)
{
  int ret = OB_SUCCESS;
  ObMicroBlockHeader header;
  int64_t header_size = 0;
  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to decompress data", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(header.deserialize(buf, size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize record header", K(ret));
  } else if (OB_UNLIKELY(size < header.header_size_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(size), "header_size", header.header_size_);
  } else {
    is_compressed = header.is_compressed_data();

    if (!is_compressed) {
      uncomp_buf = buf;
      uncomp_size = size;
    } else if (OB_FAIL(decompress_data_buf(compressor_type, buf, header_size,
        buf + header_size, size - header_size, uncomp_buf, uncomp_size))) {
      LOG_WARN("Fail to decompress data buffer", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockReader::decompress_data_buf(
    const common::ObCompressorType compressor_type,
    const char *header_buf,
    const int64_t header_size,
    const char *data_buf,
    const int64_t data_buf_size,
    const char *&uncomp_buf,
    int64_t &uncomp_size,
    ObIAllocator *ext_allocator)
{
  // uncomp_buf: header + uncomp_data
  int ret = OB_SUCCESS;
  ObMicroBlockHeader header;
  int64_t pos = 0;
  if (OB_ISNULL(data_buf) || OB_ISNULL(header_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid input", K(ret), KP(data_buf), KP(header_buf));
  } else if (OB_FAIL(header.deserialize(header_buf, header_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize record header", K(ret));
  } else {
    if (nullptr == compressor_ || compressor_->get_compressor_type() != compressor_type) {
      if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_type, compressor_))) {
        STORAGE_LOG(WARN, "Fail to get compressor, ", K(ret), K(compressor_type));
      }
    }

    const int64_t data_length = header.data_length_;
    uncomp_size = header_size + data_length;
    int64_t pos = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(ext_allocator)) {
      // decompress data to buffer from external allocator
      char *ext_uncomp_buf = nullptr;
      if (OB_FAIL(alloc_buf(*ext_allocator, uncomp_size, ext_uncomp_buf))) {
        LOG_WARN("Fail to allocate buf", K(ret), K(uncomp_size), K(header));
      } else {
        if (OB_FAIL(compressor_->decompress(data_buf, data_buf_size,
            ext_uncomp_buf + header_size, data_length, uncomp_size))) {
          LOG_WARN("compressor fail to decompress.", K(ret));
        } else if (OB_FAIL(header.serialize(ext_uncomp_buf, header_size, pos))) {
          LOG_WARN("Fail to serialize header", K(ret), K(header));
        } else {
          uncomp_buf = ext_uncomp_buf;
          uncomp_size += header_size;
        }
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(ext_uncomp_buf)) {
        ext_allocator->free(ext_uncomp_buf);
      }
    } else if (OB_FAIL(alloc_buf(uncomp_size, uncomp_buf_, uncomp_buf_size_))) {
      LOG_WARN("Fail to allocate buf", K(ret));
    } else if (OB_FAIL(compressor_->decompress(data_buf, data_buf_size,
        uncomp_buf_ + header_size, data_length, uncomp_size))) {
      LOG_WARN("Fail to decompress", K(ret));
    } else if (OB_FAIL(header.serialize(uncomp_buf_, header_size, pos))) {
          LOG_WARN("Fail to serialize header", K(ret), K(header));
    } else {
      uncomp_buf = uncomp_buf_;
      uncomp_size += header_size;
    }
  }
  return ret;
}

int ObMacroBlockReader::decrypt_and_decompress_data(
    const ObSSTableMacroBlockHeader &block_header,
    const char *buf,
    const int64_t size,
    const char *&uncomp_buf,
    int64_t &uncomp_size,
    bool &is_compressed)
{
  int ret = OB_SUCCESS;
  int64_t header_size = 0;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(!block_header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments to decompress data",
        K(ret), KP(buf), K(size), K(block_header));
  } else {
    ObMicroBlockDesMeta deserialize_meta(
        block_header.fixed_header_.compressor_type_, block_header.fixed_header_.encrypt_id_,
        block_header.fixed_header_.master_key_id_, block_header.fixed_header_.encrypt_key_);
    if (OB_FAIL(decrypt_and_decompress_data(deserialize_meta, buf, size, uncomp_buf, uncomp_size,
        is_compressed, false/*need_deep_copy*/, nullptr/*ext_allocator*/))) {
      STORAGE_LOG(WARN, "fail to decrypt and decompress data", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockReader::decompress_data_with_prealloc_buf(
    const common::ObCompressorType compressor_type,
    const char *buf,
    const int64_t size,
    char *uncomp_buf,
    const int64_t uncomp_buf_size)
{
  int ret = OB_SUCCESS;
  int64_t uncomp_size = 0;
  ObCompressorPool &comp_pool = ObCompressorPool::get_instance();
  ObCompressorType cur_type = ObCompressorType::INVALID_COMPRESSOR;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0) || OB_ISNULL(uncomp_buf)
      || OB_UNLIKELY(uncomp_buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for decompress_data_with_prealloc_buf", K(ret), KP(buf), K(size));
  } else if (size == uncomp_buf_size) {
    MEMCPY(uncomp_buf, buf, size);
  } else {
    if (OB_NOT_NULL(compressor_)
        && OB_FAIL(comp_pool.get_compressor_type(compressor_->get_compressor_name(), cur_type))) {
      LOG_WARN("Fail to get current compressor type", K(ret));
    } else if (OB_ISNULL(compressor_) || cur_type != compressor_type) {
      if (OB_FAIL(comp_pool.get_instance().get_compressor(compressor_type, compressor_))) {
        LOG_WARN("Fail to get compressor", K(ret), K(compressor_type));
      }
    }

    if (OB_FAIL(compressor_->decompress(buf, size, uncomp_buf, uncomp_buf_size, uncomp_size))) {
      LOG_WARN("Fail to decompress data", K(ret));
    } else {
      if (OB_UNLIKELY(uncomp_size != uncomp_buf_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Uncompressed size is not equal to buffer size",
            K(ret), K(uncomp_size), K(uncomp_buf_size));
      }
    }
  }
  return ret;
}

int ObMacroBlockReader::decompress_data_with_prealloc_buf(
    const char *compressor_name,
    const char *buf,
    const int64_t size,
    char *uncomp_buf,
    const int64_t uncomp_buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0) || OB_ISNULL(uncomp_buf)
      || OB_UNLIKELY(uncomp_buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for decompress_data_with_prealloc_buf", K(ret), KP(buf), K(size));
  } else if (size == uncomp_buf_size) {
      MEMCPY(uncomp_buf, buf, size);
  } else {
    if (OB_ISNULL(compressor_) || strcmp(compressor_->get_compressor_name(), compressor_name)) {
      if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_name, compressor_))) {
        STORAGE_LOG(WARN, "Fail to get compressor, ", K(ret), "compressor_name", compressor_name);
      }
    }
    if (OB_SUCC(ret)) {
      int64_t uncomp_size;
      if (OB_FAIL(compressor_->decompress(buf, size, uncomp_buf, uncomp_buf_size, uncomp_size))) {
        LOG_WARN("failed to decompress data", K(ret));
      } else {
        if (OB_UNLIKELY(uncomp_size != uncomp_buf_size)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("uncomp size is not equal", K(ret), K(uncomp_size), K(uncomp_buf_size));
        }
      }
    }
  }
  return ret;
}

int ObMacroBlockReader::alloc_buf(const int64_t req_size, char *&buf, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_size < req_size) {
    if (nullptr != buf) {
      allocator_.reuse();
      buf = nullptr;
    }
    if (NULL == (buf = static_cast<char*>(allocator_.alloc(req_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory for buf, ", K(req_size), K(ret));
    } else {
      buf_size = req_size;
    }
  }
  return ret;
}

int ObMacroBlockReader::alloc_buf(ObIAllocator &allocator, const int64_t buf_size, char *&buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for decompress buf", K(ret), K(buf_size));
  }
  return ret;
}

int ObMacroBlockReader::decrypt_and_decompress_data(
    const ObMicroBlockDesMeta &deserialize_meta,
    const char *input,
    const int64_t size,
    const char *&uncomp_buf,
    int64_t &uncomp_size,
    bool &is_compressed,
    const bool need_deep_copy,
    ObIAllocator *ext_allocator)
{
  int ret = OB_SUCCESS;
  ObMicroBlockHeader header;
  int64_t header_size = 0;
  const char *decrypt_buf = NULL;
  int64_t decrypt_size = 0;
  bool is_encrypted = false;
  int64_t pos = 0;
  if (OB_ISNULL(input)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid input data", K(ret), KP(input), K(size));
  } else if (OB_FAIL(header.deserialize(input, size, pos))) {
    LOG_WARN("Fail to deserialize record header", K(ret));
  } else if (OB_UNLIKELY(size < (header_size = header.get_serialize_size()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Fail to deserialize record header", K(ret), K(size), K(header_size));
  } else if (OB_FAIL(header.check_header_checksum())) {
    LOG_WARN("Fail to check record header checksum", K(ret));
  } else {
    const char *data_buf = input + header_size;
    int64_t data_buf_size = size - header_size;
#ifndef OB_BUILD_TDE_SECURITY
    is_compressed = header.is_compressed_data();
#else
    if (OB_UNLIKELY(ObEncryptionUtil::need_encrypt(
                                      static_cast<ObCipherOpMode>(deserialize_meta.encrypt_id_)))) {
      LOG_DEBUG("Macro data need decrypt", K(deserialize_meta.encrypt_id_), K(data_buf_size));
      const char *decrypt_buf = NULL;
      int64_t decrypt_size = 0;
      if (OB_FAIL(ObMacroBlockReader::decrypt_buf(
          deserialize_meta, data_buf, data_buf_size, decrypt_buf, decrypt_size))) {
        STORAGE_LOG(WARN, "fail to decrypt buf", K(ret));
      } else {
        data_buf = decrypt_buf;
        data_buf_size = decrypt_size;
        is_compressed = (header.data_length_ != decrypt_size);
        is_encrypted = true;
      }
    } else {
      is_compressed = header.is_compressed_data();
      is_encrypted = false;
    }
#endif

    if (OB_SUCC(ret) && !is_compressed) {
      uncomp_size = header_size + data_buf_size;
      int64_t pos = 0;
      // if need_deep_copy = false and is_encrypted = true, we also use alloc_buf() to concatenate header with data
      if (!need_deep_copy && !is_encrypted) {
        // no need to concatenate
        uncomp_buf = input;
      } else if (need_deep_copy && OB_NOT_NULL(ext_allocator)) {
        // deep copy data to buffer from external allocator
        char *ext_uncomp_buf = nullptr;
        if (OB_FAIL(alloc_buf(*ext_allocator, uncomp_size, ext_uncomp_buf))) {
          LOG_WARN("Fail to allocate buf", K(ret), K(uncomp_size));
        } else if (OB_FAIL(header.serialize(ext_uncomp_buf, uncomp_size, pos))) {
          LOG_WARN("Fail to serialize header", K(ret), K(header));
        } else {
          MEMCPY(ext_uncomp_buf + pos, data_buf, data_buf_size);
          uncomp_buf = ext_uncomp_buf;
        }

        if (OB_FAIL(ret) && OB_NOT_NULL(ext_uncomp_buf)) {
          ext_allocator->free(ext_uncomp_buf);
        }
      } else if (OB_FAIL(alloc_buf(uncomp_size, uncomp_buf_, uncomp_buf_size_))) {
        LOG_WARN("Fail to allocate buf for deepcopy", K(uncomp_size), K(ret));
      } else if (OB_FAIL(header.serialize(uncomp_buf_, uncomp_size, pos))) {
          LOG_WARN("Fail to serialize header", K(ret), K(header));
      } else {
        MEMCPY(uncomp_buf_ + pos, data_buf, data_buf_size);
        uncomp_buf = uncomp_buf_;
      }
    }

    if (OB_SUCC(ret) && is_compressed) {
      if (OB_FAIL(decompress_data_buf(deserialize_meta.compressor_type_, input, header_size,
          data_buf, data_buf_size, uncomp_buf, uncomp_size, ext_allocator))) {
        LOG_WARN("Fail to decompress data buffer", K(ret), K(header));
      }
    }

  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObMacroBlockReader::decrypt_buf(
    const ObMicroBlockDesMeta &deserialize_meta,
    const char *buf,
    const int64_t size,
    const char *&decrypt_buf,
    int64_t &decrypt_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument of input data", K(ret), KP(buf));
  } else if (OB_UNLIKELY(!ObEncryptionUtil::need_encrypt(
                                      static_cast<ObCipherOpMode>(deserialize_meta.encrypt_id_)))) {
    decrypt_buf = buf;
    decrypt_size = size;
  } else if (OB_FAIL(init_encrypter_if_needed())) {
    LOG_WARN("Failed to init encrypter", K(ret));
  } else if (OB_FAIL(encryption_->init(
      deserialize_meta.encrypt_id_,
      MTL_ID(),
      deserialize_meta.master_key_id_,
      deserialize_meta.encrypt_key_,
      share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH))) {
    LOG_WARN("Fail to init micro block encryption", K(ret));
  } else if (OB_FAIL(encryption_->decrypt(buf, size, decrypt_buf, decrypt_size))) {
    LOG_WARN("Fail to decrypt data", K(ret));
  }
  return ret;
}
#endif

ObSSTableDataBlockReader::ObSSTableDataBlockReader()
  : data_(NULL), size_(0), common_header_(), macro_header_(), linked_header_(),
    bloomfilter_header_(NULL), column_types_(NULL), column_orders_(NULL),
    column_checksum_(NULL), macro_reader_(), allocator_(ObModIds::OB_CS_SSTABLE_READER),
    hex_print_buf_(nullptr), is_trans_sstable_(false), is_inited_(false), column_type_array_cnt_(0)
{
}

ObSSTableDataBlockReader::~ObSSTableDataBlockReader()
{
}

int ObSSTableDataBlockReader::init(const char *data, const int64_t size, const bool hex_print)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(data) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(size));
  } else if (OB_FAIL(common_header_.deserialize(data, size, pos))) {
    LOG_ERROR("deserialize common header fail", K(ret), KP(data), K(size), K(pos));
  } else if (OB_FAIL(common_header_.check_integrity())) {
    LOG_ERROR("invalid common header", K(ret), K_(common_header));
  } else {
    data_ = data;
    size_ = size;
    switch (common_header_.get_type()) {
    case ObMacroBlockCommonHeader::SSTableData:
    case ObMacroBlockCommonHeader::SSTableIndex: {
      if (OB_FAIL(macro_header_.deserialize(data_, size, pos))) {
        LOG_WARN("fail to deserialize macro block header", K(ret), KP(data_), K(size), K(pos));
      } else if (OB_UNLIKELY(macro_header_.fixed_header_.micro_block_data_offset_ != pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("incorrect data offset", K(ret), K(pos), K(macro_header_));
      } else {
        column_types_ = macro_header_.column_types_;
        column_orders_ = macro_header_.column_orders_;
        column_checksum_ = macro_header_.column_checksum_;
        column_type_array_cnt_ = macro_header_.fixed_header_.get_col_type_array_cnt();
      }
      break;
    }
    case ObMacroBlockCommonHeader::LinkedBlock: {
      if (OB_FAIL(linked_header_.deserialize(data_, size, pos))) {
        LOG_WARN("fail to deserialize linked block header", K(ret), KP(data_), K(size), K(pos));
      }
      break;
    }
    case ObMacroBlockCommonHeader::BloomFilterData: {
      bloomfilter_header_ = reinterpret_cast<const ObBloomFilterMacroBlockHeader*>(data_ + pos);
      pos += sizeof(ObBloomFilterMacroBlockHeader);
      break;
    }
    case ObMacroBlockCommonHeader::SSTableMacroMeta: {
      // nothing to do.
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("Not supported macro block type", K(ret), K_(common_header));
    }

    if (OB_SUCC(ret) && hex_print) {
      if (OB_ISNULL(hex_print_buf_ = static_cast<char *>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for hex print", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

void ObSSTableDataBlockReader::reset()
{
  data_ = NULL;
  size_ = 0;
  common_header_.reset();
  macro_header_.reset();
  linked_header_.reset();
  bloomfilter_header_ = NULL;
  column_types_ = NULL;
  column_orders_ = NULL;
  column_checksum_ = NULL;
  hex_print_buf_ = nullptr;
  allocator_.reset();
  is_inited_ = false;
}

int ObSSTableDataBlockReader::dump(const uint64_t tablet_id, const int64_t scn)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableDataBlockReader is not inited", K(ret));
  } else if (check_need_print(tablet_id, scn)) {
    ObSSTablePrinter::print_common_header(&common_header_);
    switch (common_header_.get_type()) {
    case ObMacroBlockCommonHeader::SSTableData:
      ObSSTablePrinter::print_macro_block_header(&macro_header_);
      if (OB_FAIL(dump_column_info(macro_header_.fixed_header_.column_count_, macro_header_.fixed_header_.get_col_type_array_cnt()))) {
        LOG_WARN("Failed to dump column info", K(ret), K_(macro_header));
      } else if (OB_FAIL(dump_sstable_macro_block(false))) {
        LOG_WARN("Failed to dump sstable macro block", K(ret));
      }
      break;
    case ObMacroBlockCommonHeader::LinkedBlock:
      ObSSTablePrinter::print_macro_block_header(&linked_header_);
      break;
    case ObMacroBlockCommonHeader::BloomFilterData:
      ObSSTablePrinter::print_macro_block_header(bloomfilter_header_);
      if (OB_FAIL(dump_bloom_filter_data_block())) {
        LOG_WARN("Failed to dump bloomfilter macro block", K(ret));
      }
      break;
    case ObMacroBlockCommonHeader::SSTableIndex:
      ObSSTablePrinter::print_macro_block_header(&macro_header_);
      if (OB_FAIL(dump_column_info(macro_header_.fixed_header_.column_count_, macro_header_.fixed_header_.column_count_))) {
        LOG_WARN("Failed to dump column info", K(ret), K_(macro_header));
      } else if (OB_FAIL(dump_sstable_macro_block(true))) {
        LOG_WARN("Failed to dump sstable macro block", K(ret));
      }
      break;
    case ObMacroBlockCommonHeader::SSTableMacroMeta:
      // Only Dump Header
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("Not supported macro block type", K(ret), K_(common_header));
    }
  }
  return ret;
}

bool ObSSTableDataBlockReader::check_need_print(const uint64_t tablet_id, const int64_t scn)
{
  bool need_print = true;
  if (ObMacroBlockCommonHeader::SSTableData == common_header_.get_type()) {
    if ((0 != tablet_id && tablet_id != macro_header_.fixed_header_.tablet_id_)
        || (-1 != scn && scn != macro_header_.fixed_header_.logical_version_)) {
      // tablet id or logical version doesn't match, skip print
      need_print = false;
    }
  }
  return need_print;
}

int ObSSTableDataBlockReader::dump_sstable_macro_block(const bool is_index_block)
{
  int ret = OB_SUCCESS;

  ObMicroBlockBareIterator micro_iter;
  ObMacroBlockRowBareIterator macro_iter(allocator_);
  if (OB_FAIL(macro_iter.open(data_, size_))) {
    LOG_WARN("Fail to init bare macro block row iterator", K(ret));
  } else {
    ObTabletID tablet_id(macro_header_.fixed_header_.tablet_id_);
    is_trans_sstable_ = tablet_id.is_ls_tx_data_tablet();

    int64_t micro_idx = 0;

    do {
      if (OB_FAIL(dump_sstable_micro_block(micro_idx, is_index_block, macro_iter))) {
        LOG_WARN("Fail to dump sstable micro block", K(ret));
      } else {
        ++micro_idx;
      }
    } while (OB_SUCC(ret) && OB_SUCC(macro_iter.open_next_micro_block()));

    if (OB_FAIL(ret) && OB_ITER_END != ret) {
      LOG_WARN("Fail to iterate all rows in macro block", K(ret));
    } else if (FALSE_IT(ret = OB_SUCCESS)) {
    } else if (!is_index_block) {
      // dump leaf index block
      if (OB_FAIL(macro_iter.open_leaf_index_micro_block())) {
        LOG_WARN("Fail to open leaf index micro block", K(ret));
      } else if (OB_FAIL(dump_sstable_micro_block(0, true, macro_iter))) {
        LOG_WARN("Fail to dump leaf index micro block", K(ret));
      } else if (OB_FAIL(dump_macro_block_meta_block(macro_iter))) {
        LOG_WARN("Fail to dump macro meta block in macro block", K(ret));
      }
    }
  }

  return ret;
}

int ObSSTableDataBlockReader::dump_sstable_micro_block(
    const int64_t micro_idx,
    const bool is_index_block,
    ObMacroBlockRowBareIterator &macro_iter)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockData *micro_data = nullptr;
  if (OB_FAIL(macro_iter.get_curr_micro_block_data(micro_data))) {
    LOG_WARN("Fail to get curr micro block data", K(ret));
  } else if (OB_ISNULL(micro_data) || OB_UNLIKELY(!micro_data->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected invalid micro block data", K(ret), KPC(micro_data));
  } else if (OB_FAIL(dump_sstable_micro_header(*micro_data, micro_idx, is_index_block))) {
    LOG_ERROR("Failed to dump sstble micro block header", K(ret));
  } else if (OB_FAIL(dump_sstable_micro_data(is_index_block, macro_iter))) {
    LOG_ERROR("Failed to dump sstble micro block data", K(ret));
  }
  return ret;
}

int ObSSTableDataBlockReader::dump_sstable_micro_header(
    const ObMicroBlockData &micro_data,
    const int64_t micro_idx,
    const bool is_index_block)
{
  int ret = OB_SUCCESS;
  const char *micro_block_buf = micro_data.get_buf();
  const int64_t micro_block_size = micro_data.get_buf_size();
  const ObRowStoreType row_store_type = static_cast<ObRowStoreType>(macro_header_.fixed_header_.row_store_type_);
  int64_t row_cnt = 0;

  // TODO (lingchuan): dump detail encoding header
  ObMicroBlockHeader micro_block_header;
  int64_t pos = 0;
  if (OB_FAIL(micro_block_header.deserialize(micro_block_buf, micro_block_size, pos))) {
    LOG_ERROR("Failed to deserialize sstble micro block header", K(ret), K(micro_data));
  } else {
    if (ObRowStoreType::FLAT_ROW_STORE == row_store_type) {
      ObSSTablePrinter::print_micro_header(&micro_block_header);
      row_cnt = micro_block_header.row_count_;
    } else if (ObStoreFormat::is_row_store_type_with_encoding(row_store_type)) {
      const ObColumnHeader * encode_col_header = reinterpret_cast<const ObColumnHeader *>(micro_block_buf + pos);
      ObSSTablePrinter::print_encoding_micro_header(&micro_block_header);
      row_cnt = micro_block_header.row_count_;
      for (int64_t i = 0; i < macro_header_.fixed_header_.column_count_; ++i) {
        ObSSTablePrinter::print_encoding_column_header(&encode_col_header[i], i);
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "not supported store type", K(ret), K(row_store_type));
    }
  }
  if (OB_SUCC(ret)) {
    if (is_index_block) {
      ObSSTablePrinter::print_title("Index Micro Block", micro_idx, 1);
    } else {
      ObSSTablePrinter::print_title("Micro Block", micro_idx, 1);
    }
    ObSSTablePrinter::print_title("Total Rows", row_cnt, 1);
  }

  return ret;
}

int ObSSTableDataBlockReader::dump_sstable_micro_data(
    const bool is_index_block,
    ObMacroBlockRowBareIterator &macro_bare_iter)
{
  int ret = OB_SUCCESS;
  ObIndexBlockRowParser idx_row_parser;
  int64_t row_cnt = 0;
  const ObMicroBlockData *block_data = nullptr;
  const ObMicroBlockHeader *block_header = nullptr;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const ObIndexBlockRowMinorMetaInfo *minor_meta = nullptr;
  if (OB_FAIL(macro_bare_iter.get_curr_micro_block_row_cnt(row_cnt))) {
    LOG_WARN("Fail to get row count of current micro block", K(ret));
  } else if (OB_FAIL(macro_bare_iter.get_curr_micro_block_data(block_data))) {
    LOG_WARN("Fail to get curr micro block data", K(ret));
  } else if (OB_ISNULL(block_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null block data", K(ret));
  } else if (OB_ISNULL(block_header = block_data->get_micro_header())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to get micro block header", K(ret), KPC(block_data));
  }
  const ObDatumRow *row = nullptr;
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_cnt; ++row_idx) {
    if (OB_FAIL(macro_bare_iter.get_next_row(row))) {
      LOG_WARN("Fail to get next row from iter", K(ret), K(row_idx), K(row_cnt));
    } else {
      if (!is_trans_sstable_) {
        ObSSTablePrinter::print_row_title(row, row_idx);
      } else {
        fprintf(stderr, "ROW[%ld]:", row_idx);
      }
      if (OB_NOT_NULL(hex_print_buf_) && !is_trans_sstable_) {
        ObSSTablePrinter::print_store_row_hex(row, column_types_, OB_DEFAULT_MACRO_BLOCK_SIZE, hex_print_buf_);
      } else {
        ObSSTablePrinter::print_store_row(
            row, column_types_, column_type_array_cnt_, is_index_block, is_trans_sstable_);
      }

      if (is_index_block) {
        if (OB_FAIL(idx_row_parser.init(block_header->rowkey_column_count_, *row))) {
          LOG_WARN("Fail to init idx row parser", K(ret));
        } else if (OB_FAIL(idx_row_parser.get_header(idx_row_header))) {
          LOG_WARN("Fail to get index block row header", K(ret));
        } else if (OB_ISNULL(idx_row_header)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Null pointer to index block row header", K(ret));
        } else if (FALSE_IT(ObSSTablePrinter::print_index_row_header(idx_row_header))) {
        } else if (idx_row_header->is_major_node()) {
          // skip
        } else if (OB_FAIL(idx_row_parser.get_minor_meta(minor_meta))) {
          LOG_WARN("Fail to get index row minor meta info", K(ret));
        } else if (OB_ISNULL(minor_meta)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Null pointer to minor meta", K(ret));
        } else {
          ObSSTablePrinter::print_index_minor_meta(minor_meta);
        }
      }
    }
  }
  if (nullptr != hex_print_buf_) {
    ObSSTablePrinter::print_hex_micro_block(*block_data, hex_print_buf_, OB_DEFAULT_MACRO_BLOCK_SIZE);
  }
  return ret;
}

int ObSSTableDataBlockReader::dump_macro_block_meta_block(ObMacroBlockRowBareIterator &macro_iter)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockData *micro_data = nullptr;
  const ObDatumRow *row = nullptr;
  ObDataMacroBlockMeta macro_meta;
  ObSSTablePrinter::print_title("Macro Meta Micro Block");
  if (OB_FAIL(macro_iter.open_leaf_index_micro_block(true /*macro meta*/))) {
    LOG_WARN("Fail to open macro meta block in macro block", K(ret));
  } else if (OB_FAIL(macro_iter.get_curr_micro_block_data(micro_data))) {
    LOG_WARN("Fail to get curr micro block data", K(ret));
  } else if (OB_ISNULL(micro_data) || OB_UNLIKELY(!micro_data->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected invalid micro block data", K(ret), KPC(micro_data));
  } else if (OB_FAIL(dump_sstable_micro_header(*micro_data, 0, true))) {
    LOG_WARN("Failed to dump sstble micro block header", K(ret));
  } else if (OB_FAIL(macro_iter.get_next_row(row))) {
    LOG_WARN("Failed to get next meta block row", K(ret));
  } else if (OB_FAIL(macro_meta.parse_row(*const_cast<ObDatumRow *>(row)))) {
    LOG_WARN("Failed to parse macro block meta", K(ret));
  } else {
    ObSSTablePrinter::print_store_row(
            row, column_types_, micro_data->get_micro_header()->rowkey_column_count_, true, is_trans_sstable_);
    ObSSTablePrinter::print_macro_meta(&macro_meta);
  }
  return ret;
}

int ObSSTableDataBlockReader::dump_bloom_filter_data_block()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bloomfilter_header_) || OB_UNLIKELY(!bloomfilter_header_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid bloomfilter macro block header", KPC(bloomfilter_header_), K(ret));
  } else {
    bool is_compressed = false;
    const char *block_buf = data_ + bloomfilter_header_->micro_block_data_offset_;
    ObMicroBlockData micro_data;
    if (OB_FAIL(macro_reader_.decompress_data(
        bloomfilter_header_->compressor_type_,
        block_buf,
        bloomfilter_header_->micro_block_data_size_,
        micro_data.get_buf(),
        micro_data.get_buf_size(),
        is_compressed))) {
      STORAGE_LOG(WARN, "Failed to decompress bloom filter micro block data", K(ret));
    } else if (OB_UNLIKELY(!micro_data.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexcepted micro data", K(micro_data), K(ret));
    } else {
      const ObBloomFilterMicroBlockHeader *header = reinterpret_cast<const ObBloomFilterMicroBlockHeader *>(micro_data.get_buf());
      ObSSTablePrinter::print_bloom_filter_micro_header(header);
      ObSSTablePrinter::print_bloom_filter_micro_block(micro_data.get_buf() + sizeof(ObBloomFilterMicroBlockHeader),
          micro_data.get_buf_size() - sizeof(ObBloomFilterMicroBlockHeader));
    }
  }

  return ret;
}

int ObSSTableDataBlockReader::dump_column_info(const int64_t col_cnt, const int64_t type_array_col_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_cnt < 0)
      || OB_ISNULL(column_types_)
      || OB_ISNULL(column_orders_)
      || OB_ISNULL(column_checksum_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid column info", K(ret), K(col_cnt),
        KP_(column_types), KP_(column_orders), KP_(column_checksum));
  } else if (col_cnt > 0) {
    ObSSTablePrinter::print_cols_info_start("column_index", "column_type", "column_order", "column_checksum", "collation_type");
    int64_t i = 0;
    for (; i < type_array_col_cnt; ++i) {
      ObSSTablePrinter::print_cols_info_line(i, column_types_[i].get_type(), column_orders_[i],
          column_checksum_[i], column_types_[i].get_collation_type());
    }
    for (; i < col_cnt; ++i) {
      ObSSTablePrinter::print_cols_info_line(i, ObUnknownType, ASC,
          column_checksum_[i], column_types_[i].get_collation_type());
    }
    ObSSTablePrinter::print_end_line();
  }
  return ret;
}

} /* namespace blocksstable */
} /* namespace oceanbase */
