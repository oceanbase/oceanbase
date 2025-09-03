/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "sql/table_format/iceberg/ob_iceberg_utils.h"

#include "share/backup/ob_backup_io_adapter.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/ob_define.h"
#include "sql/engine/cmd/ob_load_data_file_reader.h"
#include "sql/engine/table/ob_external_table_access_service.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

int ObIcebergUtils::deep_copy_optional_string(ObIAllocator &allocator,
                                              const std::optional<ObString> &src,
                                              std::optional<ObString> dst)
{
  int ret = OB_SUCCESS;
  if (src.has_value()) {
    ObString tmp;
    OZ(ob_write_string(allocator, src.value(), tmp, true));
    OX(dst = tmp);
  } else {
    dst = std::nullopt;
  }
  return ret;
}

int ObIcebergUtils::deep_copy_map_string(ObIAllocator &allocator,
                                         const ObIArray<std::pair<ObString, ObString>> &src,
                                         ObIArray<std::pair<ObString, ObString>> &dst)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); i++) {
    const std::pair<ObString, ObString> &pair = src.at(i);
    ObString copied_key;
    ObString copied_value;
    if (OB_FAIL(ob_write_string(allocator, pair.first, copied_key, true))) {
      LOG_WARN("deep copy map key failed", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, pair.second, copied_value, true))) {
      LOG_WARN("deep copy map value failed", K(ret));
    } else if (OB_FAIL(dst.push_back(std::make_pair(copied_key, copied_value)))) {
      LOG_WARN("push value failed", K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObIcebergUtils::deep_copy_array_object(ObIAllocator &allocator,
                                           const ObIArray<T *> &src,
                                           ObIArray<T *> &dst)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
    T *src_item = src.at(i);
    T *dst_item = NULL;
    if (OB_ISNULL(src_item)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_ISNULL(dst_item = OB_NEWx(T, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_FAIL(dst_item->assign(*src_item))) {
      LOG_WARN("fail to assign", K(ret));
    } else if (OB_FAIL(dst.push_back(dst_item))) {
      LOG_WARN("push value failed", K(ret));
    }
  }
  return ret;
}

int32_t ObIcebergUtils::get_iceberg_field_id(uint64_t ob_column_id)
{
  return ob_column_id - OB_APP_MIN_COLUMN_ID + 1;
}

uint64_t ObIcebergUtils::get_ob_column_id(int32_t iceberg_field_id)
{
  return iceberg_field_id + OB_APP_MIN_COLUMN_ID - 1;
}

int ObIcebergFileIOUtils::read_table_metadata(ObIAllocator &allocator,
                                              const ObString &filename,
                                              const ObString &access_info,
                                              char *&buf,
                                              int64_t &read_size)
{
  int ret = OB_SUCCESS;
  sql::ObCSVGeneralFormat::ObCSVCompression compression
      = sql::ObCSVGeneralFormat::ObCSVCompression::INVALID;

  if (filename.suffix_match(".metadata.json.gz") || filename.suffix_match(".gz.metadata.json")) {
    compression = sql::ObCSVGeneralFormat::ObCSVCompression::GZIP;
  } else if (filename.suffix_match("metadata.json")) {
    compression = sql::ObCSVGeneralFormat::ObCSVCompression::NONE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table metadata file", K(ret), K(filename));
  }

  int64_t file_size = -1;
  if (OB_SUCC(ret)) {
    ObArenaAllocator tmp_allocator;
    SMART_VAR(ObExternalFileInfoCollector, collector, tmp_allocator)
    {
      if (OB_FAIL(collector.init(filename, access_info))) {
        LOG_WARN("failed to init collector", K(ret));
      } else if (OB_FAIL(collector.collect_file_size(filename, file_size))) {
        LOG_WARN("failed to get file size", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObExternalFileAccess file_reader;
    ObExternalFileUrlInfo file_info(filename,
                                    access_info,
                                    filename,
                                    ObString::make_empty_string(),
                                    file_size,
                                    INT64_MAX);
    ObExternalTableAccessOptions options = ObExternalTableAccessOptions::lazy_defaults();
    ObExternalFileCacheOptions cache_options(options.enable_page_cache_,
                                             options.enable_disk_cache_);

    if (OB_FAIL(file_reader.open(file_info, cache_options))) {
      LOG_WARN("fail to open file reader", K(ret), K(filename));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(file_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for file reader", K(ret), K(file_size));
    } else {
      const int64_t io_timeout_ms = THIS_WORKER.get_timeout_ts() / 1000;
      ObExternalReadInfo read_info(0, buf, file_size, io_timeout_ms);
      if (OB_FAIL(file_reader.pread(read_info, read_size))) {
        LOG_WARN("failed to read file", K(ret), K(filename), K(file_size), K(read_size));
      }
    }
    file_reader.close();
  }

  if (OB_SUCC(ret)) {
    // need to decompress
    if (sql::ObCSVGeneralFormat::ObCSVCompression::GZIP == compression) {
      ObArenaAllocator tmp_allocator;
      int64_t next_buf_size = file_size * 10;
      bool is_finished = false; // 对于 uncompressed，一轮读取就完成了，对于 compressed，我们需要
                                // guess 解压后的 buff 大小，所以可能需要多次重试
      while (OB_SUCC(ret) && !is_finished) {
        ObDecompressor *decompressor = NULL;
        char *decompressed_buf = NULL;
        int64_t consumed_size = 0;
        int64_t decompressed_size = 0;
        if (OB_FAIL(ObDecompressor::create(sql::ObCSVGeneralFormat::ObCSVCompression::GZIP,
                                           tmp_allocator,
                                           decompressor))) {
          LOG_WARN("failed to create decompressor", K(ret));
        } else if (OB_ISNULL(decompressor)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get decompressor", K(ret));
        } else if (OB_FALSE_IT(decompressed_buf = static_cast<char *>(allocator.alloc(next_buf_size)))) {
        } else if (OB_ISNULL(decompressed_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else if (OB_FAIL(decompressor->decompress(buf,
                                                    file_size,
                                                    consumed_size,
                                                    decompressed_buf,
                                                    next_buf_size,
                                                    decompressed_size))) {
          LOG_WARN("failed to decompress data", K(ret));
        } else {
          if (consumed_size >= file_size) {
            is_finished = true;
            buf = decompressed_buf;
            read_size = decompressed_size;
          } else {
            next_buf_size *= 2;
          }
        }

        if (OB_NOT_NULL(decompressor)) {
          ObDecompressor::destroy(decompressor);
        }
      }
    }
  }
  return ret;
}

int ObIcebergFileIOUtils::read(ObIAllocator &allocator,
                               const ObString &filename,
                               const ObString &access_info,
                               char *&buf,
                               int64_t &read_size,
                               bool enable_cache)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  int64_t file_size = 0;
  SMART_VAR(ObExternalFileInfoCollector, collector, tmp_allocator)
  {
    if (OB_FAIL(collector.init(filename, access_info))) {
      LOG_WARN("failed to init collector", K(ret));
    } else if (OB_FAIL(collector.collect_file_size(filename, file_size))) {
      LOG_WARN("failed to get file size", K(ret));
    } else {
      ObExternalFileAccess file_reader;
      ObExternalFileUrlInfo file_info(filename,
                                      access_info,
                                      filename,
                                      ObString::make_empty_string(),
                                      file_size,
                                      INT64_MAX);
      ObExternalTableAccessOptions options = ObExternalTableAccessOptions::lazy_defaults();
      ObExternalFileCacheOptions cache_options(options.enable_page_cache_,
                                               options.enable_disk_cache_);
      if (!enable_cache) {
        // 对于 version-hint 这种文件，不进 cache，避免读到老的 snapshot
        cache_options.reset();
      }
      if (OB_FAIL(file_reader.open(file_info, cache_options))) {
        LOG_WARN("fail to open file reader", K(ret), K(filename));
      } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(file_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for file reader", K(ret), K(file_size));
      } else {
        const int64_t io_timeout_ms = THIS_WORKER.get_timeout_ts() / 1000;
        ObExternalReadInfo read_info(0, buf, file_size, io_timeout_ms);
        if (OB_FAIL(file_reader.pread(read_info, read_size))) {
          LOG_WARN("failed to read file", K(ret), K(filename), K(file_size), K(read_size));
        }
      }
      file_reader.close();
    }
  }
  return ret;
}

int ObIcebergFileIOUtils::is_exist(const ObString &filename,
                                   const ObString &access_info,
                                   bool &existed)
{
  int ret = OB_SUCCESS;
  ObExternalIoAdapter io_adapter;
  ObObjectStorageInfo *storage_info = NULL;
  ObExternalTableStorageInfo object_storage_info;
  ObHDFSStorageInfo hdfs_storage_info;
  ObStorageType storage_type;
  if (OB_FAIL(get_storage_type_from_path_for_external_table(filename, storage_type))) {
    LOG_WARN("failed to get storage type", K(ret));
  } else if (ObStorageType::OB_STORAGE_HDFS == storage_type) {
    storage_info = &hdfs_storage_info;
  } else {
    storage_info = &object_storage_info;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(storage_info->set(storage_type, access_info.ptr()))) {
      LOG_WARN("failed to set storage info", K(ret));
    } else if (OB_FAIL(io_adapter.is_exist(filename, storage_info, existed))) {
      LOG_WARN("failed to is_exist", K(ret));
    }
  }
  return ret;
}

template <typename T>
std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                     || std::is_same_v<T, bool>,
                 int>
ObCatalogJsonUtils::get_primitive(const ObJsonObject &json_object,
                                  const ObString &key,
                                  std::optional<T> &value)
{
  int ret = OB_SUCCESS;
  ObJsonNode *json_value = json_object.get_value(key);
  if (NULL == json_value) {
    value = std::nullopt;
  } else if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
    if (ObJsonNodeType::J_INT == json_value->json_type()) {
      value = json_value->get_int();
    } else if (ObJsonNodeType::J_UINT == json_value->json_type()) {
      value = json_value->get_uint();
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid json number", K(ret), K(key), KP(json_value), K(json_value->json_type()));
    }
  } else if constexpr (std::is_same_v<T, bool>) {
    if (ObJsonNodeType::J_BOOLEAN != json_value->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid json boolean", K(ret), K(key), KP(json_value));
    } else {
      value = json_value->get_boolean();
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json type", K(ret), K(key));
  }
  return ret;
}

template <typename T>
std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                     || std::is_same_v<T, bool>,
                 int>
ObCatalogJsonUtils::get_primitive(const ObJsonObject &json_object, const ObString &key, T &value)
{
  int ret = OB_SUCCESS;
  std::optional<T> tmp;
  if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, key, tmp))) {
    LOG_WARN("failed to get json primitive value", K(ret), K(key));
  } else if (!tmp.has_value()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("required field not existed", K(ret), K(key));
  } else {
    value = tmp.value();
  }
  return ret;
}

template <typename T>
typename std::enable_if<std::is_same<T, int32_t>::value || std::is_same<T, int64_t>::value,
                        int>::type
ObCatalogJsonUtils::get_primitive_array(const ObJsonObject &json_object,
                                        const ObString &key,
                                        ObIArray<T> &value)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *json_array = json_object.get_value(key);
  if (NULL == json_array) {
    value.reset();
  } else {
    if (ObJsonNodeType::J_ARRAY != json_array->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid json array", K(ret), K(key), KP(json_array));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < json_array->element_count(); i++) {
        ObIJsonBase *json_element = NULL;
        if (OB_FAIL(json_array->get_array_element(i, json_element))) {
          LOG_WARN("failed to get json array element", K(ret));
        } else {
          if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
            if (ObJsonNodeType::J_INT == json_element->json_type()) {
              OZ(value.push_back(json_element->get_int()));
            } else if (ObJsonNodeType::J_UINT == json_element->json_type()) {
              OZ(value.push_back(json_element->get_uint()));
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid json number", K(ret), K(key));
            }
          } else if constexpr (std::is_same_v<T, bool>) {
            if (ObJsonNodeType::J_BOOLEAN == json_element->json_type()) {
              OZ(value.push_back(json_element->get_boolean()));
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid json number", K(ret), K(key));
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid primitive type", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObCatalogJsonUtils::get_string(ObIAllocator &allocator,
                                   const ObJsonObject &json_object,
                                   const ObString &key,
                                   std::optional<ObString> &value)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *json_node = json_object.get_value(key);
  if (NULL == json_node) {
    value = std::nullopt;
  } else {
    ObString tmp_string;
    if (ObJsonNodeType::J_STRING != json_node->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid json string", K(ret), K(key), KP(json_node));
    } else if (OB_FAIL(
                   ob_write_string(allocator,
                                   ObString(json_node->get_data_length(), json_node->get_data()),
                                   tmp_string,
                                   true))) {
      LOG_WARN("failed to deep copy string", K(ret), K(key));
    } else {
      value = tmp_string;
    }
  }
  return ret;
}

int ObCatalogJsonUtils::get_string(ObIAllocator &allocator,
                                   const ObJsonObject &json_object,
                                   const ObString &key,
                                   ObString &value)
{
  int ret = OB_SUCCESS;
  std::optional<ObString> tmp_string;
  if (OB_FAIL(ObCatalogJsonUtils::get_string(allocator, json_object, key, tmp_string))) {
    LOG_WARN("fail to get string", K(ret), K(key));
  } else if (!tmp_string.has_value()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("required field not existed", K(ret), K(key));
  } else {
    value = tmp_string.value();
  }
  return ret;
}

int ObCatalogJsonUtils::convert_json_object_to_map(ObIAllocator &allocator,
                                                   const ObJsonObject &json_object,
                                                   ObIArray<pair<ObString, ObString>> &values)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < json_object.element_count(); i++) {
    ObString object_key;
    ObJsonNode *object_value;
    if (OB_FAIL(json_object.get_value_by_idx(i, object_key, object_value))) {
      LOG_WARN("failed to get json object element", K(ret));
    } else if (ObJsonNodeType::J_STRING != object_value->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid type", K(ret));
    } else {
      ObString deep_copied_key;
      ObString deep_copied_value;
      OZ(ob_write_string(allocator, object_key, deep_copied_key, true));
      OZ(ob_write_string(allocator,
                         ObString(object_value->get_data_length(), object_value->get_data()),
                         deep_copied_value,
                         true));
      OZ(values.push_back(std::make_pair(deep_copied_key, deep_copied_value)));
    }
  }
  return ret;
}

int ObCatalogJsonUtils::get_string_array(ObIAllocator &allocator,
                                         const ObJsonObject &json_object,
                                         const ObString &key,
                                         ObIArray<ObString> &values)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *json_array = json_object.get_value(key);
  if (NULL == json_array) {
    values.reset();
  } else {
    if (ObJsonNodeType::J_ARRAY != json_array->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid json array", K(ret), K(key), KP(json_array));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < json_array->element_count(); i++) {
        ObIJsonBase *json_element = NULL;
        if (OB_FAIL(json_array->get_array_element(i, json_element))) {
          LOG_WARN("failed to get json array element", K(ret));
        } else if (ObJsonNodeType::J_STRING != json_element->json_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid type", K(ret));
        } else {
          ObString tmp_string;
          OZ(ob_write_string(allocator,
                             ObString(json_element->get_data_length(), json_element->get_data()),
                             tmp_string,
                             true));
          OZ(values.push_back(tmp_string));
        }
      }
    }
  }
  return ret;
}

template <avro::Type T>
int ObCatalogAvroUtils::get_binary(ObIAllocator &allocator,
                                   const avro::GenericRecord &avro_record,
                                   const ObString &key,
                                   ObString &value)
{
  int ret = OB_SUCCESS;
  std::optional<ObString> tmp_value;
  if (OB_FAIL(ObCatalogAvroUtils::get_binary<T>(allocator, avro_record, key, tmp_value))) {
    LOG_WARN("failed to get binary", K(ret), K(key));
  } else if (!tmp_value.has_value()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get empty binary", K(ret), K(key));
  } else {
    value = tmp_value.value();
  }
  return ret;
}

template <avro::Type T>
int ObCatalogAvroUtils::get_binary(ObIAllocator &allocator,
                                   const avro::GenericRecord &avro_record,
                                   const ObString &key,
                                   std::optional<ObString> &value)
{
  int ret = OB_SUCCESS;
  std::string ket_str(key.ptr(), key.length());
  if (!avro_record.hasField(ket_str)) {
    value = std::nullopt;
  } else {
    const avro::GenericDatum &field_datum = avro_record.field(ket_str);
    if (avro::Type::AVRO_NULL == field_datum.type()) {
      value = std::nullopt;
    } else if (avro::Type::AVRO_STRING == T && T == field_datum.type()) {
      const std::string &str = field_datum.value<std::string>();
      ObString tmp_string;
      OZ(ob_write_string(allocator, ObString(str.size(), str.data()), tmp_string, true));
      value = tmp_string;
    } else if (avro::Type::AVRO_BYTES == T && T == field_datum.type()) {
      const std::vector<uint8_t> &bytes = field_datum.value<std::vector<uint8_t>>();
      ObString tmp_string;
      OZ(ob_write_string(allocator,
                         ObString(bytes.size(), reinterpret_cast<const char *>(bytes.data())),
                         tmp_string,
                         true));
      value = tmp_string;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("not supported avro type", K(ret), K(T), K(field_datum.type()));
    }
  }
  return ret;
}

template <typename T>
std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                     || std::is_same_v<T, bool>,
                 int>
ObCatalogAvroUtils::get_primitive(const avro::GenericRecord &avro_record,
                                  const ObString &key,
                                  std::optional<T> &value)
{
  int ret = OB_SUCCESS;
  std::string key_str(key.ptr(), key.length());
  if (!avro_record.hasField(key_str)) {
    value = std::nullopt;
  } else {
    const avro::GenericDatum &field_datum = avro_record.field(key_str);
    if (avro::Type::AVRO_NULL == field_datum.type()) {
      value = std::nullopt;
    } else if (avro::Type::AVRO_INT == field_datum.type()
               || avro::Type::AVRO_LONG == field_datum.type()
               || avro::Type::AVRO_BOOL == field_datum.type()) {
      value = field_datum.value<T>();
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("not supported avro type", K(ret), K(field_datum.type()));
    }
  }

  return ret;
}

template <typename T>
std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                     || std::is_same_v<T, bool>,
                 int>
ObCatalogAvroUtils::get_primitive(const avro::GenericRecord &avro_record,
                                  const ObString &key,
                                  T &value)
{
  int ret = OB_SUCCESS;
  std::optional<T> tmp_value;
  if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_record, key, tmp_value))) {
    LOG_WARN("failed to get avro primitive", K(ret), K(key));
  } else if (!tmp_value.has_value()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get null avro primitive", K(ret), K(key));
  } else {
    value = tmp_value.value();
  }

  return ret;
}

template <typename K, typename V>
typename std::enable_if_t<std::is_same_v<K, int32_t> || std::is_same_v<K, int64_t>
                              || std::is_same_v<V, int32_t> || std::is_same_v<V, int64_t>
                              || std::is_same_v<V, bool>,
                          int>
ObCatalogAvroUtils::get_primitive_map(const avro::GenericRecord &avro_record,
                                      const ObString &key,
                                      ObIArray<std::pair<K, V>> &value)
{
  int ret = OB_SUCCESS;
  avro::GenericDatum avro_datum;
  if (OB_FAIL(get_value<avro::Type::AVRO_ARRAY>(avro_record, key, avro_datum))) {
    LOG_WARN("failed to get avro array", K(ret), K(key));
  } else if (avro::Type::AVRO_ARRAY == avro_datum.type()) {
    const avro::GenericArray &avro_array = avro_datum.value<avro::GenericArray>();
    for (int64_t i = 0; OB_SUCC(ret) && i < avro_array.value().size(); i++) {
      const avro::GenericDatum &element = avro_array.value()[i];
      if (avro::Type::AVRO_RECORD != element.type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid element type", K(ret), K(element.type()));
      } else {
        const avro::GenericRecord &element_record = element.value<avro::GenericRecord>();
        OZ(value.push_back(std::make_pair(element_record.field("key").value<K>(),
                                          element_record.field("value").value<V>())));
      }
    }
  }
  return ret;
}

template <avro::Type AVRO_TYPE, typename K, typename V>
std::enable_if_t<(std::is_same_v<K, int32_t> || std::is_same_v<K, int64_t>)
                     && std::is_same_v<V, ObString>,
                 int>
ObCatalogAvroUtils::get_binary_map(ObIAllocator &allocator,
                                   const avro::GenericRecord &avro_record,
                                   const ObString &key,
                                   ObIArray<std::pair<K, V>> &value)
{
  int ret = OB_SUCCESS;
  avro::GenericDatum avro_datum;
  if (OB_FAIL(get_value<avro::Type::AVRO_ARRAY>(avro_record, key, avro_datum))) {
    LOG_WARN("failed to get avro array", K(ret), K(key));
  } else if (avro::Type::AVRO_ARRAY == avro_datum.type()) {
    const avro::GenericArray &avro_array = avro_datum.value<avro::GenericArray>();
    for (int64_t i = 0; OB_SUCC(ret) && i < avro_array.value().size(); i++) {
      const avro::GenericDatum &element = avro_array.value()[i];
      if (avro::Type::AVRO_RECORD != element.type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid element type", K(ret), K(element.type()));
      } else {
        const avro::GenericRecord &element_record = element.value<avro::GenericRecord>();
        ObString copy_string;
        if (avro::Type::AVRO_STRING == AVRO_TYPE) {
          std::string tmp_string = element_record.field("value").value<std::string>();
          OZ(ob_write_string(allocator,
                             ObString(tmp_string.size(), tmp_string.data()),
                             copy_string,
                             true));
        } else if (avro::Type::AVRO_BYTES == AVRO_TYPE) {
          std::vector<uint8_t> tmp_bytes
              = element_record.field("value").value<std::vector<uint8_t>>();
          OZ(ob_write_string(
              allocator,
              ObString(tmp_bytes.size(), reinterpret_cast<const char *>(tmp_bytes.data())),
              copy_string,
              true));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid avro type", K(AVRO_TYPE));
        }
        OZ(value.push_back(std::make_pair(element_record.field("key").value<K>(), copy_string)));
      }
    }
  }
  return ret;
}

template <typename V>
std::enable_if_t<std::is_same_v<V, int32_t> || std::is_same_v<V, int64_t>, int>
ObCatalogAvroUtils::get_primitive_array(const avro::GenericRecord &avro_record,
                                        const ObString &key,
                                        ObIArray<V> &value)
{
  int ret = OB_SUCCESS;
  avro::GenericDatum avro_datum;
  if (OB_FAIL(get_value<avro::Type::AVRO_ARRAY>(avro_record, key, avro_datum))) {
    LOG_WARN("failed to get avro map", K(ret), K(key));
  } else if (avro::Type::AVRO_NULL == avro_datum.type()) {
    value.reset();
  } else if (avro::Type::AVRO_ARRAY == avro_datum.type()) {
    const avro::GenericArray &avro_array = avro_datum.value<avro::GenericArray>();
    for (int64_t i = 0; OB_SUCC(ret) && i < avro_array.value().size(); i++) {
      const avro::GenericDatum &avro_element = avro_array.value()[i];
      OZ(value.push_back(avro_element.value<V>()));
    }
  }
  return ret;
}

template <avro::Type T>
int ObCatalogAvroUtils::get_value(const avro::GenericRecord &avro_record,
                                  const ObString &key,
                                  avro::GenericDatum &value)
{
  int ret = OB_SUCCESS;
  std::string key_str(key.ptr(), key.length());
  if (!avro_record.hasField(key_str)) {
    value = avro::GenericDatum(); // assign a null GenericDatum
  } else {
    const avro::GenericDatum &field_datum = avro_record.field(key_str);
    if (avro::Type::AVRO_NULL == field_datum.type()) {
      value = avro::GenericDatum(); // assign a null GenericDatum
    } else if (T == field_datum.type()) {
      value = field_datum;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid avro type", K(ret), K(key), K(T), K(field_datum.type()));
    }
  }

  return ret;
}

} // namespace iceberg
} // namespace sql

} // namespace oceanbase
