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

#include "ob_sstable_printer.h"

namespace oceanbase {
using namespace storage;
using namespace common;
namespace blocksstable {
#define FPRINTF(args...) fprintf(stderr, ##args)
#define P_BAR() FPRINTF("|")
#define P_DASH() FPRINTF("------------------------------")
#define P_END_DASH() FPRINTF("--------------------------------------------------------------------------------")
#define P_NAME(key) FPRINTF("%s", (key))
#define P_NAME_FMT(key) FPRINTF("%30s", (key))
#define P_VALUE_STR_B(key) FPRINTF("[%s]", (key))
#define P_VALUE_INT_B(key) FPRINTF("[%d]", (key))
#define P_VALUE_BINT_B(key) FPRINTF("[%ld]", (key))
#define P_VALUE_STR(key) FPRINTF("%s", (key))
#define P_VALUE_INT(key) FPRINTF("%d", (key))
#define P_VALUE_BINT(key) FPRINTF("%ld", (key))
#define P_NAME_BRACE(key) FPRINTF("{%s}", (key))
#define P_COLOR(color) FPRINTF(color)
#define P_LB() FPRINTF("[")
#define P_RB() FPRINTF("]")
#define P_LBRACE() FPRINTF("{")
#define P_RBRACE() FPRINTF("}")
#define P_COLON() FPRINTF(":")
#define P_COMMA() FPRINTF(",")
#define P_TAB() FPRINTF("\t")
#define P_LINE_NAME(key) FPRINTF("%30s", (key))
#define P_LINE_VALUE(key, type) P_LINE_VALUE_##type((key))
#define P_LINE_VALUE_INT(key) FPRINTF("%-30d", (key))
#define P_LINE_VALUE_BINT(key) FPRINTF("%-30ld", (key))
#define P_LINE_VALUE_STR(key) FPRINTF("%-30s", (key))
#define P_END() FPRINTF("\n")

#define P_TAB_LEVEL(level)                  \
  do {                                      \
    for (int64_t i = 1; i < (level); ++i) { \
      P_TAB();                              \
      if (i != level - 1)                   \
        P_BAR();                            \
    }                                       \
  } while (0)

#define P_DUMP_LINE(type)      \
  do {                         \
    P_TAB_LEVEL(level);        \
    P_BAR();                   \
    P_LINE_NAME(name);         \
    P_BAR();                   \
    P_LINE_VALUE(value, type); \
    P_END();                   \
  } while (0)

#define P_DUMP_LINE_COLOR(type) \
  do {                          \
    P_COLOR(LIGHT_GREEN);       \
    P_TAB_LEVEL(level);         \
    P_BAR();                    \
    P_COLOR(CYAN);              \
    P_LINE_NAME(name);          \
    P_BAR();                    \
    P_COLOR(NONE_COLOR);        \
    P_LINE_VALUE(value, type);  \
    P_END();                    \
  } while (0)

// mapping to ObObjType
static const char* OB_OBJ_TYPE_NAMES[ObMaxType] = {"ObNullType",
    "ObTinyIntType",
    "ObSmallIntType",
    "ObMediumIntType",
    "ObInt32Type",
    "ObIntType",
    "ObUTinyIntType",
    "ObUSmallIntType",
    "ObUMediumIntType",
    "ObUInt32Type",
    "ObUInt64Type",
    "ObFloatType",
    "ObDoubleType",
    "ObUFloatType",
    "ObUDoubleType",
    "ObNumberType",
    "ObUNumberType",
    "ObDateTimeType",
    "ObTimestampType",
    "ObDateType",
    "ObTimeType",
    "ObYearType",
    "ObVarcharType",
    "ObCharType",
    "ObHexStringType",
    "ObExtendType",
    "ObUnknowType",
    "ObTinyTextType",
    "ObTextType",
    "ObMediumTextType",
    "ObLongTextType",
    "ObBitType",
    "ObEnumType",
    "ObSetType",
    "ObEnumInnerType",
    "ObSetInnerType",
    "ObTimestampTZType",
    "ObTimestampLTZType",
    "ObTimestampNanoType",
    "ObRawType",
    "ObIntervalYMType",
    "ObIntervalDSType",
    "ObNumberFloatType",
    "ObNVarchar2Type",
    "ObNCharType",
    "ObURowIDType",
    "ObLobType"};

void ObSSTablePrinter::print_title(const char* title, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
  }
  P_TAB_LEVEL(level);
  P_DASH();
  P_NAME_BRACE(title);
  P_DASH();
  P_END();
}

void ObSSTablePrinter::print_end_line(const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
  }
  P_TAB_LEVEL(level);
  P_END_DASH();
  P_END();
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(NONE_COLOR);
  }
}

void ObSSTablePrinter::print_title(const char* title, const int64_t value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
  }
  P_TAB_LEVEL(level);
  P_DASH();
  P_LBRACE();
  P_NAME(title);
  P_VALUE_BINT_B(value);
  P_RBRACE();
  P_DASH();
  P_END();
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(NONE_COLOR);
  }
}

void ObSSTablePrinter::print_line(const char* name, const uint32_t value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_DUMP_LINE_COLOR(INT);
  } else {
    P_DUMP_LINE(INT);
  }
}

void ObSSTablePrinter::print_line(const char* name, const uint64_t value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_DUMP_LINE_COLOR(BINT);
  } else {
    P_DUMP_LINE(BINT);
  }
}

void ObSSTablePrinter::print_line(const char* name, const int32_t value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_DUMP_LINE_COLOR(INT);
  } else {
    P_DUMP_LINE(INT);
  }
}

void ObSSTablePrinter::print_line(const char* name, const int64_t value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_DUMP_LINE_COLOR(BINT);
  } else {
    P_DUMP_LINE(BINT);
  }
}

void ObSSTablePrinter::print_line(const char* name, const char* value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_DUMP_LINE_COLOR(STR);
  } else {
    P_DUMP_LINE(STR);
  }
}

void ObSSTablePrinter::print_cols_info_start(const char* n1, const char* n2, const char* n3, const char* n4)
{
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
  }
  FPRINTF("--------{%-15s %15s %15s %15s}----------\n", n1, n2, n3, n4);
}

void ObSSTablePrinter::print_cols_info_line(const int32_t& v1, const ObObjType v2, const int64_t& v3, const int64_t& v4)
{
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
    P_BAR();
    P_COLOR(NONE_COLOR);
    FPRINTF("\t[%-15d %15s %15ld %15ld]\n", v1, OB_OBJ_TYPE_NAMES[v2], v3, v4);
  } else {
    P_BAR();
    FPRINTF("\t[%-15d %15s %15ld %15ld]\n", v1, OB_OBJ_TYPE_NAMES[v2], v3, v4);
  }
}

void ObSSTablePrinter::print_row_title(const ObStoreRow* row, const int64_t row_index)
{
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
    P_BAR();
    P_COLOR(CYAN);
    P_NAME("ROW");
    P_VALUE_BINT_B(row_index);
    P_COLON();
    if (OB_NOT_NULL(row->trans_id_ptr_)) {
      int buf_len = 1024;
      char buf[buf_len];
      MEMSET(buf, 0, buf_len);
      row->trans_id_ptr_->to_string(buf, buf_len);
      P_NAME("trans_id=");
      P_VALUE_STR(buf);
      P_COMMA();
    } else {
      P_NAME("trans_id=");
      P_VALUE_INT_B(0);
      P_COMMA();
    }
    P_NAME("flag=");
    P_VALUE_BINT_B(row->flag_);
    P_COMMA();
    P_NAME("first_dml=");
    P_VALUE_INT_B(row->get_first_dml());
    P_COMMA();
    P_NAME("dml=");
    P_VALUE_INT_B(row->get_dml());
    P_COMMA();
    P_NAME("multi_version_row_flag=");
    P_VALUE_INT_B(row->row_type_flag_.flag_);
    P_BAR();
    P_COLOR(NONE_COLOR);
  } else {
    P_BAR();
    P_NAME("ROW");
    P_VALUE_BINT_B(row_index);
    P_COLON();
    if (OB_NOT_NULL(row->trans_id_ptr_)) {
      int buf_len = 1024;
      char buf[buf_len];
      MEMSET(buf, 0, buf_len);
      row->trans_id_ptr_->to_string(buf, buf_len);
      P_NAME("trans_id=");
      P_VALUE_STR(buf);
      P_COMMA();
    } else {
      P_NAME("trans_id=");
      P_VALUE_INT_B(0);
      P_COMMA();
    }
    P_NAME("flag=");
    P_VALUE_BINT_B(row->flag_);
    P_COMMA();
    P_NAME("first_dml=");
    P_VALUE_INT_B(row->get_first_dml());
    P_COMMA();
    P_NAME("dml=");
    P_VALUE_INT_B(row->get_dml());
    P_COMMA();
    P_NAME("multi_version_row_flag=");
    P_VALUE_INT_B(row->row_type_flag_.flag_);
    P_BAR();
  }
}

void ObSSTablePrinter::print_cell(const ObObj& cell)
{
  P_VALUE_STR_B(to_cstring(cell));
}

void ObSSTablePrinter::print_common_header(const ObMacroBlockCommonHeader* common_header)
{
  print_title("Common Header");
  print_line("header_size", common_header->get_header_size());
  print_line("version", common_header->get_version());
  print_line("magic", common_header->get_magic());
  print_line("attr", common_header->get_attr());
  print_line("attr", common_header->get_previous_block_index());
  print_end_line();
}

void ObSSTablePrinter::print_macro_block_header(const ObSSTableMacroBlockHeader* sstable_header)
{
  print_title("SSTable Header");
  print_line("header_size", sstable_header->header_size_);
  print_line("version", sstable_header->version_);
  print_line("magic", sstable_header->magic_);
  print_line("attr", sstable_header->attr_);
  print_line("table_id", sstable_header->table_id_);
  print_line("data_version", sstable_header->data_version_);
  print_line("column_count", sstable_header->column_count_);
  print_line("rowkey_column_count", sstable_header->rowkey_column_count_);
  print_line("row_store_type", sstable_header->row_store_type_);
  print_line("row_count", sstable_header->row_count_);
  print_line("occupy_size", sstable_header->occupy_size_);
  print_line("micro_block_count", sstable_header->micro_block_count_);
  print_line("micro_block_size", sstable_header->micro_block_size_);
  print_line("micro_block_data_offset", sstable_header->micro_block_data_offset_);
  print_line("micro_block_index_offset", sstable_header->micro_block_index_offset_);
  print_line("micro_block_index_size", sstable_header->micro_block_index_size_);
  print_line("micro_block_endkey_offset", sstable_header->micro_block_endkey_offset_);
  print_line("micro_block_endkey_size", sstable_header->micro_block_endkey_size_);
  print_line("data_checksum", sstable_header->data_checksum_);
  print_line("compressor_name", sstable_header->compressor_name_);
  print_line("data_seq", sstable_header->data_seq_);
  print_line("partition_id", sstable_header->partition_id_);
  print_line("master_key_id", sstable_header->master_key_id_);
  print_end_line();
}

void ObSSTablePrinter::print_macro_block_header(const ObLobMacroBlockHeader* lob_macro_header)
{
  print_macro_block_header(reinterpret_cast<const ObSSTableMacroBlockHeader*>(lob_macro_header));
  print_title("Lob Macro Header Additional");
  print_line("micro_block_size_array_offset", lob_macro_header->micro_block_size_array_offset_);
  print_line("micor_block_size_array_size", lob_macro_header->micro_block_size_array_size_);
  print_end_line();
}

void ObSSTablePrinter::print_macro_block_header(const ObBloomFilterMacroBlockHeader* bf_macro_header)
{
  print_title("SSTable Bloomfilter Macro Block Header");
  print_line("header_size", bf_macro_header->header_size_);
  print_line("version", bf_macro_header->version_);
  print_line("magic", bf_macro_header->magic_);
  print_line("attr", bf_macro_header->attr_);
  print_line("table_id", bf_macro_header->table_id_);
  print_line("partition_id", bf_macro_header->partition_id_);
  print_line("data_version", bf_macro_header->data_version_);
  print_line("rowkey_column_count", bf_macro_header->rowkey_column_count_);
  print_line("row_count", bf_macro_header->row_count_);
  print_line("occupy_size", bf_macro_header->occupy_size_);
  print_line("micro_block_count", bf_macro_header->micro_block_count_);
  print_line("micro_block_data_offset", bf_macro_header->micro_block_data_offset_);
  print_line("micro_block_data_size", bf_macro_header->micro_block_data_size_);
  print_line("data_checksum", bf_macro_header->data_checksum_);
  print_line("compressor_name", bf_macro_header->compressor_name_);
  print_end_line();
}

void ObSSTablePrinter::print_micro_index(const ObStoreRowkey& endkey, const ObMicroBlockInfo& block_info)
{
  print_title("Micro Index", block_info.index_, 1);
  print_line("endkey", to_cstring(endkey));
  print_line("data offset", block_info.offset_);
  print_line("data length", block_info.size_);
  // print_line("key offset", keypos.offset_);
  // print_line("key length", keypos.length_);
  print_line("mark deletion", block_info.mark_deletion_);
  print_end_line();
}

void ObSSTablePrinter::print_micro_header(const ObMicroBlockHeader* micro_block_header)
{
  print_title("Micro Header");
  print_line("header_size", micro_block_header->header_size_);
  print_line("version", micro_block_header->version_);
  print_line("magic", micro_block_header->magic_);
  print_line("attr", micro_block_header->attr_);
  print_line("column_count", micro_block_header->column_count_);
  print_line("row_index_offset", micro_block_header->row_index_offset_);
  print_line("row_count", micro_block_header->row_count_);
  print_end_line();
}

void ObSSTablePrinter::print_encoding_micro_header(const blocksstable::ObMicroBlockHeaderV2* micro_header)
{
  print_title("Encoding Micro Header");
  print_line("header_size", micro_header->header_size_);
  print_line("version", micro_header->version_);
  print_line("row_count", micro_header->row_count_);
  print_line("var_column_count", micro_header->var_column_count_);
  print_line("row_data_offset", micro_header->row_data_offset_);
  print_line("row_index_byte", micro_header->row_index_byte_);
  print_line("extend_value_bit", micro_header->extend_value_bit_);
  print_line("store_row_header", micro_header->store_row_header_);
  print_end_line();
}

void ObSSTablePrinter::print_lob_micro_header(const ObLobMicroBlockHeader* micro_block_header)
{
  print_title("Lob Micro Header");
  print_line("header_size", micro_block_header->header_size_);
  print_line("version", micro_block_header->version_);
  print_line("magic", micro_block_header->magic_);
  print_end_line();
}

void ObSSTablePrinter::print_lob_micro_block(const char* micro_block_buf, const int64_t micro_block_size)
{
  print_title("Lob Micro Data");
  print_line("block_size", micro_block_size);
  P_VALUE_STR_B(micro_block_buf);
  P_END();
}

void ObSSTablePrinter::print_bloom_filter_micro_header(const ObBloomFilterMicroBlockHeader* micro_block_header)
{
  print_title("Bloom Filter Micro Header");
  print_line("header_size", micro_block_header->header_size_);
  print_line("version", micro_block_header->version_);
  print_line("magic", micro_block_header->magic_);
  print_line("rowkey_column_count", micro_block_header->rowkey_column_count_);
  print_line("row_count", micro_block_header->row_count_);
  print_line("reserved", micro_block_header->reserved_);
  print_end_line();
}

void ObSSTablePrinter::print_bloom_filter_micro_block(const char* micro_block_buf, const int64_t micro_block_size)
{
  print_title("Bloom Filter Micro Data");
  print_line("block_size", micro_block_size);
  P_VALUE_STR_B(micro_block_buf);
  P_END();
}

void ObSSTablePrinter::print_store_row(const ObStoreRow* row, const bool is_trans_sstable)
{
  if (is_trans_sstable) {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    int64_t pos1 = 0;
    transaction::ObTransID trans_id;
    transaction::ObTransSSTableDurableCtxInfo ctx_info;

    if (OB_FAIL(
            trans_id.deserialize(row->row_val_.cells_[0].get_string_ptr(), row->row_val_.cells_[0].val_len_, pos))) {
      STORAGE_LOG(WARN, "failed to deserialize trans_id", K(ret), K(trans_id));
    } else if (OB_FAIL(
                   ctx_info.deserialize(row->row_val_.cells_[1].v_.string_, row->row_val_.cells_[1].val_len_, pos1))) {
      STORAGE_LOG(WARN, "failed to deserialize status_info", K(ret), K(ctx_info));
    } else {
      P_VALUE_STR_B(to_cstring(*const_cast<transaction::ObTransID*>(&trans_id)));
      P_VALUE_STR_B(to_cstring(*const_cast<transaction::ObTransSSTableDurableCtxInfo*>(&ctx_info)));
    }
  } else {
    for (int64_t i = 0; i < row->row_val_.count_; ++i) {
      print_cell(row->row_val_.cells_[i]);
    }
  }
  P_END();
}
}  // namespace blocksstable
}  // namespace oceanbase
