/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cstring>
#include "ob_expr_test_utils.h"
#define private public
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObAiFuncImageUtilsTest: public ::testing::Test
{
public:
  ObAiFuncImageUtilsTest();
  virtual ~ObAiFuncImageUtilsTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObAiFuncImageUtilsTest(const ObAiFuncImageUtilsTest &other);
  ObAiFuncImageUtilsTest& operator=(const ObAiFuncImageUtilsTest &other);
protected:
  // data members
};

ObAiFuncImageUtilsTest::ObAiFuncImageUtilsTest()
{
}

ObAiFuncImageUtilsTest::~ObAiFuncImageUtilsTest()
{
}

void ObAiFuncImageUtilsTest::SetUp()
{
}

void ObAiFuncImageUtilsTest::TearDown()
{
}

TEST_F(ObAiFuncImageUtilsTest, test_get_type_from_binary)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  // Test JPEG
  const uint8_t jpeg_data[] = {0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46};
  ObString jpeg_str(sizeof(jpeg_data), reinterpret_cast<char*>(const_cast<uint8_t*>(jpeg_data)));
  ObAiFuncImageUtils::ObImageType image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, jpeg_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_JPEG, image_type);

  // Test PNG
  const uint8_t png_data[] = {0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D};
  ObString png_str(sizeof(png_data), reinterpret_cast<char*>(const_cast<uint8_t*>(png_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, png_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_PNG, image_type);

  // Test GIF
  const uint8_t gif_data[] = {0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x10, 0x00};
  ObString gif_str(sizeof(gif_data), reinterpret_cast<char*>(const_cast<uint8_t*>(gif_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, gif_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_GIF, image_type);

  // Test WebP (RIFF....WEBP)
  const uint8_t webp_data[] = {0x52, 0x49, 0x46, 0x46, 0x08, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50};
  ObString webp_str(sizeof(webp_data), reinterpret_cast<char*>(const_cast<uint8_t*>(webp_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, webp_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_WEBP, image_type);

  // Test BMP
  const uint8_t bmp_data[] = {0x42, 0x4D, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  ObString bmp_str(sizeof(bmp_data), reinterpret_cast<char*>(const_cast<uint8_t*>(bmp_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, bmp_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_BMP, image_type);

  // Test TIFF (little endian)
  const uint8_t tiff_le_data[] = {0x49, 0x49, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00};
  ObString tiff_le_str(sizeof(tiff_le_data), reinterpret_cast<char*>(const_cast<uint8_t*>(tiff_le_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, tiff_le_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_TIFF, image_type);

  // Test TIFF (big endian)
  const uint8_t tiff_be_data[] = {0x4D, 0x4D, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x00};
  ObString tiff_be_str(sizeof(tiff_be_data), reinterpret_cast<char*>(const_cast<uint8_t*>(tiff_be_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, tiff_be_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_TIFF, image_type);

  // Test ICO
  const uint8_t ico_data[] = {0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x10, 0x10};
  ObString ico_str(sizeof(ico_data), reinterpret_cast<char*>(const_cast<uint8_t*>(ico_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, ico_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_ICO, image_type);

  // Test ICNS
  const uint8_t icns_data[] = {0x69, 0x63, 0x6E, 0x73, 0x00, 0x00, 0x00, 0x00};
  ObString icns_str(sizeof(icns_data), reinterpret_cast<char*>(const_cast<uint8_t*>(icns_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, icns_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_ICNS, image_type);

  // Test SGI (RLE format)
  const uint8_t sgi_data[] = {0x01, 0xDA, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  ObString sgi_str(sizeof(sgi_data), reinterpret_cast<char*>(const_cast<uint8_t*>(sgi_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, sgi_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_SGI, image_type);

  // Test SGI (uncompressed format)
  const uint8_t sgi_unc_data[] = {0x01, 0x52, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  ObString sgi_unc_str(sizeof(sgi_unc_data), reinterpret_cast<char*>(const_cast<uint8_t*>(sgi_unc_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_type_from_binary(allocator, sgi_unc_str, image_type));
  ASSERT_EQ(ObAiFuncImageUtils::IMAGE_TYPE_SGI, image_type);

  // Test unknown type
  const uint8_t unknown_data[] = {0x00, 0x01, 0x02, 0x03};
  ObString unknown_str(sizeof(unknown_data), reinterpret_cast<char*>(const_cast<uint8_t*>(unknown_data)));
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_INVALID_DATA, ObAiFuncImageUtils::get_type_from_binary(allocator, unknown_str, image_type));

  // Test empty input
  ObString empty_str;
  image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObAiFuncImageUtils::get_type_from_binary(allocator, empty_str, image_type));
}

TEST_F(ObAiFuncImageUtilsTest, test_get_image_type_str)
{
  // Test JPEG
  const char *type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_JPEG);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "jpeg"));

  // Test PNG
  type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_PNG);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "png"));

  // Test GIF
  type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_GIF);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "gif"));

  // Test WebP
  type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_WEBP);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "webp"));

  // Test BMP
  type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_BMP);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "bmp"));

  // Test TIFF
  type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_TIFF);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "tiff"));

  // Test ICO
  type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_ICO);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "x-icon"));

  // Test DIB (maps to bmp)
  type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_DIB);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "bmp"));

  // Test ICNS
  type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_ICNS);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "icns"));

  // Test SGI
  type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_SGI);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "sgi"));

  // Test unknown type
  type_str = ObAiFuncImageUtils::get_image_type_str(ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN);
  ASSERT_NE(nullptr, type_str);
  ASSERT_EQ(0, strcmp(type_str, "unknown"));
}

TEST_F(ObAiFuncImageUtilsTest, test_get_base64_data_uri_from_binary)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  // Test JPEG to base64 data URL
  const uint8_t jpeg_data[] = {0xFF, 0xD8, 0xFF, 0xE0};
  ObString jpeg_str(sizeof(jpeg_data), reinterpret_cast<char*>(const_cast<uint8_t*>(jpeg_data)));
  ObString data_url;
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, jpeg_str, data_url));
  ASSERT_TRUE(data_url.prefix_match("data:image/jpeg;base64,"));

  // Test PNG to base64 data URL
  const uint8_t png_data[] = {0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};
  ObString png_str(sizeof(png_data), reinterpret_cast<char*>(const_cast<uint8_t*>(png_data)));
  data_url.reset();
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, png_str, data_url));
  ASSERT_TRUE(data_url.prefix_match("data:image/png;base64,"));

  // Test GIF to base64 data URL
  const uint8_t gif_data[] = {0x47, 0x49, 0x46, 0x38};
  ObString gif_str(sizeof(gif_data), reinterpret_cast<char*>(const_cast<uint8_t*>(gif_data)));
  data_url.reset();
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, gif_str, data_url));
  ASSERT_TRUE(data_url.prefix_match("data:image/gif;base64,"));

  // Test WebP to base64 data URL
  const uint8_t webp_data[] = {0x52, 0x49, 0x46, 0x46, 0x08, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50};
  ObString webp_str(sizeof(webp_data), reinterpret_cast<char*>(const_cast<uint8_t*>(webp_data)));
  data_url.reset();
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, webp_str, data_url));
  ASSERT_TRUE(data_url.prefix_match("data:image/webp;base64,"));

  // Test BMP to base64 data URL
  const uint8_t bmp_data[] = {0x42, 0x4D, 0x00, 0x00};
  ObString bmp_str(sizeof(bmp_data), reinterpret_cast<char*>(const_cast<uint8_t*>(bmp_data)));
  data_url.reset();
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, bmp_str, data_url));
  ASSERT_TRUE(data_url.prefix_match("data:image/bmp;base64,"));

  // Test TIFF to base64 data URL
  const uint8_t tiff_data[] = {0x49, 0x49, 0x2A, 0x00};
  ObString tiff_str(sizeof(tiff_data), reinterpret_cast<char*>(const_cast<uint8_t*>(tiff_data)));
  data_url.reset();
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, tiff_str, data_url));
  ASSERT_TRUE(data_url.prefix_match("data:image/tiff;base64,"));

  // Test ICO to base64 data URL
  const uint8_t ico_data[] = {0x00, 0x00, 0x01, 0x00};
  ObString ico_str(sizeof(ico_data), reinterpret_cast<char*>(const_cast<uint8_t*>(ico_data)));
  data_url.reset();
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, ico_str, data_url));
  ASSERT_TRUE(data_url.prefix_match("data:image/x-icon;base64,"));

  // Test ICNS to base64 data URL
  const uint8_t icns_data[] = {0x69, 0x63, 0x6E, 0x73};
  ObString icns_str(sizeof(icns_data), reinterpret_cast<char*>(const_cast<uint8_t*>(icns_data)));
  data_url.reset();
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, icns_str, data_url));
  ASSERT_TRUE(data_url.prefix_match("data:image/icns;base64,"));

  // Test SGI to base64 data URL
  const uint8_t sgi_data[] = {0x01, 0xDA};
  ObString sgi_str(sizeof(sgi_data), reinterpret_cast<char*>(const_cast<uint8_t*>(sgi_data)));
  data_url.reset();
  ASSERT_EQ(OB_SUCCESS, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, sgi_str, data_url));
  ASSERT_TRUE(data_url.prefix_match("data:image/sgi;base64,"));

  // Test unknown type returns error
  const uint8_t unknown_data[] = {0x00, 0x01, 0x02, 0x03};
  ObString unknown_str(sizeof(unknown_data), reinterpret_cast<char*>(const_cast<uint8_t*>(unknown_data)));
  ASSERT_EQ(OB_INVALID_DATA, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, unknown_str, data_url));

  // Test empty input
  ObString empty_str;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, empty_str, data_url));
}


TEST_F(ObAiFuncImageUtilsTest, test_is_multi_model)
{
  // Empty model name -> false
  ObString empty;
  ASSERT_FALSE(ObAIFuncUtils::is_multi_model(empty));

  // Exact match (case-insensitive) against known multi-model names
  const char *qwen3_vl = "qwen3-vl-embedding";
  ASSERT_TRUE(ObAIFuncUtils::is_multi_model(ObString(strlen(qwen3_vl), qwen3_vl)));

  const char *qwen25_vl = "qwen2.5-vl-embedding";
  ASSERT_TRUE(ObAIFuncUtils::is_multi_model(ObString(strlen(qwen25_vl), qwen25_vl)));

  const char *tongyi_plus = "tongyi-embedding-vision-plus";
  ASSERT_TRUE(ObAIFuncUtils::is_multi_model(ObString(strlen(tongyi_plus), tongyi_plus)));

  const char *tongyi_flash = "tongyi-embedding-vision-flash";
  ASSERT_TRUE(ObAIFuncUtils::is_multi_model(ObString(strlen(tongyi_flash), tongyi_flash)));

  const char *multimodal_v1 = "multimodal-embedding-v1";
  ASSERT_TRUE(ObAIFuncUtils::is_multi_model(ObString(strlen(multimodal_v1), multimodal_v1)));

  // Case-insensitive exact match
  const char *qwen3_upper = "QWEN3-VL-EMBEDDING";
  ASSERT_TRUE(ObAIFuncUtils::is_multi_model(ObString(strlen(qwen3_upper), qwen3_upper)));

  // Not in list but contains "vl" -> true
  const char *custom_vl = "my-vl-embedding-model";
  ASSERT_TRUE(ObAIFuncUtils::is_multi_model(ObString(strlen(custom_vl), custom_vl)));

  // Not in list but contains "vision" -> true
  const char *custom_vision = "my-vision-embedding-model";
  ASSERT_TRUE(ObAIFuncUtils::is_multi_model(ObString(strlen(custom_vision), custom_vision)));

  // Not in list but contains "multimodal" -> true
  const char *custom_multimodal = "my-multimodal-embedding";
  ASSERT_TRUE(ObAIFuncUtils::is_multi_model(ObString(strlen(custom_multimodal), custom_multimodal)));

  // Contains "VL" (case-insensitive) -> true
  const char *custom_vl_upper = "some-VL-model";
  ASSERT_TRUE(ObAIFuncUtils::is_multi_model(ObString(strlen(custom_vl_upper), custom_vl_upper)));

  // Not in list and no "vl"/"vision"/"multimodal" -> false (e.g. "flash" alone is not a keyword)
  const char *custom_flash = "my-flash-embedding";
  ASSERT_FALSE(ObAIFuncUtils::is_multi_model(ObString(strlen(custom_flash), custom_flash)));

  // Non-multi: no "vl", no "vision", no "multimodal" -> false
  const char *text_embed = "text-embedding-ada-002";
  ASSERT_FALSE(ObAIFuncUtils::is_multi_model(ObString(strlen(text_embed), text_embed)));

  const char *bge_base = "bge-base-zh";
  ASSERT_FALSE(ObAIFuncUtils::is_multi_model(ObString(strlen(bge_base), bge_base)));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
