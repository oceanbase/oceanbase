add_library(oblib_base INTERFACE)
add_library(oblib_base_without_pass INTERFACE)
add_library(oblib_pass INTERFACE)

if(NOT CPP_STANDARD_20)
  target_compile_options(oblib_base_without_pass INTERFACE -D_GLIBCXX_USE_CXX11_ABI=0)
endif()

get_property(EASY_INCLUDE_DIRS GLOBAL PROPERTY "EASY_INCLUDE_DIRS" )

target_include_directories(
  oblib_base_without_pass INTERFACE
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_SOURCE_DIR}/src/plugin/include
  ${CMAKE_SOURCE_DIR}/deps/easy/src
  ${CMAKE_SOURCE_DIR}/deps/oblib/src
  ${CMAKE_SOURCE_DIR}/deps/oblib/src/common
  ${CMAKE_SOURCE_DIR}/deps/easy/src/include
  ${CMAKE_SOURCE_DIR}/deps/ussl-hook
  ${CMAKE_SOURCE_DIR}/deps/ussl-hook/loop
  ${CMAKE_SOURCE_DIR}/src/objit/include
  ${CMAKE_SOURCE_DIR}/src/objit/src
  ${DEP_DIR}/include
  ${DEP_DIR}/include/libxml2
  ${DEVTOOLS_DIR}
  ${DEVTOOLS_DIR}/include
  ${DEP_DIR}/include/apr-1/
  ${DEP_DIR}/include/icu
  ${DEP_DIR}/include/icu/common
  ${DEP_DIR}/include/apache-arrow
  ${DEP_DIR}/include/apache-orc
  ${DEP_DIR}/include/fast_float
  ${USSL_INCLUDE_DIRS}
)

if(OB_BUILD_OPENSOURCE)
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${DEP_DIR}/include/mariadb
    )
else()
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${DEP_3RD_DIR}/usr/local/include
    ${DEP_3RD_DIR}/usr/include/
    )
endif()

if(OB_BUILD_SPM)
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${CMAKE_SOURCE_DIR}/close_modules/spm
  )
endif()


if(OB_BUILD_DBLINK)
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${DEP_3RD_DIR}/u01/obclient/include
    ${CMAKE_SOURCE_DIR}/close_modules/dblink/deps/oblib/src
    ${OCI_DEVEL_INC}
  )
endif()

if(OB_BUILD_ARBITRATION)
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${CMAKE_SOURCE_DIR}/close_modules/arbitration
  )
endif()

if(OB_BUILD_SHARED_STORAGE)
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${CMAKE_SOURCE_DIR}/close_modules/shared_storage/
  )
endif()

if(OB_BUILD_LOG_STORAGE_COMPRESS)
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${CMAKE_SOURCE_DIR}/close_modules/log_storage_compress
  )
endif()


if(OB_BUILD_ORACLE_PL)
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${CMAKE_SOURCE_DIR}/close_modules/oracle_pl
  )
endif()

if(OB_BUILD_TDE_SECURITY)
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${CMAKE_SOURCE_DIR}/close_modules/tde_security
  )
endif()

if(OB_BUILD_AUDIT_SECURITY)
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${CMAKE_SOURCE_DIR}/close_modules/audit_security
  )
endif()

if(OB_USE_BABASSL)
  target_include_directories(
    oblib_base_without_pass INTERFACE
    ${DEP_3RD_DIR}/usr/local/babassl-ob/include
    )
endif()

if (OB_USE_CLANG)
  # The following clang warnings should be fixed later: -Wno-unused-variable -Wno-invalid-offsetof
  set(OBLIB_COMPILE_DEFINITIONS -fno-strict-aliasing -fno-omit-frame-pointer ${MARCH_CFLAGS} ${MTUNE_CFLAGS}
    -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -D_NO_EXCEPTION
    -DOCI_LINK_RUNTIME
    -Wall -Wextra -Wformat -Werror
    -Wno-deprecated -Wno-address-of-packed-member -Wno-sign-compare -Wno-tautological-compare
    -Wno-psabi -Wno-c99-designator -Wno-int-in-bool-context -Wno-sizeof-array-div
    -Wno-implicit-const-int-float-conversion -Wno-fortify-source -Wno-non-c-typedef-for-linkage
    -Wno-uninitialized-const-reference -Wno-unused-function -Wno-atomic-alignment
    -Wno-tautological-constant-out-of-range-compare -Wno-unused-parameter
    -Wno-string-plus-int -Wno-misleading-indentation -Wno-unused-private-field
    $<$<COMPILE_LANGUAGE:CXX>:-Wno-overloaded-virtual -Wno-reserved-user-defined-literal
    -Wno-self-assign -Wno-inconsistent-missing-override -Wno-mismatched-tags -Wno-unused-variable
    -Wno-invalid-offsetof -Wno-unevaluated-expression -Wno-constant-logical-operand>
    $<$<COMPILE_LANGUAGE:C>:-Wno-unused-parameter>)
  if (CPP_STANDARD_20)
    set(OBLIB_COMPILE_DEFINITIONS ${OBLIB_COMPILE_DEFINITIONS}
        -Wno-unused-but-set-variable -Wno-null-pointer-subtraction -Wno-compound-token-split-by-macro
        -Wno-packed-non-pod -Wno-format -Wno-shorten-64-to-32 -Wno-unused-but-set-parameter -Wno-single-bit-bitfield-constant-conversion
        -Wno-enum-constexpr-conversion -Wno-covered-switch-default
        -Wno-bitwise-instead-of-logical -Wno-suggest-override -Wno-non-virtual-dtor -Wno-cast-qual
        -Wno-unused-value -Wno-macro-redefined -Wno-reinterpret-base-class -Wno-register
        -Wno-implicit-fallthrough -Wno-deprecated-non-prototype
        -Wno-ambiguous-reversed-operator -Wno-invalid-partial-specialization
        -Wno-string-concatenation)
  endif()
  target_compile_options(oblib_base_without_pass INTERFACE ${OBLIB_COMPILE_DEFINITIONS})
  if (ENABLE_SANITY)
    target_compile_options(oblib_pass
      INTERFACE
      "$<$<COMPILE_LANGUAGE:CXX>:SHELL:-Xclang -load -Xclang ${DEVTOOLS_DIR}/lib64/libsanitypass.so>"
      -fno-builtin-memset
      -fno-builtin-bzero
      -fno-builtin-memcpy
      -fno-builtin-memmove
      -fno-builtin-memcmp
      -fno-builtin-strlen
      -fno-builtin-strnlen
      -fno-builtin-strcpy
      -fno-builtin-strncpy
      -fno-builtin-strcmp
      -fno-builtin-strncmp
      -fno-builtin-strcasecmp
      -fno-builtin-strncasecmp
      -fno-builtin-vsprintf
      -fno-builtin-vsnprintf
      -fno-builtin-sprintf
      -fno-builtin-snprintf
    )
  endif()
else()
  target_compile_options(oblib_base_without_pass
    INTERFACE
    -fno-strict-aliasing -fno-omit-frame-pointer ${MARCH_CFLAGS} ${MTUNE_CFLAGS}
    -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -D_NO_EXCEPTION
    -DOCI_LINK_RUNTIME
    -Wall -Wextra -Wformat -Werror
    -Wno-deprecated
    -Wno-sign-compare
    -Wno-psabi
    -Wno-attributes
    -Wno-nonnull-compare
    -Wno-parentheses
    -Wno-implicit-fallthrough
    -Wno-switch
    -Wno-address-of-packed-member
    -Wno-ignored-qualifiers
    -Wno-format
    -Wno-type-limits
    -Wno-format-truncation
    -Wno-int-in-bool-context
    -Wno-cast-function-type
    -Wno-unused-function
    -Wno-overflow
    -Wno-unused-but-set-variable
    -Wno-memset-elt-size
    $<$<COMPILE_LANGUAGE:CXX>:
    -Wno-literal-suffix
    -Wno-invalid-offsetof
    -Wno-unused-variable
    -Wno-deprecated-copy
    -Wno-class-memaccess
    -Wno-unused-parameter
    -Wno-unused-but-set-variable
    -Wno-pragmas
    -Wno-maybe-uninitialized
    -Wno-subobject-linkage>
    $<$<COMPILE_LANGUAGE:C>:
    -Wno-unused-parameter>
    )
endif()

target_compile_features(oblib_base INTERFACE cxx_std_11)

set(OBLIB_ABSEIL_LIST
    ${DEP_DIR}/lib64/libabsl_base.a
    ${DEP_DIR}/lib64/libabsl_city.a
    ${DEP_DIR}/lib64/libabsl_debugging_internal.a
    ${DEP_DIR}/lib64/libabsl_demangle_internal.a
    ${DEP_DIR}/lib64/libabsl_hash.a
    ${DEP_DIR}/lib64/libabsl_int128.a
    ${DEP_DIR}/lib64/libabsl_low_level_hash.a
    ${DEP_DIR}/lib64/libabsl_malloc_internal.a
    ${DEP_DIR}/lib64/libabsl_raw_hash_set.a
    ${DEP_DIR}/lib64/libabsl_raw_logging_internal.a
    ${DEP_DIR}/lib64/libabsl_spinlock_wait.a
    ${DEP_DIR}/lib64/libabsl_stacktrace.a
    ${DEP_DIR}/lib64/libabsl_str_format_internal.a
    ${DEP_DIR}/lib64/libabsl_strings.a
    ${DEP_DIR}/lib64/libabsl_strings_internal.a
    ${DEP_DIR}/lib64/libabsl_symbolize.a
    ${DEP_DIR}/lib64/libabsl_synchronization.a
    ${DEP_DIR}/lib64/libabsl_throw_delegate.a
    ${DEP_DIR}/lib64/libabsl_time.a
    ${DEP_DIR}/lib64/libabsl_time_zone.a)

if(OB_BUILD_OPENSOURCE)
set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -lmariadb")
if (OB_STATIC_LINK_LGPL_DEPS)
  set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -l:libmariadbclient.a")
endif()
target_link_libraries(oblib_base_without_pass
  INTERFACE
  oss
  easy
  ${DEP_DIR}/lib/libcurl.a
  ${DEP_DIR}/lib/libssl.a
  ${DEP_DIR}/lib/libcrypto.a
  ${DEP_DIR}/lib64/libs2.a
  ${OBLIB_ABSEIL_LIST}
  ${DEP_DIR}/lib/libroaring.a
  ${LGPL_DEPS}
  $<$<STREQUAL:"${ARCHITECTURE}","x86_64">:${DEP_DIR}/lib/libunwind.a>
  ${DEP_DIR}/lib/libz.a
  ${DEP_DIR}/lib/libicui18n.a
  ${DEP_DIR}/lib/libicustubdata.a
  ${DEP_DIR}/lib/libicuuc.a
  ${DEP_DIR}/lib/libprotobuf-c.a
  ${DEP_DIR}/lib64/libarrow.a
  ${DEP_DIR}/lib64/libparquet.a
  ${DEP_DIR}/lib64/libarrow_bundled_dependencies.a
  ${DEP_DIR}/lib64/liborc.a
  ${DEP_DIR}/lib64/libsnappy.a
  ${DEP_DIR}/lib64/libprotoc.a
  ${DEP_DIR}/lib64/libprotobuf.a
  ${DEP_DIR}/lib64/liblz4.a
  ${DEP_DIR}/lib64/libzstd.a
  -L${DEP_DIR}/var/usr/lib64
  -L${DEP_DIR}/var/usr/lib
  -L${DEP_3RD_DIR}/usr/lib
  -L${DEP_3RD_DIR}/usr/lib64
  -laio -lpthread -ldl -lrt
  ${ARCH_LDFLAGS}
  ${DEP_DIR}/lib/libisal.a
 )
else()
set(ignoreMe OB_STATIC_LINK_LGPL_DEPS)
target_link_libraries(oblib_base_without_pass
  INTERFACE
  oss
  easy
  $<$<STREQUAL:"${ARCHITECTURE}","x86_64">:${DEP_DIR}/lib/libunwind.a>
  ${DEP_DIR}/lib/libcurl.a
  ${DEP_3RD_DIR}/usr/local/babassl-ob/lib/libssl.a
  ${DEP_3RD_DIR}/usr/local/babassl-ob/lib/libcrypto.a
  ${DEP_3RD_DIR}/u01/obclient/lib/libobclnt.a
  ${DEP_DIR}/lib64/libs2.a
  ${OBLIB_ABSEIL_LIST}
  ${DEP_DIR}/lib/libroaring.a
  ${DEP_DIR}/lib/libz.a
  ${DEP_DIR}/lib/libicui18n.a
  ${DEP_DIR}/lib/libicustubdata.a
  ${DEP_DIR}/lib/libicuuc.a
  ${DEP_DIR}/lib/libprotobuf-c.a
  ${DEP_DIR}/lib64/libarrow.a
  ${DEP_DIR}/lib64/libparquet.a
  ${DEP_DIR}/lib64/libarrow_bundled_dependencies.a
  ${DEP_DIR}/lib64/liborc.a
  ${DEP_DIR}/lib64/libsnappy.a
  ${DEP_DIR}/lib64/libprotoc.a
  ${DEP_DIR}/lib64/libprotobuf.a
  ${DEP_DIR}/lib64/liblz4.a
  ${DEP_DIR}/lib64/libzstd.a
  -L${DEP_DIR}/var/usr/lib64
  -L${DEP_DIR}/var/usr/lib
  -L${DEP_3RD_DIR}/usr/lib
  -L${DEP_3RD_DIR}/usr/lib64
  -laio -lpthread -ldl -lrt
  ${ARCH_LDFLAGS}
  ${DEP_DIR}/lib/libisal.a
 )
endif()
target_link_libraries(oblib_base INTERFACE oblib_base_without_pass oblib_pass)

function(oblib_add_library target)
  message(STATUS "oblib_add_library ${target}")
  add_library(${target} OBJECT ${ARGN})
  list(APPEND oblib_object_libraries ${target})
  set(oblib_object_libraries
    "${oblib_object_libraries}" CACHE INTERNAL oblib_object_libraries)
endfunction()

function(oblib_add_extra_objects)
  list(APPEND oblib_extra_objects ${ARGN})
  set(oblib_extra_objects "${oblib_extra_objects}" CACHE INTERNAL oblib_extra_objects)
endfunction()

set(oblib_object_libraries "" CACHE INTERNAL oblib_object_libraries)
set(oblib_extra_objects "" CACHE INTERNAL oblib_extra_objects)

add_subdirectory(lib)
add_subdirectory(common)
add_subdirectory(rpc)

# Define static library which contains previous object libraries.
message(STATUS "oblib_extra_objects ${oblib_extra_objects}")
set_source_files_properties(${oblib_extra_objects} PROPERTIES GENERATED TRUE)
add_library(oblib STATIC ${oblib_extra_objects})
target_link_libraries(oblib PUBLIC oblib_base ${oblib_object_libraries})
