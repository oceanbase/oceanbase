add_library(oblib_base_base INTERFACE)

message(STATUS "check deps for oblib")

get_property(EASY_INCLUDE_DIRS GLOBAL PROPERTY "EASY_INCLUDE_DIRS" )
target_include_directories(
  oblib_base_base INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${EASY_INCLUDE_DIRS}
  ${DEP_DIR}/include
)

if (OB_USE_CLANG)
  target_compile_options(oblib_base_base
    INTERFACE
    -fmax-type-align=8
    -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -D_NO_EXCEPTION 
    -Wall -Wextra -Wformat -Wno-deprecated
    -fno-omit-frame-pointer ${MARCH_CFLAGS} ${MTUNE_CFLAGS}
    -Wno-address-of-packed-member -Wno-sign-compare -Werror 
    -Wno-inconsistent-missing-override -fno-delete-null-pointer-checks
    -Wno-delete-non-virtual-dtor -Wno-dynamic-class-memaccess
    -Wno-format-security -Wno-reserved-user-defined-literal -Wno-sometimes-uninitialized
    -Wno-unused-value -Wno-self-assign -Wno-overloaded-virtual
    -Wno-unused-private-field -Wno-mismatched-tags -Wno-unused-variable
    -Wno-invalid-offsetof -Wno-tautological-compare -Wno-psabi -Wno-c99-designator)
else()
  target_compile_options(oblib_base_base
    INTERFACE
    -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS
    -D_NO_EXCEPTION -Wall -Wno-unused-parameter -Wextra -Wformat -Wno-deprecated
    -fno-strict-aliasing -fno-omit-frame-pointer ${MARCH_CFLAGS} ${MTUNE_CFLAGS}
    -Wno-psabi -Wno-sign-compare -Wno-unused-variable
    $<$<COMPILE_LANGUAGE:C>:-Wno-old-style-declaration>
    $<$<COMPILE_LANGUAGE:CXX>:-Wno-literal-suffix -Wno-invalid-offsetof>)
endif()

target_compile_features(oblib_base_base INTERFACE cxx_std_11)
target_link_libraries(oblib_base_base
  INTERFACE
  -L${DEP_DIR}/lib
  -l:libisal.a -l:libunwind.a -lpthread -ldl -lrt -l:libssl.a -l:libcrypto.a -l:libcurl.a ${ARCH_LDFLAGS})

add_library(oblib_base INTERFACE)
target_compile_options(oblib_base INTERFACE -D_GLIBCXX_USE_CXX11_ABI=0)
target_link_libraries(oblib_base INTERFACE oblib_base_base easy_base)


#PCH
if (OB_ENABLE_LIB_PCH)
  add_library(lib_pch OBJECT common/dummy.cpp)
  target_link_libraries(lib_pch oblib_base)
  set(ob_lib_pchs "" CACHE INTERNAL "ob library precompiled headers")
endif()

function(ob_lib_add_pchs rpath)
  if (OB_ENABLE_LIB_PCH)
    FOREACH(item ${ARGN})
      list(APPEND ob_lib_pchs ${rpath}/${item})
    ENDFOREACH(item)
    set(ob_lib_pchs ${ob_lib_pchs} CACHE INTERNAL "ob library precompiled headers")
  endif()
endfunction()

function(enable_lib_pch target)
  if (OB_ENABLE_LIB_PCH)
    target_precompile_headers(${target} REUSE_FROM lib_pch)
  endif()
endfunction()

set(oblib_object_libraries "" CACHE INTERNAL "ob library list")
function(ob_lib_add_target target)
  message(STATUS "oblib_add_target ${target}")
  ob_add_target(${ARGV})
  target_link_libraries(${target} PUBLIC oblib_base)
  list(APPEND oblib_object_libraries ${target})
  set(oblib_object_libraries "${oblib_object_libraries}" CACHE INTERNAL "observer library list")
  enable_lib_pch(${target})
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

if (OB_ENABLE_LIB_PCH)
  target_precompile_headers(oblib_base INTERFACE ${ob_lib_pchs})
endif()

# Define static library which contains previous object libraries.
set_source_files_properties(${oblib_extra_objects} PROPERTIES GENERATED TRUE)
add_library(oblib STATIC ${oblib_extra_objects})
target_link_libraries(oblib PUBLIC oblib_base ${oblib_object_libraries})

if (NOCOROUTINE)
  target_compile_definitions(oblib_base_base INTERFACE NOCOROUTINE=1)
endif()
