macro(ob_def_error ERR_ID ERR_MSG)
  if (NOT DEFINED ${ERR_ID})
    set(${ERR_ID} ${ERR_MSG})
  endif()
endmacro()

ob_def_error(E1001 "[E1001] Header files are not allowed in CMakeLists.txt")

