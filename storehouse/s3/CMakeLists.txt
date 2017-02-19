set(SOURCE_FILES
  s3_storage.cpp)

add_library(s3_storage_lib OBJECT
  ${SOURCE_FILES})

foreach(MODULE ${AWS_MODULES})
  get_target_property(INCLUDE_DIRS aws-cpp-sdk-${MODULE} INTERFACE_INCLUDE_DIRECTORIES)
  target_include_directories(s3_storage_lib PUBLIC ${INCLUDE_DIRS})
endforeach()
