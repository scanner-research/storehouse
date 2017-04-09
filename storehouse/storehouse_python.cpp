#include <boost/python.hpp>
#include "storehouse/storage_backend.h"
#include "storehouse/storage_config.h"

using namespace storehouse;
namespace bp = boost::python;

void translate_exception(const StoreResult& result) {
  PyErr_SetString(PyExc_UserWarning, store_result_to_string(result).c_str());
}

void attempt(StoreResult result) {
  if (result != StoreResult::Success) {
    throw result;
  }
}

RandomReadFile* make_random_read_file(StorageBackend* backend,
                                      const std::string& name) {
  RandomReadFile* file;
  attempt(backend->make_random_read_file(name, file));
  return file;
}

WriteFile* make_write_file(StorageBackend* backend, const std::string& name) {
  WriteFile* file;
  attempt(backend->make_write_file(name, file));
  return file;
}

std::string r_read(RandomReadFile* file) {
  std::vector<uint8_t> data;
  uint64_t size;
  attempt(file->get_size(size));
  attempt(file->read(0, size, data));
  return std::string(data.begin(), data.end());
}

uint64_t r_get_size(RandomReadFile* file) {
  uint64_t size;
  attempt(file->get_size(size));
  return size;
}

void w_append(WriteFile* file, const std::string& data) {
  attempt(file->append(data.size(), (const uint8_t*)data.c_str()));
}

void w_save(WriteFile* file) { attempt(file->save()); }

std::string read_all_file(StorageBackend* backend, const std::string& name) {
  RandomReadFile* file = make_random_read_file(backend, name);
  std::string contents = r_read(file);
  delete file;
  return contents;
}

void write_all_file(StorageBackend* backend, const std::string& name,
                    const std::string& data) {
  WriteFile* file = make_write_file(backend, name);
  w_append(file, data);
  w_save(file);
  delete file;
}

void make_dir(StorageBackend* backend, const std::string& name) {
  attempt(backend->make_dir(name));
}

FileInfo get_file_info(StorageBackend* backend, const std::string& name) {
  FileInfo file_info;
  backend->get_file_info(name, file_info);
  return file_info;
}

void delete_file(StorageBackend* backend, const std::string& name) {
  attempt(backend->delete_file(name));
}

void delete_dir(StorageBackend* backend, const std::string& name) {
  attempt(backend->delete_dir(name));
}

BOOST_PYTHON_MODULE(libstorehouse) {
  using namespace bp;
  register_exception_translator<StoreResult>(translate_exception);
  
  StoreResult (RandomReadFile::*rrf_read)(
    uint64_t, size_t, std::vector<uint8_t>&) = &RandomReadFile::read;
  
  class_<StorageConfig>("StorageConfig", no_init)
    .def("make_posix_config", &StorageConfig::make_posix_config,
         return_value_policy<manage_new_object>())
    .staticmethod("make_posix_config")
    .def("make_s3_config", &StorageConfig::make_s3_config,
         return_value_policy<manage_new_object>())
    .staticmethod("make_s3_config")
    .def("make_gcs_config", &StorageConfig::make_gcs_config,
         return_value_policy<manage_new_object>())
    .staticmethod("make_gcs_config");

  class_<FileInfo>("FileInfo")
    .add_property("size", &FileInfo::size)
    .add_property("file_exists", &FileInfo::file_exists)
    .add_property("file_is_folder", &FileInfo::file_is_folder);

  class_<StorageBackend, boost::noncopyable>("StorageBackend", no_init)
    .def("make_from_config", &StorageBackend::make_from_config,
         return_value_policy<manage_new_object>())
    .staticmethod("make_from_config")
    .def("make_random_read_file", &make_random_read_file,
         return_value_policy<manage_new_object>())
    .def("make_write_file", &make_write_file,
         return_value_policy<manage_new_object>())
    .def("get_file_info", &get_file_info)
    .def("read", &read_all_file)
    .def("write", &write_all_file)
    .def("make_dir", &make_dir)
    .def("delete_file", &delete_file)
    .def("delete_dir", &delete_dir);

  class_<RandomReadFile, boost::noncopyable>("RandomReadFile", no_init)
    .def("read", &r_read)
    .def("get_size", &r_get_size);

  class_<WriteFile, boost::noncopyable>("WriteFile", no_init)
    .def("append", &w_append)
    .def("save", &w_save);

  enum_<StoreResult>("StoreResult");
}
