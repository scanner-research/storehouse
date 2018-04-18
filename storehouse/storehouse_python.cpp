#include <pybind11/pybind11.h>
#include "storehouse/storage_backend.h"
#include "storehouse/storage_config.h"

using namespace storehouse;
namespace py = pybind11;

namespace {
class GILRelease {
 public:
  inline GILRelease() {
    PyEval_InitThreads();
    m_thread_state = PyEval_SaveThread();
  }

  inline ~GILRelease() {
    PyEval_RestoreThread(m_thread_state);
    m_thread_state = NULL;
  }

 private:
  PyThreadState* m_thread_state;
};
}

class StorehouseException : public std::exception {
public:
  StorehouseException(StoreResult result) : message(store_result_to_string(result)) {}
  virtual const char * what() const noexcept override {
    return message.c_str();
  }
private:
  std::string message;
};

void attempt(StoreResult result) {
  if (result != StoreResult::Success) {
    throw StorehouseException(result);
  }
}

RandomReadFile* make_random_read_file(StorageBackend* backend,
                                      const std::string& name) {
  GILRelease r;
  RandomReadFile* file;
  attempt(backend->make_random_read_file(name, file));
  return file;
}

WriteFile* make_write_file(StorageBackend* backend, const std::string& name) {
  GILRelease r;
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

std::string wrapper_r_read(RandomReadFile* file) {
  GILRelease r;
  return r_read(file);
}

std::string wrapper_r_read_offset(RandomReadFile* file, uint64_t offset,
                                  uint64_t size) {
  GILRelease r;
  std::vector<uint8_t> data;
  attempt(file->read(offset, size, data));
  return std::string(data.begin(), data.end());
}

uint64_t r_get_size(RandomReadFile* file) {
  GILRelease r;
  uint64_t size;
  attempt(file->get_size(size));
  return size;
}

void w_append(WriteFile* file, const std::string& data) {
  GILRelease r;
  attempt(file->append(data.size(), (const uint8_t*)data.c_str()));
}

void w_save(WriteFile* file) {
  GILRelease r;
  attempt(file->save());
}

std::string read_all_file(StorageBackend* backend, const std::string& name) {
  GILRelease r;
  RandomReadFile* file;
  attempt(backend->make_random_read_file(name, file));
  std::string contents = r_read(file);
  delete file;
  return contents;
}

void write_all_file(StorageBackend* backend, const std::string& name,
                    const std::string& data) {
  GILRelease r;
  WriteFile* file;
  attempt(backend->make_write_file(name, file));
  attempt(file->append(data.size(), (const uint8_t*)data.c_str()));
  attempt(file->save());
  delete file;
}

void make_dir(StorageBackend* backend, const std::string& name) {
  GILRelease r;
  attempt(backend->make_dir(name));
}

FileInfo get_file_info(StorageBackend* backend, const std::string& name) {
  GILRelease r;
  FileInfo file_info;
  backend->get_file_info(name, file_info);
  return file_info;
}

void delete_file(StorageBackend* backend, const std::string& name) {
  GILRelease r;
  attempt(backend->delete_file(name));
}

void delete_dir(StorageBackend* backend, const std::string& name) {
  GILRelease r;
  attempt(backend->delete_dir(name));
}

PYBIND11_MODULE(libstorehouse, m) {
  m.doc() = "Storehouse C library";

  py::register_exception<StorehouseException>(m, "StorehouseException");

  py::class_<StorageConfig>(m, "StorageConfig")
    .def_static("make_posix_config", &StorageConfig::make_posix_config)
    .def_static("make_s3_config", &StorageConfig::make_s3_config)
    .def_static("make_gcs_config", &StorageConfig::make_gcs_config);

  py::class_<FileInfo>(m, "FileInfo")
    .def_readonly("size", &FileInfo::size)
    .def_readonly("file_exists", &FileInfo::file_exists)
    .def_readonly("file_is_folder", &FileInfo::file_is_folder);

  py::class_<StorageBackend>(m, "StorageBackend")
    .def_static("make_from_config", &StorageBackend::make_from_config)
    .def("make_random_read_file", &make_random_read_file)
    .def("make_write_file", &make_write_file)
    .def("get_file_info", &get_file_info)
    .def("read", &read_all_file)
    .def("write", &write_all_file)
    .def("make_dir", &make_dir)
    .def("delete_file", &delete_file)
    .def("delete_dir", &delete_dir);

  py::class_<RandomReadFile>(m, "RandomReadFile")
    .def("read", &wrapper_r_read)
    .def("read_offset", &wrapper_r_read_offset)
    .def("get_size", &r_get_size);

  py::class_<WriteFile>(m, "WriteFile")
    .def("append", &w_append)
    .def("save", &w_save);
}
