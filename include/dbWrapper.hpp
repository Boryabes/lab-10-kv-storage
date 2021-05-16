// Copyright 2021 Boris Bespalkov <box.bern@yandex.ru>
//обертка над роксдб,
#ifndef INCLUDE_DBWRAPPER_HPP_
#define INCLUDE_DBWRAPPER_HPP_
#include <assert.h>

#include <boost/log/common.hpp>
#include <boost/log/core.hpp>
#include <boost/log/exceptions.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <utility>
#include <vector>
#include <map>
#include <string>
#include "ThreadPool.h"
#include "hashData.hpp"
#include "iostream"
#include "rocksdb/db.h"
class rocksdbWrapper {
 private:
  int columnSize_; //кол-во ключей
  int familyNum_; //кол-во семейств
  std::string path_; //путь где будет хранится бд
  rocksdb::DB* db_{}; //указатель по которому будет хранится бд (методу роксдбопен передаю указатель)
  std::vector<std::string> families_; //список семейств
  rocksMapHasher& hasherObj_;// ссылка на объекту которму передаю прочитанное семейство
  std::map<std::string, std::map<std::string, std::string>> mapa_; //словарь представляет базу данных

 public:
  rocksdbWrapper(int columns, int family, std::string path, //консткуртор для создания пустой бд
                 rocksMapHasher& hasher)
      : columnSize_(columns),
        familyNum_(family),
        path_(std::move(path)),
        hasherObj_(hasher) {} //ссылка на объект класса роксмэпхэшер
  rocksdbWrapper(std::map<std::string, std::map<std::string, std::string>> mapa,
                 std::string path, rocksMapHasher& hasher) //передаю мапу и из этого словаря создаю новую бд с заполненными значениями из этого словаря
      : columnSize_(0),
        familyNum_(mapa.size()),
        path_(path),
        hasherObj_(hasher),
        mapa_(mapa){}
  void createOutputDatabase();
  std::vector<std::string> getFamilyNum();
  void createDatabase();
  void createDatabase(
      std::map<std::string, std::map<std::string, std::string>> mapa);
  void getFamiliesFromBD();
  void pushData();
  void migrateDataToMap(std::string logLevel);
};

#endif  // INCLUDE_DBWRAPPER_HPP_