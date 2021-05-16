#include "hashData.hpp"

#include <boost/log/common.hpp>
#include <boost/log/core.hpp>
#include <boost/log/exceptions.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>

#include "iostream"
#include "picosha2.h"

boost::log ::trivial::severity_level whatIsLevel(std::string logLevel) { //устанавливаю уровень логирования
  if (logLevel.empty()) {
    return boost::log::trivial::severity_level::error;
  } else if (logLevel == "warning") {
    return boost::log::trivial::severity_level::warning;
  } else if (logLevel == "info") {
    return boost::log::trivial::severity_level::info;
  } else
    throw "invalid log level";
}
void rocksMapHasher::hashStorage(std::string familyName,
                                 std::map<std::string, std::string> kvStorage,
                                 std::string logLevel) {
  std::map<std::string, std::string> hashed; //создаю словарь где будут хранится хэшированные данные (квСторэдж это не хэшированные данные)
  for (auto const& kv : kvStorage) { //иду по каждому эл-у словаря и хэширую их
    std::string hash_hex_str;//строка для хэшированной строки
    picosha2::hash256_hex_string(kv.first + kv.second, hash_hex_str);//хэш256.... хэширует, 1арг строка которую будем хэшировать,2арг ссылка на переменную куда положу хэшированные данные
    hashed[kv.first] = hash_hex_str; //в словрь хэшт добавляю новый элемент, причем квФёрст ключ из нехэшированной базы, значение этого ключа будет уже хэш стркоа

    boost::log::core::get()->set_filter(boost::log::trivial::severity >=
                                        whatIsLevel(logLevel));

    BOOST_LOG_TRIVIAL(info) << "Family " << familyName << "->" //лог вывожу для вывода в консоль(вывод в консоль результата лабы)

                            << kv.first << "  hashed";
  }
  mutex.lock();
  hashedMap_[familyName] = hashed; //заполняю словрь бд хэшированными семействами (бд где значения это семейства)
  hashed.clear(); //очищаю словарь семейств
  mutex.unlock(); //разблокирую мутекс
}

void rocksMapHasher::startHashing(std::string familyName, //
                                  std::map<std::string, std::string> kvStorage,
                                  std::string logLevel) {
  familyPool_.enqueue([this, familyName, kvStorage, logLevel]() { //добавляю задачу в пулл потоков через лямбда ф-ию (начинается с кв скобок) передаю сам объект класса роксмэпхэшер, название семейства, нехэированные данные одного семейства,уровень логирования
    this->hashStorage(familyName, kvStorage, logLevel); //просто вызываю метод хэш сторэдж
  });
}
