#ifndef INCLUDE_HTMLDOWNLOADER_HPP_
#define INCLUDE_HTMLDOWNLOADER_HPP_
#include <boost/log/common.hpp>
#include <boost/log/core.hpp>
#include <boost/log/exceptions.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <map>

#include "ThreadPool.h"
#include "iostream"
#include "map"
#include "mutex"
#include "picosha2.h"
#include "utility"
class rocksMapHasher {
 private:
  std::map<std::string, std::map<std::string, std::string>> hashedMap_; //словрь в который я записываю хэшированную бд
  ThreadPool familyPool_; //пуллпотоков для многопоточности, туда пишу задачи, когда прочитаю семейства в дб врапере и добавляю задачу в пулл потоков
  std::mutex mutex; //блокировка потока когда писать в словарь с хэшированными данными(потому что несклько потоков одновременно могут писать туда)(в хэштмэп)

 public:
  std::map<std::string, std::map<std::string, std::string>> getHashedMap() { //возвращает хэшированнй словарь
    return hashedMap_;
  }
  void startHashing(std::string fimilyName, //вызываю с дб врапера, когда прочитал семейство запускаю этот метод чтобы начать хэшировать
                    std::map<std::string, std::string> kvStorage, //1арг - название семейства, 2арг- словарь в котором хранятся данные семейства
                    std::string logLevel); //уровень логирования
  explicit rocksMapHasher(int threadNum) : familyPool_(threadNum) {} //конструктор,передаю ему кол-во потоков максимальное способное одновремено обрабатывать семейства, сразу инициализирую поле этого класса
  void hashStorage(std::string familyName, //метод для хэширования
                   std::map<std::string, std::string> kvStore,
                   std::string logLevel);
};
#endif  // INCLUDE_HTMLDOWNLOADER_HPP_