#include <boost/log/common.hpp>
#include <boost/log/core.hpp>
#include <boost/log/exceptions.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/program_options.hpp>
#include <dbWrapper.hpp>

#include "hashData.hpp"
#include "iostream"
namespace po = boost::program_options;

int main(int argc, char* argv[]) {
  po::options_description desc(
      "Usage:\n  dbcs [options] <path/to/input/storage.db>\nMust have options"); //описание скрипта для ком строки(название,
  desc.add_options()("help", "produce help message")( //c помощью метода эдд опшинс добавляю аргументы принимаемые скриптом
      "log-level", po::value<std::string>(), //лог-лвл - название аргумента,
      "info|warning|error \ndefault: error")("thread-count", po::value<int>(), //описание лог-лвла
                                             "default: count of logical "
                                             "core")("output", //путь для записи резов
                                                     po::value<std::string>(),
                                                     "<path/to/output/"
                                                     "storage.db>\ndefault:"
                                                     " <path/to/input/"
                                                     "dbcs-storage.db>");

  po::variables_map vm; //вэриэбл словарь который хранит аргументы ком строки, вм название слоаваря ключ для которого лог левел,например
  po::store(po::parse_command_line(argc, argv, desc), vm); //парсит и записывает данные в словрь
  po::notify(vm); //нотификейшн - уведомления

  if (vm.count("help")) { //возвращает дескрипшн если в ком строку вбить --хелп
    std::cout << desc << "\n";
    return 1;
  }

  std::string outPath; //объявляем переменные в которых будут хранится аргументы командной строки
  int threadNum;
  std::string logLevel = vm["log-level"].as<std::string>();
  if (vm["output"].empty()) { //если пользователь ничего не ввел то по умолчанию ставлю тмп аутпут
    outPath = "/tmp/outPut"; //аутпут папка для записи новой хэш бд
  } else {
    outPath = vm["output"].as<std::string>(); // если не пустая то устанавливаю введенное пользвотелем значене
  }
  if (vm["thread-count"].empty()) { // то же самое с кол-вом потоков
    threadNum = std::thread::hardware_concurrency(); //ставлю кол-во потоков равным ядрам если ничего не ввел пользователь
  } else {
    threadNum = vm["thread-count"].as<int>(); //иначе колво потоков введение юзером
  };
  srand((int)time(nullptr)); //заполняем случайными числам базу

  rocksMapHasher hasher = rocksMapHasher(threadNum); //объект хэшер хэширует базу, передаю кол-во потоков ему
  rocksdbWrapper db = rocksdbWrapper(10, 10, "/tmp/database", hasher); //конструктор,создаю объект бд для создания новой бд, в аргументы кол-во ключей, количество семейств, путь где создать бд, передаю ссылку на объект класса хэшер для реализации продусерконсамер
  db.createDatabase(); //методы для создания бд (пустая бд)
  db.getFamiliesFromBD(); //получение имен семейств
  db.pushData(); //заполняю каждое семейство
  std::map<std::string, std::map<std::string, std::string>> mapa; //словарь для хранения бд, ключ это название семейства, значение - содержание семейства
  std::vector<std::string> fams = db.getFamilyNum(); //создаю массив строк в котором хранится названия семейств

  db.migrateDataToMap(logLevel); //переношу данные из бд в словарь (мапа)

  rocksdbWrapper outputDB =
      rocksdbWrapper(hasher.getHashedMap(), outPath, hasher); //объявляю новую бд в которую буду загружать хэшированную старую бд
  outputDB.createOutputDatabase(); //создаю бд с хэш ф-иями
}