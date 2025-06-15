#include "../../include/server/DatabaseAPI.hpp"
#include <filesystem> // 用于文件和目录操作 (需要C++17)
#include <fstream>    // 用于文件读写
#include <iostream>   // 用于在控制台打印错误信息
#include <sstream>    // 用于 std::stringstream
#include <stdexcept>  // 用于抛出异常

// DatabaseCoreImpl 现在在 DatabaseAPI.hpp 中定义。

/**
 * @brief DDLOperations的内部实现类 (Pimpl)，所有具体逻辑都在这里。
 */
class DDLOperations::Impl {
private:
  // 指向核心状态的指针
  DatabaseCoreImpl *core_impl_;

public:
  // 构造函数
  explicit Impl(DatabaseCoreImpl *core_impl) : core_impl_(core_impl) {
    if (!core_impl_) {
      throw std::invalid_argument(
          "Core implementation pointer cannot be null.");
    }
  }

  // 实现 createDatabase
  bool createDatabase(const std::string &dbName) {
    if (dbName.empty()) {
      std::cerr << "Error: Database name cannot be empty." << std::endl;
      return false;
    }
    std::filesystem::path dbPath =
        std::filesystem::path(core_impl_->rootPath) / dbName;
    try {
      if (std::filesystem::exists(dbPath)) {
        std::cerr << "Error: Database '" << dbName << "' already exists."
                  << std::endl;
        return false;
      }
      return std::filesystem::create_directory(dbPath);
    } catch (const std::filesystem::filesystem_error &e) {
      std::cerr << "Filesystem error: " << e.what() << std::endl;
      return false;
    }
  }

  // 实现 dropDatabase
  bool dropDatabase(const std::string &dbName) {
    if (dbName.empty()) {
      std::cerr << "Error: Database name cannot be empty." << std::endl;
      return false;
    }
    std::filesystem::path dbPath =
        std::filesystem::path(core_impl_->rootPath) / dbName;
    try {
      if (!std::filesystem::exists(dbPath)) {
        std::cerr << "Error: Database '" << dbName << "' does not exist."
                  << std::endl;
        return false;
      }
      if (core_impl_->currentDbName == dbName) {
        core_impl_->currentDbName.clear();
      }
      // 在删除数据库时，清空该数据库下所有表的内存数据
      // 注意：这里需要确保删除的是当前数据库下的表
      // 一个更严谨的实现会遍历 core_impl_->tables，检查每个表的完整路径
      // 但对于当前设计，我们假设表名在当前数据库下是唯一的
      // 直接清空内存中的所有表，因为删除一个数据库意味着它包含的所有表都将失效
      core_impl_->tables.clear();

      std::filesystem::remove_all(dbPath);
      return true;
    } catch (const std::filesystem::filesystem_error &e) {
      std::cerr << "Filesystem error: " << e.what() << std::endl;
      return false;
    }
  }

  // 实现 useDatabase
  bool useDatabase(const std::string &dbName) {
    if (dbName.empty()) {
      std::cerr << "Error: Database name cannot be empty." << std::endl;
      return false;
    }
    std::filesystem::path dbPath =
        std::filesystem::path(core_impl_->rootPath) / dbName;
    if (std::filesystem::is_directory(dbPath)) {
      core_impl_->currentDbName = dbName;
      // 切换数据库时，加载该数据库下的所有表到内存
      core_impl_->tables.clear(); // 清空旧的表数据
      for (const auto &entry : std::filesystem::directory_iterator(dbPath)) {
        if (entry.is_regular_file() && entry.path().extension() == ".meta") {
          std::string tableName = entry.path().stem().string();
          TableData tableData;
          tableData.name = tableName;

          // 加载元数据
          std::ifstream metaFile(entry.path());
          std::string line;
          while (std::getline(metaFile, line)) {
            std::stringstream ss(line);
            std::string name_str, type_str, is_pk_str;
            std::getline(ss, name_str, ',');
            std::getline(ss, type_str, ',');
            std::getline(ss, is_pk_str);

            DataType type = static_cast<DataType>(std::stoi(type_str));
            bool isPrimaryKey = (std::stoi(is_pk_str) == 1);
            tableData.columns.emplace_back(name_str, type, isPrimaryKey);
          }
          metaFile.close();

          // 加载数据 (如果 .dat 文件存在)
          std::filesystem::path dataFilePath = dbPath / (tableName + ".dat");
          if (std::filesystem::exists(dataFilePath)) {
            std::ifstream dataFile(dataFilePath);
            while (std::getline(dataFile, line)) {
              std::stringstream ss(line);
              std::string cell;
              Row row;
              while (std::getline(ss, cell, ',')) {
                row.push_back(cell);
              }
              tableData.rows.push_back(row);
            }
            dataFile.close();
          }
          core_impl_->tables[tableName] = tableData;
        }
      }
      std::cout << "Using database '" << dbName << "'. Loaded "
                << core_impl_->tables.size() << " tables into memory."
                << std::endl;
      return true;
    } else {
      std::cerr << "Error: Database '" << dbName << "' not found." << std::endl;
      return false;
    }
  }

  // 实现 createTable
  bool createTable(const std::string &tableName,
                   const std::vector<ColumnDefinition> &columns) {
    if (core_impl_->currentDbName.empty()) {
      std::cerr << "Error: No database selected." << std::endl;
      return false;
    }
    if (tableName.empty() || columns.empty()) {
      std::cerr << "Error: Table name or columns cannot be empty." << std::endl;
      return false;
    }

    // 检查内存中是否已存在同名表
    if (core_impl_->tables.count(tableName)) {
      std::cerr << "Error: Table '" << tableName
                << "' already exists in memory." << std::endl;
      return false;
    }

    std::filesystem::path dbPath =
        std::filesystem::path(core_impl_->rootPath) / core_impl_->currentDbName;
    std::filesystem::path tableMetaPath = dbPath / (tableName + ".meta");
    try {
      if (std::filesystem::exists(tableMetaPath)) {
        std::cerr << "Error: Table '" << tableName
                  << "' already exists on disk." << std::endl;
        return false;
      }

      // 写入元数据文件
      std::ofstream metaFile(tableMetaPath);
      if (!metaFile.is_open()) {
        std::cerr << "Error: Could not create metadata file for table '"
                  << tableName << "'." << std::endl;
        return false;
      }
      bool hasPrimaryKey = false;
      for (const auto &col : columns) {
        metaFile << col.name << "," << static_cast<int>(col.type) << ","
                 << (col.isPrimaryKey ? "1" : "0") << "\n";
        if (col.isPrimaryKey) {
          if (hasPrimaryKey) {
            std::cerr << "Error: Multiple primary keys defined for table '"
                      << tableName << "'." << std::endl;
            metaFile.close();
            std::filesystem::remove(tableMetaPath);
            return false;
          }
          hasPrimaryKey = true;
        }
      }
      metaFile.close();

      // 创建空的 .dat 文件
      std::filesystem::path dataPath = dbPath / (tableName + ".dat");
      std::ofstream dataFile(dataPath); // 创建并立即关闭，以确保文件存在
      if (!dataFile.is_open()) {
        std::cerr << "Error: Could not create data file for table '"
                  << tableName << "'." << std::endl;
        return false;
      }
      dataFile.close();

      // 如果有主键，创建空的 .idx 文件
      if (hasPrimaryKey) {
        std::filesystem::path indexPath = dbPath / (tableName + ".idx");
        std::ofstream indexFile(indexPath);
        if (!indexFile.is_open()) {
          std::cerr << "Error: Could not create index file for table '"
                    << tableName << "'." << std::endl;
          // 虽然创建索引文件失败，但表仍然可以创建（无索引），取决于设计
          // 这里我们认为失败，并尝试清理
          std::filesystem::remove(tableMetaPath);
          std::filesystem::remove(dataPath);
          return false;
        }
        indexFile.close();
      }

      // 在内存中创建 TableData 对象
      TableData newTable;
      newTable.name = tableName;
      newTable.columns = columns; // 复制列定义
      // rows 将为空，等待 DML 操作插入
      core_impl_->tables[tableName] = newTable;

      std::cout << "Table '" << tableName << "' created and loaded into memory."
                << std::endl;
      return true;
    } catch (const std::filesystem::filesystem_error &e) {
      std::cerr << "Filesystem error: " << e.what() << std::endl;
      return false;
    }
  }

  // 实现 dropTable
  bool dropTable(const std::string &tableName) {
    if (core_impl_->currentDbName.empty()) {
      std::cerr << "Error: No database selected." << std::endl;
      return false;
    }
    if (tableName.empty()) {
      std::cerr << "Error: Table name cannot be empty." << std::endl;
      return false;
    }
    std::filesystem::path dbPath =
        std::filesystem::path(core_impl_->rootPath) / core_impl_->currentDbName;
    std::filesystem::path tableMetaPath = dbPath / (tableName + ".meta");
    std::filesystem::path tableIdxPath = dbPath / (tableName + ".idx");
    std::filesystem::path tableDataPath = dbPath / (tableName + ".dat");
    try {
      if (!std::filesystem::exists(tableMetaPath)) {
        std::cerr << "Error: Table '" << tableName << "' does not exist."
                  << std::endl;
        return false;
      }
      bool success = true;
      if (std::filesystem::exists(tableMetaPath))
        success &= std::filesystem::remove(tableMetaPath);
      if (std::filesystem::exists(tableDataPath))
        success &= std::filesystem::remove(tableDataPath);
      if (std::filesystem::exists(tableIdxPath))
        success &= std::filesystem::remove(tableIdxPath);

      // 从内存中移除表数据
      core_impl_->tables.erase(tableName);

      std::cout << "Table '" << tableName << "' dropped from disk and memory."
                << std::endl;
      return success;
    } catch (const std::filesystem::filesystem_error &e) {
      std::cerr << "Filesystem error: " << e.what() << std::endl;
      return false;
    }
  }
};

// DDLOperations 公共接口的实现，将调用转发给 pImpl 对象
DDLOperations::DDLOperations(
    DatabaseCoreImpl *core_impl_ptr) // 接受 DatabaseCoreImpl*
    : pImpl(std::make_unique<Impl>(core_impl_ptr)) {
} // 使用 unique_ptr 初始化 pImpl

// 析构函数必须在这里定义，因为 pImpl 是 unique_ptr，它的析构需要 Impl
// 的完整定义
DDLOperations::~DDLOperations() = default;

bool DDLOperations::createDatabase(const std::string &dbName) {
  return pImpl->createDatabase(dbName);
}

bool DDLOperations::dropDatabase(const std::string &dbName) {
  return pImpl->dropDatabase(dbName);
}

bool DDLOperations::useDatabase(const std::string &dbName) {
  return pImpl->useDatabase(dbName);
}

bool DDLOperations::createTable(const std::string &tableName,
                                const std::vector<ColumnDefinition> &columns) {
  return pImpl->createTable(tableName, columns);
}

bool DDLOperations::dropTable(const std::string &tableName) {
  return pImpl->dropTable(tableName);
}
