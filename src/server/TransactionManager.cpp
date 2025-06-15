#include "../../include/server/DatabaseAPI.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream> // For std::stringstream
#include <stdexcept>

// DatabaseCoreImpl 现在在 DatabaseAPI.hpp 中定义。

/**
 * @brief TransactionManager的内部实现类 (Pimpl)。
 */
class TransactionManager::Impl {
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

  // 实现 beginTransaction
  void beginTransaction() {
    if (core_impl_->isTransactionActive) {
      // 也可以抛出异常
      std::cerr << "Error: Transaction already in progress. Please commit or "
                   "rollback first."
                << std::endl;
      return;
    }
    if (core_impl_->currentDbName.empty()) {
      std::cerr << "Error: No database selected. Cannot start a transaction."
                << std::endl;
      return;
    }

    // 设置事务日志文件路径
    core_impl_->transactionLogPath =
        (std::filesystem::path(core_impl_->rootPath) /
         core_impl_->currentDbName / "transaction.log")
            .string();

    // 开启一个新日志文件（会覆盖旧的）
    std::ofstream logFile(core_impl_->transactionLogPath);
    if (!logFile.is_open()) {
      std::cerr << "Error: Cannot create transaction log file." << std::endl;
      return;
    }
    logFile.close();

    core_impl_->isTransactionActive = true;
    std::cout << "Transaction started." << std::endl;
  }

  // 实现 commit
  void commit() {
    // 事务还没开始，直接返回
    if (!core_impl_->isTransactionActive) {
      std::cerr << "Error: No transaction in progress to commit." << std::endl;
      return;
    }

    std::cout << "Committing transaction..." << std::endl;

    // 1. 打开日志文件用于读取
    std::ifstream logFile(core_impl_->transactionLogPath);
    if (!logFile.is_open()) {
      std::cerr << "Error: Could not open transaction log. Rolling back."
                << std::endl;
      rollback(); // 日志文件是核心，打不开必须回滚
      return;
    }

    // ** 核心修复：现在 commit 不再“重放”日志，而是将内存中的当前状态持久化 **
    // 2. 将内存中的所有表数据写回文件
    std::filesystem::path dbPath =
        std::filesystem::path(core_impl_->rootPath) / core_impl_->currentDbName;
    bool all_operations_successful = true;
    for (auto &pair : core_impl_->tables) {
      const std::string &tableName = pair.first;
      const TableData &tableData = pair.second;
      std::filesystem::path dataFilePath = dbPath / (tableName + ".dat");

      std::ofstream dataFile(dataFilePath,
                             std::ios::trunc); // 清空文件并重新写入
      if (dataFile.is_open()) {
        for (const Row &row : tableData.rows) {
          bool firstCol = true;
          for (const std::string &cell : row) {
            if (!firstCol)
              dataFile << ",";
            dataFile << cell;
            firstCol = false;
          }
          dataFile << std::endl;
        }
        dataFile.close();
      } else {
        std::cerr << "Error: Failed to open data file for table '" << tableName
                  << "' during commit." << std::endl;
        all_operations_successful = false;
        break; // 停止处理后续表
      }
    }
    logFile.close(); // 关闭日志文件，即使它没有被逐行处理

    // 3. 如果所有操作都成功，才算提交成功
    if (all_operations_successful) {
      // 结束事务（删除日志文件，重置状态）
      cleanup();
      std::cout << "Transaction committed successfully. Data persisted to disk."
                << std::endl;
    } else {
      std::cerr << "Error: Commit failed. Rolling back." << std::endl;
      // 如果中途失败，我们回滚事务（删除日志），但要警告用户数据可能已部分写入
      cleanup();
      std::cerr << "Warning: The database might be in an inconsistent state "
                   "due to partial persistence."
                << std::endl;
    }
  }

  // 实现 rollback
  void rollback() {
    if (!core_impl_->isTransactionActive) {
      std::cerr << "Error: No transaction in progress to rollback."
                << std::endl;
      return;
    }

    std::cout << "Rolling back transaction..." << std::endl;

    // 回滚操作：重新从文件加载所有表数据到内存，从而撤销所有未提交的内存更改
    // 这是一个简单的回滚策略，不适用于复杂事务
    std::filesystem::path dbPath =
        std::filesystem::path(core_impl_->rootPath) / core_impl_->currentDbName;
    core_impl_->tables.clear(); // 清空当前内存中的所有表数据
    for (const auto &entry : std::filesystem::directory_iterator(dbPath)) {
      if (entry.is_regular_file() && entry.path().extension() == ".meta") {
        std::string tableName = entry.path().stem().string();
        TableData tableData;
        tableData.name = tableName;

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

    cleanup(); // 删除事务日志
    std::cout
        << "Transaction rolled back. In-memory data reverted to disk state."
        << std::endl;
  }

private:
  // 清理事务状态和日志文件
  void cleanup() {
    // 删除日志文件
    if (std::filesystem::exists(core_impl_->transactionLogPath)) {
      std::filesystem::remove(core_impl_->transactionLogPath);
    }
    // 重置状态
    core_impl_->isTransactionActive = false;
    core_impl_->transactionLogPath.clear();
  }
};

// --- TransactionManager 公共接口的实现 ---
TransactionManager::TransactionManager(
    DatabaseCoreImpl *core_impl_ptr) // 接受 DatabaseCoreImpl*
    : pImpl(std::make_unique<Impl>(core_impl_ptr)) {
} // 使用 unique_ptr 初始化 pImpl

// 析构函数必须在这里定义，因为 pImpl 是 unique_ptr，它的析构需要 Impl
// 的完整定义
TransactionManager::~TransactionManager() = default;

void TransactionManager::beginTransaction() { pImpl->beginTransaction(); }

void TransactionManager::commit() { pImpl->commit(); }

void TransactionManager::rollback() { pImpl->rollback(); }
