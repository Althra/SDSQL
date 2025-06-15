#include "../../include/server/DatabaseAPI.hpp"
#include <filesystem>      
#include <fstream>        
#include <iostream>        
#include <stdexcept>      

namespace {
    struct DatabaseCoreImpl {
        std::string rootPath;      // 数据库系统的根目录
        std::string currentDbName; // 当前 'USE' 的数据库名
        bool isTransactionActive = false; // 标记当前是否有事务正在进行
        std::string transactionLogPath;   // 事务日志文件的路径
        //DMLOperations* dml_ops_ = nullptr; 
    };
}

/**
 * @brief TransactionManager的内部实现类 (Pimpl)。
 */
class TransactionManager::Impl {
private:
    // 指向核心状态的指针
    DatabaseCoreImpl* core_impl_;

public:
    // 构造函数
    explicit Impl(DatabaseCoreImpl* core_impl) : core_impl_(core_impl) {
        if (!core_impl_) {
            throw std::invalid_argument("Core implementation pointer cannot be null.");
        }
    }

    // 实现 beginTransaction
    void beginTransaction() {
        if (core_impl_->isTransactionActive) {
            // 也可以抛出异常
            std::cerr << "Error: Transaction already in progress. Please commit or rollback first." << std::endl;
            return;
        }
        if (core_impl_->currentDbName.empty()) {
            std::cerr << "Error: No database selected. Cannot start a transaction." << std::endl;
            return;
        }

        // 设置事务日志文件路径
        core_impl_->transactionLogPath = 
            (std::filesystem::path(core_impl_->rootPath) / core_impl_->currentDbName / "transaction.log").string();

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
        std::cerr << "Error: Could not open transaction log. Rolling back." << std::endl;
        rollback(); // 日志文件是核心，打不开必须回滚
        return;
    }

    std::string line;
    bool all_operations_successful = true;

    // 2. 逐行读取并“重放”日志中的操作
    while (std::getline(logFile, line)) {
        if (line.empty()) {
            continue; // 跳过空行
        }

        // 3. 解析日志行 (简化版)
        std::stringstream ss(line);
        std::string command, tableName, values_str;

        // 按分号分割，获取命令、表名和值
        std::getline(ss, command, ';');
        std::getline(ss, tableName, ';');
        std::getline(ss, values_str); // 剩余部分都是值

        if (command == "INSERT") {
            // 4. 直接将值追加写入到对应表的 .dat 文件
            std::filesystem::path dataFilePath = std::filesystem::path(core_impl_->rootPath) / core_impl_->currentDbName / (tableName + ".dat");
            
            // 以追加模式打开数据文件
            std::ofstream dataFile(dataFilePath, std::ios::app); 
            if (dataFile.is_open()) {
                // 将日志中的值（例如 "1,Alice"）直接写入文件，并换行
                dataFile << values_str << std::endl;
                dataFile.close();
            } else {
                // 如果数据文件打开失败，则本次提交失败
                std::cerr << "Error: Failed to open data file for table '" << tableName << "'." << std::endl;
                all_operations_successful = false;
                break; // 停止处理后续日志
            }
        }
        // 在此可以添加 else if (command == "UPDATE") 等其他操作的简化实现
    }
    logFile.close();

    // 5. 如果所有操作都成功，才算提交成功
    if (all_operations_successful) {
        // 结束事务（删除日志文件，重置状态）
        cleanup();
        std::cout << "Transaction committed successfully." << std::endl;
    } else {
        std::cerr << "Error: Commit failed. Rolling back." << std::endl;
        // 如果中途失败，我们回滚事务（删除日志），但要警告用户数据可能已部分写入
        cleanup();
        std::cerr << "Warning: The database might be in an inconsistent state." << std::endl;
    }
}

    // 实现 rollback
    void rollback() {
        if (!core_impl_->isTransactionActive) {
            std::cerr << "Error: No transaction in progress to rollback." << std::endl;
            return;
        }
        
        std::cout << "Rolling back transaction..." << std::endl;

        //只需要删除日志文件就回滚了
        cleanup();
        std::cout << "Transaction rolled back." << std::endl;
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
// 这些函数将所有工作委托给 pImpl 对象。

TransactionManager::TransactionManager(TransactionManager::Impl* db_core_impl)
    : pImpl(db_core_impl) {}

// 析构函数必须在 .cpp 文件中定义，以确保 unique_ptr 能正确销毁 Impl 对象
TransactionManager::~TransactionManager() = default;

void TransactionManager::beginTransaction() {
    pImpl->beginTransaction();
}

void TransactionManager::commit() {
    pImpl->commit();
}

void TransactionManager::rollback() {
    pImpl->rollback();
}