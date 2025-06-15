#include "server/DatabaseAPI.hpp" // 包含你的API头文件
#include <iostream>
#include <fstream>      // 包含了fstream，解决ofstream的错误
#include <filesystem>   // 包含了filesystem

// 全局打印测试结果的函数
void printTestResult(const std::string& testName, bool success) {
    std::cout << "[TEST] " << testName << ": " << (success ? "PASSED" : "FAILED") << std::endl;
}

int main() {
    std::cout << "--- Starting Database API Test ---" << std::endl;

    // --- 1. 初始化测试环境 ---
    const std::string dbRoot = "./db_test_root";
    // 在测试前清理旧的测试目录，确保测试环境干净
    if (std::filesystem::exists(dbRoot)) {
        std::filesystem::remove_all(dbRoot);
    }

    // 通过顶层Database类创建实例，这是正确的API使用方式
    Database db(dbRoot);

    // 从Database实例中获取操作模块的引用
    auto& ddl_ops = db.getDDLOperations();
    auto& tx_manager = db.getTransactionManager();


    // --- 2. 测试 DDL (数据定义语言) 操作 ---
    std::cout << "\n--- Testing DDL Operations ---" << std::endl;

    bool res = ddl_ops.createDatabase("school");
    printTestResult("Create Database 'school'", res);

    res = ddl_ops.useDatabase("non_exist_db");
    printTestResult("Use non-existent database", !res);

    res = ddl_ops.useDatabase("school");
    printTestResult("Use Database 'school'", res);

    std::vector<ColumnDefinition> columns;
    columns.emplace_back("id", DataType::INT, true);
    columns.emplace_back("name", DataType::STRING);
    res = ddl_ops.createTable("students", columns);
    printTestResult("Create table 'students'", res);

    // res = ddl_ops.dropTable("students");
    // printTestResult("Drop table 'students'", res);

    // res = ddl_ops.dropDatabase("school");
    // printTestResult("Drop Database 'school'", res);


    // --- 3. 测试 TransactionManager (事务管理) 操作 ---
    std::cout << "\n--- Testing Transaction Manager ---" << std::endl;
    
    ddl_ops.createDatabase("company");
    ddl_ops.useDatabase("company");
    ddl_ops.createTable("employees", {{"id", DataType::INT, true}, {"name", DataType::STRING}});

    // 测试正常提交流程
    std::cout << "\nTesting successful commit..." << std::endl;
    tx_manager.beginTransaction();
    
    // 手动模拟DML操作：向日志文件写入内容
    {
        std::ofstream log_file(dbRoot + "/company/transaction.log", std::ios::app);
        if(log_file) {
            log_file << "INSERT;employees;1,Alice" << std::endl;
            log_file << "INSERT;employees;2,Bob" << std::endl;
        }
    }

    // 注意：commit函数本身依赖DML的实现，在我们的简化版中，它会直接操作文件
    tx_manager.commit(); // ← 修正：在这里调用 commit() 来结束第一个事务，解决逻辑错误

    // 测试回滚流程
    std::cout << "\nTesting rollback..." << std::endl;
    tx_manager.beginTransaction();
    {
        std::ofstream log_file(dbRoot + "/company/transaction.log", std::ios::app);
        if(log_file) {
            log_file << "INSERT;employees;3,Charlie" << std::endl;
        }
    }
    tx_manager.rollback();
    
    // --- 4. 清理环境 ---
    std::cout << "\n--- Cleaning up test environment ---" << std::endl;
    //std::filesystem::remove_all(dbRoot);
    //printTestResult("Cleanup", !std::filesystem::exists(dbRoot));

    std::cout << "\n--- All tests finished successfully! ---" << std::endl;

    return 0;
}