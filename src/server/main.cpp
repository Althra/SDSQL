// #include <filesystem> // For cleaning up test directory
// #include <fstream> // For logging in TransactionManager (if needed for simplified manual logging)
// #include <iostream>
// #include <map>
// #include <memory>
// #include <string>
// #include <vector>

// // 包含你的数据库API头文件
// #include "../../include/server/DatabaseAPI.hpp"

// // 确保在你的编译环境中，Database.cpp, DDLOperations.cpp, DMLOperations.cpp,
// // TransactionManager.cpp
// // 都被正确编译和链接。例如，如果你使用g++，可以这样编译：
// // g++ main.cpp Database.cpp DDLOperations.cpp DMLOperations.cpp
// // TransactionManager.cpp -o test_db -std=c++17 -lstdc++fs

// // 全局打印测试结果的函数
// void printTestResult(const std::string &testName, bool success) {
//   std::cout << "[TEST] " << testName << ": " << (success ? "PASSED" : "FAILED")
//             << std::endl;
// }

// // 辅助函数：打印查询结果
// void printQueryResult(std::unique_ptr<QueryResult> &result) {
//   if (!result) {
//     std::cout << "查询结果为空或发生错误。" << std::endl;
//     return;
//   }

//   if (result->getRowCount() == 0) {
//     std::cout << "查询结果为空（0行）。" << std::endl;
//     return;
//   }

//   // 打印列名
//   for (int i = 0; i < result->getColumnCount(); ++i) {
//     std::cout << result->getColumnName(i) << "\t("
//               << static_cast<int>(result->getColumnType(i)) << ")\t";
//   }
//   std::cout << std::endl;
//   for (int i = 0; i < result->getColumnCount(); ++i) {
//     std::cout << "--------\t\t";
//   }
//   std::cout << std::endl;

//   // 打印数据行
//   while (result->next()) {
//     for (int i = 0; i < result->getColumnCount(); ++i) {
//       // 根据类型获取并打印，这里统一用getString简化
//       std::cout << result->getString(i) << "\t\t";
//     }
//     std::cout << std::endl;
//   }
//   std::cout << std::endl;
// }

// int main() {
//   std::cout << "--- Starting Database API Test ---" << std::endl;

//   // --- 1. 初始化测试环境 ---
//   const std::string dbRoot = "./db_test_root";
//   const std::string dbName1 = "school_db";
//   const std::string tableName1 = "students";
//   const std::string dbName2 = "company_db";
//   const std::string tableName2 = "employees";

//   // 在测试前清理旧的测试目录，确保测试环境干净
//   if (std::filesystem::exists(dbRoot)) {
//     std::filesystem::remove_all(dbRoot);
//     std::cout << "已清理旧的测试数据目录: " << dbRoot << std::endl;
//   }

//   try {
//     // 通过顶层Database类创建实例，这是正确的API使用方式
//     Database db(dbRoot);

//     // 从Database实例中获取操作模块的引用
//     DDLOperations &ddl_ops = db.getDDLOperations();
//     DMLOperations &dml_ops = db.getDMLOperations();
//     TransactionManager &tx_manager = db.getTransactionManager();

//     // --- 2. 测试 DDL (数据定义语言) 操作 ---
//     std::cout << "\n--- Testing DDL Operations ---" << std::endl;

//     bool res = ddl_ops.createDatabase(dbName1);
//     printTestResult("Create Database '" + dbName1 + "'", res);

//     res = ddl_ops.useDatabase("non_exist_db");
//     printTestResult("Use non-existent database", !res);

//     res = ddl_ops.useDatabase(dbName1);
//     printTestResult("Use Database '" + dbName1 + "'", res);

//     std::vector<ColumnDefinition> studentColumns = {
//         {"id", DataType::INT, true}, // 主键
//         {"name", DataType::STRING},
//         {"age", DataType::INT},
//         {"grade", DataType::STRING},
//         {"is_active", DataType::BOOL}};
//     res = ddl_ops.createTable(tableName1, studentColumns);
//     printTestResult("Create table '" + tableName1 + "'", res);

//     // --- 3. 测试 DML (数据操作语言) 操作 ---
//     std::cout << "\n--- Testing DML Operations ---" << std::endl;

//     // 插入数据
//     std::cout << "\n--- 插入数据到 '" << tableName1 << "' ---" << std::endl;
//     std::map<std::string, std::string> student1 = {{"id", "101"},
//                                                    {"name", "Alice"},
//                                                    {"age", "18"},
//                                                    {"grade", "A"},
//                                                    {"is_active", "true"}};
//     if (dml_ops.insert(tableName1, student1)) {
//       std::cout << "插入 Alice 成功。" << std::endl;
//       printTestResult("Insert Alice", true);
//     } else {
//       std::cerr << "错误：插入 Alice 失败。" << std::endl;
//       printTestResult("Insert Alice", false);
//     }

//     std::map<std::string, std::string> student2 = {{"id", "102"},
//                                                    {"name", "Bob"},
//                                                    {"age", "19"},
//                                                    {"grade", "B"},
//                                                    {"is_active", "false"}};
//     if (dml_ops.insert(tableName1, student2)) {
//       std::cout << "插入 Bob 成功。" << std::endl;
//       printTestResult("Insert Bob", true);
//     } else {
//       std::cerr << "错误：插入 Bob 失败。" << std::endl;
//       printTestResult("Insert Bob", false);
//     }

//     std::map<std::string, std::string> student3 = {{"id", "103"},
//                                                    {"name", "Charlie"},
//                                                    {"age", "18"},
//                                                    {"grade", "A"},
//                                                    {"is_active", "true"}};
//     if (dml_ops.insert(tableName1, student3)) {
//       std::cout << "插入 Charlie 成功。" << std::endl;
//       printTestResult("Insert Charlie", true);
//     } else {
//       std::cerr << "错误：插入 Charlie 失败。" << std::endl;
//       printTestResult("Insert Charlie", false);
//     }

//     // 尝试插入重复主键
//     std::map<std::string, std::string> student_dup = {
//         {"id", "101"}, {"name", "Eve"}, {"age", "20"}, {"grade", "C"}};
//     bool dup_insert_res = dml_ops.insert(tableName1, student_dup) == 0;
//     printTestResult("Insert duplicate primary key (id=101)", dup_insert_res);

//     // 查询所有数据
//     std::cout << "\n--- 查询所有学生 ---" << std::endl;
//     auto allStudents = dml_ops.select(tableName1);
//     printQueryResult(allStudents);
//     printTestResult("Select all students",
//                     allStudents && allStudents->getRowCount() == 3);

//     // 带条件查询
//     std::cout << "\n--- 查询 age = 18 且 grade = 'A' 的学生 ---" << std::endl;
//     auto filteredStudents =
//         dml_ops.select(tableName1, "age = 18 AND grade = 'A'");
//     printQueryResult(filteredStudents);
//     printTestResult("Select students with age=18 AND grade=A",
//                     filteredStudents && filteredStudents->getRowCount() == 2);

//     std::cout << "\n--- 查询 is_active = true 的学生 ---" << std::endl;
//     auto activeStudents = dml_ops.select(tableName1, "is_active = 'true'");
//     printQueryResult(activeStudents);
//     printTestResult("Select active students",
//                     activeStudents && activeStudents->getRowCount() == 2);

//     // 带排序查询
//     std::cout << "\n--- 查询所有学生并按 age 排序 ---" << std::endl;
//     auto sortedByAge = dml_ops.select(tableName1, "", "age");
//     printQueryResult(sortedByAge);
//     // 验证排序：手动检查输出或更复杂的断言
//     printTestResult("Select all students ordered by age",
//                     sortedByAge != nullptr);

//     std::cout << "\n--- 查询所有学生并按 name 排序 ---" << std::endl;
//     auto sortedByName = dml_ops.select(tableName1, "", "name");
//     printQueryResult(sortedByName);
//     printTestResult("Select all students ordered by name",
//                     sortedByName != nullptr);

//     // 更新数据
//     std::cout << "\n--- 更新 Bob 的 age 为 20 ---" << std::endl;
//     std::map<std::string, std::string> updates = {{"age", "20"}};
//     int updatedRows = dml_ops.update(tableName1, updates, "name = 'Bob'");
//     printTestResult("Update Bob's age to 20", updatedRows == 1);

//     std::cout << "\n--- 再次查询所有学生以验证更新 ---" << std::endl;
//     allStudents = dml_ops.select(tableName1);
//     printQueryResult(allStudents);

//     // 删除数据
//     std::cout << "\n--- 删除 age < 20 的学生 ---" << std::endl;
//     int removedRows = dml_ops.remove(tableName1, "age < 20");
//     printTestResult("Remove students with age < 20",
//                     removedRows == 2); // Alice and Charlie should be deleted

//     std::cout << "\n--- 再次查询所有学生以验证删除 ---" << std::endl;
//     allStudents = dml_ops.select(tableName1);
//     printQueryResult(allStudents);
//     printTestResult("Select all students after removal",
//                     allStudents && allStudents->getRowCount() == 1);

//     // --- 4. 测试 TransactionManager (事务管理) 操作 ---
//     std::cout << "\n--- Testing Transaction Manager ---" << std::endl;

//     // 创建新的数据库和表用于事务测试
//     ddl_ops.createDatabase(dbName2);
//     ddl_ops.useDatabase(dbName2);
//     std::vector<ColumnDefinition> employeeColumns = {
//         {"id", DataType::INT, true},
//         {"name", DataType::STRING},
//         {"salary", DataType::DOUBLE}};
//     ddl_ops.createTable(tableName2, employeeColumns);
//     printTestResult("Create Database '" + dbName2 + "' and Table '" +
//                         tableName2 + "' for transactions",
//                     true); // Assuming success

//     // 测试正常提交流程
//     std::cout << "\nTesting successful commit..." << std::endl;
//     tx_manager.beginTransaction();
//     std::map<std::string, std::string> emp1 = {
//         {"id", "1"}, {"name", "Frank"}, {"salary", "50000.0"}};
//     dml_ops.insert(tableName2, emp1);
//     std::map<std::string, std::string> emp2 = {
//         {"id", "2"}, {"name", "Grace"}, {"salary", "60000.5"}};
//     dml_ops.insert(tableName2, emp2);
//     std::cout << "事务内插入 Frank 和 Grace。" << std::endl;
//     tx_manager.commit();
//     std::cout << "事务已提交。" << std::endl;

//     auto employeesAfterCommit = dml_ops.select(tableName2);
//     printQueryResult(employeesAfterCommit);
//     printTestResult("Employees after commit (expected 2 rows)",
//                     employeesAfterCommit &&
//                         employeesAfterCommit->getRowCount() == 2);

//     // 测试回滚流程
//     std::cout << "\nTesting rollback..." << std::endl;
//     tx_manager.beginTransaction();
//     std::map<std::string, std::string> emp3 = {
//         {"id", "3"}, {"name", "Heidi"}, {"salary", "75000.0"}};
//     dml_ops.insert(tableName2, emp3);
//     std::cout << "事务内插入 Heidi。" << std::endl;
//     tx_manager.rollback();
//     std::cout << "事务已回滚。Heidi 应该不在表中。" << std::endl;

//     auto employeesAfterRollback = dml_ops.select(tableName2);
//     printQueryResult(employeesAfterRollback);
//     printTestResult(
//         "Employees after rollback (expected 2 rows, Heidi not present)",
//         employeesAfterRollback && employeesAfterRollback->getRowCount() == 2);

//     // --- 5. 清理环境 ---
//     std::cout << "\n--- Cleaning up test environment ---" << std::endl;
//     res = ddl_ops.dropDatabase(dbName1);
//     printTestResult("Drop Database '" + dbName1 + "'", res);
//     res = ddl_ops.dropDatabase(dbName2);
//     printTestResult("Drop Database '" + dbName2 + "'", res);

//     // 最终清理整个根目录
//     std::filesystem::remove_all(dbRoot);
//     printTestResult("Final cleanup of '" + dbRoot + "'",
//                     !std::filesystem::exists(dbRoot));

//   } catch (const DatabaseException &e) {
//     std::cerr << "数据库异常: " << e.what() << std::endl;
//     printTestResult("Overall Test Execution (DatabaseException)", false);
//     return 1;
//   } catch (const std::exception &e) {
//     std::cerr << "通用异常: " << e.what() << std::endl;
//     printTestResult("Overall Test Execution (Generic Exception)", false);
//     return 1;
//   }

//   std::cout << "\n--- All tests finished. Check logs for PASSED/FAILED. ---"
//             << std::endl;

//   return 0;
// }
