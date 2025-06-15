// DMLOperations.cpp
// 该文件实现了DMLOperations类的具体逻辑，并增强了条件评估功能

#include "../../include/server/DatabaseAPI.hpp" // 包含数据库API头文件
#include <algorithm> // 用于 std::remove_if, std::sort
#include <charconv> // 用于 std::from_chars (C++17特性，用于数字转换)
#include <fstream>  // 用于 std::ofstream 和 std::ifstream
#include <iostream>
#include <map>
#include <memory>    // 用于 std::unique_ptr
#include <sstream>   // 用于 std::stringstream
#include <stdexcept> // 用于标准异常类
#include <string>
#include <vector>

// DatabaseCoreImpl 现在在 DatabaseAPI.hpp 中定义。不需要在这里重复定义。

// --- 辅助函数：类型转换（用于条件评估）---
// 这些函数在 DMLOperations.cpp 内部定义，不暴露给外部

namespace DMLHelpers { // 使用命名空间 DMLHelpers 避免全局命名冲突

// Helper to remove leading/trailing whitespace
std::string trim(const std::string &str) {
  size_t first = str.find_first_not_of(" \t\n\r");
  if (std::string::npos == first) {
    return str;
  }
  size_t last = str.find_last_not_of(" \t\n\r");
  return str.substr(first, (last - first + 1));
}

template <typename T> bool convertToType(const std::string &s, T &value) {
  if constexpr (std::is_same_v<T, int>) {
    // 使用 std::from_chars 更安全地进行数字转换
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
    return ec == std::errc();
  } else if constexpr (std::is_same_v<T, double>) {
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
    return ec == std::errc();
  } else if constexpr (std::is_same_v<T, bool>) {
    // 支持 "1", "0", "true", "false", 不区分大小写
    std::string lower_s = s;
    std::transform(lower_s.begin(), lower_s.end(), lower_s.begin(), ::tolower);
    if (lower_s == "1" || lower_s == "true") {
      value = true;
      return true;
    } else if (lower_s == "0" || lower_s == "false") {
      value = false;
      return true;
    }
    return false;
  } else if constexpr (std::is_same_v<T, std::string>) {
    value = s;
    return true;
  }
  return false; // 未知类型
}

// 前向声明 evaluateCondition，因为 evaluateSingleComparison
// 可能在未来通过递归调用 或者为了结构清晰，先声明它
bool evaluateCondition(const Row &row, const TableData &tableMetadata,
                       const std::string &condition);

/**
 * @brief 评估单个比较条件 (e.g., "column_name = 'value'", "age > 30")
 * @param row 当前行数据
 * @param tableMetadata 表的元数据
 * @param condition 单个比较条件字符串
 * @return 如果条件匹配则返回 true，否则返回 false
 */
bool evaluateSingleComparison(const Row &row, const TableData &tableMetadata,
                              const std::string &condition) {
  std::string trimmedCondition = trim(condition);
  if (trimmedCondition.empty()) {
    return true; // 空条件视为真，尽管通常不会出现
  }

  // 查找操作符及其位置，按照优先级或长度排序以便正确匹配
  std::vector<std::pair<std::string, std::string>> operators = {
      {">=", ">="}, {"<=", "<="}, {"!=", "!="},
      {"=", "="},   {">", ">"},   {"<", "<"}};

  size_t opPos = std::string::npos;
  std::string op;

  for (const auto &op_pair : operators) {
    size_t currentOpPos = trimmedCondition.find(op_pair.first);
    if (currentOpPos != std::string::npos) {
      // 找到操作符，确保不是部分匹配（例如 '=' 不应匹配 '!='）
      // 这是一个简单的启发式，更严谨的需要正则或词法分析
      // 确保找到的操作符不是更大操作符的一部分
      bool is_part_of_larger_op = false;
      if (op_pair.first == "=") {
        if (currentOpPos > 0 && (trimmedCondition[currentOpPos - 1] == '>' ||
                                 trimmedCondition[currentOpPos - 1] == '<' ||
                                 trimmedCondition[currentOpPos - 1] == '!')) {
          is_part_of_larger_op = true;
        }
      }
      // 对于双字符操作符，确保它不是意外地匹配了单字符部分
      // 修正：确保不会将单个字符的操作符误认为是双字符操作符的一部分
      // 比如对于 "A=B"，op_pair.first 是 "="，currentOpPos 是等号的位置
      // trimmedCondition.substr(currentOpPos, 2) 会是 "=B"
      // trimmedCondition.substr(currentOpPos - 1, 3) 会是 "A=B"
      // 这样写有点复杂且容易出错，更简单的办法是按长度从大到小匹配操作符
      // 当前的 operator
      // 向量已经按长度从大到小排列，所以先找到的双字符操作符会优先匹配

      if (is_part_of_larger_op) {
        continue;
      }

      opPos = currentOpPos;
      op = op_pair.second;
      break; // 找到最左边的操作符后停止
    }
  }

  if (opPos == std::string::npos) {
    std::cerr << "Warning: 无法解析单个比较条件 '" << trimmedCondition
              << "': 未找到有效操作符。" << std::endl;
    return false;
  }

  std::string colName = trim(trimmedCondition.substr(0, opPos));
  std::string valueStr = trim(trimmedCondition.substr(opPos + op.length()));

  // 如果是字符串字面量，去除单引号
  if (valueStr.length() >= 2 && valueStr.front() == '\'' &&
      valueStr.back() == '\'') {
    valueStr = valueStr.substr(1, valueStr.length() - 2);
  }

  int colIndex = tableMetadata.getColumnIndex(colName);
  if (colIndex == -1) {
    // 列不存在
    // std::cerr << "Warning: 比较条件中引用的列 '" << colName << "' 不存在。"
    // << std::endl;
    return false; // 如果列不存在，条件不匹配
  }

  // 确保行数据有效，防止越界访问
  if (colIndex >= row.size()) {
    std::cerr << "Error: 行数据中列索引越界。" << std::endl;
    return false;
  }

  const std::string &rowValueStr = row[colIndex];
  DataType colType = tableMetadata.getColumnType(colIndex);

  // 根据数据类型和操作符进行比较
  try {
    if (colType == DataType::INT) {
      int rowVal, targetVal;
      if (convertToType(rowValueStr, rowVal) &&
          convertToType(valueStr, targetVal)) {
        if (op == "=")
          return rowVal == targetVal;
        if (op == "!=")
          return rowVal != targetVal;
        if (op == ">")
          return rowVal > targetVal;
        if (op == "<")
          return rowVal < targetVal;
        if (op == ">=")
          return rowVal >= targetVal;
        if (op == "<=")
          return rowVal <= targetVal;
      }
    } else if (colType == DataType::DOUBLE) {
      double rowVal, targetVal;
      if (convertToType(rowValueStr, rowVal) &&
          convertToType(valueStr, targetVal)) {
        if (op == "=")
          return rowVal == targetVal;
        if (op == "!=")
          return rowVal != targetVal;
        if (op == ">")
          return rowVal > targetVal;
        if (op == "<")
          return rowVal < targetVal;
        if (op == ">=")
          return rowVal >= targetVal;
        if (op == "<=")
          return rowVal <= targetVal;
      }
    } else if (colType == DataType::BOOL) {
      bool rowVal, targetVal;
      if (convertToType(rowValueStr, rowVal) &&
          convertToType(valueStr, targetVal)) {
        if (op == "=")
          return rowVal == targetVal;
        if (op == "!=")
          return rowVal != targetVal;
        // 对于布尔类型，通常只支持等式和非等式
        std::cerr << "Warning: 布尔类型不支持 '>','<','>=','<=' 比较符。"
                  << std::endl;
      }
    } else { // DataType::STRING
      // 字符串的字典序比较
      if (op == "=")
        return rowValueStr == valueStr;
      if (op == "!=")
        return rowValueStr != valueStr;
      if (op == ">")
        return rowValueStr > valueStr;
      if (op == "<")
        return rowValueStr < valueStr;
      if (op == ">=")
        return rowValueStr >= valueStr;
      if (op == "<=")
        return rowValueStr <= valueStr;
    }
  } catch (const std::invalid_argument &e) {
    std::cerr << "条件评估中类型转换错误: " << e.what() << " (row_value: '"
              << rowValueStr << "', target_value: '" << valueStr << "')"
              << std::endl;
    return false;
  } catch (const std::out_of_range &e) {
    std::cerr << "条件评估中数值超出范围错误: " << e.what() << " (row_value: '"
              << rowValueStr << "', target_value: '" << valueStr << "')"
              << std::endl;
    return false;
  }

  return false; // 默认不匹配或遇到无法处理的情况
}

/**
 * @brief 评估一个复杂的条件字符串，支持 AND 和 OR 逻辑操作符。
 * 优先级：OR 的优先级低于 AND（即先处理 AND，再处理 OR）。
 * 不支持括号。
 * @param row 当前行数据
 * @param tableMetadata 表的元数据
 * @param condition 完整的条件字符串
 * @return 如果条件匹配则返回 true，否则返回 false
 */
bool evaluateCondition(const Row &row, const TableData &tableMetadata,
                       const std::string &condition) {
  std::string trimmedCondition = trim(condition);
  if (trimmedCondition.empty()) {
    return true; // 空条件，默认匹配
  }

  // Step 1: Handle OR (lowest precedence)
  size_t orPos = trimmedCondition.find(" OR ");
  if (orPos != std::string::npos) {
    std::string left = trimmedCondition.substr(0, orPos);
    std::string right =
        trimmedCondition.substr(orPos + 4); // " OR " has 4 chars
    return evaluateCondition(row, tableMetadata, left) ||
           evaluateCondition(row, tableMetadata, right);
  }

  // Step 2: Handle AND
  size_t andPos = trimmedCondition.find(" AND ");
  if (andPos != std::string::npos) {
    std::string left = trimmedCondition.substr(0, andPos);
    std::string right =
        trimmedCondition.substr(andPos + 5); // " AND " has 5 chars
    return evaluateCondition(row, tableMetadata, left) &&
           evaluateCondition(row, tableMetadata, right);
  }

  // Step 3: If no AND/OR, it's a single comparison
  return evaluateSingleComparison(row, tableMetadata, trimmedCondition);
}

// QueryResult 的具体实现类，现在在 DMLOperations.cpp 中定义
// 在 DatabaseAPI.hpp 中，只需要 QueryResult 抽象类的声明
class InMemoryQueryResult : public QueryResult {
private:
  std::vector<Row> rows_;          // 实际的结果数据
  const TableData *tableMetadata_; // 指向原始表的元数据
  mutable int currentRowIndex_;    // 当前遍历到的行索引

public:
  InMemoryQueryResult(std::vector<Row> rows, const TableData *tableMetadata)
      : rows_(std::move(rows)), tableMetadata_(tableMetadata),
        currentRowIndex_(-1) {
    if (!tableMetadata_) {
      throw std::runtime_error("InMemoryQueryResult: 表元数据为空。");
    }
  }

  ~InMemoryQueryResult() override = default;

  int getRowCount() const override { return static_cast<int>(rows_.size()); }

  int getColumnCount() const override {
    return static_cast<int>(tableMetadata_->columns.size());
  }

  std::string getColumnName(int index) const override {
    if (index < 0 || index >= tableMetadata_->columns.size()) {
      throw std::out_of_range("列索引超出范围。");
    }
    return tableMetadata_->columns[index].name;
  }

  DataType getColumnType(int index) const override {
    if (index < 0 || index >= tableMetadata_->columns.size()) {
      throw std::out_of_range("列索引超出范围。");
    }
    return tableMetadata_->columns[index].type;
  }

  bool next() override {
    currentRowIndex_++;
    return currentRowIndex_ < rows_.size();
  }

  std::string getString(int columnIndex) const override {
    if (currentRowIndex_ < 0 || currentRowIndex_ >= rows_.size()) {
      throw std::runtime_error("没有当前行或行索引超出范围。");
    }
    // 确保列索引在当前行数据范围内
    if (columnIndex < 0 || columnIndex >= rows_[currentRowIndex_].size()) {
      throw std::out_of_range("当前行数据列索引超出范围。");
    }
    return rows_[currentRowIndex_][columnIndex];
  }

  int getInt(int columnIndex) const override {
    // 确保获取的列类型匹配，否则抛出异常
    if (getColumnType(columnIndex) != DataType::INT) {
      throw std::runtime_error("尝试从非INT列获取INT类型数据。");
    }
    std::string val = getString(columnIndex);
    int intVal;
    if (DMLHelpers::convertToType(val, intVal)) {
      return intVal;
    }
    throw std::runtime_error("无法将 '" + val + "' 转换为int。");
  }

  double getDouble(int columnIndex) const override {
    // 确保获取的列类型匹配，否则抛出异常
    if (getColumnType(columnIndex) != DataType::DOUBLE) {
      throw std::runtime_error("尝试从非DOUBLE列获取DOUBLE类型数据。");
    }
    std::string val = getString(columnIndex);
    double doubleVal;
    if (DMLHelpers::convertToType(val, doubleVal)) {
      return doubleVal;
    }
    throw std::runtime_error("无法将 '" + val + "' 转换为double。");
  }
};

} // namespace DMLHelpers

// --- DMLOperations::Impl 的具体实现 ---
// 它会持有 DatabaseCoreImpl* 的指针来访问数据
class DMLOperations::Impl {
public:
  DatabaseCoreImpl *core_impl_; // 指向数据库核心实现的指针

  explicit Impl(DatabaseCoreImpl *core) : core_impl_(core) {
    if (!core_impl_) {
      throw std::invalid_argument(
          "Core implementation pointer cannot be null.");
    }
    std::cout << "DMLOperations::Impl: 已初始化。" << std::endl;
  }
  // DMLOperations::Impl 析构函数没有特别需要清理的资源，因为表数据由
  // DatabaseCoreImpl 管理
  // ~Impl() { std::cout << "DMLOperations::Impl: 已销毁。" << std::endl; }

  /**
   * @brief 向表中插入一条记录。
   * @param tableName 要插入记录的表的名称。
   * @param values 键值对，表示列名及其对应的值。
   * @return 成功插入的行数（通常为1），如果失败则返回0。
   */
  int insert(const std::string &tableName,
             const std::map<std::string, std::string> &values) {
    if (core_impl_->currentDbName.empty()) {
      std::cerr << "Error: No database selected." << std::endl;
      return 0;
    }

    // 尝试从内存中获取可修改的 TableData
    TableData *table = nullptr;
    auto it = core_impl_->tables.find(tableName);
    if (it != core_impl_->tables.end()) {
      table = &it->second;
    }

    if (!table) {
      throw TableNotFoundException("插入失败: 表 '" + tableName +
                                   "' 不存在或未加载到内存。");
    }

    Row newRow(table->columns.size());
    std::string primaryKeyValue;
    bool hasPrimaryKey = false;
    int primaryKeyColIndex = -1;

    for (const auto &colDef : table->columns) {
      int colIndex = table->getColumnIndex(colDef.name);
      if (colIndex != -1) {
        auto it_val = values.find(colDef.name);
        if (it_val != values.end()) {
          newRow[colIndex] = it_val->second;
        } else {
          // 如果某个列没有提供值，则根据类型设置默认值
          if (colDef.type == DataType::STRING)
            newRow[colIndex] = "";
          else if (colDef.type == DataType::INT)
            newRow[colIndex] = "0";
          else if (colDef.type == DataType::DOUBLE)
            newRow[colIndex] = "0.0";
          else if (colDef.type == DataType::BOOL)
            newRow[colIndex] = "0";
        }

        if (colDef.isPrimaryKey) {
          primaryKeyValue = newRow[colIndex];
          hasPrimaryKey = true;
          primaryKeyColIndex = colIndex;
        }
      }
    }

    // 检查主键重复
    if (hasPrimaryKey) {
      for (const Row &existingRow : table->rows) {
        if (primaryKeyColIndex != -1 &&
            existingRow.size() > primaryKeyColIndex &&
            existingRow[primaryKeyColIndex] == primaryKeyValue) {
          std::cerr << "Error: Duplicate primary key value '" << primaryKeyValue
                    << "' for table '" << tableName << "'." << std::endl;
          return 0; // 主键重复，插入失败
        }
      }
    }

    table->rows.push_back(newRow);
    // 如果有事务，记录到日志（简化版，实际应记录操作和数据）
    if (core_impl_->isTransactionActive) {
      std::ofstream logFile(core_impl_->transactionLogPath, std::ios::app);
      if (logFile.is_open()) {
        std::string rowDataString;
        bool firstCol = true;
        for (const auto &val : newRow) {
          if (!firstCol)
            rowDataString += ",";
          rowDataString += val;
          firstCol = false;
        }
        logFile << "INSERT_BY_NAME;" << tableName << ";" << rowDataString
                << std::endl; // 区分日志条目
        logFile.close();
      }
    }

    std::cout << "DMLOperations::Impl: 成功插入到表 '" << tableName << "'。"
              << std::endl;
    return 1; // 成功插入一行
  }

  /**
   * @brief 向表中插入一条记录，按照列的索引顺序提供值。
   * 如果提供的值的数量少于表的列数，则剩余列将使用默认值。
   * @param tableName 要插入记录的表的名称。
   * @param values_by_index 值的向量，按照表中列的定义顺序排列。
   * @return 成功插入的行数（通常为1），如果失败则返回0。
   */
  int insert(const std::string &tableName,
             const std::vector<std::string> &values_by_index) {
    if (core_impl_->currentDbName.empty()) {
      std::cerr << "Error: No database selected." << std::endl;
      return 0;
    }

    TableData *table = nullptr;
    auto it = core_impl_->tables.find(tableName);
    if (it != core_impl_->tables.end()) {
      table = &it->second;
    }

    if (!table) {
      throw TableNotFoundException("插入失败: 表 '" + tableName +
                                   "' 不存在或未加载到内存。");
    }

    // 检查提供的值的数量是否超过表的列数
    if (values_by_index.size() > table->columns.size()) {
      std::cerr << "Error: 为表 '" << tableName << "' 提供了过多的值。预期最多 "
                << table->columns.size() << " 个值，但实际提供了 "
                << values_by_index.size() << " 个。" << std::endl;
      return 0;
    }

    Row newRow(table->columns.size());
    std::string primaryKeyValue;
    bool hasPrimaryKey = false;
    int primaryKeyColIndex = -1;

    // 按照索引插入值并处理默认值
    for (size_t i = 0; i < table->columns.size(); ++i) {
      if (i < values_by_index.size()) {
        // 如果提供了该索引的值，则使用它
        newRow[i] = values_by_index[i];
      } else {
        // 如果提供的 `values_by_index` 数量不足，则为剩余列设置默认值
        const auto &colDef = table->columns[i];
        if (colDef.type == DataType::STRING)
          newRow[i] = "";
        else if (colDef.type == DataType::INT)
          newRow[i] = "0";
        else if (colDef.type == DataType::DOUBLE)
          newRow[i] = "0.0";
        else if (colDef.type == DataType::BOOL)
          newRow[i] = "0";
      }

      // 检查是否是主键列
      if (table->columns[i].isPrimaryKey) {
        primaryKeyValue = newRow[i];
        hasPrimaryKey = true;
        primaryKeyColIndex = static_cast<int>(i); // 记录主键列的索引
      }
    }

    // 检查主键重复
    if (hasPrimaryKey) {
      for (const Row &existingRow : table->rows) {
        if (primaryKeyColIndex != -1 &&
            existingRow.size() > primaryKeyColIndex &&
            existingRow[primaryKeyColIndex] == primaryKeyValue) {
          std::cerr << "Error: 主键值 '" << primaryKeyValue << "' 在表 '"
                    << tableName << "' 中重复。" << std::endl;
          return 0; // 主键重复，插入失败
        }
      }
    }

    table->rows.push_back(newRow);
    // 如果有事务，记录到日志
    if (core_impl_->isTransactionActive) {
      std::ofstream logFile(core_impl_->transactionLogPath, std::ios::app);
      if (logFile.is_open()) {
        std::string rowDataString;
        bool firstCol = true;
        for (const auto &val : newRow) {
          if (!firstCol)
            rowDataString += ",";
          rowDataString += val;
          firstCol = false;
        }
        logFile << "INSERT_BY_INDEX;" << tableName << ";" << rowDataString
                << std::endl; // 区分日志条目
        logFile.close();
      }
    }

    std::cout << "DMLOperations::Impl: 成功插入到表 '" << tableName << "'。"
              << std::endl;
    return 1; // 成功插入一行
  }

  /**
   * @brief 更新表中符合条件的记录。
   * @param tableName 要更新记录的表的名称。
   * @param updates 键值对，表示要更新的列名及其新值。
   * @param whereClause 用于筛选记录的条件字符串（例如："age > 30 AND city =
   * 'New York'"）。
   * @return 受影响的行数。
   */
  int update(const std::string &tableName,
             const std::map<std::string, std::string> &updates,
             const std::string &whereClause) {
    if (core_impl_->currentDbName.empty()) {
      std::cerr << "Error: No database selected." << std::endl;
      return 0;
    }

    TableData *table = nullptr;
    auto it = core_impl_->tables.find(tableName);
    if (it != core_impl_->tables.end()) {
      table = &it->second;
    }
    

    if (!table) {
      throw TableNotFoundException("更新失败: 表 '" + tableName +
                                   "' 不存在或未加载到内存。");
    }

    int affectedRows = 0;
    for (Row &row : table->rows) {
      if (DMLHelpers::evaluateCondition(row, *table, whereClause)) {
        // 如果有事务，记录旧行数据到日志
        std::string oldRowDataString;
        if (core_impl_->isTransactionActive) {
          bool firstCol = true;
          for (const auto &val : row) {
            if (!firstCol)
              oldRowDataString += ",";
            oldRowDataString += val;
            firstCol = false;
          }
        }

        for (const auto &pair : updates) {
          int colIndex = table->getColumnIndex(pair.first);
          if (colIndex != -1) {
            row[colIndex] = pair.second;
          } else {
            std::cerr << "Warning: 更新数据时列 '" << pair.first
                      << "' 不存在于表 '" << tableName << "' 中。" << std::endl;
          }
        }
        Row newRow(table->columns.size());
        
        // 如果有事务，记录新行数据到日志
        if (core_impl_->isTransactionActive) {
          std::ofstream logFile(core_impl_->transactionLogPath, std::ios::app);
          if (logFile.is_open()) {
            std::string newRowDataString;
            bool firstCol = true;
            for (const auto &val : newRow) {
              if (!firstCol)
                newRowDataString += ",";
              newRowDataString += val;
              firstCol = false;
            }
            // 记录 UPDATE 操作：表名;旧数据;新数据
            logFile << "UPDATE;" << tableName << ";" << oldRowDataString << ";"
                    << newRowDataString << std::endl;
            logFile.close();
          }
        }
        affectedRows++;
      }
    }
    std::cout << "DMLOperations::Impl: 更新表 '" << tableName
              << "'，受影响行数: " << affectedRows << std::endl;
    return affectedRows;
  }

  /**
   * @brief 删除表中符合条件的记录。
   * @param tableName 要删除记录的表的名称。
   * @param whereClause 用于筛选记录的条件字符串。
   * @return 被删除的行数。
   */
  int remove(const std::string &tableName, const std::string &whereClause) {
    if (core_impl_->currentDbName.empty()) {
      std::cerr << "Error: No database selected." << std::endl;
      return 0;
    }

    TableData *table = nullptr;
    auto it = core_impl_->tables.find(tableName);
    if (it != core_impl_->tables.end()) {
      table = &it->second;
    }

    if (!table) {
      throw TableNotFoundException("删除失败: 表 '" + tableName +
                                   "' 不存在或未加载到内存。");
    }

    int initialSize = static_cast<int>(table->rows.size());
    std::vector<Row> rowsToDelete; // 用于存储要删除的行，以便记录到日志

    // 使用 erase-remove idiom 删除符合条件的行
    auto newEnd = std::remove_if(
        table->rows.begin(), table->rows.end(), [&](const Row &row) {
          if (DMLHelpers::evaluateCondition(row, *table, whereClause)) {
            if (core_impl_->isTransactionActive) {
              rowsToDelete.push_back(row); // 记录被删除的行
            }
            return true; // 标记为删除
          }
          return false;
        });
    table->rows.erase(newEnd, table->rows.end());
    int removedRows = initialSize - static_cast<int>(table->rows.size());

    // 如果有事务，记录被删除的行到日志
    if (core_impl_->isTransactionActive) {
      std::ofstream logFile(core_impl_->transactionLogPath, std::ios::app);
      if (logFile.is_open()) {
        for (const auto &deletedRow : rowsToDelete) {
          std::string rowDataString;
          bool firstCol = true;
          for (const auto &val : deletedRow) {
            if (!firstCol)
              rowDataString += ",";
            rowDataString += val;
            firstCol = false;
          }
          // 记录 DELETE 操作：表名;数据
          logFile << "DELETE;" << tableName << ";" << rowDataString
                  << std::endl;
        }
        logFile.close();
      }
    }

    std::cout << "DMLOperations::Impl: 从表 '" << tableName
              << "' 删除，被删除行数: " << removedRows << std::endl;
    return removedRows;
  }

  /**
   * @brief 从表中查询记录。
   * @param tableName 要查询的表的名称。
   * @param whereClause 用于筛选记录的条件字符串（可选）。
   * @param orderBy 用于结果排序的列名（可选）。
   * @return 指向 QueryResult 对象的 unique_ptr，包含查询结果集。
   * 客户端应通过 QueryResult 迭代结果。
   */
  // 调用实例：std::unique_ptr<QueryResult> complexQueryUsers =
  // dml.select("Users", "age > 25 AND name != 'Bob'");
  std::unique_ptr<QueryResult> select(const std::string &tableName,
                                      const std::string &whereClause,
                                      const std::string &orderBy) {
    if (core_impl_->currentDbName.empty()) {
      std::cerr << "Error: No database selected." << std::endl;
      return nullptr;
    }

    const TableData *table = nullptr;
    auto it = core_impl_->tables.find(tableName);
    if (it != core_impl_->tables.end()) {
      table = &it->second;
    }

    if (!table) {
      throw TableNotFoundException("查询失败: 表 '" + tableName +
                                   "' 不存在或未加载到内存。");
    }

    std::vector<Row> resultSet;
    for (const Row &row : table->rows) {
      if (DMLHelpers::evaluateCondition(row, *table, whereClause)) {
        resultSet.push_back(row);
      }
    }

    // 简化的 orderBy 实现 (只支持单列排序)
    if (!orderBy.empty()) {
      int orderColIndex = table->getColumnIndex(orderBy);
      if (orderColIndex != -1) {
        DataType orderColType = table->getColumnType(orderColIndex);
        std::sort(resultSet.begin(), resultSet.end(),
                  [&](const Row &a, const Row &b) {
                    // 确保行有足够的元素
                    if (orderColIndex >= a.size() ||
                        orderColIndex >= b.size()) {
                      // 这不应该发生，但为了安全考虑，可以定义一个稳定顺序或抛出异常
                      return false;
                    }
                    // 根据列类型进行比较
                    if (orderColType == DataType::INT) {
                      int val_a, val_b;
                      if (DMLHelpers::convertToType(a[orderColIndex], val_a) &&
                          DMLHelpers::convertToType(b[orderColIndex], val_b)) {
                        return val_a < val_b;
                      }
                      return false; // 转换失败
                    } else if (orderColType == DataType::DOUBLE) {
                      double val_a, val_b;
                      if (DMLHelpers::convertToType(a[orderColIndex], val_a) &&
                          DMLHelpers::convertToType(b[orderColIndex], val_b)) {
                        return val_a < val_b;
                      }
                      return false; // conversion failed
                    } else { // 默认为字符串比较 (DataType::STRING 或 BOOL)
                      return a[orderColIndex] < b[orderColIndex];
                    }
                  });
      } else {
        std::cerr << "Warning: OrderBy 列 '" << orderBy
                  << "' 未找到或其类型不支持排序。" << std::endl;
      }
    }

    std::cout << "DMLOperations::Impl: 查询表 '" << tableName << "'，返回 "
              << resultSet.size() << " 行。" << std::endl;
    return std::make_unique<DMLHelpers::InMemoryQueryResult>(
        std::move(resultSet), table);
  }
};

// --- DMLOperations 类的构造函数和析构函数实现 ---
DMLOperations::DMLOperations(
    DatabaseCoreImpl *core_impl_ptr) // 接受 DatabaseCoreImpl*
    : pImpl(std::make_unique<Impl>(core_impl_ptr)) {
} // 使用 unique_ptr 初始化 pImpl

// 析构函数必须在这里定义，因为 pImpl 是 unique_ptr，它的析构需要 Impl
// 的完整定义
DMLOperations::~DMLOperations() = default;

// --- DMLOperations 公开接口的实现（转发给 Pimpl Impl） ---
int DMLOperations::insert(const std::string &tableName,
                          const std::map<std::string, std::string> &values) {
  return pImpl->insert(tableName, values);
}

// 新增的 insert 重载函数的实现
int DMLOperations::insert(const std::string &tableName,
                          const std::vector<std::string> &values_by_index) {
  return pImpl->insert(tableName, values_by_index);
}

int DMLOperations::update(const std::string &tableName,
                          const std::map<std::string, std::string> &updates,
                          const std::string &whereClause) {
  return pImpl->update(tableName, updates, whereClause);
}

int DMLOperations::remove(const std::string &tableName,
                          const std::string &whereClause) {
  return pImpl->remove(tableName, whereClause);
}

std::unique_ptr<QueryResult>
DMLOperations::select(const std::string &tableName,
                      const std::string &whereClause,
                      const std::string &orderBy) {
  return pImpl->select(tableName, whereClause, orderBy);
}