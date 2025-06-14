// DMLOperations.cpp
// 该文件实现了DMLOperations类的具体逻辑，并增强了条件评估功能

#include "DatabaseAPI.hpp" // 包含数据库API头文件

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <memory>       // 用于 std::unique_ptr
#include <algorithm>    // 用于 std::remove_if, std::sort
#include <charconv>     // 用于 std::from_chars (C++17特性，用于数字转换)
#include <stdexcept>    // 用于标准异常类

// --- 辅助函数：类型转换（用于条件评估）---
// 这些函数在 DMLOperations.cpp 内部定义，不暴露给外部

namespace DMLHelpers { // 使用命名空间 DMLHelpers 避免全局命名冲突

// Helper to remove leading/trailing whitespace
std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (std::string::npos == first) {
        return str;
    }
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, (last - first + 1));
}

template<typename T>
bool convertToType(const std::string& s, T& value) {
    if constexpr (std::is_same_v<T, int>) {
        auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
        return ec == std::errc();
    } else if constexpr (std::is_same_v<T, double>) {
        auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
        return ec == std::errc();
    } else if constexpr (std::is_same_v<T, bool>) {
        // Support "1", "0", "true", "false", case-insensitive
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
    return false; // Unknown type
}

// 前向声明 evaluateCondition，因为 evaluateSingleComparison 可能在未来通过递归调用
// 或者为了结构清晰，先声明它
bool evaluateCondition(const Row& row, const TableData& tableMetadata, const std::string& condition);

/**
 * @brief 评估单个比较条件 (e.g., "column_name = 'value'", "age > 30")
 * @param row 当前行数据
 * @param tableMetadata 表的元数据
 * @param condition 单个比较条件字符串
 * @return 如果条件匹配则返回 true，否则返回 false
 */
bool evaluateSingleComparison(const Row& row, const TableData& tableMetadata, const std::string& condition) {
    std::string trimmedCondition = trim(condition);
    if (trimmedCondition.empty()) {
        return true; // 空条件视为真，尽管通常不会出现
    }

    // 查找操作符及其位置，按照优先级或长度排序以便正确匹配
    std::vector<std::pair<std::string, std::string>> operators = {
        {">=", ">="}, {"<=", "<="}, {"!=", "!="},
        {"=", "="}, {">", ">"}, {"<", "<"}
    };

    size_t opPos = std::string::npos;
    std::string op;

    for (const auto& op_pair : operators) {
        size_t currentOpPos = trimmedCondition.find(op_pair.first);
        if (currentOpPos != std::string::npos) {
            // 找到操作符，确保不是部分匹配（例如 '=' 不应匹配 '!='）
            if (op_pair.first.length() == 1 && currentOpPos > 0) {
                 char prevChar = trimmedCondition[currentOpPos-1];
                 if ((op_pair.first == "=" && (prevChar == '>' || prevChar == '<' || prevChar == '!')) ||
                     (op_pair.first == ">" && prevChar == '<') ||
                     (op_pair.first == "<" && prevChar == '>')) {
                     continue; // Skip if it's part of a longer operator (e.g., found '=' in '!=')
                 }
            } else if (op_pair.first.length() == 2 && currentOpPos > 0) {
                 if (trimmedCondition[currentOpPos-1] == '<' && op_pair.first == ">=") continue;
            }

            opPos = currentOpPos;
            op = op_pair.second;
            break; // 找到最左边的操作符后停止
        }
    }

    if (opPos == std::string::npos) {
        std::cerr << "Warning: 无法解析单个比较条件 '" << trimmedCondition << "': 未找到有效操作符。" << std::endl;
        return false;
    }

    std::string colName = trim(trimmedCondition.substr(0, opPos));
    std::string valueStr = trim(trimmedCondition.substr(opPos + op.length()));

    // 如果是字符串字面量，去除单引号
    if (valueStr.length() >= 2 && valueStr.front() == '\'' && valueStr.back() == '\'') {
        valueStr = valueStr.substr(1, valueStr.length() - 2);
    }

    int colIndex = tableMetadata.getColumnIndex(colName);
    if (colIndex == -1) {
        return false;
    }

    // 确保行数据有效，防止越界访问
    if (colIndex >= row.size()) {
        std::cerr << "Error: 行数据中列索引越界。" << std::endl;
        return false;
    }

    const std::string& rowValueStr = row[colIndex];
    DataType colType = tableMetadata.getColumnType(colIndex);

    // 根据数据类型和操作符进行比较
    try {
        if (colType == DataType::INT) {
            int rowVal, targetVal;
            if (convertToType(rowValueStr, rowVal) && convertToType(valueStr, targetVal)) {
                if (op == "=") return rowVal == targetVal;
                if (op == "!=") return rowVal != targetVal;
                if (op == ">") return rowVal > targetVal;
                if (op == "<") return rowVal < targetVal;
                if (op == ">=") return rowVal >= targetVal;
                if (op == "<=") return rowVal <= targetVal;
            }
        } else if (colType == DataType::DOUBLE) {
            double rowVal, targetVal;
            if (convertToType(rowValueStr, rowVal) && convertToType(valueStr, targetVal)) {
                if (op == "=") return rowVal == targetVal;
                if (op == "!=") return rowVal != targetVal;
                if (op == ">") return rowVal > targetVal;
                if (op == "<") return rowVal < targetVal;
                if (op == ">=") return rowVal >= targetVal;
                if (op == "<=") return rowVal <= targetVal;
            }
        } else if (colType == DataType::BOOL) {
            bool rowVal, targetVal;
            if (convertToType(rowValueStr, rowVal) && convertToType(valueStr, targetVal)) {
                if (op == "=") return rowVal == targetVal;
                if (op == "!=") return rowVal != targetVal;
                // 对于布尔类型，通常只支持等式和非等式
                std::cerr << "Warning: 布尔类型不支持 '>','<','>=','<=' 比较符。" << std::endl;
            }
        } else { // DataType::STRING
            // 字符串的字典序比较
            if (op == "=") return rowValueStr == valueStr;
            if (op == "!=") return rowValueStr != valueStr;
            if (op == ">") return rowValueStr > valueStr;
            if (op == "<") return rowValueStr < valueStr;
            if (op == ">=") return rowValueStr >= valueStr; // 修正：此处原为 valueValueStr
            if (op == "<=") return rowValueStr <= valueStr;
        }
    } catch (const std::invalid_argument& e) {
        std::cerr << "条件评估中类型转换错误: " << e.what() << " (row_value: '" << rowValueStr << "', target_value: '" << valueStr << "')" << std::endl;
        return false;
    } catch (const std::out_of_range& e) {
        std::cerr << "条件评估中数值超出范围错误: " << e.what() << " (row_value: '" << rowValueStr << "', target_value: '" << valueStr << "')" << std::endl;
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
bool evaluateCondition(const Row& row, const TableData& tableMetadata, const std::string& condition) {
    std::string trimmedCondition = trim(condition);
    if (trimmedCondition.empty()) {
        return true; // 空条件，默认匹配
    }

    // Step 1: Handle OR (lowest precedence)
    size_t orPos = trimmedCondition.find(" OR ");
    if (orPos != std::string::npos) {
        std::string left = trimmedCondition.substr(0, orPos);
        std::string right = trimmedCondition.substr(orPos + 4); // " OR " has 4 chars
        return evaluateCondition(row, tableMetadata, left) || evaluateCondition(row, tableMetadata, right);
    }

    // Step 2: Handle AND
    size_t andPos = trimmedCondition.find(" AND ");
    if (andPos != std::string::npos) {
        std::string left = trimmedCondition.substr(0, andPos);
        std::string right = trimmedCondition.substr(andPos + 5); // " AND " has 5 chars
        return evaluateCondition(row, tableMetadata, left) && evaluateCondition(row, tableMetadata, right);
    }

    // Step 3: If no AND/OR, it's a single comparison
    return evaluateSingleComparison(row, tableMetadata, trimmedCondition);
}


// QueryResult 的具体实现类
class InMemoryQueryResult : public QueryResult {
private:
    std::vector<Row> rows_;         // 实际的结果数据
    const TableData* tableMetadata_; // 指向原始表的元数据
    int currentRowIndex_;           // 当前遍历到的行索引

public:
    InMemoryQueryResult(std::vector<Row> rows, const TableData* tableMetadata)
        : rows_(std::move(rows)), tableMetadata_(tableMetadata), currentRowIndex_(-1) {
        if (!tableMetadata_) {
            throw std::runtime_error("InMemoryQueryResult: 表元数据为空。");
        }
    }

    ~InMemoryQueryResult() override = default;

    int getRowCount() const override {
        return static_cast<int>(rows_.size());
    }

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
        try {
            return std::stoi(val);
        } catch (const std::exception& e) {
            throw std::runtime_error("无法将 '" + val + "' 转换为int: " + e.what());
        }
    }

    double getDouble(int columnIndex) const override {
        // 确保获取的列类型匹配，否则抛出异常
        if (getColumnType(columnIndex) != DataType::DOUBLE) {
            throw std::runtime_error("尝试从非DOUBLE列获取DOUBLE类型数据。");
        }
        std::string val = getString(columnIndex);
        try {
            return std::stod(val);
        } catch (const std::exception& e) {
            throw std::runtime_error("无法将 '" + val + "' 转换为double: " + e.what());
        }
    }
};

} // namespace DMLHelpers


// --- DMLOperations::Impl 的具体实现 ---
class DMLOperations::Impl {
public:
    Database::Impl* dbCoreImpl_; // 指向数据库核心实现的指针

    explicit Impl(Database::Impl* db_core_impl) : dbCoreImpl_(db_core_impl) {
        if (!db_core_impl) {
            throw std::runtime_error("DMLOperations::Impl: 数据库核心实现指针为空。");
        }
        std::cout << "DMLOperations::Impl: 已初始化。" << std::endl;
    }
    ~Impl() {
        std::cout << "DMLOperations::Impl: 已销毁。" << std::endl;
    }

    /**
     * @brief 向表中插入一条记录。
     * @param tableName 要插入记录的表的名称。
     * @param values 键值对，表示列名及其对应的值。
     * @return 成功插入的行数（通常为1），如果失败则返回0。
     */
    int insert(const std::string& tableName, const std::map<std::string, std::string>& values) {
        // --- 权限检查 ---
        if (!dbCoreImpl_->checkPermissionInternal(dbCoreImpl_->getCurrentUser(), PermissionType::INSERT, "TABLE", tableName)) {
            throw PermissionDeniedException("INSERT 权限不足: 用户 '" + dbCoreImpl_->getCurrentUser() + "' 无法在表 '" + tableName + "' 上执行插入操作。");
        }

        TableData* table = dbCoreImpl_->getMutableTableData(tableName);
        if (!table) {
            // DMLOps 现在将尝试从文件加载表，如果不存在则抛出 TableNotFoundException
            // 或者，DDLOps 应该负责确保表在内存中可用
            // 这里我们假设 DMLOperations 的 getMutableTableData 已经尝试加载或 Database::Impl 会处理
            throw TableNotFoundException("插入失败: 表 '" + tableName + "' 不存在或未加载到内存。");
        }

        Row newRow(table->columns.size());
        for (const auto& colDef : table->columns) {
            auto it = values.find(colDef.name);
            int colIndex = table->getColumnIndex(colDef.name);
            if (it != values.end() && colIndex != -1) {
                newRow[colIndex] = it->second;
            } else if (colIndex != -1) {
                newRow[colIndex] = ""; // 默认空字符串
            }
        }
        
        // 记录日志 (如果事务管理模块已实现)
        dbCoreImpl_->logOperation(LogEntry(dbCoreImpl_->currentTransactionId_, LogEntryType::INSERT, tableName, {}, newRow));

        table->rows.push_back(newRow);
        std::cout << "DMLOperations::Impl: 成功插入到表 '" << tableName << "'。" << std::endl;
        return 1;
    }

    /**
     * @brief 更新表中符合条件的记录。
     * @param tableName 要更新记录的表的名称。
     * @param updates 键值对，表示要更新的列名及其新值。
     * @param whereClause 用于筛选记录的条件字符串（例如："age > 30 AND city = 'New York'"）。
     * @return 受影响的行数。
     */
    int update(const std::string& tableName, const std::map<std::string, std::string>& updates, const std::string& whereClause) {
        // --- 权限检查 ---
        if (!dbCoreImpl_->checkPermissionInternal(dbCoreImpl_->getCurrentUser(), PermissionType::UPDATE, "TABLE", tableName)) {
            throw PermissionDeniedException("UPDATE 权限不足: 用户 '" + dbCoreImpl_->getCurrentUser() + "' 无法在表 '" + tableName + "' 上执行更新操作。");
        }

        TableData* table = dbCoreImpl_->getMutableTableData(tableName);
        if (!table) {
            throw TableNotFoundException("更新失败: 表 '" + tableName + "' 不存在或未加载到内存。");
        }

        int affectedRows = 0;
        
        for (size_t i = 0; i < table->rows.size(); ++i) { 
            Row& currentRow = table->rows[i]; 
            if (DMLHelpers::evaluateCondition(currentRow, *table, whereClause)) {
                Row oldRowState = currentRow; // 记录更新前的状态

                for (const auto& pair : updates) {
                    int colIndex = table->getColumnIndex(pair.first);
                    if (colIndex != -1) {
                        currentRow[colIndex] = pair.second;
                    } else {
                        std::cerr << "Warning: 更新数据时列 '" << pair.first << "' 不存在于表 '" << tableName << "' 中。" << std::endl;
                    }
                }
                // 记录日志
                dbCoreImpl_->logOperation(LogEntry(dbCoreImpl_->currentTransactionId_, LogEntryType::UPDATE_OLD_VALUE, tableName, oldRowState, {}, static_cast<int>(i)));
                
                affectedRows++;
            }
        }
        std::cout << "DMLOperations::Impl: 更新表 '" << tableName << "'，受影响行数: " << affectedRows << std::endl;
        return affectedRows;
    }

    /**
     * @brief 删除表中符合条件的记录。
     * @param tableName 要删除记录的表的名称。
     * @param whereClause 用于筛选记录的条件字符串。
     * @return 被删除的行数。
     */
    int remove(const std::string& tableName, const std::string& whereClause) {
        // --- 权限检查 ---
        if (!dbCoreImpl_->checkPermissionInternal(dbCoreImpl_->getCurrentUser(), PermissionType::DELETE, "TABLE", tableName)) {
            throw PermissionDeniedException("DELETE 权限不足: 用户 '" + dbCoreImpl_->getCurrentUser() + "' 无法在表 '" + tableName + "' 上执行删除操作。");
        }

        TableData* table = dbCoreImpl_->getMutableTableData(tableName);
        if (!table) {
            throw TableNotFoundException("删除失败: 表 '" + tableName + "' 不存在或未加载到内存。");
        }

        std::vector<Row> rowsToKeep;
        int removedCount = 0;
        for (const Row& row : table->rows) {
            if (DMLHelpers::evaluateCondition(row, *table, whereClause)) {
                // 记录日志
                dbCoreImpl_->logOperation(LogEntry(dbCoreImpl_->currentTransactionId_, LogEntryType::DELETE, tableName, row, {}));
                removedCount++;
            } else {
                rowsToKeep.push_back(row);
            }
        }
        table->rows = std::move(rowsToKeep); 
        
        std::cout << "DMLOperations::Impl: 从表 '" << tableName << "' 删除，被删除行数: " << removedCount << std::endl;
        return removedCount;
    }

    /**
     * @brief 从表中查询记录。
     * @param tableName 要查询的表的名称。
     * @param whereClause 用于筛选记录的条件字符串（可选）。
     * @param orderBy 用于结果排序的列名（可选）。
     * @return 指向 QueryResult 对象的 unique_ptr，包含查询结果集。
     * 客户端应通过 QueryResult 迭代结果。
     */
    std::unique_ptr<QueryResult> select(const std::string& tableName, const std::string& whereClause, const std::string& orderBy) {
        // --- 权限检查 ---
        if (!dbCoreImpl_->checkPermissionInternal(dbCoreImpl_->getCurrentUser(), PermissionType::SELECT, "TABLE", tableName)) {
            throw PermissionDeniedException("SELECT 权限不足: 用户 '" + dbCoreImpl_->getCurrentUser() + "' 无法在表 '" + tableName + "' 上执行查询操作。");
        }

        const TableData* table = dbCoreImpl_->getTableData(tableName);
        if (!table) {
            throw TableNotFoundException("查询失败: 表 '" + tableName + "' 不存在或未加载到内存。");
        }

        std::vector<Row> resultSet;
        for (const Row& row : table->rows) {
            if (DMLHelpers::evaluateCondition(row, *table, whereClause)) {
                resultSet.push_back(row);
            }
        }

        if (!orderBy.empty()) {
            int orderColIndex = table->getColumnIndex(orderBy);
            if (orderColIndex != -1) {
                DataType orderColType = table->getColumnType(orderColIndex);
                std::sort(resultSet.begin(), resultSet.end(),
                          [&](const Row& a, const Row& b) {
                              if (orderColIndex >= a.size() || orderColIndex >= b.size()) {
                                  return false; 
                              }
                              if (orderColType == DataType::INT) {
                                  return std::stoi(a[orderColIndex]) < std::stoi(b[orderColIndex]);
                              } else if (orderColType == DataType::DOUBLE) {
                                  return std::stod(a[orderColIndex]) < std::stod(b[orderColIndex]);
                              } else { 
                                  return a[orderColIndex] < b[orderColIndex];
                              }
                          });
            } else {
                std::cerr << "Warning: OrderBy 列 '" << orderBy << "' 未找到或其类型不支持排序。" << std::endl;
            }
        }
        
        std::cout << "DMLOperations::Impl: 查询表 '" << tableName << "'，返回 " << resultSet.size() << " 行。" << std::endl;
        return std::make_unique<DMLHelpers::InMemoryQueryResult>(std::move(resultSet), table);
    }
};

// --- DMLOperations 类的构造函数和析构函数实现 ---
DMLOperations::DMLOperations(Database::Impl* db_core_impl) : pImpl(std::make_unique<Impl>(db_core_impl)) {}
DMLOperations::~DMLOperations() = default;

// --- DMLOperations 公开接口的实现（转发给 Pimpl Impl） ---
int DMLOperations::insert(const std::string& tableName, const std::map<std::string, std::string>& values) {
    return pImpl->insert(tableName, values);
}

int DMLOperations::update(const std::string& tableName, const std::map<std::string, std::string>& updates, const std::string& whereClause) {
    return pImpl->update(tableName, updates, whereClause);
}

int DMLOperations::remove(const std::string& tableName, const std::string& whereClause) {
    return pImpl->remove(tableName, whereClause);
}

std::unique_ptr<QueryResult> DMLOperations::select(const std::string& tableName, const std::string& whereClause, const std::string& orderBy) {
    return pImpl->select(tableName, whereClause, orderBy);
}