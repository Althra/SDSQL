#ifndef PARSER_HPP
#define PARSER_HPP

#include "Lexer.hpp"
#include <vector>
#include <memory>
#include <string>
#include <optional>

// --- 定义所有命令的结构化对象 ---

// 命令基类
struct Command { virtual ~Command() = default; };

// 字面量
struct LiteralValue { TokenType type; std::string value; };

// WHERE 子句
struct Condition { std::string column; std::string op; LiteralValue value; };
using WhereClause = Condition;

// DDL 命令
struct CreateDatabaseCommand : public Command { std::string db_name; };
struct DropDatabaseCommand : public Command { std::string db_name; };
struct UseDatabaseCommand : public Command { std::string db_name; };
struct DropTableCommand : public Command { std::string table_name; };

struct ColumnDef { std::string name; TokenType type; bool is_primary = false; };
struct CreateTableCommand : public Command { std::string table_name; std::vector<ColumnDef> columns; };

// DML 命令
struct InsertCommand : public Command { std::string table_name; std::vector<LiteralValue> values; };
struct DeleteCommand : public Command { std::string table_name; std::optional<WhereClause> where_clause; };

struct SetClause { std::string column; LiteralValue value; };
struct UpdateCommand : public Command { std::string table_name; std::vector<SetClause> set_clauses; std::optional<WhereClause> where_clause; };

struct SelectCommand : public Command {
    bool select_all = false;
    std::vector<std::string> columns;
    std::string table_name;
    std::optional<WhereClause> where_clause;
};

// --- 语法分析器 ---
class Parser {
public:
    explicit Parser(std::vector<Token> tokens);
    std::unique_ptr<Command> parse();

private:
    std::unique_ptr<Command> parse_create();
    std::unique_ptr<Command> parse_drop();
    std::unique_ptr<Command> parse_use();
    std::unique_ptr<Command> parse_insert();
    std::unique_ptr<Command> parse_delete();
    std::unique_ptr<Command> parse_update();
    std::unique_ptr<Command> parse_select();
    
    std::optional<WhereClause> parse_optional_where();

    const Token& consume(TokenType expected);
    const Token& peek(int offset = 0);
    std::vector<Token> tokens_;
    size_t position_ = 0;
};

#endif // PARSER_HPP