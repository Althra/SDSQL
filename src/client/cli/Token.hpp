#ifndef TOKEN_HPP
#define TOKEN_HPP

#include <string>
#include <vector>
#include <iostream>

enum class TokenType {
    // Keywords for DDL
    KEYWORD_CREATE, KEYWORD_DROP, KEYWORD_TABLE, KEYWORD_DATABASE,
    KEYWORD_PRIMARY, KEYWORD_USE,

    // Keywords for DML
    KEYWORD_INSERT, KEYWORD_INTO, KEYWORD_VALUES,
    KEYWORD_SELECT, KEYWORD_FROM, KEYWORD_WHERE,
    KEYWORD_UPDATE, KEYWORD_SET,
    KEYWORD_DELETE,
    
    // Data Types
    KEYWORD_INT, KEYWORD_STRING,

    // Symbols
    IDENTIFIER,         // 标识符 (表名, 列名)
    STRING_LITERAL,     // 字符串字面量 (e.g., "hello world")
    NUMERIC_LITERAL,    // 数字字面量 (e.g., 123)

    // Operators and Delimiters
    PAREN_OPEN,         // (
    PAREN_CLOSE,        // )
    COMMA,              // ,
    SEMICOLON,          // ;
    OPERATOR,           // =, >, <
    ASTERISK,           // *

    // Control
    END_OF_INPUT,       // 输入结束
    UNKNOWN             // 未知词元
};

// 用于调试时打印 TokenType
std::string to_string(TokenType type);

struct Token {
    TokenType type;
    std::string value;
};

#endif // TOKEN_HPP