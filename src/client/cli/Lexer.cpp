#include "Lexer.hpp"
#include <cctype>
#include <unordered_map>

// 完整关键字映射表
const std::unordered_map<std::string, TokenType> KEYWORDS = {
    {"CREATE", TokenType::KEYWORD_CREATE}, {"DROP", TokenType::KEYWORD_DROP},
    {"TABLE", TokenType::KEYWORD_TABLE}, {"DATABASE", TokenType::KEYWORD_DATABASE},
    {"PRIMARY", TokenType::KEYWORD_PRIMARY}, {"USE", TokenType::KEYWORD_USE},
    {"INSERT", TokenType::KEYWORD_INSERT}, {"INTO", TokenType::KEYWORD_INTO},
    {"VALUES", TokenType::KEYWORD_VALUES}, {"SELECT", TokenType::KEYWORD_SELECT},
    {"FROM", TokenType::KEYWORD_FROM}, {"WHERE", TokenType::KEYWORD_WHERE},
    {"UPDATE", TokenType::KEYWORD_UPDATE}, {"SET", TokenType::KEYWORD_SET},
    {"DELETE", TokenType::KEYWORD_DELETE}, {"INT", TokenType::KEYWORD_INT},
    {"STRING", TokenType::KEYWORD_STRING}
};

// to_string 实现，用于调试
std::string to_string(TokenType type) {
    // ... 此处省略了完整的实现，可以根据 Token.hpp 自行补全，不影响功能 ...
    return "TokenType";
}

Lexer::Lexer(const std::string& input) : input_(input) {}

std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;
    Token token;
    do {
        token = next_token();
        tokens.push_back(token);
    } while (token.type != TokenType::END_OF_INPUT);
    return tokens;
}

char Lexer::peek() { if (position_ >= input_.length()) return '\0'; return input_[position_]; }
char Lexer::advance() { if (position_ >= input_.length()) return '\0'; return input_[position_++]; }
void Lexer::skip_whitespace() { while (position_ < input_.length() && std::isspace(peek())) advance(); }

Token Lexer::next_token() {
    skip_whitespace();
    if (position_ >= input_.length()) return {TokenType::END_OF_INPUT, ""};
    char current_char = peek();

    if (std::isalpha(current_char)) {
        std::string word;
        while (position_ < input_.length() && (std::isalnum(peek()) || peek() == '_')) word += advance();
        std::string upper_word = word;
        for(auto &c : upper_word) c = toupper(c);
        if (KEYWORDS.count(upper_word)) return {KEYWORDS.at(upper_word), upper_word};
        return {TokenType::IDENTIFIER, word};
    }
    if (std::isdigit(current_char)) {
        std::string num_str;
        while (position_ < input_.length() && std::isdigit(peek())) num_str += advance();
        return {TokenType::NUMERIC_LITERAL, num_str};
    }
    if (current_char == '"') {
        advance();
        std::string str_literal;
        while (position_ < input_.length() && peek() != '"') str_literal += advance();
        if (peek() == '"') advance();
        return {TokenType::STRING_LITERAL, str_literal};
    }
    switch (current_char) {
        case '(': advance(); return {TokenType::PAREN_OPEN, "("};
        case ')': advance(); return {TokenType::PAREN_CLOSE, ")"};
        case ',': advance(); return {TokenType::COMMA, ","};
        case ';': advance(); return {TokenType::SEMICOLON, ";"};
        case '*': advance(); return {TokenType::ASTERISK, "*"};
        case '=': case '>': case '<': advance(); return {TokenType::OPERATOR, std::string(1, current_char)};
    }
    advance();
    return {TokenType::UNKNOWN, std::string(1, current_char)};
}