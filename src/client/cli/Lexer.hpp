#ifndef LEXER_HPP
#define LEXER_HPP

#include "Token.hpp"
#include <string>
#include <vector>

class Lexer {
public:
    explicit Lexer(const std::string& input);
    std::vector<Token> tokenize();

private:
    Token next_token();
    char peek();
    char advance();
    void skip_whitespace();

    std::string input_;
    size_t position_ = 0;
};

#endif // LEXER_HPP