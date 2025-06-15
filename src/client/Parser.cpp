#include "../../include/client/Parser.hpp"
#include <stdexcept>

Parser::Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

const Token& Parser::peek(int offset) { return tokens_[position_ + offset]; }
const Token& Parser::consume(TokenType expected) {
    if (peek().type == expected) return tokens_[position_++];
    throw std::runtime_error("Syntax Error: Expected different token.");
}

// 主分派函数
std::unique_ptr<Command> Parser::parse() {
    if (peek().type == TokenType::END_OF_INPUT) return nullptr;
    switch(peek().type) {
        case TokenType::KEYWORD_CREATE: return parse_create();
        case TokenType::KEYWORD_DROP: return parse_drop();
        case TokenType::KEYWORD_USE: return parse_use();
        case TokenType::KEYWORD_INSERT: return parse_insert();
        case TokenType::KEYWORD_DELETE: return parse_delete();
        case TokenType::KEYWORD_UPDATE: return parse_update();
        case TokenType::KEYWORD_SELECT: return parse_select();
        default: throw std::runtime_error("Unsupported command: " + peek().value);
    }
}

std::unique_ptr<Command> Parser::parse_create() {
    consume(TokenType::KEYWORD_CREATE);
    if (peek().type == TokenType::KEYWORD_DATABASE) {
        consume(TokenType::KEYWORD_DATABASE);
        auto cmd = std::make_unique<CreateDatabaseCommand>();
        cmd->db_name = consume(TokenType::IDENTIFIER).value;
        return cmd;
    }
    if (peek().type == TokenType::KEYWORD_TABLE) {
        consume(TokenType::KEYWORD_TABLE);
        auto cmd = std::make_unique<CreateTableCommand>();
        cmd->table_name = consume(TokenType::IDENTIFIER).value;
        consume(TokenType::PAREN_OPEN);
        do {
            ColumnDef col;
            col.name = consume(TokenType::IDENTIFIER).value;
            col.type = consume(peek().type).type; // INT or STRING
            if (peek().type == TokenType::KEYWORD_PRIMARY) {
                consume(TokenType::KEYWORD_PRIMARY);
                col.is_primary = true;
            }
            cmd->columns.push_back(col);
        } while (peek().type == TokenType::COMMA && consume(TokenType::COMMA).type == TokenType::COMMA);
        consume(TokenType::PAREN_CLOSE);
        return cmd;
    }
    throw std::runtime_error("Syntax Error: Expected TABLE or DATABASE after CREATE.");
}

std::unique_ptr<Command> Parser::parse_drop() {
    consume(TokenType::KEYWORD_DROP);
    if (peek().type == TokenType::KEYWORD_DATABASE) {
        consume(TokenType::KEYWORD_DATABASE);
        auto cmd = std::make_unique<DropDatabaseCommand>();
        cmd->db_name = consume(TokenType::IDENTIFIER).value;
        return cmd;
    }
    if (peek().type == TokenType::KEYWORD_TABLE) {
        consume(TokenType::KEYWORD_TABLE);
        auto cmd = std::make_unique<DropTableCommand>();
        cmd->table_name = consume(TokenType::IDENTIFIER).value;
        return cmd;
    }
    throw std::runtime_error("Syntax Error: Expected TABLE or DATABASE after DROP.");
}

std::unique_ptr<Command> Parser::parse_use() {
    consume(TokenType::KEYWORD_USE);
    auto cmd = std::make_unique<UseDatabaseCommand>();
    cmd->db_name = consume(TokenType::IDENTIFIER).value;
    return cmd;
}

std::unique_ptr<Command> Parser::parse_insert() {
    auto cmd = std::make_unique<InsertCommand>();
    consume(TokenType::KEYWORD_INSERT);
    consume(TokenType::KEYWORD_INTO);
    cmd->table_name = consume(TokenType::IDENTIFIER).value;
    consume(TokenType::KEYWORD_VALUES);
    consume(TokenType::PAREN_OPEN);
    do {
        const auto& token = peek();
        cmd->values.push_back({token.type, consume(token.type).value});
    } while (peek().type == TokenType::COMMA && consume(TokenType::COMMA).type == TokenType::COMMA);
    consume(TokenType::PAREN_CLOSE);
    return cmd;
}

std::optional<WhereClause> Parser::parse_optional_where() {
    if (peek().type != TokenType::KEYWORD_WHERE) return std::nullopt;
    consume(TokenType::KEYWORD_WHERE);
    Condition cond;
    cond.column = consume(TokenType::IDENTIFIER).value;
    cond.op = consume(TokenType::OPERATOR).value;
    const auto& token = peek();
    cond.value = {token.type, consume(token.type).value};
    return cond;
}

std::unique_ptr<Command> Parser::parse_delete() {
    auto cmd = std::make_unique<DeleteCommand>();
    consume(TokenType::KEYWORD_DELETE);
    consume(TokenType::KEYWORD_FROM);
    cmd->table_name = consume(TokenType::IDENTIFIER).value;
    cmd->where_clause = parse_optional_where();
    return cmd;
}

std::unique_ptr<Command> Parser::parse_update() {
    auto cmd = std::make_unique<UpdateCommand>();
    consume(TokenType::KEYWORD_UPDATE);
    cmd->table_name = consume(TokenType::IDENTIFIER).value;
    consume(TokenType::KEYWORD_SET);
    do {
        SetClause set;
        set.column = consume(TokenType::IDENTIFIER).value;
        consume(TokenType::OPERATOR); // consume =
        const auto& token = peek();
        set.value = {token.type, consume(token.type).value};
        cmd->set_clauses.push_back(set);
    } while (peek().type == TokenType::COMMA && consume(TokenType::COMMA).type == TokenType::COMMA);
    cmd->where_clause = parse_optional_where();
    return cmd;
}

std::unique_ptr<Command> Parser::parse_select() {
    auto cmd = std::make_unique<SelectCommand>();
    consume(TokenType::KEYWORD_SELECT);
    if (peek().type == TokenType::ASTERISK) {
        consume(TokenType::ASTERISK);
        cmd->select_all = true;
    } else {
        do {
            cmd->columns.push_back(consume(TokenType::IDENTIFIER).value);
        } while (peek().type == TokenType::COMMA && consume(TokenType::COMMA).type == TokenType::COMMA);
    }
    consume(TokenType::KEYWORD_FROM);
    cmd->table_name = consume(TokenType::IDENTIFIER).value;
    cmd->where_clause = parse_optional_where();
    return cmd;
}