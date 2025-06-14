// AccessControl.cpp
// 它将依赖于 DatabaseAPI.hpp 中定义的结构和 Database::Impl 提供的内部方法。
#include "DatabaseAPI.hpp" // 包含数据库API头文件

#include <iostream>
#include <stdexcept> // For std::runtime_error
#include <algorithm> // For std::remove_if

// --- AccessControl::Impl 的具体实现 ---
class AccessControl::Impl {
public:
    Database::Impl* dbCoreImpl_; // 指向数据库核心实现的指针

    explicit Impl(Database::Impl* db_core_impl) : dbCoreImpl_(db_core_impl) {
        if (!db_core_impl) {
            throw std::runtime_error("AccessControl::Impl: 数据库核心实现指针为空。");
        }
        std::cout << "AccessControl::Impl: 已初始化。" << std::endl;
    }
    ~Impl() {
        std::cout << "AccessControl::Impl: 已销毁。" << std::endl;
    }

    /**
     * @brief 用户登录。
     * @param username 用户名。
     * @param password 密码。
     * @return 如果认证成功则返回 true，否则返回 false。
     */
    bool login(const std::string& username, const std::string& password) {
        std::cout << "AccessControl::Impl: 尝试登录用户: " << username << std::endl;
        if (dbCoreImpl_->authenticateInternal(username, password)) {
            dbCoreImpl_->setCurrentUser(username);
            std::cout << "AccessControl::Impl: 用户 '" << username << "' 登录成功。" << std::endl;
            return true;
        }
        std::cout << "AccessControl::Impl: 用户 '" << username << "' 登录失败: 用户名或密码错误。" << std::endl;
        return false;
    }

    /**
     * @brief 用户登出。
     * @return 始终返回 true。
     */
    bool logout() {
        if (!dbCoreImpl_->getCurrentUser().empty()) {
            std::cout << "AccessControl::Impl: 用户 '" << dbCoreImpl_->getCurrentUser() << "' 登出。" << std::endl;
            dbCoreImpl_->setCurrentUser(""); // 清空当前用户
        } else {
            std::cout << "AccessControl::Impl: 没有用户登录。" << std::endl;
        }
        return true;
    }

    /**
     * @brief 创建一个新用户。
     * @param username 新用户的用户名。
     * @param password 新用户的密码。
     * @return 如果成功创建用户则返回 true，否则返回 false。
     */
    bool createUser(const std::string& username, const std::string& password) {
        std::cout << "AccessControl::Impl: 尝试创建用户: " << username << std::endl;
        // 只有管理员或具有 CREATE_USER 权限的用户才能创建用户
        if (!dbCoreImpl_->checkPermissionInternal(dbCoreImpl_->getCurrentUser(), PermissionType::CREATE_USER, "SYSTEM")) { 
             std::cerr << "Permission denied: 用户 '" << dbCoreImpl_->getCurrentUser() << "' 没有创建用户的权限。" << std::endl;
             return false;
        }
        return dbCoreImpl_->createUserInternal(username, password);
    }

    /**
     * @brief 删除一个用户。
     * @param username 要删除的用户的用户名。
     * @return 如果成功删除用户则返回 true，否则返回 false。
     */
    bool dropUser(const std::string& username) {
        std::cout << "AccessControl::Impl: 尝试删除用户: " << username << std::endl;
        if (!dbCoreImpl_->checkPermissionInternal(dbCoreImpl_->getCurrentUser(), PermissionType::DROP_USER, "SYSTEM")) { 
             std::cerr << "Permission denied: 用户 '" << dbCoreImpl_->getCurrentUser() << "' 没有删除用户的权限。" << std::endl;
             return false;
        }
        return dbCoreImpl_->dropUserInternal(username);
    }

    /**
     * @brief 授予用户特定权限。
     * @param username 被授予权限的用户名。
     * @param permission 要授予的权限类型。
     * @param objectType 权限作用的对象类型（例如："TABLE", "DATABASE", "SYSTEM"）。
     * @param objectName 权限作用的具体对象名称（例如表名），可选。
     * @return 如果成功授予权限则返回 true，否则返回 false。
     */
    bool grantPermission(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "") {
        std::cout << "AccessControl::Impl: 尝试授予用户 '" << username << "' 权限: " << static_cast<int>(permission) << " on " << objectType << ":" << objectName << std::endl;
        // 只有管理员或具有 GRANT_PERMISSION 的用户才能授予权限
        if (!dbCoreImpl_->checkPermissionInternal(dbCoreImpl_->getCurrentUser(), PermissionType::GRANT_PERMISSION, "SYSTEM")) { 
             std::cerr << "Permission denied: 用户 '" << dbCoreImpl_->getCurrentUser() << "' 没有授予权限的权限。" << std::endl;
             return false;
        }
        return dbCoreImpl_->grantPermissionInternal(username, permission, objectType, objectName);
    }

    /**
     * @brief 撤销用户的特定权限。
     * @param username 被撤销权限的用户名。
     * @param permission 要撤销的权限类型。
     * @param objectType 权限作用的对象类型。
     * @param objectName 权限作用的具体对象名称，可选。
     * @return 如果成功撤销权限则返回 true，否则返回 false。
     */
    bool revokePermission(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "") {
        std::cout << "AccessControl::Impl: 尝试撤销用户 '" << username << "' 权限: " << static_cast<int>(permission) << " on " << objectType << ":" << objectName << std::endl;
        // 只有管理员或具有 REVOKE_PERMISSION 的用户才能撤销权限
        if (!dbCoreImpl_->checkPermissionInternal(dbCoreImpl_->getCurrentUser(), PermissionType::REVOKE_PERMISSION, "SYSTEM")) { 
             std::cerr << "Permission denied: 用户 '" << dbCoreImpl_->getCurrentUser() << "' 没有撤销权限的权限。" << std::endl;
             return false;
        }
        return dbCoreImpl_->revokePermissionInternal(username, permission, objectType, objectName);
    }
};

// --- AccessControl 类的构造函数和析构函数实现 ---
AccessControl::AccessControl(Database::Impl* db_core_impl) : pImpl(std::make_unique<Impl>(db_core_impl)) {}
AccessControl::~AccessControl() = default;

// --- AccessControl 公开接口的实现（转发给 Pimpl Impl） ---
bool AccessControl::login(const std::string& username, const std::string& password) {
    return pImpl->login(username, password);
}

bool AccessControl::logout() {
    return pImpl->logout();
}

bool AccessControl::createUser(const std::string& username, const std::string& password) {
    return pImpl->createUser(username, password);
}

bool AccessControl::dropUser(const std::string& username) {
    return pImpl->dropUser(username);
}

bool AccessControl::grantPermission(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName) {
    return pImpl->grantPermission(username, permission, objectType, objectName);
}

bool AccessControl::revokePermission(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName) {
    return pImpl->revokePermission(username, permission, objectType, objectName);
}