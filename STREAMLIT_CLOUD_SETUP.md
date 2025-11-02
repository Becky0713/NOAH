# 🚀 Streamlit Cloud 配置指南

## 步骤 1: 访问 Streamlit Cloud 设置页面

1. 打开浏览器，访问: **https://share.streamlit.io/**
2. 点击右上角 **"Sign in"** 或 **"Log in"**
3. 使用你的 GitHub 账号登录

## 步骤 2: 找到你的应用

1. 登录后，在 **"Your apps"** 页面找到 **"noah"** 或 **"nyc-housing-frontendapp"**
2. 点击应用名称进入应用详情页

## 步骤 3: 进入 Secrets 设置

1. 在应用详情页，点击右上角的 **⋮ (三个点菜单)**
2. 选择 **"Settings"** (设置)
3. 在左侧菜单找到 **"Secrets"** (秘密配置)
4. 点击进入

## 步骤 4: 添加数据库配置

在 "Secrets" 页面的编辑框中，粘贴以下内容：

```toml
[secrets]
DB_HOST = "你的PostgreSQL主机地址"
DB_PORT = "5432"
DB_NAME = "noah_dashboard"
DB_USER = "你的数据库用户名"
DB_PASSWORD = "你的数据库密码"
```

### 📝 替换说明：

**示例配置：**

```toml
[secrets]
DB_HOST = "postgres.railway.app"
DB_PORT = "5432"
DB_NAME = "railway"
DB_USER = "postgres"
DB_PASSWORD = "Abc123xyz"
```

或者：

```toml
[secrets]
DB_HOST = "db.example.com"
DB_PORT = "5432"
DB_NAME = "nyc_housing"
DB_USER = "admin"
DB_PASSWORD = "MySecurePassword123!"
```

## 步骤 5: 保存并触发重新部署

1. 点击 **"Save"** (保存)
2. Streamlit Cloud 会自动检测变化并重新部署
3. 等待 1-2 分钟，直到显示 **"✓ Live"**

## 步骤 6: 测试应用

1. 点击应用链接（例如: `https://your-app.streamlit.app/`）
2. 点击左侧导航栏的 **"Rent_Burden"**
3. 应该能看到数据表格

## 常见问题排查

### ❌ 问题 1: "No rent burden data available"

**可能原因:**
- Secrets 配置错误
- 数据库连接失败
- `rent_burden` 表不存在

**解决方法:**
1. 检查 Secrets 是否保存成功
2. 使用页面上的 **"Test Connection"** 按钮测试连接
3. 确认数据库表结构正确

### ❌ 问题 2: "ModuleNotFoundError: No module named 'psycopg2'"

**解决方法:**
- 这个包已经在 `requirements.txt` 中，应该会自动安装
- 如果仍有问题，检查 Render 后端部署是否成功

### ❌ 问题 3: 应用一直显示 "Deploying..."

**解决方法:**
1. 检查 GitHub 仓库是否有新的 commit
2. 查看 Streamlit Cloud 的部署日志
3. 尝试手动触发重新部署

## 📊 数据库表结构要求

确保你的数据库包含 `rent_burden` 表：

```sql
CREATE TABLE rent_burden (
    geo_id TEXT PRIMARY KEY,
    tract_name TEXT,
    rent_burden_rate DECIMAL,
    severe_burden_rate DECIMAL
);
```

### 示例数据插入：

```sql
INSERT INTO rent_burden (geo_id, tract_name, rent_burden_rate, severe_burden_rate)
VALUES 
    ('36061000100', 'Census Tract 1, Brooklyn', 0.45, 0.23),
    ('36061000200', 'Census Tract 2, Brooklyn', 0.52, 0.28),
    ('36061000300', 'Census Tract 3, Manhattan', 0.38, 0.18);
```

## 🔐 如何获取数据库信息

### 如果你的数据库在 Railway:
1. 访问: https://railway.app/
2. 选择你的 PostgreSQL 项目
3. 在 **"Variables"** 标签页查看连接信息

### 如果你的数据库在 Render:
1. 访问: https://dashboard.render.com/
2. 选择你的 PostgreSQL 服务
3. 在 **"Info"** 标签页查看连接信息

### 如果你的数据库在 Heroku:
1. 访问: https://dashboard.heroku.com/
2. 选择你的应用
3. 在 **"Settings"** → **"Reveal Config Vars"** 查看

## 📞 需要帮助？

如果以上步骤都无法解决问题，请告诉我：
1. 你看到的错误信息是什么？
2. 你的数据库是在哪个平台？
3. 能否访问 Streamlit Cloud 的 Settings 页面？
