# 🔗 从 DBeaver 到 Streamlit Cloud 配置指南

## 步骤 1: 在 DBeaver 中查看连接信息

### 获取数据库连接参数：

1. **打开 DBeaver**
2. **右键点击你的 PostgreSQL 连接**
3. **选择 "Edit Connection"** (编辑连接)
4. **记录以下信息**：

```
Host: [这里显示的主机地址]
Port: [通常是 5432]
Database: [数据库名称]
User: [用户名]
Password: [你的密码]
```

**常见例子：**
- Supabase: `db.xxxxx.supabase.co` 端口 5432
- Railway: 查看 Variables 标签
- Neon: `xxx.neon.tech` 端口 5432
- 本地/其他: 看你的具体配置

## 步骤 2: 确认数据库结构

确保你的数据库中有 `rent_burden` 表，运行以下 SQL：

```sql
-- 检查表是否存在
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public';

-- 如果 rent_burden 表不存在，创建它：
CREATE TABLE IF NOT EXISTS rent_burden (
    geo_id TEXT PRIMARY KEY,
    tract_name TEXT,
    rent_burden_rate DECIMAL,
    severe_burden_rate DECIMAL
);

-- 查看表结构
\d rent_burden;
```

## 步骤 3: 上传数据（如果需要）

### 方法 A: 使用 DBeaver 上传 CSV

1. 准备好你的 CSV 文件，格式：
```csv
geo_id,tract_name,rent_burden_rate,severe_burden_rate
36061000100,Census Tract 1,0.45,0.23
36061000200,Census Tract 2,0.52,0.28
```

2. 在 DBeaver 中：
   - 右键点击 `rent_burden` 表
   - 选择 "Import Data"
   - 选择你的 CSV 文件
   - 配置列映射（确保 geo_id 映射到 geo_id）
   - 点击 "Start"

### 方法 B: 使用 SQL 插入

```sql
INSERT INTO rent_burden (geo_id, tract_name, rent_burden_rate, severe_burden_rate)
VALUES 
    ('36061000100', 'Census Tract 1', 0.45, 0.23),
    ('36061000200', 'Census Tract 2', 0.52, 0.28);
```

## 步骤 4: 在 Streamlit Cloud 配置 Secrets

1. **访问**: https://share.streamlit.io/
2. **登录**，找到你的应用
3. **点击 ⋮ → Settings → Secrets**

4. **添加配置**（用你的实际信息替换）：

```toml
[secrets]
DB_HOST = "你的主机地址"
DB_PORT = "5432"
DB_NAME = "你的数据库名"
DB_USER = "你的用户名"
DB_PASSWORD = "你的密码"
```

**示例 (Supabase):**
```toml
[secrets]
DB_HOST = "db.abc123xyz.supabase.co"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "MySecurePassword"
```

**示例 (本地/其他):**
```toml
[secrets]
DB_HOST = "your-server.com"
DB_PORT = "5432"
DB_NAME = "nyc_housing"
DB_USER = "admin"
DB_PASSWORD = "YourPassword123"
```

5. **点击 "Save"**

## 步骤 5: 验证数据

1. **在 DBeaver 中运行**:
```sql
SELECT COUNT(*) FROM rent_burden;
SELECT * FROM rent_burden LIMIT 5;
```

2. **确保有数据**，然后等待 Streamlit 重新部署

## 步骤 6: 测试应用

1. 访问: https://your-app.streamlit.app/Rent_Burden
2. 应该能看到数据表格
3. 如果看到数据，说明连接成功！

## ⚠️ 如果数据库在本地或公司内部网

如果你的数据库不能从互联网访问（例如在本地电脑上），需要：

1. **使用云数据库**（Supabase/Railway/Neon）
2. **或使用 SSH 隧道**（复杂）
3. **或使用 SQLite 方案**（见 scripts/import_csv_to_sqlite.py）

## 🐛 常见问题

### Q: "Connection refused"
- 确保数据库允许外部连接
- 检查防火墙设置
- 某些云数据库需要配置 IP 白名单

### Q: "No rent burden data available"
- 确认表中有数据
- 使用 DBeaver 运行 `SELECT * FROM rent_burden LIMIT 5;` 验证

### Q: "ModuleNotFoundError"
- 依赖包应该会自动安装
- 检查 requirements.txt 是否包含 psycopg2-binary
