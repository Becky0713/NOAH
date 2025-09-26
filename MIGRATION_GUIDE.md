# 环境迁移指南 (Environment Migration Guide)

## 概述
这个项目设计为高度可移植，支持多种部署环境和部门迁移。

## 支持的环境

### 1. 开发环境 (Development)
```bash
# 本地开发
cp env.example .env
# 编辑 .env 文件
python -m uvicorn backend.main:app --reload
streamlit run frontend/app.py
```

### 2. 生产环境 (Production)
- **Railway** - 后端部署
- **Streamlit Cloud** - 前端部署
- **Heroku** - 全栈部署
- **Docker** - 容器化部署

### 3. 企业环境 (Enterprise)
- **内部服务器** - 使用Docker Compose
- **Kubernetes** - 使用提供的K8s配置
- **私有云** - 支持各种云平台

## 迁移步骤

### 步骤1：环境配置
```bash
# 1. 复制环境配置模板
cp env.example .env

# 2. 编辑环境变量
nano .env
```

### 步骤2：选择部署方式

#### 选项A：Docker部署（推荐）
```bash
# 构建镜像
docker-compose build

# 运行服务
docker-compose up -d

# 检查状态
docker-compose ps
```

#### 选项B：云平台部署
```bash
# Railway
railway login
railway link
railway up

# Heroku
heroku create your-app-name
git push heroku main
```

### 步骤3：环境变量配置

#### 开发环境
```env
DATA_PROVIDER=socrata
SOCRATA_APP_TOKEN=your_token
BACKEND_URL=http://localhost:8000
DEBUG=true
```

#### 生产环境
```env
DATA_PROVIDER=socrata
SOCRATA_APP_TOKEN=your_token
BACKEND_URL=https://your-backend.railway.app
DEBUG=false
ALLOWED_ORIGINS=https://your-frontend.streamlit.app
```

#### 企业环境
```env
DATA_PROVIDER=socrata
SOCRATA_APP_TOKEN=your_token
BACKEND_URL=https://internal-api.company.com
DEBUG=false
ALLOWED_ORIGINS=https://internal-dashboard.company.com
DATABASE_URL=postgresql://user:pass@db.company.com:5432/housing
```

## 部门迁移流程

### 1. 代码迁移
```bash
# 1. 克隆仓库
git clone https://github.com/Becky0713/NOAH.git
cd NOAH

# 2. 创建新分支
git checkout -b department-migration

# 3. 更新配置
cp env.example .env
# 编辑 .env 文件
```

### 2. 环境适配
```bash
# 1. 更新环境变量
# 2. 测试本地运行
# 3. 部署到新环境
# 4. 验证功能
```

### 3. 数据迁移
```bash
# 如果需要数据库迁移
python backend/ingest_socrata.py
```

## 配置管理

### 环境变量优先级
1. 系统环境变量
2. `.env` 文件
3. 默认值

### 敏感信息管理
- 使用环境变量存储API密钥
- 不要将敏感信息提交到Git
- 使用 `.env.example` 作为模板

## 故障排除

### 常见问题
1. **CORS错误** - 检查 `ALLOWED_ORIGINS` 配置
2. **API连接失败** - 检查 `BACKEND_URL` 配置
3. **数据加载失败** - 检查 `SOCRATA_APP_TOKEN` 配置

### 调试模式
```bash
# 启用调试模式
export DEBUG=true
python -m uvicorn backend.main:app --reload --log-level debug
```

## 最佳实践

### 1. 版本控制
- 使用Git标签标记版本
- 保持 `main` 分支稳定
- 使用功能分支开发

### 2. 配置管理
- 环境特定配置使用环境变量
- 提供配置模板文件
- 文档化所有配置选项

### 3. 部署策略
- 使用CI/CD自动化部署
- 测试环境验证
- 生产环境监控

## 支持

如果遇到迁移问题，请：
1. 检查环境变量配置
2. 查看日志文件
3. 参考故障排除指南
4. 联系技术支持

---

**注意**: 这个项目设计为高度可移植，支持从开发环境到生产环境的无缝迁移。
