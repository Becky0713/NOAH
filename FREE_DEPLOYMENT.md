# 永久免费部署指南 (Permanent Free Deployment Guide)

## 🎯 目标：永久免费运行你的应用

### 后端部署选项（按推荐顺序）

#### 1. **Render** ⭐⭐⭐⭐⭐
- **免费额度**：750小时/月（足够24/7运行）
- **无时间限制**
- **自动部署**GitHub
- **步骤**：
  1. 访问 [render.com](https://render.com)
  2. 连接GitHub账号
  3. 选择 `Becky0713/NOAH` 仓库
  4. 选择 "Web Service"
  5. 使用 `render.yaml` 配置
  6. 部署！

#### 2. **Fly.io** ⭐⭐⭐⭐
- **免费额度**：3个小应用
- **全球CDN**
- **步骤**：
  ```bash
  # 安装Fly CLI
  curl -L https://fly.io/install.sh | sh
  
  # 登录
  fly auth login
  
  # 部署
  fly deploy
  ```

#### 3. **Heroku** ⭐⭐⭐
- **免费额度**：550-1000小时/月
- **步骤**：
  ```bash
  # 安装Heroku CLI
  # 创建应用
  heroku create your-app-name
  
  # 部署
  git push heroku main
  ```

#### 4. **Railway** ⭐⭐
- **免费额度**：30天
- **适合**：短期测试

### 前端部署（Streamlit Cloud）⭐⭐⭐⭐⭐
- **永久免费**
- **无限制**
- **步骤**：
  1. 访问 [share.streamlit.io](https://share.streamlit.io)
  2. 连接GitHub
  3. 选择仓库和分支
  4. 设置环境变量：`BACKEND_URL`

## 🚀 一键部署脚本

我已经为你创建了 `deploy.sh` 脚本，支持多种部署方式：

```bash
# 运行部署脚本
./deploy.sh

# 选择部署方式：
# 1) Docker (本地)
# 2) Railway (30天免费)
# 3) Heroku (长期免费)
# 4) Streamlit Cloud (永久免费)
# 5) 全部部署
```

## 💡 推荐配置

### 生产环境配置
```env
# 后端环境变量
DATA_PROVIDER=socrata
SOCRATA_APP_TOKEN=你的token
HOST=0.0.0.0
PORT=8080
DEBUG=false

# 前端环境变量
BACKEND_URL=https://your-backend.onrender.com
```

### 开发环境配置
```env
# 本地开发
DATA_PROVIDER=socrata
BACKEND_URL=http://localhost:8000
DEBUG=true
```

## 🔄 迁移策略

### 从Railway迁移到Render
1. **备份数据**（如果有）
2. **部署到Render**
3. **更新前端环境变量**
4. **测试功能**
5. **删除Railway应用**

### 多平台部署（冗余）
- **主平台**：Render
- **备用平台**：Fly.io
- **前端**：Streamlit Cloud

## 📊 成本对比

| 平台 | 免费额度 | 限制 | 推荐度 |
|------|----------|------|--------|
| Render | 750小时/月 | 无 | ⭐⭐⭐⭐⭐ |
| Fly.io | 3个应用 | 无 | ⭐⭐⭐⭐ |
| Heroku | 550-1000小时/月 | 无 | ⭐⭐⭐ |
| Railway | 30天 | 时间限制 | ⭐⭐ |
| Streamlit Cloud | 无限制 | 无 | ⭐⭐⭐⭐⭐ |

## 🛠️ 故障排除

### 常见问题
1. **应用休眠** - 使用健康检查
2. **内存不足** - 优化代码
3. **超时** - 增加超时设置

### 监控和维护
- 设置健康检查
- 监控日志
- 定期备份

## 📈 扩展方案

### 当免费额度不够时
1. **优化代码** - 减少资源使用
2. **使用CDN** - 加速静态资源
3. **数据库优化** - 减少查询
4. **考虑付费** - 选择最便宜的方案

## 🎉 总结

**最佳免费组合**：
- **后端**：Render（750小时/月）
- **前端**：Streamlit Cloud（永久免费）
- **总成本**：$0/月

**备用方案**：
- **后端**：Fly.io（3个应用）
- **前端**：Streamlit Cloud（永久免费）

---

**需要帮助？** 运行 `./deploy.sh` 开始部署！
