# 快速部署指南 (Quick Deployment Guide)

## 步骤1：创建GitHub仓库

1. 访问 [GitHub.com](https://github.com) 并登录
2. 点击右上角的 "+" 号，选择 "New repository"
3. 仓库名称：`nyc-housing-hub`
4. 选择 "Public" (公开)
5. **不要**勾选 "Add a README file"
6. 点击 "Create repository"

## 步骤2：推送代码到GitHub

在终端中运行以下命令：

```bash
# 添加远程仓库 (替换 YOUR_USERNAME 为你的GitHub用户名)
git remote add origin https://github.com/YOUR_USERNAME/nyc-housing-hub.git

# 推送到GitHub
git push -u origin master
```

## 步骤3：部署后端 (免费选项)

### 选项A：Railway (推荐，免费)

1. 访问 [railway.app](https://railway.app)
2. 用GitHub账号登录
3. 点击 "New Project" → "Deploy from GitHub repo"
4. 选择你的 `nyc-housing-hub` 仓库
5. 在设置中添加环境变量：
   ```
   DATA_PROVIDER=socrata
   SOCRATA_APP_TOKEN=你的token (可选)
   ```
6. Railway会自动部署，给你一个URL，比如：`https://your-app.railway.app`

### 选项B：Heroku

1. 访问 [heroku.com](https://heroku.com)
2. 创建新应用
3. 连接GitHub仓库
4. 在设置中添加环境变量
5. 启用自动部署

## 步骤4：部署前端 (Streamlit Community Cloud)

1. 访问 [share.streamlit.io](https://share.streamlit.io)
2. 用GitHub账号登录
3. 点击 "New app"
4. 选择仓库：`YOUR_USERNAME/nyc-housing-hub`
5. 分支：`master`
6. 主文件路径：`frontend/app.py`
7. 点击 "Deploy!"

## 步骤5：连接前后端

### 方法1：通过环境变量 (推荐)

在Streamlit Cloud的设置中添加：
```
BACKEND_URL=https://your-backend-url.railway.app
```

### 方法2：修改代码

在 `frontend/app.py` 中直接修改：
```python
BACKEND_URL = "https://your-backend-url.railway.app"
```

## 步骤6：验证部署

1. 访问你的Streamlit应用URL
2. 检查是否能正常加载数据
3. 测试过滤功能
4. 查看地图是否正常显示

## 常见问题解决

### 问题1：前端无法连接后端
- 检查 `BACKEND_URL` 是否正确
- 确保后端URL可以公开访问
- 检查CORS设置

### 问题2：数据不显示
- 检查是否有 `SOCRATA_APP_TOKEN`
- 查看后端日志
- 测试API端点

### 问题3：地图不显示
- 检查是否有地理坐标数据
- 确保pydeck正确安装

## 免费部署总结

✅ **完全免费**：
- GitHub：免费代码托管
- Railway：免费后端托管
- Streamlit Cloud：免费前端托管

✅ **无需购买域名**：
- Railway提供免费子域名
- Streamlit Cloud提供免费子域名

✅ **自动部署**：
- 代码推送到GitHub自动触发部署
- 前后端分离，独立部署

## 下一步

1. 按照上述步骤部署
2. 测试所有功能
3. 分享链接给其他人查看
4. 根据需要添加更多功能

---

**需要帮助？** 查看完整的 `README.md` 或 `DEPLOYMENT.md` 文件。
