# 📋 Rent Burden 可视化功能设置指南

## ✅ 已完成

1. ✅ 添加了 `plotly` 依赖
2. ✅ 创建了 `frontend/pages/rent_burden.py` 页面
3. ✅ 添加了后端 API 端点 `/rent-burden`
4. ✅ 添加了数据库连接支持

## 📝 接下来需要做的事

### 步骤 1: 下载 NYC Census Tracts GeoJSON

你有两个选择：

**选项 A：使用我的脚本自动下载（推荐）**

```bash
# 在项目根目录运行
python scripts/download_nyc_geojson.py
```

**选项 B：手动下载**

1. 访问 https://data.cityofnewyork.us/City-Government/2010-Census-Tracts/37yn-as6i
2. 点击 "Export" → "GeoJSON"
3. 保存到 `frontend/data/nyc_tracts.geojson`

### 步骤 2: 准备数据库环境变量

**在 Streamlit Cloud 设置 Secrets：**

在 https://share.streamlit.io/ → 你的应用 → ⋮ → Settings → Secrets 添加：

```toml
[secrets]
DB_HOST = "your-postgresql-host.com"
DB_PORT = "5432"
DB_NAME = "noah_dashboard"
DB_USER = "your-username"
DB_PASSWORD = "your-password"
```

### 步骤 3: 推送代码并部署

```bash
git push origin master
```

然后：
- **Streamlit Cloud** 会自动部署前端
- **Render** 会自动部署后端

### 步骤 4: 访问新页面

部署完成后访问：
- Dashboard: `https://your-app.streamlit.app/`
- Rent Burden 页面: `https://your-app.streamlit.app/Rent_Burden`

## 🗄️ 数据库表结构

确保你的 `rent_burden` 表包含以下字段：

```sql
CREATE TABLE rent_burden (
    geo_id TEXT PRIMARY KEY,        -- Census tract GEOID (例如: "36061000100")
    tract_name TEXT,                 -- Tract name
    rent_burden_rate DECIMAL,        -- 租金负担率 (0-1)
    severe_burden_rate DECIMAL,     -- 严重负担率 (0-1)
    geometry GEOMETRY(POLYGON, 4326) -- PostGIS geometry
);
```

## 🎨 Mapbox Token（可选）

如果想使用 Mapbox 地图样式，在 Streamlit Secrets 添加：

```toml
MAPBOX_TOKEN = "your-mapbox-token"
```

然后访问: https://account.mapbox.com/

## ⚡ 快速测试

本地测试（如果有本地数据库）：

```bash
# 设置环境变量
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=noah_dashboard
export DB_USER=postgres
export DB_PASSWORD=your-password

# 运行前端
streamlit run frontend/app.py
```

然后访问: http://localhost:8501/Rent_Burden

## 🐛 故障排除

### 问题 1: "No rent burden data available"

**解决：** 检查数据库连接和表是否存在
```sql
-- 检查表是否存在
SELECT * FROM rent_burden LIMIT 5;
```

### 问题 2: "GeoJSON file not found"

**解决：** 下载 GeoJSON 文件到 `frontend/data/nyc_tracts.geojson`

### 问题 3: Map not showing

**解决：** 
- 检查 `geo_id` 格式是否与 GeoJSON 中的 `properties.GEOID` 匹配
- 确认 `rent_burden_rate` 是数值类型（0-1）

## 📊 功能特点

✅ Choropleth 地图展示  
✅ 颜色越深 = 负担越重  
✅ 颜色越浅 = 越可负担  
✅ Hover 显示详细信息  
✅ 统计数据摘要  
✅ 响应式设计  
✅ 可下载数据

