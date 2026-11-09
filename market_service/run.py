#!/usr/bin/env python3
"""
正确的启动脚本：修复路径并启动服务
"""

import sys
from pathlib import Path

# 设置项目根目录
PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

# 修补配置文件中的数据库路径（临时方案）
import yaml
config_path = PROJECT_ROOT / "config" / "config.yaml"
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# 修改为绝对路径
config['database']['path'] = str(PROJECT_ROOT / config['database']['path'])

# 将修补后的config注入到market_service.api模块
import importlib.util
spec = importlib.util.spec_from_file_location("market_service.api", PROJECT_ROOT / "market_service" / "api.py")
api_module = importlib.util.module_from_spec(spec)

# 在模块加载前设置环境变量？直接修改sys.modules
sys.modules['market_service.api'] = api_module
sys.modules['market_service'] = type(sys)('market_service')
sys.modules['market_service'].__path__ = [str(PROJECT_ROOT / "market_service")]

# 现在启动uvicorn
import uvicorn
uvicorn.run(
    "market_service.api:app",
    host=config['api']['host'],
    port=config['api']['port'],
    reload=config['api']['reload'],
    log_level="info"
)
