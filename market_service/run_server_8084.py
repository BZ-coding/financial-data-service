#!/usr/bin/env python3
import sys
sys.path.insert(0, '/home/zsd/financial-data-service')  # 迁移: 原路径 /vol1/@apphome/...
import uvicorn
if __name__ == '__main__':
    uvicorn.run("market_service.api:app", host='0.0.0.0', port=8084, reload=False)
