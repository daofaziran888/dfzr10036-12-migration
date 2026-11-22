"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import checkpoint_api, resource

# checkpoint_api 需要在通用资源路由之前注册：它带有更长的固定前缀
# /api/checkpoint-state，先注册可避免被 /api/{module} 抢先匹配。
ROUTERS = [checkpoint_api, resource]
