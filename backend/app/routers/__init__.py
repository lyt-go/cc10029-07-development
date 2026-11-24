"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import backfill, resource

# backfill 含 /api/backfill/* 固定前缀，需先于通用 /api/{module} 动态路由注册
ROUTERS = [backfill, resource]
