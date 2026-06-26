"""量化研究助手 Agent — 停止处理

当客户端中断或会话超时时调用，用于清理资源。
"""


async def handler(context):
    """
    停止处理：清理当前会话的中间状态。

    context 可用字段：
    - context.request : 包含 conversation_id 和 run_id
    - context.store   : 会话存储（可清理临时数据）
    """
    try:
        # 清理临时数据（保留消息历史）
        await context.store.delete("_temp_data")
        await context.store.delete("_price_cache")
    except Exception:
        pass

    return {"status": "stopped", "message": "会话已结束"}
