"""
量化研究助手 Agent — EdgeOne Makers 入口

轻量演示模式：云端只做行情查询 + 技术指标 + 报告解读。
策略回测引导用户回到本地 CLI（agent4quant-skills）。

框架：openai-agents-sdk
本地调试：edgeone makers dev
"""

import json
import uuid
from openai import AsyncOpenAI

# ── Agent 工具定义 ──────────────────────────────────────────

QUANT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "fetch_stock_data",
            "description": "获取指定股票的历史行情数据。支持 A 股（akShare）和港美股（yfinance）。",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "股票代码。A 股格式如 000001.SZ、600519.SH；港股如 0700.HK；美股如 AAPL"
                    },
                    "market": {
                        "type": "string",
                        "enum": ["a_share", "hk", "us"],
                        "description": "市场类型"
                    },
                    "period": {
                        "type": "string",
                        "enum": ["1mo", "3mo", "6mo", "1y"],
                        "description": "数据时间范围",
                        "default": "3mo"
                    }
                },
                "required": ["symbol", "market"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "compute_indicators",
            "description": "计算常用技术指标：MA（均线）、RSI（相对强弱）、MACD、Bollinger Bands（布林带）、KDJ、ATR（真实波幅）。",
            "parameters": {
                "type": "object",
                "properties": {
                    "indicators": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["ma", "rsi", "macd", "bollinger", "kdj", "atr"]
                        },
                        "description": "需要计算的指标列表"
                    },
                    "price_data": {
                        "type": "object",
                        "description": "从 fetch_stock_data 获取的价格数据（自动传递）"
                    }
                },
                "required": ["indicators"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "generate_analysis_report",
            "description": "根据行情数据和技术指标生成分析报告摘要。包含趋势判断、关键位、风险提示。",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "股票代码"
                    },
                    "indicators_result": {
                        "type": "object",
                        "description": "compute_indicators 的输出结果（自动传递）"
                    }
                },
                "required": ["symbol"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_cli_backtest_command",
            "description": "当用户想做策略回测时，生成可在本地终端执行的 agent4quant CLI 命令。云端不执行回测——引导用户使用本地工具。",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "股票代码"
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["sma_cross", "rsi", "momentum"],
                        "description": "回测策略"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "起始日期 YYYY-MM-DD"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "结束日期 YYYY-MM-DD"
                    }
                },
                "required": ["symbol", "strategy"]
            }
        }
    }
]

# ── 工具实现 ────────────────────────────────────────────────

FETCH_STOCK_DATA_SCRIPT = r'''
import json, sys
try:
    market = {market!r}
    symbol = {symbol!r}
    period = {period!r}

    if market == "a_share":
        try:
            import akshare as ak
            # 转换代码格式
            code = symbol.replace(".SZ", "").replace(".SH", "")
            if ".SZ" in symbol:
                sym = "sz" + code
            elif ".SH" in symbol:
                sym = "sh" + code
            else:
                sym = "sz" + code
            df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
            if df is not None and not df.empty:
                # 根据 period 截取
                periods = {{"1mo": 22, "3mo": 66, "6mo": 132, "1y": 252}}
                n = periods.get(period, 66)
                df = df.tail(n)
                result = {{
                    "symbol": symbol,
                    "market": market,
                    "source": "akshare",
                    "count": len(df),
                    "latest": {{
                        "date": str(df.iloc[-1]["日期"]) if "日期" in df.columns else str(df.index[-1]),
                        "open": float(df.iloc[-1]["开盘"]) if "开盘" in df.columns else 0,
                        "high": float(df.iloc[-1]["最高"]) if "最高" in df.columns else 0,
                        "low": float(df.iloc[-1]["最低"]) if "最低" in df.columns else 0,
                        "close": float(df.iloc[-1]["收盘"]) if "收盘" in df.columns else 0,
                        "volume": float(df.iloc[-1]["成交量"]) if "成交量" in df.columns else 0,
                    }},
                    "close_prices": [float(x) for x in df["收盘"].tolist()] if "收盘" in df.columns else [],
                    "high_prices": [float(x) for x in df["最高"].tolist()] if "最高" in df.columns else [],
                    "low_prices": [float(x) for x in df["最低"].tolist()] if "最低" in df.columns else [],
                    "volumes": [float(x) for x in df["成交量"].tolist()] if "成交量" in df.columns else [],
                }}
            else:
                result = {{"error": "akshare 返回空数据"}}
        except ImportError:
            result = {{"error": "akshare 未安装，请在沙箱中 pip install akshare"}}
        except Exception as e:
            result = {{"error": str(e)}}

    elif market in ("hk", "us"):
        try:
            import yfinance as yf
            ticker = yf.Ticker(symbol)
            periods_map = {{"1mo": "1mo", "3mo": "3mo", "6mo": "6mo", "1y": "1y"}}
            df = ticker.history(period=periods_map.get(period, "3mo"))
            if df is not None and not df.empty:
                result = {{
                    "symbol": symbol,
                    "market": market,
                    "source": "yfinance",
                    "count": len(df),
                    "latest": {{
                        "date": str(df.index[-1].date()),
                        "open": float(df.iloc[-1]["Open"]),
                        "high": float(df.iloc[-1]["High"]),
                        "low": float(df.iloc[-1]["Low"]),
                        "close": float(df.iloc[-1]["Close"]),
                        "volume": float(df.iloc[-1]["Volume"]),
                    }},
                    "close_prices": [float(x) for x in df["Close"].tolist()],
                    "high_prices": [float(x) for x in df["High"].tolist()],
                    "low_prices": [float(x) for x in df["Low"].tolist()],
                    "volumes": [float(x) for x in df["Volume"].tolist()],
                }}
            else:
                result = {{"error": "yfinance 返回空数据"}}
        except ImportError:
            result = {{"error": "yfinance 未安装，请在沙箱中 pip install yfinance"}}
        except Exception as e:
            result = {{"error": str(e)}}
    else:
        result = {{"error": f"不支持的市场: {{market}}"}}

    print(json.dumps(result, ensure_ascii=False, default=str))
except Exception as e:
    print(json.dumps({{"error": str(e)}}, ensure_ascii=False))
'''

COMPUTE_INDICATORS_SCRIPT = r'''
import json, math

data = {price_data!r}
close_prices = data.get("close_prices", [])
high_prices = data.get("high_prices", [])
low_prices = data.get("low_prices", [])
volumes = data.get("volumes", [])
indicators = {indicators!r}

result = {{}}

def sma(prices, window):
    if len(prices) < window:
        return [None] * len(prices)
    out = [None] * (window - 1)
    for i in range(window - 1, len(prices)):
        out.append(sum(prices[i-window+1:i+1]) / window)
    return out

def ema(prices, window):
    if len(prices) < 2:
        return [None] * len(prices)
    out = [prices[0]]
    multiplier = 2 / (window + 1)
    for i in range(1, len(prices)):
        out.append(prices[i] * multiplier + out[-1] * (1 - multiplier))
    return out

if "ma" in indicators and close_prices:
    result["ma5"] = sma(close_prices, 5)
    result["ma10"] = sma(close_prices, 10)
    result["ma20"] = sma(close_prices, 20)
    result["ma60"] = sma(close_prices, 60) if len(close_prices) >= 60 else [None] * len(close_prices)
    # 当前值
    result["ma_current"] = {{
        "ma5": result["ma5"][-1] if result["ma5"][-1] is not None else None,
        "ma10": result["ma10"][-1] if result["ma10"][-1] is not None else None,
        "ma20": result["ma20"][-1] if result["ma20"][-1] is not None else None,
        "ma60": result["ma60"][-1] if len(close_prices) >= 60 and result["ma60"][-1] is not None else None,
    }}

if "rsi" in indicators and close_prices and len(close_prices) >= 15:
    delta = [close_prices[i] - close_prices[i-1] for i in range(1, len(close_prices))]
    gain = [max(d, 0) for d in delta]
    loss = [abs(min(d, 0)) for d in delta]
    # Wilder's smoothing
    avg_gain = sum(gain[:14]) / 14
    avg_loss = sum(loss[:14]) / 14
    rsi = [None] * 14
    if avg_loss == 0:
        rsi.append(100.0)
    else:
        rsi.append(100 - (100 / (1 + avg_gain / avg_loss)))
    for i in range(14, len(delta)):
        avg_gain = (avg_gain * 13 + gain[i]) / 14
        avg_loss = (avg_loss * 13 + loss[i]) / 14
        if avg_loss == 0:
            rsi.append(100.0)
        else:
            rsi.append(round(100 - (100 / (1 + avg_gain / avg_loss)), 2))
    result["rsi14"] = rsi
    result["rsi_current"] = rsi[-1]

if "macd" in indicators and close_prices and len(close_prices) >= 26:
    ema12 = ema(close_prices, 12)
    ema26 = ema(close_prices, 26)
    dif = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
    dea = ema(dif, 9)
    macd_hist = [(d - e) * 2 for d, e in zip(dif, dea)]
    result["macd"] = {{
        "dif": dif[-1] if dif else None,
        "dea": dea[-1] if dea else None,
        "histogram": macd_hist[-1] if macd_hist else None,
        "signal": "golden_cross" if dif[-1] > dea[-1] and dif[-2] <= dea[-2] if len(dif) > 1 and len(dea) > 1 else False else "death_cross" if dif[-1] < dea[-1] and dif[-2] >= dea[-2] if len(dif) > 1 and len(dea) > 1 else False else "neutral",
    }}

if "bollinger" in indicators and close_prices and len(close_prices) >= 20:
    ma20 = sma(close_prices, 20)
    std20 = [None] * 19
    for i in range(19, len(close_prices)):
        window = close_prices[i-19:i+1]
        m = sum(window) / 20
        variance = sum((x - m) ** 2 for x in window) / 20
        std20.append(math.sqrt(variance))
    result["bollinger"] = {{
        "middle": ma20[-1] if ma20[-1] is not None else None,
        "upper": ma20[-1] + 2 * std20[-1] if ma20[-1] is not None and std20[-1] is not None else None,
        "lower": ma20[-1] - 2 * std20[-1] if ma20[-1] is not None and std20[-1] is not None else None,
        "position": "above_upper" if close_prices[-1] > ma20[-1] + 2 * std20[-1] else "below_lower" if close_prices[-1] < ma20[-1] - 2 * std20[-1] else "within_band",
        "width": (2 * std20[-1] / ma20[-1] * 100) if ma20[-1] and std20[-1] else None,
    }}

if "kdj" in indicators and close_prices and high_prices and low_prices and len(close_prices) >= 9:
    k, d = 50, 50
    k_values, d_values, j_values = [], [], []
    for i in range(8, len(close_prices)):
        hh = max(high_prices[i-8:i+1])
        ll = min(low_prices[i-8:i+1])
        rsv = ((close_prices[i] - ll) / (hh - ll) * 100) if hh != ll else 50
        k = 2/3 * k + 1/3 * rsv
        d = 2/3 * d + 1/3 * k
        j = 3 * k - 2 * d
        k_values.append(round(k, 2))
        d_values.append(round(d, 2))
        j_values.append(round(j, 2))
    result["kdj"] = {{
        "k": k_values[-1] if k_values else None,
        "d": d_values[-1] if d_values else None,
        "j": j_values[-1] if j_values else None,
    }}

if "atr" in indicators and high_prices and low_prices and close_prices and len(close_prices) >= 15:
    tr = []
    for i in range(1, len(close_prices)):
        tr.append(max(
            high_prices[i] - low_prices[i],
            abs(high_prices[i] - close_prices[i-1]),
            abs(low_prices[i] - close_prices[i-1])
        ))
    atr = sum(tr[:14]) / 14
    atrs = [None] * 14 + [atr]
    for i in range(14, len(tr)):
        atr = (atr * 13 + tr[i]) / 14
        atrs.append(round(atr, 4))
    result["atr14"] = atrs[-1]

# 只保留可序列化的值
def clean(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {{k: clean(v) for k, v in obj.items()}}
    if isinstance(obj, list):
        return [clean(v) for v in obj]
    return obj

print(json.dumps(clean(result), ensure_ascii=False, default=str))
'''


# ── 系统提示词 ──────────────────────────────────────────────

SYSTEM_PROMPT = """你是 Agent4Quant 量化研究助手，帮助用户查询股票行情、计算技术指标、解读市场数据。

## 你的能力

1. **行情查询**：获取 A 股、港股、美股的历史价格数据（开高低收量）
2. **技术指标**：计算 MA、RSI、MACD、布林带、KDJ、ATR
3. **报告解读**：用自然语言解读当前技术面状况
4. **回测引导**：当用户想做策略回测时，生成 agent4quant CLI 命令让他们在本地执行

## 重要边界

- 你**不做**策略回测（引导用户用本地 CLI：`pip install agent4quant && a4q backtest run ...`）
- 你**不给**投资建议、买卖信号或荐股
- 你**不预测**股价走势，只描述当前技术面状态
- 技术指标计算基于公开算法，不代表任何投资观点
- 所有分析结果仅供参考，不构成投资建议

## 交互风格

- 当用户给你一个股票代码时，主动拉数据显示摘要
- 指标解读要具体、有数据支撑，不要泛泛而谈
- 用表格格式展示多指标对比
- 每次回复末尾附简短免责声明

## 免责声明模板

> ⚠️ 以上分析结果仅供参考，不构成任何投资建议。量化研究应使用本地工具独立完成。完整回测请使用 agent4quant CLI。
"""


# ── 主 Handler ──────────────────────────────────────────────

async def handler(context):
    """
    EdgeOne Makers Agent 入口。

    context 注入字段：
    - context.request.body : dict，已解析的请求体
    - context.env           : 环境变量（AI_GATEWAY_API_KEY, AI_GATEWAY_BASE_URL）
    - context.store          : 会话级对话存储
    - context.tools          : LLM 工具清单（sandbox 原子工具）
    - context.sandbox        : 沙箱原子 API
    - context.tracer         : OpenTelemetry 追踪
    - context.conversation_id: 当前会话 ID
    """
    # 调试：查看 context.request.body 的实际内容
    body = context.request.body
    debug_info = {
        "body_type": str(type(body)),
        "body_repr": str(body)[:500],
        "has_message_key": isinstance(body, dict) and "message" in body,
        "has_query_key": isinstance(body, dict) and "query" in body,
        "body_keys": list(body.keys()) if isinstance(body, dict) else "not_a_dict",
        "conversation_id": getattr(context, "conversation_id", "no_attr"),
    }
    return {
        "status": "ok",
        "response": f"DEBUG: {json.dumps(debug_info, ensure_ascii=False, indent=2)}",
    }

    # EdgeOne Makers 已自动解析 body 为 dict
    body = context.request.body
    if body is None:
        body = {}
    if not isinstance(body, dict):
        try:
            body = json.loads(str(body))
        except Exception:
            body = {}

    user_message = ""
    if isinstance(body, dict):
        user_message = body.get("message", body.get("query", body.get("content", "")))

    if not user_message:
        return {
            "status": "ok",
            "response": "你好！我是 Agent4Quant 量化研究助手。\n\n请告诉我你想了解什么？例如：\n- 查询行情：「帮我看看 000001.SZ 最近表现」\n- 技术指标：「计算 600519.SH 的 MACD 和 RSI」\n- 回测引导：「我想回测 000300 的均线策略」",
        }

    # 初始化 OpenAI 客户端（走 EdgeOne AI Gateway）
    api_key = context.env.get("AI_GATEWAY_API_KEY", "")
    base_url = context.env.get("AI_GATEWAY_BASE_URL", "https://api.openai.com/v1")

    if not api_key:
        return {"status": "error", "response": "AI Gateway API key 未配置"}

    client = AsyncOpenAI(api_key=api_key, base_url=base_url)

    # 加载对话历史
    history = await _load_history(context.store)

    # 构建消息
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        *history,
        {"role": "user", "content": user_message},
    ]

    # ── Agent 循环（最多 5 轮工具调用）──
    max_turns = 5
    for turn in range(max_turns):
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=QUANT_TOOLS,
            tool_choice="auto",
            temperature=0.3,
        )

        choice = response.choices[0]
        msg = choice.message

        # 无工具调用 → 结束循环
        if not msg.tool_calls:
            messages.append({"role": "assistant", "content": msg.content})
            final_response = msg.content or ""
            break

        # 有工具调用 → 执行
        messages.append({
            "role": "assistant",
            "content": msg.content,
            "tool_calls": [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in msg.tool_calls
            ],
        })

        for tc in msg.tool_calls:
            tool_name = tc.function.name
            tool_args = json.loads(tc.function.arguments)

            # 在沙箱中执行工具
            tool_result = await _execute_tool(context, tool_name, tool_args)

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(tool_result, ensure_ascii=False, default=str),
            })
    else:
        # 超过最大轮数，让 LLM 总结
        final_response = "分析进行中，但步骤较多。请尝试更具体地描述你的需求。"

    # 保存对话历史
    await _save_history(
        context.store,
        history,
        user_message,
        final_response,
    )

    return {
        "status": "ok",
        "response": final_response,
    }


# ── 工具执行器 ──────────────────────────────────────────────

async def _execute_tool(context, tool_name: str, args: dict) -> dict:
    """在沙箱中执行量化工具。"""

    if tool_name == "fetch_stock_data":
        script = FETCH_STOCK_DATA_SCRIPT.format(
            symbol=args["symbol"],
            market=args["market"],
            period=args.get("period", "3mo"),
        )
        result = await _run_python(context, script)
        return result

    elif tool_name == "compute_indicators":
        price_data = args.get("price_data", {})
        indicators = args.get("indicators", ["ma", "rsi", "macd"])
        script = COMPUTE_INDICATORS_SCRIPT.format(
            price_data=json.dumps(price_data),
            indicators=json.dumps(indicators),
        )
        result = await _run_python(context, script)
        return result

    elif tool_name == "generate_analysis_report":
        return {
            "report_type": "technical_analysis",
            "symbol": args["symbol"],
            "note": "报告由 LLM 基于指标数据直接生成，更灵活准确。此工具返回标记供 LLM 使用。",
        }

    elif tool_name == "get_cli_backtest_command":
        strategy = args["strategy"]
        symbol = args["symbol"]
        cmd = f"a4q backtest run --symbol {symbol} --strategy {strategy}"
        if "start_date" in args:
            cmd += f" --start {args['start_date']}"
        if "end_date" in args:
            cmd += f" --end {args['end_date']}"
        cmd += " --result-json result.json --report-html result.html"

        return {
            "strategy": strategy,
            "symbol": symbol,
            "command": cmd,
            "install_hint": "如未安装 agent4quant，先执行：pip install agent4quant",
            "note": "策略回测在本地执行，确保策略隐私。云端不保留任何回测数据。",
        }

    return {"error": f"未知工具: {tool_name}"}


# ── 沙箱执行 ──────────────────────────────────────────────

async def _run_python(context, script: str) -> dict:
    """在沙箱中执行 Python 代码并返回解析后的结果。"""
    try:
        result = await context.sandbox.runCode("python", script)
        if isinstance(result, str):
            return json.loads(result)
        if hasattr(result, "text"):
            return json.loads(result.text)
        if hasattr(result, "stdout"):
            return json.loads(result.stdout)
        return {"raw": str(result)}
    except json.JSONDecodeError:
        return {"raw_output": str(result) if "result" in dir() else "execution error"}
    except Exception as e:
        return {"error": f"sandbox 执行失败: {str(e)}"}


# ── 对话历史管理 ──────────────────────────────────────────

async def _load_history(store, limit: int = 10) -> list:
    """从 store 加载最近 N 轮对话。"""
    try:
        messages = await store.get("messages", [])
        return messages[-limit * 2:] if messages else []
    except Exception:
        return []


async def _save_history(store, old_messages: list, user_msg: str, assistant_msg: str):
    """保存对话到 store。"""
    try:
        old_messages.append({"role": "user", "content": user_msg})
        old_messages.append({"role": "assistant", "content": assistant_msg})
        # 只保留最近 20 轮
        await store.set("messages", old_messages[-40:])
    except Exception:
        pass


# ── 请求体解析 ─────────────────────────────────────────────

async def _parse_body(body) -> dict:
    """兼容多种请求体格式。"""
    if body is None:
        return {}
    if isinstance(body, dict):
        return body
    if isinstance(body, (str, bytes)):
        try:
            return json.loads(body)
        except (json.JSONDecodeError, TypeError):
            return {}
    # stream / async reader
    if hasattr(body, "read"):
        try:
            raw = await body.read()
            return json.loads(raw)
        except Exception:
            return {}
    return {}
