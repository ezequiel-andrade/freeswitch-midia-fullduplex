import asyncio
import os
from typing import Any

import httpx


SERPAPI_URL = "https://serpapi.com/search.json"


async def search_web(
    query: str,
    num_results: int = 3,
    hl: str = "pt-BR",
    gl: str = "br",
    recency_days: int | None = None,
) -> dict:
    """Pesquisa na web via SerpAPI e retorna um resumo estruturado."""
    try:
        api_key = os.getenv("SERPAPI_API_KEY", "").strip()
        if not api_key:
            return {"success": False, "error": "SERPAPI_API_KEY não configurada."}

        q = (query or "").strip()
        if not q:
            return {"success": False, "error": "Query vazia."}

        limit = max(1, min(int(num_results), 5))

        params = {
            "engine": "google",
            "q": q,
            "api_key": api_key,
            "hl": hl,
            "gl": gl,
            "num": limit,
        }
        if recency_days is not None:
            try:
                days = int(recency_days)
                if days > 0:
                    params["tbs"] = f"qdr:d{days}"
            except Exception:
                pass

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(SERPAPI_URL, params=params)
            response.raise_for_status()
            data = response.json()

        def _str(v: Any) -> str:
            return v if isinstance(v, str) else ""

        organic = data.get("organic_results", []) or []
        items: list[dict[str, Any]] = []

        # 1) answer_box (quando o Google já responde direto)
        answer_box = data.get("answer_box") or {}
        if isinstance(answer_box, dict):
            ab_title = _str(answer_box.get("title"))
            ab_answer = _str(answer_box.get("answer")) or _str(answer_box.get("snippet"))
            ab_source = _str(answer_box.get("source"))
            ab_link = _str(answer_box.get("link"))
            if ab_title or ab_answer:
                items.append(
                    {
                        "title": ab_title or "Resposta direta",
                        "snippet": ab_answer,
                        "link": ab_link,
                        "source": ab_source or "Google",
                    }
                )

        # 2) knowledge_graph
        kg = data.get("knowledge_graph") or {}
        if isinstance(kg, dict):
            kg_title = _str(kg.get("title"))
            kg_type = _str(kg.get("type"))
            kg_desc = _str(kg.get("description"))
            if kg_title or kg_desc:
                snippet = f"{kg_title} ({kg_type}). {kg_desc}".strip()
                items.append(
                    {
                        "title": kg_title or "Knowledge Graph",
                        "snippet": snippet,
                        "link": "",
                        "source": "Google Knowledge Graph",
                    }
                )
        for r in organic[:limit]:
            items.append(
                {
                    "title": r.get("title", ""),
                    "snippet": r.get("snippet", ""),
                    "link": r.get("link", ""),
                    "source": r.get("source", ""),
                }
            )

        # 3) perguntas relacionadas como fallback extra
        if not items:
            related = data.get("related_questions", []) or []
            for r in related[:limit]:
                if isinstance(r, dict):
                    items.append(
                        {
                            "title": _str(r.get("question")) or "Pergunta relacionada",
                            "snippet": _str(r.get("snippet")),
                            "link": _str(r.get("link")),
                            "source": _str(r.get("source")) or "Google",
                        }
                    )

        if not items:
            return {"success": False, "error": "Não encontrei resultados relevantes."}

        return {"success": True, "result": {"query": q, "items": items[:limit]}}
    except httpx.TimeoutException:
        return {"success": False, "error": "A pesquisa demorou demais para responder."}
    except Exception as e:
        return {"success": False, "error": f"Erro na pesquisa web: {e}"}


async def _search_web_with_timeout(
    query: str,
    num_results: int = 3,
    hl: str = "pt-BR",
    gl: str = "br",
    recency_days: int | None = None,
) -> dict:
    try:
        return await asyncio.wait_for(
            search_web(
                query=query,
                num_results=num_results,
                hl=hl,
                gl=gl,
                recency_days=recency_days,
            ),
            timeout=12.0,
        )
    except asyncio.TimeoutError:
        return {"success": False, "error": "A pesquisa web excedeu o tempo limite."}
    except Exception as e:
        return {"success": False, "error": f"Erro inesperado na pesquisa web: {e}"}


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_web",
            "description": (
                "Pesquisa informações atuais na internet quando a pergunta depende "
                "de fatos recentes ou quando você não tem certeza da resposta."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Termo de pesquisa objetivo e específico.",
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Quantidade de resultados (1 a 5).",
                        "default": 3,
                    },
                    "hl": {
                        "type": "string",
                        "description": "Idioma da busca (ex.: pt-BR).",
                        "default": "pt-BR",
                    },
                    "gl": {
                        "type": "string",
                        "description": "País da busca (ex.: br, us).",
                        "default": "br",
                    },
                    "recency_days": {
                        "type": "integer",
                        "description": "Filtra por resultados recentes em dias (opcional, ex.: 7).",
                    },
                },
                "required": ["query"],
            },
        },
    }
]
