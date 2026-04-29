import httpx
import anthropic
from typing import AsyncIterator
from config import settings


class ClaudeClient:
    def __init__(self):
        self.client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    async def complete(self, system: str, user: str) -> str:
        msg = await self.client.messages.create(
            model      = settings.claude_model,
            max_tokens = 2048,
            system     = system,
            messages   = [{"role": "user", "content": user}]
        )
        return msg.content[0].text

    async def stream(self, system: str, user: str) -> AsyncIterator[str]:
        async with self.client.messages.stream(
            model      = settings.claude_model,
            max_tokens = 2048,
            system     = system,
            messages   = [{"role": "user", "content": user}]
        ) as s:
            async for text in s.text_stream:
                yield text


class OllamaClient:
    def __init__(self):
        self.url   = settings.ollama_url
        self.model = settings.ollama_model

    async def complete(self, system: str, user: str) -> str:
        prompt = f"[INST] {system}\n\n{user} [/INST]"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                f"{self.url}/api/generate",
                json={"model": self.model, "prompt": prompt, "stream": False}
            )
            resp.raise_for_status()
            return resp.json()["response"]

    async def stream(self, system: str, user: str) -> AsyncIterator[str]:
        import json
        prompt = f"[INST] {system}\n\n{user} [/INST]"
        async with httpx.AsyncClient(timeout=120) as client:
            async with client.stream(
                "POST", f"{self.url}/api/generate",
                json={"model": self.model, "prompt": prompt, "stream": True}
            ) as resp:
                async for line in resp.aiter_lines():
                    if line:
                        data = json.loads(line)
                        if token := data.get("response"):
                            yield token


def get_llm_client():
    return OllamaClient() if settings.llm_provider == "ollama" else ClaudeClient()
