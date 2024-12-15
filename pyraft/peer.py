import asyncio
import logging
from enum import IntEnum
from typing import Any

import aiohttp
from aiohttp.client import ClientConnectorError

from pyraft.vo import (
    AppendLogsRes,
    InstallSnapshotRes,
    JoinRes,
    LogEntry,
    NodeAddress,
    VoteRes,
)


class Method(IntEnum):
    PUT = 1
    DELETE = 2


class Peer:
    """
    处理和其他节点的通信
    """
    def __init__(self, address: NodeAddress, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self.address = address

    def __str__(self) -> str:
        return f"{self.address.host}:{self.address.port}"

    @property
    def id(self) -> NodeAddress:
        return self.address

    async def _request(
        self,
        method: Method,
        path: str,
        body: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        发送请求，支持自动重定向
        """
        timeout = aiohttp.ClientTimeout(total=0.1)
        url = f"http://{self.address.host}:{self.address.port}{path}"
        try:
            async with aiohttp.ClientSession(
                timeout=timeout, trust_env=True, auto_decompress=True
            ) as session:
                if method == Method.PUT:
                    async with session.put(
                        url, json=body, max_redirects=3, allow_redirects=True
                    ) as response:
                        return await response.json()
                elif method == Method.DELETE:
                    async with session.delete(
                        url, json=body, max_redirects=3, allow_redirects=True
                    ) as response:
                        return await response.json()
        except ClientConnectorError:
            logging.error(f"Failed to connect to {self.address}")
            return None
        except Exception:
            logging.error(f"Failed to send request to {self.address}", exc_info=True)
            return None

    async def prevote(
        self,
        term: int,
        candidate_id: NodeAddress,
        last_log_index: int,
        last_log_term: int,
    ) -> VoteRes | None:
        """
        发送预投票请求
        """
        result = await self._request(
            Method.PUT,
            "/__pyraft/prevote",
            {
                "term": term,
                "candidate_id": candidate_id.to_dict(),
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
            },
        )
        if not result:
            return None
        return VoteRes.from_dict(result)

    async def vote(
        self,
        term: int,
        candidate_id: NodeAddress,
        last_log_index: int,
        last_log_term: int,
    ) -> VoteRes | None:
        """
        发送投票请求
        """
        result = await self._request(
            Method.PUT,
            "/__pyraft/vote",
            {
                "term": term,
                "candidate_id": candidate_id.to_dict(),
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
            },
        )
        if not result:
            return None
        return VoteRes.from_dict(result)

    async def append_logs(  # noqa: PLR0913
        self,
        term: int,
        leader_id: NodeAddress,
        prev_log_index: int,
        prev_log_term: int,
        entries: list[LogEntry],
        leader_commit_index: int,
    ) -> AppendLogsRes | None:
        """
        发送日志追加请求
        """
        result = await self._request(
            Method.PUT,
            "/__pyraft/logs",
            {
                "term": term,
                "leader_id": leader_id.to_dict(),
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": [i.to_dict() for i in entries],
                "leader_commit_index": leader_commit_index,
            },
        )
        if not result:
            return None
        return AppendLogsRes.from_dict(result)

    async def install_snapshot(  # noqa: PLR0913
        self,
        term: int,
        leader_id: NodeAddress,
        last_included_index: int,
        last_included_term: int,
        offset: int,
        data: list[str],
        logs: list[LogEntry],
        peer_ids: list[NodeAddress],
        done: bool,
    ) -> InstallSnapshotRes | None:
        """
        发送快照请求
        """
        result = await self._request(
            Method.PUT,
            "/__pyraft/snapshot",
            {
                "term": term,
                "leader_id": leader_id.to_dict(),
                "last_included_index": last_included_index,
                "last_included_term": last_included_term,
                "offset": offset,
                "data": data,
                "logs": [i.to_dict() for i in logs],
                "peer_ids": [i.to_dict() for i in peer_ids],
                "done": done,
            },
        )
        if not result:
            return None
        return InstallSnapshotRes.from_dict(result)

    async def join(self, address: NodeAddress) -> JoinRes | None:
        """
        发送加入集群请求
        """
        result = await self._request(Method.PUT, f"/__pyraft/peer/{address}", {})
        if not result:
            return None
        return JoinRes.from_dict(result)

    async def leave(self, address: NodeAddress) -> JoinRes | None:
        """
        发送离开集群请求
        """
        result = await self._request(Method.DELETE, f"/__pyraft/peer/{address}", {})
        if not result:
            return None
        return JoinRes.from_dict(result)
