import asyncio
import logging
import time
from collections.abc import AsyncGenerator
from pathlib import Path
from uuid import UUID

from pyraft.state_machine import StateMachine
from pyraft.vo import NodeAddress


class Snapshot:
    """
    生成、管理、记录单个快照
    """
    def __init__(
        self,
        id: UUID,
        last_included_index: int,
        last_included_term: int,
        dir: str,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.id = id
        self.peer_ids: list[
            NodeAddress
        ] = []  # 用于记录已经接收到快照的节点,数量不多，不用持久化
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self.dir = dir
        self._loop = loop

        # 是否正在使用，如果正在使用，不允许更新
        self.is_in_use: bool = False

    def write(self, state_machine: StateMachine) -> None:
        """
        写入快照
        """
        # 写入快照是在 fork 一个新的进程中进行的，所以可以阻塞
        with Path(f"{self.dir}{self.id}.snapshot").open("wb") as f:
            # 第一行是 last_included_index,last_included_term，用于恢复时初始化
            f.write(f"{self.last_included_index},{self.last_included_term}\n".encode())
            for line in state_machine.dumps():
                f.write((line + "\n").encode())

        time.sleep(3)  # 模拟写入快照需要的时间

    async def read(self, batch_size: int) -> AsyncGenerator[list[str]]:
        """
        读取快照
        """
        with Path(f"{self.dir}{self.id}.snapshot").open("r") as f:  # noqa: ASYNC230
            # 忽略第一行
            f.readline()
            while True:
                lines = await self._loop.run_in_executor(None, f.readlines, batch_size)
                if not lines:
                    break

                yield lines

    def clear(self) -> None:
        """
        删除快照文件
        """
        snapshot_file = Path(f"{self.dir}{self.id}.snapshot")
        try:
            if snapshot_file.exists():
                snapshot_file.unlink()
        except Exception:
            logging.error(
                f"delete snapshot {self.dir}{self.id}.snapshot failed",
                exc_info=True,
            )

    def __del__(self) -> None:
        """
        删除快照文件
        """
        self.clear()
