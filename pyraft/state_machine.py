import json
import logging
from collections.abc import Generator
from typing import Any

from pyraft.vo import ADDRESS_PARTS_COUNT, LogCommand, LogEntry


class StateMachine:
    """
    状态机，这里简单实现，只是一个 dict
    """
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    def apply_log(self, log: LogEntry) -> None:
        """
        应用日志
        """
        if log.command == LogCommand.SET:
            self.data[log.args.key] = log.args.args
        elif log.command == LogCommand.DEL:
            del self.data[log.args.key]

    def get(self, key: str) -> Any:  # noqa: ANN401
        """
        获取数据
        """
        return self.data.get(key)

    def dumps(self) -> Generator[str]:
        """
        用于生成快照格式数据
        """
        for key, value in self.data.items():
            josn_value = json.dumps(value, indent=None)
            yield f"{key}/{josn_value}"

    def loads(self, lines: list[str]) -> None:
        """
        用于加载多行快照数据
        """
        for line in lines:
            self.load(line)

    def load(self, line: str) -> None:
        """
        用于加载一行快照数据
        """
        line = line.strip("\n").strip("")
        if not line:
            return
        strs = line.split("/")
        if len(strs) != ADDRESS_PARTS_COUNT:
            return
        key = strs[0]
        try:
            value = json.loads(strs[1])
            self.data[key] = value
        except Exception:
            logging.exception("Failed to load state machine")

    def clear(self) -> None:
        """
        安装快照的时候用于清空数据
        """
        self.data.clear()
