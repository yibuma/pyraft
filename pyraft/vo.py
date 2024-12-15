from dataclasses import dataclass, fields
from enum import Enum, IntEnum
from typing import Any, TypeVar, get_args

ADDRESS_PARTS_COUNT = 2  # 地址字符串按照冒号分割后的长度

HEAHEARTBEAT_INTERVAL = 0.1  # 心跳周期 单位秒
HEAHEARTBEAT_LOG_BATCH_SIZE = 10  # 单次发送日志的最大数量

MAX_WAIT_FOR_COMMIT_TIMES = 10  # 没有及时commit时，最多等待心跳周期的次数

MAX_SNAPSHOT_PAGE_SIZE = 1024  # 读取快照文件的时候最大的读取字节数

ELECTION_TIMEOUT = (
    1  # 选举超时时间，单位秒，因为心跳周期是0.1秒，所以这里是10个心跳周期
)

SNAPSHOTTING_SUCCESS = "1"  # 快照成功的消息
SNAPSHOTTING_FAILED = "0"  # 快照失败的消息


class NodeRole(IntEnum):
    LEARNER = 0  # 新增的节点，还没有参与选举
    FOLLOWER = 1  # 跟随者
    CANDIDATE = 2  # 候选者
    LEADER = 3  # 领导者

    def __str__(self) -> str:
        return self.name.title()


class LogCommand(IntEnum):
    """
    日志命令
    """
    JOIN = 1  # 集群变更 - 加入
    LEAVE = 2  # 集群变更 - 离开
    SET = 3  # 设置数据
    DEL = 4  # 删除数据


class KeyOp(IntEnum):
    SET = 1
    DEL = 2


T = TypeVar("T", bound="BaseVO")


@dataclass
class BaseVO:
    def to_dict(self) -> dict[str, Any]:
        """
        将对象转换为 dict，便于给其他节点发送数据
        """
        result = {}
        for field in fields(self):
            value = getattr(self, field.name)
            if isinstance(value, list):
                result[field.name] = [
                    item.to_dict() if isinstance(item, BaseVO) else item
                    for item in value  # type: ignore
                ]
            else:
                result[field.name] = (
                    value.to_dict() if isinstance(value, BaseVO) else value
                )
        return result  # type: ignore

    @classmethod
    def from_dict(cls: type[T], data: dict[str, Any]) -> T:
        """
        从 dict 中创建对象
        """
        field_types = {field.name: field.type for field in fields(cls)}
        kwargs = {}

        for key, value in data.items():
            if key not in field_types:
                continue

            field_type = field_types[key]
            type_args = get_args(field_type)

            if value is None:
                kwargs[key] = None
                continue

            # Handle List type
            if type_args and isinstance(value, list):
                inner_type = type_args[0]
                if issubclass(inner_type, BaseVO):
                    kwargs[key] = [inner_type.from_dict(item) for item in value]  # type: ignore
                else:
                    kwargs[key] = value
                continue

            # Handle Optional type
            if type_args and any(
                issubclass(t, BaseVO) for t in type_args if isinstance(t, type)
            ):
                vo_type = next(
                    t
                    for t in type_args
                    if isinstance(t, type) and issubclass(t, BaseVO)
                )
                kwargs[key] = vo_type.from_dict(value)
                continue

            # Handle BaseVO type
            if isinstance(field_type, type) and issubclass(field_type, BaseVO):
                kwargs[key] = field_type.from_dict(value)
                continue

            # Handle Enum type
            if isinstance(field_type, type) and issubclass(field_type, Enum):
                kwargs[key] = field_type(value)
                continue

            kwargs[key] = value

        return cls(**kwargs)


@dataclass
class NodeAddress(BaseVO):
    """
    节点地址
    """
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class LogArgs(BaseVO):
    """
    日志参数
    """
    key: str  # 要操作的数据 key，对应 StateMachine 中的 key，集群变更日志该字段无效
    args: Any  # 要操作的数据，对应 StateMachine 中的 value，集群变更日志该字段无效
    address: NodeAddress | None  # 节点地址，集群变更日志该字段有效


@dataclass
class LogEntry(BaseVO):
    """
    日志条目
    """
    index: int # 日志索引
    term: int  # 日志任期
    command: LogCommand  # 日志命令
    args: LogArgs  # 日志参数


@dataclass
class AppendLogsReq(BaseVO):
    """
    附加日志请求
    """
    term: int  # 领导者的任期
    leader_id: NodeAddress  # 领导者的地址
    prev_log_index: int  # 领导者最后一条日志的索引
    prev_log_term: int  # 领导者最后一条日志的任期
    entries: list[LogEntry]  # 日志条目
    leader_commit_index: int  # 领导者的 commit 索引


@dataclass
class AppendLogsRes(BaseVO):
    """
    附加日志响应
    """
    id: NodeAddress  # 节点地址
    term: int  # 当前任期
    success: bool  # 是否成功
    last_log_index: int  # 最后一条日志的索引
    commit_index: int  # 已经提交的日志索引
    snapshot_index: int  # 最新的快照索引


@dataclass
class VoteReq(BaseVO):
    """
    投票请求
    """
    term: int
    candidate_id: NodeAddress  # 候选者的地址
    last_log_index: int
    last_log_term: int


@dataclass
class VoteRes(BaseVO):
    term: int
    vote_granted: bool


@dataclass
class JoinReq(BaseVO):
    address: NodeAddress


@dataclass
class JoinRes(BaseVO):
    term: int
    success: bool


@dataclass
class InstallSnapshotReq(BaseVO):
    """
    安装快照请求
    """
    term: int  # 领导者的任期
    leader_id: NodeAddress  # 领导者的地址
    last_included_index: int  # 快照中最后一条日志的索引
    last_included_term: int  # 快照中最后一条日志的任期
    offset: int  # 快照数据偏移量
    data: list[str]  # 快照数据
    logs: list[LogEntry]  # 已经被压缩进快照里的最后日志
    peer_ids: list[NodeAddress]  # 所有节点地址
    done: bool  # 是否完成


@dataclass
class InstallSnapshotRes(BaseVO):
    """
    安装快照响应
    """
    term: int
    success: bool
