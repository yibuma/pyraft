import asyncio
import logging
import os
import random
import sys
from collections.abc import Coroutine
from typing import Any
from uuid import uuid4

from pyraft.peer import Peer
from pyraft.snapshot import Snapshot
from pyraft.state_machine import StateMachine
from pyraft.vo import (
    ELECTION_TIMEOUT,
    HEAHEARTBEAT_INTERVAL,
    HEAHEARTBEAT_LOG_BATCH_SIZE,
    MAX_SNAPSHOT_PAGE_SIZE,
    MAX_WAIT_FOR_COMMIT_TIMES,
    SNAPSHOTTING_FAILED,
    SNAPSHOTTING_SUCCESS,
    AppendLogsReq,
    AppendLogsRes,
    InstallSnapshotReq,
    InstallSnapshotRes,
    JoinRes,
    KeyOp,
    LogArgs,
    LogCommand,
    LogEntry,
    NodeAddress,
    NodeRole,
    VoteReq,
    VoteRes,
)


class Node:
    """
    表示一个 Raft 集群中的节点。实现了 Raft 一致性算法的核心功能。
    该类负责管理节点的状态、日志复制、leader 选举、快照等核心功能。
    每个节点可以是以下角色之一：
    - LEADER: 负责处理所有客户端请求，并管理日志复制
    - FOLLOWER: 响应来自 leader 的请求，参与投票
    - CANDIDATE: 用于 leader 选举过程中的临时状态
    - LEARNER: 新加入节点的临时状态，只接收日志不参与投票
    主要职责包括:
    1. 维护节点状态和日志
    2. 处理日志追加和复制
    3. 处理选举和投票
    4. 管理成员变更
    5. 创建和安装快照
    6. 应用日志到状态机
    属性:
        role (NodeRole): 当前节点角色
        current_term (int): 当前任期号
        voted_for (NodeAddress): 当前任期投票给谁
        leader_id (NodeAddress): 当前 leader 节点地址
        logs (list[LogEntry]): 日志条目列表
        commit_index (int): 已提交的最高日志索引
        last_applied (int): 已应用到状态机的最高日志索引
        next_index (dict[Peer, int]): 记录每个节点的下一条要发送的日志索引
        match_index (dict[Peer, int]): 记录每个节点已确认的最高日志索引
        snapshot_index (dict[Peer, int]): 记录每个节点的快照索引
        peers (list[Peer]): 集群中的其他节点列表
        learn_peers (list[Peer]): 正在学习状态的节点列表
    方法:
        key_handler: 处理键值操作请求
        get_key_handler: 处理读取键值请求
        prevote_handler: 处理预投票请求
        vote_handler: 处理投票请求
        append_logs_handler: 处理日志追加请求
        install_snapshot_handler: 处理快照安装请求
        join_handler: 处理节点加入请求
        leave_handler: 处理节点离开请求
        start: 启动节点
        timed_job: 定时任务处理
    """

    def __init__(
        self,
        address: NodeAddress,
        join: NodeAddress | None,
        snapshot_path: str,
        state_machine: StateMachine,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._loop = loop  # 异步实现中的事件循环
        self._snapshot_path = snapshot_path  # 快照文件存储路径

        self.role: NodeRole = NodeRole.LEARNER  # 默认为 Learner
        self.current_term: int = 0  # 当前任期号
        self.voted_for: NodeAddress | None = None  # 当前任期投票给谁
        self.leader_id: NodeAddress | None = None  # 当前 leader 节点地址
        self.logs: list[LogEntry] = []  # 日志条目列表
        self.commit_index: int = 0  # 已提交的最高日志索引
        self.last_applied: int = 0  # 已应用到状态机的最高日志索引

        self.next_index: dict[Peer, int] = {}  # 记录每个节点的下一条要发送的日志索引
        self.match_index: dict[Peer, int] = {}  # 记录每个节点已确认的最高日志索引
        # 记录每个节点的快照索引，便于 leader 判断是否压缩日志生成新的快照。
        # 这样避免因为日志已经被压缩，需要给落后节点频繁发送快照，提高效率。
        self.snapshot_index: dict[Peer, int] = {}
        self.peers: list[Peer] = []  # 集群中的其他节点列表
        # 正在学习状态的节点列表，用于节点加入和离开的学习状态
        # 只有新加入的节点才会是 learner 状态，其他节点都是 follower
        # 当其数据追赶上 leader 时，会变成 follower
        # 当节点处于 learner 状态时，不参与投票，只接收日志
        self.learn_peers: list[Peer] = []

        self.latlast_heartbeat: float = 0  # 最后一次收到心跳的时间戳
        self.state_machine = state_machine  # 状态机
        self.snapshot: Snapshot | None = None  # 最新的快照
        self.is_snapshotting: bool = False  # 是否正在生成快照

        self.address = address  # 当前节点地址
        self.join = join  # 加入集群的节点地址

    @property
    def id(self) -> NodeAddress:
        return self.address

    @property
    def quorum(self) -> int:
        # 计算法定人数，移除处于 learner 状态的节点
        peers_count = len(self.peers) - len(self.learn_peers) + 1
        if peers_count % 2 == 0:
            return peers_count // 2 + 1
        return (peers_count + 1) // 2

    @property
    def last_log(self) -> LogEntry:
        if not self.logs:
            raise Exception("No logs exist in the node")
        return self.logs[-1]

    @property
    def first_log(self) -> LogEntry:
        if not self.logs:
            raise Exception("No logs exist in the node")
        return self.logs[0]

    def get_state_machine_data(self) -> dict[str, Any]:
        return self.state_machine.data

    def _get_peer(self, id: NodeAddress) -> Peer:
        for i in self.peers:
            if i.id == id:
                return i
        raise ValueError(f"Peer with id {id} not found")

    async def key_handler(
        self,
        op: KeyOp,
        key: str,
        value: Any,  # noqa: ANN401
    ) -> dict[str, Any] | None:
        """
        处理键值操作的异步方法
        作为 Leader 节点处理键值存储操作(SET/DELETE)。
        该方法会创建对应的日志条目并尝试复制到其他节点。
        1. 仅 Leader 节点可以处理写操作
        2. 会等待日志提交直到超时，超时返回 status 为 pending
        3. 通过 append_logs 复制日志到其他节点
        4. 最多等待 MAX_WAIT_FOR_COMMIT_TIMES 个心跳周期
        """
        if self.role != NodeRole.LEADER:
            return None
        index = self.last_log.index + 1
        command = LogCommand.SET if op == KeyOp.SET else LogCommand.DEL
        log = LogEntry(
            index=index,
            term=self.current_term,
            command=command,
            args=LogArgs(key=key, args=value, address=self.address),
        )
        self.logs.append(log)
        await self.append_logs(index)
        for _ in range(MAX_WAIT_FOR_COMMIT_TIMES):
            # 最大等待 N 个心跳周期，如果还没有提交，就返回 pending
            if self.commit_index >= index:
                return {"status": "ok", "value": value}
            await asyncio.sleep(HEAHEARTBEAT_INTERVAL)
        return {"status": "pending", "value": value}

    def get_key_handler(self, key: str) -> Any | None:  # noqa: ANN401
        """
        非 async 方法，因为不涉及到 IO 操作，也可以减少是否为 Leader 的判断
        """
        return self.state_machine.data.get(key, None)

    async def prevote_handler(self, req: VoteReq) -> VoteRes:
        """
        处理预投票请求，这里不会实际增加 term
        """
        logging.info(f"receive prevote from {req.candidate_id}")
        res = VoteRes(term=self.current_term, vote_granted=False)
        if req.term < self.current_term:
            return res

        if self.voted_for is not None:
            if self.voted_for == req.candidate_id:
                return VoteRes(term=self.current_term, vote_granted=True)
            return res

        if self.logs and req.last_log_term < self.last_log.term:
            return res
        if (
            self.logs
            and req.last_log_term == self.last_log.term
            and req.last_log_index < self.last_log.index
        ):
            return res

        res.vote_granted = True
        return res

    async def vote_handler(self, req: VoteReq) -> VoteRes:
        """
        处理投票请求
        """
        logging.info(f"receive vote from {req.candidate_id}")
        if req.term > self.current_term:
            self.current_term = req.term
            self.role = NodeRole.FOLLOWER
            self.voted_for = None
            self.latlast_heartbeat = self._loop.time()

        res = VoteRes(term=self.current_term, vote_granted=False)
        if req.term < self.current_term:
            return res

        if self.voted_for is not None:
            if self.voted_for == req.candidate_id:
                return VoteRes(term=self.current_term, vote_granted=True)
            else:
                return res

        if self.logs and req.last_log_term < self.last_log.term:
            return res
        if (
            self.logs
            and req.last_log_term == self.last_log.term
            and req.last_log_index < self.last_log.index
        ):
            return res

        self.voted_for = req.candidate_id
        self.latlast_heartbeat = self._loop.time()
        res.vote_granted = True
        return res

    async def append_logs_handler(self, req: AppendLogsReq) -> AppendLogsRes:
        """
        处理日志追加请求
        """
        res = AppendLogsRes(
            id=self.id,
            term=self.current_term,
            success=False,
            last_log_index=self.last_log.index if self.logs else 0,
            commit_index=self.commit_index,
            snapshot_index=self.snapshot.last_included_index if self.snapshot else 0,
        )
        if self.current_term > req.term:
            return res

        if self.role != NodeRole.FOLLOWER:
            self.role = NodeRole.FOLLOWER

        self.latlast_heartbeat = self._loop.time()
        self.leader_id = req.leader_id

        if req.term > self.current_term:
            self.current_term = req.term
            self.voted_for = None

        if not self.logs:
            # 如果是最初没有日志的时候，走特殊逻辑
            return await self.append_logs_from_0(req)

        if self.last_log.index < req.prev_log_index:
            return res
        prev_log = self.logs[req.prev_log_index - self.last_log.index - 1]
        if prev_log.term != req.prev_log_term:
            return res

        if req.entries:
            for log in req.entries:
                logging.info(f"receive log from {req.leader_id}: {log}")
                if log.index <= self.last_log.index:
                    # 如果日志 index 相同，但是 term 不同，删除之后的日志
                    if self.logs[log.index - self.last_log.index - 1].term != log.term:
                        self.logs = self.logs[: log.index - self.last_log.index - 1]
                        self.logs.append(log)
                else:
                    self.logs.append(log)

        if req.leader_commit_index > self.commit_index:
            # 更新 commit index
            self.commit_index = min(req.leader_commit_index, self.last_log.index)
            self._loop.create_task(self.apply_logs_job())

        # 因为有变化 res 需要重新生成
        return AppendLogsRes(
            id=self.id,
            term=self.current_term,
            success=True,
            last_log_index=self.last_log.index,
            commit_index=self.commit_index,
            snapshot_index=self.snapshot.last_included_index if self.snapshot else 0,
        )

    async def append_logs_from_0(self, req: AppendLogsReq) -> AppendLogsRes:
        """
        处理日志追加请求，当节点没有日志时
        """
        if req.prev_log_index > 0:
            return AppendLogsRes(
                id=self.id,
                term=self.current_term,
                success=False,
                last_log_index=0,
                commit_index=self.commit_index,
                snapshot_index=self.snapshot.last_included_index
                if self.snapshot
                else 0,
            )
        if req.entries:
            for log in req.entries:
                self.logs.append(log)

        if req.leader_commit_index > self.commit_index:
            self.commit_index = min(req.leader_commit_index, self.last_log.index)
            self._loop.create_task(self.apply_logs_job())
        return AppendLogsRes(
            id=self.id,
            term=self.current_term,
            success=True,
            last_log_index=self.last_log.index,
            commit_index=self.commit_index,
            snapshot_index=self.snapshot.last_included_index if self.snapshot else 0,
        )

    async def install_snapshot_handler(
        self, req: InstallSnapshotReq
    ) -> InstallSnapshotRes:
        """
        处理快照安装请求
        """
        logging.info(f"receive install snapshot from {req.leader_id}")
        if req.term < self.current_term:
            return InstallSnapshotRes(term=self.current_term, success=False)

        if self.role == NodeRole.LEADER:
            return InstallSnapshotRes(term=self.current_term, success=False)

        self.latlast_heartbeat = self._loop.time()
        self.leader_id = req.leader_id

        if req.term > self.current_term:
            self.current_term = req.term
            self.voted_for = None

        if not self.snapshot:
            self.snapshot = Snapshot(
                id=uuid4(),
                last_included_index=req.last_included_index,
                last_included_term=req.last_included_term,
                dir=self._snapshot_path,
                loop=self._loop,
            )

        if req.offset == 0:
            self.logs = req.logs  # 重置日志为快照的最后一条
            self.state_machine.clear()  # 清空状态机
            self.last_applied = req.last_included_index
            self.commit_index = req.last_included_index
            # 从快照获取 peers 信息
            self.peers = [Peer(id, self._loop) for id in req.peer_ids if id != self.id]

        if req.data:
            self.state_machine.loads(req.data)

        if req.done:
            # 快照安装完成
            self.role = NodeRole.FOLLOWER
            logging.info(f"Snapshot installed from {req.leader_id}")

        return InstallSnapshotRes(term=self.current_term, success=True)

    async def join_handler(self, addr: NodeAddress) -> JoinRes | None:
        """
        处理节点加入请求
        """
        logging.info(f"receive join from {addr}")
        try:
            self._get_peer(addr)
            return JoinRes(term=self.current_term, success=True)
        except ValueError:
            pass
        if self.leader_id != self.address:
            return None
        if self.learn_peers:
            # 有学习的节点，说明有节点正在刚刚加入
            # 为了避免引起leader节点的可用性，一次只能加入一个节点
            return JoinRes(term=self.current_term, success=False)
        index = self.last_log.index + 1
        log = LogEntry(
            index=index,
            term=self.current_term,
            command=LogCommand.JOIN,
            args=LogArgs(key="join", args=None, address=addr),
        )
        self.logs.append(log)
        # 确认日志提交，再加入节点
        await self.append_logs(index)
        # 判断是否提交成功
        if self.commit_index >= index:
            peer = Peer(addr, self._loop)
            self.peers.append(peer)
            self.learn_peers.append(peer)
            self.next_index[peer] = self.last_log.index + 1
            self.match_index[peer] = 0
            self.snapshot_index[peer] = 0
            return JoinRes(term=self.current_term, success=True)
        else:
            # 如果没有提交成功，是有可能因为网络原因，也有可能是成功了。
            # 需要等待下次再次尝试。
            return JoinRes(term=self.current_term, success=False)

    async def leave_handler(self, addr: NodeAddress) -> JoinRes | None:
        """
        处理节点离开请求
        """
        logging.info(f"receive leave from {addr}")
        if self.leader_id != self.address:
            return None
        if self.address == addr:
            # 如果是 leader 自己离开，走特殊逻辑
            return await self.leave_myself()
        try:
            peer = self._get_peer(addr)
        except ValueError:
            return JoinRes(term=self.current_term, success=True)
        index = self.last_log.index + 1
        log = LogEntry(
            index=index,
            term=self.current_term,
            command=LogCommand.LEAVE,
            args=LogArgs(key="leave", args=None, address=peer.address),
        )
        self.logs.append(log)
        await self.append_logs(index)
        if self.commit_index >= index:
            self.peers.remove(peer)
            if peer in self.learn_peers:
                self.learn_peers.remove(peer)
            del self.next_index[peer]
            del self.match_index[peer]
            del self.snapshot_index[peer]
            return JoinRes(term=self.current_term, success=True)
        else:
            # 即便是返回失败，也可能会成功
            return JoinRes(term=self.current_term, success=False)

    async def leave_myself(self) -> JoinRes | None:
        """
        处理 leader 节点自己离开
        """
        index = self.last_log.index + 1
        log = LogEntry(
            index=index,
            term=self.current_term,
            command=LogCommand.LEAVE,
            args=LogArgs(key="leave", args=None, address=self.address),
        )
        self.logs.append(log)
        await self.append_logs(index)
        if self.commit_index >= index:
            return JoinRes(term=self.current_term, success=True)
        else:
            return JoinRes(term=self.current_term, success=False)

    # 为了增加可读性，分支和循环都出现在这函数里，便于理解
    async def append_logs(self, watch_index: int = 0) -> None:  # noqa: PLR0912
        """
        Leader 节点向其他节点追加日志
        watch_index: 表示需要关注这个 index 的日志是否已经确认，
        如果已经确认直接返回以减少等待时间，提高效率。

        注意：
        因为追加日志和心跳是两个任务，所以当这两个时间比较靠近的时候，或者网络返回略慢的时候。
        可能会发送两次一模一样的日志，follower 看起来会接收到两次相同的日志。
        """
        if self.role != NodeRole.LEADER:
            return

        self.latlast_heartbeat = self._loop.time()
        tasks: list[asyncio.Task[AppendLogsRes | None]] = []
        for peer, next_index in self.next_index.items():
            # 因为有快照的时候最后第一条是快照的最后一条。
            # 并且 Leader 启动就会有一个 join 日志，所以不会有 0
            # 这里如果next index 不大于 first index 就需要发快照。
            if next_index < self.first_log.index:
                # 发送快照的时候基本注定不会及时确认 watch index 的状态
                # 所以不监控任务状态
                self._loop.create_task(self.send_snapshot(peer))
                continue

            logs = []
            prev_log_index = 0
            prev_log_term = 0
            # 如果 next index 大于 last log index，说明没有日志需要发送
            if next_index > self.last_log.index:
                prev_log_index = self.last_log.index
                prev_log_term = self.last_log.term
            else:
                start_index = next_index - self.last_log.index - 1 + len(self.logs)
                logs = self.logs[
                    start_index : start_index + HEAHEARTBEAT_LOG_BATCH_SIZE
                ]
                # 因为 next_index < self.first_log.index 已经通过快照解决了。
                # 剩下的就是 next_index == self.first_log.index
                # 如果是有快照的时候，first log 就是快照的最后一条，
                # 因为快照的机制是其他节点已经 apply 的日志，
                # 所以 next index 一定大于 first log index，不会用到 first log
                # 如果没有快照，first log 第一条日志，所以 prev log index 是 0
                if next_index > self.first_log.index:
                    prev_log_index = self.logs[
                        next_index - self.last_log.index - 2
                    ].index
                    prev_log_term = self.logs[next_index - self.last_log.index - 2].term
                else:
                    prev_log_index = 0
                    prev_log_term = 0
            tasks.append(
                self._loop.create_task(
                    peer.append_logs(
                        term=self.current_term,
                        leader_id=self.id,
                        prev_log_index=prev_log_index,
                        prev_log_term=prev_log_term,
                        entries=logs,
                        leader_commit_index=self.commit_index,
                    )
                )
            )

        # 等待所有任务完成
        async for result in asyncio.as_completed(tasks):
            result = await result  # noqa: PLW2901
            if not result:
                continue

            # 如果 term 大于当前任期，说明有新的 leader 产生
            if result.term > self.current_term:
                self.role = NodeRole.FOLLOWER
                self.current_term = result.term
                self.voted_for = None
                return
            peer = self._get_peer(result.id)
            if result.success:
                # 如果成功，更新 next index 和 match index
                if peer in self.next_index:
                    self.next_index[peer] = min(
                        result.last_log_index + 1, self.last_log.index + 1
                    )
                    self.match_index[peer] = result.last_log_index
                    self.snapshot_index[peer] = result.snapshot_index
            elif peer in self.next_index:
                # 如果失败，减小 next index
                # 因为 commit index 的特性，直接 commit index + 1 开始，避免多次无效尝试
                self.next_index[peer] = result.commit_index + 1
            # 判断是否需要更新 commit index
            self.update_commit_index()
            # 在 watch index 已经确认的情况下，直接返回。
            # 剩余的没有更新的问题，会在下一个心跳处理。
            if watch_index > 0 and self.commit_index >= watch_index:
                return

        # 只有单节点的时候自己判断是否更新
        self.update_commit_index()

    def update_commit_index(self) -> None:
        if self.leader_id != self.address:
            return
        if not self.logs:
            return
        match_indexes = list(self.match_index.values())
        # 处理只有单节点的时候
        match_indexes.append(self.last_log.index)
        match_indexes.sort(reverse=True)
        match_index = match_indexes[self.quorum - 1]

        if self.commit_index < match_index \
            and self.logs[match_index - self.last_log.index - 1].term \
                == self.current_term:
            # 只提交当前任期的日志，之前的日志也顺带提交了
            self.commit_index = match_index
            logging.info(f"Leader {self.id} update commit index to {self.commit_index}")

    async def send_snapshot(self, peer: Peer) -> None:
        """
        发送快照,learner 和 flollower 都会调用这个方法。
        """
        if not self.snapshot or not self.leader_id:
            return
        logging.info(f"Send snapshot to {peer.id}")

        # 标记快照正在使用，避免被更新、删除
        self.snapshot.is_in_use = True
        offset = 0
        try:
            async for lines in self.snapshot.read(MAX_SNAPSHOT_PAGE_SIZE):
                await peer.install_snapshot(
                    term=self.current_term,
                    leader_id=self.leader_id,
                    last_included_index=self.snapshot.last_included_index,
                    last_included_term=self.snapshot.last_included_term,
                    offset=offset,
                    data=lines,
                    logs=[
                        self.logs[
                            self.snapshot.last_included_index - self.last_log.index - 1
                        ]
                    ],
                    peer_ids=self.snapshot.peer_ids,
                    done=False,
                )
                offset += 1
        except Exception:
            logging.error(f"Error sending snapshot to {peer.id}", exc_info=True)
        finally:
            self.snapshot.is_in_use = False
        # 最后完成发送收尾表示已经完成
        await peer.install_snapshot(
            term=self.current_term,
            leader_id=self.leader_id,
            last_included_index=self.snapshot.last_included_index,
            last_included_term=self.snapshot.last_included_term,
            offset=offset,
            data=[],
            logs=[
                self.logs[self.snapshot.last_included_index - self.last_log.index - 1]
            ],
            peer_ids=self.snapshot.peer_ids,
            done=True,
        )
        logging.info(f"Snapshot sent to {peer.id}")

    async def start(self) -> None:
        """
        启动节点
        """
        if self.join:
            # 不能从快照文件加载数据，可能会丢失。
            # 如果不想要丢失需要自己实现 state_machine 和 snapshot
            await self.start_join()
        else:
            # 如果不是 Join，说明是新集群的第一个，默认是 leader
            await self.start_leader()
        # 启动定时任务
        self._loop.create_task(self.timed_job())

    async def start_leader(self) -> None:
        """
        仅仅处理节点启动就是 leader 的逻辑
        """
        self.role = NodeRole.LEADER
        self.leader_id = self.address
        self.voted_for = None
        self.latlast_heartbeat = self._loop.time()

        # 增加一个自己 join 的日志
        # 避免一些无日志的情况，能让代码判断逻辑更简单
        self.logs.append(
            LogEntry(
                index=1,
                term=self.current_term,
                command=LogCommand.JOIN,
                args=LogArgs(key="join", args=None, address=self.address),
            )
        )

        self.next_index = {peer: self.last_log.index + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self.snapshot_index = {peer: 0 for peer in self.peers}
        logging.info(f"Node {self.id} started as leader")

    async def start_join(self) -> None:
        """
        处理节点加入集群的逻辑
        """
        if not self.join:
            return
        peer = Peer(self.join, self._loop)
        # 向集群中的一个节点发送 join 请求，
        # 就是传入的 join 参数。join 不是leader 也可以重定向。
        result = await peer.join(self.address)  # 等待 join 结果
        if not result or not result.success:
            logging.error(f"Join failed: {result}", exc_info=True)
            sys.exit(1)
        logging.info(f"Node {self.id} joined the cluster")

    async def timed_job(self) -> None:
        """
        定时任务处理
        """
        if self.role == NodeRole.LEARNER:
            # 如果是 learner，说明是新加入的节点，只需要等待 leader 的日志无其他逻辑。
            await asyncio.sleep(HEAHEARTBEAT_INTERVAL)
            self._loop.create_task(self.timed_job())
            return

        try:
            # 检查 leader 是否存活，如果超时则发起选举
            await self.election_job()
        except Exception:
            logging.error("Error in timed election job", exc_info=True)

        try:
            # 给其他节点追加日志，也是发送心跳
            await self.append_logs()
        except Exception:
            logging.error("Error in timed heartbeat job", exc_info=True)

        try:
            # 应用日志到状态机
            await self.apply_logs_job()
        except Exception:
            logging.error("Error in timed apply logs job", exc_info=True)

        try:
            # leader 节点用来判断 learner 是否已经追赶上来
            self.learn_peers_job()
        except Exception:
            logging.error("Error in timed learn peers job", exc_info=True)

        try:
            # 因为快照任务 follow 也会有可以取整创建快照
            # Leader 也会有创建快照的任务是根据所有 peer 的快照进度来取整的
            # 所以统一在定时任务里处理
            await self.snapshot_job()
        except Exception:
            logging.error("Error in timed snapshot job", exc_info=True)

        await asyncio.sleep(HEAHEARTBEAT_INTERVAL)
        # 递归调用，继续下一个定时任务，不用 await 是为了不阻塞也不会堆栈溢出
        self._loop.create_task(self.timed_job())

    async def election_job(self) -> None:
        # 随机一个选举超时时间，此次判断都是用同一个随机时间。
        # 随机时间是为了避免多个节点同时发起选举。
        delay = round(random.uniform(ELECTION_TIMEOUT, ELECTION_TIMEOUT * 2), 3)  # noqa: S311
        if self._election_pre_conditions_check(delay):
            self._loop.create_task(self.start_election(delay))

    async def apply_logs_job(self) -> None:
        """
        应用日志到状态机
        """
        while self.last_applied < self.commit_index:
            try:
                entry = self.logs[self.last_applied + 1 - self.last_log.index - 1]
                if not entry:
                    break
                if entry.command in [LogCommand.JOIN, LogCommand.LEAVE]:
                    self.apply_membership_change(entry)
                else:
                    self.state_machine.apply_log(entry)
                self.last_applied += 1

            except Exception:
                logging.error(
                    f"Error applying log entry {self.last_applied}",
                    exc_info=True,
                )

    def apply_membership_change(self, log: LogEntry) -> None:
        """
        应用集群成员变更
        """
        if not log.args.address:
            return
        logging.info(
            f"Applying membership change: {log.command.name} {log.args.address}"
        )
        if log.command == LogCommand.JOIN:
            if log.args.address == self.address:
                return
            for peer in self.peers:
                if peer.address == log.args.address:
                    return
            peer = Peer(log.args.address, self._loop)
            self.peers.append(peer)
            self.next_index[peer] = (
                self.last_log.index + 1 if self.role == NodeRole.LEADER else 0
            )
            self.match_index[peer] = 0
            self.snapshot_index[peer] = 0

        elif log.command == LogCommand.LEAVE:
            if log.args.address == self.address:
                # 如果是自己离开，直接结束进程
                logging.info(f"Node {self.id} leaving the cluster")
                os._exit(0)  # pyright: ignore
            for peer in self.peers:
                if peer.address == log.args.address:
                    self.peers.remove(peer)
                    del self.next_index[peer]
                    del self.match_index[peer]
                    del self.snapshot_index[peer]
                    return

    def learn_peers_job(self) -> None:
        """
        Leader 节点用来判断 learner 是否已经追赶上来
        """
        if self.role != NodeRole.LEADER:
            return
        if not self.learn_peers:
            return
        for peer in self.learn_peers:
            if (
                self.match_index[peer]
                >= self.commit_index - HEAHEARTBEAT_LOG_BATCH_SIZE
            ):
                # 只要日志的在一次心跳的日志数量内，就认为已经追赶上来
                logging.info(f"Node {self.id} learned {peer.id}")
                self.learn_peers.remove(peer)

    def _snapshot_target_index(self) -> int:
        """
        计算要快照目标 index
        """
        snapshot_target_index = 0
        if self.role == NodeRole.LEADER:
            # Leader 节点的快照的 index 是所有 peer 快照 index 的最小值
            snapshot_indexes = list(self.snapshot_index.values())
            if snapshot_indexes:
                snapshot_target_index = min(snapshot_indexes)
            else:
                snapshot_target_index = self.last_applied
        else:
            # Follower 节点的快照的 index 是已经应用的日志 index
            snapshot_target_index = self.last_applied

        return snapshot_target_index

    def _snapshot_job_pre_conditions_check(self, snapshot_target_index: int) -> bool:
        """
        快照任务的前置条件检查
        """
        if snapshot_target_index <= 0:
            return False
        # 如果已经有快照了，就不需要再生成快照
        if self.snapshot and self.snapshot.last_included_index >= snapshot_target_index:
            return False
        # 如果快照正在使用，就不需要再生成快照
        if self.snapshot and self.snapshot.is_in_use:
            return False

        # 只有 apply 到正好是 HEAHEARTBEAT_LOG_BATCH_SIZE 的倍数的时候才有机会生成快照
        return snapshot_target_index % HEAHEARTBEAT_LOG_BATCH_SIZE == 0

    async def snapshot_job(self) -> None:
        """
        生成快照任务，因为 fork 一个子进程，fork 比较耗时，
        所以短时间内大量日志可能会触发多次顺序快照。无影响
        """
        if self.is_snapshotting:
            return
        snapshot_target_index = self._snapshot_target_index()
        if not self._snapshot_job_pre_conditions_check(snapshot_target_index):
            return
        self.is_snapshotting = True
        # 这里直接返回，是为了父进程不阻塞，子进程继续处理，不影响阻塞定时任务
        self._loop.create_task(self._snapshot_job(snapshot_target_index))

    async def _snapshot_job(self, snapshot_target_index: int) -> None:
        """
        生成快照任务
        """
        snapshot = Snapshot(
            id=uuid4(),  # 快照 id，为了避免文件名重复
            last_included_index=snapshot_target_index,
            last_included_term=self.logs[
                snapshot_target_index - self.last_log.index - 1
            ].term,
            dir=self._snapshot_path,
            loop=self._loop,
        )

        # 创建管道，用于子进程和父进程通信
        read_fd, write_fd = os.pipe()
        # fork 一个子进程，子进程处理快照任务，父进程等待子进程完成
        pid = os.fork()
        if pid == 0:
            # 子进程全部阻塞函数操作，
            # 而且 Python机制会自动关闭文件描述符和loop，所以没有其他任务执行。
            os.close(read_fd)
            logging.info(
                f"Node {self.id} start snapshot for index {snapshot_target_index}"
            )
            result = self._snapshot_job_child(snapshot, write_fd)
            logging.info(
                f"Node {self.id} snapshot child process "
                f"{'done' if result else 'failed'} and exit"
            )
            os._exit(0)  # pyright: ignore
        else:
            # 父进程等待子进程完成，
            # Linux 和 Python的机制，虽然 fork 也不会分配新的 HTTP 请求给子进程处理。
            os.close(write_fd)
            result = await self._snapshot_job_parent(snapshot, read_fd, pid)
            status = "done" if result else "failed"
            logging.info(
                f"Node {self.id} snapshot update {status} "
                f"for index {snapshot_target_index}"
            )

    def _snapshot_job_child(self, snapshot: Snapshot, write_fd: int) -> bool:
        """
        子进程处理快照任务
        """
        msg = SNAPSHOTTING_FAILED
        w = os.fdopen(write_fd, "w")
        try:
            # 写入快照文件
            snapshot.write(self.state_machine)
            msg = SNAPSHOTTING_SUCCESS
        except Exception:
            logging.error(
                f"Error creating snapshot for index {snapshot.last_included_index}",
                exc_info=True,
            )
        finally:
            # 发送消息给父进程
            w.write(msg)
            w.flush()
            w.close()
        return msg == SNAPSHOTTING_SUCCESS

    async def _snapshot_job_parent(
        self, snapshot: Snapshot, read_fd: int, child_pid: int
    ) -> bool:
        """
        父进程快照任务，等待子进程完成，更新快照
        """
        r = os.fdopen(read_fd, "r")
        # 等待子进程完成，防止子进程出问题收不到消息，先等待子进程完成
        await self._loop.run_in_executor(None, os.waitpid, child_pid, 0)
        # 读取子进程的消息
        result = await self._loop.run_in_executor(None, r.read, 1)
        if result != SNAPSHOTTING_SUCCESS:
            logging.error(f"Node {self.id} snapshot process failed")
            return False
        while True:
            if self.snapshot and self.snapshot.is_in_use:
                # 老的如果快照正在使用，等待快照完成
                await asyncio.sleep(HEAHEARTBEAT_INTERVAL)
                continue
            try:
                old_snapshot = self.snapshot
                self.snapshot = snapshot
                peer_ids = [peer.id for peer in self.peers]
                peer_ids.append(self.id)

                if old_snapshot:
                    old_snapshot.clear()
            except Exception:
                logging.error(
                    f"Error updating snapshot for index {snapshot.last_included_index}",
                    exc_info=True,
                )
            self.is_snapshotting = False  # 更新快照完成
            self.snapshot = snapshot
            break
        return True

    def _election_pre_conditions_check(self, delay: float) -> bool:
        """
        选举任务的前置条件检查
        因为选举是 async 操作，中间随时可能有新的 leader 已经产生并给自己发了心跳。
        所以需要多次前置条件检查，避免重复发起选举。
        """
        if self.role == NodeRole.LEADER:
            return False
        return self.latlast_heartbeat + delay <= self._loop.time()

    async def _prevote_check(self) -> bool:
        """
        预投票检查，
        主要为了解决网络分区问题：
        当单个节点断网，它发起选举造成 term 一直增加，
        当网络恢复时，会因为 term 过大，造成集群重新选举。
        只有预投票成功才会发起真正的投票。
        并且不管是发送端还是接收端，都不会实际增加 term。
        """
        logging.info(f"Node {self.id} start prevote")
        # 这里的 term 不实际增加。
        votes = 1
        tasks: list[Coroutine[Any, Any, VoteRes | None]] = []
        tasks.extend(
            peer.prevote(
                self.current_term + 1,  # 预投票的 term 是当前 term + 1，不实际增加
                self.id,
                self.last_log.index,
                self.last_log.term,
            )
            for peer in self.peers
        )

        async for result in asyncio.as_completed(tasks):
            # 返回的结果仅仅用于投票结果变更实际逻辑。
            result = await result  # noqa: PLW2901
            if result and result.vote_granted:
                votes += 1
            if result and result.term > self.current_term + 1:
                return False
            if votes >= self.quorum:
                return True
        return votes >= self.quorum

    async def start_election(self, delay: float) -> None:
        """
        发起真正的选举
        """
        # 通过预投票之后才可以发起真正的投票
        if not await self._prevote_check():
            return
        # 因为预投票也是网络，async 操作，所以需要再次检查
        if not self._election_pre_conditions_check(delay):
            return

        if self.role == NodeRole.FOLLOWER:
            self.role = NodeRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        logging.info(f"Node {self.id} start election for term {self.current_term}")

        tasks: list[Coroutine[Any, Any, VoteRes | None]] = []
        tasks.extend(
            peer.vote(
                self.current_term,
                self.id,
                self.last_log.index,
                self.last_log.term,
            )
            for peer in self.peers
        )

        votes = 1
        max_term = 0  # 0 的时候是投票失败，等待下一轮。>0 的时候说明需要转换成 Follower
        results = await asyncio.gather(*tasks)
        # 投票请求是异步的，所以需要再次检查
        if not self._election_pre_conditions_check(delay):
            return

        for r in results:
            if not r:
                continue
            if r.term > self.current_term:
                # 说明有 term 大于自己的，投票直接失败，变成 follower
                max_term = max(r.term, max_term)
                continue  # 继续寻找是否有更大的 term
            if r.vote_granted:
                votes += 1
        if max_term > 0:
            self.candidate_to_follower(max_term)
            return

        if votes >= self.quorum:
            # 选举成功
            self.candidate_to_leader()
        else:
            # 等待重新发起投票
            self.latlast_heartbeat = self._loop.time()

    def candidate_to_leader(self) -> None:
        """
        选举成功，变成 Leader
        """
        self.role = NodeRole.LEADER
        self.leader_id = self.address
        self.voted_for = None
        self.latlast_heartbeat = self._loop.time()
        self.next_index = {peer: self.last_log.index + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self.snapshot_index = {peer: 0 for peer in self.peers}
        self._loop.create_task(self.append_logs())  # 立即发送心跳
        logging.info(f"Node {self.id} elected as leader for term {self.current_term}")

    def candidate_to_follower(self, term: int) -> None:
        """
        选举失败，变成 Follower
        """
        self.role = NodeRole.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.latlast_heartbeat = self._loop.time()
        logging.info(f"Node {self.id} become follower for term {self.current_term}")
