import logging

from aiohttp import web

from pyraft.node import Node
from pyraft.vo import (
    ADDRESS_PARTS_COUNT,
    AppendLogsReq,
    InstallSnapshotReq,
    KeyOp,
    NodeAddress,
    NodeRole,
    VoteReq,
)


class HTTPGateway:
    """
    HTTP 网关类，用于处理所有HTTP请求并与 Raft 节点进行交互。
    该类作为 HTTP 请求的入口点，负责处理各种 HTTP 请求并将其转换为适当的 Raft 节点操作。
    确保 Raft 节点的业务逻辑与HTTP处理逻辑的分离。
    """
    def __init__(self, node: Node) -> None:
        self.node = node

    async def home_handler(self, _: web.Request) -> web.Response:
        """
        处理首页请求并返回节点状态信息的处理函数
        此处理函数返回一个包含当前节点详细状态信息的响应,包括:
        - 节点ID
        - 节点角色
        - 当前任期
        - 提交索引
        - 最新日志索引和任期
        - 最后应用的索引
        - 日志长度
        - 快照索引
        - 法定人数
        - 对等节点信息(包括next_index、match_index和snapshot_index)
        - 学习节点列表
        - 状态机中的所有键值对数据
        同时还提供了一些API使用示例
        """
        text = f"""Welcome PyRaft

Node ID:        {self.node.id}
Role:           {self.node.role}
Current Term:   {self.node.current_term}
Commit Index:   {self.node.commit_index}
Last Log Index: {self.node.last_log.index}
Last Log Term:  {self.node.last_log.term}
Last Applied:   {self.node.last_applied}
Log Length:     {len(self.node.logs)}
Snapshot Index: {(self.node.snapshot.last_included_index
                 if self.node.snapshot else None)}
Quorum:         {self.node.quorum}
Peers:          {[
    str(i) + '(next index: ' + str(self.node.next_index[i]) +
    ', match index: ' + str(self.node.match_index[i]) +
    ', snapshot index: ' + str(self.node.snapshot_index[i]) + ')' \
        for i in self.node.peers]}
Learn Peers:    {[str(i) for i in self.node.learn_peers]}

----------------------------------------
API Usage:

1. Key-Value Store:
SET key1: `curl -i -X "PUT" http://\
{self.node.leader_id.host if self.node.leader_id else '<leader_host_ip'}:\
{self.node.leader_id.port if self.node.leader_id else '<leader_host_port'}\
/keyx -H "Content-Type: application/json" -d '{{"key": "value"}}'`
GET key1: `curl -i -X "GET" http://\
{self.node.leader_id.host if self.node.leader_id else '<leader_host_ip'}:\
{self.node.leader_id.port if self.node.leader_id else '<leader_host_port'}/keyx`
DEL key1: `curl -i -X "DELETE" http://\
{self.node.leader_id.host if self.node.leader_id else '<leader_host_ip'}:\
{self.node.leader_id.port if self.node.leader_id else '<leader_host_port'}/keyx`

2. Node Status:
Node Status: `curl -i -X "GET" http://{self.node.address.host}:{self.node.address.port}/`

3. Cluster Management:
Join Cluster:   `python main.py --host x.x.x.x --port xxxx --join \
{self.node.leader_id.host if self.node.leader_id else '<leader_host_ip'}:\
{self.node.leader_id.port if self.node.leader_id else '<leader_host_port'}`
Leave Cluster:  `curl -i -X "DELETE" http://\
{self.node.leader_id.host if self.node.leader_id else '<leader_host_ip'}:\
{self.node.leader_id.port if self.node.leader_id else '<leader_host_port'}\
/__pyraft/peer/<ip:port>`

----------------------------------------
Data in State Machine:
"""
        for key in self.node.get_state_machine_data().items():
            text += f"{key[0]}:" "\t\t" f"{key[1]}\n"
        return web.Response(text=text)

    async def get_handler(self, request: web.Request) -> web.Response:
        """
        处理HTTP GET请求的处理函数
        从请求中获取key参数并返回对应的值。如果节点不是leader，则重定向到leader节点。
        """

        key = request.match_info.get("key")
        if not key:
            raise web.HTTPNotFound

        if not self.node.leader_id:
            return web.HTTPInternalServerError(text="No leader")
        if self.node.role != NodeRole.LEADER:
            return web.HTTPFound(
                f"http://{self.node.leader_id.host}:{self.node.leader_id.port}/{key}"
            )

        result = self.node.get_key_handler(key)
        if result is None:
            return web.HTTPNotFound()
        if isinstance(result, dict):
            return web.json_response(result)
        return web.Response(text=result)

    async def _key_handler(self, request: web.Request, op: KeyOp) -> web.Response:
        """
        实际上处理HTTP PUT和DELETE请求的处理函数
        """
        key = request.match_info.get("key")
        if not key:
            raise web.HTTPNotFound

        if not self.node.leader_id:
            return web.HTTPInternalServerError(text="No leader")
        if self.node.role != NodeRole.LEADER:
            return web.HTTPFound(
                f"http://{self.node.leader_id.host}:{self.node.leader_id.port}/{key}"
            )

        data = None
        if op == KeyOp.SET and request.content_type == "application/json":
            data = await request.json()
        elif op == KeyOp.SET:
            data = await request.text()
        if op == KeyOp.SET and not data:
            raise web.HTTPBadRequest

        result = await self.node.key_handler(op, key, data)
        if not result:
            if self.node.leader_id:
                return web.HTTPFound(
                    f"http://{self.node.leader_id.host}:{self.node.leader_id.port}/{key}"
                )
            return web.HTTPInternalServerError(text="No leader")
        return web.json_response(result)

    async def put_handler(self, request: web.Request) -> web.Response:
        """
        处理HTTP PUT请求的处理函数
        """
        return await self._key_handler(request, KeyOp.SET)

    async def delete_handler(self, request: web.Request) -> web.Response:
        """
        处理HTTP DELETE请求的处理函数
        """
        return await self._key_handler(request, KeyOp.DEL)

    async def prevote(self, request: web.Request) -> web.Response:
        """
        处理预投票请求，防止网络分区的情况下，节触发选举造成 term 增加，
        网络恢复后 term 引起重发选举。
        """
        logging.info("Received prevote request. data: %s", await request.text())
        try:
            req = VoteReq.from_dict(await request.json())
            result = await self.node.prevote_handler(req)
            return web.json_response(result.to_dict())
        except Exception as e:
            logging.exception("Error handling prevote request")
            raise web.HTTPInternalServerError from e

    async def vote(self, request: web.Request) -> web.Response:
        """
        处理投票请求
        """
        try:
            req = VoteReq.from_dict(await request.json())
            result = await self.node.vote_handler(req)
            return web.json_response(result.to_dict())
        except Exception as e:
            logging.exception("Error handling prevote request")
            raise web.HTTPInternalServerError from e

    async def append_logs(self, request: web.Request) -> web.Response:
        """
        处理追加日志请求，同时也是心跳请求
        """
        try:
            req = AppendLogsReq.from_dict(await request.json())
            result = await self.node.append_logs_handler(req)
            return web.json_response(result.to_dict())
        except Exception as e:
            logging.exception(
                f"Error parsing heartbeat request data {await request.text()}"
            )
            raise web.HTTPInternalServerError from e

    async def join(self, request: web.Request) -> web.Response:
        """
        节点加入集群请求
        """
        address = request.match_info["address"]
        address = address.strip()
        if not self.node.leader_id:
            raise web.HTTPInternalServerError(text="No leader")
        if self.node.leader_id != self.node.address:
            logging.info(f"Redirecting join request to leader: {self.node.leader_id}")
            return web.HTTPFound(
                f"http://{self.node.leader_id.host}:{self.node.leader_id.port}/__pyraft/peer/{address}"
            )
        try:
            if not address or len(address.split(":")) != ADDRESS_PARTS_COUNT:
                raise web.HTTPNotFound(text=f"Address mismatch: {address}")
            address = address.split(":")
            addr = NodeAddress(host=address[0], port=int(address[1]))
            result = await self.node.join_handler(addr)
            if not result:
                return web.HTTPFound(
                    f"http://{self.node.leader_id.host}:{self.node.leader_id.port}/__pyraft/peer/{address}"
                )
            return web.json_response(result.to_dict())
        except Exception as e:
            logging.exception("Error handling join request")
            raise web.HTTPInternalServerError from e

    async def leave(self, request: web.Request) -> web.Response:
        """
        节点离开集群请求
        """
        address = request.match_info["address"]
        if not self.node.leader_id:
            raise web.HTTPInternalServerError(text="No leader")
        if self.node.leader_id != self.node.address:
            return web.HTTPFound(
                f"http://{self.node.leader_id.host}:{self.node.leader_id.port}/__pyraft/peer/{address}"
            )
        try:
            if not address or len(address.split(":")) != ADDRESS_PARTS_COUNT:
                raise web.HTTPNotFound(text=f"Address mismatch: {address}")
            address = address.split(":")
            addr = NodeAddress(host=address[0], port=int(address[1]))

            result = await self.node.leave_handler(addr)
            if not result:
                return web.HTTPFound(
                    f"http://{self.node.leader_id.host}:{self.node.leader_id.port}/__pyraft/peer/{address}"
                )
            return web.json_response(result.to_dict())
        except Exception as e:
            logging.exception("Error handling leave request")
            raise web.HTTPInternalServerError from e

    async def snapshot(self, request: web.Request) -> web.Response:
        """
        安装快照请求
        """
        try:
            req = InstallSnapshotReq.from_dict(await request.json())
            result = await self.node.install_snapshot_handler(req)
            return web.json_response(result.to_dict())
        except Exception as e:
            logging.exception("Error handling join request")
            raise web.HTTPInternalServerError from e
