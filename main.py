import argparse
import asyncio
import logging

from aiohttp import web

from pyraft.http import HTTPGateway
from pyraft.node import Node
from pyraft.state_machine import StateMachine
from pyraft.vo import NodeAddress

arg_parser = argparse.ArgumentParser(
    prog="PyRaft",
    description=("PyRaft - A Python implementation of the Raft consensus algorithm"),
    epilog="Find more information at: https://github.com/yibuma/pyraft",
)

arg_parser.add_argument(
    "--port",
    type=int,
    default=8000,
    help="Port number to run the server on. Example: --port 5000",
)

arg_parser.add_argument(
    "--host",
    type=str,
    default="localhost",
    help="Host address to bind the server to. Example: --host localhost",
)

arg_parser.add_argument(
    "--join",
    type=str,
    default="",
    help="To join an existing cluster? Specify the host:port of any member "
    "in the cluster. Example: --join localhost:5001",
)

arg_parser.add_argument(
    "--snapshot",
    type=str,
    default="./",
    help=(
        "Snapshots Path to snapshot file to load/save state. " "Example: --snapshot ./"
    ),
)

def argv_parser() -> tuple[NodeAddress, NodeAddress | None, str]:
    """
    解析命令行参数
    """
    args = arg_parser.parse_args()
    address = NodeAddress(args.host, args.port)
    join = None
    if args.join:
        host, port = args.join.split(":")
        if not host:
            raise ValueError("Join host cannot be empty")
        if not port.isnumeric():
            raise ValueError("Join port must be a number")
        port = int(port)
        if not port:
            raise ValueError("Port cannot be 0")
        join = NodeAddress(host, port)
    # 确保 snapshot_path 以 / 结尾
    snapshot_path = args.snapshot.rstrip("/") + "/"
    return address, join, snapshot_path


async def start_server(address: NodeAddress, node: Node) -> web.Application:
    """
    在事件循环中启动服务器
    """
    app = web.Application()
    http_gateway = HTTPGateway(node)
    app.add_routes(
        [
            web.get("/", http_gateway.home_handler),  # 节点状态
            web.put("/__pyraft/prevote", http_gateway.prevote),  # 预投票
            web.put("/__pyraft/vote", http_gateway.vote),  # 投票
            web.put("/__pyraft/logs", http_gateway.append_logs),  # 追加日志
            web.put("/__pyraft/peer/{address}", http_gateway.join),  # 加入集群
            web.delete("/__pyraft/peer/{address}", http_gateway.leave),  # 离开集群
            web.put("/__pyraft/snapshot", http_gateway.snapshot),  # 安装快照
            web.get("/{key}", http_gateway.get_handler),  # 获取数据
            web.put("/{key}", http_gateway.put_handler),  # 设置数据
            web.delete("/{key}", http_gateway.delete_handler),  # 删除数据
        ]
    )
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, address.host, address.port)
    await site.start()
    return app


async def start_node(
    address: NodeAddress,
    join: NodeAddress | None,
    snapshot_path: str,
    loop: asyncio.AbstractEventLoop,
) -> Node:
    """
    在事件循环中启动 node
    """
    if address == join:
        join = None
    state_machine = StateMachine()
    node = Node(address, join, snapshot_path, state_machine, loop)
    await node.start()
    return node


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.new_event_loop()  # 使用同一个异步事件循环
    asyncio.set_event_loop(loop)  # 设置为默认事件循环
    address, join, snapshot_path = argv_parser()
    # 启动节点
    node = loop.run_until_complete(start_node(address, join, snapshot_path, loop))
    # 启动服务器
    loop.run_until_complete(start_server(address, node))
    # 运行事件循环
    loop.run_forever()
