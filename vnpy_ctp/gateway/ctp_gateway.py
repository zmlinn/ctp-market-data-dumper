import sys
from datetime import datetime

from ..api import (
    MdApi
)

MAX_FLOAT = sys.float_info.max  # 浮点数极限值


class CtpGateway():
    """
    VeighNa用于对接期货CTP柜台的交易接口。
    """

    default_name: str = "CTP"

    default_setting: dict[str, str] = {
        "用户名": "",
        "密码": "",
        "经纪商代码": "",
        "交易服务器": "",
        "行情服务器": "",
        "产品名称": "",
        "授权编码": ""
    }

    def __init__(self) -> None:
        """构造函数"""
        self.md_api: "CtpMdApi" = CtpMdApi(self)

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        userid: str = setting["用户名"]
        password: str = setting["密码"]
        brokerid: str = setting["经纪商代码"]
        td_address: str = setting["交易服务器"]
        md_address: str = setting["行情服务器"]
        appid: str = setting["产品名称"]
        auth_code: str = setting["授权编码"]

        if (
                (not td_address.startswith("tcp://"))
                and (not td_address.startswith("ssl://"))
                and (not td_address.startswith("socks"))
        ):
            td_address = "tcp://" + td_address

        if (
                (not md_address.startswith("tcp://"))
                and (not md_address.startswith("ssl://"))
                and (not md_address.startswith("socks"))
        ):
            md_address = "tcp://" + md_address

        self.md_api.connect(md_address, userid, password, brokerid)

    def subscribe(self, symbol: str) -> None:
        """订阅行情"""
        self.md_api.subscribe(symbol)

    def close(self) -> None:
        """关闭接口"""
        self.md_api.close()

    def write_log(self, msg):
        print(msg)

    def write_error(self, msg, error):
        print(msg, error)


class CtpMdApi(MdApi):
    """"""

    def __init__(self, gateway: CtpGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: CtpGateway = gateway
        self.reqid: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.subscribed: set = set()

        self.userid: str = ""
        self.password: str = ""
        self.brokerid: str = ""

        self.current_date: str = datetime.now().strftime("%Y%m%d")

    def onFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("行情服务器连接成功")
        self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(f"行情服务器连接断开，原因{reason}")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户登录请求回报"""
        if not error["ErrorID"]:
            self.login_status = True
            self.gateway.write_log("行情服务器登录成功")

            for symbol in self.subscribed:
                self.subscribeMarketData(symbol)
        else:
            self.gateway.write_error("行情服务器登录失败", error)

    def onRspError(self, error: dict, reqid: int, last: bool) -> None:
        """请求报错回报"""
        self.gateway.write_error("行情接口报错", error)

    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """订阅行情回报"""
        if not error or not error["ErrorID"]:
            return

        self.gateway.write_error("行情订阅失败", error)

    def onRtnDepthMarketData(self, data: dict) -> None:
        """行情数据推送"""
        # 过滤没有时间戳的异常行情数据
        if not data["UpdateTime"]:
            return

        print(data)

    def connect(self, address: str, userid: str, password: str, brokerid: str) -> None:
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.brokerid = brokerid

        # 禁止重复发起连接，会导致异常崩溃
        if not self.connect_status:
            self.createFtdcMdApi((str('/tmp/') + "\\Md").encode("GBK"))

            self.registerFront(address)
            self.init()

            self.connect_status = True

    def login(self) -> None:
        """用户登录"""
        ctp_req: dict = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid
        }

        self.reqid += 1
        self.reqUserLogin(ctp_req, self.reqid)

    def subscribe(self, symbol: str) -> None:
        """订阅行情"""
        if self.login_status:
            self.subscribeMarketData(symbol)
        self.subscribed.add(symbol)

    def close(self) -> None:
        """关闭连接"""
        if self.connect_status:
            self.exit()

    def update_date(self) -> None:
        """更新当前日期"""
        self.current_date = datetime.now().strftime("%Y%m%d")


def adjust_price(price: float) -> float:
    """将异常的浮点数最大值（MAX_FLOAT）数据调整为0"""
    if price == MAX_FLOAT:
        price = 0
    return price
