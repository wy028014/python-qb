from socket import AF_INET, getaddrinfo, gethostname
from typing import List
from utils.utils_datetime import DateTime


class Socket():

    def get_local_ips(self) -> List[str]:
        dt = DateTime()
        ips: set = set()
        try:
            for res in getaddrinfo(gethostname(), None):
                af, socktype, proto, canonname, sa = res
                if af == AF_INET:
                    ips.add(sa[0])
            print(f"{dt.get_now()} | 获取本地ips成功: {ips}")
            return ips
        except Exception as e:
            print(f"{dt.get_now()} | 获取本地ips失败: {e}")
            return ['127.0.0.1']
