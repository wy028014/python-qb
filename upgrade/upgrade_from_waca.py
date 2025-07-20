from json import loads
from typing import List
from tqdm import tqdm
from requests import Response, post
from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime


class UpgradeFromWaca:
    def __init__(self) -> None:
        self.db: MariaDB = MariaDB(host=239, database="情报", print=False)
        self.db.connect()
        self.dt: DateTime = DateTime()

    def getData(self, data: object) -> list:
        """获取数字证书的数据信息

        :param data: 站站查询的请求参数
        :type data: object
        :return: 数据列表
        :rtype: list
        """
        url: str = f"http://10.3.32.60:2326/certificate/cyber/zzcx"
        headers: dict[str, str] = {
            f"accept": f"*/*",
            f"Content-Type": f"application/json",
        }
        response: Response = post(url, json=data, headers=headers)
        return loads(response.text)["data"]["data"]

    def getK546K547(self, date_start: str) -> List[object]:
        """获取成都->佳木斯的K546&K547次旅客列车的车站信息

        :param date_start: 始发车的日期
        :type date_start: str
        :return: 车站信息列表
        :rtype: List[object]
        """
        the1day: str = date_start
        the2day: str = self.dt.get_tomorrow(date=date_start, format=f"%Y-%m-%d")
        the3day: str = self.dt.get_tomorrow(date=the2day, format=f"%Y-%m-%d")
        return [
            {"train_date": f"{the1day}", "board_train_code": f"K546", "fromStation": f"宝鸡", "toStation": "" },
            {"train_date": f"{the1day}", "board_train_code": f"K546", "fromStation": f"杨陵", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"西安", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"延安", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"吴堡", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"吕梁", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"汾阳", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"太原", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"阳泉北", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"石家庄北", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"保定", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"涿州", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K546", "fromStation": f"北京西", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K547", "fromStation": f"北京", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K547", "fromStation": f"蓟州", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K547", "fromStation": f"唐山北", "toStation": "" },
            {"train_date": f"{the2day}", "board_train_code": f"K547", "fromStation": f"山海关", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"沈阳北", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"公主岭", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"长春", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"哈尔滨", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"哈尔滨东", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"绥化", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"庆安", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"铁力", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"朗乡", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"南岔", "toStation": "" },
            {"train_date": f"{the3day}", "board_train_code": f"K547", "fromStation": f"汤原", "toStation": "" },
        ]

    def jms(self, date_start: str) -> List[object]:
        return [
            {"train_date": f"{date_start}", "board_train_code": f"K7066", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K7205", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K5101", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7812", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"K1393","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"K7106", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7902", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"K629","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7904", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K554", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"C101", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7920", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"4016", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7916", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"6208", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7810", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7960", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"G714", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"C102", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"K1227","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7918", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D7811","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7802", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D532", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"6215","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"G938", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"4172", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7814", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"C103", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"G1238", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"C104", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7928", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"6272", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7924", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7906", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D7813","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7980", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D7815","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7816", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K630", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"G3642", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"4017","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7818", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D7833","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"G774", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K548", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7834", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"C106", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D7967","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D7817","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"K5157", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7966", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7820", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D7819","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7822", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"K5158","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"K7126", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7938", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K5120", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"6222","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D561","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7954", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"G771","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"6221", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"G772", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"K349","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D7821","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7926", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"C107", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D562", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7824", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K350", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"K7125","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7932", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"C109", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7948", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D7801","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"K553","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"4171", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"K5119","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D7835","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7826", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"C110","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"6216", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"C693", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7934", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"6271","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D7959","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D7823","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7936", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"K547","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"K1228", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"6207","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7828", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"C112", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K1394", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D521","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"C111", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7922", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D522", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D7825","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"C694","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7982", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"4018", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7836", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"G3644", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"G1240", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D7827","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D7829","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"4019","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"C114","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D7946","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"G776", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"G717","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D7831","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7988", "fromStation": f"佳木斯", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"G949","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"K7105","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D7905","fromStation": f"佳木斯","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"D531","fromStation": f"佳木斯","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"K7206", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K5102", "fromStation": f"佳木斯", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K7065", "fromStation": f"佳木斯", "toStation": "" },
        ]

    def hn(self, date_start: str) -> List[object]:
        return [
            {"train_date": f"{date_start}", "board_train_code": f"6215", "fromStation": f"桦南", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"4016","fromStation": f"桦南","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"G714", "fromStation": f"桦南东", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"4017", "fromStation": f"桦南", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7981", "fromStation": f"桦南东", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"4172", "fromStation": f"桦南", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"D7924", "fromStation": f"桦南东", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"4171", "fromStation": f"桦南", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"K5120","fromStation": f"桦南","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"D7933", "fromStation": f"桦南东", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"K5119", "fromStation": f"桦南", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"D7929","fromStation": f"桦南东","toStation": ""},
            {"train_date": f"{date_start}", "board_train_code": f"6216", "fromStation": f"桦南", "toStation": "" },
            {"train_date": f"{date_start}", "board_train_code": f"4019", "fromStation": f"桦南", "toStation": "" },
            # {"train_date": f"{date_start}","board_train_code": f"4018","fromStation": f"桦南","toStation": ""},
        ]

    def yc(self, date_start: str) -> List[object]:
        return [
            {"train_date": f"{date_start}","board_train_code": f"K7137","fromStation": f"伊春","toStation": ""},
            # {"train_date": f"{date_start}","board_train_code": f"K1021","fromStation": f"伊春","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"K7128","fromStation": f"伊春","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"6274","fromStation": f"伊春","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"6273","fromStation": f"伊春","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"K7138","fromStation": f"伊春","toStation": ""},
        ]

    def hg(self, date_start: str) -> List[object]:
        return [
            {"train_date": f"{date_start}","board_train_code": f"C102","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"4172","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C101","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C104","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C103","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C106","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C105","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C870","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C869","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C108","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C107","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C110","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C109","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C112","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"4171","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C693","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C694","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C111","fromStation": f"鹤岗","toStation": ""},
            {"train_date": f"{date_start}","board_train_code": f"C114","fromStation": f"鹤岗","toStation": ""},
        ]

    def useZZ(self, date_start: str, date_end: str, train_code: str) -> None:
        date_list: List[str] = self.dt.get_dates_between(
            date_start=date_start, date_end=date_end
        )
        for date in date_list:
            # station_list: List[object] = self.getK546K547(date_start=date)
            for i in range(4):
                if i == 0:
                    station_list: List[object] = self.jms(date_start=date)
                elif i == 1:
                    station_list: List[object] = self.hn(date_start=date)
                elif i == 2:
                    station_list: List[object] = self.yc(date_start=date)
                elif i == 3:
                    station_list: List[object] = self.hg(date_start=date)
                for station in tqdm(station_list, desc=f"{date} {station_list[0]['fromStation']} 车站列表", unit=f"个"):
                    self.dt.sleep_random(1, 3)
                    rows: list = self.getData(data=station)
                    for row in rows:
                        sql_query: str = f"""
                        SELECT id
                        FROM `乘车数据_数据表`
                        WHERE `乘车日期` = '{row['乘车日期']}'
                        AND `车次` = '{row['车次']}'
                        AND `发站` = '{row['发站']}'
                        AND `到站` = '{row['到站']}'
                        AND `证件编号` = '{row['证件编号']}'
                        AND `车厢号` = '{row['车厢号']}'
                        AND `座位号` = '{row['座位号']}'
                        AND `售票处` IS NULL
                        """
                        flag_query, res_query = self.db.query(sql=sql_query)
                        if not flag_query:
                            return
                        sql_update: str = (
                            f"""
                        UPDATE `乘车数据_数据表`
                        SET
                        `票价` = '{row['票价']}',
                        `售票处` = '{row['售票处']}',
                        `窗口` = '{row['窗口']}',
                        `操作员编号` = '{row['操作员']}',
                        `售票时间` = '{row['售票时间'].replace('/', '-').replace('.000', '')}',
                        `更新时间` = '{self.dt.get_now(format="%Y-%m-%d %H:%M:%S")}'
                        WHERE id = {res_query[0][0]};
                        """
                            if len(res_query) > 0
                            else f"""
                        INSERT INTO `乘车数据_数据表`
                        (`车次`,`乘车日期`,`乘车时间`,`发站`,`到站`,`车厢号`,`座位号`,`席别`,`姓名`,`证件编号`,`证件类型`,`售票处`,`窗口`,`操作员编号`,`售票时间`,`票号`,`票价`,`票种`,`出行状态`,`MongoDB`,`创建时间`,`更新时间`,`删除时间`)
                        VALUES ('{row['车次']}','{row['乘车日期']}','{row['乘车时间']}','{row['发站']}','{row['到站']}','{row['车厢号']}','{row['座位号']}','{row['席别']}','{row['姓名']}','{row['证件编号']}','{row['证件类型']}','{row['售票处']}','{row['窗口']}','{row['操作员']}','{row['售票时间'].replace('/', '-').replace('.000', '')}','{row['票号']}','{row['票价']}','{row['票种']}','出行', 0,'{self.dt.get_now(format="%Y-%m-%d %H:%M:%S")}','{self.dt.get_now(format="%Y-%m-%d %H:%M:%S")}',NULL);
                        """
                        )
                        flag_update = self.db.update(sql=sql_update)
                        if not flag_update:
                            return
                    sql_update = f"""
                    UPDATE `乘车数据_数据表`
                    SET `出行状态` = '退票'
                    WHERE `乘车日期` = '{station["train_date"]}'
                    AND `车次` = '{station["board_train_code"]}'
                    AND `发站` = '{station["fromStation"]}'
                    AND `售票处` IS NULL
                    """
                    flag_update = self.db.update(sql=sql_update)
                    if not flag_update:
                        return

    def close(self) -> None:
        self.db.close()
