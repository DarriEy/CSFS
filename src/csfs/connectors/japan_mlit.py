"""Japan MLIT Water Information System connector.

The Ministry of Land, Infrastructure, Transport and Tourism (MLIT) operates
Japan's river monitoring network via http://www1.river.go.jp (水文水質
データベース). The system is a legacy, frame-based, EUC-JP encoded site; this
connector targets the discharge (流量) download path directly:

* **Observations** — ``GET /cgi-bin/DspWaterData.exe?KIND=6`` returns an HTML
  page (one calendar month of hourly discharge) that embeds a link to a
  ``/dat/dload/download/*.dat`` file. The ``.dat`` (EUC-JP) has one row per
  day with 24 hourly ``value,flag`` pairs; ``-9999.99`` marks missing data.
  Discharge values are in m³/s and timestamps are JST (UTC+9).

* **Stations** — a curated, verified seed list of discharge gauging stations
  (the MLIT station search is an interactive frame UI; the seed IDs below were
  each confirmed to return real KIND=6 discharge data).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# MLIT publishes hourly discharge in Japan Standard Time.
_JST = ZoneInfo("Asia/Tokyo")

# MLIT .dat files mark missing data with -9999-family sentinels
# (-9999, -9999.0, -9999.99). Discharge is never negative, so treat any
# large-negative value as missing.
_MISSING_THRESHOLD = -9990.0

# ---------------------------------------------------------------------------
# Curated seed stations — verified to return real KIND=6 discharge data.
# Format: (native_id, name, latitude, longitude, river)
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[tuple[str, str, float, float, str]] = [
    ("301021281109010", "峠下", 43.84972, 141.80611, "留萌川"),
    ("301021281109020", "幌糠", 43.85611, 141.75917, "留萌川"),
    ("301021281109030", "大和田", 43.91361, 141.69806, "留萌川"),
    ("301021281109040", "留萌河口", 43.93667, 141.66583, "留萌川"),
    ("301050190002030", "大工又橋", 26.65611, 128.13833, "大保川"),
    ("301061281105010", "福山", 42.89028, 142.25528, "鵡川"),
    ("301061281105020", "穂別", 42.75917, 142.13639, "鵡川"),
    ("301061281105030", "栄", 42.65778, 142.07139, "鵡川"),
    ("301061281105040", "鵡川", 42.57361, 141.93611, "鵡川"),
    ("301061281105050", "稲里", 42.85083, 142.17222, "穂別川"),
    ("301131281108010", "滝ノ上", 44.19222, 143.07639, "渚滑川"),
    ("301131281108020", "上渚滑", 44.24972, 143.25917, "渚滑川"),
    ("301131281108030", "ウツツ橋", 44.34944, 143.27306, "渚滑川"),
    ("301131281108040", "渚滑橋", 44.38, 143.30056, "渚滑川"),
    ("301131281108050", "立牛(廃止)", 44.13694, 143.20056, "立牛川"),
    ("301320190001010", "川田", 26.64222, 128.16722, "福地川"),
    ("301350190002010", "奥", 26.82889, 128.28167, "奥川"),
    ("302021282206010", "閖上第二", 38.1775, 140.95361, "名取川"),
    ("302021282206030", "名取橋", 38.20611, 140.88611, "名取川"),
    ("302021282206050", "広瀬橋", 38.23556, 140.88917, "広瀬川"),
    ("302021282206060", "落合", 38.26833, 140.80861, "広瀬川"),
    ("302021282206070", "杉の下橋", 38.21583, 140.86417, "笊川"),
    ("302021282224010", "余方", 38.2175, 140.81444, "名取川"),
    ("302021282224020", "湯元", 38.23444, 140.70611, "名取川"),
    ("302021282224030", "碁石", 38.19917, 140.72639, "碁石川"),
    ("302021282224050", "馬引", 38.2075, 140.65111, "太郎川"),
    ("302021282224060", "前川", 38.17278, 140.66556, "前川"),
    ("302021282224070", "下原", 38.2, 140.605, "北川"),
    ("302061282202010", "赤川", 40.75417, 141.23333, "赤川"),
    ("302061282202020", "上野", 40.73806, 141.24444, "高瀬川"),
    ("302061282202030", "砂土路川", 40.71389, 141.27917, "砂土路川"),
    ("302061282202040", "中津川", 40.7, 141.32028, "中津川"),
    ("302061282202050", "姉沼川", 40.69333, 141.32528, "姉沼川"),
    ("302061282202080", "土場川", 40.77806, 141.25806, "土場川"),
    ("302101282209010", "矢島", 39.23194, 140.14778, "子吉川"),
    ("302101282209020", "吉沢", 39.27194, 140.11139, "子吉川"),
    ("302101282209030", "明法", 39.31528, 140.10111, "子吉川"),
    ("302101282209040", "宮内", 39.34806, 140.06667, "子吉川"),
    ("302101282209050", "二十六木橋", 39.37472, 140.06667, "子吉川"),
    ("302101282209070", "小川", 39.15778, 140.24111, "笹子川"),
    ("302101282209080", "山内", 39.32472, 140.17611, "石沢川"),
    ("302101282209090", "鮎瀬", 39.34306, 140.09167, "石沢川"),
    ("302101282209100", "館前", 39.41583, 140.07, "芋川"),
    ("303021283322020", "水府橋", 36.37694, 140.48083, "那珂川"),
    ("303021283322030", "下国井", 36.42083, 140.43472, "那珂川"),
    ("303021283322040", "野口", 36.55, 140.32611, "那珂川"),
    ("303021283322050", "川堀", 36.59444, 140.1775, "那珂川"),
    ("303021283322060", "小口", 36.75583, 140.14306, "那珂川"),
    ("303021283322070", "黒羽", 36.85694, 140.11861, "那珂川"),
    ("303021283322100", "搦手橋", 36.36891, 140.49344, "桜川"),
    ("303021283322120", "上合橋", 36.43667, 140.41806, "藤井川"),
    ("303061283310060", "亀の子橋", 35.515, 139.60889, "鶴見川"),
    ("303061283310070", "落合橋", 35.51333, 139.55194, "鶴見川"),
    ("303061283310090", "高田橋", 35.54722, 139.62056, "早淵川"),
    ("303061283310100", "鳥山", 35.50278, 139.60694, "鳥山川"),
    ("303061283310110", "浅山橋", 35.53694, 139.49833, "恩田川"),
    ("303061283310120", "寺家橋", 35.56972, 139.51222, "谷本川"),
    ("304021284402010", "馬下", 37.73806, 139.27083, "阿賀野川"),
    ("304021284402030", "満願寺", 37.82306, 139.15389, "阿賀野川"),
    ("304021284402040", "横越", 37.85306, 139.15528, "阿賀野川"),
    ("304021284402080", "善願", 37.74611, 139.19806, "早出川"),
    ("304021284415001", "山科", 37.61, 139.81917, "阿賀川"),
    ("304021284415002", "宮古", 37.55722, 139.85833, "阿賀川"),
    ("304021284415004", "馬越", 37.40389, 139.92028, "阿賀川"),
    ("304021284415005", "小谷", 37.39333, 139.92583, "阿賀川"),
    ("304021284415008", "若水", 37.26306, 139.87722, "阿賀川"),
    ("304021284415009", "田島", 37.20861, 139.77333, "阿賀川"),
    ("304021284415010", "片門", 37.57667, 139.76778, "只見川"),
    ("304021284415013", "南大橋", 37.5925, 139.88472, "日橋川"),
    ("304021284415014", "東大橋", 37.59028, 139.89778, "日橋川"),
    ("304021284415016", "新湯川", 37.49528, 139.90861, "湯川"),
    ("304021284415017", "鶴沼川", 37.30333, 139.93694, "鶴沼川"),
    ("304061288409010", "宇奈月", 36.81444, 137.58917, "黒部川"),
    ("304061288409020", "愛本", 36.85944, 137.55472, "黒部川"),
    ("304061288409030", "黒薙", 36.78583, 137.62889, "黒薙川"),
    ("304101284408030", "津沢", 36.625, 136.89222, "小矢部川"),
    ("304101284408040", "石動", 36.68083, 136.8825, "小矢部川"),
    ("304101284408060", "長江", 36.75833, 136.98306, "小矢部川"),
    ("304101284408070", "福野", 36.5875, 136.90306, "山田川"),
    ("304101284408080", "蓮沼", 36.65389, 136.86028, "渋江川"),
    ("304101284408090", "子撫川", 36.6925, 136.86917, "子撫川"),
    ("305041285510020", "加茂", 34.73944, 138.08889, "菊川"),
    ("305041285510060", "横地", 34.72889, 138.0975, "牛淵川"),
    ("305041285510070", "堂山", 34.67917, 138.0875, "牛淵川"),
    ("305041285510080", "川久保", 34.68778, 138.05306, "下小笠川"),
    ("305081285511010", "瑞浪", 35.36306, 137.25556, "庄内川"),
    ("305081285511020", "土岐", 35.3575, 137.19, "庄内川"),
    ("305081285511030", "多治見", 35.33417, 137.12694, "庄内川"),
    ("305081285511050", "志段味", 35.25167, 137.01861, "庄内川"),
    ("305081285511090", "枇杷島", 35.2025, 136.875, "庄内川"),
    ("305081285511130", "瀬古", 35.21, 136.92889, "矢田川"),
    ("305121285514010", "両郡", 34.50556, 136.54028, "櫛田川"),
    ("305121285514030", "櫛田橋", 34.54556, 136.58528, "櫛田川"),
    ("305121285514040", "西山橋", 34.48889, 136.55528, "佐奈川"),
    ("305121285521030", "塩ヶ瀬", 34.38083, 136.22222, "蓮川"),
    ("305121285521050", "田引", 34.42069, 136.29144, "櫛田川"),
    ("306041286603120", "黒津", 34.94083, 135.91639, "大戸川"),
    ("306041286603180", "野洲", 35.06222, 136.00417, "野洲川"),
    ("306041286603210", "野寺橋", 35.40222, 136.23333, "高時川"),
    ("306041286604010", "内裏野橋", 34.91667, 136.0775, "大戸川"),
    ("306041286606010", "宇治", 34.89444, 135.80389, "宇治川"),
    ("306041286606040", "向島", 34.92694, 135.76889, "宇治川"),
    ("306041286606060", "淀", 34.89667, 135.71778, "宇治川"),
    ("306041286606080", "高浜", 34.86778, 135.66972, "淀川"),
    ("306041286606090", "枚方", 34.81361, 135.63444, "淀川"),
    ("306041286606190", "加茂", 34.75889, 135.86889, "木津川"),
    ("306041286606230", "飯岡", 34.80167, 135.7975, "木津川"),
    ("306041286606260", "八幡", 34.88639, 135.70417, "木津川"),
    ("306041286606290", "亀岡", 35.01833, 135.58667, "桂川"),
    ("306041286606310", "保津峡", 35.02639, 135.64611, "桂川"),
    ("306041286606320", "天竜寺", 35.0125, 135.67861, "桂川"),
    ("306041286606330", "桂", 34.98222, 135.7125, "桂川"),
    ("306041286606340", "羽束師", 34.92722, 135.73111, "桂川"),
    ("306041286606350", "納所", 34.90833, 135.71694, "桂川"),
    ("306041286606370", "深草", 34.96583, 135.75889, "鴨川"),
    ("306041286606440", "芥川", 34.82889, 135.61472, "芥川"),
    ("306041286608010", "南田原", 34.90583, 135.37028, "猪名川"),
    ("306041286608020", "虫生", 34.87278, 135.39556, "猪名川"),
    ("306041286608030", "銀橋", 34.85417, 135.41528, "猪名川"),
    ("306041286608040", "小戸", 34.82611, 135.42222, "猪名川"),
    ("306041286608050", "軍行橋", 34.79667, 135.42306, "猪名川"),
    ("306041286608060", "猪名川橋", 34.77056, 135.43444, "猪名川"),
    ("306041286608070", "利倉", 34.76361, 135.45389, "猪名川"),
    ("306041286608080", "上食満", 34.76306, 135.43806, "藻川"),
    ("306041286609010", "上止々呂美", 34.88861, 135.46889, "余野川"),
    ("306041286609020", "吉田橋上流", 34.85556, 135.44083, "余野川"),
    ("306041286617010", "依那古", 34.71139, 136.15361, "木津川"),
    ("306041286617020", "大内", 34.74472, 136.12139, "木津川"),
    ("306041286617040", "朝屋", 34.75944, 136.115, "木津川"),
    ("306041286617060", "岩倉", 34.77778, 136.10028, "木津川"),
    ("306041286617070", "島ヶ原", 34.76778, 136.05778, "木津川"),
    ("306041286617090", "荒木", 34.77389, 136.16111, "服部川"),
    ("306041286617100", "伊賀上野橋", 34.785, 136.12278, "服部川"),
    ("306041286617110", "佐那具", 34.80111, 136.1625, "柘植川"),
    ("306041286617140", "名張", 34.62, 136.08167, "名張川"),
]


def _flag_to_quality(flag: str) -> QualityFlag:
    """Map an MLIT data flag to a CSFS quality flag.

    Flags: ``*`` 暫定値 (provisional), ``#`` 補正 (corrected/estimated),
    ``$`` 欠測 (missing), ``-`` 未登録 (unregistered), blank = observed.
    """
    f = flag.strip()
    if f == "*":
        return QualityFlag.RAW
    if f == "#":
        return QualityFlag.ESTIMATED
    if f in ("$", "-"):
        return QualityFlag.MISSING
    return QualityFlag.GOOD


@register("japan_mlit")
class JapanMlitConnector(BaseConnector):
    slug = "japan_mlit"
    display_name = "MLIT Water Information System (Japan)"
    base_url = "http://www1.river.go.jp"
    country_codes = ["JP"]

    # KIND=6 → hourly discharge (時刻流量), one calendar month per request.
    _DATA_PATH = "/cgi-bin/DspWaterData.exe"
    _DOWNLOAD_RE = re.compile(r"(/dat/dload/download/[^\"'\s]+\.dat)")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return the curated discharge gauging stations."""
        return [self._build_seed_station(row) for row in _SEED_STATIONS]

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch hourly discharge for *station_id* between *start* and *end*.

        KIND=6 serves one calendar month per request, so the range is walked
        month by month and the results filtered to the requested window.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        start = start if start.tzinfo else start.replace(tzinfo=UTC)
        end = end if end.tzinfo else end.replace(tzinfo=UTC)

        observations: list[Observation] = []
        for year, month in self._months(start, end):
            try:
                month_obs = await self._fetch_month(
                    native_id, year, month, station_id,
                )
            except ConnectorError:
                logger.warning(
                    "month_fetch_failed",
                    provider=self.slug, station=native_id,
                    year=year, month=month,
                )
                continue
            observations.extend(
                o for o in month_obs if start <= o.timestamp <= end
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24 hours of observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id, start=now - timedelta(hours=24), end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_seed_station(
        self, row: tuple[str, str, float, float, str],
    ) -> Station:
        """Create a Station model from a seed-list tuple."""
        native_id, name, lat, lon, river = row
        return Station(
            id=self._station_id(native_id),
            provider=self.slug,
            native_id=native_id,
            name=name,
            latitude=lat,
            longitude=lon,
            country_code="JP",
            river=river or None,
        )

    @staticmethod
    def _months(start: datetime, end: datetime) -> list[tuple[int, int]]:
        """Enumerate the (year, month) pairs spanned by [start, end]."""
        months: list[tuple[int, int]] = []
        year, month = start.year, start.month
        while (year, month) <= (end.year, end.month):
            months.append((year, month))
            month += 1
            if month > 12:
                year += 1
                month = 1
        return months

    async def _fetch_month(
        self,
        native_id: str,
        year: int,
        month: int,
        station_id: str,
    ) -> list[Observation]:
        """Fetch and parse one calendar month of hourly discharge."""
        bgn = f"{year}{month:02d}01"
        end = f"{year}{month:02d}31"
        try:
            page = await self._get(
                self._DATA_PATH,
                params={
                    "KIND": "6",
                    "ID": native_id,
                    "BGNDATE": bgn,
                    "ENDDATE": end,
                    "KAWABOU": "NO",
                },
            )
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to load data page for {native_id} {bgn}: {exc}",
            ) from exc

        match = self._DOWNLOAD_RE.search(page.content.decode("euc-jp", "replace"))
        if match is None:
            return []

        try:
            data = await self._get(match.group(1))
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to download data file for {native_id} {bgn}: {exc}",
            ) from exc

        return self._parse_dat(
            data.content.decode("euc-jp", "replace"), station_id,
        )

    def _parse_dat(self, text: str, station_id: str) -> list[Observation]:
        """Parse an MLIT discharge ``.dat`` file into observations.

        Each data row is ``YYYY/MM/DD`` followed by 24 ``value,flag`` pairs
        (hours 1–24). Hour *h* is timestamped at *h*:00 JST (hour 24 rolls
        into 00:00 of the next day) and converted to UTC.
        """
        observations: list[Observation] = []
        for line in text.splitlines():
            cells = line.split(",")
            if not cells or not re.match(r"^\d{4}/\d{2}/\d{2}$", cells[0].strip()):
                continue
            day = datetime.strptime(cells[0].strip(), "%Y/%m/%d")

            # cells[1:] are alternating value, flag for hours 1..24.
            for hour in range(1, 25):
                v_idx, f_idx = 2 * hour - 1, 2 * hour
                if f_idx >= len(cells):
                    break
                discharge = self._parse_value(cells[v_idx])
                flag = cells[f_idx]
                quality = (
                    QualityFlag.MISSING
                    if discharge is None
                    else _flag_to_quality(flag)
                )
                ts = (day + timedelta(hours=hour)).replace(
                    tzinfo=_JST,
                ).astimezone(UTC)
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
        return observations

    @staticmethod
    def _parse_value(raw: str) -> float | None:
        """Parse a discharge cell, returning None for missing/invalid data."""
        token = raw.strip()
        if not token:
            return None
        try:
            value = float(token)
        except ValueError:
            return None
        if value <= _MISSING_THRESHOLD:
            return None
        return value
