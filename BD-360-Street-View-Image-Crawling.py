# 360度全景图获取 (支持历史年份 + 断点续传 + 自动重试 + 智能切Key + 自动去水印 + 异常坐标容错日志)

# 导入工具模块
import csv  # 引入 csv 模块，用于读取储存着百度 API Key 的基础表格文件
import pandas as pd  ## 引入 pandas 数据分析库（简写为pd），用于高效读取、提取和处理包含大规模经纬度坐标的 CSV 文件
import requests  # 引入 requests 模块，用于向百度服务器发送网络请求
import time  # 引入 time 模块，用于控制程序的休眠（暂停），防止请求频率过快被服务器拦截
from PIL import Image  # 从 PIL (Pillow) 图像处理库引入 Image 模块，用于在内存中打开、裁剪和保存图片
import io  # 引入 io 模块，用于处理内存中的二进制字节流（把下载的原始包裹转换为 Image 能处理的图像对象）
import os  # 引入 os 模块来处理文件和文件夹路径
import math  # 引入 math 模块，提供高级数学运算支持（如正弦、余弦、开平方根等），用于在本地执行 WGS84 到 BD09 的坐标系转换公式
import json  # 引入 json 模块，用于解析百度 API 服务器返回的标准化 JSON 格式数据（例如提取报错状态码、解析历史时间轴属性等）
import re  # 用于解析时间线数据

# ==================== 核心配置区域 ====================

# 1.【核心功能开关】：是否开启历史图像获取？(填入 None 为获取最新，填入如 2016 为获取历史)
TARGET_YEAR = None

# 2. 文件路径配置
KEY_PATH = r'D:\BaiduSVICollection\key.csv'  # 存放百度地图开放平台申请到的key的路径
COORD_PATH = r'D:\BaiduSVICollection\WGSCoordinatesUsedforScraping.csv'  # 存放需要爬取的坐标数据的路径
SAVE_PICTURES_PATH = r'D:\BaiduSVICollection\BaiduSVI360'  # 存储街景图像爬取结果
FAIL_COORD_PATH = r'D:\BaiduSVICollection\FailureCoordinates\360FailureCoordinates.csv'  # 存储爬取失败的坐标点

# 3. 街景图像视觉参数配置
# pitch：垂直视角，范围[0,90]。
# width：图片宽度，范围[10,1024]，默认值[400]。
# height:图片高度，范围[10,512]，默认值[300]。
# fov：水平方向范围，范围[10,360]，fov=360即可显示整幅全景图
# 更多参数请查看百度开发平台全景静态图官方文档：https://lbsyun.baidu.com/docs/webapi?title=viewstatic/viewstatic-base
IMAGE_PITCH = 20  # 垂直视角，范围[0,90]
IMAGE_WIDTH = 1024  # 图片宽度，范围[10,1024]，默认值[400]
IMAGE_HEIGHT = 512  # 图片高度，范围[10,512]，默认值[300]
IMAGE_FOV = 360  # 水平方向范围，范围[10,360]，fov=360即可显示整幅全景图

# 4. 网络并发与请求控制配置
REQUEST_DELAY = 0.11  # 常规请求间隔时间（秒）。设为0.11意味着一秒最多请求少于10次，有效避免401报错

# 5. 图像后处理配置
CROP_BOTTOM_PIXELS = 75  # 裁剪掉底部的像素高度（用于去除百度水印）

# =========================================================

# ================= 辅助模块：错误日志记录 =================
#根据服务状态码判断状态，更多详细信息请自行查询全景静态图返回码附录：https://lbsyun.baidu.com/faq/api?title=webapi/appendix
def log_failure(fid, lng, lat, failure_type, file_path):
    """将失败的坐标点直接追加记录到现有的日志CSV中"""
    with open(file_path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([fid, lng, lat, failure_type])
# =========================================================

# =================1. 准备 Key 和坐标数据=================
key_list = []
with open(KEY_PATH, mode='r', encoding='utf-8') as f:
    lines = csv.reader(f)
    for line in lines:
        key_list.append(line)

df = pd.read_csv(COORD_PATH, encoding='utf-8')
coordinates_df = df[['FID', 'lng', 'lat']]
data_list = coordinates_df.values.tolist()

# =================2. 坐标系转换函数（完整版）=================
x_pi = 3.14159265358979324 * 3000.0 / 180.0
pi = 3.1415926535897932384626
a = 6378245.0
ee = 0.00669342162296594323

LLBAND = [75, 60, 45, 30, 15, 0]
LL2MC = [
    [-0.0015702102444, 111320.7020616939, 1704480524535203, -10338987376042340, 26112667856603880, -35149669176653700,
     26595700718403920, -10725012454188240, 1800819912950474, 82.5],
    [0.0008277824516172526, 111320.7020463578, 647795574.6671607, -4082003173.641316, 10774905663.51142,
     -15171875531.51559, 12053065338.62167, -5124939663.577472, 913311935.9512032, 67.5],
    [0.00337398766765, 111320.7020202162, 4481351.045890365, -23393751.19931662, 79682215.47186455, -115964993.2797253,
     97236711.15602145, -43661946.33752821, 8477230.501135234, 52.5],
    [0.00220636496208, 111320.7020209128, 51751.86112841131, 3796837.749470245, 992013.7397791013, -1221952.21711287,
     1340652.697009075, -620943.6990984312, 144416.9293806241, 37.5],
    [-0.0003441963504368392, 111320.7020576856, 278.2353980772752, 2485758.690035394, 6070.750963243378,
     54821.18345352118, 9540.606633304236, -2710.55326746645, 1405.483844121726, 22.5],
    [-0.0003218135878613132, 111320.7020701615, 0.00369383431289, 823725.6402795718, 0.46104986909093,
     2351.343141331292, 1.58060784298199, 8.77738589078284, 0.37238884252424, 7.45]
]

def transformlat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 * math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 * math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 * math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret

def transformlng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 * math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 * math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 * math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret

def wgs84togcj02(lng, lat):
    dlat = transformlat(lng - 105.0, lat - 35.0)
    dlng = transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    return [lng + dlng, lat + dlat]

def gcj02tobd09(lng, lat):
    z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * x_pi)
    theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * x_pi)
    return [z * math.cos(theta) + 0.0065, z * math.sin(theta) + 0.006]

def getRange(cC, cB, T):
    if cB is not None: cC = max(cC, cB)
    if T is not None: cC = min(cC, T)
    return cC

def getLoop(cC, cB, T):
    while cC > T: cC -= T - cB
    while cC < cB: cC += T - cB
    return cC

def convertor(cC, cD):
    if cC is None or cD is None: return None
    T = cD[0] + cD[1] * abs(cC.x)
    cB = abs(cC.y) / cD[9]
    cE = cD[2] + cD[3] * cB + cD[4] * cB * cB + cD[5] * cB * cB * cB + cD[6] * cB * cB * cB * cB + cD[
        7] * cB * cB * cB * cB * cB + cD[8] * cB * cB * cB * cB * cB * cB
    if cC.x < 0: T = T * -1
    if cC.y < 0: cE = cE * -1
    return [T, cE]

class LLT:
    def __init__(self, x, y):
        self.x = x
        self.y = y

def convertLL2MC(T):
    cD = None
    T.x = getLoop(T.x, -180, 180)
    T.y = getRange(T.y, -74, 74)
    cB = T
    for cC in range(0, len(LLBAND), 1):
        if cB.y >= LLBAND[cC]:
            cD = LL2MC[cC]
            break
    if cD is None:
        for cC in range(len(LLBAND) - 1, -1, -1):
            if cB.y <= -LLBAND[cC]:
                cD = LL2MC[cC]
                break
    return convertor(T, cD)

def wgstobdmc(lon, lat):
    tmplon, tmplat = wgs84togcj02(lon, lat)
    bd_lon, bd_lat = gcj02tobd09(tmplon, tmplat)
    mc = convertLL2MC(LLT(bd_lon, bd_lat))
    return mc[0], mc[1]

def wgs84_to_bd09_math(lng, lat):
    lng, lat = float(lng), float(lat)
    dlat = transformlat(lng - 105.0, lat - 35.0)
    dlng = transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sqrt(1 - ee * math.sin(radlat) * math.sin(radlat))
    gcj_lat = lat + (dlat * 180.0) / ((a * (1 - ee)) / (magic * magic * magic) * pi)
    gcj_lng = lng + (dlng * 180.0) / (a / magic * math.cos(radlat) * pi)
    z = math.sqrt(gcj_lng * gcj_lng + gcj_lat * gcj_lat) + 0.00002 * math.sin(gcj_lat * x_pi)
    theta = math.atan2(gcj_lat, gcj_lng) + 0.000003 * math.cos(gcj_lng * x_pi)
    return z * math.cos(theta) + 0.0065, z * math.sin(theta) + 0.006

# =================3. 历史时间线解析函数=================
def get_historical_panoid(wgs_lon, wgs_lat, target_year):
    if target_year is None:
        return None
    try:
        x, y = wgstobdmc(wgs_lon, wgs_lat)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

        url_qsdata = f"https://mapsv0.bdimg.com/?qt=qsdata&x={x}&y={y}&fn=jsonp69972182"
        res = requests.get(url_qsdata, headers=headers, timeout=10)
        json_str = res.text.split('(')[1].split(')')[0]
        data = json.loads(json_str)

        if data.get('result', {}).get('error') != 0:
            return None

        time.sleep(REQUEST_DELAY) # 查询时间线时也加入轻微延时防并发

        sid = data['content']['id']
        url_sdata = f"https://mapsv0.bdimg.com/?qt=sdata&sid={sid}&pc=1&fn=jsonp.p3991630"
        res2 = requests.get(url_sdata, headers=headers, timeout=10)
        json_str2 = re.findall(r'[(](.*)[)]', res2.text, re.S)[0]
        data2 = json.loads(json_str2)

        timeline = data2['content'][0]['TimeLine']
        for item in timeline:
            if str(item.get('Year')) == str(target_year):
                return item.get('ID')
        return None
    except Exception:
        return None


# =================4. 核心爬取主循环=================
key_num = 0
if not os.path.exists(SAVE_PICTURES_PATH):
    os.makedirs(SAVE_PICTURES_PATH)

file_suffix = str(TARGET_YEAR) if TARGET_YEAR else "latest"
existing_files = os.listdir(SAVE_PICTURES_PATH)
downloaded_fids = set(
    [f.replace(f'_{file_suffix}.jpg', '') for f in existing_files if f.endswith(f'_{file_suffix}.jpg')])

print(f"🔍 扫描本地目录，发现已下载了 {len(downloaded_fids)} 张 [{file_suffix}] 版本的全景图。")

for data in data_list:
    FID, lon, lat = data[0], data[1], data[2]
    current_fid_str = str(int(FID))

    if current_fid_str in downloaded_fids:
        print(f"⏩ 跳过: FID {current_fid_str} 的 [{file_suffix}] 图像已经存在。")
        continue

    bd_lon, bd_lat = wgs84_to_bd09_math(lon, lat)
    panoid = get_historical_panoid(lon, lat, TARGET_YEAR)

    max_retries = 5
    retry_count = 0
    success = False
    failure_reason = "Unknown Error"

    while retry_count < max_retries:
        if key_num >= len(key_list):
            print('\n🚨 警告：所有的 Key 都已经用完，程序结束。')
            failure_reason = "All Keys Exhausted"
            break

        key = key_list[key_num][1] if isinstance(key_list[key_num], list) else key_list[key_num]

        # ========= 动态生成请求 URL =========
        base_params = f"&pitch={IMAGE_PITCH}&width={IMAGE_WIDTH}&height={IMAGE_HEIGHT}&fov={IMAGE_FOV}&ak={key}"

        if panoid:
            url = f'https://api.map.baidu.com/panorama/v2?panoid={panoid}{base_params}'
        else:
            url = f'https://api.map.baidu.com/panorama/v2?location={bd_lon},{bd_lat}{base_params}'

        try:
            response = requests.get(url, timeout=10)
            content_type = response.headers.get('Content-Type', '')

            if 'image' in content_type:
                image_bytes = response.content
                img = Image.open(io.BytesIO(image_bytes))
                width, height = img.size

                crop_box = (0, 0, width, height - CROP_BOTTOM_PIXELS)
                cropped_img = img.crop(crop_box)

                file_path = os.path.join(SAVE_PICTURES_PATH, f"{current_fid_str}_{file_suffix}.jpg")
                cropped_img.save(file_path)
                print(f'✅ 成功下载并裁剪图片 {current_fid_str}_{file_suffix}.jpg')

                success = True
                time.sleep(REQUEST_DELAY)
                break
            else:
                error_data = response.json()
                status = str(error_data.get('status'))

                if status == '302':
                    print(f'⚠️ Key {key_num + 1} 额度已耗尽 (302)，正在自动切换...')
                    key_num += 1
                    continue
                elif status in ['211', '240', '401']:  # 【修改点2】：将 401 加入并发控制及重试列表
                    print(f'⚠️ 触发API限流保护 (Status {status})，强制暂停1秒后重试...')
                    time.sleep(1)  # 遇到并发限制，强制多等一会儿再重试
                    retry_count += 1
                    failure_reason = f"API Retry Limit Reached (Status {status})"
                elif status == '200' and 'AK' in str(error_data.get('message', '')):
                    key_num += 1
                    continue
                else:
                    failure_reason = f"API Error (Status {status}: {error_data.get('message', 'No message')})"
                    print(f'❓ FID:{current_fid_str} 下载失败，API提示: {error_data}')
                    break

        except Exception as e:
            print(f'🌐 抓取时发生网络波动: {e}，准备重试...')
            time.sleep(1)
            retry_count += 1
            failure_reason = "Network Timeout or Exception"

    # --- 容错日志写入逻辑 ---
    if not success:
        log_failure(current_fid_str, lon, lat, failure_reason, FAIL_COORD_PATH)
        print(f"📝 已将失败记录追加至日志: FID {current_fid_str}, 错误类型: {failure_reason}")

    if key_num >= len(key_list):
        break

print("\n🎉 所有的任务运行完毕！")