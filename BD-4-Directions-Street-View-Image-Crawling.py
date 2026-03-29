# 四方位街景获取 (支持历史年份 + 断点续传 + 自动重试 + 智能切Key + 自动去水印 + 自动横向合图 + 异常坐标容错日志)

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
TARGET_YEAR = None  # 例如：2016、2019 等

# 2. 文件路径配置
KEY_PATH = r'D:\BaiduSVICollection\key.csv'  # 存放百度地图开放平台申请到的key的路径
COORD_PATH = r'D:\BaiduSVICollection\WGSCoordinatesUsedforScraping.csv'  # 存放需要爬取的坐标数据的路径
SAVE_DIR = r'D:\BaiduSVICollection\BaiduSVI4Direction'  # 存储四方位单张图片的文件夹
MERGED_DIR = os.path.join(SAVE_DIR, 'Merged')  # 存储合并后长图的文件夹
FAIL_COORD_PATH = r'D:\BaiduSVICollection\FailureCoordinates\4DirectionsFailureCoordinates.csv'  # 存储爬取失败的坐标点

# 创建必要的文件夹
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)
if not os.path.exists(MERGED_DIR):
    os.makedirs(MERGED_DIR)

# 3. 街景图像视觉参数配置
# 更多参数详情请查看百度开发平台全景静态图官方文档：https://lbsyun.baidu.com/docs/webapi?title=viewstatic/viewstatic-base
IMAGE_PITCH = 20  # 垂直视角，范围[0,90]
IMAGE_WIDTH = 1024  # 图片宽度，范围[10,1024]，默认值[400]
IMAGE_HEIGHT = 512  # 图片高度，范围[10,512]，默认值[300]
IMAGE_FOV = 90  # 水平方向范围，四方位图拼接一般设定fov为90

# 4. 网络并发与请求控制配置
REQUEST_DELAY = 0.11  # 常规请求间隔时间（秒）。设为0.11意味着一秒最多请求少于10次，有效避免401报错

# 5. 图像后处理配置
CROP_BOTTOM_PIXELS = 75  # 裁剪掉底部的像素高度（用于去除百度水印）


# ====================================================

# ================= 辅助模块：错误日志记录 =================
# 根据服务状态码判断状态，更多详细信息请自行查询全景静态图返回码附录：https://lbsyun.baidu.com/faq/api?title=webapi/appendix
def log_failure(fid, lng, lat, failure_type, file_path):
    """将失败的坐标点直接追加记录到现有的日志CSV中"""
    with open(file_path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([fid, lng, lat, failure_type])


# =========================================================

# ==================== 1. 数据准备 ====================
key_list = []
with open(KEY_PATH, mode='r', encoding='utf-8') as f:
    lines = csv.reader(f)
    for line in lines:
        key_list.append(line)

df = pd.read_csv(COORD_PATH, encoding='utf-8')
coordinates_df = df[['FID', 'lng', 'lat']]
data_list = coordinates_df.values.tolist()

# ==================== 2. 坐标转化与历史数据搜寻 ====================
x_pi = 3.14159265358979324 * 3000.0 / 180.0
pi = 3.1415926535897932384626
a = 6378245.0
ee = 0.00669342162296594323

# 百度墨卡托投影纠正矩阵
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
    """纬度转换"""
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 * math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 * math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 * math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret


def transformlng(lng, lat):
    """经度转换"""
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 * math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 * math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 * math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret


def wgs84togcj02(lng, lat):
    """WGS84 -> GCJ02 (火星坐标系)"""
    dlat = transformlat(lng - 105.0, lat - 35.0)
    dlng = transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng
    return [mglng, mglat]


def gcj02tobd09(lng, lat):
    """GCJ02 -> BD09 (百度坐标系)"""
    z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * x_pi)
    theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * x_pi)
    bd_lng = z * math.cos(theta) + 0.0065
    bd_lat = z * math.sin(theta) + 0.006
    return [bd_lng, bd_lat]


def getRange(cC, cB, T):
    if cB is not None:
        cC = max(cC, cB)
    if T is not None:
        cC = min(cC, T)
    return cC


def getLoop(cC, cB, T):
    while cC > T:
        cC -= T - cB
    while cC < cB:
        cC += T - cB
    return cC


def convertor(cC, cD):
    if cC is None or cD is None:
        return None
    T = cD[0] + cD[1] * abs(cC.x)
    cB = abs(cC.y) / cD[9]
    cE = cD[2] + cD[3] * cB + cD[4] * cB * cB + cD[5] * cB * cB * cB + cD[6] * cB * cB * cB * cB + cD[
        7] * cB * cB * cB * cB * cB + cD[8] * cB * cB * cB * cB * cB * cB
    if cC.x < 0:
        T = T * -1
    if cC.y < 0:
        cE = cE * -1
    return [T, cE]


class LLT:
    def __init__(self, x, y):
        self.x = x
        self.y = y


def convertLL2MC(T):
    """百度经纬度 -> 百度墨卡托（使用标准校正矩阵）"""
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
    cE = convertor(T, cD)
    return cE


def wgstobdmc(lon, lat):
    """【关键函数】WGS84 -> 百度墨卡托（完整转换流程）"""
    tmplon, tmplat = wgs84togcj02(lon, lat)
    bd_lon, bd_lat = gcj02tobd09(tmplon, tmplat)
    baidut = LLT(bd_lon, bd_lat)
    mc = convertLL2MC(baidut)
    return mc[0], mc[1]  # 返回墨卡托 x, y


# 历史时间线解析
def get_historical_panoid(wgs_lon, wgs_lat, target_year):
    if target_year is None:
        return None
    try:
        x, y = wgstobdmc(wgs_lon, wgs_lat)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

        # 第一步：获取街景点唯一标识ID（sid）
        url_qsdata = f"https://mapsv0.bdimg.com/?qt=qsdata&x={x}&y={y}&fn=jsonp69972182"
        res = requests.get(url_qsdata, headers=headers, timeout=10)
        res_text = res.text

        json_str = res_text.split('(')[1].split(')')[0]
        data = json.loads(json_str)

        if data.get('result', {}).get('error') != 0:
            print(f"⚠️ 坐标点无街景数据，错误码：{data.get('result', {}).get('error')}")
            return None

        time.sleep(REQUEST_DELAY)  # 查询时间线时也加入轻微延时防并发

        sid = data['content']['id']
        print(f"📍 获取到sid: {sid}")

        # 第二步：获取该街景点的时间轴
        url_sdata = f"https://mapsv0.bdimg.com/?qt=sdata&sid={sid}&pc=1&fn=jsonp.p3991630"
        res2 = requests.get(url_sdata, headers=headers, timeout=10)
        res2_text = res2.text

        json_str2 = re.findall(r'[(](.*)[)]', res2_text, re.S)[0]
        data2 = json.loads(json_str2)
        timeline = data2['content'][0]['TimeLine']

        print(f"📅 该点共有 {len(timeline)} 个时间点的街景数据：")
        for item in timeline:
            print(f"   - 年份: {item.get('Year')}, ID: {item.get('ID')}")

        # 第三步：查找目标年份对应的panoid
        for item in timeline:
            if str(item.get('Year')) == str(target_year):
                panoid = item.get('ID')
                print(f"✅ 找到目标年份 {target_year} 的街景，panoid: {panoid}")
                return panoid

        available_years = [item.get('Year') for item in timeline]
        print(f"❌ 未找到年份 {target_year} 的街景，可用年份: {available_years}")
        return None

    except Exception as e:
        print(f"❌ 获取历史街景失败，详细错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


# ==================== 3. 四方图爬取模块 ====================
key_num = 0
file_suffix = str(TARGET_YEAR) if TARGET_YEAR else "latest"

# 极速断点续传
existing_merged = os.listdir(MERGED_DIR)
downloaded_fids = set(
    [f.replace(f'_{file_suffix}.jpg', '') for f in existing_merged if f.endswith(f'_{file_suffix}.jpg')])

print(f"🔍 扫描本地合并目录，发现已完成 {len(downloaded_fids)} 个 [{file_suffix}] 版本的点位。")

for data in data_list:
    FID, lon, lat = data[0], data[1], data[2]
    current_fid_str = str(int(FID))

    if current_fid_str in downloaded_fids:
        print(f"⏩ 跳过: FID {current_fid_str} 的合图已存在。")
        continue

    # 【修改】先转换为百度坐标（用于API）
    tmp_lng, tmp_lat = wgs84togcj02(lon, lat)
    bd_lon, bd_lat = gcj02tobd09(tmp_lng, tmp_lat)

    # 【修改】使用WGS坐标获取历史panoid
    panoid = get_historical_panoid(lon, lat, TARGET_YEAR)

    # 准备存放4个方向图片的列表
    fetched_images = []
    missing_headings = []
    failure_reason = "Unknown Error"

    for i in range(4):
        heading_angle = i * 90
        retry_count = 0
        success_for_this_heading = False

        while retry_count < 5:
            if key_num >= len(key_list):
                print('\n🚨 警告：所有的 Key 都已经用完啦！')
                failure_reason = "All Keys Exhausted"
                break

            key = key_list[key_num][1] if isinstance(key_list[key_num], list) else key_list[key_num]

            # ========= 动态生成请求 URL =========
            base_params = f"&pitch={IMAGE_PITCH}&width={IMAGE_WIDTH}&height={IMAGE_HEIGHT}&fov={IMAGE_FOV}&heading={heading_angle}&ak={key}"

            if panoid:
                # 有历史panoid，使用panoid获取
                url = f'https://api.map.baidu.com/panorama/v2?panoid={panoid}{base_params}'
            else:
                # 没有指定年份或找不到历史数据，使用坐标获取最新图像
                url = f'https://api.map.baidu.com/panorama/v2?location={bd_lon},{bd_lat}{base_params}'

            try:
                response = requests.get(url, timeout=10)
                content_type = response.headers.get('Content-Type', '')

                if 'image' in content_type:
                    image_bytes = response.content
                    img = Image.open(io.BytesIO(image_bytes))
                    width, height = img.size

                    # 【去除水印】裁剪掉底部的配置像素
                    crop_box = (0, 0, width, height - CROP_BOTTOM_PIXELS)
                    cropped_img = img.crop(crop_box)

                    # 保存单方向原图
                    indiv_filename = f"{current_fid_str}_{heading_angle}_{file_suffix}.jpg"
                    cropped_img.save(os.path.join(SAVE_DIR, indiv_filename))

                    fetched_images.append(cropped_img)
                    success_for_this_heading = True
                    time.sleep(REQUEST_DELAY)  # 加入请求间隔，防并发
                    break
                else:
                    error_data = response.json()
                    status = str(error_data.get('status'))

                    if status == '302':
                        print(f'⚠️ Key {key_num + 1} 额度已耗尽 (302)，正在切换...')
                        key_num += 1
                        continue
                    elif status in ['211', '240', '401']:
                        print(f'⚠️ 触发API限流保护 (Status {status})，强制暂停1秒后重试...')
                        time.sleep(1)  # 强制等待1秒
                        retry_count += 1
                        failure_reason = f"API Retry Limit Reached (Status {status})"
                    elif status == '200' and 'AK' in str(error_data.get('message', '')):
                        key_num += 1
                        continue
                    else:
                        failure_reason = f"API Error (Status {status}: {error_data.get('message', 'No message')})"
                        break

            except Exception as e:
                time.sleep(1)
                retry_count += 1
                failure_reason = "Network Timeout or Exception"

        if key_num >= len(key_list):
            break

        if not success_for_this_heading:
            missing_headings.append(heading_angle)

    if key_num >= len(key_list):
        break

    # ==================== 4. 合图逻辑模块及错误日志 ====================
    if missing_headings:
        print(f"⚠️ 报告: FID {current_fid_str} 获取完毕，但缺失方向: {missing_headings}")

    # 只有当成功抓取了至少一张图片时，才进行合图
    if len(fetched_images) > 0:
        total_width = sum(img.width for img in fetched_images)
        max_height = max(img.height for img in fetched_images)
        merged_img = Image.new('RGB', (total_width, max_height))

        x_offset = 0
        for img in fetched_images:
            merged_img.paste(img, (x_offset, 0))
            x_offset += img.width

        merged_filename = f"{current_fid_str}_{file_suffix}.jpg"
        merged_img.save(os.path.join(MERGED_DIR, merged_filename))
        print(f'✅ 成功合并并保存全景长图: {merged_filename}')
    else:
        # 如果该坐标4个方向都抓取失败，则写入错误日志
        print(f'❌ 失败: FID {current_fid_str} 未能获取任何方向的图像，跳过合图。')
        log_failure(current_fid_str, lon, lat, failure_reason, FAIL_COORD_PATH)
        print(f"📝 已将失败记录追加至日志: FID {current_fid_str}, 错误类型: {failure_reason}")

print("\n🎉 所有的任务运行完毕！")