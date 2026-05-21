"""
多存储后端：阿里云 OSS / 七牛 / 腾讯云 COS / 本地

通过 .env 配置:
    STORAGE_BACKEND=aliyun|qiniu|tencent|local
    STORAGE_BASE_URL=https://your-cdn.com    # 对外访问域名
    STORAGE_LOCAL_DIR=/home/www/uploads      # local 模式必填

阿里云:
    ALIYUN_ACCESS_KEY_ID=xxx
    ALIYUN_ACCESS_KEY_SECRET=xxx
    ALIYUN_OSS_ENDPOINT=oss-cn-shanghai.aliyuncs.com
    ALIYUN_OSS_BUCKET=your-bucket

七牛:
    QINIU_ACCESS_KEY=xxx
    QINIU_SECRET_KEY=xxx
    QINIU_BUCKET=your-bucket
    QINIU_DOMAIN=https://cdn.your-domain.com

腾讯云:
    TENCENT_SECRET_ID=xxx
    TENCENT_SECRET_KEY=xxx
    TENCENT_COS_BUCKET=your-bucket
    TENCENT_COS_REGION=ap-shanghai
"""
import os
import io
import hashlib
import logging
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
import requests

# 自动加载 .env
load_dotenv(Path(__file__).parent / ".env", override=True)

logger = logging.getLogger("Storage")

_backend = None


def _get_backend():
    global _backend
    if _backend is not None:
        return _backend
    name = os.getenv("STORAGE_BACKEND", "local")
    if name == "aliyun":
        _backend = AliyunOSSBackend()
    elif name == "qiniu":
        _backend = QiniuBackend()
    elif name == "tencent":
        _backend = TencentCOSBackend()
    else:
        _backend = LocalBackend()
    logger.info(f"Storage backend: {name}")
    return _backend


def upload_from_url(url: str, save_path: str) -> str:
    """下载图片并上传到存储，返回可访问 URL"""
    return _get_backend().put_from_url(save_path, url)


def get_url(save_path: str) -> str:
    """根据存储路径生成可访问 URL"""
    return _get_backend().url(save_path)


def exists(save_path: str) -> bool:
    """检查远程文件是否存在"""
    return _get_backend().exists(save_path)


class LocalBackend:
    def __init__(self):
        self.base_dir = os.getenv("STORAGE_LOCAL_DIR", "/home/www/uploads")
        self.base_url = os.getenv("STORAGE_BASE_URL", "").rstrip("/")

    def put(self, path, data, content_type=None) -> str:
        import os as _os
        full = _os.path.join(self.base_dir, path)
        _os.makedirs(_os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(data)
        return self.url(path)

    def put_from_url(self, path, source_url) -> str:
        logger.info(f"[local save] {source_url[:80]} -> {path}")
        resp = requests.get(source_url, stream=True, timeout=60,
                            headers={"Referer": "https://www.instagram.com/",
                                     "User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        return self.put(path, resp.content, resp.headers.get("content-type", "image/jpeg"))

    def exists(self, path) -> bool:
        import os as _os
        return _os.path.exists(_os.path.join(self.base_dir, path))

    def url(self, path) -> str:
        if self.base_url:
            return f"{self.base_url}/{path}"
        return path


class AliyunOSSBackend:
    def __init__(self):
        import oss2
        self.client = oss2.Auth(
            os.getenv("ALIYUN_ACCESS_KEY_ID"),
            os.getenv("ALIYUN_ACCESS_KEY_SECRET"),
        )
        self.bucket = oss2.Bucket(
            self.client,
            os.getenv("ALIYUN_OSS_ENDPOINT"),
            os.getenv("ALIYUN_OSS_BUCKET"),
        )
        self.base_url = os.getenv("STORAGE_BASE_URL", "").rstrip("/")

    def exists(self, path) -> bool:
        try:
            self.bucket.get_object_meta(path)
            return True
        except:
            return False

    def put(self, path, data, content_type=None):
        headers = {"Content-Type": content_type} if content_type else {}
        self.bucket.put_object(path, data, headers=headers)
        return self.url(path)

    def put_from_url(self, path, source_url) -> str:
        """OSS 直接 fetch URL，失败则下载后上传"""
        try:
            result = self.bucket.put_object_with_url(path, source_url)
            if result.status == 200:
                logger.info(f"[OSS fetch] {source_url[:80]} -> {path}")
                return self.url(path)
        except Exception:
            pass
        # 降级：下载到内存再上传
        logger.info(f"[download+upload] {source_url[:80]} -> {path}")
        resp = requests.get(source_url, stream=True, timeout=60,
                            headers={"Referer": "https://www.instagram.com/",
                                     "User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        return self.put(path, resp.content,
                        resp.headers.get("content-type", "image/jpeg"))

    def url(self, path) -> str:
        if self.base_url:
            return f"{self.base_url}/{path}"
        return f"https://{os.getenv('ALIYUN_OSS_BUCKET')}.{os.getenv('ALIYUN_OSS_ENDPOINT')}/{path}"


class QiniuBackend:
    def __init__(self):
        from qiniu import Auth, put_data
        self.auth = Auth(os.getenv("QINIU_ACCESS_KEY"), os.getenv("QINIU_SECRET_KEY"))
        self.bucket_name = os.getenv("QINIU_BUCKET")
        self.base_url = os.getenv("QINIU_DOMAIN", "").rstrip("/")

    def exists(self, path) -> bool:
        from qiniu import BucketManager
        bucket = BucketManager(self.auth)
        ret, info = bucket.stat(self.bucket_name, path)
        return info.status_code == 200

    def put(self, path, data, content_type=None):
        from qiniu import put_data
        token = self.auth.upload_token(self.bucket_name, path, 3600)
        ret, info = put_data(token, path, data)
        if info.status_code != 200:
            raise RuntimeError(f"Qiniu upload failed: {info}")
        return self.url(path)

    def put_from_url(self, path, source_url) -> str:
        try:
            from qiniu import BucketManager
            bucket = BucketManager(self.auth)
            ret, info = bucket.fetch(source_url, self.bucket_name, path)
            if info.status_code == 200:
                logger.info(f"[Qiniu fetch] {source_url[:80]} -> {path}")
                return self.url(path)
        except Exception:
            pass
        logger.info(f"[download+upload] {source_url[:80]} -> {path}")
        resp = requests.get(source_url, stream=True, timeout=60,
                            headers={"Referer": "https://www.instagram.com/",
                                     "User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        return self.put(path, resp.content)

    def url(self, path) -> str:
        return f"{self.base_url}/{path}"


class TencentCOSBackend:
    def __init__(self):
        from qcloud_cos import CosConfig, CosS3Client
        config = CosConfig(
            Region=os.getenv("TENCENT_COS_REGION"),
            SecretId=os.getenv("TENCENT_SECRET_ID"),
            SecretKey=os.getenv("TENCENT_SECRET_KEY"),
        )
        self.client = CosS3Client(config)
        self.bucket = os.getenv("TENCENT_COS_BUCKET")
        self.base_url = os.getenv("STORAGE_BASE_URL", "").rstrip("/")

    def exists(self, path) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=path)
            return True
        except:
            return False

    def put(self, path, data, content_type=None):
        kwargs = {"Body": data}
        if content_type:
            kwargs["ContentType"] = content_type
        self.client.put_object(Bucket=self.bucket, Key=path, **kwargs)
        return self.url(path)

    def put_from_url(self, path, source_url) -> str:
        try:
            logger.info(f"[COS fetch] {source_url[:80]} -> {path}")
            self.client.put_object(
                Bucket=self.bucket, Key=path,
                Body=requests.get(source_url, timeout=30).content,
            )
            return self.url(path)
        except Exception:
            logger.info(f"[download+upload] {source_url[:80]} -> {path}")
            resp = requests.get(source_url, stream=True, timeout=60,
                                headers={"Referer": "https://www.instagram.com/",
                                         "User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            return self.put(path, resp.content)

    def url(self, path) -> str:
        if self.base_url:
            return f"{self.base_url}/{path}"
        return f"https://{self.bucket}.cos.{os.getenv('TENCENT_COS_REGION')}.myqcloud.com/{path}"
