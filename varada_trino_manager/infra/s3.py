
import boto3
import fnmatch
import gzip
import io
import urlpath


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Client(metaclass=SingletonMeta):
    def __init__(self):
        self.client = boto3.client("s3")


class S3URL:
    def __init__(self, url, etag=None):
        self.url = urlpath.URL(url)
        if (self.url.scheme != "s3") and (self.url.scheme != "s3a"):
            raise ValueError(f'Bad S3 URL: "{url}"')
        self._etag = etag
        self.client = Client().client

    @property
    def bucket(self):
        return self.url.hostname

    @property
    def path(self):
        return self.url.path.lstrip("/")

    @property
    def etag(self):
        if self._etag is None:
            raise ValueError("undefined etag")
        return self._etag

    def download(self):
        stream = io.BytesIO()
        self.client.download_fileobj(self.bucket, self.path, stream)
        stream.seek(0)
        if self.url.suffix == ".gz":
            stream = gzip.GzipFile(fileobj=stream)
        return stream

    def download_text(self):
        buffer = self.download()
        try:
            return buffer.getvalue().decode()
        except:
            return buffer.read().decode()

    def __str__(self):
        return str(self.url)

    def __truediv__(self, other):
        return S3URL(self.url / other)

    def glob(self):
        prefix = self.path.split("*")[0]
        bucket = self.bucket
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["StorageClass"] == "STANDARD" and fnmatch.fnmatch(obj["Key"], self.path):
                    path = obj["Key"]
                    url = f's3://{bucket}/{path}'
                    yield S3URL(url=url, etag=obj["ETag"])

    def glob_folders(self):
        prefix = self.path.split("*")[0]
        bucket = self.bucket
        paginator = self.client.get_paginator("list_objects_v2")
        root_url = f"{self.url}/"

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            for obj in page.get("CommonPrefixes", []):
                path = obj["Prefix"]
                url = f'{self.url.scheme}://{bucket}/{path}'
                if root_url == url:  # ignore the root directory
                    continue
                yield S3URL(url=url)
