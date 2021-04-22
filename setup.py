from json import load
from setuptools import setup, find_packages


def readme():
    with open("README.md", "rb") as f:
        return f.read()


def get_version():
    with open("versions.json", "rb") as f:
        return load(f)["lib_version"]


setup(
    name="varada_trino_manager",
    version=get_version(),
    description="Varada Trino Manager",
    load_description=readme(),
    load_description_content_type="text/markdown",
    url="https://github.com/varadaio/varada-trino-manager",
    author="Ronen Hoffer",
    author_email="ronen@varada.io",
    packages=find_packages(),
    install_requires=[
        "click==7.1.2",
        "paramiko==2.7.2",
        "requests==2.25.1",
        "jsons==1.4.2",
        "sshtunnel==0.4.0",
        "Logbook==1.5.3",
        "pydantic==1.8.1"
    ],
    extras_require={
        "dev": [
            "pytest>=6.2.1",
            "tox>=3.20.1",
            "flake8>=3.8.4",
            "wheel>=0.36.2",
            "black==20.8b1",
            "autopep8==1.5.6",
        ]
    },
    zip_safe=False,
    entry_points={
        "console_scripts": ["vtm=varada_trino_manager.main:main"],
    },
)
