from time import time

from setuptools import setup


setup(
    name="schemas",
    version=f"0.1.{int(time())}",
    py_modules=["schemas"],
    python_requires=">=3.8",
)
