#!/usr/bin/python

from setuptools import setup, find_packages

setup(
    name="pystudy",
    version="0.1.0",
    keywords=("flink", "table api"),
    description="flink python table api",
    long_description="apache flink python table api",
    license="Apache apache license 2.0",
    url="http://flink.apache.org",
    author="jincheng.sunjc",
    author_email="sunjincheng121@gmail.com",
    packages=find_packages(),
    include_package_data=True,
    platforms="any",
    install_requires=['pytest>=4.4.0'],
    scripts=['module2/pyflink/app1.py', 'module2/pyflink/app2.py'],
    py_modules=['module2'],
)
