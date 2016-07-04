from setuptools import setup, find_packages
from distutils.core import setup

setup(name='MaestroOps',
      version='0.4.1',
      description='Python Automation Framework for Development Operations Teams',
      author='Matthew Corner',
      author_email='mcorner@signiant.com',
      url='https://www.signiant.com',
      packages=find_packages(),
      license='MIT',
      install_requires=['boto3>=1.0.0,<2']
     )
