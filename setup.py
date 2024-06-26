from setuptools import setup, find_packages
from distutils.core import setup

setup(name='MaestroOps',
      version='0.9.3',
      description='Python Automation Framework for Development Operations Teams',
      author='Signiant DevOps',
      author_email='devops@signiant.com',
      url='https://www.signiant.com',
      packages=find_packages(),
      license='MIT',
      install_requires=['boto3>=1.33.1']
     )
