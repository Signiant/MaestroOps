from setuptools import setup, find_packages
from distutils.core import setup

setup(name='Maestro Framework',
      version='0.1b1', #For now each increment of the revision (the number after the letter) must remain backwards compatible. Change the letter for breaking changes.
      description='Maestro ',
      author='Matthew Corner / Signiant Inc.',
      author_email='mcorner@signiant.com',
      url='https://www.signiant.com',
      packages=find_packages()
     )
