from distutils.core import setup

setup(name='TreasureDataFdw',
      version='0.0.1',
      description='A foreign data wrapper for TreasureData',
      author='Mitsunori Komatsu',
      author_email='komamitsu@gmail.com',
      url='https://github.com/komamitsu/td_fdw',
      install_requires=['td-client'],
      py_modules=['tdfdw'])