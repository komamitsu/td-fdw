from distutils.core import setup

setup(name='td-fdw',
      version='0.0.5',
      description='A foreign data wrapper for TreasureData',
      author='Mitsunori Komatsu',
      author_email='komamitsu@gmail.com',
      license = "Apache",
      url='https://github.com/komamitsu/td-fdw',
      install_requires=['td-client', 'certifi'],
      packages=['tdfdw']
)
