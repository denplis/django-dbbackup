import os
from distutils.core import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


packages = []
package_dir = "dbbackup"
for dirpath, dirnames, filenames in os.walk(package_dir):
    # ignore dirnames that start with '.'
    for i, dirname in enumerate(dirnames):
        if dirname.startswith("."):
            del dirnames[i]
    if "__init__.py" in filenames:
        pkg = dirpath.replace(os.path.sep, '.')
        if os.path.altsep:
            pkg = pkg.replace(os.path.altsep, '.')
        packages.append(pkg)


setup(
    name='django-dbbackup',
    version='1.62',
    description='Management commands to help backup and restore a project database to AmazonS3, Dropbox or local disk.',
    long_description=read('README.txt'),
    author='Michael Shepanski',
    author_email='mjs7231@gmail.com',
    install_requires=['simples3==1.0', 'dropbox>=1.3', 'poster'],
    license='BSD',
    url='http://bitbucket.org/mjs7231/django-dbbackup',
    keywords=['django', 'dropbox', 'database', 'backup', 'amazon', 's3'],
    packages=packages
)
