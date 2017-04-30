# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This file is part of QBroker, an easy to use RPC and broadcast
## client+server using AMQP.
##
## QBroker is Copyright © 2016 by Matthias Urlichs <matthias@urlichs.de>,
## it is licensed under the GPLv3. See the file `README.rst` for details,
## including optimistic statements by the author.
##
## This paragraph is auto-generated and may self-destruct at any time,
## courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
## Thus, please do not remove the next line, or insert any blank lines.
##BP

import qbroker
qbroker.setup(sync=True,gevent=True)
from setuptools import setup, find_packages
import xml.sax.saxutils
from os.path import join
import sys
import os
from setuptools.command.test import test as TestCommand

def get_version(fname='qbroker/__init__.py'):
    with open(fname) as f:
        for line in f:
            if line.startswith('__VERSION__'):
                return eval(line.split('=')[-1])

def compile_po(path):
    from msgfmt import Msgfmt
    for language in os.listdir(path):
        lc_path = join(path, language, 'LC_MESSAGES')
        if os.path.isdir(lc_path):
            for domain_file in os.listdir(lc_path):
                if domain_file.endswith('.po'):
                    file_path = join(lc_path, domain_file)
                    mo_file = join(lc_path, '%s.mo' % domain_file[:-3])
                    mo_content = Msgfmt(file_path, name=file_path).get()
                    mo = open(mo_file, 'wb')
                    mo.write(mo_content)
                    mo.close()

def read(filename):
    text = open(filename,'r').read()
    return xml.sax.saxutils.escape(text)

class PyTest(TestCommand):
    #user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ['--assert=plain']

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        os.unlink("test.log") # GAAH
        sys.exit(errno)

long_description = read('README.rst')

REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines()
                                if not i.startswith("http")]

setup(name='QBroker',
      license='GPLv3+',
      version='.'.join(str(x) for x in get_version()),
      description='QBroker is a minimal async wrapper for RPC via AMQP',
      long_description=long_description,
      author='Matthias Urlichs',
      author_email='matthias@urlichs.de',
      url='https://github.com/M-o-a-T/qbroker',
      install_requires=REQUIREMENTS,
      packages=find_packages(exclude=('tests',)),
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Topic :: Software Development :: Libraries :: Application Frameworks',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: Utilities',
      ],
      zip_safe=True,
      cmdclass = {'test': PyTest},


      )

