import os

print(os.getenv('NHDS_PYPI_USERNAME'))
print(os.path.expandvars('$NHDS_PYPI_USERNAME'))
