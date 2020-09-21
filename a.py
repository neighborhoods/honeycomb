from urllib.parse import quote
import sys

arg = sys.argv[1]
encoded = quote(arg, safe='')
print(encoded)
