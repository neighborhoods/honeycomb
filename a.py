from urllib.parse import quote
import sys

arg = sys.argv[1]
print(len(arg))
encoded = quote(arg, safe='')
print(len(encoded))
print(encoded)
