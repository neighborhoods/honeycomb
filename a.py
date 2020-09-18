from urllib.parse import quote
import sys

print(quote(sys.argv[1], safe=''))
