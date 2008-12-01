import re

def striptags(text):
    return re.compile(r'<[^>]*>').sub('', text)

