import re
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')

with open('data/training.txt', 'r') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        # Example line: __label__Business  Wall St. Bears Claw Back Into the Black (Reuters) Reuters - Short-sellers, Wall Street's dwindling\band of ultra-cynics, are seeing green again.
        match = re.match(r'^__label__(\w+)\s+(.*)$', line)
        if not match:
            continue
        category = match.group(1)
        content = match.group(2)
        doc = {
            'category': category,
            'content': content
        }
        es.index(index='ad-categories', document=doc)

print('Indexing complete.') 