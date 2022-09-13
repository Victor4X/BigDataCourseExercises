from hdfs import InsecureClient
from collections import Counter
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa

client = InsecureClient('http://namenode:9870', user='root')

# Make wordcount reachable outside of the with-statement
wordcount = None

with client.read('/alice-in-wonderland.txt', encoding='utf-8') as reader:
    wordcount = Counter(reader.read().split()).most_common(10)

# Create a DataFrame from the wordcount structure
df = pd.DataFrame(wordcount, columns=['word', 'count'])
# Create a table from the DataFrame
table = pa.Table.from_pandas(df)
# Write the table to a temporary file
pq.write_table(table, '/tmp/word-count.parquet')

client.upload('/word-count.parquet', '/tmp/word-count.parquet', overwrite=True)