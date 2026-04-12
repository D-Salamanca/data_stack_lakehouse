import clickhouse_connect

c = clickhouse_connect.get_client(
    host='play.clickhouse.com',
    port=443,
    username='play',
    password='',
    database='default',
    secure=True
)

print("=== Databases ===")
r = c.query('SHOW DATABASES')
for row in r.result_rows:
    print(f"  {row[0]}")

print("\n=== Tables in default ===")
r = c.query('SHOW TABLES IN default')
for row in r.result_rows:
    print(f"  {row[0]}")

# Check git_clickhouse
print("\n=== Tables in git_clickhouse ===")
r = c.query('SHOW TABLES IN git_clickhouse')
for row in r.result_rows:
    print(f"  {row[0]}")

# Check blogs
print("\n=== Tables in blogs ===")
r = c.query('SHOW TABLES IN blogs')
for row in r.result_rows:
    print(f"  {row[0]}")
