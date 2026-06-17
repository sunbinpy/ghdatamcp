import re
with open('server.py','r',encoding='utf-8') as f:
    content = f.read()
tools = re.findall(r'@mcp\.tool\(\)\s*\n@auth_required\s*\n@_rate_limit_decorator\s*\ndef (\w+)', content)
print(f'Total tools: {len(tools)}')
for t in tools:
    print(f'  - {t}')
has_main = 'def main():' in content
print(f'Has main(): {has_main}')
has_if = 'if __name__' in content
print(f'Has __main__: {has_if}')
print(f'Total lines: {content.count(chr(10))}')
