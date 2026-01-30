from pathlib import Path

cr_dir = Path.cwd()
cr_file = Path(__file__).name

print(f"Files in {cr_dir}:")

for filepath in cr_dir.iterdir():
    if filepath.name == cr_file:
        continue

    print(f"  -  {filepath.name}")

    if filepath.is_file():
        content = filepath.read_text(encoding='utf-8')
        print(f"   Content: {content}")