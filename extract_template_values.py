import json

# Load the JSON file
with open('tv_study_templates_decompressed/test.json', 'r') as file:
    data = json.load(file)

# Print panes
#print("Panes:")
#print(json.dumps(data['panes'], indent=2))

# Traverse sources to print id, metaInfo.name, state.symbol
print("\nSources details:")
for pane_index, pane in enumerate(data['panes']):
    print(f"\nPane {pane_index}:")
    for source in pane.get('sources', []):
        print(f"  Source ID: {source.get('id', 'N/A')}")
        if 'metaInfo' in source:
            print(f"    Name: {source['metaInfo'].get('name', 'N/A')}")
        else:
            print("    Name: N/A")
        if 'state' in source and 'symbol' in source['state']:
            print(f"    Symbol: {source['state']['symbol']}")
        else:
            print("    Symbol: N/A")
