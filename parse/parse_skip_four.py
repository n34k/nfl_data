import json

with open('./data/player_games.json', 'r') as f:
    data = json.load(f)

# Keep every 5th entry (skip 4, take 1)
filtered = data[::7]

with open('./data/games_sampled.json', 'w') as f:
    json.dump(filtered, f, indent=2)

print(f'Original:  {len(data)} entries')
print(f'Sampled:   {len(filtered)} entries')
print(f'Reduction: {100 - (len(filtered) / len(data) * 100):.1f}%')