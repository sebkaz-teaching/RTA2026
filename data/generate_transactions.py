"""
Generate 10,000 synthetic retail transactions for Lab 2 (Spark batch + windowing).

Timestamps span 3 hours: 08:00–11:00 on 2026-04-12.
Load distribution simulates a morning peak (09:15–10:30).
Output: transactions_10k.jsonl (one JSON object per line)
"""
import json
import random
import math
from datetime import datetime, timedelta

random_lognormal = lambda mu, sigma: math.exp(random.gauss(mu, sigma))

random.seed(42)

STORES = ['Warszawa', 'Kraków', 'Gdańsk', 'Wrocław']
CATEGORIES = ['elektronika', 'odzież', 'żywność', 'książki']

START = datetime(2026, 4, 12, 8, 0, 0)
TOTAL_SECONDS = 3 * 3600  # 08:00 – 11:00
N = 10_000


def morning_weight(t: float) -> float:
    """
    Load shape: slow ramp-up, peak around t=4800s (09:20), gradual drop.
    t in [0, 10800].
    """
    center = 4800
    width = 2400
    return 0.2 + math.exp(-((t - center) ** 2) / (2 * width ** 2))


def sample_timestamp() -> datetime:
    """Rejection-sample a timestamp with the morning weight distribution."""
    while True:
        t = random.uniform(0, TOTAL_SECONDS)
        if random.random() < morning_weight(t):
            return START + timedelta(seconds=t)


def generate_transaction(i: int) -> dict:
    store = random.choice(STORES)
    category = random.choice(CATEGORIES)

    # Amount: mostly small, occasional large (lognormal-ish)
    amount = round(random_lognormal(5.2, 1.1), 2)
    amount = max(5.0, min(amount, 9999.0))

    # Electronics skew higher
    if category == 'elektronika':
        amount = round(amount * 1.8, 2)
        amount = min(amount, 9999.0)

    ts = sample_timestamp()

    return {
        'tx_id': f'TX{i:05d}',
        'user_id': f'u{random.randint(1, 50):02d}',
        'amount': amount,
        'store': store,
        'category': category,
        'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
    }


output_path = 'transactions_10k.jsonl'
with open(output_path, 'w', encoding='utf-8') as f:
    for i in range(1, N + 1):
        tx = generate_transaction(i)
        f.write(json.dumps(tx, ensure_ascii=False) + '\n')

print(f"Generated {N} transactions → {output_path}")

# Quick sanity check
from collections import Counter
import json as _json

with open(output_path) as f:
    records = [_json.loads(line) for line in f]

stores = Counter(r['store'] for r in records)
cats = Counter(r['category'] for r in records)
amounts = [r['amount'] for r in records]

print(f"\nPer store:    {dict(stores)}")
print(f"Per category: {dict(cats)}")
print(f"Amount — min: {min(amounts):.2f}  max: {max(amounts):.2f}  "
      f"avg: {sum(amounts)/len(amounts):.2f}")

# Count per hour window
hours = Counter(r['timestamp'][:13] for r in records)
for h in sorted(hours):
    bar = '█' * (hours[h] // 50)
    print(f"  {h}:xx  {hours[h]:4d}  {bar}")
