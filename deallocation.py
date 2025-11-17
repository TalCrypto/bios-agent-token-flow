import pandas as pd
import numpy as np
from collections import Counter

# Load CSV
df = pd.read_csv("token.csv")

acp_contract_address = "0xa6c9ba866992cfd7fd6460ba912bfa405ada9df0" 
meta_vault_address = "0xea98fd8d0e4515fe5989ebe2b666264d25d1021c"
agent_address = "0xa908b2a981ae1106e5b047ef44604ccaa1e1c7f9"
zero_address = "0x0000000000000000000000000000000000000000"

over_redeemed_values = [771.297969, 1542.23099, 2289.540866]
un_redeemed_values = [1029.946276, 2487.47293, 3613.43577]

# Extract filtered dataframes
inflow_df = df.loc[(df['TokenSymbol'] == 'ERC20 ***') & (df['From'] == acp_contract_address) & (df['To'] == agent_address)].copy()
outflow_df = df.loc[(df['TokenSymbol'] == 'ERC20 ***') & (df['From'] == agent_address) & (df['To'] == zero_address)].copy()

# Convert TokenValue to numeric (remove commas first)
inflow_df['TokenValue'] = pd.to_numeric(inflow_df['TokenValue'].astype(str).str.replace(',', ''), errors='coerce')
outflow_df['TokenValue'] = pd.to_numeric(outflow_df['TokenValue'].astype(str).str.replace(',', ''), errors='coerce')

# Within outflow_df, merge rows with the same Transaction Hash while summing the TokenValue and keeping the remaining columns with the last one
outflow_df = outflow_df.groupby('Transaction Hash').agg({
    'TokenValue': 'sum',
    'UnixTimestamp': 'last',
    'From': 'last',
    'To': 'last',
    'TokenSymbol': 'last',
}).reset_index()

# Cut TokenValue to 6 decimal places
outflow_df['TokenValue'] = outflow_df['TokenValue'].apply(lambda x: round(x, 6))

# Calculate sums
shares_inflow = inflow_df['TokenValue'].sum()
shares_outflow = outflow_df['TokenValue'].sum()

over_redeemed_outflow_df = outflow_df[outflow_df['TokenValue'].isin(over_redeemed_values)].copy().sort_values(by='UnixTimestamp', ascending=True)
print_df = over_redeemed_outflow_df[['UnixTimestamp','Transaction Hash', 'TokenValue']]
print_df.to_csv('over_redeemed_outflow.csv', index=False)

un_redeemed_inflow_df = inflow_df[inflow_df['TokenValue'].isin(un_redeemed_values)].copy().sort_values(by='UnixTimestamp', ascending=True)
print_df = un_redeemed_inflow_df[['UnixTimestamp','Transaction Hash', 'TokenValue']]
print_df.to_csv('un_redeemed_inflow.csv', index=False)


# Check for exact matching token values
inflow_values = sorted(inflow_df['TokenValue'].dropna().tolist())
outflow_values = sorted(outflow_df['TokenValue'].dropna().tolist())

print(f"Inflow count: {len(inflow_values)}")
print(f"Outflow count: {len(outflow_values)}")
print(f"Values match exactly: {inflow_values == outflow_values}")
print(f"Sum match: {shares_inflow == shares_outflow}")
print(f"Imbalance: {shares_inflow - shares_outflow}")

if inflow_values != outflow_values:
    print("\nDifferences found:")
        
    # Show value counts
    inflow_counts = Counter(inflow_values)
    outflow_counts = Counter(outflow_values)
    
    print("\nValue frequency comparison:")
    all_values = set(inflow_values) | set(outflow_values)
    for val in sorted(all_values):
        in_count = inflow_counts.get(val, 0)
        out_count = outflow_counts.get(val, 0)
        if in_count != out_count:
            print(f"  Value {val}: inflow={in_count}, outflow={out_count}")
print()

