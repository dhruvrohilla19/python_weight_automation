import pandas as pd
import numpy as np

data = pd.read_excel('Hydrogen Economy Weighting data.xlsx')

# Ensuring the format for numerical columns
data['FF*Mcap'] = pd.to_numeric(data['FF*Mcap'], errors='coerce')
data['QC Mcap'] = pd.to_numeric(data['QC Mcap'], errors='coerce')

# Assigning Raw Weight
data['Raw Weight'] = data['FF*Mcap'] / data['FF*Mcap'].sum()

#Counting the number of Securities that are either PURE or QUASI/MARGINAL
pure_securities = (data['Classification'] == 'Pure').sum()
quasi_marginal_securities = (data['Classification'] != 'Pure').sum()

# Boolean masks for each group (make sure to use lower-case comparison)
pure_classification = data['Classification'].str.lower() == 'pure'
quasi_marginal_classification = ~pure_classification

# Calculating sum of Raw Weights for each Sub Group: Quasi/Mrg and Pure
pure_sum = data.loc[pure_classification, 'Raw Weight'].sum()
qm_sum = data.loc[quasi_marginal_classification, 'Raw Weight'].sum()

# 4. Assign 60-40, 75-25, or 90-10 split to Adjusted Weight
if quasi_marginal_securities > 12:
    pure_target, qm_target = 0.6, 0.4
elif 6 <= quasi_marginal_securities <= 11:
    pure_target, qm_target = 0.75, 0.25
else:
    pure_target, qm_target = 0.9, 0.1

data['Adjusted Weight'] = 0.0
if pure_sum > 0:
    data.loc[pure_classification, 'Adjusted Weight'] = data.loc[pure_classification, 'Raw Weight'] * (pure_target / pure_sum)
if qm_sum > 0:
    data.loc[quasi_marginal_classification, 'Adjusted Weight'] = data.loc[quasi_marginal_classification, 'Raw Weight'] * (qm_target / qm_sum)

def apply_upper_cap_and_redistribute(df, mask, cap):
    # Repeat until no weights in the bucket are above the cap
    while True:
        over_cap = df[mask & (df['Capped Weight'] > cap)]
        if over_cap.empty:
            break
        excess = (over_cap['Capped Weight'] - cap).sum()
        # Set capped weights to cap
        df.loc[over_cap.index, 'Capped Weight'] = cap
        # Find uncapped (below cap) in the bucket
        uncapped = df[mask & (df['Capped Weight'] < cap)]
        if uncapped.empty or excess <= 0:
            break
        # Distribute excess proportionally to uncapped
        weights = uncapped['Capped Weight']
        ratio = weights / weights.sum()
        df.loc[uncapped.index, 'Capped Weight'] += excess * ratio
data['Capped Weight'] = data['Adjusted Weight']
apply_upper_cap_and_redistribute(data, pure_classification, 0.08)
apply_upper_cap_and_redistribute(data, quasi_marginal_classification, 0.08)
# data.loc[pure_classification & (data['Capped Weight'])>.08, 'Capped Weight'] = .08
# data.loc[quasi_marginal_classification & (data['Capped Weight']>.08), 'Capped Weight'] = .08

#Applying lower cap in each bucket
def apply_lower_cap(df, mask, minimum_value):
    capped = df[mask & (df['Capped Weight']==.08)]
    uncapped = df[mask&(df['Capped Weight']<.08)]
    required_value_raise = uncapped['Capped Weight'] < minimum_value
    total_raise = (minimum_value - uncapped.loc[required_value_raise, 'Capped Weight']).sum()
    #Setting Lower Cap
    df.loc[mask & (df['Capped Weight']<minimum_value), 'Capped Weight'] = minimum_value

    #Reducing uncapped values above minimum proportionately
    uncapped_above_min_val = uncapped[~required_value_raise]
    if not uncapped_above_min_val.empty and total_raise>0:
        weights = uncapped_above_min_val['Capped Weight']
        ratio = weights/weights.sum()
        df.loc[uncapped_above_min_val.index, 'Capped Weight'] -= total_raise*ratio
        #If any of the records go below the minimum capping, we reset to minimum cap and go through the process again
        while(df.loc[uncapped_above_min_val.index, 'Capped Weight'] < minimum_value).any():
            required_value_raise = df.loc[uncapped_above_min_val.index, 'Capped Weight']<minimum_value
            extra_weight = (minimum_value - df.loc[uncapped_above_min_val.index[required_value_raise], 'Capped Weight']).sum()
            df.loc[uncapped_above_min_val.index[required_value_raise], 'Capped Weight'] = minimum_value
            uncapped_above_min_val = df.loc[uncapped_above_min_val.index[~required_value_raise]]
            if uncapped_above_min_val.empty or uncapped_above_min_val <= 0:
                break
            weights = uncapped_above_min_val['Capped Weight']
            ratio = weights / weights.sum()
            df.loc[uncapped_above_min_val.index, 'Capped Weight'] -= extra_weight * ratio

apply_lower_cap(data, pure_classification, 0.01)
apply_lower_cap(data, quasi_marginal_classification, 0.005)

#Applying RIC-RULE: Agg 45% cap on weights>=5%, redistributing the remaining weights to uncapped securities in the same bucket: Quasi/Marginal or PURE
def apply_ric_rule(df):
    ric_mask = df['Capped Weight']>=.5
    ric_sum = df.loc[ric_mask, 'Capped Weight'].sum()
    if ric_sum>0.45:
        #Sorting by weight descending
        sorted_values = df.loc[ric_mask, 'Capped Weight'].sort_values(ascending=False).index
        excess_weight = ric_sum - .45
        for i in sorted_values:
            weight = df.at[i, 'Capped Weight']
            reduction = min(weight-.05, excess_weight)
            if reduction>0:
                df.at[i, 'Capped Weight'] -= reduction
                excess_weight-=reduction
            if excess_weight<0:
                break
        
        #Redistribution in the same bucket or category of the securities
        for bucket_mask in [pure_classification, quasi_marginal_classification]:
            uncapped = df[bucket_mask&(df['Capped Weight']<.05)]
            if not uncapped.empty and excess_weight>0:
                weights = uncapped['Capped Weight']
                ratio = weights/weights.sum()
                df.loc[uncapped.index, 'Capped Weight'] += excess_weight*ratio
                #If any of the weights go above .05, we set the cap to .05 and repeat the process
                while (df.loc[uncapped.index, 'Capped Weight'] > 0.05).any():
                    over = df.loc[uncapped.index, 'Capped Weight'] > 0.05
                    over_amt = (df.loc[uncapped.index[over], 'Capped Weight'] - 0.05).sum()
                    df.loc[uncapped.index[over], 'Capped Weight'] = 0.05
                    uncapped = df[bucket_mask & (df['Capped Weight'] < 0.05)]
                    if uncapped.empty or over_amt <= 0:
                        break
                    weights = uncapped['Capped Weight']
                    ratio = weights / weights.sum()
                    df.loc[uncapped.index, 'Capped Weight'] += over_amt * ratio
apply_ric_rule(data)

data['Final Weight'] = data['Capped Weight'] / data['Capped Weight'].sum()
data.to_excel('Final_Edited_Output_(2).xlsx', index=False)
print(f"Program run successfully!")
