import pandas as pd
import numpy as np
data = pd.read_excel('Hydrogen Economy Weighting data.xlsx')

#Ensuring the format for numerical columns
data['FF*Mcap'] = pd.to_numeric(data['FF*Mcap'], errors='coerce')
data['QC Mcap'] = pd.to_numeric(data['QC Mcap'], errors='coerce')

#Assigning Raw Weight
data['Raw Weight'] = data['FF*Mcap'] / data['FF*Mcap'].sum()

pure_securities = (data['Classification'] == 'Pure').sum()
quasi_marginal_securities = (data['Classification']!= 'Pure').sum()

#Applying minimum weights
def apply_minimum_weights(row):
    if row['Classification'] == 'Pure':
        return max(row['Raw Weight'], 0.01 / pure_securities)
    else:
        return max(row['Raw Weight'], 0.005 / quasi_marginal_securities)
    
data['Initial Weight'] = data.apply(apply_minimum_weights, axis=1)

#Security Capping of 8%
data['Capped Weight'] = data['Initial Weight'].clip(upper=0.08)

#Applying 5% Aggregate Cap
total_weight_over5 = data[data['Capped Weight'] > 0.05]['Capped Weight'].sum()
if total_weight_over5 > 0.45:
    data['Capped Weight'] = data['Capped Weight'].apply(lambda x: min(x, 0.045))
    #Applying a secondary cap of 4.5% ^

if quasi_marginal_securities > 12:
    qm_cap = .40
elif 6<= quasi_marginal_securities <=11:
    qm_cap = .25
else:
    qm_cap = .1
qm_total = data[data['Classification']!='Pure']['Capped Weight'].sum()
if qm_total > qm_cap:
    scale = qm_cap/qm_total
    data.loc[data['Classification']!='Pure', 'Capped Weight'] *= scale

data['Final Weight'] = data['Capped Weight'] / data['Capped Weight'].sum()
data['Buffer Eligibility'] = data['QC Mcap'] >=80

data.to_excel("Output.xlsx", index=False)