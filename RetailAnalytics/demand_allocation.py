import numpy as np
from scipy.optimize import linprog

# Sample data for stores and their demand
stores_demands = {'StoreA': 100, 'StoreB': 150, 'StoreC': 200}

# Costs associated with fulfilling demand from each store
costs = {'StoreA': 20, 'StoreB': 30, 'StoreC': 25}

# Coefficients for the linear programming problem (objective function)
# Minimize total cost = sum(cost[i] * x[i]) for each store
c = np.array([costs[store] for store in stores_demands.keys()])

# Bounds for the demand at each store (0 to demand)
# Each store can fulfill from 0 to its maximum demand
x_bounds = [(0, stores_demands[store]) for store in stores_demands.keys()]

# Linear programming optimization
result = linprog(c, bounds=x_bounds, method='highs')

if result.success:
    print('Optimal allocation:', result.x)
    print('Total cost:', result.fun)
else:
    print('Optimization failed:', result.message)