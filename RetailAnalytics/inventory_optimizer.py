import numpy as np
from scipy.optimize import linprog

class InventoryOptimizer:
    def __init__(self, demand, supply, costs):
        self.demand = demand
        self.supply = supply
        self.costs = costs

    def optimize_inventory(self):
        c = self.costs
        A = []
        b = []
        
        # Supply constraints
        for s in self.supply:
            A.append(s)
            b.append(supply[s])

        # Demand constraints
        for d in self.demand:
            A.append(-1 * np.array(d))
            b.append(-self.demand[d])

        A_eq = None
        b_eq = None

        # Solve the linear programming problem
        res = linprog(c, A_ub=A, b_ub=b, A_eq=A_eq, b_eq=b_eq, method='highs')
        if res.success:
            return res.x
        else:
            return None

# Example usage:
# demand = [100, 200]
# supply = [150, 250]
# costs = [5, 4]
# optimizer = InventoryOptimizer(demand, supply, costs)
# optimal_levels = optimizer.optimize_inventory()