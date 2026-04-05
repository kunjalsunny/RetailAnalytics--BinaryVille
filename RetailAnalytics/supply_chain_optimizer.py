import numpy as np
from scipy.optimize import linprog

class SupplyChainOptimizer:
    def __init__(self, cost_matrix, supply, demand):
        self.cost_matrix = cost_matrix
        self.supply = supply
        self.demand = demand

    def optimize(self):
        # Number of warehouses and retailers
        num_warehouses = len(self.supply)
        num_retailers = len(self.demand)

        # Coefficients of the linear objective function vector (costs)
        c = self.cost_matrix.flatten()

        # Constraints for supply (<=)
        A_eq = []
        b_eq = []
        for i in range(num_warehouses):
            A_eq.append([1 if j // num_retailers == i else 0 for j in range(num_warehouses * num_retailers)])
            b_eq.append(self.supply[i])

        # Constraints for demand (>=)
        for j in range(num_retailers):
            A_eq.append([1 if j % num_retailers == j else 0 for j in range(num_warehouses * num_retailers)])
            b_eq.append(self.demand[j])

        # Convert into numpy array
        A_eq = np.array(A_eq)
        b_eq = np.array(b_eq)

        # Bounds for each variable (0 to infinity)
        x_bounds = [(0, None)] * (num_warehouses * num_retailers)

        # Solve the linear programming problem
        result = linprog(c, A_eq=A_eq, b_eq=b_eq, bounds=x_bounds, method='highs')

        if result.success:
            return result.x.reshape(num_warehouses, num_retailers)
        else:
            raise ValueError('Optimization failed')

# Example usage:
# cost_matrix = np.array([[2, 3, 1], [5, 4, 8]])
# supply = [20, 30]
# demand = [15, 25, 10]
# optimizer = SupplyChainOptimizer(cost_matrix, supply, demand)
# result = optimizer.optimize()
# print(result)