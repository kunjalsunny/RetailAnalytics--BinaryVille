from scipy.optimize import linprog

class InventoryOptimizer:
    def __init__(self, costs, constraints, bounds):
        self.costs = costs
        self.constraints = constraints
        self.bounds = bounds

    def optimize(self):
        res = linprog(
            c=self.costs,
            A_ub=self.constraints['A'],
            b_ub=self.constraints['b'],
            bounds=self.bounds,
            method="highs"
        )

        if res.success:
            return res.x, res.fun
        else:
            raise ValueError("Optimization failed: " + res.message)

if __name__ == '__main__':
    costs = [1.5, 2.0, 2.5]

    constraints = {
        'A': [
            [1, 1, 1],
            [2, 1, 0],
            [0, 2, 1]
        ],
        'b': [100, 80, 60]
    }

    # Minimum required stock added
    bounds = [(10, None), (15, None), (20, None)]

    optimizer = InventoryOptimizer(costs, constraints, bounds)
    result, min_cost = optimizer.optimize()

    print("Optimized inventory levels:", result)
    print("Minimum total cost:", min_cost)
