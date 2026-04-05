def allocate_supply_to_stores(demand, supply):
    allocation = {}
    total_demand = sum(demand.values())
    total_supply = supply

    if total_supply < total_demand:
        raise ValueError("Insufficient supply to meet demand.")

    for store, demand_amount in demand.items():
        allocation[store] = (demand_amount / total_demand) * total_supply

    return allocation

# Example usage
if __name__ == '__main__':
    forecasted_demand = {
        'Store A': 100,
        'Store B': 150,
        'Store C': 200
    }
    total_supply_available = 350
    try:
        allocation_result = allocate_supply_to_stores(forecasted_demand, total_supply_available)
        print("Supply allocation to stores:", allocation_result)
    except ValueError as e:
        print(e)