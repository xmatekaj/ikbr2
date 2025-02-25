"""
Create missing __init__.py files in the project structure
"""
import os

print("Starting creation of __init__.py files")

# List of directories where __init__.py files should exist
directories = [
    'src',
    'src/config',
    'src/config/strategy_configs',
    'src/core',
    'src/connectors',
    'src/connectors/ibkr',
    'src/trading',
    'src/trading/order_types',
    'src/strategies',
    'src/strategies/conventional',
    'src/strategies/ml',
    'src/backtesting',
    'src/backtesting/optimizers',
    'src/data',
    'src/data/processors',
    'src/data/providers',
    'src/data/storage',
    'src/models',
    'src/utils',
    'src/utils/logging',
    'tests',
    'tests/unit',
    'tests/integration'
]

# Create __init__.py files in each directory if they don't exist
for directory in directories:
    os.makedirs(directory, exist_ok=True)
    init_file = os.path.join(directory, '__init__.py')
    
    if not os.path.exists(init_file):
        print(f"Creating {init_file}")
        with open(init_file, 'w') as f:
            f.write('"""Package initialization file."""\n')
    else:
        print(f"{init_file} already exists")

print("Finished creating __init__.py files")